import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/// The plumbing: real Postgres, real Kafka, the real Relay, a consumer parked at
/// the end of the topic. Nothing here is the pattern; the pattern is in RelayTest.
class Env {

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6-alpine")
            .withUsername("outbox").withPassword("outbox").withDatabaseName("outbox")
            .withCopyFileToContainer(MountableFile.forHostPath("schema.sql"), "/docker-entrypoint-initdb.d/schema.sql");

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    static Connection db;
    static Relay relay;

    /// Three partitions so the "same aggregate, same partition" test means something.
    @BeforeAll
    static void start() throws Exception {
        Startables.deepStart(POSTGRES, KAFKA).join();
        db = connect();
        try (var admin = Admin.create(Map.of("bootstrap.servers", (Object) KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(Relay.TOPIC, 3, (short) 1))).all().get();
        }
        relay = new Relay(connect(), Relay.producer(KAFKA.getBootstrapServers()));
    }

    static Connection connect() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), "outbox", "outbox");
    }

    // --- writing the way the application does --------------------------------------

    /// The orders row and its outbox row in ONE statement: atomic by construction.
    static String insertOrder(Connection c, String customer) throws Exception {
        return scalar(c, """
                WITH o AS (INSERT INTO orders (id, customer_id, amount_cents)
                           VALUES (gen_random_uuid(), '%s', 1000) RETURNING *)
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                SELECT gen_random_uuid(), o.id, 'order.created', to_jsonb(o) FROM o
                RETURNING aggregate_id""".formatted(customer));
    }

    /// Same shape for a change to an existing order.
    static void updateOrder(Connection c, String orderId) throws Exception {
        scalar(c, """
                WITH o AS (UPDATE orders SET amount_cents = 2000 WHERE id = '%s' RETURNING *)
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                SELECT gen_random_uuid(), o.id, 'order.updated', to_jsonb(o) FROM o
                RETURNING aggregate_id""".formatted(orderId));
    }

    static String scalar(String sql) throws Exception {
        return scalar(db, sql);
    }

    static String scalar(Connection c, String sql) throws Exception {
        var rs = c.createStatement().executeQuery(sql);
        rs.next();
        return rs.getString(1);
    }

    // --- reading the topic -----------------------------------------------------------

    static KafkaConsumer<String, String> topic;
    static final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    /// Empty tables, and a consumer parked at the end of the topic so each test
    /// reads back only what it produced.
    @BeforeEach
    void startClean() throws Exception {
        db.createStatement().execute("SET lock_timeout = '5s'; TRUNCATE orders, outbox RESTART IDENTITY");
        var props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        received.clear();
        topic = new KafkaConsumer<>(props);
        var partitions = topic.partitionsFor(Relay.TOPIC).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
        topic.assign(partitions);
        topic.seekToEnd(partitions);
        partitions.forEach(topic::position); // seekToEnd is lazy; force it before the test writes anything
    }

    @AfterEach
    void closeConsumer() {
        topic.close();
    }

    /// Everything published since the test started. Waits up to 10 s for at least
    /// `atLeast` records, then a second more so stragglers (duplicates) show up too.
    static List<ConsumerRecord<String, String>> consume(int atLeast) {
        var deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (received.size() < atLeast && System.nanoTime() < deadline) {
            poll();
        }
        var grace = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (System.nanoTime() < grace) {
            poll();
        }
        return received;
    }

    static void poll() {
        topic.poll(Duration.ofMillis(200)).forEach(r -> {
            received.add(r);
            System.out.printf("  topic <- partition=%d key=%s event_type=%s event_id=%s value=%s%n",
                    r.partition(), r.key(), header(r, "event_type"), header(r, "event_id"), r.value());
        });
    }

    static List<String> keys(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::key).toList();
    }

    static Set<String> partitions(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(r -> String.valueOf(r.partition())).collect(Collectors.toSet());
    }

    static String header(ConsumerRecord<String, String> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
