import static org.junit.jupiter.api.Assertions.*;

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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/// Real Postgres, real Kafka, the real Relay. Each test writes rows the way
/// the application would (orders + outbox in one statement), calls
/// `relay.sweep()`, and reads the topic back.
class RelayTest {

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6-alpine")
            .withUsername("outbox").withPassword("outbox").withDatabaseName("outbox")
            .withCopyFileToContainer(MountableFile.forHostPath("schema.sql"), "/docker-entrypoint-initdb.d/schema.sql");

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    static Connection db;
    static Relay relay;

    @BeforeAll
    static void start() throws Exception {
        Startables.deepStart(POSTGRES, KAFKA).join();
        try (var admin = Admin.create(Map.of("bootstrap.servers", (Object) KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(Relay.TOPIC, 3, (short) 1))).all().get();
        }
        db = connect();
        relay = new Relay(connect(), Relay.producer(KAFKA.getBootstrapServers()));
    }

    @AfterAll
    static void stop() {
        KAFKA.stop();
        POSTGRES.stop();
    }

    static KafkaConsumer<String, String> topic;

    /// Empty tables, and a consumer parked at the end of the topic so each test
    /// reads back only what it produced.
    @BeforeEach
    void startClean() throws Exception {
        db.createStatement().execute("TRUNCATE orders, outbox RESTART IDENTITY");
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

    @Test
    void publishesTheOrderEventWithKeyHeadersAndPayload() throws Exception {
        var orderId = insertOrder(db, "cust-1");

        assertEquals(1, relay.sweep());

        var records = consume(1);
        assertEquals(1, records.size());
        var record = records.getFirst();
        assertEquals(orderId, record.key(), "key is the aggregate id");
        assertEquals("order.created", header(record, "event_type"));
        assertEquals(scalar("SELECT event_id::text FROM outbox"), header(record, "event_id"));
        assertTrue(record.value().contains("\"customer_id\": \"cust-1\""), "value is the payload as written");
        assertEquals("1", scalar("SELECT count(*) FROM outbox WHERE published_at IS NOT NULL"));
    }

    /// Tx A takes the lower id but commits after tx B. A cursor on id would skip A; the relay does not.
    @Test
    void aRowThatBecomesVisibleLateIsStillPublished() throws Exception {
        var a = connect();
        a.setAutoCommit(false);
        insertOutbox(a, "A");   // id 1, not committed: invisible to the relay
        insertOutbox(db, "B");  // id 2, committed

        assertEquals(1, relay.sweep(), "only B is visible");
        assertEquals(Set.of("B"), keys(consume(1)));

        a.commit();             // now id 1 appears, below the highest id already published

        assertEquals(1, relay.sweep(), "A is picked up because it is unpublished, not because of its id");
        var records = consume(2);
        assertEquals(2, records.size());
        assertEquals(Set.of("A", "B"), keys(records));
        assertEquals("0", scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL"));
    }

    /// Broker unreachable: the row stays unpublished and the next sweep retries it.
    /// The first attempt may still land on the broker once it is back (the bytes were
    /// already on the wire), so the topic can hold the event twice with the same
    /// event_id. That duplicate is the at-least-once in "at-least-once"; consumers
    /// dedupe on event_id.
    @Test
    void marksPublishedOnlyAfterTheBrokerAcks() throws Exception {
        insertOrder(db, "cust-2");
        var docker = KAFKA.getDockerClient();
        docker.pauseContainerCmd(KAFKA.getContainerId()).exec();
        try {
            assertEquals(0, relay.sweep(), "nothing acked, nothing marked");
            assertEquals("1", scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL"));
        } finally {
            docker.unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }

        assertEquals(1, relay.sweep(), "same row, published on the next sweep");
        var records = consume(1);
        assertTrue(records.size() >= 1);
        assertEquals(1, records.stream().map(r -> header(r, "event_id")).distinct().count(),
                "every copy carries the same event_id");
        assertEquals(scalar("SELECT event_id::text FROM outbox"), header(records.getFirst(), "event_id"));
    }

    // --- writing the way the application does ---------------------------------

    /// The orders row and its outbox row in ONE statement: atomic by construction.
    static String insertOrder(Connection c, String customer) throws Exception {
        var rs = c.createStatement().executeQuery("""
                WITH o AS (INSERT INTO orders (id, customer_id, amount_cents)
                           VALUES (gen_random_uuid(), '%s', 1000) RETURNING *)
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                SELECT gen_random_uuid(), o.id, 'order.created', to_jsonb(o) FROM o
                RETURNING aggregate_id""".formatted(customer));
        rs.next();
        return rs.getString(1);
    }

    static void insertOutbox(Connection c, String key) throws Exception {
        c.createStatement().execute("""
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                VALUES (gen_random_uuid(), '%s', 'order.created', '{}')""".formatted(key));
    }

    // --- reading both sides -----------------------------------------------------

    static final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    /// Everything published since the test started. Waits up to 10 s for at least
    /// `atLeast` records, then half a second more so stragglers (duplicates) show up too.
    static List<ConsumerRecord<String, String>> consume(int atLeast) {
        var deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (received.size() < atLeast && System.nanoTime() < deadline) {
            poll();
        }
        var grace = System.nanoTime() + Duration.ofMillis(500).toNanos();
        while (System.nanoTime() < grace) {
            poll();
        }
        return received;
    }

    static void poll() {
        topic.poll(Duration.ofMillis(200)).forEach(r -> {
            received.add(r);
            System.out.printf("  topic <- key=%s event_type=%s event_id=%s value=%s%n",
                    r.key(), header(r, "event_type"), header(r, "event_id"), r.value());
        });
    }

    static Set<String> keys(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::key).collect(Collectors.toSet());
    }

    static String header(ConsumerRecord<String, String> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    static String scalar(String sql) throws Exception {
        var rs = db.createStatement().executeQuery(sql);
        rs.next();
        return rs.getString(1);
    }

    static Connection connect() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), "outbox", "outbox");
    }
}
