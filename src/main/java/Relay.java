import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/// Sweeps `outbox` for rows with `published_at IS NULL`, sends each to Kafka,
/// and only after the broker acks marks the row published. No cursor: a row
/// that becomes visible late (long transaction) is still picked up, because
/// the question is "is it published?", not "is its id above where I stopped?".
public class Relay {

    public static final String TOPIC = "order.events";
    static final int BATCH = 100;

    private final Connection db;
    private final KafkaProducer<String, String> kafka;

    public Relay(Connection db, KafkaProducer<String, String> kafka) throws Exception {
        this.db = db;
        this.kafka = kafka;
        db.setAutoCommit(false);
    }

    public static KafkaProducer<String, String> producer(String bootstrapServers) {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");                 // weaker lets a leader election drop a row already marked published
        props.put("retries", "0");                // the relay owns retries: the row stays unpublished and is swept again
        props.put("enable.idempotence", "false"); // idempotent producer requires retries > 0
        props.put("max.block.ms", "3000");
        props.put("request.timeout.ms", "2000");
        props.put("delivery.timeout.ms", "3000");
        return new KafkaProducer<>(props);
    }

    /// One pass: claim up to BATCH unpublished rows, publish, mark. Returns how many were marked.
    public int sweep() throws Exception {
        var published = new ArrayList<Long>();
        try (var st = db.prepareStatement("""
                SELECT id, event_id, aggregate_id, event_type, payload
                FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT ?""")) {
            st.setInt(1, BATCH);
            var rs = st.executeQuery();
            while (rs.next()) {
                var record = new ProducerRecord<>(TOPIC, rs.getString("aggregate_id"), rs.getString("payload"));
                record.headers().add("event_id", rs.getString("event_id").getBytes(StandardCharsets.UTF_8));
                record.headers().add("event_type", rs.getString("event_type").getBytes(StandardCharsets.UTF_8));
                try {
                    kafka.send(record).get(); // blocks until acks=all
                } catch (Exception e) {
                    System.err.println("publish failed for " + rs.getString("event_id") + ", stopping batch: " + e.getMessage());
                    break; // rows already acked still get marked below; this one stays queued
                }
                published.add(rs.getLong("id"));
            }
        }
        if (!published.isEmpty()) {
            try (var st = db.prepareStatement("UPDATE outbox SET published_at = now() WHERE id = ANY (?)")) {
                st.setArray(1, db.createArrayOf("bigint", published.toArray()));
                st.executeUpdate();
            }
        }
        db.commit();
        return published.size();
    }

    /// Runs forever: drain the backlog, sleep a second, repeat.
    /// PG_URL / KAFKA env vars override the local defaults.
    public static void main(String[] args) throws Exception {
        var pg = System.getenv().getOrDefault("PG_URL", "jdbc:postgresql://localhost:5432/outbox");
        var kafka = System.getenv().getOrDefault("KAFKA", "localhost:9092");
        var relay = new Relay(DriverManager.getConnection(pg, "outbox", "outbox"), producer(kafka));
        while (true) {
            try {
                while (relay.sweep() == BATCH) {}
            } catch (Exception e) {
                relay.db.rollback();
                System.err.println("sweep failed, retrying next poll: " + e.getMessage());
            }
            Thread.sleep(1000);
        }
    }
}
