import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/// Polls `outbox` for rows with `published_at IS NULL`, sends each to Kafka,
/// and only after the broker acks marks the row published. No cursor: a row
/// that becomes visible late (long transaction) is still picked up, because
/// the question is "is it published?", not "is its id above where I stopped?".
public class Relay {

    static final String TOPIC = "order.events";
    static final int BATCH = 100;
    static final long POLL_MS = 1000;

    public static void main(String[] args) throws Exception {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");                 // weaker lets a leader election drop a row already marked published
        props.put("retries", "0");                // the relay owns retries: the row stays unpublished and is swept again
        props.put("enable.idempotence", "false"); // idempotent producer requires retries > 0
        props.put("max.block.ms", "5000");

        try (var kafka = new KafkaProducer<String, String>(props);
             var db = DriverManager.getConnection("jdbc:postgresql://localhost:5432/outbox", "outbox", "outbox")) {
            db.setAutoCommit(false);
            System.out.println("relay up: polling every " + POLL_MS + "ms");
            while (true) {
                try {
                    while (sweep(db, kafka) == BATCH) {} // drain the backlog before sleeping
                } catch (Exception e) {
                    db.rollback();
                    System.err.println("sweep failed, retrying next poll: " + e);
                }
                Thread.sleep(POLL_MS);
            }
        }
    }

    static int sweep(Connection db, KafkaProducer<String, String> kafka) throws Exception {
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
                    System.err.println("publish failed for " + rs.getString("event_id") + ", stopping batch: " + e);
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
            System.out.println("published " + published.size());
        }
        db.commit();
        return published.size();
    }
}
