import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

/// Each test writes rows the way the application would (orders + outbox in one
/// statement), calls `relay.sweep()`, and reads the topic back. Infrastructure lives in Env.
class RelayTest extends Env {

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
        assertEquals(0, relay.sweep(), "published_at is the cursor: nothing left to do");
    }

    /// The dual-write problem, solved: the outbox row lives in the business transaction,
    /// so a rollback takes the event with it. There is nothing for the relay to find.
    @Test
    void aRolledBackTransactionPublishesNothing() throws Exception {
        try (var tx = connect()) {
            tx.setAutoCommit(false);
            insertOrder(tx, "rolled-back");
            tx.rollback();
        }
        var committed = insertOrder(db, "committed");

        assertEquals(1, relay.sweep());
        assertEquals(List.of(committed), keys(consume(1)), "only the committed order reaches the topic");
        assertEquals("1", scalar("SELECT count(*) FROM outbox"));
    }

    /// Tx A takes the lower id but commits after tx B. A cursor on id would skip A; the relay does not.
    @Test
    void aRowThatBecomesVisibleLateIsStillPublished() throws Exception {
        try (var a = connect()) {
            a.setAutoCommit(false);
            var idA = insertOrder(a, "A");   // lower id, not committed: invisible to the relay
            var idB = insertOrder(db, "B");  // higher id, committed

            assertEquals(1, relay.sweep(), "only B is visible");
            assertEquals(List.of(idB), keys(consume(1)));

            a.commit();                      // now A appears, below the highest id already published

            assertEquals(1, relay.sweep(), "A is picked up because it is unpublished, not because of its id");
            assertEquals(List.of(idB, idA), keys(consume(2)));
            assertEquals(idA, scalar("SELECT aggregate_id FROM outbox ORDER BY id LIMIT 1"), "table is in id order");
            assertEquals("0", scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL"));
        }
    }

    /// key = aggregate_id, so every event of one order lands on one partition, in order.
    @Test
    void eventsOfOneAggregateShareAPartitionInOrder() throws Exception {
        var orderId = insertOrder(db, "cust-1");
        updateOrder(db, orderId);

        assertEquals(2, relay.sweep());

        var records = consume(2);
        assertEquals(List.of(orderId, orderId), keys(records));
        assertEquals(1, partitions(records).size(), "one aggregate, one partition");
        assertEquals(List.of("order.created", "order.updated"),
                records.stream().map(r -> header(r, "event_type")).toList());
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
            assertThrows(Exception.class, relay::sweep, "nothing acked: the sweep aborts, nothing is marked");
            assertEquals("1", scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL"));
        } finally {
            docker.unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }

        assertEquals(1, relay.sweep(), "same row, published on the next sweep");
        var records = consume(1);
        assertEquals(1, records.stream().map(r -> header(r, "event_id")).distinct().count(),
                "every copy carries the same event_id");
        assertEquals(scalar("SELECT event_id::text FROM outbox"), header(records.getFirst(), "event_id"));
    }
}
