# outbox-relay

Transactional outbox drained by our own poller. `orders` row and `outbox` row are written
in one statement; `Relay.sweep()` reads `WHERE published_at IS NULL`, sends to Kafka with
`acks=all`, then sets `published_at`. ~70 lines, plain JDBC + kafka-clients.

```sh
mvn test
```

That is the whole demo. `RelayTest` (infrastructure in `Env`) starts a real Postgres and a real Kafka (Testcontainers),
writes rows the way an application would, runs the relay, and prints what reached the topic:

| test | shows |
|---|---|
| `publishesTheOrderEventWithKeyHeadersAndPayload` | key = `aggregate_id`, headers `event_id` / `event_type`, value = payload; row gets `published_at`, and the next sweep finds nothing |
| `aRolledBackTransactionPublishesNothing` | the outbox row lives in the business transaction, so a rollback takes the event with it. There is nothing for the relay to find |
| `aRowThatBecomesVisibleLateIsStillPublished` | tx A takes the lower id but commits after tx B. A cursor on `id` would skip A; the relay asks "is it unpublished?" and finds it |
| `eventsOfOneAggregateShareAPartitionInOrder` | key = `aggregate_id`, so every event of one order lands on one partition, in order |
| `marksPublishedOnlyAfterTheBrokerAcks` | broker paused: the row stays unpublished; broker back: next sweep publishes it. The first attempt can still land, so the topic may hold the event twice with the same `event_id` — that is at-least-once, consumers dedupe |

To run it for real against your own Postgres and Kafka:

```sh
PG_URL=jdbc:postgresql://localhost:5432/outbox KAFKA=localhost:9092 mvn -q compile exec:java -Dexec.mainClass=Relay
```

Siblings: [outbox-debezium](https://github.com/machado-vitor/outbox-debezium) (CDC, same table) and
[outbox-jdbc-source](https://github.com/machado-vitor/outbox-jdbc-source) (the Kafka Connect poller that loses events).
