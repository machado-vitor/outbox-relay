# outbox-relay

Transactional outbox drained by our own poller. `orders` row and `outbox` row are written
in one statement; `Relay.sweep()` reads `WHERE published_at IS NULL`, sends to Kafka with
`acks=all`, then sets `published_at`. ~80 lines, plain JDBC + kafka-clients.

```sh
mvn test
```

That is the whole demo. `RelayTest` starts a real Postgres and a real Kafka (Testcontainers),
writes rows the way an application would, runs the relay, and prints what reached the topic:

| test | shows |
|---|---|
| `publishesTheOrderEventWithKeyHeadersAndPayload` | key = `aggregate_id`, headers `event_id` / `event_type`, value = payload; row gets `published_at` |
| `aRowThatBecomesVisibleLateIsStillPublished` | tx A takes the lower id but commits after tx B. A cursor on `id` would skip A; the relay asks "is it unpublished?" and finds it |
| `marksPublishedOnlyAfterTheBrokerAcks` | broker paused: the row stays unpublished; broker back: next sweep publishes it. The first attempt can still land, so the topic may hold the event twice with the same `event_id` — that is at-least-once, consumers dedupe |

To run it for real against your own Postgres and Kafka:

```sh
PG_URL=jdbc:postgresql://localhost:5432/outbox KAFKA=localhost:9092 mvn -q compile exec:java -Dexec.mainClass=Relay
```

Siblings: [outbox-debezium](../outbox-debezium) (CDC, same table) and
[outbox-jdbc-source](../outbox-jdbc-source) (the Kafka Connect poller that loses events).
