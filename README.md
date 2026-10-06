# outbox-relay

Transactional outbox drained by our own poller. `orders` row and `outbox` row are written
in one statement; `Relay.java` polls `WHERE published_at IS NULL`, sends to Kafka with
`acks=all`, then sets `published_at`. ~70 lines, plain JDBC + kafka-clients.

```sh
make up            # postgres + kafka, schema, topic
make run           # the relay, ctrl-c to stop
make order N=5     # 5 orders, each with its outbox row
make consume       # key = aggregate_id, headers = event_id/event_type, value = payload
make race          # a slow tx with a lower id commits after a fast one: both still arrive
```

Guarantees: at-least-once. A row is marked published only after the broker acks; a crash
between ack and `UPDATE` republishes it. Consumers dedupe on the `event_id` header.

Siblings: [outbox-debezium](../outbox-debezium) (CDC, same table) and
[outbox-jdbc-source](../outbox-jdbc-source) (the Kafka Connect poller that loses events).
