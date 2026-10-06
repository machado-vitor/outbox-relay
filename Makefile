TOPIC := order.events
KAFKA := docker compose exec -T kafka /opt/kafka/bin
PSQL  := docker compose exec -T postgres psql -U outbox -d outbox -v ON_ERROR_STOP=1 -qtA

.DEFAULT_GOAL := help
.PHONY: help up down reset psql order outbox consume run race

help:            ## list targets
	@grep -hE '^[a-z-]+:.*?##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | expand -t18

up:              ## start containers, apply schema, create topic
	docker compose up -d --wait
	@$(PSQL) < schema.sql
	@$(KAFKA)/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions 3 --replication-factor 1

down:            ## stop, keep data
	docker compose down

reset:           ## stop, wipe data
	docker compose down -v

psql:            ## shell on postgres
	docker compose exec postgres psql -U outbox -d outbox

order:           ## make order N=5 -- N orders, each with its outbox row in ONE statement
	@for i in $$(seq 1 $(or $(N),1)); do $(PSQL) -c "\
	  WITH o AS (INSERT INTO orders (id, customer_id, amount_cents) \
	             VALUES (gen_random_uuid(), 'cust-'||$$i, 1000) RETURNING *) \
	  INSERT INTO outbox (event_id, aggregate_id, event_type, payload) \
	  SELECT gen_random_uuid(), o.id, 'order.created', to_jsonb(o) FROM o"; done
	@echo "inserted $(or $(N),1) order(s)"

outbox:          ## rows by state
	@$(PSQL) -c "SELECT id, aggregate_id, published_at FROM outbox ORDER BY id"

consume:         ## tail the topic with key + headers
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic $(TOPIC) --from-beginning \
	  --formatter-property print.key=true --formatter-property print.headers=true

run:             ## run the relay (ctrl-c to stop)
	mvn -q -B compile exec:java

race:            ## tx A takes the lower id but commits after tx B. both A and B reach the topic
	@out=$$(mktemp); \
	  $(KAFKA)/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic $(TOPIC) \
	    --formatter-property print.key=true --timeout-ms 12000 > $$out 2>/dev/null & \
	  sleep 3; \
	  ( printf "BEGIN;\nINSERT INTO outbox (event_id, aggregate_id, event_type, payload) VALUES (gen_random_uuid(),'A','order.created','{}');\n\\\\! sleep 4\nCOMMIT;\n" | $(PSQL) ) & \
	  sleep 0.5; \
	  printf "INSERT INTO outbox (event_id, aggregate_id, event_type, payload) VALUES (gen_random_uuid(),'B','order.created','{}');\n" | $(PSQL); \
	  wait; \
	  echo "--- outbox (last 2 rows)"; $(PSQL) -c "SELECT id, aggregate_id, published_at FROM outbox ORDER BY id DESC LIMIT 2"; \
	  echo "--- published during the race: both A and B reach the topic"; cat $$out; rm $$out
