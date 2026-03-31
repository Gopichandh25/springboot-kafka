# Section 1: Kafka Mental Model

> **Audience**: You already know the theory of how Kafka works internally. This section is a
> quick, practical refresher that re-frames that knowledge from the perspective of someone who
> is about to *build* Spring Boot services on top of Kafka. Read it in 30 minutes, do the
> hands-on exercises, then move to Section 2.

---

## 1.1 Kafka Is a Distributed Commit Log

You already know this, but the implication for your code is important:

| Property | What It Means for Your Code |
|---|---|
| Append-only log | You never "update" or "delete" a message. Your consumers must handle duplicates and late-arriving data. |
| Immutable records | Once produced, the record exists until retention expires. Debugging is easier because the data is still there. |
| Ordered within a partition | You get ordering guarantees **only** inside a single partition. Global ordering across partitions does not exist. |
| Pull-based consumption | Consumers control their own pace. A slow consumer does not block others, but it will accumulate **lag**. |

### Why this matters for Spring Boot

Spring Kafka's `@KafkaListener` abstracts the poll loop, but you still need to reason about:

- **Partition assignment** — which partitions your listener instance owns
- **Offset management** — when your listener "commits" progress
- **Consumer group rebalancing** — what happens when you deploy a new instance or one crashes

---

## 1.2 Topics, Partitions, and Keys — The Practical View

```
                        Topic: order-events
               ┌──────────────────────────────────┐
               │  Partition 0: [msg0, msg1, msg5]  │  ← all messages with key hash % 3 == 0
               │  Partition 1: [msg2, msg3, msg6]  │  ← all messages with key hash % 3 == 1
               │  Partition 2: [msg4, msg7, msg8]  │  ← all messages with key hash % 3 == 2
               └──────────────────────────────────┘
```

### Key rules you will rely on daily

1. **Same key → same partition → preserved ordering for that key.**
   If you use `userId` as the key, all events for user `U-123` land in the same partition and
   are consumed in order.

2. **No key → round-robin (sticky partitioner in newer clients).**
   Use this only when ordering does not matter (e.g., log events, metrics).

3. **Partition count is set at topic creation and is hard to change safely.**
   Adding partitions later changes the key-to-partition mapping, which means existing keys may
   land in different partitions. Plan partition counts up front.

4. **Consumer group parallelism is bounded by partition count.**
   If a topic has 6 partitions and you run 8 consumer instances, 2 instances sit idle. If you
   run 3 instances, each gets 2 partitions.

### Choosing a message key — a decision you will make repeatedly

| Scenario | Good Key Choice | Why |
|---|---|---|
| Order processing | `orderId` | All events for an order stay ordered |
| User activity tracking | `userId` | Per-user event stream is ordered |
| IoT sensor data | `sensorId` | Per-device ordering |
| Notifications (no ordering needed) | `null` (no key) | Spread load evenly |

> **Warning**: Choosing a key with very low cardinality (e.g., `country`) creates hot
> partitions. Choosing a key with very high cardinality (e.g., `UUID` per event) spreads
> perfectly but loses meaningful ordering.

---

## 1.3 Offsets — Your Consumer's Bookmark

```
Partition 0:  [ 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 ]
                                  ↑
                          committed offset = 4
                          (consumer will resume from offset 5 on restart)
```

Key points you need to internalize:

| Concept | Implication |
|---|---|
| Offset is per-partition, per-consumer-group | Two different consumer groups can be at different positions in the same partition |
| Committing offset = "I have processed up to here" | If you commit before processing, you lose data on crash. If you commit after, you may reprocess on crash. |
| `auto.offset.reset = earliest` | On first join (no committed offset), start from the beginning |
| `auto.offset.reset = latest` | On first join, start from the newest message only |
| Offsets are NOT message IDs | You cannot use an offset to look up a record in another partition |

### The critical question for your Spring Boot app

> "When does my `@KafkaListener` commit the offset?"

Spring Kafka defaults to **batch commit after the listener method returns successfully**. If
your listener throws an exception, the offset is **not** committed and the message is retried
(depending on your error handler configuration). You will configure this explicitly in Section 2.

---

## 1.4 Consumer Groups — Scaling Consumption

```
Consumer Group: order-processing-group

  Instance A  ──→  Partition 0, Partition 1
  Instance B  ──→  Partition 2, Partition 3
  Instance C  ──→  Partition 4, Partition 5

  (6 partitions, 3 instances → 2 partitions each)
```

### What you need to know for your project

- **Same group ID** = load balancing. Each message is delivered to exactly one instance in the group.
- **Different group ID** = broadcast. Each group gets its own copy of every message.
- **Rebalancing** happens when instances join or leave. During rebalance, consumption pauses briefly.
- Spring Boot's `spring.kafka.consumer.group-id` property sets this.

### Two consumer groups reading the same topic

```
Topic: payment-events (3 partitions)

Group: payment-processor        Group: audit-logger
  Instance 1 → P0, P1            Instance X → P0, P1, P2
  Instance 2 → P2

Both groups consume ALL messages independently.
```

This is a common production pattern: one group processes business logic, another group writes
to an audit log or analytics pipeline.

---

## 1.5 Replication and Durability — What You Configure

You know how replication works. Here is what you actually set:

| Setting | Where | What It Does |
|---|---|---|
| `replication.factor` | Topic config | Number of copies of each partition across brokers |
| `min.insync.replicas` | Topic/broker config | Minimum replicas that must acknowledge a write |
| `acks` | Producer config | `all` = wait for all in-sync replicas. `1` = leader only. `0` = fire-and-forget. |

**Production baseline**: `replication.factor=3`, `min.insync.replicas=2`, `acks=all`.

In your Spring Boot `application.yml`:

```yaml
spring:
  kafka:
    producer:
      acks: all    # producer waits for all in-sync replicas
```

---

## 1.6 Retention — Data Lifecycle

| Retention Type | Behavior | Use Case |
|---|---|---|
| Time-based (`retention.ms`) | Delete segments older than X | Event streams (default 7 days) |
| Size-based (`retention.bytes`) | Delete oldest segments when size limit is reached | Bounded storage |
| Log compaction (`cleanup.policy=compact`) | Keep only the latest value per key | Stateful snapshots (e.g., user profile changes) |

For your first project, the defaults (7-day time-based retention) are fine. You will revisit
this when you design topic strategy in later sections.

---

## 1.7 Hands-On Exercises

### Prerequisites

- Docker and Docker Compose installed
- A terminal

### Exercise 1: Start Kafka Locally

Create a file `docker-compose.yml`:

```yaml
version: '3.8'
services:
  kafka:
    image: apache/kafka:3.7.0
    container_name: kafka-local
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
```

```bash
docker compose up -d
```

> This uses **KRaft mode** (no ZooKeeper). This is Kafka's current architecture direction.

### Exercise 2: Create a Topic and Produce Messages

```bash
# Enter the Kafka container
docker exec -it kafka-local bash

# Create a topic with 3 partitions
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic test-orders --partitions 3 --replication-factor 1

# List topics
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Produce keyed messages (key:value format)
/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic test-orders \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Type these messages (press Enter after each):

```
order-1:{"orderId":"order-1","item":"laptop","qty":1}
order-2:{"orderId":"order-2","item":"phone","qty":2}
order-1:{"orderId":"order-1","status":"confirmed"}
order-3:{"orderId":"order-3","item":"tablet","qty":1}
order-2:{"orderId":"order-2","status":"shipped"}
```

Press `Ctrl+C` to exit the producer.

### Exercise 3: Consume and Observe Partitions

```bash
# Consume from the beginning, showing key, value, partition, and offset
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic test-orders \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

**What to observe**:

- Messages with the same key (`order-1`) are in the same partition
- Messages with different keys may be in different partitions
- Within a partition, offsets are sequential

### Exercise 4: Multiple Consumer Groups

Open **two terminals**. In each, consume with a different group:

```bash
# Terminal 1 — group A
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic test-orders --from-beginning --group group-A

# Terminal 2 — group B
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic test-orders --from-beginning --group group-B
```

Both groups receive **all** messages independently. Now, in a third terminal, start another
consumer in group-A:

```bash
# Terminal 3 — also group A
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic test-orders --group group-A
```

Produce more messages and observe that group-A's members **split** the partitions between them,
while group-B's single member gets everything.

### Exercise 5: Inspect Consumer Group Offsets

```bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group group-A
```

You will see output showing each partition, the current offset, the log-end offset, and the
**lag** (how far behind the consumer is).

---

## 1.8 Summary Checklist

Before moving to Section 2, confirm you can answer these:

- [ ] Kafka is a distributed commit log, not a request-response system
- [ ] I can explain why the same key always goes to the same partition
- [ ] I can explain why ordering is only guaranteed within a partition
- [ ] I understand that adding partitions later can break key-to-partition mapping
- [ ] I know that offsets are per-partition, per-consumer-group bookmarks — not message IDs
- [ ] I have run the hands-on exercises and observed partition assignment and offsets
- [ ] I understand the difference between same-group (load balanced) and different-group (broadcast) consumption

When you can check all of these, move to **[Section 2: Spring Boot Kafka Basics](section-2-spring-boot-kafka-basics.md)**.
