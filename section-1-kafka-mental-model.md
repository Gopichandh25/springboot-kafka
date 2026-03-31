# Section 1: Building a Mental Model of Apache Kafka

> **What this chapter covers**: This chapter introduces Apache Kafka from the ground up. By the
> end, you will understand what Kafka is, how its core components work together, and why each
> design decision matters. No prior Kafka experience is assumed — only a general familiarity
> with software systems and the idea that applications sometimes need to exchange messages.

---

## 1.1 What Is Kafka? The Distributed Commit Log

At its heart, Apache Kafka is a **distributed commit log** — a system that stores a continuous,
ordered sequence of records and makes them available to any number of readers. If you have ever
worked with a database transaction log or even a simple append-only file, you already have an
intuition for the basic idea: new entries are always written to the end, and nothing that has
already been written is modified or deleted.

This design stands in contrast to traditional message queues, where a message is typically
removed once a consumer reads it. In Kafka, records stay in the log for a configurable period
of time (or indefinitely), and consumers simply maintain a pointer to their current position.
Multiple consumers can read the same data independently, each at their own pace.

The following table summarizes the four foundational properties of Kafka's commit log and
explains why each one matters:

| Property | What It Means | Why It Matters |
|---|---|---|
| **Append-only log** | New records are always added to the end of the log. There is no mechanism for updating or deleting an individual record in place. | Consumers must be designed to handle duplicates and late-arriving data, since records cannot be retroactively removed. |
| **Immutable records** | Once a record is written, its content does not change. It remains in the log until the configured retention period expires. | Immutability simplifies debugging and auditing — the original data is always available for inspection. |
| **Ordered within a partition** | Records in a single partition are strictly ordered. However, there is no global ordering across different partitions. | Applications that require ordering guarantees must be deliberate about how they route records into partitions (more on this in Section 1.2). |
| **Pull-based consumption** | Consumers request ("pull") records from Kafka at their own pace, rather than having records pushed to them. | A slow consumer does not block or slow down other consumers. However, a consumer that falls behind accumulates **lag** — the gap between the latest record and the consumer's current position. |

### Why this matters in practice

Because Kafka is a commit log rather than a traditional queue, building applications on top of
it requires thinking about a few concepts that do not arise in simpler messaging systems:

- **Partition assignment** — determining which portions of the log each consumer instance is
  responsible for reading.
- **Offset management** — tracking how far each consumer has progressed through the log and
  deciding when to save that progress.
- **Consumer group rebalancing** — handling the redistribution of work when consumer instances
  are added, removed, or crash unexpectedly.

These ideas will be explored in depth throughout this chapter and applied in
[Section 2](section-2-spring-boot-kafka-basics.md).

---

## 1.2 Topics, Partitions, and Keys

### Topics: named streams of records

A **topic** is Kafka's primary organizational unit. It represents a named category or feed of
records. For example, an e-commerce system might have a topic called `order-events` for all
events related to customer orders, and a separate topic called `payment-events` for payment
processing.

Producers write records to a topic, and consumers read records from a topic. But a topic is not
a single, monolithic log — it is divided into **partitions**.

### Partitions: the unit of parallelism and ordering

Each topic is split into one or more partitions, and each partition is an independent, ordered
log. When a record is written to a topic, it lands in exactly one partition. Here is what a
topic with three partitions looks like:

```
                        Topic: order-events
               ┌──────────────────────────────────┐
               │  Partition 0: [msg0, msg1, msg5]  │  ← all messages with key hash % 3 == 0
               │  Partition 1: [msg2, msg3, msg6]  │  ← all messages with key hash % 3 == 1
               │  Partition 2: [msg4, msg7, msg8]  │  ← all messages with key hash % 3 == 2
               └──────────────────────────────────┘
```

Partitions exist for two reasons. First, they allow Kafka to **scale horizontally**: different
partitions can live on different servers (called *brokers*), so a topic's total throughput grows
with its partition count. Second, they provide **ordering guarantees**: records within a single
partition are always read in the order they were written.

However, there is no ordering guarantee *across* partitions. If records A and B land in
different partitions, a consumer may see B before A regardless of which was written first.

### Keys: controlling which partition a record enters

Every Kafka record can optionally carry a **key**. When a key is present, Kafka uses a hash of
that key to determine the target partition. The critical consequence is:

> **Same key → same partition → preserved ordering for that key.**

For example, if you use `userId` as the key, all events for user `U-123` will land in the same
partition and will always be consumed in the order they were written. This is how Kafka
provides per-entity ordering in a distributed system.

When no key is provided, Kafka distributes records across partitions using a round-robin or
sticky assignment strategy, which spreads load evenly but provides no ordering guarantees. This
is appropriate for use cases where ordering does not matter, such as collecting log entries or
metrics.

### Important partition behaviors

There are two additional facts about partitions that have significant practical consequences:

1. **Partition count is fixed at topic creation and is difficult to change safely.** Adding
   partitions later changes the key-to-partition mapping (because the hash is computed modulo
   the partition count). Records for a given key that previously went to Partition 2 might now
   go to Partition 5. This can break any application logic that depends on per-key ordering.
   For this reason, choosing the right partition count up front is an important design decision.

2. **Consumer parallelism is bounded by the partition count.** Each partition can be assigned
   to at most one consumer within a consumer group (a concept explored in Section 1.4). If a
   topic has 6 partitions and you run 8 consumer instances, 2 of those instances will sit idle
   with no partitions to read. Conversely, if you run 3 instances, each one will be assigned
   2 partitions.

### Choosing a message key

Selecting the right key is one of the most common design decisions when working with Kafka. The
goal is to pick a key that groups related records together (preserving meaningful ordering)
while distributing records evenly enough across partitions to avoid overloading any single one.

| Scenario | Good Key Choice | Reason |
|---|---|---|
| Order processing | `orderId` | All events for a single order stay ordered |
| User activity tracking | `userId` | Each user's event stream is ordered |
| IoT sensor data | `sensorId` | Per-device ordering is preserved |
| Notifications (no ordering needed) | `null` (no key) | Load is spread evenly across partitions |

A word of caution on key cardinality: a key with very few distinct values (for example,
`country`) will concentrate most records into a small number of partitions, creating *hot
partitions* — partitions that carry a disproportionate share of the traffic. On the other
hand, a key with extremely high cardinality (such as a random UUID generated for every event)
distributes records perfectly but means that no two records share a key, so there is no
meaningful per-entity ordering.

---

## 1.3 Offsets: How Consumers Track Their Position

In a traditional message queue, the broker keeps track of which messages have been delivered
and removes them after consumption. Kafka works differently. Records remain in the log
regardless of whether they have been read, and each consumer is responsible for tracking its own
position. The mechanism for this is the **offset**.

Every record in a partition is assigned a sequential integer called its offset — starting from
0 and increasing by 1 for each new record. A consumer tracks its progress by remembering the
offset of the last record it has successfully processed. This is called **committing** the
offset. If the consumer restarts (for example, after a crash or redeployment), it resumes
reading from the committed offset.

Here is a visual representation of offsets in a single partition:

```
Partition 0:  [ 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 ]
                                  ↑
                          committed offset = 4
                          (consumer will resume from offset 5 on restart)
```

### Key properties of offsets

| Property | Explanation |
|---|---|
| **Per-partition, per-consumer-group** | Offsets are tracked independently for each combination of partition and consumer group. Two different consumer groups reading the same partition can be at completely different positions. |
| **Committing = "I have processed up to here"** | The moment of committing represents a durability boundary. If a consumer commits *before* processing a record and then crashes, that record is effectively lost (the consumer will skip it on restart). If a consumer commits *after* processing and then crashes before committing, it will reprocess the record on restart. This is the classic **at-least-once vs. at-most-once** trade-off. |
| **`auto.offset.reset = earliest`** | When a consumer group reads a partition for the first time (no committed offset exists), this setting tells it to start from the very beginning of the log. |
| **`auto.offset.reset = latest`** | On first read, start from the most recent record only, ignoring any historical data. |
| **Offsets are NOT global message IDs** | An offset is meaningful only within its own partition. Offset 5 in Partition 0 and offset 5 in Partition 1 refer to completely different records. |

### Offset management in Spring Boot

When using Spring Kafka's `@KafkaListener` annotation, the framework manages the poll loop and
offset commits on your behalf. By default, Spring Kafka commits offsets in a batch **after the
listener method returns successfully**. If the listener throws an exception, the offset is
**not** committed and the record will be retried (subject to your error handler configuration).

Understanding when and how offsets are committed is essential for building reliable consumers.
This topic is covered in detail in [Section 2](section-2-spring-boot-kafka-basics.md).

---

## 1.4 Consumer Groups: Scaling and Isolating Consumption

### What is a consumer group?

A **consumer group** is a set of consumer instances that cooperate to read from a topic. Kafka
distributes the topic's partitions among the members of the group so that each partition is
read by exactly one member. This allows consumption to scale horizontally: adding more consumer
instances to the group increases the overall throughput.

Here is an example of a consumer group with three members reading a topic with six partitions:

```
Consumer Group: order-processing-group

  Instance A  ──→  Partition 0, Partition 1
  Instance B  ──→  Partition 2, Partition 3
  Instance C  ──→  Partition 4, Partition 5

  (6 partitions, 3 instances → 2 partitions each)
```

Every consumer instance is identified by the **group ID** it connects with. The group ID
determines how Kafka distributes work:

- **Same group ID → load balancing.** Each record is delivered to exactly one instance in the
  group. This is the pattern for horizontal scaling of a single application.
- **Different group ID → independent consumption.** Each group receives its own complete copy
  of every record. This is how multiple independent applications can each read the full stream.

### Rebalancing

When a consumer instance joins or leaves a group — whether due to a new deployment, a crash,
or a scaling event — Kafka triggers a **rebalance**. During a rebalance, partition assignments
are redistributed among the remaining group members. Consumption pauses briefly during this
process. Rebalancing is automatic and is a fundamental part of how Kafka provides fault
tolerance at the consumer level.

In Spring Boot, the consumer group ID is configured via the `spring.kafka.consumer.group-id`
property (or directly in the `@KafkaListener` annotation).

### Multiple consumer groups on the same topic

A common production pattern is to have several consumer groups reading from the same topic,
each performing a different task:

```
Topic: payment-events (3 partitions)

Group: payment-processor        Group: audit-logger
  Instance 1 → P0, P1            Instance X → P0, P1, P2
  Instance 2 → P2

Both groups consume ALL messages independently.
```

In this example, one group handles business logic (processing payments), while a second group
writes every payment event to an audit log or analytics pipeline. Each group maintains its own
set of offsets and progresses through the topic independently. This decoupling is one of Kafka's
most powerful architectural features — a new downstream application can be added at any time
simply by starting a consumer with a new group ID, without affecting existing consumers.

---

## 1.5 Replication and Durability

In a production environment, Kafka runs as a cluster of multiple brokers (servers). To protect
against data loss when a broker fails, Kafka **replicates** each partition across multiple
brokers. One broker holds the **leader** replica for a given partition, and the others hold
**follower** replicas. Producers write to the leader, and the followers continuously copy data
from it. If the leader broker goes down, one of the followers is automatically promoted to
leader.

Three configuration settings control how replication and durability behave:

| Setting | Where It Is Configured | What It Does |
|---|---|---|
| `replication.factor` | Topic configuration | The total number of copies of each partition (including the leader). A replication factor of 3 means each partition exists on three different brokers. |
| `min.insync.replicas` | Topic or broker configuration | The minimum number of replicas that must confirm a write before it is considered successful. This prevents writes from succeeding when too many replicas are unavailable. |
| `acks` | Producer configuration | Controls how many acknowledgments the producer waits for. `all` means the producer waits for every in-sync replica to confirm. `1` means only the leader must confirm. `0` means the producer does not wait for any confirmation (fire-and-forget). |

A common production baseline is `replication.factor=3`, `min.insync.replicas=2`, and
`acks=all`. This ensures that data is written to at least two brokers before the producer
considers the write complete, providing strong durability guarantees.

In a Spring Boot application, the producer acknowledgment setting is configured in
`application.yml`:

```yaml
spring:
  kafka:
    producer:
      acks: all    # producer waits for all in-sync replicas
```

---

## 1.6 Retention: How Long Kafka Keeps Data

Unlike a traditional message queue that discards records after they are consumed, Kafka retains
records according to a configurable **retention policy**. This means that consumers can re-read
old data (for example, to recover from a bug or to populate a new system), and that Kafka can
serve as a durable storage layer — not just a transport mechanism.

Kafka supports three retention strategies:

| Retention Strategy | How It Works | Typical Use Case |
|---|---|---|
| **Time-based** (`retention.ms`) | Records older than the configured duration are deleted. The default is 7 days. | General-purpose event streams where historical data beyond a certain age is no longer needed. |
| **Size-based** (`retention.bytes`) | When the total size of a partition's log exceeds the configured limit, the oldest records are deleted. | Environments with strict storage budgets. |
| **Log compaction** (`cleanup.policy=compact`) | Instead of deleting old records by age or size, Kafka keeps only the **latest** record for each key, discarding older records with the same key. | Maintaining a snapshot of the latest state per entity — for example, the most recent profile update for each user. |

These strategies can also be combined. For example, a topic can use both time-based retention
and log compaction simultaneously.

Understanding retention is important when designing topics because it determines how much
historical data is available for consumers, how much disk space is needed, and whether the
topic can serve as a source of truth for rebuilding application state.

---

## 1.7 Exploring Kafka: A Practical Illustration

The concepts covered so far — topics, partitions, keys, offsets, and consumer groups — can be
observed directly by interacting with a local Kafka instance. This section walks through what
that interaction looks like, using Kafka's built-in command-line tools. The goal is not to
provide a step-by-step tutorial, but to illustrate how the theoretical concepts manifest in
practice.

### Setting up a local Kafka instance

A single-node Kafka broker can be started using Docker. The following `docker-compose.yml`
configuration runs Kafka in **KRaft mode**, which is Kafka's modern architecture that removes
the dependency on Apache ZooKeeper:

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

Starting this with `docker compose up -d` launches a single Kafka broker listening on port
9092. In a production environment, you would run multiple brokers for fault tolerance, but a
single node is sufficient for learning and experimentation.

### Creating a topic and producing records

Kafka's command-line tools can be used from inside the container. The following example
illustrates creating a topic with three partitions and then writing several keyed records to it:

```bash
# Create a topic with 3 partitions
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic test-orders --partitions 3 --replication-factor 1

# List topics to confirm creation
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Produce keyed messages (key:value format)
/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic test-orders \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Here is an example set of records that might be written. Each line has a key (before the colon)
and a value (the JSON payload after the colon):

```
order-1:{"orderId":"order-1","item":"laptop","qty":1}
order-2:{"orderId":"order-2","item":"phone","qty":2}
order-1:{"orderId":"order-1","status":"confirmed"}
order-3:{"orderId":"order-3","item":"tablet","qty":1}
order-2:{"orderId":"order-2","status":"shipped"}
```

Notice that `order-1` appears twice. Because both records share the same key, Kafka routes them
to the same partition, preserving their relative order.

### Consuming records and observing partition assignment

When consuming from this topic, Kafka's console consumer can display the key, value, partition
number, and offset for each record:

```bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic test-orders \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

In the output, several of the concepts from earlier sections become directly visible:

- Records with the same key (e.g., `order-1`) appear in the same partition.
- Records with different keys may appear in different partitions.
- Within each partition, offsets are sequential integers starting from 0.

### Observing consumer group behavior

Consumer groups can be observed by starting multiple consumers with the same or different group
IDs. For example, two consumers started with different group IDs (`group-A` and `group-B`)
will each independently receive **all** records from the topic:

```bash
# Consumer in group-A
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic test-orders --from-beginning --group group-A

# Consumer in group-B
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic test-orders --from-beginning --group group-B
```

If a second consumer joins `group-A`, Kafka triggers a rebalance and **splits** the partitions
between the two group-A members. Each member now receives only a subset of the records.
Meanwhile, `group-B`'s single consumer continues to receive everything — because it is in a
separate group with its own partition assignments.

### Inspecting consumer group offsets

Kafka provides a tool for inspecting the offset state of a consumer group:

```bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group group-A
```

The output of this command shows, for each partition, the **current offset** (how far the
consumer has read), the **log-end offset** (the latest record in the partition), and the
**lag** (the difference between the two). Lag is one of the most important operational metrics
in a Kafka-based system — it tells you how far behind a consumer is from real-time.

---

## 1.8 Key Takeaways

Apache Kafka is a distributed commit log that stores ordered, immutable sequences of records
and makes them available to multiple independent consumers. Unlike traditional message queues,
Kafka retains data according to a configurable retention policy rather than deleting records
after consumption.

Records are organized into **topics**, each of which is divided into one or more **partitions**.
Partitions are the fundamental unit of both parallelism and ordering: records within a single
partition are strictly ordered, but there are no ordering guarantees across partitions. A
record's **key** determines which partition it is routed to — records with the same key always
land in the same partition, which is how Kafka provides per-entity ordering in a distributed
system.

Consumers track their position in each partition using **offsets**, which are sequential
integers assigned to each record. Committing an offset is how a consumer marks its progress, and
the timing of that commit determines the delivery semantics (at-most-once vs. at-least-once).

**Consumer groups** allow multiple consumer instances to divide the work of reading a topic.
Within a group, each partition is assigned to exactly one consumer, providing load-balanced
consumption. Different groups read the same data independently, enabling multiple downstream
applications to process the same stream without interfering with one another.

Kafka achieves durability through **replication** — each partition is copied across multiple
brokers, and producers can be configured to wait for acknowledgment from multiple replicas
before considering a write successful. **Retention policies** control how long data is kept,
with options for time-based deletion, size-based deletion, and log compaction.

Together, these mechanisms make Kafka a powerful foundation for event-driven architectures,
real-time data pipelines, and any system where reliable, ordered, scalable messaging is
required. With this mental model in place, you are ready to begin building Kafka-backed
applications — starting with [Section 2: Spring Boot Kafka Basics](section-2-spring-boot-kafka-basics.md).
