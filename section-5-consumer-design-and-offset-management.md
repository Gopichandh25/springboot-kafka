# Section 5: Consumer Design and Offset Management

Producing messages to Kafka is conceptually straightforward: serialize a record, pick a partition, send it, and move on. The producer's job is finished the moment the broker acknowledges the write. The consumer's job, by contrast, never truly ends. A consumer must continuously poll for new records, deserialize them, execute business logic that may involve databases, HTTP calls, or other side effects, decide when to tell Kafka that processing is complete, and handle every failure mode that can occur along the way — crashes, slow dependencies, malformed data, rebalances, and duplicates. This is where correctness lives. A poorly designed consumer can silently lose messages, process the same message dozens of times, or grind to a halt under load. This section is the most important for production work, and every concept introduced here has a direct, measurable impact on the reliability of a real system.

The sections that follow assume familiarity with topics, partitions, offsets, and consumer groups from [Section 1](section-1-kafka-mental-model.md), with Spring Boot's Kafka auto-configuration from [Section 2](section-2-spring-boot-kafka-basics.md), with event envelope design from [Section 3](section-3-message-contract-design.md), and with producer delivery guarantees from [Section 4](section-4-producer-design.md).

---

## 5.1 Why Consumer Design Is the Hardest Part

When a producer sends a message to Kafka, the outcome is binary: the broker either acknowledged the write or it did not. If the write failed, the producer retries. If it succeeded, the message is durable. The producer does not need to think about what happens to the message after that point.

The consumer has no such luxury. Consider what must happen when a consumer receives a record:

1. The record must be deserialized from bytes into a usable object.
2. The business logic must execute — perhaps inserting a row into a database, calling an external API, or updating a cache.
3. The consumer must tell Kafka "I have processed this record" by committing the offset.
4. If any step fails, the consumer must decide whether to retry, skip, or route the record elsewhere.

Each of these steps can fail independently, and the *order* in which they happen relative to the offset commit determines whether the system loses messages or processes them more than once.

### The fundamental trade-off

Kafka provides **at-least-once delivery** by default. This means:

- If a consumer commits the offset **before** processing completes and then crashes, the record is lost — the consumer will not see it again after restart because Kafka believes it was already handled.
- If a consumer commits the offset **after** processing completes and then crashes before committing, the record will be delivered again on restart — leading to a duplicate.

There is no configuration setting that eliminates this trade-off. The only path to correct behavior is deliberate consumer design: choosing when to commit, making business logic safe against duplicates, and handling every failure mode explicitly.

### What makes consumer design hard

| Challenge | Why It Is Difficult |
|---|---|
| **Offset management** | Committing too early loses messages; committing too late causes duplicates. The right moment depends on your processing semantics. |
| **Rebalancing** | When consumers join or leave a group, partitions are reassigned. In-flight processing may be interrupted, and uncommitted work is repeated. |
| **Deserialization failures** | A single corrupt record can cause an infinite crash loop if the consumer has no strategy for handling it. |
| **Slow processing** | If a database call or HTTP request takes too long, Kafka may assume the consumer is dead and trigger a rebalance — which makes the problem worse. |
| **Duplicate delivery** | Kafka guarantees at-least-once delivery, not exactly-once. The consumer's business logic must be safe to execute more than once for the same record. |
| **Concurrency** | Running multiple consumer threads increases throughput but introduces thread-safety concerns for shared state. |

> **Key insight**: The producer's contract with Kafka is "store this record durably." The consumer's contract with the application is "process this record correctly, exactly the right number of times, even when everything around it is failing." That asymmetry is why consumer design demands more thought than producer design.

---

## 5.2 Auto Commit vs Manual Commit

### How auto-commit works

By default, the Kafka consumer client is configured with `enable.auto.commit=true`. This means the client periodically commits offsets in the background, on a timer controlled by `auto.commit.interval.ms` (default: 5000 milliseconds). The consumer does not wait for your application code to finish processing before committing — it simply commits whatever offset the consumer has reached at the next tick of the timer.

Here is the relevant default configuration:

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: true            # default
      properties:
        auto.commit.interval.ms: 5000     # default: commit every 5 seconds
```

### The danger of auto-commit

Auto-commit creates a timing problem that can cause either message loss or message duplication, depending on when the crash occurs relative to the commit timer.

```
Timeline of auto-commit danger:

  poll()        processing        auto-commit fires       crash!
    │               │                    │                   │
    ▼               ▼                    ▼                   ▼
────┬───────────────┬────────────────────┬───────────────────┬────
    │               │                    │                   │
    │  Records      │  Still processing  │  Offset committed │
    │  received     │  records 5-9...    │  (records 0-9)    │
    │  (0-9)        │                    │                   │
    │               │                    │                   │
                                                             │
                                              Records 5-9 were committed
                                              but NOT fully processed.
                                              On restart, consumer resumes
                                              at offset 10 → records 5-9 LOST.
```

The opposite scenario is equally problematic:

```
Timeline of auto-commit duplication:

  poll()        processing completes       crash!       auto-commit would
    │               │                        │          have fired here
    ▼               ▼                        ▼               ▼
────┬───────────────┬────────────────────────┬───────────────┬────
    │               │                        │               │
    │  Records      │  All records           │  Offset NOT   │
    │  received     │  processed             │  committed    │
    │  (0-9)        │  successfully          │  yet          │
    │               │                        │               │
                                             │
                                  Consumer restarts at offset 0.
                                  Records 0-9 are reprocessed → DUPLICATES.
```

In either case, the problem is that the commit happens on an arbitrary timer that has no relationship to whether your application code has actually finished processing the records.

### Manual commit: taking control

With manual commit, the consumer explicitly tells Kafka "I have processed this far" — and it does so at the exact moment that makes sense for the application's processing semantics. Spring Kafka provides this through its `AckMode` configuration, which controls when the framework commits offsets on your behalf.

### Spring Kafka's AckMode enum

Spring Kafka defines several acknowledgment modes. When `enable.auto.commit` is set to `false` (which Spring Kafka does by default when an `AckMode` other than the Kafka-native auto-commit is configured), the framework takes over offset management:

| AckMode | When Offsets Are Committed | Use Case |
|---|---|---|
| `BATCH` (default) | After all records returned by a single `poll()` have been processed by the listener method. | General-purpose. Safe for most workloads. Offers a good balance between safety and throughput. |
| `RECORD` | After each individual record is processed by the listener method. | When you cannot afford to reprocess any record within a batch. Slightly lower throughput than `BATCH`. |
| `MANUAL` | When your code explicitly calls `acknowledgment.acknowledge()`. The actual commit happens after the current batch is fully processed. | When you need programmatic control over acknowledgment but are fine with batch-level commit timing. |
| `MANUAL_IMMEDIATE` | Immediately when your code calls `acknowledgment.acknowledge()`. The commit happens right away, without waiting for the batch to complete. | When you need immediate offset persistence — for example, after a slow database transaction that you do not want to repeat. |
| `COUNT` | After a configured number of records have been processed. | Useful when you want to commit less frequently than every record but more predictably than per-batch. |
| `TIME` | After a configured time interval has elapsed since the last commit. | Similar to `COUNT` but time-based. |
| `COUNT_TIME` | After either the count or the time threshold is reached, whichever comes first. | Combines `COUNT` and `TIME` for balanced commit frequency. |

> **Key insight**: Spring Kafka's default `AckMode` is `BATCH`, and it disables Kafka's native `enable.auto.commit` internally. This means that even without any explicit configuration, Spring Kafka is already safer than the raw Kafka consumer client's default behavior. But for critical business processing, `MANUAL` or `MANUAL_IMMEDIATE` gives you the most control.

### Code example: manual acknowledgment

The most explicit form of offset management uses `AckMode.MANUAL_IMMEDIATE` and the `Acknowledgment` parameter:

```java
package com.example.kafkalearning.consumer;

import com.example.kafkalearning.dto.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    @KafkaListener(
        topics = "${app.kafka.topic.orders}",
        groupId = "order-processing-group"
    )
    public void handle(ConsumerRecord<String, OrderEvent> record,
                       Acknowledgment acknowledgment) {

        log.info("Received record: topic={}, partition={}, offset={}, key={}",
                record.topic(), record.partition(), record.offset(), record.key());

        try {
            // Business logic — e.g., save to database
            processOrder(record.value());

            // Only acknowledge AFTER successful processing
            acknowledgment.acknowledge();

            log.info("Successfully processed and acknowledged: offset={}",
                    record.offset());

        } catch (Exception e) {
            log.error("Failed to process record at offset={}: {}",
                    record.offset(), e.getMessage(), e);
            // Do NOT acknowledge — the record will be retried
            // (Spring Kafka's error handler determines what happens next)
        }
    }

    private void processOrder(OrderEvent event) {
        // Business logic here
    }
}
```

### Code example: RECORD mode configuration

If you want automatic per-record commits without manual `Acknowledgment` calls, use `RECORD` mode:

```java
package com.example.kafkalearning.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>>
            kafkaListenerContainerFactory(ConsumerFactory<String, Object> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // Commit after each record is processed
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        return factory;
    }
}
```

### YAML configuration for ack modes

The simplest way to configure the ack mode is through `application.yml`:

```yaml
spring:
  kafka:
    consumer:
      # Disable Kafka-native auto-commit (Spring Kafka does this automatically
      # when you set an ack-mode, but being explicit is clearer)
      enable-auto-commit: false

    listener:
      # Options: batch, record, manual, manual_immediate, count, time, count_time
      ack-mode: manual_immediate
```

For `COUNT`, `TIME`, or `COUNT_TIME` modes, additional properties control the thresholds:

```yaml
spring:
  kafka:
    listener:
      ack-mode: count_time
      ack-count: 100       # commit after 100 records
      ack-time: 10000      # or after 10 seconds, whichever comes first
```

---

## 5.3 `auto.offset.reset`: earliest vs latest

### What this setting controls

The `auto.offset.reset` configuration determines what a consumer does when it encounters a partition for which **no committed offset exists**. This is not about what happens on every startup — it is specifically about the behavior when Kafka has no record of where this consumer group left off for a given partition.

### When does "no committed offset" happen?

There are exactly three scenarios:

1. **First time a consumer group starts.** The consumer group has never read from this topic before, so there is no committed offset for any partition.
2. **Offsets have expired.** Kafka retains committed offsets for a configurable period (controlled by `offsets.retention.minutes`, which defaults to 7 days in most Kafka versions). If a consumer group stops consuming for longer than this retention period, its offsets are deleted.
3. **Partition count increased.** When new partitions are added to a topic, the consumer group has no committed offset for those new partitions.

In all other cases — normal restarts, rolling deployments, brief outages — the consumer resumes from its last committed offset regardless of this setting.

### The three options

| Setting | Behavior | When to Use |
|---|---|---|
| `earliest` | Start reading from the **beginning** of the partition log. The consumer will process all available records from offset 0 (or the earliest retained offset). | Development, testing, replay scenarios, and any system where missing a message is worse than processing a duplicate. |
| `latest` | Start reading from the **end** of the partition log. The consumer will only see records that are produced **after** it starts. All historical data is skipped. | High-volume topics where replaying history would overwhelm the consumer, or where only real-time data matters. |
| `none` | Throw an exception (`NoOffsetForPartitionException`). The consumer refuses to start without a pre-existing committed offset. | Systems that require explicit offset initialization and consider it a bug if offsets are missing. |

### Configuration

```yaml
spring:
  kafka:
    consumer:
      auto-offset-reset: earliest    # or: latest, none
```

### Scenario comparison

| Scenario | Recommended Setting | Reason |
|---|---|---|
| Learning and development | `earliest` | You want to see all messages, including ones produced before the consumer started. |
| First deployment of a new consumer group | `earliest` | Ensures no messages are missed from the initial backlog. |
| High-volume logging/metrics pipeline | `latest` | Replaying millions of historical log entries on first start would be wasteful and slow. |
| Adding a second consumer group to an existing topic | Depends on requirements | `earliest` if the new group needs full history; `latest` if it only needs new data. |
| Consumer group that was offline for > 7 days | `earliest` (with caution) | Offsets may have expired. `earliest` replays from the beginning, which may be a large volume. Plan for reprocessing time. |
| Strict offset management (e.g., financial systems) | `none` | Forces the system to fail loudly if offsets are missing, rather than silently replaying or skipping data. |

### Why `earliest` is safer for learning

During development, `earliest` ensures you always see the messages you produce — even if you produce a message and *then* start your consumer for the first time. With `latest`, those messages would be silently skipped, leading to the common frustration: "my consumer is running but I'm not seeing any messages."

### The common pitfall

The single most frequently asked question from developers new to Kafka is: *"Why isn't my consumer receiving messages?"*

The answer is almost always one of:

1. The consumer group is starting for the first time and `auto.offset.reset` is set to `latest`, so all previously produced messages are invisible.
2. The consumer is in a different consumer group than expected, so it has no committed offsets and falls back to `auto.offset.reset`.
3. The consumer is subscribing to a different topic name than the one the producer is writing to (often a typo).

Understanding `auto.offset.reset` eliminates the first two causes.

---

## 5.4 Consumer Group Rebalancing

### What is a rebalance?

A **rebalance** is the process by which Kafka redistributes partition assignments among the members of a consumer group. During a rebalance, the group coordinator (a broker that manages the group) determines which consumer gets which partitions.

### What triggers a rebalance

| Trigger | Description |
|---|---|
| **New consumer joins the group** | A new instance starts and subscribes to the same topic with the same `group.id`. |
| **Consumer leaves the group gracefully** | An instance shuts down and sends a `LeaveGroup` request. |
| **Consumer crashes or becomes unresponsive** | The broker stops receiving heartbeats within `session.timeout.ms` and considers the consumer dead. |
| **Consumer exceeds `max.poll.interval.ms`** | The consumer takes too long between `poll()` calls and is considered stuck. |
| **Topic partition count changes** | New partitions are added to a topic the group is subscribed to. |
| **Subscription pattern change** | A consumer using a regex pattern subscription encounters a new topic that matches. |

### What happens during a rebalance

1. The group coordinator tells all consumers in the group that a rebalance is starting.
2. All consumers stop processing and commit their current offsets (if possible).
3. The group coordinator assigns partitions to consumers according to the configured assignment strategy.
4. Consumers resume processing with their new partition assignments.

The critical consequence is that **all processing stops during a rebalance**. This is often called the "stop-the-world" problem. In a large consumer group with many partitions, a rebalance can take seconds or even minutes — during which no messages are being processed.

### Eager vs cooperative rebalancing

Kafka supports two fundamentally different rebalancing protocols:

```
Eager rebalancing (default):

  Before rebalance:                After rebalance:
  Consumer A → [P0, P1, P2]       Consumer A → [P0, P1]
  Consumer B → [P3, P4, P5]       Consumer B → [P3, P4]
                                   Consumer C → [P2, P5]   ← new consumer

  Step 1: ALL partitions revoked from ALL consumers
  Step 2: ALL partitions reassigned from scratch

  ┌──────────────────────────────────────────────────────────┐
  │  Consumer A:  ████████░░░░░░░░░░░░░░░░░████████████████  │
  │  Consumer B:  ████████░░░░░░░░░░░░░░░░░████████████████  │
  │  Consumer C:  ░░░░░░░░░░░░░░░░░░░░░░░░░██████████████░░  │
  │               ▲       ▲                 ▲                 │
  │            processing  all stopped    processing resumes  │
  │                        (rebalance)                        │
  └──────────────────────────────────────────────────────────┘
```

```
Cooperative rebalancing (CooperativeStickyAssignor):

  Before rebalance:                After rebalance:
  Consumer A → [P0, P1, P2]       Consumer A → [P0, P1]
  Consumer B → [P3, P4, P5]       Consumer B → [P3, P4]
                                   Consumer C → [P2, P5]

  Step 1: Only P2 revoked from A, only P5 revoked from B
  Step 2: P2 and P5 assigned to C
  Step 3: P0, P1, P3, P4 were NEVER revoked — processing continues

  ┌──────────────────────────────────────────────────────────┐
  │  Consumer A:  ████████████████████████████████████████░░  │
  │                       (P0, P1 never interrupted)          │
  │  Consumer B:  ████████████████████████████████████████░░  │
  │                       (P3, P4 never interrupted)          │
  │  Consumer C:  ░░░░░░░░░░░░░░░░░░░░░░░░░██████████████░░  │
  │               ▲                         ▲                 │
  │          only affected              reassigned            │
  │          partitions paused          partitions resume      │
  └──────────────────────────────────────────────────────────┘
```

The difference is dramatic: eager rebalancing stops *all* consumers even if only one partition needs to move, while cooperative rebalancing only interrupts the specific partitions that are changing ownership.

### Partition assignment strategies

| Strategy | Protocol | Behavior | Trade-offs |
|---|---|---|---|
| `RangeAssignor` | Eager | Assigns contiguous ranges of partitions to consumers. Consumer A gets P0-P2, Consumer B gets P3-P5. | Simple but can create uneven distribution when partition count is not evenly divisible by consumer count. |
| `RoundRobinAssignor` | Eager | Distributes partitions one at a time in round-robin order. | More even distribution than Range, but still revokes all partitions on rebalance. |
| `StickyAssignor` | Eager | Like RoundRobin but tries to keep existing assignments stable when possible. | Reduces partition movement but still uses the eager protocol (all partitions are revoked and reassigned). |
| `CooperativeStickyAssignor` | Cooperative | Like Sticky but uses the cooperative protocol — only revokes partitions that need to move. | Best for production. Minimizes processing interruption during rebalances. **Recommended.** |

### Configuring CooperativeStickyAssignor

```yaml
spring:
  kafka:
    consumer:
      properties:
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Minimizing rebalance impact

Beyond choosing the cooperative protocol, several configuration properties affect how quickly Kafka detects dead consumers and how long rebalances take:

| Property | Default | Purpose | Tuning Guidance |
|---|---|---|---|
| `session.timeout.ms` | 45000 | How long a consumer can go without sending a heartbeat before the broker considers it dead. | Lower values detect failures faster but risk false positives during GC pauses. 10000–30000 is typical. |
| `heartbeat.interval.ms` | 3000 | How frequently the consumer sends heartbeats to the broker. | Should be roughly 1/3 of `session.timeout.ms`. |
| `max.poll.interval.ms` | 300000 | Maximum time between `poll()` calls. Exceeded → consumer is removed from the group. | Must be longer than your longest possible record-processing time. See Section 5.6. |

```yaml
spring:
  kafka:
    consumer:
      properties:
        session.timeout.ms: 15000
        heartbeat.interval.ms: 5000
        max.poll.interval.ms: 300000
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Rebalance listeners in Spring Kafka

Spring Kafka allows you to react to rebalance events by implementing a `ConsumerAwareRebalanceListener`. This is useful for flushing in-progress state, logging partition assignments, or performing cleanup:

```java
package com.example.kafkalearning.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class RebalanceLogger implements ConsumerAwareRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(RebalanceLogger.class);

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer,
                                     Collection<TopicPartition> partitions) {
        log.info("Partitions assigned: {}", partitions);
    }

    @Override
    public void onPartitionsRevoked(Consumer<?, ?> consumer,
                                    Collection<TopicPartition> partitions) {
        log.warn("Partitions revoked: {} — committing offsets for in-progress work",
                partitions);
        consumer.commitSync();
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer,
                                 Collection<TopicPartition> partitions) {
        log.error("Partitions lost (not revoked cleanly): {}", partitions);
        // Do NOT commit offsets here — the partitions have already been reassigned
    }
}
```

> **Key insight**: The difference between `onPartitionsRevoked` and `onPartitionsLost` is important. `onPartitionsRevoked` is called during a normal rebalance — you still own the partitions and can safely commit offsets. `onPartitionsLost` is called when the partitions were taken away without a clean revocation (e.g., the consumer was considered dead) — committing offsets here could overwrite the new owner's progress.

---

## 5.5 Concurrency in Spring Kafka

### How Spring Kafka manages consumer threads

Spring Kafka's `ConcurrentKafkaListenerContainerFactory` creates one or more **listener containers**, each running its own Kafka consumer instance in a dedicated thread. Each thread independently polls Kafka and dispatches records to your `@KafkaListener` method.

The `concurrency` setting determines how many of these threads are created. Each thread manages its own `KafkaConsumer` instance and is assigned a subset of the topic's partitions.

### The relationship between concurrency and partition count

Kafka enforces a fundamental rule: within a single consumer group, **each partition is assigned to at most one consumer instance**. This means:

- If `concurrency` = 3 and the topic has 6 partitions, each thread gets 2 partitions.
- If `concurrency` = 6 and the topic has 6 partitions, each thread gets 1 partition.
- If `concurrency` = 9 and the topic has 6 partitions, only 6 threads get a partition — 3 threads sit idle.

```
Topic: order-events (6 partitions)
Concurrency = 3

  ┌─────────────────────────────────────────────────┐
  │            Consumer Group: order-group           │
  │                                                  │
  │   Thread-0 ──→ Partition 0, Partition 1          │
  │   Thread-1 ──→ Partition 2, Partition 3          │
  │   Thread-2 ──→ Partition 4, Partition 5          │
  │                                                  │
  └─────────────────────────────────────────────────┘

Concurrency = 6

  ┌─────────────────────────────────────────────────┐
  │            Consumer Group: order-group           │
  │                                                  │
  │   Thread-0 ──→ Partition 0                       │
  │   Thread-1 ──→ Partition 1                       │
  │   Thread-2 ──→ Partition 2                       │
  │   Thread-3 ──→ Partition 3                       │
  │   Thread-4 ──→ Partition 4                       │
  │   Thread-5 ──→ Partition 5                       │
  │                                                  │
  └─────────────────────────────────────────────────┘

Concurrency = 9

  ┌─────────────────────────────────────────────────┐
  │            Consumer Group: order-group           │
  │                                                  │
  │   Thread-0 ──→ Partition 0                       │
  │   Thread-1 ──→ Partition 1                       │
  │   Thread-2 ──→ Partition 2                       │
  │   Thread-3 ──→ Partition 3                       │
  │   Thread-4 ──→ Partition 4                       │
  │   Thread-5 ──→ Partition 5                       │
  │   Thread-6 ──→ (idle — no partition available)   │
  │   Thread-7 ──→ (idle — no partition available)   │
  │   Thread-8 ──→ (idle — no partition available)   │
  │                                                  │
  └─────────────────────────────────────────────────┘
```

### Setting concurrency

Concurrency can be set globally in YAML or per-listener in the annotation:

```yaml
# Global setting — applies to all listeners unless overridden
spring:
  kafka:
    listener:
      concurrency: 3
```

```java
// Per-listener override
@KafkaListener(
    topics = "${app.kafka.topic.orders}",
    groupId = "order-processing-group",
    concurrency = "6"                      // overrides the global setting
)
public void handle(ConsumerRecord<String, OrderEvent> record) {
    // ...
}
```

### Thread safety in listener methods

Each listener thread has its own `KafkaConsumer` instance, so there is no shared state at the Kafka client level. However, your listener method itself may access shared resources:

| Resource | Thread-Safe? | Recommendation |
|---|---|---|
| Spring-managed beans (e.g., `@Service`, `@Repository`) | Depends on implementation | Ensure beans are stateless or use proper synchronization. Spring beans are singletons by default and shared across all threads. |
| JDBC `DataSource` / JPA `EntityManager` | Yes (connection pooling handles concurrency) | Use `@Transactional` appropriately. Each thread gets its own connection from the pool. |
| In-memory caches or maps | No, unless explicitly synchronized | Use `ConcurrentHashMap` or other concurrent data structures. |
| External HTTP clients (e.g., `RestTemplate`, `WebClient`) | Typically yes | Most HTTP clients are thread-safe by design, but verify your specific client. |

### When to increase concurrency vs add more instances

| Approach | When to Use | Trade-off |
|---|---|---|
| **Increase `concurrency`** on a single instance | You have available CPU cores and memory on the existing machine. Topic has more partitions than current concurrency. | More threads share the same JVM heap and CPU. May increase GC pressure. |
| **Add more application instances** (horizontal scaling) | Each instance is resource-constrained. You want fault isolation — if one instance crashes, others continue. | Requires orchestration (Kubernetes, ECS, etc.) and increases infrastructure cost. |

The general guideline: start with `concurrency` equal to the partition count (or the number of available CPU cores, whichever is smaller). Scale horizontally when a single instance cannot keep up.

---

## 5.6 Max Poll and Processing Time

### The poll loop

Under the hood, a Kafka consumer operates in a loop: it calls `poll()` to fetch a batch of records, processes those records, and then calls `poll()` again to fetch the next batch. Two configuration properties govern the behavior of this loop, and misconfiguring them is one of the most common causes of unexpected rebalances.

### `max.poll.interval.ms`

This property sets the **maximum time allowed between consecutive `poll()` calls**. If the consumer does not call `poll()` within this interval, the group coordinator considers it dead and triggers a rebalance. The consumer is removed from the group and its partitions are reassigned to other members.

Default: **300000** (5 minutes).

### `max.poll.records`

This property sets the **maximum number of records returned by a single `poll()` call**. If the topic has many records available, a single poll might return hundreds or thousands — and the consumer must process all of them before calling `poll()` again.

Default: **500**.

### The dangerous interaction

The interaction between these two properties creates a trap: if `max.poll.records` is large and each record takes a long time to process, the total processing time can exceed `max.poll.interval.ms`, causing the consumer to be kicked from the group.

```
The max.poll.interval.ms trap:

  poll() returns            processing each record...               max.poll.interval.ms
  500 records                                                        exceeded!
    │                                                                    │
    ▼                                                                    ▼
────┬────────────────────────────────────────────────────────────────────┬────
    │                                                                    │
    │  Record 1: DB write (200ms)                                        │
    │  Record 2: DB write (200ms)                                        │
    │  Record 3: HTTP call (500ms)                                       │
    │  ...                                                               │
    │  Record 450: still processing...                                   │
    │                                                          ┌─────────┤
    │                                                          │REBALANCE│
    │                                                          └─────────┘
    │                                                                    │
    │         Total time: 500 records × ~200ms avg = ~100 seconds        │
    │         If max.poll.interval.ms < 100 seconds → kicked!            │
```

### Timing-related consumer configuration

| Property | Default | Controls | Tuning Strategy |
|---|---|---|---|
| `max.poll.interval.ms` | 300000 (5 min) | Maximum time between `poll()` calls before consumer is removed from group. | Increase if processing is legitimately slow. But first try reducing `max.poll.records`. |
| `max.poll.records` | 500 | Maximum records returned per `poll()`. | Reduce if processing per record is slow (e.g., DB calls, HTTP requests). 50–100 is common for I/O-heavy listeners. |
| `fetch.min.bytes` | 1 | Minimum data the broker must have before returning a fetch response. | Increase to reduce the number of poll round trips for low-volume topics. |
| `fetch.max.wait.ms` | 500 | Maximum time the broker waits to accumulate `fetch.min.bytes` before responding. | Increase alongside `fetch.min.bytes` for batching efficiency. |
| `session.timeout.ms` | 45000 | Heartbeat-based liveness. Separate from `max.poll.interval.ms`. | Lower for faster failure detection; higher for tolerance of GC pauses. |

### Common scenario: slow database calls

Consider a consumer that writes each record to a database. If a database call averages 50ms and `max.poll.records` is 500, processing a single batch takes at least 25 seconds. Add network latency, connection pool contention, and the occasional slow query, and you can easily exceed `max.poll.interval.ms` under load.

The fix is straightforward: reduce `max.poll.records` to a value where the worst-case processing time stays well within `max.poll.interval.ms`:

```yaml
spring:
  kafka:
    consumer:
      max-poll-records: 50          # process 50 records per poll
      properties:
        max.poll.interval.ms: 300000  # 5 minutes (default, generous margin)
```

As a rule of thumb: `max.poll.records × worst_case_processing_time_per_record` should be significantly less than `max.poll.interval.ms`. A 2x–3x safety margin is reasonable.

> **Key insight**: When you see unexpected rebalances in your logs, the first thing to check is whether `max.poll.records × average processing time` is approaching `max.poll.interval.ms`. Reducing `max.poll.records` is almost always safer than increasing `max.poll.interval.ms`, because a lower value reduces the blast radius of each batch and allows the consumer to commit offsets more frequently.

---

## 5.7 Idempotent Consumers

### Why idempotency matters

Kafka's delivery guarantee to consumers is **at-least-once**. This means that every record will be delivered at least once, but it may be delivered more than once. Duplicates are not a bug in Kafka — they are a fundamental consequence of the at-least-once design.

### When duplicates occur

| Scenario | What Happens |
|---|---|
| **Rebalance before offset commit** | Consumer processes records 0–9, but a rebalance occurs before offsets are committed. The new partition owner replays from the last committed offset, reprocessing records that were already handled. |
| **Consumer restart** | The consumer crashes after processing a record but before committing its offset. On restart, it replays from the last committed offset. |
| **Producer retry** | Even with an idempotent producer, edge cases (such as log truncation or broker failover) can occasionally result in duplicate records in the topic itself. |
| **Manual commit failure** | The consumer processes a record, attempts to commit the offset, but the commit fails (e.g., due to a rebalance in progress). The record is redelivered. |

In a production system running 24/7, these scenarios are not theoretical — they are routine. The question is not *if* duplicates will happen, but *how often* and *whether your application handles them correctly*.

### Strategies for idempotent processing

#### Strategy 1: Database upserts

Instead of using `INSERT`, which fails or creates a duplicate row when the record already exists, use an upsert operation that updates the row if it exists or inserts it if it does not:

```java
package com.example.kafkalearning.repository;

import com.example.kafkalearning.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, String> {

    /**
     * Upsert: insert the order or update it if it already exists.
     * Uses PostgreSQL ON CONFLICT syntax. Adjust for your database.
     */
    @Modifying
    @Query(value = """
        INSERT INTO orders (order_id, customer_id, amount, status, updated_at)
        VALUES (:#{#order.orderId}, :#{#order.customerId}, :#{#order.amount},
                :#{#order.status}, :#{#order.updatedAt})
        ON CONFLICT (order_id) DO UPDATE SET
            status = EXCLUDED.status,
            amount = EXCLUDED.amount,
            updated_at = EXCLUDED.updated_at
        """, nativeQuery = true)
    void upsert(OrderEntity order);
}
```

The key advantage of upserts is that processing the same record twice produces the same result as processing it once — the row simply gets overwritten with identical data.

#### Strategy 2: Deduplication with event ID

If your events carry a unique `eventId` (as recommended in [Section 3](section-3-message-contract-design.md)), you can maintain a deduplication table that tracks which events have already been processed:

```java
package com.example.kafkalearning.consumer;

import com.example.kafkalearning.dto.OrderEvent;
import com.example.kafkalearning.repository.ProcessedEventRepository;
import com.example.kafkalearning.entity.ProcessedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class DeduplicatingOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeduplicatingOrderConsumer.class);

    private final ProcessedEventRepository processedEventRepository;
    private final OrderService orderService;

    public DeduplicatingOrderConsumer(ProcessedEventRepository processedEventRepository,
                                      OrderService orderService) {
        this.processedEventRepository = processedEventRepository;
        this.orderService = orderService;
    }

    @KafkaListener(
        topics = "${app.kafka.topic.orders}",
        groupId = "order-processing-group"
    )
    @Transactional
    public void handle(ConsumerRecord<String, OrderEvent> record,
                       Acknowledgment acknowledgment) {

        String eventId = record.value().getEventId();

        // Check if this event has already been processed
        if (processedEventRepository.existsById(eventId)) {
            log.info("Duplicate event detected, skipping: eventId={}, offset={}",
                    eventId, record.offset());
            acknowledgment.acknowledge();
            return;
        }

        // Process the event
        orderService.processOrder(record.value());

        // Record that this event has been processed
        processedEventRepository.save(new ProcessedEvent(eventId, Instant.now()));

        acknowledgment.acknowledge();
        log.info("Processed event: eventId={}, offset={}", eventId, record.offset());
    }
}
```

> **Key insight**: The deduplication check and the business logic should be in the **same database transaction**. If they are not, a crash between the two operations can leave the system in an inconsistent state — either the event is marked as processed but the business logic did not complete, or the business logic completed but the event is not marked as processed (allowing a duplicate).

#### Strategy 3: Idempotency keys in business logic

Some business operations are naturally idempotent if you design them that way. For example, instead of "increment the customer's balance by $50," model the operation as "set the balance for transaction T-123 to $50." If the same transaction is applied twice, the result is the same.

#### Strategy 4: Natural idempotency (state setting vs mutation)

| Non-Idempotent (Dangerous) | Idempotent (Safe) |
|---|---|
| `UPDATE accounts SET balance = balance + 50` | `INSERT INTO transactions (txn_id, amount) VALUES ('T-123', 50) ON CONFLICT DO NOTHING` then recalculate balance from transactions |
| `INSERT INTO orders (...)` | `INSERT INTO orders (...) ON CONFLICT (order_id) DO UPDATE SET ...` |
| `counter++` | `latestValue = newValue` (set, don't increment) |
| Send email notification | Check `notification_sent` flag before sending |

### Comparing idempotency strategies

| Strategy | Pros | Cons | Best For |
|---|---|---|---|
| **Database upserts** | Simple, no extra tables, leverages DB features. | Requires a natural unique key. Not all databases support upserts equally. | CRUD operations on entities with a stable ID. |
| **Deduplication table** | Works for any operation, regardless of whether it is naturally idempotent. | Requires an extra table and an extra query per record. Table must be periodically cleaned up. | Complex operations with side effects (emails, HTTP calls). |
| **Idempotency keys** | No extra infrastructure. Business logic handles duplicates inherently. | Requires careful domain modeling. Not always possible for all operations. | Financial transactions, inventory management. |
| **Natural idempotency** | Most elegant. No overhead. | Requires rethinking how state changes are modeled. | Event-sourced systems, state-machine-based processing. |

The right choice depends on your application's requirements, but the mindset is universal: **Kafka gives you durable delivery mechanics; your application must still make business processing safe.**

---

## 5.8 Poison Messages (Deserialization Errors)

### What is a poison message?

A **poison message** (sometimes called a "poison pill") is a record that can never be successfully processed, no matter how many times the consumer retries it. The most common type is a **deserialization error** — a record whose bytes cannot be converted into the expected Java object because the data is corrupt, the schema has changed incompatibly, or the JSON is malformed.

### The infinite crash loop

Without special handling, a poison message creates a devastating failure pattern:

```
The poison message loop:

  poll() returns records       deserialize record 7       EXCEPTION!
  including record 7                  │                       │
    │                                 │                       │
    ▼                                 ▼                       ▼
────┬─────────────────────────────────┬───────────────────────┬────
    │                                 │                       │
    │  Records 5, 6, 7, 8, 9         │  Record 7: invalid    │
    │  fetched from broker            │  JSON / wrong schema  │  Consumer crashes
    │                                 │                       │  or throws exception
    │                                 │                       │
    │                                                         │
    │  Offset was not committed (crash happened during processing)
    │                                                         │
    │  Consumer restarts → resumes from last committed offset │
    │  → fetches records 5, 6, 7, 8, 9 again                 │
    │  → tries to deserialize record 7 again                  │
    │  → SAME EXCEPTION                                       │
    │  → INFINITE LOOP                                        │
    └─────────────────────────────────────────────────────────┘
```

This loop will continue forever because the record's content never changes. The consumer will never advance past offset 7, and records 8, 9, and all subsequent records are blocked.

### ErrorHandlingDeserializer

Spring Kafka provides the `ErrorHandlingDeserializer`, which wraps your real deserializer and catches deserialization exceptions. Instead of letting the exception propagate (which would crash the consumer), it creates a record with a `null` payload and stores the exception details in the record headers.

### Configuring ErrorHandlingDeserializer in application.yml

```yaml
spring:
  kafka:
    consumer:
      # Use ErrorHandlingDeserializer as the outer deserializer
      key-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        # Tell ErrorHandlingDeserializer which real deserializer to delegate to
        spring.deserializer.key.delegate.class: org.apache.kafka.common.serialization.StringDeserializer
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: "com.example.kafkalearning.dto"
```

### What the consumer sees

When a deserialization error occurs, the `ErrorHandlingDeserializer` delivers a record with:

- A `null` value (or `null` key, if the key deserialization failed).
- Exception details stored in record headers, including the original bytes and the exception class.

Your listener can detect this and handle it accordingly:

```java
package com.example.kafkalearning.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class PoisonMessageAwareConsumer {

    private static final Logger log = LoggerFactory.getLogger(PoisonMessageAwareConsumer.class);

    private static final String DESERIALIZATION_EXCEPTION_HEADER =
            "springDeserializerExceptionValue";

    @KafkaListener(
        topics = "${app.kafka.topic.orders}",
        groupId = "order-processing-group"
    )
    public void handle(ConsumerRecord<String, Object> record,
                       Acknowledgment acknowledgment) {

        // Check for deserialization errors
        Header exceptionHeader = record.headers()
                .lastHeader(DESERIALIZATION_EXCEPTION_HEADER);

        if (exceptionHeader != null) {
            log.error("Poison message detected at topic={}, partition={}, offset={}. "
                    + "Skipping record and acknowledging to prevent infinite loop.",
                    record.topic(), record.partition(), record.offset());

            // In production, route this to a dead-letter topic (see Section 6)
            acknowledgment.acknowledge();
            return;
        }

        // Normal processing
        processRecord(record);
        acknowledgment.acknowledge();
    }

    private void processRecord(ConsumerRecord<String, Object> record) {
        // Business logic here
    }
}
```

### Routing poison messages to a dead-letter topic

In a production system, you do not simply log and skip poison messages — you route them to a **dead-letter topic** (DLT) where they can be inspected, debugged, and potentially replayed after the root cause is fixed. This pattern is covered in depth in [Section 6](section-6-error-handling-retries-dlt.md), but the basic idea is:

1. The consumer detects a deserialization error.
2. It publishes the original raw bytes to a dedicated DLT (e.g., `order-events.DLT`).
3. It acknowledges the original record so the consumer can move forward.
4. A separate process monitors the DLT for manual investigation or automated replay.

> **Key insight**: The `ErrorHandlingDeserializer` is a non-negotiable requirement for any production consumer. Without it, a single malformed record can halt consumption for an entire partition indefinitely. Configuring it takes five lines of YAML and prevents one of Kafka's most common production outages.

---

## 5.9 Building a Production-Ready Consumer

The previous sections introduced individual concepts. This section combines them into a single, complete consumer that demonstrates how they work together in a realistic scenario.

### Requirements

The production-ready consumer must:

1. Use **manual acknowledgment** to control exactly when offsets are committed.
2. **Validate payloads** before processing (reject structurally invalid events).
3. **Classify errors** as recoverable (retry) or non-recoverable (skip/route to DLT).
4. **Log full context** — topic, partition, offset, and key — with every log message.
5. **Handle duplicates** via event ID deduplication.
6. **Handle poison messages** from deserialization errors.

### Package structure

```
src/main/java/com/example/kafkalearning/
├── config/
│   └── KafkaConsumerConfig.java
├── consumer/
│   └── ProductionOrderConsumer.java
├── dto/
│   └── OrderEvent.java
├── entity/
│   ├── OrderEntity.java
│   └── ProcessedEvent.java
├── repository/
│   ├── OrderRepository.java
│   └── ProcessedEventRepository.java
└── service/
    └── OrderService.java
```

### The consumer

```java
package com.example.kafkalearning.consumer;

import com.example.kafkalearning.dto.OrderEvent;
import com.example.kafkalearning.repository.ProcessedEventRepository;
import com.example.kafkalearning.entity.ProcessedEvent;
import com.example.kafkalearning.service.OrderService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class ProductionOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProductionOrderConsumer.class);
    private static final String DESER_EXCEPTION_HEADER = "springDeserializerExceptionValue";

    private final ProcessedEventRepository processedEventRepository;
    private final OrderService orderService;

    public ProductionOrderConsumer(ProcessedEventRepository processedEventRepository,
                                    OrderService orderService) {
        this.processedEventRepository = processedEventRepository;
        this.orderService = orderService;
    }

    @KafkaListener(
        topics = "${app.kafka.topic.orders}",
        groupId = "order-processing-group",
        concurrency = "3"
    )
    @Transactional
    public void handle(ConsumerRecord<String, OrderEvent> record,
                       Acknowledgment acknowledgment) {

        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String key = record.key();

        // ── Step 1: Check for poison messages (deserialization errors) ────
        Header exceptionHeader = record.headers().lastHeader(DESER_EXCEPTION_HEADER);
        if (exceptionHeader != null) {
            log.error("[POISON] Deserialization error: topic={}, partition={}, offset={}, key={}",
                    topic, partition, offset, key);
            // In production, route raw bytes to a dead-letter topic here (see Section 6)
            acknowledgment.acknowledge();
            return;
        }

        OrderEvent event = record.value();

        // ── Step 2: Validate the payload ─────────────────────────────────
        if (event == null || event.getEventId() == null || event.getOrderId() == null) {
            log.error("[INVALID] Missing required fields: topic={}, partition={}, offset={}, key={}",
                    topic, partition, offset, key);
            // Non-recoverable: acknowledge to skip, optionally route to DLT
            acknowledgment.acknowledge();
            return;
        }

        String eventId = event.getEventId();

        // ── Step 3: Deduplication check ──────────────────────────────────
        if (processedEventRepository.existsById(eventId)) {
            log.info("[DUPLICATE] Already processed: eventId={}, topic={}, partition={}, offset={}",
                    eventId, topic, partition, offset);
            acknowledgment.acknowledge();
            return;
        }

        // ── Step 4: Business logic ───────────────────────────────────────
        try {
            orderService.processOrder(event);
        } catch (RecoverableProcessingException e) {
            log.warn("[RECOVERABLE] Will be retried: eventId={}, topic={}, partition={}, "
                    + "offset={}, error={}", eventId, topic, partition, offset, e.getMessage());
            // Do NOT acknowledge — Spring Kafka's error handler will retry
            throw e;
        } catch (Exception e) {
            log.error("[NON-RECOVERABLE] Routing to DLT: eventId={}, topic={}, partition={}, "
                    + "offset={}, error={}", eventId, topic, partition, offset, e.getMessage(), e);
            // Acknowledge to skip this record; route to DLT in error handler (Section 6)
            acknowledgment.acknowledge();
            return;
        }

        // ── Step 5: Record successful processing ─────────────────────────
        processedEventRepository.save(new ProcessedEvent(eventId, Instant.now()));
        acknowledgment.acknowledge();

        log.info("[SUCCESS] Processed: eventId={}, topic={}, partition={}, offset={}",
                eventId, topic, partition, offset);
    }
}
```

### The exception class for recoverable errors

```java
package com.example.kafkalearning.consumer;

/**
 * Indicates a transient failure that should be retried.
 * Examples: database timeout, temporary network issue, downstream service unavailable.
 */
public class RecoverableProcessingException extends RuntimeException {

    public RecoverableProcessingException(String message) {
        super(message);
    }

    public RecoverableProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

### YAML configuration

```yaml
spring:
  application:
    name: kafka-consumer-production

  kafka:
    bootstrap-servers: localhost:9092

    consumer:
      group-id: order-processing-group
      auto-offset-reset: earliest

      # Use ErrorHandlingDeserializer to prevent poison message crash loops
      key-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer

      properties:
        # Delegate deserializers (the real ones)
        spring.deserializer.key.delegate.class: org.apache.kafka.common.serialization.StringDeserializer
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: "com.example.kafkalearning.dto"

        # Cooperative rebalancing for minimal processing interruption
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor

        # Rebalance tuning
        session.timeout.ms: 15000
        heartbeat.interval.ms: 5000

        # Processing time tuning
        max.poll.interval.ms: 300000
      max-poll-records: 50

    listener:
      ack-mode: manual_immediate
      concurrency: 3

app:
  kafka:
    topic:
      orders: order-events
```

### How the pieces fit together

The following diagram shows the flow of a record through the production consumer:

```
                          ┌──────────────────────┐
                          │   Kafka Broker        │
                          │   (order-events)      │
                          └──────────┬───────────┘
                                     │
                                     │  poll()
                                     ▼
                          ┌──────────────────────┐
                          │ ErrorHandling         │
                          │ Deserializer          │
                          └──────────┬───────────┘
                                     │
                           ┌─────────┴─────────┐
                           │                   │
                     deserialize OK      deserialize FAILED
                           │                   │
                           ▼                   ▼
                    ┌─────────────┐    ┌───────────────┐
                    │  Validate   │    │  Log + Ack    │
                    │  payload    │    │  (poison msg) │
                    └──────┬──────┘    └───────────────┘
                           │
                    ┌──────┴──────┐
                    │             │
               valid          invalid
                    │             │
                    ▼             ▼
             ┌────────────┐  ┌──────────────┐
             │  Dedup     │  │  Log + Ack   │
             │  check     │  │  (invalid)   │
             └─────┬──────┘  └──────────────┘
                   │
            ┌──────┴──────┐
            │             │
        new event     duplicate
            │             │
            ▼             ▼
     ┌────────────┐  ┌──────────────┐
     │  Business  │  │  Log + Ack   │
     │  logic     │  │  (skip dup)  │
     └─────┬──────┘  └──────────────┘
           │
    ┌──────┴──────┐
    │             │
  success      failure
    │             │
    │      ┌──────┴──────┐
    │      │             │
    │  recoverable  non-recoverable
    │      │             │
    ▼      ▼             ▼
  ┌─────┐ ┌──────┐  ┌──────────────┐
  │ Ack │ │Throw │  │  Log + Ack   │
  │     │ │(retry│  │  (route to   │
  │     │ │ by   │  │   DLT)       │
  └─────┘ │Spring│  └──────────────┘
          │Kafka)│
          └──────┘
```

---

## 5.10 Practical Exploration

The concepts in this section are best understood by observing them in action. The exercises below are designed to build intuition for consumer behavior under various conditions.

### Exercise 1: Observing auto-commit vs manual commit

**Goal**: See the difference between auto-commit and manual-commit behavior when a consumer crashes mid-processing.

1. **Setup**: Create a topic with 1 partition. Produce 20 messages.
2. **Auto-commit run**: Configure a consumer with `enable.auto.commit=true` and `auto.commit.interval.ms=10000` (10 seconds). Add a `Thread.sleep(1000)` in the listener to slow processing. After processing ~5 records, kill the application forcefully (`kill -9`). Restart and observe which offset it resumes from.
3. **Manual-commit run**: Configure the same consumer with `ack-mode: manual_immediate`. Repeat the same test — process ~5 records, kill, restart. Observe the difference.

**What to look for**: With auto-commit, the consumer may skip records (if the timer fired) or reprocess records (if the timer had not fired). With manual commit, the consumer precisely resumes from the last acknowledged record.

### Exercise 2: Simulating a slow consumer to trigger rebalancing

**Goal**: Observe what happens when a consumer exceeds `max.poll.interval.ms`.

1. **Setup**: Create a topic with 3 partitions. Set `max.poll.interval.ms` to 10000 (10 seconds) and `max.poll.records` to 100.
2. **Slow processing**: Add a `Thread.sleep(200)` per record in the listener. Produce 200 messages.
3. **Observe**: Watch the logs for rebalance events. The consumer should be kicked from the group after 10 seconds of processing without calling `poll()`.
4. **Fix**: Reduce `max.poll.records` to 10 and repeat. Observe that the consumer no longer gets kicked.

### Exercise 3: Testing idempotency by replaying messages

**Goal**: Verify that your consumer handles duplicate messages correctly.

1. **Setup**: Implement the deduplication table approach from Section 5.7.
2. **Produce**: Send 10 messages with distinct event IDs.
3. **Process**: Let the consumer process all 10 messages.
4. **Replay**: Use the Kafka CLI to reset the consumer group's offsets to the beginning:
   ```bash
   kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
       --group order-processing-group \
       --topic order-events \
       --reset-offsets --to-earliest --execute
   ```
5. **Observe**: Restart the consumer. It should log that all 10 messages are duplicates and skip them.

### Exercise 4: Observing a consumer crash mid-processing

**Goal**: Understand the relationship between offset commit timing and message reprocessing.

1. **Setup**: Configure the consumer with `ack-mode: batch` (the default).
2. **Produce**: Send 20 messages.
3. **Crash**: Add logic to throw an exception on the 10th message. Observe which records are reprocessed on recovery.
4. **Repeat with `ack-mode: record`**: Observe that only the failed record (and subsequent records) are reprocessed, not the entire batch.

---

## 5.11 Common Mistakes

The following table catalogs the most frequently encountered mistakes in Kafka consumer design, along with the symptom each produces and the corrective action.

| Mistake | Symptom | Fix |
|---|---|---|
| **Using auto-commit for important business processing** | Messages silently lost after a crash (offsets were committed before processing finished). | Use `ack-mode: manual_immediate` or `ack-mode: record`. Disable `enable.auto.commit`. |
| **Not handling deserialization errors** | Consumer crashes on a malformed record, restarts, crashes again — infinite loop. All subsequent messages in the partition are blocked. | Configure `ErrorHandlingDeserializer` as shown in Section 5.8. |
| **Setting `max.poll.records` too high with slow processing** | Frequent unexpected rebalances. Consumer logs show "member has failed to heartbeat" or "poll interval exceeded." Processing throughput drops. | Reduce `max.poll.records` to a value where worst-case batch processing time is well under `max.poll.interval.ms`. |
| **Assuming exactly-once delivery without idempotent code** | Duplicate database rows, double-charged payments, duplicate emails. Often discovered only in production under load. | Implement one of the idempotency strategies from Section 5.7. Design business logic to be safe for reprocessing. |
| **Not logging partition, offset, and key** | When a processing error occurs, there is no way to trace it back to the specific Kafka record. Debugging requires guesswork. | Always include `record.topic()`, `record.partition()`, `record.offset()`, and `record.key()` in log messages. |
| **Processing messages with non-idempotent side effects** | Sending duplicate emails, double-posting to external APIs, creating duplicate records in downstream systems. | Gate side effects behind idempotency checks. Use deduplication tables for operations that cannot be made naturally idempotent. |
| **Not configuring `ErrorHandlingDeserializer`** | Identical to "not handling deserialization errors" — but often discovered after a schema change breaks backward compatibility. | Always use `ErrorHandlingDeserializer` in production. It costs nothing when there are no errors and saves you from outages when there are. |
| **Setting `concurrency` higher than partition count** | Idle consumer threads that consume resources but do no work. May cause confusion during troubleshooting. | Set `concurrency` ≤ partition count. If you need more parallelism, increase the partition count first. |
| **Ignoring `auto.offset.reset` during first deployment** | Consumer starts with `latest` and silently misses all messages that were produced before it started. | Use `earliest` for first deployments where you need to process the backlog. Switch to `latest` only when you explicitly want to skip historical data. |
| **Using eager rebalancing in high-throughput environments** | Long processing pauses during deployments and scaling events. All consumers stop, even those whose partitions did not change. | Configure `CooperativeStickyAssignor` to minimize rebalance impact. |

> **Key insight**: Most of these mistakes are invisible during development and testing — they only manifest under production conditions: real load, real failures, real restarts. This is why understanding consumer design at a conceptual level matters more than memorizing configuration properties.

---

## 5.12 Key Takeaways

Consumer design is where Kafka's theoretical guarantees meet the reality of application correctness. The producer's job is simple: deliver a record to the broker. The consumer's job is complex: receive the record, process it safely, handle every failure mode, commit the offset at the right moment, and ensure that the entire process is resilient to crashes, rebalances, and duplicates.

Offsets are the foundation of consumer reliability, and the choice between auto-commit and manual commit has direct consequences for message loss and duplication. Auto-commit is a convenience that is appropriate only for non-critical workloads; for anything that matters, manual acknowledgment gives the application explicit control over when Kafka considers a record processed. The `auto.offset.reset` setting governs startup behavior when no committed offset exists — `earliest` is the safer default for most scenarios, while `latest` has its place in high-volume pipelines where historical replay is unwanted.

Rebalancing is an unavoidable part of consumer group operation, triggered by scaling events, failures, and deployments. Cooperative rebalancing with `CooperativeStickyAssignor` dramatically reduces the processing disruption compared to the default eager protocol. Concurrency, controlled by Spring Kafka's `concurrency` setting, allows a single application instance to run multiple consumer threads — but the partition count is the upper bound on useful parallelism within a single consumer group. The `max.poll.records` and `max.poll.interval.ms` settings must be tuned together to prevent the consumer from being kicked during slow processing.

Idempotency is not optional. Kafka provides at-least-once delivery, which means duplicates will occur in production. Database upserts, deduplication tables, and idempotency keys are the standard strategies for making business logic safe against reprocessing. Poison messages — records that can never be deserialized — require the `ErrorHandlingDeserializer` to prevent infinite crash loops that block an entire partition.

Kafka gives you durable delivery mechanics; your application must still make business processing safe. Offsets alone do not guarantee your database writes are idempotent. The consumer is where correctness lives, and every concept in this section exists to help you build consumers that behave correctly under the conditions that matter most: real failures in production.

**Next**: [Section 6: Error Handling, Retries, and Dead-Letter Topics](section-6-error-handling-retries-dlt.md) — where the focus shifts to classifying exceptions, configuring retry backoff, routing unrecoverable failures to dead-letter topics, and building operational workflows for failed-record remediation.
