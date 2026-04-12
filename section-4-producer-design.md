# Section 4: Producer Design for Real Applications

In the previous sections, producers appeared mostly as one-liner calls to `KafkaTemplate.send()` — enough to demonstrate topics, partitions, serialization, and message contracts. But a producer that is safe to run in production involves far more than calling `send()` and hoping for the best. Every call to `send()` is a decision about durability (will the record survive a broker crash?), ordering (will records arrive in the sequence the business expects?), efficiency (how many network round trips will this cost?), and observability (when something goes wrong at 2 AM, will the logs tell you what happened?). This section examines each of those dimensions in detail. It covers key selection, acknowledgment modes, retry semantics, batching, compression, idempotent production, record headers, and finally pulls all of these concerns together into a reusable producer module that is suitable for real Spring Boot applications.

---

## 4.1 Why Producer Design Matters

### The gap between demo code and production code

Section 2 introduced a producer that looked roughly like this:

```java
kafkaTemplate.send(topic, key, event);
```

That single line conceals a remarkable number of decisions that the Kafka client makes on your behalf — decisions that have default values which may or may not be appropriate for your application. Among them:

- How many brokers must confirm the write before the call is considered successful?
- What happens if the broker is temporarily unreachable?
- How long should the client buffer messages before sending them over the network?
- Should the payload be compressed?
- What happens if the client retries a send and the broker received the first attempt — will there be duplicates?

In a demo or tutorial, the defaults are fine. In a system processing payments, inventory updates, or user-facing notifications, each of these defaults deserves deliberate examination.

### Demo-quality vs production-quality producers

The following table illustrates the difference between a producer written for a tutorial and one written for production:

| Concern | Demo Producer | Production Producer |
|---|---|---|
| Acknowledgments | Default (`acks=1` or unset) | `acks=all` — every in-sync replica confirms |
| Retries | Default or unset | Explicit `delivery.timeout.ms`, idempotence enabled |
| Key strategy | Hardcoded or random | Deliberate, based on business ordering requirements |
| Error handling | None — fire and forget | Callback logs key, topic, partition; alerts on failure |
| Compression | None | `snappy`, `lz4`, or `zstd` depending on workload |
| Topic names | Hardcoded strings | Centralized configuration, injected via properties |
| Headers | None | Trace IDs, correlation IDs, event type metadata |
| Observability | `System.out.println` | Structured logging with topic, partition, offset |

This section works through each row of that table, explaining the what, the why, and the how.

### What this section covers

The subsections that follow address producer concerns in the order they typically matter during design:

1. **Key selection** — how keys control partition routing and ordering
2. **Acknowledgments** — how many brokers must confirm a write
3. **Retries and delivery semantics** — what happens when a send fails transiently
4. **Batching** — how the client groups messages for network efficiency
5. **Compression** — reducing payload size on the wire and on disk
6. **Idempotent production** — preventing duplicates caused by retries
7. **Record headers** — attaching metadata without modifying the event payload
8. **A reusable producer module** — pulling all concerns into a production-ready component
9. **Production habits and common mistakes** — patterns to follow and anti-patterns to avoid

---

## 4.2 Keys: Ordering and Partition Selection

### Recap: how keys determine partitions

Section 1 introduced the fundamental rule: when a Kafka record carries a key, the client hashes the key and maps the result to a partition. The formula is:

```
partition = hash(key) % numberOfPartitions
```

The default hash function in the Java client is **murmur2**. This means that for the same key and the same number of partitions, the record always lands in the same partition — and because records within a partition are strictly ordered, all records sharing a key are ordered relative to each other.

### DefaultPartitioner behavior

The Kafka Java producer uses a `DefaultPartitioner` that follows two rules:

| Condition | Behavior | Ordering Guarantee |
|---|---|---|
| Key is **not null** | `murmur2(keyBytes) % partitionCount` — deterministic partition | All records with the same key land in the same partition, preserving order |
| Key is **null** | **Sticky partitioning** — records are batched to a single partition until the batch is full, then the next batch targets a different partition | No ordering guarantee; load is spread across partitions over time |

> **Key insight**: The sticky partitioner (introduced in KIP-480) replaced the older round-robin
> behavior for null-key records. Sticky partitioning improves batching efficiency because
> consecutive null-key records go to the same partition until a batch is sent, rather than
> scattering one record per partition per batch.

### Why key choice is a producer responsibility

The key is set by the producer at send time. Consumers have no control over how records are distributed across partitions — they receive whatever partition assignment the group coordinator gives them. This means that **partition routing and ordering are entirely producer-side decisions**.

Choosing the wrong key has consequences that are difficult to fix after the fact:

- **Too few distinct key values** (e.g., using `country` as the key in a global system with 5 countries and 12 partitions) creates hot partitions — a small number of partitions handle most of the traffic.
- **Too many distinct key values with no business meaning** (e.g., a random UUID per message) distributes load perfectly but provides no per-entity ordering.
- **Changing the key strategy** after a topic is in production can break consumers that depend on per-key ordering.

### Building a key strategy

A good key answers the question: *which records must be ordered relative to each other?*

| Business Domain | Key Choice | Why |
|---|---|---|
| Order lifecycle | `orderId` | All events for a single order (created, confirmed, shipped) must be processed in sequence |
| User activity | `userId` | A user's actions must be ordered (login → browse → purchase) |
| IoT telemetry | `deviceId` | Per-device readings must arrive in time order |
| Payment processing | `paymentId` | Each payment's state transitions must be ordered |
| Multi-entity operations | `orderId:itemId` (composite) | When ordering matters at a granularity finer than the top-level entity |

### Composite keys

Sometimes a single field does not capture the right ordering boundary. For example, in an e-commerce system where each order contains multiple line items that can be updated independently, using `orderId` alone groups all line-item updates into one partition — which preserves ordering but may create hot partitions for large orders. A composite key like `orderId:itemId` provides per-item ordering while distributing load more evenly.

```java
// Building a composite key
String key = order.getOrderId() + ":" + item.getItemId();
kafkaTemplate.send(topic, key, event);
```

### Code example: key selection in a producer

```java
package com.example.kafkalearning.producer;

import com.example.kafkalearning.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Value("${app.kafka.topic.orders}")
    private String ordersTopic;

    public OrderEventProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Sends an order event using the orderId as the key.
     * This guarantees that all events for the same order land in the same
     * partition and are consumed in order.
     */
    public void publish(OrderEvent event) {
        String key = event.getOrderId(); // orderId drives partition selection

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(ordersTopic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send event key={} topic={}", key, ordersTopic, ex);
            } else {
                log.info("Sent event key={} topic={} partition={} offset={}",
                        key,
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
```

### What happens with null keys

When a producer sends a record without a key, the sticky partitioner assigns it to the current "active" partition for that topic. Once the batch targeting that partition is full (or `linger.ms` expires), the partitioner switches to a different partition. Over many sends, null-key records distribute roughly evenly across all partitions.

This is appropriate when ordering does not matter — for example, log aggregation or metric collection where every record is independent.

```java
// Null key — no ordering guarantee, load-balanced across partitions
kafkaTemplate.send(topic, null, logEntry);
```

### Summary of partitioning behaviors

| Key | Partitioner Behavior | Ordering | Load Distribution |
|---|---|---|---|
| Non-null, high cardinality | `murmur2(key) % N` — deterministic | Per-key ordering guaranteed | Even if key values are well-distributed |
| Non-null, low cardinality | `murmur2(key) % N` — deterministic | Per-key ordering guaranteed | Uneven — hot partitions likely |
| Null | Sticky partitioning (batch-level round-robin) | No ordering guarantee | Even over time |
| Custom partitioner | User-defined logic | Depends on implementation | Depends on implementation |

---

## 4.3 Acknowledgments (acks)

### What acknowledgments mean

When a producer sends a record to Kafka, the broker can respond at different stages of the write process. The `acks` configuration controls **how many brokers must confirm they have received and persisted the record** before the producer considers the write successful.

This is a fundamental durability knob. Setting it incorrectly can lead to silent data loss in production.

### The three acks settings

#### `acks=0` — fire and forget

The producer does not wait for any confirmation from the broker. As soon as the record is placed on the network buffer, the send is considered complete.

```
Producer  ──────▶  Leader Broker
                   (no response awaited)
```

- **Latency**: Lowest possible
- **Durability**: None — if the broker crashes before writing the record, it is lost
- **Use case**: Metrics, non-critical logging where speed matters more than completeness

#### `acks=1` — leader only

The producer waits until the partition leader has written the record to its local log. Follower replicas may not have received it yet.

```
Producer  ──────▶  Leader Broker  ──── (replicating to followers) ────▶  Follower 1
                        │                                                Follower 2
                        │
                   ◀── "acknowledged"
```

- **Latency**: Low
- **Durability**: Moderate — if the leader crashes *after* acknowledging but *before* followers have replicated, the record is lost
- **Use case**: Systems where occasional message loss is acceptable and low latency is important

#### `acks=all` (or `acks=-1`) — all in-sync replicas

The producer waits until the leader and **all in-sync replicas (ISR)** have written the record. This is the strongest guarantee Kafka offers.

```
Producer  ──────▶  Leader Broker  ──────▶  Follower 1 (ISR) ✓
                        │                   Follower 2 (ISR) ✓
                        │
                   ◀── "acknowledged" (only after all ISR have written)
```

- **Latency**: Highest (waits for replication)
- **Durability**: Strongest — the record survives the loss of any single broker
- **Use case**: Any data you cannot afford to lose (financial transactions, orders, user actions)

### Relationship between acks and min.insync.replicas

`acks=all` alone is not sufficient for strong durability. If only one replica is in sync (the leader itself), then `acks=all` degrades to the same behavior as `acks=1`. The broker-side setting `min.insync.replicas` controls the **minimum number of replicas that must be in sync** for the leader to accept writes.

A typical production configuration uses:

- `acks=all` on the producer
- `min.insync.replicas=2` on the topic (or broker-wide)
- Replication factor of 3

This means that at least 2 out of 3 replicas must acknowledge the write. If fewer than 2 replicas are in sync (for example, two brokers are down), the leader rejects the write with a `NotEnoughReplicasException`, which the producer can then retry.

> **Key insight**: `acks=all` + `min.insync.replicas=2` + replication factor 3 is the standard
> production configuration for data you cannot afford to lose. It tolerates one broker failure
> while still accepting writes. Setting `min.insync.replicas` equal to the replication factor
> means the topic becomes read-only if any single replica is down — this is almost never
> desirable.

### Comparison table

| Setting | Confirmation Scope | Durability | Latency | Data Loss Risk |
|---|---|---|---|---|
| `acks=0` | None | No guarantee | Lowest | High — any broker issue loses data |
| `acks=1` | Leader only | Moderate | Low | Moderate — leader crash before replication loses data |
| `acks=all` | All in-sync replicas | Strongest | Highest | Minimal — requires multiple broker failures |

### YAML configuration

```yaml
spring:
  kafka:
    producer:
      acks: all                    # strongest durability guarantee
    properties:
      # Broker-side setting (configured per-topic or cluster-wide, shown here for reference)
      # min.insync.replicas: 2
```

In Spring Boot, the `acks` property maps directly to the Kafka producer configuration property `acks`. Setting it to `all` in `application.yml` ensures that every producer created by the auto-configured `KafkaTemplate` uses full ISR acknowledgment.

---

## 4.4 Retries and Delivery Semantics

### Why retries exist

Network connections break. Broker leaders change during rolling restarts. A broker temporarily runs out of disk space. None of these failures are permanent — they resolve within seconds or minutes. Retries allow the producer to survive these transient failures without burdening application code with manual resend logic.

### The retry configuration landscape

Modern Kafka producers (2.1+) use a combination of properties to control retry behavior:

| Property | Default | What It Controls |
|---|---|---|
| `retries` | `2147483647` (MAX_INT) | Maximum number of retry attempts per record |
| `delivery.timeout.ms` | `120000` (2 minutes) | Total time allowed from `send()` to successful acknowledgment, including retries |
| `retry.backoff.ms` | `100` | Delay between consecutive retry attempts |
| `retry.backoff.max.ms` | `1000` | Maximum delay between retries (exponential backoff cap) |
| `request.timeout.ms` | `30000` | Timeout for a single broker request |

The relationship between these properties is important. The producer retries a failed send until **either** the retry count is exhausted **or** `delivery.timeout.ms` is exceeded — whichever comes first. Since the default retry count is effectively infinite, `delivery.timeout.ms` is the property that actually governs how long the producer will try.

### How retries work in practice

```
send() called
    │
    ├── Attempt 1: broker unreachable → wait retry.backoff.ms → retry
    ├── Attempt 2: broker unreachable → wait retry.backoff.ms → retry
    ├── Attempt 3: leader election in progress → wait retry.backoff.ms → retry
    ├── Attempt 4: success → ack returned to producer
    │
    └── If delivery.timeout.ms exceeded before success → TimeoutException
```

### The ordering problem with retries

When `max.in.flight.requests.per.connection` is greater than 1, the producer can have multiple batches in flight simultaneously. If batch 1 fails and is retried while batch 2 succeeds, batch 2's records arrive at the broker before batch 1's retried records — breaking the ordering that keys are supposed to guarantee.

```
Without idempotence, max.in.flight = 2:

    Batch 1 (offsets 0-4)  ──── FAIL ──── retry ────┐
    Batch 2 (offsets 5-9)  ──── OK ──────────────────┤
                                                      ▼
    Broker receives: [5,6,7,8,9] then [0,1,2,3,4]  ← ORDER BROKEN
```

There are two ways to solve this:

1. **Set `max.in.flight.requests.per.connection=1`** — only one batch is in flight at a time, so retries cannot reorder. However, this significantly reduces throughput.
2. **Enable the idempotent producer** (`enable.idempotence=true`) — the broker tracks sequence numbers and rejects out-of-order batches, forcing the producer to re-sequence. With idempotence enabled, `max.in.flight.requests.per.connection` up to 5 is safe.

> **Key insight**: The combination of `enable.idempotence=true` and
> `max.in.flight.requests.per.connection=5` provides both ordering safety and good throughput.
> This is the recommended configuration for virtually all production producers.

### Configuration example

```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 2147483647                          # effectively infinite (default)
      properties:
        enable.idempotence: true                    # prevents duplicates and ordering issues
        max.in.flight.requests.per.connection: 5    # safe with idempotence
        delivery.timeout.ms: 120000                 # 2 minutes total timeout
        retry.backoff.ms: 100                       # initial backoff between retries
        request.timeout.ms: 30000                   # single-request timeout
```

### Understanding delivery.timeout.ms

The `delivery.timeout.ms` property is the single most important timeout for producer reliability. It defines the **total time budget** for a message to be sent successfully, including:

- Time spent waiting in the buffer (batching)
- Time for the initial send attempt
- Time for all retry attempts
- Backoff delays between retries

If the total time exceeds `delivery.timeout.ms`, the producer gives up and invokes the error callback (or completes the future exceptionally). Application code should handle this case — it means a message was not delivered despite extended retry attempts.

---

## 4.5 Batching: Throughput Optimization

### How Kafka batching works

The Kafka producer does not send each record individually over the network. Instead, records destined for the same partition are **buffered into batches**. When a batch is ready — either because it is full or because a time limit has elapsed — the producer sends the entire batch in a single network request. This dramatically reduces the number of round trips and improves throughput.

```
Application sends individual records:

    record A (partition 0)  ─┐
    record B (partition 0)  ─┤──▶  Batch for partition 0  ──▶  Broker (single request)
    record C (partition 0)  ─┘

    record D (partition 1)  ─┐
    record E (partition 1)  ─┘──▶  Batch for partition 1  ──▶  Broker (single request)
```

### Batching configuration properties

Three properties control batching behavior:

| Property | Default | What It Controls |
|---|---|---|
| `batch.size` | `16384` (16 KB) | Maximum size of a single batch in bytes. When a batch reaches this size, it is sent immediately. |
| `linger.ms` | `0` | How long to wait for additional records before sending a non-full batch. A value of 0 means "send immediately." |
| `buffer.memory` | `33554432` (32 MB) | Total memory available for buffering unsent records across all partitions. If this limit is reached, `send()` blocks (up to `max.block.ms`). |

### The throughput vs latency tradeoff

The `linger.ms` property is the primary tuning knob for the throughput-latency tradeoff:

- **`linger.ms=0` (default)**: The producer sends each batch as soon as any record is available. This minimizes latency but means batches are often small (sometimes containing a single record), resulting in many network round trips.

- **`linger.ms=5` to `linger.ms=50`**: The producer waits up to this many milliseconds for additional records to arrive and fill the batch. This increases average latency by the linger amount but dramatically improves throughput by sending fewer, larger batches.

- **`linger.ms=100+`**: Aggressive batching. Appropriate for high-volume, non-latency-sensitive workloads like log aggregation or batch analytics.

### ASCII diagram: batching behavior

```
Timeline (linger.ms = 20):

    t=0ms    record A arrives → starts new batch, starts linger timer
    t=5ms    record B arrives → added to batch
    t=12ms   record C arrives → added to batch
    t=20ms   linger timer expires → batch [A, B, C] sent as single request

Timeline (linger.ms = 0):

    t=0ms    record A arrives → batch [A] sent immediately
    t=5ms    record B arrives → batch [B] sent immediately
    t=12ms   record C arrives → batch [C] sent immediately

Timeline (batch.size reached before linger):

    t=0ms    record A (8 KB) arrives → starts new batch
    t=1ms    record B (10 KB) arrives → batch full (18 KB > 16 KB batch.size)
                                      → batch [A, B] sent immediately
```

### When to tune batching

| Scenario | Recommended Settings | Reason |
|---|---|---|
| Low-latency event streaming | `linger.ms=0`, `batch.size=16384` | Minimize delay; accept lower throughput |
| High-volume data pipeline | `linger.ms=20-50`, `batch.size=65536` | Maximize throughput; tolerate small latency increase |
| Log/metric collection | `linger.ms=100-200`, `batch.size=131072` | Bulk efficiency matters most; latency is not critical |
| Default (balanced) | `linger.ms=5`, `batch.size=32768` | Reasonable compromise for most applications |

### YAML configuration example

```yaml
spring:
  kafka:
    producer:
      batch-size: 32768               # 32 KB batch size
      properties:
        linger.ms: 10                  # wait up to 10ms for batch to fill
        buffer.memory: 33554432        # 32 MB total buffer
```

> **Key insight**: Increasing `linger.ms` from 0 to even 5 milliseconds can yield a significant
> throughput improvement at minimal latency cost. For most applications, a `linger.ms` between
> 5 and 20 is a good starting point.

---

## 4.6 Compression

### Why compress

Kafka records are ultimately bytes on the wire and bytes on disk. Compression reduces both:

- **Network bandwidth** — fewer bytes travel between the producer, the broker, and eventually the consumer
- **Broker disk usage** — compressed batches are stored as-is on the broker's log segments
- **Broker throughput** — less I/O per record means the broker can handle more traffic

The cost is CPU time on the producer (compressing) and on the consumer (decompressing). For most workloads, the network and storage savings far outweigh the CPU cost.

### How compression works in Kafka

Compression is applied **at the batch level**, not per individual record. The producer compresses an entire batch of records into a single compressed payload before sending it to the broker. The broker stores the compressed batch as-is. When a consumer fetches the batch, it decompresses it locally.

```
Producer                          Broker                         Consumer
┌──────────┐                 ┌──────────────┐              ┌──────────┐
│ Batch of  │   compress     │  Stores       │   fetch      │ Decompress│
│ 50 records├──────────────▶ │  compressed   ├────────────▶ │ 50 records│
│ (120 KB)  │   (e.g. 30 KB)│  batch (30 KB)│  (30 KB)     │ (120 KB)  │
└──────────┘                 └──────────────┘              └──────────┘
```

This means that the consumer does not need to be configured with a compression type — it detects the compression codec from the batch metadata and decompresses automatically.

### Compression codecs compared

| Codec | Compression Ratio | Compression Speed | Decompression Speed | CPU Cost | Best For |
|---|---|---|---|---|---|
| `none` | 1:1 (no compression) | N/A | N/A | None | When CPU is scarce and bandwidth is unlimited |
| `gzip` | Highest (~70-80% reduction) | Slowest | Moderate | Highest | Maximum compression; batch jobs where speed is less critical |
| `snappy` | Moderate (~50-60% reduction) | Fast | Very fast | Low | General-purpose; good balance of speed and ratio |
| `lz4` | Moderate (~50-60% reduction) | Fastest | Fastest | Lowest | Latency-sensitive workloads |
| `zstd` | High (~65-75% reduction) | Moderate | Fast | Moderate | Best overall ratio-to-speed; recommended default |

### Configuration

```yaml
spring:
  kafka:
    producer:
      compression-type: snappy         # or lz4, zstd, gzip
```

### When to use compression

The short answer: **almost always in production**. JSON payloads in particular compress extremely well (often 70%+ reduction) because of their repetitive field names and structural patterns. The CPU cost on modern hardware is negligible compared to the network and storage savings.

The only case where compression might not be beneficial is when the payload is already compressed (e.g., binary blobs), records are extremely small (compression overhead exceeds savings), or the producer runs on severely CPU-constrained hardware.

> **Key insight**: Compression is a producer-side configuration. Setting `compression.type`
> on the consumer has no effect — the consumer always decompresses based on the codec stored
> in the batch header. If you forget to enable compression on the producer, no compression
> happens regardless of consumer settings.

---

## 4.7 The Idempotent Producer

### The duplicate problem

Consider the following scenario:

1. The producer sends a batch of records to the broker.
2. The broker writes the records to the partition log.
3. The broker sends an acknowledgment back to the producer.
4. The acknowledgment is lost due to a network timeout.
5. The producer, not having received the ack, assumes the send failed and retries.
6. The broker receives the retry and writes the records **again**.

The result: the same records appear twice in the partition log. The consumer will see duplicates.

```
Producer                             Broker (Partition 0)
   │                                    │
   │── send batch [A, B] ─────────────▶ │  writes [A, B] at offsets 0, 1
   │                                    │
   │   ◀── ack LOST in transit ──×      │
   │                                    │
   │── retry batch [A, B] ────────────▶ │  writes [A, B] AGAIN at offsets 2, 3
   │                                    │
   │   ◀── ack received ───────────     │
   │                                    │
   Result: partition contains [A, B, A, B]  ← DUPLICATES
```

### What enable.idempotence=true does

Enabling the idempotent producer adds a **producer ID (PID)** and a **sequence number** to every record batch. The broker tracks the last sequence number it received from each PID for each partition. If a retry arrives with a sequence number the broker has already seen, the broker silently discards the duplicate instead of appending it.

```
Producer (PID=7)                     Broker (Partition 0)
   │                                    │
   │── send batch [A, B] seq=0 ──────▶ │  writes [A, B], records seq=0 for PID=7
   │                                    │
   │   ◀── ack LOST in transit ──×      │
   │                                    │
   │── retry batch [A, B] seq=0 ──────▶│  sees seq=0 for PID=7 already recorded
   │                                    │  REJECTS duplicate, returns success
   │                                    │
   │   ◀── ack received ───────────     │
   │                                    │
   Result: partition contains [A, B]  ← NO DUPLICATES
```

### How it works internally

The idempotent producer works through a combination of three mechanisms:

1. **Producer ID (PID)**: Assigned by the broker when the producer initializes. Uniquely identifies this producer instance.
2. **Epoch**: Incremented when a producer with the same `transactional.id` restarts, ensuring old instances cannot produce duplicates.
3. **Sequence number**: Monotonically increasing per partition. The broker uses this to detect duplicates and enforce ordering.

The broker maintains a small amount of state per PID per partition (the last 5 sequence numbers, by default). This is lightweight and does not measurably impact broker performance.

### What the idempotent producer does NOT solve

It is important to understand the boundaries of idempotent production:

| Scenario | Idempotent Producer Helps? | Why |
|---|---|---|
| Network timeout causes retry of the same batch | **Yes** | Broker detects duplicate sequence number |
| Producer JVM restarts and re-sends the same business event | **No** | New PID is assigned; broker treats it as a new producer |
| Application logic sends the same event twice (e.g., double API call) | **No** | Each `send()` call gets a new sequence number |
| Consumer processes the same record twice (at-least-once delivery) | **No** | This is a consumer-side concern, not a producer concern |

For cross-session and application-level deduplication, you need application-level idempotency keys — such as the `eventId` introduced in Section 3.

### Configuration requirements

The idempotent producer requires specific settings to function:

| Property | Required Value | Reason |
|---|---|---|
| `enable.idempotence` | `true` | Enables the PID and sequence number mechanism |
| `acks` | `all` | The broker must fully replicate before acknowledging to safely track sequences |
| `retries` | > 0 (effectively any positive value) | Retries are necessary for idempotence to be useful |
| `max.in.flight.requests.per.connection` | ≤ 5 | The broker can track up to 5 in-flight batches per PID per partition |

If `enable.idempotence=true` is set but `acks` is not `all`, modern Kafka clients will automatically override `acks` to `all`. Similarly, `max.in.flight.requests.per.connection` will be capped at 5 if set higher. However, it is better practice to set these explicitly rather than relying on implicit overrides.

### Why you should always enable idempotence

In modern Kafka (2.1+), the idempotent producer has negligible performance overhead. The PID and sequence number tracking adds a few bytes per batch and minimal broker-side state. There is no reason to leave it disabled in any environment — development, staging, or production.

### YAML configuration

```yaml
spring:
  kafka:
    producer:
      acks: all
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
```

---

## 4.8 Record Headers

### What headers are

Kafka record headers are **key-value pairs of metadata** attached to a record without modifying the record's value (the payload). They are analogous to HTTP headers — they carry information *about* the message rather than being part of the message's business content.

Each header has a `String` key and a `byte[]` value. A record can carry zero or more headers.

### Use cases for headers

| Header Key | Example Value | Purpose |
|---|---|---|
| `traceId` | `abc-123-def-456` | Distributed tracing across services |
| `correlationId` | `req-789` | Linking a Kafka event back to the originating API request |
| `eventType` | `OrderCreated` | Routing or filtering without deserializing the payload |
| `sourceService` | `order-service` | Identifying which service produced the record |
| `contentType` | `application/json` | Indicating the serialization format of the payload |
| `schemaVersion` | `2` | Versioning the payload schema without adding a field to the payload |
| `timestamp` | `2024-01-15T10:30:00Z` | Producer-side timestamp (separate from the Kafka-assigned timestamp) |

> **Key insight**: Headers allow you to inspect and route messages without deserializing the
> payload. This is particularly valuable for generic middleware components (like logging
> interceptors or dead-letter-queue routers) that need metadata but do not care about the
> specific event type.

### Adding headers in Spring Kafka

The most direct way to attach headers is to create a `ProducerRecord` manually and pass it to `KafkaTemplate.send()`:

```java
package com.example.kafkalearning.producer;

import com.example.kafkalearning.dto.OrderEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

@Component
public class OrderEventProducerWithHeaders {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducerWithHeaders.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Value("${app.kafka.topic.orders}")
    private String ordersTopic;

    public OrderEventProducerWithHeaders(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(OrderEvent event, String traceId, String correlationId) {
        String key = event.getOrderId();

        // Build ProducerRecord with headers
        ProducerRecord<String, OrderEvent> record =
                new ProducerRecord<>(ordersTopic, null, key, event);

        record.headers()
                .add(new RecordHeader("traceId",
                        traceId.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("correlationId",
                        correlationId.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("eventType",
                        "OrderCreated".getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("sourceService",
                        "order-service".getBytes(StandardCharsets.UTF_8)));

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send event key={} traceId={}", key, traceId, ex);
            } else {
                log.info("Sent event key={} topic={} partition={} offset={} traceId={}",
                        key,
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        traceId);
            }
        });
    }
}
```

### Reading headers on the consumer side (preview)

Headers are available on the consumer side through the `ConsumerRecord` or through Spring Kafka's `@Header` annotation. A brief preview (covered in full in Section 5):

```java
@KafkaListener(topics = "${app.kafka.topic.orders}", groupId = "order-processing-group")
public void consume(
        @Payload OrderEvent event,
        @Header(name = "traceId", required = false) byte[] traceIdBytes,
        @Header(name = "correlationId", required = false) byte[] correlationIdBytes) {

    String traceId = traceIdBytes != null
            ? new String(traceIdBytes, StandardCharsets.UTF_8)
            : "unknown";

    log.info("Received event orderId={} traceId={}", event.getOrderId(), traceId);
}
```

Note that `required = false` is used because a consumer must be resilient to records that were produced before headers were introduced, or by a producer that does not attach them.

---

## 4.9 Building a Reusable Producer Module

### The goal

A production-quality producer should feel like infrastructure — something that services use through a clean interface without thinking about Kafka-specific details like `ProducerRecord`, `RecordHeader`, or callback handling. The goal of this subsection is to build a reusable producer module that encapsulates:

- Consistent key-building logic
- Automatic header attachment (trace IDs, correlation IDs, event type)
- Callback handling with structured logging
- Centralized topic name management
- Generic typing so the module works for any event type

### Event publisher interface

Start with a generic interface that hides all Kafka implementation details:

```java
package com.example.kafkalearning.producer;

/**
 * Generic event publisher interface.
 * Implementations handle Kafka-specific concerns (keys, headers, callbacks).
 */
public interface EventPublisher<T> {

    /**
     * Publishes an event to the appropriate topic.
     *
     * @param key     the partition key (determines ordering and partition selection)
     * @param event   the event payload
     */
    void publish(String key, T event);

    /**
     * Publishes an event with explicit trace context.
     *
     * @param key           the partition key
     * @param event         the event payload
     * @param traceId       distributed trace ID for observability
     * @param correlationId correlation ID linking to the originating request
     */
    void publish(String key, T event, String traceId, String correlationId);
}
```

### Implementation with KafkaTemplate

```java
package com.example.kafkalearning.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka-backed event publisher that handles headers, callbacks,
 * and structured logging for any event type.
 */
public class KafkaEventPublisher<T> implements EventPublisher<T> {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, T> kafkaTemplate;
    private final String topic;
    private final String sourceService;

    public KafkaEventPublisher(KafkaTemplate<String, T> kafkaTemplate,
                               String topic,
                               String sourceService) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.sourceService = sourceService;
    }

    @Override
    public void publish(String key, T event) {
        publish(key, event, null, null);
    }

    @Override
    public void publish(String key, T event, String traceId, String correlationId) {
        ProducerRecord<String, T> record = new ProducerRecord<>(topic, null, key, event);

        // Attach standard headers
        addHeader(record, "sourceService", sourceService);
        addHeader(record, "eventType", event.getClass().getSimpleName());

        if (traceId != null) {
            addHeader(record, "traceId", traceId);
        }
        if (correlationId != null) {
            addHeader(record, "correlationId", correlationId);
        }

        CompletableFuture<SendResult<String, T>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("KAFKA_SEND_FAILED topic={} key={} eventType={} traceId={}",
                        topic, key, event.getClass().getSimpleName(), traceId, ex);
            } else {
                log.info("KAFKA_SEND_OK topic={} key={} partition={} offset={} traceId={}",
                        topic,
                        key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        traceId);
            }
        });
    }

    private void addHeader(ProducerRecord<String, T> record, String headerKey, String value) {
        if (value != null) {
            record.headers().add(
                    new RecordHeader(headerKey, value.getBytes(StandardCharsets.UTF_8)));
        }
    }
}
```

### Key design decisions in this implementation

| Decision | Why |
|---|---|
| Generic type `<T>` | One publisher class works for `OrderEvent`, `PaymentEvent`, or any future event type |
| `sourceService` in constructor | Every record carries the name of the producing service, aiding cross-service debugging |
| `eventType` derived from class name | Consumers can inspect the header to determine the event type without deserializing |
| `traceId` and `correlationId` optional | Not every call site has trace context; the publisher handles nulls gracefully |
| Structured log fields (`topic=`, `key=`, `partition=`, `offset=`) | Enables log search and alerting based on specific dimensions |
| Error always logged with full context | No send failure is ever silently swallowed |

### Configuration class: wiring the publisher as a bean

```java
package com.example.kafkalearning.config;

import com.example.kafkalearning.dto.OrderEvent;
import com.example.kafkalearning.producer.EventPublisher;
import com.example.kafkalearning.producer.KafkaEventPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class ProducerConfig {

    @Value("${spring.application.name}")
    private String applicationName;

    @Value("${app.kafka.topic.orders}")
    private String ordersTopic;

    @Bean
    public EventPublisher<OrderEvent> orderEventPublisher(
            KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        return new KafkaEventPublisher<>(kafkaTemplate, ordersTopic, applicationName);
    }
}
```

### Centralized topic name management

Topic names are defined once in `application.yml` and injected wherever needed:

```yaml
spring:
  application:
    name: order-service

  kafka:
    bootstrap-servers: localhost:9092

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      compression-type: snappy
      batch-size: 32768
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        linger.ms: 10
        delivery.timeout.ms: 120000

# ─── Application-specific config ───────────────────────
app:
  kafka:
    topic:
      orders: order-events
      payments: payment-events
      notifications: notification-events
```

### Using the publisher from application code

With the infrastructure in place, application code becomes clean and Kafka-agnostic:

```java
package com.example.kafkalearning.service;

import com.example.kafkalearning.dto.OrderEvent;
import com.example.kafkalearning.producer.EventPublisher;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final EventPublisher<OrderEvent> orderEventPublisher;

    public OrderService(EventPublisher<OrderEvent> orderEventPublisher) {
        this.orderEventPublisher = orderEventPublisher;
    }

    public void createOrder(String orderId, String customer, String product,
                            int quantity, double total) {
        OrderEvent event = new OrderEvent(orderId, customer, product,
                quantity, total, "CREATED");

        // The publisher handles: key routing, headers, compression,
        // callback logging, and retry configuration
        orderEventPublisher.publish(orderId, event);
    }
}
```

Notice that `OrderService` knows nothing about `KafkaTemplate`, `ProducerRecord`, headers, or callbacks. It depends on an `EventPublisher<OrderEvent>` interface that could be backed by Kafka, an in-memory implementation for tests, or any other messaging system. This is the level of abstraction a production service should target.

### Correlation and trace ID propagation

In a real system, trace IDs typically come from an HTTP request header (e.g., `X-Trace-Id`) or a distributed tracing library (e.g., Micrometer Tracing, OpenTelemetry). The controller extracts the trace context and passes it through the service layer to the publisher:

```java
package com.example.kafkalearning.controller;

import com.example.kafkalearning.dto.OrderEvent;
import com.example.kafkalearning.dto.OrderRequest;
import com.example.kafkalearning.producer.EventPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final EventPublisher<OrderEvent> orderEventPublisher;

    public OrderController(EventPublisher<OrderEvent> orderEventPublisher) {
        this.orderEventPublisher = orderEventPublisher;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(
            @RequestBody OrderRequest request,
            @RequestHeader(value = "X-Trace-Id", required = false) String traceId,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {

        String orderId = UUID.randomUUID().toString();

        // Ensure trace context exists even if caller did not provide it
        String effectiveTraceId = traceId != null ? traceId : UUID.randomUUID().toString();
        String effectiveCorrelationId = correlationId != null
                ? correlationId : UUID.randomUUID().toString();

        OrderEvent event = new OrderEvent(orderId, request.getCustomerName(),
                request.getProduct(), request.getQuantity(),
                request.getTotalAmount(), "CREATED");

        orderEventPublisher.publish(orderId, event, effectiveTraceId, effectiveCorrelationId);

        return ResponseEntity.ok(orderId);
    }
}
```

---

## 4.10 Production Habits

The techniques covered in subsections 4.2 through 4.9 form the building blocks of a production-quality producer. This subsection distills them into a set of habits — practices that should be followed consistently across every Kafka producer in an organization.

### Centralize topic names

Never scatter topic name strings across producer classes, consumer classes, and configuration files. Define them once in `application.yml` under a custom namespace (e.g., `app.kafka.topic.*`) and inject them via `@Value` or a configuration properties class.

```java
// GOOD: topic name injected from configuration
@Value("${app.kafka.topic.orders}")
private String ordersTopic;

// BAD: topic name hardcoded in producer code
kafkaTemplate.send("order-events", key, event);
```

Centralizing topic names makes it trivial to change a topic name, ensures producers and consumers reference the same name, and makes the full list of topics visible in a single configuration file.

### Centralize key-building logic

If a key requires computation (composite keys, formatting, normalization), that logic should live in one place — either in the publisher implementation or in a dedicated key-builder utility. If key logic is duplicated across multiple call sites, it will eventually diverge.

```java
// Key builder utility
public final class KafkaKeys {

    private KafkaKeys() { }

    public static String orderKey(String orderId) {
        return orderId;
    }

    public static String orderItemKey(String orderId, String itemId) {
        return orderId + ":" + itemId;
    }
}
```

### Always log key, topic, partition, and offset on success

When a send succeeds, the callback provides the `RecordMetadata` — which includes the topic, partition, and offset where the record was written. Logging these four fields on every successful send creates an audit trail that is invaluable for debugging:

```
KAFKA_SEND_OK topic=order-events key=ORD-123 partition=2 offset=48172 traceId=abc-456
```

With this log line, an engineer can find the exact record in the topic by partition and offset, correlate it with the originating request via the trace ID, and verify that the key mapped to the expected partition.

### Never swallow send failures

The `send()` method on `KafkaTemplate` returns a `CompletableFuture`. If no callback is registered and the future completes exceptionally, the error is silently discarded. This is one of the most common and dangerous Kafka anti-patterns.

```java
// BAD: fire and forget — failures are silently lost
kafkaTemplate.send(topic, key, event);

// GOOD: register a callback that logs failures
kafkaTemplate.send(topic, key, event).whenComplete((result, ex) -> {
    if (ex != null) {
        log.error("KAFKA_SEND_FAILED topic={} key={}", topic, key, ex);
        // Optional: increment a metric, trigger an alert, write to a fallback store
    }
});
```

In production, a send failure callback should at minimum log the error with full context (topic, key, event type). Depending on the application, it may also increment a metrics counter, trigger an alert, or write the event to a fallback store (database, file) for later retry.

### Use structured logging

Structured log fields (key=value pairs) are far more useful than free-form log messages in production. They enable log aggregation tools (ELK, Splunk, Datadog) to index and search by individual dimensions:

```java
// Structured: searchable, filterable, aggregatable
log.info("KAFKA_SEND_OK topic={} key={} partition={} offset={}", topic, key, partition, offset);

// Unstructured: human-readable but hard to query
log.info("Sent message " + key + " to " + topic + " at partition " + partition);
```

### Test producer serialization and key routing

Producer-side tests should verify two things:

1. **Serialization**: The event serializes to valid JSON (or the configured format) without errors.
2. **Key routing**: For a given key and partition count, the key maps to the expected partition.

Spring Kafka's `spring-kafka-test` module provides an embedded Kafka broker that makes these tests straightforward. This is covered in detail in a dedicated testing section, but the principle belongs here: producer behavior is testable and should be tested.

---

## 4.11 Common Mistakes

This subsection catalogs mistakes that appear frequently in real-world Kafka producer code, along with the consequences and corrections.

### Mistake 1: Swallowing send failures

```java
// The silent killer — no callback, no logging, no error handling
kafkaTemplate.send(topic, key, event);
```

**Consequence**: If the send fails (timeout, serialization error, authorization failure), the application has no idea. Records are silently lost. In a payment system, this could mean a completed payment that is never recorded.

**Fix**: Always register a callback or use `.get()` to block and check for exceptions:

```java
kafkaTemplate.send(topic, key, event).whenComplete((result, ex) -> {
    if (ex != null) {
        log.error("Send failed for key={}", key, ex);
    }
});
```

### Mistake 2: Hardcoding topic names

```java
kafkaTemplate.send("order-events", key, event);      // in OrderProducer.java
kafkaTemplate.send("order-events", key, event);      // in NotificationProducer.java
```

**Consequence**: When the topic name needs to change, every occurrence must be found and updated. One miss causes a producer to silently publish to the old (or nonexistent) topic.

**Fix**: Define topic names in `application.yml` and inject them:

```yaml
app:
  kafka:
    topic:
      orders: order-events
```

```java
@Value("${app.kafka.topic.orders}")
private String ordersTopic;
```

### Mistake 3: Using acks=0 or acks=1 without understanding the tradeoff

**Consequence**: With `acks=0`, any broker issue causes silent data loss. With `acks=1`, a leader crash after acknowledgment but before replication loses the record. In many cases, developers set these for "performance" without measuring whether the performance difference is meaningful for their workload.

**Fix**: Start with `acks=all` and measure. The latency difference between `acks=1` and `acks=all` is typically a few milliseconds — negligible for most applications. Only downgrade if you have measured the impact and the use case genuinely does not require durability.

### Mistake 4: Not enabling the idempotent producer

**Consequence**: Retries (which happen automatically with default configuration) can produce duplicate records. In a system that processes financial transactions, a single retry-induced duplicate can cause a double charge.

**Fix**: Set `enable.idempotence=true`. It has been available since Kafka 0.11 and enabled by default since Kafka 3.0. There is no performance penalty worth worrying about.

### Mistake 5: Setting compression only on the consumer side

A common misconception is that compression is configured on the consumer. In reality, compression is a **producer-side** setting. The producer compresses the batch; the broker stores it compressed; the consumer decompresses automatically based on the batch header. Setting `compression.type` on the consumer has no effect.

**Fix**: Configure `compression-type` in the producer section of `application.yml`:

```yaml
spring:
  kafka:
    producer:
      compression-type: snappy
```

### Mistake 6: Ignoring linger.ms

With the default `linger.ms=0`, every `send()` call results in an immediate network request (assuming the batch contains only that record). This is wasteful in applications that produce many records in rapid succession.

**Fix**: Set `linger.ms` to a small value (5-20ms) to allow batching:

```yaml
spring:
  kafka:
    producer:
      properties:
        linger.ms: 10
```

### Summary of common mistakes

| Mistake | Risk | Fix |
|---|---|---|
| Swallowing send failures | Silent data loss | Always register a callback |
| Hardcoding topic names | Missed renames, wrong topics | Centralize in `application.yml` |
| `acks=0` or `acks=1` without measurement | Data loss on broker failure | Default to `acks=all` |
| Idempotence not enabled | Duplicate records from retries | `enable.idempotence=true` |
| Compression on consumer instead of producer | No compression actually happens | Set `compression-type` on producer |
| `linger.ms=0` with high throughput | Excessive network round trips | Set `linger.ms=5-20` |

---

## 4.12 Key Takeaways

Producer design in Apache Kafka is not a simple matter of calling `send()` and moving on. Every producer makes implicit or explicit decisions about durability (`acks`), ordering (key selection), efficiency (batching and compression), and correctness (idempotent production). Understanding these decisions — and making them deliberately rather than accepting defaults blindly — is what separates a demo-quality producer from one that is safe to run in production.

The key selection strategy determines how records are distributed across partitions and which records share an ordering guarantee. Entity-based keys like `orderId` or `userId` are the most common and effective choice. Null keys distribute load evenly but sacrifice ordering.

Acknowledgments control durability: `acks=all` combined with `min.insync.replicas=2` is the standard production configuration, tolerating a single broker failure while ensuring records are replicated before the producer considers them written.

Retries are essential for surviving transient broker failures, but they introduce the risk of duplicates and ordering violations. The idempotent producer (`enable.idempotence=true`) eliminates both risks by tracking sequence numbers per partition, and it should be enabled in every modern Kafka application.

Batching (`batch.size` and `linger.ms`) and compression (`compression.type`) are throughput optimizations that have meaningful impact even at moderate message volumes. A small `linger.ms` (5-20ms) and a compression codec like `snappy` or `lz4` are appropriate defaults for most workloads.

Record headers provide a mechanism for attaching metadata — trace IDs, correlation IDs, event types — without modifying the event payload. They are essential for observability and cross-service debugging.

Finally, wrapping all of these concerns in a reusable publisher module (a generic interface backed by a `KafkaTemplate`-based implementation) keeps application code clean, ensures consistent behavior across all producers in a codebase, and makes the Kafka integration testable and replaceable.

**Next**: [Section 5: Consumer Design and Offset Management](section-5-consumer-design-and-offset-management.md) — where the focus shifts to building reliable consumers that handle offsets, rebalancing, and failure recovery correctly.
