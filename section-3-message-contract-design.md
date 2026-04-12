# Section 3: Message Contract Design

In Sections 1 and 2, the focus was on understanding how Kafka works and how Spring Boot connects to it. The `OrderEvent` class from Section 2 was deliberately simple — a handful of fields, a default constructor, and enough structure for Jackson to serialize and deserialize it. That simplicity was appropriate for learning the mechanics, but it left a critical question unanswered: how should the data flowing through Kafka be *designed*? This section answers that question. It covers what a message contract is, why it matters far more than the code that sends or receives messages, and how to build events that are safe to evolve, easy to debug, and resilient to the inevitable changes that production systems demand.

---

## 3.1 Why Message Contracts Matter

### What is a message contract?

A **message contract** is the agreed-upon structure of data that flows through a Kafka topic. It defines which fields exist, what types they use, which fields are required, what their names mean, and how the structure is allowed to change over time. In object-oriented terms, the contract is the class that represents the Kafka message — its field names, types, and the rules governing how it can evolve.

In Section 2, the `OrderEvent` class served as a contract. It declared fields like `orderId`, `customerName`, `product`, `quantity`, `totalAmount`, `status`, and `occurredAt`. Every producer that writes to the `order-events` topic must produce JSON that matches this structure, and every consumer must be able to deserialize it. That implicit agreement between producers and consumers *is* the contract.

### Why this is where systems become maintainable or painful

In a simple application with one producer and one consumer maintained by the same developer, the contract is easy to manage — change the class, redeploy both sides, and move on. But Kafka systems rarely stay that simple. Over time, multiple services consume the same topic. Different teams own different consumers. Deployment schedules diverge. And eventually, someone needs to add a field, rename a field, or change a field's type.

This is the moment where undisciplined contracts cause real damage. The following diagram illustrates the problem:

```
    Producer (Team A)                         Consumer 1 (Team A)
    ┌──────────────────┐                     ┌──────────────────┐
    │ Adds "currency"  │──── order-events ──▶│ Updated: OK      │
    │ field to event   │         │           └──────────────────┘
    └──────────────────┘         │
                                 │           Consumer 2 (Team B)
                                 │           ┌──────────────────┐
                                 └──────────▶│ NOT updated:     │
                                             │ Deserialization  │
                                             │ failure? Silent  │
                                             │ data loss?       │
                                             └──────────────────┘
```

When Team A adds a `currency` field and deploys their producer and their consumer, everything works for them. But Team B's consumer — which was deployed weeks ago and knows nothing about the `currency` field — now receives JSON containing an unexpected property. Depending on how that consumer's deserializer is configured, one of three things happens:

| Outcome | What Happens | Consequence |
|---|---|---|
| **Deserialization crash** | Jackson throws `UnrecognizedPropertyException` and the consumer halts | Messages pile up in the topic, consumer lag grows, alerts fire |
| **Silent field drop** | Jackson ignores the unknown field (if configured with `FAIL_ON_UNKNOWN_PROPERTIES = false`) | Consumer works, but has no idea the new field exists — may make decisions with incomplete data |
| **Data corruption** | Field types change (e.g., `quantity` from `int` to `String`) and Jackson coerces incorrectly | Consumer processes garbage data without errors |

None of these outcomes is acceptable in a production system. The solution is not to avoid change — change is inevitable — but to design contracts that make change *safe*. That is what the rest of this section teaches.

### The difference between "just sending JSON" and designing a proper contract

"Just sending JSON" means serializing whatever object the producer happens to have, without thinking about who reads it, how it might change, or what metadata consumers need. A proper contract, by contrast, is deliberate about:

- **Event naming** — what the event is called and what business meaning it carries
- **Versioning** — how the structure is allowed to evolve without breaking consumers
- **Metadata** — what information every event carries beyond the business payload (IDs, timestamps, trace context)
- **Key design** — what field is used as the Kafka message key and why
- **Serialization format** — how the event is encoded on the wire (JSON, Avro, Protobuf)

Each of these dimensions is covered in the subsections that follow.

---

## 3.2 Event Naming

### Why event names carry business meaning

An event name is the first thing a developer sees when exploring a Kafka topic or reading consumer code. A well-chosen name communicates what happened in the business domain without requiring the reader to inspect the event payload. A poorly chosen name forces the reader to guess, inspect, and frequently misinterpret.

Consider a topic called `order-events`. If the events on that topic are named `OrderCreated`, `OrderConfirmed`, `OrderShipped`, and `OrderCancelled`, a new developer can immediately understand the lifecycle of an order just by reading the event names. If the events are named `OrderEvent` with a `status` field that changes, the developer must read the payload, understand every possible value of `status`, and reason about which transitions are valid.

### Past-tense naming: events describe what happened

The most important naming convention for Kafka events is to use the **past tense**. An event represents something that has already happened — a fact that has been recorded in the system. It is not a command (an instruction to do something) and it is not a request (a question about the current state).

This distinction matters because it changes how consumers think about the message:

| Style | Example | What It Implies | Problem |
|---|---|---|---|
| **Past tense (event)** | `OrderCreated` | "An order was created. Here are the details." | None — this is the correct convention |
| **Imperative (command)** | `CreateOrder` | "Please create an order." | Implies the consumer should *do* something, coupling producer to consumer behavior |
| **Noun only** | `Order` | Ambiguous — was it created? updated? deleted? | Consumers cannot determine what happened without inspecting the payload |
| **Verb phrase** | `ProcessPayment` | "Please process this payment." | Same problem as imperative — it's a command, not a fact |

> **Key insight**: Events are facts about the past. Commands are instructions for the future. Kafka topics should carry events, not commands. Name them accordingly: `PaymentAuthorized`, not `AuthorizePayment`.

### Domain-driven naming patterns

Event names should come from the **ubiquitous language** of the business domain — the terms that domain experts, product managers, and developers all use to describe what happens in the system. This is a core principle from Domain-Driven Design (DDD).

A well-named event reads like a sentence from a business process description:

```
When a customer places an order     →  OrderPlaced
When payment is authorized          →  PaymentAuthorized
When the warehouse confirms stock   →  InventoryReserved
When the package leaves the dock    →  ShipmentDispatched
When the customer receives delivery →  DeliveryConfirmed
```

### Table of good vs bad event names

| Domain Action | Good Event Name | Bad Event Name | Why Bad Is Bad |
|---|---|---|---|
| Customer places an order | `OrderPlaced` | `NewOrder` | "New" is relative — new when? Compared to what? |
| Payment is charged | `PaymentCharged` | `Payment` | No verb — impossible to know what happened |
| Item ships from warehouse | `ShipmentDispatched` | `UpdateShipment` | Imperative command, not a past-tense fact |
| User changes their email | `UserEmailChanged` | `UserUpdate` | Too vague — which attribute changed? |
| Account is suspended | `AccountSuspended` | `AccountStatus` | Status of what? Suspended? Reactivated? |
| Refund is issued | `RefundIssued` | `DoRefund` | Command, not event |
| Inventory drops below threshold | `InventoryThresholdBreached` | `LowStock` | Adjective, not event — when did it become low? |
| Subscription renews | `SubscriptionRenewed` | `RenewSubscription` | Imperative — tells the consumer to act |

---

## 3.3 Event Versioning

### Why events need to evolve

No event schema stays the same forever. Business requirements change, new features require new data, and early design decisions get revisited. The `OrderEvent` from Section 2 might start with five fields, but six months later the business needs a `currency` field, a `discountCode` field, and a `shippingAddress` object. The question is not *whether* the schema will change, but *how* it can change without breaking consumers that are still running the old version.

### Backward and forward compatibility

In Kafka, producers and consumers are deployed independently. At any given moment, a topic might be receiving events from a new producer while being consumed by both old and new consumers. This means two kinds of compatibility matter:

| Compatibility Type | Definition | Why It Matters |
|---|---|---|
| **Backward compatible** | A new consumer can read events produced by an old producer | New consumer code deploys first and must handle old events that are still in the topic |
| **Forward compatible** | An old consumer can read events produced by a new producer | Old consumer code hasn't been updated yet but must handle new events arriving on the topic |

In practice, **forward compatibility** is the more urgent concern in Kafka systems. The producer is usually updated first (it has the new business data to publish), and consumers catch up later. If the new event breaks old consumers, those consumers fail in production.

### Required vs optional fields

The single most important rule of event versioning is:

> **Key insight**: Never remove a required field from an event. Never change the type of an existing field. These are the two changes most likely to break consumers.

Here is why. When a consumer deserializes a JSON event, it maps JSON keys to Java fields. If a field the consumer expects is missing, the Java field gets `null` (for objects) or a default value (for primitives). If the consumer's code does not handle `null` — for example, it calls `event.getOrderId().length()` — it throws a `NullPointerException`. If a field's type changes from `int` to `String`, Jackson may throw a deserialization exception or silently coerce the value, producing incorrect data.

### Adding new optional fields: the safe default strategy

The safest way to evolve an event is to **add new fields with no required constraint**. Old consumers that do not know about the new field simply ignore it (if configured correctly), and new consumers can use it when present.

```java
package com.example.kafkalearning.events;

import java.time.Instant;

/**
 * Version 1 of the order event — the original schema.
 */
public class OrderCreatedEventV1 {

    private String eventId;
    private String eventType;
    private int eventVersion;
    private Instant occurredAt;

    private String orderId;
    private String customerName;
    private String product;
    private int quantity;
    private double totalAmount;

    // constructors, getters, setters omitted for brevity
}
```

Six months later, a `currency` field and a `discountCode` field are needed:

```java
package com.example.kafkalearning.events;

import java.time.Instant;

/**
 * Version 2 — adds currency and discountCode.
 * Both fields are optional (nullable) to maintain forward compatibility.
 * Consumers running V1 code will simply ignore these fields.
 */
public class OrderCreatedEventV2 {

    private String eventId;
    private String eventType;
    private int eventVersion;
    private Instant occurredAt;

    private String orderId;
    private String customerName;
    private String product;
    private int quantity;
    private double totalAmount;

    // ─── New fields in V2 (optional, nullable) ──────────────
    private String currency;        // e.g., "USD", "EUR"
    private String discountCode;    // e.g., "SUMMER2024", null if no discount

    // constructors, getters, setters omitted for brevity
}
```

### The eventVersion field

Including an explicit `eventVersion` field in every event gives consumers a way to know which version of the schema they are dealing with. A consumer can inspect the version and branch its logic:

```java
package com.example.kafkalearning.consumer;

import com.example.kafkalearning.events.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    @KafkaListener(topics = "${app.kafka.topic.orders}", groupId = "${spring.kafka.consumer.group-id}")
    public void handle(OrderCreatedEvent event) {
        if (event.getEventVersion() >= 2) {
            log.info("Processing V2 event: orderId={}, currency={}",
                    event.getOrderId(), event.getCurrency());
        } else {
            log.info("Processing V1 event: orderId={}, currency not available",
                    event.getOrderId());
        }
    }
}
```

### Jackson's handling of unknown properties

By default, Jackson's `ObjectMapper` throws an exception when it encounters a JSON property that does not map to any field in the target Java class. This is the default behavior that causes the "old consumer, new producer" failure described earlier. The fix is to configure Jackson to ignore unknown properties:

```java
package com.example.kafkalearning.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Critical for forward compatibility: ignore fields the consumer doesn't know about
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        return mapper;
    }
}
```

Alternatively, this can be declared at the class level using an annotation:

```java
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderCreatedEvent {
    // fields...
}
```

> **Key insight**: Always configure `FAIL_ON_UNKNOWN_PROPERTIES = false` in any consumer that reads from a Kafka topic shared with other teams. This is the single most important Jackson setting for forward compatibility.

### Safe vs unsafe schema changes

| Change | Safe? | Why |
|---|---|---|
| Add a new optional field | ✅ Safe | Old consumers ignore it, new consumers use it |
| Add a new required field | ⚠️ Risky | Old events in the topic lack this field — new consumer code must handle `null` |
| Remove an optional field | ⚠️ Risky | New producers stop sending it, but old consumers may still expect it (even if nullable, some code may not handle its absence) |
| Remove a required field | ❌ Unsafe | Consumers that rely on this field will throw `NullPointerException` or produce incorrect results |
| Rename a field | ❌ Unsafe | Effectively removes the old field and adds a new one — breaks both old and new consumers during the transition |
| Change a field's type (e.g., `int` → `String`) | ❌ Unsafe | Jackson may fail to deserialize or silently coerce to wrong value |
| Change a field's semantic meaning | ❌ Unsafe | No deserialization error, but consumers interpret the data incorrectly — the worst kind of bug |
| Add a new value to an enum field | ⚠️ Risky | Consumers with a `switch` statement or `@JsonEnumDefaultValue` may not handle the new value |
| Increment `eventVersion` and add optional fields | ✅ Safe | Consumers can branch on version; unknown fields are ignored |

---

## 3.4 Metadata Fields (The Event Envelope)

### Why every event should carry metadata

The business payload of an event — the order ID, the product name, the quantity — answers the question "what happened?" But operating a production Kafka system requires answering many more questions: "When did it happen? Which service produced it? Is this a duplicate? How do I trace this event through a chain of services? What version of the schema is this?"

These questions are answered by **metadata fields** — data about the event itself, rather than about the business action it represents. Together, the metadata fields form an **event envelope** that wraps the business payload.

### The anatomy of a well-designed event

```
┌─────────────────────────────────────────────────────┐
│                   Event Envelope                     │
│                                                      │
│  eventId:      "a1b2c3d4-e5f6-..."   (UUID)         │
│  eventType:    "OrderCreated"         (routing)      │
│  eventVersion: 1                      (schema)       │
│  occurredAt:   "2024-07-15T10:..."    (business)     │
│  source:       "order-service"        (origin)       │
│  traceId:      "abc-123-def-456"      (tracing)      │
│  correlationId:"req-789-xyz"          (correlation)  │
│                                                      │
│  ┌───────────────────────────────────────────────┐   │
│  │            Business Payload                    │   │
│  │                                                │   │
│  │  orderId:      "ORD-001"                       │   │
│  │  customerName: "Alice"                         │   │
│  │  product:      "Laptop"                        │   │
│  │  quantity:     1                               │   │
│  │  totalAmount:  999.99                          │   │
│  └───────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### Metadata fields explained

| Field | Type | Purpose | Example Value |
|---|---|---|---|
| `eventId` | `String` (UUID) | Uniquely identifies this specific event instance. Enables deduplication — if a consumer sees the same `eventId` twice, it knows the second is a retry or duplicate. | `"f47ac10b-58cc-4372-a567-0e02b2c3d479"` |
| `eventType` | `String` | Identifies what kind of event this is. Enables routing and filtering — a consumer can check the type before attempting deserialization. | `"OrderCreated"` |
| `eventVersion` | `int` | The schema version of this event. Enables safe deserialization — consumers can branch on version or reject events they don't understand. | `1` |
| `occurredAt` | `Instant` | The timestamp when the business event happened in the real world. Not when the message was published, and not when the consumer processed it. | `"2024-07-15T10:30:00Z"` |
| `source` | `String` | The name of the service or component that produced this event. Critical for debugging in a multi-service environment. | `"order-service"` |
| `traceId` | `String` | A distributed tracing identifier that follows a request across service boundaries. If the event was triggered by an HTTP request, this is typically the trace ID from that request. | `"abc123def456"` |
| `correlationId` | `String` | Links related events together. For example, all events in a single order workflow might share the same correlation ID, even if they are produced by different services. | `"order-flow-789"` |

### eventId: deduplication and tracing

The `eventId` is a UUID generated by the producer at the moment the event is created. Its primary purpose is **deduplication**. In Kafka, messages can be delivered more than once due to producer retries, consumer rebalances, or at-least-once delivery semantics. Without an `eventId`, a consumer has no way to know whether two identical-looking messages are genuinely two separate events or the same event delivered twice.

A consumer that needs exactly-once processing can store processed `eventId` values (in a database, a cache, or an in-memory set) and skip any event whose ID has already been seen:

```java
package com.example.kafkalearning.consumer;

import com.example.kafkalearning.events.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class IdempotentOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(IdempotentOrderConsumer.class);

    // In production, use a persistent store (database, Redis) instead of in-memory
    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    @KafkaListener(topics = "${app.kafka.topic.orders}", groupId = "${spring.kafka.consumer.group-id}")
    public void handle(OrderCreatedEvent event) {
        if (processedEventIds.contains(event.getEventId())) {
            log.warn("Duplicate event detected, skipping: eventId={}", event.getEventId());
            return;
        }

        log.info("Processing event: eventId={}, orderId={}", event.getEventId(), event.getOrderId());
        // ... business logic ...

        processedEventIds.add(event.getEventId());
    }
}
```

### occurredAt: when the business event happened

The `occurredAt` timestamp records when the business event happened in the real world — for example, when the customer clicked "Place Order." This is distinct from two other timestamps that are sometimes confused with it:

| Timestamp | What It Records | Who Sets It |
|---|---|---|
| `occurredAt` | When the business event happened | The producer, at event creation time |
| Kafka record timestamp | When the message was written to the broker | Kafka broker (or producer, depending on `message.timestamp.type`) |
| `processedAt` | When the consumer processed the message | The consumer, at processing time |

These three timestamps can differ significantly. If the producer batches messages, there may be seconds between `occurredAt` and the Kafka record timestamp. If the consumer has lag, there may be minutes or hours between the Kafka record timestamp and `processedAt`. Business logic should almost always use `occurredAt`, not the Kafka timestamp or the processing time.

### traceId and correlationId: distributed tracing

In a microservices architecture, a single user action (like placing an order) can trigger a chain of events across many services:

```
User clicks          Order Service          Payment Service        Shipping Service
"Place Order"        produces               consumes               consumes
     │                OrderCreated            OrderCreated           PaymentAuthorized
     │                    │                       │                       │
     ▼                    ▼                       ▼                       ▼
  HTTP POST ──────▶  Kafka topic  ──────▶   Kafka topic   ──────▶  Kafka topic
  traceId=T1       order-events          payment-events          shipping-events
                   traceId=T1            traceId=T1              traceId=T1
```

The `traceId` follows the entire chain. When debugging a production issue ("why was order ORD-123 never shipped?"), an engineer can search logs for the trace ID and see every step the request went through, across all services.

The `correlationId` serves a similar but slightly different purpose: it groups related events that belong to the same business workflow. While a `traceId` typically originates from the first HTTP request, a `correlationId` might be a business identifier (like the order ID) that links all events in an order's lifecycle, even if they were triggered by separate HTTP requests.

---

## 3.5 Key Design

### Recap: key → partition → ordering

Section 1 established the fundamental relationship between message keys and partition assignment:

```
Message key  ──hash──▶  Partition number  ──▶  Ordering guarantee within partition
```

When a Kafka producer sends a message with a key, Kafka computes `hash(key) % partition_count` to determine which partition the message lands in. All messages with the same key go to the same partition, and all messages in a partition are strictly ordered. This is the mechanism that provides **per-entity ordering** in Kafka.

### Why key choice is a contract design decision

Key selection is often treated as a producer implementation detail — something decided when writing the `kafkaTemplate.send()` call. But the key is actually part of the message contract, because it determines:

1. **Which messages are ordered relative to each other** — consumers rely on ordering guarantees that depend on the key
2. **How partitions are loaded** — a bad key choice creates hot partitions that bottleneck the system
3. **What happens during partition reassignment** — changing the key strategy on an existing topic breaks ordering for in-flight messages

If a consumer is built with the assumption that all events for a given `orderId` arrive in order, and the producer changes from using `orderId` as the key to using `customerId`, that assumption is silently violated. No error is thrown — the consumer simply processes events in the wrong order and produces incorrect results.

> **Key insight**: The message key is part of the contract between producers and consumers. Changing the key strategy is a breaking change that requires coordination, just like changing a field name or type.

### Keys for ordering

The most common use of message keys is to guarantee ordering for a specific entity. The key should be the identifier of the entity whose events must be processed in order:

```java
package com.example.kafkalearning.producer;

import com.example.kafkalearning.events.OrderCreatedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderEventProducer {

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(OrderCreatedEvent event) {
        // orderId as key guarantees all events for the same order are in the same partition
        kafkaTemplate.send("order-events", event.getOrderId(), event);
    }
}
```

### Keys for load distribution

Sometimes ordering is less important than even load distribution. In those cases, the key should have **high cardinality** — many distinct values — so that messages spread evenly across partitions. Using a UUID as the key, or sending messages with no key at all (which triggers round-robin or sticky partitioning), achieves even distribution.

| Goal | Key Strategy | Trade-off |
|---|---|---|
| Per-order ordering | `orderId` | If one order generates far more events than others, its partition gets more load |
| Per-user ordering | `userId` | Power users create hot partitions |
| Even distribution, no ordering | `null` (no key) | No per-entity ordering guarantee |
| Even distribution with traceability | Random UUID | Every message traceable by key, but no ordering |

### Composite keys

In multi-tenant systems or systems with hierarchical entities, a **composite key** combines two identifiers to achieve both isolation and ordering:

```java
// Ensures all events for a tenant's orders are in the same partition
String compositeKey = event.getTenantId() + ":" + event.getOrderId();
kafkaTemplate.send("order-events", compositeKey, event);
```

A composite key like `tenantId:orderId` guarantees that all events for order `ORD-123` within tenant `TENANT-A` land in the same partition. However, events for the same order in different tenants may land in different partitions, which is usually the desired behavior — tenants are isolated from each other.

### What happens when you change key strategy on an existing topic

Changing the key strategy on a topic that already contains data is a **breaking change**. Here is why:

1. The partition assignment formula is `hash(key) % partition_count`
2. If the key changes (e.g., from `orderId` to `customerId`), a different hash is computed
3. New events for the same business entity now land in a different partition
4. Consumers that depend on per-entity ordering see events arrive out of order
5. Any consumer logic that assumes "all events for entity X are in partition P" breaks silently

The safe way to change a key strategy is to:

1. Create a new topic with the new key strategy
2. Migrate producers to the new topic
3. Run consumers on both topics during the transition
4. Decommission the old topic after all old events have been processed

### Key design patterns by domain

| Domain | Recommended Key | Reasoning |
|---|---|---|
| E-commerce orders | `orderId` | All lifecycle events for one order are ordered |
| User activity tracking | `userId` | Activity for one user is ordered; high cardinality ensures even distribution |
| IoT sensor data | `sensorId` or `deviceId` | Per-device readings are ordered |
| Payment processing | `paymentId` or `orderId` | Depends on whether per-payment or per-order ordering matters more |
| Multi-tenant SaaS | `tenantId:entityId` | Tenant isolation with per-entity ordering |
| Audit logging | `null` or random UUID | Ordering rarely matters; even distribution is the priority |
| Chat / messaging | `conversationId` | Messages in a conversation are ordered |
| Inventory management | `warehouseId:skuId` | Per-SKU ordering within a warehouse |

---

## 3.6 Serialization Choices

### Why serialization matters for contracts

Serialization is the process of converting a Java object into bytes that can be stored in a Kafka record, and deserialization is the reverse. The choice of serialization format has a direct impact on how contracts are enforced, how events evolve, and how easy they are to debug.

Section 2 used JSON serialization via Jackson. This is the simplest approach and the right one for learning, but it is not the only option. This subsection compares the four most common serialization formats used with Kafka.

### JSON

JSON is a text-based, human-readable format. In a Spring Boot application, Jackson's `JsonSerializer` and `JsonDeserializer` handle the conversion automatically.

**Advantages**:
- Human-readable — events can be inspected with `kafka-console-consumer` or any text editor
- No additional infrastructure required (no Schema Registry)
- Familiar to every developer
- Flexible — fields can be added or removed without any tooling

**Disadvantages**:
- No schema enforcement — there is no external authority that validates events match a contract
- Larger message size compared to binary formats (field names are repeated in every message)
- Schema evolution is purely by convention — nothing prevents a producer from sending a field with the wrong type

### Avro

Apache Avro is a compact binary format that uses a **schema** to define the structure of the data. Schemas are typically stored in a **Schema Registry** (such as Confluent Schema Registry), and producers and consumers reference schemas by ID rather than embedding the schema in every message.

**Advantages**:
- Compact binary encoding — significantly smaller messages than JSON
- Schema Registry enforces compatibility rules (backward, forward, full)
- Schema evolution is formalized — the registry rejects incompatible changes
- Schemas are language-agnostic — Java, Python, Go consumers can all use the same schema

**Disadvantages**:
- Requires running a Schema Registry alongside Kafka
- Not human-readable — inspecting messages requires Avro-specific tooling
- Schema definition language (`.avsc` files) has a learning curve
- Spring Boot integration requires additional dependencies (`spring-cloud-stream` or `kafka-avro-serializer`)

### Protobuf

Protocol Buffers (Protobuf) is Google's binary serialization format. Like Avro, it defines schemas (`.proto` files) and produces compact binary output. Unlike Avro, Protobuf schemas are designed to be backward and forward compatible by default — every field has a numeric tag, and unknown fields are preserved rather than discarded.

**Advantages**:
- Compact binary encoding
- Backward and forward compatibility is built into the format
- Strong tooling and language support (Java, Go, Python, C++, and more)
- Fields are identified by numeric tags, not names — renaming a field does not break compatibility

**Disadvantages**:
- Requires running a Schema Registry for Kafka integration (or embedding schemas manually)
- Not human-readable
- `.proto` file compilation adds a build step
- Less common than Avro in the Kafka ecosystem (though growing)

### JSON Schema

JSON Schema is a middle ground: events are still JSON (human-readable), but a schema stored in a Schema Registry validates the structure. This gives JSON the same evolution guarantees that Avro provides.

**Advantages**:
- Human-readable messages (still JSON on the wire)
- Schema Registry enforces evolution rules
- Easier migration path from plain JSON — add the registry, start enforcing

**Disadvantages**:
- Larger message size than Avro or Protobuf (still text-based)
- Schema Registry required
- Less mature tooling compared to Avro for Kafka

### Comparison table

| Feature | JSON | Avro | Protobuf | JSON Schema |
|---|---|---|---|---|
| **Message format** | Text (JSON) | Binary | Binary | Text (JSON) |
| **Human-readable** | ✅ Yes | ❌ No | ❌ No | ✅ Yes |
| **Message size** | Large | Small | Small | Large |
| **Schema enforcement** | ❌ None | ✅ Schema Registry | ✅ Schema Registry | ✅ Schema Registry |
| **Evolution rules** | Convention only | Registry-enforced | Built-in + Registry | Registry-enforced |
| **Infrastructure required** | None | Schema Registry | Schema Registry | Schema Registry |
| **Spring Boot support** | Built-in (`spring-kafka`) | Via Confluent or Spring Cloud | Via Confluent or Spring Cloud | Via Confluent |
| **Learning curve** | Low | Medium | Medium | Low–Medium |
| **Debugging ease** | Easy (plain text) | Hard (binary) | Hard (binary) | Easy (plain text) |
| **Best for** | Learning, simple apps, internal tools | Production, multi-team, strict evolution | Production, polyglot, gRPC integration | Teams migrating from JSON to enforced schemas |

### Why JSON is the right starting choice

For learning and for applications where a single team controls both the producer and consumer, JSON is the right choice. It requires no additional infrastructure, produces readable output that is easy to debug, and is fully supported by Spring Boot's `spring-kafka` library out of the box. The configuration from Section 2 — `JsonSerializer` on the producer, `JsonDeserializer` on the consumer — is all that is needed.

### When to move to Avro or Protobuf

The signals that indicate it is time to move beyond JSON are:

1. **Multiple teams** consume the same topic, and there is no single owner who can coordinate schema changes
2. **Message volume is high** and the JSON overhead (repeated field names, text encoding) creates measurable cost in storage or network bandwidth
3. **Schema evolution mistakes** have caused production incidents — a producer deployed a breaking change that crashed consumers
4. **Regulatory or audit requirements** demand that message schemas be formally versioned and that incompatible changes be rejected before deployment

At that point, Avro or Protobuf with a Schema Registry formalizes the contract and makes incompatible changes mechanically impossible rather than relying on developer discipline.

---

## 3.7 Practical Event Design: A Complete Example

This section refactors the simple `OrderEvent` from Section 2 into a well-designed event with a proper envelope, metadata fields, and a clean separation between the envelope and the business payload.

### Step 1: The base event envelope

Every event in the system shares the same metadata fields. A base class captures those fields so that every concrete event type inherits them:

```java
package com.example.kafkalearning.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Base class for all domain events.
 * Contains metadata fields (the "envelope") that every event must carry.
 *
 * Subclasses add business-specific payload fields.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class BaseEvent {

    private String eventId;
    private String eventType;
    private int eventVersion;
    private Instant occurredAt;
    private String source;
    private String traceId;
    private String correlationId;

    // ─── Default constructor required by Jackson ───────────────
    protected BaseEvent() {
    }

    protected BaseEvent(String eventType, int eventVersion, String source,
                        String traceId, String correlationId) {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = eventType;
        this.eventVersion = eventVersion;
        this.occurredAt = Instant.now();
        this.source = source;
        this.traceId = traceId;
        this.correlationId = correlationId;
    }

    // ─── Getters and Setters ─────────────────────────────────

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public int getEventVersion() { return eventVersion; }
    public void setEventVersion(int eventVersion) { this.eventVersion = eventVersion; }

    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    @Override
    public String toString() {
        return "eventId='" + eventId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventVersion=" + eventVersion +
                ", occurredAt=" + occurredAt +
                ", source='" + source + '\'' +
                ", traceId='" + traceId + '\'' +
                ", correlationId='" + correlationId + '\'';
    }
}
```

### Step 2: A concrete event with business payload

The `OrderCreatedEvent` extends the base envelope and adds the order-specific fields:

```java
package com.example.kafkalearning.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Published when a new order is created.
 *
 * Business payload: orderId, customerName, product, quantity, totalAmount.
 * Metadata fields are inherited from BaseEvent.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderCreatedEvent extends BaseEvent {

    private static final String EVENT_TYPE = "OrderCreated";
    private static final int CURRENT_VERSION = 1;

    private String orderId;
    private String customerName;
    private String product;
    private int quantity;
    private double totalAmount;

    // ─── Default constructor required by Jackson ───────────────
    public OrderCreatedEvent() {
    }

    public OrderCreatedEvent(String orderId, String customerName, String product,
                             int quantity, double totalAmount,
                             String source, String traceId, String correlationId) {
        super(EVENT_TYPE, CURRENT_VERSION, source, traceId, correlationId);
        this.orderId = orderId;
        this.customerName = customerName;
        this.product = product;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
    }

    // ─── Getters and Setters ─────────────────────────────────

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getCustomerName() { return customerName; }
    public void setCustomerName(String customerName) { this.customerName = customerName; }

    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }

    @Override
    public String toString() {
        return "OrderCreatedEvent{" +
                super.toString() +
                ", orderId='" + orderId + '\'' +
                ", customerName='" + customerName + '\'' +
                ", product='" + product + '\'' +
                ", quantity=" + quantity +
                ", totalAmount=" + totalAmount +
                '}';
    }
}
```

### Step 3: The producer sending the well-designed event

The producer now creates a fully populated event with all metadata fields:

```java
package com.example.kafkalearning.producer;

import com.example.kafkalearning.events.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private static final String SOURCE = "order-service";

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @Value("${app.kafka.topic.orders}")
    private String ordersTopic;

    public OrderEventProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String orderId, String customerName, String product,
                        int quantity, double totalAmount, String traceId) {

        // correlationId ties all events in this order's lifecycle together
        String correlationId = "order-flow-" + orderId;

        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId, customerName, product, quantity, totalAmount,
                SOURCE, traceId, correlationId
        );

        log.info("Publishing event: eventId={}, eventType={}, key={}, topic={}",
                event.getEventId(), event.getEventType(), orderId, ordersTopic);

        CompletableFuture<SendResult<String, OrderCreatedEvent>> future =
                kafkaTemplate.send(ordersTopic, orderId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event: eventId={}, error={}",
                        event.getEventId(), ex.getMessage(), ex);
            } else {
                log.info("Event published: eventId={}, topic={}, partition={}, offset={}",
                        event.getEventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
```

### Step 4: Jackson configuration for safe deserialization

The `ObjectMapper` is configured globally to handle unknown properties and Java 8 date/time types:

```java
package com.example.kafkalearning.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // Support java.time.Instant and other JSR-310 types
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Forward compatibility: ignore fields the consumer class doesn't have
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        return mapper;
    }
}
```

### Step 5: Application configuration

The `application.yml` configures the Kafka producer and consumer to use JSON serialization with the well-designed event classes:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      properties:
        enable.idempotence: true
    consumer:
      group-id: order-consumer-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.example.kafkalearning.events"
        spring.json.value.default.type: "com.example.kafkalearning.events.OrderCreatedEvent"

app:
  kafka:
    topic:
      orders: order-events
```

### What the produced JSON looks like

When the producer sends an `OrderCreatedEvent`, the JSON on the wire looks like this:

```json
{
  "eventId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "eventType": "OrderCreated",
  "eventVersion": 1,
  "occurredAt": "2024-07-15T10:30:00Z",
  "source": "order-service",
  "traceId": "abc-123-def-456",
  "correlationId": "order-flow-ORD-001",
  "orderId": "ORD-001",
  "customerName": "Alice",
  "product": "Laptop",
  "quantity": 1,
  "totalAmount": 999.99
}
```

Compare this to the minimal `OrderEvent` from Section 2, which had only the business fields. The well-designed event carries all the metadata needed for deduplication (`eventId`), debugging (`source`, `traceId`), correlation (`correlationId`), and safe evolution (`eventVersion`).

---

## 3.8 Common Mistakes

This section catalogs the most frequent contract design mistakes encountered in real Kafka applications, explains why each one is harmful, and describes the correct approach.

### Mistake 1: Coupling REST DTOs to Kafka events

This mistake was introduced in Section 2, but it is worth reinforcing because it is the single most common source of contract problems. When the same class is used for both the REST API request body and the Kafka event, changing the REST API (which happens frequently — field renames, new validation rules, different API versions) automatically changes the Kafka contract, potentially breaking every consumer.

```
                ┌──────────────────────────────────┐
                │  OrderDTO (shared)                │
                │  Used by REST controller          │──── changing one
                │  Used by Kafka producer           │     breaks the other
                │  Used by Kafka consumer           │
                └──────────────────────────────────┘

                           vs.

    ┌─────────────────┐     ┌────────────────────┐     ┌────────────────────┐
    │  OrderRequest    │     │  OrderCreatedEvent  │     │  OrderResponse     │
    │  (REST input)    │     │  (Kafka contract)   │     │  (REST output)     │
    └─────────────────┘     └────────────────────┘     └────────────────────┘
         evolves                  evolves                    evolves
       independently            independently              independently
```

The fix is simple: separate classes for REST and Kafka, mapped explicitly in the service layer.

### Mistake 2: Missing eventId

Without an `eventId`, consumers cannot deduplicate messages. In Kafka's at-least-once delivery model, duplicates are not a theoretical concern — they happen routinely during rebalances, retries, and broker failovers. A consumer without deduplication logic processes the same event twice, leading to double-charges, duplicate notifications, or corrupted state.

| With `eventId` | Without `eventId` |
|---|---|
| Consumer checks: "Have I seen this ID?" → skip if yes | Consumer has no way to distinguish a retry from a new event |
| Deduplication logic is straightforward | Must rely on business-level idempotency (checking if the order already exists in the database), which is more complex and error-prone |
| Works across restarts if IDs are stored persistently | No solution except hoping duplicates don't happen |

### Mistake 3: Using mutable schemas without versioning

When events evolve without an `eventVersion` field, consumers have no way to know which version of the schema they are processing. They must guess based on which fields are present — a fragile strategy that breaks whenever a field is both optional and absent for legitimate business reasons (not because of a schema change).

With an `eventVersion` field, the consumer can make a definitive determination:

```java
if (event.getEventVersion() == 1) {
    // V1 logic — currency field does not exist
} else if (event.getEventVersion() >= 2) {
    // V2 logic — currency field is available
}
```

Without it, the consumer resorts to guessing:

```java
if (event.getCurrency() != null) {
    // Maybe this is V2... or maybe it's V2 with no currency set?
}
```

### Mistake 4: Timestamp confusion

Events sometimes carry multiple timestamps with unclear names. A field called `timestamp` is ambiguous — does it mean when the business event occurred, when the message was published, or when the consumer processed it? These three moments can differ by seconds, minutes, or even hours.

The fix is to use explicit names:

| Bad Name | Good Name | Meaning |
|---|---|---|
| `timestamp` | `occurredAt` | When the business event happened |
| `timestamp` | `publishedAt` | When the producer sent the message (rarely needed — the Kafka record timestamp covers this) |
| `timestamp` | `processedAt` | When the consumer handled the message |

In most cases, only `occurredAt` needs to be in the event payload. The Kafka record timestamp serves as `publishedAt`, and the consumer can record `processedAt` in its own logs or database.

### Mistake 5: Over-engineering events on day one

It is tempting to design a maximally flexible event schema from the start — with deeply nested objects, multiple levels of polymorphism, a generic `Map<String, Object>` payload field, or an elaborate type hierarchy. This usually backfires:

- **Nested objects** make deserialization fragile — a null in a nested path causes `NullPointerException` chains
- **Generic payloads** (`Map<String, Object>`) defeat the purpose of having a typed contract
- **Deep type hierarchies** make it hard to understand what a specific event contains without reading multiple classes

The better approach is to start simple and evolve:

1. **Day one**: Flat event with metadata fields + business fields. No nesting, no generics.
2. **When a second event type appears**: Extract the common metadata into a base class.
3. **When schema changes happen**: Add optional fields, bump the version.
4. **When multiple teams share the topic**: Consider moving from JSON to Avro/Protobuf with a Schema Registry.

> **Key insight**: Design for today's requirements, but structure for tomorrow's evolution. A well-designed flat event with an `eventVersion` field is better than an over-engineered framework that no one on the team understands.

---

## 3.9 Key Takeaways

This section covered the design of Kafka message contracts — the agreed-upon structures that flow between producers and consumers. The central argument is that message contract design is not a secondary concern to be addressed after the code works; it is the *primary* determinant of whether a Kafka-based system remains maintainable as it grows.

Events should be named in the past tense using domain language (`OrderCreated`, not `CreateOrder`) because they represent facts about what has happened, not commands for what should happen. Every event should carry metadata — an `eventId` for deduplication, an `eventType` for routing, an `eventVersion` for safe schema evolution, an `occurredAt` timestamp for when the business event happened, a `source` for identifying the producer, and trace/correlation IDs for distributed debugging. These metadata fields form an envelope that wraps the business payload and provides the operational infrastructure that production systems require.

Schema evolution must be treated with discipline. Adding optional fields is safe; removing or renaming fields is not. Jackson's `FAIL_ON_UNKNOWN_PROPERTIES` setting must be disabled on consumers to allow forward compatibility. An explicit `eventVersion` field gives consumers a reliable way to branch on schema version rather than guessing from field presence.

Key design is part of the contract, not just a producer implementation detail. The message key determines ordering and partition assignment, and changing it is a breaking change. JSON is the right serialization format for learning and simple applications; Avro and Protobuf with a Schema Registry are the right choice for production systems with multiple teams and strict evolution requirements.

Finally, REST DTOs and Kafka events must remain separate classes that evolve independently, events should not be over-engineered on day one, and every event should carry a UUID event ID from its first version — retrofitting deduplication later is far harder than including it from the start.

**Next**: [Section 4: Producer Design](section-4-producer-design.md) — where the focus shifts to building robust, production-ready Kafka producers with retry strategies, error handling, and delivery guarantees.
