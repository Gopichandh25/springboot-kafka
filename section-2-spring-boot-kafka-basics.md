# Section 2: Spring Boot Kafka Basics

> **Audience**: You understand Kafka's mental model (topics, partitions, keys, offsets, consumer
> groups). Now you will build your first Spring Boot + Kafka application from scratch, with
> production-quality structure from the start.
>
> This section is comprehensive. Work through it subsection by subsection.

---

## Table of Contents

- [2.1 Project Setup](#21-project-setup)
- [2.2 Docker Compose for Local Kafka](#22-docker-compose-for-local-kafka)
- [2.3 Application Configuration](#23-application-configuration)
- [2.4 Kafka Configuration Class](#24-kafka-configuration-class)
- [2.5 The Message DTO](#25-the-message-dto)
- [2.6 Producer with KafkaTemplate](#26-producer-with-kafkatemplate)
- [2.7 Consumer with @KafkaListener](#27-consumer-with-kafkalistener)
- [2.8 REST Controller to Trigger Events](#28-rest-controller-to-trigger-events)
- [2.9 Running and Verifying](#29-running-and-verifying)
- [2.10 Deep Dive: KafkaTemplate Internals](#210-deep-dive-kafkatemplate-internals)
- [2.11 Deep Dive: @KafkaListener Internals](#211-deep-dive-kafkalistener-internals)
- [2.12 Deep Dive: Externalized Configuration](#212-deep-dive-externalized-configuration)
- [2.13 Multiple Consumer Groups](#213-multiple-consumer-groups)
- [2.14 Custom Serializers and Trusted Packages](#214-custom-serializers-and-trusted-packages)
- [2.15 Package Structure Best Practices](#215-package-structure-best-practices)
- [2.16 Common Mistakes and How to Avoid Them](#216-common-mistakes-and-how-to-avoid-them)
- [2.17 Complete Working Project](#217-complete-working-project)
- [2.18 Exercises](#218-exercises)
- [2.19 Exit Criteria](#219-exit-criteria)

---

## 2.1 Project Setup

### Dependencies

Use [Spring Initializr](https://start.spring.io/) or add these to your `pom.xml` manually:

```xml
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.3.0</version>   <!-- Use the latest stable 3.x -->
</parent>

<dependencies>
    <!-- Core Spring Boot -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <!-- Spring Kafka -->
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>

    <!-- Useful for cleaner DTOs -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>

    <!-- Testing -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

### What `spring-kafka` gives you

When you add `spring-kafka`, Spring Boot auto-configures:

| Component | What It Does |
|---|---|
| `KafkaTemplate` | Sends messages to Kafka topics |
| `KafkaListenerContainerFactory` | Creates listener containers that poll Kafka and dispatch to your `@KafkaListener` methods |
| `KafkaAdmin` | Auto-creates topics declared as `NewTopic` beans |
| `KafkaProperties` | Binds all `spring.kafka.*` properties |
| Serializer/Deserializer wiring | Connects your configured serializers to producers and consumers |

> **Key insight**: Spring Boot reads your `application.yml`, creates the necessary Kafka
> clients (`KafkaProducer`, `KafkaConsumer`), and wraps them in Spring abstractions. You almost
> never create these clients directly.

---

## 2.2 Docker Compose for Local Kafka

Create `docker-compose.yml` at the project root:

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
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
```

```bash
docker compose up -d
```

> **Why `AUTO_CREATE_TOPICS_ENABLE: false`?** In production, you want explicit topic creation
> with controlled partition counts and configurations. Disabling auto-creation forces you to
> declare topics properly — a good habit to start now.

---

## 2.3 Application Configuration

Create `src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: kafka-learning

  kafka:
    # ─── Broker connection ───────────────────────────────
    bootstrap-servers: localhost:9092

    # ─── Producer configuration ──────────────────────────
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all                          # wait for all in-sync replicas
      retries: 3                         # retry transient failures
      properties:
        enable.idempotence: true         # prevent duplicate sends on retry
        max.in.flight.requests.per.connection: 5  # safe with idempotence enabled

    # ─── Consumer configuration ──────────────────────────
    consumer:
      group-id: order-processing-group
      auto-offset-reset: earliest        # on first join, start from beginning
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.example.kafkalearning.dto"

# ─── Application-specific config ───────────────────────
app:
  kafka:
    topic:
      orders: order-events
```

### Line-by-line explanation

| Property | Purpose |
|---|---|
| `bootstrap-servers` | Initial broker(s) your app contacts. The client discovers the full cluster from here. |
| `key-serializer` / `key-deserializer` | How message keys are converted to/from bytes. `StringSerializer` is the standard choice. |
| `value-serializer` | `JsonSerializer` converts your Java objects to JSON bytes before sending. |
| `value-deserializer` | `JsonDeserializer` converts JSON bytes back into Java objects on consumption. |
| `acks: all` | The producer waits until all in-sync replicas have written the record. Maximum durability. |
| `retries: 3` | The producer retries transient errors (network blips, leader elections) up to 3 times. |
| `enable.idempotence: true` | The broker deduplicates retried sends. Prevents the same message being written twice. |
| `group-id` | Identifies this consumer as part of a group. All instances with the same group-id share the load. |
| `auto-offset-reset: earliest` | If no committed offset exists (first run), start consuming from the oldest message. |
| `spring.json.trusted.packages` | Security: only deserialize classes from these packages. Prevents arbitrary class instantiation. |
| `app.kafka.topic.orders` | Custom property. Centralizes topic names so they are not scattered across code. |

---

## 2.4 Kafka Configuration Class

Create `src/main/java/com/example/kafkalearning/config/KafkaConfig.java`:

```java
package com.example.kafkalearning.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topic.orders}")
    private String ordersTopic;

    /**
     * Declares the topic programmatically.
     * Spring Kafka's KafkaAdmin auto-creates this topic on startup
     * if it does not already exist on the broker.
     */
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(ordersTopic)
                .partitions(3)
                .replicas(1)       // use 3 in production with multi-broker
                .build();
    }
}
```

### Why declare topics as beans?

- **Explicit control**: You decide partition count, replication factor, and topic configs.
- **Repeatable**: When your app starts, `KafkaAdmin` ensures the topic exists. If it already
  exists with the same settings, nothing happens. If settings differ, it logs a warning but
  does not modify the existing topic (by default).
- **Documentation**: Your topic inventory lives in code, not in tribal knowledge.

### Alternative: Multiple topics

```java
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name("order-events").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentEventsTopic() {
        return TopicBuilder.name("payment-events").partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic notificationEventsTopic() {
        return TopicBuilder.name("notification-events")
                .partitions(3)
                .replicas(1)
                .config("retention.ms", "86400000") // 1 day retention
                .build();
    }
}
```

---

## 2.5 The Message DTO

Create `src/main/java/com/example/kafkalearning/dto/OrderEvent.java`:

```java
package com.example.kafkalearning.dto;

import java.time.Instant;

/**
 * Event published when an order action occurs.
 *
 * IMPORTANT: This DTO is the Kafka message contract.
 * Keep it separate from your REST API request/response models.
 */
public class OrderEvent {

    private String orderId;
    private String customerName;
    private String product;
    private int quantity;
    private double totalAmount;
    private String status;           // CREATED, CONFIRMED, SHIPPED, etc.
    private Instant occurredAt;

    // ─── Default constructor required by Jackson deserialization ─────
    public OrderEvent() {
    }

    public OrderEvent(String orderId, String customerName, String product,
                      int quantity, double totalAmount, String status) {
        this.orderId = orderId;
        this.customerName = customerName;
        this.product = product;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
        this.status = status;
        this.occurredAt = Instant.now();
    }

    // ─── Getters and Setters ─────────────────────────────────────────

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

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", customerName='" + customerName + '\'' +
                ", product='" + product + '\'' +
                ", quantity=" + quantity +
                ", totalAmount=" + totalAmount +
                ", status='" + status + '\'' +
                ", occurredAt=" + occurredAt +
                '}';
    }
}
```

### REST request model (separate from the Kafka event)

Create `src/main/java/com/example/kafkalearning/dto/OrderRequest.java`:

```java
package com.example.kafkalearning.dto;

/**
 * HTTP request body for creating an order.
 * This is NOT the Kafka event model — it maps to what the REST client sends.
 */
public class OrderRequest {

    private String customerName;
    private String product;
    private int quantity;
    private double totalAmount;

    public OrderRequest() {
    }

    public OrderRequest(String customerName, String product, int quantity, double totalAmount) {
        this.customerName = customerName;
        this.product = product;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
    }

    public String getCustomerName() { return customerName; }
    public void setCustomerName(String customerName) { this.customerName = customerName; }

    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }
}
```

> **Why two models?** Your HTTP API and your Kafka event contract **will** diverge. The REST
> request might not include `orderId` (generated server-side), `status` (set by business logic),
> or `occurredAt` (set at publish time). Coupling them now creates pain later.

---

## 2.6 Producer with KafkaTemplate

Create `src/main/java/com/example/kafkalearning/producer/OrderEventProducer.java`:

```java
package com.example.kafkalearning.producer;

import com.example.kafkalearning.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final String topic;

    public OrderEventProducer(
            KafkaTemplate<String, OrderEvent> kafkaTemplate,
            @Value("${app.kafka.topic.orders}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /**
     * Sends an order event to Kafka.
     *
     * Key = orderId → all events for the same order go to the same partition
     *                  → ordering is preserved per order
     */
    public CompletableFuture<SendResult<String, OrderEvent>> sendOrderEvent(OrderEvent event) {
        String key = event.getOrderId();

        log.info("Publishing event: key={}, topic={}, event={}", key, topic, event);

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event: key={}, topic={}", key, topic, ex);
            } else {
                log.info("Event published successfully: key={}, topic={}, partition={}, offset={}",
                        key,
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }
}
```

### Understanding `KafkaTemplate.send()`

`KafkaTemplate` provides several send methods:

```java
// Method 1: topic + value (no key — round-robin partitioning)
kafkaTemplate.send("my-topic", event);

// Method 2: topic + key + value (key-based partitioning)
kafkaTemplate.send("my-topic", "order-123", event);

// Method 3: topic + partition + key + value (explicit partition — rarely used)
kafkaTemplate.send("my-topic", 0, "order-123", event);

// Method 4: ProducerRecord (full control: headers, timestamp, etc.)
ProducerRecord<String, OrderEvent> record = new ProducerRecord<>(
    "my-topic",                    // topic
    null,                          // partition (null = let partitioner decide)
    "order-123",                   // key
    event,                         // value
    List.of(                       // headers
        new RecordHeader("traceId", "abc-123".getBytes()),
        new RecordHeader("source", "order-service".getBytes())
    )
);
kafkaTemplate.send(record);
```

### What happens under the hood

```
Your code                   Spring Kafka                    Kafka Broker
    │                           │                               │
    │  kafkaTemplate.send()     │                               │
    │ ─────────────────────────>│                               │
    │                           │  serialize key (String)       │
    │                           │  serialize value (JSON)       │
    │                           │  determine partition           │
    │                           │     (hash of key % partitions) │
    │                           │  add to batch buffer          │
    │                           │  ──────────────────────────>  │
    │                           │                     batch sent │
    │                           │  <──────────────────────────  │
    │                           │        ack (with metadata)    │
    │  CompletableFuture        │                               │
    │    completes with         │                               │
    │    SendResult             │                               │
    │ <─────────────────────────│                               │
```

### Key points about the producer

1. **`send()` is asynchronous.** It returns a `CompletableFuture`. The message is buffered and
   sent in a batch. If you need to block until the send completes:

   ```java
   SendResult<String, OrderEvent> result = kafkaTemplate.send(topic, key, event).get();
   ```

   But **do not block in production hot paths** — use the callback approach shown above.

2. **Batching** is automatic. The producer batches messages headed for the same partition and
   sends them together for throughput. Controlled by `batch.size` and `linger.ms`.

3. **Retries** happen automatically on transient failures (network errors, leader elections).
   With `enable.idempotence=true`, retries are safe from duplicates.

4. **Never swallow exceptions.** The `whenComplete` callback shown above logs failures. In
   production, you would also trigger alerts or increment metrics.

---

## 2.7 Consumer with @KafkaListener

Create `src/main/java/com/example/kafkalearning/consumer/OrderEventConsumer.java`:

```java
package com.example.kafkalearning.consumer;

import com.example.kafkalearning.dto.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    /**
     * Listens for OrderEvent messages on the configured topic.
     *
     * Spring Kafka handles:
     * - Polling the broker
     * - Deserializing the message
     * - Calling this method
     * - Committing the offset (after successful return)
     */
    @KafkaListener(
        topics = "${app.kafka.topic.orders}",
        groupId = "${spring.kafka.consumer.group-id}"
    )
    public void handleOrderEvent(ConsumerRecord<String, OrderEvent> record) {
        log.info("Received event: topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());

        OrderEvent event = record.value();

        // ─── Your business logic goes here ──────────────────────────
        processOrder(event);
    }

    private void processOrder(OrderEvent event) {
        log.info("Processing order: orderId={}, status={}, product={}, qty={}",
                event.getOrderId(),
                event.getStatus(),
                event.getProduct(),
                event.getQuantity());

        // In a real application:
        // - Save to database
        // - Call downstream services
        // - Update order state machine
        // For now, just log it.
    }
}
```

### Alternative listener signatures

Spring Kafka supports multiple listener method signatures:

```java
// ─── Option 1: Just the value ────────────────────────────
@KafkaListener(topics = "order-events")
public void handle(OrderEvent event) {
    // Simple, but you lose access to key, partition, offset
}

// ─── Option 2: ConsumerRecord (recommended for learning) ─
@KafkaListener(topics = "order-events")
public void handle(ConsumerRecord<String, OrderEvent> record) {
    // Full access to key, value, partition, offset, headers, timestamp
    String key = record.key();
    OrderEvent event = record.value();
    int partition = record.partition();
    long offset = record.offset();
}

// ─── Option 3: With @Header annotations ──────────────────
@KafkaListener(topics = "order-events")
public void handle(
        OrderEvent event,
        @Header(KafkaHeaders.RECEIVED_KEY) String key,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset) {
    // Access specific metadata via headers
}

// ─── Option 4: With Acknowledgment (manual commit) ───────
@KafkaListener(topics = "order-events")
public void handle(ConsumerRecord<String, OrderEvent> record,
                   Acknowledgment acknowledgment) {
    // Process the record...
    processOrder(record.value());
    // Manually commit the offset
    acknowledgment.acknowledge();
}

// ─── Option 5: Batch listener ────────────────────────────
@KafkaListener(topics = "order-events")
public void handle(List<ConsumerRecord<String, OrderEvent>> records) {
    // Process a batch of records at once
    for (ConsumerRecord<String, OrderEvent> record : records) {
        processOrder(record.value());
    }
}
```

### How @KafkaListener works under the hood

```
Kafka Broker              Spring Kafka Listener Container           Your @KafkaListener Method
     │                              │                                        │
     │   poll() every 5 seconds     │                                        │
     │ <────────────────────────────│                                        │
     │                              │                                        │
     │   returns batch of records   │                                        │
     │ ────────────────────────────>│                                        │
     │                              │                                        │
     │                              │  deserialize each record              │
     │                              │  ───────────────────────────────────> │
     │                              │              handleOrderEvent(record)  │
     │                              │                                        │
     │                              │  <─────────────────────────────────── │
     │                              │              method returns normally   │
     │                              │                                        │
     │                              │  commit offset                        │
     │   offset commit              │                                        │
     │ <────────────────────────────│                                        │
```

### Offset commit behavior (critical to understand)

| Mode | How It Works | When to Use |
|---|---|---|
| `BATCH` (default) | Commit after all records from the last `poll()` are processed | Most applications |
| `RECORD` | Commit after each individual record | When each record is expensive and you want fine-grained progress |
| `MANUAL` | You call `acknowledgment.acknowledge()` | When you need exact control (e.g., commit after DB write) |
| `MANUAL_IMMEDIATE` | Same as MANUAL but commits immediately instead of waiting for the next poll | Rare, for very specific use cases |

To change the ack mode:

```yaml
spring:
  kafka:
    listener:
      ack-mode: MANUAL   # or RECORD, BATCH, MANUAL_IMMEDIATE
```

Or in a configuration class:

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, OrderEvent>
        kafkaListenerContainerFactory(ConsumerFactory<String, OrderEvent> consumerFactory) {

    ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    return factory;
}
```

---

## 2.8 REST Controller to Trigger Events

Create `src/main/java/com/example/kafkalearning/controller/OrderController.java`:

```java
package com.example.kafkalearning.controller;

import com.example.kafkalearning.dto.OrderEvent;
import com.example.kafkalearning.dto.OrderRequest;
import com.example.kafkalearning.producer.OrderEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderEventProducer producer;

    public OrderController(OrderEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody OrderRequest request) {
        // Generate order ID server-side
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8);

        // Convert REST request to Kafka event
        // Notice: the event has fields the request does not (orderId, status, occurredAt)
        OrderEvent event = new OrderEvent(
                orderId,
                request.getCustomerName(),
                request.getProduct(),
                request.getQuantity(),
                request.getTotalAmount(),
                "CREATED"
        );

        // Publish asynchronously
        producer.sendOrderEvent(event);

        return ResponseEntity.accepted().body("Order accepted: " + orderId);
    }
}
```

### The flow in action

```
HTTP Client                 OrderController              OrderEventProducer           Kafka
    │                            │                            │                        │
    │  POST /api/orders          │                            │                        │
    │  { customerName, ... }     │                            │                        │
    │ ──────────────────────────>│                            │                        │
    │                            │  generate orderId          │                        │
    │                            │  build OrderEvent          │                        │
    │                            │  ──────────────────────>   │                        │
    │                            │     sendOrderEvent(event)  │                        │
    │                            │                            │  kafkaTemplate.send()  │
    │                            │                            │ ─────────────────────> │
    │  202 Accepted              │                            │                        │
    │ <──────────────────────────│                            │                        │
    │                            │                            │                        │
    │                            │                   (async)  │  ack from broker       │
    │                            │                            │ <───────────────────── │
    │                            │                            │  log success           │
```

> **Note**: The HTTP response returns **immediately** (`202 Accepted`) without waiting for
> Kafka acknowledgment. This is an asynchronous, fire-and-forget pattern from the HTTP client's
> perspective. The Kafka producer handles delivery asynchronously.

---

## 2.9 Running and Verifying

### Step 1: Start Kafka

```bash
docker compose up -d
```

### Step 2: Run the Spring Boot application

```bash
./mvnw spring-boot:run
```

You should see logs like:

```
KafkaConfig: Topic 'order-events' created with 3 partitions
o.a.k.c.p.KafkaProducer: [Producer clientId=producer-1] ... connected to node
o.s.k.l.KafkaMessageListenerContainer: order-processing-group: partitions assigned: [order-events-0, order-events-1, order-events-2]
```

### Step 3: Send a test event

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerName": "Alice",
    "product": "Laptop",
    "quantity": 1,
    "totalAmount": 1299.99
  }'
```

Response:

```
Order accepted: ORD-a1b2c3d4
```

### Step 4: Check the application logs

You should see **both** producer and consumer logs:

```
OrderEventProducer : Publishing event: key=ORD-a1b2c3d4, topic=order-events, event=OrderEvent{...}
OrderEventProducer : Event published successfully: key=ORD-a1b2c3d4, topic=order-events, partition=1, offset=0
OrderEventConsumer : Received event: topic=order-events, partition=1, offset=0, key=ORD-a1b2c3d4, value=OrderEvent{...}
OrderEventConsumer : Processing order: orderId=ORD-a1b2c3d4, status=CREATED, product=Laptop, qty=1
```

### Step 5: Verify partition consistency

Send multiple orders with the same logical key to verify ordering:

```bash
# These two requests create different orders, but watch the partition assignments
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName":"Bob","product":"Phone","quantity":2,"totalAmount":599.99}'

curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName":"Charlie","product":"Tablet","quantity":1,"totalAmount":449.99}'
```

Each order gets a unique `orderId`, so they may land in different partitions. Check the logs
to see which partition each order went to.

---

## 2.10 Deep Dive: KafkaTemplate Internals

### How Spring Boot creates KafkaTemplate

When your application starts:

1. Spring Boot reads `spring.kafka.producer.*` properties
2. It creates a `ProducerFactory<K, V>` (specifically `DefaultKafkaProducerFactory`)
3. The `ProducerFactory` holds a map of configuration properties (bootstrap servers, serializers, acks, etc.)
4. `KafkaTemplate` is constructed with this factory
5. When you call `send()`, `KafkaTemplate` gets a `KafkaProducer` from the factory and sends the record

### Configuration hierarchy

```
application.yml                   KafkaProperties                DefaultKafkaProducerFactory
spring.kafka.producer.*    →    Spring Boot reads these    →    creates producer with these configs
                                                                 │
                                                           KafkaTemplate wraps the producer
```

### Creating a custom KafkaTemplate (when you need it)

Sometimes you need more control than `application.yml` provides:

```java
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, OrderEvent> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Batching tuning
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);      // 16 KB
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);           // wait up to 5ms for batch

        // Compression
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, OrderEvent> kafkaTemplate(
            ProducerFactory<String, OrderEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
```

> **When to use custom config vs `application.yml`**: Start with `application.yml`. Move to
> Java config when you need multiple `KafkaTemplate` beans with different configurations (e.g.,
> one for high-throughput events, one for critical financial events with different acks).

---

## 2.11 Deep Dive: @KafkaListener Internals

### The listener container lifecycle

```
Application Startup
      │
      ▼
Spring scans for @KafkaListener annotations
      │
      ▼
For each @KafkaListener, creates a KafkaMessageListenerContainer
      │
      ▼
Each container starts a consumer thread that:
  1. Calls consumer.poll() in a loop
  2. Deserializes records
  3. Dispatches to your @KafkaListener method
  4. Commits offsets based on ack-mode
      │
      ▼
On shutdown: stops polling, commits final offsets, closes consumers
```

### Concurrency

By default, each `@KafkaListener` creates **one** consumer thread. If your topic has 3
partitions, one thread handles all 3 partitions.

To parallelize:

```java
@KafkaListener(
    topics = "${app.kafka.topic.orders}",
    groupId = "order-processing-group",
    concurrency = "3"          // creates 3 consumer threads
)
public void handleOrderEvent(ConsumerRecord<String, OrderEvent> record) {
    // Each thread handles a subset of partitions
}
```

Or globally in `application.yml`:

```yaml
spring:
  kafka:
    listener:
      concurrency: 3
```

> **Rule**: `concurrency` should be ≤ partition count. If `concurrency > partitions`, some
> threads sit idle. If `concurrency < partitions`, each thread handles multiple partitions.

### Container factory customization

For advanced listener behavior:

```java
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent>
            kafkaListenerContainerFactory(ConsumerFactory<String, OrderEvent> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        // Error handler — covered in depth in Section 6
        factory.setCommonErrorHandler(new DefaultErrorHandler());

        return factory;
    }
}
```

---

## 2.12 Deep Dive: Externalized Configuration

### Property-driven vs Java-driven configuration

| Approach | Best For | Example |
|---|---|---|
| `application.yml` only | Simple apps, single producer/consumer | `spring.kafka.producer.acks=all` |
| Java `@Configuration` | Multiple producers/consumers with different configs | Custom `ProducerFactory` beans |
| Combination | Most production apps | Basic config in YAML, advanced overrides in Java |

### Environment-specific configuration

```
src/main/resources/
  ├── application.yml                  # shared defaults
  ├── application-local.yml            # local Docker Kafka
  ├── application-dev.yml              # dev cluster
  └── application-prod.yml             # production cluster
```

`application.yml` (shared):

```yaml
spring:
  kafka:
    producer:
      acks: all
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer

app:
  kafka:
    topic:
      orders: order-events
```

`application-local.yml`:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: order-processing-local
```

`application-dev.yml`:

```yaml
spring:
  kafka:
    bootstrap-servers: kafka-dev-1:9092,kafka-dev-2:9092,kafka-dev-3:9092
    consumer:
      group-id: order-processing-dev
```

`application-prod.yml`:

```yaml
spring:
  kafka:
    bootstrap-servers: kafka-prod-1:9092,kafka-prod-2:9092,kafka-prod-3:9092
    consumer:
      group-id: order-processing-prod
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: >
        org.apache.kafka.common.security.plain.PlainLoginModule required
        username="${KAFKA_USERNAME}"
        password="${KAFKA_PASSWORD}";
```

Activate a profile:

```bash
# Local development
./mvnw spring-boot:run -Dspring-boot.run.profiles=local

# Or via environment variable
SPRING_PROFILES_ACTIVE=prod java -jar app.jar
```

### Centralizing topic names

Never scatter topic name strings across your codebase:

```java
/**
 * Centralized topic name constants.
 * Referenced from @KafkaListener, producers, and KafkaConfig.
 */
public final class KafkaTopics {
    private KafkaTopics() {} // prevent instantiation

    public static final String ORDER_EVENTS = "${app.kafka.topic.orders}";
    // Add more topics as your project grows
}
```

Usage:

```java
@KafkaListener(topics = KafkaTopics.ORDER_EVENTS)
public void handle(ConsumerRecord<String, OrderEvent> record) { ... }
```

Or use a properties class:

```java
@ConfigurationProperties(prefix = "app.kafka.topic")
public class KafkaTopicProperties {
    private String orders;
    private String payments;
    private String notifications;

    // getters and setters
}
```

---

## 2.13 Multiple Consumer Groups

### Why multiple groups?

A common pattern: one topic, multiple independent consumers.

```
                Topic: order-events
              ┌──────────────────────────┐
              │  Partition 0             │
              │  Partition 1             │
              │  Partition 2             │
              └──────────┬───────────────┘
                         │
            ┌────────────┴────────────┐
            ▼                         ▼
    Group: order-processor     Group: order-auditor
    (processes the order)       (writes to audit log)
    (each message once)         (each message once)
```

### Implementation

```java
@Service
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    @KafkaListener(
        topics = "${app.kafka.topic.orders}",
        groupId = "order-processing-group"
    )
    public void processOrder(ConsumerRecord<String, OrderEvent> record) {
        log.info("[PROCESSOR] partition={}, offset={}, key={}",
                record.partition(), record.offset(), record.key());
        // Business logic: save to DB, update state, etc.
    }
}
```

```java
@Service
public class OrderAuditConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderAuditConsumer.class);

    @KafkaListener(
        topics = "${app.kafka.topic.orders}",
        groupId = "order-audit-group"
    )
    public void auditOrder(ConsumerRecord<String, OrderEvent> record) {
        log.info("[AUDITOR] partition={}, offset={}, key={}",
                record.partition(), record.offset(), record.key());
        // Audit logic: write to audit table, send to analytics, etc.
    }
}
```

**Both groups receive every message**, but process them independently. Each group tracks its
own offsets.

### Verifying group behavior

After publishing a few events, inspect the consumer groups:

```bash
docker exec -it kafka-local /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list

# Output:
# order-processing-group
# order-audit-group

docker exec -it kafka-local /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group order-processing-group
```

You will see each group's offset position per partition.

---

## 2.14 Custom Serializers and Trusted Packages

### The "trusted packages" problem

When using `JsonDeserializer`, Spring Kafka needs to know which Java classes are safe to
instantiate from incoming JSON. By default, it trusts nothing (security first).

If you see this error:

```
The class 'com.example.kafkalearning.dto.OrderEvent' is not in the trusted packages
```

Fix it in `application.yml`:

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.trusted.packages: "com.example.kafkalearning.dto"
```

Or trust all packages (only for development):

```yaml
spring.json.trusted.packages: "*"
```

### Type mapping

When the producer sends a JSON message, Spring Kafka adds a `__TypeId__` header containing the
fully qualified class name. The consumer uses this header to determine which class to
deserialize into.

If producer and consumer use different class names (common in microservices), configure type
mapping:

**Producer side:**

```yaml
spring:
  kafka:
    producer:
      properties:
        spring.json.type.mapping: "orderEvent:com.example.producer.dto.OrderEvent"
```

**Consumer side:**

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.type.mapping: "orderEvent:com.example.consumer.dto.OrderEventReceived"
```

The logical name `orderEvent` bridges the two different class names.

### Custom serializer/deserializer (rarely needed)

If you need full control:

```java
public class OrderEventSerializer implements Serializer<OrderEvent> {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public byte[] serialize(String topic, OrderEvent data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error serializing OrderEvent", e);
        }
    }
}
```

> **Recommendation**: Use Spring's built-in `JsonSerializer`/`JsonDeserializer` for JSON.
> Only write custom serializers when using Avro, Protobuf, or a non-standard format.

---

## 2.15 Package Structure Best Practices

### Recommended structure for a Kafka-enabled Spring Boot app

```
src/main/java/com/example/kafkalearning/
├── KafkaLearningApplication.java          # @SpringBootApplication
│
├── config/
│   └── KafkaConfig.java                   # Topic declarations, factory customization
│
├── controller/
│   └── OrderController.java               # REST endpoints
│
├── dto/
│   ├── OrderRequest.java                  # HTTP request model
│   └── OrderEvent.java                    # Kafka event model
│
├── producer/
│   └── OrderEventProducer.java            # KafkaTemplate wrapper
│
├── consumer/
│   ├── OrderEventConsumer.java            # Business processing listener
│   └── OrderAuditConsumer.java            # Audit/analytics listener
│
└── service/
    └── OrderService.java                  # Business logic (called by controller & consumer)
```

### Key principles

| Principle | Why |
|---|---|
| **Separate `dto` for HTTP and Kafka models** | They will diverge. REST request ≠ Kafka event. |
| **`producer/` package** | Producers are infrastructure. Keep them separate from business logic. |
| **`consumer/` package** | Listeners are entry points, like controllers. Separate from services. |
| **`service/` for business logic** | Both controllers and consumers call into services. Do not put business logic in listeners. |
| **`config/` for Kafka config** | Topic declarations and factory beans live here. |

### The service layer pattern

Your `@KafkaListener` method should be **thin** — just like a REST controller:

```java
// ❌ Bad: business logic in the listener
@KafkaListener(topics = "order-events")
public void handle(OrderEvent event) {
    // 50 lines of business logic here
    // Database calls, external API calls, validation...
}

// ✅ Good: listener delegates to a service
@KafkaListener(topics = "order-events")
public void handle(ConsumerRecord<String, OrderEvent> record) {
    log.info("Received: key={}, partition={}, offset={}",
            record.key(), record.partition(), record.offset());
    orderService.processOrderEvent(record.value());
}
```

This makes your business logic testable without Kafka infrastructure.

---

## 2.16 Common Mistakes and How to Avoid Them

### Mistake 1: Not setting trusted packages

```
org.apache.kafka.common.errors.SerializationException:
Error deserializing key/value for partition order-events-0 at offset 0
```

**Fix**: Set `spring.json.trusted.packages` in your consumer config.

### Mistake 2: Coupling REST DTOs to Kafka events

Your REST API adds a field? Now your Kafka event contract changes too? That is coupling.

**Fix**: Separate `OrderRequest` (HTTP) from `OrderEvent` (Kafka). Map between them explicitly.

### Mistake 3: Ignoring send failures

```java
// ❌ Bad: fire and forget with no error handling
kafkaTemplate.send(topic, key, event);
```

**Fix**: Always attach a callback or handle the future:

```java
// ✅ Good
kafkaTemplate.send(topic, key, event).whenComplete((result, ex) -> {
    if (ex != null) {
        log.error("Send failed for key={}", key, ex);
        // Alert, metric, fallback logic
    }
});
```

### Mistake 4: Hardcoding topic names

```java
// ❌ Bad
@KafkaListener(topics = "order-events")
```

**Fix**: Use externalized properties:

```java
// ✅ Good
@KafkaListener(topics = "${app.kafka.topic.orders}")
```

### Mistake 5: Not logging enough context

```java
// ❌ Bad: what topic? what partition? what offset?
log.info("Received order: {}", event.getOrderId());

// ✅ Good: full context for debugging
log.info("Received: topic={}, partition={}, offset={}, key={}, orderId={}",
        record.topic(), record.partition(), record.offset(),
        record.key(), event.getOrderId());
```

### Mistake 6: Setting concurrency higher than partition count

```yaml
# Topic has 3 partitions
spring.kafka.listener.concurrency: 10   # 7 threads will be idle!
```

**Fix**: Set `concurrency` ≤ partition count.

### Mistake 7: Using `auto-offset-reset: latest` without understanding it

With `latest`, if your consumer starts for the first time (no committed offsets), it
skips all existing messages and only sees new ones. Use `earliest` during development so you
do not silently miss messages.

---

## 2.17 Complete Working Project

### Main application class

```java
package com.example.kafkalearning;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaLearningApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaLearningApplication.class, args);
    }
}
```

### Complete `application.yml`

```yaml
server:
  port: 8080

spring:
  application:
    name: kafka-learning

  kafka:
    bootstrap-servers: localhost:9092

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5

    consumer:
      group-id: order-processing-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.example.kafkalearning.dto"

    listener:
      ack-mode: RECORD    # commit after each record (good for learning, visible behavior)

app:
  kafka:
    topic:
      orders: order-events
```

### File layout recap

```
kafka-learning/
├── docker-compose.yml
├── pom.xml
└── src/
    └── main/
        ├── java/com/example/kafkalearning/
        │   ├── KafkaLearningApplication.java
        │   ├── config/
        │   │   └── KafkaConfig.java
        │   ├── controller/
        │   │   └── OrderController.java
        │   ├── dto/
        │   │   ├── OrderEvent.java
        │   │   └── OrderRequest.java
        │   ├── producer/
        │   │   └── OrderEventProducer.java
        │   └── consumer/
        │       └── OrderEventConsumer.java
        └── resources/
            └── application.yml
```

---

## 2.18 Exercises

### Exercise 1: Basic flow

1. Start Kafka with Docker Compose
2. Run the application
3. Send 5 orders via `curl`
4. Observe the logs — verify producer and consumer logs match

### Exercise 2: Partition observation

1. Send 10+ orders
2. Note which partition each order lands in
3. Verify: is the partition assignment consistent for the same `orderId`? (Yes — because
   `orderId` is the key)

### Exercise 3: Add a second consumer group

1. Create an `OrderAuditConsumer` with `groupId = "order-audit-group"`
2. Send orders
3. Verify both consumers receive every message
4. Use `kafka-consumer-groups.sh --describe` to see both groups' offsets

### Exercise 4: Experiment with `auto-offset-reset`

1. Stop the application
2. Delete the consumer group: `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group order-processing-group`
3. Produce 3 new messages using the console producer
4. Start the application with `auto-offset-reset: latest`
5. Observe: does the consumer see the 3 messages? (No — `latest` skips existing messages on first join)
6. Change to `earliest` and repeat — now it sees them

### Exercise 5: Manual offset commit

1. Change `ack-mode` to `MANUAL` in `application.yml`
2. Add `Acknowledgment` parameter to your listener
3. Call `acknowledgment.acknowledge()` after processing
4. Verify behavior: if you comment out the `acknowledge()` call and restart, the message
   is redelivered

### Exercise 6: Send with headers

Add tracing headers to your producer:

```java
ProducerRecord<String, OrderEvent> record = new ProducerRecord<>(
    topic, null, key, event,
    List.of(new RecordHeader("traceId", UUID.randomUUID().toString().getBytes()))
);
kafkaTemplate.send(record);
```

Read them in the consumer:

```java
@KafkaListener(topics = "${app.kafka.topic.orders}")
public void handle(ConsumerRecord<String, OrderEvent> record) {
    Header traceHeader = record.headers().lastHeader("traceId");
    String traceId = traceHeader != null ? new String(traceHeader.value()) : "unknown";
    log.info("traceId={}, orderId={}", traceId, record.value().getOrderId());
}
```

---

## 2.19 Exit Criteria

Before moving to Section 3, confirm:

- [ ] You can create a Spring Boot project with `spring-kafka` and connect to a local Kafka broker
- [ ] You can send messages using `KafkaTemplate` with a key and receive them in a `@KafkaListener`
- [ ] You understand how `application.yml` drives Kafka configuration
- [ ] You can create topics programmatically using `NewTopic` beans
- [ ] You understand the difference between consumer groups (same group = load balanced, different group = broadcast)
- [ ] You have observed partition assignment, offsets, and consumer group lag
- [ ] You can explain when offset commit happens and what `ack-mode` controls
- [ ] Your REST DTOs and Kafka event models are separate classes
- [ ] Your producer logs key, topic, partition, and offset on success and failure
- [ ] Your consumer logs full context (topic, partition, offset, key) for every received message

**Next**: [Section 3: Message Contract Design](section-3-message-contract-design.md) — where you learn to design events that are safe to evolve.
