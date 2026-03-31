# Section 2: Spring Boot Kafka Basics

Every distributed system needs a way for its parts to communicate asynchronously, and Apache Kafka has become the standard tool for that job. Spring Boot, through the `spring-kafka` library, provides a high-level abstraction layer that handles the boilerplate of connecting to Kafka, serializing messages, managing consumer threads, and committing offsets. This section explains how each piece of that integration works — from project dependencies to producer and consumer internals — so that the underlying mechanics are clear throughout the code examples that follow.

---

## 2.1 Project Dependencies

A Spring Boot application that talks to Kafka requires a small set of Maven dependencies. The central one is `spring-kafka`, which brings in the Apache Kafka client libraries and the Spring abstractions built on top of them.

A typical `pom.xml` looks like this:

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

### What `spring-kafka` provides automatically

When the `spring-kafka` dependency is on the classpath, Spring Boot's auto-configuration creates several components without any explicit code:

| Component | What It Does |
|---|---|
| `KafkaTemplate` | Sends messages to Kafka topics |
| `KafkaListenerContainerFactory` | Creates listener containers that poll Kafka and dispatch to `@KafkaListener` methods |
| `KafkaAdmin` | Auto-creates topics declared as `NewTopic` beans |
| `KafkaProperties` | Binds all `spring.kafka.*` properties |
| Serializer/Deserializer wiring | Connects configured serializers to producers and consumers |

> **Key insight**: Spring Boot reads `application.yml`, creates the necessary Kafka
> clients (`KafkaProducer`, `KafkaConsumer`), and wraps them in Spring abstractions. Application
> code almost never creates these clients directly.

---

## 2.2 Local Kafka with Docker Compose

During development, a single-node Kafka cluster running in Docker is the simplest way to have a broker available locally. The following Docker Compose file defines a KRaft-mode Kafka broker (no ZooKeeper required) that listens on port 9092:

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

One setting here deserves special attention: `AUTO_CREATE_TOPICS_ENABLE` is set to `false`. By default, Kafka will silently create a topic the first time a producer or consumer references it — using default partition counts and replication factors that are almost never what a production system needs. Disabling auto-creation forces topics to be declared explicitly, with controlled partition counts and configurations. This is a good habit to adopt from the start because it mirrors how production clusters are managed.

Running `docker compose up -d` starts the broker in the background. The Spring Boot application can then connect to `localhost:9092`.

---

## 2.3 Application Configuration

Spring Boot centralizes Kafka configuration in `application.yml`. Rather than scattering connection details and serialization settings across Java classes, everything lives in one place that is easy to read, override per environment, and review in pull requests.

Here is a representative configuration file:

```yaml
spring:
  application:
    name: kafka-learning

  kafka:
    # ─── Broker connection ─────────────────────────────
    bootstrap-servers: localhost:9092

    # ─── Producer configuration ──────────────────────
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all                          # wait for all in-sync replicas
      retries: 3                         # retry transient failures
      properties:
        enable.idempotence: true         # prevent duplicate sends on retry
        max.in.flight.requests.per.connection: 5  # safe with idempotence enabled

    # ─── Consumer configuration ──────────────────────
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

### Property-by-property explanation

| Property | Purpose |
|---|---|
| `bootstrap-servers` | Initial broker(s) the app contacts. The client discovers the full cluster from here. |
| `key-serializer` / `key-deserializer` | How message keys are converted to/from bytes. `StringSerializer` is the standard choice. |
| `value-serializer` | `JsonSerializer` converts Java objects to JSON bytes before sending. |
| `value-deserializer` | `JsonDeserializer` converts JSON bytes back into Java objects on consumption. |
| `acks: all` | The producer waits until all in-sync replicas have written the record. Maximum durability. |
| `retries: 3` | The producer retries transient errors (network blips, leader elections) up to 3 times. |
| `enable.idempotence: true` | The broker deduplicates retried sends. Prevents the same message being written twice. |
| `group-id` | Identifies this consumer as part of a group. All instances with the same group-id share the load. |
| `auto-offset-reset: earliest` | If no committed offset exists (first run), start consuming from the oldest message. |
| `spring.json.trusted.packages` | Security: only deserialize classes from these packages. Prevents arbitrary class instantiation. |
| `app.kafka.topic.orders` | Custom property. Centralizes topic names so they are not scattered across code. |

---

## 2.4 Programmatic Topic Declaration

Kafka topics must exist before messages can be produced to them (especially when auto-creation is disabled, as recommended above). Spring Kafka's `KafkaAdmin` component can create topics automatically at application startup by scanning the Spring context for `NewTopic` beans.

Here is what a topic configuration class looks like:

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

- **Explicit control**: The partition count, replication factor, and topic-level configs are all specified in code.
- **Repeatable**: When the app starts, `KafkaAdmin` ensures the topic exists. If it already exists with the same settings, nothing happens. If settings differ, it logs a warning but does not modify the existing topic (by default).
- **Documentation**: The topic inventory lives in code, not in tribal knowledge or runbooks.

### Declaring multiple topics

As an application grows, it typically publishes to several topics. Each one is simply another `NewTopic` bean:

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

Messages flowing through Kafka need a well-defined structure. In a Spring Boot application, this structure takes the form of plain Java objects (DTOs) that the `JsonSerializer` converts to JSON bytes on the producer side and the `JsonDeserializer` reconstructs on the consumer side.

### The Kafka event model

The `OrderEvent` class represents the data that actually travels through Kafka:

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

### The REST request model (kept separate from the Kafka event)

The HTTP layer has its own DTO that maps to what a REST client sends:

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

### Why two models?

The HTTP API and the Kafka event contract **will** diverge over time. The REST request might not include `orderId` (generated server-side), `status` (set by business logic), or `occurredAt` (set at publish time). Coupling them into a single class means that a change in one contract forces a change in the other — a maintenance problem that grows with every new field. Keeping them separate from the start avoids this pain entirely.

---

## 2.6 The Producer: Sending Messages with KafkaTemplate

`KafkaTemplate` is the primary Spring abstraction for sending messages to Kafka. It wraps the native `KafkaProducer`, handles serialization, determines which partition to write to, and exposes the result as a `CompletableFuture`.

Here is what a producer service looks like:

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

### The various `send()` methods

`KafkaTemplate` provides several overloads of `send()`, each offering a different level of control:

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

The following diagram traces the journey of a single `send()` call:

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

### Key points about producer behavior

1. **`send()` is asynchronous.** It returns a `CompletableFuture`. The message is buffered and
   sent in a batch. Blocking until the send completes is possible:

   ```java
   SendResult<String, OrderEvent> result = kafkaTemplate.send(topic, key, event).get();
   ```

   However, **blocking in production hot paths defeats the purpose of asynchronous messaging** — the callback approach shown in the producer service above is preferred.

2. **Batching** is automatic. The producer batches messages headed for the same partition and
   sends them together for throughput. This behavior is controlled by `batch.size` and `linger.ms`.

3. **Retries** happen automatically on transient failures (network errors, leader elections).
   With `enable.idempotence=true`, retries are safe from duplicates.

4. **Never swallow exceptions.** The `whenComplete` callback shown above logs failures. In
   production, this is where alerts would be triggered or metrics incremented.

---

## 2.7 The Consumer: Receiving Messages with @KafkaListener

On the consumption side, Spring Kafka provides the `@KafkaListener` annotation. A method annotated with `@KafkaListener` is automatically invoked whenever a new message arrives on the specified topic. Behind the scenes, Spring Kafka manages the polling loop, deserialization, thread management, and offset commits.

Here is what a consumer service looks like:

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

        // ─── Business logic goes here ──────────────────────
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
    }
}
```

### Alternative listener signatures

Spring Kafka supports multiple method signatures for listener methods, each providing a different trade-off between simplicity and access to metadata:

```java
// ─── Option 1: Just the value ────────────────────
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

// ─── Option 3: With @Header annotations ──────────────
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

// ─── Option 5: Batch listener ────────────────────
@KafkaListener(topics = "order-events")
public void handle(List<ConsumerRecord<String, OrderEvent>> records) {
    // Process a batch of records at once
    for (ConsumerRecord<String, OrderEvent> record : records) {
        processOrder(record.value());
    }
}
```

### How @KafkaListener works under the hood

The following diagram illustrates the polling and dispatch cycle that Spring Kafka manages:

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

### Offset commit behavior

Understanding when offsets are committed is critical because it determines whether a message can be redelivered after a failure. Spring Kafka supports several acknowledgment modes:

| Mode | How It Works | When to Use |
|---|---|---|
| `BATCH` (default) | Commit after all records from the last `poll()` are processed | Most applications |
| `RECORD` | Commit after each individual record | When each record is expensive and you want fine-grained progress |
| `MANUAL` | You call `acknowledgment.acknowledge()` | When you need exact control (e.g., commit after DB write) |
| `MANUAL_IMMEDIATE` | Same as MANUAL but commits immediately instead of waiting for the next poll | Rare, for very specific use cases |

The ack mode can be set in `application.yml`:

```yaml
spring:
  kafka:
    listener:
      ack-mode: MANUAL   # or RECORD, BATCH, MANUAL_IMMEDIATE
```

Or through a configuration class:

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

## 2.8 The REST Controller: Bridging HTTP and Kafka

In most applications, Kafka messages are not produced in isolation — they originate from some external trigger. A common pattern is an HTTP endpoint that accepts a request, builds a Kafka event from it, and publishes that event asynchronously. The REST controller serves as the bridge between the synchronous HTTP world and the asynchronous Kafka world.

Here is what such a controller looks like:

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

### The end-to-end flow

The following diagram traces a request from the HTTP client through the controller, into Kafka, and back out to the consumer:

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

Notice that the HTTP response returns **immediately** with `202 Accepted` without waiting for the Kafka acknowledgment. This is an asynchronous, fire-and-forget pattern from the HTTP client's perspective. The Kafka producer handles delivery confirmation asynchronously through the `CompletableFuture` callback.

---

## 2.9 Observing the System in Action

When the application starts, several things happen in sequence that are visible in the log output. Understanding what these logs mean provides insight into how Spring Kafka initializes its components.

### Application startup

On startup, Spring Boot creates the Kafka infrastructure beans and connects to the broker. The logs typically show something like this:

```
KafkaConfig: Topic 'order-events' created with 3 partitions
o.a.k.c.p.KafkaProducer: [Producer clientId=producer-1] ... connected to node
o.s.k.l.KafkaMessageListenerContainer: order-processing-group: partitions assigned: [order-events-0, order-events-1, order-events-2]
```

The first line confirms that `KafkaAdmin` created the topic. The second shows the producer establishing a connection. The third — and most important for understanding consumers — shows the consumer group receiving its partition assignments. Since there is a single consumer instance and three partitions, that one consumer is assigned all three.

### Producing and consuming a message

When an HTTP request arrives, the producer and consumer both log their activity. A typical exchange looks like this:

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

The application logs then show **both** sides of the Kafka exchange:

```
OrderEventProducer : Publishing event: key=ORD-a1b2c3d4, topic=order-events, event=OrderEvent{...}
OrderEventProducer : Event published successfully: key=ORD-a1b2c3d4, topic=order-events, partition=1, offset=0
OrderEventConsumer : Received event: topic=order-events, partition=1, offset=0, key=ORD-a1b2c3d4, value=OrderEvent{...}
OrderEventConsumer : Processing order: orderId=ORD-a1b2c3d4, status=CREATED, product=Laptop, qty=1
```

Several things are worth noting in this output. The `partition=1` in the producer log matches the `partition=1` in the consumer log — the consumer read from the exact partition the producer wrote to. The `offset=0` indicates this was the first message written to that partition. And the `key=ORD-a1b2c3d4` appears in both logs, confirming that the message key survives the serialization/deserialization round trip.

### Observing partition distribution

Sending multiple orders with different order IDs illustrates how Kafka distributes messages across partitions:

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName":"Bob","product":"Phone","quantity":2,"totalAmount":599.99}'

curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName":"Charlie","product":"Tablet","quantity":1,"totalAmount":449.99}'
```

Each order gets a unique `orderId` as its key, so they may land in different partitions. Checking the logs reveals which partition each order was assigned to — a direct consequence of Kafka's key-based partitioning algorithm (`hash(key) % partition_count`).

---

## 2.10 Deep Dive: KafkaTemplate Internals

### How Spring Boot creates KafkaTemplate

When the application starts, Spring Boot follows a specific chain to construct the `KafkaTemplate`:

1. Spring Boot reads `spring.kafka.producer.*` properties from `application.yml`
2. It creates a `ProducerFactory<K, V>` (specifically `DefaultKafkaProducerFactory`)
3. The `ProducerFactory` holds a map of configuration properties (bootstrap servers, serializers, acks, etc.)
4. `KafkaTemplate` is constructed with this factory
5. When `send()` is called, `KafkaTemplate` gets a `KafkaProducer` from the factory and sends the record

### The configuration hierarchy

```
application.yml                   KafkaProperties                DefaultKafkaProducerFactory
spring.kafka.producer.*    →    Spring Boot reads these    →    creates producer with these configs
                                                                 │
                                                           KafkaTemplate wraps the producer
```

### Creating a custom KafkaTemplate

Sometimes `application.yml` does not provide enough control — for example, when an application needs multiple `KafkaTemplate` beans with different configurations (one for high-throughput events, another for critical financial events with different ack settings). In those cases, a Java configuration class can define the producer factory and template explicitly:

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

Spring Kafka manages the entire consumer lifecycle through listener containers. The process works as follows:

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

By default, each `@KafkaListener` creates **one** consumer thread. If the topic has 3 partitions, that single thread handles all 3 partitions.

To parallelize consumption, the `concurrency` attribute controls how many consumer threads are created:

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

For advanced listener behavior, a custom `ConcurrentKafkaListenerContainerFactory` can be defined:

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

Spring Kafka supports two approaches to configuration, and most production applications use a combination of both:

| Approach | Best For | Example |
|---|---|---|
| `application.yml` only | Simple apps, single producer/consumer | `spring.kafka.producer.acks=all` |
| Java `@Configuration` | Multiple producers/consumers with different configs | Custom `ProducerFactory` beans |
| Combination | Most production apps | Basic config in YAML, advanced overrides in Java |

### Environment-specific configuration

Spring profiles allow different Kafka settings per environment. A typical layout looks like this:

```
src/main/resources/
  ├── application.yml                  # shared defaults
  ├── application-local.yml            # local Docker Kafka
  ├── application-dev.yml              # dev cluster
  └── application-prod.yml             # production cluster
```

The shared `application.yml` contains settings that are the same everywhere:

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

Each profile file then overrides only what differs:

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

Profiles are activated at runtime:

```bash
# Local development
./mvnw spring-boot:run -Dspring-boot.run.profiles=local

# Or via environment variable
SPRING_PROFILES_ACTIVE=prod java -jar app.jar
```

### Centralizing topic names

Topic name strings should never be scattered across the codebase. One approach is a constants class that holds SpEL property references:

```java
/**
 * Centralized topic name constants.
 * Referenced from @KafkaListener, producers, and KafkaConfig.
 */
public final class KafkaTopics {
    private KafkaTopics() {} // prevent instantiation

    public static final String ORDER_EVENTS = "${app.kafka.topic.orders}";
    // Add more topics as the project grows
}
```

Usage becomes clean and consistent:

```java
@KafkaListener(topics = KafkaTopics.ORDER_EVENTS)
public void handle(ConsumerRecord<String, OrderEvent> record) { ... }
```

Another approach is a `@ConfigurationProperties` class:

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

### Why multiple groups matter

One of Kafka's most powerful patterns is allowing a single topic to serve multiple independent consumers. Each consumer group maintains its own offset tracking, so every group receives **every** message — but processes them for different purposes.

```
                Topic: order-events
              ┌──────────────────────────┐
              │  Partition 0             │
              │  Partition 1             │
              │  Partition 2             │
              └──────────┴───────────────┘
                         │
            ┌────────────┴────────────┐
            ▼                         ▼
    Group: order-processor     Group: order-auditor
    (processes the order)       (writes to audit log)
    (each message once)         (each message once)
```

### Implementation

Two separate listener classes, each with a different `groupId`, achieve this pattern:

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

**Both groups receive every message**, but process them independently. Each group tracks its own offsets, so one group falling behind or reprocessing messages does not affect the other.

### Inspecting consumer groups

Kafka's command-line tools provide visibility into consumer group state. The following commands show all registered groups and the offset position for each partition:

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

The `--describe` output shows each partition's current offset, log-end offset, and lag — the number of messages the consumer has not yet processed.

---

## 2.14 Serialization, Deserialization, and Trusted Packages

### The trusted packages problem

When using `JsonDeserializer`, Spring Kafka needs to know which Java classes are safe to instantiate from incoming JSON. By default, it trusts nothing — a security-first approach that prevents arbitrary class instantiation from untrusted Kafka messages.

If the trusted packages are not configured, the application will fail with an error like:

```
The class 'com.example.kafkalearning.dto.OrderEvent' is not in the trusted packages
```

The fix is straightforward in `application.yml`:

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.trusted.packages: "com.example.kafkalearning.dto"
```

For development-only convenience, all packages can be trusted (never do this in production):

```yaml
spring.json.trusted.packages: "*"
```

### Type mapping across services

When the producer sends a JSON message, Spring Kafka adds a `__TypeId__` header containing the fully qualified class name. The consumer uses this header to determine which class to deserialize into.

In microservice architectures, the producer and consumer often use different class names for the same event. Type mapping bridges this gap:

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

The logical name `orderEvent` acts as an alias that bridges the two different class names, allowing services to evolve their internal type names independently.

### Custom serializers (rarely needed)

In most cases, Spring's built-in `JsonSerializer`/`JsonDeserializer` are sufficient for JSON payloads. Custom serializers are only necessary when using binary formats like Avro or Protobuf, or when special serialization logic is required:

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

A well-organized package structure makes it easy to find components and understand their roles at a glance:

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

`@KafkaListener` methods should be **thin** — just like REST controllers. They receive, log, and delegate to a service layer where the actual business logic lives:

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

This separation makes business logic testable without Kafka infrastructure — a unit test can call `orderService.processOrderEvent()` directly without starting a Kafka broker.

---

## 2.16 Common Mistakes and How to Avoid Them

### Mistake 1: Not setting trusted packages

```
org.apache.kafka.common.errors.SerializationException:
Error deserializing key/value for partition order-events-0 at offset 0
```

**Why it happens**: The `JsonDeserializer` refuses to instantiate classes from packages it does not trust.

**Solution**: Set `spring.json.trusted.packages` in the consumer configuration.

### Mistake 2: Coupling REST DTOs to Kafka events

When the same class is used for both the HTTP request body and the Kafka message, adding a field to the REST API silently changes the Kafka event contract — and vice versa.

**Solution**: Maintain separate `OrderRequest` (HTTP) and `OrderEvent` (Kafka) classes. Map between them explicitly in the controller.

### Mistake 3: Ignoring send failures

```java
// ❌ Bad: fire and forget with no error handling
kafkaTemplate.send(topic, key, event);
```

**Why it matters**: If the broker is down or the topic does not exist, the message is lost silently.

**Solution**: Always attach a callback or handle the future:

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

**Why it matters**: Hardcoded strings are invisible to search tools, cannot be overridden per environment, and create duplication when the same topic name appears in producers, consumers, and configuration classes.

**Solution**: Use externalized properties:

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

When debugging Kafka issues in production, knowing the exact partition and offset of a problematic message is the difference between a five-minute fix and a multi-hour investigation.

### Mistake 6: Setting concurrency higher than partition count

```yaml
# Topic has 3 partitions
spring.kafka.listener.concurrency: 10   # 7 threads will be idle!
```

**Why it matters**: Kafka assigns at most one consumer thread per partition within a group. Extra threads are wasted resources.

**Solution**: Set `concurrency` ≤ partition count.

### Mistake 7: Using `auto-offset-reset: latest` without understanding it

With `latest`, if a consumer starts for the first time (no committed offsets), it skips all existing messages and only sees new ones. This is correct behavior in some production scenarios, but during development it often causes confusion when messages appear to "vanish." Using `earliest` during development ensures that all existing messages are consumed, making the system's behavior easier to observe and understand.

---

## 2.17 Complete Working Project

Putting all the pieces together, here is the full structure of the application discussed throughout this section.

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

### File layout

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

## 2.18 Practical Exploration

The concepts in this section become much clearer when observed in a running system. The following activities are designed to illuminate specific behaviors that are difficult to appreciate from reading alone.

### Observing the basic flow

With Kafka running and the application started, sending several orders via `curl` and watching the application logs reveals the full producer-consumer cycle. The producer logs show the topic, partition, and offset of each sent message; the consumer logs show the same metadata on the receiving end. Comparing the two confirms that messages are arriving correctly and that the serialization/deserialization round trip preserves all fields.

### Partition distribution

Sending 10 or more orders and noting which partition each one lands in demonstrates Kafka's key-based partitioning in action. Because each order gets a unique `orderId` as its key, different orders may land in different partitions. The key observation: the same `orderId` always maps to the same partition, which is how Kafka guarantees per-key ordering.

### Adding a second consumer group

Creating an `OrderAuditConsumer` with `groupId = "order-audit-group"` (as shown in section 2.13) and then sending orders demonstrates the broadcast behavior of multiple consumer groups. Both the processor and the auditor receive every message. The `kafka-consumer-groups.sh --describe` command shows each group's independent offset tracking.

### Experimenting with `auto-offset-reset`

Understanding offset reset behavior becomes concrete by observing what happens when a consumer group is deleted and recreated. If the group is deleted (via `kafka-consumer-groups.sh --delete`), new messages are produced while the consumer is offline, and the consumer restarts with `auto-offset-reset: latest`, those messages are skipped — the consumer only sees messages produced after it joined. Switching to `earliest` and repeating the experiment shows the opposite: all existing messages are consumed. This behavior is fundamental to understanding how Kafka consumers recover from downtime.

### Manual offset commits

Changing the `ack-mode` to `MANUAL` and adding an `Acknowledgment` parameter to the listener method gives direct control over when offsets are committed. The interesting observation: if `acknowledgment.acknowledge()` is not called and the application restarts, the message is redelivered — because from Kafka's perspective, it was never successfully processed. This demonstrates why offset management matters for exactly-once or at-least-once processing guarantees.

### Message headers

Headers provide a way to attach metadata to messages without modifying the message body. Sending a `ProducerRecord` with a custom `traceId` header and reading it in the consumer illustrates this pattern:

```java
ProducerRecord<String, OrderEvent> record = new ProducerRecord<>(
    topic, null, key, event,
    List.of(new RecordHeader("traceId", UUID.randomUUID().toString().getBytes()))
);
kafkaTemplate.send(record);
```

On the consumer side:

```java
@KafkaListener(topics = "${app.kafka.topic.orders}")
public void handle(ConsumerRecord<String, OrderEvent> record) {
    Header traceHeader = record.headers().lastHeader("traceId");
    String traceId = traceHeader != null ? new String(traceHeader.value()) : "unknown";
    log.info("traceId={}, orderId={}", traceId, record.value().getOrderId());
}
```

This is the foundation of distributed tracing across Kafka-connected services.

---

## 2.19 Key Takeaways

This section covered the complete anatomy of a Spring Boot application that produces and consumes Kafka messages. The core concepts worth retaining are:

The `spring-kafka` dependency triggers Spring Boot's auto-configuration, which creates `KafkaTemplate`, listener container factories, and `KafkaAdmin` — all driven by properties in `application.yml`. Topics are best declared as `NewTopic` beans so that their partition counts and replication factors are explicit, version-controlled, and automatically applied at startup.

On the producer side, `KafkaTemplate.send()` is asynchronous and returns a `CompletableFuture`. Messages are serialized, partitioned (by hashing the key), batched, and sent to the broker. With `enable.idempotence=true`, retries are safe from duplicate writes.

On the consumer side, `@KafkaListener` methods are invoked by a listener container that manages polling, deserialization, dispatch, and offset commits. The `ack-mode` setting controls when offsets are committed — `BATCH` for most applications, `MANUAL` when precise control is needed.

Consumer groups are central to Kafka's design. Consumers with the same `group-id` share the load across partitions; consumers with different group IDs each receive every message independently. Concurrency should match or be less than the partition count.

REST DTOs and Kafka event models should be separate classes because their contracts evolve independently. Trusted packages must be configured to allow the `JsonDeserializer` to instantiate event classes. Topic names should be externalized into `application.yml` and referenced via property placeholders, never hardcoded.

Finally, good operational hygiene — logging full context (topic, partition, offset, key) on both the producer and consumer sides, handling send failures explicitly, and structuring code into clear packages — makes the difference between a Kafka application that is debuggable and one that is not.

**Next**: [Section 3: Message Contract Design](section-3-message-contract-design.md) — where the focus shifts to designing events that are safe to evolve over time.
