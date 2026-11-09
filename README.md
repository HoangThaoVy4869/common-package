# @vyhoang/microservice-clean

> [!WARNING]
> **Disclaimer / Important Notice:**  
> This package is built primarily for internal projects and specific architectural requirements of the author. Updates, enhancements, and potential breaking changes strictly depend on the roadmap and needs of those internal projects. If this package no longer fits your use case, please consider looking for or building an alternative solution.

Shared backend utilities and infrastructure helpers for Node.js / NestJS microservices built with Clean Architecture.

This package is intentionally exposed through subpath imports only.

---

## Installation

Install the package directly from npm:

```bash
npm install @vyhoang/microservice-clean
```

### Peer Dependencies

Install optional peer dependencies depending on the modules your service uses:

```bash
# Core DTO Validation (Required if using validateDto() helper)
npm install class-transformer class-validator @nestjs/common reflect-metadata

# Redis Cache & Distributed Lock
npm install ioredis

# Elasticsearch Client & Document Search
npm install @elastic/elasticsearch

# gRPC Microservices
npm install @grpc/grpc-js

# Multi-language i18n
npm install nestjs-i18n
```

---

## Import Rules

Import from supported subpaths:

```ts
// Core (Entities, Value Objects, Interfaces, Errors, Validation, Response Envelopes)
import { 
  validateDto,
  ResponseHandler, 
  BusinessError, 
  NotFoundError, 
  DuplicateError, 
  ConflictError,
  UnauthorizedError, 
  ForbiddenError, 
  RequestContext, 
  REQUEST_METADATA,
  ILogger,
  ILanguageService,
  IRedisService,
  IIdempotencyRepository,
  IEventBus,
  ICommandBus,
  IMessageBus,
  BusMessageHandler,
  IElasticSearchClient,
  IElasticClient
} from "@vyhoang/microservice-clean/core";

// Messaging (RabbitMQ & Kafka as independent transports)
import { 
  RabbitMQConnection, 
  RabbitMQEventBus, 
  KafkaConnection, 
  KafkaEventBus,
  MessageEnvelope
} from "@vyhoang/microservice-clean/infrastructure/messaging";

// Redis Cache & Distributed Lock (supports both natural args & object options)
import { RedisService, BaseRedisService, createRedisClient } from "@vyhoang/microservice-clean/infrastructure/cache";

// Elasticsearch Client & Search Service
import { ElasticSearchService, createElasticClient, getSharedElasticClient } from "@vyhoang/microservice-clean/infrastructure/elasticsearch";

// Structured & Pretty Console Logging (Zero external dependencies)
import { createLogger, PinoLogger, pinoElastic } from "@vyhoang/microservice-clean/infrastructure/logger";

// Multi-language, Idempotency, Validation, Filters, Metadata
import { createI18nOptions, createI18nAsyncConfig, LanguageService } from "@vyhoang/microservice-clean/infrastructure/languages";
import { RedisIdempotencyRepository, withIdempotency } from "@vyhoang/microservice-clean/infrastructure/idempotency";
import { GrpcValidationPipe, ValidationSchema, TypedSchema } from "@vyhoang/microservice-clean/infrastructure/validation";
import { GrpcBusinessExceptionFilter } from "@vyhoang/microservice-clean/infrastructure/filters";
import { MetadataMapper } from "@vyhoang/microservice-clean/infrastructure/mapper";

// Distributed Transactional Outbox Pattern Engine
import { 
  OutboxRelay, 
  IOutboxStore, 
  ClaimedOutboxEvent, 
  OutboxRelayConfig, 
  loadOutboxRelayConfig, 
  calculateOutboxRetryDelay 
} from "@vyhoang/microservice-clean/infrastructure/outbox";

// Generic MongoDB Base Repository
import { BaseRepository, IRepositoryMapper } from "@vyhoang/microservice-clean/infrastructure/repositories";
```

> [!CAUTION]
> **Direct root import is not supported:**
> ```ts
> import { ... } from "@vyhoang/microservice-clean"; // ❌ NOT SUPPORTED
> ```

---

## Modules & Usage Guide
### 1. Messaging Architecture (`infrastructure/messaging`)

Unified messaging engine supporting **RabbitMQ** (Worker Queues & Direct Commands) and **Kafka** (High-throughput Event Streaming) as independent, enterprise-grade transports.


#### A. RabbitMQ Messaging Engine

High-reliability messaging engine implementing an enterprise **two-tier message safety net** with built-in **Alternate Exchange (AE)** for unrouted events and **Dead Letter Queue (DLQ)** retry topology.

#### Topology Architecture Diagram

```text
  [ Publisher ]
       │ publishEvent("order.created.v1")
       ▼
 ┌───────────────────────────┐   alternateExchange: 'unrouted.events.exchange'
 │ Fanout Exchange           ├────────────────────────────────────────┐
 │ (order.created.v1)        │ (when no queues are bound)             │
 └─────────────┬─────────────┘                                        ▼
               │ (bound)                                  ┌───────────────────────────┐
               ▼                                          │ unrouted.events.exchange  │
 ┌───────────────────────────┐                            └─────────────┬─────────────┘
 │ Main Consumer Queue       │                                          ▼
 │ (notification.order.q)    │                            ┌───────────────────────────┐
 └─────────────┬─────────────┘                            │ unrouted.events.q         │
               ▼                                          └───────────────────────────┘
       [ Consumer Handler ]                                (Central sink for unrouted msgs)
               │
               ├─► Success: ACK
               │
               └─► Failure (Retry < maxRetries):
                        │
                        ▼ (TTL Delay)
               ┌───────────────────────────┐
               │ notification.order.q.retry│ (x-message-ttl: 5000ms)
               └────────────┬──────────────┘
                            │ (Auto redelivery back to Main Queue)
                            ▼
               (Retry count exceeded):
                        │
                        ▼
               ┌───────────────────────────┐
               │ notification.dlq          │ (Dead Letter Queue for poison pills)
               └───────────────────────────┘
```

#### Tier 1: Publisher Routing Safety (Alternate Exchange)
- All event exchanges asserted via `publishEvent` or `subscribeEvent` automatically configure:
  ```ts
  { durable: true, alternateExchange: 'unrouted.events.exchange' }
  ```
- If a publisher emits an event and no active consumer queues are bound to the exchange, the message is automatically routed to `unrouted.events.q` rather than silently dropped.
- You can inspect or subscribe to `DEFAULT_UNROUTED_EVENTS_QUEUE` to alert or monitor unrouted/orphaned domain events across the cluster.

#### Tier 2: Consumer Execution Safety (Retry Loop & DLQ)
When subscribing to events or handling commands, provide `queueName` and a `retry` policy to activate durable retry backoff:
- **Retry Queue**: Automatically created as `<queueName>.retry` with `x-message-ttl`. After expiry, messages route back to `<queueName>` for re-execution.
- **Dead Letter Queue**: After `maxRetries` attempts fail, messages are moved to the specified `deadLetterQueue` (or `<queueName>.dlq`) with header `x-event-bus-retry-count` preserved.

#### Connection Ownership & Lifecycle (RabbitMQ)
Create one long-lived `RabbitMQConnection` per service process and inject it into `RabbitMQEventBus`:

```ts
import {
  RabbitMQConnection,
  RabbitMQEventBus,
  DEFAULT_ALTERNATE_EXCHANGE,
  DEFAULT_UNROUTED_EVENTS_QUEUE
} from "@vyhoang/microservice-clean/infrastructure/messaging";

const connection = new RabbitMQConnection({
  urls: [process.env.RABBITMQ_URL || "amqp://localhost:5672"],
  connectTimeoutMs: 30_000,
  publishTimeoutMs: 15_000,
  alternateExchange: {
    enabled: true,
    exchangeName: DEFAULT_ALTERNATE_EXCHANGE,
    queueName: DEFAULT_UNROUTED_EVENTS_QUEUE,
  },
});

const rabbitBus = new RabbitMQEventBus(connection);
await connection.connect();

// Publish fan-out event:
await rabbitBus.publishEvent(
  "order.created.v1", 
  { orderId: '123', total: 500000 },
  {
    headers: { 'traceparent': '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' }
  }
);

// Send point-to-point command:
await rabbitBus.sendCommand("payment.refund.q", { paymentId: "pay-123", amount: 100000 });

// Subscribe to event with Retry Backoff & DLQ:
await rabbitBus.subscribeEvent(
  "order.created.v1",
  async (payload, rawMessage, envelope) => {
    // Business logic
  },
  {
    queueName: "notification.order.q",
    prefetch: 10,
    retry: {
      maxRetries: 3,
      retryDelayMs: 5000,
      deadLetterQueue: "notification.dlq"
    }
  }
);

// Graceful shutdown:
await rabbitBus.close();
await connection.close();
```

---

#### B. Kafka Messaging Architecture

Kafka is the **integration-event backbone** between services, built for event replay, long retention, per-partition ordering, and independent consumer groups.

```ts
import { KafkaConnection, KafkaEventBus } from "@vyhoang/microservice-clean/infrastructure/messaging";

const kafka = new KafkaConnection({
  clientId: process.env.KAFKA_CLIENT_ID || "order-service",
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
  allowAutoTopicCreation: false,
  // Native librdkafka controls. Idempotence is enabled by default by this package.
  requestTimeoutMs: 30_000,
  // Cluster & provisioning defaults (configurable via options or env vars)
  defaultTopicReplication: 1,      // or via KAFKA_DEFAULT_REPLICATION_FACTOR
  defaultMinInSyncReplicas: 1,     // or via KAFKA_DEFAULT_MIN_INSYNC_REPLICAS
  defaultNumPartitions: 4,         // or via KAFKA_DEFAULT_NUM_PARTITIONS
  producerConfig: {
    "compression.codec": "zstd",  // or via KAFKA_PRODUCER_COMPRESSION
    "linger.ms": 5,               // or via KAFKA_PRODUCER_LINGER_MS
    "delivery.timeout.ms": 120_000,
  },
  librdkafkaConfig: {
    "socket.keepalive.enable": true,
  },
  topics: [
    { topic: "order.events",   numPartitions: 4, replicationFactor: 1 },
    { topic: "payment.events", numPartitions: 4, replicationFactor: 1 },
  ],
});
await kafka.connect();

const kafkaBus = new KafkaEventBus(kafka, {
  consumer: {
    // Cooperative-sticky rebalance prevents stop-the-world cluster rebalances
    partitionAssignmentStrategy: "cooperative-sticky",
    maxPollIntervalMs: 300_000, // 5 mins heartbeat tolerance
  }
});
```

`topics` is convenient for local development. In production, provision both
event topics and their `-dlq` topics through IaC, then omit `topics` here so
the application identity does not require Kafka Admin/CreateTopics permission.
The package uses native `librdkafka` configuration: pass cross-client settings
via `librdkafkaConfig` and producer-only settings via `producerConfig`.
For local-only DLQ creation, set `consumer: { dlq: { provision: true } }` on
`KafkaEventBus`; production consumers expect their DLQ topic to exist already.

#### C. Independent Messaging Usage Pattern

When a service (e.g. Notification Service) consumes events from Kafka and dispatches background worker jobs to RabbitMQ, call both transports independently without artificial coupling:

| Transport | Channel | Use Case |
|---|---|---|
| **Kafka (`KafkaEventBus`)** | Topics (`order.events`, `payment.events`) | Fan-out domain events, audit logs, event streaming |
| **RabbitMQ (`RabbitMQEventBus`)** | Direct Queues (`email.q`, `payment.refund.q`) | Point-to-point jobs, async worker tasks with individual ACK/NACK |

```ts
import { 
  KafkaConnection, 
  KafkaEventBus, 
  RabbitMQConnection, 
  RabbitMQEventBus 
} from "@vyhoang/microservice-clean/infrastructure/messaging";

// 1. Initialize Kafka for Domain Events
const kafka = new KafkaConnection({ clientId: "noti-service", brokers: ["localhost:9092"] });
await kafka.connect();
const kafkaBus = new KafkaEventBus(kafka);

// 2. Initialize RabbitMQ for Worker Queues
const rabbit = new RabbitMQConnection({ url: "amqp://localhost" });
await rabbit.connect();
const rabbitBus = new RabbitMQEventBus(rabbit);

// Consume event from Kafka, then dispatch worker task to RabbitMQ
await kafkaBus.subscribeEvent("order.created.v1", async (order: any) => {
  await rabbitBus.sendCommand("email.dispatch.q", { to: order.customerEmail, template: "welcome" });
});

// Graceful shutdown
await kafkaBus.close();
await rabbitBus.close();
await kafka.close();
await rabbit.close();
```

#### Kafka Publish & Subscribe

```ts
// Producer-side (e.g. order outbox relay):
await kafkaBus.publishEvent("order.created.v1", {
  orderId: "ord-123",
  aggregateId: "ord-123",   // <- partition key; events of same aggregate stay ordered
  total: 500000,
});

// Consumer-side (e.g. notification service):
await kafkaBus.subscribeEvent(
  "order.status-changed.v1",
  async (payload, rawMessage, envelope) => {
    // payload.orderId, payload.status ...
  },
  {
    queueName: "notification-order",       // consumer group id (durable offset)
    partitionAssignmentStrategy: "cooperative-sticky", // default
    maxPollIntervalMs: 300_000,              // 5 minutes
    retry: { maxRetries: 3, retryDelayMs: 5000 },
  }
);

// Graceful shutdown:
await kafkaBus.close();
await kafka.close();
```

#### Topic Naming Convention

`eventName` → `topic` mapping is provided by a `KafkaTopicResolver`. The default
resolver maps the first segment of the dotted event name to `<segment>.events`:

| Event name | Topic |
|------------|-------|
| `order.created.v1` | `order.events` |
| `order.status-changed.v1` | `order.events` |
| `payment.failed.v1` | `payment.events` |
| `email-verification.requested.v1` | `account.events` |

Provide a custom resolver in `KafkaEventBusOptions.topics.resolveTopic` when you
need finer-grained topics.

#### Partition Key & Ordering

The producer derives the partition key from `payload.aggregateId ?? payload.id`
(or a custom `partitionKey` resolver). Events of the same aggregate land in the
same partition → Kafka preserves their order per partition. Use this for event
sourcing (e.g. replay all `order.created` of one order in order).

#### Retry, Poison Pills & Dead-Letter (DLQ)

The consumer commits only after a successful handler invocation.
- **Poison Pills & Malformed Payloads**: If a message cannot be deserialized (e.g., malformed non-JSON data produced by an untyped producer), the consumer preserves the raw string payload along with error details and **immediately routes it to `<topic>-dlq`**, committing the offset to prevent partition blockage.
- **Permanent Business Errors** (e.g. schema/validation failure): Immediately forwarded to `<topic>-dlq` and the offset is committed to keep the partition moving.
- **Transient Infrastructure Errors** (e.g. database timeout, network glitch): Pauses the affected partition, seeks back to the failed offset, and retries after `retryDelayMs`. After exceeding `maxRetries`, the record is produced to `<topic>-dlq` and source offset is committed.

#### Consumer Rebalance & Heartbeat Resilience
- **Cooperative Sticky Strategy**: Consumer groups default to `cooperative-sticky` (incremental cooperative rebalance), preventing "stop-the-world" partition reassignments when consumers scale up/down.
- **Configurable `max.poll.interval.ms`**: Defaults to 300,000ms (5 minutes) so long-running event processing batches won't trigger premature consumer group evictions.

#### Replay / Backfill

Kafka stores events within topic retention limits. To replay events:
1. **Dedicated consumer group**: Start a new consumer group with `fromBeginning: true` (and bypass or clear the deduplication inbox):
   ```ts
   const syncBus = new KafkaEventBus(connection, {
     consumer: { groupId: "backfill-group-v2", fromBeginning: true }
   });
   ```
2. **Kafka CLI**: Reset offsets directly for an existing group:
   ```bash
   kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group order-group --topic order.events --reset-offsets --to-earliest --execute
   ```

#### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated list of Kafka broker endpoints |
| `KAFKA_CLIENT_ID` | Service client ID | Identifier for the Kafka client instance |
| `KAFKA_DEFAULT_NUM_PARTITIONS` | `1` | Default partitions when auto-creating topics |
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | `1` | Default replication factor (e.g. 3 in production cluster) |
| `KAFKA_DEFAULT_MIN_INSYNC_REPLICAS` | `1` | Default min.insync.replicas (e.g. 2 in production cluster) |
| `KAFKA_PRODUCER_COMPRESSION` | `zstd` / `lz4` | Producer compression algorithm (`lz4`, `zstd`, `snappy`, `gzip`) |
| `KAFKA_PRODUCER_LINGER_MS` | `5` | Micro-batch linger time in milliseconds for higher throughput |

---



### 2. Redis Cache & Distributed Lock (`infrastructure/cache`)

Supports connection URL (`REDIS_URI` / `redis://...`) or granular host/port/credentials, automatic JSON serialization, atomic distributed locking, hashes, sets, and lists.

> [!TIP]
> **Dual-Signature Support**: `RedisService` supports both **clean direct parameters** and **object-based options** for 100% backward compatibility.

```ts
import { RedisService, BaseRedisService, createRedisClient } from "@vyhoang/microservice-clean/infrastructure/cache";

// Initialize (automatically reads REDIS_URI or REDIS_HOST / REDIS_PORT / REDIS_PASSWORD from env)
const redis = new RedisService();

// --- 1. Natural Direct Arguments (Recommended) ---
// Key-Value with auto JSON serialization & TTL in seconds:
await redis.set('user:100', { id: 100, name: 'Alice' }, 3600);
const user = await redis.get<{ id: number; name: string }>('user:100');
await redis.del('user:100');

// Safe Distributed Lock (SET NX EX + Atomic Lua Script Release):
const acquired = await redis.acquireLock('lock:order:123', 30, 'worker-1');
if (acquired) {
  try {
    // Process exclusive critical section...
  } finally {
    await redis.releaseLock('lock:order:123', 'worker-1');
  }
}

// Hashes, Lists, Sets:
await redis.hset('profile:100', { age: 25, role: 'admin' });
const profile = await redis.hgetall('profile:100');
await redis.rpush('queue:tasks', { taskId: 'task-1' });
const nextTask = await redis.lpop('queue:tasks');
await redis.sadd('tags:post:1', 'typescript', 'microservices');
const tags = await redis.smembers('tags:post:1');

// --- 2. Legacy Object Syntax (Fully Backward Compatible) ---
await redis.set({ key: 'user:100', value: { id: 100, name: 'Alice' }, ttl: 3600 });
const legacyUser = await redis.get({ key: 'user:100' });
await redis.acquireLock({ key: 'lock:order:123', ttlSeconds: 30, lockValue: 'worker-1' });
await redis.releaseLock({ key: 'lock:order:123', lockValue: 'worker-1' });
```

---

### 3. Multi-language i18n (`infrastructure/languages`)

Streamlined configuration for `nestjs-i18n`, eliminating boilerplate:

```ts
// config/i18n.config.ts
import * as path from 'path';
import { createI18nAsyncConfig } from '@vyhoang/microservice-clean/infrastructure/languages';

export const i18nConfig = createI18nAsyncConfig({
    localesPath: path.join(process.cwd(), 'locales'),
    fallbackLanguage: 'en'
});

// app.module.ts
import { I18nModule } from 'nestjs-i18n';
import { i18nConfig } from './config/i18n.config';

@Module({
    imports: [
        I18nModule.forRootAsync(i18nConfig.i18n)
    ]
})
export class AppModule {}
```

`LanguageService` adapter implementing `ILanguageService`:
```ts
import { LanguageService } from '@vyhoang/microservice-clean/infrastructure/languages';
import { I18nService } from 'nestjs-i18n';

// Inject I18nService from NestJS:
const languageService = new LanguageService(i18n);
const text = await languageService.translate('messages.welcome', { lang: 'vi', fallback: 'Welcome' });
```

---

### 4. Idempotency Helper (`infrastructure/idempotency`)

Prevents duplicate message handling across RabbitMQ consumers, event subscribers, and background workers:

```ts
import { 
  RedisIdempotencyRepository, 
  withIdempotency 
} from '@vyhoang/microservice-clean/infrastructure/idempotency';
import { RedisService } from '@vyhoang/microservice-clean/infrastructure/cache';

const redis = new RedisService();
const idempotencyRepo = new RedisIdempotencyRepository(redis, {
  keyPrefix: 'idempotency:notification:',
  defaultTtlSeconds: 86400 // Auto expires after 24h
});

// Inside message consumer:
await withIdempotency(
  idempotencyRepo,
  {
    key: event.idempotencyKey,
    ownerId: event.messageId,
    releaseOnError: true // Releases claim on failure so retries can re-process
  },
  async () => {
    // Deliver notification / execute usecase
    await sendNotification(event);
  }
);
```

---

### 5. Input Validation (`infrastructure/validation` & `core`)

Clean Architecture dictates that **Boundary / Structural Validation** belongs strictly to the **Inbound Infrastructure layer** (Controllers, Gateway handlers, Message consumers) *before* passing execution to Use Cases. This ensures Use Cases focus 100% on domain orchestration and business logic without defensive boilerplate checks.

This package provides **two specialized validation approaches** tailored to your service architecture:

---

#### Approach A: Schema-Based Validation for gRPC Microservices (`GrpcValidationPipe`)
> **Best for:** Internal gRPC Microservices, Protobuf contracts, high-throughput systems.  
> **Key benefits:** Zero duplicate class definitions, runs on raw Protobuf payloads, ultra-fast plain JS execution, seamless integration with `GrpcBusinessExceptionFilter`.

##### Supported Schema Rules (`FieldRule`)
| Rule | Type | Description |
| :--- | :--- | :--- |
| `required` | `boolean` | Field must not be `undefined`, `null`, or empty string `""` |
| `type` | `'string' | 'number' | 'boolean' | 'array' | 'object'` | Validates primitive type |
| `trim` | `boolean` | Trims whitespace before checking emptiness (strings) |
| `minLength` / `maxLength` | `number` | String length boundaries |
| `pattern` | `RegExp` | Regular expression validation |
| `enum` | `(string | number)[]` | Whitelist allowed values |
| `integer` | `boolean` | Requires safe integer (`Number.isSafeInteger`) |
| `positive` | `boolean` | Requires numeric value > 0 |
| `nonNegative` | `boolean` | Requires numeric value >= 0 |
| `min` / `max` | `number` | Numeric value boundaries |
| `minItems` / `maxItems` | `number` | Array length boundaries |
| `uniqueBy` | `string` | Rejects duplicate property values in array items (e.g. `'variantId'`) |
| `itemSchema` | `ValidationSchema` | Validates every object element inside an array |
| `fields` | `ValidationSchema` | Validates nested single object properties |
| `custom` | `(value, root) => string | boolean` | Custom function (return string for custom error message) |

##### Usage Example (gRPC Service)

> [!TIP]
> **Type Safety with `TypedSchema<T>`:**
> Using `TypedSchema<MyDTO>` bridges compile-time safety and runtime validation without leaking framework code into your Application layer:
> - **Auto-complete**: IDE automatically suggests valid field names from your DTO.
> - **Typo Prevention**: Mistyped property names trigger immediate compile errors.
> - **Refactor-Safe**: Modifying a DTO field immediately alerts you if the schema is out of sync.

```ts
// 1. Define method validation schemas using TypedSchema<T> (tied to Application DTO):
import { TypedSchema, ValidationSchema } from '@vyhoang/microservice-clean/infrastructure/validation';
import { InventoryOperationInputDTO } from '@product/application/dto';

// TypedSchema<T> provides full auto-completion and prevents typo mismatches with your UseCase DTO:
export const inventoryOperationSchema: TypedSchema<InventoryOperationInputDTO> = {
  orderId: { required: true, type: 'string', trim: true, minLength: 1, message: 'orderId is required' },
  items: {
    required: true,
    type: 'array',
    minItems: 1,
    uniqueBy: 'variantId', // Rejects duplicate variantId within the request
    message: 'At least one item is required',
    itemSchema: {
      variantId: { required: true, type: 'string', minLength: 1, message: 'variantId is required' },
      quantity: { required: true, type: 'number', integer: true, positive: true, message: 'quantity must be a positive integer' }
    }
  }
};

export const grpcMethodSchemas: Record<string, ValidationSchema> = {
  confirmInventory: inventoryOperationSchema,
  reserveInventory: inventoryOperationSchema,
  releaseInventory: inventoryOperationSchema
};
```

```ts
// 2. Subclass or inject GrpcValidationPipe:
import { Injectable } from '@nestjs/common';
import { GrpcValidationPipe as BaseGrpcValidationPipe } from '@vyhoang/microservice-clean/infrastructure/validation';
import { grpcMethodSchemas } from './grpc.validator';

@Injectable()
export class GrpcValidationPipe extends BaseGrpcValidationPipe {
  constructor() {
    super(grpcMethodSchemas);
  }
}
```

```ts
// 3. Validate at the Inbound gRPC Controller before invoking the UseCase:
@UseFilters(new GrpcBusinessExceptionFilter(mapToGrpcStatus))
@Controller()
export class InventoryGrpcController {
  constructor(
    private readonly confirmInventoryUseCase: ConfirmInventoryUseCase,
    private readonly validationPipe: GrpcValidationPipe
  ) {}

  @GrpcMethod('InventoryService', 'ConfirmInventory')
  async confirmInventory(data: InventoryRequest): Promise<InventoryResponse> {
    // Fail-fast at the boundary. Throws BusinessError(4000) mapped to status.INVALID_ARGUMENT:
    await this.validationPipe.validate('confirmInventory', data);

    // Map to Application DTO and invoke clean Use Case:
    const dto = InventoryGrpcMapper.toOperationDTO(data);
    return InventoryGrpcMapper.toResponse(await this.confirmInventoryUseCase.execute(dto));
  }
}
```

---

#### Approach B: Class-Based Validation for HTTP REST APIs / API Gateways (`validateDto`)
> **Best for:** HTTP REST Controllers, API Gateways, Swagger/OpenAPI documentation, or Message Consumers using `class-validator`.  
> **Key benefits:** Seamless integration with NestJS Swagger, auto-casts types via `class-transformer`, strict whitelist protection.

##### Clean Architecture Note
Execute `validateDto()` at the **Inbound Controller / Gateway boundary**, NEVER inside Use Cases. Use Cases should receive clean, strongly-typed interfaces or domain value objects.

##### Usage Example (HTTP Controller / Gateway)

```ts
// 1. Define DTO class with class-validator decorators:
import { IsString, IsEmail, MinLength, IsOptional, IsInt, Min } from 'class-validator';

export class CreateAccountDto {
  @IsEmail({}, { message: 'Invalid email address' })
  email!: string;

  @IsString()
  @MinLength(6, { message: 'Password must have at least 6 characters' })
  password!: string;

  @IsOptional()
  @IsInt()
  @Min(18, { message: 'Age must be 18 or older' })
  age?: number;
}
```

```ts
// 2. Validate at HTTP Controller / Consumer boundary before calling UseCase:
import { Controller, Post, Body } from '@nestjs/common';
import { validateDto } from '@vyhoang/microservice-clean/infrastructure/validation';

@Controller('accounts')
export class AccountController {
  constructor(private readonly createAccountUseCase: CreateAccountUseCase) {}

  @Post()
  async createAccount(@Body() rawPayload: unknown) {
    // Validates, whitelists, and transforms into typed instance (throws BadRequestException by default):
    const dto = await validateDto(CreateAccountDto, rawPayload);

    // Pass validated DTO to Use Case:
    return this.createAccountUseCase.execute(dto);
  }
}
```

##### Custom Error Factory (for gRPC or Custom Exceptions)
You can customize the error thrown by `validateDto` via the `errorFactory` option:

```ts
import { validateDto } from '@vyhoang/microservice-clean/infrastructure/validation';
import { BusinessError } from '@vyhoang/microservice-clean/core';

const dto = await validateDto(CreateAccountDto, rawPayload, {
  errorFactory: (errors, className) => new BusinessError(4000, `Validation failed: ${className}`, { errors })
});
```

---

### 6. gRPC Business Exception Filter (`infrastructure/filters`)

Translates `BusinessError` into standard gRPC status codes and attaches `Metadata ('error-code')`:

```ts
import { GrpcBusinessExceptionFilter } from '@vyhoang/microservice-clean/infrastructure/filters';

// Optional custom status code mapper:
app.useGlobalFilters(new GrpcBusinessExceptionFilter((code) => {
  if (code === 4000) return 3; // status.INVALID_ARGUMENT
  if (code === 4040) return 5; // status.NOT_FOUND
  return 13; // status.INTERNAL
}));
```

---

### 7. Distributed Request Context & Metadata (`core` & `infrastructure/mapper`)

Bi-directional conversion between gRPC `Metadata` and `RequestContext`:

```ts
import { RequestContext } from "@vyhoang/microservice-clean/core";
import { MetadataMapper } from "@vyhoang/microservice-clean/infrastructure/mapper";

// Parse context from incoming gRPC metadata:
const context = MetadataMapper.toRequestContext(grpcMetadata);
console.log(context.requestId, context.userId, context.roles, context.language);

// Serialize outbound metadata for downstream service calls:
const outboundMetadata = MetadataMapper.fromRequestContext(context);
```

---

### 8. Core Errors (`core`)

Standard domain errors extending `BusinessError`:

```ts
import { 
  BusinessError, 
  NotFoundError, 
  DuplicateError, 
  ConflictError, 
  UnauthorizedError, 
  ForbiddenError 
} from '@vyhoang/microservice-clean/core';

throw new NotFoundError('Account not found', 4040);
throw new UnauthorizedError('Session expired', 4010);
throw new DuplicateError('Email already registered', 4090);
```

---

### 9. Structured & Colorized Logger (`infrastructure/logger`)

High-performance, zero-dependency logging engine designed specifically for microservices. Fully implements domain `ILogger` port with zero external runtime dependencies on Pino:

- **Beautiful Development Output**: In non-production environments (`NODE_ENV !== 'production'`), logs are colorized, formatted with millisecond timestamps, service name tags, correlation/trace IDs, and colorized JSON inspector.
- **Production Structured JSON**: In production (`NODE_ENV === 'production'`), emits high-throughput, NDJSON-compliant single-line JSON records.
- **Contextual Tracing**: Built-in `.child({ correlationId, userId })` method scopes request or transaction metadata down asynchronous call chains.
- **Optional Direct Elasticsearch Streaming**: Pass `elastic: { node: '...' }` to ship logs directly to Elasticsearch in the background with automatic batching, retry resilience, and ECS `@timestamp` compliance.
- **Pino-Compatible**: Can wrap an existing external Pino instance via `new PinoLogger(externalPinoInstance)` if your service already has a custom Pino pipeline.

#### Basic Usage (`createLogger`)

```ts
import { createLogger, PinoLogger, pinoElastic } from '@vyhoang/microservice-clean/infrastructure/logger';
import { ILogger } from '@vyhoang/microservice-clean/core';

// 1. Initialize logger for your microservice:
export const logger: ILogger = createLogger({
  serviceName: 'order-service',
  level: 'info',
  // Optional background Elasticsearch log shipping:
  elastic: process.env.ELASTICSEARCH_NODE ? {
    node: process.env.ELASTICSEARCH_NODE,
    auth: {
      username: process.env.ELASTIC_USER || 'elastic',
      password: process.env.ELASTIC_PASSWORD || 'changeme'
    },
    index: 'order-service-logs-%{DATE}', // Daily rotating index: order-service-logs-2026-10-16
    flushBytes: 1000,
    flushInterval: 5000
  } : undefined
});

// 2. Structured log messages with arbitrary metadata:
logger.info('Order checkout initialized', { 
  orderId: 'ord-999', 
  userId: 'usr-123', 
  total: 450000 
});

// 3. Error logging with automatic stack trace extraction:
logger.error(new Error('Payment gateway timeout'), { 
  orderId: 'ord-999', 
  provider: 'VNPAY' 
});

// 4. Scoped Child Logger for Request / Trace Tracking:
const reqLogger = (logger as PinoLogger).child({
  correlationId: 'req-4bf92f3577b3',
  userId: 'usr-123'
});
reqLogger.info('Processing payment verification');
// Dev Terminal output:
// 10:56:45.123 [INFO ] [order-service] (trace:req-4bf9) Processing payment verification
```

---

### 10. Elasticsearch Search & Document Service (`infrastructure/elasticsearch`)

Enterprise document indexing, full-text searching, and management client implementing the domain `IElasticSearchClient` interface:

> [!NOTE]
> **Peer Dependency**: Requires `@elastic/elasticsearch` (`npm install @elastic/elasticsearch`).

- **Clean Architecture Compliant**: Encapsulates Elasticsearch client specifics within the infrastructure layer; Use Cases interact strictly with `IElasticSearchClient`.
- **Automatic Fallback Configuration**: Reads `ELASTICSEARCH_NODE` / `ELASTICSEARCH_NODES` and credentials directly from environment variables if not provided explicitly.
- **Type-Safe Search & Retrieval**: Generics-enabled `search<T>()` and `get<T>()` return typed document payloads without manual casting.
- **Fail-Safe Missing Document Handling**: `get()` and `delete()` return `null` / `false` when a document does not exist instead of throwing 404 HTTP errors.

#### Usage Example

```ts
import { 
  ElasticSearchService, 
  createElasticClient, 
  getSharedElasticClient 
} from '@vyhoang/microservice-clean/infrastructure/elasticsearch';
import { IElasticSearchClient } from '@vyhoang/microservice-clean/core';

// 1. Initialize service (reads ELASTICSEARCH_NODE from env or accepts config options):
const esService: IElasticSearchClient = new ElasticSearchService({
  node: process.env.ELASTICSEARCH_NODE || 'http://localhost:9200',
  auth: {
    username: process.env.ELASTIC_USER || 'elastic',
    password: process.env.ELASTIC_PASSWORD || 'changeme'
  }
});

// Check cluster health:
const isAlive = await esService.ping();

// 2. Index / Overwrite a Document:
interface ProductDocument {
  id: string;
  name: string;
  category: string;
  price: number;
}

await esService.index<ProductDocument>({
  index: 'products',
  id: 'prod-100',
  document: {
    id: 'prod-100',
    name: 'Wireless Mechanical Keyboard',
    category: 'electronics',
    price: 1500000
  }
});

// 3. Retrieve a Document by ID (returns null if not found):
const product = await esService.get<ProductDocument>({
  index: 'products',
  id: 'prod-100'
});

// 4. Search Documents:
const results = await esService.search<ProductDocument>({
  index: 'products',
  query: {
    match: { category: 'electronics' }
  },
  size: 10,
  from: 0
});

// 5. Partial Update:
await esService.update<ProductDocument>({
  index: 'products',
  id: 'prod-100',
  doc: { price: 1350000 }
});

// 6. Count and Delete:
const totalCount = await esService.count({
  index: 'products',
  query: { match_all: {} }
});
const deleted = await esService.delete({ index: 'products', id: 'prod-100' });
```

---

### 11. Transactional Outbox Pattern (`infrastructure/outbox`)

High-reliability, framework-agnostic **Transactional Outbox Relay Engine** providing at-least-once event delivery semantics across microservices.

#### Why Outbox Pattern?
In distributed microservices, dual-writing to a local database and a message broker (RabbitMQ/Kafka) without distributed transactions inevitably causes inconsistency if the broker or network fails after the database commit. The Outbox Pattern guarantees consistency by saving events to an Outbox table in the **same database transaction** as the business entity, then asynchronously relaying them to the broker.

#### Architecture & Safety Guarantees

```text
  [ Use Case ]
       │
       ▼ (1. Atomic DB Transaction)
  ┌─────────────────────────────────────────────────────────┐
  │ Local Database (PostgreSQL / MongoDB)                   │
  │   - Business Entities                                   │
  │   - Outbox Table (status: PENDING, next_attempt_at)     │
  └────────────────────────────┬────────────────────────────┘
                               │
                               │ (2. claimBatch with lease & FOR UPDATE SKIP LOCKED)
                               ▼
  ┌─────────────────────────────────────────────────────────┐
  │ OutboxRelay Engine (Single or Multiple Replicas)        │
  │   - Lease Concurrency & Renewal                         │
  │   - Exponential Backoff + Jitter                        │
  │   - Deduplication (messageId: event.id)                 │
  └─────────────┬───────────────────────────┬───────────────┘
                │                           │
  (3. Publish)  ▼                           ▼ (4. Mark Status)
  ┌───────────────────────────┐   ┌─────────────────────────┐
  │ Message Bus               │   │ Local Database          │
  │ (Kafka / RabbitMQ)        │   │ - markPublished()       │
  │                           │   │ - markFailed()          │
  └───────────────────────────┘   └─────────────────────────┘
```

- **Distributed Lease-Based Concurrency**: Multiple relay instances can run simultaneously across cluster replicas. Each instance generates a unique `instanceId` (`hostname:pid:uuid`) and acquires an atomic lease on a batch (`claimBatch`).
- **Pessimistic Locking & Skip Locked**: On PostgreSQL, uses `FOR UPDATE SKIP LOCKED` for zero-contention concurrent worker polling. On MongoDB, uses atomic `findOneAndUpdate` with optimistic lease checks.
- **Dynamic Lease Renewal**: Automatically renews expiring leases (`renewClaims`) when the remaining time is below `leaseRenewThresholdMs`, preventing other workers from stealing long-running publish jobs.
- **Loss Detection & Safety**: If a lease is lost (e.g. prolonged network partition), the relay detects this and aborts status updates to prevent split-brain overwrites.
- **Exponential Backoff with Jitter**: Failed publish attempts are rescheduled with exponential backoff and up to 20% random jitter (`calculateOutboxRetryDelay`), preventing thundering herds.
- **Deduplication Message ID**: Each published event carries `messageId: event.id` in options/headers, enabling consumers with `withIdempotency` to safely deduplicate redeliveries.
- **Graceful Shutdown**: On application termination, the relay stops accepting new batches, waits up to `shutdownGraceMs` for active jobs, and releases unfinished claims (`releaseClaims`) back to `PENDING` for other instances to pick up immediately.

#### Core Interfaces

```ts
export interface IOutboxStore {
  claimBatch(claimedBy: string, batchSize: number, leaseMs: number): Promise<ClaimedOutboxEvent[]>;
  renewClaims(ids: string[], claimedBy: string, leaseMs: number): Promise<Set<string>>;
  markPublished(id: string, claimedBy: string): Promise<boolean>;
  markFailed(id: string, claimedBy: string, nextAttemptAt: Date, error: string): Promise<boolean>;
  releaseClaims(ids: string[], claimedBy: string): Promise<number | void>;
}
```

#### Usage Example (NestJS)

```ts
import { 
  OutboxRelay, 
  IOutboxStore, 
  loadOutboxRelayConfig 
} from "@vyhoang/microservice-clean/infrastructure/outbox";
import { IMessageBus, ILogger } from "@vyhoang/microservice-clean/core";

@Injectable()
export class OrderOutboxRelay implements OnModuleInit, BeforeApplicationShutdown {
  private readonly relay: OutboxRelay;

  constructor(
    private readonly outboxRepository: IOrderOutboxRepository, // implements IOutboxStore
    private readonly eventBus: IMessageBus,
    private readonly logger: ILogger,
    config: ConfigService
  ) {
    this.relay = new OutboxRelay(this.outboxRepository, this.eventBus, this.logger, {
      name: 'OrderOutboxRelay',
      config: loadOutboxRelayConfig((key) => config.get(key))
    });
  }

  onModuleInit(): void {
    this.relay.start();
  }

  async beforeApplicationShutdown(): Promise<void> {
    await this.relay.stop();
  }

  /** Run single batch on demand (useful for testing) */
  async runOnce(): Promise<void> {
    await this.relay.runOnce();
  }
}
```

#### Environment Variables Configuration

| Variable | Default | Description |
|---|---|---|
| `OUTBOX_POLL_INTERVAL_MS` | `1000` | Polling loop interval in milliseconds |
| `OUTBOX_BATCH_SIZE` | `50` | Maximum number of events claimed per batch (capped at 1000) |
| `OUTBOX_LEASE_MS` | `60000` | Lease duration in milliseconds for claimed events |
| `OUTBOX_LEASE_RENEW_THRESHOLD_MS` | `15000` | Remaining time threshold to trigger batch lease renewal |
| `OUTBOX_RETRY_BASE_MS` | `1000` | Base delay for exponential backoff retry |
| `OUTBOX_RETRY_MAX_MS` | `300000` | Maximum cap for retry delay (5 minutes) |
| `OUTBOX_SHUTDOWN_GRACE_MS` | `30000` | Maximum wait time during graceful shutdown |

---

### 12. Generic MongoDB Base Repository (`infrastructure/repositories`)

Standard Clean Architecture base repository for MongoDB / Mongoose collections, encapsulating domain-to-persistence mapping and common query operations:

```ts
import { BaseRepository, IRepositoryMapper } from "@vyhoang/microservice-clean/infrastructure/repositories";
import { Model } from "mongoose";

export class MongoAccountRepository extends BaseRepository<Account, AccountDocument> {
  constructor(model: Model<AccountDocument>, mapper: IRepositoryMapper<Account, AccountDocument>) {
    super(model, mapper);
  }

  // Pre-built CRUD & query helpers out of the box:
  // - findById(id)
  // - create(entity)
  // - update(filter, entity)
  // - save(entity)
  // - deleteById(id)
  // - findOneByFilter(filter, options)
  // - findManyByFilter(filter, options)
  // - updateOneByFilter(filter, fields)
  // - updateManyByFilter(filter, fields)
  // - countByFilter(filter)
  // - existsByFilter(filter)
  // - deleteOneByFilter(filter)
}
```

---

## Build Package

```bash
npm run clean
npm run build
```

## Requirements

- Node.js `>=20`
- TypeScript `>=5.0`

## License

ISC

