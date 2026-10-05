# @vyhoang/microservice-clean

> [!WARNING]
> **Disclaimer / Important Notice:**  
> This package is built primarily for internal projects and specific architectural requirements of the author. Updates, enhancements, and potential breaking changes strictly depend on the roadmap and needs of those internal projects. If this package no longer fits your use case, please consider looking for or building an alternative solution.

Shared backend utilities and infrastructure helpers for Node.js / NestJS microservices built with Clean Architecture.

This package is intentionally exposed through subpath imports only.

---

## Installation

```bash
npm install @vyhoang/microservice-clean
```

### Peer Dependencies

Install optional peer dependencies depending on the modules you use:

```bash
# Core & Validation (recommended for NestJS services)
npm install @nestjs/common class-validator class-transformer reflect-metadata

# Redis Cache & Distributed Lock
npm install ioredis

# gRPC Microservices
npm install @grpc/grpc-js

# Multi-language i18n
npm install nestjs-i18n
```

---

## Import Rules

Import from supported subpaths:

```ts
// Core (Entities, Value Objects, Interfaces, Errors, Response Envelopes)
import { 
  ResponseHandler, 
  BusinessError, 
  NotFoundError, 
  DuplicateError, 
  UnauthorizedError, 
  ForbiddenError, 
  RequestContext, 
  REQUEST_METADATA,
  ILanguageService,
  IRedisService,
  IIdempotencyRepository
} from "@vyhoang/microservice-clean/core";

// Infrastructure subpaths (Recommended to import specific domains for minimal bundle footprint)
import { RabbitMQConnection, RabbitMQEventBus } from "@vyhoang/microservice-clean/infrastructure/queue";
import { RedisService, BaseRedisService } from "@vyhoang/microservice-clean/infrastructure/cache";
import { createI18nOptions, createI18nAsyncConfig, LanguageService } from "@vyhoang/microservice-clean/infrastructure/languages";
import { RedisIdempotencyRepository, withIdempotency } from "@vyhoang/microservice-clean/infrastructure/idempotency";
import { GrpcValidationPipe, ValidationSchema } from "@vyhoang/microservice-clean/infrastructure/validation";
import { GrpcBusinessExceptionFilter } from "@vyhoang/microservice-clean/infrastructure/filters";
import { MetadataMapper } from "@vyhoang/microservice-clean/infrastructure/mapper";
```

> [!CAUTION]
> **Direct root import is not supported:**
> ```ts
> import { ... } from "@vyhoang/microservice-clean"; // ❌ NOT SUPPORTED
> ```

---

## Modules & Usage Guide

### 1. RabbitMQ Messaging Architecture (`infrastructure/queue`)

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

#### Connection Ownership & Lifecycle
Create one long-lived `RabbitMQConnection` per service process and inject it into your adapters. `RabbitMQEventBus` does not own the connection. The application bootstrap or module shutdown must close the shared connection after messaging adapters have drained:

```ts
import {
  RabbitMQConnection,
  RabbitMQEventBus,
  DEFAULT_ALTERNATE_EXCHANGE,
  DEFAULT_UNROUTED_EVENTS_QUEUE
} from "@vyhoang/microservice-clean/infrastructure/queue";

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

const eventBus = new RabbitMQEventBus(connection);
await connection.connect();

// Publish event (preserves trace headers like traceparent for OpenTelemetry):
await eventBus.publishEvent(
  "order.created.v1", 
  { orderId: '123', total: 500000 },
  {
    headers: { 'traceparent': '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' }
  }
);

// Subscribe to event with Retry Backoff & DLQ:
await eventBus.subscribeEvent(
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
await eventBus.close();
await connection.close();
```

---

### 2. Redis Cache & Distributed Lock (`infrastructure/cache`)

Supports connection URL (`REDIS_URI` / `redis://...`) or granular host/port/credentials, automatic JSON serialization, atomic distributed locking, hashes, sets, and lists:

```ts
import { RedisService, BaseRedisService, getRedisClient } from "@vyhoang/microservice-clean/infrastructure/cache";

// Initialize (automatically reads REDIS_URI or REDIS_HOST / REDIS_PORT / REDIS_PASSWORD from env)
const redis = new RedisService();

// Basic Key-Value (auto JSON.stringify & JSON.parse)
await redis.set({ key: 'user:100', value: { id: 100, name: 'Alice' }, ttl: 3600 });
const user = await redis.get<{ id: number; name: string }>({ key: 'user:100' });

// Safe Distributed Lock (uses SET NX EX and atomic Lua Script release)
const acquired = await redis.acquireLock({ key: 'lock:order:123', ttlSeconds: 30, lockValue: 'worker-1' });
if (acquired) {
  try {
    // Process exclusive critical section...
  } finally {
    await redis.releaseLock({ key: 'lock:order:123', lockValue: 'worker-1' });
  }
}

// Hashes, Sets, Lists
await redis.hset({ key: 'profile:1', data: { age: 25, role: 'admin' } });
const profile = await redis.hgetall({ key: 'profile:1' });
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

### 5. gRPC Validation Pipe & Business Filter (`infrastructure/validation` & `infrastructure/filters`)

#### `GrpcValidationPipe`
Validates payload data against schemas and throws standard `BusinessError`:

```ts
import { GrpcValidationPipe, ValidationSchema } from '@vyhoang/microservice-clean/infrastructure/validation';

const schemas: Record<string, ValidationSchema> = {
  CreateAccount: {
    email: { required: true, type: 'string', pattern: /^[^\s@]+@[^\s@]+\.[^\s@]+$/ },
    age: { required: false, type: 'number', min: 18 }
  }
};

export class AccountGrpcValidationPipe extends GrpcValidationPipe {
  constructor() {
    super(schemas);
  }
}
```

#### `GrpcBusinessExceptionFilter`
Translates `BusinessError` into standard gRPC status codes and attaches `Metadata ('error-code')`:

```ts
import { GrpcBusinessExceptionFilter } from '@vyhoang/microservice-clean/infrastructure/filters';

// Optional custom status code mapper:
app.useGlobalFilters(new GrpcBusinessExceptionFilter((code) => {
  if (code === 404) return 5; // status.NOT_FOUND
  return 13; // status.INTERNAL
}));
```

---

### 6. Distributed Request Context & Metadata (`core` & `infrastructure/mapper`)

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

### 7. Core Errors (`core`)

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
