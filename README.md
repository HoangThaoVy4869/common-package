# @vyhoang/microservice-clean

Shared backend utilities and infrastructure helpers for Node.js microservices built with Clean Architecture.

This package is intentionally exposed through subpath imports only.

## Installation

```bash
npm install @vyhoang/microservice-clean
```

Some features depend on framework-specific packages. Install the optional peer dependencies required by the subpaths your app uses.

## Import Rules

Use the supported subpath entrypoints:

```ts
import { ResponseHandler, Exception, validateDto, RequestContext, REQUEST_METADATA } from "@vyhoang/microservice-clean/core";
import { RedisService } from "@vyhoang/microservice-clean/infrastructure/cache";
import {
  RabbitMQConnection,
  RabbitMQEventBus,
  DEFAULT_ALTERNATE_EXCHANGE,
  DEFAULT_UNROUTED_EVENTS_QUEUE
} from "@vyhoang/microservice-clean/infrastructure/queue";
import { MetadataMapper } from "@vyhoang/microservice-clean/infrastructure/mapper";
```

Prefer feature subpaths such as `infrastructure/cache`, `infrastructure/queue`,
`infrastructure/logger`, and `infrastructure/mapper`. The broad
`infrastructure` entrypoint is kept for compatibility, but importing it loads
all of its runtime peer dependencies.

Do not import from:

```ts
import { ... } from "@vyhoang/microservice-clean";
```

Root package import is intentionally not supported.

## Included Modules

### `core`

- **Constants & Exception helpers**: Standard business exception hierarchy with error codes.
- **Shared Interfaces**: `IMessageBus`, `RequestContext`, `REQUEST_METADATA`.
- **Response helpers**: Standardized HTTP/gRPC API payload envelope.
- **Validation & ID utilities**: DTO validation (`class-validator`), UUIDv7 generation.

### `infrastructure`

- **`queue`**: High-reliability RabbitMQ Event Bus with built-in **Alternate Exchange (AE)** and **Dead Letter Queue (DLQ)** retry topology.
- **`cache`**: Redis service helper with atomic locking and serialization.
- **`mapper`**: gRPC `MetadataMapper` for seamless W3C & request context propagation (`requestId`, `userId`, `merchantId`, `roles`, `scopes`, `sessionId`, `clientIp`, `language`, etc.).
- **`logger`**: Structured logging and Elasticsearch stream helpers.
- **`repositories`**: Base persistence repository patterns.

---

## RabbitMQ Messaging Architecture

### 1. Connection Ownership

Create one long-lived `RabbitMQConnection` per service process and inject it into the adapters that need RabbitMQ. The connection is framework-agnostic and can be registered as a singleton provider by NestJS or created directly during bootstrap.

```ts
import {
  RabbitMQConnection,
  RabbitMQEventBus,
} from "@vyhoang/microservice-clean/infrastructure/queue";

const connection = new RabbitMQConnection({
  urls: [process.env.RABBITMQ_URL || "amqp://localhost:5672"],
  connectTimeoutMs: 30_000,
  publishTimeoutMs: 15_000,
  // Alternate Exchange configuration (enabled by default)
  alternateExchange: {
    enabled: true,
    exchangeName: "unrouted.events.exchange",
    queueName: "unrouted.events.q",
  },
});

const eventBus = new RabbitMQEventBus(connection);
await connection.connect();
```

> [!IMPORTANT]
> `RabbitMQEventBus` never owns the connection. Application bootstrap must close the shared connection after all messaging adapters have drained:
> ```ts
> await eventBus.close();
> await connection.close();
> ```

---

### 2. Message Safety Nets: Alternate Exchange & DLQ Topology

`@vyhoang/microservice-clean` implements an enterprise **two-tier message safety net**:

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
- If a publisher emits an event and no active consumers are bound to the exchange, the message is routed to `unrouted.events.q` rather than silently dropped.
- You can inspect or subscribe to `DEFAULT_UNROUTED_EVENTS_QUEUE` in your alert service to monitor orphaned events.

#### Tier 2: Consumer Execution Safety (Retry Loop & DLQ)
When subscribing to events or handling commands, provide `queueName` and `retry` policy to activate durable retry backoff:

```ts
await eventBus.subscribeEvent(
  "order.status-changed.v1",
  async (payload, rawMessage, envelope) => {
    // Process business logic
  },
  {
    queueName: "notification.inbound.order.q",
    prefetch: 10,
    retry: {
      maxRetries: 3,
      retryDelayMs: 5_000,
      deadLetterQueue: "notification.dlq",
    },
  }
);
```

- **Retry Queue**: Automatically created as `<queueName>.retry` with `x-message-ttl`. After expiry, messages route back to `<queueName>` for re-execution.
- **Dead Letter Queue**: After `maxRetries` attempts fail, messages are moved to the specified `deadLetterQueue` (or `<queueName>.dlq`) with headers `x-event-bus-retry-count` preserved.

---

## Request Context & Metadata Propagation

For distributed tracing and contextual information across HTTP, gRPC, and RabbitMQ:

```ts
import { RequestContext, REQUEST_METADATA } from "@vyhoang/microservice-clean/core";
import { MetadataMapper } from "@vyhoang/microservice-clean/infrastructure/mapper";
import { Metadata } from "@grpc/grpc-js";

// Extract RequestContext from gRPC metadata:
const context = MetadataMapper.toRequestContext(grpcMetadata);
console.log(context.requestId, context.userId, context.roles);

// Serialize RequestContext to outbound gRPC metadata:
const outboundMetadata: Metadata = MetadataMapper.fromRequestContext({
  requestId: "req-123",
  userId: "user-456",
  roles: ["BUYER", "MERCHANT"],
  language: "vi",
  currencyCode: "VND",
  regionCode: "VN",
});
```

---

## Build

```bash
npm run build
```

## Requirements

- Node.js `>=20`

## License

ISC
