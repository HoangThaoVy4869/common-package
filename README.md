# @vyhoang/microservice-clean

Shared backend utilities and infrastructure helpers for Node.js microservices.

This package ships as a TypeScript-friendly CommonJS library with compiled output in `dist/`.

## Installation

```bash
npm install @vyhoang/microservice-clean
```

Some features depend on framework-specific packages such as NestJS, Moleculer, Mongoose, Redis, or Elasticsearch. Those are declared as optional peer dependencies, so only install the ones your app actually uses.

## Exports

The package supports both root imports and subpath imports:

```ts
import { ResponseHandler, Exception } from "@vyhoang/microservice-clean";
import { validateDto } from "@vyhoang/microservice-clean/core";
import { RedisService, RabbitMQEventBus } from "@vyhoang/microservice-clean/infrastructure";
```

## Included Modules

### `core`

- Constants and exception helpers
- Shared interfaces
- Response helpers
- Utility functions such as DTO validation and ID generation

### `infrastructure`

- Redis cache helpers
- SMS providers and fallback handling
- RabbitMQ event bus
- Logger and Elasticsearch stream helpers
- Moleculer language and mixin helpers
- Repository and mapper base classes

## Build

```bash
npm run build
```

## Project Structure

```text
src/
  core/
  infrastructure/
dist/
```

## Requirements

- Node.js `>=20`

## License

ISC
