# Changelog

All notable changes to PhoenixMicro are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-05-02

### Added
- Initial stable release
- RabbitMQ, Kafka, NATS, Redis Streams, and in-memory transports
- Broadway-backed consumer pipelines with backpressure
- Synchronous RPC with correlation IDs, timeouts, and retry
- Versioned schema registry with automatic migration
- Composable middleware: Logger, Metrics, Retry, Tracing, CircuitBreaker, Idempotency
- ETS-backed circuit breaker (closed / open / half-open)
- Saga orchestration with automatic step compensation
- Outbox pattern with PostgreSQL-backed Relay
- Full telemetry event coverage
- Plug-compatible health endpoint
- Phoenix LiveDashboard integration page
- Mix tasks: gen.consumer, gen.saga, gen.migration, health
- Comprehensive ExDoc documentation and guides
