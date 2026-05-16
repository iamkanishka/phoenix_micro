# PhoenixMicro

[![Hex.pm](https://img.shields.io/hexpm/v/phoenix_micro.svg)](https://hex.pm/packages/phoenix_micro)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/phoenix_micro)
[![License](https://img.shields.io/hexpm/l/phoenix_micro.svg)](LICENSE)

**Production-grade microservices toolkit for Elixir/Phoenix.**

 Phoenix Microservices built natively for OTP and the BEAM VM. PhoenixMicro gives your Phoenix application a full microservices substrate — transports, RPC, schema registry, circuit breakers, sagas, outbox pattern, and more.

## Features

| Feature                 | Description                                                 |
| ----------------------- | ----------------------------------------------------------- |
| **Multiple Transports** | RabbitMQ, Kafka, NATS, Redis Streams, in-memory (test)      |
| **Broadway Pipelines**  | Backpressure-aware message processing via GenStage          |
| **Typed RPC**           | Synchronous request/reply with correlation IDs and timeouts |
| **Schema Registry**     | Versioned, typed message contracts with automatic migration |
| **Circuit Breaker**     | ETS-backed, 3-state (closed/open/half-open) per-topic fuses |
| **Saga Orchestration**  | Sequential steps with automatic compensation on failure     |
| **Outbox Pattern**      | Transactional messaging via PostgreSQL — zero message loss  |
| **Middleware Pipeline** | Composable: logger, metrics, retry, tracing, idempotency    |
| **Telemetry**           | Built-in `:telemetry` events + LiveDashboard page           |
| **Health Endpoint**     | Plug-compatible `/health` with transport + CB status        |

## Installation

> **Zero native deps.** `phoenix_micro` itself compiles on any platform —
> Windows, Linux, macOS — with no C compiler, no rebar3, and no native code.
> Add the dep for your chosen transport to YOUR app's `mix.exs`:

```elixir
def deps do
  [
    {:phoenix_micro, "~> 1.0"},

    # Add exactly ONE transport dep to YOUR app (not phoenix_micro):
    {:gnat, "~> 1.7"},           # NATS   — pure Elixir, no rebar3, works everywhere
    # {:redix, "~> 1.5"},        # Redis  — pure Elixir, no rebar3, works everywhere
    # {:amqp, "~> 3.3"},         # RabbitMQ — needs rebar3 / escript on PATH
    # {:kafka_ex, "~> 0.13"},    # Kafka  — needs C compiler (crc32cer native dep)

    # Recommended for production pipelines (pure Elixir):
    {:broadway, "~> 1.0"},
  ]
end
```

| Transport     | Dep (add to YOUR app) | Linux/macOS         | Windows                        |
| ------------- | --------------------- | ------------------- | ------------------------------ |
| NATS          | `{:gnat, "~> 1.7"}`   | ✅ Pure Elixir      | ✅ Pure Elixir                 |
| Redis Streams | `{:redix, "~> 1.5"}`  | ✅ Pure Elixir      | ✅ Pure Elixir                 |
| RabbitMQ      | `{:amqp, "~> 3.3"}`   | ✅ with rebar3      | ⚠️ Needs `escript.exe` on PATH |
| Kafka         | none (built-in)       | ✅ Pure Elixir      | ✅ Pure Elixir                 |
| In-memory     | none (built-in)       | ✅ Always available | ✅ Always available            |

## Quick Start

### 1. Configure

```elixir
# config/config.exs
config :phoenix_micro,
  transport: :rabbitmq,
  transports: [
    rabbitmq: [url: "amqp://guest:guest@localhost", exchange: "my_app"]
  ],
  consumers: [MyApp.Payments.CreatedConsumer]
```

### 2. Define a consumer

```elixir
defmodule MyApp.Payments.CreatedConsumer do
  use PhoenixMicro.Consumer

  topic "payments.created"
  concurrency 10
  retry max_attempts: 3, base_delay: 500
  dead_letter_topic "payments.created.dlq"

  middleware [
    PhoenixMicro.Middleware.Logger,
    PhoenixMicro.Middleware.Metrics,
    {PhoenixMicro.Middleware.CircuitBreaker, threshold: 5}
  ]

  @impl PhoenixMicro.Consumer
  def handle(%PhoenixMicro.Message{} = message, _ctx) do
    %{"amount" => amount, "currency" => currency} = message.payload
    case MyApp.Payments.process(amount, currency) do
      {:ok, _}         -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
```

### 3. Publish and RPC

```elixir
# Async publish
PhoenixMicro.publish("payments.created", %{amount: 100, currency: "USD"})

# Sync publish
:ok = PhoenixMicro.publish_sync("payments.created", %{amount: 100, currency: "USD"})

# RPC
{:ok, result} = PhoenixMicro.rpc("math.sum", [1, 2, 3])
{:ok, result} = PhoenixMicro.rpc("math", "sum", [1, 2, 3], timeout: 3_000)
```

## Transports

### RabbitMQ

```elixir
config :phoenix_micro, transport: :rabbitmq,
  transports: [rabbitmq: [url: "amqp://localhost", exchange: "my_app", prefetch_count: 10]]
```

### NATS

```elixir
config :phoenix_micro, transport: :nats,
  transports: [nats: [host: "localhost", port: 4222, queue_group: "my_app"]]
```

### Redis Streams

```elixir
config :phoenix_micro, transport: :redis_streams,
  transports: [redis_streams: [url: "redis://localhost:6379", consumer_group: "my_app"]]
```

### Kafka

```elixir
config :phoenix_micro, transport: :kafka,
  transports: [kafka: [brokers: [{"localhost", 9092}], group_id: "my_app"]]
```

### In-memory (testing)

```elixir
# config/test.exs
config :phoenix_micro, transport: :memory
```

## Schema Registry

```elixir
defmodule MyApp.Schemas.PaymentCreated do
  use PhoenixMicro.Schema

  schema_version 2
  topic "payments.created"

  field :payment_id,   :string,  required: true
  field :amount_cents, :integer, required: true
  field :currency,     :string,  required: true, default: "USD"

  def migrate(1, payload) do
    cents = round(Map.get(payload, "amount", 0) * 100)
    payload |> Map.delete("amount") |> Map.put("amount_cents", cents)
  end
end

# In your consumer
{:ok, payload} = PhoenixMicro.Schema.decode(MyApp.Schemas.PaymentCreated, message.payload)
```

## Middleware

```elixir
middleware [
  PhoenixMicro.Middleware.Logger,
  PhoenixMicro.Middleware.Metrics,
  PhoenixMicro.Middleware.Retry,
  {PhoenixMicro.Middleware.CircuitBreaker, threshold: 5, reset_timeout_ms: 30_000},
  {PhoenixMicro.Middleware.Idempotency, store: PhoenixMicro.Middleware.Idempotency.ETSStore}
]
```

Custom middleware:

```elixir
defmodule MyApp.Middleware.Auth do
  @behaviour PhoenixMicro.Middleware

  @impl PhoenixMicro.Middleware
  def call(message, next) do
    if valid_token?(message.headers["authorization"]) do
      next.(message)
    else
      {:error, :unauthorized}
    end
  end
end
```

## Sagas

```elixir
defmodule MyApp.OrderSaga do
  use PhoenixMicro.Saga

  step :reserve_inventory,
    execute: fn ctx ->
      case Inventory.reserve(ctx.product_id, ctx.quantity) do
        {:ok, r} -> {:ok, Map.put(ctx, :reservation_id, r.id)}
        err      -> err
      end
    end,
    compensate: fn ctx -> Inventory.release(ctx.reservation_id) end

  step :charge_payment,
    execute: fn ctx ->
      case Payments.charge(ctx.user_id, ctx.amount) do
        {:ok, c} -> {:ok, Map.put(ctx, :charge_id, c.id)}
        err      -> err
      end
    end,
    compensate: fn ctx -> Payments.refund(ctx.charge_id) end
end

# Run it
{:ok, ctx} = MyApp.OrderSaga.run(%{product_id: "p1", quantity: 2, user_id: "u1", amount: 4999})
```

## Outbox Pattern

```elixir
# Generate migration
mix phoenix_micro.gen.migration

# Use inside Ecto transaction
Repo.transaction(fn ->
  order = Repo.insert!(Order.changeset(%Order{}, params))
  :ok = PhoenixMicro.Outbox.enqueue("orders.placed", %{id: order.id})
end)
```

## Observability

```elixir
# Health endpoint
forward "/health", PhoenixMicro.Phoenix.HealthPlug

# LiveDashboard
live_dashboard "/dashboard",
  additional_pages: [phoenix_micro: PhoenixMicro.LiveDashboard.Page]

# Default logger
PhoenixMicro.Telemetry.attach_default_logger(:info)
```

## Testing

```elixir
# config/test.exs
config :phoenix_micro, transport: :memory

# Tests
setup do
  name = :erlang.unique_integer([:positive, :monotonic])
  {:ok, _pid} = start_supervised({PhoenixMicro.Transport.Memory, [name: name]})
  %{transport: name}
end
```

## Mix Tasks

```bash
mix phoenix_micro.gen.consumer MyApp.Payments.CreatedConsumer --topic payments.created
mix phoenix_micro.gen.saga MyApp.OrderSaga --steps reserve,charge,confirm
mix phoenix_micro.gen.migration
mix phoenix_micro.health --url http://localhost:4000/health --exit-code
```

## License

MIT
