# Getting Started

This guide walks you through adding PhoenixMicro to an existing Phoenix 1.7+ application and publishing your first message in under five minutes using the built-in in-memory transport — no broker installation required.

## Prerequisites

- Elixir 1.16+
- Phoenix 1.7+
- OTP 26+

## Step 1 — Add the dependency

```elixir
# mix.exs
def deps do
  [
    {:phoenix_micro, "~> 1.0"},
    {:broadway, "~> 1.0"}   # required for production pipelines
  ]
end
```

```bash
mix deps.get
```

PhoenixMicro starts as its own OTP application — no manual supervision tree entry needed.

## Step 2 — Configure (in-memory to start)

The **in-memory transport** requires zero external services. Use it to get your
consumers working before adding a real broker.

```elixir
# config/config.exs
config :phoenix_micro,
  transport: :memory,
  consumers: []   # start empty, register below

# config/test.exs
config :phoenix_micro,
  transport: :memory,
  consumers: []
```

## Step 3 — Generate your first consumer

```bash
mix phoenix_micro.gen.consumer MyApp.Payments.CreatedConsumer \
  --topic payments.created \
  --concurrency 5 \
  --retry 3
```

This generates:

- `lib/my_app/payments/created_consumer.ex`
- `test/my_app/payments/created_consumer_test.exs`

Open the generated file and implement `handle/2`:

```elixir
defmodule MyApp.Payments.CreatedConsumer do
  use PhoenixMicro.Consumer

  topic "payments.created"
  concurrency 5
  retry max_attempts: 3, base_delay: 500
  dead_letter_topic "payments.created.dlq"

  middleware [
    PhoenixMicro.Middleware.Logger,
    PhoenixMicro.Middleware.Metrics
  ]

  @impl PhoenixMicro.Consumer
  def handle(%PhoenixMicro.Message{} = message, _ctx) do
    %{"amount" => amount, "currency" => currency} = message.payload
    IO.puts("💳 Payment received: #{amount} #{currency}")
    :ok
  end
end
```

## Step 4 — Register the consumer

```elixir
# config/config.exs
config :phoenix_micro,
  transport: :memory,
  consumers: [MyApp.Payments.CreatedConsumer]
```

## Step 5 — Add the health endpoint

```elixir
# lib/my_app_web/router.ex
scope "/" do
  forward "/health", PhoenixMicro.Phoenix.HealthPlug
end
```

## Step 6 — Start and publish

```bash
iex -S mix phx.server
```

In the IEx session:

```elixir
# Publish a message
PhoenixMicro.publish("payments.created", %{
  "amount" => 99.99,
  "currency" => "USD",
  "payment_id" => "pay_001"
})
# => :ok
# You'll see the IO.puts output from your consumer immediately

# Synchronous — waits for broker ack
:ok = PhoenixMicro.publish_sync("payments.created", %{"amount" => 50.0, "currency" => "EUR"})

# Inspect what was published
PhoenixMicro.Transport.Memory.messages()
```

Visit `http://localhost:4000/health` to see the JSON status.

## Step 7 — Switch to a real broker

Once your consumers are working, switch the transport in config.
**No consumer code changes required.**

### Kafka (built-in — zero deps)

```elixir
config :phoenix_micro,
  transport: :kafka,
  transports: [
    kafka: [
      brokers: [{"localhost", 9092}],
      group_id: "my_app"
    ]
  ]
```

### NATS (add `{:gnat, "~> 1.7"}` to deps)

```elixir
config :phoenix_micro,
  transport: :nats,
  transports: [
    nats: [host: "localhost", port: 4222, queue_group: "my_app"]
  ]
```

### RabbitMQ (add `{:amqp, "~> 3.3"}` to deps)

```elixir
config :phoenix_micro,
  transport: :rabbitmq,
  transports: [
    rabbitmq: [url: "amqp://guest:guest@localhost", exchange: "my_app"]
  ]
```

### Redis Streams (add `{:redix, "~> 1.5"}` to deps)

```elixir
config :phoenix_micro,
  transport: :redis_streams,
  transports: [
    redis_streams: [url: "redis://localhost:6379", consumer_group: "my_app"]
  ]
```

## Next steps

- [Transports guide](transports.md) — detailed per-broker config and Docker Compose examples
- [Middleware guide](middleware.md) — circuit breakers, idempotency, tracing
- [Schema Registry guide](schemas.md) — typed, versioned message contracts
- [Sagas guide](sagas.md) — distributed transactions with compensation
- [RPC guide](rpc.md) — synchronous request/reply
- [Outbox guide](outbox.md) — exactly-once delivery via PostgreSQL
- [Testing guide](testing.md) — in-memory transport helpers and patterns
- [Deployment guide](deployment.md) — production config, health checks, releases
