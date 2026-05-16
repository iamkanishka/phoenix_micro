# Deployment & Production

## Environment variables

Use `{:system, "VAR"}` in config to read from environment at runtime:

```elixir
# config/runtime.exs
import Config

if config_env() == :prod do
  config :phoenix_micro,
    transport: String.to_existing_atom(System.get_env("MESSAGE_TRANSPORT", "kafka")),
    transports: [
      kafka: [
        brokers: parse_brokers(System.get_env("KAFKA_BROKERS", "localhost:9092")),
        group_id:  System.get_env("KAFKA_GROUP_ID", "my_app"),
        client_id: System.get_env("KAFKA_CLIENT_ID", "my_app"),
        acks:      String.to_integer(System.get_env("KAFKA_ACKS", "1"))
      ],
      nats: [
        host:        System.get_env("NATS_HOST", "localhost"),
        port:        String.to_integer(System.get_env("NATS_PORT", "4222")),
        queue_group: System.get_env("NATS_QUEUE_GROUP", "my_app")
      ],
      rabbitmq: [
        url:      {:system, "RABBITMQ_URL"},
        exchange: System.get_env("RABBITMQ_EXCHANGE", "my_app")
      ],
      redis_streams: [
        url:            {:system, "REDIS_URL"},
        consumer_group: System.get_env("REDIS_CONSUMER_GROUP", "my_app")
      ]
    ]
end

defp parse_brokers(str) do
  str
  |> String.split(",")
  |> Enum.map(fn hp ->
    case String.split(String.trim(hp), ":") do
      [host, port] -> {host, String.to_integer(port)}
      [host]       -> {host, 9092}
    end
  end)
end
```

## Recommended production consumer stack

```elixir
defmodule MyApp.Payments.CreatedConsumer do
  use PhoenixMicro.Consumer

  topic "payments.created"
  concurrency 20
  pipeline :broadway          # explicit Broadway backpressure
  retry max_attempts: 5, base_delay: 1_000, max_delay: 60_000
  dead_letter_topic "payments.created.dlq"

  middleware [
    PhoenixMicro.Middleware.Logger,
    PhoenixMicro.Middleware.Metrics,
    {PhoenixMicro.Middleware.CircuitBreaker,
     threshold: 10,
     window_ms: 60_000,
     reset_timeout_ms: 60_000},
    {PhoenixMicro.Middleware.Idempotency,
     store: MyApp.Middleware.RedisIdempotencyStore},
    PhoenixMicro.Middleware.Tracing
  ]
end
```

## Health checks

Add to your router for load-balancer health probes:

```elixir
# router.ex
scope "/" do
  forward "/health", PhoenixMicro.Phoenix.HealthPlug
end
```

Returns HTTP 200 with JSON when healthy, HTTP 503 when degraded
(open circuit breakers or disconnected transport).

CI / pre-deploy health gate:

```bash
mix phoenix_micro.health \
  --url "${APP_URL}/health" \
  --exit-code \
  --format json
```

Returns exit code 1 if status is not `"ok"` — useful in deploy pipelines.

## LiveDashboard

Add PhoenixMicro's metrics page to LiveDashboard:

```elixir
# router.ex (dev / staging only)
if Application.compile_env(:my_app, :dev_routes) do
  import Phoenix.LiveDashboard.Router

  scope "/dev" do
    pipe_through :browser

    live_dashboard "/dashboard",
      metrics: MyAppWeb.Telemetry,
      additional_pages: [
        phoenix_micro: PhoenixMicro.LiveDashboard.Page
      ]
  end
end
```

Shows: transport connectivity, active consumers, circuit breaker states, saga metrics,
message throughput graphs (auto-refreshes every 2 seconds).

## Telemetry in production

Wire `PhoenixMicro.Telemetry.metrics/0` into your reporter:

```elixir
# application.ex
def start(_type, _args) do
  children = [
    {TelemetryMetricsPrometheus, metrics: metrics()},
    MyAppWeb.Endpoint
  ]
  Supervisor.start_link(children, strategy: :one_for_one)
end

defp metrics do
  # Your app metrics +
  PhoenixMicro.Telemetry.metrics()
end
```

## Kafka production checklist

- Set `acks: -1` (all replicas) for durability-critical topics
- Set `session_timeout_ms: 60_000` for stable group membership under load
- Monitor consumer lag — commit offset only after successful handler return
- Use separate `group_id` per application / deployment environment
- Keep `heartbeat_ms` ≤ `session_timeout_ms / 3`

```elixir
config :phoenix_micro,
  transport: :kafka,
  transports: [
    kafka: [
      url:                "kafka://broker1:9092,broker2:9092,broker3:9092",
      group_id:           System.get_env("KAFKA_GROUP_ID"),
      acks:               -1,
      ack_timeout_ms:     15_000,
      session_timeout_ms: 60_000,
      heartbeat_ms:       15_000,
      fetch_wait_ms:      250,
      max_bytes:          5_242_880  # 5 MB
    ]
  ]
```

## Clustering considerations

- Each node runs its own consumer processes
- **Kafka:** consumer group coordinator handles partition assignment automatically
- **NATS:** queue groups distribute load automatically
- **Redis Streams:** each node needs a unique `consumer_name`
- **RabbitMQ:** competing consumers on the same queue distribute load automatically
- The circuit breaker state is **per-node** (ETS) — tune thresholds accordingly
- The outbox Relay runs **per-node** — use a distributed lock if you want single-relay:

```elixir
# Use :global or a Redlock to elect one relay per cluster
{:ok, _} = :global.register_name({:outbox_relay, node()}, self())
```

## Graceful shutdown

PhoenixMicro transports trap exits and close broker connections cleanly.
Broadway drains in-flight messages before stopping.

Set an appropriate shutdown timeout in your release:

```elixir
# rel/config.exs  (or config/releases.exs in newer Phoenix)
release :my_app do
  shutdown_timeout: 30_000   # ms — allow in-flight messages to drain
end
```

## Docker / OTP releases

```dockerfile
FROM hexpm/elixir:1.16.3-erlang-26.2.5-alpine-3.20.0 AS build

WORKDIR /app
COPY mix.exs mix.lock ./
RUN mix deps.get --only prod
RUN MIX_ENV=prod mix deps.compile

COPY . .
RUN MIX_ENV=prod mix release

FROM alpine:3.20.0
RUN apk add --no-cache libstdc++ openssl ncurses-libs
WORKDIR /app
COPY --from=build /app/_build/prod/rel/my_app ./

ENV PHX_SERVER=true \
    MESSAGE_TRANSPORT=kafka \
    KAFKA_BROKERS=kafka:9092 \
    KAFKA_GROUP_ID=my_app_prod

CMD ["bin/my_app", "start"]
```

## Outbox in production

Monitor the relay to catch stuck rows:

```elixir
# Add to your telemetry/alerting
def check_outbox_health(repo, schema) do
  pending = repo.aggregate(schema, :count, :id,
    where: [relayed_at: nil, failed_at: nil])

  failed = repo.aggregate(schema, :count, :id,
    where: [failed_at: {:not, nil}])

  if pending > 1_000 do
    MyApp.Alerts.warn(:outbox_backlog, %{count: pending})
  end

  if failed > 0 do
    MyApp.Alerts.error(:outbox_failures, %{count: failed})
  end

  %{pending: pending, failed: failed}
end
```
