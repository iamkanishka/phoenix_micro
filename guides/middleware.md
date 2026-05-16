# Middleware

Middleware wraps message handling in composable, ordered layers — outermost first.
Every middleware receives the `PhoenixMicro.Message` struct and a `next` function
representing the rest of the chain.

## Composing middleware

```elixir
defmodule MyApp.Payments.CreatedConsumer do
  use PhoenixMicro.Consumer

  topic "payments.created"

  # Middleware runs left-to-right (Logger outermost, Idempotency innermost)
  middleware [
    PhoenixMicro.Middleware.Logger,
    PhoenixMicro.Middleware.Metrics,
    PhoenixMicro.Middleware.Retry,
    {PhoenixMicro.Middleware.CircuitBreaker,
     threshold: 5, window_ms: 60_000, reset_timeout_ms: 30_000},
    {PhoenixMicro.Middleware.Idempotency,
     store: PhoenixMicro.Middleware.Idempotency.ETSStore}
  ]
end
```

## Built-in middleware

### Logger

Emits a structured `debug` log on message receipt and a `debug` or `warning`
log on outcome.

```elixir
middleware [PhoenixMicro.Middleware.Logger]
```

### Metrics

Emits `:telemetry.span` events for every message. Use with `PhoenixMicro.Telemetry.metrics/0`
to wire into `TelemetryMetricsPrometheus` or similar.

```elixir
middleware [PhoenixMicro.Middleware.Metrics]
```

### Retry

Middleware-level retry with exponential backoff and jitter — runs _before_ the
consumer-level retry configured via `retry max_attempts: N`.

```elixir
middleware [
  {PhoenixMicro.Middleware.Retry,
   max: 3,
   base_delay: 200,
   max_delay: 5_000}
]
```

### CircuitBreaker

ETS-backed 3-state fuse per consumer topic.

| State        | Behaviour                                                                   |
| ------------ | --------------------------------------------------------------------------- |
| `:closed`    | Normal — all messages processed                                             |
| `:open`      | Rejecting — returns `{:error, :circuit_open}` immediately                   |
| `:half_open` | Probing — allows one message through; closes on success, reopens on failure |

```elixir
middleware [
  {PhoenixMicro.Middleware.CircuitBreaker,
   threshold:        10,       # failures within window before opening
   window_ms:        60_000,   # sliding failure window
   reset_timeout_ms: 30_000}   # how long to stay open before half-open probe
]
```

Emits telemetry on trip and reset:

- `[:phoenix_micro, :circuit_breaker, :tripped]`
- `[:phoenix_micro, :circuit_breaker, :reset]`
- `[:phoenix_micro, :circuit_breaker, :rejected]`

### Idempotency

Deduplicates messages by `message.id`. Pluggable store — use ETS in dev/test,
Redis or PostgreSQL in production.

```elixir
# Dev / test
middleware [
  {PhoenixMicro.Middleware.Idempotency,
   store: PhoenixMicro.Middleware.Idempotency.ETSStore}
]

# Production — custom Redis store
middleware [
  {PhoenixMicro.Middleware.Idempotency,
   store: MyApp.Middleware.RedisIdempotencyStore}
]
```

Custom store (implement the `PhoenixMicro.IdempotencyStore` behaviour):

```elixir
defmodule MyApp.Middleware.RedisIdempotencyStore do
  @behaviour PhoenixMicro.IdempotencyStore

  @impl PhoenixMicro.IdempotencyStore
  def seen?(id) do
    case MyApp.Redis.get("idem:#{id}") do
      {:ok, nil} -> false
      {:ok, _}   -> true
      _          -> false
    end
  end

  @impl PhoenixMicro.IdempotencyStore
  def mark_seen(id) do
    MyApp.Redis.set("idem:#{id}", "1", ex: 86_400)
    :ok
  end
end
```

### Tracing

OpenTelemetry-compatible. Extracts W3C `traceparent` headers and creates spans.
Degrades gracefully if `:opentelemetry` is not installed.

```elixir
middleware [PhoenixMicro.Middleware.Tracing]
```

Requires in your app's deps (optional):

```elixir
{:opentelemetry, "~> 1.3"},
{:opentelemetry_api, "~> 1.3"}
```

## Writing custom middleware

Implement the `PhoenixMicro.Middleware` behaviour with a single `call/2` callback:

```elixir
defmodule MyApp.Middleware.TenantContext do
  @behaviour PhoenixMicro.Middleware

  require Logger

  @impl PhoenixMicro.Middleware
  def call(%PhoenixMicro.Message{} = message, next) do
    tenant_id = Map.get(message.headers, "x-tenant-id", "default")

    # Set for the duration of this message
    Process.put(:current_tenant_id, tenant_id)
    Logger.metadata(tenant_id: tenant_id)

    result = next.(message)

    # Always clean up — even on error
    Process.delete(:current_tenant_id)
    result
  end
end
```

```elixir
defmodule MyApp.Middleware.RateLimiter do
  @behaviour PhoenixMicro.Middleware

  @impl PhoenixMicro.Middleware
  def call(%PhoenixMicro.Message{} = message, next) do
    topic = message.topic

    case MyApp.RateLimiter.check(topic) do
      :ok ->
        next.(message)

      {:error, :rate_exceeded} ->
        # Returning an error causes the consumer to retry / DLQ
        {:error, :rate_exceeded}
    end
  end
end
```

## Middleware composition order

```
consumer: middleware [A, B, C]

A.call(message, fn ->
  B.call(message, fn ->
    C.call(message, fn ->
      handler.(message)   # your handle/2
    end)
  end)
end)
```

Errors propagate back outward through each layer, so Logger and Metrics always
see the final outcome — regardless of which middleware or handler caused the failure.

## Accessing middleware state

The full middleware chain result is available in `handle_error/3`:

```elixir
@impl PhoenixMicro.Consumer
def handle_error(message, reason, _ctx) do
  Logger.error("Giving up on #{message.id}: #{inspect(reason)}")
  MyApp.Alerts.notify(:consumer_failure, message, reason)
  :ok
end
```
