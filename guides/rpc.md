# RPC

PhoenixMicro's RPC system provides synchronous request/reply over any transport.
No HTTP, no gRPC — the same message broker you use for pub/sub handles RPC.

## How it works

1. Caller generates a `correlation_id` and a unique `reply_to` inbox topic
2. Caller publishes the request to the target topic with `reply_to` and `correlation_id` set
3. Handler consumer publishes a response to `message.reply_to`
4. Caller's RPC GenServer matches the `correlation_id` and returns `{:ok, result}`
5. No reply within `timeout` → `{:error, :timeout}`

## Making RPC calls

```elixir
# Topic form — simplest
{:ok, result} = PhoenixMicro.rpc("math.sum", [1, 2, 3])

# Service + pattern form
{:ok, result} = PhoenixMicro.rpc("math", "sum", [1, 2, 3])

# With options
{:ok, result} = PhoenixMicro.rpc("math", "sum", [1, 2, 3],
  timeout: 5_000,  # ms (default: 5_000)
  retry:   2       # retries on timeout (default: 0)
)

# Handle all cases
case PhoenixMicro.rpc("payments", "charge", %{amount: 100}) do
  {:ok, charge}          -> process(charge)
  {:error, :timeout}     -> handle_timeout()
  {:error, reason}       -> handle_error(reason)
end
```

## Implementing an RPC handler

The handler consumer **must** publish a reply to `message.reply_to`:

```elixir
defmodule MyApp.RPC.MathConsumer do
  use PhoenixMicro.Consumer

  topic "math.sum"
  concurrency 20

  @impl PhoenixMicro.Consumer
  def handle(%PhoenixMicro.Message{} = message, _ctx) do
    result = Enum.sum(message.payload)

    if message.reply_to do
      PhoenixMicro.publish(message.reply_to, result,
        headers: %{"x-correlation-id" => message.correlation_id}
      )
    end

    :ok
  end
end
```

## Pattern routing

`PhoenixMicro.rpc("service", "pattern", payload)` publishes to `"service.pattern"`.
Route multiple patterns in a single consumer with wildcards:

```elixir
defmodule MyApp.RPC.MathRouter do
  use PhoenixMicro.Consumer

  topic "math.*"       # handles math.sum, math.multiply, math.max, etc.
  concurrency 20

  @impl PhoenixMicro.Consumer
  def handle(%PhoenixMicro.Message{} = message, _ctx) do
    pattern = Map.get(message.headers, "x-pattern", "")
    result  = dispatch(pattern, message.payload)

    if message.reply_to do
      PhoenixMicro.publish(message.reply_to, result,
        headers: %{"x-correlation-id" => message.correlation_id}
      )
    end

    :ok
  end

  defp dispatch("sum",      nums),  do: Enum.sum(nums)
  defp dispatch("multiply", nums),  do: Enum.reduce(nums, 1, &*/2)
  defp dispatch("max",      nums),  do: Enum.max(nums)
  defp dispatch(unknown, _payload), do: {:error, "unknown pattern: #{unknown}"}
end
```

## Timeouts and retries

```elixir
# Default: 5 second timeout, no retry
PhoenixMicro.rpc("slow.service", payload)

# Tight timeout for fast SLA-bound services
PhoenixMicro.rpc("inventory.check", payload, timeout: 500)

# Retry on timeout — for idempotent operations only
PhoenixMicro.rpc("idempotent.lookup", payload, timeout: 2_000, retry: 3)
```

> **Warning:** Only use `retry` for truly idempotent operations. If the handler
> already processed the request but the reply was lost in transit, retrying
> will process it again.

## Circuit-breaking RPC callers

Wrap RPC calls in a circuit breaker to prevent timeout cascades:

```elixir
alias PhoenixMicro.Middleware.CircuitBreaker

case CircuitBreaker.call("payments_rpc_fuse", fn ->
  PhoenixMicro.rpc("payments", "charge", payload, timeout: 2_000)
end, threshold: 5, reset_timeout_ms: 30_000) do
  {:ok, charge}           -> charge
  {:error, :circuit_open} -> {:error, :payments_unavailable}
  {:error, :timeout}      -> {:error, :payments_slow}
  {:error, reason}        -> {:error, reason}
end
```

## Telemetry events

| Event                               | Measurements      | Metadata                          |
| ----------------------------------- | ----------------- | --------------------------------- |
| `[:phoenix_micro, :rpc, :request]`  | `%{count: 1}`     | `%{topic: t, correlation_id: id}` |
| `[:phoenix_micro, :rpc, :response]` | `%{duration: ns}` | `%{topic: t, correlation_id: id}` |
| `[:phoenix_micro, :rpc, :timeout]`  | `%{count: 1}`     | `%{topic: t, correlation_id: id}` |

## Testing RPC

```elixir
# config/test.exs
config :phoenix_micro, transport: :memory, consumers: []

defmodule MyApp.RPC.MathTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.Transport.Memory

  setup do
    Memory.clear()

    # Register a fake RPC handler
    Memory.subscribe("math.sum", fn msg ->
      if msg.reply_to do
        PhoenixMicro.publish(msg.reply_to, Enum.sum(msg.payload),
          headers: %{"x-correlation-id" => msg.correlation_id}
        )
      end
      :ok
    end, [])

    :ok
  end

  test "returns sum of numbers" do
    assert {:ok, 6} = PhoenixMicro.rpc("math.sum", [1, 2, 3], timeout: 1_000)
  end

  test "returns timeout when no handler replies" do
    assert {:error, :timeout} = PhoenixMicro.rpc("ghost.service", [], timeout: 100)
  end
end
```

## Best practices

- **Keep RPC calls under 1 second.** Blocking the calling process for longer
  creates backpressure and head-of-line blocking.
- **Prefer events for workflows.** Use RPC for lookups (user profile, inventory check)
  and use pub/sub + sagas for multi-step business operations.
- **Set realistic timeouts.** The default 5 s is conservative. Most services should
  respond in under 100 ms. Match the timeout to your P99 latency + buffer.
- **Never retry mutations via RPC.** Only retry reads or truly idempotent writes.
