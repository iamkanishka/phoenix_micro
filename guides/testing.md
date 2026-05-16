# Testing

PhoenixMicro is designed for fast, deterministic tests. The in-memory transport
provides the full pub/sub API with no external services, broker setup, or async
timing issues.

## Setup

```elixir
# config/test.exs
config :phoenix_micro,
  transport: :memory,
  consumers: []   # register consumers in individual tests, not globally
```

## ExUnit setup pattern

```elixir
defmodule MyApp.SomeTest do
  use ExUnit.Case, async: false  # async: false when using Memory (shared state)

  alias PhoenixMicro.Transport.Memory

  setup do
    Memory.clear()   # reset published messages and DLQ between tests
    :ok
  end
end
```

## Testing a consumer handler directly

The fastest and most reliable approach — call `handle/2` directly, no transport needed:

```elixir
defmodule MyApp.Payments.CreatedConsumerTest do
  use ExUnit.Case, async: true   # async: true is fine — no shared transport state

  alias PhoenixMicro.Message
  alias MyApp.Payments.CreatedConsumer

  @valid_payload %{
    "payment_id"   => "pay_001",
    "order_id"     => "ord_001",
    "amount_cents" => 4999,
    "currency"     => "USD",
    "user_id"      => "user_001"
  }

  test "handles a valid payment message" do
    msg = Message.new("payments.created", @valid_payload)
    assert :ok = CreatedConsumer.handle(msg, %{})
  end

  test "returns error for missing required fields" do
    msg = Message.new("payments.created", %{"bad" => "data"})
    assert {:error, _reason} = CreatedConsumer.handle(msg, %{})
  end

  test "handles v1 payload migration transparently" do
    v1_payload = %{"payment_id" => "pay_v1", "order_id" => "ord_v1",
                   "amount" => 49.99, "user_id" => "u1"}
    msg = Message.new("payments.created", v1_payload)
    assert :ok = CreatedConsumer.handle(msg, %{})
  end
end
```

## Testing publish → consumer integration

Use `Memory.messages/0` to assert what was published:

```elixir
defmodule MyApp.Payments.IntegrationTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.Transport.Memory

  setup do
    Memory.clear()
    :ok
  end

  test "publishing a payment triggers an inventory check" do
    PhoenixMicro.publish("payments.created", %{
      "payment_id"   => "pay_001",
      "order_id"     => "ord_001",
      "amount_cents" => 4999,
      "currency"     => "USD",
      "user_id"      => "user_001"
    })

    # wait for async consumer to process and publish downstream
    Memory.wait_for_messages("inventory.check", 1, timeout: 2_000)

    msgs = Memory.messages()
    inv_check = Enum.find(msgs, &(&1.topic == "inventory.check"))
    assert inv_check != nil
    assert inv_check.payload["order_id"] == "ord_001"
  end

  test "failed messages go to DLQ after exhausting retries" do
    PhoenixMicro.publish("payments.created", %{"trigger_failure" => true})
    Memory.wait_for_messages("payments.created.dlq", 1, timeout: 5_000)

    refute Enum.empty?(Memory.dlq_messages())
    [dlq_msg] = Memory.dlq_messages()
    assert dlq_msg.metadata[:original_topic] == "payments.created"
  end
end
```

## Testing sagas

```elixir
defmodule MyApp.OrderSagaTest do
  use ExUnit.Case, async: true

  alias MyApp.OrderSaga

  @valid %{
    user_id:           "user_001",
    items:             [%{product_id: "p1", quantity: 2, price_cents: 1999}],
    payment_method_id: "pm_test",
    user_email:        "test@example.com"
  }

  test "completes all steps on success" do
    assert {:ok, ctx} = OrderSaga.run(@valid)
    assert is_binary(ctx.order_id)
    assert is_binary(ctx.reservation_id)
    assert is_binary(ctx.charge_id)
  end

  test "compensates when items list is empty" do
    ctx = Map.put(@valid, :items, [])
    assert {:compensated, :validate_order, :no_items} = OrderSaga.run(ctx)
  end

  test "calculates total correctly across items" do
    assert {:ok, ctx} = OrderSaga.run(@valid)
    expected = Enum.reduce(@valid.items, 0, fn i, acc -> acc + i.price_cents * i.quantity end)
    assert ctx.total_cents == expected
  end
end
```

## Testing schemas

```elixir
defmodule MyApp.Schemas.PaymentCreatedTest do
  use ExUnit.Case, async: true

  alias MyApp.Schemas.PaymentCreated

  @valid %{
    "payment_id"   => "pay_001",
    "order_id"     => "ord_001",
    "amount_cents" => 4999,
    "user_id"      => "user_001"
  }

  test "validates correct payload" do
    assert {:ok, _v} = PaymentCreated.validate(@valid)
  end

  test "applies default currency" do
    {:ok, v} = PaymentCreated.validate(@valid)
    assert v["currency"] == "USD"
  end

  test "fails on missing required fields" do
    assert {:error, errors} = PaymentCreated.validate(%{})
    assert Keyword.has_key?(errors, :payment_id)
  end

  test "migrates v1 (float) to v2 (cents)" do
    v1 = %{"payment_id" => "p", "order_id" => "o", "amount" => 49.99, "user_id" => "u"}
    m  = PaymentCreated.migrate(1, v1)
    assert m["amount_cents"] == 4999
    refute Map.has_key?(m, "amount")
  end
end
```

## Testing RPC

```elixir
defmodule MyApp.RPC.MathTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.Transport.Memory

  setup do
    Memory.clear()
    # Stub the RPC handler
    Memory.subscribe("math.sum", fn msg ->
      if msg.reply_to do
        PhoenixMicro.publish(msg.reply_to, Enum.sum(msg.payload),
          headers: %{"x-correlation-id" => msg.correlation_id})
      end
      :ok
    end, [])
    :ok
  end

  test "rpc returns sum" do
    assert {:ok, 6} = PhoenixMicro.rpc("math.sum", [1, 2, 3], timeout: 1_000)
  end

  test "rpc returns timeout when no reply" do
    assert {:error, :timeout} = PhoenixMicro.rpc("ghost", [], timeout: 100)
  end
end
```

## Testing middleware

```elixir
defmodule MyApp.Middleware.TenantContextTest do
  use ExUnit.Case, async: true

  alias PhoenixMicro.Message
  alias MyApp.Middleware.TenantContext

  test "sets tenant from header" do
    msg = Message.new("topic", %{}, headers: %{"x-tenant-id" => "acme"})
    parent = self()

    TenantContext.call(msg, fn _m ->
      send(parent, {:tenant, TenantContext.current()})
      :ok
    end)

    assert_receive {:tenant, "acme"}
  end

  test "uses default tenant when header absent" do
    msg = Message.new("topic", %{})
    parent = self()

    TenantContext.call(msg, fn _m ->
      send(parent, {:tenant, TenantContext.current()})
      :ok
    end)

    assert_receive {:tenant, "default"}
  end

  test "clears tenant context after call" do
    msg = Message.new("topic", %{}, headers: %{"x-tenant-id" => "acme"})
    TenantContext.call(msg, fn _m -> :ok end)
    assert TenantContext.current() == nil
  end
end
```

## Memory transport helpers reference

```elixir
alias PhoenixMicro.Transport.Memory

# All messages published in this test session
Memory.messages()
#=> [%PhoenixMicro.Message{topic: "payments.created", ...}, ...]

# Messages that reached the DLQ (after max retries exhausted)
Memory.dlq_messages()

# Reset all state — call in setup
Memory.clear()

# Block until N messages arrive on topic (raises on timeout)
Memory.wait_for_messages("payments.created", 1, timeout: 2_000)
Memory.wait_for_messages("payments.created", 3, timeout: 5_000)

# Subscribe directly (for RPC stubs or custom assertions)
Memory.subscribe("math.sum", fn msg -> :ok end, [])
```

## Using Mox for external dependencies

```elixir
# test/support/mocks.ex
Mox.defmock(MyApp.PaymentsMock, for: MyApp.Payments.Behaviour)

# test/my_app/payments/created_consumer_test.exs
import Mox

setup :verify_on_exit!

test "calls the payments module with correct args" do
  expect(MyApp.PaymentsMock, :process, fn 4999, "USD" -> {:ok, :processed} end)

  msg = Message.new("payments.created", %{
    "payment_id" => "p", "order_id" => "o",
    "amount_cents" => 4999, "currency" => "USD", "user_id" => "u"
  })

  assert :ok = MyApp.Payments.CreatedConsumer.handle(msg, %{})
end
```

## CI configuration

```yaml
# .github/workflows/ci.yml
- name: Run tests
  run: mix test
  env:
    # No broker env vars needed — using :memory transport
    MIX_ENV: test
```

The in-memory transport means CI requires **no external services at all**.
