# Sagas & Compensation

The Saga pattern provides distributed transaction semantics without two-phase commit. Each step runs sequentially; if any step fails, all previously completed steps are compensated in reverse order.

## Defining a saga

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
    compensate: fn ctx ->
      Inventory.release(ctx.reservation_id)
      :ok
    end,
    timeout: 5_000,   # ms, optional
    retries: 2        # step-level retries, optional

  step :charge_payment,
    execute: fn ctx ->
      case Payments.charge(ctx.user_id, ctx.amount_cents) do
        {:ok, c} -> {:ok, Map.put(ctx, :charge_id, c.id)}
        err      -> err
      end
    end,
    compensate: fn ctx ->
      Payments.refund(ctx.charge_id)
      :ok
    end

  step :send_confirmation_email,
    execute: fn ctx ->
      Mailer.deliver(ctx.user_email, :order_confirmed, ctx)
    end,
    compensate: fn _ctx -> :ok end  # emails can't be un-sent
end
```

## Running a saga

### Synchronous

```elixir
context = %{
  product_id: "prod_123",
  quantity: 2,
  user_id: "user_456",
  amount_cents: 4999,
  user_email: "user@example.com"
}

case MyApp.OrderSaga.run(context) do
  {:ok, final_ctx} ->
    # all steps succeeded; final_ctx has all accumulated data
    {:ok, final_ctx.charge_id}

  {:compensated, failed_step, reason} ->
    # a step failed; all previous steps were compensated
    {:error, {failed_step, reason}}
end
```

### Asynchronous

```elixir
{:ok, saga_id} = MyApp.OrderSaga.start(context)

# Poll for result
case MyApp.OrderSaga.status(saga_id) do
  {:ok, final_ctx}              -> # done
  {:compensated, step, reason}  -> # failed + rolled back
  {:running, step}              -> # still executing
end
```

## How compensation works

Given steps A → B → C:

1. A executes successfully
2. B executes successfully
3. C **fails**
4. B's compensate runs
5. A's compensate runs
6. Saga returns `{:compensated, :c, reason}`

The final failing step's compensate is **not** called (it didn't succeed).

## Telemetry events

| Event                                      | Metadata                                     |
| ------------------------------------------ | -------------------------------------------- |
| `[:phoenix_micro, :saga, :started]`        | `%{saga_id: id, steps: [...]}`               |
| `[:phoenix_micro, :saga, :step_started]`   | `%{saga_id: id, step: name}`                 |
| `[:phoenix_micro, :saga, :step_completed]` | `%{saga_id: id, step: name, duration: ns}`   |
| `[:phoenix_micro, :saga, :step_failed]`    | `%{saga_id: id, step: name, reason: reason}` |
| `[:phoenix_micro, :saga, :compensating]`   | `%{saga_id: id, step: name}`                 |
| `[:phoenix_micro, :saga, :compensated]`    | `%{saga_id: id}`                             |
| `[:phoenix_micro, :saga, :completed]`      | `%{saga_id: id, duration: ns}`               |

## Generate a saga

```bash
mix phoenix_micro.gen.saga MyApp.OrderSaga --steps reserve_inventory,charge_payment,send_email
```
