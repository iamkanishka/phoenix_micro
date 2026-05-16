# Outbox Pattern

The outbox pattern guarantees that a message is published if and only if a database transaction commits — preventing the "dual-write" race condition.

## The problem

```elixir
# WRONG — race condition
Repo.insert!(order)
PhoenixMicro.publish("orders.placed", %{id: order.id})  # could fail or be lost
```

## The solution

```elixir
# CORRECT — atomic
Repo.transaction(fn ->
  order = Repo.insert!(Order.changeset(%Order{}, params))
  :ok = PhoenixMicro.Outbox.enqueue("orders.placed", %{id: order.id})
end)
```

If the transaction commits, the message row is guaranteed to exist.
The Relay GenServer polls for unrelayed rows and publishes them.

## Setup

### 1. Generate the migration

```bash
mix phoenix_micro.gen.migration
mix ecto.migrate
```

This creates an `outbox_messages` table with columns:
`id`, `topic`, `payload`, `headers`, `attempt`, `relayed_at`, `failed_at`, `last_error`, `inserted_at`, `updated_at`

### 2. Configure

```elixir
config :phoenix_micro,
  outbox: [
    schema: MyApp.OutboxMessage,
    repo: MyApp.Repo,
    poll_interval_ms: 1_000,
    batch_size: 100,
    max_attempts: 5
  ]
```

### 3. Create the Ecto schema (optional — use the generated one)

```elixir
defmodule MyApp.OutboxMessage do
  use Ecto.Schema

  schema "outbox_messages" do
    field :topic,      :string
    field :payload,    :map
    field :headers,    :map,     default: %{}
    field :attempt,    :integer, default: 0
    field :relayed_at, :utc_datetime_usec
    field :failed_at,  :utc_datetime_usec
    field :last_error, :string
    timestamps()
  end
end
```

### 4. Use in transactions

```elixir
def place_order(params) do
  Repo.transaction(fn ->
    order = Repo.insert!(Order.changeset(%Order{}, params))

    :ok = PhoenixMicro.Outbox.enqueue(
      "orders.placed",
      %{id: order.id, user_id: order.user_id, total: order.total},
      headers: %{"x-source" => "web"}
    )

    order
  end)
end
```

## Relay behaviour

- Polls every `poll_interval_ms` for rows where `relayed_at IS NULL AND failed_at IS NULL`
- Publishes via the active transport
- On success: sets `relayed_at`
- On failure: increments `attempt`, sets `last_error`
- After `max_attempts`: sets `failed_at` (row is abandoned, emit telemetry alert)

## Monitoring

```elixir
# Rows stuck in the outbox (not yet relayed)
pending = Repo.aggregate(MyApp.OutboxMessage, :count, :id,
  where: [relayed_at: nil, failed_at: nil])

# Failed rows (exhausted retries)
failed = Repo.aggregate(MyApp.OutboxMessage, :count, :id,
  where: [failed_at: {:not, nil}])
```
