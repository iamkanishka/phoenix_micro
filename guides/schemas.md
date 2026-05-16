# Schema Registry

PhoenixMicro's schema registry provides versioned, typed message contracts with
automatic migration between versions. Schemas are defined once, auto-registered
at compile time, and decoded transparently in any consumer.

## Defining a schema

```elixir
defmodule MyApp.Schemas.PaymentCreated do
  use PhoenixMicro.Schema

  schema_version 2
  topic "payments.created"

  field :payment_id,   :string,  required: true
  field :order_id,     :string,  required: true
  field :amount_cents, :integer, required: true
  field :currency,     :string,  required: true, default: "USD"
  field :user_id,      :string,  required: true
  field :metadata,     :map,     required: false

  @doc "Migrate a v1 payload (float amount) to v2 (integer cents)."
  def migrate(1, payload) do
    cents = payload |> Map.get("amount", 0.0) |> Kernel.*(100) |> round()
    payload |> Map.delete("amount") |> Map.put("amount_cents", cents)
  end
end
```

## Field types

| Type       | Elixir type  | Description           |
| ---------- | ------------ | --------------------- |
| `:string`  | `String.t()` | UTF-8 binary          |
| `:integer` | `integer()`  | Any integer           |
| `:float`   | `float()`    | Any float             |
| `:boolean` | `boolean()`  | `true` / `false`      |
| `:map`     | `map()`      | Any nested map        |
| `:list`    | `list()`     | Any list              |
| `:atom`    | `atom()`     | Atom (string in JSON) |
| `:any`     | `term()`     | No type validation    |

## Field options

| Option      | Type      | Default | Description                   |
| ----------- | --------- | ------- | ----------------------------- |
| `required:` | `boolean` | `false` | Validation fails if absent    |
| `default:`  | `term`    | `nil`   | Used when the field is absent |

## Validating a payload

```elixir
case MyApp.Schemas.PaymentCreated.validate(payload) do
  {:ok, validated} ->
    # validated: payload with defaults applied and types verified
    process(validated)

  {:error, errors} ->
    # errors: keyword list [{:field_name, "reason"}, ...]
    Logger.error("Validation failed: #{inspect(errors)}")
    {:error, :invalid_payload}
end
```

## Decoding in a consumer (with auto-migration)

`Schema.decode/2` reads the `x-schema-version` message header to determine the
source version, applies all necessary `migrate/2` calls, and validates the result.

```elixir
defmodule MyApp.Consumers.PaymentConsumer do
  use PhoenixMicro.Consumer

  topic "payments.created"

  @impl PhoenixMicro.Consumer
  def handle(message, _ctx) do
    case PhoenixMicro.Schema.decode(MyApp.Schemas.PaymentCreated, message.payload) do
      {:ok, payload} ->
        # payload is validated and at current schema_version
        MyApp.Payments.process(payload)

      {:error, {:incompatible_version, got, supported}} ->
        Logger.error("Unsupported schema version #{got}, supported: #{inspect(supported)}")
        {:error, :unsupported_version}

      {:error, errors} ->
        {:error, errors}
    end
  end
end
```

## Publishing with schema validation

```elixir
payload = %{
  payment_id:   "pay_001",
  order_id:     "ord_001",
  amount_cents: 4999,
  user_id:      "user_001"
}

case PhoenixMicro.Schema.publish(MyApp.Schemas.PaymentCreated, payload) do
  :ok              -> :ok
  {:error, errors} -> {:error, errors}
end
```

`Schema.publish/2` validates the payload, sets the `x-schema-version` header
automatically, then publishes to the schema's registered topic.

## Schema versioning strategy

### When to increment the version

| Change             | Action                                          |
| ------------------ | ----------------------------------------------- |
| Add optional field | Same version — backward compatible              |
| Add required field | **Increment version** — old producers omit it   |
| Remove field       | **Increment version** — old consumers expect it |
| Rename field       | **Increment version** — add `migrate/2`         |
| Change field type  | **Increment version** — add `migrate/2`         |

### Multi-version migration chain

```elixir
defmodule MyApp.Schemas.OrderPlaced do
  use PhoenixMicro.Schema

  schema_version 3
  topic "orders.placed"

  field :order_id,    :string,  required: true
  field :user_id,     :string,  required: true   # was :customer_id in v1
  field :total_cents, :integer, required: true   # was :total (float) in v2
  field :currency,    :string,  required: true,  default: "USD"
  field :items,       :list,    required: true

  # v1 → v2: rename customer_id to user_id
  def migrate(1, payload) do
    payload
    |> Map.put("user_id", Map.get(payload, "customer_id"))
    |> Map.delete("customer_id")
    |> migrate(2)   # chain to next migration
  end

  # v2 → v3: convert total (float) to total_cents (integer)
  def migrate(2, payload) do
    cents = round(Map.get(payload, "total", 0) * 100)
    payload
    |> Map.put("total_cents", cents)
    |> Map.delete("total")
  end
end
```

`Schema.decode/2` chains `migrate/2` calls automatically:
`migrate(1, payload) → migrate(2, result) → validate(result)`

## Compatible versions declaration

```elixir
use PhoenixMicro.Schema

schema_version 2
topic "payments.created"
compatible_with [1]   # can decode v1 payloads via migrate/2
```

## Registry API

Schemas auto-register at compile time via `@after_compile`. Query at runtime:

```elixir
# Latest version for a topic
{:ok, module}  = PhoenixMicro.Schema.Registry.lookup("payments.created")

# All registered schemas
modules = PhoenixMicro.Schema.Registry.all()

# All versions for a topic, ordered oldest → newest
versions = PhoenixMicro.Schema.Registry.versions("payments.created")
# => [{1, MyApp.Schemas.PaymentCreatedV1}, {2, MyApp.Schemas.PaymentCreated}]
```

## Testing schemas

```elixir
defmodule MyApp.Schemas.PaymentCreatedTest do
  use ExUnit.Case, async: true

  alias MyApp.Schemas.PaymentCreated

  @valid %{
    "payment_id" => "pay_001",
    "order_id"   => "ord_001",
    "amount_cents" => 4999,
    "user_id"    => "user_001"
  }

  test "validates a correct payload" do
    assert {:ok, _validated} = PaymentCreated.validate(@valid)
  end

  test "applies default currency" do
    assert {:ok, v} = PaymentCreated.validate(@valid)
    assert v["currency"] == "USD"
  end

  test "rejects missing required fields" do
    assert {:error, errors} = PaymentCreated.validate(%{})
    assert Keyword.has_key?(errors, :payment_id)
    assert Keyword.has_key?(errors, :amount_cents)
  end

  test "rejects non-map payload" do
    assert {:error, _errors} = PaymentCreated.validate("not a map")
    assert {:error, _errors} = PaymentCreated.validate(nil)
  end

  test "migrates v1 to v2" do
    v1 = Map.put(@valid, "amount", 49.99) |> Map.delete("amount_cents")
    migrated = PaymentCreated.migrate(1, v1)
    assert migrated["amount_cents"] == 4999
    refute Map.has_key?(migrated, "amount")
  end
end
```
