# Kafka Transport Guide

This guide covers everything you need to connect `phoenix_micro` to Apache
Kafka — from a local Docker setup to production multi-broker clusters.

## Why no Kafka dep in phoenix_micro?

`phoenix_micro` lists **zero** Kafka dependencies in its own `mix.exs`.
This is intentional: every available Kafka client for Elixir pulls in native C
or C++ code that requires a compiler toolchain:

| Client             | Native dep        | Needed for         |
| ------------------ | ----------------- | ------------------ |
| `kafka_ex ~> 0.13` | `:crc32cer` (C)   | CRC32 checksums    |
| `brod ~> 3.17`     | `:snappyer` (C++) | Snappy compression |

On **Windows** without Visual Studio Build Tools, these cause the `cl.exe not
found` error you may have seen. By keeping them out of `phoenix_micro`'s deps,
the library compiles cleanly everywhere. You add the Kafka client only to _your_
application, only when you actually need Kafka.

---

## Step-by-step setup

### Step 1 — Choose a client

Add exactly one of these to **your application's** `mix.exs`:

```elixir
# Option A — kafka_ex (works on all platforms with build tools)
{:kafka_ex, "~> 0.13"}

# Option B — brod (Linux/macOS only)
{:brod, "~> 3.17"}
```

> #### Windows + kafka_ex {: .tip}
>
> `kafka_ex` needs `:crc32cer` which is written in C. To compile it on Windows:
>
> 1. Download [Visual Studio Build Tools 2022](https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2022)
> 2. Run the installer and select **"Desktop development with C++"**
> 3. Restart your terminal
> 4. Run `mix deps.compile crc32cer --force`
>
> Alternatively, use **RabbitMQ**, **NATS**, or **Redis Streams** — they
> compile on Windows with no C toolchain at all.

### Step 2 — Configure kafka_ex

```elixir
# config/config.exs
config :kafka_ex,
  brokers: [{"localhost", 9092}],
  consumer_group: "my_app_consumers",
  use_ssl: false,
  required_acks: 1,
  ack_timeout_ms: 3_000,
  # Use "kayrock" for Kafka 0.11+ (recommended)
  # Use "0.9.0"   for older Kafka clusters
  kafka_version: "kayrock",
  # How long to wait for messages when polling (ms)
  fetch_wait_max_ms: 100
```

### Step 3 — Configure PhoenixMicro

```elixir
# config/config.exs
config :phoenix_micro,
  transport: :kafka,
  transports: [
    kafka: [
      # URL form — easiest to read
      url: "kafka://localhost:9092",

      # Or explicit list (choose one, not both)
      # brokers: [{"localhost", 9092}],

      group_id:     "my_app_consumers",   # consumer group
      client_id:    "my_app",             # identifies this app in Kafka
      begin_offset: :latest               # :latest | :earliest | integer
    ]
  ]
```

### Step 4 — Start a local Kafka

```yaml
# docker-compose.yml
version: "3.9"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_RETENTION_HOURS: 24
```

```bash
docker-compose up -d
# Check it's running:
docker-compose logs kafka | grep "started"
```

### Step 5 — Define a consumer

```elixir
defmodule MyApp.Consumers.PaymentConsumer do
  use PhoenixMicro.Consumer

  topic "payments.created"
  transport :kafka
  concurrency 5

  retry max_attempts: 3,
        base_delay: 1_000,
        max_delay: 30_000,
        jitter: true

  middleware [
    PhoenixMicro.Middleware.Logger,
    PhoenixMicro.Middleware.Metrics
  ]

  dead_letter_topic "payments.created.dlq"

  def handle(message, _ctx) do
    %{"payment_id" => id, "amount_cents" => amount} = message.payload
    MyApp.Payments.record(id, amount)
    :ok
  end
end
```

### Step 6 — Register in config

```elixir
# config/config.exs
config :phoenix_micro,
  consumers: [MyApp.Consumers.PaymentConsumer]
```

Or register dynamically at runtime:

```elixir
{:ok, _pid} = PhoenixMicro.register_consumer(MyApp.Consumers.PaymentConsumer)
```

---

## Publishing to Kafka

```elixir
# Async (default)
PhoenixMicro.publish("payments.created", %{
  payment_id: "pay_abc123",
  amount_cents: 9999,
  currency: "USD"
})

# Sync — waits for broker acknowledgment
:ok = PhoenixMicro.publish_sync("payments.created", %{payment_id: "pay_abc123"})

# With options
PhoenixMicro.publish("payments.created", payload,
  partition: 2,    # target partition (default: 0)
  sync: true       # wait for ack
)

# Batch publish
PhoenixMicro.publish_batch([
  {"payments.created", %{payment_id: "pay_1", amount_cents: 100}},
  {"payments.created", %{payment_id: "pay_2", amount_cents: 200}},
  {"orders.placed",    %{order_id: "ord_1"}}
])
```

---

## Multi-broker production setup

```elixir
# config/runtime.exs
config :kafka_ex,
  brokers: [
    {System.get_env("KAFKA_BROKER_1", "b1.example.com"), 9092},
    {System.get_env("KAFKA_BROKER_2", "b2.example.com"), 9092},
    {System.get_env("KAFKA_BROKER_3", "b3.example.com"), 9092}
  ],
  consumer_group: System.get_env("KAFKA_GROUP_ID", "my_app_prod"),
  use_ssl: System.get_env("KAFKA_USE_SSL", "false") == "true",
  required_acks: 1,
  ack_timeout_ms: 5_000,
  kafka_version: "kayrock"

config :phoenix_micro,
  transport: :kafka,
  transports: [
    kafka: [
      # Multi-broker URL (comma-separated)
      url: System.get_env("KAFKA_URL", "kafka://b1.example.com:9092,b2.example.com:9092"),
      group_id:  System.get_env("KAFKA_GROUP_ID", "my_app_prod"),
      client_id: System.get_env("KAFKA_CLIENT_ID", "my_app"),
      begin_offset: :latest,
      pool_size: String.to_integer(System.get_env("KAFKA_POOL_SIZE", "10"))
    ]
  ]
```

---

## Dead-letter queue

Messages that fail after all retry attempts are published to
`<original_topic>.dlq` automatically:

```elixir
defmodule MyApp.Consumers.PaymentDLQ do
  use PhoenixMicro.Consumer

  topic "payments.created.dlq"
  transport :kafka
  concurrency 2

  def handle(message, _ctx) do
    # Inspect why it failed
    reason = message.headers["x-nack-reason"]
    MyApp.Alerting.notify(:dlq_message, %{topic: message.topic, reason: reason})
    MyApp.Repo.insert!(%MyApp.DeadLetter{message_id: message.id, reason: reason})
    :ok
  end
end
```

---

## Schema validation with Kafka

Combine the Schema DSL with Kafka to enforce contracts at the consumer:

```elixir
defmodule MyApp.Events.PaymentCreated do
  use PhoenixMicro.Schema

  schema_version 2
  topic "payments.created"

  field :payment_id,   :string,  required: true
  field :amount_cents, :integer, required: true
  field :currency,     :string,  required: true, default: "USD"

  compatible_with [1]

  def migrate(1, payload) do
    amount = Map.get(payload, "amount", 0)
    payload |> Map.delete("amount") |> Map.put("amount_cents", round(amount * 100))
  end
end

defmodule MyApp.Consumers.PaymentConsumer do
  use PhoenixMicro.Consumer
  topic "payments.created"
  transport :kafka

  def handle(message, _ctx) do
    # Auto-migrates v1 payloads to v2 before validation
    case PhoenixMicro.Schema.decode(MyApp.Events.PaymentCreated, message.payload, message.headers) do
      {:ok, event} ->
        process(event)
        :ok

      {:error, {:schema_validation_failed, errors}} ->
        Logger.warning("Schema invalid: #{inspect(errors)}")
        :nack  # → DLQ immediately, no retry
    end
  end
end
```

---

## Testing with Kafka

Use Memory transport in tests — no Kafka broker needed:

```elixir
# config/test.exs
config :phoenix_micro,
  transport: :memory,
  consumers: []
```

```elixir
defmodule MyApp.PaymentConsumerTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.{Consumer, Message}

  setup do
    start_supervised!({PhoenixMicro.Transport.Memory, []})
    :ok
  end

  test "handles valid payment" do
    msg = Message.new("payments.created", %{
      "payment_id" => "pay_1",
      "amount_cents" => 999,
      "currency" => "USD"
    })

    ctx = %{
      transport: :memory,
      topic: msg.topic,
      attempt: 1,
      transport_mod: PhoenixMicro.Transport.Memory,
      message: msg
    }

    assert :ok = Consumer.dispatch(MyApp.Consumers.PaymentConsumer, msg, ctx)
  end

  test "routes invalid schema to DLQ" do
    msg = Message.new("payments.created", %{"bad" => "payload"})
    ctx = %{transport: :memory, topic: msg.topic, attempt: 1,
            transport_mod: PhoenixMicro.Transport.Memory, message: msg}

    # Consumer should nack invalid schema
    assert :nack = Consumer.dispatch(MyApp.Consumers.PaymentConsumer, msg, ctx)

    # Verify DLQ received it
    dlq_msgs = PhoenixMicro.Transport.Memory.dlq_messages()
    assert length(dlq_msgs) == 1
  end
end
```

---

## Troubleshooting

### `cl.exe not found` (Windows)

```
** (Mix) Could not compile dependency :crc32cer
```

**Fix:** Install [Visual Studio Build Tools 2022](https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2022),
select **"Desktop development with C++"**, restart terminal, run:

```bash
mix deps.compile crc32cer --force
```

**Easier fix:** Use RabbitMQ, NATS, or Redis Streams instead:

```elixir
config :phoenix_micro, transport: :rabbitmq
```

### `No Kafka client found` warning at startup

```
[PhoenixMicro.Transport.Kafka] No Kafka client found.
```

You configured `:kafka` as the transport but didn't add `kafka_ex` or `brod`
to your app's `mix.exs`. Add one:

```elixir
{:kafka_ex, "~> 0.13"}
```

### Connection timeouts

```
[Kafka] Connect failed: :timeout, retrying in 3142ms
```

Kafka is not reachable. Check:

```bash
# Is Kafka running?
docker ps | grep kafka

# Can you reach port 9092?
telnet localhost 9092

# Check advertised listeners match what you're connecting to
docker logs kafka | grep ADVERTISED
```

### Messages not being received

1. Check `begin_offset: :earliest` to read from the start of the topic
2. Verify the consumer group ID matches your `config :kafka_ex, consumer_group:`
3. Ensure the topic exists: `docker exec kafka kafka-topics --list --bootstrap-server localhost:9092`

### Creating topics manually

```bash
docker exec kafka kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic payments.created
```
