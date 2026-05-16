# Transports

PhoenixMicro supports five transports. Only one is active at a time (set via `:transport`).
Switching transports requires only a config change — no consumer or publisher code changes.

> **Zero native deps.** `phoenix_micro` itself compiles on any platform with no C compiler,
> no rebar3, and no native code. Kafka is fully built-in. NATS and Redis are pure Elixir.
> Only RabbitMQ requires a system toolchain (`escript.exe` / rebar3).

## Transport comparison

| Transport     | Dep to add to YOUR app | Linux/macOS | Windows                        | Notes                     |
| ------------- | ---------------------- | ----------- | ------------------------------ | ------------------------- |
| **Kafka**     | **none — built-in**    | ✅          | ✅                             | Pure Elixir wire protocol |
| **In-memory** | **none — built-in**    | ✅          | ✅                             | Dev/test only             |
| NATS          | `{:gnat, "~> 1.7"}`    | ✅          | ✅                             | Pure Elixir               |
| Redis Streams | `{:redix, "~> 1.5"}`   | ✅          | ✅                             | Pure Elixir               |
| RabbitMQ      | `{:amqp, "~> 3.3"}`    | ✅          | ⚠️ Needs `escript.exe` on PATH | Uses rebar3               |

---

## Kafka (built-in)

**No dependency required.** PhoenixMicro implements the Kafka binary wire protocol
natively over `:gen_tcp`. No `kafka_ex`, no `:brod`, no `crc32cer`, no C compiler.

### Supported Kafka APIs

| API Key | Name            | Used for                              |
| ------- | --------------- | ------------------------------------- |
| 0       | Produce         | Publishing messages                   |
| 1       | Fetch           | Consuming messages                    |
| 2       | ListOffsets     | Resolve `:latest`/`:earliest` offsets |
| 8       | OffsetCommit    | Commit consumer offsets               |
| 9       | OffsetFetch     | Fetch committed offsets               |
| 10      | FindCoordinator | Locate group coordinator broker       |
| 11      | JoinGroup       | Join consumer group                   |
| 12      | Heartbeat       | Keep group membership alive           |
| 13      | LeaveGroup      | Clean shutdown from group             |
| 14      | SyncGroup       | Receive partition assignment          |

### Configuration

```elixir
config :phoenix_micro,
  transport: :kafka,
  transports: [
    kafka: [
      # Broker connection (pick one):
      brokers: [{"localhost", 9092}],
      # OR: url: "kafka://broker1:9092,broker2:9092,broker3:9092",

      group_id:           "my_app",
      client_id:          "my_app_client",
      begin_offset:       :latest,       # :latest | :earliest | integer
      acks:               1,             # 0=none, 1=leader, -1=all replicas
      ack_timeout_ms:     5_000,
      max_bytes:          1_048_576,     # 1 MB per fetch
      fetch_wait_ms:      500,           # max block time per poll
      heartbeat_ms:       3_000,
      session_timeout_ms: 30_000
    ]
  ]
```

### Per-message publish options

```elixir
PhoenixMicro.publish("my.topic", payload)

# With options
PhoenixMicro.publish("my.topic", payload,
  partition: 2,          # target specific partition
  acks: -1,              # wait for all replicas
  ack_timeout: 10_000    # override default ack timeout
)
```

### Consumer with Kafka transport override

```elixir
defmodule MyApp.Consumers.PaymentConsumer do
  use PhoenixMicro.Consumer

  topic "payments.created"
  transport :kafka           # explicit — uses kafka even if default is :memory
  concurrency 10
  retry max_attempts: 5
  dead_letter_topic "payments.created.dlq"

  @impl PhoenixMicro.Consumer
  def handle(message, _ctx) do
    IO.inspect(message.metadata)  # %{offset: 42, partition: 0, group_id: "my_app"}
    :ok
  end
end
```

### Docker Compose

```yaml
services:
  kafka:
    image: bitnami/kafka:3.7
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_NODE_ID: "0"
      KAFKA_CFG_PROCESS_ROLES: "broker,controller"
      KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "0@kafka:9093"
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
```

### Multi-broker (production)

```elixir
config :phoenix_micro,
  transport: :kafka,
  transports: [
    kafka: [
      url: "kafka://broker1:9092,broker2:9092,broker3:9092",
      group_id: "my_app_prod",
      acks: -1,              # require all replicas for durability
      session_timeout_ms: 60_000
    ]
  ]
```

---

## NATS

**Add to YOUR app:** `{:gnat, "~> 1.7"}` — pure Elixir, no rebar3.

```elixir
config :phoenix_micro,
  transport: :nats,
  transports: [
    nats: [
      host:        "localhost",
      port:        4222,
      queue_group: "my_app",   # load-balance across instances
      username:    "user",     # optional
      password:    "pass",     # optional
      tls:         false       # optional
    ]
  ]
```

- Queue groups distribute load across consumer instances automatically
- Core NATS is fire-and-forget — ack/nack are local-only
- Supports `*` (single token) and `>` (multi-token) wildcards: `payments.*`, `events.>`
- Automatic reconnection with exponential backoff on connection loss
- Handler dispatch via `Task.Supervisor` — crashes never kill the transport

### Docker Compose

```yaml
services:
  nats:
    image: nats:2.10
    ports:
      - "4222:4222"
```

---

## Redis Streams

**Add to YOUR app:** `{:redix, "~> 1.5"}` — pure Elixir, no rebar3.

```elixir
config :phoenix_micro,
  transport: :redis_streams,
  transports: [
    redis_streams: [
      url:            "redis://localhost:6379",
      consumer_group: "my_app",
      consumer_name:  "node1",       # unique per node/instance
      batch_size:     10,
      block_ms:       1_000,         # XREADGROUP blocking timeout
      max_pending_ms: 60_000         # XAUTOCLAIM PEL recovery threshold
    ]
  ]
```

- Consumer groups via `XREADGROUP`/`XACK` — competing consumers out of the box
- PEL recovery: unacked messages reclaimed after `max_pending_ms` via `XAUTOCLAIM`
- DLQ routing to `dlq:<stream>` prefix stream

### Docker Compose

```yaml
services:
  redis:
    image: redis:7.2
    ports:
      - "6379:6379"
```

---

## RabbitMQ

**Add to YOUR app:** `{:amqp, "~> 3.3"}`

> ⚠️ **Windows:** `amqp` depends on `rabbit_common` which builds with rebar3. You need
> `escript.exe` (part of Erlang/OTP) on your `PATH`. On Linux/macOS this works automatically.

```elixir
config :phoenix_micro,
  transport: :rabbitmq,
  transports: [
    rabbitmq: [
      url:            "amqp://guest:guest@localhost",
      exchange:       "my_app",      # topic exchange name
      prefetch_count: 10,            # per-consumer QoS
      durable:        true
    ]
  ]
```

- Topic exchange with `#` (multi-word) and `*` (single-word) wildcards
- Publisher confirms for reliable publishing
- Dead-letter exchange (DLX) routing on NACK
- Automatic reconnection with exponential backoff
- Handler dispatch via `Task.Supervisor`
- `find_handler` correctly matches routing keys against subscription patterns

### Docker Compose

```yaml
services:
  rabbitmq:
    image: rabbitmq:3.13-management
    ports:
      - "5672:5672"
      - "15672:15672" # management UI
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
```

---

## In-memory (testing)

Built-in. No dependency. No external service. Always running alongside any real transport.

```elixir
# config/test.exs
config :phoenix_micro,
  transport: :memory,
  consumers: []
```

### Test helpers

```elixir
alias PhoenixMicro.Transport.Memory

Memory.messages()               # all published messages in this session
Memory.dlq_messages()           # all dead-lettered messages
Memory.clear()                  # reset state between tests

# Block until N messages arrive on a topic (or timeout)
Memory.wait_for_messages("payments.created", 1, timeout: 2_000)
```

### Wildcards

The memory transport supports NATS-style wildcards:

- `*` — matches a single dot-separated token: `payments.*` matches `payments.created`
- `>` — matches zero or more tokens: `events.>` matches `events.a.b.c`

---

## Switching transports

Because all consumer and publisher code is transport-agnostic, switching is a
one-line config change:

```elixir
# Was: config :phoenix_micro, transport: :rabbitmq
# Now: config :phoenix_micro, transport: :kafka
```

No consumer modules, handlers, or publish calls need to change.
