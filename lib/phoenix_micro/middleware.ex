defmodule PhoenixMicro.Middleware do
  @moduledoc """
  Behaviour for pluggable message-processing middleware.

  Middleware modules intercept messages as they flow into consumer handlers,
  allowing cross-cutting concerns (logging, metrics, tracing, auth) to be
  composed without modifying handler logic.

  ## Interface

      defmodule MyApp.AuthMiddleware do
        @behaviour PhoenixMicro.Middleware

        @impl PhoenixMicro.Middleware
        def call(%PhoenixMicro.Message{} = message, next) do
          if authorized?(message.headers) do
            next.(message)
          else
            {:error, :unauthorized}
          end
        end
      end

  ## Composing middleware

  Middleware is specified on a consumer using the `middleware/1` macro:

      defmodule MyConsumer do
        use PhoenixMicro.Consumer

        topic "payments.*"
        middleware [MyApp.AuthMiddleware, PhoenixMicro.Middleware.Logger]
      end

  Middleware is executed **left to right** (outermost first).
  """

  alias PhoenixMicro.Message

  @type next :: (Message.t() -> :ok | {:error, term()})

  @callback call(Message.t(), next()) :: :ok | {:error, term()}
end

# ---------------------------------------------------------------------------
# Built-in: Logger
# ---------------------------------------------------------------------------

defmodule PhoenixMicro.Middleware.Logger do
  @moduledoc """
  Logs message receipt and processing outcome at the `:debug` level.
  Logs failures at the `:warning` level.
  """

  @behaviour PhoenixMicro.Middleware

  require Logger

  alias PhoenixMicro.Message

  @impl PhoenixMicro.Middleware
  def call(%Message{} = message, next) do
    Logger.debug(
      "[PhoenixMicro] Received #{message.topic} id=#{message.id} attempt=#{message.attempt}"
    )

    start = System.monotonic_time(:microsecond)
    result = next.(message)
    duration = System.monotonic_time(:microsecond) - start

    case result do
      :ok ->
        Logger.debug(
          "[PhoenixMicro] Processed #{message.topic} id=#{message.id} in #{duration}µs"
        )

      {:error, reason} ->
        Logger.warning(
          "[PhoenixMicro] Failed #{message.topic} id=#{message.id} in #{duration}µs — #{inspect(reason)}"
        )
    end

    result
  end
end

# ---------------------------------------------------------------------------
# Built-in: Metrics (Telemetry)
# ---------------------------------------------------------------------------

defmodule PhoenixMicro.Middleware.Metrics do
  @moduledoc """
  Emits Telemetry span events for each message: `:start`, `:stop`, `:exception`.
  Compatible with `Telemetry.Metrics` and `TelemetryMetricsStatsd` etc.
  """

  @behaviour PhoenixMicro.Middleware

  alias PhoenixMicro.Message

  @impl PhoenixMicro.Middleware
  def call(%Message{} = message, next) do
    metadata = %{topic: message.topic, attempt: message.attempt, id: message.id}

    :telemetry.span(
      [:phoenix_micro, :message],
      metadata,
      fn ->
        result = next.(message)
        {result, Map.put(metadata, :result, result)}
      end
    )
  end
end

# ---------------------------------------------------------------------------
# Built-in: Retry (middleware-level, for transports that need it)
# ---------------------------------------------------------------------------

defmodule PhoenixMicro.Middleware.Retry do
  @moduledoc """
  Middleware-level retry with exponential backoff.
  This is complementary to Consumer-level retry — use this when you want
  to retry at the middleware layer (e.g. transient DB errors) before
  propagating failure to the Consumer's retry logic.

  ## Options (pass in consumer middleware list as `{Retry, max: 2}`):

      middleware [{PhoenixMicro.Middleware.Retry, max: 2, base_delay: 100}]
  """

  @behaviour PhoenixMicro.Middleware

  require Logger

  alias PhoenixMicro.Consumer.RetryScheduler
  alias PhoenixMicro.Message

  @impl PhoenixMicro.Middleware
  def call(%Message{} = message, next, opts \\ []) do
    max_attempts = Keyword.get(opts, :max, 3)

    retry_opts = [
      base_delay: Keyword.get(opts, :base_delay, 200),
      max_delay: Keyword.get(opts, :max_delay, 5_000),
      jitter: true
    ]

    do_call(message, next, max_attempts, 1, retry_opts)
  end

  defp do_call(message, next, max, attempt, opts) do
    case next.(message) do
      :ok ->
        :ok

      {:error, reason} when attempt < max ->
        delay = RetryScheduler.next_delay(attempt, opts)

        Logger.debug(
          "[Middleware.Retry] Retrying attempt #{attempt + 1} in #{delay}ms: #{inspect(reason)}"
        )

        Process.sleep(delay)
        do_call(Message.increment_attempt(message), next, max, attempt + 1, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end
end

# ---------------------------------------------------------------------------
# Built-in: Tracing (OpenTelemetry-compatible)
# ---------------------------------------------------------------------------

defmodule PhoenixMicro.Middleware.Tracing do
  @moduledoc """
  Distributed tracing middleware. Propagates trace context from message headers
  and creates a span for each processed message.

  Designed to work with `:opentelemetry` if available, but degrades gracefully
  to a no-op if OTel is not in the application.
  """

  @behaviour PhoenixMicro.Middleware

  alias PhoenixMicro.Message

  @impl PhoenixMicro.Middleware
  def call(%Message{} = message, next) do
    if Code.ensure_loaded?(:otel_tracer) do
      trace_with_otel(message, next)
    else
      next.(message)
    end
  end

  defp trace_with_otel(message, next) do
    span_name = "phoenix_micro.consume #{message.topic}"

    attrs = [
      {"messaging.system", "phoenix_micro"},
      {"messaging.destination", message.topic},
      {"messaging.message_id", message.id},
      {"messaging.operation", "receive"}
    ]

    # All OTel calls via apply/3 — :otel_tracer and :otel_span are Erlang modules
    # that don't exist at compile time unless opentelemetry is a hard dep.
    apply(:otel_tracer, :with_span, [
      span_name,
      %{attributes: attrs},
      fn _span ->
        result = next.(message)

        case result do
          :ok -> apply(:otel_span, :set_status, [:ok, ""])
          {:error, reason} -> apply(:otel_span, :set_status, [:error, inspect(reason)])
        end

        result
      end
    ])
  rescue
    _e -> next.(message)
  end
end

# ---------------------------------------------------------------------------
# Built-in: Idempotency
# ---------------------------------------------------------------------------

defmodule PhoenixMicro.Middleware.Idempotency do
  @moduledoc """
  Deduplication middleware. Checks whether a message ID has already been
  successfully processed using a configured `PhoenixMicro.IdempotencyStore`.

  Skips processing (returns `:ok`) if the message was already processed.
  Marks the message as processed after successful handling.

  Configure the store in your application config:

      config :phoenix_micro,
        idempotency_store: MyApp.IdempotencyStore  # implements the behaviour

  ## Built-in ETS store

  A simple ETS-backed store is provided for development/testing:

      idempotency_store: PhoenixMicro.Middleware.Idempotency.ETSStore
  """

  @behaviour PhoenixMicro.Middleware

  require Logger

  alias PhoenixMicro.{Config, Message}

  @impl PhoenixMicro.Middleware
  def call(%Message{} = message, next) do
    store = Config.get(:idempotency_store)

    if store && store.seen?(message.id) do
      Logger.debug("[Idempotency] Skipping duplicate message #{message.id}")
      :ok
    else
      result = next.(message)

      if store && result == :ok do
        store.mark_seen(message.id)
      end

      result
    end
  end
end

defmodule PhoenixMicro.IdempotencyStore do
  @moduledoc "Behaviour for idempotency stores."

  @callback seen?(String.t()) :: boolean()
  @callback mark_seen(String.t()) :: :ok
end

defmodule PhoenixMicro.Middleware.Idempotency.ETSStore do
  @moduledoc "ETS-backed idempotency store for development and testing."

  @behaviour PhoenixMicro.IdempotencyStore
  @table :phoenix_micro_idempotency

  def child_spec(_opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, []}, type: :worker}
  end

  def start_link do
    _table = :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    :ignore
  end

  @impl PhoenixMicro.IdempotencyStore
  def seen?(id), do: :ets.member(@table, id)

  @impl PhoenixMicro.IdempotencyStore
  def mark_seen(id) do
    :ets.insert(@table, {id, :os.system_time(:second)})
    :ok
  end
end
