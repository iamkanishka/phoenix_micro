defmodule PhoenixMicro.Telemetry do
  @moduledoc """
  Centralised Telemetry event definitions and emission helpers.

  ## Events emitted

  | Event | Measurements | Metadata |
  |-------|-------------|----------|
  | `[:phoenix_micro, :message, :received]` | `%{count: 1}` | topic, transport, attempt |
  | `[:phoenix_micro, :message, :processed]` | `%{duration: t}` | topic, transport, consumer |
  | `[:phoenix_micro, :message, :failed]` | `%{count: 1}` | topic, reason, final |
  | `[:phoenix_micro, :message, :published]` | `%{count: 1}` | topic, transport, batched |
  | `[:phoenix_micro, :rpc, :request]` | `%{count: 1}` | topic, correlation_id |
  | `[:phoenix_micro, :rpc, :response]` | `%{duration: t}` | topic, correlation_id |
  | `[:phoenix_micro, :rpc, :timeout]` | `%{count: 1}` | topic, correlation_id |
  | `[:phoenix_micro, :transport, :connected]` | `%{count: 1}` | transport |
  | `[:phoenix_micro, :transport, :disconnected]` | `%{count: 1}` | transport, reason |

  ## Attaching a default logger handler

  In your application's `start/2`:

      PhoenixMicro.Telemetry.attach_default_logger()
  """

  @doc "Emits a message_received event."
  @spec message_received(String.t(), map()) :: :ok
  def message_received(topic, meta \\ %{}) do
    :telemetry.execute(
      [:phoenix_micro, :message, :received],
      %{count: 1},
      Map.merge(%{topic: topic}, meta)
    )
  end

  @doc "Emits a message_processed event with duration."
  @spec message_processed(String.t(), map()) :: :ok
  def message_processed(topic, meta \\ %{}) do
    :telemetry.execute(
      [:phoenix_micro, :message, :processed],
      %{duration: Map.get(meta, :duration, 0)},
      Map.merge(%{topic: topic}, meta)
    )
  end

  @doc "Emits a message_failed event."
  @spec message_failed(String.t(), map()) :: :ok
  def message_failed(topic, meta \\ %{}) do
    :telemetry.execute(
      [:phoenix_micro, :message, :failed],
      %{count: 1},
      Map.merge(%{topic: topic}, meta)
    )
  end

  @doc "Emits a message_published event."
  @spec message_published(String.t(), map()) :: :ok
  def message_published(topic, meta \\ %{}) do
    :telemetry.execute(
      [:phoenix_micro, :message, :published],
      %{count: 1},
      Map.merge(%{topic: topic}, meta)
    )
  end

  @doc "Emits an RPC request event."
  @spec rpc_request(String.t(), map()) :: :ok
  def rpc_request(topic, meta \\ %{}) do
    :telemetry.execute(
      [:phoenix_micro, :rpc, :request],
      %{count: 1},
      Map.merge(%{topic: topic}, meta)
    )
  end

  @doc "Emits an RPC response event with duration."
  @spec rpc_response(String.t(), map()) :: :ok
  def rpc_response(topic, meta \\ %{}) do
    :telemetry.execute(
      [:phoenix_micro, :rpc, :response],
      %{duration: Map.get(meta, :duration, 0)},
      Map.merge(%{topic: topic}, meta)
    )
  end

  @doc "Emits an RPC timeout event."
  @spec rpc_timeout(String.t(), map()) :: :ok
  def rpc_timeout(topic, meta \\ %{}) do
    :telemetry.execute(
      [:phoenix_micro, :rpc, :timeout],
      %{count: 1},
      Map.merge(%{topic: topic}, meta)
    )
  end

  @doc "Emits a transport_connected event."
  @spec transport_connected(atom()) :: :ok
  def transport_connected(transport) do
    :telemetry.execute(
      [:phoenix_micro, :transport, :connected],
      %{count: 1},
      %{transport: transport}
    )
  end

  @doc "Emits a transport_disconnected event."
  @spec transport_disconnected(atom(), term()) :: :ok
  def transport_disconnected(transport, reason) do
    :telemetry.execute(
      [:phoenix_micro, :transport, :disconnected],
      %{count: 1},
      %{transport: transport, reason: reason}
    )
  end

  # ---------------------------------------------------------------------------
  # Default logger handler
  # ---------------------------------------------------------------------------

  @doc """
  Attaches a default Logger-based Telemetry handler for all PhoenixMicro events.
  Useful for development; in production prefer structured metrics exporters.
  """
  @spec attach_default_logger(keyword()) :: :ok
  def attach_default_logger(opts \\ []) do
    level = Keyword.get(opts, :level, :info)

    events = [
      [:phoenix_micro, :message, :received],
      [:phoenix_micro, :message, :processed],
      [:phoenix_micro, :message, :failed],
      [:phoenix_micro, :message, :published],
      [:phoenix_micro, :rpc, :request],
      [:phoenix_micro, :rpc, :response],
      [:phoenix_micro, :rpc, :timeout],
      [:phoenix_micro, :transport, :connected],
      [:phoenix_micro, :transport, :disconnected]
    ]

    _attach =
      :telemetry.attach_many(
        "phoenix_micro_default_logger",
        events,
        &__MODULE__.handle_event/4,
        %{level: level}
      )

    :ok
  end

  @doc false
  def handle_event(event, measurements, metadata, %{level: level}) do
    require Logger

    event_name = event |> Enum.map(&to_string/1) |> Enum.join(".")

    Logger.log(
      level,
      "[Telemetry] #{event_name} measurements=#{inspect(measurements)} meta=#{inspect(metadata)}"
    )
  end

  # ---------------------------------------------------------------------------
  # Metrics definitions (for use with Telemetry.Metrics)
  # ---------------------------------------------------------------------------

  @doc """
  Returns a list of `Telemetry.Metrics` metric definitions.

  ## Usage with Phoenix

      # In your Telemetry module
      def metrics do
        PhoenixMicro.Telemetry.metrics() ++ [
          # your other metrics
        ]
      end
  """
  @spec metrics() :: [struct()]
  def metrics do
    import Telemetry.Metrics

    [
      counter("phoenix_micro.message.received.count",
        tags: [:topic, :transport],
        description: "Total messages received"
      ),
      sum("phoenix_micro.message.processed.count",
        event_name: [:phoenix_micro, :message, :processed],
        measurement: fn _measurements -> 1 end,
        tags: [:topic, :transport]
      ),
      distribution("phoenix_micro.message.processed.duration",
        measurement: :duration,
        tags: [:topic],
        unit: {:native, :millisecond}
      ),
      counter("phoenix_micro.message.failed.count",
        tags: [:topic, :transport]
      ),
      counter("phoenix_micro.message.published.count",
        tags: [:topic, :transport]
      ),
      counter("phoenix_micro.rpc.request.count",
        tags: [:topic]
      ),
      distribution("phoenix_micro.rpc.response.duration",
        measurement: :duration,
        tags: [:topic],
        unit: {:native, :millisecond}
      ),
      counter("phoenix_micro.rpc.timeout.count",
        tags: [:topic]
      )
    ]
  end
end
