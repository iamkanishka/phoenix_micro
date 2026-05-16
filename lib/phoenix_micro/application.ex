defmodule PhoenixMicro.Application do
  @moduledoc """
  OTP Application entry point for `phoenix_micro`.

  Starts the full supervision tree:

      PhoenixMicro.Supervisor (one_for_one)
      ├── Registry
      ├── CircuitBreaker.Store
      ├── Schema.Registry
      ├── Phoenix.MetricsStore
      ├── Transport.Memory (always)
      ├── Transport.* (configured)
      ├── Producer
      ├── RPC
      ├── ConsumerManager (DynamicSupervisor)
      └── Saga.Supervisor (DynamicSupervisor)

  After the tree starts, any consumers listed in config are auto-registered:

      config :phoenix_micro,
        consumers: [MyApp.Payments.CreatedConsumer, MyApp.Orders.PlacedConsumer]
  """

  use Application

  require Logger

  @impl Application
  def start(_type, _args) do
    Logger.info("[PhoenixMicro] Starting v#{version()} supervision tree")

    case PhoenixMicro.Supervisor.start_link([]) do
      {:ok, pid} ->
        auto_register_consumers()
        {:ok, pid}

      {:error, reason} = err ->
        Logger.error("[PhoenixMicro] Failed to start: #{inspect(reason)}")
        err
    end
  end

  @impl Application
  def stop(_state) do
    Logger.info("[PhoenixMicro] Stopped")
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp auto_register_consumers do
    consumers = Application.get_env(:phoenix_micro, :consumers, [])

    Enum.each(consumers, fn module ->
      case PhoenixMicro.Supervisor.ConsumerManager.start_consumer(module) do
        {:ok, _pid} ->
          Logger.info("[PhoenixMicro] Auto-registered consumer #{inspect(module)}")

        {:error, reason} ->
          Logger.error(
            "[PhoenixMicro] Failed to auto-register #{inspect(module)}: #{inspect(reason)}"
          )
      end
    end)
  end

  defp version do
    to_string(Application.spec(:phoenix_micro, :vsn))
  rescue
    _e -> "dev"
  end
end
