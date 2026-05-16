defmodule PhoenixMicro.Config do
  @moduledoc """
  Centralised configuration loader and validator for `phoenix_micro`.

  Configuration lives in `config/*.exs`:

      config :phoenix_micro,
        transport: :rabbitmq,
        transports: [
          rabbitmq: [url: "amqp://localhost"],
          kafka: [brokers: [{"localhost", 9092}], group_id: "my_app"],
          nats: [host: "localhost", port: 4222],
          memory: []
        ],
        default_timeout: 5_000,
        default_retry: [max_attempts: 3, base_delay: 500],
        serializer: PhoenixMicro.Serializer.JSON,
        telemetry_enabled: true

  Runtime config via `{:system, "ENV_VAR"}` tuples is also supported.
  """

  require Logger

  @transport_schema [
    url: [type: :string, required: false],
    host: [type: :string, required: false],
    port: [type: :integer, required: false],
    brokers: [type: :any, required: false],
    group_id: [type: :string, required: false],
    username: [type: :string, required: false],
    password: [type: :string, required: false],
    vhost: [type: :string, required: false],
    queue: [type: :string, required: false],
    ssl: [type: :boolean, required: false],
    reconnect_interval: [type: :pos_integer, default: 2_000],
    max_reconnect_attempts: [type: {:or, [:pos_integer, :atom]}, default: :infinity]
  ]

  @root_schema NimbleOptions.new!(
                 transport: [
                   type: {:in, [:rabbitmq, :kafka, :nats, :redis_streams, :memory]},
                   default: :memory,
                   doc:
                     "The default transport to use when none is specified on publish/subscribe."
                 ],
                 transports: [
                   type: :keyword_list,
                   default: [memory: []],
                   doc:
                     "Per-transport configuration. Keys are transport atoms, values are keyword lists."
                 ],
                 default_timeout: [
                   type: :pos_integer,
                   default: 5_000,
                   doc: "Default RPC timeout in milliseconds."
                 ],
                 default_retry: [
                   type: :keyword_list,
                   default: [max_attempts: 3, base_delay: 500, max_delay: 30_000, jitter: true],
                   doc: "Default retry configuration for consumers and RPC calls."
                 ],
                 serializer: [
                   type: :atom,
                   default: PhoenixMicro.Serializer.JSON,
                   doc: "Module implementing `PhoenixMicro.Serializer` behaviour."
                 ],
                 telemetry_enabled: [
                   type: :boolean,
                   default: true,
                   doc: "Whether to emit Telemetry events."
                 ],
                 idempotency_store: [
                   type: {:or, [:atom, nil]},
                   default: nil,
                   doc:
                     "Module implementing `PhoenixMicro.IdempotencyStore` behaviour, or nil to disable."
                 ]
               )

  @doc """
  Returns the fully-validated application configuration.
  Raises `NimbleOptions.ValidationError` on bad config.
  """
  @spec get() :: keyword() | map()
  def get do
    raw = Application.get_all_env(:phoenix_micro)
    validated = NimbleOptions.validate!(raw, @root_schema)
    resolve_env_vars(validated)
  end

  @doc """
  Returns a single top-level config key with an optional default.
  """
  @spec get(atom(), term()) :: term()
  def get(key, default \\ nil) do
    Keyword.get(get(), key, default)
  end

  @doc """
  Returns the config for a specific transport.
  """
  @spec transport_config(atom()) :: keyword() | map()
  def transport_config(transport_name) do
    get()
    |> Keyword.get(:transports, [])
    |> Keyword.get(transport_name, [])
    |> validate_transport_config!(transport_name)
  end

  @doc """
  Returns the active (default) transport module atom.
  """
  @spec active_transport() :: atom()
  def active_transport do
    transport_name = get(:transport, :memory)
    transport_module(transport_name)
  end

  @doc """
  Maps a transport atom name to its implementation module.
  """
  @spec transport_module(atom()) :: module()
  def transport_module(:rabbitmq), do: PhoenixMicro.Transport.RabbitMQ
  def transport_module(:kafka), do: PhoenixMicro.Transport.Kafka
  def transport_module(:nats), do: PhoenixMicro.Transport.NATS
  def transport_module(:redis_streams), do: PhoenixMicro.Transport.RedisStreams
  def transport_module(:memory), do: PhoenixMicro.Transport.Memory
  def transport_module(mod) when is_atom(mod), do: mod

  @doc """
  Returns the retry options, merging consumer-level overrides with defaults.
  """
  @spec retry_opts(keyword()) :: keyword()
  def retry_opts(overrides \\ []) do
    defaults = get(:default_retry, [])
    Keyword.merge(defaults, overrides)
  end

  # --- Private ---

  defp validate_transport_config!(config, name) do
    case NimbleOptions.validate(config, NimbleOptions.new!(@transport_schema)) do
      {:ok, validated} ->
        validated

      {:error, err} ->
        Logger.warning(
          "[PhoenixMicro.Config] Invalid config for transport #{name}: #{inspect(err)}"
        )

        config
    end
  end

  defp resolve_env_vars(config) when is_list(config) do
    Enum.map(config, fn
      {k, {:system, env_var}} ->
        {k, System.get_env(env_var) || raise("Missing env var: #{env_var}")}

      {k, {:system, env_var, default}} ->
        {k, System.get_env(env_var, default)}

      {k, v} when is_list(v) ->
        {k, resolve_env_vars(v)}

      pair ->
        pair
    end)
  end

  defp resolve_env_vars(v), do: v
end
