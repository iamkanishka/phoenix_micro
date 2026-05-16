defmodule PhoenixMicro.Schema do
  @moduledoc """
  Typed message schema contracts with version negotiation and
  backward-compatibility enforcement.

  Sub-modules:
  - `PhoenixMicro.Schema.Field`     — field struct & type validation
  - `PhoenixMicro.Schema.Validator` — validation engine
  - `PhoenixMicro.Schema.Migrator`  — version migration chains
  - `PhoenixMicro.Schema.Registry`  — ETS topic→module registry

  ## Defining a schema

      defmodule MyApp.Events.PaymentCreated do
        use PhoenixMicro.Schema

        schema_version 2
        topic "payments.created"

        field :payment_id,   :string,  required: true
        field :amount_cents, :integer, required: true
        field :currency,     :string,  required: true, default: "USD"
        field :metadata,     :map,     required: false

        compatible_with [1]

        def migrate(1, payload) do
          {amount, rest} = Map.pop(payload, "amount")
          Map.put(rest, "amount_cents", round(amount * 100))
        end
      end

  ## Publishing with schema validation

      PhoenixMicro.Schema.publish(MyApp.Events.PaymentCreated, %{
        payment_id: "pay_123",
        amount_cents: 9999,
        currency: "USD"
      })

  ## Decoding in consumers

      def handle(message, _ctx) do
        {:ok, event} =
          PhoenixMicro.Schema.decode(MyApp.Events.PaymentCreated,
            message.payload, message.headers)
        process(event)
        :ok
      end
  """

  alias PhoenixMicro.Schema.Migrator

  # ---------------------------------------------------------------------------
  # Behaviour
  # ---------------------------------------------------------------------------

  @type field_type :: :string | :integer | :float | :boolean | :map | :list | :atom | :any
  @type field_opts :: [required: boolean(), default: term(), description: String.t()]
  @type field_def :: {atom(), field_type(), field_opts()}
  @type validation_error :: {atom(), String.t()}

  @callback schema_version() :: pos_integer()
  @callback topic() :: String.t()
  @callback fields() :: [field_def()]
  @callback compatible_versions() :: [pos_integer()]

  @doc "Validates a raw map against the schema."
  @callback validate(map()) :: {:ok, map()} | {:error, [validation_error()]}

  @doc "Migrates a payload from an older version to the current version."
  @callback migrate(from_version :: pos_integer(), payload :: map()) :: map()

  @optional_callbacks [migrate: 2]

  # ---------------------------------------------------------------------------
  # DSL Macro
  # ---------------------------------------------------------------------------

  defmacro __using__(_opts) do
    quote do
      @behaviour PhoenixMicro.Schema

      import PhoenixMicro.Schema,
        only: [schema_version: 1, topic: 1, field: 2, field: 3, compatible_with: 1]

      Module.register_attribute(__MODULE__, :pm_schema_version, accumulate: false)
      Module.register_attribute(__MODULE__, :pm_schema_topic, accumulate: false)
      Module.register_attribute(__MODULE__, :pm_schema_fields, accumulate: true)
      Module.register_attribute(__MODULE__, :pm_schema_compatible, accumulate: false)

      @pm_schema_version 1
      @pm_schema_compatible []

      @before_compile PhoenixMicro.Schema
      @after_compile PhoenixMicro.Schema
    end
  end

  defmacro schema_version(v), do: quote(do: @pm_schema_version(unquote(v)))
  defmacro topic(t), do: quote(do: @pm_schema_topic(unquote(t)))
  defmacro compatible_with(versions), do: quote(do: @pm_schema_compatible(unquote(versions)))

  defmacro field(name, type, opts \\ []) do
    quote do
      @pm_schema_fields {unquote(name), unquote(type), unquote(opts)}
    end
  end

  defmacro __before_compile__(_compile_env) do
    quote do
      @impl PhoenixMicro.Schema
      def schema_version, do: @pm_schema_version

      @impl PhoenixMicro.Schema
      def topic, do: @pm_schema_topic

      @impl PhoenixMicro.Schema
      def fields do
        @pm_schema_fields
        |> Enum.reverse()
        |> Enum.map(fn {name, type, opts} -> {name, type, opts} end)
      end

      @impl PhoenixMicro.Schema
      def compatible_versions, do: @pm_schema_compatible

      @impl PhoenixMicro.Schema
      def validate(payload) when is_map(payload) do
        PhoenixMicro.Schema.Validator.validate(fields(), payload)
      end

      def validate(_non_map), do: {:error, [{:payload, "must be a map"}]}

      @impl PhoenixMicro.Schema
      def migrate(_from_version, payload), do: payload

      defoverridable migrate: 2
    end
  end

  @doc false
  def __after_compile__(env, _bytecode) do
    module = env.module

    if function_exported?(module, :topic, 0) and not is_nil(module.topic()) do
      PhoenixMicro.Schema.Registry.register(module)
    end
  end

  # ---------------------------------------------------------------------------
  # Runtime API
  # ---------------------------------------------------------------------------

  @doc """
  Validates and publishes a message with schema metadata in headers.
  Returns `{:error, {:schema_validation_failed, errors}}` if validation fails.
  """
  @spec publish(module(), map(), keyword()) :: :ok | {:error, term()}
  def publish(schema_module, payload, opts \\ []) do
    case schema_module.validate(payload) do
      {:ok, validated} ->
        headers = %{
          "x-schema-version" => Integer.to_string(schema_module.schema_version()),
          "x-schema-module" => inspect(schema_module)
        }

        existing_headers = Keyword.get(opts, :headers, %{})
        merged = Keyword.put(opts, :headers, Map.merge(existing_headers, headers))
        PhoenixMicro.publish(schema_module.topic(), validated, merged)

      {:error, errors} ->
        {:error, {:schema_validation_failed, errors}}
    end
  end

  @doc """
  Decodes and validates a raw payload against a schema, migrating if needed.

  Pass the message headers so the migrator can detect older payload versions.
  Returns `{:ok, validated_payload}` or `{:error, reason}`.
  """
  @spec decode(module(), map(), map()) :: {:ok, map()} | {:error, term()}
  def decode(schema_module, payload, headers \\ %{}) do
    current_version = schema_module.schema_version()
    payload_version = Migrator.version_from_headers(headers) || current_version

    with {:ok, migrated} <-
           maybe_migrate(schema_module, payload_version, current_version, payload) do
      schema_module.validate(migrated)
    end
  end

  @doc "Returns all registered schema modules."
  @spec all_schemas() :: [module()]
  def all_schemas, do: PhoenixMicro.Schema.Registry.all()

  @doc "Returns the schema module registered for a given topic."
  @spec schema_for_topic(String.t()) :: {:ok, module()} | {:error, :not_found}
  def schema_for_topic(topic), do: PhoenixMicro.Schema.Registry.lookup(topic)

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp maybe_migrate(_schema, version, version, payload), do: {:ok, payload}

  defp maybe_migrate(schema_module, payload_version, _current, payload) do
    Migrator.migrate(schema_module, payload_version, payload)
  end
end
