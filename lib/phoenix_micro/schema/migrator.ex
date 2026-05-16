defmodule PhoenixMicro.Schema.Migrator do
  @moduledoc """
  Version migration engine for `PhoenixMicro.Schema`.

  When a consumer receives a message tagged with an older schema version,
  the Migrator chains the schema module's `migrate/2` callbacks to bring
  the payload up to the current version.

  ## How it works

  Given:
  - `payload_version = 1`
  - `current_version = 3`
  - `compatible_versions = [1, 2]`

  The migrator calls:

      payload
      |> schema.migrate(1, _)   # 1 → 2
      |> schema.migrate(2, _)   # 2 → 3

  Each `migrate/2` implementation only needs to handle a single hop.

  ## Example

      defmodule MyApp.Events.PaymentCreated do
        use PhoenixMicro.Schema

        schema_version 3
        topic "payments.created"

        field :payment_id,   :string,  required: true
        field :amount_cents, :integer, required: true
        field :currency,     :string,  required: true, default: "USD"

        # Tell the system which older versions we can handle
        compatible_with [1, 2]

        # v1 stored amount in dollars — convert to cents
        def migrate(1, payload) do
          Map.update(payload, "amount", 0, fn a -> round(a * 100) end)
          |> Map.put_new("currency", "USD")
        end

        # v2 renamed payment_ref → payment_id
        def migrate(2, payload) do
          {ref, rest} = Map.pop(payload, "payment_ref")
          Map.put(rest, "payment_id", ref)
        end
      end
  """

  @doc """
  Migrates a payload from `from_version` to the schema's current version.

  Returns `{:ok, migrated_payload}` or `{:error, reason}`.
  """
  @spec migrate(module(), pos_integer(), map()) :: {:ok, map()} | {:error, term()}
  def migrate(schema_module, from_version, payload) do
    current = schema_module.schema_version()
    compatible = schema_module.compatible_versions()

    cond do
      from_version == current ->
        {:ok, payload}

      from_version > current ->
        {:error, {:future_version, from_version, current}}

      from_version not in compatible ->
        {:error, {:incompatible_version, from_version, current, compatible}}

      true ->
        result = chain_migrations(schema_module, from_version, current - 1, payload)
        {:ok, result}
    end
  end

  @doc """
  Extracts the schema version from message headers.
  Returns `nil` if no version header is present.
  """
  @spec version_from_headers(map()) :: pos_integer() | nil
  def version_from_headers(headers) when is_map(headers) do
    case Map.get(headers, "x-schema-version") do
      nil -> nil
      v when is_binary(v) -> String.to_integer(v)
      v when is_integer(v) -> v
    end
  end

  def version_from_headers(_other), do: nil

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  # Chain migrate/2 calls from `from_version` up to `to_version` (inclusive).
  # Each call migrates one version step: migrate(N, payload) takes N → N+1.
  defp chain_migrations(schema_module, from_version, to_version, payload) do
    Enum.reduce(from_version..to_version, payload, fn version, acc ->
      schema_module.migrate(version, acc)
    end)
  end
end
