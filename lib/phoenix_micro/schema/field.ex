defmodule PhoenixMicro.Schema.Field do
  @moduledoc """
  Represents a single field definition in a `PhoenixMicro.Schema`.

  Fields are defined via the `field/2` and `field/3` macros:

      field :payment_id,   :string,  required: true
      field :amount_cents, :integer, required: true
      field :currency,     :string,  required: true, default: "USD"
      field :metadata,     :map,     required: false

  ## Supported types

  | Type       | Elixir check              |
  |------------|---------------------------|
  | `:string`  | `is_binary/1`             |
  | `:integer` | `is_integer/1`            |
  | `:float`   | `is_float/1` or integer   |
  | `:boolean` | `is_boolean/1`            |
  | `:map`     | `is_map/1`                |
  | `:list`    | `is_list/1`               |
  | `:atom`    | `is_atom/1` or binary     |
  | `:any`     | always passes             |
  """

  @enforce_keys [:name, :type]

  defstruct [
    :name,
    :type,
    required: false,
    default: nil,
    description: nil
  ]

  @type field_type :: :string | :integer | :float | :boolean | :map | :list | :atom | :any
  @type t :: %__MODULE__{
          name: atom(),
          type: field_type(),
          required: boolean(),
          default: term(),
          description: String.t() | nil
        }

  @doc """
  Builds a `Field` struct from the DSL arguments.
  """
  @spec new(atom(), field_type(), keyword()) :: t()
  def new(name, type, opts \\ []) do
    %__MODULE__{
      name: name,
      type: type,
      required: Keyword.get(opts, :required, false),
      default: Keyword.get(opts, :default),
      description: Keyword.get(opts, :description)
    }
  end

  @doc """
  Validates a single value against this field's type.
  Returns `:ok` or `{:error, message}`.
  """
  @spec validate_value(t(), term()) :: :ok | {:error, String.t()}
  def validate_value(%__MODULE__{} = field, value) do
    cond do
      is_nil(value) and field.required ->
        {:error, "is required"}

      is_nil(value) ->
        :ok

      not valid_type?(value, field.type) ->
        {:error, "expected #{field.type}, got #{inspect(value)}"}

      true ->
        :ok
    end
  end

  @doc "Returns true if the value matches the given type."
  @spec valid_type?(term(), field_type()) :: boolean()
  def valid_type?(v, :string), do: is_binary(v)
  def valid_type?(v, :integer), do: is_integer(v)
  def valid_type?(v, :float), do: is_float(v) or is_integer(v)
  def valid_type?(v, :boolean), do: is_boolean(v)
  def valid_type?(v, :map), do: is_map(v)
  def valid_type?(v, :list), do: is_list(v)
  def valid_type?(v, :atom), do: is_atom(v) or is_binary(v)
  def valid_type?(_v, :any), do: true
end
