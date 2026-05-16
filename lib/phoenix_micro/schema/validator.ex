defmodule PhoenixMicro.Schema.Validator do
  @moduledoc """
  Validation engine for `PhoenixMicro.Schema`.

  Takes a list of `field_def` tuples `{name, type, opts}` and a raw map,
  and returns `{:ok, normalised_map}` or `{:error, [{field, reason}]}`.

  This module is called internally by every schema module's generated
  `validate/1` callback. You can also call it directly if you have field
  definitions at runtime:

      fields = [
        {:amount_cents, :integer, required: true},
        {:currency,    :string,  required: true, default: "USD"}
      ]

      PhoenixMicro.Schema.Validator.validate(fields, %{"amount_cents" => 999})
      # => {:ok, %{"amount_cents" => 999, "currency" => "USD"}}
  """

  alias PhoenixMicro.Schema.Field

  @type field_def :: {atom(), Field.field_type(), keyword()}
  @type validation_error :: {atom(), String.t()}
  @type result :: {:ok, map()} | {:error, [validation_error()]}

  @doc """
  Validates `payload` against the list of field definitions.

  Keys are normalised to strings. Missing required fields produce errors.
  Missing optional fields with defaults have defaults applied.
  """
  @spec validate([field_def()], map()) :: result()
  def validate(fields, payload) when is_list(fields) and is_map(payload) do
    normalised = normalise_keys(payload)

    errors =
      Enum.flat_map(fields, fn {name, type, opts} ->
        field = Field.new(name, type, opts)
        key = to_string(name)
        value = Map.get(normalised, key)

        case Field.validate_value(field, value) do
          :ok -> []
          {:error, msg} -> [{name, msg}]
        end
      end)

    if errors == [] do
      {:ok, apply_defaults(fields, normalised)}
    else
      {:error, errors}
    end
  end

  def validate(_fields, _payload), do: {:error, [{:payload, "must be a map"}]}

  @doc """
  Returns a human-readable summary of validation errors.

      PhoenixMicro.Schema.Validator.format_errors([{:amount_cents, "is required"}])
      # => "amount_cents: is required"
  """
  @spec format_errors([validation_error()]) :: String.t()
  def format_errors(errors) do
    errors
    |> Enum.map(fn {field, msg} -> "#{field}: #{msg}" end)
    |> Enum.join(", ")
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp normalise_keys(payload) do
    Map.new(payload, fn
      {k, v} when is_atom(k) -> {to_string(k), v}
      {k, v} -> {k, v}
    end)
  end

  defp apply_defaults(fields, payload) do
    Enum.reduce(fields, payload, fn {name, _type, opts}, acc ->
      key = to_string(name)

      case {Map.has_key?(acc, key), Keyword.get(opts, :default)} do
        {false, default} when not is_nil(default) -> Map.put(acc, key, default)
        _no_default -> acc
      end
    end)
  end
end
