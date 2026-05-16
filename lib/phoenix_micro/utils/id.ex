defmodule PhoenixMicro.Utils.ID do
  @moduledoc """
  UUID v4 generation and correlation ID utilities.

  Provides a dependency-free UUID v4 generator using `:crypto.strong_rand_bytes/1`
  and a short correlation ID format suitable for log tracing.
  """

  @doc """
  Generates a RFC 4122 UUID v4 string.

  Uses `:crypto.strong_rand_bytes/1` — no external dependencies.

  ## Example

      iex> PhoenixMicro.Utils.ID.uuid4()
      "550e8400-e29b-41d4-a716-446655440000"
  """
  @spec uuid4() :: String.t()
  def uuid4 do
    # 16 random bytes
    <<b0::48, _v4::4, b1::12, _variant::2, b2::62>> = :crypto.strong_rand_bytes(16)

    # Set version = 4 (bits 48-51), variant = 0b10 (bits 64-65)
    <<a::32, b::16, c::16, d::16, e::48>> = <<b0::48, 4::4, b1::12, 0b10::2, b2::62>>

    hex_a = Integer.to_string(a, 16) |> String.downcase() |> String.pad_leading(8, "0")
    hex_b = Integer.to_string(b, 16) |> String.downcase() |> String.pad_leading(4, "0")
    hex_c = Integer.to_string(c, 16) |> String.downcase() |> String.pad_leading(4, "0")
    hex_d = Integer.to_string(d, 16) |> String.downcase() |> String.pad_leading(4, "0")
    hex_e = Integer.to_string(e, 16) |> String.downcase() |> String.pad_leading(12, "0")

    "#{hex_a}-#{hex_b}-#{hex_c}-#{hex_d}-#{hex_e}"
  end

  @doc """
  Generates a short correlation ID (16 hex chars) for use in log tracing.

  Shorter than a full UUID — suitable for HTTP headers and log fields.

  ## Example

      iex> PhoenixMicro.Utils.ID.correlation_id()
      "a3f2e1b4c8d9e0f1"
  """
  @spec correlation_id() :: String.t()
  def correlation_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end

  @doc """
  Returns true if the given string is a valid UUID v4.

  ## Example

      iex> PhoenixMicro.Utils.ID.valid_uuid?("550e8400-e29b-41d4-a716-446655440000")
      true
  """
  @spec valid_uuid?(String.t()) :: boolean()
  def valid_uuid?(str) when is_binary(str) do
    Regex.match?(
      ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      str
    )
  end

  def valid_uuid?(_other), do: false
end
