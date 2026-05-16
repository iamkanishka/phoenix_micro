defmodule PhoenixMicro.Serializer do
  @moduledoc """
  Behaviour for pluggable message serialization.

  The serializer is responsible for encoding `PhoenixMicro.Message` payloads
  to binary when publishing, and decoding binary back to terms when consuming.
  """

  @type encoded :: binary()
  @type decoded :: term()

  @callback encode!(term()) :: encoded()
  @callback decode!(encoded()) :: decoded()
  @callback content_type() :: String.t()
end

defmodule PhoenixMicro.Serializer.JSON do
  @moduledoc """
  Default JSON serializer using `Jason`.
  """

  @behaviour PhoenixMicro.Serializer

  @impl PhoenixMicro.Serializer
  @spec encode!(term()) :: binary()
  def encode!(term), do: Jason.encode!(term)

  @impl PhoenixMicro.Serializer
  @spec decode!(binary()) :: term()
  def decode!(binary) do
    case Jason.decode(binary) do
      {:ok, decoded} ->
        decoded

      {:error, err} ->
        raise "PhoenixMicro JSON decode error: #{inspect(err)} for: #{inspect(binary)}"
    end
  end

  @impl PhoenixMicro.Serializer
  @spec content_type() :: String.t()
  def content_type, do: "application/json"
end

defmodule PhoenixMicro.Serializer.PassThrough do
  @moduledoc """
  No-op serializer — passes binaries through unchanged.
  Useful for transports (e.g. gRPC) that handle serialization externally.
  """

  @behaviour PhoenixMicro.Serializer

  @impl PhoenixMicro.Serializer
  @spec encode!(term()) :: binary()
  def encode!(bin) when is_binary(bin), do: bin
  def encode!(term), do: :erlang.term_to_binary(term)

  @impl PhoenixMicro.Serializer
  @spec decode!(binary()) :: binary()
  def decode!(bin) when is_binary(bin), do: bin

  @impl PhoenixMicro.Serializer
  @spec content_type() :: String.t()
  def content_type, do: "application/octet-stream"
end
