defmodule PhoenixMicro.Utils.Encoding do
  @moduledoc """
  Shared encoding/decoding helpers used across transports and the producer.

  Provides a unified entry point for payload serialization so all transports
  produce and consume messages in the same format regardless of the underlying
  broker's wire format.
  """

  @doc """
  Encodes `payload` using the configured serializer.
  Returns a binary.
  """
  @spec encode!(term()) :: binary()
  def encode!(payload) do
    serializer().encode!(payload)
  end

  @doc """
  Decodes `binary` using the configured serializer.
  Returns the decoded term.
  """
  @spec decode!(binary()) :: term()
  def decode!(binary) do
    serializer().decode!(binary)
  end

  @doc """
  Returns the MIME content-type for the configured serializer.
  """
  @spec content_type() :: String.t()
  def content_type do
    serializer().content_type()
  end

  @doc """
  Safely encodes a term — returns `{:ok, binary}` or `{:error, reason}`.
  """
  @spec encode(term()) :: {:ok, binary()} | {:error, Exception.t()}
  def encode(payload) do
    {:ok, encode!(payload)}
  rescue
    e -> {:error, e}
  end

  @doc """
  Safely decodes a binary — returns `{:ok, term}` or `{:error, reason}`.
  """
  @spec decode(binary()) :: {:ok, term()} | {:error, Exception.t()}
  def decode(binary) do
    {:ok, decode!(binary)}
  rescue
    e -> {:error, e}
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp serializer do
    # Guard against Config being unavailable (e.g. in minimal test environments)
    if Code.ensure_loaded?(PhoenixMicro.Config) do
      PhoenixMicro.Config.get(:serializer, PhoenixMicro.Serializer.JSON)
    else
      Application.get_env(:phoenix_micro, :serializer, PhoenixMicro.Serializer.JSON)
    end
  end
end
