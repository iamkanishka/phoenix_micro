defmodule PhoenixMicro.Utils.EncodingTest do
  use ExUnit.Case, async: true

  # PhoenixMicro.Utils.Encoding delegates to the configured serializer (Jason by default).
  # Since Jason requires hex dependencies not available in CI without network,
  # these tests verify the Encoding module's contract using a test-only serializer
  # injected via application config.

  defmodule StubSerializer do
    @moduledoc false
    @behaviour PhoenixMicro.Serializer

    @impl PhoenixMicro.Serializer
    def encode!(term) when is_binary(term), do: ~s("#{term}")
    def encode!(term) when is_map(term), do: inspect(term)
    def encode!(term) when is_list(term), do: inspect(term)
    def encode!(term), do: inspect(term)

    @impl PhoenixMicro.Serializer
    def decode!(binary) when is_binary(binary), do: binary

    @impl PhoenixMicro.Serializer
    def content_type, do: "application/x-stub"
  end

  setup do
    previous = Application.get_env(:phoenix_micro, :serializer)

    Application.put_env(:phoenix_micro, :serializer, StubSerializer)

    on_exit(fn ->
      if previous do
        Application.put_env(:phoenix_micro, :serializer, previous)
      else
        Application.delete_env(:phoenix_micro, :serializer)
      end
    end)

    :ok
  end

  describe "encode!/1" do
    test "returns a binary for any input" do
      result = PhoenixMicro.Utils.Encoding.encode!("hello")
      assert is_binary(result)
    end

    test "returns a binary for maps" do
      result = PhoenixMicro.Utils.Encoding.encode!(%{a: 1})
      assert is_binary(result)
    end

    test "returns a binary for lists" do
      result = PhoenixMicro.Utils.Encoding.encode!([1, 2, 3])
      assert is_binary(result)
    end
  end

  describe "decode!/1" do
    test "returns a term for a binary" do
      result = PhoenixMicro.Utils.Encoding.decode!("some binary")
      assert is_binary(result)
    end
  end

  describe "encode/1 (safe)" do
    test "returns {:ok, binary} on success" do
      assert {:ok, bin} = PhoenixMicro.Utils.Encoding.encode(%{x: 1})
      assert is_binary(bin)
    end

    test "returns {:error, reason} when serializer raises" do
      # Temporarily override serializer with one that raises
      defmodule RaisingSerializer do
        @behaviour PhoenixMicro.Serializer
        def encode!(_term), do: raise("encode failed")
        def decode!(_bin), do: raise("decode failed")
        def content_type, do: "text/plain"
      end

      Application.put_env(:phoenix_micro, :serializer, RaisingSerializer)
      assert {:error, _reason} = PhoenixMicro.Utils.Encoding.encode(%{})
      Application.put_env(:phoenix_micro, :serializer, StubSerializer)
    end
  end

  describe "decode/1 (safe)" do
    test "returns {:ok, term} on success" do
      assert {:ok, result} = PhoenixMicro.Utils.Encoding.decode("hello")
      assert is_binary(result)
    end
  end

  describe "content_type/0" do
    test "returns the configured serializer's content type" do
      assert PhoenixMicro.Utils.Encoding.content_type() == "application/x-stub"
    end
  end
end
