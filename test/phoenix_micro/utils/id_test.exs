defmodule PhoenixMicro.Utils.IDTest do
  use ExUnit.Case, async: true

  alias PhoenixMicro.Utils.ID

  describe "uuid4/0" do
    test "generates a valid UUID v4 string" do
      id = ID.uuid4()
      assert is_binary(id)
      assert byte_size(id) == 36

      assert Regex.match?(
               ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
               id
             )
    end

    test "generates unique values" do
      ids = for _i <- 1..1_000, do: ID.uuid4()
      assert Enum.count(Enum.uniq(ids)) == 1_000
    end

    test "version nibble is always '4'" do
      for _i <- 1..100 do
        id = ID.uuid4()
        parts = String.split(id, "-")
        version_char = parts |> Enum.at(2) |> String.first()
        assert version_char == "4"
      end
    end

    test "variant bits are always '8', '9', 'a', or 'b'" do
      for _i <- 1..100 do
        id = ID.uuid4()
        parts = String.split(id, "-")
        variant_char = parts |> Enum.at(3) |> String.first()
        assert variant_char in ["8", "9", "a", "b"]
      end
    end
  end

  describe "correlation_id/0" do
    test "generates a 16-character hex string" do
      cid = ID.correlation_id()
      assert is_binary(cid)
      assert byte_size(cid) == 16
      assert Regex.match?(~r/^[0-9a-f]{16}$/, cid)
    end

    test "generates unique values" do
      ids = for _i <- 1..500, do: ID.correlation_id()
      assert Enum.count(Enum.uniq(ids)) == 500
    end
  end

  describe "valid_uuid?/1" do
    test "returns true for valid UUID v4" do
      assert ID.valid_uuid?("550e8400-e29b-41d4-a716-446655440000")
      assert ID.valid_uuid?(ID.uuid4())
    end

    test "returns false for invalid strings" do
      refute ID.valid_uuid?("not-a-uuid")
      # v1 not v4
      refute ID.valid_uuid?("550e8400-e29b-11d4-a716-446655440000")
      refute ID.valid_uuid?("")
      refute ID.valid_uuid?(nil)
      refute ID.valid_uuid?(123)
    end
  end
end
