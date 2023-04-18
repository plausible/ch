defmodule Ch.TypesTest do
  use ExUnit.Case, async: true

  describe "decode_type/1" do
    test "scalar types" do
      assert decode_type("String") == :string
      assert decode_type("Int8") == :i8
      assert decode_type("UInt8") == :u8
    end

    test "array" do
      assert decode_type("Array(Int8)") == {:array, :i8}
      assert decode_type("Array(String)") == {:array, :string}
      assert decode_type("Array(Array(String))") == {:array, {:array, :string}}
      assert decode_type("Array(Tuple(UInt8, String))") == {:array, {:tuple, [:u8, :string]}}

      assert decode_type("Array(Tuple(String, Array(String)))") ==
               {:array, {:tuple, [:string, {:array, :string}]}}
    end

    test "tuple" do
      assert decode_type("Tuple(UInt8, UInt8)") == {:tuple, [:u8, :u8]}

      assert decode_type("Tuple(a Int8, b Tuple(c Int8, d Array(Int8)))") ==
               {:tuple, [{"a", :i8}, {"b", {:tuple, [{"c", :i8}, {"d", {:array, :i8}}]}}]}
    end
  end

  def decode_type(type), do: decode_type(type, [], [])

  def decode_type("String" <> rest, acc, stack) do
    decode_type(rest, [:string | acc], stack)
  end

  def decode_type("Int8" <> rest, acc, stack) do
    decode_type(rest, [:i8 | acc], stack)
  end

  def decode_type("UInt8" <> rest, acc, stack) do
    decode_type(rest, [:u8 | acc], stack)
  end

  def decode_type("Array(" <> rest, acc, stack) do
    decode_type(rest, [], [{:array, acc} | stack])
  end

  def decode_type("Tuple(" <> rest, acc, stack) do
    decode_type(rest, [], [{:tuple, acc} | stack])
  end

  def decode_type(<<?), rest::bytes>>, acc, stack) do
    case stack do
      [] ->
        decode_type(rest, acc, stack)

      [frame | stack] ->
        case frame do
          {:array, original_acc} ->
            [type] = acc
            decode_type(rest, [{:array, type} | original_acc], stack)

          {:tuple, original_acc} ->
            decode_type(rest, [{:tuple, :lists.reverse(acc)} | original_acc], stack)
        end
    end
  end

  def decode_type(<<?,, rest::bytes>>, acc, stack) do
    decode_type(rest, acc, stack)
  end

  def decode_type(<<?\s, rest::bytes>>, acc, stack) do
    decode_type(rest, acc, stack)
  end

  def decode_type("", [type], _stack = []), do: type
end
