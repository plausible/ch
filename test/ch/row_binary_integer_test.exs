defmodule Ch.RowBinaryIntegerTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  import Bitwise

  @uint_types [
    u8: 8,
    u16: 16,
    u32: 32,
    u64: 64,
    u128: 128,
    u256: 256
  ]

  @int_types [
    i8: 8,
    i16: 16,
    i32: 32,
    i64: 64,
    i128: 128,
    i256: 256
  ]

  describe "unsigned integers" do
    property "encode and decode all bit patterns as little-endian values" do
      check all {type, bits, bytes, value} <- uint_value() do
        assert encoded_binary(type, value) == bytes
        assert RowBinary.decode_rows(bytes, [type]) == [[value]]
        assert bit_size(bytes) == bits
      end
    end

    test "reject out-of-range and non-integer values" do
      for {type, bits} <- @uint_types do
        max = (1 <<< bits) - 1
        type_name = "UInt#{bits}"

        assert_raise ArgumentError, "invalid #{type_name}: -1", fn ->
          RowBinary.encode(type, -1)
        end

        assert_raise ArgumentError, "invalid #{type_name}: #{max + 1}", fn ->
          RowBinary.encode(type, max + 1)
        end

        assert_raise ArgumentError, "invalid #{type_name}: \"1\"", fn ->
          RowBinary.encode(type, "1")
        end
      end
    end
  end

  describe "signed integers" do
    property "encode and decode all bit patterns as little-endian values" do
      check all {type, bits, bytes, value} <- int_value() do
        assert encoded_binary(type, value) == bytes
        assert RowBinary.decode_rows(bytes, [type]) == [[value]]
        assert bit_size(bytes) == bits
      end
    end

    test "reject out-of-range and non-integer values" do
      for {type, bits} <- @int_types do
        min = -(1 <<< (bits - 1))
        max = (1 <<< (bits - 1)) - 1
        type_name = "Int#{bits}"

        assert_raise ArgumentError, "invalid #{type_name}: #{min - 1}", fn ->
          RowBinary.encode(type, min - 1)
        end

        assert_raise ArgumentError, "invalid #{type_name}: #{max + 1}", fn ->
          RowBinary.encode(type, max + 1)
        end

        assert_raise ArgumentError, "invalid #{type_name}: \"1\"", fn ->
          RowBinary.encode(type, "1")
        end
      end
    end
  end

  test "boundary values round-trip" do
    for {type, bits} <- @uint_types do
      max = (1 <<< bits) - 1

      assert RowBinary.decode_rows(encoded_binary(type, 0), [type]) == [[0]]

      assert RowBinary.decode_rows(encoded_binary(type, max), [type]) == [[max]]
    end

    for {type, bits} <- @int_types do
      min = -(1 <<< (bits - 1))
      max = (1 <<< (bits - 1)) - 1

      assert RowBinary.decode_rows(encoded_binary(type, min), [type]) == [[min]]

      assert RowBinary.decode_rows(encoded_binary(type, max), [type]) == [[max]]
    end
  end

  defp uint_value do
    gen all {type, bits} <- member_of(@uint_types),
            bytes <- binary(length: div(bits, 8)) do
      {type, bits, bytes, :binary.decode_unsigned(bytes, :little)}
    end
  end

  defp int_value do
    gen all {type, bits} <- member_of(@int_types),
            bytes <- binary(length: div(bits, 8)) do
      unsigned = :binary.decode_unsigned(bytes, :little)
      signed_limit = 1 <<< (bits - 1)
      value = if unsigned >= signed_limit, do: unsigned - (1 <<< bits), else: unsigned

      {type, bits, bytes, value}
    end
  end

  defp encoded_binary(type, value) do
    case RowBinary.encode(type, value) do
      byte when is_integer(byte) -> <<byte>>
      iodata -> IO.iodata_to_binary(iodata)
    end
  end
end
