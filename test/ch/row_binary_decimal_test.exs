defmodule Ch.RowBinaryDecimalTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  import Bitwise

  @decimal_types [decimal32: 32, decimal64: 64, decimal128: 128, decimal256: 256]

  property "valid coefficients preserve every signed bit pattern" do
    check all {type, size, bytes, coefficient} <- decimal_coefficient() do
      decimal = Decimal.new(coefficient)
      encoded = type |> RowBinary.encode(decimal) |> IO.iodata_to_binary()

      assert encoded == bytes
      assert [[decoded]] = RowBinary.decode_rows(encoded, [type])
      assert Decimal.equal?(decoded, decimal)
      assert bit_size(encoded) == size
    end
  end

  test "rejects coefficients outside each storage width" do
    for {name, size} <- @decimal_types do
      type = {name, 0}
      min = -(1 <<< (size - 1))
      max = (1 <<< (size - 1)) - 1

      assert encoded_binary(type, Decimal.new(min)) == <<min::little-signed-size(size)>>
      assert encoded_binary(type, Decimal.new(max)) == <<max::little-signed-size(size)>>

      assert_raise ArgumentError, ~r/out of range/, fn ->
        RowBinary.encode(type, Decimal.new(min - 1))
      end

      assert_raise ArgumentError, ~r/out of range/, fn ->
        RowBinary.encode(type, Decimal.new(max + 1))
      end
    end
  end

  test "regression: Decimal32 overflow never wraps or becomes zero" do
    for value <- ["2147483648", "4294967296"] do
      assert_raise ArgumentError, ~r/out of range/, fn ->
        RowBinary.encode({:decimal32, 0}, Decimal.new(value))
      end
    end
  end

  test "rejects overflow introduced by scaling or rounding" do
    type = {:decimal32, 2}

    assert_raise ArgumentError, ~r/out of range/, fn ->
      RowBinary.encode(type, Decimal.new("21474836.48"))
    end

    assert_raise ArgumentError, ~r/out of range/, fn ->
      RowBinary.encode({:decimal32, 0}, Decimal.new("2147483647.5"))
    end

    assert_raise ArgumentError, ~r/out of range/, fn ->
      RowBinary.encode({:decimal32, 0}, Decimal.new("-2147483648.5"))
    end
  end

  test "declared precision rejects coefficients that still fit the storage width" do
    for precision <- [1, 9, 10, 18, 19, 38, 39, 76] do
      max = Integer.pow(10, precision) - 1
      type = {:decimal, precision, 0}

      assert Decimal.equal?(
               type
               |> RowBinary.encode(Decimal.new(max))
               |> IO.iodata_to_binary()
               |> then(&RowBinary.decode_rows(&1, [type]))
               |> get_in([Access.at(0), Access.at(0)]),
               Decimal.new(max)
             )

      assert_raise ArgumentError, ~r/exceeds precision/, fn ->
        RowBinary.encode(type, Decimal.new(max + 1))
      end

      assert_raise ArgumentError, ~r/exceeds precision/, fn ->
        RowBinary.encode(type, Decimal.new(-max - 1))
      end
    end
  end

  test "rejects non-finite decimals" do
    for decimal <- [Decimal.new("NaN"), Decimal.new("Infinity"), Decimal.new("-Infinity")] do
      assert_raise ArgumentError, "ClickHouse Decimal values must be finite", fn ->
        RowBinary.encode({:decimal32, 0}, decimal)
      end
    end
  end

  test "rejects invalid precision and scale before encoding rows" do
    for type <- [
          {:decimal, 0, 0},
          {:decimal, 77, 0},
          {:decimal, 9, -1},
          {:decimal, 9, 10},
          {:decimal32, 10},
          {:decimal64, 19},
          {:decimal128, 39},
          {:decimal256, 77}
        ] do
      assert_raise ArgumentError, ~r/invalid Decimal/, fn ->
        RowBinary.encode_rows([[Decimal.new(0)]], [type])
      end
    end
  end

  defp decimal_coefficient do
    gen all {name, size} <- member_of(@decimal_types),
            bytes <- binary(length: div(size, 8)) do
      unsigned = :binary.decode_unsigned(bytes, :little)
      signed_limit = 1 <<< (size - 1)
      coefficient = if unsigned >= signed_limit, do: unsigned - (1 <<< size), else: unsigned

      {{name, 0}, size, bytes, coefficient}
    end
  end

  defp encoded_binary(type, decimal) do
    type |> RowBinary.encode(decimal) |> IO.iodata_to_binary()
  end
end
