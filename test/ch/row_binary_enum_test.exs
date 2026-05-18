defmodule Ch.RowBinaryEnumTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  @enum8 "Enum8('low' = -1, 'zero' = 0, 'high' = 1)"
  @enum16 "Enum16('small' = -129, 'medium' = 0, 'large' = 128)"

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "Enum params round-trip through ClickHouse", %{pool: pool} do
    check all {type, value} <- enum_param() do
      assert Ch.query!(pool, "SELECT {value:#{type}}", %{"value" => value}).rows == [[value]]
    end
  end

  property "Enum arrays round-trip as query params through ClickHouse", %{pool: pool} do
    check all {type, values} <- enum_array_param() do
      assert Ch.query!(pool, "SELECT {value:Array(#{type})}", %{"value" => values}).rows == [
               [values]
             ]
    end
  end

  test "RowBinary Enum inserts accept names and integer values", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_enum_values (
      id UInt64,
      e8 #{@enum8},
      e16 #{@enum16},
      e8s Array(#{@enum8}),
      maybe Nullable(#{@enum16})
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_enum_values") end)

    rows = [
      [0, "low", "small", ["low", "zero", "high"], nil],
      [1, 0, 0, [-1, 0, 1], "medium"],
      [18_446_744_073_709_551_615, 1, 128, ["high"], -129]
    ]

    types = ["UInt64", @enum8, @enum16, "Array(#{@enum8})", "Nullable(#{@enum16})"]
    rowbinary = RowBinary.encode_rows(rows, types)
    Ch.query!(pool, ["INSERT INTO row_binary_enum_values FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_enum_values ORDER BY id").rows == [
             [0, "low", "small", ["low", "zero", "high"], nil],
             [1, "zero", "medium", ["low", "zero", "high"], "medium"],
             [18_446_744_073_709_551_615, "high", "large", ["high"], "small"]
           ]
  end

  test "RowBinary rejects missing enum names and out-of-range enum values" do
    assert_raise ArgumentError, ~r/enum value "missing" not found/, fn ->
      RowBinary.encode_rows([["missing"]], [@enum8])
    end

    assert_raise ArgumentError, "invalid Int8: 128", fn ->
      RowBinary.encode_rows([[128]], [@enum8])
    end

    assert_raise ArgumentError, "invalid Int16: 32768", fn ->
      RowBinary.encode_rows([[32_768]], [@enum16])
    end
  end

  defp enum_param do
    one_of([
      typed_enum(@enum8, member_of(["low", "zero", "high"])),
      typed_enum(@enum16, member_of(["small", "medium", "large"]))
    ])
  end

  defp enum_array_param do
    one_of([
      typed_enum_array(@enum8, member_of(["low", "zero", "high"])),
      typed_enum_array(@enum16, member_of(["small", "medium", "large"]))
    ])
  end

  defp typed_enum(type, generator) do
    gen all value <- generator do
      {type, value}
    end
  end

  defp typed_enum_array(type, generator) do
    gen all values <- list_of(generator, max_length: 8) do
      {type, values}
    end
  end
end
