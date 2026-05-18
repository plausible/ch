defmodule Ch.RowBinaryNullableTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "Nullable params round-trip through ClickHouse", %{pool: pool} do
    check all {type, value, expected} <- nullable_param() do
      assert Ch.query!(pool, "SELECT {value:Nullable(#{type})}", %{"value" => value}).rows == [
               [expected]
             ]
    end
  end

  property "arrays of Nullable params round-trip through ClickHouse", %{pool: pool} do
    check all {type, values, expected} <- nullable_array_param() do
      assert Ch.query!(pool, "SELECT {value:Array(Nullable(#{type}))}", %{"value" => values}).rows ==
               [[expected]]
    end
  end

  test "RowBinary Nullable inserts cover present and null values", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_nullable_values (
      id UInt64,
      maybe_string Nullable(String),
      maybe_int Nullable(Int32),
      maybe_bool Nullable(Bool),
      maybe_date Nullable(Date),
      maybe_tuple Tuple(Nullable(String), Nullable(UInt8)),
      maybe_strings Array(Nullable(String))
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_nullable_values") end)

    rows = [
      [0, nil, nil, nil, nil, {nil, nil}, []],
      [1, "hello", -1, true, ~D[2024-02-29], {"tuple", 1}, ["a", nil, "b"]],
      [18_446_744_073_709_551_615, "", 0, false, ~D[1970-01-01], {"", 0}, [nil]]
    ]

    types = [
      "UInt64",
      "Nullable(String)",
      "Nullable(Int32)",
      "Nullable(Bool)",
      "Nullable(Date)",
      "Tuple(Nullable(String), Nullable(UInt8))",
      "Array(Nullable(String))"
    ]

    rowbinary = RowBinary.encode_rows(rows, types)
    Ch.query!(pool, ["INSERT INTO row_binary_nullable_values FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_nullable_values ORDER BY id").rows == rows
  end

  defp nullable_param do
    one_of([
      typed_nullable("String", one_of([constant(nil), safe_string()])),
      typed_nullable("Int32", one_of([constant(nil), integer(-2_147_483_648..2_147_483_647)])),
      typed_nullable("UInt8", one_of([constant(nil), integer(0..255)])),
      typed_nullable("Bool", one_of([constant(nil), boolean()])),
      typed_nullable("Date", one_of([constant(nil), date_gen()]))
    ])
  end

  defp nullable_array_param do
    one_of([
      typed_nullable_array("String", one_of([constant(nil), safe_string()])),
      typed_nullable_array("UInt8", one_of([constant(nil), integer(0..255)])),
      typed_nullable_array("Bool", one_of([constant(nil), boolean()])),
      typed_nullable_array("Date", one_of([constant(nil), date_gen()]))
    ])
  end

  defp typed_nullable(type, generator) do
    gen all value <- generator do
      {type, value, value}
    end
  end

  defp typed_nullable_array(type, generator) do
    gen all values <- list_of(generator, max_length: 8) do
      {type, values, values}
    end
  end

  defp date_gen do
    gen all days <- integer(0..20_000) do
      Date.add(~D[1970-01-01], days)
    end
  end

  defp safe_string do
    string(:printable, max_length: 32)
  end
end
