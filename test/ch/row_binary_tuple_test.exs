defmodule Ch.RowBinaryTupleTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "Tuple params round-trip through ClickHouse", %{pool: pool} do
    check all tuple <- tuple_param() do
      assert Ch.query!(pool, "SELECT {value:Tuple(String, UInt8, Bool)}", %{"value" => tuple}).rows ==
               [[tuple]]
    end
  end

  property "arrays of Tuple params round-trip through ClickHouse", %{pool: pool} do
    check all tuples <- list_of(tuple_param(), max_length: 8) do
      assert Ch.query!(pool, "SELECT {value:Array(Tuple(String, UInt8, Bool))}", %{
               "value" => tuples
             }).rows == [[tuples]]
    end
  end

  property "RowBinary Tuple inserts round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_tuple_property (
      id UInt64,
      value Tuple(String, UInt8, Bool)
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_tuple_property") end)

    check all rows <- rowbinary_tuple_rows(), max_runs: 25 do
      Ch.query!(pool, "TRUNCATE TABLE row_binary_tuple_property")

      rowbinary = RowBinary.encode_rows(rows, ["UInt64", "Tuple(String, UInt8, Bool)"])
      Ch.query!(pool, ["INSERT INTO row_binary_tuple_property FORMAT RowBinary\n" | rowbinary])

      assert Ch.query!(pool, "SELECT * FROM row_binary_tuple_property ORDER BY id").rows ==
               Enum.sort_by(rows, &List.first/1)
    end
  end

  test "RowBinary Tuple inserts cover nested containers and nil defaults", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_tuple_representative (
      id UInt64,
      simple Tuple(String, UInt8),
      nested Tuple(Array(UInt8), Map(String, Nullable(String))),
      tuple_array Array(Tuple(String, UInt8))
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_tuple_representative") end)

    rows = [
      [0, {"", 0}, {[], %{}}, []],
      [
        1,
        {"one", 1},
        {[1, 2, 3], %{"a" => "b", "nil" => nil}},
        [{"a", 1}, {"b", 2}]
      ],
      [18_446_744_073_709_551_615, nil, {[], %{"x" => nil}}, [{"max", 255}]]
    ]

    types = [
      "UInt64",
      "Tuple(String, UInt8)",
      "Tuple(Array(UInt8), Map(String, Nullable(String)))",
      "Array(Tuple(String, UInt8))"
    ]

    rowbinary = RowBinary.encode_rows(rows, types)

    Ch.query!(pool, ["INSERT INTO row_binary_tuple_representative FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_tuple_representative ORDER BY id").rows == [
             [0, {"", 0}, {[], %{}}, []],
             [
               1,
               {"one", 1},
               {[1, 2, 3], %{"a" => "b", "nil" => nil}},
               [{"a", 1}, {"b", 2}]
             ],
             [18_446_744_073_709_551_615, {"", 0}, {[], %{"x" => nil}}, [{"max", 255}]]
           ]
  end

  test "RowBinary rejects tuple values with missing elements" do
    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([["not a tuple"]], ["Tuple(String, UInt8)"])
    end
  end

  defp rowbinary_tuple_rows do
    gen all ids <- uniq_list_of(integer(0..18_446_744_073_709_551_615), max_length: 16),
            tuples <- list_of(tuple_param(), length: length(ids)) do
      Enum.zip_with(ids, tuples, fn id, tuple -> [id, tuple] end)
    end
  end

  defp tuple_param do
    gen all string <- safe_string(),
            int <- integer(0..255),
            bool <- boolean() do
      {string, int, bool}
    end
  end

  defp safe_string do
    string(:printable, max_length: 32)
  end
end
