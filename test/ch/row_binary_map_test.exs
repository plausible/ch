defmodule Ch.RowBinaryMapTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "Map params round-trip through ClickHouse", %{pool: pool} do
    check all map <- map_of(safe_key(), integer(0..255), max_length: 8) do
      assert Ch.query!(pool, "SELECT {value:Map(String, UInt8)}", %{"value" => map}).rows == [
               [map]
             ]
    end
  end

  property "RowBinary Map inserts round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_map_property (
      id UInt64,
      value Map(String, UInt8)
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_map_property") end)

    check all rows <- rowbinary_map_rows(), max_runs: 25 do
      Ch.query!(pool, "TRUNCATE TABLE row_binary_map_property")

      rowbinary = RowBinary.encode_rows(rows, ["UInt64", "Map(String, UInt8)"])
      Ch.query!(pool, ["INSERT INTO row_binary_map_property FORMAT RowBinary\n" | rowbinary])

      assert Ch.query!(pool, "SELECT * FROM row_binary_map_property ORDER BY id").rows ==
               Enum.sort_by(rows, &List.first/1)
    end
  end

  test "RowBinary Map inserts cover nested and nullable values", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_map_representative (
      id UInt64,
      strings Map(String, String),
      nullable_strings Map(String, Nullable(String)),
      arrays Map(String, Array(UInt8)),
      tuple_values Map(String, Tuple(UInt8, Bool))
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_map_representative") end)

    rows = [
      [0, %{}, %{}, %{}, %{}],
      [
        1,
        %{"a" => "b"},
        %{"present" => "value", "missing" => nil},
        %{"nums" => [1, 2, 3], "empty" => []},
        %{"one" => {1, true}, "zero" => {0, false}}
      ]
    ]

    types = [
      "UInt64",
      "Map(String, String)",
      "Map(String, Nullable(String))",
      "Map(String, Array(UInt8))",
      "Map(String, Tuple(UInt8, Bool))"
    ]

    rowbinary = RowBinary.encode_rows(rows, types)
    Ch.query!(pool, ["INSERT INTO row_binary_map_representative FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_map_representative ORDER BY id").rows == rows
  end

  test "RowBinary accepts key-value lists and rejects invalid map values" do
    assert IO.iodata_to_binary(RowBinary.encode({:map, :string, :u8}, [{"a", 1}, {"b", 2}])) ==
             IO.iodata_to_binary(RowBinary.encode({:map, :string, :u8}, [{"a", 1}, {"b", 2}]))

    assert_raise CaseClauseError, fn ->
      RowBinary.encode({:map, :string, :u8}, a: 1)
    end

    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([["not a map"]], ["Map(String, UInt8)"])
    end
  end

  defp rowbinary_map_rows do
    gen all ids <- uniq_list_of(integer(0..18_446_744_073_709_551_615), max_length: 16),
            maps <-
              list_of(map_of(safe_key(), integer(0..255), max_length: 8), length: length(ids)) do
      Enum.zip_with(ids, maps, fn id, map -> [id, map] end)
    end
  end

  defp safe_key do
    string(:alphanumeric, min_length: 1, max_length: 16)
  end
end
