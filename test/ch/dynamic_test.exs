defmodule Ch.DynamicTest do
  use ExUnit.Case

  @moduletag :dynamic

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "it works", %{conn: conn} do
    dynamic = fn literal ->
      [row] = Ch.query!(conn, "select #{literal}::Dynamic as d, dynamicType(d)").rows
      row
    end

    assert dynamic.("'Hello, World!'") == ["Hello, World!", "String"]
    assert dynamic.("0") == ["0", "String"]
    assert dynamic.("true") == [true, "Bool"]
    assert dynamic.("(1+1)") == [2, "UInt16"]
    assert dynamic.("['a', 'b', 'c']::Array(String)") == [["a", "b", "c"], "Array(String)"]

    assert dynamic.("[[1,2,3], [1,2], [3]]::Array(Array(UInt8))") == [
             [[1, 2, 3], [1, 2], [3]],
             "Array(Array(UInt8))"
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/dynamic#creating-dynamic
  test "creating dynamic", %{conn: conn} do
    # Using Dynamic type in table column definition:
    Ch.query!(conn, "CREATE TABLE test (d Dynamic) ENGINE = Memory;")
    on_exit(fn -> Ch.Test.query("DROP TABLE test", [], database: Ch.Test.database()) end)
    Ch.query!(conn, "INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);")

    assert Ch.query!(conn, "SELECT d, dynamicType(d) FROM test;").rows == [
             [nil, "None"],
             [42, "Int64"],
             ["Hello, World!", "String"],
             [[1, 2, 3], "Array(Int64)"]
           ]

    # Using CAST from ordinary column:
    assert Ch.query!(conn, "SELECT 'Hello, World!'::Dynamic AS d, dynamicType(d);").rows == [
             ["Hello, World!", "String"]
           ]

    # Using CAST from Variant column:
    assert Ch.query!(
             conn,
             "SELECT multiIf((number % 3) = 0, number, (number % 3) = 1, range(number + 1), NULL)::Dynamic AS d, dynamicType(d) FROM numbers(3)",
             [],
             settings: [
               enable_variant_type: 1,
               use_variant_as_common_type: 1
             ]
           ).rows == [
             [0, "UInt64"],
             [[0, 1], "Array(UInt64)"],
             [nil, "None"]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/dynamic#reading-dynamic-nested-types-as-subcolumns
  test "reading dynamic nested types as subcolumns", %{conn: conn} do
    Ch.query!(conn, "CREATE TABLE test (d Dynamic) ENGINE = Memory;")
    on_exit(fn -> Ch.Test.query("DROP TABLE test", [], database: Ch.Test.database()) end)
    Ch.query!(conn, "INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);")

    assert Ch.query!(
             conn,
             "SELECT d, dynamicType(d), d.String, d.Int64, d.`Array(Int64)`, d.Date, d.`Array(String)` FROM test;"
           ).rows == [
             [nil, "None", nil, nil, [], nil, []],
             [42, "Int64", nil, 42, [], nil, []],
             ["Hello, World!", "String", "Hello, World!", nil, [], nil, []],
             [[1, 2, 3], "Array(Int64)", nil, nil, [1, 2, 3], nil, []]
           ]

    assert Ch.query!(
             conn,
             "SELECT toTypeName(d.String), toTypeName(d.Int64), toTypeName(d.`Array(Int64)`), toTypeName(d.Date), toTypeName(d.`Array(String)`)  FROM test LIMIT 1;"
           ).rows == [
             [
               "Nullable(String)",
               "Nullable(Int64)",
               "Array(Int64)",
               "Nullable(Date)",
               "Array(String)"
             ]
           ]

    assert Ch.query!(
             conn,
             "SELECT d, dynamicType(d), dynamicElement(d, 'String'), dynamicElement(d, 'Int64'), dynamicElement(d, 'Array(Int64)'), dynamicElement(d, 'Date'), dynamicElement(d, 'Array(String)') FROM test;"
           ).rows == [
             [nil, "None", nil, nil, [], nil, []],
             [42, "Int64", nil, 42, [], nil, []],
             ["Hello, World!", "String", "Hello, World!", nil, [], nil, []],
             [[1, 2, 3], "Array(Int64)", nil, nil, [1, 2, 3], nil, []]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/dynamic#converting-a-string-column-to-a-dynamic-column-through-parsing
  test "converting a string column to a dynamic column through parsing", %{conn: conn} do
    assert Ch.query!(
             conn,
             "SELECT CAST(materialize(map('key1', '42', 'key2', 'true', 'key3', '2020-01-01')), 'Map(String, Dynamic)') as map_of_dynamic, mapApply((k, v) -> (k, dynamicType(v)), map_of_dynamic) as map_of_dynamic_types;",
             [],
             settings: [cast_string_to_dynamic_use_inference: 1]
           ).rows == [
             [
               %{"key1" => 42, "key2" => true, "key3" => ~D[2020-01-01]},
               %{"key1" => "Int64", "key2" => "Bool", "key3" => "Date"}
             ]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/dynamic#converting-a-dynamic-column-to-an-ordinary-column
  test "converting a dynamic column to an ordinary column", %{conn: conn} do
    Ch.query!(conn, "CREATE TABLE test (d Dynamic) ENGINE = Memory;")
    on_exit(fn -> Ch.Test.query("DROP TABLE test", [], database: Ch.Test.database()) end)
    Ch.query!(conn, "INSERT INTO test VALUES (NULL), (42), ('42.42'), (true), ('e10');")

    assert Ch.query!(conn, "SELECT d::Nullable(Float64) FROM test;").rows == [
             [nil],
             [42.0],
             [42.42],
             [1.0],
             [0.0]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/dynamic#converting-a-variant-column-to-dynamic-column
end
