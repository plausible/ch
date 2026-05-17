defmodule Ch.ConnectionTest do
  use ExUnit.Case, async: true

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "selects without params", %{pool: pool} do
    assert Ch.query!(pool, "select 1").rows == [[1]]
  end

  test "accepts query settings", %{pool: pool} do
    assert Ch.query!(pool, "show settings like 'async_insert'", %{}, settings: [async_insert: 1]).rows ==
             [["async_insert", "Bool", "1"]]

    assert Ch.query!(pool, "show settings like 'async_insert'", %{}, settings: [async_insert: 0]).rows ==
             [["async_insert", "Bool", "0"]]
  end

  test "creates and drops a table", %{pool: pool} do
    Ch.query!(pool, "CREATE TABLE connection_test_create(a UInt8) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE connection_test_create") end)

    assert Ch.query!(pool, "SHOW TABLES LIKE 'connection_test_create'").rows == [
             ["connection_test_create"]
           ]
  end

  test "inserts values and insert-selects rows", %{pool: pool} do
    Help.query!("CREATE TABLE connection_test_insert(a UInt8 DEFAULT 1, b String) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE connection_test_insert") end)

    assert Ch.query!(pool, """
           INSERT INTO connection_test_insert VALUES
           (1, 'a'), (2, 'b'), (NULL, NULL)
           """) == nil

    assert Ch.query!(pool, "SELECT * FROM connection_test_insert ORDER BY a, b").rows == [
             [1, ""],
             [1, "a"],
             [2, "b"]
           ]

    assert Ch.query!(
             pool,
             """
             INSERT INTO connection_test_insert(a, b)
             SELECT a, b FROM connection_test_insert WHERE a > {min:UInt8}
             """,
             %{"min" => 1}
           ) == nil

    assert Ch.query!(pool, "SELECT * FROM connection_test_insert WHERE a > 1").rows == [
             [2, "b"],
             [2, "b"]
           ]
  end

  test "inserts RowBinary data", %{pool: pool} do
    Help.query!("CREATE TABLE connection_test_rowbinary(a UInt8, b String) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE connection_test_rowbinary") end)

    rows = [[1, "a"], [2, "b"], [3, "c"]]
    rowbinary = RowBinary.encode_rows(rows, ["UInt8", "String"])

    assert Ch.query!(pool, [
             "INSERT INTO connection_test_rowbinary FORMAT RowBinary\n" | rowbinary
           ]) == nil

    assert Ch.query!(pool, "SELECT * FROM connection_test_rowbinary ORDER BY a").rows == rows
  end

  test "returns readonly errors", %{pool: pool} do
    Help.query!("CREATE TABLE connection_test_readonly(a UInt8) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE connection_test_readonly") end)

    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "INSERT INTO connection_test_readonly VALUES (1)", %{},
               settings: [readonly: 1]
             )

    assert message =~ "Cannot execute query in readonly mode"
  end

  test "deletes rows", %{pool: pool} do
    Help.query!("""
    CREATE TABLE connection_test_delete(a UInt8, b String)
    ENGINE MergeTree
    ORDER BY tuple()
    """)

    on_exit(fn -> Help.query!("DROP TABLE connection_test_delete") end)

    Ch.query!(pool, "INSERT INTO connection_test_delete VALUES (1, 'a'), (2, 'b')")

    assert Ch.query!(pool, "DELETE FROM connection_test_delete WHERE 1", %{},
             settings: [mutations_sync: 1]
           ) == nil

    assert Ch.query!(pool, "SELECT * FROM connection_test_delete").rows == []
  end

  test "decodes representative scalar types", %{pool: pool} do
    assert Ch.query!(pool, """
           SELECT
             -1::Int8,
             1::UInt8,
             true,
             'abc'::String,
             toDecimal32(2, 4),
             '417ddc5d-e556-4d27-95dd-a34d84e46a50'::UUID,
             '2022-01-01'::Date,
             '1900-01-01'::Date32
           """).rows == [
             [
               -1,
               1,
               true,
               "abc",
               Decimal.new("2.0000"),
               Base.decode16!("417ddc5de5564d2795dda34d84e46a50", case: :lower),
               ~D[2022-01-01],
               ~D[1900-01-01]
             ]
           ]
  end

  test "decodes compound types", %{pool: pool} do
    assert Ch.query!(pool, """
           SELECT
             map('hello', 100::UInt64, 'pg', 13::UInt64),
             tuple('a', 1),
             [1, 2, 3],
             CAST((10, 20), 'Point')
           """).rows == [
             [
               %{"hello" => 100, "pg" => 13},
               {"a", 1},
               [1, 2, 3],
               {10.0, 20.0}
             ]
           ]
  end

  test "inserts and selects nullable/default values", %{pool: pool} do
    Help.query!("""
    CREATE TABLE connection_test_nulls (
      a UInt8,
      b Nullable(UInt8),
      c UInt8 DEFAULT 10,
      d Nullable(UInt8) DEFAULT 10
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE connection_test_nulls") end)

    rowbinary =
      RowBinary.encode_rows(
        [[nil, nil, nil, nil]],
        ["UInt8", "Nullable(UInt8)", "UInt8", "Nullable(UInt8)"]
      )

    Ch.query!(pool, [
      "INSERT INTO connection_test_nulls(a, b, c, d) FORMAT RowBinary\n" | rowbinary
    ])

    assert Ch.query!(pool, "SELECT * FROM connection_test_nulls").rows == [[0, nil, 0, nil]]
  end

  test "inserts RowBinaryWithNamesAndTypes", %{pool: pool} do
    Help.query!("""
    CREATE TABLE connection_test_names_types (
      country_code FixedString(2),
      rare_string LowCardinality(String),
      maybe_int32 Nullable(Int32)
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE connection_test_names_types") end)

    names = ["country_code", "rare_string", "maybe_int32"]
    types = ["FixedString(2)", "LowCardinality(String)", "Nullable(Int32)"]
    rows = [["AB", "rare", -42], ["CD", "other", nil]]

    rowbinary = [
      RowBinary.encode_names_and_types(names, types)
      | RowBinary.encode_rows(rows, types)
    ]

    Ch.query!(pool, [
      "INSERT INTO connection_test_names_types FORMAT RowBinaryWithNamesAndTypes\n"
      | rowbinary
    ])

    assert Ch.query!(pool, "SELECT * FROM connection_test_names_types ORDER BY country_code").rows ==
             rows
  end

  test "selects many columns in RowBinaryWithNamesAndTypes", %{pool: pool} do
    select = Enum.map_join(1..1000, ", ", fn i -> "#{i} AS col_#{i}" end)

    assert %{names: columns, rows: [row]} = Ch.query!(pool, "SELECT #{select}")

    assert length(columns) == 1000
    assert List.first(columns) == "col_1"
    assert List.last(columns) == "col_1000"
    assert length(row) == 1000
    assert List.first(row) == 1
    assert List.last(row) == 1000
  end
end
