defmodule Ch.ConnectionTest do
  use ExUnit.Case, async: true

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "selects without params", %{pool: pool} do
    assert Ch.query!(pool, "select 1").rows == [[1]]
  end

  test "selects with named params", %{pool: pool} do
    assert Ch.query!(pool, "select {a:UInt8}", %{"a" => 1}).rows == [[1]]
    assert Ch.query!(pool, "select {b:Bool}", %{"b" => true}).rows == [[true]]
    assert Ch.query!(pool, "select {b:Bool}", %{"b" => false}).rows == [[false]]
    assert Ch.query!(pool, "select {n:Nullable(Nothing)}", %{"n" => nil}).rows == [[nil]]
    assert Ch.query!(pool, "select {a:Float32}", %{"a" => 1.0}).rows == [[1.0]]
    assert Ch.query!(pool, "select {a:String}", %{"a" => "a&b=c"}).rows == [["a&b=c"]]
    assert Ch.query!(pool, "select {a:String}", %{"a" => "a\n"}).rows == [["a\n"]]
    assert Ch.query!(pool, "select {a:String}", %{"a" => "a\t"}).rows == [["a\t"]]

    assert Ch.query!(pool, "select {a:Array(String)}", %{"a" => ["a\tb"]}).rows == [
             [["a\tb"]]
           ]

    assert Ch.query!(pool, "select {a:Array(Bool)}", %{"a" => [true, false]}).rows == [
             [[true, false]]
           ]

    assert Ch.query!(pool, "select {a:Array(Nullable(String))}", %{
             "a" => ["a", nil, "b"]
           }).rows == [[["a", nil, "b"]]]

    assert Ch.query!(pool, "select {a:Decimal(9,4)}", %{
             "a" => Decimal.new("2000.333")
           }).rows == [[Decimal.new("2000.3330")]]

    assert Ch.query!(pool, "select {a:Date}", %{"a" => ~D[2022-01-01]}).rows == [
             [~D[2022-01-01]]
           ]

    assert Ch.query!(pool, "select {a:Date32}", %{"a" => ~D[2022-01-01]}).rows == [
             [~D[2022-01-01]]
           ]

    uuid = "9B29BD20-924C-4DE5-BDB3-8C2AA1FCE1FC"
    uuid_bin = uuid |> String.replace("-", "") |> Base.decode16!()

    assert Ch.query!(pool, "select {a:UUID}", %{"a" => uuid}).rows == [[uuid_bin]]
  end

  test "accepts query settings", %{pool: pool} do
    assert Ch.query!(pool, "show settings like 'async_insert'", %{}, settings: [async_insert: 1]).rows ==
             [["async_insert", "Bool", "1"]]

    assert Ch.query!(pool, "show settings like 'async_insert'", %{}, settings: [async_insert: 0]).rows ==
             [["async_insert", "Bool", "0"]]
  end

  test "creates and drops a table", %{pool: pool} do
    Ch.query!(pool, "CREATE TABLE connection_test_create(a UInt8) ENGINE Memory")

    on_exit(fn ->
      {:ok, cleanup} = Ch.start_link()
      Ch.query!(cleanup, "DROP TABLE connection_test_create")
      Ch.stop(cleanup)
    end)

    assert Ch.query!(pool, "SHOW TABLES LIKE 'connection_test_create'").rows == [
             ["connection_test_create"]
           ]
  end

  test "returns readonly errors for create", %{pool: pool} do
    assert {:error, %Ch.Error{message: message}} =
             Ch.query(
               pool,
               "CREATE TABLE connection_test_create_readonly(a UInt8) ENGINE Memory",
               %{},
               settings: [readonly: 1]
             )

    assert message =~ "Cannot execute query in readonly mode"
  end

  test "inserts values and insert-selects rows", %{pool: pool} do
    Ch.query!(
      pool,
      "CREATE TABLE connection_test_insert(a UInt8 DEFAULT 1, b String) ENGINE Memory"
    )

    on_exit(fn ->
      {:ok, cleanup} = Ch.start_link()
      Ch.query!(cleanup, "DROP TABLE connection_test_insert")
      Ch.stop(cleanup)
    end)

    assert %Ch.Result{names: nil, rows: nil, data: nil} =
             Ch.query!(pool, """
             INSERT INTO connection_test_insert VALUES
             (1, 'a'), (2, 'b'), (NULL, NULL)
             """)

    assert Ch.query!(pool, "SELECT * FROM connection_test_insert ORDER BY a, b").rows == [
             [1, ""],
             [1, "a"],
             [2, "b"]
           ]

    assert %Ch.Result{names: nil, rows: nil, data: nil} =
             Ch.query!(
               pool,
               """
               INSERT INTO connection_test_insert(a, b)
               SELECT a, b FROM connection_test_insert WHERE a > {min:UInt8}
               """,
               %{"min" => 1}
             )

    assert Ch.query!(pool, "SELECT * FROM connection_test_insert WHERE a > 1").rows == [
             [2, "b"],
             [2, "b"]
           ]
  end

  test "inserts RowBinary data", %{pool: pool} do
    Ch.query!(pool, "CREATE TABLE connection_test_rowbinary(a UInt8, b String) ENGINE Memory")

    on_exit(fn ->
      {:ok, cleanup} = Ch.start_link()
      Ch.query!(cleanup, "DROP TABLE connection_test_rowbinary")
      Ch.stop(cleanup)
    end)

    rows = [[1, "a"], [2, "b"], [3, "c"]]
    rowbinary = RowBinary.encode_rows(rows, ["UInt8", "String"])

    assert %Ch.Result{names: nil, rows: nil, data: nil} =
             Ch.query!(pool, [
               "INSERT INTO connection_test_rowbinary FORMAT RowBinary\n" | rowbinary
             ])

    assert Ch.query!(pool, "SELECT * FROM connection_test_rowbinary ORDER BY a").rows == rows
  end

  test "returns readonly errors", %{pool: pool} do
    Ch.query!(pool, "CREATE TABLE connection_test_readonly(a UInt8) ENGINE Memory")

    on_exit(fn ->
      {:ok, cleanup} = Ch.start_link()
      Ch.query!(cleanup, "DROP TABLE connection_test_readonly")
      Ch.stop(cleanup)
    end)

    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "INSERT INTO connection_test_readonly VALUES (1)", %{},
               settings: [readonly: 1]
             )

    assert message =~ "Cannot execute query in readonly mode"
  end

  test "deletes rows", %{pool: pool} do
    Ch.query!(pool, """
    CREATE TABLE connection_test_delete(a UInt8, b String)
    ENGINE MergeTree
    ORDER BY tuple()
    """)

    on_exit(fn ->
      {:ok, cleanup} = Ch.start_link()
      Ch.query!(cleanup, "DROP TABLE connection_test_delete")
      Ch.stop(cleanup)
    end)

    Ch.query!(pool, "INSERT INTO connection_test_delete VALUES (1, 'a'), (2, 'b')")

    assert %Ch.Result{names: nil, rows: nil, data: nil} =
             Ch.query!(pool, "DELETE FROM connection_test_delete WHERE 1", %{},
               settings: [mutations_sync: 1]
             )

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
    Ch.query!(pool, """
    CREATE TABLE connection_test_nulls (
      a UInt8,
      b Nullable(UInt8),
      c UInt8 DEFAULT 10,
      d Nullable(UInt8) DEFAULT 10
    ) ENGINE Memory
    """)

    on_exit(fn ->
      {:ok, cleanup} = Ch.start_link()
      Ch.query!(cleanup, "DROP TABLE connection_test_nulls")
      Ch.stop(cleanup)
    end)

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

  test "inserts nullable input rows and applies defaults", %{pool: pool} do
    Ch.query!(pool, """
    CREATE TABLE connection_test_input_default(
      n Int32,
      s String DEFAULT 'secret'
    ) ENGINE Memory
    """)

    on_exit(fn ->
      {:ok, cleanup} = Ch.start_link()
      Ch.query!(cleanup, "DROP TABLE connection_test_input_default")
      Ch.stop(cleanup)
    end)

    rows = [[1, nil], [4_294_967_295, nil]]
    rowbinary = RowBinary.encode_rows(rows, ["UInt32", "Nullable(String)"])

    Ch.query!(pool, [
      """
      INSERT INTO connection_test_input_default
        SELECT id, name
        FROM input('id UInt32, name Nullable(String)')
        FORMAT RowBinary
      """
      | rowbinary
    ])

    assert Ch.query!(pool, "SELECT * FROM connection_test_input_default ORDER BY n").rows == [
             [-1, "secret"],
             [1, "secret"]
           ]
  end

  test "inserts RowBinaryWithNamesAndTypes", %{pool: pool} do
    Ch.query!(pool, """
    CREATE TABLE connection_test_names_types (
      country_code FixedString(2),
      rare_string LowCardinality(String),
      maybe_int32 Nullable(Int32)
    ) ENGINE Memory
    """)

    on_exit(fn ->
      {:ok, cleanup} = Ch.start_link()
      Ch.query!(cleanup, "DROP TABLE connection_test_names_types")
      Ch.stop(cleanup)
    end)

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

  test "returns RowBinaryWithNamesAndTypes type mismatch errors", %{pool: pool} do
    Ch.query!(pool, """
    CREATE TABLE connection_test_names_types_mismatch (
      country_code FixedString(2),
      rare_string LowCardinality(String),
      maybe_int32 Nullable(Int32)
    ) ENGINE Memory
    """)

    on_exit(fn ->
      {:ok, cleanup} = Ch.start_link()
      Ch.query!(cleanup, "DROP TABLE connection_test_names_types_mismatch")
      Ch.stop(cleanup)
    end)

    names = ["country_code", "rare_string", "maybe_int32"]
    rows = [["AB", "rare", -42]]

    rowbinary = [
      RowBinary.encode_names_and_types(names, ["FixedString(2)", "String", "Nullable(Int32)"])
      | RowBinary.encode_rows(rows, ["FixedString(2)", "String", "Nullable(Int32)"])
    ]

    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, [
               "INSERT INTO connection_test_names_types_mismatch FORMAT RowBinaryWithNamesAndTypes\n"
               | rowbinary
             ])

    assert message =~ "Type of 'rare_string' must be LowCardinality(String), not String"
  end

  test "inserts and selects geo types", %{pool: pool} do
    Ch.query!(pool, "CREATE TABLE connection_test_geo_point(p Point) ENGINE Memory")
    Ch.query!(pool, "CREATE TABLE connection_test_geo_ring(r Ring) ENGINE Memory")
    Ch.query!(pool, "CREATE TABLE connection_test_geo_polygon(pg Polygon) ENGINE Memory")

    Ch.query!(
      pool,
      "CREATE TABLE connection_test_geo_multipolygon(mp MultiPolygon) ENGINE Memory"
    )

    on_exit(fn ->
      {:ok, cleanup} = Ch.start_link()
      Ch.query!(cleanup, "DROP TABLE connection_test_geo_point")
      Ch.query!(cleanup, "DROP TABLE connection_test_geo_ring")
      Ch.query!(cleanup, "DROP TABLE connection_test_geo_polygon")
      Ch.query!(cleanup, "DROP TABLE connection_test_geo_multipolygon")
      Ch.stop(cleanup)
    end)

    Ch.query!(pool, "INSERT INTO connection_test_geo_point VALUES((10, 10))")

    Ch.query!(pool, [
      "INSERT INTO connection_test_geo_point FORMAT RowBinary\n",
      RowBinary.encode_rows([[{20, 20}]], ["Point"])
    ])

    assert Ch.query!(pool, "SELECT p FROM connection_test_geo_point ORDER BY p").rows == [
             [{10.0, 10.0}],
             [{20.0, 20.0}]
           ]

    ring = [{20, 20}, {0, 0}, {0, 20}]

    Ch.query!(
      pool,
      "INSERT INTO connection_test_geo_ring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)])"
    )

    Ch.query!(pool, [
      "INSERT INTO connection_test_geo_ring FORMAT RowBinary\n",
      RowBinary.encode_rows([[ring]], ["Ring"])
    ])

    assert Ch.query!(pool, "SELECT r FROM connection_test_geo_ring ORDER BY r").rows == [
             [[{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}]],
             [[{20.0, 20.0}, {0.0, 0.0}, {0.0, 20.0}]]
           ]

    polygon = [[{0, 1.0}, {10, 3.2}], [], [{2, 2}]]

    Ch.query!(
      pool,
      "INSERT INTO connection_test_geo_polygon VALUES([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]])"
    )

    Ch.query!(pool, [
      "INSERT INTO connection_test_geo_polygon FORMAT RowBinary\n",
      RowBinary.encode_rows([[polygon]], ["Polygon"])
    ])

    assert Ch.query!(pool, "SELECT pg FROM connection_test_geo_polygon ORDER BY pg").rows == [
             [[[{0.0, 1.0}, {10.0, 3.2}], [], [{2.0, 2.0}]]],
             [
               [
                 [{20.0, 20.0}, {50.0, 20.0}, {50.0, 50.0}, {20.0, 50.0}],
                 [{30.0, 30.0}, {50.0, 50.0}, {50.0, 30.0}]
               ]
             ]
           ]

    multipolygon = [[[{0.0, 1.0}, {10.0, 3.0}], [], [{2, 2}]], [], [[{3, 3}]]]

    Ch.query!(
      pool,
      "INSERT INTO connection_test_geo_multipolygon VALUES([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]]])"
    )

    Ch.query!(pool, [
      "INSERT INTO connection_test_geo_multipolygon FORMAT RowBinary\n",
      RowBinary.encode_rows([[multipolygon]], ["MultiPolygon"])
    ])

    assert Ch.query!(pool, "SELECT mp FROM connection_test_geo_multipolygon ORDER BY mp").rows ==
             [
               [
                 [
                   [[{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}]],
                   [
                     [{20.0, 20.0}, {50.0, 20.0}, {50.0, 50.0}, {20.0, 50.0}],
                     [{30.0, 30.0}, {50.0, 50.0}, {50.0, 30.0}]
                   ]
                 ]
               ],
               [[[[{0.0, 1.0}, {10.0, 3.0}], [], [{2.0, 2.0}]], [], [[{3.0, 3.0}]]]]
             ]
  end

  test "accepts database and auth through headers", %{pool: pool} do
    Ch.query!(pool, "CREATE DATABASE connection_test_database_header")

    on_exit(fn ->
      {:ok, cleanup} = Ch.start_link()
      Ch.query!(cleanup, "DROP DATABASE connection_test_database_header")
      Ch.stop(cleanup)
    end)

    Ch.query!(
      pool,
      "CREATE TABLE connection_test_database_header.example(a UInt8) ENGINE Memory"
    )

    assert Ch.query!(pool, "SHOW TABLES", %{},
             headers: [{"x-clickhouse-database", "connection_test_database_header"}]
           ).rows == [["example"]]

    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "SELECT 1", %{},
               headers: [{"x-clickhouse-user", "no-exists"}, {"x-clickhouse-key", "wrong"}]
             )

    assert message =~ "AUTHENTICATION_FAILED"

    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "SELECT 1", %{}, headers: [{"x-clickhouse-database", "no-db"}])

    assert message =~ "UNKNOWN_DATABASE"
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
