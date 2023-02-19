defmodule Ch.ConnectionTest do
  use ExUnit.Case

  setup_all do
    conn = start_supervised!(Ch)
    Application.put_env(:elixir, :time_zone_database, Ch.Tzdb)
    on_exit(fn -> Application.delete_env(:elixir, :time_zone_database) end)
    {:ok, conn: conn}
  end

  test "select 1", %{conn: conn} do
    assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select 1")
  end

  test "select param", %{conn: conn} do
    assert {:ok, %{num_rows: 1, rows: [["a"]]}} =
             Ch.query(conn, "select {a:String}", %{"a" => "a"})
  end

  describe "types" do
    test "multiple types", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[1, "a"]]}} =
               Ch.query(conn, "select {a:Int8}, {b:String}", %{"a" => 1, "b" => "a"})
    end

    test "ints", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select {a:Int8}", %{"a" => 1})

      assert {:ok, %{num_rows: 1, rows: [[-1000]]}} =
               Ch.query(conn, "select {a:Int16}", %{"a" => -1000})

      assert {:ok, %{num_rows: 1, rows: [[100_000]]}} =
               Ch.query(conn, "select {a:Int32}", %{"a" => 100_000})

      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select {a:Int64}", %{"a" => 1})
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select {a:Int128}", %{"a" => 1})
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select {a:Int256}", %{"a" => 1})
    end

    test "uints", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select {a:UInt8}", %{"a" => 1})
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select {a:UInt16}", %{"a" => 1})
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select {a:UInt32}", %{"a" => 1})
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select {a:UInt64}", %{"a" => 1})

      assert {:ok, %{num_rows: 1, rows: [[1]]}} =
               Ch.query(conn, "select {a:UInt128}", %{"a" => 1})

      assert {:ok, %{num_rows: 1, rows: [[1]]}} =
               Ch.query(conn, "select {a:UInt256}", %{"a" => 1})
    end

    test "fixed string", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[<<0, 0>>]]}} =
               Ch.query(conn, "select {a:FixedString(2)}", %{"a" => ""})

      assert {:ok, %{num_rows: 1, rows: [["a" <> <<0>>]]}} =
               Ch.query(conn, "select {a:FixedString(2)}", %{"a" => "a"})

      assert {:ok, %{num_rows: 1, rows: [["aa"]]}} =
               Ch.query(conn, "select {a:FixedString(2)}", %{"a" => "aa"})

      assert {:ok, %{num_rows: 1, rows: [["aaaaa"]]}} =
               Ch.query(conn, "select {a:FixedString(5)}", %{"a" => "aaaaa"})
    end

    test "decimal", %{conn: conn} do
      assert {:ok,
              %{
                num_rows: 1,
                rows: [[Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(9, 4)"]]
              }} ==
               Ch.query(conn, "SELECT toDecimal32(2, 4) AS x, x / 3, toTypeName(x)")

      assert {:ok,
              %{
                num_rows: 1,
                rows: [[Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(18, 4)"]]
              }} ==
               Ch.query(conn, "SELECT toDecimal64(2, 4) AS x, x / 3, toTypeName(x)")

      assert {:ok,
              %{
                num_rows: 1,
                rows: [[Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(38, 4)"]]
              }} ==
               Ch.query(conn, "SELECT toDecimal128(2, 4) AS x, x / 3, toTypeName(x)")

      assert {:ok,
              %{
                num_rows: 1,
                rows: [[Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(76, 4)"]]
              }} ==
               Ch.query(conn, "SELECT toDecimal256(2, 4) AS x, x / 3, toTypeName(x)")
    end

    test "boolean", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[true, "Bool"]]}} =
               Ch.query(conn, "select true as col, toTypeName(col)")

      assert {:ok, %{num_rows: 1, rows: [[1, "UInt8"]]}} =
               Ch.query(conn, "select true == 1 as col, toTypeName(col)")

      assert {:ok, %{num_rows: 1, rows: [[true, false]]}} = Ch.query(conn, "select true, false")

      # TODO query!
      Ch.query(conn, "create table test_bool(A Int64, B Bool) engine = Memory")
      on_exit(fn -> Ch.query(conn, "drop table test_bool") end)

      Ch.query(conn, "INSERT INTO test_bool VALUES (1, true),(2,0)")

      assert {:ok, %{num_rows: 2, rows: [[1, true], [2, false]]}} =
               Ch.query(conn, "SELECT * FROM test_bool")
    end

    test "uuid", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[<<_::16-bytes>>]]}} =
               Ch.query(conn, "select generateUUIDv4()")

      assert {:ok, %{num_rows: 1, rows: [[uuid]]}} =
               Ch.query(conn, "select {uuid:UUID}", %{
                 "uuid" => "417ddc5d-e556-4d27-95dd-a34d84e46a50"
               })

      assert uuid ==
               "417ddc5d-e556-4d27-95dd-a34d84e46a50"
               |> String.replace("-", "")
               |> Base.decode16!(case: :lower)

      Ch.query(conn, " CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog")
      on_exit(fn -> Ch.query(conn, "drop table t_uuid") end)

      Ch.query(conn, "INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'")

      assert {:ok, %{num_rows: 1, rows: [[<<_::16-bytes>>, "Example 1"]]}} =
               Ch.query(conn, "SELECT * FROM t_uuid")

      Ch.query(conn, "INSERT INTO t_uuid (y) VALUES ('Example 2')")

      assert {:ok,
              %{
                num_rows: 2,
                rows: [[<<_::16-bytes>>, "Example 1"], [<<0::128>>, "Example 2"]]
              }} = Ch.query(conn, "SELECT * FROM t_uuid")
    end

    @tag skip: true
    test "json", %{conn: conn} do
      Ch.query(conn, "CREATE TABLE json(o JSON) ENGINE = Memory")
      on_exit(fn -> Ch.query(conn, "drop table json") end)

      Ch.query(conn, ~s|INSERT INTO json VALUES ('{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}')|)

      assert {:ok, %{num_rows: 1, rows: [[1, 2, 3]]}} =
               Ch.query(conn, "SELECT o.a, o.b.c, o.b.d[3] FROM json")

      # TODO
      Ch.query(conn, "SELECT o FROM json")
    end

    test "enum", %{conn: conn} do
      Ch.query(conn, "CREATE TABLE t_enum(x Enum('hello' = 1, 'world' = 2)) ENGINE = TinyLog")
      on_exit(fn -> Ch.query(conn, "drop table t_enum") end)

      Ch.query(conn, "INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')")

      assert {:error,
              %Ch.Error{
                code: 36,
                message:
                  "Code: 36. DB::Exception: Unknown element 'a' for enum: While executing ValuesBlockInputFormat. (BAD_ARGUMENTS)" <>
                    _
              }} = Ch.query(conn, "INSERT INTO t_enum values('a')")

      assert {:ok, %{num_rows: 3, rows: [["hello"], ["world"], ["hello"]]}} =
               Ch.query(conn, "SELECT * FROM t_enum")

      assert {:ok, %{num_rows: 3, rows: [[1], [2], [1]]}} =
               Ch.query(conn, "SELECT CAST(x, 'Int8') FROM t_enum")

      assert {:ok, %{num_rows: 1, rows: [["Enum8('a' = 1, 'b' = 2)"]]}} =
               Ch.query(conn, "SELECT toTypeName(CAST('a', 'Enum(\\'a\\' = 1, \\'b\\' = 2)'))")

      assert {:ok, %{num_rows: 1, rows: [["a"]]}} =
               Ch.query(conn, "SELECT CAST('a', 'Enum(\\'a\\' = 1, \\'b\\' = 2)')")

      assert {:ok, %{num_rows: 1, rows: [["b"]]}} =
               Ch.query(conn, "select {enum:Enum('a' = 1, 'b' = 2)}", %{"enum" => "b"})

      assert {:ok, %{num_rows: 1, rows: [["b"]]}} =
               Ch.query(conn, "select {enum:Enum('a' = 1, 'b' = 2)}", %{"enum" => 2})
    end

    @tag skip: true
    test "map", %{conn: conn} do
      Ch.query(conn, "CREATE TABLE table_map (a Map(String, UInt64)) ENGINE=Memory")
      on_exit(fn -> Ch.query(conn, "drop table table_map") end)

      Ch.query(
        conn,
        "INSERT INTO table_map VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30})"
      )

      assert {:ok, %{num_rows: 3, rows: [[10], [20], [30]]}} =
               Ch.query(conn, "SELECT a['key2'] FROM table_map")

      assert_raise ArgumentError, "Map(String, UInt64) type is not supported", fn ->
        Ch.query(conn, "select a from table_map")
      end
    end

    test "datetime", %{conn: conn} do
      Ch.query(
        conn,
        "CREATE TABLE dt(`timestamp` DateTime('Asia/Istanbul'), `event_id` UInt8) ENGINE = TinyLog"
      )

      on_exit(fn -> Ch.query(conn, "drop table dt") end)

      Ch.query(conn, "INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2)")

      assert {:ok,
              %{
                num_rows: 2,
                rows: [
                  [DateTime.new!(~D[2019-01-01], ~T[03:00:00], "Asia/Istanbul"), 1],
                  [DateTime.new!(~D[2019-01-01], ~T[00:00:00], "Asia/Istanbul"), 2]
                ]
              }} == Ch.query(conn, "SELECT * FROM dt")

      assert {:ok, %{num_rows: 1, rows: [[~N[2022-12-12 12:00:00]]]}} =
               Ch.query(conn, "select {dt:DateTime}", %{"dt" => ~N[2022-12-12 12:00:00]})

      assert {:ok, %{num_rows: 1, rows: [[~U[2022-12-12 12:00:00Z]]]}} =
               Ch.query(conn, "select {dt:DateTime('UTC')}", %{"dt" => ~N[2022-12-12 12:00:00]})

      assert {:ok,
              %{
                num_rows: 1,
                rows: [[DateTime.new!(~D[2022-12-12], ~T[12:00:00], "Asia/Bangkok")]]
              }} ==
               Ch.query(conn, "select {dt:DateTime('Asia/Bangkok')}", %{
                 "dt" => ~N[2022-12-12 12:00:00]
               })
    end

    test "date32", %{conn: conn} do
      Ch.query(conn, "CREATE TABLE new(`timestamp` Date32, `event_id` UInt8) ENGINE = TinyLog;")
      on_exit(fn -> Ch.query(conn, "drop table new") end)

      Ch.query(conn, "INSERT INTO new VALUES (4102444800, 1), ('2100-01-01', 2)")

      assert {:ok, %{num_rows: 2, rows: [[~D[2100-01-01], 1], [~D[2100-01-01], 2]]}} =
               Ch.query(conn, "SELECT * FROM new")

      assert {:ok, %{num_rows: 1, rows: [[~D[1900-01-01]]]}} =
               Ch.query(conn, "select {date:Date32}", %{"date" => ~D[1900-01-01]})

      # TODO strange stuff, one day is lost
      assert {:ok, %{num_rows: 1, rows: [[~D[2299-12-31]]]}} =
               Ch.query(conn, "select {date:Date32}", %{"date" => ~D[2300-01-01]})
    end

    test "datetime64", %{conn: conn} do
      Ch.query(
        conn,
        "CREATE TABLE dt(`timestamp` DateTime64(3, 'Asia/Istanbul'), `event_id` UInt8) ENGINE = TinyLog"
      )

      on_exit(fn -> Ch.query(conn, "drop table dt") end)

      Ch.query(
        conn,
        "INSERT INTO dt Values (1546300800123, 1), (1546300800.123, 2), ('2019-01-01 00:00:00', 3)"
      )

      assert {
               :ok,
               %{
                 num_rows: 3,
                 rows: [
                   [DateTime.new!(~D[2019-01-01], ~T[03:00:00.123], "Asia/Istanbul"), 1],
                   [DateTime.new!(~D[2019-01-01], ~T[03:00:00.123], "Asia/Istanbul"), 2],
                   [DateTime.new!(~D[2019-01-01], ~T[00:00:00.000], "Asia/Istanbul"), 3]
                 ]
               }
             } == Ch.query(conn, "SELECT * FROM dt")

      for precision <- 0..9 do
        expected = ~N[2022-01-01 12:00:00]

        assert {:ok, %{num_rows: 1, rows: [[datetime]]}} =
                 Ch.query(conn, "select {dt:DateTime64(#{precision})}", %{"dt" => expected})

        assert NaiveDateTime.compare(datetime, ~N[2022-01-01 12:00:00]) == :eq
      end

      assert {:ok, %{num_rows: 1, rows: [[~U[2022-01-01 12:00:00.123Z]]]}} =
               Ch.query(conn, "select {dt:DateTime64(3,'UTC')}", %{
                 "dt" => ~N[2022-01-01 12:00:00.123]
               })

      assert {:ok, %{num_rows: 1, rows: [[~U[1900-01-01 12:00:00.123Z]]]}} =
               Ch.query(conn, "select {dt:DateTime64(3,'UTC')}", %{
                 "dt" => ~N[1900-01-01 12:00:00.123]
               })

      assert {:ok,
              %{
                num_rows: 1,
                rows: [[DateTime.new!(~D[2022-01-01], ~T[12:00:00.123], "Asia/Bangkok")]]
              }} ==
               Ch.query(conn, "select {dt:DateTime64(3,'Asia/Bangkok')}", %{
                 "dt" => ~N[2022-01-01 12:00:00.123]
               })
    end
  end

  test "nullable", %{conn: conn} do
    Ch.query(
      conn,
      "CREATE TABLE nullable (`n` Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple()"
    )

    on_exit(fn -> Ch.query(conn, "drop table nullable") end)

    Ch.query(conn, "INSERT INTO nullable VALUES (1) (NULL) (2) (NULL)")

    assert {:ok, %{num_rows: 4, rows: [[0], [1], [0], [1]]}} =
             Ch.query(conn, "SELECT n.null FROM nullable")

    assert {:ok, %{num_rows: 4, rows: [[1], [nil], [2], [nil]]}} =
             Ch.query(conn, "SELECT n FROM nullable")

    # TODO nullable array, map, tuple, decimal, fixed string
  end
end
