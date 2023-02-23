defmodule Ch.ConnectionTest do
  use ExUnit.Case

  setup do
    {:ok, conn: start_supervised!(Ch)}
  end

  describe "query" do
    test "select without params", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select 1")
    end

    test "select with types", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select 1", [], types: [:u8])
    end

    test "select with params", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select {a:UInt8}", %{"a" => 1})

      assert {:ok, %{num_rows: 1, rows: [[1.0]]}} =
               Ch.query(conn, "select {a:Float32}", %{"a" => 1.0})

      assert {:ok, %{num_rows: 1, rows: [["a&b=c"]]}} =
               Ch.query(conn, "select {a:String}", %{"a" => "a&b=c"})

      assert {:ok, %{num_rows: 1, rows: [[Decimal.new("2000.3330")]]}} ==
               Ch.query(conn, "select {a:Decimal(9,4)}", %{"a" => Decimal.new("2000.333")})

      assert {:ok, %{num_rows: 1, rows: [[~D[2022-01-01]]]}} ==
               Ch.query(conn, "select {a:Date}", %{"a" => ~D[2022-01-01]})

      assert {:ok, %{num_rows: 1, rows: [[~D[2022-01-01]]]}} ==
               Ch.query(conn, "select {a:Date32}", %{"a" => ~D[2022-01-01]})

      assert {:ok, %{num_rows: 1, rows: [[~N[2022-01-01 12:00:00]]]}} ==
               Ch.query(conn, "select {a:DateTime}", %{"a" => ~N[2022-01-01 12:00:00]})

      assert {:ok, %{num_rows: 1, rows: [[~N[2022-01-01 12:00:00.123000]]]}} ==
               Ch.query(conn, "select {a:DateTime64(3)}", %{"a" => ~N[2022-01-01 12:00:00.123]})

      assert {:ok, %{num_rows: 1, rows: [[~U[2022-01-01 12:00:00Z]]]}} ==
               Ch.query(conn, "select {a:DateTime('UTC')}", %{"a" => ~U[2022-01-01 12:00:00Z]})

      assert {:ok, %{num_rows: 1, rows: [[["a", "b'", "\\'c"]]]}} ==
               Ch.query(conn, "select {a:Array(String)}", %{"a" => ["a", "b'", "\\'c"]})

      assert {:ok, %{num_rows: 1, rows: [[[1, 2, 3]]]}} ==
               Ch.query(conn, "select {a:Array(UInt8)}", %{"a" => [1, 2, 3]})

      assert {:ok, %{num_rows: 1, rows: [[[[1], [2, 3], []]]]}} ==
               Ch.query(conn, "select {a:Array(Array(UInt8))}", %{"a" => [[1], [2, 3], []]})

      uuid = "9B29BD20-924C-4DE5-BDB3-8C2AA1FCE1FC"
      uuid_bin = uuid |> String.replace("-", "") |> Base.decode16!()

      assert {:ok, %{num_rows: 1, rows: [[^uuid_bin]]}} =
               Ch.query(conn, "select {a:UUID}", %{"a" => uuid})

      # TODO
      # assert {:ok, %{num_rows: 1, rows: [[^uuid_bin]]}} =
      #          Ch.query(conn, "select {a:UUID}", %{"a" => uuid_bin})

      # pseudo-positional bind
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select {$0:UInt8}", [1])
    end

    test "select with options", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "1"]]}} =
               Ch.query(conn, "show settings like 'async_insert'", [], settings: [async_insert: 1])

      assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "0"]]}} =
               Ch.query(conn, "show settings like 'async_insert'", [], settings: [async_insert: 0])
    end

    test "create", %{conn: conn} do
      assert {:ok, %{num_rows: 0, rows: []}} =
               Ch.query(conn, "create table create_example(a UInt8) engine = Memory")

      on_exit(fn -> Ch.Test.drop_table("create_example") end)
    end

    test "create with options", %{conn: conn} do
      assert {:error, %Ch.Error{code: 164, message: message}} =
               Ch.query(conn, "create table create_example(a UInt8) engine = Memory", [],
                 settings: [readonly: 1]
               )

      assert message =~ ~r/Cannot execute query in readonly mode/
    end

    test "insert", %{conn: conn} do
      assert {:ok, %{num_rows: 0, rows: []}} =
               Ch.query(conn, "create table insert_t(a UInt8, b String) engine = Memory")

      on_exit(fn -> Ch.Test.drop_table("insert_t") end)

      # values
      assert {:ok, %{num_rows: 2, rows: []}} =
               Ch.query(conn, "insert into insert_t values (1,'a'), (2,'b')")

      # readonly
      assert {:error, %Ch.Error{code: 164}} =
               Ch.query(conn, "insert into insert_t values (1,'a'), (2,'b')", [],
                 settings: [readonly: 1]
               )

      # chunked rowbinary stream
      stream =
        Stream.map([[3, "c"], [4, "d"]], fn row ->
          Ch.RowBinary.encode_row(row, [:u8, :string])
        end)

      assert {:ok, %{num_rows: 2, rows: []}} =
               Ch.query(conn, ["insert into ", "insert_t(a, b)"], stream, format: "RowBinary")

      assert {:ok, %{num_rows: 4, rows: [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]}} =
               Ch.query(conn, "select * from insert_t order by a")
    end

    test "delete", %{conn: conn} do
      assert {:ok, %{num_rows: 0, rows: []}} =
               Ch.query(
                 conn,
                 "create table delete_t(a UInt8, b String) engine = MergeTree order by tuple()"
               )

      on_exit(fn -> Ch.Test.drop_table("delete_t") end)

      assert {:ok, %{num_rows: 2, rows: []}} =
               Ch.query(conn, "insert into delete_t values (1,'a'), (2,'b')")

      assert {:ok, %{num_rows: 0, rows: []}} =
               Ch.query(conn, "delete from delete_t where 1", [],
                 settings: [allow_experimental_lightweight_delete: 1]
               )
    end
  end

  describe "query!" do
    test "select without params", %{conn: conn} do
      assert %{num_rows: 1, rows: [[1]]} = Ch.query!(conn, "select 1")
    end
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

      Ch.query!(conn, "create table fixed_string_t(a FixedString(3)) engine = Memory")
      on_exit(fn -> Ch.Test.drop_table("fixed_string_t") end)

      stream =
        Stream.map([[""], ["a"], ["aa"], ["aaa"]], fn row ->
          Ch.RowBinary.encode_row(row, [{:string, 3}])
        end)

      assert {:ok, %{num_rows: 4}} =
               Ch.query(conn, "insert into fixed_string_t(a)", stream, format: "RowBinary")

      assert {:ok,
              %{num_rows: 4, rows: [[<<0, 0, 0>>], ["a" <> <<0, 0>>], ["aa" <> <<0>>], ["aaa"]]}} =
               Ch.query(conn, "select * from fixed_string_t")
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

      Ch.query!(conn, "create table decimal_t(d Decimal(9,4)) engine = Memory")
      on_exit(fn -> Ch.Test.drop_table("decimal_t") end)

      assert %{num_rows: 3} =
               Ch.query!(
                 conn,
                 "insert into decimal_t(d)",
                 _rows =
                   Stream.map(
                     [
                       [Decimal.new("2.66")],
                       [Decimal.new("2.6666")],
                       [Decimal.new("2.66666")]
                     ],
                     fn row -> Ch.RowBinary.encode_row(row, [{:decimal, 9, 4}]) end
                   ),
                 format: "RowBinary"
               )

      assert %{
               num_rows: 3,
               rows: [
                 [Decimal.new("2.6600")],
                 [Decimal.new("2.6666")],
                 [Decimal.new("2.6667")]
               ]
             } == Ch.query!(conn, "select * from decimal_t")
    end

    test "boolean", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[true, "Bool"]]}} =
               Ch.query(conn, "select true as col, toTypeName(col)")

      assert {:ok, %{num_rows: 1, rows: [[1, "UInt8"]]}} =
               Ch.query(conn, "select true == 1 as col, toTypeName(col)")

      assert {:ok, %{num_rows: 1, rows: [[true, false]]}} = Ch.query(conn, "select true, false")

      Ch.query!(conn, "create table test_bool(A Int64, B Bool) engine = Memory")
      on_exit(fn -> Ch.Test.drop_table("test_bool") end)

      Ch.query!(conn, "INSERT INTO test_bool VALUES (1, true),(2,0)")

      Ch.query!(
        conn,
        "insert into test_bool(A, B)",
        Stream.map([[3, true], [4, false], [5, nil]], fn row ->
          Ch.RowBinary.encode_row(row, [:i64, :boolean])
        end),
        format: "RowBinary"
      )

      assert {:ok,
              %{num_rows: 5, rows: [[1, true], [2, false], [3, true], [4, false], [5, false]]}} =
               Ch.query(conn, "SELECT * FROM test_bool ORDER BY A")
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

      Ch.query!(conn, " CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog")
      on_exit(fn -> Ch.Test.drop_table("t_uuid") end)

      Ch.query!(conn, "INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'")

      assert {:ok, %{num_rows: 1, rows: [[<<_::16-bytes>>, "Example 1"]]}} =
               Ch.query(conn, "SELECT * FROM t_uuid")

      Ch.query!(conn, "INSERT INTO t_uuid (y) VALUES ('Example 2')")

      Ch.query!(
        conn,
        "insert into t_uuid(x,y)",
        Stream.map([[uuid, "Example 3"]], fn row ->
          Ch.RowBinary.encode_row(row, [:uuid, :string])
        end),
        format: "RowBinary"
      )

      assert {:ok,
              %{
                num_rows: 3,
                rows: [
                  [<<_::16-bytes>>, "Example 1"],
                  [<<0::128>>, "Example 2"],
                  [^uuid, "Example 3"]
                ]
              }} = Ch.query(conn, "SELECT * FROM t_uuid")
    end

    @tag skip: true
    test "json", %{conn: conn} do
      Ch.query!(conn, "CREATE TABLE json(o JSON) ENGINE = Memory")
      on_exit(fn -> Ch.Test.drop_table("json") end)

      Ch.query!(conn, ~s|INSERT INTO json VALUES ('{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}')|)

      assert {:ok, %{num_rows: 1, rows: [[1, 2, 3]]}} =
               Ch.query(conn, "SELECT o.a, o.b.c, o.b.d[3] FROM json")

      # TODO
      Ch.query(conn, "SELECT o FROM json")
    end

    test "enum", %{conn: conn} do
      Ch.query!(conn, "CREATE TABLE t_enum(x Enum('hello' = 1, 'world' = 2)) ENGINE = TinyLog")
      on_exit(fn -> Ch.Test.drop_table("t_enum") end)

      Ch.query!(conn, "INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')")

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
      Ch.query!(conn, "CREATE TABLE table_map (a Map(String, UInt64)) ENGINE=Memory")
      on_exit(fn -> Ch.Test.drop_table("table_map") end)

      Ch.query!(
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
      Ch.query!(
        conn,
        "CREATE TABLE dt(`timestamp` DateTime('Asia/Istanbul'), `event_id` UInt8) ENGINE = TinyLog"
      )

      on_exit(fn -> Ch.Test.drop_table("dt") end)

      Ch.query!(conn, "INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2)")

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

    # TODO are negatives correct? what's the range?
    test "date32", %{conn: conn} do
      Ch.query!(conn, "CREATE TABLE new(`timestamp` Date32, `event_id` UInt8) ENGINE = TinyLog;")
      on_exit(fn -> Ch.Test.drop_table("new") end)

      Ch.query!(conn, "INSERT INTO new VALUES (4102444800, 1), ('2100-01-01', 2)")

      assert {:ok, %{num_rows: 2, rows: [[~D[2100-01-01], 1], [~D[2100-01-01], 2]]}} =
               Ch.query(conn, "SELECT * FROM new")

      assert {:ok, %{num_rows: 1, rows: [[~D[1900-01-01]]]}} =
               Ch.query(conn, "select {date:Date32}", %{"date" => ~D[1900-01-01]})

      # TODO strange stuff, one day is lost
      assert {:ok, %{num_rows: 1, rows: [[~D[2299-12-31]]]}} =
               Ch.query(conn, "select {date:Date32}", %{"date" => ~D[2300-01-01]})

      Ch.query!(
        conn,
        "insert into new",
        Ch.RowBinary.encode_rows([[~D[1960-01-01], 3]], [:date32, :u8]),
        format: "RowBinary"
      )

      assert %{
               num_rows: 3,
               rows: [
                 [~D[2100-01-01], 1],
                 [~D[2100-01-01], 2],
                 [~D[1960-01-01], 3]
               ]
             } = Ch.query!(conn, "SELECT * FROM new")

      assert %{num_rows: 1, rows: [[3]]} =
               Ch.query!(conn, "SELECT event_id FROM new WHERE timestamp = '1960-01-01'")
    end

    test "datetime64", %{conn: conn} do
      Ch.query!(
        conn,
        "CREATE TABLE dt(`timestamp` DateTime64(3, 'Asia/Istanbul'), `event_id` UInt8) ENGINE = TinyLog"
      )

      on_exit(fn -> Ch.Test.drop_table("dt") end)

      Ch.query!(
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

      Ch.query!(
        conn,
        "insert into dt(timestamp, event_id)",
        Ch.RowBinary.encode_rows(
          [[~N[2021-01-01 12:00:00.123456], 4], [~N[2021-01-01 12:00:00], 5]],
          [{:datetime64, :millisecond}, :u8]
        ),
        format: "RowBinary"
      )

      assert {
               :ok,
               %{
                 num_rows: 2,
                 rows: [
                   [DateTime.new!(~D[2021-01-01], ~T[15:00:00.123], "Asia/Istanbul"), 4],
                   [DateTime.new!(~D[2021-01-01], ~T[15:00:00.000], "Asia/Istanbul"), 5]
                 ]
               }
             } == Ch.query(conn, "SELECT * FROM dt WHERE timestamp > '2020-01-01'")

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

    test "nullable", %{conn: conn} do
      Ch.query!(
        conn,
        "CREATE TABLE nullable (`n` Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple()"
      )

      on_exit(fn -> Ch.Test.drop_table("nullable") end)

      Ch.query!(conn, "INSERT INTO nullable VALUES (1) (NULL) (2) (NULL)")

      assert {:ok, %{num_rows: 4, rows: [[0], [1], [0], [1]]}} =
               Ch.query(conn, "SELECT n.null FROM nullable")

      assert {:ok, %{num_rows: 4, rows: [[1], [nil], [2], [nil]]}} =
               Ch.query(conn, "SELECT n FROM nullable")
    end
  end

  describe "options" do
    @tag capture_log: true
    test "can provide custom timeout", %{conn: conn} do
      assert {:error, %Mint.TransportError{reason: :timeout} = error} =
               Ch.query(conn, "select sleep(1)", _params = [], timeout: 100)

      assert Exception.message(error) == "timeout"
    end

    test "errors on invalid creds", %{conn: conn} do
      assert {:error, %Ch.Error{code: 516} = error} =
               Ch.query(conn, "select 1 + 1", _params = [],
                 username: "no-exists",
                 password: "wrong"
               )

      assert Exception.message(error) =~
               "Code: 516. DB::Exception: no-exists: Authentication failed: password is incorrect or there is no user with such name. (AUTHENTICATION_FAILED)"
    end

    test "errors on invalid database", %{conn: conn} do
      assert {:error, %Ch.Error{code: 81} = error} =
               Ch.query(conn, "select 1 + 1", _params = [], database: "no-db")

      assert Exception.message(error) =~
               "Code: 81. DB::Exception: Database `no-db` doesn't exist. (UNKNOWN_DATABASE)"
    end

    test "can provide custom database", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[2]]}} =
               Ch.query(conn, "select 1 + 1", [], database: "default")
    end
  end

  # transactions are not supported by clickhouse but
  # we still allow them for Repo.checkout
  describe "transactions" do
    test "commit", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[2]]}} =
               DBConnection.transaction(conn, fn conn ->
                 Ch.query!(conn, "select 1 + 1")
               end)
    end

    test "rollback", %{conn: conn} do
      assert_raise Ch.Error, ~r/UNKNOWN_TABLE/, fn ->
        DBConnection.transaction(conn, fn conn ->
          Ch.query!(conn, "select * from non_table")
        end)
      end
    end

    test "status", %{conn: conn} do
      assert :idle == DBConnection.status(conn)
    end
  end

  # since clickhouse doesn't have prepared statement
  # we return the query as is from handle_prepare
  describe "prepare" do
    test "no-op", %{conn: conn} do
      query = Ch.Query.build("select 1 + 1", [])
      assert {:ok, query} == DBConnection.prepare(conn, query)
    end
  end

  describe "streams" do
    test "not supported", %{conn: conn} do
      assert_raise Ch.Error, "cursors are not supported", fn ->
        DBConnection.transaction(conn, fn conn ->
          query = Ch.Query.build("select 1 + 1", [])

          conn
          |> DBConnection.stream(query, [])
          |> Stream.run()
        end)
      end
    end
  end
end
