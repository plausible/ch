defmodule Ch.ConnectionTest do
  use ExUnit.Case
  alias Ch.RowBinary

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "select without params", %{conn: conn} do
    assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select 1")
  end

  test "select with types", %{conn: conn} do
    assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select 1", [], types: ["UInt8"])
  end

  test "select with params", %{conn: conn} do
    assert {:ok, %{num_rows: 1, rows: [[1]]}} = Ch.query(conn, "select {a:UInt8}", %{"a" => 1})

    assert {:ok, %{num_rows: 1, rows: [[true]]}} =
             Ch.query(conn, "select {b:Bool}", %{"b" => true})

    assert {:ok, %{num_rows: 1, rows: [[false]]}} =
             Ch.query(conn, "select {b:Bool}", %{"b" => false})

    assert {:ok, %{num_rows: 1, rows: [[1.0]]}} =
             Ch.query(conn, "select {a:Float32}", %{"a" => 1.0})

    assert {:ok, %{num_rows: 1, rows: [["a&b=c"]]}} =
             Ch.query(conn, "select {a:String}", %{"a" => "a&b=c"})

    assert {:ok, %{num_rows: 1, rows: [["a\n"]]}} =
             Ch.query(conn, "select {a:String}", %{"a" => "a\n"})

    assert {:ok, %{num_rows: 1, rows: [["a\t"]]}} =
             Ch.query(conn, "select {a:String}", %{"a" => "a\t"})

    assert {:ok, %{num_rows: 1, rows: [row]}} =
             Ch.query(conn, "select {a:Decimal(9,4)}", %{"a" => Decimal.new("2000.333")})

    assert row == [Decimal.new("2000.3330")]

    assert {:ok, %{num_rows: 1, rows: [[~D[2022-01-01]]]}} =
             Ch.query(conn, "select {a:Date}", %{"a" => ~D[2022-01-01]})

    assert {:ok, %{num_rows: 1, rows: [[~D[2022-01-01]]]}} =
             Ch.query(conn, "select {a:Date32}", %{"a" => ~D[2022-01-01]})

    naive_noon = ~N[2022-01-01 12:00:00]

    # datetimes in params are sent in text and ClickHouse translates them to UTC from server timezone by default
    # see https://clickhouse.com/docs/en/sql-reference/data-types/datetime
    #     https://kb.altinity.com/altinity-kb-queries-and-syntax/time-zones/
    assert {:ok, %{num_rows: 1, rows: [[naive_datetime]], headers: headers}} =
             Ch.query(conn, "select {naive:DateTime}", %{"naive" => naive_noon})

    # to make this test pass for contributors with non UTC timezone we perform the same steps as ClickHouse
    # i.e. we give server timezone to the naive datetime and shift it to UTC before comparing with the result
    {_, timezone} = List.keyfind!(headers, "x-clickhouse-timezone", 0)

    assert naive_datetime ==
             naive_noon
             |> DateTime.from_naive!(timezone)
             |> DateTime.shift_zone!("Etc/UTC")
             |> DateTime.to_naive()

    # when the timezone information is provided in the type, we don't need to rely on server timezone
    assert {:ok, %{num_rows: 1, rows: [[bkk_datetime]]}} =
             Ch.query(conn, "select {$0:DateTime('Asia/Bangkok')}", [naive_noon])

    assert bkk_datetime == DateTime.from_naive!(naive_noon, "Asia/Bangkok")

    assert {:ok, %{num_rows: 1, rows: [[~U[2022-01-01 12:00:00Z]]]}} =
             Ch.query(conn, "select {$0:DateTime('UTC')}", [naive_noon])

    naive_noon_ms = ~N[2022-01-01 12:00:00.123]

    assert {:ok, %{num_rows: 1, rows: [[naive_datetime]]}} =
             Ch.query(conn, "select {$0:DateTime64(3)}", [naive_noon_ms])

    assert NaiveDateTime.compare(
             naive_datetime,
             naive_noon_ms
             |> DateTime.from_naive!(timezone)
             |> DateTime.shift_zone!("Etc/UTC")
             |> DateTime.to_naive()
           ) == :eq

    assert {:ok, %{num_rows: 1, rows: [[["a", "b'", "\\'c"]]]}} =
             Ch.query(conn, "select {a:Array(String)}", %{"a" => ["a", "b'", "\\'c"]})

    assert {:ok, %{num_rows: 1, rows: [[["a\n", "b\tc"]]]}} =
             Ch.query(conn, "select {a:Array(String)}", %{"a" => ["a\n", "b\tc"]})

    assert {:ok, %{num_rows: 1, rows: [[[1, 2, 3]]]}} =
             Ch.query(conn, "select {a:Array(UInt8)}", %{"a" => [1, 2, 3]})

    assert {:ok, %{num_rows: 1, rows: [[[[1], [2, 3], []]]]}} =
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

  test "utc datetime query param encoding", %{conn: conn} do
    utc = ~U[2021-01-01 12:00:00Z]
    msk = DateTime.new!(~D[2021-01-01], ~T[15:00:00], "Europe/Moscow")
    naive = utc |> DateTime.shift_zone!(Ch.Test.clickhouse_tz(conn)) |> DateTime.to_naive()

    assert Ch.query!(conn, "select {$0:DateTime} as d, toString(d)", [utc]).rows ==
             [[~N[2021-01-01 12:00:00], to_string(naive)]]

    assert Ch.query!(conn, "select {$0:DateTime('UTC')} as d, toString(d)", [utc]).rows ==
             [[utc, "2021-01-01 12:00:00"]]

    assert Ch.query!(conn, "select {$0:DateTime('Europe/Moscow')} as d, toString(d)", [utc]).rows ==
             [[msk, "2021-01-01 15:00:00"]]
  end

  test "utc datetime64 query param encoding", %{conn: conn} do
    utc = ~U[2021-01-01 12:00:00.123456Z]
    msk = DateTime.new!(~D[2021-01-01], ~T[15:00:00.123456], "Europe/Moscow")
    naive = utc |> DateTime.shift_zone!(Ch.Test.clickhouse_tz(conn)) |> DateTime.to_naive()

    assert Ch.query!(conn, "select {$0:DateTime64(6)} as d, toString(d)", [utc]).rows ==
             [[~N[2021-01-01 12:00:00.123456], to_string(naive)]]

    assert Ch.query!(conn, "select {$0:DateTime64(6, 'UTC')} as d, toString(d)", [utc]).rows ==
             [[utc, "2021-01-01 12:00:00.123456"]]

    assert Ch.query!(conn, "select {$0:DateTime64(6,'Europe/Moscow')} as d, toString(d)", [utc]).rows ==
             [[msk, "2021-01-01 15:00:00.123456"]]

    # this test case gaurds against a previous bug where DateTimes with a microsecond value of 0 and precision > 0 would
    # get encoded as a val like "1.6095024e9" which Clickhouse would be unable to parse to a DateTime.
    utc_with_zero_microsec = ~U[2021-01-01 12:00:00.000000Z]
    naive_with_zero_microsec = utc_with_zero_microsec |> DateTime.shift_zone!(Ch.Test.clickhouse_tz(conn)) |> DateTime.to_naive()
    assert Ch.query!(conn, "select {$0:DateTime64(6)} as d, toString(d)", [utc_with_zero_microsec]).rows ==
             [[~N[2021-01-01 12:00:00.000000], to_string(naive_with_zero_microsec)]]
  end

  test "select with options", %{conn: conn} do
    assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "1"]]}} =
             Ch.query(conn, "show settings like 'async_insert'", [], settings: [async_insert: 1])

    assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "0"]]}} =
             Ch.query(conn, "show settings like 'async_insert'", [], settings: [async_insert: 0])
  end

  test "create", %{conn: conn} do
    assert {:ok, %{num_rows: nil, rows: []}} =
             Ch.query(conn, "create table create_example(a UInt8) engine = Memory")
  end

  test "create with options", %{conn: conn} do
    assert {:error, %Ch.Error{code: 164, message: message}} =
             Ch.query(conn, "create table create_example(a UInt8) engine = Memory", [],
               settings: [readonly: 1]
             )

    assert message =~ ~r/Cannot execute query in readonly mode/
  end

  describe "insert" do
    setup %{conn: conn} do
      table = "insert_t_#{System.unique_integer([:positive])}"

      Ch.query!(
        conn,
        "create table #{table}(a UInt8 default 1, b String) engine = Memory"
      )

      {:ok, table: table}
    end

    test "values", %{conn: conn, table: table} do
      assert {:ok, %{num_rows: 3}} =
               Ch.query(
                 conn,
                 "insert into {table:Identifier} values (1, 'a'),(2,'b'),   (null,       null)",
                 %{"table" => table}
               )

      assert {:ok, %{rows: rows}} =
               Ch.query(conn, "select * from {table:Identifier}", %{"table" => table})

      assert rows == [[1, "a"], [2, "b"], [1, ""]]

      assert {:ok, %{num_rows: 2}} =
               Ch.query(
                 conn,
                 "insert into {$0:Identifier}(a, b) values ({$1:UInt8},{$2:String}),({$3:UInt8},{$4:String})",
                 [table, 4, "d", 5, "e"]
               )

      assert {:ok, %{rows: rows}} =
               Ch.query(conn, "select * from {table:Identifier} where a >= 4", %{"table" => table})

      assert rows == [[4, "d"], [5, "e"]]
    end

    test "when readonly", %{conn: conn, table: table} do
      settings = [readonly: 1]

      assert {:error, %Ch.Error{code: 164, message: message}} =
               Ch.query(
                 conn,
                 "insert into {table:Identifier} values (1, 'a'), (2, 'b')",
                 %{"table" => table},
                 settings: settings
               )

      assert message =~ "Cannot execute query in readonly mode."
    end

    test "automatic RowBinary", %{conn: conn, table: table} do
      stmt = "insert into #{table}(a, b) format RowBinary"
      types = ["UInt8", "String"]
      rows = [[1, "a"], [2, "b"]]
      assert %{num_rows: 2} = Ch.query!(conn, stmt, rows, types: types)

      assert %{rows: rows} =
               Ch.query!(conn, "select * from {table:Identifier}", %{"table" => table})

      assert rows == [[1, "a"], [2, "b"]]
    end

    test "manual RowBinary", %{conn: conn, table: table} do
      stmt = "insert into #{table}(a, b) format RowBinary"

      types = ["UInt8", "String"]
      rows = [[1, "a"], [2, "b"]]
      data = RowBinary.encode_rows(rows, types)

      assert %{num_rows: 2} = Ch.query!(conn, stmt, data, encode: false)

      assert %{rows: rows} =
               Ch.query!(conn, "select * from {table:Identifier}", %{"table" => table})

      assert rows == [[1, "a"], [2, "b"]]
    end

    test "chunked", %{conn: conn, table: table} do
      types = ["UInt8", "String"]
      rows = [[1, "a"], [2, "b"], [3, "c"]]

      stream =
        rows
        |> Stream.chunk_every(2)
        |> Stream.map(fn chunk -> RowBinary.encode_rows(chunk, types) end)

      assert {:ok, %{num_rows: 3}} =
               Ch.query(
                 conn,
                 "insert into #{table}(a, b) format RowBinary",
                 stream,
                 encode: false
               )

      assert {:ok, %{rows: rows}} =
               Ch.query(conn, "select * from {table:Identifier}", %{"table" => table})

      assert rows == [[1, "a"], [2, "b"], [3, "c"]]
    end

    test "select", %{conn: conn, table: table} do
      assert {:ok, %{num_rows: 3}} =
               Ch.query(
                 conn,
                 "insert into {table:Identifier} values (1, 'a'), (2, 'b'), (null, null)",
                 %{"table" => table}
               )

      assert {:ok, %{num_rows: 3}} =
               Ch.query(
                 conn,
                 "insert into {table:Identifier}(a, b) select a, b from {table:Identifier}",
                 %{"table" => table}
               )

      assert {:ok, %{rows: rows}} =
               Ch.query(conn, "select * from {table:Identifier}", %{"table" => table})

      assert rows == [[1, "a"], [2, "b"], [1, ""], [1, "a"], [2, "b"], [1, ""]]

      assert {:ok, %{num_rows: 2}} =
               Ch.query(
                 conn,
                 "insert into {$0:Identifier}(a, b) select a, b from {$0:Identifier} where a > {$1:UInt8}",
                 [table, 1]
               )

      assert {:ok, %{rows: new_rows}} =
               Ch.query(conn, "select * from {table:Identifier}", %{"table" => table})

      assert new_rows -- rows == [[2, "b"], [2, "b"]]
    end
  end

  test "delete", %{conn: conn} do
    Ch.query!(
      conn,
      "create table delete_t(a UInt8, b String) engine = MergeTree order by tuple()"
    )

    assert {:ok, %{num_rows: 2}} = Ch.query(conn, "insert into delete_t values (1,'a'), (2,'b')")

    settings = [allow_experimental_lightweight_delete: 1]

    assert {:ok, %{rows: [], command: :delete}} =
             Ch.query(conn, "delete from delete_t where 1", [], settings: settings)
  end

  test "query!", %{conn: conn} do
    assert %{num_rows: 1, rows: [[1]]} = Ch.query!(conn, "select 1")
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

      assert {:ok, %{num_rows: 4}} =
               Ch.query(
                 conn,
                 "insert into fixed_string_t(a) format RowBinary",
                 [
                   [""],
                   ["a"],
                   ["aa"],
                   ["aaa"]
                 ],
                 types: ["FixedString(3)"]
               )

      assert Ch.query!(conn, "select * from fixed_string_t").rows == [
               [<<0, 0, 0>>],
               ["a" <> <<0, 0>>],
               ["aa" <> <<0>>],
               ["aaa"]
             ]
    end

    test "decimal", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [row]}} =
               Ch.query(conn, "SELECT toDecimal32(2, 4) AS x, x / 3, toTypeName(x)")

      assert row == [Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(9, 4)"]

      assert {:ok, %{num_rows: 1, rows: [row]}} =
               Ch.query(conn, "SELECT toDecimal64(2, 4) AS x, x / 3, toTypeName(x)")

      assert row == [Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(18, 4)"]

      assert {:ok, %{num_rows: 1, rows: [row]}} =
               Ch.query(conn, "SELECT toDecimal128(2, 4) AS x, x / 3, toTypeName(x)")

      assert row == [Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(38, 4)"]

      assert {:ok, %{num_rows: 1, rows: [row]}} =
               Ch.query(conn, "SELECT toDecimal256(2, 4) AS x, x / 3, toTypeName(x)")

      assert row == [Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(76, 4)"]

      Ch.query!(conn, "create table decimal_t(d Decimal32(4)) engine = Memory")

      assert %{num_rows: 3} =
               Ch.query!(
                 conn,
                 "insert into decimal_t(d) format RowBinary",
                 _rows = [
                   [Decimal.new("2.66")],
                   [Decimal.new("2.6666")],
                   [Decimal.new("2.66666")]
                 ],
                 types: ["Decimal32(4)"]
               )

      assert Ch.query!(conn, "select * from decimal_t").rows == [
               [Decimal.new("2.6600")],
               [Decimal.new("2.6666")],
               [Decimal.new("2.6667")]
             ]
    end

    test "boolean", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[true, "Bool"]]}} =
               Ch.query(conn, "select true as col, toTypeName(col)")

      assert {:ok, %{num_rows: 1, rows: [[1, "UInt8"]]}} =
               Ch.query(conn, "select true == 1 as col, toTypeName(col)")

      assert {:ok, %{num_rows: 1, rows: [[true, false]]}} = Ch.query(conn, "select true, false")

      Ch.query!(conn, "create table test_bool(A Int64, B Bool) engine = Memory")

      Ch.query!(conn, "INSERT INTO test_bool VALUES (1, true),(2,0)")

      Ch.query!(
        conn,
        "insert into test_bool(A, B) format RowBinary",
        _rows = [[3, true], [4, false]],
        types: ["Int64", "Bool"]
      )

      # anything > 0 is `true`, here `2` is `true`
      Ch.query!(conn, "insert into test_bool(A, B) values (5, 2)")

      assert %{
               rows: [
                 [1, true, 1],
                 [2, false, 0],
                 [3, true, 3],
                 [4, false, 0],
                 [5, true, 5]
               ]
             } = Ch.query!(conn, "SELECT *, A * B FROM test_bool ORDER BY A")
    end

    test "uuid", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [[<<_::16-bytes>>]]}} =
               Ch.query(conn, "select generateUUIDv4()")

      assert {:ok, %{num_rows: 1, rows: [[uuid, "417ddc5d-e556-4d27-95dd-a34d84e46a50"]]}} =
               Ch.query(conn, "select {uuid:UUID} as u, toString(u)", %{
                 "uuid" => "417ddc5d-e556-4d27-95dd-a34d84e46a50"
               })

      assert uuid ==
               "417ddc5d-e556-4d27-95dd-a34d84e46a50"
               |> String.replace("-", "")
               |> Base.decode16!(case: :lower)

      Ch.query!(conn, " CREATE TABLE t_uuid (x UUID, y String) ENGINE Memory")
      Ch.query!(conn, "INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'")

      assert {:ok, %{num_rows: 1, rows: [[<<_::16-bytes>>, "Example 1"]]}} =
               Ch.query(conn, "SELECT * FROM t_uuid")

      Ch.query!(conn, "INSERT INTO t_uuid (y) VALUES ('Example 2')")

      Ch.query!(
        conn,
        "insert into t_uuid(x,y) format RowBinary",
        _rows = [[uuid, "Example 3"]],
        types: ["UUID", "String"]
      )

      assert {:ok,
              %{
                num_rows: 3,
                rows: [
                  [<<_::16-bytes>>, "Example 1"],
                  [<<0::128>>, "Example 2"],
                  [^uuid, "Example 3"]
                ]
              }} = Ch.query(conn, "SELECT * FROM t_uuid ORDER BY y")
    end

    test "json", %{conn: conn} do
      settings = [allow_experimental_object_type: 1]

      Ch.query!(conn, "CREATE TABLE json(o JSON) ENGINE = Memory", [], settings: settings)

      Ch.query!(conn, ~s|INSERT INTO json VALUES ('{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}')|)

      assert Ch.query!(conn, "SELECT o.a, o.b.c, o.b.d[3] FROM json").rows == [[1, 2, 3]]

      # named tuples are not supported yet
      assert_raise ArgumentError, fn -> Ch.query!(conn, "SELECT o FROM json") end
    end

    # TODO enum16

    test "enum8", %{conn: conn} do
      assert {:ok, %{num_rows: 1, rows: [["Enum8('a' = 1, 'b' = 2)"]]}} =
               Ch.query(conn, "SELECT toTypeName(CAST('a', 'Enum(\\'a\\' = 1, \\'b\\' = 2)'))")

      assert {:ok, %{num_rows: 1, rows: [["a"]]}} =
               Ch.query(conn, "SELECT CAST('a', 'Enum(\\'a\\' = 1, \\'b\\' = 2)')")

      assert {:ok, %{num_rows: 1, rows: [["b"]]}} =
               Ch.query(conn, "select {enum:Enum('a' = 1, 'b' = 2)}", %{"enum" => "b"})

      assert {:ok, %{num_rows: 1, rows: [["b"]]}} =
               Ch.query(conn, "select {enum:Enum('a' = 1, 'b' = 2)}", %{"enum" => 2})

      assert {:ok, %{num_rows: 1, rows: [["b"]]}} =
               Ch.query(conn, "select {enum:Enum16('a' = 1, 'b' = 2)}", %{"enum" => 2})

      Ch.query!(
        conn,
        "CREATE TABLE t_enum(i UInt8, x Enum('hello' = 1, 'world' = 2)) ENGINE Memory"
      )

      Ch.query!(conn, "INSERT INTO t_enum VALUES (0, 'hello'), (1, 'world'), (2, 'hello')")

      assert Ch.query!(conn, "SELECT *, CAST(x, 'Int8') FROM t_enum ORDER BY i").rows == [
               [0, "hello", 1],
               [1, "world", 2],
               [2, "hello", 1]
             ]

      Ch.query!(
        conn,
        "INSERT INTO t_enum(i, x) FORMAT RowBinary",
        _rows = [[3, "hello"], [4, "world"], [5, 1], [6, 2]],
        types: ["UInt8", "Enum8('hello' = 1, 'world' = 2)"]
      )

      assert Ch.query!(conn, "SELECT *, CAST(x, 'Int8') FROM t_enum ORDER BY i").rows == [
               [0, "hello", 1],
               [1, "world", 2],
               [2, "hello", 1],
               [3, "hello", 1],
               [4, "world", 2],
               [5, "hello", 1],
               [6, "world", 2]
             ]

      # TODO nil enum
    end

    test "map", %{conn: conn} do
      assert Ch.query!(
               conn,
               "SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map"
             ).rows == [[%{1 => "Ready", 2 => "Steady", 3 => "Go"}]]

      assert Ch.query!(conn, "select {map:Map(String, UInt8)}", %{
               "map" => %{"pg" => 13, "hello" => 100}
             }).rows == [[%{"hello" => 100, "pg" => 13}]]

      Ch.query!(conn, "CREATE TABLE table_map (a Map(String, UInt64)) ENGINE=Memory")

      Ch.query!(
        conn,
        "INSERT INTO table_map VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30})"
      )

      assert Ch.query!(conn, "SELECT a['key2'] FROM table_map").rows == [[10], [20], [30]]

      assert Ch.query!(conn, "INSERT INTO table_map VALUES ({'key3':100}), ({})")

      assert Ch.query!(conn, "SELECT a['key3'] FROM table_map ORDER BY 1 DESC").rows == [
               [100],
               [0],
               [0],
               [0],
               [0]
             ]

      assert Ch.query!(
               conn,
               "INSERT INTO table_map FORMAT RowBinary",
               _rows = [
                 [%{"key10" => 20, "key20" => 40}],
                 # empty map
                 [%{}],
                 # null map
                 [nil],
                 # empty proplist map
                 [[]],
                 [[{"key50", 100}]]
               ],
               types: ["Map(String, UInt64)"]
             )

      assert Ch.query!(conn, "SELECT * FROM table_map ORDER BY a ASC").rows == [
               [%{}],
               [%{}],
               [%{}],
               [%{}],
               [%{"key1" => 1, "key2" => 10}],
               [%{"key1" => 2, "key2" => 20}],
               [%{"key1" => 3, "key2" => 30}],
               [%{"key10" => 20, "key20" => 40}],
               [%{"key3" => 100}],
               [%{"key50" => 100}]
             ]
    end

    test "tuple", %{conn: conn} do
      assert Ch.query!(conn, "SELECT tuple(1,'a') AS x, toTypeName(x)").rows == [
               [{1, "a"}, "Tuple(UInt8, String)"]
             ]

      assert Ch.query!(conn, "SELECT {$0:Tuple(Int8, String)}", [{-1, "abs"}]).rows == [
               [{-1, "abs"}]
             ]

      assert Ch.query!(conn, "SELECT tuple('a') AS x").rows == [[{"a"}]]

      assert Ch.query!(conn, "SELECT tuple(1, NULL) AS x, toTypeName(x)").rows == [
               [{1, nil}, "Tuple(UInt8, Nullable(Nothing))"]
             ]

      # TODO named tuples
      Ch.query!(conn, "CREATE TABLE tuples_t (`a` Tuple(String, Int64)) ENGINE = Memory")

      assert %{num_rows: 2} =
               Ch.query!(conn, "INSERT INTO tuples_t VALUES (('y', 10)), (('x',-10))")

      assert %{num_rows: 2} =
               Ch.query!(
                 conn,
                 "INSERT INTO tuples_t FORMAT RowBinary",
                 _rows = [[{"a", 20}], [{"b", 30}]],
                 types: ["Tuple(String, Int64)"]
               )

      assert Ch.query!(conn, "SELECT a FROM tuples_t ORDER BY a.1 ASC").rows == [
               [{"a", 20}],
               [{"b", 30}],
               [{"x", -10}],
               [{"y", 10}]
             ]
    end

    test "datetime", %{conn: conn} do
      Ch.query!(
        conn,
        "CREATE TABLE dt(`timestamp` DateTime('Asia/Istanbul'), `event_id` UInt8) ENGINE = Memory"
      )

      Ch.query!(conn, "INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2)")

      assert {:ok, %{num_rows: 2, rows: rows}} =
               Ch.query(conn, "SELECT *, toString(timestamp) FROM dt")

      assert rows == [
               [
                 DateTime.new!(~D[2019-01-01], ~T[03:00:00], "Asia/Istanbul"),
                 1,
                 "2019-01-01 03:00:00"
               ],
               [
                 DateTime.new!(~D[2019-01-01], ~T[00:00:00], "Asia/Istanbul"),
                 2,
                 "2019-01-01 00:00:00"
               ]
             ]

      naive_noon = ~N[2022-12-12 12:00:00]

      # datetimes in params are sent in text and ClickHouse translates them to UTC from server timezone by default
      # see https://clickhouse.com/docs/en/sql-reference/data-types/datetime
      #     https://kb.altinity.com/altinity-kb-queries-and-syntax/time-zones/
      assert {:ok,
              %{num_rows: 1, rows: [[naive_datetime, "2022-12-12 12:00:00"]], headers: headers}} =
               Ch.query(conn, "select {$0:DateTime} as d, toString(d)", [naive_noon])

      # to make this test pass for contributors with non UTC timezone we perform the same steps as ClickHouse
      # i.e. we give server timezone to the naive datetime and shift it to UTC before comparing with the result
      {_, timezone} = List.keyfind!(headers, "x-clickhouse-timezone", 0)

      assert naive_datetime ==
               naive_noon
               |> DateTime.from_naive!(timezone)
               |> DateTime.shift_zone!("Etc/UTC")
               |> DateTime.to_naive()

      assert {:ok, %{num_rows: 1, rows: [[~U[2022-12-12 12:00:00Z], "2022-12-12 12:00:00"]]}} =
               Ch.query(conn, "select {$0:DateTime('UTC')} as d, toString(d)", [naive_noon])

      assert {:ok, %{num_rows: 1, rows: rows}} =
               Ch.query(conn, "select {$0:DateTime('Asia/Bangkok')} as d, toString(d)", [
                 naive_noon
               ])

      assert rows == [
               [
                 DateTime.new!(~D[2022-12-12], ~T[12:00:00], "Asia/Bangkok"),
                 "2022-12-12 12:00:00"
               ]
             ]

      # simulate unknown timezone
      prev_tz_db = Calendar.get_time_zone_database()
      Calendar.put_time_zone_database(Calendar.UTCOnlyTimeZoneDatabase)
      on_exit(fn -> Calendar.put_time_zone_database(prev_tz_db) end)

      assert_raise ArgumentError, ~r/:utc_only_time_zone_database/, fn ->
        Ch.query(conn, "select {$0:DateTime('Asia/Tokyo')}", [naive_noon])
      end
    end

    # TODO are negatives correct? what's the range?
    test "date32", %{conn: conn} do
      Ch.query!(conn, "CREATE TABLE new(`timestamp` Date32, `event_id` UInt8) ENGINE = Memory;")
      Ch.query!(conn, "INSERT INTO new VALUES (4102444800, 1), ('2100-01-01', 2)")

      assert {:ok,
              %{
                num_rows: 2,
                rows: [first_event, [~D[2100-01-01], 2, "2100-01-01"]]
              }} = Ch.query(conn, "SELECT *, toString(timestamp) FROM new")

      # TODO use timezone info to be more exact
      assert first_event in [
               [~D[2099-12-31], 1, "2099-12-31"],
               [~D[2100-01-01], 1, "2100-01-01"]
             ]

      assert {:ok, %{num_rows: 1, rows: [[~D[1900-01-01], "1900-01-01"]]}} =
               Ch.query(conn, "select {$0:Date32} as d, toString(d)", [~D[1900-01-01]])

      # max
      assert {:ok, %{num_rows: 1, rows: [[~D[2299-12-31], "2299-12-31"]]}} =
               Ch.query(conn, "select {$0:Date32} as d, toString(d)", [~D[2299-12-31]])

      # min
      assert {:ok, %{num_rows: 1, rows: [[~D[1900-01-01], "1900-01-01"]]}} =
               Ch.query(conn, "select {$0:Date32} as d, toString(d)", [~D[1900-01-01]])

      Ch.query!(
        conn,
        "insert into new(timestamp, event_id) format RowBinary",
        _rows = [[~D[1960-01-01], 3]],
        types: ["Date32", "UInt8"]
      )

      assert %{
               num_rows: 3,
               rows: [
                 first_event,
                 [~D[2100-01-01], 2, "2100-01-01"],
                 [~D[1960-01-01], 3, "1960-01-01"]
               ]
             } = Ch.query!(conn, "SELECT *, toString(timestamp) FROM new ORDER BY event_id")

      # TODO use timezone info to be more exact
      assert first_event in [
               [~D[2099-12-31], 1, "2099-12-31"],
               [~D[2100-01-01], 1, "2100-01-01"]
             ]

      assert %{num_rows: 1, rows: [[3]]} =
               Ch.query!(conn, "SELECT event_id FROM new WHERE timestamp = '1960-01-01'")
    end

    test "datetime64", %{conn: conn} do
      Ch.query!(
        conn,
        "CREATE TABLE datetime64_t(`timestamp` DateTime64(3, 'Asia/Istanbul'), `event_id` UInt8) ENGINE = Memory"
      )

      Ch.query!(
        conn,
        "INSERT INTO datetime64_t Values (1546300800123, 1), (1546300800.123, 2), ('2019-01-01 00:00:00', 3)"
      )

      assert {:ok, %{num_rows: 3, rows: rows}} =
               Ch.query(conn, "SELECT *, toString(timestamp) FROM datetime64_t")

      assert rows == [
               [
                 DateTime.new!(~D[2019-01-01], ~T[03:00:00.123], "Asia/Istanbul"),
                 1,
                 "2019-01-01 03:00:00.123"
               ],
               [
                 DateTime.new!(~D[2019-01-01], ~T[03:00:00.123], "Asia/Istanbul"),
                 2,
                 "2019-01-01 03:00:00.123"
               ],
               [
                 DateTime.new!(~D[2019-01-01], ~T[00:00:00.000], "Asia/Istanbul"),
                 3,
                 "2019-01-01 00:00:00.000"
               ]
             ]

      Ch.query!(
        conn,
        "insert into datetime64_t(event_id, timestamp) format RowBinary",
        _rows = [
          [4, ~N[2021-01-01 12:00:00.123456]],
          [5, ~N[2021-01-01 12:00:00]]
        ],
        types: ["UInt8", "DateTime64(3)"]
      )

      assert {:ok, %{num_rows: 2, rows: rows}} =
               Ch.query(
                 conn,
                 "SELECT *, toString(timestamp)  FROM datetime64_t WHERE timestamp > '2020-01-01'"
               )

      assert rows == [
               [
                 DateTime.new!(~D[2021-01-01], ~T[15:00:00.123], "Asia/Istanbul"),
                 4,
                 "2021-01-01 15:00:00.123"
               ],
               [
                 DateTime.new!(~D[2021-01-01], ~T[15:00:00.000], "Asia/Istanbul"),
                 5,
                 "2021-01-01 15:00:00.000"
               ]
             ]

      for precision <- 0..9 do
        naive_noon = ~N[2022-01-01 12:00:00]

        # datetimes in params are sent in text and ClickHouse translates them to UTC from server timezone by default
        # see https://clickhouse.com/docs/en/sql-reference/data-types/datetime
        #     https://kb.altinity.com/altinity-kb-queries-and-syntax/time-zones/
        assert {:ok, %{num_rows: 1, rows: [[naive_datetime]], headers: headers}} =
                 Ch.query(conn, "select {$0:DateTime64(#{precision})}", [naive_noon])

        # to make this test pass for contributors with non UTC timezone we perform the same steps as ClickHouse
        # i.e. we give server timezone to the naive datetime and shift it to UTC before comparing with the result
        {_, timezone} = List.keyfind!(headers, "x-clickhouse-timezone", 0)

        expected =
          naive_noon
          |> DateTime.from_naive!(timezone)
          |> DateTime.shift_zone!("Etc/UTC")
          |> DateTime.to_naive()

        assert NaiveDateTime.compare(naive_datetime, expected) == :eq
      end

      assert {:ok,
              %{num_rows: 1, rows: [[~U[2022-01-01 12:00:00.123Z], "2022-01-01 12:00:00.123"]]}} =
               Ch.query(conn, "select {dt:DateTime64(3,'UTC')} as d, toString(d)", %{
                 "dt" => ~N[2022-01-01 12:00:00.123]
               })

      assert {:ok,
              %{num_rows: 1, rows: [[~U[1900-01-01 12:00:00.123Z], "1900-01-01 12:00:00.123"]]}} =
               Ch.query(conn, "select {dt:DateTime64(3,'UTC')} as d, toString(d)", %{
                 "dt" => ~N[1900-01-01 12:00:00.123]
               })

      assert {:ok, %{num_rows: 1, rows: [row]}} =
               Ch.query(conn, "select {dt:DateTime64(3,'Asia/Bangkok')} as d, toString(d)", %{
                 "dt" => ~N[2022-01-01 12:00:00.123]
               })

      assert row == [
               DateTime.new!(~D[2022-01-01], ~T[12:00:00.123], "Asia/Bangkok"),
               "2022-01-01 12:00:00.123"
             ]
    end

    test "nullable", %{conn: conn} do
      Ch.query!(
        conn,
        "CREATE TABLE nullable (`n` Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple()"
      )

      Ch.query!(conn, "INSERT INTO nullable VALUES (1) (NULL) (2) (NULL)")

      assert {:ok, %{num_rows: 4, rows: [[0], [1], [0], [1]]}} =
               Ch.query(conn, "SELECT n.null FROM nullable")

      assert {:ok, %{num_rows: 4, rows: [[1], [nil], [2], [nil]]}} =
               Ch.query(conn, "SELECT n FROM nullable")

      # weird thing about nullables is that, similar to bool, in binary format, any byte larger than 0 is `null`
      assert {:ok, %{num_rows: 5}} =
               Ch.query(
                 conn,
                 "insert into nullable format RowBinary",
                 <<1, 2, 3, 4, 5>>,
                 encode: false
               )

      assert %{num_rows: 1, rows: [[count]]} =
               Ch.query!(conn, "select count(*) from nullable where n is null")

      assert count == 2 + 5
    end

    test "nullable + default", %{conn: conn} do
      Ch.query!(conn, """
      CREATE TABLE ch_nulls (
        a UInt8,
        b UInt8 NULL,
        c UInt8 DEFAULT 10,
        d Nullable(UInt8) DEFAULT 10,
      ) ENGINE Memory
      """)

      Ch.query!(
        conn,
        "INSERT INTO ch_nulls(a, b, c, d) FORMAT RowBinary",
        [[nil, nil, nil, nil]],
        types: ["UInt8", "Nullable(UInt8)", "UInt8", "Nullable(UInt8)"]
      )

      # default is ignored...
      assert Ch.query!(conn, "SELECT * FROM ch_nulls").rows == [[0, nil, 0, nil]]
    end

    # based on https://github.com/ClickHouse/clickhouse-java/pull/1345/files
    test "nullable + input() + default", %{conn: conn} do
      Ch.query!(conn, """
      CREATE TABLE test_insert_default_value(
        n Int32,
        s String DEFAULT 'secret'
      ) ENGINE Memory
      """)

      Ch.query!(
        conn,
        """
        INSERT INTO test_insert_default_value
          SELECT id, name
          FROM input('id UInt32, name Nullable(String)')
          FORMAT RowBinary\
        """,
        [[1, nil], [-1, nil]],
        types: ["UInt32", "Nullable(String)"]
      )

      assert Ch.query!(conn, "SELECT * FROM test_insert_default_value ORDER BY n").rows ==
               [
                 [-1, "secret"],
                 [1, "secret"]
               ]
    end

    test "can decode casted Point", %{conn: conn} do
      assert Ch.query!(conn, "select cast((0, 1) as Point)").rows == [
               _row = [_point = {0.0, 1.0}]
             ]
    end

    test "can encode and then decode Point in query params", %{conn: conn} do
      assert Ch.query!(conn, "select {$0:Point}", [{10, 10}]).rows == [
               _row = [_point = {10.0, 10.0}]
             ]
    end

    test "can insert and select Point", %{conn: conn} do
      Ch.query!(conn, "CREATE TABLE geo_point (p Point) ENGINE = Memory()")
      Ch.query!(conn, "INSERT INTO geo_point VALUES((10, 10))")
      Ch.query!(conn, "INSERT INTO geo_point FORMAT RowBinary", [[{20, 20}]], types: ["Point"])

      assert Ch.query!(conn, "SELECT p, toTypeName(p) FROM geo_point ORDER BY p ASC").rows == [
               [{10.0, 10.0}, "Point"],
               [{20.0, 20.0}, "Point"]
             ]

      # to make our RowBinary is not garbage in garbage out we also test a text format response
      assert conn
             |> Ch.query!(
               "SELECT p, toTypeName(p) FROM geo_point ORDER BY p ASC FORMAT JSONCompact"
             )
             |> Map.fetch!(:rows)
             |> Jason.decode!()
             |> Map.fetch!("data") == [
               [[10, 10], "Point"],
               [[20, 20], "Point"]
             ]
    end

    test "can decode casted Ring", %{conn: conn} do
      ring = [{0.0, 1.0}, {10.0, 3.0}]
      assert Ch.query!(conn, "select cast([(0,1),(10,3)] as Ring)").rows == [_row = [ring]]
    end

    test "can encode and then decode Ring in query params", %{conn: conn} do
      ring = [{0.0, 1.0}, {10.0, 3.0}]
      assert Ch.query!(conn, "select {$0:Ring}", [ring]).rows == [_row = [ring]]
    end

    test "can insert and select Ring", %{conn: conn} do
      Ch.query!(conn, "CREATE TABLE geo_ring (r Ring) ENGINE = Memory()")
      Ch.query!(conn, "INSERT INTO geo_ring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)])")

      ring = [{20, 20}, {0, 0}, {0, 20}]
      Ch.query!(conn, "INSERT INTO geo_ring FORMAT RowBinary", [[ring]], types: ["Ring"])

      assert Ch.query!(conn, "SELECT r, toTypeName(r) FROM geo_ring ORDER BY r ASC").rows == [
               [[{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}], "Ring"],
               [[{20.0, 20.0}, {0.0, 0.0}, {0.0, 20.0}], "Ring"]
             ]

      # to make our RowBinary is not garbage in garbage out we also test a text format response
      assert Ch.query!(
               conn,
               "SELECT r, toTypeName(r) FROM geo_ring ORDER BY r ASC FORMAT JSONCompact"
             ).rows
             |> Jason.decode!()
             |> Map.fetch!("data") == [
               [[[0, 0], [10, 0], [10, 10], [0, 10]], "Ring"],
               [[[20, 20], [0, 0], [0, 20]], "Ring"]
             ]
    end

    test "can decode casted Polygon", %{conn: conn} do
      polygon = [[{0.0, 1.0}, {10.0, 3.0}], [], [{2, 2}]]

      assert Ch.query!(conn, "select cast([[(0,1),(10,3)],[],[(2,2)]] as Polygon)").rows == [
               _row = [polygon]
             ]
    end

    test "can encode and then decode Polygon in query params", %{conn: conn} do
      polygon = [[{0.0, 1.0}, {10.0, 3.0}], [], [{2, 2}]]
      assert Ch.query!(conn, "select {$0:Polygon}", [polygon]).rows == [_row = [polygon]]
    end

    test "can insert and select Polygon", %{conn: conn} do
      Ch.query!(conn, "CREATE TABLE geo_polygon (pg Polygon) ENGINE = Memory()")

      Ch.query!(
        conn,
        "INSERT INTO geo_polygon VALUES([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]])"
      )

      polygon = [[{0, 1.0}, {10, 3.2}], [], [{2, 2}]]
      Ch.query!(conn, "INSERT INTO geo_polygon FORMAT RowBinary", [[polygon]], types: ["Polygon"])

      assert Ch.query!(conn, "SELECT pg, toTypeName(pg) FROM geo_polygon ORDER BY pg ASC").rows ==
               [
                 [[[{0.0, 1.0}, {10.0, 3.2}], [], [{2.0, 2.0}]], "Polygon"],
                 [
                   [
                     [{20.0, 20.0}, {50.0, 20.0}, {50.0, 50.0}, {20.0, 50.0}],
                     [{30.0, 30.0}, {50.0, 50.0}, {50.0, 30.0}]
                   ],
                   "Polygon"
                 ]
               ]

      # to make our RowBinary is not garbage in garbage out we also test a text format response
      assert Ch.query!(
               conn,
               "SELECT pg, toTypeName(pg) FROM geo_polygon ORDER BY pg ASC FORMAT JSONCompact"
             ).rows
             |> Jason.decode!()
             |> Map.fetch!("data") == [
               [[[[0, 1], [10, 3.2]], [], [[2, 2]]], "Polygon"],
               [
                 [[[20, 20], [50, 20], [50, 50], [20, 50]], [[30, 30], [50, 50], [50, 30]]],
                 "Polygon"
               ]
             ]
    end

    test "can decode casted MultiPolygon", %{conn: conn} do
      multipolygon = [[[{0.0, 1.0}, {10.0, 3.0}], [], [{2, 2}]], [], [[{3, 3}]]]

      assert Ch.query!(
               conn,
               "select cast([[[(0,1),(10,3)],[],[(2,2)]],[],[[(3, 3)]]] as MultiPolygon)"
             ).rows == [
               _row = [multipolygon]
             ]
    end

    test "can encode and then decode MultiPolygon in query params", %{conn: conn} do
      multipolygon = [[[{0.0, 1.0}, {10.0, 3.0}], [], [{2, 2}]], [], [[{3, 3}]]]

      assert Ch.query!(conn, "select {$0:MultiPolygon}", [multipolygon]).rows == [
               _row = [multipolygon]
             ]
    end

    test "can insert and select MultiPolygon", %{conn: conn} do
      Ch.query!(conn, "CREATE TABLE geo_multipolygon (mpg MultiPolygon) ENGINE = Memory()")

      Ch.query!(
        conn,
        "INSERT INTO geo_multipolygon VALUES([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]])"
      )

      multipolygon = [[[{0.0, 1.0}, {10.0, 3.0}], [], [{2, 2}]], [], [[{3, 3}]]]

      Ch.query!(conn, "INSERT INTO geo_multipolygon FORMAT RowBinary", [[multipolygon]],
        types: ["MultiPolygon"]
      )

      assert Ch.query!(conn, "SELECT mpg, toTypeName(mpg) FROM geo_multipolygon ORDER BY mpg ASC").rows ==
               [
                 _row = [
                   _multipolygon = [
                     _polygon = [
                       _ring = [{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}]
                     ],
                     [
                       [{20.0, 20.0}, {50.0, 20.0}, {50.0, 50.0}, {20.0, 50.0}],
                       [{30.0, 30.0}, {50.0, 50.0}, {50.0, 30.0}]
                     ]
                   ],
                   "MultiPolygon"
                 ],
                 [
                   [
                     [
                       [{0.0, 1.0}, {10.0, 3.0}],
                       [],
                       [{2.0, 2.0}]
                     ],
                     [],
                     [
                       [{3.0, 3.0}]
                     ]
                   ],
                   "MultiPolygon"
                 ]
               ]

      # to make our RowBinary is not garbage in garbage out we also test a text format response
      assert Ch.query!(
               conn,
               "SELECT mpg, toTypeName(mpg) FROM geo_multipolygon ORDER BY mpg ASC FORMAT JSONCompact"
             ).rows
             |> Jason.decode!()
             |> Map.fetch!("data") == [
               [
                 [
                   [[[0, 0], [10, 0], [10, 10], [0, 10]]],
                   [[[20, 20], [50, 20], [50, 50], [20, 50]], [[30, 30], [50, 50], [50, 30]]]
                 ],
                 "MultiPolygon"
               ],
               [[[[[0, 1], [10, 3]], [], [[2, 2]]], [], [[[3, 3]]]], "MultiPolygon"]
             ]
    end
  end

  describe "options" do
    # this test is flaky, sometimes it raises due to ownership timeout
    @tag capture_log: true, skip: true
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
               "Code: 516. DB::Exception: no-exists: Authentication failed: password is incorrect, or there is no user with such name. (AUTHENTICATION_FAILED)"
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

  describe "transactions" do
    test "commit", %{conn: conn} do
      DBConnection.transaction(conn, fn conn ->
        Ch.query!(conn, "select 1 + 1")
      end)
    end

    test "rollback", %{conn: conn} do
      DBConnection.transaction(conn, fn conn ->
        DBConnection.rollback(conn, :some_reason)
      end)
    end

    test "status", %{conn: conn} do
      assert DBConnection.status(conn) == :idle
    end
  end

  describe "stream" do
    test "sends mint http packets", %{conn: conn} do
      stmt = "select number from system.numbers limit 1000"

      drop_ref = fn packets ->
        Enum.map(packets, fn
          {tag, _ref, data} -> {tag, data}
          {tag, _ref} -> tag
        end)
      end

      packets =
        Ch.run(conn, fn conn ->
          conn
          |> Ch.stream(stmt)
          |> Enum.flat_map(drop_ref)
        end)

      assert [{:status, 200}, {:headers, headers} | _rest] = packets

      assert List.keyfind!(headers, "transfer-encoding", 0) == {"transfer-encoding", "chunked"}

      assert data_packets =
               packets
               |> Enum.filter(&match?({:data, _data}, &1))
               |> Enum.map(fn {:data, data} -> data end)

      assert length(data_packets) >= 2
      assert RowBinary.decode_rows(Enum.join(data_packets)) == Enum.map(0..999, &[&1])

      assert List.last(packets) == :done
    end

    test "decodes RowBinary", %{conn: conn} do
      stmt = "select number from system.numbers limit 1000"

      rows =
        Ch.run(conn, fn conn ->
          conn
          |> Ch.stream(stmt, _params = [], types: [:u64])
          |> Enum.into([])
        end)

      assert List.flatten(rows) == Enum.into(0..999, [])
    end

    test "disconnects on early halt", %{conn: conn} do
      logs =
        ExUnit.CaptureLog.capture_log(fn ->
          Ch.run(conn, fn conn ->
            conn |> Ch.stream("select number from system.numbers") |> Enum.take(1)
          end)

          assert Ch.query!(conn, "select 1 + 1").rows == [[2]]
        end)

      assert logs =~
               "disconnected: ** (Ch.Error) cannot stop stream before receiving full response"
    end
  end

  describe "prepare" do
    test "no-op", %{conn: conn} do
      query = Ch.Query.build("select 1 + 1")

      assert {:error, %Ch.Error{message: "prepared statements are not supported"}} =
               DBConnection.prepare(conn, query)
    end
  end

  describe "start_link/1" do
    test "can pass options to start_link/1" do
      db = "#{Ch.Test.database()}_#{System.unique_integer([:positive])}"
      {:ok, _} = Ch.Test.sql_exec("CREATE DATABASE {db:Identifier}", %{"db" => db})

      on_exit(fn ->
        {:ok, _} = Ch.Test.sql_exec("DROP DATABASE {db:Identifier}", %{"db" => db})
      end)

      {:ok, conn} = Ch.start_link(database: db)
      Ch.query!(conn, "create table example(a UInt8) engine=Memory")
      assert {:ok, %{rows: [["example"]]}} = Ch.query(conn, "show tables")
    end

    test "can start without options" do
      {:ok, conn} = Ch.start_link()
      assert {:ok, %{num_rows: 1, rows: [[2]]}} = Ch.query(conn, "select 1 + 1")
    end
  end

  describe "RowBinaryWithNamesAndTypes" do
    setup %{conn: conn} do
      Ch.query!(conn, """
      create table if not exists row_binary_names_and_types_t (
        country_code FixedString(2),
        rare_string LowCardinality(String),
        maybe_int32 Nullable(Int32)
      ) engine Memory
      """)

      on_exit(fn -> Ch.Test.sql_exec("truncate row_binary_names_and_types_t") end)
    end

    test "error on type mismatch", %{conn: conn} do
      stmt = "insert into row_binary_names_and_types_t format RowBinaryWithNamesAndTypes"
      rows = [["AB", "rare", -42]]
      names = ["country_code", "rare_string", "maybe_int32"]

      opts = [
        names: names,
        types: [Ch.Types.fixed_string(2), Ch.Types.string(), Ch.Types.nullable(Ch.Types.u32())]
      ]

      assert {:error, %Ch.Error{code: 117, message: message}} = Ch.query(conn, stmt, rows, opts)
      assert message =~ "Type of 'rare_string' must be LowCardinality(String), not String"

      opts = [
        names: names,
        types: [
          Ch.Types.fixed_string(2),
          Ch.Types.low_cardinality(Ch.Types.string()),
          Ch.Types.nullable(Ch.Types.u32())
        ]
      ]

      assert {:error, %Ch.Error{code: 117, message: message}} = Ch.query(conn, stmt, rows, opts)
      assert message =~ "Type of 'maybe_int32' must be Nullable(Int32), not Nullable(UInt32)"
    end

    test "ok on valid types", %{conn: conn} do
      stmt = "insert into row_binary_names_and_types_t format RowBinaryWithNamesAndTypes"
      rows = [["AB", "rare", -42]]
      names = ["country_code", "rare_string", "maybe_int32"]

      opts = [
        names: names,
        types: [
          Ch.Types.fixed_string(2),
          Ch.Types.low_cardinality(Ch.Types.string()),
          Ch.Types.nullable(Ch.Types.i32())
        ]
      ]

      assert {:ok, %{num_rows: 1}} = Ch.query(conn, stmt, rows, opts)
    end
  end
end
