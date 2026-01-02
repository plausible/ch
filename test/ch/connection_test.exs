defmodule Ch.ConnectionTest do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]
  alias Ch.RowBinary

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  defp parameterize_query_options(ctx, custom_options) do
    if extra_options = ctx[:query_options] do
      Keyword.merge(extra_options, custom_options)
    else
      custom_options
    end
  end

  defp query(%{conn: conn} = ctx, sql, params \\ [], custom_options \\ []) do
    options = parameterize_query_options(ctx, custom_options)
    Ch.query(conn, sql, params, options)
  end

  defp query!(%{conn: conn} = ctx, sql, params \\ [], custom_options \\ []) do
    options = parameterize_query_options(ctx, custom_options)
    Ch.query!(conn, sql, params, options)
  end

  test "select without params", ctx do
    assert {:ok, %{num_rows: 1, rows: [[1]]}} = query(ctx, "select 1")
  end

  test "select with types", ctx do
    assert {:ok, %{num_rows: 1, rows: [[1]]}} =
             query(ctx, "select 1", [], types: ["UInt8"])
  end

  test "select with params", ctx do
    assert {:ok, %{num_rows: 1, rows: [[1]]}} = query(ctx, "select {a:UInt8}", %{"a" => 1})

    assert {:ok, %{num_rows: 1, rows: [[true]]}} =
             query(ctx, "select {b:Bool}", %{"b" => true})

    assert {:ok, %{num_rows: 1, rows: [[false]]}} =
             query(ctx, "select {b:Bool}", %{"b" => false})

    assert {:ok, %{num_rows: 1, rows: [[nil]]}} =
             query(ctx, "select {n:Nullable(Nothing)}", %{"n" => nil})

    assert {:ok, %{num_rows: 1, rows: [[1.0]]}} =
             query(ctx, "select {a:Float32}", %{"a" => 1.0})

    assert {:ok, %{num_rows: 1, rows: [["a&b=c"]]}} =
             query(ctx, "select {a:String}", %{"a" => "a&b=c"})

    assert {:ok, %{num_rows: 1, rows: [["a\n"]]}} =
             query(ctx, "select {a:String}", %{"a" => "a\n"})

    assert {:ok, %{num_rows: 1, rows: [["a\t"]]}} =
             query(ctx, "select {a:String}", %{"a" => "a\t"})

    assert {:ok, %{num_rows: 1, rows: [[["a\tb"]]]}} =
             query(ctx, "select {a:Array(String)}", %{"a" => ["a\tb"]})

    assert {:ok, %{num_rows: 1, rows: [[[true, false]]]}} =
             query(ctx, "select {a:Array(Bool)}", %{"a" => [true, false]})

    assert {:ok, %{num_rows: 1, rows: [[["a", nil, "b"]]]}} =
             query(ctx, "select {a:Array(Nullable(String))}", %{"a" => ["a", nil, "b"]})

    assert {:ok, %{num_rows: 1, rows: [row]}} =
             query(ctx, "select {a:Decimal(9,4)}", %{"a" => Decimal.new("2000.333")})

    assert row == [Decimal.new("2000.3330")]

    assert {:ok, %{num_rows: 1, rows: [[~D[2022-01-01]]]}} =
             query(ctx, "select {a:Date}", %{"a" => ~D[2022-01-01]})

    assert {:ok, %{num_rows: 1, rows: [[~D[2022-01-01]]]}} =
             query(ctx, "select {a:Date32}", %{"a" => ~D[2022-01-01]})

    naive_noon = ~N[2022-01-01 12:00:00]

    # datetimes in params are sent in text and ClickHouse translates them to UTC from server timezone by default
    # see https://clickhouse.com/docs/en/sql-reference/data-types/datetime
    #     https://kb.altinity.com/altinity-kb-queries-and-syntax/time-zones/
    assert {:ok, %{num_rows: 1, rows: [[naive_datetime]], headers: headers}} =
             query(ctx, "select {naive:DateTime}", %{"naive" => naive_noon})

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
             query(ctx, "select {$0:DateTime('Asia/Bangkok')}", [naive_noon])

    assert bkk_datetime == DateTime.from_naive!(naive_noon, "Asia/Bangkok")

    assert {:ok, %{num_rows: 1, rows: [[~U[2022-01-01 12:00:00Z]]]}} =
             query(ctx, "select {$0:DateTime('UTC')}", [naive_noon])

    naive_noon_ms = ~N[2022-01-01 12:00:00.123]

    assert {:ok, %{num_rows: 1, rows: [[naive_datetime]]}} =
             query(ctx, "select {$0:DateTime64(3)}", [naive_noon_ms])

    assert NaiveDateTime.compare(
             naive_datetime,
             naive_noon_ms
             |> DateTime.from_naive!(timezone)
             |> DateTime.shift_zone!("Etc/UTC")
             |> DateTime.to_naive()
           ) == :eq

    assert {:ok, %{num_rows: 1, rows: [[["a", "b'", "\\'c"]]]}} =
             query(ctx, "select {a:Array(String)}", %{"a" => ["a", "b'", "\\'c"]})

    assert {:ok, %{num_rows: 1, rows: [[["a\n", "b\tc"]]]}} =
             query(ctx, "select {a:Array(String)}", %{"a" => ["a\n", "b\tc"]})

    assert {:ok, %{num_rows: 1, rows: [[[1, 2, 3]]]}} =
             query(ctx, "select {a:Array(UInt8)}", %{"a" => [1, 2, 3]})

    assert {:ok, %{num_rows: 1, rows: [[[[1], [2, 3], []]]]}} =
             query(ctx, "select {a:Array(Array(UInt8))}", %{"a" => [[1], [2, 3], []]})

    uuid = "9B29BD20-924C-4DE5-BDB3-8C2AA1FCE1FC"
    uuid_bin = uuid |> String.replace("-", "") |> Base.decode16!()

    assert {:ok, %{num_rows: 1, rows: [[^uuid_bin]]}} =
             query(ctx, "select {a:UUID}", %{"a" => uuid})

    # TODO
    # assert {:ok, %{num_rows: 1, rows: [[^uuid_bin]]}} =
    #          query(ctx, "select {a:UUID}", %{"a" => uuid_bin})

    # pseudo-positional bind
    assert {:ok, %{num_rows: 1, rows: [[1]]}} = query(ctx, "select {$0:UInt8}", [1])
  end

  test "utc datetime query param encoding", ctx do
    utc = ~U[2021-01-01 12:00:00Z]
    msk = DateTime.new!(~D[2021-01-01], ~T[15:00:00], "Europe/Moscow")
    naive = utc |> DateTime.shift_zone!(Ch.Test.clickhouse_tz(ctx.conn)) |> DateTime.to_naive()

    assert query!(ctx, "select {$0:DateTime} as d, toString(d)", [utc]).rows ==
             [[~N[2021-01-01 12:00:00], to_string(naive)]]

    assert query!(ctx, "select {$0:DateTime('UTC')} as d, toString(d)", [utc]).rows ==
             [[utc, "2021-01-01 12:00:00"]]

    assert query!(ctx, "select {$0:DateTime('Europe/Moscow')} as d, toString(d)", [utc]).rows ==
             [[msk, "2021-01-01 15:00:00"]]
  end

  test "non-utc datetime query param encoding", ctx do
    jp = DateTime.shift_zone!(~U[2021-01-01 12:34:56Z], "Asia/Tokyo")
    assert inspect(jp) == "#DateTime<2021-01-01 21:34:56+09:00 JST Asia/Tokyo>"

    assert [[utc, jp]] =
             query!(ctx, "select {$0:DateTime('UTC')}, {$0:DateTime('Asia/Tokyo')}", [jp]).rows

    assert inspect(utc) == "~U[2021-01-01 12:34:56Z]"
    assert inspect(jp) == "#DateTime<2021-01-01 21:34:56+09:00 JST Asia/Tokyo>"
  end

  test "non-utc datetime rowbinary encoding", ctx do
    query!(ctx, "create table ch_non_utc_datetimes(name String, datetime DateTime) engine Memory")
    on_exit(fn -> Ch.Test.query("drop table ch_non_utc_datetimes") end)

    utc = ~U[2024-12-21 05:35:19.886393Z]

    taipei = DateTime.shift_zone!(utc, "Asia/Taipei")
    tokyo = DateTime.shift_zone!(utc, "Asia/Tokyo")
    vienna = DateTime.shift_zone!(utc, "Europe/Vienna")

    rows = [["taipei", taipei], ["tokyo", tokyo], ["vienna", vienna]]

    query!(ctx, "insert into ch_non_utc_datetimes(name, datetime) format RowBinary", rows,
      types: ["String", "DateTime"]
    )

    result =
      query!(ctx, "select name, cast(datetime as DateTime('UTC')) from ch_non_utc_datetimes")
      |> Map.fetch!(:rows)
      |> Map.new(fn [name, datetime] -> {name, datetime} end)

    assert result["taipei"] == ~U[2024-12-21 05:35:19Z]
    assert result["tokyo"] == ~U[2024-12-21 05:35:19Z]
    assert result["vienna"] == ~U[2024-12-21 05:35:19Z]
  end

  test "utc datetime64 query param encoding", ctx do
    utc = ~U[2021-01-01 12:00:00.123456Z]
    msk = DateTime.new!(~D[2021-01-01], ~T[15:00:00.123456], "Europe/Moscow")
    naive = utc |> DateTime.shift_zone!(Ch.Test.clickhouse_tz(ctx.conn)) |> DateTime.to_naive()

    assert query!(ctx, "select {$0:DateTime64(6)} as d, toString(d)", [utc]).rows ==
             [[~N[2021-01-01 12:00:00.123456], to_string(naive)]]

    assert query!(ctx, "select {$0:DateTime64(6, 'UTC')} as d, toString(d)", [utc]).rows ==
             [[utc, "2021-01-01 12:00:00.123456"]]

    assert query!(ctx, "select {$0:DateTime64(6,'Europe/Moscow')} as d, toString(d)", [utc]).rows ==
             [[msk, "2021-01-01 15:00:00.123456"]]
  end

  test "utc datetime64 zero microseconds query param encoding", ctx do
    # this test case guards against a previous bug where DateTimes with a microsecond value of 0 and precision > 0 would
    # get encoded as a val like "1.6095024e9" which ClickHouse would be unable to parse to a DateTime.
    utc = ~U[2021-01-01 12:00:00.000000Z]
    naive = utc |> DateTime.shift_zone!(Ch.Test.clickhouse_tz(ctx.conn)) |> DateTime.to_naive()

    assert query!(ctx, "select {$0:DateTime64(6)} as d, toString(d)", [utc]).rows ==
             [[~N[2021-01-01 12:00:00.000000], to_string(naive)]]
  end

  test "utc datetime64 microseconds with more precision than digits", ctx do
    # this test case guards against a previous bug where DateTimes with a microsecond value of with N digits
    # and a precision > N would be encoded with a space like `234235234. 234123`
    utc = ~U[2024-05-26 20:00:46.099856Z]
    naive = utc |> DateTime.shift_zone!(Ch.Test.clickhouse_tz(ctx.conn)) |> DateTime.to_naive()

    assert query!(ctx, "select {$0:DateTime64(6)} as d, toString(d)", [utc]).rows ==
             [[~N[2024-05-26 20:00:46.099856Z], to_string(naive)]]
  end

  test "select with options", ctx do
    assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "1"]]}} =
             query(ctx, "show settings like 'async_insert'", [], settings: [async_insert: 1])

    assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "0"]]}} =
             query(ctx, "show settings like 'async_insert'", [], settings: [async_insert: 0])
  end

  test "create", ctx do
    assert {:ok, %{command: :create, num_rows: nil, rows: [], data: []}} =
             query(ctx, "create table create_example(a UInt8) engine = Memory")

    on_exit(fn -> Ch.Test.query("drop table create_example") end)
  end

  test "create with options", ctx do
    assert {:error, %Ch.Error{code: 164, message: message}} =
             query(ctx, "create table create_example(a UInt8) engine = Memory", [],
               settings: [readonly: 1]
             )

    assert message =~ ~r/Cannot execute query in readonly mode/
  end

  describe "insert" do
    setup ctx do
      table = "insert_t_#{System.unique_integer([:positive])}"

      query!(
        ctx,
        "create table #{table}(a UInt8 default 1, b String) engine = Memory"
      )

      {:ok, table: table}
    end

    test "values", %{table: table} = ctx do
      assert {:ok, %{num_rows: 3}} =
               query(
                 ctx,
                 "insert into {table:Identifier} values (1, 'a'),(2,'b'),   (null,       null)",
                 %{"table" => table}
               )

      assert {:ok, %{rows: rows}} =
               query(ctx, "select * from {table:Identifier}", %{"table" => table})

      assert rows == [[1, "a"], [2, "b"], [1, ""]]

      assert {:ok, %{num_rows: 2}} =
               query(
                 ctx,
                 "insert into {$0:Identifier}(a, b) values ({$1:UInt8},{$2:String}),({$3:UInt8},{$4:String})",
                 [table, 4, "d", 5, "e"]
               )

      assert {:ok, %{rows: rows}} =
               query(ctx, "select * from {table:Identifier} where a >= 4", %{"table" => table})

      assert rows == [[4, "d"], [5, "e"]]
    end

    test "when readonly", %{table: table} = ctx do
      settings = [readonly: 1]

      assert {:error, %Ch.Error{code: 164, message: message}} =
               query(
                 ctx,
                 "insert into {table:Identifier} values (1, 'a'), (2, 'b')",
                 %{"table" => table},
                 settings: settings
               )

      assert message =~ "Cannot execute query in readonly mode."
    end

    test "automatic RowBinary", %{table: table} = ctx do
      stmt = "insert into #{table}(a, b) format RowBinary"
      types = ["UInt8", "String"]
      rows = [[1, "a"], [2, "b"]]
      assert %{num_rows: 2} = query!(ctx, stmt, rows, types: types)

      assert %{rows: rows} =
               query!(ctx, "select * from {table:Identifier}", %{"table" => table})

      assert rows == [[1, "a"], [2, "b"]]
    end

    test "manual RowBinary", %{table: table} = ctx do
      stmt = "insert into #{table}(a, b) format RowBinary"

      types = ["UInt8", "String"]
      rows = [[1, "a"], [2, "b"]]
      data = RowBinary.encode_rows(rows, types)

      assert %{num_rows: 2} = query!(ctx, stmt, data, encode: false)

      assert %{rows: rows} =
               query!(ctx, "select * from {table:Identifier}", %{"table" => table})

      assert rows == [[1, "a"], [2, "b"]]
    end

    test "chunked", %{table: table} = ctx do
      types = ["UInt8", "String"]
      rows = [[1, "a"], [2, "b"], [3, "c"]]

      stream =
        rows
        |> Stream.chunk_every(2)
        |> Stream.map(fn chunk -> RowBinary.encode_rows(chunk, types) end)

      assert {:ok, %{num_rows: 3}} =
               query(
                 ctx,
                 "insert into #{table}(a, b) format RowBinary",
                 stream,
                 encode: false
               )

      assert {:ok, %{rows: rows}} =
               query(ctx, "select * from {table:Identifier}", %{"table" => table})

      assert rows == [[1, "a"], [2, "b"], [3, "c"]]
    end

    test "select", %{table: table} = ctx do
      assert {:ok, %{num_rows: 3}} =
               query(
                 ctx,
                 "insert into {table:Identifier} values (1, 'a'), (2, 'b'), (null, null)",
                 %{"table" => table}
               )

      assert {:ok, %{num_rows: 3}} =
               query(
                 ctx,
                 "insert into {table:Identifier}(a, b) select a, b from {table:Identifier}",
                 %{"table" => table}
               )

      assert {:ok, %{rows: rows}} =
               query(ctx, "select * from {table:Identifier}", %{"table" => table})

      assert rows == [[1, "a"], [2, "b"], [1, ""], [1, "a"], [2, "b"], [1, ""]]

      assert {:ok, %{num_rows: 2}} =
               query(
                 ctx,
                 "insert into {$0:Identifier}(a, b) select a, b from {$0:Identifier} where a > {$1:UInt8}",
                 [table, 1]
               )

      assert {:ok, %{rows: new_rows}} =
               query(ctx, "select * from {table:Identifier}", %{"table" => table})

      assert new_rows -- rows == [[2, "b"], [2, "b"]]
    end
  end

  test "delete", ctx do
    query!(ctx, "create table delete_t(a UInt8, b String) engine = MergeTree order by tuple()")
    on_exit(fn -> Ch.Test.query("drop table delete_t") end)

    assert {:ok, %{num_rows: 2}} = query(ctx, "insert into delete_t values (1,'a'), (2,'b')")

    settings = [allow_experimental_lightweight_delete: 1]

    assert {:ok, %{rows: [], data: [], command: :delete}} =
             query(ctx, "delete from delete_t where 1", [], settings: settings)
  end

  test "query!", ctx do
    assert %{num_rows: 1, rows: [[1]]} = query!(ctx, "select 1")
  end

  describe "types" do
    test "multiple types", ctx do
      assert {:ok, %{num_rows: 1, rows: [[1, "a"]]}} =
               query(ctx, "select {a:Int8}, {b:String}", %{"a" => 1, "b" => "a"})
    end

    test "ints", ctx do
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = query(ctx, "select {a:Int8}", %{"a" => 1})

      assert {:ok, %{num_rows: 1, rows: [[-1000]]}} =
               query(ctx, "select {a:Int16}", %{"a" => -1000})

      assert {:ok, %{num_rows: 1, rows: [[100_000]]}} =
               query(ctx, "select {a:Int32}", %{"a" => 100_000})

      assert {:ok, %{num_rows: 1, rows: [[1]]}} = query(ctx, "select {a:Int64}", %{"a" => 1})
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = query(ctx, "select {a:Int128}", %{"a" => 1})
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = query(ctx, "select {a:Int256}", %{"a" => 1})
    end

    test "uints", ctx do
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = query(ctx, "select {a:UInt8}", %{"a" => 1})
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = query(ctx, "select {a:UInt16}", %{"a" => 1})
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = query(ctx, "select {a:UInt32}", %{"a" => 1})
      assert {:ok, %{num_rows: 1, rows: [[1]]}} = query(ctx, "select {a:UInt64}", %{"a" => 1})

      assert {:ok, %{num_rows: 1, rows: [[1]]}} =
               query(ctx, "select {a:UInt128}", %{"a" => 1})

      assert {:ok, %{num_rows: 1, rows: [[1]]}} =
               query(ctx, "select {a:UInt256}", %{"a" => 1})
    end

    test "fixed string", ctx do
      assert {:ok, %{num_rows: 1, rows: [[<<0, 0>>]]}} =
               query(ctx, "select {a:FixedString(2)}", %{"a" => ""})

      assert {:ok, %{num_rows: 1, rows: [["a" <> <<0>>]]}} =
               query(ctx, "select {a:FixedString(2)}", %{"a" => "a"})

      assert {:ok, %{num_rows: 1, rows: [["aa"]]}} =
               query(ctx, "select {a:FixedString(2)}", %{"a" => "aa"})

      assert {:ok, %{num_rows: 1, rows: [["aaaaa"]]}} =
               query(ctx, "select {a:FixedString(5)}", %{"a" => "aaaaa"})

      query!(ctx, "create table fixed_string_t(a FixedString(3)) engine = Memory")
      on_exit(fn -> Ch.Test.query("drop table fixed_string_t") end)

      assert {:ok, %{num_rows: 4}} =
               query(
                 ctx,
                 "insert into fixed_string_t(a) format RowBinary",
                 [
                   [""],
                   ["a"],
                   ["aa"],
                   ["aaa"]
                 ],
                 types: ["FixedString(3)"]
               )

      assert query!(ctx, "select * from fixed_string_t").rows == [
               [<<0, 0, 0>>],
               ["a" <> <<0, 0>>],
               ["aa" <> <<0>>],
               ["aaa"]
             ]
    end

    test "decimal", ctx do
      assert {:ok, %{num_rows: 1, rows: [row]}} =
               query(ctx, "SELECT toDecimal32(2, 4) AS x, x / 3, toTypeName(x)")

      assert row == [Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(9, 4)"]

      assert {:ok, %{num_rows: 1, rows: [row]}} =
               query(ctx, "SELECT toDecimal64(2, 4) AS x, x / 3, toTypeName(x)")

      assert row == [Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(18, 4)"]

      assert {:ok, %{num_rows: 1, rows: [row]}} =
               query(ctx, "SELECT toDecimal128(2, 4) AS x, x / 3, toTypeName(x)")

      assert row == [Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(38, 4)"]

      assert {:ok, %{num_rows: 1, rows: [row]}} =
               query(ctx, "SELECT toDecimal256(2, 4) AS x, x / 3, toTypeName(x)")

      assert row == [Decimal.new("2.0000"), Decimal.new("0.6666"), "Decimal(76, 4)"]

      query!(ctx, "create table decimal_t(d Decimal32(4)) engine = Memory")
      on_exit(fn -> Ch.Test.query("drop table decimal_t") end)

      assert %{num_rows: 3} =
               query!(
                 ctx,
                 "insert into decimal_t(d) format RowBinary",
                 _rows = [
                   [Decimal.new("2.66")],
                   [Decimal.new("2.6666")],
                   [Decimal.new("2.66666")]
                 ],
                 types: ["Decimal32(4)"]
               )

      assert query!(ctx, "select * from decimal_t").rows == [
               [Decimal.new("2.6600")],
               [Decimal.new("2.6666")],
               [Decimal.new("2.6667")]
             ]
    end

    test "boolean", ctx do
      assert {:ok, %{num_rows: 1, rows: [[true, "Bool"]]}} =
               query(ctx, "select true as col, toTypeName(col)")

      assert {:ok, %{num_rows: 1, rows: [[1, "UInt8"]]}} =
               query(ctx, "select true == 1 as col, toTypeName(col)")

      assert {:ok, %{num_rows: 1, rows: [[true, false]]}} = query(ctx, "select true, false")

      query!(ctx, "create table test_bool(A Int64, B Bool) engine = Memory")
      on_exit(fn -> Ch.Test.query("drop table test_bool") end)

      query!(ctx, "INSERT INTO test_bool VALUES (1, true),(2,0)")

      query!(
        ctx,
        "insert into test_bool(A, B) format RowBinary",
        _rows = [[3, true], [4, false]],
        types: ["Int64", "Bool"]
      )

      # anything > 0 is `true`, here `2` is `true`
      query!(ctx, "insert into test_bool(A, B) values (5, 2)")

      assert %{
               rows: [
                 [1, true, 1],
                 [2, false, 0],
                 [3, true, 3],
                 [4, false, 0],
                 [5, true, 5]
               ]
             } = query!(ctx, "SELECT *, A * B FROM test_bool ORDER BY A")
    end

    test "uuid", ctx do
      assert {:ok, %{num_rows: 1, rows: [[<<_::16-bytes>>]]}} =
               query(ctx, "select generateUUIDv4()")

      assert {:ok, %{num_rows: 1, rows: [[uuid, "417ddc5d-e556-4d27-95dd-a34d84e46a50"]]}} =
               query(ctx, "select {uuid:UUID} as u, toString(u)", %{
                 "uuid" => "417ddc5d-e556-4d27-95dd-a34d84e46a50"
               })

      assert uuid ==
               "417ddc5d-e556-4d27-95dd-a34d84e46a50"
               |> String.replace("-", "")
               |> Base.decode16!(case: :lower)

      query!(ctx, " CREATE TABLE t_uuid (x UUID, y String) ENGINE Memory")
      on_exit(fn -> Ch.Test.query("drop table t_uuid") end)

      query!(ctx, "INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'")

      assert {:ok, %{num_rows: 1, rows: [[<<_::16-bytes>>, "Example 1"]]}} =
               query(ctx, "SELECT * FROM t_uuid")

      query!(ctx, "INSERT INTO t_uuid (y) VALUES ('Example 2')")

      query!(
        ctx,
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
              }} = query(ctx, "SELECT * FROM t_uuid ORDER BY y")
    end

    @tag :skip
    test "json", ctx do
      settings = [allow_experimental_object_type: 1]

      query!(ctx, "CREATE TABLE json(o JSON) ENGINE = Memory", [], settings: settings)

      query!(ctx, ~s|INSERT INTO json VALUES ('{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}')|)

      assert query!(ctx, "SELECT o.a, o.b.c, o.b.d[3] FROM json").rows == [[1, 2, 3]]

      # named tuples are not supported yet
      assert_raise ArgumentError, fn -> query!(ctx, "SELECT o FROM json") end
    end

    @tag :json
    test "json as string", ctx do
      # after v25 ClickHouse started rendering numbers in JSON as strings
      [[version]] = query!(ctx, "select version()").rows

      parse_version = fn version ->
        version |> String.split(".") |> Enum.map(&String.to_integer/1)
      end

      version = parse_version.(version)
      numbers_as_strings? = version >= [25] and version <= [25, 8]

      [expected1, expected2] =
        if numbers_as_strings? do
          [
            [[~s|{"answer":"42"}|]],
            [[~s|{"a":"42"}|], [~s|{"b":"10"}|]]
          ]
        else
          [
            [[~s|{"answer":42}|]],
            [[~s|{"a":42}|], [~s|{"b":10}|]]
          ]
        end

      assert query!(ctx, ~s|select '{"answer":42}'::JSON::String|, [],
               settings: [enable_json_type: 1]
             ).rows == expected1

      query!(ctx, "CREATE TABLE test_json_as_string(json JSON) ENGINE = Memory", [],
        settings: [enable_json_type: 1]
      )

      on_exit(fn -> Ch.Test.query("DROP TABLE test_json_as_string") end)

      query!(
        ctx,
        "INSERT INTO test_json_as_string(json) FORMAT RowBinary",
        _rows = [[Jason.encode_to_iodata!(%{"a" => 42})], [Jason.encode_to_iodata!(%{"b" => 10})]],
        types: [:string],
        settings: [
          enable_json_type: 1,
          input_format_binary_read_json_as_string: 1
        ]
      )

      assert query!(ctx, "select json::String from test_json_as_string", [],
               settings: [enable_json_type: 1]
             ).rows == expected2
    end

    # TODO enum16

    test "enum8", ctx do
      assert {:ok, %{num_rows: 1, rows: [["Enum8('a' = 1, 'b' = 2)"]]}} =
               query(ctx, "SELECT toTypeName(CAST('a', 'Enum(\\'a\\' = 1, \\'b\\' = 2)'))")

      assert {:ok, %{num_rows: 1, rows: [["a"]]}} =
               query(ctx, "SELECT CAST('a', 'Enum(\\'a\\' = 1, \\'b\\' = 2)')")

      assert {:ok, %{num_rows: 1, rows: [["b"]]}} =
               query(ctx, "select {enum:Enum('a' = 1, 'b' = 2)}", %{"enum" => "b"})

      assert {:ok, %{num_rows: 1, rows: [["b"]]}} =
               query(ctx, "select {enum:Enum('a' = 1, 'b' = 2)}", %{"enum" => 2})

      assert {:ok, %{num_rows: 1, rows: [["b"]]}} =
               query(ctx, "select {enum:Enum16('a' = 1, 'b' = 2)}", %{"enum" => 2})

      query!(
        ctx,
        "CREATE TABLE t_enum(i UInt8, x Enum('hello' = 1, 'world' = 2)) ENGINE Memory"
      )

      on_exit(fn -> Ch.Test.query("DROP TABLE t_enum") end)

      query!(ctx, "INSERT INTO t_enum VALUES (0, 'hello'), (1, 'world'), (2, 'hello')")

      assert query!(ctx, "SELECT *, CAST(x, 'Int8') FROM t_enum ORDER BY i").rows == [
               [0, "hello", 1],
               [1, "world", 2],
               [2, "hello", 1]
             ]

      query!(
        ctx,
        "INSERT INTO t_enum(i, x) FORMAT RowBinary",
        _rows = [[3, "hello"], [4, "world"], [5, 1], [6, 2]],
        types: ["UInt8", "Enum8('hello' = 1, 'world' = 2)"]
      )

      assert query!(ctx, "SELECT *, CAST(x, 'Int8') FROM t_enum ORDER BY i").rows == [
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

    test "map", ctx do
      assert query!(
               ctx,
               "SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map"
             ).rows == [[%{1 => "Ready", 2 => "Steady", 3 => "Go"}]]

      assert query!(ctx, "select {map:Map(String, UInt8)}", %{
               "map" => %{"pg" => 13, "hello" => 100}
             }).rows == [[%{"hello" => 100, "pg" => 13}]]

      query!(ctx, "CREATE TABLE table_map (a Map(String, UInt64)) ENGINE=Memory")
      on_exit(fn -> Ch.Test.query("DROP TABLE table_map") end)

      query!(
        ctx,
        "INSERT INTO table_map VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30})"
      )

      assert query!(ctx, "SELECT a['key2'] FROM table_map").rows == [[10], [20], [30]]

      assert query!(ctx, "INSERT INTO table_map VALUES ({'key3':100}), ({})")

      assert query!(ctx, "SELECT a['key3'] FROM table_map ORDER BY 1 DESC").rows == [
               [100],
               [0],
               [0],
               [0],
               [0]
             ]

      assert query!(
               ctx,
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

      assert query!(ctx, "SELECT * FROM table_map ORDER BY a ASC").rows == [
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

    test "tuple", ctx do
      assert query!(ctx, "SELECT tuple(1,'a') AS x, toTypeName(x)").rows == [
               [{1, "a"}, "Tuple(UInt8, String)"]
             ]

      assert query!(ctx, "SELECT {$0:Tuple(Int8, String)}", [{-1, "abs"}]).rows == [
               [{-1, "abs"}]
             ]

      assert query!(ctx, "SELECT tuple('a') AS x").rows == [[{"a"}]]

      assert query!(ctx, "SELECT tuple(1, NULL) AS x, toTypeName(x)").rows == [
               [{1, nil}, "Tuple(UInt8, Nullable(Nothing))"]
             ]

      # TODO named tuples
      query!(ctx, "CREATE TABLE tuples_t (`a` Tuple(String, Int64)) ENGINE = Memory")
      on_exit(fn -> Ch.Test.query("DROP TABLE tuples_t") end)

      assert %{num_rows: 2} =
               query!(ctx, "INSERT INTO tuples_t VALUES (('y', 10)), (('x',-10))")

      assert %{num_rows: 2} =
               query!(
                 ctx,
                 "INSERT INTO tuples_t FORMAT RowBinary",
                 _rows = [[{"a", 20}], [{"b", 30}]],
                 types: ["Tuple(String, Int64)"]
               )

      assert query!(ctx, "SELECT a FROM tuples_t ORDER BY a.1 ASC").rows == [
               [{"a", 20}],
               [{"b", 30}],
               [{"x", -10}],
               [{"y", 10}]
             ]
    end

    test "datetime", ctx do
      query!(
        ctx,
        "CREATE TABLE dt(`timestamp` DateTime('Asia/Istanbul'), `event_id` UInt8) ENGINE = Memory"
      )

      on_exit(fn -> Ch.Test.query("DROP TABLE dt") end)

      query!(ctx, "INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2)")

      assert {:ok, %{num_rows: 2, rows: rows}} =
               query(ctx, "SELECT *, toString(timestamp) FROM dt")

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
               query(ctx, "select {$0:DateTime} as d, toString(d)", [naive_noon])

      # to make this test pass for contributors with non UTC timezone we perform the same steps as ClickHouse
      # i.e. we give server timezone to the naive datetime and shift it to UTC before comparing with the result
      {_, timezone} = List.keyfind!(headers, "x-clickhouse-timezone", 0)

      assert naive_datetime ==
               naive_noon
               |> DateTime.from_naive!(timezone)
               |> DateTime.shift_zone!("Etc/UTC")
               |> DateTime.to_naive()

      assert {:ok, %{num_rows: 1, rows: [[~U[2022-12-12 12:00:00Z], "2022-12-12 12:00:00"]]}} =
               query(ctx, "select {$0:DateTime('UTC')} as d, toString(d)", [naive_noon])

      assert {:ok, %{num_rows: 1, rows: rows}} =
               query(ctx, "select {$0:DateTime('Asia/Bangkok')} as d, toString(d)", [
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
        query(ctx, "select {$0:DateTime('Asia/Tokyo')}", [naive_noon])
      end
    end

    # TODO are negatives correct? what's the range?
    test "date32", ctx do
      query!(ctx, "CREATE TABLE new(`timestamp` Date32, `event_id` UInt8) ENGINE = Memory;")
      on_exit(fn -> Ch.Test.query("DROP TABLE new") end)

      query!(ctx, "INSERT INTO new VALUES (4102444800, 1), ('2100-01-01', 2)")

      assert {:ok,
              %{
                num_rows: 2,
                rows: [first_event, [~D[2100-01-01], 2, "2100-01-01"]]
              }} = query(ctx, "SELECT *, toString(timestamp) FROM new")

      # TODO use timezone info to be more exact
      assert first_event in [
               [~D[2099-12-31], 1, "2099-12-31"],
               [~D[2100-01-01], 1, "2100-01-01"]
             ]

      assert {:ok, %{num_rows: 1, rows: [[~D[1900-01-01], "1900-01-01"]]}} =
               query(ctx, "select {$0:Date32} as d, toString(d)", [~D[1900-01-01]])

      # max
      assert {:ok, %{num_rows: 1, rows: [[~D[2299-12-31], "2299-12-31"]]}} =
               query(ctx, "select {$0:Date32} as d, toString(d)", [~D[2299-12-31]])

      # min
      assert {:ok, %{num_rows: 1, rows: [[~D[1900-01-01], "1900-01-01"]]}} =
               query(ctx, "select {$0:Date32} as d, toString(d)", [~D[1900-01-01]])

      query!(
        ctx,
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
             } = query!(ctx, "SELECT *, toString(timestamp) FROM new ORDER BY event_id")

      # TODO use timezone info to be more exact
      assert first_event in [
               [~D[2099-12-31], 1, "2099-12-31"],
               [~D[2100-01-01], 1, "2100-01-01"]
             ]

      assert %{num_rows: 1, rows: [[3]]} =
               query!(ctx, "SELECT event_id FROM new WHERE timestamp = '1960-01-01'")
    end

    # https://clickhouse.com/docs/sql-reference/data-types/time
    @tag :time
    test "time", ctx do
      settings = [enable_time_time64_type: 1]

      query!(ctx, "CREATE TABLE time_t(`time` Time, `event_id` UInt8) ENGINE = Memory", [],
        settings: settings
      )

      on_exit(fn ->
        Ch.Test.query("DROP TABLE time_t", [], settings: settings)
      end)

      query!(ctx, "INSERT INTO time_t VALUES ('100:00:00', 1), (12453, 2)", [],
        settings: settings
      )

      # ClickHouse supports Time values of [-999:59:59, 999:59:59]
      # and Elixir's Time supports values of [00:00:00, 23:59:59]
      # so we raise an error when ClickHouse's Time value is out of Elixir's Time range

      assert_raise ArgumentError,
                   "ClickHouse Time value 3.6e5 (seconds) is out of Elixir's Time range (00:00:00.000000 - 23:59:59.999999)",
                   fn -> query!(ctx, "select * from time_t", [], settings: settings) end

      query!(
        ctx,
        "INSERT INTO time_t(time, event_id) FORMAT RowBinary",
        _rows = [
          [~T[00:00:00], 3],
          [~T[12:34:56], 4],
          [~T[23:59:59], 5]
        ],
        settings: settings,
        types: ["Time", "UInt8"]
      )

      assert query!(ctx, "select * from time_t where event_id > 1 order by event_id", [],
               settings: settings
             ).rows ==
               [[~T[03:27:33], 2], [~T[00:00:00], 3], [~T[12:34:56], 4], [~T[23:59:59], 5]]
    end

    # https://clickhouse.com/docs/sql-reference/data-types/time64
    @tag :time
    test "Time64(3)", ctx do
      settings = [enable_time_time64_type: 1]

      query!(
        ctx,
        "CREATE TABLE time64_3_t(`time` Time64(3), `event_id` UInt8) ENGINE = Memory",
        [],
        settings: settings
      )

      on_exit(fn ->
        Ch.Test.query("DROP TABLE time64_3_t", [], settings: settings)
      end)

      query!(
        ctx,
        "INSERT INTO time64_3_t VALUES (15463123, 1), (154600.123, 2), ('100:00:00', 3);",
        [],
        settings: settings
      )

      # ClickHouse supports Time64 values of [-999:59:59.999999999, 999:59:59.999999999]
      # and Elixir's Time supports values of [00:00:00.000000, 23:59:59.999999]
      # so we raise an error when ClickHouse's Time64 value is out of Elixir's Time range

      assert_raise ArgumentError,
                   "ClickHouse Time value 154600.123 (seconds) is out of Elixir's Time range (00:00:00.000000 - 23:59:59.999999)",
                   fn -> query!(ctx, "select * from time64_3_t", [], settings: settings) end

      query!(
        ctx,
        "INSERT INTO time64_3_t(time, event_id) FORMAT RowBinary",
        _rows = [
          [~T[00:00:00.000000], 4],
          [~T[12:34:56.012300], 5],
          [~T[12:34:56.123456], 6],
          [~T[12:34:56.120000], 7],
          [~T[23:59:59.999999], 8]
        ],
        settings: settings,
        types: ["Time64(3)", "UInt8"]
      )

      assert query!(
               ctx,
               "select * from time64_3_t where time < {max_elixir_time:Time64(6)} order by event_id",
               %{"max_elixir_time" => ~T[23:59:59.999999]},
               settings: settings
             ).rows ==
               [
                 [~T[04:17:43.123], 1],
                 [~T[00:00:00.000], 4],
                 [~T[12:34:56.012], 5],
                 [~T[12:34:56.123], 6],
                 [~T[12:34:56.120], 7],
                 [~T[23:59:59.999], 8]
               ]
    end

    @tag :time
    test "Time64(6)", ctx do
      settings = [enable_time_time64_type: 1]

      query!(
        ctx,
        "CREATE TABLE time64_6_t(`time` Time64(6), `event_id` UInt8) ENGINE = Memory",
        [],
        settings: settings
      )

      on_exit(fn ->
        Ch.Test.query("DROP TABLE time64_6_t", [], settings: settings)
      end)

      query!(
        ctx,
        "INSERT INTO time64_6_t(time, event_id) FORMAT RowBinary",
        _rows = [
          [~T[00:00:00.000000], 1],
          [~T[12:34:56.123456], 2],
          [~T[12:34:56.123000], 3],
          [~T[12:34:56.000123], 4],
          [~T[23:59:59.999999], 5]
        ],
        settings: settings,
        types: ["Time64(6)", "UInt8"]
      )

      assert query!(
               ctx,
               "select * from time64_6_t order by event_id",
               [],
               settings: settings
             ).rows ==
               [
                 [~T[00:00:00.000000], 1],
                 [~T[12:34:56.123456], 2],
                 [~T[12:34:56.123000], 3],
                 [~T[12:34:56.000123], 4],
                 [~T[23:59:59.999999], 5]
               ]
    end

    @tag :time
    test "Time64(9)", ctx do
      settings = [enable_time_time64_type: 1]

      query!(
        ctx,
        "CREATE TABLE time64_9_t(`time` Time64(9), `event_id` UInt8) ENGINE = Memory",
        [],
        settings: settings
      )

      on_exit(fn ->
        Ch.Test.query("DROP TABLE time64_9_t", [], settings: settings)
      end)

      query!(
        ctx,
        "INSERT INTO time64_9_t(time, event_id) FORMAT RowBinary",
        _rows = [
          [~T[00:00:00.000000], 1],
          [~T[12:34:56.123456], 2],
          [~T[12:34:56.123000], 3],
          [~T[12:34:56.000123], 4],
          [~T[23:59:59.999999], 5]
        ],
        settings: settings,
        types: ["Time64(9)", "UInt8"]
      )

      assert query!(
               ctx,
               "select * from time64_9_t order by event_id",
               [],
               settings: settings
             ).rows ==
               [
                 [~T[00:00:00.000000], 1],
                 [~T[12:34:56.123456], 2],
                 [~T[12:34:56.123000], 3],
                 [~T[12:34:56.000123], 4],
                 [~T[23:59:59.999999], 5]
               ]
    end

    test "datetime64", ctx do
      query!(
        ctx,
        "CREATE TABLE datetime64_t(`timestamp` DateTime64(3, 'Asia/Istanbul'), `event_id` UInt8) ENGINE = Memory"
      )

      on_exit(fn -> Ch.Test.query("DROP TABLE datetime64_t") end)

      query!(
        ctx,
        "INSERT INTO datetime64_t Values (1546300800123, 1), (1546300800.123, 2), ('2019-01-01 00:00:00', 3)"
      )

      assert {:ok, %{num_rows: 3, rows: rows}} =
               query(ctx, "SELECT *, toString(timestamp) FROM datetime64_t")

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

      query!(
        ctx,
        "insert into datetime64_t(event_id, timestamp) format RowBinary",
        _rows = [
          [4, ~N[2021-01-01 12:00:00.123456]],
          [5, ~N[2021-01-01 12:00:00]]
        ],
        types: ["UInt8", "DateTime64(3)"]
      )

      assert {:ok, %{num_rows: 2, rows: rows}} =
               query(
                 ctx,
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
                 query(ctx, "select {$0:DateTime64(#{precision})}", [naive_noon])

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
               query(ctx, "select {dt:DateTime64(3,'UTC')} as d, toString(d)", %{
                 "dt" => ~N[2022-01-01 12:00:00.123]
               })

      assert {:ok,
              %{num_rows: 1, rows: [[~U[1900-01-01 12:00:00.123Z], "1900-01-01 12:00:00.123"]]}} =
               query(ctx, "select {dt:DateTime64(3,'UTC')} as d, toString(d)", %{
                 "dt" => ~N[1900-01-01 12:00:00.123]
               })

      assert {:ok, %{num_rows: 1, rows: [row]}} =
               query(ctx, "select {dt:DateTime64(3,'Asia/Bangkok')} as d, toString(d)", %{
                 "dt" => ~N[2022-01-01 12:00:00.123]
               })

      assert row == [
               DateTime.new!(~D[2022-01-01], ~T[12:00:00.123], "Asia/Bangkok"),
               "2022-01-01 12:00:00.123"
             ]
    end

    test "nullable", ctx do
      query!(
        ctx,
        "CREATE TABLE nullable (`n` Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple()"
      )

      on_exit(fn -> Ch.Test.query("DROP TABLE nullable") end)

      query!(ctx, "INSERT INTO nullable VALUES (1) (NULL) (2) (NULL)")

      assert {:ok, %{num_rows: 4, rows: [[0], [1], [0], [1]]}} =
               query(ctx, "SELECT n.null FROM nullable")

      assert {:ok, %{num_rows: 4, rows: [[1], [nil], [2], [nil]]}} =
               query(ctx, "SELECT n FROM nullable")

      # weird thing about nullables is that, similar to bool, in binary format, any byte larger than 0 is `null`
      assert {:ok, %{num_rows: 5}} =
               query(
                 ctx,
                 "insert into nullable format RowBinary",
                 <<1, 2, 3, 4, 5>>,
                 encode: false
               )

      assert %{num_rows: 1, rows: [[count]]} =
               query!(ctx, "select count(*) from nullable where n is null")

      assert count == 2 + 5
    end

    test "nullable + default", ctx do
      query!(ctx, """
      CREATE TABLE ch_nulls (
        a UInt8,
        b UInt8 NULL,
        c UInt8 DEFAULT 10,
        d Nullable(UInt8) DEFAULT 10,
      ) ENGINE Memory
      """)

      on_exit(fn -> Ch.Test.query("DROP TABLE ch_nulls") end)

      query!(
        ctx,
        "INSERT INTO ch_nulls(a, b, c, d) FORMAT RowBinary",
        [[nil, nil, nil, nil]],
        types: ["UInt8", "Nullable(UInt8)", "UInt8", "Nullable(UInt8)"]
      )

      # default is ignored...
      assert query!(ctx, "SELECT * FROM ch_nulls").rows == [[0, nil, 0, nil]]
    end

    # based on https://github.com/ClickHouse/clickhouse-java/pull/1345/files
    test "nullable + input() + default", ctx do
      query!(ctx, """
      CREATE TABLE test_insert_default_value(
        n Int32,
        s String DEFAULT 'secret'
      ) ENGINE Memory
      """)

      on_exit(fn -> Ch.Test.query("DROP TABLE test_insert_default_value") end)

      query!(
        ctx,
        """
        INSERT INTO test_insert_default_value
          SELECT id, name
          FROM input('id UInt32, name Nullable(String)')
          FORMAT RowBinary\
        """,
        [[1, nil], [-1, nil]],
        types: ["UInt32", "Nullable(String)"]
      )

      assert query!(ctx, "SELECT * FROM test_insert_default_value ORDER BY n").rows ==
               [
                 [-1, "secret"],
                 [1, "secret"]
               ]
    end

    test "can decode casted Point", ctx do
      assert query!(ctx, "select cast((0, 1) as Point)").rows == [
               _row = [_point = {0.0, 1.0}]
             ]
    end

    test "can encode and then decode Point in query params", ctx do
      assert query!(ctx, "select {$0:Point}", [{10, 10}]).rows == [
               _row = [_point = {10.0, 10.0}]
             ]
    end

    test "can insert and select Point", ctx do
      query!(ctx, "CREATE TABLE geo_point (p Point) ENGINE = Memory()")
      on_exit(fn -> Ch.Test.query("DROP TABLE geo_point") end)

      query!(ctx, "INSERT INTO geo_point VALUES((10, 10))")
      query!(ctx, "INSERT INTO geo_point FORMAT RowBinary", [[{20, 20}]], types: ["Point"])

      assert query!(ctx, "SELECT p, toTypeName(p) FROM geo_point ORDER BY p ASC").rows == [
               [{10.0, 10.0}, "Point"],
               [{20.0, 20.0}, "Point"]
             ]

      # to make our RowBinary is not garbage in garbage out we also test a text format response
      assert query!(
               ctx,
               "SELECT p, toTypeName(p) FROM geo_point ORDER BY p ASC FORMAT JSONCompact"
             ).rows
             |> Jason.decode!()
             |> Map.fetch!("data") == [
               [[10, 10], "Point"],
               [[20, 20], "Point"]
             ]
    end

    test "can decode casted Ring", ctx do
      ring = [{0.0, 1.0}, {10.0, 3.0}]
      assert query!(ctx, "select cast([(0,1),(10,3)] as Ring)").rows == [_row = [ring]]
    end

    test "can encode and then decode Ring in query params", ctx do
      ring = [{0.0, 1.0}, {10.0, 3.0}]
      assert query!(ctx, "select {$0:Ring}", [ring]).rows == [_row = [ring]]
    end

    test "can insert and select Ring", ctx do
      query!(ctx, "CREATE TABLE geo_ring (r Ring) ENGINE = Memory()")
      on_exit(fn -> Ch.Test.query("DROP TABLE geo_ring") end)

      query!(ctx, "INSERT INTO geo_ring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)])")

      ring = [{20, 20}, {0, 0}, {0, 20}]
      query!(ctx, "INSERT INTO geo_ring FORMAT RowBinary", [[ring]], types: ["Ring"])

      assert query!(ctx, "SELECT r, toTypeName(r) FROM geo_ring ORDER BY r ASC").rows == [
               [[{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}], "Ring"],
               [[{20.0, 20.0}, {0.0, 0.0}, {0.0, 20.0}], "Ring"]
             ]

      # to make our RowBinary is not garbage in garbage out we also test a text format response
      assert query!(
               ctx,
               "SELECT r, toTypeName(r) FROM geo_ring ORDER BY r ASC FORMAT JSONCompact"
             ).rows
             |> Jason.decode!()
             |> Map.fetch!("data") == [
               [[[0, 0], [10, 0], [10, 10], [0, 10]], "Ring"],
               [[[20, 20], [0, 0], [0, 20]], "Ring"]
             ]
    end

    test "can decode casted Polygon", ctx do
      polygon = [[{0.0, 1.0}, {10.0, 3.0}], [], [{2, 2}]]

      assert query!(ctx, "select cast([[(0,1),(10,3)],[],[(2,2)]] as Polygon)").rows == [
               _row = [polygon]
             ]
    end

    test "can encode and then decode Polygon in query params", ctx do
      polygon = [[{0.0, 1.0}, {10.0, 3.0}], [], [{2, 2}]]
      assert query!(ctx, "select {$0:Polygon}", [polygon]).rows == [_row = [polygon]]
    end

    test "can insert and select Polygon", ctx do
      query!(ctx, "CREATE TABLE geo_polygon (pg Polygon) ENGINE = Memory()")
      on_exit(fn -> Ch.Test.query("DROP TABLE geo_polygon") end)

      query!(
        ctx,
        "INSERT INTO geo_polygon VALUES([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]])"
      )

      polygon = [[{0, 1.0}, {10, 3.2}], [], [{2, 2}]]
      query!(ctx, "INSERT INTO geo_polygon FORMAT RowBinary", [[polygon]], types: ["Polygon"])

      assert query!(ctx, "SELECT pg, toTypeName(pg) FROM geo_polygon ORDER BY pg ASC").rows ==
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
      assert query!(
               ctx,
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

    test "can decode casted MultiPolygon", ctx do
      multipolygon = [[[{0.0, 1.0}, {10.0, 3.0}], [], [{2, 2}]], [], [[{3, 3}]]]

      assert query!(
               ctx,
               "select cast([[[(0,1),(10,3)],[],[(2,2)]],[],[[(3, 3)]]] as MultiPolygon)"
             ).rows == [
               _row = [multipolygon]
             ]
    end

    test "can encode and then decode MultiPolygon in query params", ctx do
      multipolygon = [[[{0.0, 1.0}, {10.0, 3.0}], [], [{2, 2}]], [], [[{3, 3}]]]

      assert query!(ctx, "select {$0:MultiPolygon}", [multipolygon]).rows == [
               _row = [multipolygon]
             ]
    end

    test "can insert and select MultiPolygon", ctx do
      query!(ctx, "CREATE TABLE geo_multipolygon (mpg MultiPolygon) ENGINE = Memory()")
      on_exit(fn -> Ch.Test.query("DROP TABLE geo_multipolygon") end)

      query!(
        ctx,
        "INSERT INTO geo_multipolygon VALUES([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]])"
      )

      multipolygon = [[[{0.0, 1.0}, {10.0, 3.0}], [], [{2, 2}]], [], [[{3, 3}]]]

      query!(ctx, "INSERT INTO geo_multipolygon FORMAT RowBinary", [[multipolygon]],
        types: ["MultiPolygon"]
      )

      assert query!(ctx, "SELECT mpg, toTypeName(mpg) FROM geo_multipolygon ORDER BY mpg ASC").rows ==
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
      assert query!(
               ctx,
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
    test "can provide custom timeout", ctx do
      assert {:error, %Mint.TransportError{reason: :timeout} = error} =
               query(ctx, "select sleep(1)", _params = [], timeout: 100)

      assert Exception.message(error) == "timeout"
    end

    test "errors on invalid creds", ctx do
      assert {:error, %Ch.Error{code: 516} = error} =
               query(ctx, "select 1 + 1", _params = [],
                 username: "no-exists",
                 password: "wrong"
               )

      assert Exception.message(error) =~
               "Code: 516. DB::Exception: no-exists: Authentication failed: password is incorrect, or there is no user with such name. (AUTHENTICATION_FAILED)"
    end

    test "errors on invalid database", ctx do
      assert {:error, %Ch.Error{code: 81} = error} =
               query(ctx, "select 1 + 1", _params = [], database: "no-db")

      assert Exception.message(error) =~ "`no-db`"
      assert Exception.message(error) =~ "UNKNOWN_DATABASE"
    end

    test "can provide custom database", ctx do
      assert {:ok, %{num_rows: 1, rows: [[2]]}} =
               query(ctx, "select 1 + 1", [], database: "default")
    end
  end

  describe "transactions" do
    test "commit", ctx do
      DBConnection.transaction(ctx.conn, fn conn ->
        ctx = Map.put(ctx, :conn, conn)
        query!(ctx, "select 1 + 1")
      end)
    end

    test "rollback", ctx do
      DBConnection.transaction(ctx.conn, fn conn ->
        DBConnection.rollback(conn, :some_reason)
      end)
    end

    test "status", ctx do
      assert DBConnection.status(ctx.conn) == :idle
    end
  end

  describe "stream" do
    test "emits result structs containing raw data", ctx do
      results =
        DBConnection.run(ctx.conn, fn conn ->
          conn
          |> Ch.stream(
            "select number from system.numbers limit {limit:UInt64}",
            %{"limit" => 10_000},
            decode: false
          )
          |> Enum.into([])
        end)

      assert length(results) >= 2

      assert results
             |> Enum.map(& &1.data)
             |> IO.iodata_to_binary()
             |> RowBinary.decode_rows() == Enum.map(0..9999, &[&1])
    end

    test "disconnects on early halt", ctx do
      logs =
        ExUnit.CaptureLog.capture_log(fn ->
          Ch.run(ctx.conn, fn conn ->
            conn |> Ch.stream("select number from system.numbers") |> Enum.take(1)
          end)

          assert query!(ctx, "select 1 + 1").rows == [[2]]
        end)

      assert logs =~
               "disconnected: ** (Ch.Error) stopping stream before receiving full response by closing connection"
    end
  end

  describe "prepare" do
    test "no-op", ctx do
      query = Ch.Query.build("select 1 + 1")

      assert {:error, %Ch.Error{message: "prepared statements are not supported"}} =
               DBConnection.prepare(ctx.conn, query)
    end
  end

  describe "start_link/1" do
    test "can pass options to start_link/1", ctx do
      db = "#{Ch.Test.database()}_#{System.unique_integer([:positive])}"
      Ch.Test.query("CREATE DATABASE {db:Identifier}", %{"db" => db})
      on_exit(fn -> Ch.Test.query("DROP DATABASE {db:Identifier}", %{"db" => db}) end)

      {:ok, conn} = Ch.start_link(database: db)
      ctx = Map.put(ctx, :conn, conn)
      query!(ctx, "create table example(a UInt8) engine=Memory")
      assert {:ok, %{rows: [["example"]]}} = query(ctx, "show tables")
    end

    test "can start without options", ctx do
      {:ok, conn} = Ch.start_link()
      ctx = Map.put(ctx, :conn, conn)
      assert {:ok, %{num_rows: 1, rows: [[2]]}} = query(ctx, "select 1 + 1")
    end
  end

  describe "RowBinaryWithNamesAndTypes" do
    setup ctx do
      query!(ctx, """
      create table if not exists row_binary_names_and_types_t (
        country_code FixedString(2),
        rare_string LowCardinality(String),
        maybe_int32 Nullable(Int32)
      ) engine Memory
      """)

      on_exit(fn -> Ch.Test.query("truncate row_binary_names_and_types_t") end)
    end

    test "error on type mismatch", ctx do
      stmt = "insert into row_binary_names_and_types_t format RowBinaryWithNamesAndTypes"
      rows = [["AB", "rare", -42]]
      names = ["country_code", "rare_string", "maybe_int32"]

      opts = [
        names: names,
        types: [Ch.Types.fixed_string(2), Ch.Types.string(), Ch.Types.nullable(Ch.Types.u32())]
      ]

      assert {:error, %Ch.Error{code: 117, message: message}} = query(ctx, stmt, rows, opts)
      assert message =~ "Type of 'rare_string' must be LowCardinality(String), not String"

      opts = [
        names: names,
        types: [
          Ch.Types.fixed_string(2),
          Ch.Types.low_cardinality(Ch.Types.string()),
          Ch.Types.nullable(Ch.Types.u32())
        ]
      ]

      assert {:error, %Ch.Error{code: 117, message: message}} = query(ctx, stmt, rows, opts)
      assert message =~ "Type of 'maybe_int32' must be Nullable(Int32), not Nullable(UInt32)"
    end

    test "ok on valid types", ctx do
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

      assert {:ok, %{num_rows: 1}} = query(ctx, stmt, rows, opts)
    end

    test "select with lots of columns", ctx do
      select = Enum.map_join(1..1000, ", ", fn i -> "#{i} as col_#{i}" end)
      stmt = "select #{select} format RowBinaryWithNamesAndTypes"

      assert %Ch.Result{columns: columns, rows: [row]} = query!(ctx, stmt)

      assert length(columns) == 1000
      assert List.first(columns) == "col_1"
      assert List.last(columns) == "col_1000"

      assert length(row) == 1000
      assert List.first(row) == 1
      assert List.last(row) == 1000
    end
  end
end
