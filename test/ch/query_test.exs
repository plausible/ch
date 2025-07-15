defmodule Ch.QueryTest do
  use ExUnit.Case, async: true
  alias Ch.Query

  test "to_string" do
    query = Query.build(["select ", 1 + ?0, ?+, 2 + ?0])
    assert to_string(query) == "select 1+2"
  end

  describe "command" do
    test "without command provided" do
      assert Query.build("select 1+2").command == :select
      assert Query.build("SELECT 1+2").command == :select
      assert Query.build("   select 1+2").command == :select
      assert Query.build("\t\n\t\nSELECT 1+2").command == :select

      assert Query.build("""

             select 1+2
             """).command == :select

      assert Query.build(["select 1+2"]).command == :select
      assert Query.build([?S, ?E, ?L | "ECT 1"]).command == :select

      assert Query.build("with insert as (select 1) select * from insert").command == :select
      assert Query.build("insert into table(a, b) values(1, 2)").command == :insert

      assert Query.build("insert into table(a, b) select b, c from table2 where b = 'update'").command ==
               :insert
    end

    test "with nil command provided" do
      assert Query.build("select 1+2", command: nil).command == :select
    end

    test "with command provided" do
      assert Query.build("select 1+2", command: :custom).command == :custom
    end

    @tag skip: true
    test "TODO" do
      assert Query.build("Select 1+2").command == :select
    end
  end

  # adapted from https://github.com/elixir-ecto/postgrex/blob/master/test/query_test.exs
  describe "query" do
    setup do
      {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
    end

    test "iodata", %{conn: conn} do
      assert [[123]] = Ch.query!(conn, ["S", ?E, ["LEC" | "T"], " ", ~c"123"]).rows
    end

    test "decode basic types", %{conn: conn} do
      assert [[nil]] = Ch.query!(conn, "SELECT NULL").rows
      assert [[true, false]] = Ch.query!(conn, "SELECT true, false").rows
      assert [["e"]] = Ch.query!(conn, "SELECT 'e'::char").rows
      assert [["ẽ"]] = Ch.query!(conn, "SELECT 'ẽ'::char").rows
      assert [[42]] = Ch.query!(conn, "SELECT 42").rows
      assert [[42.0]] = Ch.query!(conn, "SELECT 42::float").rows
      assert [[42.0]] = Ch.query!(conn, "SELECT 42.0").rows
      # TODO [[:NaN]] ?
      assert [[nil]] = Ch.query!(conn, "SELECT 'NaN'::float").rows
      # TODO [[:int]] ?
      assert [[nil]] = Ch.query!(conn, "SELECT 'inf'::float").rows
      # TODO [[:"-inf"]] ?
      assert [[nil]] = Ch.query!(conn, "SELECT '-inf'::float").rows
      assert [["ẽric"]] = Ch.query!(conn, "SELECT 'ẽric'").rows
      assert [["ẽric"]] = Ch.query!(conn, "SELECT 'ẽric'::varchar").rows
      # TODO
      # assert [[<<1, 2, 3>>]] = Ch.query!(conn, "SELECT '\\001\\002\\003'::bytea").rows
    end

    test "decode numeric", %{conn: conn} do
      assert [[Decimal.new("42.0000000000")]] == Ch.query!(conn, "SELECT 42::numeric(10,10)").rows
    end

    @tag skip: true
    test "decode json/jsonb", %{conn: conn} do
      assert_raise ArgumentError, "Object('json') type is not supported", fn ->
        assert [[%{"foo" => 42}]] == Ch.query!(conn, "SELECT '{\"foo\": 42}'::json").rows
      end
    end

    test "decode uuid", %{conn: conn} do
      uuid = <<160, 238, 188, 153, 156, 11, 78, 248, 187, 109, 107, 185, 189, 56, 10, 17>>

      assert [[^uuid]] =
               Ch.query!(conn, "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID").rows
    end

    test "decode time", %{conn: conn} do
      assert Ch.query!(conn, "SELECT '12:34:56'::time", [],
               settings: [enable_time_time64_type: 1]
             ).rows == [[~T[12:34:56]]]
    end

    test "decode arrays", %{conn: conn} do
      assert [[[]]] = Ch.query!(conn, "SELECT []").rows
      assert [[[1]]] = Ch.query!(conn, "SELECT [1]").rows
      assert [[[1, 2]]] = Ch.query!(conn, "SELECT [1,2]").rows
      assert [[[[0], [1]]]] = Ch.query!(conn, "SELECT [[0],[1]]").rows
      assert [[[[0]]]] = Ch.query!(conn, "SELECT [[0]]").rows
    end

    test "decode tuples", %{conn: conn} do
      assert [[{"Hello", 123}]] = Ch.query!(conn, "select ('Hello', 123)").rows
      assert [[{"Hello", 123}]] = Ch.query!(conn, "select ('Hello' as a, 123 as b)").rows
      assert [[{"Hello", 123}]] = Ch.query!(conn, "select ('Hello' as a_, 123 as b)").rows
      # TODO
      # assert [[{"Hello", 123}]] = Ch.query!(conn, "select ('Hello' as a$, 123 as b)").rows
    end

    test "decode network types", %{conn: conn} do
      assert [[{127, 0, 0, 1} = ipv4]] = Ch.query!(conn, "SELECT '127.0.0.1'::inet4").rows
      assert :inet.ntoa(ipv4) == ~c"127.0.0.1"

      assert [[{0, 0, 0, 0, 0, 0, 0, 1} = ipv6]] = Ch.query!(conn, "SELECT '::1'::inet6").rows
      assert :inet.ntoa(ipv6) == ~c"::1"

      assert [[ipv6]] = Ch.query!(conn, "SELECT '2001:44c8:129:2632:33:0:252:2'::inet6").rows
      assert :inet.ntoa(ipv6) == ~c"2001:44c8:129:2632:33:0:252:2"
    end

    test "decoded binaries copy behaviour", %{conn: conn} do
      text = "hello world"
      assert [[bin]] = Ch.query!(conn, "SELECT {$0:String}", [text]).rows
      assert :binary.referenced_byte_size(bin) == :binary.referenced_byte_size("hello world")

      # For OTP 20+ refc binaries up to 64 bytes might be copied during a GC
      text = String.duplicate("hello world", 6)
      assert [[bin]] = Ch.query!(conn, "SELECT {$0:String}", [text]).rows
      assert :binary.referenced_byte_size(bin) == byte_size(text)
    end

    test "encode basic types", %{conn: conn} do
      # TODO
      # assert [[nil, nil]] = query("SELECT $1::text, $2::int", [nil, nil])
      assert [[true, false]] = Ch.query!(conn, "SELECT {$0:bool}, {$1:Bool}", [true, false]).rows
      assert [["ẽ"]] = Ch.query!(conn, "SELECT {$0:char}", ["ẽ"]).rows
      assert [[42]] = Ch.query!(conn, "SELECT {$0:int}", [42]).rows
      assert [[42.0, 43.0]] = Ch.query!(conn, "SELECT {$0:float}, {$1:float}", [42, 43.0]).rows
      assert [[nil, nil]] = Ch.query!(conn, "SELECT {$0:float}, {$1:float}", ["NaN", "nan"]).rows
      assert [[nil]] = Ch.query!(conn, "SELECT {$0:float}", ["inf"]).rows
      assert [[nil]] = Ch.query!(conn, "SELECT {$0:float}", ["-inf"]).rows
      assert [["ẽric"]] = Ch.query!(conn, "SELECT {$0:varchar}", ["ẽric"]).rows
      assert [[<<1, 2, 3>>]] = Ch.query!(conn, "SELECT {$0:bytea}", [<<1, 2, 3>>]).rows
    end

    test "encode numeric", %{conn: conn} do
      nums = [
        {"42", "numeric(2,0)"},
        {"0.4242", "numeric(4,4)"},
        {"42.4242", "numeric(6,4)"},
        {"1.001", "numeric(4,3)"},
        {"1.00123", "numeric(6,5)"},
        {"0.01", "numeric(3,2)"},
        {"0.00012345", "numeric(9,8)"},
        {"1000000000", "numeric(10,0)"},
        {"1000000000.0", "numeric(11,1)"},
        {"123456789123456789123456789", "numeric(27,0)"},
        {"123456789123456789123456789.123456789", "numeric(36,9)"},
        {"1.1234500000", "numeric(11,10)"},
        {"1.0000000000", "numeric(11,10)"},
        {"1.111101", "numeric(7,6)"},
        {"1.1111111101", "numeric(11,10)"},
        {"1.11110001", "numeric(9,8)"},
        # {"NaN", "numeric(1,0)"},
        {"-42", "numeric(2,0)"}
      ]

      Enum.each(nums, fn {num, type} ->
        dec = Decimal.new(num)
        assert [[dec]] == Ch.query!(conn, "SELECT {$0:#{type}}", [dec]).rows
      end)
    end

    test "encode integers and floats as numeric", %{conn: conn} do
      dec = Decimal.new(1)
      assert [[dec]] == Ch.query!(conn, "SELECT {$0:numeric(1,0)}", [1]).rows

      dec = Decimal.from_float(1.0)
      assert [[dec]] == Ch.query!(conn, "SELECT {$0:numeric(2,1)}", [1.0]).rows
    end

    @tag skip: true
    test "encode json/jsonb", %{conn: conn} do
      json = %{"foo" => 42}
      assert [[json]] == Ch.query!(conn, "SELECT {$0::json}", [json]).rows
    end

    test "encode uuid", %{conn: conn} do
      # TODO
      uuid = <<0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15>>
      uuid_hex = "00010203-0405-0607-0809-0a0b0c0d0e0f"
      assert [[^uuid]] = Ch.query!(conn, "SELECT {$0:UUID}", [uuid_hex]).rows
    end

    test "encode arrays", %{conn: conn} do
      assert [[[]]] = Ch.query!(conn, "SELECT {$0:Array(integer)}", [[]]).rows
      assert [[[1]]] = Ch.query!(conn, "SELECT {$0:Array(integer)}", [[1]]).rows
      assert [[[1, 2]]] = Ch.query!(conn, "SELECT {$0:Array(integer)}", [[1, 2]]).rows

      assert [[["1"]]] = Ch.query!(conn, "SELECT {$0:Array(String)}", [["1"]]).rows
      assert [[[true]]] = Ch.query!(conn, "SELECT {$0:Array(Bool)}", [[true]]).rows

      assert [[[~D[2023-01-01]]]] =
               Ch.query!(conn, "SELECT {$0:Array(Date)}", [[~D[2023-01-01]]]).rows

      assert [[[Ch.Test.to_clickhouse_naive(conn, ~N[2023-01-01 12:00:00])]]] ==
               Ch.query!(conn, "SELECT {$0:Array(DateTime)}", [[~N[2023-01-01 12:00:00]]]).rows

      assert [[[~U[2023-01-01 12:00:00Z]]]] ==
               Ch.query!(conn, "SELECT {$0:Array(DateTime('UTC'))}", [[~N[2023-01-01 12:00:00]]]).rows

      assert [[[~N[2023-01-01 12:00:00]]]] ==
               Ch.query!(conn, "SELECT {$0:Array(DateTime)}", [[~U[2023-01-01 12:00:00Z]]]).rows

      assert [[[~U[2023-01-01 12:00:00Z]]]] ==
               Ch.query!(conn, "SELECT {$0:Array(DateTime('UTC'))}", [[~U[2023-01-01 12:00:00Z]]]).rows

      assert [[[[0], [1]]]] =
               Ch.query!(conn, "SELECT {$0:Array(Array(integer))}", [[[0], [1]]]).rows

      assert [[[[0]]]] = Ch.query!(conn, "SELECT {$0:Array(Array(integer))}", [[[0]]]).rows
      # assert [[[1, nil, 3]]] = Ch.query!(conn, "SELECT {$0:Array(integer)}", [[1, nil, 3]]).rows
    end

    test "encode network types", %{conn: conn} do
      # TODO, or wrap in custom struct like in postgrex
      # assert [["127.0.0.1/32"]] =
      #          Ch.query!(conn, "SELECT {$0:inet4}::text", [{127, 0, 0, 1}]).rows

      assert [[{127, 0, 0, 1}]] = Ch.query!(conn, "SELECT {$0:text}::inet4", ["127.0.0.1"]).rows

      assert [[{0, 0, 0, 0, 0, 0, 0, 1}]] =
               Ch.query!(conn, "SELECT {$0:text}::inet6", ["::1"]).rows
    end

    test "result struct", %{conn: conn} do
      assert {:ok, res} = Ch.query(conn, "SELECT 123 AS a, 456 AS b")
      assert %Ch.Result{} = res
      assert res.command == :select
      assert res.columns == ["a", "b"]
      assert res.num_rows == 1
    end

    test "empty result struct", %{conn: conn} do
      assert %Ch.Result{} = res = Ch.query!(conn, "select number, 'a' as b from numbers(0)")
      assert res.command == :select
      assert res.columns == ["number", "b"]
      assert res.rows == []
      assert res.num_rows == 0
    end

    test "error struct", %{conn: conn} do
      assert {:error, %Ch.Error{}} = Ch.query(conn, "SELECT 123 + 'a'")
    end

    test "error code", %{conn: conn} do
      assert {:error, %Ch.Error{code: 62}} = Ch.query(conn, "wat")
    end

    test "connection works after failure in execute", %{conn: conn} do
      assert {:error, %Ch.Error{}} = Ch.query(conn, "wat")
      assert [[42]] = Ch.query!(conn, "SELECT 42").rows
    end

    test "async test", %{conn: conn} do
      self_pid = self()

      Enum.each(1..10, fn _ ->
        spawn_link(fn ->
          send(self_pid, Ch.query!(conn, "SELECT sleep(0.05)").rows)
        end)
      end)

      assert [[42]] = Ch.query!(conn, "SELECT 42").rows

      Enum.each(1..10, fn _ ->
        assert_receive [[0]]
      end)
    end

    test "query struct interpolates to statement" do
      assert "#{%Ch.Query{statement: "SELECT 1"}}" == "SELECT 1"
    end
  end

  test "query before and after idle ping" do
    opts = [backoff_type: :stop, idle_interval: 1]
    {:ok, pid} = Ch.start_link(opts)
    assert {:ok, _} = Ch.query(pid, "SELECT 42")
    :timer.sleep(20)
    assert {:ok, _} = Ch.query(pid, "SELECT 42")
    :timer.sleep(20)
    assert {:ok, _} = Ch.query(pid, "SELECT 42")
  end
end
