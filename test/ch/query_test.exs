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

      assert Query.build("with insert as (select 1) select * from insert").command == :select
      assert Query.build("insert into table(a, b) values(1, 2)").command == :insert

      assert Query.build("insert into table(a, b) select b, c from table2 where b = 'update'").command ==
               :insert
    end

    test "with nil command provided" do
      assert Query.build("select 1+2", nil).command == :select
    end

    test "with command provided" do
      assert Query.build("select 1+2", :custom).command == :custom
    end

    @tag skip: true
    test "TODO" do
      assert Query.build("Select 1+2").command == :select
    end
  end

  # adapted from https://github.com/elixir-ecto/postgrex/blob/master/test/query_test.exs
  describe "query" do
    setup do
      {:ok, conn: start_supervised!(Ch)}
    end

    test "iodata", %{conn: conn} do
      assert [[123]] = Ch.query!(conn, ["S", ?E, ["LEC" | "T"], " ", '123']).rows
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

    test "decode arrays", %{conn: conn} do
      assert [[[]]] = Ch.query!(conn, "SELECT []").rows
      assert [[[1]]] = Ch.query!(conn, "SELECT [1]").rows
      assert [[[1, 2]]] = Ch.query!(conn, "SELECT [1,2]").rows
      assert [[[[0], [1]]]] = Ch.query!(conn, "SELECT [[0],[1]]").rows
      assert [[[[0]]]] = Ch.query!(conn, "SELECT [[0]]").rows
    end

    test "decode network types", %{conn: conn} do
      assert [[{127, 0, 0, 1} = ipv4]] = Ch.query!(conn, "SELECT '127.0.0.1'::inet4").rows
      assert :inet.ntoa(ipv4) == '127.0.0.1'

      assert [[{0, 0, 0, 0, 0, 0, 0, 1} = ipv6]] = Ch.query!(conn, "SELECT '::1'::inet6").rows
      assert :inet.ntoa(ipv6) == '::1'

      assert [[ipv6]] = Ch.query!(conn, "SELECT '2001:44c8:129:2632:33:0:252:2'::inet6").rows
      assert :inet.ntoa(ipv6) == '2001:44c8:129:2632:33:0:252:2'
    end
  end
end
