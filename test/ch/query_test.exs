defmodule Ch.QueryTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn: start_supervised!(Ch)}
  end

  test "iodata", %{conn: conn} do
    assert Ch.query!(conn, ["S", ?E, ["LEC" | "T"], " ", ~c"123"]).rows == [[123]]
  end

  test "decode basic types", %{conn: conn} do
    assert Ch.query!(conn, "SELECT NULL").rows == [[nil]]
    assert Ch.query!(conn, "SELECT true, false").rows == [[true, false]]
    assert Ch.query!(conn, "SELECT 'e'::char").rows == [["e"]]
    assert Ch.query!(conn, "SELECT 'ẽ'::char").rows == [["ẽ"]]
    assert Ch.query!(conn, "SELECT 42").rows == [[42]]
    assert Ch.query!(conn, "SELECT 42::float").rows == [[42.0]]
    assert Ch.query!(conn, "SELECT 42.0").rows == [[42.0]]
    assert Ch.query!(conn, "SELECT 'NaN'::float").rows == [[nil]]
    assert Ch.query!(conn, "SELECT 'inf'::float").rows == [[nil]]
    assert Ch.query!(conn, "SELECT '-inf'::float").rows == [[nil]]
    assert Ch.query!(conn, "SELECT 'ẽric'").rows == [["ẽric"]]
    assert Ch.query!(conn, "SELECT 'ẽric'::varchar").rows == [["ẽric"]]
  end

  test "encode positional params", %{conn: conn} do
    assert Ch.query!(conn, "SELECT {$0:String}", ["hello"]).rows == [["hello"]]
    assert Ch.query!(conn, "SELECT {$0:Bool}, {$1:Bool}", [true, false]).rows == [[true, false]]
    assert Ch.query!(conn, "SELECT {$0:Int64}", [42]).rows == [[42]]
    assert Ch.query!(conn, "SELECT {$0:Array(String)}", [["a", "b"]]).rows == [[["a", "b"]]]
  end

  test "result struct", %{conn: conn} do
    assert {:ok, %Ch.Result{} = res} = Ch.query(conn, "SELECT 123 AS a, 456 AS b")
    assert res.command == :select
    assert res.columns == ["a", "b"]
    assert res.rows == [[123, 456]]
    assert res.num_rows == 1
  end

  test "empty result struct", %{conn: conn} do
    assert %Ch.Result{} = res = Ch.query!(conn, "select number, 'a' as b from numbers(0)")
    assert res.command == :select
    assert res.columns == ["number", "b"]
    assert res.rows == []
    assert res.num_rows == 0
  end

  test "error struct and connection reuse after error", %{conn: conn} do
    assert {:error, %Ch.Error{}} = Ch.query(conn, "SELECT 123 + 'a'")
    assert {:error, %Ch.Error{code: 62}} = Ch.query(conn, "wat")
    assert Ch.query!(conn, "SELECT 42").rows == [[42]]
  end

  test "concurrent queries", %{conn: conn} do
    parent = self()

    for _ <- 1..10 do
      spawn_link(fn -> send(parent, Ch.query!(conn, "SELECT sleep(0.05)").rows) end)
    end

    assert Ch.query!(conn, "SELECT 42").rows == [[42]]

    for _ <- 1..10 do
      assert_receive [[0]], 1_000
    end
  end
end
