defmodule Ch.MultipartTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "sends multipart", %{conn: conn} do
    sql = "SELECT {a:String}, {b:String}"
    params = %{"a" => "A", "b" => "B"}

    assert %Ch.Result{rows: [["A", "B"]]} =
             Ch.query!(conn, sql, params, multipart: true)
  end

  test "sends positional parameters correctly", %{conn: conn} do
    sql = "SELECT {$0:String}, {$1:Int32}"
    params = ["pos0", 42]

    assert %Ch.Result{rows: [["pos0", 42]]} =
             Ch.query!(conn, sql, params, multipart: true)
  end
end
