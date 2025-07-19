defmodule Ch.DynamicTest do
  use ExUnit.Case

  @moduletag :dynamic

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "it works", %{conn: conn} do
    assert Ch.query!(conn, "select 'Hello, World!'::Dynamic AS d, dynamicType(d)").rows == [
             ["Hello, World!", "String"]
           ]

    assert Ch.query!(conn, "select 0::Dynamic AS d, dynamicType(d)").rows == [
             ["0", "String"]
           ]

    assert Ch.query!(conn, "select true::Dynamic AS d, dynamicType(d)").rows == [
             [true, "Bool"]
           ]

    assert Ch.query!(conn, "select (1+1)::Dynamic AS d, dynamicType(d)").rows == [
             [2, "UInt16"]
           ]
  end
end
