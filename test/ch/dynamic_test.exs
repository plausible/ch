defmodule Ch.DynamicTest do
  use ExUnit.Case

  @moduletag :dynamic

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "it works", %{conn: conn} do
    assert Ch.query!(conn, "select 'Hello, World!'::Dynamic AS d, dynamicType(d)").rows == []
  end
end
