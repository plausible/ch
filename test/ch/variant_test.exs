defmodule Ch.VariantTest do
  use ExUnit.Case

  @moduletag :variant

  setup do
    conn = start_supervised!({Ch, database: Ch.Test.database()})
    {:ok, conn: conn}
  end

  test "it works", %{conn: conn} do
    assert Ch.query!(conn, "select [1]::Variant(UInt64, String, Array(UInt64))").rows == [[[1]]]
    assert Ch.query!(conn, "select 0::Variant(UInt64, String, Array(UInt64))").rows == [[0]]

    assert Ch.query!(conn, "select 'Hello, World!'::Variant(UInt64, String, Array(UInt64))").rows ==
             [["Hello, World!"]]
  end
end
