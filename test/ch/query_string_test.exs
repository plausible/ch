defmodule Ch.QueryStringTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "binaries are escaped properly", %{conn: conn} do
    assert Ch.query!(conn, "select {s:String}", %{"s" => "\\"}).rows == []
  end
end
