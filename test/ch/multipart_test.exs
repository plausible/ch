defmodule Ch.MultipartTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "sends multipart", %{conn: conn} do
    assert Ch.query!(conn, "SELECT {a:String}, {b:String}", %{"a" => "A", "b" => "B"},
             multipart: true
           ).rows == [["A", "B"]]
  end
end
