defmodule Ch.QueryStringTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn: start_supervised!(Ch)}
  end

  test "binaries are escaped properly", %{conn: conn} do
    for s <- ["\t", "\n", "\\", "'", "\b", "\f", "\r", "\0"] do
      assert Ch.query!(conn, "select {s:String}", %{"s" => s}).rows == [[s]]
    end

    assert Ch.query!(conn, "select splitByChar('\t', 'abc\t123')").rows == [[["abc", "123"]]]

    assert Ch.query!(conn, "select splitByChar('\t', {arg1:String})", %{
             "arg1" => "abc\t123"
           }).rows == [[["abc", "123"]]]
  end
end
