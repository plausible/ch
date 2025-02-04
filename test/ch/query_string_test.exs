defmodule Ch.QueryStringTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn: start_supervised!({Ch, Ch.Test.client_opts()})}
  end

  # For more info see
  # https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters
  # "escaped" format is the same as https://clickhouse.com/docs/en/interfaces/formats#tabseparated-data-formatting
  test "binaries are escaped properly", %{conn: conn} do
    for s <- ["\t", "\n", "\\", "'", "\b", "\f", "\r", "\0"] do
      assert Ch.query!(conn, "select {s:String}", %{"s" => s}).rows == [[s]]
    end

    # example from https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters
    assert Ch.query!(conn, "select splitByChar('\t', 'abc\t123')").rows ==
             [[["abc", "123"]]]

    assert Ch.query!(conn, "select splitByChar('\t', {arg1:String})", %{"arg1" => "abc\t123"}).rows ==
             [[["abc", "123"]]]
  end
end
