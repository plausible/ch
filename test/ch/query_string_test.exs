defmodule Ch.QueryStringTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  # For more info see
  # https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters
  # "escaped" format is the same as https://clickhouse.com/docs/en/interfaces/formats#tabseparated-data-formatting
  test "string parameters are escaped", %{pool: pool} do
    for s <- ["\t", "\n", "\\", "'", "\b", "\f", "\r", "\0"] do
      assert Ch.query!(pool, "select {s:String}", %{"s" => s}).rows == [[s]]
    end

    # example from https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters
    assert Ch.query!(pool, "select splitByChar('\t', 'abc\t123')").rows ==
             [[["abc", "123"]]]

    assert Ch.query!(pool, "select splitByChar('\t', {arg1:String})", %{"arg1" => "abc\t123"}).rows ==
             [[["abc", "123"]]]
  end
end
