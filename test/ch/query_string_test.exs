defmodule Ch.QueryStringTest do
  use ExUnit.Case,
    async: true,
    parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]

  setup ctx do
    {:ok, query_options: ctx[:query_options] || []}
  end

  setup do
    {:ok, conn: start_supervised!(Ch)}
  end

  # For more info see
  # https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters
  # "escaped" format is the same as https://clickhouse.com/docs/en/interfaces/formats#tabseparated-data-formatting
  test "binaries are escaped properly", %{conn: conn, query_options: query_options} do
    for s <- ["\t", "\n", "\\", "'", "\b", "\f", "\r", "\0"] do
      assert Ch.query!(conn, "select {s:String}", %{"s" => s}, query_options).rows == [[s]]
    end

    # example from https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters
    assert Ch.query!(conn, "select splitByChar('\t', 'abc\t123')", [], query_options).rows ==
             [[["abc", "123"]]]

    assert Ch.query!(
             conn,
             "select splitByChar('\t', {arg1:String})",
             %{"arg1" => "abc\t123"},
             query_options
           ).rows ==
             [[["abc", "123"]]]
  end
end
