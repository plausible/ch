defmodule Ch.QueryStringTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn: start_supervised!(Ch)}
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

  test "decimal params are bounded" do
    query = Ch.Query.build("select {d:Decimal(76, 0)}")

    {query_params, _headers, body} =
      DBConnection.Query.encode(query, %{"d" => Decimal.new("1e1000000")}, [])

    encoded =
      case query_params do
        [{"param_d", value}] -> value
        [] -> IO.iodata_to_binary(body)
      end

    assert encoded =~ "1E+1000000"
    assert byte_size(encoded) < 300
  end
end
