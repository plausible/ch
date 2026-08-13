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

  test "numeric DateTime parameters retain their instant across ClickHouse timezones", %{
    conn: conn,
    query_options: query_options
  } do
    datetimes = [
      ~U[1969-12-31 23:59:58.500000Z],
      ~U[1969-12-31 23:59:58.999999Z],
      ~U[1970-01-01 00:00:00.000000Z],
      ~U[1970-01-01 00:00:00.000001Z],
      ~U[1970-01-01 00:00:00.1Z],
      ~U[1970-01-01 00:00:00.5Z],
      ~U[1970-01-01 00:00:00.999999Z],
      ~U[1970-01-01 00:00:01.000000Z],
      DateTime.shift_zone!(~U[1969-12-31 23:59:58.500000Z], "America/New_York"),
      DateTime.shift_zone!(~U[1970-01-01 00:00:00.500000Z], "Europe/Moscow")
    ]

    for datetime <- datetimes do
      expected = DateTime.to_unix(datetime, :microsecond)

      assert Ch.query!(
               conn,
               "select toUnixTimestamp64Micro({datetime:DateTime64(6, 'Europe/Moscow')})",
               %{"datetime" => datetime},
               query_options
             ).rows == [[expected]]
    end

    array_datetimes = Enum.take(datetimes, 6)
    expected_datetimes = Enum.map(array_datetimes, &DateTime.to_unix(&1, :microsecond))

    assert Ch.query!(
             conn,
             """
             select arrayMap(
               datetime -> toUnixTimestamp64Micro(datetime),
               {datetimes:Array(DateTime64(6, 'UTC'))}
             )
             """,
             %{"datetimes" => array_datetimes},
             query_options
           ).rows == [[expected_datetimes]]
  end

  # ClickHouse currently drops the sign for fractional Unix timestamps between -1 and 0:
  # https://github.com/ClickHouse/ClickHouse/issues/96745
  test "ClickHouse treats encoded negative fractional timestamps above -1 as positive", %{
    conn: conn,
    query_options: query_options
  } do
    cases = [
      {~U[1969-12-31 23:59:59.999999Z], 1},
      {~U[1969-12-31 23:59:59.500000Z], 500_000},
      {~U[1969-12-31 23:59:59.000001Z], 999_999}
    ]

    for {datetime, parsed_unix} <- cases do
      assert Ch.query!(
               conn,
               "select toUnixTimestamp64Micro({datetime:DateTime64(6, 'UTC')})",
               %{"datetime" => datetime},
               query_options
             ).rows == [[parsed_unix]]
    end

    datetimes = Enum.map(cases, &elem(&1, 0))
    parsed_unix = Enum.map(cases, &elem(&1, 1))

    assert Ch.query!(
             conn,
             """
             select arrayMap(
               datetime -> toUnixTimestamp64Micro(datetime),
               {datetimes:Array(DateTime64(6, 'UTC'))}
             )
             """,
             %{"datetimes" => datetimes},
             query_options
           ).rows == [[parsed_unix]]
  end
end
