defmodule Ch.TimezoneTest do
  use ExUnit.Case,
    async: true,
    parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]

  use ExUnitProperties

  import Ch.Test, only: [parameterize_query!: 2, parameterize_query!: 3]

  test "session timezone is reported by ClickHouse", ctx do
    ctx = setup_conn(ctx, "Asia/Taipei")
    result = parameterize_query!(ctx, "SELECT timezone()")

    # The session setting changes both timezone() and the timezone response header, just like
    # configuring the ClickHouse server timezone does.
    assert result.rows == [["Asia/Taipei"]]

    assert List.keyfind!(result.headers, "x-clickhouse-timezone", 0) ==
             {"x-clickhouse-timezone", "Asia/Taipei"}
  end

  test "naive datetime params use the session timezone", ctx do
    ctx = setup_conn(ctx, "Europe/Berlin")

    # Naive datetimes are sent as text, so ClickHouse interprets them in the session timezone
    # before Ch decodes the underlying instant as a naive UTC datetime.
    assert parameterize_query!(
             ctx,
             "SELECT {naive:DateTime} AS d, toString(d)",
             %{"naive" => ~N[2022-01-01 12:00:00]}
           ).rows == [[~N[2022-01-01 11:00:00], "2022-01-01 12:00:00"]]

    # Cover the positional parameter path and verify that daylight-saving time is observed.
    assert parameterize_query!(
             ctx,
             "SELECT {$0:DateTime} AS d, toString(d)",
             [~N[2022-07-01 12:00:00]]
           ).rows == [[~N[2022-07-01 10:00:00], "2022-07-01 12:00:00"]]
  end

  test "naive datetime64 params use the session timezone", ctx do
    ctx = setup_conn(ctx, "Asia/Tokyo")

    # Preserve coverage for every supported DateTime64 precision from the former integration test.
    for precision <- 0..9 do
      assert [[datetime]] =
               parameterize_query!(
                 ctx,
                 "SELECT {$0:DateTime64(#{precision})}",
                 [~N[2022-01-01 12:00:00]]
               ).rows

      assert NaiveDateTime.compare(datetime, ~N[2022-01-01 03:00:00]) == :eq
    end

    # A fractional value also verifies that timezone conversion retains subsecond precision.
    assert parameterize_query!(
             ctx,
             "SELECT {$0:DateTime64(3)} AS d, toString(d)",
             [~N[2022-01-01 12:00:00.123]]
           ).rows == [[~N[2022-01-01 03:00:00.123], "2022-01-01 12:00:00.123"]]
  end

  property "naive datetime64 params round-trip through ClickHouse", ctx do
    ctx = setup_conn(ctx, "UTC")

    check all precision <- integer(0..9),
              datetime <- naive_datetime_gen() do
      assert parameterize_query!(
               ctx,
               "SELECT {$0:DateTime64(#{precision})}",
               [datetime]
             ).rows == [[truncate_naive_datetime(datetime, precision)]]
    end
  end

  test "array datetime params use the session timezone", ctx do
    ctx = setup_conn(ctx, "Asia/Kathmandu")

    # Composite params apply the same session conversion to each naive DateTime element.
    assert parameterize_query!(ctx, "SELECT {$0:Array(DateTime)}", [
             [~N[2022-01-01 12:00:00]]
           ]).rows == [[[~N[2022-01-01 06:15:00]]]]

    assert parameterize_query!(ctx, "SELECT {$0:Array(DateTime)}", [
             [~U[2022-01-01 12:00:00Z]]
           ]).rows == [[[~N[2022-01-01 12:00:00]]]]
  end

  @tag :dynamic
  test "dynamic datetimes use the session timezone", ctx do
    ctx = setup_conn(ctx, "America/Los_Angeles")

    # Dynamic preserves the unqualified datetime type and its session-relative instant.
    assert parameterize_query!(ctx, """
           SELECT
             '2022-01-01 12:00:00'::DateTime::Dynamic,
             '2022-01-01 12:00:00.123'::DateTime64(3)::Dynamic
           """).rows == [[~N[2022-01-01 20:00:00], ~N[2022-01-01 20:00:00.123]]]
  end

  test "UTC datetime params keep their instant and render in the session timezone", ctx do
    ctx = setup_conn(ctx, "Australia/Sydney")

    # UTC DateTimes are encoded as instants; the session timezone only affects their text rendering.
    assert parameterize_query!(
             ctx,
             "SELECT {$0:DateTime} AS d, toString(d), {$1:DateTime64(3)} AS d64, toString(d64)",
             [~U[2022-01-01 12:00:00Z], ~U[2022-01-01 12:00:00.123Z]]
           ).rows == [
             [
               ~N[2022-01-01 12:00:00],
               "2022-01-01 23:00:00",
               ~N[2022-01-01 12:00:00.123],
               "2022-01-01 23:00:00.123"
             ]
           ]
  end

  defp setup_conn(ctx, timezone) do
    conn =
      start_supervised!(
        {Ch, database: Ch.Test.database(), pool_size: 1, settings: [session_timezone: timezone]}
      )

    Map.put(ctx, :conn, conn)
  end

  defp naive_datetime_gen do
    gen all seconds <-
              one_of([
                member_of([-2_208_988_800, -2, -1, 0, 1, 4_102_444_800]),
                integer(-2_208_988_800..4_102_444_800)
              ]),
            microsecond <-
              one_of([
                member_of([0, 1, 999, 1_000, 999_999]),
                integer(0..999_999)
              ]) do
      seconds
      |> DateTime.from_unix!()
      |> DateTime.to_naive()
      |> NaiveDateTime.add(microsecond, :microsecond)
    end
  end

  defp truncate_naive_datetime(datetime, precision) do
    precision = min(precision, 6)
    {microsecond, _precision} = datetime.microsecond
    scale = Integer.pow(10, 6 - precision)
    %{datetime | microsecond: {div(microsecond, scale) * scale, precision}}
  end
end
