defmodule Ch.RowBinaryDateTimeTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "Date and Date32 params round-trip through ClickHouse", %{pool: pool} do
    check all {type, value} <- date_param() do
      assert Ch.query!(pool, "SELECT {value:#{type}}", %{"value" => value}).rows == [[value]]
    end
  end

  property "DateTime and DateTime64 params round-trip through ClickHouse", %{pool: pool} do
    check all {type, value, expected} <- datetime_param() do
      assert Ch.query!(pool, "SELECT {value:#{type}}", %{"value" => value}).rows == [[expected]]
    end
  end

  @tag :time
  property "Time and Time64 params round-trip through ClickHouse", %{pool: pool} do
    check all {type, value, expected} <- time_param() do
      assert Ch.query!(pool, "SELECT {value:#{type}}", %{"value" => value}).rows == [[expected]]
    end
  end

  test "query params cover nullable, arrays, and deterministic date/time cases", %{pool: pool} do
    assert Ch.query!(
             pool,
             """
             SELECT
               {date:Date},
               {date32:Date32},
               {datetime:DateTime('UTC')},
               {datetime64:DateTime64(6, 'UTC')},
               {nullable:Nullable(Date)},
               {dates:Array(Date)}
             """,
             %{
               "date" => ~D[2024-02-29],
               "date32" => ~D[1960-01-01],
               "datetime" => ~U[2024-01-02 03:04:05Z],
               "datetime64" => ~U[2024-01-02 03:04:05.123456Z],
               "nullable" => nil,
               "dates" => [~D[1970-01-01], ~D[2024-02-29]]
             }
           ).rows == [
             [
               ~D[2024-02-29],
               ~D[1960-01-01],
               ~U[2024-01-02 03:04:05Z],
               ~U[2024-01-02 03:04:05.123456Z],
               nil,
               [~D[1970-01-01], ~D[2024-02-29]]
             ]
           ]
  end

  @tag :time
  test "query params cover deterministic Time and Time64 cases", %{pool: pool} do
    assert Ch.query!(
             pool,
             """
             SELECT
              {time:Time},
               {time64_0:Time64(0)},
               {time64_3:Time64(3)},
               {time64_6:Time64(6)},
               {times:Array(Time64(3))}
             """,
             %{
               "time" => ~T[12:34:56],
               "time64_0" => ~T[12:34:56.987654],
               "time64_3" => ~T[12:34:56.987654],
               "time64_6" => ~T[12:34:56.987654],
               "times" => [~T[00:00:00.987654], ~T[23:59:59.999999]]
             }
           ).rows == [
             [
               ~T[12:34:56],
               ~T[12:34:56],
               ~T[12:34:56.987],
               ~T[12:34:56.987654],
               [~T[00:00:00.987], ~T[23:59:59.999]]
             ]
           ]
  end

  property "RowBinary Date and DateTime inserts round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_date_time_property (
      id UInt8,
      d Date,
      d32 Date32,
      dt DateTime('UTC'),
      dt64 DateTime64(6, 'UTC')
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_date_time_property") end)

    check all rows <- rowbinary_date_time_rows() do
      Ch.query!(pool, "TRUNCATE TABLE row_binary_date_time_property")

      rowbinary =
        RowBinary.encode_rows(
          rows,
          ["UInt8", "Date", "Date32", "DateTime('UTC')", "DateTime64(6, 'UTC')"]
        )

      Ch.query!(pool, ["INSERT INTO row_binary_date_time_property FORMAT RowBinary\n" | rowbinary])

      assert Ch.query!(pool, "SELECT * FROM row_binary_date_time_property ORDER BY id").rows ==
               Enum.sort_by(rows, &List.first/1)
    end
  end

  @tag :time
  property "RowBinary Time inserts round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_time_property (
      id UInt8,
      t Time,
      t64_0 Time64(0),
      t64_3 Time64(3),
      t64_6 Time64(6)
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_time_property") end)

    check all rows <- rowbinary_time_rows() do
      Ch.query!(pool, "TRUNCATE TABLE row_binary_time_property")

      rowbinary =
        RowBinary.encode_rows(rows, ["UInt8", "Time", "Time64(0)", "Time64(3)", "Time64(6)"])

      Ch.query!(pool, ["INSERT INTO row_binary_time_property FORMAT RowBinary\n" | rowbinary])

      expected =
        rows
        |> Enum.map(fn [id, t, t64_0, t64_3, t64_6] ->
          [id, truncate_time(t, 0), truncate_time(t64_0, 0), truncate_time(t64_3, 3), t64_6]
        end)
        |> Enum.sort_by(&List.first/1)

      assert Ch.query!(pool, "SELECT * FROM row_binary_time_property ORDER BY id").rows ==
               expected
    end
  end

  test "RowBinary inserts cover nullable, arrays, tuples, and defaults", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_date_time_representative (
      id UInt8,
      d Date,
      nullable_date Nullable(Date),
      dates Array(Date),
      dt DateTime('UTC'),
      dt64 DateTime64(6, 'UTC'),
      pair Tuple(Date, DateTime('UTC'))
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_date_time_representative") end)

    rows = [
      [
        1,
        ~D[1970-01-01],
        nil,
        [],
        ~U[1970-01-01 00:00:00Z],
        ~U[1970-01-01 00:00:00.000001Z],
        {~D[2024-02-29], ~U[2024-01-02 03:04:05Z]}
      ],
      [
        2,
        nil,
        ~D[2024-02-29],
        [~D[1970-01-01], ~D[2024-02-29]],
        ~U[2024-01-02 03:04:05Z],
        ~U[2024-01-02 03:04:05.123456Z],
        {~D[1970-01-01], ~U[1970-01-01 00:00:00Z]}
      ]
    ]

    types = [
      "UInt8",
      "Date",
      "Nullable(Date)",
      "Array(Date)",
      "DateTime('UTC')",
      "DateTime64(6, 'UTC')",
      "Tuple(Date, DateTime('UTC'))"
    ]

    rowbinary = RowBinary.encode_rows(rows, types)

    Ch.query!(pool, [
      "INSERT INTO row_binary_date_time_representative FORMAT RowBinary\n" | rowbinary
    ])

    assert Ch.query!(pool, "SELECT * FROM row_binary_date_time_representative ORDER BY id").rows ==
             [
               [
                 1,
                 ~D[1970-01-01],
                 nil,
                 [],
                 ~U[1970-01-01 00:00:00Z],
                 ~U[1970-01-01 00:00:00.000001Z],
                 {~D[2024-02-29], ~U[2024-01-02 03:04:05Z]}
               ],
               [
                 2,
                 ~D[1970-01-01],
                 ~D[2024-02-29],
                 [~D[1970-01-01], ~D[2024-02-29]],
                 ~U[2024-01-02 03:04:05Z],
                 ~U[2024-01-02 03:04:05.123456Z],
                 {~D[1970-01-01], ~U[1970-01-01 00:00:00Z]}
               ]
             ]
  end

  test "RowBinary rejects invalid date/time values" do
    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([["2024-01-01"]], ["Date"])
    end

    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([["2024-01-01 00:00:00"]], ["DateTime('UTC')"])
    end
  end

  defp date_param do
    one_of([
      typed_value("Date", date_gen()),
      typed_value("Date32", date32_gen())
    ])
  end

  defp datetime_param do
    one_of([
      typed_datetime("DateTime('UTC')", utc_datetime(), 0),
      typed_datetime("DateTime64(0, 'UTC')", utc_datetime64(), 0),
      typed_datetime("DateTime64(3, 'UTC')", utc_datetime64(), 3),
      typed_datetime("DateTime64(6, 'UTC')", utc_datetime64(), 6)
    ])
  end

  defp time_param do
    one_of([
      typed_time("Time", time_second_gen(), 0),
      typed_time("Time64(0)", time_gen(), 0),
      typed_time("Time64(3)", time_gen(), 3),
      typed_time("Time64(6)", time_gen(), 6)
    ])
  end

  defp typed_value(type, generator) do
    gen all value <- generator do
      {type, value}
    end
  end

  defp typed_datetime(type, generator, precision) do
    gen all value <- generator do
      {type, value, truncate_datetime(value, precision)}
    end
  end

  defp typed_time(type, generator, precision) do
    gen all value <- generator do
      {type, value, truncate_time(value, precision)}
    end
  end

  defp rowbinary_date_time_rows do
    gen all ids <- uniq_list_of(integer(0..255), max_length: 12),
            values <-
              list_of(
                fixed_list([
                  date_gen(),
                  date32_gen(),
                  utc_datetime(),
                  utc_datetime64()
                ]),
                length: length(ids)
              ) do
      Enum.zip_with(ids, values, fn id, values -> [id | values] end)
    end
  end

  defp rowbinary_time_rows do
    gen all ids <- uniq_list_of(integer(0..255), max_length: 12),
            values <-
              list_of(
                fixed_list([
                  time_gen(),
                  time_gen(),
                  time_gen(),
                  time_gen()
                ]),
                length: length(ids)
              ) do
      Enum.zip_with(ids, values, fn id, values -> [id | values] end)
    end
  end

  defp date_gen do
    gen all days <- integer(0..20_000) do
      Date.add(~D[1970-01-01], days)
    end
  end

  defp date32_gen do
    gen all days <- integer(-25_567..120_529) do
      Date.add(~D[1970-01-01], days)
    end
  end

  defp utc_datetime do
    gen all date <- date_gen(),
            hour <- integer(0..23),
            minute <- integer(0..59),
            second <- integer(0..59) do
      DateTime.new!(date, Time.new!(hour, minute, second), "Etc/UTC")
    end
  end

  defp utc_datetime64 do
    gen all date <- date_gen(),
            hour <- integer(0..23),
            minute <- integer(0..59),
            second <- integer(0..59),
            microsecond <- integer(0..999_999) do
      DateTime.new!(date, Time.new!(hour, minute, second, {microsecond, 6}), "Etc/UTC")
    end
  end

  defp time_gen do
    gen all hour <- integer(0..23),
            minute <- integer(0..59),
            second <- integer(0..59),
            microsecond <- integer(0..999_999) do
      Time.new!(hour, minute, second, {microsecond, 6})
    end
  end

  defp time_second_gen do
    gen all hour <- integer(0..23),
            minute <- integer(0..59),
            second <- integer(0..59) do
      Time.new!(hour, minute, second)
    end
  end

  defp truncate_datetime(datetime, 6), do: datetime

  defp truncate_datetime(datetime, precision) do
    {microsecond, _} = datetime.microsecond
    scale = Integer.pow(10, 6 - precision)
    microsecond = div(microsecond, scale) * scale

    %{datetime | microsecond: {microsecond, precision}}
  end

  defp truncate_time(time, 6), do: time

  defp truncate_time(time, precision) do
    {microsecond, _} = time.microsecond
    scale = Integer.pow(10, 6 - precision)
    microsecond = div(microsecond, scale) * scale

    %{time | microsecond: {microsecond, precision}}
  end
end
