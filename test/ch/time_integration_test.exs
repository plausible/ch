defmodule Ch.TimeIntegrationTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag :time

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "Time params round-trip through ClickHouse at second precision", %{pool: pool} do
    check all time <- time_second_gen() do
      assert Ch.query!(pool, "SELECT {value:Time}", %{"value" => time}).rows ==
               [[time]]
    end
  end

  property "Time64 params round-trip through ClickHouse at their declared precision", %{
    pool: pool
  } do
    check all precision <- integer(0..6),
              time <- time_gen() do
      assert Ch.query!(pool, "SELECT {value:Time64(#{precision})}", %{"value" => time}).rows ==
               [[truncate_time(time, precision)]]
    end
  end

  test "Time and Time64 can be inserted with RowBinary and selected", %{pool: pool} do
    Help.query!("""
    CREATE TABLE time_integration_rowbinary(
      t Time,
      t64_0 Time64(0),
      t64_3 Time64(3),
      t64_6 Time64(6)
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE time_integration_rowbinary") end)

    rows = [
      [~T[00:00:00.987654], ~T[00:00:00.987654], ~T[00:00:00.987654], ~T[00:00:00.987654]],
      [~T[12:34:56.987654], ~T[12:34:56.987654], ~T[12:34:56.987654], ~T[12:34:56.987654]],
      [~T[23:59:59.999999], ~T[23:59:59.999999], ~T[23:59:59.999999], ~T[23:59:59.999999]]
    ]

    types = ["Time", "Time64(0)", "Time64(3)", "Time64(6)"]
    rowbinary = Ch.RowBinary.encode_rows(rows, types)

    Ch.query!(pool, ["INSERT INTO time_integration_rowbinary FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM time_integration_rowbinary ORDER BY t").rows == [
             [~T[00:00:00], ~T[00:00:00], ~T[00:00:00.987], ~T[00:00:00.987654]],
             [~T[12:34:56], ~T[12:34:56], ~T[12:34:56.987], ~T[12:34:56.987654]],
             [~T[23:59:59], ~T[23:59:59], ~T[23:59:59.999], ~T[23:59:59.999999]]
           ]
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

  defp truncate_time(time, precision) do
    {microsecond, _} = time.microsecond

    microsecond =
      if precision == 6 do
        microsecond
      else
        scale = Integer.pow(10, 6 - precision)
        div(microsecond, scale) * scale
      end

    %{time | microsecond: {microsecond, precision}}
  end
end
