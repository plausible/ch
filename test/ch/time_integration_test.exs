defmodule Ch.TimeIntegrationTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag :time

  setup do
    {:ok, pool: start_supervised!({Ch, database: Ch.Test.database()})}
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
    check all precision <- integer(0..9),
              time <- time_gen() do
      assert Ch.query!(pool, "SELECT {value:Time64(#{precision})}", %{"value" => time}).rows ==
               [[truncate_time(time, precision)]]
    end
  end

  test "Time and Time64 can be inserted with RowBinary and selected", %{pool: pool} do
    Ch.Test.query("""
    CREATE TABLE time_integration_rowbinary(
      t Time,
      t64_0 Time64(0),
      t64_3 Time64(3),
      t64_6 Time64(6)
    ) ENGINE Memory
    """)

    on_exit(fn -> Ch.Test.query("DROP TABLE time_integration_rowbinary") end)

    rows = [
      [~T[00:00:00.987654], ~T[00:00:00.987654], ~T[00:00:00.987654], ~T[00:00:00.987654]],
      [~T[12:34:56.987654], ~T[12:34:56.987654], ~T[12:34:56.987654], ~T[12:34:56.987654]],
      [~T[23:59:59.999999], ~T[23:59:59.999999], ~T[23:59:59.999999], ~T[23:59:59.999999]]
    ]

    types = ["Time", "Time64(0)", "Time64(3)", "Time64(6)"]
    rowbinary = Ch.RowBinary.encode_rows(rows, types)

    Ch.query!(pool, "INSERT INTO time_integration_rowbinary FORMAT RowBinary", rowbinary,
      encode: false
    )

    assert Ch.query!(pool, "SELECT * FROM time_integration_rowbinary ORDER BY t").rows == [
             [~T[00:00:00], ~T[00:00:00], ~T[00:00:00.987], ~T[00:00:00.987654]],
             [~T[12:34:56], ~T[12:34:56], ~T[12:34:56.987], ~T[12:34:56.987654]],
             [~T[23:59:59], ~T[23:59:59], ~T[23:59:59.999], ~T[23:59:59.999999]]
           ]
  end

  test "Ecto-cast Time and Time64 values retain their declared precision when inserted", %{
    pool: pool
  } do
    Ch.Test.query("""
    CREATE TABLE time_integration_ecto(
      t Time,
      t64_0 Time64(0),
      t64_3 Time64(3),
      t64_6 Time64(6),
      t64_9 Time64(9)
    ) ENGINE Memory
    """)

    on_exit(fn -> Ch.Test.query("DROP TABLE time_integration_ecto") end)

    types = ["Time", "Time64(0)", "Time64(3)", "Time64(6)", "Time64(9)"]

    rows =
      for input <- [~T[12:34:56.987654], "12:34:57.123456"] do
        Enum.map(types, fn type ->
          ecto_type = Ecto.ParameterizedType.init(Ch, type: type)
          assert {:ok, time} = Ecto.Type.cast(ecto_type, input)
          time
        end)
      end

    Ch.query!(pool, "INSERT INTO time_integration_ecto FORMAT RowBinary", rows, types: types)

    query = "SELECT * FROM time_integration_ecto ORDER BY t"

    assert Ch.query!(pool, query).rows == [
             [
               ~T[12:34:56],
               ~T[12:34:56],
               ~T[12:34:56.987],
               ~T[12:34:56.987654],
               ~T[12:34:56.987654]
             ],
             [
               ~T[12:34:57],
               ~T[12:34:57],
               ~T[12:34:57.123],
               ~T[12:34:57.123456],
               ~T[12:34:57.123456]
             ]
           ]

    result = Ch.query!(pool, query, [], format: "RowBinary", decode: false)

    # Read the stored ticks as integers so decoding Time64(9) cannot hide lost precision.
    assert result.data
           |> IO.iodata_to_binary()
           |> Ch.RowBinary.decode_rows(["Int32", "Int64", "Int64", "Int64", "Int64"]) == [
             [45_296, 45_296, 45_296_987, 45_296_987_654, 45_296_987_654_000],
             [45_297, 45_297, 45_297_123, 45_297_123_456, 45_297_123_456_000]
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
    precision = min(precision, 6)
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
