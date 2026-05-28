defmodule Ch.RowBinaryAggregateWrapperTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "SimpleAggregateFunction values use the nested RowBinary encoding", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_simple_aggregate_property (
      id UInt64,
      max_int SimpleAggregateFunction(max, UInt64),
      min_string SimpleAggregateFunction(min, String),
      max_date SimpleAggregateFunction(max, Date)
    ) ENGINE AggregatingMergeTree ORDER BY id
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_simple_aggregate_property") end)

    check all rows <- simple_aggregate_rows(), max_runs: 20 do
      Ch.query!(pool, "TRUNCATE TABLE row_binary_simple_aggregate_property")

      types = [
        "UInt64",
        "SimpleAggregateFunction(max, UInt64)",
        "SimpleAggregateFunction(min, String)",
        "SimpleAggregateFunction(max, Date)"
      ]

      rowbinary = RowBinary.encode_rows(rows, types)

      Ch.query!(pool, [
        "INSERT INTO row_binary_simple_aggregate_property FORMAT RowBinary\n" | rowbinary
      ])

      assert Ch.query!(pool, "SELECT * FROM row_binary_simple_aggregate_property ORDER BY id").rows ==
               Enum.sort_by(rows, &List.first/1)
    end
  end

  test "AggregateFunction states can be populated from RowBinary input values", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_aggregate_function_values (
      id UInt64,
      updated SimpleAggregateFunction(max, DateTime),
      name AggregateFunction(argMax, String, DateTime)
    ) ENGINE AggregatingMergeTree ORDER BY id
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_aggregate_function_values") end)

    rows = [
      [1, ~N[2024-01-01 00:00:00], "old"],
      [1, ~N[2024-01-02 00:00:00], "new"],
      [2, ~N[2024-01-01 00:00:00], "only"]
    ]

    rowbinary = RowBinary.encode_rows(rows, ["UInt64", "DateTime", "String"])

    Ch.query!(pool, [
      """
      INSERT INTO row_binary_aggregate_function_values
        SELECT id, updated, arrayReduce('argMaxState', [name], [updated])
        FROM input('id UInt64, updated DateTime, name String')
        FORMAT RowBinary
      """
      | rowbinary
    ])

    assert Ch.query!(
             pool,
             """
             SELECT id, max(updated), argMaxMerge(name)
             FROM row_binary_aggregate_function_values
             GROUP BY id
             ORDER BY id
             """
           ).rows == [
             [1, ~N[2024-01-02 00:00:00], "new"],
             [2, ~N[2024-01-01 00:00:00], "only"]
           ]
  end

  defp simple_aggregate_rows do
    gen all ids <- uniq_list_of(integer(0..18_446_744_073_709_551_615), max_length: 12),
            values <-
              list_of(
                fixed_list([
                  integer(0..18_446_744_073_709_551_615),
                  safe_string(),
                  date_gen()
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

  defp safe_string do
    string(:printable, max_length: 32)
  end
end
