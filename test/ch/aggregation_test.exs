defmodule Ch.AggregationTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "select SimpleAggregateFunction types", %{pool: pool} do
    Help.query!("""
    CREATE TABLE candle_fragments (
      ticker LowCardinality(String),
      time DateTime('UTC') CODEC(Delta, Default),
      high Float64 CODEC(Delta, Default),
      open Float64 CODEC(Delta, Default),
      close Float64 CODEC(Delta, Default),
      low  Float64 CODEC(Delta, Default),
    ) ENGINE = MergeTree()
    ORDER BY (ticker, time)
    """)

    on_exit(fn -> Help.query!("drop table candle_fragments") end)

    Help.query!("""
    CREATE MATERIALIZED VIEW candles_one_hour_amt
    (
      ticker LowCardinality(String),
      time DateTime('UTC') CODEC(Delta, Default),
      high SimpleAggregateFunction(max, Float64) CODEC(Delta, Default),
      open AggregateFunction(argMin, Float64, DateTime('UTC')),
      close AggregateFunction(argMax , Float64, DateTime('UTC')),
      low SimpleAggregateFunction(min, Float64) CODEC(Delta, Default)
    )
    ENGINE = AggregatingMergeTree()
    ORDER BY (ticker, time)
    AS
    SELECT
      t.ticker AS ticker,
      toStartOfHour(t.time) AS time,
      max(t.high) AS high,
      argMinState(t.open, t.time) AS open,
      argMaxState(t.close, t.time) AS close,
      min(t.low) AS low
    FROM candle_fragments t
    GROUP BY ticker, time
    """)

    on_exit(fn -> Help.query!("drop view candles_one_hour_amt") end)

    Ch.query!(pool, """
    INSERT INTO candle_fragments(ticker, time, high, open, close, low) VALUES
    ('INTC', '2023-04-13 20:33:00', 32, 32, 32, 32),
    ('INTC', '2023-04-13 20:34:00', 33, 33, 33, 33),
    ('INTC', '2023-04-13 20:35:00', 32, 32, 31, 26),
    ('INTC', '2023-04-13 20:36:00', 32, 27, 27, 27)
    """)

    assert pool
           |> Ch.query!("""
           SELECT
             t.ticker AS ticker,
             toStartOfHour(t.time) AS start_time,
             toStartOfHour(t.time) + interval 1 hour AS end_time,
             toStartOfHour(t.time)::DATE AS date,
             max(t.high) AS high,
             argMinMerge(t.open) AS open,
             argMaxMerge(t.close) AS close,
             min(t.low) AS low
           FROM candles_one_hour_amt t
           GROUP BY ticker, time
           """)
           |> Help.to_maps() ==
             [
               %{
                 "ticker" => "INTC",
                 "start_time" => ~U[2023-04-13 20:00:00Z],
                 "end_time" => ~U[2023-04-13 21:00:00Z],
                 "date" => ~D[2023-04-13],
                 "high" => 33.0,
                 "open" => 32.0,
                 "close" => 27.0,
                 "low" => 26.0
               }
             ]
  end

  # based on https://github.com/ClickHouse/clickhouse-java/issues/1232
  test "insert AggregateFunction via input()", %{pool: pool} do
    Help.query!("""
    CREATE TABLE test_insert_aggregate_function (
      uid Int16,
      updated SimpleAggregateFunction(max, DateTime),
      name AggregateFunction(argMax, String, DateTime)
    ) ENGINE AggregatingMergeTree ORDER BY uid
    """)

    on_exit(fn -> Help.query!("drop table test_insert_aggregate_function") end)

    rows = [
      [1, ~N[2020-01-02 00:00:00], "b"],
      [1, ~N[2020-01-01 00:00:00], "a"]
    ]

    rowbinary = Ch.RowBinary.encode_rows(rows, _types = ["Int16", "DateTime", "String"])

    insert = """
    INSERT INTO test_insert_aggregate_function
      SELECT uid, updated, arrayReduce('argMaxState', [name], [updated])
      FROM input('uid Int16, updated DateTime, name String')
      FORMAT RowBinary
    """

    Ch.query!(pool, [insert | rowbinary])

    assert Ch.query!(pool, """
           SELECT uid, max(updated) AS updated, argMaxMerge(name)
           FROM test_insert_aggregate_function
           GROUP BY uid
           """).rows == [
             [1, ~N[2020-01-02 00:00:00], "b"]
           ]
  end

  # https://kb.altinity.com/altinity-kb-schema-design/ingestion-aggregate-function/
  describe "altinity examples" do
    setup do
      rows = [
        [1231, ~N[2020-01-02 00:00:00], "Jane"],
        [1231, ~N[2020-01-01 00:00:00], "John"]
      ]

      rowbinary = Ch.RowBinary.encode_rows(rows, ["Int16", "DateTime", "String"])
      {:ok, rowbinary: rowbinary}
    end

    test "ephemeral column", %{pool: pool, rowbinary: rowbinary} do
      Help.query!("""
      CREATE TABLE test_users_ephemeral_column (
        uid Int16,
        updated SimpleAggregateFunction(max, DateTime),
        name_stub String Ephemeral,
        name AggregateFunction(argMax, String, DateTime) DEFAULT arrayReduce('argMaxState', [name_stub], [updated])
      ) ENGINE AggregatingMergeTree ORDER BY uid
      """)

      on_exit(fn -> Help.query!("drop table test_users_ephemeral_column") end)

      Ch.query!(pool, [
        "INSERT INTO test_users_ephemeral_column(uid, updated, name_stub) FORMAT RowBinary\n"
        | rowbinary
      ])

      assert Ch.query!(pool, """
             SELECT uid, max(updated) AS updated, argMaxMerge(name)
             FROM test_users_ephemeral_column
             GROUP BY uid
             """).rows == [
               [1231, ~N[2020-01-02 00:00:00], "Jane"]
             ]
    end

    test "input function", %{pool: pool, rowbinary: rowbinary} do
      Help.query!("""
      CREATE TABLE test_users_input_function (
        uid Int16,
        updated SimpleAggregateFunction(max, DateTime),
        name AggregateFunction(argMax, String, DateTime)
      ) ENGINE AggregatingMergeTree ORDER BY uid
      """)

      on_exit(fn -> Help.query!("drop table test_users_input_function") end)

      Ch.query!(pool, [
        """
        INSERT INTO test_users_input_function
          SELECT uid, updated, arrayReduce('argMaxState', [name], [updated])
          FROM input('uid Int16, updated DateTime, name String') FORMAT RowBinary
        """
        | rowbinary
      ])

      assert Ch.query!(pool, """
             SELECT uid, max(updated) AS updated, argMaxMerge(name)
             FROM test_users_input_function
             GROUP BY uid
             """).rows == [
               [1231, ~N[2020-01-02 00:00:00], "Jane"]
             ]
    end

    test "materialized view and null engine", %{pool: pool, rowbinary: rowbinary} do
      Help.query!("""
      CREATE TABLE test_users_mv_ne (
        uid Int16,
        updated SimpleAggregateFunction(max, DateTime),
        name AggregateFunction(argMax, String, DateTime)
      ) ENGINE AggregatingMergeTree ORDER BY uid
      """)

      on_exit(fn -> Help.query!("drop table test_users_mv_ne") end)

      Help.query!("""
      CREATE TABLE test_users_ne (
        uid Int16,
        updated DateTime,
        name String
      ) ENGINE Null
      """)

      on_exit(fn -> Help.query!("drop table test_users_ne") end)

      Help.query!("""
      CREATE MATERIALIZED VIEW test_users_mv TO test_users_mv_ne AS
        SELECT uid, updated, arrayReduce('argMaxState', [name], [updated]) name
        FROM test_users_ne
      """)

      on_exit(fn -> Help.query!("drop view test_users_mv") end)

      Ch.query!(pool, ["INSERT INTO test_users_ne FORMAT RowBinary\n" | rowbinary])

      assert Ch.query!(pool, """
             SELECT uid, max(updated) AS updated, argMaxMerge(name)
             FROM test_users_mv_ne
             GROUP BY uid
             """).rows == [
               [1231, ~N[2020-01-02 00:00:00], "Jane"]
             ]
    end
  end
end
