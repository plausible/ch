defmodule Ch.AggregationTest do
  use ExUnit.Case

  setup do
    conn = start_supervised!(Ch)

    create_table = """
    create table ch_candle_fragments (
      ticker LowCardinality(String),
      time DateTime('UTC') CODEC(Delta, Default),
      high Float64 CODEC(Delta, Default),
      open Float64 CODEC(Delta, Default),
      close Float64 CODEC(Delta, Default),
      low  Float64 CODEC(Delta, Default),
    ) ENGINE = MergeTree()
    ORDER BY (ticker, time)
    """

    Ch.query!(conn, create_table)
    on_exit(fn -> Ch.Test.sql_exec("drop table ch_candle_fragments") end)

    create_table = """
    CREATE MATERIALIZED VIEW ch_candles_one_hour_amt
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
    select
    t.ticker as ticker,
    toStartOfHour(t.time) as time,
    max(t.high) as high,
    argMinState(t.open, t.time) as open,
    argMaxState(t.close, t.time) as close,
    min(t.low) as low
    from ch_candle_fragments t
    group by ticker, time
    """

    Ch.query!(conn, create_table)
    on_exit(fn -> Ch.Test.sql_exec("drop table ch_candles_one_hour_amt") end)

    insert_query = """
      insert into ch_candle_fragments
        (ticker, time, high, open, close, low)
      VALUES
      -- 1681410780  UTC
      -- 1681410840 '2023-04-13 18:34:00' UTC
      -- 1681410900 '2023-04-13 18:35:00' UTC
      -- 1681410960 '2023-04-13 18:36:00' UTC
      ('INTC', '2023-04-13 20:33:00', 32, 32, 32, 32),
      ('INTC', '2023-04-13 20:34:00', 33, 33, 33, 33),
      ('INTC', '2023-04-13 20:35:00', 32, 32, 31, 26),
      ('INTC', '2023-04-13 20:36:00', 32, 27, 27, 27)
    """

    Ch.query!(conn, insert_query)

    {:ok, conn: conn}
  end

  test "can decode aggregation types", %{conn: conn} do
    query = """
    select
    t.ticker as ticker,
    toStartOfHour(t.time) as start_time,
    toStartOfHour(t.time) + interval 1 hour as end_time,
    toStartOfHour(t.time)::DATE as date,
    max(t.high) as high,
    argMinMerge(t.open) as open,
    argMaxMerge(t.close) as close,
    min(t.low) as low
    from ch_candles_one_hour_amt t
    group by ticker, time
    """

    %{rows: rows} = Ch.query!(conn, query)

    expected = [
      [
        "INTC",
        ~U[2023-04-13 20:00:00Z],
        ~U[2023-04-13 21:00:00Z],
        ~D[2023-04-13],
        33.0,
        32.0,
        27.0,
        26.0
      ]
    ]

    assert rows == expected
  end
end
