defmodule Ch.ConnectionIntegrationTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "runs concurrent queries", %{pool: pool} do
    parent = self()

    for _ <- 1..10 do
      spawn_link(fn -> send(parent, Ch.query!(pool, "SELECT sleep(0.05)").rows) end)
    end

    assert Ch.query!(pool, "SELECT 42").rows == [[42]]

    for _ <- 1..10 do
      assert_receive [[0]]
    end
  end

  test "identifier params can address tables", %{pool: pool} do
    Help.query!("CREATE TABLE connection_integration_identifier_params (a UInt8) ENGINE Memory")

    on_exit(fn ->
      Help.query!("DROP TABLE connection_integration_identifier_params")
    end)

    Ch.query!(pool, "INSERT INTO {table:Identifier} VALUES (1), (2)", %{
      "table" => "connection_integration_identifier_params"
    })

    assert Ch.query!(pool, "SELECT sum(a) FROM {table:Identifier}", %{
             "table" => "connection_integration_identifier_params"
           }).rows == [[3]]
  end

  test "supports RowBinaryWithNamesAndTypes payloads", %{pool: pool} do
    Help.query!("""
    CREATE TABLE connection_integration_rowbinary_names_types (
      country_code FixedString(2),
      rare_string LowCardinality(String),
      maybe_int32 Nullable(Int32)
    ) ENGINE Memory
    """)

    on_exit(fn ->
      Help.query!("DROP TABLE connection_integration_rowbinary_names_types")
    end)

    names = ["country_code", "rare_string", "maybe_int32"]
    types = ["FixedString(2)", "LowCardinality(String)", "Nullable(Int32)"]
    rows = [["AB", "rare", -42], ["CD", "another", nil]]

    rowbinary = [
      Ch.RowBinary.encode_names_and_types(names, types)
      | Ch.RowBinary.encode_rows(rows, types)
    ]

    Ch.query!(pool, [
      "INSERT INTO connection_integration_rowbinary_names_types FORMAT RowBinaryWithNamesAndTypes\n"
      | rowbinary
    ])

    assert Ch.query!(
             pool,
             "SELECT * FROM connection_integration_rowbinary_names_types ORDER BY country_code"
           ).rows == rows
  end
end
