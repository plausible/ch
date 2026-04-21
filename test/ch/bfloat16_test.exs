defmodule Ch.BFloat16Test do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]

  @moduletag :bf16

  setup ctx do
    {:ok,
     query_options: ctx[:query_options] || [],
     conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "plain", %{conn: conn, query_options: query_options} do
    assert Ch.query!(conn, "select 1.75::BFloat16", _no_params = %{}, query_options).rows == [
             [1.75]
           ]
  end

  test "send and read back via params", %{conn: conn, query_options: query_options} do
    assert Ch.query!(conn, "select {value:BFloat16} as value", %{"value" => 1.75}, query_options).rows ==
             [[1.75]]
  end

  test "send and read back via rowbinary", %{conn: conn, query_options: query_options} do
    rows = [
      [1.75],
      [-1.75],
      [0]
    ]

    query_options = Keyword.merge(query_options, types: ["BFloat16"])

    assert Ch.query!(
             conn,
             "select bf16 from input('bf16 BFloat16') format RowBinary",
             rows,
             query_options
           ).rows == rows
  end
end
