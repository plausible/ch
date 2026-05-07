defmodule Ch.BFloat16Test do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]
  use ExUnitProperties

  import Bitwise

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

  property "send and read back via rowbinary", %{conn: conn, query_options: query_options} do
    table = "bf16_#{System.unique_integer([:positive])}"

    Ch.query!(conn, "create table #{table} (idx UInt8, bf16 BFloat16) engine Memory")
    on_exit(fn -> Ch.Test.query("drop table if exists #{table}") end)

    query_options = Keyword.merge(query_options, types: ["UInt8", "BFloat16"])

    check all values <- list_of(bfloat16(), length: 20), max_runs: 10 do
      Ch.query!(conn, "truncate table #{table}")

      rows =
        values
        |> Enum.with_index()
        |> Enum.map(fn {value, idx} -> [idx, value] end)

      assert %{num_rows: 20} =
               Ch.query!(
                 conn,
                 "insert into #{table} (idx, bf16) format RowBinary",
                 rows,
                 query_options
               )

      assert Ch.query!(conn, "select bf16 from #{table} order by idx").rows ==
               Enum.map(values, &[&1])
    end
  end

  defp bfloat16 do
    integer(0..0xFFFF)
    |> filter(fn bits -> (bits &&& 0x7F80) != 0x7F80 end)
    |> map(&bfloat16_to_float/1)
  end

  defp bfloat16_to_float(bits) do
    <<float::32-float>> = <<bits::16, 0::16>>
    float
  end
end
