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

  property "plain", %{conn: conn, query_options: query_options} do
    check all value <- bfloat16() do
      assert Ch.query!(
               conn,
               "select #{Float.to_string(value)}::BFloat16",
               _no_params = %{},
               query_options
             ).rows ==
               [[value]]
    end
  end

  property "send and read back via params", %{conn: conn, query_options: query_options} do
    check all value <- bfloat16() do
      assert Ch.query!(
               conn,
               "select {value:BFloat16} as value",
               %{"value" => value},
               query_options
             ).rows ==
               [[value]]
    end
  end

  property "send and read back via rowbinary", %{conn: conn, query_options: query_options} do
    table = "bf16_#{System.unique_integer([:positive])}"

    Ch.query!(conn, "create table #{table} (idx UInt8, bf16 BFloat16) engine Memory")
    on_exit(fn -> Ch.Test.query("drop table if exists #{table}") end)

    query_options = Keyword.merge(query_options, types: ["UInt8", "BFloat16"])

    check all values <- list_of(bfloat16(), length: 20) do
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
    integer(-1_000_000..1_000_000)
    |> map(&(&1 / 16))
    |> map(&float_to_bfloat16/1)
    |> map(&bfloat16_to_float/1)
  end

  defp float_to_bfloat16(float) do
    <<bits::32>> = <<float::32-float>>

    upper = bits >>> 16
    lower = bits &&& 0xFFFF

    if lower > 0x8000 or (lower == 0x8000 and (upper &&& 1) == 1) do
      upper + 1
    else
      upper
    end
  end

  defp bfloat16_to_float(bits) do
    <<float::32-float>> = <<bits::16, 0::16>>
    float
  end
end
