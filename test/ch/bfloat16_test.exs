defmodule Ch.BFloat16Test do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]
  use ExUnitProperties

  import Ch.RowBinary
  import Bitwise

  @moduletag :bf16

  @bf16_edges [
    0x0000,
    0x8000,
    0x0001,
    0x8001,
    0x007F,
    0x807F,
    0x0080,
    0x8080,
    0x3F80,
    0xBF80,
    0x3FE0,
    0x7F7F,
    0xFF7F
  ]

  setup ctx do
    {:ok,
     query_options: ctx[:query_options] || [],
     conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  property "plain finite values", %{conn: conn, query_options: query_options} do
    check all value <- bounded_bfloat16() do
      assert Ch.query!(
               conn,
               "select #{Float.to_string(value)}::BFloat16",
               _no_params = %{},
               query_options
             ).rows ==
               [[value]]
    end
  end

  property "finite params round-trip through ClickHouse casts", %{
    conn: conn,
    query_options: query_options
  } do
    check all value <- bounded_bfloat16() do
      assert Ch.query!(
               conn,
               "select {value:BFloat16} as value",
               %{"value" => value},
               query_options
             ).rows ==
               [[value]]
    end
  end

  test "special values decode as nil", %{conn: conn, query_options: query_options} do
    assert Ch.query!(
             conn,
             "select 'nan'::BFloat16, 'inf'::BFloat16, '-inf'::BFloat16",
             [],
             query_options
           ).rows ==
             [[nil, nil, nil]]
  end

  property "RowBinary encodes finite values as their BFloat16 bits" do
    check all bits <- finite_bfloat16_bits() do
      value = bfloat16_to_float(bits)

      assert encode(:bf16, value) == <<bits::16-little>>
    end
  end

  property "RowBinary decodes finite BFloat16 bit patterns" do
    check all bits <- finite_bfloat16_bits() do
      assert decode_rows(<<bits::16-little>>, [:bf16]) == [[bfloat16_to_float(bits)]]
    end
  end

  property "RowBinary decodes non-finite BFloat16 bit patterns as nil" do
    check all bits <- non_finite_bfloat16_bits() do
      assert decode_rows(<<bits::16-little>>, [:bf16]) == [[nil]]
    end
  end

  test "RowBinary covers BFloat16 edge bit patterns" do
    for bits <- @bf16_edges do
      value = bfloat16_to_float(bits)

      assert encode(:bf16, value) == <<bits::16-little>>
      assert decode_rows(<<bits::16-little>>, [:bf16]) == [[value]]
    end
  end

  property "finite RowBinary values round-trip through ClickHouse", %{
    conn: conn,
    query_options: query_options
  } do
    table = "bf16_#{System.unique_integer([:positive])}"

    Ch.query!(conn, "create table #{table} (idx UInt8, bf16 BFloat16) engine Memory")
    on_exit(fn -> Ch.Test.query("drop table if exists #{table}") end)

    query_options = Keyword.merge(query_options, types: ["UInt8", "BFloat16"])

    check all bits <- list_of(finite_bfloat16_bits(), length: 20) do
      Ch.query!(
        conn,
        "create table if not exists #{table} (idx UInt8, bf16 BFloat16) engine Memory"
      )

      Ch.query!(conn, "truncate table #{table}")

      values = Enum.map(bits, &bfloat16_to_float/1)

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

  defp bounded_bfloat16 do
    integer(-1_000_000..1_000_000)
    |> map(&(&1 / 16))
    |> map(&float_to_bfloat16/1)
    |> map(&bfloat16_to_float/1)
  end

  defp finite_bfloat16_bits do
    gen all sign <- integer(0..1),
            exponent <- integer(0..0xFE),
            fraction <- integer(0..0x7F) do
      sign <<< 15 ||| exponent <<< 7 ||| fraction
    end
  end

  defp non_finite_bfloat16_bits do
    gen all sign <- integer(0..1),
            fraction <- integer(0..0x7F) do
      sign <<< 15 ||| 0x7F80 ||| fraction
    end
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
