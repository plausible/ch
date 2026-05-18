defmodule Ch.BFloat16Test do
  use ExUnit.Case, async: true
  use ExUnitProperties

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

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "plain finite values", %{pool: pool} do
    check all value <- bounded_bfloat16() do
      assert Ch.query!(
               pool,
               "select #{Float.to_string(value)}::BFloat16",
               _no_params = %{}
             ).rows ==
               [[value]]
    end
  end

  property "finite params round-trip through ClickHouse casts", %{
    pool: pool
  } do
    check all value <- bounded_bfloat16() do
      assert Ch.query!(
               pool,
               "select {value:BFloat16} as value",
               %{"value" => value}
             ).rows ==
               [[value]]
    end
  end

  test "special values decode as nil", %{pool: pool} do
    assert Ch.query!(
             pool,
             "select 'nan'::BFloat16, 'inf'::BFloat16, '-inf'::BFloat16",
             %{}
           ).rows ==
             [[nil, nil, nil]]
  end

  test "RowBinary edge values round-trip through ClickHouse", %{
    pool: pool
  } do
    table = "bf16_edges"
    insert = "insert into bf16_edges (idx, bf16) format RowBinary"

    create_table(pool, table)

    on_exit(fn ->
      Help.query!("drop table if exists {table:Identifier}", %{"table" => table})
    end)

    values = Enum.map(@bf16_edges, &bfloat16_to_float/1)

    assert_rowbinary_round_trip(pool, table, insert, values)
  end

  property "finite RowBinary values round-trip through ClickHouse", %{
    pool: pool
  } do
    table = "bf16_finite"
    insert = "insert into bf16_finite (idx, bf16) format RowBinary"

    create_table(pool, table)

    on_exit(fn ->
      Help.query!("drop table if exists {table:Identifier}", %{"table" => table})
    end)

    check all bits <- list_of(finite_bfloat16_bits(), length: 20) do
      create_table(pool, table, if_not_exists: true)

      values = Enum.map(bits, &bfloat16_to_float/1)

      assert_rowbinary_round_trip(pool, table, insert, values)
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

  defp create_table(pool, table, opts \\ []) do
    exists = if opts[:if_not_exists], do: " if not exists", else: ""

    Ch.query!(
      pool,
      "create table#{exists} {table:Identifier} (idx UInt8, bf16 BFloat16) engine Memory",
      %{"table" => table}
    )
  end

  defp assert_rowbinary_round_trip(pool, table, insert, values) do
    Ch.query!(pool, "truncate table {table:Identifier}", %{"table" => table})

    rows =
      values
      |> Enum.with_index()
      |> Enum.map(fn {value, idx} -> [idx, value] end)

    rowbinary = Ch.RowBinary.encode_rows(rows, ["UInt8", "BFloat16"])

    assert %Ch.Result{names: nil, rows: nil, data: nil} =
             Ch.query!(pool, [insert, ?\n | rowbinary])

    assert Ch.query!(pool, "select bf16 from {table:Identifier} order by idx", %{"table" => table}).rows ==
             Enum.map(values, &[&1])
  end
end
