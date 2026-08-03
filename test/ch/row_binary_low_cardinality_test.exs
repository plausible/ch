defmodule Ch.RowBinaryLowCardinalityTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "LowCardinality params round-trip through ClickHouse", %{pool: pool} do
    check all {type, value, expected} <- low_cardinality_param() do
      assert Ch.query!(pool, "SELECT {value:LowCardinality(#{type})}", %{"value" => value}).rows ==
               [[expected]]
    end
  end

  property "RowBinary LowCardinality inserts use the nested encoding", %{pool: pool} do
    Ch.query!(
      pool,
      """
      CREATE TABLE row_binary_low_cardinality_property (
        id UInt64,
        string LowCardinality(String),
        fixed LowCardinality(FixedString(4)),
        int LowCardinality(UInt64)
      ) ENGINE Memory
      """,
      %{},
      settings: %{"allow_suspicious_low_cardinality_types" => 1}
    )

    on_exit(fn -> Help.query!("DROP TABLE row_binary_low_cardinality_property") end)

    check all rows <- rowbinary_low_cardinality_rows(), max_runs: 25 do
      Ch.query!(pool, "TRUNCATE TABLE row_binary_low_cardinality_property")

      types = [
        "UInt64",
        "LowCardinality(String)",
        "LowCardinality(FixedString(4))",
        "LowCardinality(UInt64)"
      ]

      rowbinary = RowBinary.encode_rows(rows, types)

      Ch.query!(pool, [
        "INSERT INTO row_binary_low_cardinality_property FORMAT RowBinary\n" | rowbinary
      ])

      expected =
        rows
        |> Enum.map(fn [id, string, fixed, int] ->
          [id, string, fixed <> :binary.copy(<<0>>, 4 - byte_size(fixed)), int]
        end)
        |> Enum.sort_by(&List.first/1)

      assert Ch.query!(pool, "SELECT * FROM row_binary_low_cardinality_property ORDER BY id").rows ==
               expected
    end
  end

  test "LowCardinality arrays round-trip through ClickHouse", %{pool: pool} do
    Ch.query!(
      pool,
      """
      CREATE TABLE row_binary_low_cardinality_representative (
        id UInt64,
        strings Array(LowCardinality(String)),
        ints Array(LowCardinality(UInt64))
      ) ENGINE Memory
      """,
      %{},
      settings: %{"allow_suspicious_low_cardinality_types" => 1}
    )

    on_exit(fn -> Help.query!("DROP TABLE row_binary_low_cardinality_representative") end)

    rows = [
      [0, [], []],
      [1, ["a", "b", "a"], [0, 1, 1, 2]]
    ]

    types = [
      "UInt64",
      "Array(LowCardinality(String))",
      "Array(LowCardinality(UInt64))"
    ]

    rowbinary = RowBinary.encode_rows(rows, types)

    Ch.query!(pool, [
      "INSERT INTO row_binary_low_cardinality_representative FORMAT RowBinary\n" | rowbinary
    ])

    assert Ch.query!(pool, "SELECT * FROM row_binary_low_cardinality_representative ORDER BY id").rows ==
             rows
  end

  defp low_cardinality_param do
    one_of([
      typed_param("String", safe_string(), & &1),
      typed_param("FixedString(4)", binary(max_length: 4), fn value ->
        value <> :binary.copy(<<0>>, 4 - byte_size(value))
      end),
      typed_param("UInt64", integer(0..9_007_199_254_740_991), & &1)
    ])
  end

  defp typed_param(type, generator, expected_fun) do
    gen all value <- generator do
      {type, value, expected_fun.(value)}
    end
  end

  defp rowbinary_low_cardinality_rows do
    gen all ids <- uniq_list_of(integer(0..18_446_744_073_709_551_615), max_length: 12),
            values <-
              list_of(
                fixed_list([
                  safe_string(),
                  binary(max_length: 4),
                  integer(0..9_007_199_254_740_991)
                ]),
                length: length(ids)
              ) do
      Enum.zip_with(ids, values, fn id, values -> [id | values] end)
    end
  end

  defp safe_string do
    string(:printable, max_length: 32)
  end
end
