defmodule Ch.VariantTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  # https://clickhouse.com/docs/sql-reference/data-types/variant

  @moduletag :variant

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "basic", %{pool: pool} do
    assert Ch.query!(pool, "select null::Variant(UInt64, String, Array(UInt64))").rows ==
             [[nil]]

    assert Ch.query!(pool, "select [1]::Variant(UInt64, String, Array(UInt64))").rows ==
             [[[1]]]

    assert Ch.query!(pool, "select 0::Variant(UInt64, String, Array(UInt64))").rows ==
             [[0]]

    assert Ch.query!(pool, "select 'Hello, World!'::Variant(UInt64, String, Array(UInt64))").rows ==
             [["Hello, World!"]]
  end

  # https://github.com/plausible/ch/issues/272
  test "ordering internal types", %{pool: pool} do
    test = %{
      "'hello'" => "hello",
      "-10" => -10,
      "true" => true,
      "map('hello', null::Nullable(String))" => %{"hello" => nil},
      "map('hello', 'world'::Nullable(String))" => %{"hello" => "world"}
    }

    for {value, expected} <- test do
      assert Ch.query!(
               pool,
               "select #{value}::Variant(String, Int32, Bool, Map(String, Nullable(String)))"
             ).rows == [[expected]]
    end
  end

  test "with a table", %{pool: pool} do
    # https://clickhouse.com/docs/sql-reference/data-types/variant#creating-variant
    Help.query!("""
    CREATE TABLE variant_test_table (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
    """)

    on_exit(fn -> Help.query!("DROP TABLE variant_test_table") end)

    Ch.query!(
      pool,
      "INSERT INTO variant_test_table VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);"
    )

    assert Ch.query!(pool, "SELECT v FROM variant_test_table").rows == [
             [nil],
             [42],
             ["Hello, World!"],
             [[1, 2, 3]]
           ]

    # https://clickhouse.com/docs/sql-reference/data-types/variant#reading-variant-nested-types-as-subcolumns
    assert Ch.query!(
             pool,
             "SELECT v, v.String, v.UInt64, v.`Array(UInt64)` FROM variant_test_table;"
           ).rows ==
             [
               [nil, nil, nil, []],
               [42, nil, 42, []],
               ["Hello, World!", "Hello, World!", nil, []],
               [[1, 2, 3], nil, nil, [1, 2, 3]]
             ]

    assert Ch.query!(
             pool,
             "SELECT v, variantElement(v, 'String'), variantElement(v, 'UInt64'), variantElement(v, 'Array(UInt64)') FROM variant_test_table;"
           ).rows == [
             [nil, nil, nil, []],
             [42, nil, 42, []],
             ["Hello, World!", "Hello, World!", nil, []],
             [[1, 2, 3], nil, nil, [1, 2, 3]]
           ]
  end

  test "rowbinary", %{pool: pool} do
    Help.query!("""
    CREATE TABLE variant_test_rowbinary (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
    """)

    on_exit(fn -> Help.query!("DROP TABLE variant_test_rowbinary") end)

    rows = [[nil], [42], ["Hello, World!"], [[1, 2, 3]]]

    rowbinary = Ch.RowBinary.encode_rows(rows, ["Variant(UInt64, String, Array(UInt64))"])

    Ch.query!(pool, ["INSERT INTO variant_test_rowbinary FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT v FROM variant_test_rowbinary").rows == rows
  end

  property "RowBinary Variant inserts round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE variant_test_property (
      id UInt64,
      v Variant(UInt64, String, Array(UInt64), Map(String, String))
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE variant_test_property") end)

    check all rows <- variant_rows(), max_runs: 25 do
      Ch.query!(pool, "TRUNCATE TABLE variant_test_property")

      rowbinary =
        Ch.RowBinary.encode_rows(
          rows,
          ["UInt64", "Variant(UInt64, String, Array(UInt64), Map(String, String))"]
        )

      Ch.query!(pool, ["INSERT INTO variant_test_property FORMAT RowBinary\n" | rowbinary])

      assert Ch.query!(pool, "SELECT * FROM variant_test_property ORDER BY id").rows ==
               Enum.sort_by(rows, &List.first/1)
    end
  end

  test "RowBinary Variant rejects values that match no branch" do
    assert_raise ArgumentError, ~s[no matching type found for encoding true as Variant], fn ->
      Ch.RowBinary.encode_rows([[true]], ["Variant(UInt64, String, Array(UInt64))"])
    end
  end

  defp variant_rows do
    gen all ids <- uniq_list_of(integer(0..18_446_744_073_709_551_615), max_length: 12),
            values <- list_of(variant_value(), length: length(ids)) do
      Enum.zip_with(ids, values, fn id, value -> [id, value] end)
    end
  end

  defp variant_value do
    one_of([
      constant(nil),
      integer(0..9_007_199_254_740_991),
      string(:printable, min_length: 1, max_length: 32),
      list_of(integer(0..9_007_199_254_740_991), min_length: 1, max_length: 8),
      map_of(
        string(:alphanumeric, min_length: 1, max_length: 16),
        string(:printable, max_length: 32),
        min_length: 1,
        max_length: 8
      )
    ])
  end
end
