defmodule Ch.VariantTest do
  use ExUnit.Case, async: true

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
    CREATE TABLE variant_test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
    """)

    on_exit(fn -> Help.query!("DROP TABLE variant_test") end)

    Ch.query!(
      pool,
      "INSERT INTO variant_test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);"
    )

    assert Ch.query!(pool, "SELECT v FROM variant_test").rows == [
             [nil],
             [42],
             ["Hello, World!"],
             [[1, 2, 3]]
           ]

    # https://clickhouse.com/docs/sql-reference/data-types/variant#reading-variant-nested-types-as-subcolumns
    assert Ch.query!(pool, "SELECT v, v.String, v.UInt64, v.`Array(UInt64)` FROM variant_test;").rows ==
             [
               [nil, nil, nil, []],
               [42, nil, 42, []],
               ["Hello, World!", "Hello, World!", nil, []],
               [[1, 2, 3], nil, nil, [1, 2, 3]]
             ]

    assert Ch.query!(
             pool,
             "SELECT v, variantElement(v, 'String'), variantElement(v, 'UInt64'), variantElement(v, 'Array(UInt64)') FROM variant_test;"
           ).rows == [
             [nil, nil, nil, []],
             [42, nil, 42, []],
             ["Hello, World!", "Hello, World!", nil, []],
             [[1, 2, 3], nil, nil, [1, 2, 3]]
           ]
  end

  test "rowbinary", %{pool: pool} do
    Help.query!("""
    CREATE TABLE variant_test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
    """)

    on_exit(fn -> Help.query!("DROP TABLE variant_test") end)

    rows = [[nil], [42], ["Hello, World!"], [[1, 2, 3]]]

    rowbinary = Ch.RowBinary.encode_rows(rows, ["Variant(UInt64, String, Array(UInt64))"])

    Ch.query!(pool, ["INSERT INTO variant_test FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT v FROM variant_test").rows == rows
  end
end
