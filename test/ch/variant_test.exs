defmodule Ch.VariantTest do
  use ExUnit.Case

  # https://clickhouse.com/docs/sql-reference/data-types/variant

  @moduletag :variant

  setup do
    conn = start_supervised!({Ch, database: Ch.Test.database()})
    {:ok, conn: conn}
  end

  test "basic", %{conn: conn} do
    assert Ch.query!(conn, "select null::Variant(UInt64, String, Array(UInt64))").rows == [[nil]]
    assert Ch.query!(conn, "select [1]::Variant(UInt64, String, Array(UInt64))").rows == [[[1]]]
    assert Ch.query!(conn, "select 0::Variant(UInt64, String, Array(UInt64))").rows == [[0]]

    assert Ch.query!(conn, "select 'Hello, World!'::Variant(UInt64, String, Array(UInt64))").rows ==
             [["Hello, World!"]]
  end

  # https://github.com/plausible/ch/issues/272
  test "ordering internal types", %{conn: conn} do
    test = %{
      "'hello'" => "hello",
      "-10" => -10,
      "true" => true,
      "map('hello', null::Nullable(String))" => %{"hello" => nil},
      "map('hello', 'world'::Nullable(String))" => %{"hello" => "world"}
    }

    for {value, expected} <- test do
      assert Ch.query!(
               conn,
               "select #{value}::Variant(String, Int32, Bool, Map(String, Nullable(String)))"
             ).rows == [[expected]]
    end
  end

  test "with a table", %{conn: conn} do
    # https://clickhouse.com/docs/sql-reference/data-types/variant#creating-variant
    Ch.query!(conn, """
    CREATE TABLE variant_test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
    """)

    on_exit(fn -> Ch.Test.query("DROP TABLE variant_test", [], database: Ch.Test.database()) end)

    Ch.query!(
      conn,
      "INSERT INTO variant_test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);"
    )

    assert Ch.query!(conn, "SELECT v FROM variant_test").rows == [
             [nil],
             [42],
             ["Hello, World!"],
             [[1, 2, 3]]
           ]

    # https://clickhouse.com/docs/sql-reference/data-types/variant#reading-variant-nested-types-as-subcolumns
    assert Ch.query!(conn, "SELECT v, v.String, v.UInt64, v.`Array(UInt64)` FROM variant_test;").rows ==
             [
               [nil, nil, nil, []],
               [42, nil, 42, []],
               ["Hello, World!", "Hello, World!", nil, []],
               [[1, 2, 3], nil, nil, [1, 2, 3]]
             ]

    assert Ch.query!(
             conn,
             "SELECT v, variantElement(v, 'String'), variantElement(v, 'UInt64'), variantElement(v, 'Array(UInt64)') FROM variant_test;"
           ).rows == [
             [nil, nil, nil, []],
             [42, nil, 42, []],
             ["Hello, World!", "Hello, World!", nil, []],
             [[1, 2, 3], nil, nil, [1, 2, 3]]
           ]
  end

  test "rowbinary", %{conn: conn} do
    Ch.query!(conn, """
    CREATE TABLE variant_test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
    """)

    on_exit(fn -> Ch.Test.query("DROP TABLE variant_test", [], database: Ch.Test.database()) end)

    Ch.query!(
      conn,
      "INSERT INTO variant_test FORMAT RowBinary",
      [[nil], [42], ["Hello, World!"], [[1, 2, 3]]],
      types: ["Variant(UInt64, String, Array(UInt64))"]
    )

    assert Ch.query!(conn, "SELECT v FROM variant_test").rows == [
             [nil],
             [42],
             ["Hello, World!"],
             [[1, 2, 3]]
           ]
  end
end
