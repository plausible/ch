defmodule Ch.RowBinaryStringTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  @string_table "row_binary_string_property"
  @fixed_string_table "row_binary_fixed_string_property"
  @array_table "row_binary_string_array_property"

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "String values inserted as RowBinary round-trip through ClickHouse", %{pool: pool} do
    Help.query!("CREATE TABLE #{@string_table}(id UInt8, s String) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE #{@string_table}") end)

    check all rows <- string_rows() do
      Help.query!("TRUNCATE TABLE #{@string_table}")

      rowbinary = RowBinary.encode_rows(rows, ["UInt8", "String"])
      Ch.query!(pool, ["INSERT INTO #{@string_table} FORMAT RowBinary\n" | rowbinary])

      assert Ch.query!(pool, "SELECT * FROM #{@string_table} ORDER BY id").rows == rows
    end
  end

  property "FixedString values inserted as RowBinary are padded by ClickHouse", %{pool: pool} do
    Help.query!("CREATE TABLE #{@fixed_string_table}(id UInt8, s FixedString(8)) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE #{@fixed_string_table}") end)

    check all rows <- fixed_string_rows(8) do
      Help.query!("TRUNCATE TABLE #{@fixed_string_table}")

      rowbinary = RowBinary.encode_rows(rows, ["UInt8", "FixedString(8)"])
      Ch.query!(pool, ["INSERT INTO #{@fixed_string_table} FORMAT RowBinary\n" | rowbinary])

      expected =
        Enum.map(rows, fn [id, value] ->
          [id, value <> :binary.copy(<<0>>, 8 - byte_size(value))]
        end)

      assert Ch.query!(pool, "SELECT * FROM #{@fixed_string_table} ORDER BY id").rows == expected
    end
  end

  property "string-like arrays inserted as RowBinary round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE #{@array_table}(
      id UInt8,
      strings Array(String),
      fixed_strings Array(FixedString(4)),
      nullable_strings Array(Nullable(String)),
      low_cardinality_strings Array(LowCardinality(String)),
      low_cardinality_fixed_strings Array(LowCardinality(FixedString(4)))
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE #{@array_table}") end)

    types = [
      "UInt8",
      "Array(String)",
      "Array(FixedString(4))",
      "Array(Nullable(String))",
      "Array(LowCardinality(String))",
      "Array(LowCardinality(FixedString(4)))"
    ]

    check all rows <- string_array_rows(4) do
      Help.query!("TRUNCATE TABLE #{@array_table}")

      rowbinary = RowBinary.encode_rows(rows, types)
      Ch.query!(pool, ["INSERT INTO #{@array_table} FORMAT RowBinary\n" | rowbinary])

      expected =
        Enum.map(rows, fn [id, strings, fixed_strings, nullable_strings, lc_strings, lc_fixed] ->
          [
            id,
            strings,
            pad_all(fixed_strings, 4),
            nullable_strings,
            lc_strings,
            pad_all(lc_fixed, 4)
          ]
        end)

      assert Ch.query!(pool, "SELECT * FROM #{@array_table} ORDER BY id").rows == expected
    end
  end

  test "String values preserve boundary lengths, invalid UTF-8, and null bytes through ClickHouse",
       %{
         pool: pool
       } do
    Help.query!("CREATE TABLE row_binary_string_examples(id UInt8, s String) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE row_binary_string_examples") end)

    rows =
      [
        "",
        "a",
        :binary.copy("a", 127),
        :binary.copy("a", 128),
        :binary.copy("a", 16_383),
        :binary.copy("a", 16_384),
        <<0>>,
        <<0, 1, 2, 3>>,
        <<0xFF, 0xFE, 0xFD>>,
        "\x61\xF0\x80\x80\x80b",
        "/some/url" <> <<0xAE>> <> "-/",
        "/opportunity/category/جوائز-ومسابقات"
      ]
      |> Enum.with_index()
      |> Enum.map(fn {value, id} -> [id, value] end)

    rowbinary = RowBinary.encode_rows(rows, ["UInt8", "String"])
    Ch.query!(pool, ["INSERT INTO row_binary_string_examples FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_string_examples ORDER BY id").rows == rows
  end

  test "String and FixedString nils encode as empty/null bytes through ClickHouse", %{pool: pool} do
    Help.query!("CREATE TABLE row_binary_string_nils(s String, f FixedString(3)) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE row_binary_string_nils") end)

    rowbinary = RowBinary.encode_rows([[nil, nil]], ["String", "FixedString(3)"])
    Ch.query!(pool, ["INSERT INTO row_binary_string_nils FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_string_nils").rows == [["", <<0, 0, 0>>]]
  end

  test "reject string values that RowBinary cannot encode" do
    for invalid <- [1, :atom, %{}, {:ok, "string"}] do
      assert_raise CaseClauseError, fn ->
        RowBinary.encode(:string, invalid)
      end
    end
  end

  test "reject fixed string values that RowBinary cannot encode" do
    assert_raise FunctionClauseError, fn ->
      RowBinary.encode({:fixed_string, 2}, "abc")
    end

    for invalid <- [1, :atom, ["a"], %{}] do
      assert_raise FunctionClauseError, fn ->
        RowBinary.encode({:fixed_string, 2}, invalid)
      end
    end
  end

  defp string_rows do
    gen all values <- list_of(small_binary(), min_length: 1, max_length: 16) do
      values
      |> Enum.with_index()
      |> Enum.map(fn {value, id} -> [id, value] end)
    end
  end

  defp fixed_string_rows(size) do
    gen all values <- list_of(binary(max_length: size), min_length: 1, max_length: 16) do
      values
      |> Enum.with_index()
      |> Enum.map(fn {value, id} -> [id, value] end)
    end
  end

  defp string_array_rows(fixed_size) do
    gen all rows <-
              list_of(
                {list_of(small_binary(), max_length: 8),
                 list_of(binary(max_length: fixed_size), max_length: 8),
                 list_of(one_of([small_binary(), constant(nil)]), max_length: 8),
                 list_of(small_binary(), max_length: 8),
                 list_of(binary(max_length: fixed_size), max_length: 8)},
                min_length: 1,
                max_length: 8
              ) do
      rows
      |> Enum.with_index()
      |> Enum.map(fn {{strings, fixed_strings, nullable_strings, lc_strings, lc_fixed}, id} ->
        [id, strings, fixed_strings, nullable_strings, lc_strings, lc_fixed]
      end)
    end
  end

  defp small_binary do
    one_of([
      binary(max_length: 64),
      member_of([
        <<0>>,
        <<0, 1, 2, 3>>,
        <<0xFF, 0xFE, 0xFD>>,
        "\x61\xF0\x80\x80\x80b",
        "/some/url" <> <<0xAE>> <> "-/",
        "/opportunity/category/جوائز-ومسابقات"
      ])
    ])
  end

  defp pad_all(values, size) do
    Enum.map(values, fn value ->
      value <> :binary.copy(<<0>>, size - byte_size(value))
    end)
  end
end
