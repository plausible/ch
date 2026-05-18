defmodule Ch.RowBinaryBoolTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "Bool params round-trip through ClickHouse", %{pool: pool} do
    for value <- [true, false] do
      assert Ch.query!(pool, "SELECT {value:Bool}", %{"value" => value}).rows == [[value]]
    end
  end

  property "Bool arrays round-trip as query params through ClickHouse", %{pool: pool} do
    check all values <- list_of(boolean(), max_length: 16) do
      assert Ch.query!(pool, "SELECT {value:Array(Bool)}", %{"value" => values}).rows == [
               [values]
             ]
    end
  end

  test "query params cover nullable and empty Bool arrays", %{pool: pool} do
    assert Ch.query!(
             pool,
             "SELECT {t:Bool}, {f:Bool}, {n:Nullable(Bool)}, {empty:Array(Bool)}",
             %{"t" => true, "f" => false, "n" => nil, "empty" => []}
           ).rows == [[true, false, nil, []]]
  end

  property "invalid Bool params are rejected by ClickHouse", %{pool: pool} do
    check all value <- invalid_bool_param() do
      assert {:error, %Ch.Error{message: message}} =
               Ch.query(pool, "SELECT {value:Bool}", %{"value" => value})

      assert message =~ "Bool"
    end
  end

  test "RowBinary Bool inserts round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_bool_values (
      id UInt64,
      value Bool
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_bool_values") end)

    rows = [
      [0, false],
      [1, true],
      [18_446_744_073_709_551_615, false]
    ]

    rowbinary = RowBinary.encode_rows(rows, ["UInt64", "Bool"])
    Ch.query!(pool, ["INSERT INTO row_binary_bool_values FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_bool_values ORDER BY id").rows == rows
  end

  test "RowBinary inserts cover nullable, arrays, tuples, maps, and defaults", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_bool_representative (
      id UInt64,
      value Bool,
      nullable Nullable(Bool),
      bools Array(Bool),
      pair Tuple(Bool, Bool),
      mapped Map(String, Bool)
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_bool_representative") end)

    rows = [
      [1, true, nil, [], {true, false}, %{}],
      [2, false, true, [true, false, true], {false, true}, %{"a" => true, "b" => false}],
      [18_446_744_073_709_551_615, nil, false, [false], {true, true}, %{"zero" => false}]
    ]

    types = [
      "UInt64",
      "Bool",
      "Nullable(Bool)",
      "Array(Bool)",
      "Tuple(Bool, Bool)",
      "Map(String, Bool)"
    ]

    rowbinary = RowBinary.encode_rows(rows, types)
    Ch.query!(pool, ["INSERT INTO row_binary_bool_representative FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_bool_representative ORDER BY id").rows == [
             [1, true, nil, [], {true, false}, %{}],
             [2, false, true, [true, false, true], {false, true}, %{"a" => true, "b" => false}],
             [18_446_744_073_709_551_615, false, false, [false], {true, true}, %{"zero" => false}]
           ]
  end

  test "RowBinary rejects invalid Bool values" do
    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([["true"]], ["Bool"])
    end
  end

  defp invalid_bool_param do
    gen all suffix <- string(:alphanumeric, min_length: 1, max_length: 16) do
      "not_bool_#{suffix}"
    end
  end
end
