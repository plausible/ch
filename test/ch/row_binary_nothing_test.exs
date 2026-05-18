defmodule Ch.RowBinaryNothingTest do
  use ExUnit.Case, async: true

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "Nothing values returned by ClickHouse decode as nil", %{pool: pool} do
    assert Ch.query!(
             pool,
             "SELECT NULL::Nullable(Nothing), []::Array(Nothing), {value:Nullable(Nothing)}",
             %{"value" => nil}
           ).rows == [[nil, [], nil]]
  end

  test "empty Array(Nothing) query params round-trip through ClickHouse", %{pool: pool} do
    assert Ch.query!(pool, "SELECT {value:Array(Nothing)}", %{"value" => []}).rows == [[[]]]
    assert IO.iodata_to_binary(RowBinary.encode_rows([[[]]], ["Array(Nothing)"])) == <<0>>
  end

  test "RowBinary rejects non-empty Array(Nothing) input" do
    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([[[nil]]], ["Array(Nothing)"])
    end
  end
end
