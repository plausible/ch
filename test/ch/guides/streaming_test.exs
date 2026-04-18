defmodule Ch.Guides.StreamingTest do
  # Tests from pages/streaming.md
  use ExUnit.Case, async: true
  import Ch.RowBinary

  # Simulates a RowBinaryWithNamesAndTypes response split into N chunks,
  # runs it through decode_start/decode_continue, collects all rows.
  defp stream_decode(binary, chunk_size) do
    headers = [{"x-clickhouse-format", "RowBinaryWithNamesAndTypes"}]
    state = Ch.HTTP.decode_start(headers)

    chunks = for <<chunk::binary-size(chunk_size) <- binary>>, do: chunk
    remainder_size = rem(byte_size(binary), chunk_size)

    chunks =
      if remainder_size > 0 do
        chunks ++ [binary_part(binary, byte_size(binary) - remainder_size, remainder_size)]
      else
        chunks
      end

    {names, rows, state} =
      Enum.reduce(chunks, {nil, [], state}, fn chunk, {names, rows_acc, state} ->
        case Ch.HTTP.decode_continue(chunk, state) do
          {:rows, new_rows, chunk_names, state} ->
            {names || chunk_names, rows_acc ++ new_rows, state}

          {:more, state} ->
            {names, rows_acc, state}
        end
      end)

    {:ok, final_names, final_rows} = Ch.HTTP.decode_continue(:end_of_input, state)
    {names || final_names, rows ++ final_rows}
  end

  describe "decode_start/decode_continue" do
    test "single chunk — full response at once" do
      types = ["UInt64", "String"]
      names = ["id", "name"]
      rows = [[1, "a"], [2, "b"], [3, "c"]]

      binary =
        IO.iodata_to_binary([
          encode_names_and_types(names, types),
          encode_rows(rows, types)
        ])

      assert stream_decode(binary, byte_size(binary)) == {names, rows}
    end

    test "byte-by-byte chunks — header and rows split at every boundary" do
      types = ["UInt64", "String"]
      names = ["id", "name"]
      rows = Enum.map(1..20, fn i -> [i, "row_#{i}"] end)

      binary =
        IO.iodata_to_binary([
          encode_names_and_types(names, types),
          encode_rows(rows, types)
        ])

      assert stream_decode(binary, 1) == {names, rows}
    end

    test "various chunk sizes produce the same result" do
      types = ["UInt32", "String", "Bool"]
      names = ["n", "s", "b"]
      rows = Enum.map(1..50, fn i -> [i, "val#{i}", rem(i, 2) == 0] end)

      binary =
        IO.iodata_to_binary([
          encode_names_and_types(names, types),
          encode_rows(rows, types)
        ])

      for chunk_size <- [1, 3, 7, 13, 64, 256, byte_size(binary)] do
        assert stream_decode(binary, chunk_size) == {names, rows},
               "failed with chunk_size=#{chunk_size}"
      end
    end

    test "empty response" do
      headers = [{"x-clickhouse-format", "RowBinaryWithNamesAndTypes"}]
      state = Ch.HTTP.decode_start(headers)
      assert {:ok, [], []} = Ch.HTTP.decode_continue(:end_of_input, state)
    end

    test "non-RowBinary format accumulates raw body" do
      headers = [{"x-clickhouse-format", "TabSeparated"}]
      state = Ch.HTTP.decode_start(headers)
      {:more, state} = Ch.HTTP.decode_continue("col1\tcol2\n", state)
      {:more, state} = Ch.HTTP.decode_continue("val1\tval2\n", state)
      assert {:ok, [], [_body]} = Ch.HTTP.decode_continue(:end_of_input, state)
    end
  end

  @tag :integration
  describe "live ClickHouse — streaming SELECT" do
    test "streams 1_000_000 rows from system.numbers"
    test "handles connection close mid-stream"
    test "works in passive mode receive loop"
  end
end
