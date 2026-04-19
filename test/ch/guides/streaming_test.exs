defmodule Ch.Guides.StreamingTest do
  # Tests from pages/streaming.md
  use ExUnit.Case, async: true
  import Ch.RowBinary

  # Simulates a RowBinaryWithNamesAndTypes response split into N chunks,
  # runs it through decode_start/decode_continue, collects all rows.
  defp stream_decode(binary, chunk_size) do
    state = Ch.HTTP.decode_start()
    headers = [{"x-clickhouse-format", "RowBinaryWithNamesAndTypes"}]

    responses = [
      {:status, nil, 200},
      {:headers, nil, headers}
    ]

    responses =
      responses ++
        for <<chunk::binary-size(chunk_size) <- binary>>, do: {:data, nil, chunk}

    remainder_size = rem(byte_size(binary), chunk_size)

    responses =
      if remainder_size > 0 do
        responses ++
          [{:data, nil, binary_part(binary, byte_size(binary) - remainder_size, remainder_size)}]
      else
        responses
      end

    responses = responses ++ [{:done, nil}]

    {names, rows, state} =
      Enum.reduce(responses, {nil, [], state}, fn resp, {names, rows_acc, state} ->
        case Ch.HTTP.decode_continue(state, resp) do
          {:rows, new_rows, chunk_names, new_state} ->
            {names || chunk_names, rows_acc ++ new_rows, new_state}

          {:cont, new_state} ->
            {names, rows_acc, new_state}

          {:ok, chunk_names, new_rows} ->
            {names || chunk_names, rows_acc ++ new_rows, nil}

          :ok ->
            {names, rows_acc, nil}
        end
      end)

    {names, rows}
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
      state = Ch.HTTP.decode_start()
      {:cont, state} = Ch.HTTP.decode_continue(state, {:status, nil, 200})
      {:cont, state} = Ch.HTTP.decode_continue(state, {:headers, nil, []})
      assert :ok = Ch.HTTP.decode_continue(state, {:done, nil})
    end

    test "unknown format accumulates error body" do
      state = Ch.HTTP.decode_start()
      headers = [{"x-clickhouse-format", "TabSeparated"}]
      {:cont, state} = Ch.HTTP.decode_continue(state, {:status, nil, 200})
      {:cont, state} = Ch.HTTP.decode_continue(state, {:headers, nil, headers})
      {:cont, state} = Ch.HTTP.decode_continue(state, {:data, nil, "col1\tcol2\n"})
      assert {:error, {:unknown_format, "TabSeparated"}} = Ch.HTTP.decode_continue(state, {:done, nil})
    end
  end

  @tag :integration
  describe "live ClickHouse — streaming SELECT" do
    test "streams 1_000_000 rows from system.numbers"
    test "handles connection close mid-stream"
    test "works in passive mode receive loop"
  end
end
