defmodule Ch.Guides.CompressionTest do
  # Tests from pages/compression.md
  use ExUnit.Case, async: true

  @types ["UInt64", "String", "DateTime"]
  @names ["id", "name", "created_at"]

  defp sample_body do
    rows = [[1, "pageview", DateTime.utc_now()]]
    header = Ch.RowBinary.encode_names_and_types(@names, @types)
    encoded = Ch.RowBinary.encode_rows(rows, @types)

    IO.iodata_to_binary([
      "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes\n",
      header | encoded
    ])
  end

  describe "zstd (OTP 28 stdlib)" do
    test "sets content-encoding header in path" do
      _body = sample_body()
      path = Ch.HTTP.path(%{}, [{"content-encoding", "zstd"}])

      assert path =~ "content-encoding=zstd"
    end

    test "zstd roundtrip: compressed body decompresses to original" do
      body = sample_body()
      compressed = :zstd.compress(body)
      assert :zstd.decompress(compressed) == body
    end

    test "streaming decode with manual zstd decompression" do
      rows = [[1, "pageview", ~N[2024-01-01 00:00:00]]]

      rb_body =
        IO.iodata_to_binary([
          Ch.RowBinary.encode_names_and_types(@names, @types),
          Ch.RowBinary.encode_rows(rows, @types)
        ])

      compressed = :zstd.compress(rb_body)

      headers = [
        {"x-clickhouse-format", "RowBinaryWithNamesAndTypes"},
        {"content-encoding", "zstd"}
      ]

      # Manual decompression before decoding
      body =
        case List.keyfind(headers, "content-encoding", 0) do
          {_, "zstd"} -> :zstd.decompress(compressed)
          _ -> compressed
        end

      state = Ch.HTTP.decode_start()
      {:cont, state} = Ch.HTTP.decode_continue(state, {:status, nil, 200})
      {:cont, state} = Ch.HTTP.decode_continue(state, {:headers, nil, headers})
      assert {:rows, ^rows, @names, _state} = Ch.HTTP.decode_continue(state, {:data, nil, body})
    end
  end

  describe "gzip (stdlib)" do
    test "sets content-encoding in path" do
      _body = sample_body()
      path = Ch.HTTP.path(%{}, [{"content-encoding", "gzip"}])

      assert path =~ "content-encoding=gzip"
    end

    test "gzip roundtrip: compressed body decompresses to original" do
      body = sample_body()
      compressed = :zlib.gzip(body)
      assert :zlib.gunzip(compressed) == body
    end

    test "streaming decode with manual gzip decompression" do
      rows = [[1, "pageview", ~N[2024-01-01 00:00:00]]]

      rb_body =
        IO.iodata_to_binary([
          Ch.RowBinary.encode_names_and_types(@names, @types),
          Ch.RowBinary.encode_rows(rows, @types)
        ])

      gzipped = :zlib.gzip(rb_body)

      headers = [
        {"x-clickhouse-format", "RowBinaryWithNamesAndTypes"},
        {"content-encoding", "gzip"}
      ]

      # Manual decompression before decoding
      body =
        case List.keyfind(headers, "content-encoding", 0) do
          {_, "gzip"} -> :zlib.gunzip(gzipped)
          _ -> gzipped
        end

      state = Ch.HTTP.decode_start()
      {:cont, state} = Ch.HTTP.decode_continue(state, {:status, nil, 200})
      {:cont, state} = Ch.HTTP.decode_continue(state, {:headers, nil, headers})
      assert {:rows, ^rows, @names, _state} = Ch.HTTP.decode_continue(state, {:data, nil, body})
    end
  end

  describe "lz4 (nimble_lz4)" do
    @tag :lz4
    test "lz4 roundtrip and path setting" do
      body = sample_body()
      {:ok, compressed} = NimbleLz4.compress(body)
      assert NimbleLz4.decompress(compressed) == {:ok, body}

      path = Ch.HTTP.path(%{}, [{"content-encoding", "lz4"}])
      assert path =~ "content-encoding=lz4"
    end
  end

  @tag :integration
  describe "live ClickHouse" do
    # Requires ClickHouse at localhost:8123
    # Run with: mix test --include integration

    test "INSERT with gzip compression succeeds"
    test "INSERT with lz4 compression succeeds"
    test "SELECT response with accept-encoding: gzip is decompressed automatically"
  end
end
