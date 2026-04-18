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
    test "encodes body as zstd and sets content-encoding header" do
      body = sample_body()
      compressed = :zstd.compress(body)

      {_path, headers, ^compressed} =
        Ch.HTTP.encode(compressed, %{}, headers: [{"content-encoding", "zstd"}])

      assert List.keyfind(headers, "content-encoding", 0) == {"content-encoding", "zstd"}
    end

    test "zstd roundtrip: compressed body decompresses to original" do
      body = sample_body()
      compressed = :zstd.compress(body)
      assert :zstd.decompress(compressed) == body
    end

    test "decode/3 auto-decompresses zstd response" do
      rows = [[1, "pageview", ~N[2024-01-01 00:00:00]]]
      rb_body = IO.iodata_to_binary([
        Ch.RowBinary.encode_names_and_types(@names, @types),
        Ch.RowBinary.encode_rows(rows, @types)
      ])
      compressed = :zstd.compress(rb_body)

      headers = [
        {"x-clickhouse-format", "RowBinaryWithNamesAndTypes"},
        {"content-encoding", "zstd"}
      ]

      assert {:ok, @names, ^rows} = Ch.HTTP.decode(200, headers, compressed)
    end
  end

  describe "gzip (stdlib)" do
    test "encodes body as gzip and sets content-encoding header" do
      body = sample_body()
      compressed = :zlib.gzip(body)

      {_path, headers, ^compressed} =
        Ch.HTTP.encode(compressed, %{}, headers: [{"content-encoding", "gzip"}])

      assert List.keyfind(headers, "content-encoding", 0) == {"content-encoding", "gzip"}
    end

    test "gzip roundtrip: compressed body decompresses to original" do
      body = sample_body()
      compressed = :zlib.gzip(body)
      assert :zlib.gunzip(compressed) == body
    end

    test "decode/3 auto-decompresses gzip response" do
      rows = [[1, "pageview", ~N[2024-01-01 00:00:00]]]
      rb_body = IO.iodata_to_binary([
        Ch.RowBinary.encode_names_and_types(@names, @types),
        Ch.RowBinary.encode_rows(rows, @types)
      ])
      gzipped = :zlib.gzip(rb_body)

      headers = [
        {"x-clickhouse-format", "RowBinaryWithNamesAndTypes"},
        {"content-encoding", "gzip"}
      ]

      assert {:ok, @names, ^rows} = Ch.HTTP.decode(200, headers, gzipped)
    end
  end

  describe "lz4 (nimble_lz4)" do
    @tag :lz4
    test "encode and decode lz4 compressed body" do
      body = sample_body()
      {:ok, compressed} = NimbleLz4.compress(body)
      assert NimbleLz4.decompress(compressed) == {:ok, body}

      {_path, headers, ^compressed} =
        Ch.HTTP.encode(compressed, %{}, headers: [{"content-encoding", "lz4"}])

      assert List.keyfind(headers, "content-encoding", 0) == {"content-encoding", "lz4"}
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
