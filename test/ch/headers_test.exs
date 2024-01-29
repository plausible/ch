defmodule Ch.HeadersTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn} = Ch.start_link()
    {:ok, conn: conn}
  end

  test "can request gzipped response through headers", %{conn: conn} do
    assert {:ok, %{data: data, headers: headers}} =
             Ch.query(conn, "select number from system.numbers limit 100", [],
               decode: false,
               settings: [enable_http_compression: 1],
               headers: [{"accept-encoding", "gzip"}]
             )

    assert :proplists.get_value("content-type", headers) == "application/octet-stream"
    assert :proplists.get_value("content-encoding", headers) == "gzip"
    assert :proplists.get_value("x-clickhouse-format", headers) == "RowBinaryWithNamesAndTypes"

    # https://en.wikipedia.org/wiki/Gzip
    assert <<0x1F, 0x8B, _rest::bytes>> = IO.iodata_to_binary(data)
  end

  test "can request lz4 response through headers", %{conn: conn} do
    assert {:ok, %{data: data, headers: headers}} =
             Ch.query(conn, "select number from system.numbers limit 100", [],
               decode: false,
               settings: [enable_http_compression: 1],
               headers: [{"accept-encoding", "lz4"}]
             )

    assert :proplists.get_value("content-type", headers) == "application/octet-stream"
    assert :proplists.get_value("content-encoding", headers) == "lz4"
    assert :proplists.get_value("x-clickhouse-format", headers) == "RowBinaryWithNamesAndTypes"

    # https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)
    assert <<0x04, 0x22, 0x4D, 0x18, _rest::bytes>> = IO.iodata_to_binary(data)
  end

  test "can request zstd response through headers", %{conn: conn} do
    assert {:ok, %{data: data, headers: headers}} =
             Ch.query(conn, "select number from system.numbers limit 100", [],
               decode: false,
               settings: [enable_http_compression: 1],
               headers: [{"accept-encoding", "zstd"}]
             )

    assert :proplists.get_value("content-type", headers) == "application/octet-stream"
    assert :proplists.get_value("content-encoding", headers) == "zstd"
    assert :proplists.get_value("x-clickhouse-format", headers) == "RowBinaryWithNamesAndTypes"

    # https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)
    assert <<0x28, 0xB5, 0x2F, 0xFD, _rest::bytes>> = IO.iodata_to_binary(data)
  end
end
