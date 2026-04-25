defmodule Ch.HeadersTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn: start_supervised!(Ch)}
  end

  test "can request gzipped response through headers", %{conn: conn} do
    assert {:ok, %Ch.Result{data: data, rows: data, headers: headers}} =
             Ch.query(
               conn,
               "select number from system.numbers limit 100",
               [],
               decode: false,
               settings: [enable_http_compression: 1],
               headers: [{"accept-encoding", "gzip"}]
             )

    assert :proplists.get_value("content-type", headers) == "application/octet-stream"
    assert :proplists.get_value("content-encoding", headers) == "gzip"
    assert :proplists.get_value("x-clickhouse-format", headers) == "RowBinaryWithNamesAndTypes"
    assert <<0x1F, 0x8B, _rest::bytes>> = data
  end

  test "can request zstd response through headers", %{conn: conn} do
    assert {:ok, %Ch.Result{data: data, rows: data, headers: headers}} =
             Ch.query(
               conn,
               "select number from system.numbers limit 100",
               [],
               decode: false,
               settings: [enable_http_compression: 1],
               headers: [{"accept-encoding", "zstd"}]
             )

    assert :proplists.get_value("content-type", headers) == "application/octet-stream"
    assert :proplists.get_value("content-encoding", headers) == "zstd"
    assert :proplists.get_value("x-clickhouse-format", headers) == "RowBinaryWithNamesAndTypes"
    assert <<0x28, 0xB5, 0x2F, 0xFD, _rest::bytes>> = data
  end
end
