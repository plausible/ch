defmodule Ch.CompressionTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "can request gzipped response through headers", %{pool: pool} do
    # https://en.wikipedia.org/wiki/Gzip
    assert <<0x1F, 0x8B, _rest::bytes>> =
             Ch.query!(
               pool,
               "select number from system.numbers limit {limit:UInt16}",
               %{"limit" => 10000},
               headers: [
                 {"accept-encoding", "gzip"},
                 {"x-clickhouse-format", "CSV"}
               ]
             )
  end

  test "can request lz4 response through headers", %{pool: pool} do
    # https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)
    assert <<0x04, 0x22, 0x4D, 0x18, _rest::bytes>> =
             Ch.query!(
               pool,
               "select number from system.numbers limit {limit:UInt16}",
               %{"limit" => 10000},
               headers: [
                 {"accept-encoding", "lz4"},
                 {"x-clickhouse-format", "CSV"}
               ]
             )
  end

  test "can request zstd response through headers", %{pool: pool} do
    assert <<0x28, 0xB5, 0x2F, 0xFD, _rest::bytes>> =
             Ch.query!(
               pool,
               "select number from system.numbers limit {limit:UInt16}",
               %{"limit" => 10000},
               headers: [{"accept-encoding", "zstd"}, {"x-clickhouse-format", "CSV"}]
             )
  end

  test "automatically decompresses and decodes ZSTD RowBinaryWithNamesAndTypes", %{pool: pool} do
    assert %{names: ["number"], rows: rows} =
             Ch.query!(
               pool,
               "select number from system.numbers limit {limit:UInt16}",
               %{"limit" => 10000},
               headers: [{"accept-encoding", "zstd"}]
             )

    assert length(rows) == 10000
  end

  test "automatically decompresses and decodes GZIP RowBinaryWithNamesAndTypes", %{pool: pool} do
    assert %{names: ["number"], rows: rows} =
             Ch.query!(
               pool,
               "select number from system.numbers limit {limit:UInt16}",
               %{"limit" => 10000},
               headers: [{"accept-encoding", "gzip"}]
             )

    assert length(rows) == 10000
  end
end
