defmodule Ch.CompressionTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "can request GZIP response through headers", %{pool: pool} do
    assert <<0x1F, 0x8B, _rest::bytes>> =
             data =
             pool
             |> Ch.query!(
               "select number from system.numbers limit {limit:UInt32}",
               %{"limit" => 1_000_000},
               headers: [{"accept-encoding", "gzip"}, {"x-clickhouse-format", "RowBinary"}]
             )
             |> IO.iodata_to_binary()

    assert byte_size(data) == 1_513_706
  end

  test "can request LZ4 response through headers", %{pool: pool} do
    assert <<0x04, 0x22, 0x4D, 0x18, _rest::bytes>> =
             data =
             pool
             |> Ch.query!(
               "select number from system.numbers limit {limit:UInt32}",
               %{"limit" => 1_000_000},
               headers: [{"accept-encoding", "lz4"}, {"x-clickhouse-format", "RowBinary"}]
             )
             |> IO.iodata_to_binary()

    assert byte_size(data) == 4_004_633
  end

  test "can request ZSTD response through headers", %{pool: pool} do
    assert <<0x28, 0xB5, 0x2F, 0xFD, _rest::bytes>> =
             data =
             pool
             |> Ch.query!(
               "select number from system.numbers limit {limit:UInt32}",
               %{"limit" => 1_000_000},
               headers: [{"accept-encoding", "zstd"}, {"x-clickhouse-format", "RowBinary"}]
             )
             |> IO.iodata_to_binary()

    assert byte_size(data) == 1_052_492
  end

  test "automatically decompresses and decodes ZSTD RowBinaryWithNamesAndTypes", %{pool: pool} do
    assert %{names: ["number"], rows: rows} =
             Ch.query!(
               pool,
               "select number from system.numbers limit {limit:UInt32}",
               %{"limit" => 1_000_000},
               headers: [{"accept-encoding", "zstd"}]
             )

    assert length(rows) == 1_000_000
  end

  test "automatically decompresses and decodes GZIP RowBinaryWithNamesAndTypes", %{pool: pool} do
    assert %{names: ["number"], rows: rows} =
             Ch.query!(
               pool,
               "select number from system.numbers limit {limit:UInt32}",
               %{"limit" => 1_000_000},
               headers: [{"accept-encoding", "gzip"}]
             )

    assert length(rows) == 1_000_000
  end
end
