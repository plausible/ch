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
               headers: [{"accept-encoding", "gzip"}, {"x-clickhouse-format", "RowBinary"}],
               settings: %{"enable_http_compression" => 1}
             )
             |> Map.fetch!(:data)
             |> IO.iodata_to_binary()
  end

  test "can request LZ4 response through headers", %{pool: pool} do
    assert <<0x04, 0x22, 0x4D, 0x18, _rest::bytes>> =
             data =
             pool
             |> Ch.query!(
               "select number from system.numbers limit {limit:UInt32}",
               %{"limit" => 1_000_000},
               headers: [{"accept-encoding", "lz4"}, {"x-clickhouse-format", "RowBinary"}],
               settings: %{"enable_http_compression" => 1}
             )
             |> Map.fetch!(:data)
             |> IO.iodata_to_binary()
  end

  test "can request ZSTD response through headers", %{pool: pool} do
    assert <<0x28, 0xB5, 0x2F, 0xFD, _rest::bytes>> =
             data =
             pool
             |> Ch.query!(
               "select number from system.numbers limit {limit:UInt32}",
               %{"limit" => 1_000_000},
               headers: [{"accept-encoding", "zstd"}, {"x-clickhouse-format", "RowBinary"}],
               settings: %{"enable_http_compression" => 1}
             )
             |> Map.fetch!(:data)
             |> IO.iodata_to_binary()
  end

  test "automatically decompresses and decodes ZSTD RowBinaryWithNamesAndTypes", %{pool: pool} do
    assert %{names: ["number"], rows: rows} =
             Ch.query!(
               pool,
               "select number from system.numbers limit {limit:UInt32}",
               %{"limit" => 1_000_000},
               headers: [{"accept-encoding", "zstd"}],
               settings: %{"enable_http_compression" => 1}
             )

    assert length(rows) == 1_000_000
  end

  test "automatically decompresses and decodes GZIP RowBinaryWithNamesAndTypes", %{pool: pool} do
    assert %{names: ["number"], rows: rows} =
             Ch.query!(
               pool,
               "select number from system.numbers limit {limit:UInt32}",
               %{"limit" => 1_000_000},
               headers: [{"accept-encoding", "gzip"}],
               settings: %{"enable_http_compression" => 1}
             )

    assert length(rows) == 1_000_000
  end

  test "automatically handles empty ZSTD RowBinaryWithNamesAndTypes responses", %{pool: pool} do
    on_exit(fn -> Help.query!("DROP TABLE compression_test_zstd_empty_response") end)

    assert %Ch.Result{names: nil, rows: nil, data: nil, headers: headers} =
             Ch.query!(
               pool,
               "CREATE TABLE compression_test_zstd_empty_response(a UInt8) ENGINE Memory",
               %{},
               headers: [{"accept-encoding", "zstd"}],
               settings: %{"enable_http_compression" => 1}
             )

    assert is_list(headers)
  end

  test "automatically handles empty GZIP RowBinaryWithNamesAndTypes responses", %{pool: pool} do
    on_exit(fn -> Help.query!("DROP TABLE compression_test_gzip_empty_response") end)

    assert %Ch.Result{names: nil, rows: nil, data: nil, headers: headers} =
             Ch.query!(
               pool,
               "CREATE TABLE compression_test_gzip_empty_response(a UInt8) ENGINE Memory",
               %{},
               headers: [{"accept-encoding", "gzip"}],
               settings: %{"enable_http_compression" => 1}
             )

    assert is_list(headers)
  end

  test "automatically decompresses ZSTD error responses", %{pool: pool} do
    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "SELECT missing_column", %{},
               headers: [{"accept-encoding", "zstd"}],
               settings: %{"enable_http_compression" => 1}
             )

    assert message =~ "UNKNOWN_IDENTIFIER"
    refute message =~ <<0x28, 0xB5, 0x2F, 0xFD>>
  end

  test "automatically decompresses GZIP error responses", %{pool: pool} do
    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "SELECT missing_column", %{},
               headers: [{"accept-encoding", "gzip"}],
               settings: %{"enable_http_compression" => 1}
             )

    assert message =~ "UNKNOWN_IDENTIFIER"
    refute message =~ <<0x1F, 0x8B>>
  end

  test "can send ZSTD compressed RowBinaryWithNamesAndTypes payloads", %{pool: pool} do
    Help.query!("CREATE TABLE compression_test_zstd_payload(id UInt8, name String) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE compression_test_zstd_payload") end)

    names = ["id", "name"]
    types = ["UInt8", "String"]
    rows = [[1, "one"], [2, "two"]]

    payload =
      :zstd.compress([
        "INSERT INTO compression_test_zstd_payload FORMAT RowBinaryWithNamesAndTypes\n",
        Ch.RowBinary.encode_names_and_types(names, types),
        Ch.RowBinary.encode_rows(rows, types)
      ])

    assert %Ch.Result{names: nil, rows: nil, data: nil} =
             Ch.query!(pool, payload, %{}, headers: [{"content-encoding", "zstd"}])

    assert Ch.query!(pool, "SELECT * FROM compression_test_zstd_payload ORDER BY id").rows == rows

    assert Ch.query!(pool, "SELECT * FROM compression_test_zstd_payload ORDER BY id", %{},
             headers: [{"accept-encoding", "zstd"}],
             settings: %{"enable_http_compression" => 1}
           ).rows == rows
  end
end
