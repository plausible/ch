defmodule Ch.CompressionTest do
  use ExUnit.Case

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "gzip", %{conn: conn} do
    Ch.query!(conn, "create table gzip_insert_test(i Int64, s String) engine Memory")

    rowbinary =
      Ch.RowBinary.encode_rows(
        [[1, "alice"], [2, "bob"], [3, "alice"]],
        _types = ["Int64", "String"]
      )

    compressed = :zlib.gzip(["insert into gzip_insert_test(i, s) format RowBinary\n" | rowbinary])

    assert %Ch.Result{num_rows: 3} =
             Ch.query!(
               conn,
               compressed,
               _no_params = [],
               command: :insert,
               headers: [{"content-encoding", "gzip"}]
             )

    assert Ch.query!(conn, "select i, s from gzip_insert_test order by i").rows == [
             [1, "alice"],
             [2, "bob"],
             [3, "alice"]
           ]
  end

  # NOTE: ClickHouse uses custom LZ4 frame format
  # TODO: example https://github.com/ClickHouse/clickhouse-rs/blob/main/src/compression/lz4.rs
  #       we need cityhash to generate checksum
  @tag :skip
  test "lz4"

  # TODO: https://github.com/erlang/otp/pull/9316
  @tag :skip
  test "zstd"
end
