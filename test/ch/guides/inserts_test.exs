defmodule Ch.Guides.InsertsTest do
  # Tests from pages/inserts.md
  use ExUnit.Case, async: true
  import Ch.RowBinary

  @types ["UInt64", "String", "DateTime"]
  @names ["id", "name", "created_at"]
  @rows [[1, "pageview", ~N[2024-01-01 00:00:00]], [2, "click", ~N[2024-01-01 00:01:00]]]

  describe "INSERT body construction" do
    test "builds correct RowBinaryWithNamesAndTypes body" do
      header = encode_names_and_types(@names, @types)
      rows_binary = encode_rows(@rows, @types)

      body =
        IO.iodata_to_binary([
          "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes\n",
          header | rows_binary
        ])

      # The body must start with the SQL statement
      assert String.starts_with?(body, "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes\n")

      # The RowBinary header is intact — decode_names_and_rows can parse the data portion
      data = binary_part(body, 52, byte_size(body) - 52)
      [names | decoded] = decode_names_and_rows(data)
      assert names == @names
      assert decoded == @rows
    end

    test "statement and RowBinary must be compressed together" do
      header = encode_names_and_types(@names, @types)
      rows_encoded = encode_rows(@rows, @types)

      body =
        IO.iodata_to_binary([
          "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes\n",
          header | rows_encoded
        ])

      compressed = :zlib.gzip(body)
      assert :zlib.gunzip(compressed) == body

      path = Ch.HTTP.path(%{}, [{"content-encoding", "gzip"}])
      assert path =~ "content-encoding=gzip"
    end
  end

  @tag :integration
  describe "live ClickHouse" do
    test "INSERT 1 row"
    test "INSERT 100_000 rows in one batch"
    test "INSERT with gzip compression"
    test "response includes x-clickhouse-summary with written_rows"
    test "GenServer buffer flushes on size threshold"
    test "GenServer buffer flushes on time threshold"
  end
end
