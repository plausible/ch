defmodule Ch.Guides.JsonTest do
  # Tests from pages/json.md
  use ExUnit.Case, async: true
  import Ch.RowBinary

  describe "JSON stored in String columns" do
    test "encode and decode JSON-in-String roundtrip" do
      types = ["UInt64", "String"]
      names = ["id", "metadata"]

      rows = [
        [1, JSON.encode!(%{"source" => "web", "browser" => "Firefox"})],
        [2, JSON.encode!(%{"source" => "mobile", "os" => "iOS"})]
      ]

      encoded =
        IO.iodata_to_binary([
          encode_names_and_types(names, types),
          encode_rows(rows, types)
        ])

      assert [^names | decoded_rows] = decode_names_and_rows(encoded)
      assert decoded_rows == rows
    end

    test "JSON values survive RowBinary encode/decode as strings" do
      json = JSON.encode!(%{"nested" => %{"key" => [1, 2, 3]}})
      encoded = IO.iodata_to_binary(encode(:string, json))
      assert decode_rows(encoded, [:string]) == [[json]]
    end
  end

  describe "native JSON type" do
    test "JSON map encodes and decodes as Elixir map" do
      types = ["UInt64", "JSON"]
      names = ["id", "data"]

      rows = [
        [1, %{"action" => "click", "element" => "button"}],
        [2, %{"action" => "view", "page" => "/home"}]
      ]

      encoded = IO.iodata_to_binary(encode_rows(rows, types))
      assert decode_rows(encoded, types) == rows
    end

    test "JSON list encodes and decodes as Elixir list" do
      types = ["JSON"]
      rows = [[[1, 2, 3]], [nil]]

      encoded = IO.iodata_to_binary(encode_rows(rows, types))
      assert decode_rows(encoded, types) == rows
    end
  end

  @tag :integration
  describe "live ClickHouse" do
    test "INSERT and SELECT with String column containing JSON"
    test "INSERT and SELECT with native JSON column type"
    test "SELECT JSON sub-field with data.field accessor syntax"
  end
end
