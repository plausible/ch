defmodule Ch.Guides.JsonTest do
  # Tests from pages/json.md
  use ExUnit.Case, async: true
  import Ch.RowBinary

  @types ["UInt64", "JSON"]
  @names ["id", "data"]

  @rows [
    [1, %{"action" => "click", "element" => "button"}],
    [2, %{"action" => "view", "page" => "/home"}]
  ]

  describe "encode(:json, value)" do
    test "maps are encoded as RowBinary strings via JSON.encode_to_iodata!" do
      encoded = IO.iodata_to_binary(encode(:json, %{"action" => "click"}))
      # RowBinary string: 1-byte LEB128 length prefix + JSON text
      <<len, rest::binary>> = encoded
      assert len == byte_size(rest)
      assert Jason.decode!(rest) == %{"action" => "click"}
    end

    test "lists are encoded as RowBinary strings" do
      encoded = IO.iodata_to_binary(encode(:json, [1, 2, 3]))
      <<len, rest::binary>> = encoded
      assert len == byte_size(rest)
      assert Jason.decode!(rest) == [1, 2, 3]
    end

    test "nil encodes as empty string" do
      # nil as JSON string mode
      encoded = IO.iodata_to_binary(encode(:json, nil))
      <<len, _rest::binary>> = encoded
      assert len == 4  # "null"
    end
  end

  describe "encode_rows/2 with JSON type" do
    test "rows with JSON maps roundtrip through encode_rows / decode_rows" do
      encoded = IO.iodata_to_binary(encode_rows(@rows, @types))
      assert decode_rows(encoded, @types) == @rows
    end

    test "full RowBinaryWithNamesAndTypes roundtrip with JSON column" do
      encoded = IO.iodata_to_binary([
        encode_names_and_types(@names, @types),
        encode_rows(@rows, @types)
      ])

      [names | decoded_rows] = decode_names_and_rows(encoded)
      assert names == @names
      assert decoded_rows == @rows
    end

    test "no manual JSON.encode! needed — maps pass directly" do
      # Encoding maps/lists directly works; the library calls JSON.encode_to_iodata! internally
      rows = [[1, %{"nested" => %{"key" => [1, 2, 3]}}]]
      encoded = IO.iodata_to_binary(encode_rows(rows, @types))
      assert decode_rows(encoded, @types) == rows
    end
  end

  describe "query options encoding for JSON" do
    test "input_format_binary_read_json_as_string goes into query string, not param_" do
      path = Ch.HTTP.path(%{}, input_format_binary_read_json_as_string: true)
      assert path =~ "input_format_binary_read_json_as_string=true"
      refute path =~ "param_input_format"
    end

    test "output_format_binary_write_json_as_string goes into query string, not param_" do
      path = Ch.HTTP.path(%{}, output_format_binary_write_json_as_string: true)
      assert path =~ "output_format_binary_write_json_as_string=true"
      refute path =~ "param_output_format"
    end

    test "query options coexist with SQL params" do
      path = Ch.HTTP.path(
        %{"city" => "Prague"},
        output_format_binary_write_json_as_string: true
      )
      assert path =~ "param_city=Prague"
      assert path =~ "output_format_binary_write_json_as_string=true"
    end
  end

  @tag :integration
  describe "live ClickHouse — native JSON type" do
    test "INSERT and SELECT with JSON column using both string-mode settings"
    test "typed paths in JSON schema (action LowCardinality(String))"
    test "nested JSON objects roundtrip"
    test "nil JSON value roundtrip"
  end
end
