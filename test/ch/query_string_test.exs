defmodule Ch.QueryStringTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  describe "path/2" do
    property "starts with / and only adds ? when there is a query string" do
      check all params <- params(),
                options <- query_options() do
        path = Ch.HTTP.path(params, options)
        query = URI.parse(path).query

        if params == %{} and Enum.empty?(options) do
          assert path == "/"
          assert query == nil
        else
          assert path =~ ~r"^/\\?"
          assert query != nil
          refute String.ends_with?(path, "?")
          refute String.ends_with?(path, "&")
          refute path =~ "?&"
        end
      end
    end

    property "encodes params with param_ prefix and leaves options unprefixed" do
      check all params <- params(),
                options <- query_options() do
        decoded = Ch.HTTP.path(params, options) |> query_string() |> URI.decode_query()

        for {key, value} <- params do
          assert decoded["param_#{key}"] == expected_param(value)
        end

        for {key, value} <- options do
          assert decoded[to_string(key)] == to_string(value)
        end
      end
    end

    property "accepts maps and keyword lists for options" do
      check all options <- query_options() do
        map_options = Map.new(options)

        assert Ch.HTTP.path(%{}, options) |> query_string() |> URI.decode_query() ==
                 Ch.HTTP.path(%{}, map_options) |> query_string() |> URI.decode_query()
      end
    end
  end

  describe "param encoding" do
    property "escapes top-level string params as ClickHouse escaped text" do
      check all string <- safe_string() do
        decoded = decoded_param(string)

        assert decoded == expected_param(string)

        assert decoded ==
                 string
                 |> String.replace("\\", "\\\\")
                 |> String.replace("\t", "\\\t")
                 |> String.replace("\n", "\\\n")
      end
    end

    property "encodes scalar params" do
      check all value <- scalar_param() do
        assert decoded_param(value) == expected_param(value)
      end
    end

    property "encodes array params" do
      check all values <- list_of(array_scalar(), max_length: 8) do
        assert decoded_param(values) == expected_param(values)
      end
    end

    property "encodes tuple params like arrays with parentheses" do
      check all values <- list_of(array_scalar(), max_length: 8) do
        tuple = List.to_tuple(values)
        assert decoded_param(tuple) == expected_param(tuple)
      end
    end

    property "encodes map params as ClickHouse map literals" do
      check all entries <- uniq_list_of({safe_string(), array_scalar()}, max_length: 8) do
        map = Map.new(entries)
        assert decoded_param(map) == expected_param(map)
      end
    end
  end

  describe "ClickHouse round-trip" do
    setup do
      {:ok, pool: start_supervised!(Ch)}
    end

    # For more info see
    # https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters
    # "escaped" format is the same as https://clickhouse.com/docs/en/interfaces/formats#tabseparated-data-formatting
    property "string parameters round-trip through ClickHouse", %{pool: pool} do
      check all string <- safe_string() do
        assert Ch.query!(pool, "select {s:String}", %{"s" => string}).rows == [[string]]
      end
    end

    test "string parameters are escaped", %{pool: pool} do
      for s <- ["\t", "\n", "\\", "'", "\b", "\f", "\r", "\0"] do
        assert Ch.query!(pool, "select {s:String}", %{"s" => s}).rows == [[s]]
      end

      assert Ch.query!(pool, "select splitByChar('\t', 'abc\t123')").rows ==
               [[["abc", "123"]]]

      assert Ch.query!(pool, "select splitByChar('\t', {arg1:String})", %{"arg1" => "abc\t123"}).rows ==
               [[["abc", "123"]]]
    end
  end

  defp query_string(path) do
    case URI.parse(path).query do
      nil -> ""
      query -> query
    end
  end

  defp decoded_param(value) do
    Ch.HTTP.path(%{"value" => value})
    |> query_string()
    |> URI.decode_query()
    |> Map.fetch!("param_value")
  end

  defp params do
    map_of(safe_key(), param(), max_length: 8)
  end

  defp query_options do
    map(
      map_of(safe_key(), one_of([integer(), boolean(), safe_string()]), max_length: 8),
      &Map.to_list/1
    )
  end

  defp param do
    one_of([
      scalar_param(),
      list_of(array_scalar(), max_length: 5),
      map_of(safe_string(), array_scalar(), max_length: 5)
    ])
  end

  defp scalar_param do
    one_of([
      integer(),
      float(),
      boolean(),
      constant(nil),
      safe_string(),
      decimal_gen(),
      date_gen(),
      naive_datetime_gen(),
      time_gen()
    ])
  end

  defp array_scalar do
    one_of([
      integer(),
      float(),
      boolean(),
      constant(nil),
      safe_string(),
      decimal_gen(),
      date_gen(),
      naive_datetime_gen(),
      time_gen()
    ])
  end

  defp safe_key do
    string(:alphanumeric, min_length: 1, max_length: 16)
  end

  defp safe_string do
    string(:printable, max_length: 32)
  end

  defp decimal_gen do
    gen all sign <- member_of([1, -1]),
            coef <- integer(0..999_999),
            exp <- integer(-8..8) do
      Decimal.new(sign, coef, exp)
    end
  end

  defp date_gen do
    gen all days <- integer(0..36_500) do
      Date.add(~D[1970-01-01], days)
    end
  end

  defp naive_datetime_gen do
    gen all date <- date_gen(),
            hour <- integer(0..23),
            minute <- integer(0..59),
            second <- integer(0..59),
            microsecond <- integer(0..999_999) do
      NaiveDateTime.new!(date, Time.new!(hour, minute, second, {microsecond, 6}))
    end
  end

  defp time_gen do
    gen all hour <- integer(0..23),
            minute <- integer(0..59),
            second <- integer(0..59),
            microsecond <- integer(0..999_999) do
      Time.new!(hour, minute, second, {microsecond, 6})
    end
  end

  defp expected_param(value) when is_integer(value), do: Integer.to_string(value)
  defp expected_param(value) when is_float(value), do: Float.to_string(value)
  defp expected_param(value) when is_boolean(value), do: Atom.to_string(value)
  defp expected_param(nil), do: "\\N"
  defp expected_param(%Decimal{} = value), do: Decimal.to_string(value, :scientific)
  defp expected_param(%Date{} = value), do: Date.to_iso8601(value)
  defp expected_param(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp expected_param(%Time{} = value), do: Time.to_iso8601(value)

  defp expected_param(value) when is_binary(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("\t", "\\\t")
    |> String.replace("\n", "\\\n")
  end

  defp expected_param(value) when is_tuple(value) do
    "(" <> (value |> Tuple.to_list() |> Enum.map_join(",", &expected_array_param/1)) <> ")"
  end

  defp expected_param(value) when is_list(value) do
    "[" <> Enum.map_join(value, ",", &expected_array_param/1) <> "]"
  end

  defp expected_param(value) when is_map(value) do
    "{" <> (value |> Map.to_list() |> Enum.map_join(",", &expected_map_param/1)) <> "}"
  end

  defp expected_array_param(value) when is_binary(value) do
    "'" <> (value |> String.replace("'", "''") |> String.replace("\\", "\\\\")) <> "'"
  end

  defp expected_array_param(nil), do: "null"

  defp expected_array_param(%value{} = param) when value in [Date, NaiveDateTime],
    do: "'" <> expected_param(param) <> "'"

  defp expected_array_param(value), do: expected_param(value)

  defp expected_map_param({key, value}) do
    expected_array_param(key) <> ":" <> expected_array_param(value)
  end
end
