defmodule Ch.Query do
  @moduledoc "Query struct wrapping the SQL statement."
  defstruct [:statement, :command, :encode, :decode]

  @type t :: %__MODULE__{statement: iodata, command: command, encode: boolean, decode: boolean}

  @doc false
  @spec build(iodata, [Ch.query_option()]) :: t
  def build(statement, opts \\ []) do
    command = Keyword.get(opts, :command) || extract_command(statement)
    encode = Keyword.get(opts, :encode, true)
    decode = Keyword.get(opts, :decode, true)
    %__MODULE__{statement: statement, command: command, encode: encode, decode: decode}
  end

  statements = [
    {"SELECT", :select},
    {"INSERT", :insert},
    {"CREATE", :create},
    {"ALTER", :alter},
    {"DELETE", :delete},
    {"SYSTEM", :system},
    {"SHOW", :show},
    # as of clickhouse 22.8, WITH is only allowed in SELECT
    # https://clickhouse.com/docs/en/sql-reference/statements/select/with/
    {"WITH", :select},
    {"GRANT", :grant},
    {"EXPLAIN", :explain},
    {"REVOKE", :revoke},
    {"ATTACH", :attach},
    {"CHECK", :check},
    {"DESCRIBE", :describe},
    {"DETACH", :detach},
    {"DROP", :drop},
    {"EXISTS", :exists},
    {"KILL", :kill},
    {"OPTIMIZE", :optimize},
    {"RENAME", :rename},
    {"EXCHANGE", :exchange},
    {"SET", :set},
    {"TRUNCATE", :truncate},
    {"USE", :use},
    {"WATCH", :watch}
  ]

  command_union =
    statements
    |> Enum.map(fn {_, command} -> command end)
    |> Enum.reduce(&{:|, [], [&1, &2]})

  @type command :: unquote(command_union)

  defp extract_command(statement)

  for {statement, command} <- statements do
    defp extract_command(unquote(statement) <> _), do: unquote(command)
    defp extract_command(unquote(String.downcase(statement)) <> _), do: unquote(command)
  end

  defp extract_command(<<whitespace, rest::bytes>>) when whitespace in [?\s, ?\t, ?\n] do
    extract_command(rest)
  end

  defp extract_command([first_segment | _] = statement) do
    extract_command(first_segment) || extract_command(IO.iodata_to_binary(statement))
  end

  defp extract_command(_other), do: nil
end

defimpl DBConnection.Query, for: Ch.Query do
  alias Ch.{Query, Result, RowBinary}

  @spec parse(Query.t(), [Ch.query_option()]) :: Query.t()
  def parse(query, _opts), do: query

  @spec describe(Query.t(), [Ch.query_option()]) :: Query.t()
  def describe(query, _opts), do: query

  # stream: insert init
  @spec encode(Query.t(), {:stream, term}, [Ch.query_option()]) ::
          {:stream, {[{String.t(), String.t()}], Mint.Types.headers(), iodata}}
  def encode(query, {:stream, params}, opts) do
    {:stream, encode(query, params, opts)}
  end

  # stream: insert data chunk
  @spec encode(Query.t(), {:stream, Mint.Types.request_ref(), iodata | :eof}, [Ch.query_option()]) ::
          {:stream, Mint.Types.request_ref(), iodata | :eof}
  def encode(_query, {:stream, ref, data}, _opts) do
    {:stream, ref, data}
  end

  @spec encode(Query.t(), params, [Ch.query_option()]) ::
          {query_params, Mint.Types.headers(), body}
        when params: map | [term] | [row :: [term]] | iodata | Enumerable.t(),
             query_params: [{String.t(), String.t()}],
             body: iodata | Enumerable.t()

  def encode(%Query{command: :insert, encode: false, statement: statement}, data, opts) do
    body =
      case data do
        _ when is_list(data) or is_binary(data) -> [statement, ?\n | data]
        _ -> Stream.concat([[statement, ?\n]], data)
      end

    {_query_params = [], headers(opts), body}
  end

  def encode(%Query{command: :insert, statement: statement}, params, opts) do
    cond do
      names = Keyword.get(opts, :names) ->
        types = Keyword.fetch!(opts, :types)
        header = RowBinary.encode_names_and_types(names, types)
        data = RowBinary.encode_rows(params, types)
        {_query_params = [], headers(opts), [statement, ?\n, header | data]}

      format_row_binary?(statement) ->
        types = Keyword.fetch!(opts, :types)
        data = RowBinary.encode_rows(params, types)
        {_query_params = [], headers(opts), [statement, ?\n | data]}

      true ->
        {query_params(params), headers(opts), statement}
    end
  end

  def encode(%Query{statement: statement}, params, opts) do
    types = Keyword.get(opts, :types)
    default_format = if types, do: "RowBinary", else: "RowBinaryWithNamesAndTypes"
    format = Keyword.get(opts, :format) || default_format
    {query_params(params), [{"x-clickhouse-format", format} | headers(opts)], statement}
  end

  defp format_row_binary?(statement) when is_binary(statement) do
    statement |> String.trim_trailing() |> String.ends_with?("RowBinary")
  end

  defp format_row_binary?(statement) when is_list(statement) do
    statement
    |> IO.iodata_to_binary()
    |> format_row_binary?()
  end

  # stream: select result
  @spec decode(Query.t(), result, [Ch.query_option()]) :: result when result: Result.t()
  def decode(_query, %Result{} = result, _opts), do: result
  # stream: insert result
  @spec decode(Query.t(), ref, [Ch.query_option()]) :: ref when ref: Mint.Types.request_ref()
  def decode(_query, ref, _opts) when is_reference(ref), do: ref

  @spec decode(Query.t(), [response], [Ch.query_option()]) :: Result.t()
        when response: Mint.Types.status() | Mint.Types.headers() | binary
  def decode(%Query{command: :insert}, responses, _opts) do
    [_status, headers | _data] = responses

    num_rows =
      if summary = get_header(headers, "x-clickhouse-summary") do
        summary = Jason.decode!(summary)

        if written_rows = Map.get(summary, "written_rows") do
          String.to_integer(written_rows)
        end
      end

    %Result{num_rows: num_rows, rows: nil, command: :insert, headers: headers}
  end

  def decode(%Query{decode: false, command: command}, responses, _opts) when is_list(responses) do
    # TODO potentially fails on x-progress-headers
    [_status, headers | data] = responses
    %Result{rows: data, data: data, command: command, headers: headers}
  end

  def decode(%Query{command: command}, responses, opts) when is_list(responses) do
    # TODO potentially fails on x-progress-headers
    [_status, headers | data] = responses

    case get_header(headers, "x-clickhouse-format") do
      "RowBinary" ->
        types = Keyword.fetch!(opts, :types)
        rows = data |> IO.iodata_to_binary() |> RowBinary.decode_rows(types)
        %Result{num_rows: length(rows), rows: rows, command: command, headers: headers}

      "RowBinaryWithNamesAndTypes" ->
        rows = data |> IO.iodata_to_binary() |> RowBinary.decode_rows()
        %Result{num_rows: length(rows), rows: rows, command: command, headers: headers}

      _other ->
        %Result{rows: data, data: data, command: command, headers: headers}
    end
  end

  defp get_header(headers, key) do
    case List.keyfind(headers, key, 0) do
      {_, value} -> value
      nil = not_found -> not_found
    end
  end

  defp query_params(params) when is_map(params) do
    Enum.map(params, fn {k, v} -> {"param_#{k}", encode_param(v)} end)
  end

  defp query_params(params) when is_list(params) do
    params
    |> Enum.with_index()
    |> Enum.map(fn {v, idx} -> {"param_$#{idx}", encode_param(v)} end)
  end

  defp encode_param(n) when is_integer(n), do: Integer.to_string(n)
  defp encode_param(f) when is_float(f), do: Float.to_string(f)

  # TODO possibly speed up
  # For more info see
  # https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters
  # "escaped" format is the same as https://clickhouse.com/docs/en/interfaces/formats#tabseparated-data-formatting
  defp encode_param(b) when is_binary(b) do
    escape_param([{"\\", "\\\\"}, {"\t", "\\\t"}, {"\n", "\\\n"}], b)
  end

  defp encode_param(b) when is_boolean(b), do: Atom.to_string(b)
  defp encode_param(%Decimal{} = d), do: Decimal.to_string(d, :normal)
  defp encode_param(%Date{} = date), do: Date.to_iso8601(date)
  defp encode_param(%NaiveDateTime{} = naive), do: NaiveDateTime.to_iso8601(naive)

  defp encode_param(%DateTime{time_zone: "Etc/UTC", microsecond: microsecond} = dt) do
    case microsecond do
      {val, precision} when val > 0 and precision > 0 ->
        size = round(:math.pow(10, precision))
        unix = DateTime.to_unix(dt, size)
        seconds = div(unix, size)
        fractional = rem(unix, size)

        IO.iodata_to_binary([
          Integer.to_string(seconds),
          ?.,
          String.pad_leading(Integer.to_string(fractional), precision, "0")
        ])

      _ ->
        dt |> DateTime.to_unix(:second) |> Integer.to_string()
    end
  end

  defp encode_param(%DateTime{} = dt) do
    raise ArgumentError, "non-UTC timezones are not supported for encoding: #{dt}"
  end

  defp encode_param(tuple) when is_tuple(tuple) do
    IO.iodata_to_binary([?(, encode_array_params(Tuple.to_list(tuple)), ?)])
  end

  defp encode_param(a) when is_list(a) do
    IO.iodata_to_binary([?[, encode_array_params(a), ?]])
  end

  defp encode_param(m) when is_map(m) do
    IO.iodata_to_binary([?{, encode_map_params(Map.to_list(m)), ?}])
  end

  defp encode_array_params([last]), do: encode_array_param(last)

  defp encode_array_params([s | rest]) do
    [encode_array_param(s), ?, | encode_array_params(rest)]
  end

  defp encode_array_params([] = empty), do: empty

  defp encode_map_params([last]), do: encode_map_param(last)

  defp encode_map_params([kv | rest]) do
    [encode_map_param(kv), ?, | encode_map_params(rest)]
  end

  defp encode_map_params([] = empty), do: empty

  defp encode_array_param(s) when is_binary(s) do
    [?', escape_param([{"'", "''"}, {"\\", "\\\\"}], s), ?']
  end

  defp encode_array_param(%s{} = param) when s in [Date, NaiveDateTime] do
    [?', encode_param(param), ?']
  end

  defp encode_array_param(v), do: encode_param(v)

  defp encode_map_param({k, v}) do
    [encode_array_param(k), ?:, encode_array_param(v)]
  end

  defp escape_param([{pattern, replacement} | escapes], param) do
    param = String.replace(param, pattern, replacement)
    escape_param(escapes, param)
  end

  defp escape_param([], param), do: param

  @spec headers(Keyword.t()) :: Mint.Types.headers()
  defp headers(opts), do: Keyword.get(opts, :headers, [])
end

defimpl String.Chars, for: Ch.Query do
  def to_string(%{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
