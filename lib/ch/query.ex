defmodule Ch.Query do
  @moduledoc "Query struct wrapping the SQL statement."
  defstruct [:statement, :command, :encode, :decode, :multipart]

  @typedoc """
  The Query struct.

  ## Fields

    * `:statement` - The SQL statement to be executed (as `t:iodata/0`).
    * `:command` - The detected or enforced SQL command type (e.g., `:select`, `:insert`).
    * `:encode` - Whether to encode parameters (defaults to `true`).
    * `:decode` - Whether to decode the response (defaults to `true`).
    * `:multipart` - Whether to use `multipart/form-data` for the request (defaults to `false`).
  """
  @type t :: %__MODULE__{
          statement: iodata,
          command: command,
          encode: boolean,
          decode: boolean,
          multipart: boolean
        }

  @doc false
  @spec build(iodata, [Ch.query_option()]) :: t
  def build(statement, opts \\ []) do
    command = Keyword.get(opts, :command) || extract_command(statement)
    encode = Keyword.get(opts, :encode, true)
    decode = Keyword.get(opts, :decode, true)
    multipart = Keyword.get(opts, :multipart, false)

    %__MODULE__{
      statement: statement,
      command: command,
      encode: encode,
      decode: decode,
      multipart: multipart
    }
  end

  statements = [
    {"SELECT", :select},
    {"INSERT", :insert},
    {"CREATE", :create},
    {"ALTER", :alter},
    {"DELETE", :delete},
    {"SYSTEM", :system},
    {"SHOW", :show},
    # as of ClickHouse 24.11, WITH is only allowed in SELECT
    # https://clickhouse.com/docs/en/sql-reference/statements/select/with/
    {"WITH", :select},
    {"GRANT", :grant},
    {"EXPLAIN", :explain},
    {"REVOKE", :revoke},
    {"UPDATE", :update},
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
    {"WATCH", :watch},
    {"MOVE", :move},
    {"UNDROP", :undrop}
  ]

  command_union =
    statements
    |> Enum.map(fn {_, command} -> command end)
    |> Enum.reduce(&{:|, [], [&1, &2]})

  @typedoc """
  Atom representing the type of SQL command.

  Derived automatically from the start of the SQL statement (e.g., `"SELECT ..."` -> `:select`),
  or provided explicitly via options.
  """
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
  @dialyzer :no_improper_lists
  alias Ch.{Query, Result, RowBinary}
  alias Ch.Encode.{Multipart, Parameters}

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
        {Parameters.encode_many(params), headers(opts), statement}
    end
  end

  def encode(%Query{multipart: true, statement: statement}, params, opts) do
    types = Keyword.get(opts, :types)
    default_format = if types, do: "RowBinary", else: "RowBinaryWithNamesAndTypes"
    format = Keyword.get(opts, :format) || default_format

    boundary = "ChFormBoundary" <> Base.url_encode64(:crypto.strong_rand_bytes(24))
    content_type = "multipart/form-data; boundary=\"#{boundary}\""
    enc_boundary = "--#{boundary}\r\n"
    multipart = multipart_params(params, enc_boundary)
    multipart = add_multipart_part(multipart, "query", statement, enc_boundary)
    multipart = [multipart | "--#{boundary}--\r\n"]

    {_no_query_params = [],
     [{"x-clickhouse-format", format}, {"content-type", content_type} | headers(opts)], multipart}
  end

  def encode(%Query{statement: statement}, params, opts) do
    Multipart.encode(statement, params, opts)
  end

  defp multipart_params(params, boundary) when is_map(params) do
    multipart_named_params(Map.to_list(params), boundary, [])
  end

  defp multipart_params(params, boundary) when is_list(params) do
    multipart_positional_params(params, 0, boundary, [])
  end

  defp multipart_named_params([{name, value} | params], boundary, acc) do
    acc =
      add_multipart_part(
        acc,
        "param_" <> URI.encode_www_form(name),
        encode_param(value),
        boundary
      )

    multipart_named_params(params, boundary, acc)
  end

  defp multipart_named_params([], _boundary, acc), do: acc

  defp multipart_positional_params([value | params], idx, boundary, acc) do
    acc =
      add_multipart_part(
        acc,
        "param_$" <> Integer.to_string(idx),
        encode_param(value),
        boundary
      )

    multipart_positional_params(params, idx + 1, boundary, acc)
  end

  defp multipart_positional_params([], _idx, _boundary, acc), do: acc

  @compile inline: [add_multipart_part: 4]
  defp add_multipart_part(multipart, name, value, boundary) do
    part = [
      boundary,
      "content-disposition: form-data; name=\"",
      name,
      "\"\r\n\r\n",
      value,
      "\r\n"
    ]

    case multipart do
      [] -> part
      _ -> [multipart | part]
    end
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
        [names | rows] = data |> IO.iodata_to_binary() |> RowBinary.decode_names_and_rows()

        %Result{
          num_rows: length(rows),
          columns: names,
          rows: rows,
          command: command,
          headers: headers
        }

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

  @spec headers(Keyword.t()) :: Mint.Types.headers()
  defp headers(opts), do: Keyword.get(opts, :headers, [])
end

defimpl String.Chars, for: Ch.Query do
  def to_string(%{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
