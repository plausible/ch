defmodule Ch.HTTP do
  @moduledoc """
  Stateless helpers for `Mint.HTTP1` with ClickHouse-specific encoding and decoding.

  Provides three layers of functionality:

    1. **Deadline / timeout helpers** — convert between relative millisecond timeouts
       and absolute monotonic deadlines, so a single deadline propagates correctly
       across multiple network calls.

    2. **Request encoding** — build a `{path, headers, body}` triple ready for
       `Mint.HTTP1.request/5`. Parameter binding is handled transparently.

    3. **Response decoding** — single-shot (`decode/3`) or streaming
       (`decode_start/1` + `decode_continue/2`) decoding of ClickHouse HTTP responses.

  The caller retains full control of the connection lifecycle and the HTTP method.
  Body compression is the caller's responsibility: compress `body` manually and pass
  `{"content-encoding", "gzip"}` in `opts[:headers]`. Responses with
  `content-encoding: gzip` are decompressed automatically by `decode/3`.

  ## Single-shot usage

      deadline = Ch.HTTP.to_deadline(to_timeout(second: 15))

      {:ok, conn} =
        Mint.HTTP1.connect(:http, "localhost", 8123,
          mode: :passive,
          timeout: Ch.HTTP.to_timeout(deadline)
        )

      try do
        {path, headers, body} = Ch.HTTP.encode("CREATE TABLE demo(a Int64) ENGINE Null")

        with {:ok, _ref, conn} <- Mint.HTTP1.request(conn, "POST", path, headers, body),
             {:ok, {status, headers, body}, conn} <- Ch.HTTP.recv_all(conn, deadline),
             :ok <- Ch.HTTP.decode(status, headers, body) do
          :ok
        end
      after
        Mint.HTTP1.close(conn)
      end

  ## Streaming

  For large result sets, use `decode_start/1` + `decode_continue/2` to process rows
  as Mint data chunks arrive, without buffering the entire response body. The caller
  handles `:status` and `:headers` responses, then passes only data to the decoder:

      # active-mode receive loop (passive mode: same but with Mint.HTTP1.recv/3)
      receive do
        message ->
          {:ok, conn, responses} = Mint.HTTP1.stream(conn, message)

          Enum.reduce(responses, state, fn
            {:status, _ref, _status}, state ->
              state

            {:headers, _ref, headers}, _state ->
              Ch.HTTP.decode_start(headers)

            {:data, _ref, chunk}, state ->
              case Ch.HTTP.decode_continue(chunk, state) do
                {:rows, rows, names, state} -> process_rows(rows, names); state
                {:more, state} -> state
              end

            {:done, _ref}, state ->
              {:ok, names, rows} = Ch.HTTP.decode_continue(:end_of_input, state)
              done(names, rows)
          end)
      end
  """

  import Kernel, except: [to_timeout: 1]

  @typedoc """
  Represents a deadline for an operation.

  Either `:infinity` or `{:deadline, timestamp}` where `timestamp` is an absolute
  time in milliseconds from `System.monotonic_time(:millisecond)`.
  """
  @type deadline :: {:deadline, integer} | :infinity

  @typedoc """
  Opaque streaming decoder state.

  Returned by `decode_start/1` and updated by each call to `decode_continue/2`.
  """
  @opaque decode_state ::
            {:awaiting_rb_header, buf :: binary}
            | {:decoding_rows, names :: [String.t()], types :: [term], row_state :: term,
               remainder :: binary}
            | {:raw, acc :: iodata}

  @doc """
  Converts a relative timeout (milliseconds) or existing `t:deadline/0` to a `t:deadline/0`.

  Passing an already-converted `{:deadline, _}` tuple is a no-op, making this safe to
  call at multiple layers of the call stack without double-adding the offset.
  """
  @spec to_deadline(timeout | deadline) :: deadline
  def to_deadline(:infinity), do: :infinity
  def to_deadline({:deadline, _timestamp} = deadline), do: deadline

  def to_deadline(timeout) when is_integer(timeout) do
    {:deadline, System.monotonic_time(:millisecond) + timeout}
  end

  @doc """
  Returns the remaining milliseconds until `deadline`, suitable for passing to Mint.

  Always returns `>= 0`; clamps to `0` if the deadline has already passed (Mint does
  not accept negative timeouts).
  """
  @spec to_timeout(timeout | deadline) :: timeout
  def to_timeout(:infinity), do: :infinity
  def to_timeout(timeout) when is_integer(timeout), do: timeout

  def to_timeout({:deadline, timestamp}) do
    max(0, timestamp - System.monotonic_time(:millisecond))
  end

  @doc """
  Encodes a ClickHouse HTTP request with no parameters.

  Equivalent to `encode(statement, %{}, [])`.
  """
  @spec encode(statement :: iodata) ::
          {path :: String.t(), headers :: Mint.Types.headers(), body :: iodata}
  def encode(statement) do
    encode(statement, %{}, [])
  end

  @doc """
  Encodes a ClickHouse HTTP request with parameters and options.

  Returns `{path, headers, body}` ready for `Mint.HTTP1.request/5`. The HTTP method
  (`"POST"`) and connection lifecycle remain the caller's responsibility.

  ## Parameters

  Parameters are encoded using ClickHouse's
  [escaped HTTP format](https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters),
  which follows the same escaping rules as ClickHouse's TSV format: tab (`\\t`),
  newline (`\\n`), and backslash (`\\`) are backslash-escaped.

    * **Named** — `%{"city" => "Prague"}` → `?param_city=Prague`
    * **Positional** — `["Prague", 42]` → `?param_$0=Prague&param_$1=42`

  ## Options

    * `:headers` — additional Mint-style headers forwarded verbatim, e.g.
      `[{"x-clickhouse-user", "alice"}, {"x-clickhouse-key", "secret"}]`.

  ## Body

  The returned `body` is the `statement` iodata unchanged. No RowBinary encoding or
  compression is applied — those are the caller's responsibility:

      compressed = :zlib.compress(IO.iodata_to_binary(statement))
      {path, headers, body} =
        Ch.HTTP.encode(compressed, %{}, headers: [{"content-encoding", "gzip"}])
  """
  @spec encode(
          statement :: iodata,
          params :: %{String.t() => term} | [term],
          opts :: keyword
        ) ::
          {path :: String.t(), headers :: Mint.Types.headers(), body :: iodata}
  def encode(statement, params, opts) do
    query_params = encode_params(params)

    path =
      case query_params do
        [] -> "/"
        _ -> "/?" <> URI.encode_query(query_params)
      end

    headers = Keyword.get(opts, :headers, [])
    {path, headers, statement}
  end

  @doc """
  Receives a complete HTTP response from a passive `Mint.HTTP1` connection.

  Accumulates all Mint response messages until `{:done, ref}` and returns the
  raw `{status, headers, body}` triple, which can be passed directly to `decode/3`.

  Accepts a plain timeout in milliseconds or a `t:deadline/0`. When given a deadline,
  the remaining time is recomputed before each `Mint.HTTP1.recv/3` call.
  """
  @spec recv_all(Mint.HTTP1.t(), timeout | deadline) ::
          {:ok, {status :: non_neg_integer, Mint.Types.headers(), body :: binary}, Mint.HTTP1.t()}
          | {:error, Mint.HTTP1.t(), Mint.Types.error()}
  def recv_all(conn, timeout_or_deadline) do
    deadline = to_deadline(timeout_or_deadline)
    do_recv_all(conn, _status = nil, _headers = [], _data = [], deadline)
  end

  defp do_recv_all(conn, status, headers, data, deadline) do
    case Mint.HTTP1.recv(conn, 0, to_timeout(deadline)) do
      {:ok, conn, responses} ->
        case handle_responses(responses, status, headers, data) do
          {:ok, status, headers, body} ->
            {:ok, {status, headers, body}, conn}

          {:more, status, headers, data} ->
            do_recv_all(conn, status, headers, data, deadline)

          {:error, reason} ->
            {:error, conn, reason}
        end

      {:error, conn, reason, _responses} ->
        {:error, conn, reason}
    end
  end

  @dialyzer {:no_improper_lists, handle_responses: 4}
  defp handle_responses([{:status, _ref, status} | rest], _status, headers, data) do
    handle_responses(rest, status, headers, data)
  end

  defp handle_responses([{:headers, _ref, new_headers} | rest], status, prev_headers, data) do
    handle_responses(rest, status, prev_headers ++ new_headers, data)
  end

  defp handle_responses([{:data, _ref, new_data} | rest], status, headers, prev_data) do
    handle_responses(rest, status, headers, [prev_data | new_data])
  end

  defp handle_responses([{:done, _ref} | _rest], status, headers, data) do
    {:ok, status, headers, IO.iodata_to_binary(data)}
  end

  defp handle_responses([{:error, _ref, reason} | _rest], _status, _headers, _data) do
    {:error, reason}
  end

  defp handle_responses([], status, headers, data) do
    {:more, status, headers, data}
  end

  @doc """
  Decodes a complete ClickHouse HTTP response.

  Accepts the `{status, headers, body}` triple returned by `recv_all/2`.
  Handles errors, decompression, and `RowBinaryWithNamesAndTypes` decoding.

    * Non-200 status → `{:error, Ch.Error.t()}` with code and message from ClickHouse.
    * `content-encoding: gzip` → automatically decompressed before parsing.
    * `x-clickhouse-format: RowBinaryWithNamesAndTypes` → `{:ok, names, rows}`.
    * Empty body (DDL, INSERT without result) → `:ok`.
    * Other or absent format → `{:ok, [], [body]}` with the raw binary.

  ## Example

      {:ok, {status, headers, body}, conn} = Ch.HTTP.recv_all(conn, deadline)
      case Ch.HTTP.decode(status, headers, body) do
        {:ok, names, rows} -> ...
        :ok -> ...
        {:error, error} -> ...
      end
  """
  @spec decode(
          status :: non_neg_integer,
          headers :: Mint.Types.headers(),
          body :: binary
        ) ::
          :ok
          | {:ok, names :: [String.t()], rows :: [[term]]}
          | {:error, Ch.Error.t()}
  def decode(status, headers, body)

  def decode(status, headers, body) when status != 200 do
    code =
      case get_header(headers, "x-clickhouse-exception-code") do
        nil -> nil
        code -> String.to_integer(code)
      end

    {:error, Ch.Error.exception(code: code, message: body)}
  end

  def decode(200, headers, body) do
    body = maybe_decompress(body, get_header(headers, "content-encoding"))

    case get_header(headers, "x-clickhouse-format") do
      "RowBinaryWithNamesAndTypes" ->
        [names | rows] = Ch.RowBinary.decode_names_and_rows(body)
        {:ok, names, rows}

      _other ->
        case body do
          "" -> :ok
          _ -> {:ok, [], [body]}
        end
    end
  end

  @doc """
  Initialises a streaming ClickHouse response decoder from response headers.

  Inspects `x-clickhouse-format` to determine how to decode incoming data chunks.
  The returned `t:decode_state/0` is passed to `decode_continue/2` along with each
  binary chunk extracted from `{:data, ref, chunk}` Mint responses.

  The caller is responsible for handling `{:status, _, _}` and `{:headers, _, _}`
  responses before calling `decode_start/1`, and for passing `{:done, _}` as
  `:end_of_input` to `decode_continue/2`.

  ## Example

      {:headers, _ref, headers} = ... # from Mint.HTTP1.stream/2
      state = Ch.HTTP.decode_start(headers)

      {:data, _ref, chunk} = ...
      case Ch.HTTP.decode_continue(chunk, state) do
        {:rows, rows, names, state} -> ...
        {:more, state} -> ...
      end

      {:done, _ref} = ...
      {:ok, names, []} = Ch.HTTP.decode_continue(:end_of_input, state)
  """
  @spec decode_start(headers :: Mint.Types.headers()) :: decode_state
  def decode_start(headers) do
    case get_header(headers, "x-clickhouse-format") do
      "RowBinaryWithNamesAndTypes" -> {:awaiting_rb_header, <<>>}
      _other -> {:raw, []}
    end
  end

  @doc """
  Feeds a binary chunk into a streaming decoder, advancing its state.

  Pass binary chunks extracted from `{:data, ref, chunk}` Mint response tuples.
  When the response is complete (`:done` received from Mint), pass `:end_of_input`
  to finalise and retrieve any remaining output.

  ## Return values

    * `{:rows, rows, names, state}` — one or more complete rows decoded. `names` is
      the list of column names from the `RowBinaryWithNamesAndTypes` header.
      Continue calling `decode_continue/2` with the next chunk.
    * `{:more, state}` — chunk consumed, no complete rows yet (e.g. still accumulating
      the RowBinary header). Continue with the next chunk.
    * `{:ok, names, rows}` — stream complete. If rows were emitted incrementally via
      `{:rows, ...}`, the final `rows` list here will be empty.
    * `{:error, Ch.Error.t()}` — decoding failed.
  """
  @spec decode_continue(chunk :: binary | :end_of_input, decode_state) ::
          {:rows, rows :: [[term]], names :: [String.t()], decode_state}
          | {:more, decode_state}
          | {:ok, names :: [String.t()], rows :: [[term]]}
          | {:error, Ch.Error.t()}
  def decode_continue(:end_of_input, state) do
    flush_state(state)
  end

  def decode_continue(chunk, {:awaiting_rb_header, buf}) when is_binary(chunk) do
    buf = buf <> chunk

    case Ch.RowBinary.decode_header(buf) do
      :more ->
        {:more, {:awaiting_rb_header, buf}}

      {:ok, names, types, rest} ->
        {rows, remainder, row_state} = Ch.RowBinary.decode_rows_continue(rest, types, nil)
        new_state = {:decoding_rows, names, types, row_state, remainder}

        case rows do
          [] -> {:more, new_state}
          _ -> {:rows, rows, names, new_state}
        end
    end
  end

  def decode_continue(chunk, {:decoding_rows, names, types, row_state, remainder})
      when is_binary(chunk) do
    {rows, new_remainder, new_row_state} =
      Ch.RowBinary.decode_rows_continue(remainder <> chunk, types, row_state)

    new_state = {:decoding_rows, names, types, new_row_state, new_remainder}

    case rows do
      [] -> {:more, new_state}
      _ -> {:rows, rows, names, new_state}
    end
  end

  def decode_continue(chunk, {:raw, acc}) when is_binary(chunk) do
    {:more, {:raw, [acc | chunk]}}
  end

  defp flush_state({:awaiting_rb_header, <<>>}) do
    {:ok, [], []}
  end

  defp flush_state({:awaiting_rb_header, _buf}) do
    {:error,
     Ch.Error.exception(code: nil, message: "incomplete RowBinaryWithNamesAndTypes header")}
  end

  defp flush_state({:decoding_rows, names, _types, _row_state, _remainder}) do
    # All rows already emitted via {:rows, ...} during streaming
    {:ok, names, []}
  end

  defp flush_state({:raw, acc}) do
    case IO.iodata_to_binary(acc) do
      "" -> {:ok, [], []}
      body -> {:ok, [], [body]}
    end
  end

  ## Private helpers

  defp maybe_decompress(body, "gzip"), do: :zlib.gunzip(body)
  defp maybe_decompress(body, "zstd"), do: :zstd.decompress(body)
  defp maybe_decompress(body, _encoding), do: body


  defp get_header(headers, key) do
    case List.keyfind(headers, key, 0) do
      {_, value} -> value
      nil -> nil
    end
  end

  # Encodes query parameters for ClickHouse HTTP URL binding.
  #
  # ClickHouse uses an "escaped" parameter format identical to its TSV format escaping
  # (see https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters):
  # tab (\t), newline (\n), and backslash (\) are backslash-escaped.
  #
  # Named params:      %{"city" => "Prague"} → [{"param_city", "Prague"}]
  # Positional params: ["Prague", 42]        → [{"param_$0", "Prague"}, {"param_$1", "42"}]
  defp encode_params(params) when is_map(params) do
    Enum.map(params, fn {k, v} -> {"param_#{k}", encode_param(v)} end)
  end

  defp encode_params(params) when is_list(params) do
    params
    |> Enum.with_index()
    |> Enum.map(fn {v, idx} -> {"param_$#{idx}", encode_param(v)} end)
  end

  defp encode_param(n) when is_integer(n), do: Integer.to_string(n)
  defp encode_param(f) when is_float(f), do: Float.to_string(f)

  defp encode_param(b) when is_binary(b) do
    escape_param([{"\\", "\\\\"}, {"\t", "\\\t"}, {"\n", "\\\n"}], b)
  end

  defp encode_param(b) when is_boolean(b), do: Atom.to_string(b)
  defp encode_param(nil), do: "\\N"
  defp encode_param(%Decimal{} = d), do: Decimal.to_string(d, :normal)
  defp encode_param(%Date{} = date), do: Date.to_iso8601(date)
  defp encode_param(%NaiveDateTime{} = naive), do: NaiveDateTime.to_iso8601(naive)
  defp encode_param(%Time{} = time), do: Time.to_iso8601(time)

  defp encode_param(%DateTime{microsecond: microsecond} = dt) do
    dt = DateTime.shift_zone!(dt, "Etc/UTC")

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

  defp encode_array_param(nil), do: "null"

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
end
