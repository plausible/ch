defmodule Ch.HTTP do
  @moduledoc """
  Stateless helpers for `Mint.HTTP1` with ClickHouse-specific encoding and decoding.
  """

  import Kernel, except: [to_timeout: 1]

  @typedoc """
  Represents a deadline for an operation.

  Either `:infinity` or `{:deadline, timestamp}` where `timestamp` is an absolute
  time in milliseconds from `System.monotonic_time(:millisecond)`.
  """
  @type deadline :: {:deadline, integer} | :infinity

  @doc """
  Converts a relative timeout (milliseconds) to a `t:deadline/0`.
  """
  @spec to_deadline(timeout | deadline) :: deadline
  def to_deadline(:infinity), do: :infinity
  def to_deadline({:deadline, _timestamp} = deadline), do: deadline

  def to_deadline(timeout) when is_integer(timeout) do
    {:deadline, System.monotonic_time(:millisecond) + timeout}
  end

  @doc """
  Returns the remaining milliseconds until a `t:deadline/0`.
  """
  @spec to_timeout(timeout | deadline) :: timeout
  def to_timeout(:infinity), do: :infinity
  def to_timeout(timeout) when is_integer(timeout), do: timeout

  def to_timeout({:deadline, timestamp}) do
    max(0, timestamp - System.monotonic_time(:millisecond))
  end

  @doc """
  Builds the request path for a ClickHouse HTTP request.

  ### Examples

      iex> Ch.HTTP.path(%{})
      "/"

      iex> Ch.HTTP.path(%{"city" => "Prague"})
      "/?param_city=Prague"

      iex> Ch.HTTP.path(%{}, output_format_binary_write_json_as_string: true)
      "/?output_format_binary_write_json_as_string=true"

      iex> Ch.HTTP.path(%{"city" => "Prague"}, %{"query_id" => "550e8400"})
      "/?param_city=Prague&query_id=550e8400"

  """
  @spec path(%{String.t() => term}, Enumerable.t()) :: String.t()
  def path(params, options \\ []) do
    case encode_params(params) ++ options do
      [] -> "/"
      qp -> "/?" <> URI.encode_query(qp)
    end
  end

  @doc """
  Initialises a streaming ClickHouse response decoder.

  Accepts an optional `decoders` map, mapping from format name to a decoder function.

  Only `RowBinaryWithNamesAndTypes` format is supported by default. For all other formats,
  the data is left as is.
  """
  def decode_start(opts \\ []) do
    decoders =
      Keyword.get(opts, :decoders, %{
        "RowBinaryWithNamesAndTypes" => &__MODULE__.decode_rowbinary_stream/2,
        :_ => &__MODULE__.decode_raw_stream/2
      })

    {:init, decoders}
  end

  @doc false
  def decode_rowbinary_stream(new_data, {:rows, names, types, prev_data, state}) do
    data = prev_data <> new_data
    {rows, rest, state} = Ch.RowBinary.decode_rows_continue(data, types, state)
    {:more, %{names: names, rows: rows}, {:rows, names, types, rest, state}}
  end

  def decode_rowbinary_stream(new_data, state) do
    data =
      case state do
        :init -> new_data
        {:header, prev_data} -> prev_data <> new_data
      end

    case Ch.RowBinary.decode_header(data) do
      :more -> {:more, [], {:header, data}}
      {:ok, names, types, rest} -> decode_rowbinary_stream(rest, {:rows, names, types, rest, nil})
    end
  end

  @doc false
  def decode_raw_stream(data, state) do
    {:more, data, state}
  end

  @doc """
  Feeds a Mint response tuple into the streaming decoder.

  This function handles the entire Mint response lifecycle (`:status`, `:headers`,
  `:data`, `:done`, `:error`) for a single request.
  """
  @spec decode_continue(Mint.Types.response(), decoder) ::
          :ok
          | {:more, decoded, decoder}
          | {:error, error}
          | :done
        when decoded: term,
             decoder: term,
             error: Mint.Types.error() | Ch.Error.t()
  def decode_continue(response, decoder)

  def decode_continue({:status, _ref, status}, {:init, decoders}) do
    {:cont, {:status, status, decoders}}
  end

  def decode_continue({:headers, _ref, headers}, {:status, 200, decoders}, ) do
    format = get_header(headers, "x-clickhouse-format")

    state =
      cond do
        format == "RowBinaryWithNamesAndTypes" ->
          {:rowbinary, <<>>}

        format == nil ->
          {:empty}

        decoder = decoders[format] ->
          {:custom, decoder, decoder.decode_start(headers)}

        true ->
          {:unknown_format, format}
      end

    {:cont, state}
  end

  def decode_continue({:headers, _ref, headers}, {:status, _status, decoders}) do
    code =
      if code = get_header(headers, "x-clickhouse-exception-code") do
        String.to_integer(code)
      end

    {:cont, {:error_body, status, code, []}}
  end

  def decode_continue({:data, _ref, chunk}, decoder) do
    decode_continue_data(state, chunk)
  end

  def decode_continue({:done, _ref}, decoder) do
    decode_continue_data(state, :done)
  end

  def decode_continue({:error, _ref, reason}, _decoder) do
    {:error, reason}
  end

  defp decode_continue_data(state, chunk_or_done)

  defp decode_continue_data({:custom, decoder, state}, chunk_or_done) do
    case decoder.decode_continue(state, chunk_or_done) do
      {:rows, rows, names, new_state} -> {:rows, rows, names, {:custom, decoder, new_state}}
      {:cont, new_state} -> {:cont, {:custom, decoder, new_state}}
      {:ok, names, rows} -> {:ok, names, rows}
      :ok -> :ok
      {:error, error} -> {:error, error}
    end
  end

  # --- :done (finalise) ---

  # empty body before RowBinary header — DDL/INSERT sent with wrong format header?
  defp decode_continue_data({:rowbinary, <<>>}, :done), do: :ok

  defp decode_continue_data({:rowbinary, _buf}, :done) do
    {:error,
     Ch.Error.exception(code: nil, message: "incomplete RowBinaryWithNamesAndTypes header")}
  end

  defp decode_continue_data({:decoding_rows, names, _types, _row_state, _remainder}, :done) do
    # all rows emitted via {:rows, ...} during streaming
    {:ok, names, []}
  end

  defp decode_continue_data({:empty}, :done), do: :ok

  defp decode_continue_data({:unknown_format, format}, :done) do
    {:error, {:unknown_format, format}}
  end

  defp decode_continue_data({:error_body, _status, code, acc}, :done) do
    {:error, Ch.Error.exception(code: code, message: IO.iodata_to_binary(acc))}
  end

  # --- binary chunks ---

  defp decode_continue_data({:rowbinary, buf}, chunk) when is_binary(chunk) do
    buf = buf <> chunk

    case Ch.RowBinary.decode_header(buf) do
      :more ->
        {:cont, {:rowbinary, buf}}

      {:ok, names, types, rest} ->
        {rows, remainder, row_state} = Ch.RowBinary.decode_rows_continue(rest, types, nil)
        new_state = {:decoding_rows, names, types, row_state, remainder}

        case rows do
          [] -> {:cont, new_state}
          _ -> {:rows, rows, names, new_state}
        end
    end
  end

  defp decode_continue_data({:decoding_rows, names, types, row_state, remainder}, chunk)
       when is_binary(chunk) do
    {rows, new_remainder, new_row_state} =
      Ch.RowBinary.decode_rows_continue(remainder <> chunk, types, row_state)

    new_state = {:decoding_rows, names, types, new_row_state, new_remainder}

    case rows do
      [] -> {:cont, new_state}
      _ -> {:rows, rows, names, new_state}
    end
  end

  defp decode_continue_data({:empty}, chunk) when is_binary(chunk) do
    # unexpected data on what should be an empty response; ignore
    {:cont, {:empty}}
  end

  defp decode_continue_data({:unknown_format, format}, chunk) when is_binary(chunk) do
    # discard chunks; error reported at :done
    {:cont, {:unknown_format, format}}
  end

  defp decode_continue_data({:error_body, status, code, acc}, chunk) when is_binary(chunk) do
    {:cont, {:error_body, status, code, [acc | chunk]}}
  end

  ## Private helpers

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
  defp encode_params(params) when is_map(params) do
    Enum.map(params, fn {k, v} -> {"param_#{k}", encode_param(v)} end)
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
