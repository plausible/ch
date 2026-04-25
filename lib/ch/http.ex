defmodule Ch.HTTP do
  @moduledoc """
  Stateless helpers for `Mint.HTTP1` for ClickHouse.
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
  @spec path(Enumerable.t(), Enumerable.t()) :: String.t()
  def path(params, options \\ []) do
    case encode_params(params) ++ encode_options(options) do
      [] -> "/"
      qp -> "/?" <> URI.encode_query(qp)
    end
  end

  # Encodes query parameters for ClickHouse HTTP URL binding.
  #
  # ClickHouse uses an "escaped" parameter format identical to its TSV format escaping
  # (see https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters):
  # tab (\t), newline (\n), and backslash (\) are backslash-escaped.
  defp encode_params(params) when is_map(params) do
    params |> Map.to_list() |> encode_params()
  end

  defp encode_params(params) when is_list(params) do
    Enum.map(params, fn {k, v} -> {"param_#{k}", encode_param(v)} end)
  end

  defp encode_options(options) when is_map(options) do
    options |> Map.to_list() |> encode_options()
  end

  defp encode_options(options) when is_list(options) do
    Enum.map(options, fn {k, v} -> {to_string(k), v} end)
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
