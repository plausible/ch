defmodule Ch.Encode.Parameters do
  @moduledoc false

  @doc """
  Encodes a map/list of parameters into a list of clickhouse parameter tuples.

  The format is `[{"param_<key>", "<value>"}, ...]`.
  """
  @spec encode_many(map | [term]) :: [{String.t(), String.t()}]
  def encode_many(params) when is_map(params) do
    Enum.map(params, fn {k, v} -> {"param_#{k}", encode(v)} end)
  end

  def encode_many(params) when is_list(params) do
    params
    |> Enum.with_index()
    |> Enum.map(fn {v, idx} -> {"param_$#{idx}", encode(v)} end)
  end

  @doc """
  Encodes a clickhouse parameter to a string.
  """
  @spec encode(term) :: binary
  def encode(n) when is_integer(n), do: Integer.to_string(n)
  def encode(f) when is_float(f), do: Float.to_string(f)

  # TODO possibly speed up
  # For more info see
  # https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters
  # "escaped" format is the same as https://clickhouse.com/docs/en/interfaces/formats#tabseparated-data-formatting
  def encode(b) when is_binary(b) do
    escape_param([{"\\", "\\\\"}, {"\t", "\\\t"}, {"\n", "\\\n"}], b)
  end

  def encode(b) when is_boolean(b), do: Atom.to_string(b)
  def encode(%Decimal{} = d), do: Decimal.to_string(d, :normal)
  def encode(%Date{} = date), do: Date.to_iso8601(date)
  def encode(%NaiveDateTime{} = naive), do: NaiveDateTime.to_iso8601(naive)

  def encode(%DateTime{microsecond: microsecond} = dt) do
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

  def encode(tuple) when is_tuple(tuple) do
    IO.iodata_to_binary([?(, encode_array_params(Tuple.to_list(tuple)), ?)])
  end

  def encode(a) when is_list(a) do
    IO.iodata_to_binary([?[, encode_array_params(a), ?]])
  end

  def encode(m) when is_map(m) do
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
    [?', encode(param), ?']
  end

  defp encode_array_param(v), do: encode(v)

  defp encode_map_param({k, v}) do
    [encode_array_param(k), ?:, encode_array_param(v)]
  end

  defp escape_param([{pattern, replacement} | escapes], param) do
    param = String.replace(param, pattern, replacement)
    escape_param(escapes, param)
  end

  defp escape_param([], param), do: param
end
