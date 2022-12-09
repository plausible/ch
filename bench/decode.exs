defmodule CSV do
  def decode_rows([_names, types | rows]) do
    decode_rows(rows, atom_types(types))
  end

  defp atom_types([type | rest]), do: [atom_type(type) | atom_types(rest)]
  defp atom_types([] = done), do: done

  defp atom_type("String" <> _), do: :string
  defp atom_type("UInt" <> _), do: :integer
  defp atom_type("Int" <> _), do: :integer
  defp atom_type("DateTime" <> _), do: :datetime
  defp atom_type("Date" <> _), do: :date
  defp atom_type("Array(" <> inner_type), do: {:array, atom_type(inner_type)}

  defp decode_rows([row | rest], types) do
    [decode_row(types, row) | decode_rows(rest, types)]
  end

  defp decode_rows([] = done, _types), do: done

  defp decode_row([t | types], [v | row]) do
    [decode_value(t, v) | decode_row(types, row)]
  end

  defp decode_row([] = done, []), do: done

  # TODO
  defp decode_array("," <> rest, type, inner_acc, acc) do
    decode_array(rest, type, "", [decode_value(type, inner_acc) | acc])
  end

  defp decode_array("]" <> rest, type, inner_acc, acc) do
    decode_array(rest, type, "", [decode_value(type, inner_acc) | acc])
  end

  defp decode_array("[" <> rest, type, inner_acc, acc) do
    decode_array(rest, type, inner_acc, acc)
  end

  defp decode_array(<<v, rest::bytes>>, type, inner_acc, acc) do
    decode_array(rest, type, <<inner_acc::bytes, v>>, acc)
  end

  defp decode_array("", _type, "", acc), do: :lists.reverse(acc)

  defp decode_value(:string, s), do: s
  defp decode_value(:integer, i), do: String.to_integer(i)
  defp decode_value(:datetime, dt), do: NaiveDateTime.from_iso8601!(dt)
  defp decode_value(:date, d), do: Date.from_iso8601!(d)
  defp decode_value({:array, t}, a), do: decode_array(a, t, "", [])

  def encode_rows(rows) do
    Enum.map(rows, fn row ->
      Enum.map(row, fn
        a when is_list(a) -> encode_array_param(a)
        other -> other
      end)
    end)
  end

  defp encode_param(a) when is_list(a) do
    IO.iodata_to_binary([?[, encode_array_param(a), ?]])
  end

  defp encode_array_param([s | rest]) when is_binary(s) do
    # TODO faster escaping
    [?', String.replace(s, "'", "\\'"), "'," | encode_array_param(rest)]
  end

  defp encode_array_param([el | rest]) do
    [encode_param(el), "," | encode_array_param(rest)]
  end

  defp encode_array_param([] = done), do: done
end

rows = [
  [1, "1", ~N[2022-11-26 09:38:24], ["here", "goes", "the", "string"]],
  [2, "2", ~N[2022-11-26 09:38:25], ["oh, no", "it's", "an", "array"]],
  [3, "3", ~N[2022-11-26 09:38:26], ["but it consists", "of", "strings"]]
]

header = [["a", "b", "c", "d"], ["UInt32", "String", "DateTime", "Array(String)"]]

csv =
  (header ++ CSV.encode_rows(rows)) |> NimbleCSV.RFC4180.dump_to_iodata() |> IO.iodata_to_binary()

row_binary =
  IO.iodata_to_binary([
    <<4>>,
    Ch.Protocol.encode_rows(header, [:string, :string, :string, :string]),
    Ch.Protocol.encode_rows(rows, [:u32, :string, :datetime, {:array, :string}])
  ])

Benchee.run(
  %{
    "csv" => fn ->
      csv
      |> NimbleCSV.RFC4180.parse_string(skip_headers: false)
      |> CSV.decode_rows()
    end,
    "row_binary" => fn ->
      Ch.Protocol.decode_rows(row_binary)
    end
  },
  memory_time: 2
)
