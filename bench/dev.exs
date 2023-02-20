defmodule Decoder do
  @compile {:bin_opt_info, true}

  def body_rec(<<i::32-little-signed, rest::bytes>>) do
    [i | body_rec(rest)]
  end

  def body_rec(<<>>), do: []

  def pivot(types, <<data::bytes>>) do
    pivot__1(types, acc, data, types)
  end

  defp pivot_row([type | types], acc, <<bin::bytes>>, stack) do
    decode_cell(types, bin, [{:row, [types, acc]}])
  end

  defp pivot_row([], acc, <<bin::bytes>>, types) do
    pivot__1(types, acc, bin, types)
  end

  defp pivot_array(acc, type, count, <<bin::bytes>>, stack) when count > 0 do
    decode_cell(type, bin, [{:array, acc, type, count - 1}])
  end

  defp pivot_array(acc, _type, _count, <<bin::bytes>>, stack) do
    next(:lists.reverse(acc), bin, stack)
  end

  @compile inline: [decode_cell: 3]
  defp decode_cell(type, <<bin::bytes>>, stack) do
    case type do
      :i32 ->
        <<i::32-little-signed, bin::bytes>> = bin
        next(i, bin, stack)

      {:array, type} ->
        <<0::1, count::7, bin::bytes>> = bin
        pivot_array([], type, count, bin, stack)
    end
  end

  @compile inline: [next: 3]
  defp next(value, <<bin::bytes>>, [next | stack]) do
    case next do
      {:array, acc, type, count} -> pivot_array([value | acc], type, count, bin, stack)
      {:row, acc, types} -> pivot_row(types, [value | acc], bin, stack)
      types -> pivot_row(types, )
    end
  end
end

# TODO consider Native

ints = 0..1000

# data =
#   Enum.into(ints, [])
#   |> Ch.RowBinary.encode_row(List.duplicate(:i32, Enum.count(ints)))
#   |> IO.iodata_to_binary()

cols = [
  Enum.into(ints, []),
  Enum.map(ints, &to_string/1)
]

Benchee.run(
  %{
    # "body_rec" => fn -> Decoder.body_rec(data) end,
    # "pivot" => fn -> Decoder.pivot(data, :i32, Enum.count(ints)) end

    # "apply" => fn -> apply(Kernel, :+, [1, 2]) end,
    # "no-apply" => fn -> 1 + 2 end
    "transpose" => fn -> Matrix.transpose(cols) end
  },
  memory_time: 2
)
