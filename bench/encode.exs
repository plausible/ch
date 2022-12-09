enumerable = [
  [1, "1", ~N[2022-11-26 09:38:24], ["here", "goes", "the", "string"]],
  [2, "2", ~N[2022-11-26 09:38:25], ["oh, no", "it's", "an", "array"]],
  [3, "3", ~N[2022-11-26 09:38:26], ["but it consists", "of", "strings"]]
]

types = [:u32, :string, :datetime, {:array, :string}]

Benchee.run(
  %{
    "csv stream" => fn ->
      enumerable |> NimbleCSV.RFC4180.dump_to_stream() |> Stream.run()
    end,
    "csv" => fn ->
      NimbleCSV.RFC4180.dump_to_iodata(enumerable)
    end,
    "row_binary stream" => fn ->
      enumerable |> Stream.map(&Ch.Protocol.encode_row(&1, types)) |> Stream.run()
    end,
    "row_binary" => fn ->
      Ch.Protocol.encode_rows(enumerable, types)
    end
  },
  memory_time: 2
)
