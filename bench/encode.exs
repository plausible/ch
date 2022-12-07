enumerable = [
  [1, "1", ~N[2022-11-26 09:38:24]],
  [2, "2", ~N[2022-11-26 09:38:25]],
  [3, "3", ~N[2022-11-26 09:38:26]]
]

types = [:u32, :string, :datetime]

Benchee.run(
  %{
    "csv stream" => fn ->
      enumerable |> NimbleCSV.RFC4180.dump_to_stream() |> Stream.run()
    end,
    "csv eager" => fn ->
      NimbleCSV.RFC4180.dump_to_iodata(enumerable)
    end,
    "row_binary stream" => fn ->
      enumerable |> Stream.map(&Ch.Protocol.encode_row(&1, types)) |> Stream.run()
    end,
    "row_binary eager" => fn ->
      Ch.Protocol.encode_rows(enumerable, types)
    end
  },
  memory_time: 2
)
