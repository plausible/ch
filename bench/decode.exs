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
