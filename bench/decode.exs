alias Ch.RowBinary

rows = [
  [10000, "1qortoiawuefglads", ~N[2022-11-26 09:38:24], ["here", "goes", "the", "string"]],
  [
    200_000,
    "2asdkfhjal3uirvlakwuehglvkajwgelvkajwgeflkvjawgef.kjavbwefasdmaksdjfblkwegr",
    ~N[2022-11-26 09:38:25],
    ["oh, no", "it's", "an", "array"]
  ],
  [
    3_000_000,
    "3i4naiefnalsidufaksf7kstdfiastdkfa7sdtkfqv3,jhwev,mfhasvdmfhasvd,fhasv,dfjhagsdfkjagusdkfjasgdfkjahsgdfkjhasdglksdfguslidufglasdu glaiusdgl iausgdfl iuasdgf ads af adssssa",
    ~N[2022-11-26 09:38:26],
    ["but it consists", "of", "strings"]
  ]
]

header = [["a", "b", "c", "d"], ["UInt32", "String", "DateTime", "Array(String)"]]

csv_with_names_and_types =
  (header ++ CSV.encode_rows(rows))
  |> NimbleCSV.RFC4180.dump_to_iodata()
  |> IO.iodata_to_binary()

row_binary =
  IO.iodata_to_binary(RowBinary.encode_rows(rows, [:u32, :string, :datetime, {:array, :string}]))

row_binary_with_names_and_types =
  IO.iodata_to_binary([
    _cols_count = 4,
    RowBinary.encode_rows(header, [:string, :string, :string, :string]),
    row_binary
  ])

Benchee.run(
  %{
    "CSVWithNamesAndTypes" => fn ->
      csv_with_names_and_types
      |> NimbleCSV.RFC4180.parse_string(skip_headers: false)
      |> CSV.decode_rows()
    end,
    "RowBinaryWithNamesAndTypes" => fn ->
      RowBinary.decode_rows(row_binary_with_names_and_types)
    end,
    "RowBinary string=utf8" => fn ->
      RowBinary.decode_rows(row_binary, [:u32, :string, {:datetime, nil}, {:array, :string}])
    end,
    "RowBinary string=binary" => fn ->
      RowBinary.decode_rows(row_binary, [:u32, :binary, {:datetime, nil}, {:array, :binary}])
    end
  },
  memory_time: 2
)
