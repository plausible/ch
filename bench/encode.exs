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

cols = [
  [10000, 200_000, 3_000_000],
  [
    "1qortoiawuefglads",
    "2asdkfhjal3uirvlakwuehglvkajwgelvkajwgeflkvjawgef.kjavbwefasdmaksdjfblkwegr",
    "3i4naiefnalsidufaksf7kstdfiastdkfa7sdtkfqv3,jhwev,mfhasvdmfhasvd,fhasv,dfjhagsdfkjagusdkfjasgdfkjahsgdfkjhasdglksdfguslidufglasdu glaiusdgl iausgdfl iuasdgf ads af adssssa"
  ],
  [~N[2022-11-26 09:38:24], ~N[2022-11-26 09:38:25], ~N[2022-11-26 09:38:26]],
  [
    ["here", "goes", "the", "string"],
    ["oh, no", "it's", "an", "array"],
    ["but it consists", "of", "strings"]
  ]
]

types = {:u32, :string, :datetime, {:array, :string}}

Benchee.run(
  %{
    # "csv stream" => fn ->
    #   rows |> NimbleCSV.RFC4180.dump_to_stream() |> Stream.run()
    # end,
    # "csv" => fn ->
    #   NimbleCSV.RFC4180.dump_to_iodata(rows)
    # end,
    # "encode_rows + csv stream" => fn ->
    #   rows |> Stream.map(&CSV.encode_row/1) |> NimbleCSV.RFC4180.dump_to_stream() |> Stream.run()
    # end,
    # "encode_rows + csv" => fn ->
    #   rows |> CSV.encode_rows() |> NimbleCSV.RFC4180.dump_to_iodata()
    # end,
    # "row_binary stream" => fn ->
    #   rows |> Stream.map(&RowBinary.encode_row(&1, types)) |> Stream.run()
    # end,
    "row_binary" => fn ->
      RowBinary.encode_rows(rows, types)
    end,
    "encode_cols" => fn ->
      RowBinary.encode_cols(cols, types)
    end
  },
  memory_time: 2
)
