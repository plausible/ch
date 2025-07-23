Benchee.run(
  %{
    "query string encode" => fn input ->
      DBConnection.Query.Ch.Query.encode(input.query, input.params, input.opts)
    end,
    "multipart encode" => fn input ->
      DBConnection.Query.Ch.Query.multipart_encode(input.query, input.params, input.opts)
    end,
    "custom multipart encode" => fn input ->
      DBConnection.Query.Ch.Query.custom_multipart_encode(input.query, input.params, input.opts)
    end
  },
  inputs: %{
    "basic" => %{
      query: %Ch.Query{
        statement: "select {a:UInt8}",
        command: :select,
        encode: true,
        decode: true
      },
      params: %{"a" => 1},
      opts: []
    },
    "100 params" => %{
      query: %Ch.Query{
        statement: "select " <> Enum.map_join(1..100, ", ", &"{param_$#{&1}:UInt8}"),
        command: :select,
        encode: true,
        decode: true
      },
      params: Enum.to_list(1..100),
      opts: []
    }
  }
  # profile_after: true
)
