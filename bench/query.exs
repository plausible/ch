Benchee.run(
  %{
    "query string encode" => fn input ->
      DBConnection.Query.Ch.Query.encode(input.query, input.params, input.opts)
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
    }
  }
)
