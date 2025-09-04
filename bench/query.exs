Benchee.run(
  %{
    "query string encode" => fn input ->
      DBConnection.Query.Ch.Query.query_string_encode(input.query, input.params, input.opts)
    end,
    "multipart encode" => fn input ->
      DBConnection.Query.Ch.Query.multipart_encode(input.query, input.params, input.opts)
    end,
    "custom multipart encode" => fn input ->
      DBConnection.Query.Ch.Query.custom_multipart_encode(input.query, input.params, input.opts)
    end
  },
  inputs: %{
    "0 params" => %{
      query: %Ch.Query{
        statement: "select 1",
        command: :select,
        encode: true,
        decode: true
      },
      params: [],
      opts: []
    },
    "1 named param" => %{
      query: %Ch.Query{
        statement: "select {a:UInt8}",
        command: :select,
        encode: true,
        decode: true
      },
      params: %{"a" => 1},
      opts: []
    },
    "10 named params" => %{
      query: %Ch.Query{
        statement: "select " <> Enum.map_join(1..10, ", ", &"{a#{&1}:UInt8}"),
        command: :select,
        encode: true,
        decode: true
      },
      params: Map.new(1..10, &{"a#{&1}", &1}),
      opts: []
    },
    "10 positional params" => %{
      query: %Ch.Query{
        statement: "select " <> Enum.map_join(1..10, ", ", &"{$#{&1}:UInt8}"),
        command: :select,
        encode: true,
        decode: true
      },
      params: Enum.to_list(1..10),
      opts: []
    },
    "100 positional params" => %{
      query: %Ch.Query{
        statement: "select " <> Enum.map_join(1..100, ", ", &"{$#{&1}:UInt8}"),
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
