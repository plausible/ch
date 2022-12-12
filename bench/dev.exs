Benchee.run(
  %{
    "nullable" => fn -> Ch.Protocol.encode(:u16, 1) end,
    "nullable nil" => fn -> Ch.Protocol.encode(:u16, nil) end,
    "non-nullable" => fn -> Ch.Protocol.encode(:i16, 1) end,
    "non-nullable nil" => fn -> Ch.Protocol.encode(:i16, nil || 0) end
  },
  memory_time: 2
)
