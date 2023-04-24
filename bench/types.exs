types = ["String", "DateTime", "DateTime('UTC')", "Array(String)", "Array(Tuple(String, UInt64))"]
inputs = Map.new(types, fn type -> {type, type} end)

Benchee.run(
  %{"decode/1" => fn type -> Ch.Types.decode(type) end},
  inputs: inputs
)
