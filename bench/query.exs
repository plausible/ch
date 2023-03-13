# Ch.Query.extract_command

Benchee.run(%{"extract_command" => fn input -> Ch.Query.extract_command(input) end},
  memory_time: 2,
  inputs: %{"select 1" => "select 1"}
)
