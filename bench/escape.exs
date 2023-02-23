# we need string escaping when encoding params into a query string

Benchee.run(
  %{
    "to_iodata" => fn input -> Ch.Connection.to_iodata(input, 0, input, []) end,
    ":binary.replace" => fn input ->
      input |> :binary.replace("\\", "\\\\", [:global]) |> :binary.replace("'", "\\'", [:global])
    end,
    "String.replace" => fn input ->
      input |> String.replace("\\", "\\\\") |> String.replace("'", "\\'")
    end
  },
  memory_time: 2,
  inputs: %{
    "small" => "aksdflaskjdfhl'ak'sdj,\'h'\\fl",
    "mid" =>
      "asldjf alskdf kajsdhgfkajshgdkjhfasjhdfhjkaskhjd     'a'sdf ' '''asdf asdf /asdf asd/f /as\df a\sdf\ as\df \as\\\\asdf as\\\ \\\\'\'\'"
  }
)
