string = Ecto.ParameterizedType.init(Ch, type: "String")
int64 = Ecto.ParameterizedType.init(Ch, type: "Int64")
nullable_string = Ecto.ParameterizedType.init(Ch, type: "Nullable(String)")
low_cardinality_string = Ecto.ParameterizedType.init(Ch, type: "LowCardinality(String)")
tuple = Ecto.ParameterizedType.init(Ch, type: "Tuple(String, Int64)")
map = Ecto.ParameterizedType.init(Ch, type: "Map(String, UInt64)")

Benchee.run(
  %{
    "String" => fn -> Ecto.Type.cast(string, "value") end,
    "Int64" => fn -> Ecto.Type.cast(int64, 10) end,
    "Nullable(String)" => fn -> Ecto.Type.cast(nullable_string, "value") end,
    "LowCardinality(String)" => fn -> Ecto.Type.cast(low_cardinality_string, "value") end,
    "Tuple(String, Int64)" => fn -> Ecto.Type.cast(tuple, {"value", 10}) end,
    "Map(String, UInt64)" => fn -> Ecto.Type.cast(map, %{"value" => 10}) end
  },
  measure_function_call_overhead: true
  # profile_after: :eprof
)
