defmodule Ch.Test do
  def dump_type(:string), do: "String"

  def dump_type(:u8), do: "UInt8"
  def dump_type(:u16), do: "UInt16"
  def dump_type(:u32), do: "UInt32"
  def dump_type(:u64), do: "UInt64"
  def dump_type(:u128), do: "UInt128"
  def dump_type(:u256), do: "UInt256"

  def dump_type(:i8), do: "Int8"
  def dump_type(:i16), do: "Int16"
  def dump_type(:i32), do: "Int32"
  def dump_type(:i64), do: "Int64"
  def dump_type(:i128), do: "Int128"
  def dump_type(:i256), do: "Int256"

  def dump_type(:f32), do: "Float32"
  def dump_type(:f64), do: "Float64"

  def dump_type(:date), do: "Date"
  def dump_type(:date32), do: "Date32"

  def dump_type(:datetime), do: "DateTime"
  def dump_type({:datetime, nil}), do: "DateTime"
  def dump_type({:datetime, timezone}), do: "DateTime('#{timezone}')"

  def dump_type({:datetime64, precision, nil}), do: "DateTime64(#{precision})"
  def dump_type({:datetime64, precision, timezone}), do: "DateTime64(#{precision}, '#{timezone}')"

  def dump_type({:array, type}), do: "Array(#{dump_type(type)})"
end
