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

  # makes an http request to clickhouse bypassing dbconnection
  def sql_exec(sql, opts \\ []) do
    with {:ok, conn} <- Ch.Connection.connect(opts) do
      try do
        case Ch.Connection.handle_execute(Ch.Query.build(sql, opts), [], opts, conn) do
          {:ok, _query, result, _conn} -> {:ok, result}
          {:error, reason, _conn} -> {:error, reason}
          {:disconnect, reason, _conn} -> {:error, reason}
        end
      after
        :ok = Ch.Connection.disconnect(:normal, conn)
      end
    end
  end

  def drop_table(table) do
    sql_exec("drop table `#{table}`")
  end
end
