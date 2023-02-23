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

  def dump_type({:nullable, type}), do: "Nullable(#{dump_type(type)})"

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

  def intercept_packets(socket, buffer \\ <<>>) do
    receive do
      {:tcp, ^socket, packet} ->
        buffer = buffer <> packet

        if complete?(buffer) do
          buffer
        else
          intercept_packets(socket, buffer)
        end
    end
  end

  defp complete?(buffer) do
    with {:ok, rest} <- eat_status(buffer),
         {:ok, content_length, rest} <- eat_headers(rest) do
      verify_body(content_length, rest)
    else
      _ -> false
    end
  end

  defp eat_status(buffer) do
    case :erlang.decode_packet(:http_bin, buffer, []) do
      {:ok, _, rest} -> {:ok, rest}
      {:more, _} -> {:more, buffer}
    end
  end

  defp eat_headers(buffer, content_length \\ nil) do
    case :erlang.decode_packet(:httph_bin, buffer, []) do
      {:ok, {_, _, :"Content-Length", _, content_length}, rest} ->
        eat_headers(rest, String.to_integer(content_length))

      {:ok, {_, _, :"Transfer-Encoding", _, "chunked"}, rest} ->
        eat_headers(rest, :chunked)

      {:ok, :http_eoh, rest} ->
        {:ok, content_length, rest}

      {:ok, _, rest} ->
        eat_headers(rest, content_length)

      {:more, _} ->
        {:more, buffer}
    end
  end

  defp verify_body(:chunked, chunks) do
    String.ends_with?(chunks, "\r\n0\r\n\r\n")
  end

  defp verify_body(content_length, body) do
    byte_size(body) == content_length
  end
end
