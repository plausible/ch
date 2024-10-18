# On-disk INSERT buffer

Here how you could do it

```elixir
defmodule WriteBuffer do
  use GenServer

  # 5 MB
  max_buffer_size = 5_000_000

  def insert(rows) do
    row_binary = Ch.RowBinary.encode_many(rows, unquote(encoding_types))
    GenServer.call(__MODULE__, {:buffer, row_binary})
  end

  def init(opts) do
    {:ok, fd} = :file.open()
    %{fd: fd, buffer_size: 0}
  end

  def handle_call({:buffer, row_binary}, _from, state) do
    new_buffer_size = state.buffer_size + IO.iodata_length(row_binary)
    :file.write(state.fd, row_binary)

    if new_buffer_size < unquote(max_buffer_size) do
      %{state | buffer_size: new_buffer_size}
    else
      flush(state)
    end
  end
end
```

See [tests](../test/ch/on_disk_buffer_test.exs) for more.

TODO: notes on using it in docker and "surviving" restarts
