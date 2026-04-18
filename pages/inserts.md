# Inserts

ClickHouse is optimised for large batch INSERTs. Inserting rows one-by-one is an
antipattern — each INSERT triggers a merge on the server. Aim for **100k–1M rows per
batch** or at minimum flush every few seconds.

## Format

Use `RowBinaryWithNamesAndTypes`. The INSERT statement prefix declares the format:

```elixir
statement = "INSERT INTO events (id, name, created_at) FORMAT RowBinaryWithNamesAndTypes\n"
types = ["UInt64", "String", "DateTime"]
names = ["id", "name", "created_at"]

header = Ch.RowBinary.encode_names_and_types(names, types)
rows = Ch.RowBinary.encode_rows([[1, "pageview", DateTime.utc_now()]], types)

body = IO.iodata_to_binary([statement, header | rows])
{path, headers, _body} = Ch.HTTP.encode(body)
```

## Batching with a GenServer

A simple GenServer accumulates rows and flushes on size or time threshold:

```elixir
defmodule EventBuffer do
  use GenServer

  @flush_interval :timer.seconds(5)
  @max_rows 100_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def insert(rows), do: GenServer.cast(__MODULE__, {:insert, rows})

  def init(opts) do
    schedule_flush()
    {:ok, %{rows: [], count: 0, conn: nil, opts: opts}}
  end

  def handle_cast({:insert, new_rows}, %{count: count} = state) do
    state = %{state | rows: [state.rows | new_rows], count: count + length(new_rows)}

    if state.count >= @max_rows do
      {:noreply, flush(state)}
    else
      {:noreply, state}
    end
  end

  def handle_info(:flush, state), do: {:noreply, flush(state)}

  defp flush(%{rows: []} = state), do: state

  defp flush(state) do
    # build and send INSERT here, then reset
    schedule_flush()
    %{state | rows: [], count: 0}
  end

  defp schedule_flush, do: Process.send_after(self(), :flush, @flush_interval)
end
```

## Batching with ETS for concurrent writers

When multiple processes produce rows concurrently, use ETS as a lock-free accumulator:

```elixir
# In application startup:
:ets.new(:event_buffer, [:bag, :public, :named_table])

# From any process:
:ets.insert(:event_buffer, {:row, [id, name, created_at]})

# In a periodic flusher:
rows = :ets.tab2list(:event_buffer) |> Enum.map(fn {:row, r} -> r end)
:ets.delete_all_objects(:event_buffer)
# INSERT rows ...
```

ETS gives you concurrent inserts without a GenServer bottleneck. Use `:ets.select_delete/2`
for atomic take-and-clear on busier tables.

## x-clickhouse-summary

A successful INSERT response includes an `x-clickhouse-summary` header:

```json
{"written_rows": "150000", "written_bytes": "3145728", ...}
```

Extract it from the response headers — see `Ch.HTTP.decode/3` which returns it
parsed as a map in the result.

## Tests

See [`test/ch/guides/inserts_test.exs`](../test/ch/guides/inserts_test.exs).
