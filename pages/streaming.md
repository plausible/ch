# Streaming

For large `SELECT` results, use `decode_start/1` + `decode_continue/2` to process
rows as Mint chunks arrive rather than buffering the entire response body first.

## When to use streaming

- Result sets large enough that buffering into memory is expensive (millions of rows)
- You want to pipe rows into a `Stream`, write to a file, or forward to another system
- You need first-row latency (start processing before the response is complete)

## API

`decode_start/1` initialises a decoder from response headers (inspects
`x-clickhouse-format`). `decode_continue/2` accepts raw binary chunks extracted
from `{:data, ref, chunk}` Mint responses, and returns rows incrementally.

```
{:rows, rows, names, state}  -- rows decoded from this chunk; continue
{:more, state}               -- no complete rows yet; continue
{:ok, names, []}             -- done (all rows already emitted via :rows)
{:error, Ch.Error.t()}       -- ClickHouse error
```

## Passive mode (recv loop)

```elixir
{path, headers, body} = Ch.HTTP.encode("SELECT number FROM system.numbers LIMIT 10000000")
{:ok, _ref, conn} = Mint.HTTP1.request(conn, "POST", path, headers, body)

state = nil

conn =
  Stream.resource(
    fn -> {conn, state} end,
    fn {conn, state} ->
      case Mint.HTTP1.recv(conn, 0, 5_000) do
        {:ok, conn, responses} ->
          {rows, state} =
            Enum.reduce(responses, {[], state}, fn
              {:status, _ref, _status}, acc ->
                acc

              {:headers, _ref, headers}, {rows, _state} ->
                {rows, Ch.HTTP.decode_start(headers)}

              {:data, _ref, chunk}, {rows, state} ->
                case Ch.HTTP.decode_continue(chunk, state) do
                  {:rows, new_rows, _names, state} -> {rows ++ new_rows, state}
                  {:more, state} -> {rows, state}
                end

              {:done, _ref}, acc ->
                acc
            end)

          {rows, {conn, state}}

        {:error, conn, _reason, _} ->
          {:halt, {conn, state}}
      end
    end,
    fn {conn, _state} -> Mint.HTTP1.close(conn) end
  )
  |> Stream.each(fn row -> IO.inspect(row) end)
  |> Stream.run()
```

## Active mode

In active mode, responses arrive as messages. The decoder state carries across
`receive` iterations:

```elixir
defp recv_loop(conn, state) do
  receive do
    message ->
      case Mint.HTTP1.stream(conn, message) do
        {:ok, conn, responses} ->
          state =
            Enum.reduce(responses, state, fn
              {:status, _ref, _status}, state ->
                state

              {:headers, _ref, headers}, _state ->
                Ch.HTTP.decode_start(headers)

              {:data, _ref, chunk}, state ->
                case Ch.HTTP.decode_continue(chunk, state) do
                  {:rows, rows, names, state} ->
                    handle_rows(rows, names)
                    state

                  {:more, state} ->
                    state
                end

              {:done, _ref}, state ->
                {:done, state}
            end)

          case state do
            {:done, _} -> :ok
            state -> recv_loop(conn, state)
          end
      end
  end
end
```

## Chunk boundary handling

`decode_continue/2` handles data arriving split across RowBinary structural boundaries — the
RowBinary names/types header may arrive across multiple chunks. This is tested exhaustively
byte-by-byte in `Ch.RowBinary` tests.

## Tests

See [`test/ch/guides/streaming_test.exs`](../test/ch/guides/streaming_test.exs).
