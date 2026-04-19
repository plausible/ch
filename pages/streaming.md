# Streaming

For large `SELECT` results, use `decode_start/1` + `decode_continue/2` to process
rows as Mint chunks arrive rather than buffering the entire response body first.

## When to use streaming

- Result sets large enough that buffering into memory is expensive (millions of rows)
- You want to pipe rows into a `Stream`, write to a file, or forward to another system
- You need first-row latency (start processing before the response is complete)

## API

`decode_start/1` initialises the decoder configuration (e.g. custom `decoders`).
`decode_continue/2` accepts Mint response tuples directly (`:status`, `:headers`,
`:data`, `:done`, `:error`) and returns decoded rows or transition states.

```
{:rows, rows, names, state}  -- rows decoded from this chunk; continue
{:cont, state}               -- tuple consumed, state advanced; continue
{:ok, names, rows}           -- done (emits any final rows)
:ok                        -- done (no rows, e.g. DDL/INSERT)
{:error, Ch.Error.t()}       -- ClickHouse server-side error
{:error, reason}             -- Mint connection or transport error
```

## Passive mode (recv loop)

```elixir
path = Ch.HTTP.path(%{})
body = "SELECT number FROM system.numbers LIMIT 10000000"
{:ok, _ref, conn} = Mint.HTTP1.request(conn, "POST", path, [], body)

state = Ch.HTTP.decode_start()

conn =
  Stream.resource(
    fn -> {conn, state} end,
    fn {conn, state} ->
      case Mint.HTTP1.recv(conn, 0, 5_000) do
        {:ok, conn, responses} ->
          # Feed Mint responses directly to the decoder
          {rows, state} =
            Enum.reduce(responses, {[], state}, fn resp, {rows, state} ->
              case Ch.HTTP.decode_continue(state, resp) do
                {:rows, new_rows, _names, state} -> {rows ++ new_rows, state}
                {:cont, state} -> {rows, state}
                {:ok, _names, new_rows} -> {rows ++ new_rows, state}
                _ -> {rows, state}
              end
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
            Enum.reduce(responses, state, fn resp, state ->
              case Ch.HTTP.decode_continue(state, resp) do
                {:rows, rows, names, state} ->
                  handle_rows(rows, names)
                  state

                {:cont, state} ->
                  state

                # Check for termination in a real app
                _ ->
                  state
              end
            end)

          recv_loop(conn, state)
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
