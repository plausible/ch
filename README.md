# Ch

[![Documentation badge](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/ch)
[![Hex.pm badge](https://img.shields.io/badge/Package%20on%20hex.pm-informational)](https://hex.pm/packages/ch)
[![Benchmarks badge](https://img.shields.io/badge/Benchmarks-orange)](https://plausible.github.io/ch/benchmarks/)

Minimal HTTP [ClickHouse](https://clickhouse.com) client for Elixir.

Three layers:

- **`Ch.HTTP`** — stateless helpers for `Mint.HTTP1`: encode requests, receive and decode responses (single-shot or streaming), deadline propagation
- **`Ch.Pool`** — `NimblePool` of `Mint.HTTP1` connections tuned for ClickHouse (short keepalive, lazy connect)
- **`Ch.Buffer`** — data structure for accumulating rows as `RowBinaryWithNamesAndTypes` for `INSERT`

Used in [Ecto ClickHouse adapter](https://github.com/plausible/ecto_ch).

## Installation

```elixir
defp deps do
  [
    {:ch, "~> 0.9.0"}
  ]
end
```

## Usage

```elixir
deadline = Ch.HTTP.to_deadline(to_timeout(second: 15))

{:ok, conn} =
  Mint.HTTP1.connect(:http, "localhost", 8123,
    mode: :passive,
    timeout: Ch.HTTP.to_timeout(deadline)
  )

try do
  {path, headers, body} = Ch.HTTP.encode("SELECT 1")

  with {:ok, _ref, conn} <- Mint.HTTP1.request(conn, "POST", path, headers, body),
       {:ok, {200, _headers, body}, conn} <- Ch.HTTP.recv_all(conn, deadline),
       {:ok, _names, rows} <- Ch.HTTP.decode(200, _headers, body) do
    rows
  end
after
  Mint.HTTP1.close(conn)
end
```

See [guides](./guides) and [tests](./test) for more examples.

## [Benchmarks](./bench)

Results tracked over time at [plausible.github.io/ch/benchmarks](https://plausible.github.io/ch/benchmarks/).
See [bench/](./bench) for local benchmark scripts.
