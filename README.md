# Ch

[![Documentation badge](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/ch)
[![Hex.pm badge](https://img.shields.io/badge/Package%20on%20hex.pm-informational)](https://hex.pm/packages/ch)

Minimal HTTP ClickHouse client for Elixir.

Used in [Ecto ClickHouse adapter.](https://github.com/plausible/ecto_ch)

## Installation

```elixir
defp deps do
  [
    {:ch, "~> 0.3.0"}
  ]
end
```

## Usage

Please see the tests for usage examples:

- [Select rows](./tests/examples/select_rows_test.exs)
- [Insert rows](./tests/examples/insert_rows_test.exs)
- [Insert RowBinary](./tests/examples/insert_rowbinary_test.exs)
- [Insert CSV](./tests/examples/insert_csv_test.exs)
- [Insert compressed RowBinary](./tests/examples/insert_compressed_rowbinary_test.exs)
- [Insert chunked RowBinary](./tests/examples/insert_chunked_rowbinary_test.exs)
- [Insert chunked and compressed RowBinary](./tests/examples/insert_chunked_compressed_rowbinary_test.exs)
- [Insert from a file](./tests/examples/insert_from_file_test.exs)
- [Custom settings](./tests/custom_settings_test.exs)
- [Custom headers](./tests/custom_headers_test.exs)

## Caveats

#### Timestamps in query parameters

> [!WARNING]
> - `%NaiveDateTime{}` is encoded as text to make it assume the column's or ClickHouse server's timezone
> - `%DateTime{time_zone: "Etc/UTC"}` is encoded as a unix timestamp and is treated as UTC timestamp by ClickHouse
> - `%DateTime{time_zone: time_zone}` is shifted to `"Etc/UTC"` and then encoded as a unix timestamp, this requires a [time zone database](https://hexdocs.pm/elixir/1.17.1/DateTime.html#module-time-zone-database) to be configured


TODO: See test.

#### NULL in RowBinary

It's the same as in [`ch-go`](https://clickhouse.com/docs/en/integrations/go#nullable)

> At insert time, Nil can be passed for both the normal and Nullable version of a column. For the former, the default value for the type will be persisted, e.g., an empty string for string. For the nullable version, a NULL value will be stored in ClickHouse.

TODO: See test.

#### UTF-8 in RowBinary

When decoding [`String`](https://clickhouse.com/docs/en/sql-reference/data-types/string) columns non UTF-8 characters are replaced with `�` (U+FFFD). This behaviour is similar to [`toValidUTF8`](https://clickhouse.com/docs/en/sql-reference/functions/string-functions#tovalidutf8) and [JSON format.](https://clickhouse.com/docs/en/interfaces/formats#json)

TODO: See test.

#### Timezones in RowBinary

Decoding non-UTC datetimes like `DateTime('Asia/Taipei')` requires a [timezone database.](https://hexdocs.pm/elixir/DateTime.html#module-time-zone-database)

TODO: See test.

Encoding non-UTC datetimes is possible but slow.

TODO: See test.

## Benchmarks

Please see [CI Results](https://github.com/plausible/ch/actions/workflows/bench.yml) (make sure to click the latest workflow run and scroll down to "Artifacts") for [some of our benchmarks.](./bench/)
