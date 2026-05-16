# Ch

[![Documentation badge](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/ch)
[![Hex.pm badge](https://img.shields.io/badge/Package%20on%20hex.pm-informational)](https://hex.pm/packages/ch)
[![Coveralls](https://img.shields.io/coverallsCoverage/github/plausible/ch?branch=master&style=flat&label=Coverage)](https://coveralls.io/github/plausible/ch?branch=master)

HTTP [ClickHouse](https://clickhouse.com) client for Elixir.

Used in [Ecto ClickHouse adapter.](https://github.com/plausible/ecto_ch)

### Key features

- RowBinary
- Native query parameters
- Per query settings

## Installation

```elixir
defp deps do
  [
    {:ch, "~> 0.9.0"}
  ]
end
```
