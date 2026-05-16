defmodule Help do
  @moduledoc false

  @pool Ch.TestPool

  def session_id(%{module: module, test: test}) do
    rand =
      Base.hex_encode32(
        <<
          System.system_time(:nanosecond)::64,
          :erlang.phash2(self(), 16_777_216)::24,
          :erlang.unique_integer()::32
        >>,
        case: :lower
      )

    "#{module}-#{test}-#{rand}"
  end

  def start_link_pool(url) do
    Ch.start_link(name: @pool, url: url, pool_size: 100)
  end

  def query(statement, params \\ %{}, options \\ []) do
    Ch.query(@pool, statement, params, options)
  end

  def query!(statement, params \\ %{}, options \\ []) do
    Ch.query!(@pool, statement, params, options)
  end

  def to_maps(%{names: names, rows: rows}) do
    Enum.map(rows, fn row -> names |> Enum.zip(row) |> Map.new() end)
  end
end
