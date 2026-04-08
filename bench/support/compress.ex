defmodule Compress do
  def zstd_stream(input) when is_list(input) do
    {:ok, ctx} = :zstd.context(:compress)
    zstd_stream_continue(input, ctx)
  end

  defp zstd_stream_continue([value | rest], ctx) do
    {:continue, c} = :zstd.stream(ctx, value)
    [c | zstd_stream_continue(rest, ctx)]
  end

  defp zstd_stream_continue([], ctx) do
    {:done, c} = :zstd.finish(ctx, [])
    c
  end
end
