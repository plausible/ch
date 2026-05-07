defmodule GitHubActionBenchmarkFormatter do
  @behaviour Benchee.Formatter

  @impl Benchee.Formatter
  def format(%Benchee.Suite{scenarios: scenarios}, opts) do
    suite_name = opt(opts, :suite_name)

    Enum.map(scenarios, fn scenario ->
      stats = scenario.run_time_data.statistics

      %{
        name: benchmark_name(suite_name, scenario),
        unit: "ips",
        value: Float.round(stats.ips, 4),
        range: deviation(stats),
        extra: extra(scenario),
        biggerIsBetter: true
      }
    end)
  end

  @impl Benchee.Formatter
  def write(data, opts) do
    path = opt(opts, :file)

    path
    |> Path.dirname()
    |> File.mkdir_p!()

    File.write!(path, Jason.encode_to_iodata!(data, pretty: true))
  end

  defp benchmark_name(suite_name, scenario) do
    [suite_name, scenario.name, scenario.input_name]
    |> Enum.reject(&is_nil_or_empty/1)
    |> Enum.join(" - ")
  end

  defp deviation(stats) do
    "stddev #{Float.round(stats.std_dev_ratio * 100, 2)}%"
  end

  defp extra(scenario) do
    stats = scenario.run_time_data.statistics

    [
      "average: #{format_duration(stats.average)}",
      "median: #{format_duration(stats.median)}"
    ]
    |> Enum.join("\n")
  end

  defp format_duration(nanoseconds) when nanoseconds >= 1_000_000 do
    "#{Float.round(nanoseconds / 1_000_000, 2)} ms"
  end

  defp format_duration(nanoseconds) when nanoseconds >= 1_000 do
    "#{Float.round(nanoseconds / 1_000, 2)} us"
  end

  defp format_duration(nanoseconds) do
    "#{Float.round(nanoseconds, 2)} ns"
  end

  defp opt(opts, key) when is_list(opts), do: Keyword.fetch!(opts, key)
  defp opt(opts, key), do: Map.fetch!(opts, key)

  defp is_nil_or_empty(nil), do: true
  defp is_nil_or_empty(""), do: true
  defp is_nil_or_empty(_), do: false
end
