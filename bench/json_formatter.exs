defmodule Ch.Bench.JSONFormatter do
  @behaviour Benchee.Formatter

  @configuration_fields [
    :time,
    :warmup,
    :memory_time,
    :reduction_time,
    :parallel,
    :percentiles,
    :max_sample_size,
    :exclude_outliers,
    :measure_function_call_overhead
  ]

  @impl Benchee.Formatter
  def format(%Benchee.Suite{} = suite, opts) do
    version = Map.fetch!(opts, :benchmark_version)

    system =
      suite.system
      |> Map.from_struct()
      |> Map.put(:architecture, :erlang.system_info(:system_architecture) |> List.to_string())

    machine = machine_key(Map.fetch!(opts, :machine_prefix), system)
    output_root = Map.fetch!(opts, :output_root)

    path =
      Path.join([output_root, "machine=#{machine}", "version=#{safe(version)}", "benchmark.json"])

    data =
      %{
        schema_version: 1,
        benchmark_version: version,
        machine: machine,
        generated_at: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601(),
        ci: System.get_env("CI") == "true",
        units: %{
          configuration_time: "nanosecond",
          run_time_samples: "nanosecond",
          run_time_ips: "iterations_per_second",
          memory_usage_samples: "byte",
          reductions_samples: "count"
        },
        system: system,
        configuration: Map.take(Map.from_struct(suite.configuration), @configuration_fields),
        scenarios: Enum.map(suite.scenarios, &scenario/1)
      }
      |> json_safe()
      |> JSON.encode_to_iodata!()

    %{data: data, path: path, output_root: output_root}
  end

  @impl Benchee.Formatter
  def write(%{data: data, path: path, output_root: output_root}, _opts) do
    path |> Path.dirname() |> File.mkdir_p!()
    File.write!(path, data)
    File.write!(Path.join(output_root, "current-result-path"), path)
    IO.puts("Benchmark artifact: #{path}")
  end

  defp machine_key(prefix, system) do
    [
      prefix,
      system.os,
      system.cpu_speed,
      "#{system.num_cores}-cores",
      system.available_memory,
      system.architecture
    ]
    |> Enum.join("-")
    |> safe()
  end

  defp safe(value), do: String.replace(value, ~r/[^a-zA-Z0-9._-]/, "-")

  defp scenario(scenario) do
    %{
      name: scenario.name,
      job_name: scenario.job_name,
      input_name: scenario.input_name,
      tag: scenario.tag,
      run_time: scenario.run_time_data,
      memory_usage: scenario.memory_usage_data,
      reductions: scenario.reductions_data
    }
  end

  defp json_safe(%_{} = struct), do: struct |> Map.from_struct() |> json_safe()

  defp json_safe(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), json_safe(value)} end)
  end

  defp json_safe(list) when is_list(list), do: Enum.map(list, &json_safe/1)
  defp json_safe(tuple) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> json_safe()
  defp json_safe(atom) when is_atom(atom) and atom not in [true, false, nil], do: to_string(atom)
  defp json_safe(value), do: value
end
