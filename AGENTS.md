# Benchmark history

Performance results live on the orphan `benchmark-results` branch. They are stored as self-describing Benchee JSON under:

```text
data/machine=<machine>/version=<git-sha>/benchmark.json
latest/<machine>/benchmark.json
```

Machine partitions are derived from the execution environment, OS, CPU model, online core count, total memory, and architecture. CI partitions start with `github-actions-`, while local partitions start with `local-`. Never compare results across machine partitions. A benchmark version is the commit SHA whose code was measured.

The queries below use `clickhouse local`. If the ClickHouse binary is not installed, install it with `curl https://clickhouse.com/cli | sh` followed by `~/.local/bin/clickhousectl local use latest`.

Run benchmarks locally with `mix benchmark`. To include Benchee's built-in TProf pass, use `BENCHMARK_PROFILE=tprof mix benchmark`. JSON results are written below `bench/output/`, partitioned by detected machine specifications and SHA. Each file includes schema/run identity; timestamp and CI provenance; OS, architecture, CPU, cores, memory, Elixir, Erlang, and JIT details; benchmark configuration and units; and raw samples plus runtime, memory, and reduction statistics for every scenario. Override `BENCHMARK_MACHINE_PREFIX`, `BENCHMARK_VERSION`, `BENCHMARK_TIME`, or `BENCHMARK_WARMUP` when a reproducible series needs fixed settings; hardware fields are always detected.

An abridged result from the demo RowBinary benchmark looks like this. Actual files contain all samples and statistics, plus similarly shaped `memory_usage` and `reductions` collections for each scenario.

```json
{
  "schema_version": 1,
  "benchmark_version": "e19815a...",
  "machine": "local-macOS-Apple-M2-8-cores-8-GB-aarch64-apple-darwin",
  "generated_at": "2026-08-03T12:43:41Z",
  "ci": false,
  "units": {
    "configuration_time": "nanosecond",
    "run_time_samples": "nanosecond",
    "run_time_ips": "iterations_per_second",
    "memory_usage_samples": "byte",
    "reductions_samples": "count"
  },
  "system": {
    "os": "macOS",
    "architecture": "aarch64-apple-darwin",
    "cpu_speed": "Apple M2",
    "num_cores": 8,
    "available_memory": "8 GB",
    "elixir": "1.20.2",
    "erlang": "29.0.3",
    "jit_enabled?": true
  },
  "configuration": {
    "time": 20000000.0,
    "warmup": 10000000.0,
    "parallel": 1,
    "percentiles": [50, 99],
    "max_sample_size": 20000
  },
  "scenarios": [
    {
      "job_name": "RowBinary.encode_rows/2",
      "input_name": "1000 rows",
      "run_time": {
        "samples": [357000, 339500, 551708],
        "statistics": {
          "ips": 2485.05,
          "average": 402405.88,
          "median": 388562.5,
          "std_dev_ratio": 0.1445,
          "percentiles": {"25": 349646.0, "50": 388562.5, "75": 454552.5, "99": 555458.0},
          "sample_size": 50
        }
      }
    }
  ]
}
```

To query history without modifying the working tree:

```sh
git fetch origin benchmark-results
git worktree add .context/benchmark-results origin/benchmark-results
clickhouse local -q "SELECT _file, scenario.job_name, scenario.input_name, round(scenario.run_time.statistics.ips, 2) AS ips FROM file('.context/benchmark-results/data/machine=github-actions-*/version=*/benchmark.json', JSONEachRow) ARRAY JOIN scenarios AS scenario ORDER BY _file DESC, scenario.job_name, scenario.input_name FORMAT PrettyCompact"
```

For the same comparison used by CI, copy two result files to `bench/compare/baseline.json` and `bench/compare/current.json`, then run:

```sh
clickhouse local --queries-file bench/compare.sql
```

The SQL treats an absolute throughput change below 5% as noise (`no material change`). Inspect raw samples in `scenario.run_time.samples` before attributing small changes to code. Remove the `.context/benchmark-results` worktree with `git worktree remove .context/benchmark-results` when finished.
