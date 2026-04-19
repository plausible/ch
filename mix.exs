defmodule Ch.MixProject do
  use Mix.Project

  @source_url "https://github.com/plausible/ch"
  @version "0.9.0"

  def project do
    [
      app: :ch,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      description: "HTTP ClickHouse driver for Elixir",
      deps: deps(),
      aliases: [
        docs: [&telemetry_docs/1, "docs"]
      ],

      # Test coverage
      test_coverage: [
        ignore_modules: [
          Help
        ]
      ],

      # Dialyzer
      dialyzer: [
        plt_local_path: "plts",
        plt_core_path: "plts"
      ],

      # Docs
      name: "Ch",
      docs: [
        main: "readme",
        source_url: @source_url,
        source_ref: "v#{@version}",
        extras: ["README.md", "CHANGELOG.md", "pages/telemetry-events.md"],
        skip_undefined_reference_warnings_on: ["CHANGELOG.md"]
      ],

      # Hex
      package: [
        licenses: ["MIT"],
        # TODO add org=plausible, and link to plausible.io?
        links: %{"GitHub" => @source_url}
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(:bench), do: ["lib", "bench/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mint, "~> 1.0"},
      {:nimble_pool, "~> 1.0"},
      {:nimble_options, "~> 1.0"},
      {:telemetry, "~> 1.0"},
      {:telemetry_docs, "~> 0.1.0", only: :dev},
      {:decimal, "~> 2.0"},
      {:ecto, "~> 3.13.0", optional: true},
      {:benchee, "~> 1.0", only: :bench},
      # {:benchee_github_action_benchmark,
      #  github: "ruslandoga/benchee_github_action_benchmark", only: :bench},
      {:dialyxir, "~> 1.0", only: [:dev, :test], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev},
      {:tz, "~> 0.28.1", only: :test},
      {:nimble_lz4, "~> 1.0", only: [:dev, :test, :bench]},
      {:stream_data, "~> 1.0", only: :test},
      {:credo, "~> 1.0", only: [:dev, :test]}
    ]
  end

  defp telemetry_docs(_args) do
    Mix.Task.run("loadpaths")

    {sections, _bindings} = Code.eval_file("pages/telemetry_events.exs")
    sections_md = TelemetryDocs.sections_to_markdown(sections)

    summary_list =
      sections
      |> Enum.flat_map(&Keyword.fetch!(&1, :events))
      |> Enum.map_join("\n", fn {name, opts} ->
        name = Atom.to_string(name)

        # Converts "[:ch, :query, :start]" to "ch-query-start"
        anchor =
          name
          |> String.replace(["[", "]", ":"], "")
          |> String.replace(", ", "-")

        "- [`#{name}`](##{anchor}) - #{Keyword.fetch!(opts, :doc)}"
      end)

    preface = """
    # Telemetry Events

    Ch emits the following Telemetry events:

    #{summary_list}

    > #### Time Units {: .warning}
    >
    > All `:duration` and `:system_time` measurements are in the `:native` time unit. See `System.convert_time_unit/3` for how to convert it to "human" units.

    """

    File.write!("pages/telemetry-events.md", preface <> sections_md)
  end
end
