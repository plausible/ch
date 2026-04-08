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
        extras: ["README.md", "CHANGELOG.md"],
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
      {:nimble_pool, "~> 1.1"},
      {:nimble_options, "~> 1.1"},
      {:telemetry, "~> 1.4"},
      {:telemetry_docs, "~> 0.1.0"},
      {:decimal, "~> 2.0"},
      {:ecto, "~> 3.13.0", optional: true},
      {:benchee, "~> 1.0", only: :bench},
      {:dialyxir, "~> 1.0", only: [:dev, :test], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :docs},
      {:tz, "~> 0.28.1", only: :test},
      {:nimble_lz4, "~> 1.1", only: [:dev, :test, :bench]},
      {:stream_data, "~> 1.3", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test]}
    ]
  end
end
