defmodule Ch.MixProject do
  use Mix.Project

  def project do
    [
      app: :ch,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mint, "~> 1.4"},
      {:db_connection, "~> 2.4"},
      {:jason, "~> 1.4"},
      {:benchee, "~> 1.1", only: [:bench]},
      {:nimble_csv, "~> 1.2", only: [:bench]},
      {:dialyxir, "~> 1.2", only: [:dev], runtime: false}
    ]
  end
end
