defmodule Fishbowl.MixProject do
  use Mix.Project

  @app :fishbowl

  def project do
    [
      app: @app,
      version: "1.0.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Etl.Main, []},
      extra_applications: [:logger, :runtime_tools, :tools]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.4", only: [:dev, :test], runtime: false},
      {:csv, "~> 2.3"},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:elixir_uuid, "~> 1.2"},
      {:ex_aws, "~> 2.0"},
      {:ex_aws_s3, "~> 2.0"},
      {:export, "~> 0.1.0"},
      {:flow, "~> 1.0"},
      {:hackney, "~> 1.9"},
      {:heap, "~> 2.0"},
      {:jason, "~> 1.2"},
      {:jiffy, "~> 1.0.8"},
      {:memoize, "~> 1.3"},
      {:msgpax, "~> 2.0"},
      {:prestige, "~> 1.0"},
      {:recase, "~> 0.5"},
      {:sweet_xml, "~> 0.6"},
      {:temp, "~> 0.4"},
      {:typed_struct, "~> 0.1.4"}
    ]
  end
end
