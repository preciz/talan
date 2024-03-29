defmodule Talan.MixProject do
  use Mix.Project

  @version "0.1.2"
  @github "https://github.com/preciz/talan"

  def project do
    [
      app: :talan,
      version: @version,
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package(),
      homepage_url: @github,
      description: """
      Probabilistic data structures powered by atomics.
      Bloom filter, Counting bloom filter, Linear counter (cardinality)
      """
    ]
  end

  def application do
    [
      extra_applications: []
    ]
  end

  defp deps do
    [
      {:murmur, "~> 1.0"},
      {:abit, "~> 0.3"},
      {:ex_doc, "~> 0.29", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "Talan",
      source_ref: "v#{@version}",
      source_url: @github
    ]
  end

  defp package do
    [
      maintainers: ["Barna Kovacs"],
      licenses: ["MIT"],
      links: %{"GitHub" => @github}
    ]
  end
end
