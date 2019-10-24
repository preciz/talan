defmodule Talan.MixProject do
  use Mix.Project

  def project do
    [
      app: :talan,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    []
  end

  defp deps do
    [
      {:murmur, "~> 1.0"},
      {:abit, "~> 0.3"},
    ]
  end
end
