defmodule Probabilistic.CounterTest do
  use ExUnit.Case

  alias Probabilistic.Counter
  doctest Counter

  test "cardinality estimation is close to real" do
    c = Probabilistic.Counter.new(100_000)

    1..10_000 |> Enum.each(fn n -> Probabilistic.Counter.put(c, n) end)

    cardinality = c |> Probabilistic.Counter.cardinality()

    assert 9900..10100 |> Enum.member?(cardinality)
  end
end
