defmodule Talan.CounterTest do
  use ExUnit.Case

  alias Talan.Counter
  doctest Counter

  test "cardinality estimation is close to real" do
    c = Talan.Counter.new(100_000)

    1..10_000 |> Enum.each(fn n -> Talan.Counter.put(c, n) end)

    cardinality = c |> Talan.Counter.cardinality()

    assert 9900..10100 |> Enum.member?(cardinality)
  end
end
