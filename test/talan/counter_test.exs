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

  test "new/2 creates a Counter with custom options" do
    custom_hash_function = fn x -> :erlang.phash2(x) end
    c = Counter.new(10_000, hash_function: custom_hash_function)
    assert %Counter{} = c
    assert c.hash_function == custom_hash_function
  end

  test "put/2 adds elements to the Counter" do
    c = Counter.new(1000)
    assert :ok = Counter.put(c, "test")
    assert :ok = Counter.put(c, "test")
    assert Counter.cardinality(c) == 1
  end

  test "cardinality/1 returns 0 for empty Counter" do
    c = Counter.new(1000)
    assert Counter.cardinality(c) == 0
  end
end
