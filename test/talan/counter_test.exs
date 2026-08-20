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

  test "new/2 validates expected cardinality and options" do
    for expected_cardinality <- [0, -1, 1.0, :invalid] do
      assert_raise ArgumentError, ~r/expected_cardinality must be a positive integer/, fn ->
        Counter.new(expected_cardinality)
      end
    end

    assert_raise ArgumentError, ~r/options must be a keyword list/, fn ->
      apply(Counter, :new, [1000, %{hash_function: &is_integer/1}])
    end

    assert_raise ArgumentError, ~r/unknown options: \[:unknown\]/, fn ->
      Counter.new(1000, unknown: true)
    end

    for hash_function <- [:invalid, fn _left, _right -> 0 end] do
      assert_raise ArgumentError, ~r/hash_function must be a one-argument function/, fn ->
        Counter.new(1000, hash_function: hash_function)
      end
    end
  end

  test "new/2 allocates at least ten bits per expected element" do
    for expected_cardinality <- [1, 7, 1_000, 10_000] do
      counter = Counter.new(expected_cardinality)
      required_bits = expected_cardinality * 10

      assert counter.filter_length >= required_bits
      assert counter.filter_length < required_bits + 64
    end
  end

  test "put/2 adds elements to the Counter" do
    c = Counter.new(1000)
    assert :ok = Counter.put(c, "test")
    assert :ok = Counter.put(c, "test")
    assert Counter.cardinality(c) == 1
  end

  test "clear/1 resets the Counter in place" do
    counter = Counter.new(1000)
    Counter.put(counter, "present")

    assert Counter.cardinality(counter) == 1
    assert Counter.clear(counter) == counter
    assert Counter.cardinality(counter) == 0
  end

  test "cardinality/1 returns 0 for empty Counter" do
    c = Counter.new(1000)
    assert Counter.cardinality(c) == 0
  end

  test "cardinality/1 returns the filter length when all bits are set" do
    c = Counter.new(1, hash_function: fn value -> value end)

    Enum.each(0..(c.filter_length - 1), &Counter.put(c, &1))

    assert Counter.cardinality(c) == c.filter_length
  end
end
