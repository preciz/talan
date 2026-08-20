defmodule Talan.StreamTest do
  use ExUnit.Case

  doctest Talan.Stream

  test "rejects duplicate elements" do
    list = ~w(a b c a b c d a)

    bloom_filter = Talan.BloomFilter.new(1000, false_positive_probability: 0.01)
    uniq_list = Talan.Stream.uniq(list, bloom_filter) |> Enum.to_list()

    assert ["a", "b", "c", "d"] == uniq_list
  end

  test "re-enumeration observes the mutated Bloom filter" do
    bloom_filter = Talan.BloomFilter.new(1000, hash_functions: [fn value -> value end])
    stream = Talan.Stream.uniq([1, 2, 1], bloom_filter)

    assert Enum.to_list(stream) == [1, 2]
    assert Enum.to_list(stream) == []
  end
end
