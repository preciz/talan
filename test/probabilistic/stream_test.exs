defmodule Probabilistic.StreamTest do
  use ExUnit.Case

  doctest Probabilistic.Stream

  test "rejects non uniq elements" do
    list = ~w(a b c a b c d a)

    bloom_filter = Probabilistic.BloomFilter.new(1000, false_positive_probability: 0.01)
    uniq_list = Probabilistic.Stream.uniq(list, bloom_filter) |> Enum.to_list

    assert ["a", "b", "c", "d"] == uniq_list
  end
end
