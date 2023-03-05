defmodule Talan.StreamTest do
  use ExUnit.Case

  doctest Talan.Stream

  test "rejects non uniq elements" do
    list = ~w(a b c a b c d a)

    bloom_filter = Talan.BloomFilter.new(1000, false_positive_probability: 0.01)
    uniq_list = Talan.Stream.uniq(list, bloom_filter) |> Enum.to_list()

    assert ["a", "b", "c", "d"] == uniq_list
  end
end
