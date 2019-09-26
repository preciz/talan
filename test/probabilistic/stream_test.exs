defmodule Probabilistic.StreamTest do
  use ExUnit.Case

  doctest Probabilistic.Stream

  test "rejects non uniq elements" do
    uniq_list = ~w(a b c a b c d) |> Probabilistic.Stream.uniq |> Enum.to_list

    assert ["a", "b", "c", "d"] == uniq_list
  end
end
