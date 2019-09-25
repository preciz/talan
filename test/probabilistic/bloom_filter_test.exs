defmodule Probabilistic.Membership.BloomFilterTest do
  use ExUnit.Case

  alias Probabilistic.BloomFilter

  doctest BloomFilter

  test "empty has no member" do
    b = BloomFilter.new(1024, 0.01)

    assert BloomFilter.member?(b, "hello") == false
  end

  test "member?" do
    b = BloomFilter.new(1024, 0.01)

    BloomFilter.put(b, "hello")

    assert BloomFilter.member?(b, "hello") == true

    assert BloomFilter.member?(b, "ok") == false
  end

  test "test member? 2" do
    b = BloomFilter.new(1024, 0.01)

    before_result =
      for i <- 1..100 do
        b |> BloomFilter.member?(i)
      end

    assert before_result |> Enum.all?(&(&1 == false))

    for i <- 1..100 do
      b |> BloomFilter.put(i)
    end

    after_result =
      for i <- 1..100 do
        b |> BloomFilter.member?(i)
      end

    assert after_result |> Enum.all?()
  end

  test "merge" do
    hash_functions = [
      BloomFilter.seed_murmur_hash_fun(5),
      BloomFilter.seed_murmur_hash_fun(7)
    ]

    b1 = BloomFilter.new(1024, 0.01, hash_functions)
    b2 = BloomFilter.new(1024, 0.01, hash_functions)

    BloomFilter.put(b1, "hello")
    BloomFilter.put(b2, "world")

    b3 = BloomFilter.merge([b1, b2])

    assert BloomFilter.member?(b3, "hello") == true
    assert BloomFilter.member?(b3, "world") == true
    assert BloomFilter.member?(b3, "abcde") == false
    assert BloomFilter.member?(b3, "okkkk") == false
  end

  test "intersection" do
    hash_functions = [
      BloomFilter.seed_murmur_hash_fun(5),
      BloomFilter.seed_murmur_hash_fun(7)
    ]

    b1 = BloomFilter.new(1024, 0.01, hash_functions)
    b2 = BloomFilter.new(1024, 0.01, hash_functions)

    BloomFilter.put(b1, "hello")
    BloomFilter.put(b2, "hello")
    BloomFilter.put(b2, "world")

    b3 = BloomFilter.intersection([b1, b2])

    assert BloomFilter.member?(b3, "hello") == true
    assert BloomFilter.member?(b3, "world") == false
  end
end
