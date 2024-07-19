defmodule Talan.BloomFilterTest do
  use ExUnit.Case

  alias Talan.BloomFilter

  doctest BloomFilter

  test "empty has no member" do
    b = BloomFilter.new(1024)

    assert BloomFilter.member?(b, "hello") == false
  end

  test "member?" do
    b = BloomFilter.new(1024)

    BloomFilter.put(b, "hello")

    assert BloomFilter.member?(b, "hello") == true

    assert BloomFilter.member?(b, "ok") == false
  end

  test "member? with multiple elements" do
    b = BloomFilter.new(1024)

    before_result =
      for i <- 1..100 do
        BloomFilter.member?(b, i)
      end

    assert Enum.all?(before_result, &(&1 == false))

    for i <- 1..100 do
      BloomFilter.put(b, i)
    end

    after_result =
      for i <- 1..100 do
        BloomFilter.member?(b, i)
      end

    assert Enum.all?(after_result)
  end

  test "merge" do
    hash_functions = Talan.seed_n_murmur_hash_fun(2)

    b1 = BloomFilter.new(1024, hash_functions: hash_functions)
    b2 = BloomFilter.new(1024, hash_functions: hash_functions)

    BloomFilter.put(b1, "hello")
    BloomFilter.put(b2, "world")

    b3 = BloomFilter.merge([b1, b2])

    assert BloomFilter.member?(b3, "hello") == true
    assert BloomFilter.member?(b3, "world") == true
    assert BloomFilter.member?(b3, "abcde") == false
    assert BloomFilter.member?(b3, "okkkk") == false
  end

  test "intersection" do
    hash_functions = Talan.seed_n_murmur_hash_fun(2)

    b1 = BloomFilter.new(1024, hash_functions: hash_functions)
    b2 = BloomFilter.new(1024, hash_functions: hash_functions)

    BloomFilter.put(b1, "hello")
    BloomFilter.put(b2, "hello")
    BloomFilter.put(b2, "world")

    b3 = BloomFilter.intersection([b1, b2])

    assert BloomFilter.member?(b3, "hello") == true
    assert BloomFilter.member?(b3, "world") == false
  end
end
