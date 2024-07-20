defmodule Talan.BloomFilterTest do
  use ExUnit.Case

  alias Talan.BloomFilter

  doctest BloomFilter

  test "new/2 creates a BloomFilter with default options" do
    b = BloomFilter.new(1000)
    assert %BloomFilter{} = b
    assert length(b.hash_functions) == 7
  end

  test "new/2 creates a BloomFilter with custom options" do
    custom_hash_functions = [&:erlang.phash2/1]

    b =
      BloomFilter.new(1000,
        false_positive_probability: 0.001,
        hash_functions: custom_hash_functions
      )

    assert %BloomFilter{} = b
    assert b.hash_functions == custom_hash_functions
  end

  test "new/2 raises error for invalid false_positive_probability" do
    assert_raise ArgumentError, fn ->
      BloomFilter.new(1000, false_positive_probability: 2.0)
    end
  end

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

  test "required_hash_function_count/1" do
    assert BloomFilter.required_hash_function_count(0.01) == 7
    assert BloomFilter.required_hash_function_count(0.001) == 10
    assert BloomFilter.required_hash_function_count(0.0001) == 14
  end

  test "required_filter_length/2" do
    assert BloomFilter.required_filter_length(10_000, 0.01) == 95851
  end

  test "hash_term/2" do
    b = BloomFilter.new(1000)
    hashes = BloomFilter.hash_term(b, :test_term)
    assert is_list(hashes)
    assert length(hashes) == 7
    assert Enum.all?(hashes, &is_integer/1)
  end

  test "false_positive_probability/1" do
    b = BloomFilter.new(1000)
    assert BloomFilter.false_positive_probability(b) == 0.0

    BloomFilter.put(b, "item1")
    fpp = BloomFilter.false_positive_probability(b)
    assert fpp > 0.0 and fpp < 1.0
  end

  test "bits_info/1" do
    b = BloomFilter.new(1000)
    info = BloomFilter.bits_info(b)
    assert %{total_bits: _, set_bits_count: _, set_ratio: _} = info
    assert info.set_bits_count == 0
    assert info.set_ratio == 0.0

    BloomFilter.put(b, "item1")
    updated_info = BloomFilter.bits_info(b)
    assert updated_info.set_bits_count > 0
    assert updated_info.set_ratio > 0.0
  end

  test "serialize and deserialize" do
    original = BloomFilter.new(1000)
    BloomFilter.put(original, "item1")
    BloomFilter.put(original, "item2")

    serialized = BloomFilter.serialize(original)
    assert is_binary(serialized)

    deserialized = BloomFilter.deserialize(serialized)
    assert %BloomFilter{} = deserialized
    assert deserialized.filter_length == original.filter_length
    assert length(deserialized.hash_functions) == length(original.hash_functions)

    # Check that the deserialized filter has the same members
    assert BloomFilter.member?(deserialized, "item1")
    assert BloomFilter.member?(deserialized, "item2")
    refute BloomFilter.member?(deserialized, "item3")

    # Check that the serialized and deserialized filters have the same bits set
    original_info = BloomFilter.bits_info(original)
    deserialized_info = BloomFilter.bits_info(deserialized)
    assert original_info == deserialized_info
  end
end
