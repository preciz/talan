defmodule Talan.BloomFilterTest do
  use ExUnit.Case

  alias Talan.BloomFilter

  doctest BloomFilter

  test "new/2 creates a BloomFilter with default options" do
    b = BloomFilter.new(1000)
    assert %BloomFilter{} = b
    assert length(b.hash_functions) == 7
  end

  test "new/2 allocates at least the required number of bits" do
    cardinality = 10_000
    false_positive_probability = 0.01

    required_bits =
      BloomFilter.required_filter_length(cardinality, false_positive_probability)

    b =
      BloomFilter.new(cardinality,
        false_positive_probability: false_positive_probability
      )

    assert b.filter_length >= required_bits
    assert b.filter_length < required_bits + 64
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
    for probability <- [0, 1, -0.1, 1.1, :invalid] do
      assert_raise ArgumentError, ~r/false_positive_probability/, fn ->
        BloomFilter.new(1000, false_positive_probability: probability)
      end
    end
  end

  test "new/2 validates cardinality and options" do
    for cardinality <- [0, -1, 1.0, :invalid] do
      assert_raise ArgumentError, ~r/cardinality must be a positive integer/, fn ->
        BloomFilter.new(cardinality)
      end
    end

    assert_raise ArgumentError, ~r/options must be a keyword list/, fn ->
      apply(BloomFilter, :new, [1000, %{hash_functions: []}])
    end

    assert_raise ArgumentError, ~r/unknown options: \[:unknown\]/, fn ->
      BloomFilter.new(1000, unknown: true)
    end
  end

  test "new/2 validates custom hash functions" do
    for hash_functions <- [:invalid, [fn _left, _right -> 0 end], [&is_integer/1, :invalid]] do
      assert_raise ArgumentError, ~r/hash_functions/, fn ->
        BloomFilter.new(1000, hash_functions: hash_functions)
      end
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

  test "put/2 does not partially update the filter when hashing fails" do
    b =
      BloomFilter.new(100,
        hash_functions: [fn _term -> 0 end, fn _term -> raise "hash failed" end]
      )

    assert_raise RuntimeError, "hash failed", fn -> BloomFilter.put(b, :term) end
    assert Abit.bit_at(b.atomics_ref, 0) == 0
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

  test "merge rejects filters with different sizes or hash functions" do
    hash_functions = [fn term -> :erlang.phash2(term) end]

    assert_raise ArgumentError, ~r/same size and use the same hash functions/, fn ->
      BloomFilter.merge([
        BloomFilter.new(100, hash_functions: hash_functions),
        BloomFilter.new(1000, hash_functions: hash_functions)
      ])
    end

    assert_raise ArgumentError, ~r/same size and use the same hash functions/, fn ->
      BloomFilter.merge([
        BloomFilter.new(100, hash_functions: [fn _term -> 1 end]),
        BloomFilter.new(100, hash_functions: [fn _term -> 2 end])
      ])
    end
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

  test "intersection rejects filters with different sizes or hash functions" do
    hash_functions = [fn term -> :erlang.phash2(term) end]

    assert_raise ArgumentError, ~r/same size and use the same hash functions/, fn ->
      BloomFilter.intersection([
        BloomFilter.new(100, hash_functions: hash_functions),
        BloomFilter.new(1000, hash_functions: hash_functions)
      ])
    end

    assert_raise ArgumentError, ~r/same size and use the same hash functions/, fn ->
      BloomFilter.intersection([
        BloomFilter.new(100, hash_functions: [fn _term -> 1 end]),
        BloomFilter.new(100, hash_functions: [fn _term -> 2 end])
      ])
    end
  end

  test "required_hash_function_count/1" do
    assert BloomFilter.required_hash_function_count(0.01) == 7
    assert BloomFilter.required_hash_function_count(0.001) == 10
    assert BloomFilter.required_hash_function_count(0.0001) == 14

    for probability <- [0.0, 1.0, -0.1, 2.0] do
      assert_raise ArgumentError, ~r/false_positive_probability/, fn ->
        BloomFilter.required_hash_function_count(probability)
      end
    end
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

  test "deserialize rejects external terms that would create new atoms" do
    atom_name = "talan_atom_that_must_not_be_created_#{System.unique_integer([:positive])}"
    atom_name_size = byte_size(atom_name)
    unsafe_binary = <<131, 100, atom_name_size::16, atom_name::binary>>

    assert_raise ArgumentError, fn ->
      BloomFilter.deserialize(unsafe_binary)
    end
  end

  test "cardinality when all bits are set" do
    b = BloomFilter.new(10, false_positive_probability: 0.1)

    for i <- 0..(b.filter_length - 1) do
      Abit.set_bit_at(b.atomics_ref, i, 1)
    end

    hash_count = length(b.hash_functions)
    assert BloomFilter.cardinality(b) == round(b.filter_length / hash_count)
  end
end
