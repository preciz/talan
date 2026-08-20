defmodule Talan.CountingBloomFilterTest do
  use ExUnit.Case

  alias Talan.CountingBloomFilter

  doctest CountingBloomFilter

  test "new/2 creates a CountingBloomFilter with default options" do
    cbf = CountingBloomFilter.new(1000)
    assert %CountingBloomFilter{} = cbf
  end

  test "new/2 creates a CountingBloomFilter with custom options" do
    cbf = CountingBloomFilter.new(1000, counters_bit_size: 16, signed: false)
    assert %CountingBloomFilter{} = cbf
  end

  test "new/2 allocates one counter per Bloom filter bit" do
    for counters_bit_size <- [2, 4, 8, 16, 32] do
      cbf = CountingBloomFilter.new(1000, counters_bit_size: counters_bit_size)

      assert cbf.counter.size == cbf.filter_length
    end
  end

  test "put/2 and count/2 work correctly" do
    cbf = CountingBloomFilter.new(1000)
    CountingBloomFilter.put(cbf, "test")
    CountingBloomFilter.put(cbf, "test")
    assert CountingBloomFilter.count(cbf, "test") == 2
  end

  test "put/2 does not partially update counters when hashing fails" do
    cbf =
      CountingBloomFilter.new(100,
        hash_functions: [fn _term -> 0 end, fn _term -> raise "hash failed" end]
      )

    assert_raise RuntimeError, "hash failed", fn -> CountingBloomFilter.put(cbf, :term) end
    assert Abit.Counter.get(cbf.counter, 0) == 0
  end

  test "count/2 uses the minimum counter to limit collision inflation" do
    cbf =
      CountingBloomFilter.new(1000,
        hash_functions: [
          fn _term -> 0 end,
          fn
            :target -> 1
            :colliding -> 2
          end
        ]
      )

    CountingBloomFilter.put(cbf, :target)
    Enum.each(1..10, fn _ -> CountingBloomFilter.put(cbf, :colliding) end)

    assert CountingBloomFilter.count(cbf, :target) == 1
  end

  test "delete/2 decrements count" do
    cbf = CountingBloomFilter.new(1000)
    CountingBloomFilter.put(cbf, "test")
    CountingBloomFilter.put(cbf, "test")
    CountingBloomFilter.delete(cbf, "test")
    assert CountingBloomFilter.count(cbf, "test") == 1
  end

  test "count/2 can return a negative count after deleting an absent term" do
    cbf = CountingBloomFilter.new(1000)

    CountingBloomFilter.delete(cbf, "absent")

    assert CountingBloomFilter.count(cbf, "absent") == -1
  end

  test "member?/2 returns correct membership" do
    cbf = CountingBloomFilter.new(1000)
    CountingBloomFilter.put(cbf, "test")
    assert CountingBloomFilter.member?(cbf, "test")
    refute CountingBloomFilter.member?(cbf, "not_present")
  end

  test "does not allocate a redundant Bloom filter bit array" do
    cbf = CountingBloomFilter.new(1000, hash_functions: [fn _term -> 0 end])

    refute Map.has_key?(cbf, :bloom_filter)
    refute Map.has_key?(cbf, :atomics_ref)
    assert cbf.counter.size == cbf.filter_length
  end

  test "concurrent puts and deletes leave membership consistent with the count" do
    cbf =
      CountingBloomFilter.new(1000,
        counters_bit_size: 32,
        hash_functions: [fn _term -> 0 end]
      )

    operations = [:put | List.duplicate(:put, 100) ++ List.duplicate(:delete, 100)]

    operations
    |> Task.async_stream(
      fn
        :put -> CountingBloomFilter.put(cbf, "test")
        :delete -> CountingBloomFilter.delete(cbf, "test")
      end,
      max_concurrency: 20,
      ordered: false
    )
    |> Enum.each(fn result -> assert result == {:ok, :ok} end)

    assert CountingBloomFilter.count(cbf, "test") == 1
    assert CountingBloomFilter.member?(cbf, "test")
  end

  test "cardinality/1 returns correct estimation" do
    cbf = CountingBloomFilter.new(1000)
    Enum.each(1..100, fn i -> CountingBloomFilter.put(cbf, "item_#{i}") end)
    cardinality = CountingBloomFilter.cardinality(cbf)
    assert cardinality >= 95 and cardinality <= 105
  end

  test "cardinality/1 handles empty and saturated counters" do
    cbf = CountingBloomFilter.new(10, false_positive_probability: 0.1)

    assert CountingBloomFilter.cardinality(cbf) == 0

    Enum.each(0..(cbf.counter.size - 1), fn index ->
      Abit.Counter.put(cbf.counter, index, 1)
    end)

    hash_count = length(cbf.hash_functions)
    assert CountingBloomFilter.cardinality(cbf) == round(cbf.counter.size / hash_count)
  end

  test "false_positive_probability/1 returns a value between 0 and 1" do
    cbf = CountingBloomFilter.new(1000)
    Enum.each(1..100, fn i -> CountingBloomFilter.put(cbf, "item_#{i}") end)
    fpp = CountingBloomFilter.false_positive_probability(cbf)
    assert fpp > 0 and fpp < 1
  end

  test "cardinality and false-positive probability derive from counters" do
    cbf = CountingBloomFilter.new(1000, hash_functions: [fn term -> term end])

    Enum.each(0..9, &CountingBloomFilter.put(cbf, &1))

    assert CountingBloomFilter.cardinality(cbf) == 10

    assert CountingBloomFilter.false_positive_probability(cbf) ==
             10 / cbf.filter_length
  end
end
