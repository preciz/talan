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

  test "put/2 and count/2 work correctly" do
    cbf = CountingBloomFilter.new(1000)
    CountingBloomFilter.put(cbf, "test")
    CountingBloomFilter.put(cbf, "test")
    assert CountingBloomFilter.count(cbf, "test") == 2
  end

  test "delete/2 decrements count" do
    cbf = CountingBloomFilter.new(1000)
    CountingBloomFilter.put(cbf, "test")
    CountingBloomFilter.put(cbf, "test")
    CountingBloomFilter.delete(cbf, "test")
    assert CountingBloomFilter.count(cbf, "test") == 1
  end

  test "member?/2 returns correct membership" do
    cbf = CountingBloomFilter.new(1000)
    CountingBloomFilter.put(cbf, "test")
    assert CountingBloomFilter.member?(cbf, "test")
    refute CountingBloomFilter.member?(cbf, "not_present")
  end

  test "cardinality/1 returns correct estimation" do
    cbf = CountingBloomFilter.new(1000)
    Enum.each(1..100, fn i -> CountingBloomFilter.put(cbf, "item_#{i}") end)
    cardinality = CountingBloomFilter.cardinality(cbf)
    assert cardinality >= 95 and cardinality <= 105
  end

  test "false_positive_probability/1 returns a value between 0 and 1" do
    cbf = CountingBloomFilter.new(1000)
    Enum.each(1..100, fn i -> CountingBloomFilter.put(cbf, "item_#{i}") end)
    fpp = CountingBloomFilter.false_positive_probability(cbf)
    assert fpp > 0 and fpp < 1
  end
end
