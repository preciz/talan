defmodule Talan.PropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Talan.BloomFilter
  alias Talan.CountingBloomFilter
  alias Talan.LinearCounter

  @universe 0..127

  property "to_bitstring/1 preserves every bit" do
    check all(bitstring <- bitstring(max_length: 256)) do
      bits = Talan.to_bitstring(bitstring)

      assert length(bits) == bit_size(bitstring)
      assert Enum.all?(bits, &(&1 in [0, 1]))
      assert Enum.reduce(bits, <<>>, fn bit, acc -> <<acc::bitstring, bit::1>> end) == bitstring
    end
  end

  property "BloomFilter membership matches a collision-free set model" do
    check all(values <- list_of(integer(@universe), max_length: 80)) do
      filter = BloomFilter.new(128, hash_functions: [&identity/1])
      Enum.each(values, &BloomFilter.put(filter, &1))
      expected = MapSet.new(values)

      Enum.each(@universe, fn value ->
        assert BloomFilter.member?(filter, value) == MapSet.member?(expected, value)
      end)
    end
  end

  property "BloomFilter merge and intersection match set operations" do
    check all(
            left_values <- list_of(integer(@universe), max_length: 80),
            right_values <- list_of(integer(@universe), max_length: 80)
          ) do
      hash_functions = [&identity/1]
      left = BloomFilter.new(128, hash_functions: hash_functions)
      right = BloomFilter.new(128, hash_functions: hash_functions)

      Enum.each(left_values, &BloomFilter.put(left, &1))
      Enum.each(right_values, &BloomFilter.put(right, &1))

      merged = BloomFilter.merge([left, right])
      intersected = BloomFilter.intersection([left, right])
      union_model = MapSet.union(MapSet.new(left_values), MapSet.new(right_values))
      intersection_model = MapSet.intersection(MapSet.new(left_values), MapSet.new(right_values))

      Enum.each(@universe, fn value ->
        assert BloomFilter.member?(merged, value) == MapSet.member?(union_model, value)

        assert BloomFilter.member?(intersected, value) ==
                 MapSet.member?(intersection_model, value)
      end)
    end
  end

  property "CountingBloomFilter follows a signed frequency model" do
    operation = tuple({member_of([:put, :delete]), integer(@universe)})

    check all(operations <- list_of(operation, max_length: 100)) do
      filter =
        CountingBloomFilter.new(128,
          counters_bit_size: 16,
          hash_functions: [&identity/1]
        )

      model =
        Enum.reduce(operations, %{}, fn {action, value}, counts ->
          increment = if action == :put, do: 1, else: -1
          apply(CountingBloomFilter, action, [filter, value])
          Map.update(counts, value, increment, &(&1 + increment))
        end)

      Enum.each(@universe, fn value ->
        expected_count = Map.get(model, value, 0)
        expected_member? = expected_count > 0

        assert CountingBloomFilter.count(filter, value) == expected_count
        assert CountingBloomFilter.member?(filter, value) == expected_member?
      end)
    end
  end

  property "LinearCounter cardinality is invariant under duplicates and ordering" do
    check all(values <- list_of(integer(@universe), max_length: 100)) do
      counter = LinearCounter.new(128, hash_function: &identity/1)
      Enum.each(values, &LinearCounter.put(counter, &1))
      cardinality = LinearCounter.cardinality(counter)

      values
      |> Enum.reverse()
      |> Enum.each(&LinearCounter.put(counter, &1))

      assert LinearCounter.cardinality(counter) == cardinality
    end
  end

  property "Stream.uniq/2 matches Enum.uniq/1 with collision-free hashes" do
    check all(values <- list_of(integer(@universe), max_length: 100)) do
      filter = BloomFilter.new(128, hash_functions: [&identity/1])

      assert values |> Talan.Stream.uniq(filter) |> Enum.to_list() == Enum.uniq(values)
    end
  end

  defp identity(value), do: value
end
