defmodule Probabilistic.CountingBloomFilter do
  @moduledoc """
  Counting bloom filters support probabilistic deletion of elements.
  """

  alias Probabilistic.BloomFilter, as: BF
  alias Probabilistic.CountingBloomFilter, as: CBF

  defstruct [:bloom_filter, :counter]

  @doc """
  ## Counter options:
    * `:counters_bit_size` - bit size of counters, defaults to 8
    * `:signed` - to have signed or unsigned counters

  ## BloomFilter options:
    * `:false_positive_probability` - a float, defaults to 0.01
    * `:hash_functions` - a list of hash functions, defaults to randomly seeded murmur
  """
  def new(capacity, options \\ []) do
    bloom_filter = BF.new(capacity, options)

    counters_bit_size = options |> Keyword.get(:counters_bit_size, 8)
    signed = options |> Keyword.get(:signed, true)

    counter =
      Abit.Counter.new(
        bloom_filter.filter_length * counters_bit_size,
        counters_bit_size,
        signed: signed
      )

    %CBF{
      bloom_filter: bloom_filter,
      counter: counter
    }
  end

  @doc """
  Puts `term` into `bloom_filter` and increments counters in `counter`.

  After this the `member?/2` function will return `true`
  for the membership of `term` unless bits representing
  membership are modified by the `delete/2` function.

  Returns `:ok`.
  """
  def put(%CBF{bloom_filter: bloom_filter, counter: counter}, term) do
    hashes = BF.hash_term(bloom_filter, term)

    BF.put_hashes(bloom_filter, hashes)

    hashes
    |> Enum.each(fn hash ->
      Abit.Counter.add(counter, hash, 1)
    end)

    :ok
  end

  @doc """
  Probabilistically delete `term` from `bloom_filter` and
  decrement counters in `counter`.
  """
  def delete(%CBF{bloom_filter: bloom_filter, counter: counter}, term) do
    hashes = BF.hash_term(bloom_filter, term)

    hashes
    |> Enum.each(fn hash ->
      Abit.Counter.add(counter, hash, -1)

      if Abit.Counter.get(counter, hash) <= 0 do
        Abit.set_bit(bloom_filter.atomics_ref, hash, 0)
      end
    end)

    :ok
  end

  @doc """
  See `Probabilistic.BloomFilter.member?/2` for
  docs.
  """
  def member?(%CBF{bloom_filter: bloom_filter}, term) do
    BF.member?(bloom_filter, term)
  end

  @doc """
  Returns probabilistic count of term in `counter`.
  """
  def count(%CBF{bloom_filter: bloom_filter, counter: counter}, term) do
    hashes = BF.hash_term(bloom_filter, term)

    hashes
    |> Enum.map(fn hash ->
      Abit.Counter.get(counter, hash)
    end)
    |> Enum.min()
  end

  @doc """
  See `Probabilistic.BloomFilter.estimate_element_count/1` for
  docs.
  """
  def estimate_element_count(%CBF{bloom_filter: bloom_filter}) do
    BF.estimate_element_count(bloom_filter)
  end

  @doc """
  See `Probabilistic.BloomFilter.current_false_positive_probability/1` for
  docs.
  """
  def current_false_positive_probability(%CBF{bloom_filter: bloom_filter}) do
    BF.current_false_positive_probability(bloom_filter)
  end
end
