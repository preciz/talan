defmodule Probabilistic.CountingBloomFilter do
  @moduledoc """
  Counting bloom filters support probabilistic deletion
  of elements but have higher memory consumption.
  They need to store a counter of N bits for every bloom filter bit.
  """

  alias Probabilistic.BloomFilter, as: BF
  alias Probabilistic.CountingBloomFilter, as: CBF

  @enforce_keys [:bloom_filter, :counter]
  defstruct [:bloom_filter, :counter]

  @type t :: %__MODULE__{
          bloom_filter: reference,
          counter: Abit.Counter.t()
        }

  @doc """
  Returns a new `%Probabilistic.CountingBloomFilter{}` struct.

  `cardinality` is the expected number of unique items. Duplicated items
  can be infinite.

  ## Options
    * `:counters_bit_size` - bit size of counters, defaults to `8`
    * `:signed` - to have signed or unsigned counters, defaults to `true`
    * `:false_positive_probability` - a float, defaults to `0.01`
    * `:hash_functions` - a list of hash functions, defaults to randomly seeded murmur

  ## Examples
      iex> cbf = Probabilistic.CountingBloomFilter.new(10_000)
      iex> cbf |> Probabilistic.CountingBloomFilter.put("hat")
      iex> cbf |> Probabilistic.CountingBloomFilter.put("hat")
      iex> cbf |> Probabilistic.CountingBloomFilter.put("phone")
      :ok
      iex> cbf |> Probabilistic.CountingBloomFilter.count("hat")
      2
  """
  @spec new(pos_integer, list) :: t
  def new(cardinality, options \\ []) do
    bloom_filter = BF.new(cardinality, options)

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

  ## Examples
      iex> cbf = Probabilistic.CountingBloomFilter.new(10_000)
      iex> cbf |> Probabilistic.CountingBloomFilter.put("hat")
      :ok
  """
  @spec put(t, any) :: :ok
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

  ## Examples
      iex> cbf = Probabilistic.CountingBloomFilter.new(10_000)
      iex> cbf |> Probabilistic.CountingBloomFilter.put("hat")
      iex> cbf |> Probabilistic.CountingBloomFilter.count("hat")
      1
      iex> cbf |> Probabilistic.CountingBloomFilter.delete("hat")
      :ok
      iex> cbf |> Probabilistic.CountingBloomFilter.count("hat")
      0
      iex> cbf |> Probabilistic.CountingBloomFilter.delete("this wasn't there")
      iex> cbf |> Probabilistic.CountingBloomFilter.count("this wasn't there")
      -1
  """
  @spec delete(t, any) :: :ok
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
  See `Probabilistic.BloomFilter.member?/2` for docs.

  ## Examples
      iex> cbf = Probabilistic.CountingBloomFilter.new(10_000)
      iex> cbf |> Probabilistic.CountingBloomFilter.put("hat")
      iex> cbf |> Probabilistic.CountingBloomFilter.member?("hat")
      true
  """
  @spec member?(t, any) :: boolean
  def member?(%CBF{bloom_filter: bloom_filter}, term) do
    BF.member?(bloom_filter, term)
  end

  @doc """
  Returns probabilistic count of term in `counter`.

  This means that (given no hash collisions) it returns how many times
  the item was put into the CountingBloomFilter. A few hash collisions
  should be also fine since it returns the average count of the counters.
  An single item is hashed with multiple counters.

  ## Examples
      iex> cbf = Probabilistic.CountingBloomFilter.new(10_000)
      iex> cbf |> Probabilistic.CountingBloomFilter.put("hat")
      iex> cbf |> Probabilistic.CountingBloomFilter.put("hat")
      iex> cbf |> Probabilistic.CountingBloomFilter.put("hat")
      iex> cbf |> Probabilistic.CountingBloomFilter.count("hat")
      3
  """
  @spec count(t, any) :: non_neg_integer
  def count(%CBF{bloom_filter: bloom_filter, counter: counter}, term) do
    hashes = BF.hash_term(bloom_filter, term)

    counters =
      hashes
      |> Enum.map(fn hash ->
        Abit.Counter.get(counter, hash)
      end)

    round(Enum.sum(counters) / length(counters))
  end

  @doc """
  See `Probabilistic.BloomFilter.cardinality/1` for docs.

  ## Examples
      iex> cbf = Probabilistic.CountingBloomFilter.new(10_000)
      iex> cbf |> Probabilistic.CountingBloomFilter.put("hat")
      iex> cbf |> Probabilistic.CountingBloomFilter.put("hat")
      iex> cbf |> Probabilistic.CountingBloomFilter.put("hat")
      iex> cbf |> Probabilistic.CountingBloomFilter.put("car keys")
      iex> cbf |> Probabilistic.CountingBloomFilter.cardinality()
      2
  """
  @spec cardinality(t) :: non_neg_integer
  def cardinality(%CBF{bloom_filter: bloom_filter}) do
    BF.cardinality(bloom_filter)
  end

  @doc """
  See `Probabilistic.BloomFilter.false_positive_probability/1` for
  docs.
  """
  @spec false_positive_probability(t) :: float
  def false_positive_probability(%CBF{bloom_filter: bloom_filter}) do
    BF.false_positive_probability(bloom_filter)
  end
end
