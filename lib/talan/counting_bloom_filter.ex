defmodule Talan.CountingBloomFilter do
  @moduledoc """
  Counting Bloom filter implementation with **safe concurrent access**,
  powered by the [:atomics](http://erlang.org/doc/man/atomics.html) module.

  ## Features

    * Fixed-size Counting Bloom filter
    * Concurrent reads and writes
    * Custom and default hash functions
    * Estimate number of unique elements
    * Estimate false positive probability

  Counting bloom filters support probabilistic deletion
  of elements but have higher memory consumption because
  they need to store a counter of N bits for every Bloom filter bit.

  Counters are the source of truth for membership. Updates to individual
  counters are atomic, but reads concurrent with writes may observe an
  operation in progress.
  """

  alias Talan.BloomFilter, as: BF
  alias Talan.CountingBloomFilter, as: CBF
  alias Talan.Validation

  @counter_bit_sizes [2, 4, 8, 16, 32]
  @options [:counters_bit_size, :signed, :false_positive_probability, :hash_functions]

  @enforce_keys [:filter_length, :hash_functions, :counter]
  defstruct [:filter_length, :hash_functions, :counter]

  @type t :: %__MODULE__{
          filter_length: pos_integer(),
          hash_functions: nonempty_list(Talan.hash_function()),
          counter: Abit.Counter.t()
        }

  @type counters_bit_size :: 2 | 4 | 8 | 16 | 32
  @type option ::
          {:counters_bit_size, counters_bit_size()}
          | {:signed, boolean()}
          | {:false_positive_probability, float()}
          | {:hash_functions, list(Talan.hash_function())}
  @type options :: list(option())

  @doc """
  Returns a new `%Talan.CountingBloomFilter{}` struct.

  `cardinality` is the expected number of unique items. Duplicate items do not
  count toward the expected cardinality.

  Raises `ArgumentError` if `cardinality` or any option is invalid.

  ## Options
    * `:counters_bit_size` - bit size of counters, defaults to `8`
    * `:signed` - whether counters are signed, defaults to `true`
    * `:false_positive_probability` - a float, defaults to `0.01`
    * `:hash_functions` - a list of functions that each accept a term and return a
      non-negative integer, defaults to randomly seeded Murmur

  ## Examples

      iex> cbf = Talan.CountingBloomFilter.new(10_000)
      iex> cbf |> Talan.CountingBloomFilter.put("hat")
      iex> cbf |> Talan.CountingBloomFilter.put("hat")
      iex> cbf |> Talan.CountingBloomFilter.put("phone")
      :ok
      iex> cbf |> Talan.CountingBloomFilter.count("hat")
      2
      iex> cbf |> Talan.CountingBloomFilter.count("phone")
      1
  """
  @spec new(pos_integer()) :: t()
  @spec new(pos_integer(), options()) :: t()
  def new(cardinality, options \\ []) do
    Validation.positive_integer!(cardinality, :cardinality)
    Validation.options!(options, @options)

    bloom_options = Keyword.take(options, [:false_positive_probability, :hash_functions])
    {filter_length, hash_functions} = BF.configuration(cardinality, bloom_options)

    counters_bit_size = options |> Keyword.get(:counters_bit_size, 8)
    signed = options |> Keyword.get(:signed, true)

    Validation.one_of!(counters_bit_size, @counter_bit_sizes, :counters_bit_size)
    Validation.boolean!(signed, :signed)

    counter =
      Abit.Counter.new(
        filter_length,
        counters_bit_size,
        signed: signed
      )

    %CBF{
      filter_length: filter_length,
      hash_functions: hash_functions,
      counter: counter
    }
  end

  @doc """
  Puts `term` into the filter by incrementing its counters.

  After insertion, `member?/2` will return `true` for this `term` unless
  `delete/2` modifies the counters representing its membership.

  Returns `:ok`.

  ## Examples

      iex> cbf = Talan.CountingBloomFilter.new(10_000)
      iex> cbf |> Talan.CountingBloomFilter.put("hat")
      :ok
  """
  @spec put(t, any) :: :ok
  def put(
        %CBF{
          counter: counter,
          filter_length: filter_length,
          hash_functions: hash_functions
        },
        term
      ) do
    update(counter, filter_length, hash_functions, term, 1)
  end

  @doc """
  Probabilistically deletes `term` from `bloom_filter` and
  decrements counters in `counter`.

  ## Examples

      iex> cbf = Talan.CountingBloomFilter.new(10_000)
      iex> cbf |> Talan.CountingBloomFilter.put("hat")
      iex> cbf |> Talan.CountingBloomFilter.count("hat")
      1
      iex> cbf |> Talan.CountingBloomFilter.delete("hat")
      :ok
      iex> cbf |> Talan.CountingBloomFilter.count("hat")
      0
      iex> cbf |> Talan.CountingBloomFilter.delete("this wasn't there")
      iex> cbf |> Talan.CountingBloomFilter.count("this wasn't there")
      -1
  """
  @spec delete(t, any) :: :ok
  def delete(
        %CBF{
          counter: counter,
          filter_length: filter_length,
          hash_functions: hash_functions
        },
        term
      ) do
    update(counter, filter_length, hash_functions, term, -1)
  end

  @doc """
  Clears every counter in `counting_bloom_filter` and returns the same filter.

  The filter is reset in place without reallocating its packed counter storage.
  Clearing individual atomic words is safe during concurrent access, but the
  filter is not cleared as one atomic operation. Concurrent reads may observe a
  partially cleared filter, and concurrent updates may be cleared or remain
  visible depending on their timing.

  ## Examples

      iex> cbf = Talan.CountingBloomFilter.new(1000)
      iex> Talan.CountingBloomFilter.put(cbf, "hat")
      iex> Talan.CountingBloomFilter.clear(cbf) == cbf
      true
      iex> Talan.CountingBloomFilter.count(cbf, "hat")
      0
  """
  @spec clear(t()) :: t()
  def clear(%CBF{counter: counter} = counting_bloom_filter) do
    Abit.Counter.clear(counter)
    counting_bloom_filter
  end

  @doc """
  See `Talan.BloomFilter.member?/2` for docs.

  ## Examples

      iex> cbf = Talan.CountingBloomFilter.new(10_000)
      iex> cbf |> Talan.CountingBloomFilter.put("hat")
      iex> cbf |> Talan.CountingBloomFilter.member?("hat")
      true
  """
  @spec member?(t, any) :: boolean
  def member?(
        %CBF{
          counter: counter,
          filter_length: filter_length,
          hash_functions: hash_functions
        },
        term
      ) do
    member_hashes?(counter, filter_length, hash_functions, term)
  end

  @doc """
  Returns the estimated number of times `term` was inserted. The estimate is the
  minimum of the counters selected by the term's hashes. Hash collisions may
  inflate the estimate.

  ## Examples

      iex> cbf = Talan.CountingBloomFilter.new(10_000)
      iex> cbf |> Talan.CountingBloomFilter.put("hat")
      iex> cbf |> Talan.CountingBloomFilter.put("hat")
      iex> cbf |> Talan.CountingBloomFilter.put("hat")
      iex> cbf |> Talan.CountingBloomFilter.count("hat")
      3
  """
  @spec count(t, any) :: integer
  def count(
        %CBF{
          counter: counter,
          filter_length: filter_length,
          hash_functions: hash_functions
        },
        term
      ) do
    min_count(counter, filter_length, hash_functions, term)
  end

  @doc """
  See `Talan.BloomFilter.cardinality/1` for docs.

  ## Examples

      iex> cbf = Talan.CountingBloomFilter.new(10_000)
      iex> cbf |> Talan.CountingBloomFilter.put("hat")
      iex> cbf |> Talan.CountingBloomFilter.put("hat")
      iex> cbf |> Talan.CountingBloomFilter.put("hat")
      iex> cbf |> Talan.CountingBloomFilter.put("car keys")
      iex> cbf |> Talan.CountingBloomFilter.cardinality()
      2
  """
  @spec cardinality(t) :: non_neg_integer
  def cardinality(%CBF{
        counter: counter,
        filter_length: filter_length,
        hash_functions: hash_functions
      }) do
    Talan.estimate_cardinality(
      filter_length,
      positive_counter_count(counter),
      length(hash_functions)
    )
  end

  @doc """
  See `Talan.BloomFilter.false_positive_probability/1` for
  docs.
  """
  @spec false_positive_probability(t) :: float
  def false_positive_probability(%CBF{
        counter: counter,
        filter_length: filter_length,
        hash_functions: hash_functions
      }) do
    :math.pow(positive_counter_count(counter) / filter_length, length(hash_functions))
  end

  defp update(counter, filter_length, [hash_fun | hash_functions], term, increment) do
    hash = rem(hash_fun.(term), filter_length)

    update(counter, filter_length, hash_functions, term, increment)
    Abit.Counter.add(counter, hash, increment)
    :ok
  end

  defp update(_counter, _filter_length, [], _term, _increment), do: :ok

  defp member_hashes?(counter, filter_length, [hash_fun | hash_functions], term) do
    hash = rem(hash_fun.(term), filter_length)

    member_hashes?(counter, filter_length, hash_functions, term) and
      Abit.Counter.get(counter, hash) > 0
  end

  defp member_hashes?(_counter, _filter_length, [], _term), do: true

  defp min_count(_counter, _filter_length, [], _term), do: raise(Enum.EmptyError)

  defp min_count(counter, filter_length, [hash_fun], term) do
    Abit.Counter.get(counter, rem(hash_fun.(term), filter_length))
  end

  defp min_count(counter, filter_length, [hash_fun | hash_functions], term) do
    hash = rem(hash_fun.(term), filter_length)
    minimum = min_count(counter, filter_length, hash_functions, term)

    min(Abit.Counter.get(counter, hash), minimum)
  end

  defp positive_counter_count(counter) do
    Enum.count(counter, fn value -> value > 0 end)
  end
end
