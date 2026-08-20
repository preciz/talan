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
          filter_length: pos_integer,
          hash_functions: list,
          counter: Abit.Counter.t()
        }

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
  @spec new(pos_integer, keyword) :: t
  def new(cardinality, options \\ []) do
    Validation.positive_integer!(cardinality, :cardinality)
    Validation.options!(options, @options)

    {filter_length, hash_functions} = BF.configuration(cardinality, options)

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
    set_counter_count = Enum.count(counter, fn value -> value > 0 end)
    hash_function_count = length(hash_functions)

    cond do
      set_counter_count == 0 ->
        0

      set_counter_count <= hash_function_count ->
        1

      filter_length == set_counter_count ->
        round(filter_length / hash_function_count)

      true ->
        est =
          :math.log(filter_length - set_counter_count) -
            :math.log(filter_length)

        round(filter_length * -est / hash_function_count)
    end
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
    set_counter_count = Enum.count(counter, fn value -> value > 0 end)
    hash_function_count = length(hash_functions)

    :math.pow(set_counter_count / filter_length, hash_function_count)
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

    if member_hashes?(counter, filter_length, hash_functions, term) do
      Abit.Counter.get(counter, hash) > 0
    else
      false
    end
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
end
