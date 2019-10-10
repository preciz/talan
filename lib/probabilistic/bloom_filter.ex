defmodule Probabilistic.BloomFilter do
  @moduledoc """
  Bloom filter implementation with **concurrent accessibility**, powered by [`:atomics`](http://erlang.org/doc/man/atomics.html) module.

  "A Bloom filter is a space-efficient probabilistic data structure,
  conceived by Burton Howard Bloom in 1970,
  that is used to test whether an element is a member of a set"

  [Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter#CITEREFZhiwangJungangJian2010)

  ## Credits
  Partly inspired by [Blex](https://github.com/gyson/blex)

  ## Features

  * Fixed size Bloom filter
  * Concurrent reads & writes
  * Custom & default hash functions
  * Merge multiple Bloom filters into one
  * Intersection of multiple Bloom filters
  * Estimate number of unique elements

  ## Examples
      iex> b = BloomFilter.new(1000)
      iex> b |> BloomFilter.put("Barna")
      iex> b |> BloomFilter.member?("Barna")
      true
      iex> b |> BloomFilter.member?("Kovacs")
      false
  """

  alias __MODULE__, as: BF

  @enforce_keys [:atomics_ref, :filter_length, :hash_functions]
  defstruct [:atomics_ref, :filter_length, :hash_functions]

  @type t :: %__MODULE__{
          atomics_ref: reference,
          filter_length: non_neg_integer,
          hash_functions: list
        }

  @doc """
  Returns a new `%Probabilistic.BloomFilter{}` for the desired `cardinality`.

  ## Options
    * `:false_positive_probability` - a float, defaults to 0.01
    * `:hash_functions` - a list of hash functions, defaults to randomly seeded murmur

  ## Examples
      iex> bloom_filter = Probabilistic.BloomFilter.new(1_000_000)
      iex> bloom_filter |> Probabilistic.BloomFilter.put("push the tempo")
      :ok
  """
  @spec new(pos_integer, list) :: t
  def new(cardinality, options \\ []) when is_integer(cardinality) and cardinality > 0 do
    false_positive_probability = options |> Keyword.get(:false_positive_probability, 0.01)
    hash_functions = options |> Keyword.get(:hash_functions, [])

    if false_positive_probability <= 0 || false_positive_probability >= 1 do
      raise ArgumentError, """
      false_positive_probability must be a float between 0 and 1.
      E.g. 0.01

      Got: #{inspect(false_positive_probability)}
      """
    end

    hash_functions =
      case hash_functions do
        [] ->
          hash_count = required_hash_function_count(false_positive_probability)

          Probabilistic.seed_n_murmur_hash_fun(hash_count)

        list ->
          list
      end

    filter_length = required_filter_length(cardinality, false_positive_probability)

    atomics_arity = max(div(filter_length, 64), 1)

    atomics_ref = :atomics.new(atomics_arity, signed: false)

    %BF{
      atomics_ref: atomics_ref,
      filter_length: atomics_arity * 64,
      hash_functions: hash_functions
    }
  end

  @doc """
  Returns count of required hash functions for `filter_length` and `false_positive_probability`

  [Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions)

  ## Example
      iex> Probabilistic.BloomFilter.required_hash_function_count(0.01)
      7
  """
  def required_hash_function_count(false_positive_probability) do
    -:math.log2(false_positive_probability) |> ceil()
  end

  @doc """
  Returns the required bit count given
  `cardinality` - Number of elements that will be inserted
  `false_positive_probability` - Desired false positive probability of membership

  [Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions)
  """
  def required_filter_length(cardinality, false_positive_probability)
      when is_integer(cardinality) and cardinality > 0 and false_positive_probability > 0 and
             false_positive_probability < 1 do
    import :math, only: [log: 1, pow: 2]

    ceil(-cardinality * log(false_positive_probability) / pow(log(2), 2))
  end

  @doc """
  Puts `term` into `bloom_filter` a `%Probabilistic.BloomFilter{}` struct.

  After this the `member?` function will always return `true`
  for the membership of `term`.

  Returns `:ok`.
  """
  def put(%BF{} = bloom_filter, term) do
    hashes = hash_term(bloom_filter, term)

    put_hashes(bloom_filter, hashes)

    :ok
  end

  @doc false
  def put_hashes(%BF{atomics_ref: atomics_ref}, hashes) when is_list(hashes) do
    hashes
    |> Enum.each(fn hash ->
      Abit.set_bit(atomics_ref, hash, 1)
    end)
  end

  @doc """
  Checks for membership of `term` in `bloom_filter`.

  Returns `false` if not a member. (definitely not member)
  Returns `true` if maybe a member. (possibly member)
  """
  def member?(%BF{atomics_ref: atomics_ref} = bloom_filter, term) do
    hashes = hash_term(bloom_filter, term)

    do_member?(atomics_ref, hashes)
  end

  defp do_member?(atomics_ref, [hash | hashes_tl]) do
    if Abit.bit_at(atomics_ref, hash) == 1 do
      do_member?(atomics_ref, hashes_tl)
    else
      false
    end
  end

  defp do_member?(_, []), do: true

  @doc """
  Hashes `term` with all `hash_functions` of `%Probabilistic.BloomFilter{}`.

  Returns a list of hashed values.
  """
  def hash_term(%BF{filter_length: filter_length, hash_functions: hash_functions}, term) do
    do_hash_term(filter_length, hash_functions, term)
  end

  defp do_hash_term(filter_length, hash_functions, term, acc \\ [])

  defp do_hash_term(filter_length, [hash_fun | tl], term, acc) do
    new_acc = [rem(hash_fun.(term), filter_length) | acc]

    do_hash_term(filter_length, tl, term, new_acc)
  end

  defp do_hash_term(_, [], _, acc), do: acc

  @doc """
  Merge multiple `%Probabilistic.BloomFilter{}` structs's atomics into one new struct.

  Note: To work correctly filters with identical size & hash functions must be used.

  Returns a new `%Probabilistic.BloomFilter{}` struct which set bits are the merged set bits of
  the bloom filters in the `list`.
  """
  def merge([]), do: []

  def merge(list = [first = %BF{atomics_ref: first_atomics_ref} | _tl]) do
    %{size: size} = :atomics.info(first_atomics_ref)

    new_atomics_ref = :atomics.new(size, signed: false)

    list
    |> Enum.reduce(
      new_atomics_ref,
      fn %BF{atomics_ref: atomics_ref}, acc ->
        Abit.merge(acc, atomics_ref)
      end
    )

    %BF{first | atomics_ref: new_atomics_ref}
  end

  @doc """
  Intersection of `%Probabilistic.BloomFilter{}` structs's atomics into one new struct.

  Note: To work correctly filters with identical size & hash functions must be used.

  Returns a new `%BloomFilter{}` struct which set bits are the intersection
  the bloom filters in the `list`.
  """
  @spec intersection(nonempty_list(t)) :: t
  def intersection(list = [first = %BF{atomics_ref: first_atomics_ref} | _tl]) do
    %{size: size} = :atomics.info(first_atomics_ref)

    new_atomics_ref = :atomics.new(size, signed: false)

    Abit.merge(new_atomics_ref, first_atomics_ref)

    list
    |> Enum.reduce(
      new_atomics_ref,
      fn %BF{atomics_ref: atomics_ref}, acc ->
        Abit.intersect(acc, atomics_ref)
      end
    )

    %BF{first | atomics_ref: new_atomics_ref}
  end

  @doc """
  Returns an non negative integer representing the
  estimated cardinality count of unique elements in the filter.
  """
  @spec cardinality(t) :: non_neg_integer
  def cardinality(%BF{
        atomics_ref: atomics_ref,
        filter_length: filter_length,
        hash_functions: hash_functions
      }) do
    set_bits_count = Abit.set_bits_count(atomics_ref)

    hash_function_count = length(hash_functions)

    cond do
      set_bits_count < hash_function_count ->
        0

      set_bits_count == hash_function_count ->
        1

      filter_length == set_bits_count ->
        round(filter_length / hash_function_count)

      true ->
        est = :math.log(filter_length - set_bits_count) - :math.log(filter_length)

        round(filter_length * -est / hash_function_count)
    end
  end

  @doc """
  Returns a float representing current estimated
  false positivity probability.
  """
  @spec false_positive_probability(t()) :: float()
  def false_positive_probability(%BF{
        atomics_ref: atomics_ref,
        filter_length: filter_length,
        hash_functions: hash_functions
      }) do
    bits_not_set_count = filter_length - Abit.set_bits_count(atomics_ref)

    hash_function_count = length(hash_functions)

    :math.pow(1 - bits_not_set_count / filter_length, hash_function_count)
  end

  @doc """
  Returns a map representing the bit state of the `atomics_ref`.

  Use this for debugging purposes.
  """
  @spec bits_info(t()) :: map()
  def bits_info(%BF{atomics_ref: atomics_ref, filter_length: filter_length}) do
    set_bits_count = Abit.set_bits_count(atomics_ref)

    %{
      total_bits: filter_length,
      set_bits_count: set_bits_count,
      set_ratio: set_bits_count / filter_length
    }
  end
end
