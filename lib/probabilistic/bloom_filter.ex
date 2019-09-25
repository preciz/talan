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

  ## Example
      iex> b = BloomFilter.new(1000, 0.01)
      iex> b |> BloomFilter.put("Barna")
      iex> b |> BloomFilter.member?("Barna")
      true
      iex> b |> BloomFilter.member?("Kovacs")
      false
  """

  alias __MODULE__, as: BF

  @enforce_keys [:atomics_ref, :filter_length, :hash_functions]
  defstruct [:atomics_ref, :filter_length, :hash_functions]

  @doc """
  Returns a new `%Probabilistic.BloomFilter{}` with default false_positive_probability 0.01
  and hash_functions murmur3 & :erlang.phash2.
  """
  def new(capacity, false_positive_probability \\ 0.01, hash_functions \\ [])
      when is_integer(capacity) and capacity >= 1 and false_positive_probability > 0 and
             false_positive_probability < 1 and is_list(hash_functions) do
    hash_functions =
      case hash_functions do
        [] ->
          hash_count = required_hash_function_count(false_positive_probability)

          random_numbers = Enum.take_random(1..(hash_count * 2), hash_count)

          for r <- random_numbers, do: seed_murmur_hash_fun(r)

        list ->
          list
      end

    filter_length = required_filter_length(capacity, false_positive_probability)

    arity = div(filter_length, 64) + 1

    atomics_ref = :atomics.new(arity, signed: false)

    %BF{
      atomics_ref: atomics_ref,
      filter_length: arity * 64,
      hash_functions: hash_functions
    }
  end

  @doc """
  Returns count of required hash functions for `filter_length` and `false_positive_probability`

  [Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions)
  """
  def required_hash_function_count(false_positive_probability) do
    -:math.log2(false_positive_probability) |> ceil()
  end

  @doc """
  Returns the required bit count given
  `capacity` - Number of elements that will be inserted
  `false_positive_probability` - Desired false positive probability of membership

  [Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions)
  """
  def required_filter_length(capacity, false_positive_probability)
      when is_integer(capacity) and capacity > 0 and false_positive_probability > 0 and
             false_positive_probability < 1 do
    import :math, only: [log: 1, pow: 2]

    ceil(-capacity * log(false_positive_probability) / pow(log(2), 2))
  end

  @doc false
  def seed_murmur_hash_fun(n) do
    fn elem -> Murmur.hash_x64_128(elem, n) end
  end

  @doc """
  Puts `term` into `bloom_filter` a `%Probabilistic.BloomFilter{}` struct.

  After this the `member?` function will always return `true`
  for the membership of `term`.

  Returns `bloom_filter`.
  """
  def put(%BF{atomics_ref: atomics_ref} = bloom_filter, term) do
    hash_term(bloom_filter, term)
    |> Enum.each(fn hash ->
      Probabilistic.Atomics.put_bit(atomics_ref, hash)
    end)

    bloom_filter
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
    if Probabilistic.Atomics.bit_at(atomics_ref, hash) == 1 do
      true
    else
      do_member?(atomics_ref, hashes_tl)
    end
  end

  defp do_member?(_, []), do: false

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

  Note: To work correctly filters with same size & hash functions must be used.

  Returns a new `%Probabilistic.BloomFilter{}` struct which set bits are the merged set bits of
  the bloom filters in the `list`.
  """
  def merge([]), do: []

  def merge(list = [first = %BF{atomics_ref: first_atomics_ref} | _tl]) do
    new_atomics_ref = Probabilistic.Atomics.new_like(first_atomics_ref)

    list
    |> Enum.reduce(
      new_atomics_ref,
      fn %BF{atomics_ref: atomics_ref}, acc ->
        Probabilistic.Atomics.merge_bitwise(acc, atomics_ref)
      end
    )

    %BF{first | atomics_ref: new_atomics_ref}
  end

  @doc """
  Intersection of BloomFilter structs atomics into one new struct.

  Note: To work correctly filters with same size & hash functions must be used.

  Returns a new `%BloomFilter{}` struct which set bits are the intersection
  the bloom filters in the `list`.
  """
  def intersection([]), do: []

  def intersection(list = [first = %BF{atomics_ref: first_atomics_ref} | _tl]) do
    new_atomics_ref = Probabilistic.Atomics.new_like(first_atomics_ref)

    Probabilistic.Atomics.merge_bitwise(new_atomics_ref, first_atomics_ref)

    list
    |> Enum.reduce(
      new_atomics_ref,
      fn %BF{atomics_ref: atomics_ref}, acc ->
        Probabilistic.Atomics.intersect_bitwise(acc, atomics_ref)
      end
    )

    %BF{first | atomics_ref: new_atomics_ref}
  end

  @doc """
  Estimates count of unique elemenets in the filter.

  Returns an integer >= 0.
  """
  def estimate_element_count(%BF{
        atomics_ref: atomics_ref,
        filter_length: filter_length,
        hash_functions: hash_functions
      }) do
    set_bits_count = Probabilistic.Atomics.set_bits_count(atomics_ref)

    hash_function_count = length(hash_functions)

    estimate_element_count(filter_length, set_bits_count, hash_function_count)
  end

  def estimate_element_count(_, set_bits_count, hash_function_count)
      when set_bits_count < hash_function_count do
    0
  end

  def estimate_element_count(_, set_bits_count, hash_function_count)
      when set_bits_count == hash_function_count do
    1
  end

  def estimate_element_count(filter_length, set_bits_count, hash_function_count)
      when filter_length == set_bits_count do
    round(filter_length / hash_function_count)
  end

  def estimate_element_count(filter_length, set_bits_count, hash_function_count) do
    est = :math.log(filter_length - set_bits_count) - :math.log(filter_length)

    round(filter_length * -est / hash_function_count)
  end

  @doc """
  Returns current estimated false positivity probability.
  """
  def current_false_positive_probability(%BF{
        atomics_ref: atomics_ref,
        filter_length: filter_length,
        hash_functions: hash_functions
      }) do
    bits_not_set_count = filter_length - Probabilistic.Atomics.set_bits_count(atomics_ref)

    hash_function_count = length(hash_functions)

    :math.pow(1 - (bits_not_set_count / filter_length), hash_function_count)
  end

  @doc """
  Returns general info of bits.
  """
  def bits_info(%BF{atomics_ref: atomics_ref, filter_length: filter_length}) do
    set_bits_count = Probabilistic.Atomics.set_bits_count(atomics_ref)

    %{
      total_bits: filter_length,
      set_bits_count: set_bits_count,
      set_ratio: set_bits_count / filter_length
    }
  end
end
