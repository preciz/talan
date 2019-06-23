defmodule Probabilistic.Membership.BloomFilter do
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
      iex> BloomFilter.put("Barna")
      iex> BloomFilter.member?(b, "Barna")
      true
      iex> BloomFilter.member?(b, "Kovacs")
      false
  """

  import Bitwise

  alias __MODULE__

  @enforce_keys [
    :atomics_ref,
    :filter_length,
    :hash_functions,
    :put_counter
  ]
  defstruct [
    :atomics_ref,
    :filter_length,
    :hash_functions,
    :put_counter
  ]

  @doc """
  Returns a new %BloomFilter{} with default false_positive_probability 0.01
  and hash_functions murmur3 & :erlang.phash2.
  """
  def new(capacity, false_positive_probability \\ 0.01, hash_functions \\ []) when is_list(hash_functions) do
    hash_functions =
      case hash_functions do
        [] ->
          phash2_range = 1 <<< 32

          [
            &Murmur.hash_x64_128/1,
            fn elem -> :erlang.phash2(elem, phash2_range) end
          ]

        list -> list
    end

    filter_length = required_filter_length(capacity, false_positive_probability)

    new_with_count(filter_length, hash_functions)
  end

  @doc """
  Returns a new BloomFilter struct
  """
  def new_with_count(filter_length, hash_functions) do
    arity = div(filter_length, 64) + 1

    atomics_ref = :atomics.new(arity, signed: false)

    %BloomFilter{
      atomics_ref: atomics_ref,
      filter_length: arity * 64,
      hash_functions: hash_functions,
      put_counter: 0
    }
  end

  @doc """
  Returns the required bit count given
  `capacity` - Number of elements that will be inserted
  `false_positive_probability` - Desired false positive probability of membership

  [https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions](https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions)
  """
  def required_filter_length(capacity, false_positive_probability)
      when is_integer(capacity) and capacity > 0 and false_positive_probability > 0 and
             false_positive_probability < 1 do
    import :math, only: [log: 1, pow: 2]

    (-capacity * log(false_positive_probability) / pow(log(2), 2))
    |> ceil
  end

  @doc """
  Puts elem into BloomFilter.

  Returns BloomFilter.
  """
  def put(
        filter = %BloomFilter{
          filter_length: filter_length,
          atomics_ref: atomics_ref,
          hash_functions: hash_functions,
          put_counter: put_counter
        },
        elem
      ) do
    hash_functions
    |> Enum.each(fn hash_fun ->
      hash = rem(hash_fun.(elem), filter_length)

      Probabilistic.Atomics.put_bit(atomics_ref, hash)
    end)

    %BloomFilter{filter | put_counter: put_counter + 1}
  end

  @doc """
  Checks for membership.

  Returns `false` if not a member. (definitely not in set)
  Returns `true` if maybe a member. (possibly in set)
  """
  def member?(
        %BloomFilter{
          atomics_ref: atomics_ref,
          filter_length: filter_length,
          hash_functions: hash_functions
        },
        elem
      ) do
    member?(atomics_ref, filter_length, hash_functions, elem)
  end

  def member?(atomics_ref, filter_length, hash_functions, elem) do
    hash_functions
    |> Enum.reduce_while(
      true,
      fn hash_fun, acc ->
        if acc do
          hash = rem(hash_fun.(elem), filter_length)

          {:cont, Probabilistic.Atomics.bit_at(atomics_ref, hash) == 1}
        else
          {:halt, acc}
        end
      end
    )
  end

  def approximate_element_count do
  end

  def current_false_positive_probability do
  end

  @doc """
  Merge multiple BloomFilter structs atomics into one new struct.
  """
  def merge([]), do: []

  def merge(list = [first = %BloomFilter{atomics_ref: first_atomics_ref} | _tl]) do
    new_atomics_ref = Probabilistic.Atomics.new_like(first_atomics_ref)

    list
    |> Enum.reduce(
      new_atomics_ref,
      fn %BloomFilter{atomics_ref: atomics_ref}, acc ->
        Probabilistic.Atomics.merge_bitwise(acc, atomics_ref)
      end
    )

    %BloomFilter{
      first
      | atomics_ref: new_atomics_ref,
        put_counter: list |> Enum.reduce(0, &(&1.put_counter + &2))
    }
  end

  @doc """
  Intersection of BloomFilter structs atomics into one new struct.
  """
  def intersection([]), do: []

  def intersection(list = [first = %BloomFilter{atomics_ref: first_atomics_ref} | _tl]) do
    new_atomics_ref = Probabilistic.Atomics.new_like(first_atomics_ref)

    Probabilistic.Atomics.merge_bitwise(new_atomics_ref, first_atomics_ref)

    list
    |> Enum.reduce(
      new_atomics_ref,
      fn %BloomFilter{atomics_ref: atomics_ref}, acc ->
        Probabilistic.Atomics.intersect_bitwise(acc, atomics_ref)
      end
    )

    # put_counter_is_inherited from first `%BloomFilter{}` struct
    %BloomFilter{first | atomics_ref: new_atomics_ref}
  end

  @doc """
  Estimates count of unique elemenets in the filter.
  """
  def estimate_element_count(%BloomFilter{
        atomics_ref: atomics_ref,
        filter_length: filter_length,
        hash_functions: hash_functions
      }) do
    set_bits_count = Probabilistic.Atomics.set_bits_count(atomics_ref)

    # prevent :math.log(0) error
    fill_ratio =
      case set_bits_count / filter_length do
        f when f < 1 -> f
        _ -> 0.99
      end

    round(-filter_length / length(hash_functions) * :math.log(1 - fill_ratio))
  end
end
