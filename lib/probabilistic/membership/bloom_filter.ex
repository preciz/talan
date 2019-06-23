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
  * Custom hash functions
  * Merge multiple Bloom filters into one
  * Intersection of multiple Bloom filters
  """

  import Bitwise

  alias __MODULE__

  @enforce_keys [
    :atomics_ref,
    :bit_count,
    :hash_functions,
    :put_counter
  ]
  defstruct [
    :atomics_ref,
    :bit_count,
    :hash_functions,
    :put_counter
  ]

  @doc """
  Returns a new %BloomFilter{} with default false_positive_probability 0.01
  and hash_functions murmur3 & :erlang.phash2.
  """
  def new(capacity) do
    false_positive_probability = 0.01

    phash2_range = 1 <<< 32

    hash_functions = [
      &Murmur.hash_x64_128/1,
      fn elem -> :erlang.phash2(elem, phash2_range) end
    ]

    new(capacity, false_positive_probability, hash_functions)
  end

  def new(capacity, false_positive_probability, hash_functions) when is_list(hash_functions) do
    bit_count = required_bit_count(capacity, false_positive_probability)

    new_with_count(bit_count, hash_functions)
  end

  @doc """
  Returns a new BloomFilter struct
  """
  def new_with_count(bit_count, hash_functions) do
    arity = div(bit_count, 64) + 1

    atomics_ref = :atomics.new(arity, signed: false)

    %BloomFilter{
      atomics_ref: atomics_ref,
      bit_count: arity * 64,
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
  def required_bit_count(capacity, false_positive_probability)
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
          bit_count: bit_count,
          atomics_ref: atomics_ref,
          hash_functions: hash_functions,
          put_counter: put_counter
        },
        elem
      ) do
    hash_functions
    |> Enum.each(fn hash_fun ->
      hash = rem(hash_fun.(elem), bit_count)

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
          bit_count: bit_count,
          hash_functions: hash_functions
        },
        elem
      ) do
    member?(atomics_ref, bit_count, hash_functions, elem)
  end

  def member?(atomics_ref, bit_count, hash_functions, elem) do
    hash_functions
    |> Enum.reduce_while(
      true,
      fn hash_fun, acc ->
        if acc do
          hash = rem(hash_fun.(elem), bit_count)

          {:cont, Probabilistic.Atomics.bit?(atomics_ref, hash)}
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
end
