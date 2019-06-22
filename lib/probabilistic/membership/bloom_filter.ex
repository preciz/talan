defmodule Probabilistic.Membership.BloomFilter do
  import Bitwise

  alias __MODULE__

  @default_false_positive_probability 0.01

  @enforce_keys [
    :atomics_ref,
    :bit_count,
    :hash_functions,
    :call_to_add_count
  ]
  defstruct [
    :atomics_ref,
    :bit_count,
    :hash_functions,
    :call_to_add_count
  ]

  def new(
        capacity,
        false_positive_probability \\ @default_false_positive_probability,
        hash_functions \\ :default
      ) do
    bit_count = required_bit_count(capacity, false_positive_probability)

    new_with_count(bit_count, hash_functions)
  end

  @doc """
  Returns a new BloomFilter struct
  """
  def new_with_count(bit_count, hash_functions \\ :default) do
    arity = bit_count |> atomics_arity

    atomics_ref = :atomics.new(arity, signed: false)

    %BloomFilter{
      atomics_ref: atomics_ref,
      bit_count: arity * 64,
      hash_functions: hash_functions,
      call_to_add_count: 0
    }
  end

  @doc false
  def atomics_arity(bit_count) do
    div(bit_count, 64) + 1
  end

  @doc """
  Returns the required bit count for

  `number_of_elements` - Number of elements that will be inserted
  `false_positive_probability` - Desired false positive probability of membership

  [https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions](https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions)
  """
  def required_bit_count(
        number_of_elements,
        false_positive_probability \\ @default_false_positive_probability
      ) do
    import :math, only: [log: 1, pow: 2]

    (-number_of_elements * log(false_positive_probability) / pow(log(2), 2))
    |> ceil
  end

  def current_false_positive_probability(bloom_filter) do
  end

  @phash2_range 1 <<< 32

  def add(
        filter = %BloomFilter{
          bit_count: bit_count,
          atomics_ref: atomics_ref,
          hash_functions: :default,
          call_to_add_count: call_to_add_count
        },
        elem
      ) do
    murmur = rem(Murmur.hash_x64_128(elem), bit_count)

    phash2 = rem(:erlang.phash2(elem, @phash2_range), bit_count)

    atomics_add(atomics_ref, murmur)
    atomics_add(atomics_ref, phash2)

    %BloomFilter{
      filter
      | call_to_add_count: call_to_add_count + 1
    }
  end

  def atomics_add(atomics_ref, bit_index) do
    idx = bit_index |> atomics_index

    current_value = :atomics.get(atomics_ref, idx)

    next_value = current_value ||| 1 <<< atomics_bit_pos(bit_index)

    :atomics.put(atomics_ref, idx, next_value)
  end

  def atomics_index(bit_index) do
    div(bit_index, 64) + 1
  end

  def atomics_bit_pos(bit_index) do
    rem(bit_index, 64)
  end

  def member?(
        filter = %BloomFilter{
          bit_count: bit_count,
          atomics_ref: atomics_ref,
          hash_functions: :default
        },
        elem
      ) do
    murmur = rem(Murmur.hash_x64_128(elem), bit_count)

    phash2 = rem(:erlang.phash2(elem, @phash2_range), bit_count)

    atomics_member?(atomics_ref, murmur) && atomics_member?(atomics_ref, phash2)
  end

  def atomics_member?(atomics_ref, bit_index) do
    idx = bit_index |> atomics_index

    current_value = :atomics.get(atomics_ref, idx)

    (current_value ||| 1 <<< atomics_bit_pos(bit_index)) == current_value
  end

  def approximate_element_count do
  end

  def set_bits_count do
  end

  def false_positive_probability do
  end

  def merge do
    # merge two bloom filters that have the same hash functions
    # with Bitwise OR?
  end

  def intersection do
    # Bitwise XOR
  end
end
