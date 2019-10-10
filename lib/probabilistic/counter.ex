defmodule Probabilistic.Counter do
  @moduledoc """
  """

  @enforce_keys [:atomics_ref, :filter_length, :hash_function]
  defstruct [:atomics_ref, :filter_length, :hash_function]

  alias Probabilistic.Counter

  @spec new(non_neg_integer, list) :: map
  def new(expected_cardinality, options \\ []) do
    hash_function = options |> Keyword.get(:hash_function, &Murmur.hash_x64_128/1)
    # good defaults
    required_size = max(1, floor(expected_cardinality * 10 / 64))

    %Counter{
      atomics_ref: :atomics.new(required_size, signed: false),
      filter_length: required_size * 64,
      hash_function: hash_function
    }
  end

  @spec put(map, any) :: :ok
  def put(%Counter{} = counter, term) do
    hash = rem(counter.hash_function.(term), counter.filter_length)

    Abit.set_bit(counter.atomics_ref, hash, 1)
  end

  @spec count(map) :: non_neg_integer
  def count(%Counter{atomics_ref: atomics_ref}) do
    bit_count = Abit.bit_count(atomics_ref)
    set_bit_count = Abit.set_bits_count(atomics_ref)
    unset_bit_count = bit_count - set_bit_count

    -bit_count * :math.log(unset_bit_count / bit_count)
  end
end

