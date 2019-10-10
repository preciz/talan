defmodule Probabilistic.Counter do
  @moduledoc """
  Linear probabilistic counter to estimate cardinality.

  For more info about linear probabilistic counting:
  [linear probabilistic counting](https://www.waitingforcode.com/big-data-algorithms/cardinality-estimation-linear-probabilistic-counting/read)
  """

  @enforce_keys [:atomics_ref, :filter_length, :hash_function]
  defstruct [:atomics_ref, :filter_length, :hash_function]

  @type t :: %__MODULE__{
          atomics_ref: reference,
          filter_length: non_neg_integer,
          hash_function: function
        }

  alias Probabilistic.Counter

  @doc """
  Returns a new `%Probabilistic.Counter{}` struct.

  `expected_cardinality` is the max number of uniq items the counter will
  handle with approx 1% of error rate.
  """
  @spec new(non_neg_integer, list) :: t
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

  @doc """
  """
  @spec put(t, any) :: :ok
  def put(%Counter{} = counter, term) do
    hash = rem(counter.hash_function.(term), counter.filter_length)

    Abit.set_bit(counter.atomics_ref, hash, 1)
  end

  @doc """
  Returns the estimated cardinality (Estimated uniq element count)
  for the given `%Probabilistic.Counter{}` struct.
  """
  @spec cardinality(t) :: non_neg_integer
  def cardinality(%Counter{atomics_ref: atomics_ref}) do
    bit_count = Abit.bit_count(atomics_ref)
    set_bit_count = Abit.set_bits_count(atomics_ref)
    unset_bit_count = bit_count - set_bit_count

    round(-bit_count * :math.log(unset_bit_count / bit_count))
  end
end
