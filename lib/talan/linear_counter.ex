defmodule Talan.LinearCounter do
  @moduledoc """
  Linear probabilistic counter implementation with **safe concurrent access**,
  powered by the [:atomics](http://erlang.org/doc/man/atomics.html) module for cardinality estimation.

  Cardinality is the count of unique elements.

  For more information about linear probabilistic counting:
  [linear probabilistic counting](https://www.waitingforcode.com/big-data-algorithms/cardinality-estimation-linear-probabilistic-counting/read)
  """

  @enforce_keys [:atomics_ref, :filter_length, :hash_function]
  defstruct [:atomics_ref, :filter_length, :hash_function]

  @type t :: %__MODULE__{
          atomics_ref: reference(),
          filter_length: pos_integer(),
          hash_function: Talan.hash_function()
        }
  @type option :: {:hash_function, Talan.hash_function()}
  @type options :: list(option())

  alias Talan.LinearCounter
  alias Talan.Validation

  @options [:hash_function]

  @doc """
  Returns a new `%Talan.LinearCounter{}` struct.

  `expected_cardinality` is the maximum number of unique items the counter will
  handle with an approximately 1% error rate.

  Raises `ArgumentError` if `expected_cardinality` or any option is invalid.

  ## Options
    * `:hash_function` - a function that accepts a term and returns a non-negative
      integer, defaults to `Murmur.hash_x64_128/1`

  ## Examples

      iex> c = Talan.LinearCounter.new(10_000)
      iex> c |> Talan.LinearCounter.put(["you", :can, Hash, {"any", "elixir", "term"}])
      iex> c |> Talan.LinearCounter.put("more")
      iex> c |> Talan.LinearCounter.put("another")
      iex> c |> Talan.LinearCounter.cardinality()
      3
  """
  @spec new(pos_integer()) :: t()
  @spec new(pos_integer(), options()) :: t()
  def new(expected_cardinality, options \\ []) do
    Validation.positive_integer!(expected_cardinality, :expected_cardinality)
    Validation.options!(options, @options)

    hash_function = options |> Keyword.get(:hash_function, &Murmur.hash_x64_128/1)
    Validation.function!(hash_function, :hash_function)

    # Allocate ten bits per expected element, rounded up to a complete atomic word.
    required_size = div(expected_cardinality * 10 + 63, 64)

    %LinearCounter{
      atomics_ref: :atomics.new(required_size, signed: false),
      filter_length: required_size * 64,
      hash_function: hash_function
    }
  end

  @doc """
  Hashes `term` and sets a bit to record that the term has been seen.

  Doesn't store the `term` so it's space-efficient.
  Uses `:atomics` so it's mutable and highly concurrent.

  Returns `:ok`.

  ## Examples

      iex> c = Talan.LinearCounter.new(10_000)
      iex> c |> Talan.LinearCounter.put(["you", :can, Hash, {"any", "elixir", "term"}])
      :ok
  """
  @spec put(t, any) :: :ok
  def put(
        %LinearCounter{
          atomics_ref: atomics_ref,
          filter_length: filter_length,
          hash_function: hash_function
        },
        term
      ) do
    hash = rem(hash_function.(term), filter_length)

    Abit.set_bit_at(atomics_ref, hash, 1)

    :ok
  end

  @doc """
  Clears every bit in `counter` and returns the same counter.

  The counter is reset in place without reallocating its atomics reference.
  Clearing individual atomic words is safe during concurrent access, but the
  counter is not cleared as one atomic operation. Concurrent cardinality reads
  may observe a partially cleared counter, and concurrent writes may be cleared
  or remain set depending on their timing.

  ## Examples

      iex> counter = Talan.LinearCounter.new(1000)
      iex> Talan.LinearCounter.put(counter, "Barna")
      iex> Talan.LinearCounter.clear(counter) == counter
      true
      iex> Talan.LinearCounter.cardinality(counter)
      0
  """
  @spec clear(t()) :: t()
  def clear(%LinearCounter{atomics_ref: atomics_ref} = counter) do
    Abit.clear(atomics_ref)
    counter
  end

  @doc """
  Returns the estimated cardinality for the given
  `%Talan.LinearCounter{}` struct.

  ## Examples

      iex> c = Talan.LinearCounter.new(10_000)
      iex> c |> Talan.LinearCounter.put(["you", :can, Hash, {"any", "elixir", "term"}])
      iex> c |> Talan.LinearCounter.put(["you", :can, Hash, {"any", "elixir", "term"}])
      iex> c |> Talan.LinearCounter.cardinality()
      1
      iex> c |> Talan.LinearCounter.put("more")
      iex> c |> Talan.LinearCounter.cardinality()
      2
  """
  @spec cardinality(t) :: non_neg_integer
  def cardinality(%LinearCounter{atomics_ref: atomics_ref}) do
    bit_count = Abit.bit_count(atomics_ref)
    set_bit_count = Abit.set_bits_count(atomics_ref)
    unset_bit_count = bit_count - set_bit_count

    if unset_bit_count == 0 do
      bit_count
    else
      round(-bit_count * :math.log(unset_bit_count / bit_count))
    end
  end
end
