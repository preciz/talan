defmodule Talan.Counter do
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
          atomics_ref: reference,
          filter_length: non_neg_integer,
          hash_function: function
        }

  alias Talan.Counter

  @doc """
  Returns a new `%Talan.Counter{}` struct.

  `expected_cardinality` is the maximum number of unique items the counter will
  handle with an approximately 1% error rate.

  ## Options
    * `:hash_function` - a function that accepts a term and returns a non-negative
      integer, defaults to `Murmur.hash_x64_128/1`

  ## Examples

      iex> c = Talan.Counter.new(10_000)
      iex> c |> Talan.Counter.put(["you", :can, Hash, {"any", "elixir", "term"}])
      iex> c |> Talan.Counter.put("more")
      iex> c |> Talan.Counter.put("another")
      iex> c |> Talan.Counter.cardinality()
      3
  """
  @spec new(non_neg_integer, list) :: t
  def new(expected_cardinality, options \\ []) do
    hash_function = options |> Keyword.get(:hash_function, &Murmur.hash_x64_128/1)

    # Allocate ten bits per expected element, rounded up to a complete atomic word.
    required_size = max(1, div(expected_cardinality * 10 + 63, 64))

    %Counter{
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

      iex> c = Talan.Counter.new(10_000)
      iex> c |> Talan.Counter.put(["you", :can, Hash, {"any", "elixir", "term"}])
      :ok
  """
  @spec put(t, any) :: :ok
  def put(%Counter{} = counter, term) do
    hash = rem(counter.hash_function.(term), counter.filter_length)

    Abit.set_bit_at(counter.atomics_ref, hash, 1)

    :ok
  end

  @doc """
  Returns the estimated cardinality for the given
  `%Talan.Counter{}` struct.

  ## Examples

      iex> c = Talan.Counter.new(10_000)
      iex> c |> Talan.Counter.put(["you", :can, Hash, {"any", "elixir", "term"}])
      iex> c |> Talan.Counter.put(["you", :can, Hash, {"any", "elixir", "term"}])
      iex> c |> Talan.Counter.cardinality()
      1
      iex> c |> Talan.Counter.put("more")
      iex> c |> Talan.Counter.cardinality()
      2
  """
  @spec cardinality(t) :: non_neg_integer
  def cardinality(%Counter{atomics_ref: atomics_ref}) do
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
