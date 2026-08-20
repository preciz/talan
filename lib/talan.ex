defmodule Talan do
  @moduledoc """
  Fast, concurrent probabilistic data structures built on Erlang's `:atomics` module.

    * `Talan.BloomFilter` - bloom filter based on `:atomics`
    * `Talan.CountingBloomFilter` - counting bloom filter based on `:atomics`
    * `Talan.Counter` - linear probabilistic counter based on `:atomics`
  """

  @type hash_function :: (term() -> non_neg_integer())

  @doc false
  @spec seed_n_murmur_hash_fun(0) :: []
  @spec seed_n_murmur_hash_fun(pos_integer()) :: nonempty_list(hash_function())
  def seed_n_murmur_hash_fun(hash_count) do
    range = 1..(hash_count * 50)

    Enum.take_random(range, hash_count)
    |> Enum.map(&seed_murmur_hash_fun/1)
  end

  @doc false
  @spec seed_murmur_hash_fun(non_neg_integer()) :: hash_function()
  def seed_murmur_hash_fun(n) do
    fn term -> Murmur.hash_x64_128(term, n) end
  end

  @doc false
  @spec estimate_cardinality(pos_integer(), non_neg_integer(), pos_integer()) ::
          non_neg_integer()
  def estimate_cardinality(capacity, occupied, hash_count) do
    cond do
      occupied == 0 ->
        0

      occupied <= hash_count ->
        1

      occupied == capacity ->
        round(capacity / hash_count)

      true ->
        estimate = :math.log(capacity - occupied) - :math.log(capacity)
        round(capacity * -estimate / hash_count)
    end
  end

  @doc false
  @spec to_bitstring(bitstring()) :: list(0 | 1)
  def to_bitstring(<<>>) do
    []
  end

  def to_bitstring(<<bit::1, rest::bitstring>>) do
    [bit | to_bitstring(rest)]
  end
end
