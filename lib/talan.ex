defmodule Talan do
  @moduledoc """
  Fast & concurrent probabilistic data structures
  built on top of :atomics with **concurrent accessibility**.

  `Talan.BloomFilter` - bloom filter based on `:atomics`
  `Talan.CountingBloomFilter` - counting bloom filter based on `:atomics`
  `Talan.Counter` - linear probabilistic counter based on `:atomics`
  """

  @doc false
  def seed_n_murmur_hash_fun(hash_count) do
    range = 1..(hash_count * 50)

    Enum.take_random(range, hash_count)
    |> Enum.map(&seed_murmur_hash_fun/1)
  end

  @doc false
  def seed_murmur_hash_fun(n) do
    fn term -> Murmur.hash_x64_128(term, n) end
  end

  @doc false
  def to_bitstring(<<>>) do
    []
  end

  def to_bitstring(<<bit::1, rest::bitstring>>)do
    [bit | to_bitstring(rest)]
  end
end
