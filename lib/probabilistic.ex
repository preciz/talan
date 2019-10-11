defmodule Probabilistic do
  @moduledoc """
  Fast & concurrent probabilistic data structures
  built on top of :atomics.
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
end
