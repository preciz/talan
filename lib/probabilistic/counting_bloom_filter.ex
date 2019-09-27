defmodule Probabilistic.CountingBloomFilter do
  @moduledoc """
  Counting bloom filters support probabilistic deletion of elements.
  """

  alias Probabilistic.BloomFilter, as: BF
  alias Probabilistic.CountingBloomFilter, as: CBF

  @enforce_keys [:bloom_filter, :counter_ref]
  defstruct [:bloom_filter, :counter_ref]

  def new(capacity, options \\ []) do
    bloom_filter = %{atomics_ref: bf_atomics_ref} = BF.new(capacity, options)

    %{size: size} = bf_atomics_ref |> :atomics.info

    {:atomics, counter_ref} = :counters.new(size * 64, [])

    %CBF{
      bloom_filter: bloom_filter,
      counter_ref: counter_ref,
    }
  end
end
