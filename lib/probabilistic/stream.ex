defmodule Probabilistic.Stream do
  alias Probabilistic.BloomFilter

  @doc """
  Returns a probabilisticly uniq stream.

  Its main advantage is that it doesn't store elements
  emitted by the stream.
  Instead it uses a bloom filter for membership check.

  The stream never returns duplicate elements but it
  sometimes detects false positive duplicates depending on the
  bloom filter it uses.
  """
  def uniq(enum) do
    bloom_filter = BloomFilter.new(1_000_000)

    uniq(enum, bloom_filter)
  end

  def uniq(enum, bloom_filter) do
    enum
    |> Stream.reject(fn x ->
      is_member = bloom_filter |> BloomFilter.member?(x)

      if not is_member do
        bloom_filter |> BloomFilter.put(x)
      end

      is_member
    end)
  end
end
