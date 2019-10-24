defmodule Probabilistic.Stream do
  alias Probabilistic.BloomFilter

  @doc """
  Returns a probabilistically uniq stream.

  Its main advantage is that it doesn't store elements
  emitted by the stream.
  Instead it uses a bloom filter for membership check.

  The stream never returns duplicate elements but it
  sometimes detects false positive duplicates depending
  on the bloom filter it uses.
  False positives are faulty duplicate detections that
  get rejected.

  ## Examples

      iex> list = ["a", "b", "c", "a", "b"]
      iex> bloom_filter = Probabilistic.BloomFilter.new(100_000, false_positive_probability: 0.001)
      iex> Probabilistic.Stream.uniq(list, bloom_filter) |> Enum.to_list
      ["a", "b", "c"]
  """
  @spec uniq(Enumerable.t(), BloomFilter.t()) :: Enumerable.t()
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
