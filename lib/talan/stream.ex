defmodule Talan.Stream do
  alias Talan.BloomFilter

  @doc """
  Returns a stream that probabilistically removes duplicates.

  Its main advantage is that it doesn't store elements
  emitted by the stream.
  Instead it uses a Bloom filter for membership checks.

  The stream never returns duplicate elements but it
  sometimes detects false-positive duplicates depending
  on the Bloom filter it uses.
  A false positive causes a unique element to be incorrectly
  rejected as a duplicate.

  ## Examples

      iex> list = ["a", "b", "c", "a", "b"]
      iex> bloom_filter = Talan.BloomFilter.new(100_000, false_positive_probability: 0.001)
      iex> Talan.Stream.uniq(list, bloom_filter) |> Enum.to_list()
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
