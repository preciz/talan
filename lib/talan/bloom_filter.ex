defmodule Talan.BloomFilter do
  @moduledoc """
  Bloom filter implementation with **concurrent accessibility**,
  powered by [:atomics](http://erlang.org/doc/man/atomics.html) module.

  "A Bloom filter is a space-efficient probabilistic data structure,
  conceived by Burton Howard Bloom in 1970,
  that is used to test whether an element is a member of a set"

  [Bloom filter on Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter#CITEREFZhiwangJungangJian2010)

  ## Credit

  Partly inspired by [Blex](https://github.com/gyson/blex)

  ## Features

    * Fixed size Bloom filter
    * Concurrent reads & writes
    * Custom & default hash functions
    * Merge multiple Bloom filters into one
    * Intersect multiple Bloom filters into one
    * Estimate number of unique elements
    * Estimate current false positive probability

  ## Examples

      iex> b = Talan.BloomFilter.new(1000)
      iex> b |> Talan.BloomFilter.put("Barna")
      iex> b |> Talan.BloomFilter.member?("Barna")
      true
      iex> b |> Talan.BloomFilter.member?("Kovacs")
      false
  """

  alias __MODULE__, as: BF

  @enforce_keys [:atomics_ref, :filter_length, :hash_functions]
  defstruct [:atomics_ref, :filter_length, :hash_functions]

  @type t :: %__MODULE__{
          atomics_ref: reference,
          filter_length: non_neg_integer,
          hash_functions: list
        }

  @doc """
  Returns a new `%Talan.BloomFilter{}` for the desired `cardinality`.

  `cardinality` is the expected number of unique items. Duplicated items
  can be infinite.

  ## Options
    * `:false_positive_probability` - a float, defaults to 0.01
    * `:hash_functions` - a list of hash functions, defaults to randomly seeded murmur

  ## Examples

      iex> bloom_filter = Talan.BloomFilter.new(1_000_000)
      iex> bloom_filter |> Talan.BloomFilter.put("Barna Kovacs")
      :ok
  """
  @spec new(pos_integer, list) :: t
  def new(cardinality, options \\ []) when is_integer(cardinality) and cardinality > 0 do
    false_positive_probability = options |> Keyword.get(:false_positive_probability, 0.01)
    hash_functions = options |> Keyword.get(:hash_functions, [])

    if false_positive_probability <= 0 || false_positive_probability >= 1 do
      raise ArgumentError, """
      false_positive_probability must be a float between 0 and 1.
      E.g. 0.01

      Got: #{inspect(false_positive_probability)}
      """
    end

    hash_functions =
      case hash_functions do
        [] ->
          hash_count = required_hash_function_count(false_positive_probability)

          Talan.seed_n_murmur_hash_fun(hash_count)

        list ->
          list
      end

    filter_length = required_filter_length(cardinality, false_positive_probability)

    atomics_arity = max(div(filter_length, 64), 1)

    atomics_ref = :atomics.new(atomics_arity, signed: false)

    %BF{
      atomics_ref: atomics_ref,
      filter_length: atomics_arity * 64,
      hash_functions: hash_functions
    }
  end

  @doc """
  Returns the count of required hash functions for the
  given `false_positive_probability`.

  [Wikipedia - Bloom filter - Optimal number of hash functions](https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions)

  ## Examples

      iex> Talan.BloomFilter.required_hash_function_count(0.01)
      7
      iex> Talan.BloomFilter.required_hash_function_count(0.001)
      10
      iex> Talan.BloomFilter.required_hash_function_count(0.0001)
      14
  """
  @spec required_hash_function_count(float) :: non_neg_integer
  def required_hash_function_count(false_positive_probability) do
    -:math.log2(false_positive_probability)
    |> Float.ceil()
    |> round()
  end

  @doc """
  Returns the required bit count given

  * `cardinality` - Number of unique elements that will be inserted
  * `false_positive_probability` - Desired false positive probability of membership

  [Wikipedia - Bloom filter - Optimal number of hash functions](https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions)

  ## Examples

      iex> Talan.BloomFilter.required_filter_length(10_000, 0.01)
      95851
  """
  @spec required_filter_length(non_neg_integer, float) :: non_neg_integer
  def required_filter_length(cardinality, false_positive_probability)
      when is_integer(cardinality) and cardinality > 0 and false_positive_probability > 0 and
             false_positive_probability < 1 do
    import :math, only: [log: 1, pow: 2]

    Float.ceil(-cardinality * log(false_positive_probability) / pow(log(2), 2))
    |> round()
  end

  @doc """
  Puts `term` into `bloom_filter` a `%Talan.BloomFilter{}` struct.

  After this the `member?` function will always return `true`
  for the membership of `term`.

  Returns `:ok`.

  ## Examples

      iex> b = Talan.BloomFilter.new(1000)
      iex> b |> Talan.BloomFilter.put("Chris McCord")
      :ok
      iex> b |> Talan.BloomFilter.put("Jose Valim")
      :ok
  """
  @spec put(t, any) :: :ok
  def put(%BF{} = bloom_filter, term) do
    hashes = hash_term(bloom_filter, term)

    put_hashes(bloom_filter, hashes)

    :ok
  end

  @doc false
  def put_hashes(%BF{atomics_ref: atomics_ref}, hashes) when is_list(hashes) do
    hashes
    |> Enum.each(fn hash ->
      Abit.set_bit_at(atomics_ref, hash, 1)
    end)
  end

  @doc """
  Checks for membership of `term` in `bloom_filter`.

  Returns `false` if not a member. (definitely not member)
  Returns `true` if maybe a member. (possibly member)

  ## Examples

      iex> b = Talan.BloomFilter.new(1000)
      iex> b |> Talan.BloomFilter.member?("Barna Kovacs")
      false
      iex> b |> Talan.BloomFilter.put("Barna Kovacs")
      iex> b |> Talan.BloomFilter.member?("Barna Kovacs")
      true
  """
  @spec member?(t, any) :: boolean
  def member?(%BF{atomics_ref: atomics_ref} = bloom_filter, term) do
    hashes = hash_term(bloom_filter, term)

    do_member?(atomics_ref, hashes)
  end

  defp do_member?(atomics_ref, [hash | hashes_tl]) do
    if Abit.bit_at(atomics_ref, hash) == 1 do
      do_member?(atomics_ref, hashes_tl)
    else
      false
    end
  end

  defp do_member?(_, []), do: true

  @doc """
  Hashes `term` with all `hash_functions` of `%Talan.BloomFilter{}`.

  Returns a list of hashed values.

  ## Examples

      b = Talan.BloomFilter.new(1000)
      Talan.BloomFilter.hash_term(b, :any_term_can_be_hashed)
      [9386, 8954, 8645, 4068, 5445, 6914, 2844]
  """
  @spec hash_term(t, any) :: list(integer)
  def hash_term(%BF{filter_length: filter_length, hash_functions: hash_functions}, term) do
    do_hash_term(filter_length, hash_functions, term)
  end

  defp do_hash_term(filter_length, hash_functions, term, acc \\ [])

  defp do_hash_term(filter_length, [hash_fun | tl], term, acc) do
    new_acc = [rem(hash_fun.(term), filter_length) | acc]

    do_hash_term(filter_length, tl, term, new_acc)
  end

  defp do_hash_term(_, [], _, acc), do: acc

  @doc """
  Merge multiple `%Talan.BloomFilter{}` structs's atomics into one new struct.

  Note: To work correctly filters with identical size & hash functions must be used.

  Returns a new `%Talan.BloomFilter{}` struct which set bits are the merged set bits of
  the bloom filters in the `list`.

  ## Examples

      iex> hash_functions = Talan.seed_n_murmur_hash_fun(7)
      iex> b1 = Talan.BloomFilter.new(1000, hash_functions: hash_functions)
      iex> b1 |> Talan.BloomFilter.put("GitHub")
      iex> b2 = Talan.BloomFilter.new(1000, hash_functions: hash_functions)
      iex> b2 |> Talan.BloomFilter.put("Octocat")
      :ok
      iex> b3 = Talan.BloomFilter.merge([b1, b2])
      iex> b3 |> Talan.BloomFilter.member?("GitHub")
      true
      iex> b3 |> Talan.BloomFilter.member?("Octocat")
      true
  """
  @spec merge(nonempty_list(t)) :: t
  def merge(list = [first = %BF{atomics_ref: first_atomics_ref} | _tl]) do
    %{size: size} = :atomics.info(first_atomics_ref)

    new_atomics_ref = :atomics.new(size, signed: false)

    list
    |> Enum.reduce(
      new_atomics_ref,
      fn %BF{atomics_ref: atomics_ref}, acc ->
        Abit.merge(acc, atomics_ref)
      end
    )

    %BF{first | atomics_ref: new_atomics_ref}
  end

  @doc """
  Intersection of `%Talan.BloomFilter{}` structs's atomics into one new struct.

  Note: To work correctly filters with identical size & hash functions must be used.

  Returns a new `%BloomFilter{}` struct which set bits are the intersection
  the bloom filters in the `list`.

  ## Examples

      iex> hash_functions = Talan.seed_n_murmur_hash_fun(7)
      iex> b1 = Talan.BloomFilter.new(1000, hash_functions: hash_functions)
      iex> b1 |> Talan.BloomFilter.put("GitHub")
      iex> b2 = Talan.BloomFilter.new(1000, hash_functions: hash_functions)
      iex> b2 |> Talan.BloomFilter.put("GitHub")
      iex> b2 |> Talan.BloomFilter.put("Octocat")
      :ok
      iex> b3 = Talan.BloomFilter.intersection([b1, b2])
      iex> b3 |> Talan.BloomFilter.member?("GitHub")
      true
      iex> b3 |> Talan.BloomFilter.member?("Octocat")
      false
  """
  @spec intersection(nonempty_list(t)) :: t
  def intersection(list = [first = %BF{atomics_ref: first_atomics_ref} | _tl]) do
    %{size: size} = :atomics.info(first_atomics_ref)

    new_atomics_ref = :atomics.new(size, signed: false)

    Abit.merge(new_atomics_ref, first_atomics_ref)

    list
    |> Enum.reduce(
      new_atomics_ref,
      fn %BF{atomics_ref: atomics_ref}, acc ->
        Abit.intersect(acc, atomics_ref)
      end
    )

    %BF{first | atomics_ref: new_atomics_ref}
  end

  @doc """
  Returns a non negative integer representing the
  estimated cardinality count of unique elements in the filter.

  ## Examples

      iex> b = Talan.BloomFilter.new(1000)
      iex> b |> Talan.BloomFilter.cardinality()
      0
      iex> b |> Talan.BloomFilter.put("Barna")
      iex> b |> Talan.BloomFilter.cardinality()
      1
      iex> b |> Talan.BloomFilter.put("Barna")
      iex> b |> Talan.BloomFilter.cardinality()
      1
      iex> b |> Talan.BloomFilter.put("Kovacs")
      iex> b |> Talan.BloomFilter.cardinality()
      2
  """
  @spec cardinality(t) :: non_neg_integer
  def cardinality(%BF{
        atomics_ref: atomics_ref,
        filter_length: filter_length,
        hash_functions: hash_functions
      }) do
    set_bits_count = Abit.set_bits_count(atomics_ref)
    hash_function_count = length(hash_functions)

    cond do
      set_bits_count == 0 ->
        0

      set_bits_count <= hash_function_count ->
        1

      filter_length == set_bits_count ->
        round(filter_length / hash_function_count)

      true ->
        est = :math.log(filter_length - set_bits_count) - :math.log(filter_length)

        round(filter_length * -est / hash_function_count)
    end
  end

  @doc """
  Returns a float representing current estimated
  false positive probability.

  ## Examples

      iex> b = Talan.BloomFilter.new(1000)
      iex> b |> Talan.BloomFilter.false_positive_probability()
      0.0 # fpp zero when bloom filter is empty
      iex> b |> Talan.BloomFilter.put("Barna") # fpp increases
      iex> b |> Talan.BloomFilter.put("Kovacs")
      iex> fpp = b |> Talan.BloomFilter.false_positive_probability()
      iex> fpp > 0 && fpp < 1
      true
  """
  @spec false_positive_probability(t()) :: float()
  def false_positive_probability(%BF{
        atomics_ref: atomics_ref,
        filter_length: filter_length,
        hash_functions: hash_functions
      }) do
    bits_not_set_count = filter_length - Abit.set_bits_count(atomics_ref)

    hash_function_count = length(hash_functions)

    :math.pow(1 - bits_not_set_count / filter_length, hash_function_count)
  end

  @doc """
  Returns a map representing the bit state of the `atomics_ref`.

  Use this for debugging purposes.

  ## Examples

      iex> b = Talan.BloomFilter.new(1000)
      iex> b |> Talan.BloomFilter.bits_info()
      %{total_bits: 9536, set_bits_count: 0, set_ratio: 0.0}
  """
  @spec bits_info(t()) :: map()
  def bits_info(%BF{atomics_ref: atomics_ref, filter_length: filter_length}) do
    set_bits_count = Abit.set_bits_count(atomics_ref)

    %{
      total_bits: filter_length,
      set_bits_count: set_bits_count,
      set_ratio: set_bits_count / filter_length
    }
  end

  @doc """
  Serializes the Bloom filter into a binary.

  This function converts the Bloom filter structure into a binary format,
  which can be used for storage or transmission.

  ## Examples

      iex> bloom_filter = Talan.BloomFilter.new(1000)
      iex> serialized = Talan.BloomFilter.serialize(bloom_filter)
      iex> is_binary(serialized)
      true

  """
  @doc since: "0.1.3"
  @spec serialize(t()) :: binary
  def serialize(%BF{
        atomics_ref: atomics_ref,
        filter_length: filter_length,
        hash_functions: hash_functions
      }) do
    %{
      atomics_ref: Abit.Atomics.serialize(atomics_ref),
      filter_length: filter_length,
      hash_functions: hash_functions
    }
    |> :erlang.term_to_binary()
  end

  @doc """
  Deserializes a binary into a Bloom filter.

  This function takes a binary that was previously created by `serialize/1`
  and reconstructs the Bloom filter structure.

  ## Examples

      iex> bloom_filter = Talan.BloomFilter.new(1000)
      iex> serialized = Talan.BloomFilter.serialize(bloom_filter)
      iex> deserialized = Talan.BloomFilter.deserialize(serialized)
      iex> is_struct(deserialized, Talan.BloomFilter)
      true

  """
  @doc since: "0.1.3"
  @spec deserialize(binary) :: t()
  def deserialize(binary) when is_binary(binary) do
    map =
      binary
      |> :erlang.binary_to_term()
      |> Map.update!(:atomics_ref, &Abit.Atomics.deserialize(&1))

    struct!(__MODULE__, map)
  end
end
