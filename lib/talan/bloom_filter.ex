defmodule Talan.BloomFilter do
  @moduledoc """
  Bloom filter implementation with **safe concurrent access**,
  powered by the [:atomics](http://erlang.org/doc/man/atomics.html) module.

  "A Bloom filter is a space-efficient probabilistic data structure,
  conceived by Burton Howard Bloom in 1970,
  that is used to test whether an element is a member of a set"

  [Bloom filter on Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter#CITEREFZhiwangJungangJian2010)

  ## Credit

  Partly inspired by [Blex](https://github.com/gyson/blex)

  ## Features

    * Fixed-size Bloom filter
    * Concurrent reads and writes
    * Custom and default hash functions
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
  alias Talan.Validation

  @options [:false_positive_probability, :hash_functions]

  @enforce_keys [:atomics_ref, :filter_length, :hash_functions]
  defstruct [:atomics_ref, :filter_length, :hash_functions]

  @type t :: %__MODULE__{
          atomics_ref: reference(),
          filter_length: pos_integer(),
          hash_functions: nonempty_list(Talan.hash_function())
        }

  @type option ::
          {:false_positive_probability, float()}
          | {:hash_functions, list(Talan.hash_function())}
  @type options :: list(option())
  @type bits_info :: %{
          total_bits: pos_integer(),
          set_bits_count: non_neg_integer(),
          set_ratio: float()
        }

  @doc """
  Returns a new `%Talan.BloomFilter{}` for the desired `cardinality`.

  `cardinality` is the expected number of unique items. Duplicate items do not
  count toward the expected cardinality.

  Raises `ArgumentError` if `cardinality` or any option is invalid.

  ## Options
    * `:false_positive_probability` - a float, defaults to 0.01
    * `:hash_functions` - a list of functions that each accept a term and return a
      non-negative integer, defaults to randomly seeded Murmur

  ## Examples

      iex> bloom_filter = Talan.BloomFilter.new(1_000_000)
      iex> bloom_filter |> Talan.BloomFilter.put("Barna Kovacs")
      :ok
  """
  @spec new(pos_integer()) :: t()
  @spec new(pos_integer(), options()) :: t()
  def new(cardinality, options \\ []) do
    Validation.positive_integer!(cardinality, :cardinality)
    Validation.options!(options, @options)

    {filter_length, hash_functions} = configuration(cardinality, options)
    atomics_ref = :atomics.new(div(filter_length, 64), signed: false)

    %BF{
      atomics_ref: atomics_ref,
      filter_length: filter_length,
      hash_functions: hash_functions
    }
  end

  @doc false
  @spec configuration(pos_integer()) :: {pos_integer(), nonempty_list(Talan.hash_function())}
  @spec configuration(pos_integer(), options()) ::
          {pos_integer(), nonempty_list(Talan.hash_function())}
  def configuration(cardinality, options \\ []) do
    Validation.positive_integer!(cardinality, :cardinality)

    false_positive_probability = options |> Keyword.get(:false_positive_probability, 0.01)
    hash_functions = options |> Keyword.get(:hash_functions, [])

    Validation.probability!(false_positive_probability, :false_positive_probability)
    Validation.functions!(hash_functions, :hash_functions)

    hash_functions =
      case hash_functions do
        [] ->
          hash_count = required_hash_function_count(false_positive_probability)

          Talan.seed_n_murmur_hash_fun(hash_count)

        list ->
          list
      end

    filter_length = required_filter_length(cardinality, false_positive_probability)

    atomics_arity = div(filter_length + 63, 64)

    {atomics_arity * 64, hash_functions}
  end

  @doc """
  Returns the required number of hash functions for the
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
  @spec required_hash_function_count(float()) :: pos_integer()
  def required_hash_function_count(false_positive_probability) do
    Validation.probability!(false_positive_probability, :false_positive_probability)

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
  @spec required_filter_length(pos_integer(), float()) :: pos_integer()
  def required_filter_length(cardinality, false_positive_probability)
      when is_integer(cardinality) and cardinality > 0 and false_positive_probability > 0 and
             false_positive_probability < 1 do
    import :math, only: [log: 1, pow: 2]

    Float.ceil(-cardinality * log(false_positive_probability) / pow(log(2), 2))
    |> round()
  end

  @doc """
  Puts `term` into `bloom_filter`, a `%Talan.BloomFilter{}` struct.

  After insertion, `member?/2` will always return `true` for this `term`.

  Returns `:ok`.

  ## Examples

      iex> b = Talan.BloomFilter.new(1000)
      iex> b |> Talan.BloomFilter.put("Chris McCord")
      :ok
      iex> b |> Talan.BloomFilter.put("Jose Valim")
      :ok
  """
  @spec put(t, any) :: :ok
  def put(
        %BF{
          atomics_ref: atomics_ref,
          filter_length: filter_length,
          hash_functions: hash_functions
        },
        term
      ) do
    do_put(atomics_ref, filter_length, hash_functions, term)
  end

  @doc false
  @spec put_hashes(t(), list(non_neg_integer())) :: :ok
  def put_hashes(%BF{atomics_ref: atomics_ref}, hashes) when is_list(hashes) do
    do_put_hashes(atomics_ref, hashes)
  end

  defp do_put(atomics_ref, filter_length, [hash_fun | hash_functions], term) do
    hash = rem(hash_fun.(term), filter_length)

    # Hash functions are evaluated from left to right before bits are changed,
    # while bit updates retain hash_term/2's historical reverse order.
    do_put(atomics_ref, filter_length, hash_functions, term)
    Abit.set_bit_at(atomics_ref, hash, 1)
  end

  defp do_put(_atomics_ref, _filter_length, [], _term), do: :ok

  defp do_put_hashes(atomics_ref, [hash | hashes]) do
    Abit.set_bit_at(atomics_ref, hash, 1)
    do_put_hashes(atomics_ref, hashes)
  end

  defp do_put_hashes(_atomics_ref, []), do: :ok

  @doc """
  Clears every bit in `bloom_filter` and returns the same filter.

  The filter is reset in place without reallocating its atomics reference.
  Clearing individual atomic words is safe during concurrent access, but the
  filter is not cleared as one atomic operation. Concurrent reads may observe a
  partially cleared filter, and concurrent writes may be cleared or remain set
  depending on their timing.

  ## Examples

      iex> b = Talan.BloomFilter.new(1000)
      iex> Talan.BloomFilter.put(b, "Barna")
      iex> Talan.BloomFilter.clear(b) == b
      true
      iex> Talan.BloomFilter.member?(b, "Barna")
      false
  """
  @spec clear(t()) :: t()
  def clear(%BF{atomics_ref: atomics_ref} = bloom_filter) do
    Abit.clear(atomics_ref)
    bloom_filter
  end

  @doc """
  Checks for membership of `term` in `bloom_filter`.

  Returns `false` if the term is definitely absent.
  Returns `true` if the term may be present.

  ## Examples

      iex> b = Talan.BloomFilter.new(1000)
      iex> b |> Talan.BloomFilter.member?("Barna Kovacs")
      false
      iex> b |> Talan.BloomFilter.put("Barna Kovacs")
      iex> b |> Talan.BloomFilter.member?("Barna Kovacs")
      true
  """
  @spec member?(t, any) :: boolean
  def member?(
        %BF{
          atomics_ref: atomics_ref,
          filter_length: filter_length,
          hash_functions: hash_functions
        },
        term
      ) do
    do_member?(atomics_ref, filter_length, hash_functions, term)
  end

  defp do_member?(atomics_ref, filter_length, [hash_fun | hash_functions], term) do
    hash = rem(hash_fun.(term), filter_length)

    do_member?(atomics_ref, filter_length, hash_functions, term) and
      Abit.bit_at(atomics_ref, hash) == 1
  end

  defp do_member?(_atomics_ref, _filter_length, [], _term), do: true

  @doc """
  Hashes `term` with all `hash_functions` of `%Talan.BloomFilter{}`. Custom hash
  functions must return non-negative integers.

  Returns a list of hashed values.

  ## Examples

      iex> b = Talan.BloomFilter.new(1000, hash_functions: [fn _term -> 42 end])
      iex> Talan.BloomFilter.hash_term(b, :any_term_can_be_hashed)
      [42]
  """
  @spec hash_term(t(), term()) :: nonempty_list(non_neg_integer())
  def hash_term(%BF{filter_length: filter_length, hash_functions: hash_functions}, term) do
    do_hash_term(filter_length, hash_functions, term)
  end

  @doc false
  @spec hash_term(pos_integer(), list(Talan.hash_function()), term()) ::
          list(non_neg_integer())
  def hash_term(filter_length, hash_functions, term) do
    do_hash_term(filter_length, hash_functions, term)
  end

  defp do_hash_term(filter_length, hash_functions, term, acc \\ [])

  defp do_hash_term(filter_length, [hash_fun | tl], term, acc) do
    new_acc = [rem(hash_fun.(term), filter_length) | acc]

    do_hash_term(filter_length, tl, term, new_acc)
  end

  defp do_hash_term(_, [], _, acc), do: acc

  @doc """
  Merges the atomics of multiple `%Talan.BloomFilter{}` structs into one new struct.

  All filters must have the same size and use the same hash functions.

  Returns a new `%Talan.BloomFilter{}` struct whose set bits are the merged set bits of
  the bloom filters in the `list`.

  ## Examples

      iex> hash_functions = [fn term -> :erlang.phash2(term) end]
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
  def merge([first = %BF{atomics_ref: first_atomics_ref} | _tl] = list) do
    validate_compatible_filters!(list)

    %{size: size} = :atomics.info(first_atomics_ref)

    new_atomics_ref = :atomics.new(size, signed: false)

    Enum.each(list, fn %BF{atomics_ref: atomics_ref} ->
      Abit.union(new_atomics_ref, atomics_ref)
    end)

    %BF{first | atomics_ref: new_atomics_ref}
  end

  @doc """
  Intersects the atomics of multiple `%Talan.BloomFilter{}` structs into one new struct.

  All filters must have the same size and use the same hash functions.

  Returns a new `%BloomFilter{}` struct whose set bits are the intersection of
  the Bloom filters in the `list`.

  ## Examples

      iex> hash_functions = [fn term -> :erlang.phash2(term) end]
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
  def intersection([first = %BF{atomics_ref: first_atomics_ref} | filters] = list) do
    validate_compatible_filters!(list)

    %{size: size} = :atomics.info(first_atomics_ref)

    new_atomics_ref = :atomics.new(size, signed: false)

    Abit.union(new_atomics_ref, first_atomics_ref)

    Enum.each(filters, fn %BF{atomics_ref: atomics_ref} ->
      Abit.intersect(new_atomics_ref, atomics_ref)
    end)

    %BF{first | atomics_ref: new_atomics_ref}
  end

  @doc """
  Returns a non-negative integer representing the estimated number of unique
  elements in the filter.

  A saturated filter has no unset bits, so its cardinality cannot be estimated
  reliably; in that case this function returns a finite fallback value.

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
    Talan.estimate_cardinality(
      filter_length,
      Abit.set_bits_count(atomics_ref),
      length(hash_functions)
    )
  end

  @doc """
  Returns a float representing the current estimated
  false-positive probability.

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
      %{total_bits: 9600, set_bits_count: 0, set_ratio: 0.0}
  """
  @spec bits_info(t()) :: bits_info()
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

  The binary embeds the filter's hash functions and is only portable to
  environments running compatible code. It is not a stable interchange format
  across code upgrades.

  ## Examples

      iex> bloom_filter = Talan.BloomFilter.new(1000)
      iex> serialized = Talan.BloomFilter.serialize(bloom_filter)
      iex> is_binary(serialized)
      true

  """
  @doc since: "0.1.3"
  @spec serialize(t()) :: binary()
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

  The modules and code versions defining the serialized hash functions must be
  available and compatible.

  ## Examples

      iex> bloom_filter = Talan.BloomFilter.new(1000)
      iex> serialized = Talan.BloomFilter.serialize(bloom_filter)
      iex> deserialized = Talan.BloomFilter.deserialize(serialized)
      iex> is_struct(deserialized, Talan.BloomFilter)
      true

  """
  @doc since: "0.1.3"
  @spec deserialize(binary()) :: t()
  def deserialize(binary) when is_binary(binary) do
    map =
      binary
      |> :erlang.binary_to_term([:safe])
      |> Map.update!(:atomics_ref, &Abit.Atomics.deserialize(&1))

    struct!(__MODULE__, map)
  end

  defp validate_compatible_filters!([
         %BF{
           atomics_ref: first_atomics_ref,
           filter_length: first_filter_length,
           hash_functions: first_hash_functions
         }
         | filters
       ]) do
    %{size: first_atomics_size} = :atomics.info(first_atomics_ref)

    Enum.each(filters, fn %BF{
                            atomics_ref: atomics_ref,
                            filter_length: filter_length,
                            hash_functions: hash_functions
                          } ->
      %{size: atomics_size} = :atomics.info(atomics_ref)

      if atomics_size != first_atomics_size or filter_length != first_filter_length or
           hash_functions != first_hash_functions do
        raise ArgumentError,
              "all Bloom filters must have the same size and use the same hash functions"
      end
    end)
  end
end
