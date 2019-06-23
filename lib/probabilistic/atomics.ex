defmodule Probabilistic.Atomics do
  @moduledoc """
  Defines helpers for Erlang :atomics module.
  """

  import Bitwise

  @doc """
  Create a new unsigned atomic array with `size` like atomic `ref`.
  """
  def new_like(ref) when is_reference(ref), do: new_like(ref |> :atomics.info())

  def new_like(%{min: 0, size: size}), do: :atomics.new(size, signed: false)

  @doc """
  Merge atomics with Bitwise OR operator.

  `ref_b` will be merged into `ref_a`.
  """
  def merge_bitwise(ref_a, ref_b) do
    %{size: size} = ref_a |> :atomics.info()

    merge_bitwise(ref_a, ref_b, size)
  end

  def merge_bitwise(ref_a, _, 0), do: ref_a

  def merge_bitwise(ref_a, ref_b, index) do
    :atomics.put(
      ref_a,
      index,
      :atomics.get(ref_a, index) ||| :atomics.get(ref_b, index)
    )

    next_index = index - 1

    merge_bitwise(ref_a, ref_b, next_index)
  end

  @doc """
  Bitwise Intersection of atomics using Bitwise AND operator.
  """
  def intersect_bitwise(ref_a, ref_b) do
    %{size: size} = ref_a |> :atomics.info()

    intersect_bitwise(ref_a, ref_b, size)
  end

  def intersect_bitwise(ref_a, _, 0), do: ref_a

  def intersect_bitwise(ref_a, ref_b, index) do
    :atomics.put(
      ref_a,
      index,
      :atomics.get(ref_a, index) &&& :atomics.get(ref_b, index)
    )

    next_index = index - 1

    intersect_bitwise(ref_a, ref_b, next_index)
  end

  @doc """
  Sets the bit at `bit_index` to 1 in the atomic `ref`.
  """
  def put_bit(ref, bit_index) do
    {atomics_index, integer_bit_index} = bit_position(bit_index)

    current_value = :atomics.get(ref, atomics_index)

    next_value = current_value ||| (1 <<< integer_bit_index)

    :atomics.put(ref, atomics_index, next_value)
  end

  defp bit_position(bit_index) when is_integer(bit_index) and bit_index >= 0 do
    atomics_index = div(bit_index, 64) + 1

    integer_bit_index = rem(bit_index, 64)

    {atomics_index, integer_bit_index}
  end

  @doc """
  Returns bit value at `bit_index` in atomic `ref`.
  """
  def bit_at(ref, bit_index) when is_reference(ref) and is_integer(bit_index) do
    {atomics_index, integer_bit_index} = bit_position(bit_index)

    current_value = :atomics.get(ref, atomics_index)

    case current_value ||| (1 <<< integer_bit_index) do
      ^current_value -> 1
      _else -> 0
    end
  end

  @doc """
  Returns number of bits set to one in atomic `ref`.
  """
  def set_bits_count(ref) when is_reference(ref) do
    %{size: size} = ref |> :atomics.info()

    set_bits_count(ref, size, 0)
  end

  def set_bits_count(_, 0, acc), do: acc

  def set_bits_count(ref, index, acc) do
    count_at_index = Probabilistic.Bit.count_set_bits(:atomics.get(ref, index))

    new_acc = acc + count_at_index

    next_index = index - 1

    set_bits_count(ref, next_index, new_acc)
  end
end
