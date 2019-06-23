defmodule Probabilistic.Atomics do
  @moduledoc """
  Defines helpers for Erlang :atomics module.
  """

  import Bitwise

  @doc """
  Create a new atomic array with options like `ref`.
  """
  def new_like(ref) when is_reference(ref), do: new_like(ref |> :atomics.info)

  def new_like(%{min: 0, size: size}), do: :atomics.new(size, signed: false)

  def new_like(%{min: _, size: size}), do: :atomics.new(size, signed: true)

  @doc """
  Merge atomics with Bitwise OR operator.

  `ref_b` will be merged into `ref_a`.
  """
  def merge_bitwise(ref_a, ref_b) do
    %{size: size} = ref_a |> :atomics.info

    merge_bitwise(ref_a, ref_b, size)
  end

  def merge_bitwise(ref_a,  _, 0), do: ref_a

  def merge_bitwise(ref_a, ref_b, index) do
    :atomics.put(
      ref_a,
      index,
      :atomics.get(ref_a, index) ||| :atomics.get(ref_b, index)
    )

    merge_bitwise(ref_a, ref_b, index - 1)
  end

  @doc """
  Bitwise Intersection of atomics using Bitwise AND operator.
  """
  def intersect_bitwise(ref_a, ref_b) do
    %{size: size} = ref_a |> :atomics.info

    intersect_bitwise(ref_a, ref_b, size)
  end

  def intersect_bitwise(ref_a, _, 0), do: ref_a

  def intersect_bitwise(ref_a, ref_b, index) do
    :atomics.put(
      ref_a,
      index,
      :atomics.get(ref_a, index) &&& :atomics.get(ref_b, index)
    )

    intersect_bitwise(ref_a, ref_b, index - 1)
  end

  @doc """
  Sets the bit at `bit_index` to 1 in the `atomics_ref`.
  """
  def put_bit(atomics_ref, bit_index) do
    idx = div(bit_index, 64) + 1

    bit_pos = rem(idx, 64)

    current_value = :atomics.get(atomics_ref, idx)

    next_value = current_value ||| 1 <<< bit_pos

    :atomics.put(atomics_ref, idx, next_value)
  end

  @doc """
  Returns `true` if bit is 1, `false` otherwise.
  """
  def bit?(atomics_ref, bit_index) do
    idx = div(bit_index, 64) + 1

    bit_pos = rem(idx, 64)

    current_value = :atomics.get(atomics_ref, idx)

    (current_value ||| 1 <<< bit_pos) == current_value
  end
end
