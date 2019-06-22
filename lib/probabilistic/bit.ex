defmodule Probabilistic.Bit do
  @moduledoc """
  Defines helpers for bitmasks
  """

  import Bitwise

  @doc """
  Returns count of bits set to 1
  """
  def count_bits(int, acc \\ 0)

  def count_bits(0, acc), do: acc

  def count_bits(int, acc) when is_integer(int) and is_integer(acc) do
    case int &&& 1 do
      0 -> count_bits(int >>> 1, acc)
      1 -> count_bits(int >>> 1, acc + 1)
    end
  end
end
