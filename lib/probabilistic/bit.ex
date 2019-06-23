defmodule Probabilistic.Bit do
  @moduledoc """
  Defines helpers for bitmasks.
  """

  import Bitwise

  @doc """
  Returns count of bits set to 1 in an Integer.
  """
  def count_set_bits(int, acc \\ 0)

  def count_set_bits(0, acc), do: acc

  def count_set_bits(int, acc) when is_integer(int) and is_integer(acc) do
    case int &&& 1 do
      0 ->
        int = int >>> 1

        count_set_bits(int, acc)
      1 ->
        int = int >>> 1

        new_acc = acc + 1

        count_set_bits(int, new_acc)
    end
  end
end
