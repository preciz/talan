defmodule Probabilistic.Bit do
  @moduledoc """
  Defines helpers for bitmasks.
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

  @doc """
  Returns count of bits set to 1
  """
  def count_64_bits(
         <<b_01::1, b_02::1, b_03::1, b_04::1, b_05::1, b_06::1, b_07::1, b_08::1, b_09::1,
           b_10::1, b_11::1, b_12::1, b_13::1, b_14::1, b_15::1, b_16::1, b_17::1, b_18::1,
           b_19::1, b_20::1, b_21::1, b_22::1, b_23::1, b_24::1, b_25::1, b_26::1, b_27::1,
           b_28::1, b_29::1, b_30::1, b_31::1, b_32::1, b_33::1, b_34::1, b_35::1, b_36::1,
           b_37::1, b_38::1, b_39::1, b_40::1, b_41::1, b_42::1, b_43::1, b_44::1, b_45::1,
           b_46::1, b_47::1, b_48::1, b_49::1, b_50::1, b_51::1, b_52::1, b_53::1, b_54::1,
           b_55::1, b_56::1, b_57::1, b_58::1, b_59::1, b_60::1, b_61::1, b_62::1, b_63::1,
           b_64::1>>
       ) do
    b_01 + b_02 + b_03 + b_04 + b_05 + b_06 + b_07 + b_08 + b_09 + b_10 + b_11 + b_12 + b_13 +
      b_14 + b_15 + b_16 + b_17 + b_18 + b_19 + b_20 + b_21 + b_22 + b_23 + b_24 + b_25 + b_26 +
      b_27 + b_28 + b_29 + b_30 + b_31 + b_32 + b_33 + b_34 + b_35 + b_36 + b_37 + b_38 + b_39 +
      b_40 + b_41 + b_42 + b_43 + b_44 + b_45 + b_46 + b_47 + b_48 + b_49 + b_50 + b_51 + b_52 +
      b_53 + b_54 + b_55 + b_56 + b_57 + b_58 + b_59 + b_60 + b_61 + b_62 + b_63 + b_64
  end
end
