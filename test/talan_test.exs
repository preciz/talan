defmodule TalanTest do
  use ExUnit.Case, async: true
  doctest Talan

  test "seed_n_murmur_hash_fun/1 returns the correct number of hash functions" do
    hash_functions = Talan.seed_n_murmur_hash_fun(5)
    assert length(hash_functions) == 5
    assert Enum.all?(hash_functions, &is_function(&1, 1))
  end

  test "seed_murmur_hash_fun/1 returns a function" do
    hash_fun = Talan.seed_murmur_hash_fun(42)
    assert is_function(hash_fun, 1)
  end

  test "to_bitstring/1 converts binary to list of bits" do
    assert Talan.to_bitstring(<<1, 2, 3>>) == [
             0,
             0,
             0,
             0,
             0,
             0,
             0,
             1,
             0,
             0,
             0,
             0,
             0,
             0,
             1,
             0,
             0,
             0,
             0,
             0,
             0,
             0,
             1,
             1
           ]

    assert Talan.to_bitstring(<<>>) == []
  end

  test "all public functions have typespecs" do
    {:ok, modules} = :application.get_key(:talan, :modules)

    for module <- modules do
      exports = MapSet.new(module.__info__(:functions))
      {:ok, specs} = Code.Typespec.fetch_specs(module)

      specified =
        specs
        |> Enum.map(fn {{name, arity}, _specs} -> {name, arity} end)
        |> MapSet.new()

      missing =
        exports
        |> MapSet.difference(specified)
        |> Enum.reject(fn {name, _arity} -> name == :__struct__ end)

      assert missing == [], "#{inspect(module)} is missing typespecs for #{inspect(missing)}"
    end
  end
end
