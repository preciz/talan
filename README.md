# Talán

![Actions Status](https://github.com/preciz/talan/workflows/test/badge.svg)

Probabilistic data structures in Elixir:
  * Bloom filter for membership estimation
  * Counting bloom filter for membership & cardinality estimation with delete support
  * Linear probabilistic counter for cardinality estimation

Documentation can be found at [https://hexdocs.pm/talan](https://hexdocs.pm/talan).

Talán is a Hungarian adverb meaning: maybe, perhaps, probably.

## Installation

Add `talan` to your list of dependencies in `mix.exs`:

**Note**: it requires OTP-21.2.1 or later.

```elixir
def deps do
  [
    {:talan, "~> 0.1.0"}
  ]
end
```

## Usage

```elixir
alias Talan.BloomFilter

bloom_filter = BloomFilter.new(1000)
bloom_filter |> BloomFilter.put("Barna")
bloom_filter |> BloomFilter.member?("Barna")
true
bloom_filter |> BloomFilter.member?("Kovacs")
false
```

## License

Talán is [MIT licensed](LICENSE).
