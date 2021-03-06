# Talán

[![Build Status](https://travis-ci.org/preciz/talan.svg?branch=master)](https://travis-ci.org/preciz/talan)

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

## License

Talán is [MIT licensed](LICENSE).
