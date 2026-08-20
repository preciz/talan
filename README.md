# Talán

[![test](https://github.com/preciz/talan/actions/workflows/test.yml/badge.svg)](https://github.com/preciz/talan/actions/workflows/test.yml)

Probabilistic data structures for Elixir, backed by Erlang's `:atomics`:

- `Talan.BloomFilter` — membership and cardinality estimation
- `Talan.CountingBloomFilter` — membership, frequency, and cardinality estimation with deletion
- `Talan.LinearCounter` — cardinality estimation
- `Talan.Stream.uniq/2` — bounded-memory stream deduplication

The data structures are mutable and support concurrent access. See the
[documentation](https://hexdocs.pm/talan) for API details and concurrency semantics.

## Installation

Talan requires Elixir 1.14 and OTP 25 or later.

```elixir
def deps do
  [
    {:talan, "~> 0.2.1"}
  ]
end
```

## Examples

### Bloom filter

```elixir
filter = Talan.BloomFilter.new(1_000)
:ok = Talan.BloomFilter.put(filter, "Barna")
true = Talan.BloomFilter.member?(filter, "Barna")
```

### Counting Bloom filter

```elixir
filter = Talan.CountingBloomFilter.new(1_000)
:ok = Talan.CountingBloomFilter.put(filter, "hat")
:ok = Talan.CountingBloomFilter.put(filter, "hat")
2 = Talan.CountingBloomFilter.count(filter, "hat")
:ok = Talan.CountingBloomFilter.delete(filter, "hat")
1 = Talan.CountingBloomFilter.count(filter, "hat")
```

`put/2` and `delete/2` return `{:error, :value_out_of_bounds}` when a packed
counter would overflow or underflow.

### Linear counter

```elixir
counter = Talan.LinearCounter.new(10_000)
:ok = Talan.LinearCounter.put(counter, "Barna")
1 = Talan.LinearCounter.cardinality(counter)
```

### Stream deduplication

```elixir
filter = Talan.BloomFilter.new(10_000, hash_functions: [fn value -> value end])

[1, 2, 3] =
  [1, 2, 1, 3]
  |> Talan.Stream.uniq(filter)
  |> Enum.to_list()
```

Bloom filter false positives can cause `Talan.Stream.uniq/2` to reject unique values. The
function mutates its filter, so re-enumerating the stream can produce different results.

## License

[MIT](LICENSE)
