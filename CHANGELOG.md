# Changelog for Talan

## v0.1.4
  * Fix cardinality estimation when `set_bits_count` <= `hash_function_count` due to overlap

## v0.1.3
  * Implement BloomFilter.serialize/1 and BloomFilter.deserialize/1

## v0.1.2
  * Update murmur dependency to 1.0.3 to fix compilation warnings

## v0.1.1
  * Fix - work with elixir 1.7 by not using Kernel.floor/1 & Kernel.ceil/1
