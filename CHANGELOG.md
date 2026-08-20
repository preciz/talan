# Changelog for Talan

## Unreleased
  * Improve documentation wording and fix typos
  * Update locked dependencies, including Murmur to 2.0.1 and ExDoc to 0.40.3
  * Return a finite cardinality estimate when a linear counter is saturated
  * Round Bloom filter storage up so it always meets the required bit count
  * Allocate exactly one Counting Bloom filter counter per Bloom filter bit
  * Validate Bloom filter compatibility before merge and intersection operations

## v0.2.1
  * Improve syntax and language
  * Update `abit` to `~> 0.4.0` and `ex_doc` to `~> 0.40`
  * Replace deprecated `Abit.merge/2` with `Abit.union/2`

## v0.2.0
  * BREAKING: Use Murmur v2.0 that fixes 128-bit hash generation to be consistent with the original implementation

## v0.1.4
  * Fix cardinality estimation when `set_bits_count` <= `hash_function_count` due to overlap

## v0.1.3
  * Implement BloomFilter.serialize/1 and BloomFilter.deserialize/1

## v0.1.2
  * Update murmur dependency to 1.0.3 to fix compilation warnings

## v0.1.1
  * Fix - work with Elixir 1.7 by not using Kernel.floor/1 and Kernel.ceil/1
