# Changelog for Talan

## v1.0.0 - 2026-08-20
  * Update `abit` to `~> 1.0.0` and require Elixir 1.14 / OTP 25 or later
  * Correct Counting Bloom membership, frequency, cardinality, and storage behavior
  * Allocate filters and counters to fully meet their requested capacity
  * Return finite cardinality estimates for saturated filters and counters
  * Reject incompatible Bloom filters before merge and intersection operations
  * Safely decode serialized Bloom filters without creating unsafe runtime terms
  * Harden public input validation and typespecs
  * Add in-place `clear/1` operations for all data structures
  * Rename `Talan.Counter` to `Talan.LinearCounter`
  * Surface Counting Bloom counter overflow and underflow errors

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
