#!/usr/bin/env just --justfile

nightly := "nightly-2025-06-20"
msrv := "1.87"
rust := env("RUSTUP_TOOLCHAIN", "stable")
feature-set := "mock,tls-ring,server,client,duplex"
extended-features := "mock,tls,tls-ring,server,client,duplex"

# Run all checks
all: fmt check-all deny clippy examples docs test machete udeps msrv
    @echo "All checks passed 🍻"

# Check for unused dependencies
udeps: udeps-one udeps-hack

# [private]
udeps-one:
    CARGO_TARGET_DIR="target/udeps" cargo +{{ nightly }} udeps --all-features

# [private]
udeps-hack:
    CARGO_TARGET_DIR="target/udeps" cargo +{{ nightly }} hack udeps --each-feature

# Use machete to check for unused dependencies
machete:
    cargo +{{ rust }} machete --skip-target-dir

alias c := check

# Check compilation
check:
    cargo +{{ rust }} check --all-targets --all-features

# Check compilation across all features
check-all: check check-hack-each check-hack-powerset check-hack-all-targets

# Check feature combinations
check-hack: check-hack-each check-hack-powerset check-hack-all-targets

cargo-hack-args := "--target-dir target/hack/"

[private]
check-hack-each:
    cargo +{{ rust }} hack check {{ cargo-hack-args }} --each-feature --skip tls

[private]
check-hack-powerset:
    cargo +{{ rust }} hack check {{ cargo-hack-args }} --feature-powerset --skip docs,tls,tls-aws-lc

[private]
check-hack-tests: (check-hack-targets "tests")

[private]
check-hack-examples: (check-hack-targets "examples")

[private]
check-hack-benches: (check-hack-targets "benches")

[private]
check-hack-all-targets: (check-hack-targets "all-targets")

# Check compilation combinations for a specific target
check-hack-targets targets='tests':
    cargo +{{ rust }} hack check --{{ targets }} {{ cargo-hack-args }} --no-private --feature-powerset --exclude-no-default-features --include-features {{ feature-set }}

# Build the library in release mode
build:
    cargo +{{ rust }} build --release

# Run clippy
clippy:
    cargo +{{ rust }} clippy --all-targets --all-features -- -D warnings

# Check examples
examples:
    cargo +{{ rust }} check --examples --all-features

alias d := docs
alias doc := docs

# Build documentation
docs:
    cargo +{{ rust }} doc --all-features --no-deps

# Build and read documentation
read: docs
    cargo +{{ rust }} doc --all-features --no-deps --open

# Check support for MSRV
msrv:
    cargo +{{ msrv }} check --target-dir target/msrv/ --all-targets --all-features
    cargo +{{ msrv }} doc --target-dir target/msrv/ --all-features --no-deps

alias t := test

# Run cargo tests
test: test-build test-run

[private]
test-build:
    cargo +{{ rust }} nextest run --features {{ extended-features }} --no-run

[private]
test-run:
    cargo +{{ rust }} nextest run --features {{ extended-features }}
    cargo +{{ rust }} test --features {{ extended-features }} --doc

# Run coverage tests
coverage:
    cargo +{{ rust }} tarpaulin -o html --features {{ extended-features }}

alias timing := timings

# Compile with timing checks
timings:
    cargo +{{ rust }} build --features {{ extended-features }} --timings

# Run deny checks
deny:
    cargo +{{ rust }} deny check

# Run fmt checks
fmt:
    cargo +{{ rust }} fmt --all --check

fmt-run:
    cargo +{{ rust }} fmt --all

# Run pre-commit checks
pre-commit:
    pre-commit run --all-files

[private]
pre-commit-ci:
    SKIP=cargo-machete,fmt,check,clippy pre-commit run --color=always --all-files --show-diff-on-failure --hook-stage commit
