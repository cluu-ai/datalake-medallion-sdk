# Task runner for datalake-medallion-sdk (https://just.systems). Mirrors CI.

default:
    @just --list

# Build the extension into the active virtualenv
develop:
    maturin develop

fmt:
    cargo fmt --all
    ruff format python pytests

check:
    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features -- -D warnings
    ruff format --check python pytests
    ruff check python pytests
    mypy
    python -m mypy.stubtest datalake_medallion_sdk._core

test-rust:
    cargo test --all-targets
    cargo test --doc

test-python: develop
    pytest pytests -q

test: test-rust test-python

# Rust integration test against LocalStack (just up)
test-localstack:
    AWS_ENDPOINT_URL=http://localhost:4566 cargo test --lib -- --ignored

bench:
    cargo bench -- --noplot

deny:
    cargo deny check

secrets:
    gitleaks git . --config .gitleaks.toml --log-opts="--all" --redact

fuzz:
    cd datalake-medallion-sdk && cargo +nightly fuzz run contract_compile -- -max_total_time=60

wheel:
    maturin build --release --out dist
    twine check --strict dist/*

up:
    docker compose -f docker-compose.dev.yml up -d --wait

down:
    docker compose -f docker-compose.dev.yml down
