# Contributing

Thanks for helping. This guide covers setup, the checks every change must pass, and how contributions are licensed.

## Setup

Requirements: Rust 1.94.1+ (`rustup`), Python 3.12+. No Docker is needed: Python end-to-end tests run against an in-process S3 emulator (moto).

Release and benchmark builds use fat LTO, and each `rustc` process can take 0.5–1 GB of RAM. `dev.ps1` and `justfile` cap parallel compilation through `CARGO_BUILD_JOBS` (default 4, or a quarter of the CPU threads); set it explicitly on machines with less memory, e.g. `CARGO_BUILD_JOBS=2`.

```bash
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                        # builds the native extension with maturin
```

## Checks

Run before opening a pull request; CI runs the same commands.

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets && cargo test --doc
ruff format --check python pytests && ruff check python pytests
mypy && python -m mypy.stubtest datalake_medallion_sdk._core
pytest pytests
cargo deny check
```

The Rust integration test against LocalStack is ignored by default: `AWS_ENDPOINT_URL=http://localhost:4566 cargo test --lib -- --ignored`.

## Standards

- Every record must go through `BronzePipeline::prepare`. Changes that add a path to storage that skips validation, PII hashing or the dead-letter flow are not accepted.
- No placeholders in shipped code: no `todo!`, no stubbed return values, no test-only functions in the public API. Documented behaviour must have a test.
- Library code does not use `unwrap`, `expect` or `panic!` (enforced by clippy); errors are typed (`DatalakeError`).
- Error messages, logs and dead-letter entries never include record values.
- Public Rust items are documented (`#![deny(missing_docs)]`); Python public APIs are typed and the `.pyi` stubs match the native module (`stubtest`).
- Commits follow [Conventional Commits](https://www.conventionalcommits.org/).

## Pull requests

1. Branch from `main`.
2. Add tests and update `CHANGELOG.md` under `[Unreleased]`.
3. Sign off every commit (`git commit -s`) to certify the [Developer Certificate of Origin](https://developercertificate.org/).
4. CI must be green before review.

## Licensing

Unless you state otherwise, any contribution you submit for inclusion is dual licensed under MIT OR Apache-2.0, as the project, without additional terms or conditions.
