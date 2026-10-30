# Contributing

Thanks for helping. This guide covers setup, the checks every change must pass, and how contributions are licensed.

## Setup

Requirements: Rust 1.94.1+ (`rustup`), Python 3.12+. No Docker is needed: Python end-to-end tests run against an in-process S3 emulator (moto).

Release and benchmark builds use fat LTO, and each `rustc` process can take 0.5–1 GB of RAM. `dev.ps1` and `justfile` cap parallel compilation through `CARGO_BUILD_JOBS` (default 4, or a quarter of the CPU threads); set it explicitly on machines with less memory, e.g. `CARGO_BUILD_JOBS=2`.

```bash
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                        # builds the native extension with maturin
```

CI installs the same `dev` extra from `requirements/ci.txt`, pinned by hash. Regenerate it whenever the extra changes; the command is at the top of the file.

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

The Rust integration test against LocalStack (optional, needs Docker) is ignored by default: `AWS_ENDPOINT_URL=http://localhost:4566 cargo test --lib -- --ignored`.

## Standards

- Every record must go through `BronzePipeline::prepare`. Changes that add a path to storage that skips validation, PII protection or the quarantine flow are not accepted.
- No placeholders in shipped code: no `todo!`, no stubbed return values, no test-only functions in the public API. Documented behaviour must have a test.
- Library code does not use `unwrap`, `expect` or `panic!` (enforced by clippy); errors are typed (`DatalakeError`).
- Error messages, logs and dead-letter entries never include record values.
- Public Rust items are documented (`#![deny(missing_docs)]`); Python public APIs are typed and the `.pyi` stubs match the native module (`stubtest`).
- Commits follow [Conventional Commits](https://www.conventionalcommits.org/).

## Design principles

Changes are judged against these goals; a change that trades one away needs a stated reason.

- **Fast and lean.** Arrow in, Arrow out, no copies or Python objects on the hot path. A release is never slower than the previous one (see *Release gates*).
- **Bounded memory.** Anything that buffers has a cap (`chunk_rows`, `row_group_mb`, `max_inflight_parts`, `max_open_days`...), so memory does not grow with the input.
- **Polymorphic inputs.** Every write path accepts Arrow (the fast path), dicts and JSON; Rust and Python expose the same capabilities.
- **Per-call tenancy.** Tenant, destination bucket/prefix and protection are given per call or per profile; nothing is global.
- **Opt-in features.** Encryption, compression changes, event-time partitioning and any new behaviour are off by default and set per contract or per call; existing data and readers keep working.
- **Integrity first.** Writes are idempotent (deterministic keys from the batch or stream id), invalid records are quarantined, failed uploads are aborted. Retrying never duplicates data.
- **Fail at load, not at write.** Whatever a contract declares is checked when the contract is registered. A setting the SDK cannot honour must be rejected there, never discovered when data is written.

## Release gates

Each release (and each pull request that touches the paths involved) must show:

1. **Every declared option works in the shipped artifact.** Settings a contract or call can choose (codecs, types, modes) come from one list in the code, and tests run through that list in Rust *and* against the built wheel in Python. Cargo features that back an option are listed in `Cargo.toml`, not left to defaults.
2. **No performance regression.** Throughput and peak memory are compared with the previous release on the same machine, old and new builds run back to back and alternated (`examples/stream_throughput.rs`, `examples/compression_bench.rs`). Network or emulator time is measured separately from the SDK's own time.
3. **Both languages covered.** New behaviour has Rust tests and Python tests; `.pyi` stubs match (`stubtest`).
4. **Docs match behaviour.** README and `docs/` describe new options with their defaults; published benchmark numbers use generic hardware descriptions and contain no account ids, bucket names, ARNs or other environment details.

## Pull requests

1. Branch from `main`.
2. Add tests and update `CHANGELOG.md` under `[Unreleased]`.
3. Sign off every commit (`git commit -s`) to certify the [Developer Certificate of Origin](https://developercertificate.org/).
4. CI must be green before review.

## Licensing

Unless you state otherwise, any contribution you submit for inclusion is dual licensed under MIT OR Apache-2.0, as the project, without additional terms or conditions.
