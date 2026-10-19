## Summary

What changes and why.

## Checklist

- [ ] Tests added or updated; `cargo test --all-targets` and `pytest pytests` pass
- [ ] `cargo clippy --all-targets -- -D warnings`, `ruff check`, `mypy` and `stubtest` pass
- [ ] `.pyi` stubs and README updated if the public API changed
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
- [ ] Commits signed off (`git commit -s`, DCO)

## Release gates

- [ ] New options are off by default and set per contract or per call
- [ ] Options come from one list in the code; tests cover every entry in Rust and against the wheel
- [ ] Contract settings that cannot be honoured are rejected at registration
- [ ] Buffers added by this change are capped
- [ ] Benchmarks vs the previous release (back to back) show no regression, or N/A
- [ ] Published numbers and docs contain no account or environment details

## Safety

- [ ] No new path to storage that skips `BronzePipeline::prepare`
- [ ] No record values in errors, logs or dead-letter messages
- [ ] No credentials or real personal data in code, tests or fixtures

## Breaking changes

None / describe the migration path.
