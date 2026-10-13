## Summary

What changes and why.

## Checklist

- [ ] Tests added or updated; `cargo test --all-targets` and `pytest pytests` pass
- [ ] `cargo clippy --all-targets -- -D warnings`, `ruff check`, `mypy` and `stubtest` pass
- [ ] `.pyi` stubs and README updated if the public API changed
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
- [ ] Commits signed off (`git commit -s`, DCO)

## Safety

- [ ] No new path to storage that skips `BronzePipeline::prepare`
- [ ] No record values in errors, logs or dead-letter messages
- [ ] No credentials or real personal data in code, tests or fixtures

## Breaking changes

None / describe the migration path.
