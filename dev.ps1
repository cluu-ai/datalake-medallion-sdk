<#
.SYNOPSIS
    Developer task runner for datalake-medallion-sdk (Windows / PowerShell 7).

.DESCRIPTION
    Runs the same checks as CI using the native toolchain (rustup + Python).
    No Docker is required; `up`/`down` only manage the optional LocalStack S3
    container used by the ignored Rust integration test.

.EXAMPLE
    ./dev.ps1 setup    # create .venv and build the extension
    ./dev.ps1 check    # formatting, lints, types, stubs
    ./dev.ps1 test     # Rust and Python tests
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet("setup", "build", "check", "test", "test-rust", "test-python", "bench", "deny", "secrets", "wheel", "up", "down", "help")]
    [string]$Task = "help"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Each LTO release/bench rustc process can take ~0.5-1 GB of RAM; cap parallelism
# locally unless the caller chose a value.
if (-not $env:CARGO_BUILD_JOBS) {
    $env:CARGO_BUILD_JOBS = [Math]::Max(2, [int][Math]::Floor([Environment]::ProcessorCount / 4))
}

$Venv = Join-Path $PSScriptRoot ".venv"
$Py = if ($IsWindows -or $env:OS -eq "Windows_NT") { Join-Path $Venv "Scripts/python.exe" } else { Join-Path $Venv "bin/python" }

function Run([string]$Label, [scriptblock]$Cmd) {
    Write-Host "==> $Label" -ForegroundColor Cyan
    & $Cmd
    if ($LASTEXITCODE -ne 0) { throw "$Label failed (exit $LASTEXITCODE)" }
}

function Require-Venv {
    if (-not (Test-Path $Py)) { throw "Run './dev.ps1 setup' first." }
}

switch ($Task) {
    "setup" {
        if (Get-Command uv -ErrorAction SilentlyContinue) {
            Run "create .venv" { uv venv $Venv --python 3.12 }
            Run "install dev extras" { uv pip install --python $Py -e ".[dev]" }
        } else {
            Run "create .venv" { python -m venv $Venv }
            Run "install dev extras" { & $Py -m pip install -e ".[dev]" }
        }
    }
    "build" {
        Require-Venv
        Run "maturin develop" { & $Py -m maturin develop }
    }
    "check" {
        Require-Venv
        Run "cargo fmt" { cargo fmt --all -- --check }
        Run "cargo clippy" { cargo clippy --all-targets --all-features -- -D warnings }
        Run "ruff format" { & $Py -m ruff format --check python pytests }
        Run "ruff check" { & $Py -m ruff check python pytests }
        Run "mypy" { & $Py -m mypy }
        Run "stubtest" { & $Py -m mypy.stubtest datalake_medallion_sdk._core }
    }
    "test-rust" {
        Run "cargo test" { cargo test --all-targets }
        Run "doctests" { cargo test --doc }
    }
    "test-python" {
        Require-Venv
        Run "maturin develop" { & $Py -m maturin develop }
        Run "pytest" { & $Py -m pytest pytests -q }
    }
    "test" {
        & $PSCommandPath test-rust
        & $PSCommandPath test-python
    }
    "bench" { Run "criterion" { cargo bench -- --noplot } }
    "deny" { Run "cargo deny" { cargo deny check } }
    "secrets" {
        Run "gitleaks (history)" { gitleaks git . --config .gitleaks.toml --log-opts="--all" --redact }
    }
    "wheel" {
        Require-Venv
        Run "maturin build" { & $Py -m maturin build --release --out dist }
        Run "twine check" { & $Py -m twine check --strict dist/* }
    }
    "up" { Run "LocalStack S3" { docker compose -f docker-compose.dev.yml up -d --wait } }
    "down" { Run "LocalStack S3" { docker compose -f docker-compose.dev.yml down } }
    default { Get-Help $PSCommandPath -Detailed }
}
