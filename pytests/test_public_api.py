from __future__ import annotations

import subprocess
import sys

import datalake_medallion_sdk as sdk


def test_version_matches_package_metadata() -> None:
    from importlib.metadata import version

    assert sdk.__version__ == version("datalake-medallion-sdk")


def test_all_exports_resolve() -> None:
    for name in sdk.__all__:
        assert hasattr(sdk, name), name


def test_retired_components_are_absent() -> None:
    for name in (
        "FirehoseClient",
        "HotColdDualClient",
        "PolarsLambdaClient",
        "TenantDbRouter",
        "GlueCatalogEnsurer",
        "DatalakeContextBuilder",
        "IngestionClientFactory",
        "invoke_hook_for_test",
    ):
        assert not hasattr(sdk, name), name
        assert not hasattr(sdk._core, name), name


def test_exception_hierarchy() -> None:
    assert issubclass(sdk.DatalakeError, Exception)
    for exc in (
        sdk.ContractBootError,
        sdk.ConfigurationError,
        sdk.TenantViolationError,
        sdk.MalformedPayloadError,
        sdk.RecordRejectedError,
        sdk.StorageError,
    ):
        assert issubclass(exc, sdk.DatalakeError), exc
    for exc in (
        sdk.SchemaViolationError,
        sdk.HookError,
        sdk.SemanticViolationError,
        sdk.TypeCoercionError,
    ):
        assert issubclass(exc, sdk.RecordRejectedError), exc


def test_imports_without_pyarrow_polars_or_boto3() -> None:
    code = (
        "import sys\n"
        "for m in ('pyarrow', 'polars', 'boto3', 'botocore', 'pydantic'):\n"
        "    sys.modules[m] = None\n"
        "import datalake_medallion_sdk as sdk\n"
        "r = sdk.ContractRegistry.from_schemas([{'type': 'object', 'x-dl-domain': 'd',"
        " 'x-dl-datatype': 't', 'x-dl-schema_version': '1'}])\n"
        "sdk.BronzeIngestionClient(r)\n"
        "print('ok')\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "ok"
