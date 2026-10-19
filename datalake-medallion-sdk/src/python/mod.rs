//! Python bindings (`datalake_medallion_sdk._core`).
// PyO3 requires owned arguments in #[pymethods] signatures.
#![allow(clippy::needless_pass_by_value)]

mod client;
mod hooks;
mod otel;
mod registry;
mod tenancy;

use pyo3::prelude::*;

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    crate::error::register_exceptions(m)?;
    m.add_class::<registry::PyContractRegistry>()?;
    m.add_class::<client::PyBronzeIngestionClient>()?;
    m.add_class::<client::PyIngestReport>()?;
    m.add_class::<client::PyStreamReport>()?;
    m.add_class::<tenancy::PyPiiProtection>()?;
    m.add_class::<tenancy::PyTenantProfile>()?;
    m.add_class::<tenancy::PyTenantRegistry>()?;
    m.add_class::<tenancy::PyPiiDecryptor>()?;
    m.add_function(wrap_pyfunction!(otel::otel_logs_contract, m)?)?;
    m.add_function(wrap_pyfunction!(otel::flatten_otlp_logs, m)?)?;
    Ok(())
}
