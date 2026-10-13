//! Python bindings (`datalake_medallion_sdk._core`).
// PyO3 requires owned arguments in #[pymethods] signatures.
#![allow(clippy::needless_pass_by_value)]

mod client;
mod hooks;
mod registry;

use pyo3::prelude::*;

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    crate::error::register_exceptions(m)?;
    m.add_class::<registry::PyContractRegistry>()?;
    m.add_class::<client::PyBronzeIngestionClient>()?;
    m.add_class::<client::PyIngestReport>()?;
    Ok(())
}
