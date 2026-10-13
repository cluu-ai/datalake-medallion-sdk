use pyo3::prelude::*;
use serde_json::{Map, Value};

use crate::validation::codes;
use crate::validation::hooks::{HookOutcome, RecordHook};

/// Adapts a Python callable `(dict) -> PreIngestHookResult` to [`RecordHook`].
///
/// The GIL is held only while the callable runs. Exceptions are reported by
/// type name only, because their messages may contain record values.
pub(crate) struct PyRecordHook {
    callable: Py<PyAny>,
}

impl PyRecordHook {
    pub(crate) fn new(callable: Py<PyAny>) -> Self {
        Self { callable }
    }

    fn call(&self, py: Python<'_>, record: Map<String, Value>) -> PyResult<HookOutcome> {
        let json = py.import("json")?;
        let arg = json.call_method1("loads", (Value::Object(record).to_string(),))?;
        let result = self.callable.bind(py).call1((arg,))?;

        let status = result.getattr("status")?;
        let status: String = match status.getattr("value") {
            Ok(v) => v.extract()?,
            Err(_) => status.extract()?,
        };
        let invalid = |reason: &str| HookOutcome::Reject {
            code: codes::HOOK_INVALID_RESULT.to_owned(),
            reason: reason.to_owned(),
        };
        Ok(match status.as_str() {
            "ACCEPT" => {
                let payload = result.getattr("payload")?;
                if payload.is_none() {
                    return Ok(invalid("hook accepted without a payload"));
                }
                let text: String = json.call_method1("dumps", (payload,))?.extract()?;
                match serde_json::from_str(&text) {
                    Ok(Value::Object(m)) => HookOutcome::Accept(m),
                    _ => invalid("hook payload is not a JSON object"),
                }
            }
            "REJECT" => HookOutcome::Reject {
                code: result
                    .getattr("error_code")?
                    .extract::<Option<String>>()?
                    .unwrap_or_default(),
                reason: result
                    .getattr("reason")?
                    .extract::<Option<String>>()?
                    .unwrap_or_else(|| "rejected by hook".to_owned()),
            },
            _ => invalid("hook status must be ACCEPT or REJECT"),
        })
    }
}

impl RecordHook for PyRecordHook {
    fn apply(&self, record: Map<String, Value>) -> HookOutcome {
        Python::try_attach(|py| {
            self.call(py, record).unwrap_or_else(|err| {
                let name = err
                    .get_type(py)
                    .name()
                    .map_or_else(|_| "exception".to_owned(), |n| n.to_string());
                HookOutcome::Failed {
                    message: format!("hook raised {name}"),
                }
            })
        })
        .unwrap_or_else(|| HookOutcome::Failed {
            message: "Python interpreter unavailable".to_owned(),
        })
    }
}
