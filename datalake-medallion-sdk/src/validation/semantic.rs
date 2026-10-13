//! Level 5: semantic rules that JSON Schema cannot express.
//!
//! The SDK ships one domain-agnostic rule: a value of a field declared with
//! `"format": "date-time"` must not lie further in the future than the
//! configured skew. Domain rules belong in a pre-ingest hook (Level 4).

use chrono::{DateTime, Duration, Utc};
use serde_json::{Map, Value};

use super::{codes, level, Rejection};

/// Rejects the record when any of `date_time_fields` is later than `now + max_skew`.
///
/// Values that are not RFC 3339 strings are ignored here; format assertion
/// already happened at Level 3.
#[must_use]
pub fn check_future_timestamps(
    record: &Map<String, Value>,
    date_time_fields: &[String],
    now: DateTime<Utc>,
    max_skew: Duration,
) -> Option<Rejection> {
    let limit = now + max_skew;
    date_time_fields.iter().find_map(|field| {
        let ts = record.get(field)?.as_str()?;
        let parsed = DateTime::parse_from_rfc3339(ts).ok()?;
        (parsed.with_timezone(&Utc) > limit).then(|| {
            Rejection::new(
                level::SEMANTIC,
                codes::FUTURE_TIMESTAMP,
                format!(
                    "field '{field}' is more than {}s in the future",
                    max_skew.num_seconds()
                ),
            )
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => unreachable!(),
        }
    }

    fn fields() -> Vec<String> {
        vec!["occurred_at".to_owned()]
    }

    #[test]
    fn past_and_near_future_pass() {
        let now = Utc::now();
        for ts in [now - Duration::days(365), now + Duration::minutes(30)] {
            let r = rec(json!({"occurred_at": ts.to_rfc3339()}));
            assert!(check_future_timestamps(&r, &fields(), now, Duration::hours(1)).is_none());
        }
    }

    #[test]
    fn far_future_is_rejected_without_echoing_value() {
        let now = Utc::now();
        let ts = (now + Duration::hours(3)).to_rfc3339();
        let r = rec(json!({"occurred_at": ts.clone()}));
        let rej = check_future_timestamps(&r, &fields(), now, Duration::hours(1)).unwrap();
        assert_eq!(rej.code, codes::FUTURE_TIMESTAMP);
        assert_eq!(rej.level, level::SEMANTIC);
        assert!(!rej.message.contains(&ts));
    }

    #[test]
    fn offsets_are_normalized_to_utc() {
        let now = Utc::now();
        let shifted = (now + Duration::minutes(10))
            .with_timezone(&chrono::FixedOffset::east_opt(5 * 3600).unwrap())
            .to_rfc3339();
        let r = rec(json!({"occurred_at": shifted}));
        assert!(check_future_timestamps(&r, &fields(), now, Duration::hours(1)).is_none());
    }

    #[test]
    fn undeclared_and_invalid_values_are_ignored() {
        let now = Utc::now();
        let r = rec(json!({"occurred_at": "not-a-date", "other": "2999-01-01T00:00:00Z"}));
        assert!(check_future_timestamps(&r, &fields(), now, Duration::hours(1)).is_none());
    }
}
