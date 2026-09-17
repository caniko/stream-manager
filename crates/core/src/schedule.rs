//! Schedule handling: absolute UTC storage + IANA timezone display.
//!
//! Default timezone is Europe/Berlin. Ambiguous (fall-back) and
//! nonexistent (spring-forward) local times are explicit errors surfaced
//! in the editor — never silently shifted.

use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;
use thiserror::Error;

pub const DEFAULT_TIMEZONE: &str = "Europe/Berlin";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ScheduleError {
    #[error("unknown timezone: {0}")]
    UnknownTimezone(String),
    #[error("ambiguous local time (DST fall-back): pick an explicit offset")]
    Ambiguous,
    #[error("nonexistent local time (DST spring-forward): pick a valid time")]
    Nonexistent,
    #[error("end must be after start")]
    EndBeforeStart,
}

/// Parse a local `YYYY-MM-DD HH:MM` time in `tz_name` into absolute UTC.
pub fn local_to_utc(local: &str, tz_name: &str) -> Result<DateTime<Utc>, ScheduleError> {
    let tz: Tz = tz_name
        .parse()
        .map_err(|_| ScheduleError::UnknownTimezone(tz_name.into()))?;
    let naive = NaiveDateTime::parse_from_str(local, "%Y-%m-%d %H:%M")
        .map_err(|_| ScheduleError::Nonexistent)?;
    match naive.and_local_timezone(tz) {
        chrono::LocalResult::Single(dt) => Ok(dt.with_timezone(&Utc)),
        chrono::LocalResult::Ambiguous(_, _) => Err(ScheduleError::Ambiguous),
        chrono::LocalResult::None => Err(ScheduleError::Nonexistent),
    }
}

/// Format stored RFC3339 UTC for the `YYYY-MM-DD HH:MM` local editor.
/// Falls back to the raw value (or empty) when it does not parse, so no
/// timestamp is ever silently shifted by a display round-trip.
pub fn utc_to_local_input(utc_rfc3339: &str, tz_name: &str) -> String {
    if utc_rfc3339.trim().is_empty() {
        return String::new();
    }
    let parsed = chrono::DateTime::parse_from_rfc3339(utc_rfc3339.trim());
    let Ok(dt) = parsed else {
        return utc_rfc3339.to_string();
    };
    let Ok(tz): Result<Tz, _> = tz_name.parse() else {
        return utc_rfc3339.to_string();
    };
    dt.with_timezone(&tz).format("%Y-%m-%d %H:%M").to_string()
}

/// Validate a start/end pair (both absolute UTC).
pub fn validate_range(
    start: &DateTime<Utc>,
    end: &Option<DateTime<Utc>>,
) -> Result<(), ScheduleError> {
    if let Some(e) = end {
        if e <= start {
            return Err(ScheduleError::EndBeforeStart);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_berlin_winter() {
        let utc = local_to_utc("2026-01-15 20:00", "Europe/Berlin").unwrap();
        assert_eq!(utc.to_rfc3339(), "2026-01-15T19:00:00+00:00");
    }

    #[test]
    fn rejects_spring_forward_gap() {
        // 2026-03-29 02:30 does not exist in Europe/Berlin.
        assert_eq!(
            local_to_utc("2026-03-29 02:30", "Europe/Berlin").unwrap_err(),
            ScheduleError::Nonexistent
        );
    }

    #[test]
    fn rejects_fall_back_ambiguity() {
        // 2026-10-25 02:30 happens twice in Europe/Berlin.
        assert_eq!(
            local_to_utc("2026-10-25 02:30", "Europe/Berlin").unwrap_err(),
            ScheduleError::Ambiguous
        );
    }

    #[test]
    fn rejects_end_before_start() {
        let s = local_to_utc("2026-01-15 20:00", "Europe/Berlin").unwrap();
        let e = local_to_utc("2026-01-15 19:00", "Europe/Berlin").unwrap();
        assert_eq!(
            validate_range(&s, &Some(e)).unwrap_err(),
            ScheduleError::EndBeforeStart
        );
    }
}
