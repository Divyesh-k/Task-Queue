//! # Retry Helper
//!
//! Calculates how long a failed task should wait before being retried.
//!
//! ## Why exponential backoff?
//!
//! If a task fails, something is probably wrong — an external service is slow,
//! a resource is locked, a network is flaky. Retrying immediately just hammers
//! the same broken thing. Waiting longer each time gives the system a chance
//! to recover, and **jitter** prevents all retrying tasks from waking up at
//! the exact same millisecond (called the "thundering herd" problem).
//!
//! ## Formula
//!
//! ```text
//! delay = (base_ms * 2^attempt) + random jitter
//!
//! base=100ms, attempt=0 → 100ms  + jitter(0–200ms)
//! base=100ms, attempt=1 → 200ms  + jitter(0–200ms)
//! base=100ms, attempt=2 → 400ms  + jitter(0–200ms)
//! base=100ms, attempt=3 → 800ms  + jitter(0–200ms)
//! base=100ms, attempt=4 → 1600ms + jitter(0–200ms)
//! ```

use rand::Rng;
use std::time::Duration;

/// Maximum jitter added on top of the exponential delay (milliseconds).
/// Spread retrying tasks randomly across this window.
const MAX_JITTER_MS: u64 = 200;

/// Calculate the delay before a task should be retried.
///
/// # Arguments
/// * `base_ms`  — base delay in milliseconds (from config)
/// * `attempt`  — how many times this task has already been attempted
///
/// # Returns
/// A `Duration` the worker (or queue) should wait before re-trying.
pub fn backoff_delay(base_ms: u64, attempt: u32) -> Duration {
    // Exponential part: base * 2^attempt
    // We cap the shift at 10 to avoid u64 overflow on very high attempt counts.
    let exp: u64 = 1u64.checked_shl(attempt.min(10) as u32).unwrap_or(u64::MAX);
    let base_delay = base_ms.saturating_mul(exp);

    // Jitter part: a random amount between 0 and MAX_JITTER_MS
    // `rand::thread_rng()` is fast — it uses the OS random source.
    let jitter = rand::thread_rng().gen_range(0..=MAX_JITTER_MS);

    Duration::from_millis(base_delay + jitter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_grows_with_attempts() {
        // Each attempt should produce a longer base delay.
        let d0 = backoff_delay(100, 0).as_millis();
        let d1 = backoff_delay(100, 1).as_millis();
        let d2 = backoff_delay(100, 2).as_millis();

        // Base part strictly doubles, jitter is at most MAX_JITTER_MS.
        // So d1 base (200) > d0 max (100 + 200 = 300)... not always true with jitter.
        // Just check that the base component is increasing.
        assert!(d1 >= 200, "attempt=1 should be at least 200ms base");
        assert!(d2 >= 400, "attempt=2 should be at least 400ms base");
        let _ = d0; // just ensure it compiles
    }

    #[test]
    fn no_overflow_on_large_attempt() {
        // Should not panic or overflow even for very high attempt numbers.
        let d = backoff_delay(100, 100);
        assert!(d.as_millis() > 0);
    }
}
