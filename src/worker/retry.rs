use rand::Rng;
use std::time::Duration;

const MAX_JITTER_MS: u64 = 200;

/// Returns how long a failed task should wait before being retried.
/// Uses exponential backoff with jitter: `base_ms * 2^attempt + random(0..200ms)`.
pub fn backoff_delay(base_ms: u64, attempt: u32) -> Duration {
    let exp: u64 = 1u64.checked_shl(attempt.min(10) as u32).unwrap_or(u64::MAX);
    let base_delay = base_ms.saturating_mul(exp);
    let jitter = rand::thread_rng().gen_range(0..=MAX_JITTER_MS);
    Duration::from_millis(base_delay + jitter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_grows_with_attempts() {
        let d1 = backoff_delay(100, 1).as_millis();
        let d2 = backoff_delay(100, 2).as_millis();

        assert!(d1 >= 200, "attempt=1 should be at least 200ms base");
        assert!(d2 >= 400, "attempt=2 should be at least 400ms base");
    }

    #[test]
    fn no_overflow_on_large_attempt() {
        let d = backoff_delay(100, 100);
        assert!(d.as_millis() > 0);
    }
}
