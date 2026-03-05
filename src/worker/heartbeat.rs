//! # Heartbeat Tracker
//!
//! Tracks whether each active worker is still alive.
//!
//! ## The problem
//!
//! When a worker is executing a task, it goes silent — it's busy working.
//! From the outside, a *busy* worker looks identical to a *stuck* or *dead*
//! worker. We need a way to tell them apart.
//!
//! ## The solution — heartbeats
//!
//! Each worker sends a "ping" to the HeartbeatTracker every N seconds
//! while it's running (even mid-task). The tracker records the timestamp.
//!
//! A separate monitor loop runs periodically and checks:
//!   "Has any worker's last heartbeat timestamp gone stale?"
//!
//! If `now - last_seen > timeout`, the worker is declared dead.
//!
//! ## What happens when a worker is declared dead
//!
//! 1. The monitor logs a warning.
//! 2. The task that worker was running gets its status reset to `Pending`
//!    — it goes back into the queue for another worker to pick up.
//! 3. The dead worker's slot is restarted by the pool manager.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────
// WorkerState — per-worker tracking info
// ─────────────────────────────────────────────────────────────

/// Everything the tracker knows about one worker.
#[derive(Debug, Clone)]
pub struct WorkerState {
    /// The unique ID of this worker (e.g. "worker-0", "worker-1").
    pub worker_id: String,
    /// When did this worker last send a heartbeat?
    pub last_seen: Instant,
    /// Which task is this worker currently executing? (None = idle)
    pub current_task: Option<Uuid>,
}

// ─────────────────────────────────────────────────────────────
// HeartbeatTracker — shared registry
// ─────────────────────────────────────────────────────────────

/// Shared registry of all active workers and their last heartbeat times.
///
/// Wrapped in `Arc<Mutex<...>>` so both:
/// - workers can call `beat()` from their own async tasks
/// - the monitor loop can call `check_timeouts()` from a separate task
///
/// All without any worker needing to know about the others.
#[derive(Clone)]
pub struct HeartbeatTracker {
    inner: Arc<Mutex<HashMap<String, WorkerState>>>,
    /// How long without a heartbeat before a worker is declared dead.
    timeout: Duration,
}

impl HeartbeatTracker {
    /// Create a new tracker with the given timeout duration.
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    /// Register a new worker when it starts.
    ///
    /// Called once per worker at spawn time. Seeds the `last_seen`
    /// timestamp so the monitor doesn't immediately declare it dead.
    pub async fn register(&self, worker_id: &str) {
        let mut map = self.inner.lock().await;
        map.insert(
            worker_id.to_string(),
            WorkerState {
                worker_id: worker_id.to_string(),
                last_seen: Instant::now(),
                current_task: None,
            },
        );
        info!(worker_id, "Worker registered in heartbeat tracker");
    }

    /// Called by a worker periodically to say "I'm still alive".
    ///
    /// Also updates which task the worker is currently running,
    /// so the monitor knows which task to requeue if the worker dies.
    pub async fn beat(&self, worker_id: &str, current_task: Option<Uuid>) {
        let mut map = self.inner.lock().await;
        if let Some(state) = map.get_mut(worker_id) {
            state.last_seen = Instant::now();
            state.current_task = current_task;
        }
    }

    /// Remove a worker from the tracker when it shuts down cleanly.
    pub async fn deregister(&self, worker_id: &str) {
        let mut map = self.inner.lock().await;
        map.remove(worker_id);
        info!(worker_id, "Worker deregistered from heartbeat tracker");
    }

    /// Check all registered workers for heartbeat timeouts.
    ///
    /// Returns a list of `(worker_id, task_id)` pairs for every worker
    /// whose heartbeat has gone stale. The caller (pool manager) is
    /// responsible for requeuing the tasks and restarting the workers.
    ///
    /// The timed-out workers are **not removed** from the tracker here —
    /// the pool manager removes them via `deregister()` when it handles
    /// the failure.
    pub async fn check_timeouts(&self) -> Vec<(String, Option<Uuid>)> {
        let map = self.inner.lock().await;
        let now = Instant::now();

        map.values()
            .filter(|state| {
                // Worker is timed out if it hasn't beaten within the timeout window.
                now.duration_since(state.last_seen) > self.timeout
            })
            .map(|state| {
                warn!(
                    worker_id = %state.worker_id,
                    task_id   = ?state.current_task,
                    elapsed_secs = now.duration_since(state.last_seen).as_secs(),
                    "Heartbeat timeout detected"
                );
                (state.worker_id.clone(), state.current_task)
            })
            .collect()
    }

    /// How many workers are currently registered.
    pub async fn worker_count(&self) -> usize {
        self.inner.lock().await.len()
    }
}
