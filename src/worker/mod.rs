//! # Worker Pool
//!
//! Spawns N concurrent worker tasks that continuously pull from the queue,
//! execute tasks, and report results.
//!
//! ## Module layout
//!
//! ```text
//! mod.rs        ← Pool (spawns workers) + Worker (single execution loop)
//! heartbeat.rs  ← HeartbeatTracker (liveness registry)
//! retry.rs      ← backoff_delay() (exponential + jitter)
//! ```
//!
//! ## Concurrency model
//!
//! ```text
//! Pool::start()
//!   │
//!   ├── tokio::spawn ─→ Worker 0 loop { pop→execute→complete/fail }
//!   ├── tokio::spawn ─→ Worker 1 loop { pop→execute→complete/fail }
//!   ├── tokio::spawn ─→ Worker 2 loop { pop→execute→complete/fail }
//!   ├── tokio::spawn ─→ Worker 3 loop { pop→execute→complete/fail }
//!   └── tokio::spawn ─→ monitor  loop { check heartbeat timeouts }
//! ```
//!
//! All N+1 tasks share the same `TaskQueue` (via `Arc` clone) and the
//! same `HeartbeatTracker` (also `Arc`-cloned).
//!
//! ## Graceful shutdown
//!
//! A `CancellationToken` from `tokio_util::sync` is passed to every worker.
//! When the pool calls `token.cancel()`, each worker finishes its *current*
//! task loop iteration and then exits cleanly — no task is abandoned.

pub mod heartbeat;
pub mod retry;

use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::queue::TaskQueue;

use heartbeat::HeartbeatTracker;

// ─────────────────────────────────────────────────────────────
// WorkerConfig — everything a worker needs to know
// ─────────────────────────────────────────────────────────────

/// Runtime settings for the worker pool. In the full system these
/// come from `config/default.toml`, but here we pass them directly.
#[derive(Clone)]
pub struct WorkerConfig {
    /// How many worker tasks to spawn.
    pub count: usize,
    /// Workers send a heartbeat every this many seconds.
    pub heartbeat_interval_secs: u64,
    /// A worker is declared dead if no heartbeat for this many seconds.
    pub heartbeat_timeout_secs: u64,
    /// Base backoff delay in milliseconds for exponential retry.
    pub base_delay_ms: u64,
    /// Simulated task failure rate (0.0 = never fail, 1.0 = always fail).
    /// In a real system tasks fail on their own; this lets us demo retry logic.
    pub simulated_failure_rate: f64,
}

// ─────────────────────────────────────────────────────────────
// Pool — the supervisor
// ─────────────────────────────────────────────────────────────

/// The worker pool supervisor.
///
/// Create one with `Pool::start()` and it manages everything:
/// spawning workers, monitoring heartbeats, handling graceful shutdown.
pub struct Pool {
    /// Cancellation token — call `token.cancel()` to shut down all workers.
    pub token: CancellationToken,
    /// Shared heartbeat tracker, exposed so callers can inspect worker health.
    pub heartbeat: HeartbeatTracker,
}

impl Pool {
    /// Spawn the worker pool.
    ///
    /// ## What this does
    /// 1. Creates a `CancellationToken` (the shared "stop" signal).
    /// 2. Creates a `HeartbeatTracker` (the liveness registry).
    /// 3. Spawns N worker tasks (each gets its own queue clone + token clone).
    /// 4. Spawns a monitor task that watches for heartbeat timeouts.
    /// 5. Returns the `Pool` handle immediately — everything runs in the background.
    pub async fn start(queue: TaskQueue, config: WorkerConfig) -> Self {
        let token = CancellationToken::new();
        let heartbeat = HeartbeatTracker::new(config.heartbeat_timeout_secs);

        // ── Spawn N workers ───────────────────────────────────
        for i in 0..config.count {
            let worker_id = format!("worker-{}", i);
            let q = queue.clone();
            let tok = token.clone();
            let hb = heartbeat.clone();
            let cfg = config.clone();

            // Register before spawning so the monitor doesn't immediately
            // flag it as timed out.
            hb.register(&worker_id).await;

            tokio::spawn(async move {
                run_worker(worker_id, q, hb, tok, cfg).await;
            });
        }

        info!(workers = config.count, "Worker pool started");

        // ── Spawn heartbeat monitor ───────────────────────────
        {
            let hb = heartbeat.clone();
            let q = queue.clone();
            let tok = token.clone();
            let base_delay_ms = config.base_delay_ms;

            tokio::spawn(async move {
                run_monitor(q, hb, tok, base_delay_ms).await;
            });
        }

        Self { token, heartbeat }
    }

    /// Signal all workers to stop and wait for them to finish.
    ///
    /// Workers complete their current task and then exit. This typically
    /// takes at most one task-execution cycle (a few hundred ms in our demo).
    pub async fn shutdown(self) {
        info!("Pool shutdown requested — signalling all workers");
        self.token.cancel();

        // Give workers time to finish their current task.
        // In production you'd join the JoinHandles; for the demo a sleep is fine.
        tokio::time::sleep(Duration::from_millis(500)).await;
        info!("Pool shutdown complete");
    }
}

// ─────────────────────────────────────────────────────────────
// run_worker — one worker's entire lifecycle
// ─────────────────────────────────────────────────────────────

/// The main loop for a single worker.
///
/// ## Loop steps
/// 1. Check cancellation token → exit if cancelled.
/// 2. `queue.pop()` → get the next task (or sleep 100ms if none).
/// 3. Send heartbeat saying "I'm alive and working on task X".
/// 4. Execute the task (simulated here with a random sleep + failure roll).
/// 5. On success → `queue.complete()`.
///    On failure → `queue.fail()` → queue handles retry / DLQ logic.
/// 6. Send heartbeat saying "I'm idle".
/// 7. Go back to step 1.
async fn run_worker(
    worker_id: String,
    queue: TaskQueue,
    heartbeat: HeartbeatTracker,
    token: CancellationToken,
    config: WorkerConfig,
) {
    info!(worker_id, "Worker started");

    // How often the worker sends heartbeats while executing a long task.
    let hb_interval = Duration::from_secs(config.heartbeat_interval_secs);

    loop {
        // ── Step 1: Check for shutdown signal ─────────────────
        // `token.is_cancelled()` is non-blocking — just reads an atomic bool.
        if token.is_cancelled() {
            info!(worker_id, "Worker received shutdown signal — exiting");
            break;
        }

        // ── Step 2: Get next task ─────────────────────────────
        // `queue.pop()` returns `None` if there's nothing eligible right now
        // (queue empty, or all tasks are waiting for their retry_after).
        let task = match queue.pop().await {
            Ok(Some(t)) => t,
            Ok(None) => {
                // No task available — idle sleep to avoid busy-spinning.
                // During this sleep we also respect cancellation.
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                    _ = token.cancelled() => { break; }
                }
                continue;
            }
            Err(e) => {
                error!(worker_id, error = %e, "Queue pop error");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let task_id = task.id;
        info!(
            worker_id,
            task_id  = %task_id,
            priority = task.priority,
            attempt  = task.attempt,
            "Worker executing task"
        );

        // ── Step 3: Send heartbeat (I'm alive, working on task_id) ──
        heartbeat.beat(&worker_id, Some(task_id)).await;

        // ── Step 4: Execute the task ──────────────────────────
        // In production this is where you'd run real work.
        // Here we simulate it: random duration + random failure.
        let success = simulate_execution(
            &worker_id,
            task_id,
            config.simulated_failure_rate,
            hb_interval,
            &heartbeat,
            &token,
        )
        .await;

        // ── Step 5: Report the result ─────────────────────────
        if success {
            info!(worker_id, task_id = %task_id, "Task execution succeeded");
            if let Err(e) = queue.complete(task_id).await {
                error!(worker_id, task_id = %task_id, error = %e, "Failed to mark task complete");
            }
        } else {
            warn!(worker_id, task_id = %task_id, "Task execution failed — calling queue.fail()");
            if let Err(e) = queue.fail(task_id, config.base_delay_ms).await {
                error!(worker_id, task_id = %task_id, error = %e, "Failed to record task failure");
            }
        }

        // ── Step 6: Reset heartbeat to idle ───────────────────
        heartbeat.beat(&worker_id, None).await;
    }

    // Clean up — deregister so the monitor stops watching this worker.
    heartbeat.deregister(&worker_id).await;
    info!(worker_id, "Worker exited cleanly");
}

// ─────────────────────────────────────────────────────────────
// simulate_execution — fake "real work"
// ─────────────────────────────────────────────────────────────

/// Simulate executing a task.
///
/// In a real system this would call an external service, run a shell
/// command, write to a database, etc.
///
/// Here we:
/// 1. Sleep for 50–500ms (random, simulates variable workload).
/// 2. Send a heartbeat mid-way so the monitor knows we're not stuck.
/// 3. Roll a dice: fail with `failure_rate` probability.
///
/// Returns `true` (success) or `false` (failure).
async fn simulate_execution(
    worker_id: &str,
    task_id: Uuid,
    failure_rate: f64,
    _hb_interval: Duration,
    heartbeat: &HeartbeatTracker,
    token: &CancellationToken,
) -> bool {
    // ── Compute random values BEFORE any await ────────────────
    // ThreadRng (!Send) must not be held across an await point.
    // We compute everything we need in a scoped block so it is
    // dropped before the first tokio::select!.
    let (half, will_fail): (Duration, bool) = {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let work_ms = rng.gen_range(50u64..=500);
        let roll: f64 = rng.gen();
        (Duration::from_millis(work_ms / 2), roll < failure_rate)
    }; // ← rng dropped here, no Send issue

    // First half of simulated work — respect shutdown signal
    tokio::select! {
        _ = tokio::time::sleep(half) => {}
        _ = token.cancelled() => {
            // Treat mid-shutdown as failure so the task gets retried.
            return false;
        }
    }

    // Mid-task heartbeat — "I'm still alive"
    heartbeat.beat(worker_id, Some(task_id)).await;

    // Second half of simulated work
    tokio::select! {
        _ = tokio::time::sleep(half) => {}
        _ = token.cancelled() => {
            return false;
        }
    }

    // Return pre-computed result (no rng needed here)
    !will_fail
}

// ─────────────────────────────────────────────────────────────
// run_monitor — the heartbeat watchdog
// ─────────────────────────────────────────────────────────────

/// The heartbeat monitor runs in its own task.
///
/// ## What it does every second
/// 1. Call `heartbeat.check_timeouts()` → get list of dead workers.
/// 2. For each dead worker: if it had a task, re-fail that task so
///    it goes back into the pending queue (or eventually the DLQ).
/// 3. Deregister the dead worker from the tracker.
///    (In a full system the pool would also spawn a replacement worker.)
///
/// The monitor exits when the cancellation token fires.
async fn run_monitor(
    queue: TaskQueue,
    heartbeat: HeartbeatTracker,
    token: CancellationToken,
    base_delay_ms: u64,
) {
    info!("Heartbeat monitor started");

    loop {
        // Wait 1 second between checks, but exit immediately on cancel.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = token.cancelled() => {
                info!("Heartbeat monitor exiting");
                break;
            }
        }

        // ── Check for dead workers ─────────────────────────────
        let dead = heartbeat.check_timeouts().await;

        for (dead_worker_id, maybe_task_id) in dead {
            warn!(worker_id = %dead_worker_id, "Declaring worker dead");

            // If the worker had a task in-flight, fail it so it gets retried.
            if let Some(task_id) = maybe_task_id {
                warn!(
                    worker_id = %dead_worker_id,
                    task_id   = %task_id,
                    "Re-failing in-flight task from dead worker"
                );
                if let Err(e) = queue.fail(task_id, base_delay_ms).await {
                    error!(task_id = %task_id, error = %e, "Could not re-fail task");
                }
            }

            // Remove from tracker — in a full system we'd also spawn
            // a replacement worker here using a JoinSet or similar.
            heartbeat.deregister(&dead_worker_id).await;
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Exposed for tests
// ─────────────────────────────────────────────────────────────

/// Run a single worker to completion (used in tests / demos).
/// Public so `main.rs` can call it directly without the full pool.
pub async fn run_single_worker(
    worker_id: String,
    queue: TaskQueue,
    heartbeat: HeartbeatTracker,
    token: CancellationToken,
    config: WorkerConfig,
) {
    run_worker(worker_id, queue, heartbeat, token, config).await;
}
