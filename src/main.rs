//! Task-Queue — entry point / demo runner.
//!
//! Each time we finish a module we update this file to demo the new feature.
//!
//! Current demo: Queue Module
//! Run with:  cargo run

mod error;
mod queue;
mod task;
mod wal;

use tracing::info;

use crate::error::WalResult;
use crate::queue::TaskQueue;
use crate::wal::Wal;

const WAL_PATH: &str = "./data/wal.log";
const MAX_CAPACITY: usize = 10;
const CHECKPOINT_INTERVAL: usize = 5;
const BASE_DELAY_MS: u64 = 100;

#[tokio::main]
async fn main() -> WalResult<()> {
    // Pretty log output. Set RUST_LOG=debug for verbose WAL-level logs.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("=== Task Queue — Queue Module Demo ===\n");

    // ── Open WAL + build Queue ────────────────────────────────────────────────
    //
    // TaskQueue::new() calls wal.recover() internally.
    // If data/wal.log exists from a previous run, tasks are restored
    // automatically — you don't need to do anything special.
    let wal = Wal::open(WAL_PATH, CHECKPOINT_INTERVAL).await?;
    let queue = TaskQueue::new(wal, MAX_CAPACITY).await?;

    info!(
        "Queue ready. Capacity: {}  Pending: {}\n",
        MAX_CAPACITY,
        queue.pending_count().await
    );

    // ─────────────────────────────────────────────────────────────────────────
    // PHASE 1 — Push tasks with different priorities
    //
    // Notice: we push them in order 1, 5, 3, 2, 4
    // After push(), the queue sorts them as 5, 4, 3, 2, 1
    // Workers always pop the highest priority first.
    // ─────────────────────────────────────────────────────────────────────────
    info!("--- Phase 1: Pushing 5 tasks (priorities 1,5,3,2,4) ---");

    let mut task_ids = Vec::new();

    for priority in [1u8, 5, 3, 2, 4] {
        let task_id = queue
            .push(
                format!("do-work-priority-{}", priority).into_bytes(),
                priority,
                3, // max 3 retries before dead-lettering
            )
            .await?;

        task_ids.push(task_id);
        info!(
            priority,
            task_id = %task_id,
            queue_len = queue.pending_count().await,
            "✅ Pushed task"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PHASE 2 — Pop tasks (should come out in priority order: 5, 4, 3, 2, 1)
    //
    // pop() finds the first Pending task whose retry_after has passed.
    // It records TaskStarted in the WAL, sets status=Running, returns the task.
    // ─────────────────────────────────────────────────────────────────────────
    info!("\n--- Phase 2: Workers popping tasks (expect order 5,4,3,2,1) ---");

    let mut popped = Vec::new();
    while let Some(task) = queue.pop().await? {
        info!(
            priority  = task.priority,
            task_id   = %task.id,
            status    = ?task.status,
            "📦 Worker popped task"
        );
        popped.push(task);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PHASE 3 — Complete some, fail others
    //
    // complete() → WAL records TaskCompleted, task removed from queue
    // fail()     → WAL records TaskFailed, task stays in queue with
    //              status=Pending and retry_after set (exponential backoff)
    // ─────────────────────────────────────────────────────────────────────────
    info!("\n--- Phase 3: Completing and failing tasks ---");

    // Complete the first 2 tasks (highest priority ones: 5, 4)
    for task in popped.iter().take(2) {
        queue.complete(task.id).await?;
        info!(
            priority = task.priority,
            task_id  = %task.id,
            "✅ Task completed"
        );
    }

    // Fail task #3 (priority=3) — it will be retried
    if let Some(task) = popped.get(2) {
        queue.fail(task.id, BASE_DELAY_MS).await?;
        info!(
            priority = task.priority,
            task_id  = %task.id,
            "❌ Task failed — retry scheduled with backoff"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PHASE 4 — Exhaust retries → Dead Letter Queue
    //
    // We repeatedly fail the same task until its attempt count exceeds
    // max_retries (3). On the 4th failure it gets dead-lettered.
    // ─────────────────────────────────────────────────────────────────────────
    info!("\n--- Phase 4: Exhausting retries → Dead Letter Queue ---");

    // Use the last task in our list (priority=1, max_retries=3)
    let dlq_task_id = *task_ids.last().unwrap();

    // Pop it so it's in Running state
    // (We set retry_after=None by calling fail until we dead-letter it)
    // Instead, bypass pop() for demo: push a fresh task with max_retries=1
    let disposable_id = queue
        .push(b"this-will-dlq".to_vec(), 1, 1) // max_retries = 1
        .await?;
    info!(task_id = %disposable_id, "Pushed disposable task (max_retries=1)");

    // Pop it into Running state
    if let Some(t) = queue.pop().await? {
        // Fail it twice — second fail pushes attempt(2) > max_retries(1) → DLQ
        queue.fail(t.id, BASE_DELAY_MS).await?; // attempt=1, retries left=0
        info!("Attempt 1 failed — one retry left");

        // We need to pop again for attempt 2, but retry_after is set.
        // For the demo we manually call fail again (simulating time passing).
        // In the real system workers wait for retry_after before popping.
        queue.fail(t.id, BASE_DELAY_MS).await?; // attempt=2, exceeds max=1 → DLQ
        info!("Attempt 2 failed — max retries exceeded → Dead Letter Queue");
    }

    let _ = dlq_task_id; // suppress unused warning

    // ─────────────────────────────────────────────────────────────────────────
    // PHASE 5 — Inspect DLQ and final queue state
    // ─────────────────────────────────────────────────────────────────────────
    info!("\n--- Phase 5: Final queue state ---");

    info!("  Pending tasks  : {}", queue.pending_count().await);
    info!("  Dead-lettered  : {}", queue.dead_letter_count().await);

    info!("\n  Dead Letter Queue contents:");
    for task in queue.dead_letter_tasks().await {
        info!(
            task_id  = %task.id,
            priority = task.priority,
            attempts = task.attempt,
            "  ☠  DLQ task"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PHASE 6 — Backpressure demo
    //
    // Fill the queue to MAX_CAPACITY, then try to push one more.
    // The 11th push should return Err(QueueFull).
    // ─────────────────────────────────────────────────────────────────────────
    info!(
        "\n--- Phase 6: Backpressure (capacity = {}) ---",
        MAX_CAPACITY
    );

    // Fill remaining slots
    let pending_now = queue.pending_count().await;
    let slots_left = MAX_CAPACITY.saturating_sub(pending_now);

    for i in 0..slots_left {
        queue.push(format!("fill-{}", i).into_bytes(), 1, 0).await?;
    }

    info!("Queue filled to capacity ({})", queue.pending_count().await);

    // Now try to push one more — should be rejected
    match queue.push(b"overflow".to_vec(), 1, 0).await {
        Err(crate::error::AppError::QueueFull { capacity }) => {
            info!(
                "🚫 Backpressure triggered: queue full at {} tasks. Push rejected!",
                capacity
            );
        }
        Ok(_) => info!("(unexpected: push succeeded)"),
        Err(e) => info!("Unexpected error: {}", e),
    }

    info!("\n=== Queue Module Demo Complete ===");
    info!("Data files: {} and {}.snapshot", WAL_PATH, WAL_PATH);

    Ok(())
}
