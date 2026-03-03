//! Task-Queue — entry point.
//!
//! Right now this file is a **runnable demo of the WAL module**.
//! It shows you exactly what happens when tasks are written, how we
//! recover from a simulated crash, and how a checkpoint is created.
//!
//! Run it with:
//!   cargo run
//!
//! You will see all WAL events printed to the console, and the
//! files `data/wal.log` and `data/wal.log.snapshot` created on disk.

mod error;
mod task;
mod wal;

use std::collections::HashMap;

use tracing::info;
use uuid::Uuid;

use crate::error::WalResult;
use crate::task::Task;
use crate::wal::{Wal, WalEvent};

// ── Constants ────────────────────────────────────────────────
const WAL_PATH: &str = "./data/wal.log";

/// Number of entries before auto-checkpoint fires in the demo.
/// Set very low (3) so you can see a checkpoint happen quickly.
const CHECKPOINT_INTERVAL: usize = 3;

#[tokio::main]
async fn main() -> WalResult<()> {
    // Set up pretty log output.
    // RUST_LOG=debug cargo run  ← shows every WAL record written
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("=== Task Queue WAL Demo ===\n");

    // ─────────────────────────────────────────────────────────
    // PHASE 1: Submit some tasks and write them to the WAL
    // ─────────────────────────────────────────────────────────
    info!("--- Phase 1: Submitting tasks ---");

    // This is our in-memory state — a map of task ID → Task.
    // In the full system this will live inside the Queue struct.
    let mut in_memory: HashMap<Uuid, Task> = HashMap::new();

    // Open (or create) the WAL file.
    let mut wal = Wal::open(WAL_PATH, CHECKPOINT_INTERVAL).await?;

    // Create 5 tasks and record each one to the WAL.
    let mut task_ids: Vec<Uuid> = Vec::new();

    for i in 1..=5 {
        // Create a new task with a simple text payload.
        let task = Task::new(
            format!("task-payload-{}", i).into_bytes(), // payload
            i as u8,                                    // priority
            3,                                          // max retries
        );
        let task_id = task.id;
        task_ids.push(task_id);

        info!(task_id = %task_id, priority = i, "Submitting task");

        // 🔑 KEY STEP: Write to disk FIRST, then add to memory.
        // This ordering is what "Write-Ahead" means.
        wal.append(WalEvent::TaskSubmitted(task.clone()), &in_memory)
            .await?;
        in_memory.insert(task_id, task);

        info!(queue_len = in_memory.len(), "Task added to in-memory queue");
    }

    // ─────────────────────────────────────────────────────────
    // PHASE 2: Mark some tasks as started and completed
    // ─────────────────────────────────────────────────────────
    info!("\n--- Phase 2: Processing tasks ---");

    // Start task 0
    let t0 = task_ids[0];
    info!(task_id = %t0, "Worker starting task");
    wal.append(WalEvent::TaskStarted { task_id: t0 }, &in_memory)
        .await?;
    wal::apply_event(&mut in_memory, WalEvent::TaskStarted { task_id: t0 });

    // Complete task 0
    info!(task_id = %t0, "Worker completed task");
    wal.append(WalEvent::TaskCompleted { task_id: t0 }, &in_memory)
        .await?;
    wal::apply_event(&mut in_memory, WalEvent::TaskCompleted { task_id: t0 });

    // Fail task 1
    let t1 = task_ids[1];
    info!(task_id = %t1, "Worker failed task (attempt 1)");
    wal.append(
        WalEvent::TaskFailed {
            task_id: t1,
            attempt: 1,
        },
        &in_memory,
    )
    .await?;
    wal::apply_event(
        &mut in_memory,
        WalEvent::TaskFailed {
            task_id: t1,
            attempt: 1,
        },
    );

    // ─────────────────────────────────────────────────────────
    // PHASE 3: Simulate a crash by dropping everything and recovering
    // ─────────────────────────────────────────────────────────
    info!("\n--- Phase 3: Simulating crash + recovery ---");

    // Drop in-memory state — gone!
    drop(in_memory);
    // Drop WAL handle — file is closed.
    drop(wal);

    info!("💥 Server crashed! RAM lost, but WAL file is safe on disk.");
    info!("Restarting and recovering from WAL...\n");

    // Reopen the WAL — this is the server restarting.
    let mut wal = Wal::open(WAL_PATH, CHECKPOINT_INTERVAL).await?;

    // Recovery replays every record from disk and rebuilds the task map.
    let recovered = wal.recover().await?;

    info!(
        "\n✅ Recovery complete! Tasks recovered: {}",
        recovered.len()
    );
    info!("\n--- Recovered task states ---");

    // Print each recovered task's status so you can see the result.
    let mut sorted: Vec<_> = recovered.values().collect();
    sorted.sort_by_key(|t| t.submitted_at);

    for task in sorted {
        info!(
            id       = %task.id,
            status   = ?task.status,
            priority = task.priority,
            attempt  = task.attempt,
            "Recovered task"
        );
    }

    info!("\n=== Demo complete ===");
    info!("Check these files on disk:");
    info!("  WAL log:    {}", WAL_PATH);
    info!("  Snapshot:   {}.snapshot", WAL_PATH);
    info!(
        "\nThe checkpoint fired after every {} writes.",
        CHECKPOINT_INTERVAL
    );

    Ok(())
}
