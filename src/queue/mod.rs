//! # Queue Module
//!
//! The in-memory task queue — the central hub of the system.
//!
//! ## Relationship to the WAL
//!
//! The Queue wraps the WAL. Every state change goes through this contract:
//!
//! ```text
//! 1. Write event to WAL (disk)   ← durable, survives crash
//! 2. Apply change to memory      ← fast, used by workers
//! ```
//!
//! Workers never touch the WAL directly — they call `queue.pop()` and
//! `queue.complete()`. The queue handles durability internally.
//!
//! ## Thread safety design
//!
//! ```text
//! TaskQueue (cheap to clone — just an Arc pointer)
//!  └── Arc<Mutex<TaskQueueInner>>   ← actual data, locked per operation
//!       ├── pending:     VecDeque<Task>   ← tasks waiting for workers
//!       └── dead_letter: VecDeque<Task>   ← tasks that exceeded max_retries
//!
//! TaskQueue (separate Arc pointer)
//!  └── Arc<Mutex<Wal>>              ← WAL file handle, locked per write
//! ```
//!
//! Both locks are short-lived — grabbed, mutated, released immediately.
//! This avoids deadlocks and keeps throughput high.

use std::collections::VecDeque;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::error::{AppError, WalResult};
use crate::task::{Task, TaskStatus};
use crate::wal::{apply_event, Wal, WalEvent};

// ─────────────────────────────────────────────────────────────
// Inner state — only accessible while holding the Mutex
// ─────────────────────────────────────────────────────────────

/// The actual queue data, protected by a `Mutex`.
///
/// Split from `TaskQueue` so that `TaskQueue` can be cheaply cloned
/// (it's just an `Arc` pointer) while this inner struct holds the real data.
struct TaskQueueInner {
    /// Tasks waiting to be picked up by a worker, sorted high→low priority.
    /// We use `VecDeque` because we pop from the front and insert anywhere.
    pending: VecDeque<Task>,

    /// Tasks that failed more than `max_retries` times.
    /// Kept here for inspection / alerting. Never re-processed automatically.
    dead_letter: VecDeque<Task>,

    /// Hard limit on how many tasks can be in `pending` at once.
    /// Exceeding this returns `Err(QueueFull)` (backpressure).
    max_capacity: usize,
}

impl TaskQueueInner {
    fn new(max_capacity: usize) -> Self {
        Self {
            pending: VecDeque::new(),
            dead_letter: VecDeque::new(),
            max_capacity,
        }
    }

    /// How many tasks are currently waiting for a worker.
    fn len(&self) -> usize {
        self.pending.len()
    }

    /// Is the queue at or over its configured capacity?
    fn is_full(&self) -> bool {
        self.pending.len() >= self.max_capacity
    }

    /// Insert a task into `pending` in **priority order** (highest first).
    ///
    /// ## How priority insertion works
    ///
    /// We scan from the front until we find a task whose priority is
    /// strictly less than the new task's priority.  We insert just before
    /// that position, so equal-priority tasks are FIFO within their group.
    ///
    /// ```text
    /// Before: [pri=5] [pri=3] [pri=1]
    /// Insert pri=4:
    ///   index 0 → pri=5 >= 4 → skip
    ///   index 1 → pri=3 <  4 → insert here!
    /// After:  [pri=5] [pri=4] [pri=3] [pri=1]
    /// ```
    fn insert_by_priority(&mut self, task: Task) {
        // Find the index of the first task with a lower priority.
        let pos = self
            .pending
            .iter()
            .position(|t| t.priority < task.priority)
            .unwrap_or(self.pending.len()); // if none found, append to the end

        self.pending.insert(pos, task);
    }
}

// ─────────────────────────────────────────────────────────────
// TaskQueue — the public handle
// ─────────────────────────────────────────────────────────────

/// Public handle to the task queue.
///
/// Cheap to clone — internally just reference-counted pointers.
/// Every clone shares the same underlying data and WAL file.
///
/// ## Usage pattern
/// ```text
/// let queue = TaskQueue::new(wal, 10_000).await?;
/// let q2 = queue.clone();   // Worker 2 gets its own handle
/// let q3 = queue.clone();   // Worker 3 gets its own handle
/// // All three handles share the same VecDeque and WAL file.
/// ```
#[derive(Clone)]
pub struct TaskQueue {
    /// The actual in-memory data, shared across all clones.
    inner: Arc<Mutex<TaskQueueInner>>,

    /// The WAL, shared so every mutation is durably recorded.
    wal: Arc<Mutex<Wal>>,
}

impl TaskQueue {
    // ── Construction ─────────────────────────────────────────

    /// Create a new TaskQueue, recovering any tasks persisted in the WAL.
    ///
    /// ## What happens here
    /// 1. Call `wal.recover()` — reads the WAL file and snapshot from disk,
    ///    replays all events, and returns a `HashMap<Uuid, Task>`.
    /// 2. Filter to only `Pending` tasks (completed/failed tasks don't
    ///    need to go back into the queue — they're already done).
    /// 3. Insert each recovered pending task in priority order.
    pub async fn new(mut wal: Wal, max_capacity: usize) -> WalResult<Self> {
        // ── Step 1: Recover from disk ─────────────────────────
        // This replays the WAL file (and snapshot if present) to rebuild
        // the full task map as it was before the last shutdown/crash.
        let recovered_tasks = wal.recover().await?;

        // ── Step 2: Rebuild the pending queue ─────────────────
        let mut inner = TaskQueueInner::new(max_capacity);

        let mut recovered_count = 0;
        for task in recovered_tasks.into_values() {
            // Only re-queue tasks that were waiting or running when we crashed.
            // A "Running" task gets reset to "Pending" because the worker that
            // had it is gone — we need to reassign it.
            match task.status {
                TaskStatus::Pending | TaskStatus::Running => {
                    inner.insert_by_priority(task);
                    recovered_count += 1;
                }
                // Completed / Failed / DeadLettered tasks are terminal —
                // we don't re-add them to the queue.
                _ => {}
            }
        }

        if recovered_count > 0 {
            info!(
                count = recovered_count,
                "Recovered tasks re-queued after restart"
            );
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            wal: Arc::new(Mutex::new(wal)),
        })
    }

    // ── Push — client submits a new task ─────────────────────

    /// Accept a new task from a client and add it to the queue.
    ///
    /// ## Steps
    /// 1. **Backpressure check** — if the queue is full, return `QueueFull`
    ///    immediately. The broker will send a `Reject` frame to the client.
    /// 2. **WAL write** — record `TaskSubmitted` to disk before touching RAM.
    /// 3. **Memory update** — insert the task in priority order.
    ///
    /// Returns the new task's UUID so the broker can send it in the `Ack`.
    pub async fn push(&self, payload: Vec<u8>, priority: u8, max_retries: u32) -> WalResult<Uuid> {
        // ── Step 1: Backpressure check ────────────────────────
        // Lock is held just long enough to read the length, then released.
        {
            let inner = self.inner.lock().await;
            if inner.is_full() {
                warn!(
                    capacity = inner.max_capacity,
                    "Queue full — rejecting task (backpressure)"
                );
                return Err(AppError::QueueFull {
                    capacity: inner.max_capacity,
                });
            }
        } // ← lock released here

        // ── Step 2: Create task ───────────────────────────────
        // Build the Task struct in memory (assigns a UUID, timestamps, etc.)
        let task = Task::new(payload, priority, max_retries);
        let task_id = task.id;

        // ── Step 3: WAL write ─────────────────────────────────
        // WRITE TO DISK FIRST — this is the "Write-Ahead" part of WAL.
        // If we crash after this line but before the next, recovery will
        // find this event and re-add the task. The client will retry if
        // they don't get an Ack — so at worst the task is duplicated,
        // never lost.
        {
            let inner = self.inner.lock().await;
            let tasks_snapshot = Self::build_snapshot(&inner);
            let mut wal = self.wal.lock().await;
            wal.append(WalEvent::TaskSubmitted(task.clone()), &tasks_snapshot)
                .await?;
        }

        // ── Step 4: Memory update ─────────────────────────────
        // Now safe to add to RAM — disk is already updated.
        {
            let mut inner = self.inner.lock().await;
            inner.insert_by_priority(task);
            debug!(task_id = %task_id, queue_len = inner.len(), "Task enqueued");
        }

        Ok(task_id)
    }

    // ── Pop — worker requests the next task ──────────────────

    /// Give the next highest-priority `Pending` task to a worker.
    ///
    /// ## Steps
    /// 1. Find the first task in `pending` that is eligible to run:
    ///    - status must be `Pending`
    ///    - `retry_after` must be `None` or in the past
    /// 2. Record `TaskStarted` in the WAL.
    /// 3. Update the task's status to `Running` in memory.
    /// 4. Return the task to the caller (the worker).
    ///
    /// Returns `None` if no eligible task exists right now.
    pub async fn pop(&self) -> WalResult<Option<Task>> {
        let now = Utc::now();

        // ── Find an eligible task ─────────────────────────────
        // We only hold the lock long enough to find the task index.
        let task_index = {
            let inner = self.inner.lock().await;
            inner.pending.iter().position(|t| {
                t.status == TaskStatus::Pending
                    && t.retry_after.map_or(true, |retry_at| retry_at <= now)
            })
        };

        let Some(index) = task_index else {
            // No eligible task — worker should wait and retry later.
            return Ok(None);
        };

        // ── WAL write ─────────────────────────────────────────
        // Record that a worker is starting this task BEFORE updating RAM.
        let task_id = {
            let inner = self.inner.lock().await;
            inner.pending[index].id
        };

        {
            let inner = self.inner.lock().await;
            let tasks_snapshot = Self::build_snapshot(&inner);
            let mut wal = self.wal.lock().await;
            wal.append(WalEvent::TaskStarted { task_id }, &tasks_snapshot)
                .await?;
        }

        // ── Memory update ─────────────────────────────────────
        // Now flip the status to Running in RAM.
        let task = {
            let mut inner = self.inner.lock().await;
            // The index is still valid because we're the only thread
            // modifying `pending` (we hold the Mutex).
            let task = &mut inner.pending[index];
            task.status = TaskStatus::Running;
            task.updated_at = now;
            task.clone()
        };

        debug!(task_id = %task.id, "Task dispatched to worker");
        Ok(Some(task))
    }

    // ── Complete — worker finished successfully ───────────────

    /// Mark a task as successfully completed.
    ///
    /// ## Steps
    /// 1. Record `TaskCompleted` in the WAL.
    /// 2. Remove the task from `pending` in memory.
    pub async fn complete(&self, task_id: Uuid) -> WalResult<()> {
        // WAL write first
        {
            let inner = self.inner.lock().await;
            let tasks_snapshot = Self::build_snapshot(&inner);
            let mut wal = self.wal.lock().await;
            wal.append(WalEvent::TaskCompleted { task_id }, &tasks_snapshot)
                .await?;
        }

        // Remove from pending
        {
            let mut inner = self.inner.lock().await;
            inner.pending.retain(|t| t.id != task_id);
        }

        info!(task_id = %task_id, "Task completed");
        Ok(())
    }

    // ── Fail — worker reported an error ──────────────────────

    /// Handle a task failure from a worker.
    ///
    /// ## Decision logic
    ///
    /// ```text
    /// task.attempt + 1 <= task.max_retries?
    ///   YES → increment attempt, calculate retry_after, set status=Pending
    ///         The task stays in `pending` and will be picked up again
    ///         once retry_after has passed.
    ///   NO  → dead-letter the task (move to dead_letter queue)
    ///         Never retried again automatically.
    /// ```
    ///
    /// ## Exponential backoff formula
    ///
    /// ```text
    /// delay = base_delay_ms * 2^attempt
    ///
    /// attempt=0 → 100ms * 1   = 100ms
    /// attempt=1 → 100ms * 2   = 200ms
    /// attempt=2 → 100ms * 4   = 400ms
    /// attempt=3 → 100ms * 8   = 800ms
    /// attempt=4 → 100ms * 16  = 1600ms
    /// ```
    ///
    /// (Jitter is added in the Worker module on top of this base delay.)
    pub async fn fail(&self, task_id: Uuid, base_delay_ms: u64) -> WalResult<()> {
        // Read current state of the task
        let (attempt, max_retries) =
            {
                let inner = self.inner.lock().await;
                let task = inner.pending.iter().find(|t| t.id == task_id).ok_or(
                    AppError::TaskNotFound {
                        task_id: task_id.to_string(),
                    },
                )?;
                (task.attempt, task.max_retries)
            };

        let new_attempt = attempt + 1;

        if new_attempt <= max_retries {
            // ── Retry path ────────────────────────────────────
            // Calculate when the task can be retried (exponential backoff).
            let delay_ms = base_delay_ms * (1 << attempt); // base * 2^attempt
            let retry_after = Utc::now() + chrono::Duration::milliseconds(delay_ms as i64);

            warn!(
                task_id   = %task_id,
                attempt   = new_attempt,
                max       = max_retries,
                delay_ms,
                "Task failed — scheduling retry"
            );

            // WAL: record the failure with the new attempt count
            {
                let inner = self.inner.lock().await;
                let tasks_snapshot = Self::build_snapshot(&inner);
                let mut wal = self.wal.lock().await;
                wal.append(
                    WalEvent::TaskFailed {
                        task_id,
                        attempt: new_attempt,
                    },
                    &tasks_snapshot,
                )
                .await?;
            }

            // Memory: update attempt + retry_after, reset status to Pending
            {
                let mut inner = self.inner.lock().await;
                if let Some(task) = inner.pending.iter_mut().find(|t| t.id == task_id) {
                    task.attempt = new_attempt;
                    task.status = TaskStatus::Pending; // back in the pool
                    task.retry_after = Some(retry_after);
                    task.updated_at = Utc::now();
                }
            }
        } else {
            // ── Dead-letter path ─────────────────────────────
            warn!(
                task_id = %task_id,
                attempts = new_attempt,
                "Task exceeded max retries — moving to Dead Letter Queue"
            );

            // WAL: record dead-lettering
            {
                let inner = self.inner.lock().await;
                let tasks_snapshot = Self::build_snapshot(&inner);
                let mut wal = self.wal.lock().await;
                wal.append(WalEvent::TaskDeadLettered { task_id }, &tasks_snapshot)
                    .await?;
            }

            // Memory: move task from pending → dead_letter
            {
                let mut inner = self.inner.lock().await;
                if let Some(pos) = inner.pending.iter().position(|t| t.id == task_id) {
                    let mut task = inner.pending.remove(pos).unwrap();
                    task.status = TaskStatus::DeadLettered;
                    task.updated_at = Utc::now();
                    inner.dead_letter.push_back(task);
                }
            }
        }

        Ok(())
    }

    // ── Metrics / inspection helpers ──────────────────────────

    /// Number of tasks currently waiting for a worker.
    pub async fn pending_count(&self) -> usize {
        self.inner.lock().await.len()
    }

    /// Number of tasks in the Dead Letter Queue.
    pub async fn dead_letter_count(&self) -> usize {
        self.inner.lock().await.dead_letter.len()
    }

    /// Snapshot all dead-lettered tasks (for logging / alerting).
    pub async fn dead_letter_tasks(&self) -> Vec<Task> {
        self.inner
            .lock()
            .await
            .dead_letter
            .iter()
            .cloned()
            .collect()
    }

    // ── Private helpers ───────────────────────────────────────

    /// Build a `HashMap<Uuid, Task>` from the current inner state.
    ///
    /// The WAL's `append()` function needs all tasks so it can write
    /// a checkpoint snapshot when the interval is reached.
    fn build_snapshot(inner: &TaskQueueInner) -> std::collections::HashMap<Uuid, Task> {
        inner
            .pending
            .iter()
            .chain(inner.dead_letter.iter())
            .map(|t| (t.id, t.clone()))
            .collect()
    }
}
