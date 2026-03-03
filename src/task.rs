use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Every task that flows through the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Globally unique ID for this task.
    pub id: Uuid,
    /// The work to be done — opaque bytes. Workers decide how to interpret it.
    pub payload: Vec<u8>,
    /// 0 = lowest priority, 255 = highest.
    pub priority: u8,
    /// Current lifecycle state.
    pub status: TaskStatus,
    /// How many times we've tried to run this task.
    pub attempt: u32,
    /// Maximum attempts before the task is dead-lettered.
    pub max_retries: u32,
    /// When the client first submitted this task.
    pub submitted_at: DateTime<Utc>,
    /// Last time the status changed.
    pub updated_at: DateTime<Utc>,
    /// If retrying, don't attempt before this time (exponential backoff).
    pub retry_after: Option<DateTime<Utc>>,
}

/// All states a task can be in during its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Waiting in the queue, not yet picked up.
    Pending,
    /// A worker is currently executing this task.
    Running,
    /// Finished successfully — terminal state.
    Completed,
    /// Execution failed; will be retried after `retry_after`.
    Failed,
    /// Exceeded `max_retries` — moved to the Dead Letter Queue.
    DeadLettered,
}

impl Task {
    /// Create a fresh task from a client submission.
    pub fn new(payload: Vec<u8>, priority: u8, max_retries: u32) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            payload,
            priority,
            status: TaskStatus::Pending,
            attempt: 0,
            max_retries,
            submitted_at: now,
            updated_at: now,
            retry_after: None,
        }
    }
}
