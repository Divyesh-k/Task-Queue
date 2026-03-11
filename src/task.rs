use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    pub payload: Vec<u8>,
    pub priority: u8,
    pub status: TaskStatus,
    pub attempt: u32,
    pub max_retries: u32,
    pub submitted_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub retry_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    DeadLettered,
}

impl Task {
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
