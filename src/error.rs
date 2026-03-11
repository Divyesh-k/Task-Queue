use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL corruption at byte offset {offset}: {reason}")]
    WalCorruption { offset: u64, reason: String },

    #[error("Queue is full (capacity: {capacity})")]
    QueueFull { capacity: usize },

    #[error("Encode/decode error: {0}")]
    Codec(String),

    #[error("Task not found: {task_id}")]
    TaskNotFound { task_id: String },

    #[error("Worker heartbeat timed out: {worker_id}")]
    HeartbeatTimeout { worker_id: String },

    #[error("Task {task_id} exceeded max retries")]
    MaxRetriesExceeded { task_id: String },

    #[error("Shutdown signal received")]
    Shutdown,
}

pub type WalResult<T> = Result<T, AppError>;
