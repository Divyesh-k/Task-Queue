use thiserror::Error;

/// Unified error type used across all modules.
/// By deriving `Error` with `thiserror`, each variant automatically
/// implements `std::error::Error` with the message format we specify.
#[derive(Debug, Error)]
pub enum AppError {
    /// Any file / socket / OS-level error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A WAL record had a wrong CRC — disk corruption or partial write after crash
    #[error("WAL corruption at byte offset {offset}: {reason}")]
    WalCorruption { offset: u64, reason: String },

    /// The in-memory queue has hit its configured max capacity
    #[error("Queue is full (capacity: {capacity})")]
    QueueFull { capacity: usize },

    /// Serialization / deserialization failure
    #[error("Encode/decode error: {0}")]
    Codec(String),

    /// Task ID was referenced but doesn't exist
    #[error("Task not found: {task_id}")]
    TaskNotFound { task_id: String },

    /// A worker stopped sending heartbeats
    #[error("Worker heartbeat timed out: {worker_id}")]
    HeartbeatTimeout { worker_id: String },

    /// Task has been attempted too many times
    #[error("Task {task_id} exceeded max retries")]
    MaxRetriesExceeded { task_id: String },

    /// Graceful shutdown was requested
    #[error("Shutdown signal received")]
    Shutdown,
}

/// Handy alias so functions can write `WalResult<T>` instead of `Result<T, AppError>`.
pub type WalResult<T> = Result<T, AppError>;
