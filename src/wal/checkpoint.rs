//! Checkpoint — snapshot the full task state to disk, then truncate the WAL.
//!
//! ## Why checkpoints?
//!
//! Without checkpointing, the WAL would grow forever — every task event
//! ever recorded would be in the file.  Recovery would have to replay
//! thousands of old "Completed" records just to rebuild a small queue.
//!
//! A checkpoint solves this:
//! 1. Serialise the current in-memory task map → write to `<wal>.snapshot`
//! 2. Truncate the WAL file to zero bytes
//!
//! On the next recovery the server loads the snapshot first (instant full
//! state), then replays only the *new* WAL entries written after the
//! checkpoint.  This keeps recovery fast no matter how long the server
//! has been running.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use uuid::Uuid;

use crate::error::{AppError, WalResult};
use crate::task::Task;

// ─────────────────────────────────────────────────────────────
// Snapshot file format
// ─────────────────────────────────────────────────────────────

/// The entire in-memory state captured at checkpoint time.
/// Serialised with `bincode` just like WAL records.
#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    /// All tasks known to the system at checkpoint time.
    tasks: Vec<Task>,
}

// ─────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────

/// Serialise `tasks` and write them to `snapshot_path`.
///
/// The snapshot is just a `bincode`-encoded [`Snapshot`] struct — no CRC
/// needed because we write it atomically (temp file + rename trick):
/// 1. Write to `<path>.tmp`
/// 2. Rename to `<path>` (atomic on every POSIX OS)
///
/// This means we never have a half-written snapshot on disk.
pub async fn write_checkpoint(snapshot_path: &str, tasks: &HashMap<Uuid, Task>) -> WalResult<()> {
    let snapshot = Snapshot {
        tasks: tasks.values().cloned().collect(),
    };

    // Encode to bytes
    let bytes = bincode::serde::encode_to_vec(&snapshot, bincode::config::standard())
        .map_err(|e| AppError::Codec(e.to_string()))?;

    // Write to a temp file first
    let tmp_path = format!("{}.tmp", snapshot_path);
    tokio::fs::write(&tmp_path, &bytes).await?;

    // Atomic rename — replaces the old snapshot in one syscall
    tokio::fs::rename(&tmp_path, snapshot_path).await?;

    info!(
        path = snapshot_path,
        tasks = tasks.len(),
        bytes = bytes.len(),
        "Checkpoint written"
    );

    Ok(())
}

/// Load a snapshot from `snapshot_path` and return the task map.
///
/// Called at the very start of [`super::Wal::recover`] so that the WAL
/// replay only has to process records written *after* the checkpoint.
pub async fn load_checkpoint(snapshot_path: &str) -> WalResult<HashMap<Uuid, Task>> {
    let bytes = tokio::fs::read(snapshot_path).await?;

    let (snapshot, _): (Snapshot, _) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).map_err(|e| {
            AppError::WalCorruption {
                offset: 0,
                reason: e.to_string(),
            }
        })?;

    let tasks: HashMap<Uuid, Task> = snapshot.tasks.into_iter().map(|t| (t.id, t)).collect();

    debug!(tasks = tasks.len(), "Snapshot loaded");

    Ok(tasks)
}
