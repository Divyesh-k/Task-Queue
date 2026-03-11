use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use uuid::Uuid;

use crate::error::{AppError, WalResult};
use crate::task::Task;

#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    tasks: Vec<Task>,
}

/// Write the current task state to disk atomically (temp file + rename).
pub async fn write_checkpoint(snapshot_path: &str, tasks: &HashMap<Uuid, Task>) -> WalResult<()> {
    let snapshot = Snapshot {
        tasks: tasks.values().cloned().collect(),
    };

    let bytes = bincode::serde::encode_to_vec(&snapshot, bincode::config::standard())
        .map_err(|e| AppError::Codec(e.to_string()))?;

    let tmp_path = format!("{}.tmp", snapshot_path);
    tokio::fs::write(&tmp_path, &bytes).await?;
    tokio::fs::rename(&tmp_path, snapshot_path).await?;

    info!(
        path = snapshot_path,
        tasks = tasks.len(),
        bytes = bytes.len(),
        "Checkpoint written"
    );

    Ok(())
}

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
