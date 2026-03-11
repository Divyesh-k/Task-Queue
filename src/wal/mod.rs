pub mod checkpoint;

use std::collections::HashMap;
use std::io::{Read, Seek};
use std::path::Path;

use crc32fast::Hasher as CrcHasher;
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::error::{AppError, WalResult};
use crate::task::{Task, TaskStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEvent {
    TaskSubmitted(Task),
    TaskStarted { task_id: Uuid },
    TaskCompleted { task_id: Uuid },
    TaskFailed { task_id: Uuid, attempt: u32 },
    TaskDeadLettered { task_id: Uuid },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub sequence: u64,
    pub event: WalEvent,
}

pub struct Wal {
    path: String,
    snapshot_path: String,
    next_seq: u64,
    checkpoint_interval: usize,
    writes_since_checkpoint: usize,
    file: tokio::fs::File,
}

impl Wal {
    pub async fn open(path: &str, checkpoint_interval: usize) -> WalResult<Self> {
        if let Some(parent) = Path::new(path).parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;

        let snapshot_path = format!("{}.snapshot", path);

        info!(wal_path = path, "WAL opened");

        Ok(Self {
            path: path.to_string(),
            snapshot_path,
            next_seq: 0,
            checkpoint_interval,
            writes_since_checkpoint: 0,
            file,
        })
    }

    pub async fn append(&mut self, event: WalEvent, tasks: &HashMap<Uuid, Task>) -> WalResult<()> {
        let record = WalRecord {
            sequence: self.next_seq,
            event,
        };

        let record_bytes: Vec<u8> =
            bincode::serde::encode_to_vec(&record, bincode::config::standard())
                .map_err(|e| AppError::Codec(e.to_string()))?;

        let crc = crc32_of(&record_bytes);
        let length = record_bytes.len() as u32;

        let mut entry = Vec::with_capacity(8 + record_bytes.len());
        entry.extend_from_slice(&crc.to_be_bytes());
        entry.extend_from_slice(&length.to_be_bytes());
        entry.extend_from_slice(&record_bytes);

        self.file.write_all(&entry).await?;
        self.file.flush().await?;

        debug!(seq = self.next_seq, "WAL record written");

        self.next_seq += 1;
        self.writes_since_checkpoint += 1;

        if self.writes_since_checkpoint >= self.checkpoint_interval {
            self.checkpoint(tasks).await?;
        }

        Ok(())
    }

    async fn checkpoint(&mut self, tasks: &HashMap<Uuid, Task>) -> WalResult<()> {
        info!(
            "WAL checkpoint ({} records since last)",
            self.writes_since_checkpoint
        );

        checkpoint::write_checkpoint(&self.snapshot_path, tasks).await?;

        drop(std::mem::replace(
            &mut self.file,
            tokio::fs::File::open("/dev/null").await?,
        ));

        tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.path)
            .await?;

        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        self.writes_since_checkpoint = 0;
        info!("WAL checkpoint done");

        Ok(())
    }

    pub async fn recover(&mut self) -> WalResult<HashMap<Uuid, Task>> {
        let mut tasks = if Path::new(&self.snapshot_path).exists() {
            info!(path = %self.snapshot_path, "Loading snapshot");
            checkpoint::load_checkpoint(&self.snapshot_path).await?
        } else {
            HashMap::new()
        };

        let wal_path = self.path.clone();
        if !Path::new(&wal_path).exists() {
            info!("No WAL file — starting fresh");
            return Ok(tasks);
        }

        let mut file = std::fs::OpenOptions::new().read(true).open(&wal_path)?;

        let file_len = file.metadata()?.len();
        let mut offset: u64 = 0;
        let mut max_seq: u64 = 0;
        let mut records_replayed: usize = 0;

        loop {
            if offset >= file_len {
                break;
            }

            let stored_crc = match read_u32(&mut file) {
                Ok(v) => v,
                Err(_) => {
                    warn!(offset, "WAL: unexpected EOF reading CRC");
                    break;
                }
            };

            let length = match read_u32(&mut file) {
                Ok(v) => v as usize,
                Err(_) => {
                    warn!(offset, "WAL: unexpected EOF reading length");
                    break;
                }
            };

            let mut record_bytes = vec![0u8; length];
            if let Err(e) = file.read_exact(&mut record_bytes) {
                warn!(offset, error = %e, "WAL: truncated record body");
                break;
            }

            let computed_crc = crc32_of(&record_bytes);
            if stored_crc != computed_crc {
                warn!(
                    offset,
                    stored = stored_crc,
                    computed = computed_crc,
                    "WAL: CRC mismatch — stopping replay"
                );
                break;
            }

            let (record, _): (WalRecord, _) =
                bincode::serde::decode_from_slice(&record_bytes, bincode::config::standard())
                    .map_err(|e| AppError::WalCorruption {
                        offset,
                        reason: e.to_string(),
                    })?;

            if record.sequence >= max_seq {
                max_seq = record.sequence + 1;
            }

            apply_event(&mut tasks, record.event);

            records_replayed += 1;
            offset = file.stream_position()?;
        }

        self.next_seq = max_seq;

        info!(
            records_replayed,
            tasks_recovered = tasks.len(),
            next_seq = self.next_seq,
            "WAL recovery complete"
        );

        Ok(tasks)
    }
}

pub fn apply_event(tasks: &mut HashMap<Uuid, Task>, event: WalEvent) {
    use chrono::Utc;
    match event {
        WalEvent::TaskSubmitted(task) => {
            tasks.insert(task.id, task);
        }
        WalEvent::TaskStarted { task_id } => {
            if let Some(t) = tasks.get_mut(&task_id) {
                t.status = TaskStatus::Running;
                t.updated_at = Utc::now();
            }
        }
        WalEvent::TaskCompleted { task_id } => {
            if let Some(t) = tasks.get_mut(&task_id) {
                t.status = TaskStatus::Completed;
                t.updated_at = Utc::now();
            }
        }
        WalEvent::TaskFailed { task_id, attempt } => {
            if let Some(t) = tasks.get_mut(&task_id) {
                t.status = TaskStatus::Failed;
                t.attempt = attempt;
                t.updated_at = Utc::now();
            }
        }
        WalEvent::TaskDeadLettered { task_id } => {
            if let Some(t) = tasks.get_mut(&task_id) {
                t.status = TaskStatus::DeadLettered;
                t.updated_at = Utc::now();
            }
        }
    }
}

fn crc32_of(data: &[u8]) -> u32 {
    let mut hasher = CrcHasher::new();
    hasher.update(data);
    hasher.finalize()
}

fn read_u32(file: &mut std::fs::File) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}
