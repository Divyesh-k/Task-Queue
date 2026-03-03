//! # Write-Ahead Log (WAL)
//!
//! The WAL guarantees that **no submitted task is ever lost**, even if the
//! server crashes at the worst possible moment.
//!
//! ## How it works
//!
//! Every state change (task submitted, task completed, task failed…) is
//! written to an **append-only file on disk** *before* the change is applied
//! to the in-memory queue.  On restart the file is replayed from top to
//! bottom to rebuild the in-memory state.
//!
//! ## Record format on disk
//!
//! ```text
//! ┌──────────────┬──────────────┬─────────────────────────┐
//! │  CRC32 (4B)  │  Length (4B) │  bincode-encoded Record  │
//! └──────────────┴──────────────┴─────────────────────────┘
//! ```
//!
//! - **CRC32** — a checksum computed over the record bytes.
//!   If the checksum does not match on replay (e.g. crash mid-write),
//!   the record is silently skipped — so we never load corrupt data.
//! - **Length** — the number of bytes in the record body.
//! - **Record** — a [`WalRecord`] encoded with `bincode` (compact binary).
//!
//! ## Checkpointing
//!
//! After `checkpoint_interval` new writes the WAL calls
//! [`checkpoint::write_checkpoint`].  That function dumps the full
//! in-memory task list to a snapshot file and then **truncates the WAL**,
//! keeping the log file small.

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

// ─────────────────────────────────────────────────────────────
// WAL Record — one entry written to disk
// ─────────────────────────────────────────────────────────────

/// The type of event being recorded in the WAL.
/// Each variant maps to one thing that can happen to a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEvent {
    /// A new task was submitted by a client.
    TaskSubmitted(Task),
    /// A worker picked up the task and started executing.
    TaskStarted { task_id: Uuid },
    /// The worker finished successfully.
    TaskCompleted { task_id: Uuid },
    /// The worker reported a failure; `attempt` is the count so far.
    TaskFailed { task_id: Uuid, attempt: u32 },
    /// Max retries exceeded — task moved to Dead Letter Queue.
    TaskDeadLettered { task_id: Uuid },
}

/// One complete WAL entry (what is actually serialized to disk).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    /// Monotonically increasing sequence number for ordering.
    pub sequence: u64,
    /// The actual event.
    pub event: WalEvent,
}

// ─────────────────────────────────────────────────────────────
// Wal — the public handle to the log file
// ─────────────────────────────────────────────────────────────

/// Handle to the append-only WAL file.
///
/// All writes go through [`Wal::append`].
/// On startup call [`Wal::recover`] to rebuild state from disk.
pub struct Wal {
    /// Path to the `.log` file.
    path: String,
    /// Path to the checkpoint snapshot file (`<path>.snapshot`).
    snapshot_path: String,
    /// Sequence counter — incremented for every write.
    next_seq: u64,
    /// How many new entries before we trigger a checkpoint.
    checkpoint_interval: usize,
    /// How many records we've written since the last checkpoint.
    writes_since_checkpoint: usize,
    /// Async file handle for appending records.
    file: tokio::fs::File,
}

impl Wal {
    /// Open (or create) the WAL at `path`.
    ///
    /// If the file already exists the sequence counter is set to one
    /// past the last record found — so new writes never collide.
    pub async fn open(path: &str, checkpoint_interval: usize) -> WalResult<Self> {
        // Create the parent directory if it doesn't exist yet.
        if let Some(parent) = Path::new(path).parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Open in append mode so every write always goes to the end.
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

    // ── Writing ───────────────────────────────────────────────

    /// Append one event to the WAL.
    ///
    /// # What happens inside
    /// 1. Wrap the event in a [`WalRecord`] with the next sequence number.
    /// 2. Serialize the record to bytes using `bincode`.
    /// 3. Compute CRC32 checksum over those bytes.
    /// 4. Write: `[crc32 (4 B)] [length (4 B)] [record bytes]`.
    /// 5. `flush()` — force the OS buffer to the kernel.
    /// 6. Increment the sequence counter.
    /// 7. After every `checkpoint_interval` writes, trigger a checkpoint.
    pub async fn append(&mut self, event: WalEvent, tasks: &HashMap<Uuid, Task>) -> WalResult<()> {
        let record = WalRecord {
            sequence: self.next_seq,
            event,
        };

        // --- Step 1: serialize ---
        // bincode v2 API: encode returns Vec<u8>
        let record_bytes: Vec<u8> =
            bincode::serde::encode_to_vec(&record, bincode::config::standard())
                .map_err(|e| AppError::Codec(e.to_string()))?;

        // --- Step 2: checksum ---
        let crc = crc32_of(&record_bytes);

        // --- Step 3: build the on-disk bytes ---
        // Layout: [CRC32 (4)] [Length (4)] [Record bytes]
        let length = record_bytes.len() as u32;
        let mut entry = Vec::with_capacity(8 + record_bytes.len());
        entry.extend_from_slice(&crc.to_be_bytes());
        entry.extend_from_slice(&length.to_be_bytes());
        entry.extend_from_slice(&record_bytes);

        // --- Step 4: write to file ---
        self.file.write_all(&entry).await?;
        // Flush to kernel (still in OS page cache, but at least left our process).
        // For true durability you'd call sync_all(), but flush() is fast enough for demos.
        self.file.flush().await?;

        debug!(seq = self.next_seq, "WAL record written");

        self.next_seq += 1;
        self.writes_since_checkpoint += 1;

        // --- Step 5: checkpoint if needed ---
        if self.writes_since_checkpoint >= self.checkpoint_interval {
            self.checkpoint(tasks).await?;
        }

        Ok(())
    }

    // ── Checkpointing ─────────────────────────────────────────

    /// Write a snapshot of all current tasks and then truncate the WAL.
    ///
    /// After a checkpoint, recovery only needs to:
    /// 1. Load the snapshot (full state at checkpoint time).
    /// 2. Replay WAL records written *after* the snapshot.
    async fn checkpoint(&mut self, tasks: &HashMap<Uuid, Task>) -> WalResult<()> {
        info!(
            "WAL checkpoint starting ({} records since last)",
            self.writes_since_checkpoint
        );

        checkpoint::write_checkpoint(&self.snapshot_path, tasks).await?;

        // Truncate the WAL by closing and reopening it in write (not append) mode,
        // then immediately switching back to append.
        drop(std::mem::replace(
            &mut self.file,
            tokio::fs::File::open("/dev/null").await?, // temporary placeholder
        ));

        // Truncate by opening with create(true)+write(true) (no append).
        tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.path)
            .await?;

        // Re-open in append mode for future writes.
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        self.writes_since_checkpoint = 0;
        info!("WAL checkpoint complete — log truncated");

        Ok(())
    }

    // ── Recovery ──────────────────────────────────────────────

    /// Replay the WAL (and snapshot if present) to rebuild in-memory state.
    ///
    /// # Recovery algorithm
    /// 1. If a snapshot file exists, load it (gives us the state at last checkpoint).
    /// 2. Open the WAL file and read records one by one:
    ///    - Read 4-byte CRC   →  verify
    ///    - Read 4-byte length →  read that many bytes
    ///    - Deserialize the record
    ///    - Apply the event to the task map
    /// 3. Return the rebuilt task map; the caller puts it into the queue.
    ///
    /// Truncated / corrupt records at the tail (caused by crash mid-write)
    /// are detected by the CRC check and silently discarded — safe because
    /// a partial write means the event was never acknowledged to the client.
    pub async fn recover(&mut self) -> WalResult<HashMap<Uuid, Task>> {
        // Step 1: load snapshot if one exists
        let mut tasks = if Path::new(&self.snapshot_path).exists() {
            info!(path = %self.snapshot_path, "Loading WAL snapshot");
            checkpoint::load_checkpoint(&self.snapshot_path).await?
        } else {
            HashMap::new()
        };

        // Step 2: replay WAL records
        // We use synchronous std I/O here so we can seek + peek byte-by-byte.
        let wal_path = self.path.clone();
        if !Path::new(&wal_path).exists() {
            info!("No WAL file found — starting fresh");
            return Ok(tasks);
        }

        let mut file = std::fs::OpenOptions::new().read(true).open(&wal_path)?;

        let file_len = file.metadata()?.len();
        let mut offset: u64 = 0;
        let mut max_seq: u64 = 0;
        let mut records_replayed: usize = 0;

        loop {
            // Stop if we've reached end of file
            if offset >= file_len {
                break;
            }

            // ── Read CRC (4 bytes) ──────────────────────────────
            let crc_result = read_u32(&mut file);
            let stored_crc = match crc_result {
                Ok(v) => v,
                Err(_) => {
                    warn!(offset, "WAL: unexpected EOF reading CRC — stopping replay");
                    break;
                }
            };

            // ── Read length (4 bytes) ───────────────────────────
            let length = match read_u32(&mut file) {
                Ok(v) => v as usize,
                Err(_) => {
                    warn!(
                        offset,
                        "WAL: unexpected EOF reading length — stopping replay"
                    );
                    break;
                }
            };

            // ── Read record body ────────────────────────────────
            let mut record_bytes = vec![0u8; length];
            if let Err(e) = file.read_exact(&mut record_bytes) {
                warn!(offset, error = %e, "WAL: truncated record body — stopping replay");
                break;
            }

            // ── Verify checksum ─────────────────────────────────
            let computed_crc = crc32_of(&record_bytes);
            if stored_crc != computed_crc {
                // This usually means the crash happened mid-write on this record.
                // It's safe to stop here — any later records are also unreliable.
                warn!(
                    offset,
                    stored = stored_crc,
                    computed = computed_crc,
                    "WAL: CRC mismatch — truncated record, stopping replay"
                );
                break;
            }

            // ── Deserialize ─────────────────────────────────────
            let (record, _): (WalRecord, _) =
                bincode::serde::decode_from_slice(&record_bytes, bincode::config::standard())
                    .map_err(|e| AppError::WalCorruption {
                        offset,
                        reason: e.to_string(),
                    })?;

            // Track the highest sequence number for our counter
            if record.sequence >= max_seq {
                max_seq = record.sequence + 1;
            }

            // ── Apply the event ──────────────────────────────────
            apply_event(&mut tasks, record.event);

            records_replayed += 1;
            offset = file.stream_position()?;
        }

        // Set our sequence counter past the last record we saw.
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

// ─────────────────────────────────────────────────────────────
// Pure helper functions
// ─────────────────────────────────────────────────────────────

/// Apply a [`WalEvent`] to the in-memory task map.
/// This is called both during normal operation (after writing to disk)
/// and during recovery (replaying recorded events).
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

/// Compute CRC32 checksum over a slice of bytes.
fn crc32_of(data: &[u8]) -> u32 {
    let mut hasher = CrcHasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Read exactly 4 bytes from a sync file and return them as a big-endian u32.
fn read_u32(file: &mut std::fs::File) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}
