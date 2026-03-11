use std::collections::VecDeque;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::error::{AppError, WalResult};
use crate::task::{Task, TaskStatus};
use crate::wal::{Wal, WalEvent};

struct TaskQueueInner {
    pending: VecDeque<Task>,
    dead_letter: VecDeque<Task>,
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

    fn len(&self) -> usize {
        self.pending.len()
    }

    fn is_full(&self) -> bool {
        self.pending.len() >= self.max_capacity
    }

    fn insert_by_priority(&mut self, task: Task) {
        let pos = self
            .pending
            .iter()
            .position(|t| t.priority < task.priority)
            .unwrap_or(self.pending.len());

        self.pending.insert(pos, task);
    }
}

/// Shared handle to the task queue. Cheap to clone — just an Arc pointer.
#[derive(Clone)]
pub struct TaskQueue {
    inner: Arc<Mutex<TaskQueueInner>>,
    wal: Arc<Mutex<Wal>>,
}

impl TaskQueue {
    pub async fn new(mut wal: Wal, max_capacity: usize) -> WalResult<Self> {
        let recovered_tasks = wal.recover().await?;

        let mut inner = TaskQueueInner::new(max_capacity);

        let mut recovered_count = 0;
        for task in recovered_tasks.into_values() {
            match task.status {
                TaskStatus::Pending | TaskStatus::Running => {
                    inner.insert_by_priority(task);
                    recovered_count += 1;
                }
                _ => {}
            }
        }

        if recovered_count > 0 {
            info!(count = recovered_count, "Recovered tasks re-queued");
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            wal: Arc::new(Mutex::new(wal)),
        })
    }

    pub async fn push(&self, payload: Vec<u8>, priority: u8, max_retries: u32) -> WalResult<Uuid> {
        {
            let inner = self.inner.lock().await;
            if inner.is_full() {
                warn!(capacity = inner.max_capacity, "Queue full — rejecting task");
                return Err(AppError::QueueFull {
                    capacity: inner.max_capacity,
                });
            }
        }

        let task = Task::new(payload, priority, max_retries);
        let task_id = task.id;

        {
            let inner = self.inner.lock().await;
            let tasks_snapshot = Self::build_snapshot(&inner);
            let mut wal = self.wal.lock().await;
            wal.append(WalEvent::TaskSubmitted(task.clone()), &tasks_snapshot)
                .await?;
        }

        {
            let mut inner = self.inner.lock().await;
            inner.insert_by_priority(task);
            debug!(task_id = %task_id, queue_len = inner.len(), "Task enqueued");
        }

        Ok(task_id)
    }

    pub async fn pop(&self) -> WalResult<Option<Task>> {
        let now = Utc::now();

        let task_index = {
            let inner = self.inner.lock().await;
            inner.pending.iter().position(|t| {
                t.status == TaskStatus::Pending
                    && t.retry_after.map_or(true, |retry_at| retry_at <= now)
            })
        };

        let Some(index) = task_index else {
            return Ok(None);
        };

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

        let task = {
            let mut inner = self.inner.lock().await;
            let task = &mut inner.pending[index];
            task.status = TaskStatus::Running;
            task.updated_at = now;
            task.clone()
        };

        debug!(task_id = %task.id, "Task dispatched to worker");
        Ok(Some(task))
    }

    pub async fn complete(&self, task_id: Uuid) -> WalResult<()> {
        {
            let inner = self.inner.lock().await;
            let tasks_snapshot = Self::build_snapshot(&inner);
            let mut wal = self.wal.lock().await;
            wal.append(WalEvent::TaskCompleted { task_id }, &tasks_snapshot)
                .await?;
        }

        {
            let mut inner = self.inner.lock().await;
            inner.pending.retain(|t| t.id != task_id);
        }

        info!(task_id = %task_id, "Task completed");
        Ok(())
    }

    pub async fn fail(&self, task_id: Uuid, base_delay_ms: u64) -> WalResult<()> {
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
            let delay_ms = base_delay_ms * (1 << attempt);
            let retry_after = Utc::now() + chrono::Duration::milliseconds(delay_ms as i64);

            warn!(
                task_id   = %task_id,
                attempt   = new_attempt,
                max       = max_retries,
                delay_ms,
                "Task failed — scheduling retry"
            );

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

            {
                let mut inner = self.inner.lock().await;
                if let Some(task) = inner.pending.iter_mut().find(|t| t.id == task_id) {
                    task.attempt = new_attempt;
                    task.status = TaskStatus::Pending;
                    task.retry_after = Some(retry_after);
                    task.updated_at = Utc::now();
                }
            }
        } else {
            warn!(
                task_id = %task_id,
                attempts = new_attempt,
                "Task exceeded max retries — dead lettering"
            );

            {
                let inner = self.inner.lock().await;
                let tasks_snapshot = Self::build_snapshot(&inner);
                let mut wal = self.wal.lock().await;
                wal.append(WalEvent::TaskDeadLettered { task_id }, &tasks_snapshot)
                    .await?;
            }

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

    pub async fn pending_count(&self) -> usize {
        self.inner.lock().await.len()
    }

    pub async fn dead_letter_count(&self) -> usize {
        self.inner.lock().await.dead_letter.len()
    }

    pub async fn dead_letter_tasks(&self) -> Vec<Task> {
        self.inner
            .lock()
            .await
            .dead_letter
            .iter()
            .cloned()
            .collect()
    }

    fn build_snapshot(inner: &TaskQueueInner) -> std::collections::HashMap<Uuid, Task> {
        inner
            .pending
            .iter()
            .chain(inner.dead_letter.iter())
            .map(|t| (t.id, t.clone()))
            .collect()
    }
}
