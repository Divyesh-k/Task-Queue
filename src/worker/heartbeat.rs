use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct WorkerState {
    pub worker_id: String,
    pub last_seen: Instant,
    pub current_task: Option<Uuid>,
}

#[derive(Clone)]
pub struct HeartbeatTracker {
    inner: Arc<Mutex<HashMap<String, WorkerState>>>,
    timeout: Duration,
}

impl HeartbeatTracker {
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    pub async fn register(&self, worker_id: &str) {
        let mut map = self.inner.lock().await;
        map.insert(
            worker_id.to_string(),
            WorkerState {
                worker_id: worker_id.to_string(),
                last_seen: Instant::now(),
                current_task: None,
            },
        );
        info!(worker_id, "Worker registered");
    }

    pub async fn beat(&self, worker_id: &str, current_task: Option<Uuid>) {
        let mut map = self.inner.lock().await;
        if let Some(state) = map.get_mut(worker_id) {
            state.last_seen = Instant::now();
            state.current_task = current_task;
        }
    }

    pub async fn deregister(&self, worker_id: &str) {
        let mut map = self.inner.lock().await;
        map.remove(worker_id);
        info!(worker_id, "Worker deregistered");
    }

    pub async fn check_timeouts(&self) -> Vec<(String, Option<Uuid>)> {
        let map = self.inner.lock().await;
        let now = Instant::now();

        map.values()
            .filter(|state| now.duration_since(state.last_seen) > self.timeout)
            .map(|state| {
                warn!(
                    worker_id = %state.worker_id,
                    task_id   = ?state.current_task,
                    elapsed_secs = now.duration_since(state.last_seen).as_secs(),
                    "Heartbeat timeout"
                );
                (state.worker_id.clone(), state.current_task)
            })
            .collect()
    }

    pub async fn worker_count(&self) -> usize {
        self.inner.lock().await.len()
    }
}
