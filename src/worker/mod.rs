pub mod heartbeat;
pub mod retry;

use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::queue::TaskQueue;

use heartbeat::HeartbeatTracker;

#[derive(Clone)]
pub struct WorkerConfig {
    pub count: usize,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_timeout_secs: u64,
    pub base_delay_ms: u64,
    /// Simulated failure rate for demos (0.0 = never fail, 1.0 = always fail).
    pub simulated_failure_rate: f64,
}

pub struct Pool {
    pub token: CancellationToken,
    pub heartbeat: HeartbeatTracker,
}

impl Pool {
    pub async fn start(queue: TaskQueue, config: WorkerConfig) -> Self {
        let token = CancellationToken::new();
        let heartbeat = HeartbeatTracker::new(config.heartbeat_timeout_secs);

        for i in 0..config.count {
            let worker_id = format!("worker-{}", i);
            let q = queue.clone();
            let tok = token.clone();
            let hb = heartbeat.clone();
            let cfg = config.clone();

            hb.register(&worker_id).await;

            tokio::spawn(async move {
                run_worker(worker_id, q, hb, tok, cfg).await;
            });
        }

        info!(workers = config.count, "Worker pool started");

        {
            let hb = heartbeat.clone();
            let q = queue.clone();
            let tok = token.clone();
            let base_delay_ms = config.base_delay_ms;

            tokio::spawn(async move {
                run_monitor(q, hb, tok, base_delay_ms).await;
            });
        }

        Self { token, heartbeat }
    }

    pub async fn shutdown(self) {
        info!("Stopping worker pool");
        self.token.cancel();
        tokio::time::sleep(Duration::from_millis(500)).await;
        info!("Worker pool stopped");
    }
}

async fn run_worker(
    worker_id: String,
    queue: TaskQueue,
    heartbeat: HeartbeatTracker,
    token: CancellationToken,
    config: WorkerConfig,
) {
    info!(worker_id, "Worker started");

    let hb_interval = Duration::from_secs(config.heartbeat_interval_secs);

    loop {
        if token.is_cancelled() {
            info!(worker_id, "Worker stopping");
            break;
        }

        let task = match queue.pop().await {
            Ok(Some(t)) => t,
            Ok(None) => {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                    _ = token.cancelled() => { break; }
                }
                continue;
            }
            Err(e) => {
                error!(worker_id, error = %e, "Queue pop error");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let task_id = task.id;
        info!(
            worker_id,
            task_id  = %task_id,
            priority = task.priority,
            attempt  = task.attempt,
            "Executing task"
        );

        heartbeat.beat(&worker_id, Some(task_id)).await;

        let success = simulate_execution(
            &worker_id,
            task_id,
            config.simulated_failure_rate,
            hb_interval,
            &heartbeat,
            &token,
        )
        .await;

        if success {
            info!(worker_id, task_id = %task_id, "Task succeeded");
            if let Err(e) = queue.complete(task_id).await {
                error!(worker_id, task_id = %task_id, error = %e, "Failed to mark task complete");
            }
        } else {
            warn!(worker_id, task_id = %task_id, "Task failed");
            if let Err(e) = queue.fail(task_id, config.base_delay_ms).await {
                error!(worker_id, task_id = %task_id, error = %e, "Failed to record failure");
            }
        }

        heartbeat.beat(&worker_id, None).await;
    }

    heartbeat.deregister(&worker_id).await;
    info!(worker_id, "Worker exited");
}

async fn simulate_execution(
    worker_id: &str,
    task_id: Uuid,
    failure_rate: f64,
    _hb_interval: Duration,
    heartbeat: &HeartbeatTracker,
    token: &CancellationToken,
) -> bool {
    // Compute random values before any await — ThreadRng is !Send.
    let (half, will_fail): (Duration, bool) = {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let work_ms = rng.gen_range(50u64..=500);
        let roll: f64 = rng.gen();
        (Duration::from_millis(work_ms / 2), roll < failure_rate)
    };

    tokio::select! {
        _ = tokio::time::sleep(half) => {}
        _ = token.cancelled() => { return false; }
    }

    heartbeat.beat(worker_id, Some(task_id)).await;

    tokio::select! {
        _ = tokio::time::sleep(half) => {}
        _ = token.cancelled() => { return false; }
    }

    !will_fail
}

async fn run_monitor(
    queue: TaskQueue,
    heartbeat: HeartbeatTracker,
    token: CancellationToken,
    base_delay_ms: u64,
) {
    info!("Heartbeat monitor started");

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = token.cancelled() => {
                info!("Heartbeat monitor exiting");
                break;
            }
        }

        let dead = heartbeat.check_timeouts().await;

        for (dead_worker_id, maybe_task_id) in dead {
            warn!(worker_id = %dead_worker_id, "Worker declared dead");

            if let Some(task_id) = maybe_task_id {
                warn!(
                    worker_id = %dead_worker_id,
                    task_id   = %task_id,
                    "Re-failing in-flight task from dead worker"
                );
                if let Err(e) = queue.fail(task_id, base_delay_ms).await {
                    error!(task_id = %task_id, error = %e, "Could not re-fail task");
                }
            }

            heartbeat.deregister(&dead_worker_id).await;
        }
    }
}

pub async fn run_single_worker(
    worker_id: String,
    queue: TaskQueue,
    heartbeat: HeartbeatTracker,
    token: CancellationToken,
    config: WorkerConfig,
) {
    run_worker(worker_id, queue, heartbeat, token, config).await;
}
