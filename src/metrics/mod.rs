use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::queue::TaskQueue;

pub struct MetricsCounters {
    pub submitted: AtomicU64,
    pub completed: AtomicU64,
    pub failed: AtomicU64,
    pub dead_lettered: AtomicU64,
    started_at: Instant,
}

impl MetricsCounters {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            dead_lettered: AtomicU64::new(0),
            started_at: Instant::now(),
        })
    }

    pub fn inc_submitted(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_completed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_dead_lettered(&self) {
        self.dead_lettered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }
}

pub struct MetricsServer;

impl MetricsServer {
    pub async fn start(
        addr: &str,
        queue: TaskQueue,
        counters: Arc<MetricsCounters>,
        token: CancellationToken,
    ) {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => {
                info!(addr, "Metrics server listening");
                l
            }
            Err(e) => {
                error!(addr, error = %e, "Failed to bind metrics server");
                return;
            }
        };

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((mut stream, peer)) => {
                            debug!(peer = %peer, "Metrics request");

                            let q        = queue.clone();
                            let counters = counters.clone();

                            tokio::spawn(async move {
                                if let Err(e) = handle_request(&mut stream, &q, &counters).await {
                                    warn!(error = %e, "Metrics request error");
                                }
                            });
                        }
                        Err(e) => warn!(error = %e, "Metrics accept error"),
                    }
                }
                _ = token.cancelled() => {
                    info!("Metrics server shutting down");
                    break;
                }
            }
        }
    }
}

async fn handle_request(
    stream: &mut tokio::net::TcpStream,
    queue: &TaskQueue,
    counters: &MetricsCounters,
) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let request = std::str::from_utf8(&buf[..n]).unwrap_or("");

    let first_line = request.lines().next().unwrap_or("");
    let path = first_line.split_whitespace().nth(1).unwrap_or("/");

    if path == "/metrics" {
        let body = build_metrics_body(queue, counters).await;

        let response = format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: text/plain; version=0.0.4\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {}",
            body.len(),
            body
        );

        stream.write_all(response.as_bytes()).await?;
    } else {
        let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        stream.write_all(response.as_bytes()).await?;
    }

    Ok(())
}

async fn build_metrics_body(queue: &TaskQueue, counters: &MetricsCounters) -> String {
    let pending = queue.pending_count().await;
    let dead_letter = queue.dead_letter_count().await;

    let submitted = counters.submitted.load(Ordering::Relaxed);
    let completed = counters.completed.load(Ordering::Relaxed);
    let failed = counters.failed.load(Ordering::Relaxed);
    let dead_lettered = counters.dead_lettered.load(Ordering::Relaxed);
    let uptime = counters.uptime_secs();

    format!(
        "# HELP task_queue_pending Number of tasks waiting for a worker\n\
         # TYPE task_queue_pending gauge\n\
         task_queue_pending {pending}\n\
         \n\
         # HELP task_queue_dead_letter Number of tasks in the Dead Letter Queue\n\
         # TYPE task_queue_dead_letter gauge\n\
         task_queue_dead_letter {dead_letter}\n\
         \n\
         # HELP task_queue_submitted_total Total tasks ever submitted\n\
         # TYPE task_queue_submitted_total counter\n\
         task_queue_submitted_total {submitted}\n\
         \n\
         # HELP task_queue_completed_total Total tasks completed successfully\n\
         # TYPE task_queue_completed_total counter\n\
         task_queue_completed_total {completed}\n\
         \n\
         # HELP task_queue_failed_total Total task failure events (including retries)\n\
         # TYPE task_queue_failed_total counter\n\
         task_queue_failed_total {failed}\n\
         \n\
         # HELP task_queue_dead_lettered_total Total tasks moved to Dead Letter Queue\n\
         # TYPE task_queue_dead_lettered_total counter\n\
         task_queue_dead_lettered_total {dead_lettered}\n\
         \n\
         # HELP task_queue_uptime_seconds Seconds since broker started\n\
         # TYPE task_queue_uptime_seconds counter\n\
         task_queue_uptime_seconds {uptime}\n"
    )
}
