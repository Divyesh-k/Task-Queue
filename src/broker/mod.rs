//! # Broker Module
//!
//! The TCP server — the central orchestrator that connects:
//!
//! ```text
//!                       ┌─────────────────────────────────────┐
//!  Client TCP ──────────┤  Broker                             │
//!  Client TCP ──────────┤   ├── per-connection handler        │
//!  Client TCP ──────────┤   │    ├── reads Frame from socket  │
//!                       │   │    ├── calls queue.push()       │
//!                       │   │    └── sends Ack / Reject back  │
//!                       │   │                                 │
//!                       │   └── Worker Pool (N workers)       │
//!                       │        ├── worker-0: queue.pop() → execute │
//!                       │        ├── worker-1: queue.pop() → execute │
//!                       │        └── worker-N: queue.pop() → execute │
//!                       └─────────────────────────────────────┘
//! ```
//!
//! ## Lifecycle
//!
//! 1. `Broker::run()` → bind TCP socket, spawn worker pool
//! 2. Accept loop → for each new connection → `tokio::spawn(handle_connection(...))`
//! 3. Each connection handler → reads frames in a loop → pushes to queue → replies
//! 4. Ctrl+C signal → cancel token fires → workers drain → server exits

pub mod connection;

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::error::WalResult;
use crate::queue::TaskQueue;
use crate::worker::{Pool, WorkerConfig};

use connection::handle_connection;

// ─────────────────────────────────────────────────────────────
// BrokerConfig
// ─────────────────────────────────────────────────────────────

/// All the settings the broker needs at startup.
#[derive(Clone)]
pub struct BrokerConfig {
    /// Full address to listen on, e.g. "127.0.0.1:7777"
    pub listen_addr: String,
    /// How many worker tasks to spawn.
    pub worker_count: usize,
    /// Worker heartbeat interval in seconds.
    pub heartbeat_interval_secs: u64,
    /// Worker heartbeat timeout in seconds.
    pub heartbeat_timeout_secs: u64,
    /// Base retry backoff in milliseconds.
    pub base_delay_ms: u64,
    /// Probability a simulated task fails (0.0 = never, 1.0 = always).
    pub simulated_failure_rate: f64,
}

// ─────────────────────────────────────────────────────────────
// Broker
// ─────────────────────────────────────────────────────────────

pub struct Broker {
    config: BrokerConfig,
    queue: TaskQueue,
}

impl Broker {
    pub fn new(queue: TaskQueue, config: BrokerConfig) -> Self {
        Self { config, queue }
    }

    /// Bind, spawn workers, and run the accept loop.
    ///
    /// ## What happens here
    ///
    /// 1. Bind a TCP listener to `config.listen_addr`.
    /// 2. Start the worker pool (N workers + heartbeat monitor).
    /// 3. Accept connections in a loop:
    ///    - Each accepted `TcpStream` gets its own `tokio::spawn` task.
    ///    - The spawned task calls `handle_connection()`.
    ///    - If the accept loop itself errors, log and continue.
    /// 4. When the cancellation token fires (Ctrl+C), break the loop
    ///    and call `pool.shutdown()`.
    pub async fn run(self, token: CancellationToken) -> WalResult<()> {
        // ── 1. Bind listener ──────────────────────────────────
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        info!(addr = %self.config.listen_addr, "Broker listening");

        // ── 2. Start worker pool ──────────────────────────────
        let worker_config = WorkerConfig {
            count: self.config.worker_count,
            heartbeat_interval_secs: self.config.heartbeat_interval_secs,
            heartbeat_timeout_secs: self.config.heartbeat_timeout_secs,
            base_delay_ms: self.config.base_delay_ms,
            simulated_failure_rate: self.config.simulated_failure_rate,
        };
        let pool = Pool::start(self.queue.clone(), worker_config).await;
        info!("Worker pool started ({} workers)", self.config.worker_count);

        // ── 3. Accept loop ────────────────────────────────────
        loop {
            tokio::select! {
                // New connection arrived
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            info!(peer = %peer_addr, "Client connected");

                            let q = self.queue.clone();
                            let tok = token.clone();

                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, q, tok).await {
                                    warn!(peer = %peer_addr, error = %e, "Connection closed with error");
                                }
                                info!(peer = %peer_addr, "Client disconnected");
                            });
                        }
                        Err(e) => {
                            // A single accept failure (e.g. out of file descriptors)
                            // should not kill the entire server.
                            error!(error = %e, "Accept error — continuing");
                        }
                    }
                }

                // Shutdown signal
                _ = token.cancelled() => {
                    info!("Broker received shutdown signal — stopping accept loop");
                    break;
                }
            }
        }

        // ── 4. Graceful shutdown ──────────────────────────────
        info!("Waiting for workers to finish...");
        pool.shutdown().await;
        info!("Broker shut down cleanly");

        Ok(())
    }
}
