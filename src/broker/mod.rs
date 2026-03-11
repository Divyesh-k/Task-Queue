pub mod connection;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::error::WalResult;
use crate::queue::TaskQueue;
use crate::worker::{Pool, WorkerConfig};

use connection::handle_connection;

#[derive(Clone)]
pub struct BrokerConfig {
    pub listen_addr: String,
    pub worker_count: usize,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_timeout_secs: u64,
    pub base_delay_ms: u64,
    /// Probability a simulated task fails (0.0 = never, 1.0 = always).
    pub simulated_failure_rate: f64,
}

pub struct Broker {
    config: BrokerConfig,
    queue: TaskQueue,
}

impl Broker {
    pub fn new(queue: TaskQueue, config: BrokerConfig) -> Self {
        Self { config, queue }
    }

    pub async fn run(self, token: CancellationToken) -> WalResult<()> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        info!(addr = %self.config.listen_addr, "Broker listening");

        let worker_config = WorkerConfig {
            count: self.config.worker_count,
            heartbeat_interval_secs: self.config.heartbeat_interval_secs,
            heartbeat_timeout_secs: self.config.heartbeat_timeout_secs,
            base_delay_ms: self.config.base_delay_ms,
            simulated_failure_rate: self.config.simulated_failure_rate,
        };
        let pool = Pool::start(self.queue.clone(), worker_config).await;
        info!("Worker pool started ({} workers)", self.config.worker_count);

        loop {
            tokio::select! {
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
                            error!(error = %e, "Accept error — continuing");
                        }
                    }
                }

                _ = token.cancelled() => {
                    info!("Broker shutting down");
                    break;
                }
            }
        }

        pool.shutdown().await;
        info!("Broker shut down");

        Ok(())
    }
}
