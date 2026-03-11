mod broker;
mod error;
mod metrics;
mod protocol;
mod queue;
mod task;
mod wal;
mod worker;

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::broker::{Broker, BrokerConfig};
use crate::error::WalResult;
use crate::metrics::{MetricsCounters, MetricsServer};
use crate::protocol::{
    message::{Message, SubmitMsg},
    TaskQueueCodec,
};
use crate::queue::TaskQueue;
use crate::wal::Wal;

const WAL_PATH: &str = "./data/wal.log";
const BROKER_ADDR: &str = "127.0.0.1:7777";
const METRICS_ADDR: &str = "127.0.0.1:8888";

#[tokio::main]
async fn main() -> WalResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Starting task queue — WAL + Queue + Workers + TCP Broker + Metrics");

    let token = CancellationToken::new();
    let counters = MetricsCounters::new();

    let wal = Wal::open(WAL_PATH, 10).await?;
    let queue = TaskQueue::new(wal, 100).await?;

    {
        let q = queue.clone();
        let c = counters.clone();
        let t = token.clone();
        tokio::spawn(async move {
            MetricsServer::start(METRICS_ADDR, q, c, t).await;
        });
    }

    let broker = Broker::new(
        queue.clone(),
        BrokerConfig {
            listen_addr: BROKER_ADDR.into(),
            worker_count: 4,
            heartbeat_interval_secs: 2,
            heartbeat_timeout_secs: 8,
            base_delay_ms: 100,
            simulated_failure_rate: 0.25,
        },
    );

    let broker_token = token.clone();
    tokio::spawn(async move {
        if let Err(e) = broker.run(broker_token).await {
            tracing::error!(error = %e, "Broker error");
        }
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    info!("Submitting 8 tasks...");

    let stream = TcpStream::connect(BROKER_ADDR).await?;
    let mut framed = Framed::new(stream, TaskQueueCodec);

    let tasks = vec![
        ("generate-pdf", 10u8, 3u32),
        ("send-newsletter", 8, 2),
        ("transcode-video", 9, 3),
        ("process-payment", 7, 3),
        ("update-index", 5, 1),
        ("archive-logs", 3, 0),
        ("warm-cache", 2, 2),
        ("ping-healthcheck", 1, 0),
    ];

    for (name, priority, max_retries) in &tasks {
        let frame = Message::Submit(SubmitMsg {
            payload: name.as_bytes().to_vec(),
            priority: *priority,
            max_retries: *max_retries,
        })
        .into_frame()
        .unwrap();

        framed.send(frame).await.unwrap();

        match framed.next().await {
            Some(Ok(reply)) => match Message::from_frame(&reply).unwrap() {
                Message::Ack(ack) => {
                    counters.inc_submitted();
                    info!(task = name, priority, task_id = %ack.task_id, "Accepted");
                }
                Message::Reject(rej) => {
                    info!(task = name, reason = %rej.reason, "Rejected");
                }
                _ => {}
            },
            _ => {}
        }
    }

    info!("All tasks submitted — workers processing");

    tokio::time::sleep(Duration::from_millis(200)).await;
    scrape_metrics(METRICS_ADDR).await;

    tokio::time::sleep(Duration::from_secs(4)).await;
    scrape_metrics(METRICS_ADDR).await;

    info!("Shutting down...");
    token.cancel();
    tokio::time::sleep(Duration::from_millis(800)).await;

    info!("Done");
    Ok(())
}

async fn scrape_metrics(addr: &str) {
    let url = format!("http://{}/metrics", addr);

    let mut stream = match TcpStream::connect(addr).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "Could not connect to metrics endpoint");
            return;
        }
    };

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let request = format!(
        "GET /metrics HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        addr
    );

    if stream.write_all(request.as_bytes()).await.is_err() {
        return;
    }

    let mut response = String::new();
    let _ = stream.read_to_string(&mut response).await;

    if let Some(body_start) = response.find("\r\n\r\n") {
        let body = &response[body_start + 4..];
        info!("Metrics at {}:\n{}", url, body);
    }
}
