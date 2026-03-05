//! Task-Queue — entry point.
//!
//! Current demo: Broker Module — full live TCP server + client
//!
//! Run:  cargo run
//!
//! What you'll see:
//!   1. Broker binds on 127.0.0.1:7777
//!   2. Worker pool starts (4 workers)
//!   3. A simulated client connects over TCP and sends 5 Submit frames
//!   4. Broker replies with Ack (task accepted) or Reject (queue full)
//!   5. Workers pick up tasks, execute them, complete/fail as usual
//!   6. After 4 seconds, graceful shutdown

mod broker;
mod error;
mod protocol;
mod queue;
mod task;
mod wal;
mod worker;

use std::time::Duration;

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::broker::{Broker, BrokerConfig};
use crate::error::WalResult;
use crate::protocol::{
    message::{Message, SubmitMsg},
    TaskQueueCodec,
};
use crate::queue::TaskQueue;
use crate::wal::Wal;

const WAL_PATH: &str = "./data/wal.log";
const BROKER_ADDR: &str = "127.0.0.1:7777";

#[tokio::main]
async fn main() -> WalResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("=== Task Queue — Broker Module Demo ===\n");

    // ── Build WAL + Queue ────────────────────────────────────────────────────
    let wal = Wal::open(WAL_PATH, 10).await?;
    let queue = TaskQueue::new(wal, 100).await?;

    // ── Build Broker ─────────────────────────────────────────────────────────
    let config = BrokerConfig {
        listen_addr: BROKER_ADDR.into(),
        worker_count: 4,
        heartbeat_interval_secs: 2,
        heartbeat_timeout_secs: 8,
        base_delay_ms: 100,
        simulated_failure_rate: 0.20, // 20% tasks randomly fail → retry
    };

    let broker = Broker::new(queue.clone(), config);
    let token = CancellationToken::new();

    // ── Spawn broker in the background ───────────────────────────────────────
    // The broker runs its accept loop in a separate task so our main()
    // can continue to connect a client.
    let broker_token = token.clone();
    tokio::spawn(async move {
        if let Err(e) = broker.run(broker_token).await {
            tracing::error!(error = %e, "Broker error");
        }
    });

    // Give the broker a moment to bind and start listening.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ─────────────────────────────────────────────────────────────────────────
    // SIMULATED CLIENT
    //
    // In a real system this would be an actual client application running on
    // a different machine. Here we simulate it in the same process so you can
    // see the full round-trip without needing a separate terminal.
    // ─────────────────────────────────────────────────────────────────────────
    info!("--- Simulated client connecting to {} ---\n", BROKER_ADDR);

    let stream = TcpStream::connect(BROKER_ADDR).await?;
    let mut framed = Framed::new(stream, TaskQueueCodec);

    // Send 5 Submit messages with varying priorities
    let tasks_to_submit = vec![
        ("render-report", 9u8, 3u32),
        ("send-email", 5, 2),
        ("resize-image", 7, 3),
        ("sync-database", 3, 1),
        ("clean-cache", 1, 0),
    ];

    info!("Submitting {} tasks...\n", tasks_to_submit.len());

    for (name, priority, max_retries) in tasks_to_submit {
        // Build and send the Submit frame
        let submit_msg = Message::Submit(SubmitMsg {
            payload: name.as_bytes().to_vec(),
            priority,
            max_retries,
        });

        let frame = submit_msg.into_frame().expect("encode failed");
        framed.send(frame).await.expect("send failed");

        // Wait for the broker's reply (Ack or Reject)
        match framed.next().await {
            Some(Ok(reply_frame)) => {
                match Message::from_frame(&reply_frame).expect("decode reply") {
                    Message::Ack(ack) => {
                        info!(
                            task = name, priority, max_retries,
                            task_id = %ack.task_id,
                            "  ✅ Broker replied: Ack"
                        );
                    }
                    Message::Reject(rej) => {
                        info!(
                            task = name,
                            reason = %rej.reason,
                            "  🚫 Broker replied: Reject"
                        );
                    }
                    other => tracing::warn!("Unexpected reply: {:?}", other),
                }
            }
            Some(Err(e)) => tracing::error!(error = %e, "Frame decode error"),
            None => tracing::warn!("Connection closed by broker"),
        }
    }

    info!("\n--- All tasks submitted. Workers are processing... ---");

    // Let workers run for a few seconds so you can see processing output
    tokio::time::sleep(Duration::from_secs(4)).await;

    // ─────────────────────────────────────────────────────────────────────────
    // GRACEFUL SHUTDOWN
    // ─────────────────────────────────────────────────────────────────────────
    info!("\n--- Graceful shutdown ---");
    token.cancel();

    // Give the shutdown a moment to propagate
    tokio::time::sleep(Duration::from_millis(800)).await;

    info!("\n=== Broker Module Demo Complete ===");
    info!("The full system is now working end-to-end:");
    info!("  Client → TCP → Broker → Queue (WAL-backed) → Worker Pool → Done");

    Ok(())
}
