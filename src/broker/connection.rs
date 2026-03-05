//! # Connection Handler
//!
//! Manages the full lifecycle of one client TCP connection.
//!
//! ## What happens per connection
//!
//! ```text
//! TcpStream
//!   └── Framed<TcpStream, TaskQueueCodec>
//!         ↓ Each frame received:
//!         │
//!         ├── Submit   → queue.push(payload, priority, max_retries)
//!         │               OK  → send Ack { task_id }
//!         │               Err → send Reject { reason }
//!         │
//!         ├── Complete → queue.complete(task_id)
//!         │               (no reply needed — fire and forget)
//!         │
//!         ├── Fail     → queue.fail(task_id)
//!         │               (no reply needed)
//!         │
//!         └── Heartbeat → log "worker X is alive" (no reply)
//!
//! Connection ends when:
//!   - Client closes the socket (stream ends)
//!   - Framing error (bad magic, unknown type, oversized payload)
//!   - Cancellation token fires (broker shutdown)
//! ```

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::AppError;
use crate::protocol::{
    message::{AckMsg, Message, RejectMsg},
    TaskQueueCodec,
};
use crate::queue::TaskQueue;

/// Handle one client TCP connection from start to finish.
///
/// # Why `Framed<TcpStream, TaskQueueCodec>`?
///
/// A raw `TcpStream` gives you bytes. `Framed` wraps it with our
/// `TaskQueueCodec` to give you a `Stream<Item=Frame>` for reads
/// and a `Sink<Frame>` for writes. No manual byte handling needed —
/// the codec takes care of magic validation, length-prefix, etc.
pub async fn handle_connection(
    stream: TcpStream,
    queue: TaskQueue,
    token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Wrap the TCP stream with our binary codec.
    // `framed` is now both a Stream (for reading) and Sink (for writing).
    let mut framed = Framed::new(stream, TaskQueueCodec);

    loop {
        tokio::select! {
            // ── Read next frame from client ──────────────────────
            maybe_frame = framed.next() => {
                match maybe_frame {
                    None => {
                        // Client closed the connection gracefully.
                        debug!("Client EOF — connection closed");
                        break;
                    }

                    Some(Err(e)) => {
                        // Framing error (bad magic, unknown type, oversized).
                        // Log it and close the connection — do NOT crash the server.
                        warn!(error = %e, "Frame decode error — closing connection");
                        break;
                    }

                    Some(Ok(frame)) => {
                        // Decode the raw frame into a typed Message
                        let msg = match Message::from_frame(&frame) {
                            Ok(m) => m,
                            Err(e) => {
                                warn!(error = %e, "Failed to decode message payload");
                                break;
                            }
                        };

                        // Dispatch to the right handler based on message type
                        handle_message(msg, &mut framed, &queue).await;
                    }
                }
            }

            // ── Broker shutdown signal ───────────────────────────
            _ = token.cancelled() => {
                info!("Connection handler received shutdown — closing this connection");
                break;
            }
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────
// handle_message — dispatch one typed message
// ─────────────────────────────────────────────────────────────

/// Route a decoded message to the correct queue operation.
///
/// Takes a mutable reference to the Framed so it can send replies.
/// (Only Submit needs a reply — Ack or Reject.)
async fn handle_message(
    msg: Message,
    framed: &mut Framed<TcpStream, TaskQueueCodec>,
    queue: &TaskQueue,
) {
    match msg {
        // ── Submit: client wants to enqueue a task ───────────────────
        //
        // Flow:
        //   1. Call queue.push() → WAL write first, then memory
        //   2. Success → send Ack { task_id } back to client
        //   3. QueueFull → send Reject { reason } back to client
        //   4. Other error → log it, send Reject
        Message::Submit(submit) => {
            info!(
                priority = submit.priority,
                max_retries = submit.max_retries,
                payload_len = submit.payload.len(),
                "Received Submit"
            );

            match queue
                .push(submit.payload, submit.priority, submit.max_retries)
                .await
            {
                Ok(task_id) => {
                    info!(task_id = %task_id, "Task accepted → sending Ack");
                    let reply = Message::Ack(AckMsg {
                        task_id: task_id.to_string(),
                    });
                    send_reply(framed, reply).await;
                }

                Err(AppError::QueueFull { capacity }) => {
                    warn!(capacity, "Queue full → sending Reject");
                    let reply = Message::Reject(RejectMsg {
                        reason: format!("Queue full (capacity {})", capacity),
                    });
                    send_reply(framed, reply).await;
                }

                Err(e) => {
                    error!(error = %e, "Unexpected error on push → sending Reject");
                    let reply = Message::Reject(RejectMsg {
                        reason: format!("Internal error: {}", e),
                    });
                    send_reply(framed, reply).await;
                }
            }
        }

        // ── Complete: worker reports task finished ───────────────────
        //
        // Workers send this over their own TCP connection.
        // No reply is needed — the worker just moves to queue.pop() again.
        Message::Complete(c) => match Uuid::parse_str(&c.task_id) {
            Ok(id) => {
                info!(worker = %c.worker_id, task_id = %id, "Task completed");
                if let Err(e) = queue.complete(id).await {
                    error!(error = %e, "Failed to mark task complete");
                }
            }
            Err(e) => warn!(raw = %c.task_id, error = %e, "Invalid task_id in Complete"),
        },

        // ── Fail: worker reports task failure ────────────────────────
        //
        // Queue decides retry vs. DLQ based on attempt count.
        Message::Fail(f) => match Uuid::parse_str(&f.task_id) {
            Ok(id) => {
                warn!(
                    worker   = %f.worker_id,
                    task_id  = %id,
                    error    = %f.error,
                    "Task failed"
                );
                if let Err(e) = queue.fail(id, 100).await {
                    error!(error = %e, "Failed to record task failure");
                }
            }
            Err(e) => warn!(raw = %f.task_id, error = %e, "Invalid task_id in Fail"),
        },

        // ── Heartbeat: worker liveness ping ─────────────────────────
        //
        // In the full system this updates the HeartbeatTracker.
        // Here we just log it — the tracker lives inside the Worker pool.
        Message::Heartbeat(h) => {
            debug!(worker_id = %h.worker_id, "Heartbeat received");
        }

        // ── Ack / Reject are broker→client only ──────────────────────
        // A client should never send these to the broker.
        Message::Ack(_) | Message::Reject(_) => {
            warn!("Received unexpected Ack/Reject from client — ignoring");
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

/// Encode a Message into a Frame and send it over the Framed sink.
///
/// Errors are logged but not propagated — a reply failure shouldn't
/// crash the entire connection handler.
async fn send_reply(framed: &mut Framed<TcpStream, TaskQueueCodec>, msg: Message) {
    match msg.into_frame() {
        Ok(frame) => {
            if let Err(e) = framed.send(frame).await {
                warn!(error = %e, "Failed to send reply to client");
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to encode reply frame");
        }
    }
}
