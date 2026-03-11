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

pub async fn handle_connection(
    stream: TcpStream,
    queue: TaskQueue,
    token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut framed = Framed::new(stream, TaskQueueCodec);

    loop {
        tokio::select! {
            maybe_frame = framed.next() => {
                match maybe_frame {
                    None => {
                        debug!("Client disconnected");
                        break;
                    }

                    Some(Err(e)) => {
                        warn!(error = %e, "Frame decode error — closing connection");
                        break;
                    }

                    Some(Ok(frame)) => {
                        let msg = match Message::from_frame(&frame) {
                            Ok(m) => m,
                            Err(e) => {
                                warn!(error = %e, "Failed to decode message");
                                break;
                            }
                        };

                        handle_message(msg, &mut framed, &queue).await;
                    }
                }
            }

            _ = token.cancelled() => {
                info!("Connection shutting down");
                break;
            }
        }
    }

    Ok(())
}

async fn handle_message(
    msg: Message,
    framed: &mut Framed<TcpStream, TaskQueueCodec>,
    queue: &TaskQueue,
) {
    match msg {
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
                    info!(task_id = %task_id, "Task accepted");
                    let reply = Message::Ack(AckMsg {
                        task_id: task_id.to_string(),
                    });
                    send_reply(framed, reply).await;
                }

                Err(AppError::QueueFull { capacity }) => {
                    warn!(capacity, "Queue full");
                    let reply = Message::Reject(RejectMsg {
                        reason: format!("Queue full (capacity {})", capacity),
                    });
                    send_reply(framed, reply).await;
                }

                Err(e) => {
                    error!(error = %e, "Push error");
                    let reply = Message::Reject(RejectMsg {
                        reason: format!("Internal error: {}", e),
                    });
                    send_reply(framed, reply).await;
                }
            }
        }

        Message::Complete(c) => match Uuid::parse_str(&c.task_id) {
            Ok(id) => {
                info!(worker = %c.worker_id, task_id = %id, "Task completed");
                if let Err(e) = queue.complete(id).await {
                    error!(error = %e, "Failed to mark task complete");
                }
            }
            Err(e) => warn!(raw = %c.task_id, error = %e, "Invalid task_id in Complete"),
        },

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

        Message::Heartbeat(h) => {
            debug!(worker_id = %h.worker_id, "Heartbeat received");
        }

        Message::Ack(_) | Message::Reject(_) => {
            warn!("Received unexpected Ack/Reject from client — ignoring");
        }
    }
}

async fn send_reply(framed: &mut Framed<TcpStream, TaskQueueCodec>, msg: Message) {
    match msg.into_frame() {
        Ok(frame) => {
            if let Err(e) = framed.send(frame).await {
                warn!(error = %e, "Failed to send reply");
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to encode reply frame");
        }
    }
}
