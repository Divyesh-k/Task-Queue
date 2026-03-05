//! Task-Queue — entry point.
//!
//! Current demo: Protocol Module
//! Shows the full encode → wire bytes → decode round-trip for every message type.
//!
//! Run:  cargo run
//! Run tests: cargo test  (codec unit tests run too)

mod error;
mod protocol;
mod queue;
mod task;
mod wal;
mod worker;

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};
use tracing::info;

use crate::error::WalResult;
use crate::protocol::{
    frame::MessageType,
    message::{AckMsg, CompleteMsg, FailMsg, HeartbeatMsg, Message, RejectMsg, SubmitMsg},
    TaskQueueCodec,
};

#[tokio::main]
async fn main() -> WalResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("=== Task Queue — Protocol Module Demo ===\n");

    // ─────────────────────────────────────────────────────────────────────────
    // The messages we'll encode
    // Each variant represents one direction of communication in the system.
    // ─────────────────────────────────────────────────────────────────────────
    let messages = vec![
        // Client → Broker: submit a new task
        Message::Submit(SubmitMsg {
            payload: b"process-invoice-42".to_vec(),
            priority: 8,
            max_retries: 3,
        }),
        // Broker → Client: task accepted
        Message::Ack(AckMsg {
            task_id: "550e8400-e29b-41d4-a716-446655440000".into(),
        }),
        // Broker → Client: queue full, rejected
        Message::Reject(RejectMsg {
            reason: "Queue capacity exceeded (10000 tasks)".into(),
        }),
        // Worker → Broker: heartbeat ping
        Message::Heartbeat(HeartbeatMsg {
            worker_id: "worker-2".into(),
        }),
        // Worker → Broker: task done
        Message::Complete(CompleteMsg {
            worker_id: "worker-2".into(),
            task_id: "550e8400-e29b-41d4-a716-446655440000".into(),
        }),
        // Worker → Broker: task failed
        Message::Fail(FailMsg {
            worker_id: "worker-2".into(),
            task_id: "550e8400-e29b-41d4-a716-446655440000".into(),
            error: "Connection refused to downstream service".into(),
        }),
    ];

    info!("--- Round-trip demo for all 6 message types ---\n");

    for msg in messages {
        let label = format!("{:?}", msg_type_of(&msg));

        // ── Step 1: Message → Frame ──────────────────────────────────
        // `into_frame()` JSON-encodes the inner struct → payload bytes,
        // wraps in Frame { message_type, payload }.
        let frame = msg.clone().into_frame().expect("into_frame failed");

        // ── Step 2: Frame → wire bytes ───────────────────────────────
        // The codec writes: [0xDE 0xAD][type byte][length u32][payload]
        let mut wire_buf = BytesMut::new();
        TaskQueueCodec
            .encode(frame.clone(), &mut wire_buf)
            .expect("encode failed");

        // ── Step 3: Show the wire bytes ──────────────────────────────
        // Print as hex so you can see exactly what travels on the TCP socket.
        let hex: Vec<String> = wire_buf.iter().map(|b| format!("{:02X}", b)).collect();

        info!("Message type : {}", label);
        info!("Payload JSON : {}", String::from_utf8_lossy(&frame.payload));
        info!("Wire bytes   : {} bytes total", wire_buf.len());
        let header_hex = hex[..7.min(hex.len())].join(" ");
        let suffix = if hex.len() > 7 {
            format!(" ... ({} payload bytes)", frame.payload.len())
        } else {
            String::new()
        };
        info!("Wire hex     : {}{}", header_hex, suffix);

        // ── Step 4: Decode wire bytes back → Frame ───────────────────
        // This is what the RECEIVER does — reads bytes off TCP, decodes.
        let mut decode_buf = wire_buf.clone();
        let decoded_frame = TaskQueueCodec
            .decode(&mut decode_buf)
            .expect("decode must not error")
            .expect("must produce a frame");

        // ── Step 5: Frame → typed Message ────────────────────────────
        let decoded_msg = Message::from_frame(&decoded_frame).expect("from_frame failed");

        // ── Verify roundtrip ─────────────────────────────────────────
        assert_eq!(
            decoded_frame.message_type, frame.message_type,
            "message type must survive round-trip"
        );
        assert_eq!(
            decoded_frame.payload, frame.payload,
            "payload must survive round-trip"
        );

        info!("Decoded type : {:?}", decoded_frame.message_type);
        info!("Round-trip   : ✅ OK\n");

        let _ = decoded_msg; // used to prove decoding succeeded
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Demo: partial frame simulation
    //
    // Show that the decoder correctly returns None when data is incomplete,
    // and only returns Some(frame) once all bytes have arrived.
    // ─────────────────────────────────────────────────────────────────────────
    info!("--- Partial frame simulation ---");
    info!("(Simulates TCP delivering bytes in two chunks)\n");

    // Encode a HeartbeatMsg into a full wire buffer
    let hb_frame = Message::Heartbeat(HeartbeatMsg {
        worker_id: "worker-0".into(),
    })
    .into_frame()
    .unwrap();
    let mut full_buf = BytesMut::new();
    TaskQueueCodec.encode(hb_frame, &mut full_buf).unwrap();
    let total_bytes = full_buf.len();

    // Simulate receiving only the first 4 bytes (less than 7-byte header)
    let mut partial = BytesMut::from(&full_buf[..4]);
    let result = TaskQueueCodec.decode(&mut partial).unwrap();
    info!(
        "4 bytes received → decoder returned: {:?}",
        result.map(|_| "Some(Frame)").unwrap_or("None")
    );

    // Now simulate receiving all bytes
    let mut complete = full_buf;
    let result = TaskQueueCodec.decode(&mut complete).unwrap();
    info!(
        "All {} bytes received → decoder returned: {}\n",
        total_bytes,
        result.map(|_| "Some(Frame) ✅").unwrap_or("None")
    );

    info!("=== Protocol Module Demo Complete ===");
    info!("Run 'cargo test' to also run the 5 codec unit tests.");

    Ok(())
}

/// Helper to get the MessageType from a Message for display.
fn msg_type_of(msg: &Message) -> MessageType {
    match msg {
        Message::Submit(_) => MessageType::Submit,
        Message::Ack(_) => MessageType::Ack,
        Message::Reject(_) => MessageType::Reject,
        Message::Heartbeat(_) => MessageType::Heartbeat,
        Message::Complete(_) => MessageType::Complete,
        Message::Fail(_) => MessageType::Fail,
    }
}
