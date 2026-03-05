//! # Typed Messages
//!
//! Sits one layer above [`Frame`]. A Frame just holds raw JSON bytes;
//! this module turns those bytes into real Rust structs you can work with.
//!
//! ## The two-layer design
//!
//! ```text
//! Layer 2 (this file): Message enum / typed structs
//!          ↕  serde_json::to_vec / from_slice
//! Layer 1  (frame.rs): Frame { message_type, payload: Vec<u8> }
//!          ↕  Encoder / Decoder (codec.rs)
//! Layer 0  (TCP):      raw bytes on the wire
//! ```
//!
//! You almost never work with raw bytes or Frame directly — you call
//! `Message::from_frame()` to decode and `Message::into_frame()` to encode.

use serde::{Deserialize, Serialize};

use super::frame::{Frame, FrameError, MessageType};

// ─────────────────────────────────────────────────────────────
// Individual message structs
// ─────────────────────────────────────────────────────────────

// ── Client → Broker ──────────────────────────────────────────

/// Sent by a client to submit a new task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitMsg {
    /// The task data — opaque bytes (base64 when serialised to JSON).
    pub payload: Vec<u8>,
    /// Priority 0–255: higher value = processed first.
    pub priority: u8,
    /// How many times to retry on failure before dead-lettering.
    pub max_retries: u32,
}

// ── Broker → Client ──────────────────────────────────────────

/// Sent by the broker when a task is accepted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckMsg {
    /// The UUID the broker assigned to this task.
    pub task_id: String,
}

/// Sent by the broker when the queue is full.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectMsg {
    /// Human-readable reason (e.g. "Queue capacity exceeded").
    pub reason: String,
}

// ── Worker ↔ Broker ──────────────────────────────────────────

/// Periodic liveness ping from a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMsg {
    /// Which worker is pinging (e.g. "worker-0").
    pub worker_id: String,
}

/// Worker reports successful task completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteMsg {
    pub worker_id: String,
    pub task_id: String,
}

/// Worker reports task failure — broker triggers retry/DLQ logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailMsg {
    pub worker_id: String,
    pub task_id: String,
    /// Human-readable description of what went wrong.
    pub error: String,
}

// ─────────────────────────────────────────────────────────────
// Message — the unified enum
// ─────────────────────────────────────────────────────────────

/// All possible typed messages in the protocol.
///
/// Use [`Message::from_frame`] to decode a raw [`Frame`] into one of these.
/// Use [`Message::into_frame`] to encode one of these into a [`Frame`]
/// ready to hand to the codec for transmission.
#[derive(Debug, Clone)]
pub enum Message {
    Submit(SubmitMsg),
    Ack(AckMsg),
    Reject(RejectMsg),
    Heartbeat(HeartbeatMsg),
    Complete(CompleteMsg),
    Fail(FailMsg),
}

impl Message {
    /// Decode a [`Frame`]'s payload bytes into a typed `Message`.
    ///
    /// # Errors
    /// Returns `FrameError::Io` if the JSON payload can't be parsed.
    pub fn from_frame(frame: &Frame) -> Result<Self, FrameError> {
        // Helper closure to turn a serde_json error into a FrameError.
        let parse = |e: serde_json::Error| {
            FrameError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        };

        let msg = match frame.message_type {
            MessageType::Submit => {
                Message::Submit(serde_json::from_slice(&frame.payload).map_err(parse)?)
            }
            MessageType::Ack => {
                Message::Ack(serde_json::from_slice(&frame.payload).map_err(parse)?)
            }
            MessageType::Reject => {
                Message::Reject(serde_json::from_slice(&frame.payload).map_err(parse)?)
            }
            MessageType::Heartbeat => {
                Message::Heartbeat(serde_json::from_slice(&frame.payload).map_err(parse)?)
            }
            MessageType::Complete => {
                Message::Complete(serde_json::from_slice(&frame.payload).map_err(parse)?)
            }
            MessageType::Fail => {
                Message::Fail(serde_json::from_slice(&frame.payload).map_err(parse)?)
            }
        };

        Ok(msg)
    }

    /// Encode a typed `Message` into a [`Frame`] ready for the codec.
    ///
    /// # How it works
    /// 1. Match on the variant to get the message type byte.
    /// 2. `serde_json::to_vec()` the inner struct → payload bytes.
    /// 3. Wrap in `Frame::new(type, payload)`.
    pub fn into_frame(self) -> Result<Frame, FrameError> {
        let encode = |e: serde_json::Error| {
            FrameError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        };

        let (msg_type, payload) = match self {
            Message::Submit(m) => (MessageType::Submit, serde_json::to_vec(&m).map_err(encode)?),
            Message::Ack(m) => (MessageType::Ack, serde_json::to_vec(&m).map_err(encode)?),
            Message::Reject(m) => (MessageType::Reject, serde_json::to_vec(&m).map_err(encode)?),
            Message::Heartbeat(m) => (
                MessageType::Heartbeat,
                serde_json::to_vec(&m).map_err(encode)?,
            ),
            Message::Complete(m) => (
                MessageType::Complete,
                serde_json::to_vec(&m).map_err(encode)?,
            ),
            Message::Fail(m) => (MessageType::Fail, serde_json::to_vec(&m).map_err(encode)?),
        };

        Ok(Frame::new(msg_type, payload))
    }
}
