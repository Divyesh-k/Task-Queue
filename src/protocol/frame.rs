//! # Frame — the wire format
//!
//! Every message exchanged between clients and the broker is wrapped in
//! a Frame. The binary layout on the TCP stream is:
//!
//! ```text
//! Byte offset  Size    Field
//! ──────────────────────────────────────────────────────────
//!      0       2 B     Magic  = 0xDEAD  (sync marker)
//!      2       1 B     Type   (MessageType as u8)
//!      3       4 B     Length (payload size, big-endian u32)
//!      7       Length  Payload (JSON-encoded message body)
//! ──────────────────────────────────────────────────────────
//!  Total header = 7 bytes fixed, then variable payload
//! ```
//!
//! ## Why magic bytes?
//!
//! TCP is a *stream* protocol — data arrives as a river of bytes, not
//! discrete packets. If a connection hiccups or a client sends garbage,
//! we might lose our place in the stream. The magic `0xDEAD` gives us
//! a **sync marker**: if the decoder ever reads something that isn't
//! `0xDEAD` at the expected position, it knows immediately that
//! something is wrong and can close the connection safely.
//!
//! ## Why big-endian for Length?
//!
//! Network byte order is always big-endian (most significant byte first).
//! This is a convention from TCP/IP itself — every field wider than a byte
//! in a custom protocol should be big-endian for consistency.

use serde::{Deserialize, Serialize};
use thiserror::Error;

// ─────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────

/// Every frame starts with these two magic bytes.
/// `0xDE 0xAD` — memorable, unlikely to appear randomly.
pub const MAGIC: u16 = 0xDEAD;

/// Fixed header size in bytes: 2 (magic) + 1 (type) + 4 (length).
pub const HEADER_LEN: usize = 7;

/// Maximum payload we'll accept — 64 KiB.
/// Prevents a misbehaving client from sending a 4 GB "payload"
/// and exhausting the broker's memory before we even read it.
pub const MAX_PAYLOAD: u32 = 64 * 1024;

// ─────────────────────────────────────────────────────────────
// MessageType — the 1-byte opcode
// ─────────────────────────────────────────────────────────────

/// Identifies which kind of message a Frame carries.
///
/// Each variant is assigned a fixed byte value (`#[repr(u8)]`).
/// This byte is what actually travels on the wire.
///
/// We use explicit byte values (0x01…0x06) rather than letting
/// the compiler pick numbers — if we ever add variants in the middle
/// or reorder them, existing clients won't break.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    /// Client → Broker: "Please process this task."
    Submit = 0x01,
    /// Broker → Client: "Task accepted, here is its UUID."
    Ack = 0x02,
    /// Broker → Client: "Queue full, I cannot accept your task right now."
    Reject = 0x03,
    /// Worker → Broker: "I am alive and healthy." (periodic ping)
    Heartbeat = 0x04,
    /// Worker → Broker: "The task finished successfully."
    Complete = 0x05,
    /// Worker → Broker: "The task failed, please trigger retry logic."
    Fail = 0x06,
}

impl TryFrom<u8> for MessageType {
    type Error = FrameError;

    /// Convert a raw byte from the wire into a `MessageType`.
    /// Returns `FrameError::UnknownType` if the byte is not one we know.
    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            0x01 => Ok(MessageType::Submit),
            0x02 => Ok(MessageType::Ack),
            0x03 => Ok(MessageType::Reject),
            0x04 => Ok(MessageType::Heartbeat),
            0x05 => Ok(MessageType::Complete),
            0x06 => Ok(MessageType::Fail),
            other => Err(FrameError::UnknownType(other)),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Frame — one decoded message
// ─────────────────────────────────────────────────────────────

/// A fully decoded protocol frame.
///
/// After the codec strips the Magic + Length header, what remains is:
/// - `message_type` — which operation this is
/// - `payload`      — raw JSON bytes; parse with [`Message::from_frame`]
#[derive(Debug, Clone)]
pub struct Frame {
    pub message_type: MessageType,
    /// Raw JSON-encoded payload bytes.
    /// Call [`crate::protocol::Message::from_frame`] to get a typed value.
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(message_type: MessageType, payload: Vec<u8>) -> Self {
        Self {
            message_type,
            payload,
        }
    }
}

// ─────────────────────────────────────────────────────────────
// FrameError — everything that can go wrong at the frame level
// ─────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum FrameError {
    /// The first two bytes were not `0xDEAD`.
    /// Almost certainly means we're out of sync with the stream.
    #[error("Bad magic bytes: expected 0xDEAD, got {0:#06x}")]
    BadMagic(u16),

    /// The type byte is not one of the 6 defined opcodes.
    #[error("Unknown message type byte: {0:#04x}")]
    UnknownType(u8),

    /// The client claimed a payload larger than MAX_PAYLOAD.
    /// We refuse to allocate that much memory — close the connection.
    #[error("Payload too large: {0} bytes (max {MAX_PAYLOAD})")]
    PayloadTooLarge(u32),

    /// Any underlying I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
