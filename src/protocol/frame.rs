use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Magic bytes at the start of every frame. Acts as a sync marker.
pub const MAGIC: u16 = 0xDEAD;

/// Fixed header size: 2 (magic) + 1 (type) + 4 (length).
pub const HEADER_LEN: usize = 7;

/// Maximum accepted payload size — prevents memory exhaustion from bad clients.
pub const MAX_PAYLOAD: u32 = 64 * 1024;

/// Identifies the type of message a frame carries.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    Submit = 0x01,
    Ack = 0x02,
    Reject = 0x03,
    Heartbeat = 0x04,
    Complete = 0x05,
    Fail = 0x06,
}

impl TryFrom<u8> for MessageType {
    type Error = FrameError;

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

/// A fully decoded protocol frame.
#[derive(Debug, Clone)]
pub struct Frame {
    pub message_type: MessageType,
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

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("Bad magic bytes: expected 0xDEAD, got {0:#06x}")]
    BadMagic(u16),

    #[error("Unknown message type byte: {0:#04x}")]
    UnknownType(u8),

    #[error("Payload too large: {0} bytes (max {MAX_PAYLOAD})")]
    PayloadTooLarge(u32),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
