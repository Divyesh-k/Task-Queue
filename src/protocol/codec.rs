//! # Codec — BytesMut ↔ Frame converter
//!
//! This is the bridge between raw TCP bytes and typed [`Frame`]s.
//!
//! ## How tokio-util codecs work
//!
//! When you wrap a `TcpStream` in `FramedRead<TcpStream, TaskQueueCodec>`,
//! tokio feeds incoming bytes into `decode()` every time new data arrives.
//! The codec either:
//!   - Returns `Ok(None)`       → not enough bytes yet, wait for more
//!   - Returns `Ok(Some(frame))`→ one complete frame decoded, hand it up
//!   - Returns `Err(e)`         → bad magic / unknown type → close connection
//!
//! On the write side `FramedWrite<TcpStream, TaskQueueCodec>` calls
//! `encode()` for each `Frame` you send, and handles buffering for you.
//!
//! ## Partial reads — the most important correctness property
//!
//! TCP can fragment data arbitrarily. A 100-byte frame might arrive as:
//!   - 3 bytes, then 97 bytes
//!   - 100 separate 1-byte reads
//!   - All 100 bytes at once
//!
//! The decoder must handle every case. The trick is:
//!   1. Check we have `>= HEADER_LEN` bytes (7). If not → return None.
//!   2. Peek at Length (don't advance cursor yet).
//!   3. Check we have `>= HEADER_LEN + Length` bytes. If not → return None.
//!   4. Only then advance the cursor and pull the frame out.

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::frame::{Frame, FrameError, MessageType, HEADER_LEN, MAGIC, MAX_PAYLOAD};

/// The codec that implements the task-queue binary protocol.
///
/// Wrap a `TcpStream` with this to get a stream/sink of [`Frame`]s:
/// ```text
/// let framed = Framed::new(tcp_stream, TaskQueueCodec);
/// // framed is now a Stream<Item=Frame> + Sink<Frame>
/// ```
pub struct TaskQueueCodec;

// ─────────────────────────────────────────────────────────────
// Decoder — bytes → Frame
// ─────────────────────────────────────────────────────────────

impl Decoder for TaskQueueCodec {
    type Item = Frame;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        // ── Guard 1: Do we have a full header? ────────────────
        // We need at least 7 bytes before we can read anything.
        // Tell BytesMut to reserve space so the next read fills it faster.
        if src.len() < HEADER_LEN {
            src.reserve(HEADER_LEN - src.len());
            return Ok(None); // come back when more bytes arrive
        }

        // ── Peek at magic (bytes 0-1) ─────────────────────────
        // We use indexing (not `src.get_u16()`) because we're not
        // advancing the cursor yet — we first need to verify everything
        // before committing to consuming any bytes.
        let magic = u16::from_be_bytes([src[0], src[1]]);
        if magic != MAGIC {
            return Err(FrameError::BadMagic(magic));
        }

        // ── Peek at type (byte 2) ──────────────────────────────
        let type_byte = src[2];

        // ── Peek at payload length (bytes 3-6) ────────────────
        let payload_len = u32::from_be_bytes([src[3], src[4], src[5], src[6]]);

        if payload_len > MAX_PAYLOAD {
            return Err(FrameError::PayloadTooLarge(payload_len));
        }

        let total_len = HEADER_LEN + payload_len as usize;

        // ── Guard 2: Do we have the full payload yet? ──────────
        if src.len() < total_len {
            // Tell BytesMut how many more bytes we need.
            src.reserve(total_len - src.len());
            return Ok(None); // come back when more bytes arrive
        }

        // ── All bytes present — now consume them ───────────────
        // `advance()` moves the cursor forward WITHOUT copying.
        src.advance(HEADER_LEN); // skip past magic + type + length

        // `split_to()` splits the BytesMut at `payload_len` bytes,
        // returning the payload half and leaving the rest in `src`.
        let payload = src.split_to(payload_len as usize).to_vec();

        // Parse the type byte (returns Err if unknown)
        let message_type = MessageType::try_from(type_byte)?;

        Ok(Some(Frame::new(message_type, payload)))
    }
}

// ─────────────────────────────────────────────────────────────
// Encoder — Frame → bytes
// ─────────────────────────────────────────────────────────────

impl Encoder<Frame> for TaskQueueCodec {
    type Error = FrameError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), FrameError> {
        let payload_len = frame.payload.len() as u32;

        if payload_len > MAX_PAYLOAD {
            return Err(FrameError::PayloadTooLarge(payload_len));
        }

        // Pre-allocate exactly the right number of bytes.
        // This avoids repeated reallocation as we push bytes in.
        dst.reserve(HEADER_LEN + payload_len as usize);

        // Write magic (2 bytes, big-endian)
        dst.put_u16(MAGIC);
        // Write message type (1 byte)
        dst.put_u8(frame.message_type as u8);
        // Write payload length (4 bytes, big-endian)
        dst.put_u32(payload_len);
        // Write payload bytes
        dst.extend_from_slice(&frame.payload);

        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────
// Unit tests — round-trip every message type
// ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::MessageType;

    /// Encode a frame then decode it back — must get the same frame.
    fn round_trip(msg_type: MessageType, payload: &[u8]) -> Frame {
        // Encode
        let original = Frame::new(msg_type, payload.to_vec());
        let mut buf = BytesMut::new();
        TaskQueueCodec
            .encode(original, &mut buf)
            .expect("encode should not fail");

        // Decode
        TaskQueueCodec
            .decode(&mut buf)
            .expect("decode should not fail")
            .expect("should produce a frame")
    }

    #[test]
    fn round_trip_submit() {
        let payload = br#"{"payload":[],"priority":5,"max_retries":3}"#;
        let frame = round_trip(MessageType::Submit, payload);
        assert_eq!(frame.message_type, MessageType::Submit);
        assert_eq!(frame.payload, payload);
    }

    #[test]
    fn round_trip_ack() {
        let payload = br#"{"task_id":"550e8400-e29b-41d4-a716-446655440000"}"#;
        let frame = round_trip(MessageType::Ack, payload);
        assert_eq!(frame.message_type, MessageType::Ack);
    }

    #[test]
    fn round_trip_all_types() {
        let types = [
            MessageType::Submit,
            MessageType::Ack,
            MessageType::Reject,
            MessageType::Heartbeat,
            MessageType::Complete,
            MessageType::Fail,
        ];
        for t in types {
            let f = round_trip(t, b"{}");
            assert_eq!(f.message_type, t);
        }
    }

    #[test]
    fn partial_header_returns_none() {
        // Only 3 bytes — not enough for a full 7-byte header
        let mut buf = BytesMut::from(&[0xDE, 0xAD, 0x01][..]);
        let result = TaskQueueCodec.decode(&mut buf).expect("should not error");
        assert!(result.is_none(), "should wait for more bytes");
    }

    #[test]
    fn bad_magic_returns_error() {
        // Magic is 0xCAFE instead of 0xDEAD
        let mut buf = BytesMut::from(&[0xCA, 0xFE, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00][..]);
        let result = TaskQueueCodec.decode(&mut buf);
        assert!(matches!(result, Err(FrameError::BadMagic(0xCAFE))));
    }

    #[test]
    fn payload_too_large_returns_error() {
        // Claim a payload of MAX_PAYLOAD + 1
        let too_big = (MAX_PAYLOAD + 1).to_be_bytes();
        let mut buf = BytesMut::from(
            &[
                0xDE, 0xAD, 0x01, too_big[0], too_big[1], too_big[2], too_big[3],
            ][..],
        );
        let result = TaskQueueCodec.decode(&mut buf);
        assert!(matches!(result, Err(FrameError::PayloadTooLarge(_))));
    }
}
