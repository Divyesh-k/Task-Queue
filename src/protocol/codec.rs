use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::frame::{Frame, FrameError, MessageType, HEADER_LEN, MAGIC, MAX_PAYLOAD};

pub struct TaskQueueCodec;

impl Decoder for TaskQueueCodec {
    type Item = Frame;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        if src.len() < HEADER_LEN {
            src.reserve(HEADER_LEN - src.len());
            return Ok(None);
        }

        let magic = u16::from_be_bytes([src[0], src[1]]);
        if magic != MAGIC {
            return Err(FrameError::BadMagic(magic));
        }

        let type_byte = src[2];
        let payload_len = u32::from_be_bytes([src[3], src[4], src[5], src[6]]);

        if payload_len > MAX_PAYLOAD {
            return Err(FrameError::PayloadTooLarge(payload_len));
        }

        let total_len = HEADER_LEN + payload_len as usize;

        if src.len() < total_len {
            src.reserve(total_len - src.len());
            return Ok(None);
        }

        src.advance(HEADER_LEN);
        let payload = src.split_to(payload_len as usize).to_vec();
        let message_type = MessageType::try_from(type_byte)?;

        Ok(Some(Frame::new(message_type, payload)))
    }
}

impl Encoder<Frame> for TaskQueueCodec {
    type Error = FrameError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), FrameError> {
        let payload_len = frame.payload.len() as u32;

        if payload_len > MAX_PAYLOAD {
            return Err(FrameError::PayloadTooLarge(payload_len));
        }

        dst.reserve(HEADER_LEN + payload_len as usize);

        dst.put_u16(MAGIC);
        dst.put_u8(frame.message_type as u8);
        dst.put_u32(payload_len);
        dst.extend_from_slice(&frame.payload);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::MessageType;

    fn round_trip(msg_type: MessageType, payload: &[u8]) -> Frame {
        let original = Frame::new(msg_type, payload.to_vec());
        let mut buf = BytesMut::new();
        TaskQueueCodec
            .encode(original, &mut buf)
            .expect("encode should not fail");

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
        let mut buf = BytesMut::from(&[0xDE, 0xAD, 0x01][..]);
        let result = TaskQueueCodec.decode(&mut buf).expect("should not error");
        assert!(result.is_none(), "should wait for more bytes");
    }

    #[test]
    fn bad_magic_returns_error() {
        let mut buf = BytesMut::from(&[0xCA, 0xFE, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00][..]);
        let result = TaskQueueCodec.decode(&mut buf);
        assert!(matches!(result, Err(FrameError::BadMagic(0xCAFE))));
    }

    #[test]
    fn payload_too_large_returns_error() {
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
