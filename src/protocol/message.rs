use serde::{Deserialize, Serialize};

use super::frame::{Frame, FrameError, MessageType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitMsg {
    pub payload: Vec<u8>,
    pub priority: u8,
    pub max_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckMsg {
    pub task_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectMsg {
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMsg {
    pub worker_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteMsg {
    pub worker_id: String,
    pub task_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailMsg {
    pub worker_id: String,
    pub task_id: String,
    pub error: String,
}

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
    pub fn from_frame(frame: &Frame) -> Result<Self, FrameError> {
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
