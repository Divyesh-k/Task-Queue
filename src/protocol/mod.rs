//! # Protocol Module
//!
//! Custom binary TCP framing layer.
//!
//! ## Why a custom protocol instead of HTTP?
//!
//! HTTP has a lot of overhead: text headers, status lines, keep-alive
//! negotiation, etc. For an internal task queue where we control both
//! ends, a lean binary protocol is:
//!   - Faster to encode/decode (no text parsing)
//!   - Smaller on the wire (7-byte header vs. 200+ byte HTTP header)
//!   - Easier to reason about alignment and framing
//!
//! ## Module layout
//!
//! ```text
//! frame.rs   → Frame struct + MessageType enum + wire constants
//! codec.rs   → Encoder/Decoder (convert BytesMut ↔ Frame)
//! message.rs → Typed message structs + Message enum
//! ```

pub mod codec;
pub mod frame;
pub mod message;

pub use codec::TaskQueueCodec;
pub use frame::{Frame, MessageType, MAGIC};
pub use message::Message;
