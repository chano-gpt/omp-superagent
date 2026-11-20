use std::cell::OnceCell;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::transport::{Channel, Packet, Result};

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
pub(crate) enum MessageUpdates {
    #[default]
    #[serde(rename = "full")]
    Full,
    #[serde(rename = "activity-v1")]
    Activity,
}

/// Activity is a private runtime contract, not stock delta mode. The runtime
/// needs a heartbeat for incremental updates, but reads content from terminal
/// messages. Replace known incremental assistant deltas one for one and leave
/// all other event, receipt and response shapes intact.
///
/// Managed opt-in never changes upstream OMP's stream or the observation
/// queue. Only the owner-to-opted-in-peer frames become smaller.
pub(crate) struct EventFanout<'a> {
    full: &'a Packet,
    incremental: bool,
    activity: OnceCell<Result<Packet>>,
}

impl<'a> EventFanout<'a> {
    pub(crate) fn new(full: &'a Packet) -> Self {
        let frame = full.frame.as_ref();
        let incremental = frame["type"] == "message_update"
            && matches!(
                frame["assistantMessageEvent"]["type"].as_str(),
                Some("text_delta" | "thinking_delta" | "toolcall_delta")
            );
        Self {
            full,
            incremental,
            activity: OnceCell::new(),
        }
    }

    pub(crate) fn send(&self, peer: &Channel) -> Result<()> {
        let packet =
            if self.incremental && matches!(peer.message_updates()?, MessageUpdates::Activity) {
                self.activity
                    .get_or_init(|| {
                        let mut frame = json!({"type": "host_message_update_activity"});
                        for key in ["messageId", "id"] {
                            if let Some(value @ Value::String(_)) = self.full.frame.get(key) {
                                frame[key] = value.clone();
                            }
                        }
                        Packet::new(frame)
                    })
                    .as_ref()
                    .map_err(|&error| error)?
            } else {
                self.full
            };
        peer.send_packet(packet.clone())
    }
}
