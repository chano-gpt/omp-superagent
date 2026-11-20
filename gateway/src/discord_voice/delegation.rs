use anyhow::{Result, bail, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

use super::Event;

const MAX_RESPONSES: usize = 12;

#[derive(Default)]
pub(super) struct Responses {
    count: usize,
    active: Option<Turn>,
    continuation: Option<Continuation>,
    known: HashMap<String, String>,
    seen_calls: HashSet<String>,
}

struct Turn {
    id: String,
    delegation: Option<String>,
    continuation: bool,
    call: Option<Call>,
}

struct Call {
    id: String,
    text: Option<String>,
}

struct Continuation {
    delegation: Option<String>,
    call_id: String,
    valid_request: bool,
    result_sent: bool,
}

impl Responses {
    pub(super) fn delegation(&mut self, delegation: &Value) -> Result<()> {
        if delegation["target"] != "responses" || delegation["response_id"].is_null() {
            return Ok(());
        }
        let id = required_id(&delegation["response_id"])?;
        let value = required_id(&delegation["id"])?;
        let active = self.active.as_mut().filter(|turn| turn.id == id);
        let known = self
            .known
            .get(id)
            .map(String::as_str)
            .or_else(|| active.as_ref().and_then(|turn| turn.delegation.as_deref()));
        ensure!(
            known.is_none_or(|old| old == value),
            "Conflicting Live delegation"
        );
        ensure!(
            self.known.contains_key(id) || self.known.len() < MAX_RESPONSES,
            "OpenAI Live delegation budget reached"
        );
        self.known.insert(id.to_owned(), value.to_owned());
        if let Some(turn) = active
            && turn.delegation.is_none()
        {
            turn.delegation = Some(value.to_owned());
        }
        Ok(())
    }

    pub(super) fn event(&mut self, envelope: &Value) -> Result<Event> {
        let event = &envelope["event"];
        let id = event["response"]["id"].as_str();
        let explicit = envelope["delegation_id"].as_str();
        ensure!(
            explicit.is_none_or(valid_id),
            "Invalid Live delegation identifier"
        );
        let delegation =
            explicit.or_else(|| id.and_then(|id| self.known.get(id).map(String::as_str)));
        match event["type"].as_str() {
            Some("response.created") => {
                let id = required_id(&event["response"]["id"])?;
                if self.active.as_ref().is_some_and(|turn| turn.id == id) {
                    return Ok(Event::None);
                }
                ensure!(
                    self.active.is_none() && self.count < MAX_RESPONSES,
                    "Overlapping OpenAI Live responses or budget reached"
                );
                let previous = self.continuation.take();
                let expected = previous.as_ref().and_then(|old| old.delegation.as_deref());
                ensure!(
                    previous.as_ref().is_none_or(|old| old.result_sent)
                        && compatible(delegation, expected),
                    "Uncorrelated OpenAI Live continuation"
                );
                self.count += 1;
                self.active = Some(Turn {
                    id: id.to_owned(),
                    delegation: delegation.or(expected).map(str::to_owned),
                    continuation: previous.is_some(),
                    call: None,
                });
            }
            Some("response.output_item.done") => {
                let item = &event["item"];
                if item["type"] != "function_call"
                    || (!item["status"].is_null() && item["status"] != "completed")
                {
                    return Ok(Event::None);
                }
                let id = required_id(&item["call_id"])?;
                if self.seen_calls.contains(id) {
                    return Ok(Event::None);
                }
                let Some(turn) = self.active.as_mut() else {
                    bail!("Unexpected OpenAI Live function call");
                };
                ensure!(
                    !turn.continuation
                        && turn.call.is_none()
                        && compatible(delegation, turn.delegation.as_deref())
                        && event["response_id"].as_str().is_none_or(|id| id == turn.id),
                    "Uncorrelated OpenAI Live function call"
                );
                if turn.delegation.is_none() {
                    turn.delegation = delegation.map(str::to_owned);
                }
                self.seen_calls.insert(id.to_owned());
                turn.call = Some(Call {
                    id: id.to_owned(),
                    text: request(item),
                });
            }
            Some("response.completed" | "response.cancelled") => {
                if !self
                    .active
                    .as_ref()
                    .is_some_and(|turn| Some(turn.id.as_str()) == id)
                {
                    return Ok(Event::None);
                }
                let Some(turn) = self.active.take() else {
                    return Ok(Event::None);
                };
                ensure!(
                    compatible(delegation, turn.delegation.as_deref()),
                    "Conflicting Live response delegation"
                );
                ensure!(
                    self.count < MAX_RESPONSES,
                    "OpenAI Live response budget reached"
                );
                if event["type"] == "response.completed"
                    && let Some(call) = turn.call
                {
                    ensure!(
                        self.count + 1 < MAX_RESPONSES,
                        "OpenAI Live response budget reached"
                    );
                    self.continuation = Some(Continuation {
                        delegation: turn.delegation.or_else(|| delegation.map(str::to_owned)),
                        call_id: call.id.clone(),
                        valid_request: call.text.is_some(),
                        result_sent: false,
                    });
                    return Ok(Event::Draft {
                        id: call.id,
                        text: call.text,
                    });
                }
            }
            Some("response.failed" | "response.incomplete" | "error") => {
                bail!("OpenAI Live delegation failed or was incomplete")
            }
            Some(_) => {}
            None => bail!("Invalid OpenAI Live delegated response"),
        }
        Ok(Event::None)
    }

    pub(super) fn result(&mut self, id: &str) -> Result<bool> {
        let continuation = self
            .continuation
            .as_mut()
            .filter(|old| old.call_id == id && !old.result_sent)
            .ok_or_else(|| anyhow::anyhow!("Unexpected OpenAI Live draft result"))?;
        continuation.result_sent = true;
        Ok(continuation.valid_request)
    }
}

fn compatible(first: Option<&str>, second: Option<&str>) -> bool {
    first
        .zip(second)
        .is_none_or(|(first, second)| first == second)
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 256
}

fn required_id(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|id| valid_id(id))
        .ok_or_else(|| anyhow::anyhow!("Invalid Live response identifier"))
}

fn request(item: &Value) -> Option<String> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Args {
        text: String,
    }
    if item["name"] != "forward_to_head" {
        return None;
    }
    let arguments = item["arguments"]
        .as_str()
        .filter(|args| args.len() <= 8_192)?;
    let request: Args = serde_json::from_str(arguments).ok()?;
    let text = request.text.trim();
    (!text.is_empty() && text.encode_utf16().count() <= 1_000).then(|| text.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn delegation_never_publishes_partial_cancelled_or_continuation_work() {
        let mut state = Responses::default();
        let one = |kind, id| json!({"delegation_id":"delegation","event":{"type":kind,"response":{"id":id}}});
        let call = |response, id| {
            json!({"delegation_id":"delegation","event":{
                "type":"response.output_item.done", "response_id":response,
                "item":{"type":"function_call","call_id":id,"name":"forward_to_head","arguments":"{\"text\":\"Check shipping\"}"}
            }})
        };
        state.event(&one("response.created", "cancelled")).unwrap();
        assert!(matches!(
            state.event(&call("cancelled", "old")).unwrap(),
            Event::None
        ));
        assert!(matches!(
            state
                .event(&one("response.cancelled", "cancelled"))
                .unwrap(),
            Event::None
        ));
        assert!(state.result("old").is_err());

        state.event(&one("response.created", "spoken")).unwrap();
        state.event(&call("spoken", "draft")).unwrap();
        assert!(
            matches!(state.event(&one("response.completed", "spoken")).unwrap(),
            Event::Draft { id, text: Some(text) } if id == "draft" && text == "Check shipping")
        );
        assert!(state.result("another").is_err());
        assert!(state.result("draft").unwrap());
        state
            .event(&one("response.created", "continuation"))
            .unwrap();
        assert!(state.event(&call("continuation", "attempt")).is_err());
    }
}
