use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::adapter::EffectError;

use super::Inner;
use super::media::TEXT_LIMIT;
use super::wire::{Inbound, Route, Ts, js_len};

#[derive(Deserialize)]
struct History {
    messages: Vec<Value>,
    has_more: Option<bool>,
    is_limited: Option<bool>,
    response_metadata: Option<Metadata>,
}

#[derive(Deserialize)]
struct Metadata {
    next_cursor: Option<String>,
}

impl Inner {
    pub(super) async fn history_message(
        &self,
        route: &Route,
        ts: &Ts,
    ) -> Result<Value, EffectError> {
        let mut args = json!({
            "channel": route.channel_id, "oldest": ts, "latest": ts, "inclusive": true,
            "limit": 1, "include_all_metadata": true,
        });
        let thread = route.thread_id.as_ref().filter(|thread| *thread != ts);
        let method = if let Some(root) = thread {
            args["ts"] = json!(root);
            args["limit"] = json!(2);
            "conversations.replies"
        } else {
            "conversations.history"
        };
        let history: History = self.api.call(method, args).await?;
        history
            .messages
            .into_iter()
            .find(|message| {
                message["ts"] == ts.as_str()
                    && thread.is_none_or(|root| message["thread_ts"] == root.as_str())
            })
            .ok_or_else(|| {
                EffectError::rejected(
                    "Slack stored message is not accessible in its original conversation",
                )
            })
    }

    pub(super) async fn fetch_after(
        &self,
        route: Route,
        source: Ts,
    ) -> Result<Vec<Inbound>, EffectError> {
        self.destination(&route).await?;
        let mut cursor: Option<String> = None;
        let mut oldest = source.clone();
        let mut latest: Option<Ts> = None;
        let mut seen_cursors = HashSet::new();
        let mut results = Vec::new();
        let mut seen_inputs = HashMap::new();
        loop {
            let mut args = json!({ "channel": route.channel_id, "oldest": oldest, "inclusive": false, "limit": 100 });
            if let Some(value) = &latest {
                args["latest"] = json!(value);
            }
            if let Some(value) = &cursor {
                args["cursor"] = json!(value);
            }
            let method = if let Some(root) = &route.thread_id {
                args["ts"] = json!(root);
                "conversations.replies"
            } else {
                "conversations.history"
            };
            let page: History = self.api.call(method, args).await?;
            if page.is_limited == Some(true) {
                return Err(EffectError::rejected(
                    "Slack history is incomplete or retention-limited",
                ));
            }
            let mut low: Option<Ts> = None;
            let mut high: Option<Ts> = None;
            for message in &page.messages {
                let timestamp = message["ts"]
                    .as_str()
                    .and_then(Ts::parse)
                    .ok_or_else(|| EffectError::rejected("Slack returned malformed history"))?;
                if low
                    .as_ref()
                    .is_none_or(|low| timestamp.compare(low).is_lt())
                {
                    low = Some(timestamp.clone());
                }
                if high
                    .as_ref()
                    .is_none_or(|high| timestamp.compare(high).is_gt())
                {
                    high = Some(timestamp.clone());
                }
                if timestamp.compare(&source).is_gt()
                    && let Some(input) = self.normalize(message, route.clone())?
                    && input.route.thread_id == route.thread_id
                {
                    if let Some(index) = seen_inputs.get(&input.id).copied() {
                        results[index] = (timestamp, input);
                    } else {
                        seen_inputs.insert(input.id.clone(), results.len());
                        results.push((timestamp, input));
                    }
                }
            }
            let next = page
                .response_metadata
                .and_then(|meta| meta.next_cursor)
                .map(|cursor| cursor.trim().to_owned())
                .filter(|cursor| !cursor.is_empty());
            if let Some(cursor_id) = next {
                if !seen_cursors.insert(cursor_id.clone()) {
                    return Err(EffectError::rejected(
                        "Slack history pagination cursor did not advance",
                    ));
                }
                cursor = Some(cursor_id);
                continue;
            }
            cursor = None;
            if page.has_more != Some(true) {
                break;
            }
            if route.thread_id.is_some() {
                oldest = high
                    .filter(|high| high.compare(&oldest).is_gt())
                    .ok_or_else(|| {
                        EffectError::rejected("Slack reply pagination did not advance")
                    })?;
            } else {
                latest = Some(
                    low.filter(|low| {
                        low.compare(&source).is_gt()
                            && latest
                                .as_ref()
                                .is_none_or(|latest| low.compare(latest) == Ordering::Less)
                    })
                    .ok_or_else(|| {
                        EffectError::rejected("Slack history pagination did not advance")
                    })?,
                );
            }
        }
        results.sort_by(|(left, _), (right, _)| left.compare(right));
        Ok(results.into_iter().map(|(_, input)| input).collect())
    }

    pub(super) async fn create_thread(
        &self,
        route: Route,
        title: &str,
        source: Option<Ts>,
    ) -> Result<Route, EffectError> {
        self.destination(&route).await?;
        if route.thread_id.is_some() {
            return Err(EffectError::rejected(
                "Slack cannot nest conversation threads",
            ));
        }
        if title.trim().is_empty() || js_len(title) > TEXT_LIMIT {
            return Err(EffectError::rejected(
                "Slack thread title must contain 1–3000 characters",
            ));
        }
        let root = match source {
            Some(ts) => {
                let source = self.history_message(&route, &ts).await?;
                if !self.owner_message(&source)
                    || source
                        .get("thread_ts")
                        .is_some_and(|root| root != ts.as_str())
                {
                    return Err(EffectError::rejected(
                        "Slack thread source is not an authorized owner root message",
                    ));
                }
                ts
            }
            None => self.post(&route, title, None).await?,
        };
        Ok(Route {
            thread_id: Some(root),
            ..route.physical()
        })
    }
}
