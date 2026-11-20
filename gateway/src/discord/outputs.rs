use std::time::Duration;

use reqwest::{Method, RequestBuilder, Response, Url, multipart};
use serde::Deserialize;
use serde_json::{Value, json};

use super::rest::{FILE_LIMIT, Rest, broken, no_mentions};
use super::{
    Effect, EffectError, State, markdown,
    model::{RemoteMessage, Route, hash, snowflake, utf16},
};

const MESSAGE_LIMIT: usize = 2_000;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Output {
    id: String,
    route: Route,
    text: String,
    reply_to: Option<MessageReference>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MessageReference {
    route: Route,
    message_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Activity {
    Received,
    Thinking,
    Idle,
}

#[derive(Deserialize)]
pub(super) struct Attachment {
    id: String,
    url: String,
    size: u64,
}

pub(super) fn nonce(id: &str, route: &Route) -> String {
    hash(&json!([id, route.key(), 0]))
}

pub(super) fn attachment_url(value: &str) -> Option<Url> {
    let url = Url::parse(value).ok()?;
    if url.scheme() != "https"
        || !matches!(
            url.host_str(),
            Some("cdn.discordapp.com" | "media.discordapp.net")
        )
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.fragment().is_some()
        || url.path().to_ascii_lowercase().contains("%2f")
        || url.path().to_ascii_lowercase().contains("%5c")
    {
        return None;
    }
    let path = url.path_segments()?.collect::<Vec<_>>();
    matches!(path.as_slice(), ["attachments", channel, id, name]
        if snowflake(channel) && snowflake(id) && !name.is_empty())
    .then_some(url)
}

pub(super) struct Payload {
    pub body: Value,
    file: Option<(String, String)>,
}

impl Payload {
    pub fn new(text: &str, nonce: &str, components: Option<Value>) -> Effect<Self> {
        let rendered = markdown::render(text, FILE_LIMIT);
        if rendered.len() > FILE_LIMIT {
            return Err(EffectError::rejected(
                "Discord text attachment exceeds the safe 8 MiB upload limit",
            ));
        }
        let (content, file) = if utf16(&rendered) > MESSAGE_LIMIT {
            (
                "Full text is attached (exceeds Discord's message limit).".to_owned(),
                Some((format!("omp-{nonce}.txt"), rendered.into_owned())),
            )
        } else if rendered.is_empty() {
            ("(empty output)".to_owned(), None)
        } else {
            (rendered.into_owned(), None)
        };
        let mut body = json!({ "content": content, "allowed_mentions": no_mentions() });
        if let Some(components) = components {
            body["components"] = components;
        }
        if let Some((name, _)) = &file {
            body["attachments"] = json!([{ "id": 0, "filename": name }]);
        }
        Ok(Self { body, file })
    }

    pub fn create(&mut self, nonce: &str) {
        self.body["nonce"] = json!(nonce);
        self.body["enforce_nonce"] = json!(true);
    }

    pub fn edit(&mut self) {
        if self.file.is_none() {
            self.body["attachments"] = json!([]);
        }
    }

    pub fn apply(self, request: RequestBuilder) -> RequestBuilder {
        match self.file {
            None => request.json(&self.body),
            Some((name, text)) => request.multipart(
                multipart::Form::new()
                    .text("payload_json", self.body.to_string())
                    .part(
                        "files[0]",
                        multipart::Part::bytes(text.into_bytes()).file_name(name),
                    ),
            ),
        }
    }
}

impl State {
    pub(super) async fn send(&self, output: &Output) -> Effect<String> {
        if let Some(reply) = &output.reply_to
            && (!snowflake(&reply.message_id) || reply.route.physical() != output.route.physical())
        {
            return Err(EffectError::rejected(
                "Discord reply requires a real message in the exact destination conversation",
            ));
        }
        let channel = self.destination(&output.route).await?;
        let nonce = nonce(&output.id, &output.route);
        let mut payload = Payload::new(&output.text, &nonce, None)?;
        payload.create(&nonce);
        if let Some(reply) = &output.reply_to {
            payload.body["message_reference"] =
                json!({"message_id": reply.message_id, "fail_if_not_exists": false});
        }
        let request = self
            .rest
            .request(Method::POST, &format!("channels/{}/messages", channel.id));
        let sent: RemoteMessage = Rest::response(payload.apply(request)).await?;
        if !sent.belongs(&output.route) || !snowflake(&sent.id) {
            return Err(broken(
                "Discord message receipt identity could not be verified",
            ));
        }
        Ok(sent.id)
    }

    pub(super) async fn activity(
        &self,
        source: &MessageReference,
        activity: Activity,
    ) -> Effect<()> {
        if !snowflake(&source.message_id) {
            return Err(EffectError::rejected(
                "Activity requires a Discord message snowflake",
            ));
        }
        let channel = self.destination(&source.route).await?;
        let (method, emoji) = match activity {
            Activity::Received => (Method::PUT, "%F0%9F%91%80"),
            Activity::Thinking => (Method::PUT, "%F0%9F%92%AD"),
            Activity::Idle => (Method::DELETE, "%F0%9F%92%AD"),
        };
        Rest::empty(self.rest.request(
            method,
            &format!(
                "channels/{}/messages/{}/reactions/{emoji}/@me",
                channel.id, source.message_id
            ),
        ))
        .await
    }

    pub(super) async fn download(&self, route: &Route, file: &Attachment) -> Effect<Response> {
        self.assert_route(route)?;
        let url = attachment_url(&file.url).ok_or_else(|| {
            EffectError::rejected("Attachment is not an authorized Discord CDN attachment")
        })?;
        let mut segments = url.path_segments().into_iter().flatten().skip(1);
        if file.size > 9_007_199_254_740_991
            || segments.next() != Some(route.destination())
            || segments.next() != Some(file.id.as_str())
        {
            return Err(EffectError::rejected(
                "Discord attachment does not belong to this destination and resource",
            ));
        }
        self.destination(route).await?;
        let response = self
            .rest
            .client
            .get(url)
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .map_err(|_| EffectError::rejected("Discord attachment download failed"))?;
        if !response.status().is_success() {
            return Err(EffectError::rejected(
                "Discord attachment download was rejected",
            ));
        }
        Ok(response)
    }
}
