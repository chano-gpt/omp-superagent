use std::time::Duration;

use reqwest::header::{AUTHORIZATION, HeaderValue};
use reqwest::{Client, Method, RequestBuilder, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::{Effect, EffectError, model::Interaction, snowflake};

const API: &str = "https://discord.com/api/v10";
pub(super) const FILE_LIMIT: usize = 8 * 1024 * 1024;

pub(super) fn broken(message: &str) -> EffectError {
    EffectError::uncertain(message)
}

pub(super) fn no_mentions() -> Value {
    json!({ "parse": [], "replied_user": false })
}

pub(super) struct Rest {
    pub client: Client,
    pub(super) api: String,
    token: String,
    authorization: HeaderValue,
}

#[derive(Default, Deserialize)]
struct Failure {
    code: Option<u64>,
    retry_after: Option<f64>,
}

pub(super) enum Lookup<T> {
    Found(T),
    Missing,
}

impl Rest {
    pub fn new(client: Client, token: String) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !token.trim().is_empty(),
            "Configured Discord credential is empty"
        );
        let mut authorization = HeaderValue::from_str(&format!("Bot {token}"))
            .map_err(|_| anyhow::anyhow!("Invalid Discord credential"))?;
        authorization.set_sensitive(true);
        Ok(Self {
            client,
            api: API.to_owned(),
            token,
            authorization,
        })
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.client
            .request(method, format!("{}/{path}", self.api))
            .header(AUTHORIZATION, self.authorization.clone())
            .timeout(Duration::from_secs(15))
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Effect<T> {
        Self::response(self.request(Method::GET, path)).await
    }

    pub async fn lookup<T: DeserializeOwned>(&self, path: &str) -> Effect<Lookup<T>> {
        let response = send(self.request(Method::GET, path)).await?;
        if response.status() == StatusCode::NOT_FOUND {
            let failure = response
                .json::<Failure>()
                .await
                .map_err(|_| broken("Discord lookup outcome is unknown"))?;
            if matches!(failure.code, Some(10003 | 10008)) {
                return Ok(Lookup::Missing);
            }
            return Err(EffectError::rejected("Discord lookup was rejected (404)"));
        }
        decode(check(response).await?).await.map(Lookup::Found)
    }

    pub async fn response<T: DeserializeOwned>(request: RequestBuilder) -> Effect<T> {
        decode(check(send(request).await?).await?).await
    }

    pub async fn empty(request: RequestBuilder) -> Effect<()> {
        check(send(request).await?).await.map(|_| ())
    }

    pub async fn callback(&self, interaction: &Interaction, body: Value) -> Effect<()> {
        // The URL is a per-interaction credential; never put it in diagnostics.
        if !snowflake(&interaction.id)
            || interaction.token.is_empty()
            || interaction.token.len() > 512
            || !interaction
                .token
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
        {
            return Err(EffectError::rejected("Invalid Discord interaction receipt"));
        }
        Self::empty(
            self.client
                .post(format!(
                    "{}/interactions/{}/{}/callback",
                    self.api, interaction.id, interaction.token
                ))
                .timeout(Duration::from_secs(3))
                .json(&body),
        )
        .await
    }

    pub async fn ephemeral(&self, interaction: &Interaction, text: &str) -> Effect<()> {
        self.callback(interaction, json!({
            "type": 4, "data": { "content": text, "flags": 64, "allowed_mentions": no_mentions() }
        })).await
    }
}

async fn send(request: RequestBuilder) -> Effect<reqwest::Response> {
    request.send().await.map_err(|error| {
        if error.is_builder() || error.is_connect() {
            EffectError::rejected("Discord REST request could not be sent")
        } else {
            broken("Discord REST transport outcome is uncertain")
        }
    })
}

async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Effect<T> {
    response
        .json()
        .await
        .map_err(|_| broken("Discord REST response could not be verified"))
}

async fn check(response: reqwest::Response) -> Effect<reqwest::Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    if status.is_client_error() && status != StatusCode::REQUEST_TIMEOUT {
        let retry = status == StatusCode::TOO_MANY_REQUESTS;
        let header = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<f64>().ok());
        let mut error = EffectError::rejected(format!(
            "Discord REST request rejected ({})",
            status.as_u16()
        ));
        if retry {
            let delay = response
                .json::<Failure>()
                .await
                .map_or(header, |value| value.retry_after.or(header));
            error.retry_after_ms = delay
                .and_then(|secs| Duration::try_from_secs_f64(secs).ok())
                .and_then(|duration| u64::try_from(duration.as_millis()).ok());
        }
        return Err(error);
    }
    Err(EffectError::uncertain(format!(
        "Discord REST outcome is uncertain ({})",
        status.as_u16()
    )))
}
