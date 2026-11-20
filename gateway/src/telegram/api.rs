use std::time::Duration;

use reqwest::{Client, RequestBuilder, Response, StatusCode, multipart::Form};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};

use crate::adapter::EffectError;

use super::model::{Identity, SAFE_INTEGER};

const MAX_API_BYTES: usize = 16 * 1024 * 1024;
const API_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Clone, Copy)]
enum EditError {
    Unchanged,
    Missing,
    Other,
}

#[derive(Clone)]
pub(super) struct ApiError {
    code: Option<u16>,
    retry_after_ms: Option<u64>,
    message: Option<&'static str>,
    edit: EditError,
}

impl ApiError {
    const fn uncertain(message: &'static str) -> Self {
        Self {
            code: None,
            retry_after_ms: None,
            message: Some(message),
            edit: EditError::Other,
        }
    }

    fn remote(code: u16, description: &str, retry: Option<u64>) -> Self {
        let edit = if code == 400 && description.contains("message is not modified") {
            EditError::Unchanged
        } else if code == 400 && description.contains("message to edit not found") {
            EditError::Missing
        } else {
            EditError::Other
        };
        Self {
            code: Some(code),
            retry_after_ms: retry
                .filter(|_| code == 429)
                .map(|seconds| seconds.saturating_mul(1000)),
            message: None,
            edit,
        }
    }

    pub const fn code(&self) -> Option<u16> {
        self.code
    }

    pub const fn unchanged(&self) -> bool {
        matches!(self.edit, EditError::Unchanged)
    }

    pub const fn missing(&self) -> bool {
        matches!(self.edit, EditError::Missing)
    }

    pub const fn is_rejected(&self) -> bool {
        matches!(self.code, Some(400..=407 | 409..=499))
    }

    fn safe_message(&self) -> String {
        match self.code {
            Some(status) if self.is_rejected() => {
                format!("Telegram API rejected operation ({status})")
            }
            Some(status) => format!("Telegram operation outcome is unknown ({status})"),
            None => self
                .message
                .unwrap_or("Telegram operation outcome is unknown")
                .to_owned(),
        }
    }

    pub fn into_anyhow(self) -> anyhow::Error {
        anyhow::anyhow!(self.safe_message())
    }
}

impl From<ApiError> for EffectError {
    fn from(error: ApiError) -> Self {
        let mut safe = if error.is_rejected() {
            Self::rejected(error.safe_message())
        } else {
            Self::uncertain(error.safe_message())
        };
        safe.retry_after_ms = error.retry_after_ms;
        safe
    }
}

#[derive(Deserialize)]
struct Envelope {
    ok: bool,
    result: Option<Value>,
    error_code: Option<u16>,
    #[serde(default)]
    description: String,
    parameters: Option<Parameters>,
}

#[derive(Deserialize)]
struct Parameters {
    retry_after: Option<u64>,
}

#[derive(Clone)]
pub(super) struct Api {
    method_base: String,
    file_base: String,
    http: Client,
}

impl Api {
    pub fn new(http: Client, token: String) -> Self {
        Self {
            method_base: format!("https://api.telegram.org/bot{token}/"),
            file_base: format!("https://api.telegram.org/file/bot{token}/"),
            http,
        }
    }

    pub async fn bot_user(&self) -> Result<Identity, ApiError> {
        let me: Identity = self.call("getMe", &json!({})).await?;
        if !me.is_bot || me.id == 0 || me.id > SAFE_INTEGER {
            return Err(ApiError::uncertain("Telegram getMe did not identify a bot"));
        }
        Ok(me)
    }

    #[cfg(test)]
    pub fn test_instance(http: Client, base: &str) -> Self {
        Self {
            method_base: format!("{base}/bot900:LOCAL_TEST_ONLY/"),
            file_base: format!("{base}/file/bot900:LOCAL_TEST_ONLY/"),
            http,
        }
    }

    pub async fn call<T: DeserializeOwned>(
        &self,
        method: &'static str,
        body: &Value,
    ) -> Result<T, ApiError> {
        self.execute(
            self.http
                .post(format!("{}{method}", self.method_base))
                .json(body),
        )
        .await
    }

    pub async fn send_document<T: DeserializeOwned>(&self, body: Form) -> Result<T, ApiError> {
        self.execute(
            self.http
                .post(format!("{}sendDocument", self.method_base))
                .multipart(body),
        )
        .await
    }

    async fn execute<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T, ApiError> {
        // Do not retain reqwest errors, API descriptions, URLs or response payloads.
        let network = |_| ApiError::uncertain("Telegram HTTP request did not complete");
        let malformed = || ApiError::uncertain("Telegram returned an invalid API response");
        let mut answer = request.timeout(API_TIMEOUT).send().await.map_err(network)?;
        let status = answer.status();
        if status.is_redirection() {
            return Err(ApiError::uncertain("Telegram API redirects are forbidden"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = answer.chunk().await.map_err(network)? {
            if bytes.len().saturating_add(chunk.len()) > MAX_API_BYTES {
                return Err(ApiError::uncertain(
                    "Telegram API response exceeded its size bound",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let envelope: Envelope = serde_json::from_slice(&bytes).map_err(|_| malformed())?;
        if !envelope.ok {
            // An HTTP status alone might be a proxy response after Telegram
            // applied the operation. Only an authoritative Bot API envelope
            // can prove a rejection and release a new-card attempt fence.
            let code = envelope
                .error_code
                .filter(|code| {
                    (400..=599).contains(code)
                        && (status == StatusCode::OK || *code == status.as_u16())
                })
                .ok_or_else(malformed)?;
            if envelope.result.is_some() {
                return Err(malformed());
            }
            return Err(ApiError::remote(
                code,
                &envelope.description,
                envelope.parameters.and_then(|value| value.retry_after),
            ));
        }
        if status != StatusCode::OK || envelope.error_code.is_some() {
            return Err(malformed());
        }
        serde_json::from_value(envelope.result.ok_or_else(malformed)?).map_err(|_| malformed())
    }

    pub async fn download(&self, path: &str) -> Result<Response, EffectError> {
        let url = format!("{}{path}", self.file_base);
        let response = self
            .http
            .get(&url)
            .timeout(API_TIMEOUT)
            .send()
            .await
            .map_err(|_| EffectError::uncertain("Telegram file request did not complete"))?;
        if response.status().is_redirection() || response.url().as_str() != url {
            return Err(EffectError::rejected(
                "Telegram file redirects or origin substitutions are forbidden",
            ));
        }
        if !response.status().is_success() {
            return Err(ApiError::remote(response.status().as_u16(), "", None).into());
        }
        Ok(response)
    }
}
