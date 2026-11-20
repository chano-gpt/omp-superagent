use reqwest::{
    Response,
    header::{CONTENT_LENGTH, HeaderValue},
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};

use crate::adapter::EffectError;

use super::{
    Core,
    model::{Route, SAFE_INTEGER, decode, file_id, hash},
};

const DOWNLOAD_LIMIT: u64 = 20 * 1024 * 1024;

#[derive(Deserialize)]
pub(super) struct Media {
    pub file_id: String,
    pub file_unique_id: String,
    #[serde(default, deserialize_with = "present_size")]
    pub file_size: Option<u64>,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub duration: Option<f64>,
    #[serde(default)]
    pub width: u64,
    #[serde(default)]
    pub height: u64,
}

fn present_size<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    let size = u64::deserialize(deserializer)?;
    if size > SAFE_INTEGER {
        return Err(serde::de::Error::custom("Invalid Telegram file size"));
    }
    Ok(Some(size))
}

// Keep field order for existing JSON.stringify-based file and edit identities.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileIdentity {
    id: String,
    name: String,
    url: String,
    #[serde(deserialize_with = "Option::deserialize")]
    size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Attachment {
    #[serde(flatten)]
    download: FileIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    voice: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_seconds: Option<f64>,
}

pub(super) enum MediaKind {
    Photo,
    Document,
    Audio,
    Video,
    Voice,
    VideoNote,
}

impl Attachment {
    pub fn from_media(media: &Media, kind: MediaKind) -> Option<Self> {
        if !file_id(&media.file_id) {
            return None;
        }
        let (label, extension, content_type) = match kind {
            MediaKind::Photo => ("photo", ".jpg", Some("image/jpeg")),
            MediaKind::Document => ("document", "", None),
            MediaKind::Audio => ("audio", "", None),
            MediaKind::Video => ("video", ".mp4", Some("video/mp4")),
            MediaKind::Voice => ("voice", ".ogg", Some("audio/ogg")),
            MediaKind::VideoNote => ("video-note", ".mp4", Some("video/mp4")),
        };
        Some(Self {
            download: FileIdentity {
                name: media
                    .file_name
                    .clone()
                    .unwrap_or_else(|| format!("{label}-{}{extension}", media.file_unique_id)),
                url: format!("telegram-file:{}", media.file_id),
                id: media.file_id.clone(),
                size: media.file_size,
                content_type: media
                    .mime_type
                    .clone()
                    .or_else(|| content_type.map(str::to_owned))
                    .filter(|mime| !mime.is_empty()),
            },
            voice: matches!(kind, MediaKind::Voice).then_some(true),
            duration_seconds: media
                .duration
                .filter(|duration| *duration >= 0.0 && duration.is_finite()),
        })
    }

    pub fn revision(text: &str, files: &[Self]) -> String {
        let identities: Vec<_> = files.iter().map(|file| &file.download).collect();
        hash(&json!([text, identities]).to_string())
    }

    fn key(&self, route: &Route) -> String {
        let identity = json!([route.physical_key(), self.download.id]).to_string();
        format!("telegram:{}:file:{}", route.account_id, hash(&identity))
    }

    fn file_identity(&self) -> String {
        let file = &self.download;
        let mut value = json!({"id":file.id,"name":file.name,"url":file.url,"size":file.size});
        if let Some(content_type) = file.content_type.as_ref().filter(|mime| !mime.is_empty()) {
            value["contentType"] = json!(content_type);
        }
        value.to_string()
    }

    pub fn save(&self, core: &Core, route: &Route) -> Result<(), EffectError> {
        core.set(&self.key(route), &self.file_identity())
    }
}

#[derive(Deserialize)]
struct RemoteFile {
    file_id: String,
    #[serde(default, deserialize_with = "present_size")]
    file_size: Option<u64>,
    file_path: Option<String>,
}

pub(super) fn safe_path(path: &str) -> bool {
    let Some((prefix, tail)) = path.split_once('/') else {
        return false;
    };
    file_id(prefix)
        && tail.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, b'_' | b'-' | b'.'))
        })
}

impl Core {
    pub(super) async fn open_attachment(
        &self,
        route: &Value,
        file: &Value,
    ) -> Result<Response, EffectError> {
        let route: Route = decode(route)?;
        self.policy.authorize(&self.identity().await?, &route)?;
        let file: Attachment = decode(file)?;
        let download = &file.download;
        if !file_id(&download.id)
            || download.url != format!("telegram-file:{}", download.id)
            || download.size.is_some_and(|size| size > DOWNLOAD_LIMIT)
            || self.get(&file.key(&route))?.as_deref() != Some(&file.file_identity())
        {
            return Err(EffectError::rejected(
                "Telegram attachment does not belong to an admitted message on this route",
            ));
        }
        // Use a typed file response separate from the transport envelope, so
        // malformed file metadata is a definite unsafe-resource rejection.
        let remote: Value = self
            .api
            .call("getFile", &json!({"file_id":download.id}))
            .await?;
        let unsafe_file =
            || EffectError::rejected("Telegram returned an unsafe or oversized file resource");
        let remote: RemoteFile = serde_json::from_value(remote).map_err(|_| unsafe_file())?;
        if remote.file_id != download.id
            || remote.file_size.is_some_and(|size| {
                size > DOWNLOAD_LIMIT || download.size.is_some_and(|expected| size != expected)
            })
        {
            return Err(unsafe_file());
        }
        let path = remote
            .file_path
            .as_deref()
            .filter(|path| safe_path(path))
            .ok_or_else(unsafe_file)?;
        let mut response = self.api.download(path).await?;
        let size = download.size.or(remote.file_size);
        let limit = size.unwrap_or(DOWNLOAD_LIMIT);
        if response
            .content_length()
            .is_some_and(|length| length > limit || size.is_some_and(|size| size != length))
        {
            return Err(unsafe_file());
        }
        // Preserve an authenticated getFile byte count for the Gateway
        // stream verifier even if Telegram sent a chunked response.
        if let Some(size) = size.filter(|_| !response.headers().contains_key(CONTENT_LENGTH)) {
            let length = HeaderValue::from_str(&size.to_string()).map_err(|_| unsafe_file())?;
            response.headers_mut().insert(CONTENT_LENGTH, length);
        }
        // The Gateway's attachment stream enforces its admission size, byte
        // ceiling and end-of-stream framing even for unknown/chunked lengths.
        Ok(response)
    }
}
