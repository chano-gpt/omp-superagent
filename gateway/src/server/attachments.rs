use std::io;

use axum::{
    body::{Body, Bytes},
    http::{HeaderValue, header},
    response::Response,
};
use futures_util::stream;
use serde_json::{Map, Value};

use super::{
    Hub,
    events::safe_integer,
    protocol::{Fault, WireResult, object, rejected},
};

const MAX_ATTACHMENT: u64 = 20 * 1024 * 1024;

impl Hub {
    pub(super) async fn attachment(&self, data: &Map<String, Value>) -> WireResult<Response> {
        let route = data.get("route").unwrap_or(&Value::Null);
        let attachment = data.get("attachment").unwrap_or(&Value::Null);
        let size = match object(attachment)?.get("size") {
            Some(Value::Null) => None,
            Some(value) => Some(
                safe_integer(value)
                    .and_then(|size| u64::try_from(size).ok())
                    .filter(|size| *size <= MAX_ATTACHMENT)
                    .ok_or_else(|| rejected("Invalid attachment size"))?,
            ),
            None => return Err(rejected("Invalid attachment size")),
        };
        let upstream = self
            .adapters
            .attachment(route, attachment)
            .await
            .map_err(Fault::from)?;
        let status = upstream.status();
        if matches!(status.as_u16(), 101 | 204 | 205 | 304) {
            return Err(rejected("Attachment response has no body"));
        }
        let bytes_limit = size.unwrap_or(MAX_ATTACHMENT);
        let length = upstream
            .headers()
            .get(header::CONTENT_LENGTH)
            .map(|length| {
                length
                    .to_str()
                    .ok()
                    .filter(|text| {
                        !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
                    })
                    .and_then(|text| text.parse::<u64>().ok())
                    .filter(|length| *length <= bytes_limit)
                    .ok_or_else(|| rejected("Attachment response exceeds declared size"))
                    .map(|parsed| (length.clone(), parsed))
            })
            .transpose()?;
        let content_type = upstream
            .headers()
            .get(header::CONTENT_TYPE)
            .cloned()
            .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"));
        let state = Transfer {
            upstream,
            bytes_limit,
            expected: size.or_else(|| length.as_ref().map(|(_, size)| *size)),
            bytes: 0,
            payload: None,
            complete: false,
        };
        let mut response =
            Response::new(Body::from_stream(stream::try_unfold(state, Transfer::next)));
        *response.status_mut() = status;
        let headers = response.headers_mut();
        headers.insert(header::CONTENT_TYPE, content_type);
        headers.insert(
            "x-gateway-attachment-protocol",
            HeaderValue::from_static("1"),
        );
        if let Some((length, _)) = length {
            headers.insert("x-gateway-attachment-length", length);
        }
        Ok(response)
    }
}

struct Transfer {
    upstream: reqwest::Response,
    bytes_limit: u64,
    expected: Option<u64>,
    bytes: u64,
    payload: Option<Bytes>,
    complete: bool,
}

impl Transfer {
    async fn next(mut self) -> io::Result<Option<(Bytes, Self)>> {
        if self.complete {
            return Ok(None);
        }
        if let Some(payload) = self.payload.take() {
            return Ok(Some((payload, self)));
        }
        while let Some(chunk) = self
            .upstream
            .chunk()
            .await
            .map_err(|_| io::Error::other("Attachment transfer interrupted"))?
        {
            if chunk.is_empty() {
                continue;
            }
            let len = u32::try_from(chunk.len())
                .map_err(|_| io::Error::other("Attachment chunk exceeds limit"))?;
            self.bytes = self
                .bytes
                .checked_add(u64::from(len))
                .ok_or_else(|| io::Error::other("Attachment size overflow"))?;
            if self.bytes > self.bytes_limit {
                return Err(io::Error::other("Attachment stream exceeds declared size"));
            }
            self.payload = Some(chunk);
            return Ok(Some((Bytes::copy_from_slice(&len.to_be_bytes()), self)));
        }
        if self.expected.is_some_and(|expected| expected != self.bytes) {
            return Err(io::Error::other("Attachment stream was truncated"));
        }
        // Only clean EOF with matching size earns the zero-length receipt.
        self.complete = true;
        Ok(Some((Bytes::from_static(&[0, 0, 0, 0]), self)))
    }
}
