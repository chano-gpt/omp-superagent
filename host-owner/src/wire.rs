// allow: SIZE_OK — the complete RPC wire codec and its boundary tests are owned here.
use std::borrow::Cow;
use std::io;

use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

/// Stock output limit, including the newline.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Stock v2 limit on the logical JSON, before base64 encoding.
pub const MAX_REASSEMBLED_BYTES: usize = 64 * 1024 * 1024;
const CHUNK_BYTES: usize = 256 * 1024;
const MAX_CHUNKS: usize = MAX_REASSEMBLED_BYTES / CHUNK_BYTES;
const MAX_CHUNK_BASE64: usize = CHUNK_BYTES.div_ceil(3) * 4;
const MAX_COMMAND_BYTES: usize = 16 * 1024 * 1024;

fn extend_buffer(buffer: &mut Vec<u8>, bytes: &[u8], limit: usize) {
    let needed = buffer.len() + bytes.len();
    if needed > buffer.capacity() {
        let target = needed.max(buffer.capacity() * 2).min(limit);
        buffer.reserve_exact(target - buffer.len());
    }
    buffer.extend_from_slice(bytes);
}

struct Pending {
    chunk_id: String,
    count: usize,
    byte_length: usize,
    next_index: usize,
    bytes: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Chunk {
    chunk_id: String,
    index: usize,
    count: usize,
    byte_length: usize,
    data: String,
}

/// One decoder per input stream. A framing failure makes that stream unusable.
pub struct Decoder {
    line_limit: usize,
    line: Vec<u8>,
    pending: Option<Pending>,
    failed: bool,
}

impl Default for Decoder {
    fn default() -> Self {
        Self {
            line_limit: MAX_FRAME_BYTES,
            line: Vec::new(),
            pending: None,
            failed: false,
        }
    }
}

impl Decoder {
    /// Stock clients send commands as unchunked JSONL even after negotiating v2.
    pub fn for_commands() -> Self {
        Self {
            line_limit: MAX_COMMAND_BYTES,
            ..Self::default()
        }
    }

    fn accept(&mut self, frame: Value, physical_bytes: usize) -> Result<Option<Value>> {
        ensure!(frame.is_object(), "RPC frame must be an object");
        if frame["type"] != "rpc_chunk" {
            ensure!(self.pending.is_none(), "RPC chunk sequence interrupted");
            return Ok(Some(frame));
        }

        ensure!(
            physical_bytes <= MAX_FRAME_BYTES,
            "RPC chunk exceeded the transport limit"
        );
        let chunk: Chunk = serde_json::from_value(frame).context("Invalid RPC chunk metadata")?;
        ensure!(
            (1..=128).contains(&chunk.chunk_id.encode_utf16().count())
                && (2..=MAX_CHUNKS).contains(&chunk.count)
                && chunk.index < chunk.count
                && (MAX_FRAME_BYTES..=MAX_REASSEMBLED_BYTES).contains(&chunk.byte_length)
                && chunk.byte_length <= chunk.count * CHUNK_BYTES,
            "Invalid RPC chunk metadata"
        );
        let pending = match &mut self.pending {
            Some(pending) => {
                ensure!(
                    pending.chunk_id == chunk.chunk_id
                        && pending.count == chunk.count
                        && pending.byte_length == chunk.byte_length
                        && pending.next_index == chunk.index,
                    "RPC chunk sequence mismatch"
                );
                pending
            }
            slot @ None => {
                ensure!(chunk.index == 0, "RPC chunk sequence must start at index 0");
                slot.insert(Pending {
                    chunk_id: chunk.chunk_id,
                    count: chunk.count,
                    byte_length: chunk.byte_length,
                    next_index: 0,
                    bytes: Vec::new(),
                })
            }
        };
        ensure!(
            !chunk.data.is_empty() && chunk.data.len() <= MAX_CHUNK_BASE64,
            "Invalid RPC chunk payload size"
        );
        let bytes = STANDARD
            .decode(chunk.data)
            .context("Invalid RPC chunk base64")?;
        ensure!(
            bytes.len() <= CHUNK_BYTES,
            "RPC chunk payload exceeds the transport limit"
        );
        let received = pending.bytes.len() + bytes.len();
        let remaining = pending.count - chunk.index - 1;
        ensure!(
            (received + remaining..=received + remaining * CHUNK_BYTES)
                .contains(&pending.byte_length),
            "RPC chunk sequence length mismatch"
        );
        extend_buffer(&mut pending.bytes, &bytes, pending.byte_length);
        pending.next_index += 1;
        if remaining > 0 {
            return Ok(None);
        }

        let completed = self.pending.take().context("RPC chunk buffer missing")?;
        let frame: Value =
            serde_json::from_slice(&completed.bytes).context("Invalid reassembled RPC JSON")?;
        ensure!(frame.is_object(), "RPC frame must be an object");
        Ok(Some(frame))
    }

    async fn read(&mut self, reader: &mut (impl AsyncBufRead + Unpin)) -> Result<Option<Value>> {
        loop {
            let available = reader.fill_buf().await.context("Reading RPC frame")?;
            if available.is_empty() {
                ensure!(self.line.is_empty(), "Unterminated RPC frame at EOF");
                ensure!(
                    self.pending.is_none(),
                    "Unfinished RPC chunk sequence at EOF"
                );
                return Ok(None);
            }

            let room = self.line_limit - self.line.len();
            match available[..available.len().min(room)]
                .iter()
                .position(|&b| b == b'\n')
            {
                None => {
                    // Without a newline, reaching the limit already makes a valid line impossible.
                    ensure!(
                        available.len() < room,
                        "RPC frame exceeded the transport limit"
                    );
                    extend_buffer(&mut self.line, available, self.line_limit);
                    let used = available.len();
                    reader.consume(used);
                }
                Some(end) => {
                    let physical_bytes = self.line.len() + end + 1;
                    let tail = &available[..end];
                    let line = if self.line.is_empty() {
                        tail
                    } else {
                        extend_buffer(&mut self.line, tail, self.line_limit);
                        &self.line
                    };
                    let frame = if line.iter().all(|&b| matches!(b, b' ' | b'\t' | b'\r')) {
                        None
                    } else {
                        Some(serde_json::from_slice(line).context("Invalid RPC JSON line")?)
                    };
                    reader.consume(end + 1);
                    if self.line.capacity() > 64 * 1024 {
                        self.line = Vec::new();
                    } else {
                        self.line.clear();
                    }
                    if let Some(frame) = frame
                        && let Some(decoded) = self.accept(frame, physical_bytes)?
                    {
                        return Ok(Some(decoded));
                    }
                }
            }
        }
    }
}

/// Reads a complete JSONL object, possibly reassembled from consecutive v2 chunks.
/// Incomplete input lives in the decoder so cancelling a pending read does not lose bytes.
pub async fn read_frame(
    reader: &mut (impl AsyncBufRead + Unpin),
    decoder: &mut Decoder,
) -> Result<Option<Value>> {
    ensure!(
        !decoder.failed,
        "RPC decoder is closed after a framing failure"
    );
    let result = decoder.read(reader).await;
    if result.is_err() {
        decoder.failed = true;
        decoder.line = Vec::new();
        decoder.pending = None;
    }
    result
}

struct BoundedJson {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl io::Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            self.exceeded = true;
            return Err(io::Error::other("RPC JSON size limit"));
        }
        extend_buffer(&mut self.bytes, bytes, self.limit);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn bounded_json(value: &impl Serialize, limit: usize) -> Result<Option<Vec<u8>>> {
    let mut output = BoundedJson {
        bytes: Vec::new(),
        limit,
        exceeded: false,
    };
    match serde_json::to_writer(&mut output, value) {
        Ok(()) => Ok(Some(output.bytes)),
        Err(_) if output.exceeded => Ok(None),
        Err(error) => Err(error).context("Encoding RPC JSON"),
    }
}

async fn write_line(writer: &mut (impl AsyncWrite + Unpin), bytes: &mut Vec<u8>) -> Result<()> {
    let json_len = bytes.len();
    bytes.reserve_exact(1);
    bytes.push(b'\n');
    let result = writer
        .write_all(bytes)
        .await
        .context("Writing RPC JSON line");
    bytes.truncate(json_len);
    result
}

/// Stock CLI stdin is raw JSONL: negotiation changes only the CLI's output encoding.
pub async fn write_command(writer: &mut (impl AsyncWrite + Unpin), frame: &Value) -> Result<()> {
    ensure!(frame.is_object(), "RPC frame must be an object");
    let mut bytes = bounded_json(frame, MAX_COMMAND_BYTES)?
        .context("RPC command exceeded the transport limit")?;
    write_line(writer, &mut bytes).await?;
    writer.flush().await.context("Flushing RPC command")
}

#[derive(Clone, Copy)]
struct ShrinkPass {
    string: usize,
    array: usize,
    object: usize,
}

const SHRINK_PASSES: [ShrinkPass; 7] = [
    ShrinkPass {
        string: 256 * 1024,
        array: 512,
        object: 512,
    },
    ShrinkPass {
        string: 64 * 1024,
        array: 256,
        object: 256,
    },
    ShrinkPass {
        string: 16 * 1024,
        array: 128,
        object: 128,
    },
    ShrinkPass {
        string: 4 * 1024,
        array: 64,
        object: 64,
    },
    ShrinkPass {
        string: 1024,
        array: 32,
        object: 32,
    },
    ShrinkPass {
        string: 256,
        array: 8,
        object: 16,
    },
    ShrinkPass {
        string: 64,
        array: 1,
        object: 8,
    },
];

fn shrink_string(value: &str, limit: usize) -> Cow<'_, str> {
    if value.len() <= limit {
        return Cow::Borrowed(value);
    }
    let total = value.encode_utf16().count();
    if total <= limit {
        return Cow::Borrowed(value);
    }
    let mut units = 0;
    let mut end = 0;
    for scalar in value.chars() {
        if units + scalar.len_utf16() > limit.saturating_sub(80) {
            break;
        }
        units += scalar.len_utf16();
        end += scalar.len_utf8();
    }
    Cow::Owned(format!(
        "{}\n…[{} chars elided for RPC frame]",
        &value[..end],
        total - units
    ))
}

#[derive(Clone, Copy)]
enum View<'a> {
    Json(&'a Value),
    Messages(&'a [Value]),
    MessageCount(usize),
    Terminal(&'a Map<String, Value>, &'a [Value], usize),
}

/// Views serialize compaction/trimming without cloning potentially large JSON subtrees.
struct Projection<'a> {
    view: View<'a>,
    pass: Option<ShrinkPass>,
}

impl<'a> Projection<'a> {
    fn object<S: Serializer>(
        &self,
        serializer: S,
        fields: impl Iterator<Item = (&'a str, View<'a>)>,
        count: usize,
    ) -> std::result::Result<S::Ok, S::Error> {
        let keep = self.pass.map_or(count, |pass| pass.object.min(count));
        let mut map = serializer.serialize_map(None)?;
        for (key, view) in fields.take(keep) {
            map.serialize_entry(
                key,
                &Self {
                    view,
                    pass: self.pass,
                },
            )?;
        }
        if keep < count {
            map.serialize_entry("rpcFrameElidedKeys", &(count - keep))?;
        }
        map.end()
    }
}

impl Serialize for Projection<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match (self.view, self.pass) {
            (View::Json(value), None) => value.serialize(serializer),
            (View::Json(Value::String(value)), Some(pass)) => {
                shrink_string(value, pass.string).serialize(serializer)
            }
            (View::Json(Value::Object(map)), Some(_)) => self.object(
                serializer,
                map.iter()
                    .map(|(key, value)| (key.as_str(), View::Json(value))),
                map.len(),
            ),
            (View::Json(Value::Array(values)), Some(_)) => Self {
                view: View::Messages(values),
                pass: self.pass,
            }
            .serialize(serializer),
            (View::Json(value), Some(_)) => value.serialize(serializer),
            (View::Messages(values), pass) => {
                let keep = pass.map_or(values.len(), |pass| pass.array.min(values.len()));
                let mut seq = serializer.serialize_seq(None)?;
                for value in &values[..keep] {
                    seq.serialize_element(&Self {
                        view: View::Json(value),
                        pass,
                    })?;
                }
                if keep < values.len() {
                    seq.serialize_element(&format!(
                        "…[{} items elided for RPC frame]",
                        values.len() - keep
                    ))?;
                }
                seq.end()
            }
            (View::MessageCount(count), _) => count.serialize(serializer),
            (View::Terminal(map, messages, total), _) => {
                let count_missing = !map.contains_key("messageCount");
                let fields = map
                    .iter()
                    .map(|(key, value)| {
                        (
                            key.as_str(),
                            match key.as_str() {
                                "messages" => View::Messages(messages),
                                "messageCount" => View::MessageCount(total),
                                _ => View::Json(value),
                            },
                        )
                    })
                    .chain(count_missing.then_some(("messageCount", View::MessageCount(total))));
                self.object(serializer, fields, map.len() + usize::from(count_missing))
            }
        }
    }
}

fn overflow(frame: &Value, message_count: Option<usize>) -> Value {
    let metadata = |key: &str| frame[key].as_str().map(|s| shrink_string(s, 1024));
    match frame["type"].as_str() {
        Some("response") => {
            let mut response = json!({
                "type": "response", "command": metadata("command").unwrap_or(Cow::Borrowed("unknown")),
                "success": false, "error": "RPC response exceeded the transport limit",
            });
            if let Some(id) = metadata("id") {
                response["id"] = json!(id);
            }
            response
        }
        Some("agent_end") => {
            let count = message_count.map_or_else(
                || {
                    frame["messageCount"]
                        .as_number()
                        .map_or_else(|| json!(0), |count| json!(count))
                },
                |count| json!(count),
            );
            json!({ "type": "agent_end", "messages": [], "messageCount": count })
        }
        _ => {
            let mut error = json!({ "type": "rpc_frame_error", "error": "RPC frame exceeded the transport limit" });
            if let Some(kind) = metadata("type") {
                error["originalType"] = json!(kind);
            }
            error
        }
    }
}

enum Encoded {
    Line(Vec<u8>),
    Reduced(Vec<u8>),
    Chunks(Vec<u8>),
}

impl Encoded {
    async fn write(&mut self, writer: &mut (impl AsyncWrite + Unpin)) -> Result<()> {
        match self {
            Self::Line(bytes) | Self::Reduced(bytes) => write_line(writer, bytes).await?,
            Self::Chunks(bytes) => {
                let chunk_id = Uuid::new_v4().to_string();
                let count = bytes.len().div_ceil(CHUNK_BYTES);
                for (index, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
                    let envelope = json!({
                        "type": "rpc_chunk", "chunkId": &chunk_id, "index": index, "count": count,
                        "byteLength": bytes.len(), "data": STANDARD.encode(chunk),
                    });
                    write_line(writer, &mut serde_json::to_vec(&envelope)?).await?;
                }
            }
        }
        writer.flush().await.context("Flushing RPC frame")
    }
}

/// Per-peer history of the leading message snapshots actually delivered to that peer.
#[derive(Default)]
pub struct Encoder {
    snapshots: Vec<Box<[u8]>>,
    snapshot_bytes: usize,
    prefix_closed: bool,
}

impl Encoder {
    fn encode(&self, frame: &Value, version: u8) -> Result<Encoded> {
        ensure!(
            matches!(version, 1 | 2),
            "Unsupported RPC protocol version: {version}"
        );
        let map = frame.as_object().context("RPC frame must be an object")?;
        let terminal = frame["type"] == "agent_end" && frame["messages"].is_array();
        let single = MAX_FRAME_BYTES - 1;
        let limit = if version == 2 {
            MAX_REASSEMBLED_BYTES
        } else {
            single
        };
        let initial_limit = if terminal { single } else { limit };
        if let Some(bytes) = bounded_json(frame, initial_limit)? {
            return Ok(if bytes.len() <= single {
                Encoded::Line(bytes)
            } else {
                Encoded::Chunks(bytes)
            });
        }

        let mut view = View::Json(frame);
        let mut message_count = None;
        if terminal {
            let messages = frame["messages"]
                .as_array()
                .context("Invalid RPC terminal messages")?;
            let mut prefix = 0;
            for (message, snapshot) in messages.iter().zip(&self.snapshots) {
                if bounded_json(message, snapshot.len())?.as_deref() != Some(snapshot.as_ref()) {
                    break;
                }
                prefix += 1;
            }
            message_count = Some(messages.len());
            view = View::Terminal(map, &messages[prefix..], messages.len());
            if let Some(bytes) = bounded_json(&Projection { view, pass: None }, limit)? {
                return Ok(if bytes.len() <= single {
                    Encoded::Reduced(bytes)
                } else {
                    Encoded::Chunks(bytes)
                });
            }
        }

        if version == 1 && frame["type"] != "response" {
            for pass in SHRINK_PASSES {
                if let Some(bytes) = bounded_json(
                    &Projection {
                        view,
                        pass: Some(pass),
                    },
                    single,
                )? {
                    return Ok(Encoded::Reduced(bytes));
                }
            }
        }
        Ok(Encoded::Reduced(serde_json::to_vec(&overflow(
            frame,
            message_count,
        ))?))
    }

    /// Write one frame, applying terminal compaction only against snapshots delivered on this stream.
    pub async fn write_frame(
        &mut self,
        writer: &mut (impl AsyncWrite + Unpin),
        frame: &Value,
        version: u8,
    ) -> Result<()> {
        let mut encoded = self.encode(frame, version)?;
        encoded.write(writer).await?;
        match frame["type"].as_str() {
            Some("agent_start") => *self = Self::default(),
            Some("agent_end") if frame["willContinue"] != true => *self = Self::default(),
            Some("message_end") if !self.prefix_closed => {
                // Both bytes and allocation count are bounded. If the prefix cannot be
                // extended, retaining the terminal suffix is safe and lossless.
                let delivered = match encoded {
                    Encoded::Line(_) | Encoded::Chunks(_) => None,
                    Encoded::Reduced(bytes) => Some(serde_json::from_slice::<Value>(&bytes)?),
                };
                let snapshot = delivered.as_ref().unwrap_or(frame).get("message");
                if let Some(message) = snapshot
                    && self.snapshots.len() < 4096
                    && let Some(bytes) =
                        bounded_json(message, MAX_REASSEMBLED_BYTES - self.snapshot_bytes)?
                {
                    self.snapshot_bytes += bytes.len();
                    self.snapshots.push(bytes.into_boxed_slice());
                } else {
                    self.prefix_closed = true;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Stateless framing for channels which do not forward a stream of agent messages.
/// A failed or cancelled write may have written part of the frame; close that writer.
pub async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    frame: &Value,
    version: u8,
) -> Result<()> {
    Encoder::default()
        .encode(frame, version)?
        .write(writer)
        .await
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    use super::*;
    use tokio::io::{BufReader, duplex};

    #[tokio::test]
    async fn keeps_partial_lines_across_cancellation_and_requires_newline_at_eof() {
        let (input, mut producer) = duplex(64);
        let mut reader = BufReader::with_capacity(4, input);
        let mut decoder = Decoder::default();
        producer.write_all(b"{\"type\":").await.unwrap();
        let mut reading = Box::pin(read_frame(&mut reader, &mut decoder));
        poll_fn(|cx| {
            assert!(Future::poll(reading.as_mut(), cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(reading);

        producer
            .write_all(b"\"ready\"}\n{\"type\":\"notice\"}")
            .await
            .unwrap();
        producer.shutdown().await.unwrap();
        assert_eq!(
            read_frame(&mut reader, &mut decoder).await.unwrap(),
            Some(json!({"type":"ready"}))
        );
        assert_eq!(
            read_frame(&mut reader, &mut decoder)
                .await
                .unwrap_err()
                .to_string(),
            "Unterminated RPC frame at EOF"
        );
        assert!(
            read_frame(&mut reader, &mut decoder)
                .await
                .unwrap_err()
                .to_string()
                .contains("closed")
        );
    }

    #[tokio::test]
    async fn caps_partial_lines_but_passes_stock_large_raw_commands() {
        let mut payload = vec![b'x'; MAX_FRAME_BYTES];
        {
            let mut reader = BufReader::with_capacity(173, payload.as_slice());
            assert_eq!(
                read_frame(&mut reader, &mut Decoder::default())
                    .await
                    .unwrap_err()
                    .to_string(),
                "RPC frame exceeded the transport limit"
            );
        }
        payload.clear();

        let exact = json!({"data":"x".repeat(MAX_FRAME_BYTES - b"{\"data\":\"\"}\n".len())});
        write_frame(&mut payload, &exact, 2).await.unwrap();
        assert_eq!(payload.len(), MAX_FRAME_BYTES);
        assert_eq!(
            read_frame(&mut payload.as_slice(), &mut Decoder::default())
                .await
                .unwrap(),
            Some(exact)
        );
        payload.clear();

        let command = json!({"type":"prompt", "message":"é".repeat(MAX_FRAME_BYTES / 2)});
        write_command(&mut payload, &command).await.unwrap();
        assert_eq!(payload.iter().filter(|&&b| b == b'\n').count(), 1);
        assert_eq!(
            read_frame(&mut payload.as_slice(), &mut Decoder::for_commands())
                .await
                .unwrap(),
            Some(command)
        );
    }

    #[tokio::test]
    async fn writes_stock_v2_envelopes_and_reassembles_multibyte_utf8_across_chunks() {
        // The first chunk ends after the first byte of é. UTF-8 belongs to the logical
        // frame, not each individual base64-decoded slice.
        let mut message = "x".repeat(CHUNK_BYTES - 10);
        message.push_str(&"é".repeat(MAX_FRAME_BYTES / 2));
        let frame = json!({"data":message, "type":"notice"});
        let mut payload = Vec::new();
        write_frame(&mut payload, &frame, 2).await.unwrap();

        let expected = serde_json::to_vec(&frame).unwrap();
        let mut chunk_id = None;
        for (index, line) in payload.split_inclusive(|b| *b == b'\n').enumerate() {
            assert!(line.len() <= MAX_FRAME_BYTES);
            let fields: Value = serde_json::from_slice(line).unwrap();
            assert_eq!(fields["type"], "rpc_chunk");
            assert_eq!(fields["index"], json!(index));
            assert_eq!(fields["count"], json!(expected.len().div_ceil(CHUNK_BYTES)));
            assert_eq!(fields["byteLength"], json!(expected.len()));
            let id = fields["chunkId"].as_str().unwrap();
            assert_eq!(chunk_id.get_or_insert_with(|| id.to_owned()).as_str(), id);
            let chunk = STANDARD.decode(fields["data"].as_str().unwrap()).unwrap();
            let start = index * CHUNK_BYTES;
            assert_eq!(
                chunk.as_slice(),
                &expected[start..expected.len().min(start + CHUNK_BYTES)]
            );
            if index == 0 {
                assert!(
                    std::str::from_utf8(&chunk)
                        .unwrap_err()
                        .error_len()
                        .is_none()
                );
            }
        }

        let mut input = BufReader::new(payload.as_slice());
        let mut decoder = Decoder::default();
        assert_eq!(
            read_frame(&mut input, &mut decoder).await.unwrap(),
            Some(frame)
        );
        assert_eq!(read_frame(&mut input, &mut decoder).await.unwrap(), None);
    }

    #[tokio::test]
    async fn rejects_out_of_order_noncanonical_truncated_and_interleaved_chunks() {
        let first = json!({
            "type":"rpc_chunk", "index":0, "chunkId":"rpc-13",
            "count":4, "byteLength":1024 * 1024, "data": STANDARD.encode(vec![b'a'; 256 * 1024])
        });
        let mut wrong_index = first.clone();
        wrong_index["index"] = json!(2);
        let mut wrong_base64 = first.clone();
        wrong_base64["data"] = json!("AB==");
        let mut wrong_id = first.clone();
        wrong_id["index"] = json!(1);
        wrong_id["chunkId"] = json!("rpc-14");
        let mut excessive_count = first.clone();
        excessive_count["count"] = json!(257);
        let mut excessive_bytes = first.clone();
        excessive_bytes["byteLength"] = json!((64 * 1024 * 1024) + 1);
        let mut short_chunk = first.clone();
        short_chunk["data"] = json!("AA==");
        let ready = json!({"type":"ready"});
        for (sequence, expected) in [
            (
                vec![&wrong_index],
                "RPC chunk sequence must start at index 0",
            ),
            (vec![&wrong_base64], "Invalid RPC chunk base64"),
            (vec![&excessive_count], "Invalid RPC chunk metadata"),
            (vec![&excessive_bytes], "Invalid RPC chunk metadata"),
            (vec![&short_chunk], "RPC chunk sequence length mismatch"),
            (vec![&first], "Unfinished RPC chunk sequence at EOF"),
            (vec![&first, &wrong_id], "RPC chunk sequence mismatch"),
            (vec![&first, &ready], "RPC chunk sequence interrupted"),
        ] {
            let mut input = Vec::new();
            for chunk in sequence {
                serde_json::to_writer(&mut input, chunk).unwrap();
                input.push(b'\n');
            }
            let error = read_frame(&mut input.as_slice(), &mut Decoder::default())
                .await
                .unwrap_err();
            assert_eq!(error.to_string(), expected);
        }
    }

    #[tokio::test]
    async fn v1_terminal_compacts_only_the_previously_delivered_prefix() {
        let delivered = Value::String("a".repeat(MAX_FRAME_BYTES / 2));
        let unseen = Value::String("b".repeat(MAX_FRAME_BYTES / 2));
        let mut encoder = Encoder::default();
        let mut output = Vec::new();
        encoder
            .write_frame(
                &mut output,
                &json!({"type":"message_end", "message":delivered}),
                1,
            )
            .await
            .unwrap();
        output.clear();
        let terminal =
            json!({"type":"agent_end", "messages":[delivered, unseen], "willContinue":true});
        encoder
            .write_frame(&mut output, &terminal, 1)
            .await
            .unwrap();
        let expected =
            json!({"type":"agent_end", "messages":[unseen], "messageCount":2, "willContinue":true});
        assert_eq!(
            read_frame(&mut output.as_slice(), &mut Decoder::default())
                .await
                .unwrap(),
            Some(expected)
        );

        output.clear();
        encoder
            .write_frame(&mut output, &json!({"type":"agent_start"}), 1)
            .await
            .unwrap();
        output.clear();
        encoder
            .write_frame(&mut output, &terminal, 1)
            .await
            .unwrap();
        let projected = read_frame(&mut output.as_slice(), &mut Decoder::default())
            .await
            .unwrap()
            .unwrap();
        let messages = projected["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert!(
            messages[0]
                .as_str()
                .unwrap()
                .contains("chars elided for RPC frame")
        );
    }

    #[tokio::test]
    async fn v1_shortening_preserves_first_wire_fields_not_sorted_keys() {
        let mut body = Map::new();
        body.insert("z-first".into(), json!(true));
        for i in 0..540 {
            body.insert(format!("field-{i}"), json!("x".repeat(2048)));
        }
        let frame = json!({"type":"notice", "data":body});
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &frame, 1).await.unwrap();
        let projected: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(projected["data"]["z-first"], true);
        assert!(projected["data"].get("field-539").is_none());
    }

    #[tokio::test]
    async fn oversized_responses_keep_their_correlation_in_both_versions() {
        for (version, size) in [(1, MAX_FRAME_BYTES), (2, MAX_REASSEMBLED_BYTES)] {
            let mut frame =
                json!({"type":"response", "success":true, "command":"get_state", "id":"original"});
            frame["data"] = Value::String("x".repeat(size));
            let mut output = Vec::new();
            write_frame(&mut output, &frame, version).await.unwrap();
            assert_eq!(
                read_frame(&mut output.as_slice(), &mut Decoder::default())
                    .await
                    .unwrap(),
                Some(json!({
                    "type":"response", "success":false, "command":"get_state", "id":"original",
                    "error":"RPC response exceeded the transport limit",
                })),
            );
        }
    }

    #[tokio::test]
    async fn v1_unshrinkable_terminal_and_diagnostic_keep_distinct_envelopes() {
        let mut terminal = json!({"type":"agent_end", "messages":[{"text":"not streamed"}]});
        let mut diagnostic = json!({"type":"notice"});
        let oversized_key = "x".repeat(MAX_FRAME_BYTES);
        terminal[oversized_key.as_str()] = Value::Null;
        diagnostic[oversized_key.as_str()] = Value::Null;
        for (frame, expected) in [
            (
                terminal,
                json!({"type":"agent_end", "messages":[], "messageCount":1}),
            ),
            (
                diagnostic,
                json!({
                    "type":"rpc_frame_error", "originalType":"notice", "error":"RPC frame exceeded the transport limit",
                }),
            ),
        ] {
            let mut output = Vec::new();
            write_frame(&mut output, &frame, 1).await.unwrap();
            assert_eq!(
                read_frame(&mut output.as_slice(), &mut Decoder::default())
                    .await
                    .unwrap(),
                Some(expected),
            );
        }
    }
}
