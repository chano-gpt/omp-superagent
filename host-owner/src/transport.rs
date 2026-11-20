use std::{
    collections::HashMap,
    fmt, io,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    time::timeout,
};
use uuid::Uuid;

use crate::{projection::MessageUpdates, wire};

pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const LONG_TIMEOUT: Duration = Duration::from_secs(30);

const QUEUE_FRAMES: usize = 1024;
const REQUEST_BYTES: usize = 16 * 1024 * 1024;
const BACKLOG_BYTES: usize = wire::MAX_REASSEMBLED_BYTES * 2;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Error {
    Unavailable(&'static str),
    Disconnected,
    TimedOut,
}

impl Error {
    pub(crate) const fn ambiguous(self) -> bool {
        matches!(self, Self::Disconnected | Self::TimedOut)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::Unavailable(message) => message,
            Self::Disconnected => "Host disconnected; request outcome may be unknown",
            Self::TimedOut => "Host request timed out; its outcome may be unknown",
        })
    }
}

impl std::error::Error for Error {}

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy)]
pub(crate) enum Link {
    Stdio,
    Control,
    Peer,
}

// An event has one allocation shared by all peer writers. Measure once, not
// once per peer, so a blocked reader cannot retain arbitrary queued data.
#[derive(Clone)]
pub(crate) struct Packet {
    pub(crate) frame: Arc<Value>,
    bytes: usize,
}

impl Packet {
    pub(crate) fn new(frame: Value) -> Result<Self> {
        struct Size(usize);
        impl io::Write for Size {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0 = self.0.saturating_add(bytes.len());
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut size = Size(1); // JSONL delimiter
        serde_json::to_writer(&mut size, &frame)
            .map_err(|_| Error::Unavailable("Host frame could not be encoded"))?;
        Ok(Self {
            frame: Arc::new(frame),
            bytes: size.0,
        })
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

enum Outgoing {
    Frame {
        packet: Packet,
        version: u8,
        _bytes: OwnedSemaphorePermit,
    },
    EndInput,
}

struct Connection {
    outbound: mpsc::Sender<Outgoing>,
    bytes: Arc<Semaphore>,
    inflight: Arc<Semaphore>,
    status: watch::Sender<Status>,
    book: Mutex<Book>,
    link: Link,
}

#[derive(Clone, Copy, Default)]
struct Status {
    ready: bool,
    closed: bool,
}

struct Book {
    version: u8,
    message_updates: MessageUpdates,
    pending: HashMap<String, oneshot::Sender<Value>>,
}

#[derive(Clone)]
pub(crate) struct Channel(Arc<Connection>);

pub(crate) struct Pump {
    outgoing: mpsc::Receiver<Outgoing>,
}

impl Channel {
    pub(crate) fn new(link: Link) -> (Self, Pump) {
        let (outbound, outgoing) = mpsc::channel(QUEUE_FRAMES);
        let (status, _) = watch::channel(Status::default());
        let version = if matches!(link, Link::Control) { 2 } else { 1 };
        let channel = Self(Arc::new(Connection {
            outbound,
            bytes: Arc::new(Semaphore::new(BACKLOG_BYTES)),
            inflight: Arc::new(Semaphore::new(QUEUE_FRAMES)),
            status,
            book: Mutex::new(Book {
                version,
                message_updates: MessageUpdates::default(),
                pending: HashMap::new(),
            }),
            link,
        }));
        (channel, Pump { outgoing })
    }

    pub(crate) fn start<R, W, F, G, Event, Response>(
        &self,
        pump: Pump,
        input: R,
        output: W,
        on_frame: F,
        on_response: G,
    ) -> tokio::task::JoinHandle<bool>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
        F: Fn(Value, &Channel) -> Event + Send + Sync + 'static,
        G: Fn(Value) -> Response + Send + Sync + 'static,
        Event: Future<Output = ()> + Send,
        Response: Future<Output = Value> + Send,
    {
        let reader = self.clone();
        let writer = self.clone();
        tokio::spawn(async move {
            let mut closed = writer.0.status.subscribe();
            let worker = writer.write(pump.outgoing, output);
            tokio::select! {
                () = Channel::wait_closed(&mut closed) => {}
                result = worker => {
                    if result.is_err() && !matches!(writer.0.link, Link::Stdio) {
                        writer.close();
                    }
                    // Native stdout can still contain receipts after stdin fails
                    // or is closed intentionally, so let its reader finish.
                }
            }
        });
        tokio::spawn(async move {
            let mut closed = reader.0.status.subscribe();
            let read = reader.read(input, &on_frame, &on_response);
            let drained = tokio::select! {
                () = Channel::wait_closed(&mut closed) => false,
                result = read => result.is_ok(),
            };
            reader.close();
            drained
        })
    }

    async fn read<R, F, G, Event, Response>(
        &self,
        input: R,
        on_frame: &F,
        on_response: &G,
    ) -> anyhow::Result<()>
    where
        R: AsyncRead + Unpin,
        F: Fn(Value, &Self) -> Event,
        G: Fn(Value) -> Response,
        Event: Future<Output = ()>,
        Response: Future<Output = Value>,
    {
        let mut input = BufReader::new(input);
        let mut decoder = match self.0.link {
            Link::Peer => wire::Decoder::for_commands(),
            Link::Stdio | Link::Control => wire::Decoder::default(),
        };
        // Complete handling of each frame before decoding the next one.
        // Native observation backpressure never suspends the control reader.
        while let Some(frame) = wire::read_frame(&mut input, &mut decoder).await? {
            if matches!(self.0.link, Link::Peer) {
                on_frame(frame, self).await;
                continue;
            }
            match frame["type"].as_str() {
                Some("ready") => self.0.status.send_modify(|status| status.ready = true),
                Some("response") => {
                    let pending = frame["id"]
                        .as_str()
                        .and_then(|id| self.0.book.lock().ok()?.pending.remove(id));
                    // The owner must account for a response before its command
                    // continuation can run (including late responses).
                    let frame = on_response(frame).await;
                    if let Some(pending) = pending {
                        // Dropping an undeliverable response does not replay it.
                        let _expired_request = pending.send(frame).err();
                    }
                }
                _ => on_frame(frame, self).await,
            }
        }
        Ok(())
    }

    async fn write<W>(
        &self,
        mut queue: mpsc::Receiver<Outgoing>,
        mut output: W,
    ) -> anyhow::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let mut encoder = wire::Encoder::default();
        while let Some(item) = queue.recv().await {
            match item {
                Outgoing::Frame {
                    packet,
                    version,
                    _bytes,
                } => match self.0.link {
                    Link::Stdio => wire::write_command(&mut output, &packet.frame).await?,
                    Link::Control => wire::write_frame(&mut output, &packet.frame, version).await?,
                    Link::Peer => {
                        encoder
                            .write_frame(&mut output, &packet.frame, version)
                            .await?
                    }
                },
                Outgoing::EndInput => break,
            }
        }
        output.shutdown().await?;
        Ok(())
    }

    async fn wait_closed(status: &mut watch::Receiver<Status>) {
        // All senders gone also means the transport is closed.
        let _closed = status.wait_for(|value| value.closed).await;
    }

    pub(crate) async fn closed(&self) {
        Self::wait_closed(&mut self.0.status.subscribe()).await;
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.0.status.borrow().closed
    }

    pub(crate) fn close(&self) {
        self.0.status.send_if_modified(|status| {
            if status.closed {
                return false;
            }
            status.closed = true;
            true
        });
        if let Ok(mut book) = self.0.book.lock() {
            book.pending.clear();
        }
    }

    pub(crate) async fn ready(&self) -> Result<()> {
        let mut status = self.0.status.subscribe();
        let ready = timeout(
            // Startup (including extracting native modules) is not an ordinary
            // RPC request. The launcher's outer deadline includes preparation.
            LONG_TIMEOUT,
            status.wait_for(|value| value.ready || value.closed),
        )
        .await
        .map_err(|_| Error::TimedOut)?
        .map_err(|_| Error::Disconnected)?;
        if ready.ready && !ready.closed {
            Ok(())
        } else {
            Err(Error::Disconnected)
        }
    }

    pub(crate) fn set_v2(&self) -> Result<()> {
        self.0.book.lock().map_err(|_| Error::Disconnected)?.version = 2;
        Ok(())
    }

    pub(crate) fn set_message_updates(&self, mode: MessageUpdates) -> Result<()> {
        self.0
            .book
            .lock()
            .map_err(|_| Error::Disconnected)?
            .message_updates = mode;
        Ok(())
    }

    pub(crate) fn message_updates(&self) -> Result<MessageUpdates> {
        Ok(self
            .0
            .book
            .lock()
            .map_err(|_| Error::Disconnected)?
            .message_updates)
    }

    pub(crate) fn max_response_bytes(&self) -> Result<usize> {
        let version = self.0.book.lock().map_err(|_| Error::Disconnected)?.version;
        Ok(if version == 2 {
            wire::MAX_REASSEMBLED_BYTES
        } else {
            wire::MAX_FRAME_BYTES - 1
        })
    }

    pub(crate) fn send(&self, frame: Value) -> Result<()> {
        self.send_packet(Packet::new(frame)?)
    }

    pub(crate) fn admit_command(&self) -> Result<OwnedSemaphorePermit> {
        Arc::clone(&self.0.inflight)
            .try_acquire_owned()
            .map_err(|_| Error::Disconnected)
    }

    pub(crate) fn send_packet(&self, packet: Packet) -> Result<()> {
        if self.is_closed() {
            return Err(Error::Disconnected);
        }
        let version = self.0.book.lock().map_err(|_| Error::Disconnected)?.version;
        let bytes = if !matches!(self.0.link, Link::Stdio)
            && version == 2
            && packet.bytes > wire::MAX_FRAME_BYTES
        {
            // 4/3 base64 expansion and bounded per-fragment JSON envelopes.
            packet.bytes.div_ceil(3).saturating_mul(4).saturating_add(
                packet
                    .bytes
                    .div_ceil(wire::MAX_FRAME_BYTES / 4)
                    .saturating_mul(256),
            )
        } else {
            packet.bytes
        };
        let result = u32::try_from(bytes)
            .ok()
            .and_then(|bytes| Arc::clone(&self.0.bytes).try_acquire_many_owned(bytes).ok())
            .is_some_and(|permit| {
                self.0
                    .outbound
                    .try_send(Outgoing::Frame {
                        packet,
                        version,
                        _bytes: permit,
                    })
                    .is_ok()
            });
        if result {
            Ok(())
        } else {
            self.close();
            Err(Error::Disconnected)
        }
    }

    pub(crate) async fn end_input(&self) -> Result<()> {
        timeout(REQUEST_TIMEOUT, self.0.outbound.send(Outgoing::EndInput))
            .await
            .map_err(|_| Error::TimedOut)?
            .map_err(|_| Error::Disconnected)
    }

    pub(crate) async fn request(&self, frame: Value) -> Result<Value> {
        self.request_with(frame, &new_id(), Some(REQUEST_TIMEOUT))
            .await
    }

    pub(crate) async fn request_with(
        &self,
        frame: Value,
        id: &str,
        wait: Option<Duration>,
    ) -> Result<Value> {
        self.begin_request(frame, id)?.response(wait).await
    }

    pub(crate) fn begin_request<'a>(
        &'a self,
        mut frame: Value,
        id: &'a str,
    ) -> Result<Request<'a>> {
        frame["id"] = Value::String(id.to_owned());
        let packet = Packet::new(frame)?;
        if packet.bytes > REQUEST_BYTES.saturating_add(1) {
            self.close();
            return Err(Error::Disconnected);
        }
        let (sender, receiver) = oneshot::channel();
        {
            let mut book = self.0.book.lock().map_err(|_| Error::Disconnected)?;
            if book.pending.len() >= QUEUE_FRAMES || book.pending.contains_key(id) {
                drop(book);
                self.close();
                return Err(Error::Disconnected);
            }
            book.pending.insert(id.to_owned(), sender);
        }
        let pending = Pending { channel: self, id };
        self.send_packet(packet)?;
        Ok(Request {
            receiver,
            _pending: pending,
        })
    }
}

pub(crate) struct Request<'a> {
    receiver: oneshot::Receiver<Value>,
    _pending: Pending<'a>,
}

impl Request<'_> {
    pub(crate) async fn response(self, wait: Option<Duration>) -> Result<Value> {
        match wait {
            Some(duration) => timeout(duration, self.receiver)
                .await
                .map_err(|_| Error::TimedOut)?,
            None => self.receiver.await,
        }
        .map_err(|_| Error::Disconnected)
    }
}

struct Pending<'a> {
    channel: &'a Channel,
    id: &'a str,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if let Ok(mut book) = self.channel.0.book.lock() {
            book.pending.remove(self.id);
        }
    }
}

pub(crate) fn new_id() -> String {
    Uuid::new_v4().to_string()
}
