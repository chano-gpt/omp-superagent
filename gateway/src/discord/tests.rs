use std::path::PathBuf;
use std::sync::{Arc, Mutex as SyncMutex};
use std::time::Duration;

use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State as AxumState},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
    time,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

use super::*;
use crate::{
    adapter::Hub,
    store::{GatewayReceipt, Store},
};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("omp-discord-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).expect("create isolated Gateway state");
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("Could not remove Discord test state: {error}");
        }
    }
}

#[derive(Clone)]
struct RestFixture {
    socket: String,
    emitted: mpsc::UnboundedSender<(String, Value)>,
    last_card: Arc<SyncMutex<Value>>,
}

async fn rest(AxumState(fixture): AxumState<RestFixture>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let method = request.method().as_str().to_owned();
    let authorized = request
        .headers()
        .get("authorization")
        .is_some_and(|value| value == "Bot offline-token");
    let body: Value =
        serde_json::from_slice(&to_bytes(request.into_body(), 64 * 1024).await.unwrap())
            .unwrap_or(Value::Null);
    if path.starts_with("/interactions/") {
        assert!(
            !authorized,
            "the interaction token does not authorize sending a bot credential"
        );
        fixture.emitted.send((path, body)).unwrap();
        return StatusCode::NO_CONTENT.into_response();
    }
    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let result = match (method.as_str(), path.as_str()) {
        ("GET", "/gateway/bot") => json!({"url": fixture.socket, "shards": 1,
            "session_start_limit":{"total":1000,"remaining":1000,"reset_after":86_400_000_u64,"max_concurrency":1}}),
        ("GET", "/channels/90") => {
            json!({"id":"90", "guild_id":"77", "type":11, "parent_id":"88", "name":"task thread"})
        }
        ("GET", "/channels/90/messages/206") => {
            json!({"id":"206","channel_id":"90","author":{"id":"42"},
            "type":0,"content":"edited by owner", "edited_timestamp":"2026-11-21T01:02:03.456000+00:00"})
        }
        ("GET", "/channels/89") => json!({"id":"89", "guild_id":"77", "type":0}),
        ("GET", "/channels/99") => json!({"id":"99", "type":1, "recipients":[{"id":"42"}]}),
        ("GET", "/channels/98") => {
            json!({"id":"98", "type":3, "recipients":[{"id":"42"},{"id":"43"}]})
        }
        ("POST", "/channels/99/messages") => {
            fixture.emitted.send((path, body.clone())).unwrap();
            if body["content"] == "ambiguous" {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            let remote = json!({"id":"700", "channel_id":"99", "author":{"id":"71","bot":true},
                "components":body.get("components").cloned().unwrap_or_else(|| json!([])) });
            *fixture.last_card.lock().unwrap() = remote.clone();
            remote
        }
        ("GET", "/channels/99/messages/700") => fixture.last_card.lock().unwrap().clone(),
        ("PATCH", "/channels/99/messages/700") => {
            let mut card = fixture.last_card.lock().unwrap();
            card["components"] = body["components"].clone();
            fixture.emitted.send((path, body)).unwrap();
            card.clone()
        }
        ("PUT", path) if path.contains("/reactions/") => {
            return StatusCode::NO_CONTENT.into_response();
        }
        _ => return (StatusCode::NOT_FOUND, Json(json!({"code":10003}))).into_response(),
    };
    Json(result).into_response()
}

type FixtureSocket = WebSocketStream<TcpStream>;

async fn wire(ws: &mut FixtureSocket, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

async fn dispatch(ws: &mut FixtureSocket, sequence: u64, event: &str, data: Value) {
    wire(ws, json!({"op":0, "s":sequence, "t":event, "d":data})).await;
}

async fn identify(listener: &TcpListener) -> FixtureSocket {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = accept_async(stream).await.unwrap();
    wire(&mut ws, json!({"op":10,"d":{"heartbeat_interval":60_000}})).await;
    let identify: Value =
        serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(identify["op"], 2);
    let intents = identify["d"]["intents"].as_u64().unwrap();
    assert_eq!(
        intents & (1 << 7),
        0,
        "voice intent must not be enabled when voice is not configured"
    );
    assert_eq!(
        intents & (1 | 1 << 9 | 1 << 12 | 1 << 15),
        1 | 1 << 9 | 1 << 12 | 1 << 15
    );
    dispatch(
        &mut ws,
        1,
        "READY",
        json!({"user":{"id":"71","bot":true},
        "application":{"id":"72"}, "session_id":"test-session",
        "resume_gateway_url": format!("ws://{}/", listener.local_addr().unwrap())}),
    )
    .await;
    ws
}

async fn received(emitted: &mut mpsc::UnboundedReceiver<(String, Value)>, expected: &str) -> Value {
    let (path, body) = time::timeout(Duration::from_secs(5), emitted.recv())
        .await
        .expect("Discord REST operation never arrived")
        .expect("Discord REST recorder closed");
    assert_eq!(path, expected);
    body
}

async fn event(hub: &Hub, store: &Store) -> Value {
    time::timeout(Duration::from_secs(5), async {
        loop {
            let changed = hub.changed.notified();
            if let Some(next) = store.next(None).unwrap() {
                store
                    .receipt(next.sequence, GatewayReceipt::Accepted)
                    .unwrap();
                return next.event;
            }
            changed.await;
        }
    })
    .await
    .expect("Discord event was not admitted")
}

fn message(id: &str, guild: Option<&str>, channel: &str, owner: &str, text: &str) -> Value {
    let mut payload =
        json!({"id":id,"channel_id":channel,"author":{"id":owner},"type":0,"content":text});
    if let Some(guild) = guild {
        payload["guild_id"] = json!(guild);
    }
    payload
}

fn interaction(id: &str, kind: u8, data: Value) -> Value {
    json!({"application_id":"72","user":{"id":"42"},"channel":{"id":"99","type":1},
        "token":"offline-interaction","type":kind,"id":id,"data":data})
}

#[tokio::test]
async fn websocket_ingress_and_rest_cards_keep_original_discord_identity() {
    let temporary = Temp::new();
    let store = Arc::new(Store::open(&temporary.0).unwrap());
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", http.local_addr().unwrap());
    let (emitted, mut observed) = mpsc::unbounded_channel();
    let last_card = Arc::new(SyncMutex::new(Value::Null));
    let router = Router::new().fallback(rest).with_state(RestFixture {
        socket: format!("ws://{}/", ws.local_addr().unwrap()),
        emitted,
        last_card: Arc::clone(&last_card),
    });
    let server: JoinHandle<()> = tokio::spawn(async move {
        axum::serve(http, router).await.unwrap();
    });
    let hub = Hub::with_factory(&json!({"discord":{}}), Arc::clone(&store), |_, _, ctx| {
        let mut rest = Rest::new(ctx.http.clone(), "offline-token".to_owned())?;
        rest.api.clone_from(&api);
        Ok(Box::new(Discord {
            state: Arc::new(State {
                ctx,
                rest,
                config: Config {
                    token_env: String::new(),
                    owner_id: "42".to_owned(),
                    allowed_guild_ids: HashSet::from(["77".to_owned()]),
                    allowed_channel_ids: HashSet::from(["88".to_owned()]),
                },
                stopping: AtomicBool::new(false),
                identity: OnceLock::new(),
                channels: RwLock::default(),
                voice: None,
                cards: Cards::default(),
                purpose_lock: Mutex::new(()),
            }),
            task: Mutex::new(None),
        }))
    })
    .unwrap();
    let (started, mut socket) = time::timeout(Duration::from_secs(5), async {
        tokio::join!(hub.start(), identify(&ws))
    })
    .await
    .unwrap();
    started.unwrap();
    dispatch(
        &mut socket,
        2,
        "GUILD_CREATE",
        json!({"id":"77",
        "channels":[{"id":"88","type":0,"name":"parent","topic":null},{"id":"89","type":0}],
        "threads":[{"id":"90","type":11,"parent_id":"88","name":"task thread"}]}),
    )
    .await;
    for sequence in 3..1203 {
        if sequence % 2 == 0 {
            dispatch(
                &mut socket,
                sequence,
                "MESSAGE_UPDATE",
                json!({"channel_id":"89","id":sequence.to_string(),
                "edited_timestamp":"2026-11-21T01:02:03.456000+00:00"}),
            )
            .await;
        } else {
            dispatch(
                &mut socket,
                sequence,
                "MESSAGE_CREATE",
                message(
                    &sequence.to_string(),
                    Some("77"),
                    "90",
                    "43",
                    "unrelated chatter",
                ),
            )
            .await;
        }
    }
    dispatch(
        &mut socket,
        1203,
        "MESSAGE_CREATE",
        message("201", Some("77"), "90", "42", "owner thread"),
    )
    .await;
    dispatch(
        &mut socket,
        1204,
        "INTERACTION_CREATE",
        interaction("501", 2, json!({"type":1,"name":"status"})),
    )
    .await;
    assert_eq!(
        received(
            &mut observed,
            "/interactions/501/offline-interaction/callback"
        )
        .await["data"]["flags"],
        64
    );
    assert_eq!(
        store.pending().unwrap(),
        0,
        "native approval barrier precedes normal intake"
    );
    hub.ready.send_replace(true);
    let thread = event(&hub, &store).await;
    assert_eq!(thread["input"]["route"]["threadId"], "90");
    assert_eq!(thread["input"]["route"]["channelId"], "88");
    assert_eq!(
        thread["input"]["discordContext"]["thread"]["name"],
        "task thread"
    );
    dispatch(
        &mut socket,
        1205,
        "MESSAGE_CREATE",
        message("202", Some("77"), "90", "43", "wrong owner"),
    )
    .await;
    dispatch(
        &mut socket,
        1206,
        "MESSAGE_CREATE",
        message("203", Some("77"), "89", "42", "wrong parent"),
    )
    .await;
    dispatch(
        &mut socket,
        1207,
        "MESSAGE_UPDATE",
        json!({"channel_id":"90","id":"206",
        "edited_timestamp":"2026-11-21T01:02:03.456000+00:00"}),
    )
    .await;
    let edit = event(&hub, &store).await;
    assert_eq!(edit["input"]["messageId"], "206");
    assert_eq!(edit["input"]["text"], "edited by owner");
    assert_eq!(edit["input"]["route"]["threadId"], "90");
    dispatch(
        &mut socket,
        1208,
        "MESSAGE_CREATE",
        message("204", None, "98", "42", "group DM"),
    )
    .await;
    dispatch(
        &mut socket,
        1209,
        "MESSAGE_CREATE",
        message("205", None, "99", "42", "owner DM"),
    )
    .await;
    let dm = event(&hub, &store).await;
    assert_eq!(
        dm["input"]["messageId"], "205",
        "foreign events never enter ordered durable intake"
    );
    let mut route = dm["input"]["route"].clone();
    route["topicId"] = json!("original");
    dispatch(
        &mut socket,
        1210,
        "INTERACTION_CREATE",
        interaction(
            "502",
            2,
            json!({"type":1,"name":"compact",
        "options":[{"type":3,"name":"text","value":" details "}]}),
        ),
    )
    .await;
    received(
        &mut observed,
        "/interactions/502/offline-interaction/callback",
    )
    .await;
    let command = event(&hub, &store).await;
    assert_eq!(command["input"]["interactionId"], "502");
    assert_eq!(command["input"]["messageId"], Value::Null);
    assert_eq!(command["input"]["text"], "/compact details");
    assert!(!command.to_string().contains("offline-interaction"));

    let approval = json!({"id":"approval","target":"main","generation":1,"requestId":"request",
        "route":route,"state":"pending","remoteId":null,
        "request":{"method":"confirm","title":"Check intent","message":"Allow?"}});
    assert_eq!(
        hub.effect("showApproval", &[approval]).await.unwrap(),
        "700"
    );
    let outgoing = received(&mut observed, "/channels/99/messages").await;
    let button = outgoing["components"][0]["components"][0]["custom_id"].clone();
    let body = json!({"custom_id":button,"component_type":2});
    let mut forged = interaction("503", 3, body.clone());
    let card = last_card.lock().unwrap().clone();
    forged["message"] = card.clone();
    forged["message"]["id"] = json!("701");
    dispatch(&mut socket, 1211, "INTERACTION_CREATE", forged).await;
    assert_eq!(
        received(
            &mut observed,
            "/interactions/503/offline-interaction/callback"
        )
        .await["type"],
        4
    );
    assert_eq!(store.pending().unwrap(), 0);
    let mut genuine = interaction("504", 3, body);
    genuine["message"] = card;
    dispatch(&mut socket, 1212, "INTERACTION_CREATE", genuine).await;
    assert_eq!(
        received(
            &mut observed,
            "/interactions/504/offline-interaction/callback"
        )
        .await["type"],
        6
    );
    let answer = event(&hub, &store).await;
    assert_eq!(answer["approvalId"], "approval");
    assert_eq!(answer["route"], route);
    assert_eq!(answer["response"], json!({"confirmed": true}));
    received(&mut observed, "/channels/99/messages/700").await;

    let send = hub
        .effect(
            "send",
            &[json!({"id":"output","route":route,"text":"ambiguous"})],
        )
        .await;
    assert_eq!(send.unwrap_err().certainty, "uncertain");
    received(&mut observed, "/channels/99/messages").await;
    assert!(
        observed.try_recv().is_err(),
        "ambiguous REST effect must not be retried in the adapter"
    );

    wire(&mut socket, json!({"op":7})).await;
    let (stream, _) = time::timeout(Duration::from_secs(5), ws.accept())
        .await
        .unwrap()
        .unwrap();
    let mut resumed = accept_async(stream).await.unwrap();
    wire(
        &mut resumed,
        json!({"op":10,"d":{"heartbeat_interval":60_000}}),
    )
    .await;
    let frame = time::timeout(Duration::from_secs(5), resumed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let resume: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
    assert_eq!(resume["op"], 6);
    assert_eq!(resume["d"]["session_id"], "test-session");
    assert_eq!(resume["d"]["seq"], 1212);
    dispatch(
        &mut resumed,
        1213,
        "MESSAGE_CREATE",
        message("205", None, "99", "42", "owner DM"),
    )
    .await;
    dispatch(
        &mut resumed,
        1214,
        "MESSAGE_CREATE",
        message("207", None, "99", "42", "while offline"),
    )
    .await;
    dispatch(&mut resumed, 1215, "RESUMED", json!({"trace":[]})).await;
    assert_eq!(event(&hub, &store).await["input"]["messageId"], "207");
    assert_eq!(event(&hub, &store).await["kind"], "reconnect");
    hub.stop().await.unwrap();
    server.abort();
}
