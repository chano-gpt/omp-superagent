use super::*;
use serde_json::{Value, json};

fn route() -> Route {
    Route {
        account: Id::parse("11").unwrap(),
        owner: Id::parse("22").unwrap(),
        guild: Id::parse("33").unwrap(),
        text: Id::parse("44").unwrap(),
    }
}

fn card(offer: &Offer) -> Value {
    json!({"id":"505","author":{"id":"11","bot":true},"channel_id":"44",
        "content":offer.content,"timestamp":"2026-01-01T00:00:00.000Z"})
}

fn reply() -> Value {
    json!({"id":"606","author":{"id":"22"},"channel_id":"44","guild_id":"33",
        "content":"/voice confirm","timestamp":"2026-01-01T00:00:01Z","type":19,
        "message_reference":{"message_id":"505","channel_id":"44","guild_id":"33"}})
}

fn verified(offer: &Offer) -> Delivery {
    let receipt: Receipt = serde_json::from_value(card(offer)).unwrap();
    Delivery {
        id: offer.id,
        posted: Some(
            receipt
                .verify(offer.route, &offer.content, offer.text.clone())
                .unwrap(),
        ),
    }
}

#[test]
fn only_original_owner_reply_on_exact_card_can_submit_once() {
    let mut drafts = Drafts::default();
    let offer = drafts
        .begin(route(), "/restart is the term to find")
        .unwrap();
    assert!(
        drafts
            .consume(&serde_json::from_value(reply()).unwrap())
            .is_none()
    );
    assert!(drafts.finish(verified(&offer), None));
    for (key, change) in [
        ("author", json!({"id":"333"})),
        ("guild_id", json!("55")),
        ("channel_id", json!("55")),
        ("edited_timestamp", json!("2026-01-01T00:00:01Z")),
        ("webhook_id", json!("77")),
        ("timestamp", json!("2025-12-31T23:59:59Z")),
        (
            "message_reference",
            json!({"message_id":"505","channel_id":"99","guild_id":"33"}),
        ),
        (
            "message_reference",
            json!({"message_id":"505","channel_id":"44","guild_id":"33","type":1}),
        ),
        ("message_snapshots", json!([{}])),
        ("content", json!(" /voice confirm ")),
    ] {
        let mut message = reply();
        message[key] = change;
        assert!(
            drafts
                .consume(&serde_json::from_value(message).unwrap())
                .is_none(),
            "{key}"
        );
    }
    let confirmation = serde_json::from_value(reply()).unwrap();
    let work = drafts.consume(&confirmation).unwrap();
    assert!(work.starts_with(
        "Work proposed in a live Discord voice session and confirmed by the owner in writing."
    ));
    assert!(work.ends_with("\n\n/restart is the term to find"));
    assert!(drafts.consume(&confirmation).is_none());
}

#[test]
fn verification_or_revocation_failure_cannot_activate_a_draft() {
    let mut drafts = Drafts::default();
    let mut offer = drafts.begin(route(), "Check the release").unwrap();
    for (key, change) in [
        ("author", json!({"id":"111","bot":true})),
        ("author", json!({"id":"11","bot":false})),
        ("guild_id", json!("55")),
        ("channel_id", json!("55")),
        ("content", json!("wrong card")),
        ("timestamp", json!("invalid date")),
    ] {
        let mut receipt = card(&offer);
        receipt[key] = change;
        let receipt: Receipt = serde_json::from_value(receipt).unwrap();
        assert!(
            receipt
                .verify(route(), &offer.content, offer.text.clone())
                .is_err(),
            "{key}"
        );
    }
    assert!(!drafts.finish(
        Delivery {
            id: offer.id,
            posted: None
        },
        None
    ));
    assert!(
        drafts
            .begin(route(), "Do not retry ambiguous delivery")
            .is_err()
    );
    drafts.clear();
    offer = drafts.begin(route(), "Check the release").unwrap();
    drafts.clear();
    assert!(
        offer
            .cancelled
            .try_recv()
            .is_err_and(|error| matches!(error, oneshot::error::TryRecvError::Closed))
    );
    assert!(!drafts.finish(verified(&offer), None));
    assert!(
        drafts
            .consume(&serde_json::from_value(reply()).unwrap())
            .is_none()
    );
}

#[test]
fn isolated_literal_never_forms_a_second_discord_fence() {
    let card = render("Text ``` <@1234>\u{202e} \u{2060} \\ draft").unwrap();
    assert_eq!(card.matches("```").count(), 2);
    assert!(card.contains("Text \\u{60}\\u{60}\\u{60} <@1234>\\u{202e} \\u{2060} \\u{5c} draft"));
    assert!(render(&"😀".repeat(1_001)).is_err());
    assert!(render(&"`".repeat(400)).is_err());
}

#[tokio::test]
async fn actual_rest_create_receipt_publishes_one_card_and_ignores_late_acks() {
    use axum::{Json, Router, extract::State as AxumState, routing::post};
    use tokio::{net::TcpListener, sync::mpsc, time::timeout};

    async fn discord(
        AxumState(deliver): AxumState<mpsc::Sender<(Value, oneshot::Sender<()>)>>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        let (accept, accepted) = oneshot::channel();
        deliver.send((body.clone(), accept)).await.unwrap();
        match accepted.await {
            Ok(()) | Err(_) => {}
        }
        Json(
            json!({"id":"505","author":{"id":"11","bot":true},"channel_id":"44",
            "content":body["content"],"timestamp":"2026-01-01T00:00:00.000Z"}),
        )
    }

    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (deliver, mut delivered) = mpsc::channel(1);
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
        .unwrap();
    let mut rest = Rest::new(http, "offline-only".into()).unwrap();
    rest.api = format!("http://{}", socket.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            socket,
            Router::new()
                .route("/channels/44/messages", post(discord))
                .with_state(deliver),
        )
        .await
        .unwrap();
    });

    timeout(Duration::from_secs(3), async {
        for revoke in [true, false] {
            let mut drafts = Drafts::default();
            let offer = drafts
                .begin(route(), "Review <@222> with no notification")
                .unwrap();
            let send = offer.publish(&rest, Instant::now() + Duration::from_secs(2));
            let observe = async {
                let (body, accept) = delivered.recv().await.unwrap();
                assert_eq!(
                    body["allowed_mentions"],
                    json!({"parse": [], "replied_user": false})
                );
                assert_eq!(body["flags"], 4);
                assert!(
                    body["content"]
                        .as_str()
                        .unwrap()
                        .contains("Review <@222> with no notification")
                );
                if revoke {
                    drafts.clear();
                }
                match accept.send(()) {
                    Ok(()) | Err(()) => {}
                }
            };
            let (receipt, ()) = tokio::join!(send, observe);
            assert_eq!(drafts.finish(receipt, None), !revoke);
            let confirm = serde_json::from_value(reply()).unwrap();
            assert_eq!(drafts.consume(&confirm).is_some(), !revoke);
        }
    })
    .await
    .unwrap();
    server.abort();
}
