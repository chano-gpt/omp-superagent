use anyhow::{Context as _, Result};
use serde_json::{Value, json};

use crate::store::GatewayReceipt;

use super::super::{
    ingress,
    model::{Identity, Policy},
    poll::fresh_offset,
};
use super::{Fixture, group, route};

#[tokio::test]
async fn conflicting_durable_message_keeps_its_telegram_update_unacked() -> Result<()> {
    let mut fixture = Fixture::start().await?;
    let original = json!({"kind":"message","input":{"id":"telegram:900:-100:10:original","route":route(),
        "messageId":"10","receivedAt":100000,"text":"durable original","attachments":[]}});
    assert!(fixture.store.admit(&original)?);
    fixture.hub.ready.send_replace(true);
    let (_, reply) = fixture.poll().await?;
    let conflict = json!({"message_id":10,"date":100,"chat":group(),"message_thread_id":7,
        "from":{"id":42,"is_bot":false},"text":"different payload with the same source identity"});
    reply
        .send(json!([{"update_id":4,"message":conflict}]))
        .expect("poll receiver");

    let (retry, _outstanding) = fixture.poll().await?;
    assert!(retry.get("offset").is_none());
    assert!(fixture.store.get_meta("telegram:900:offset")?.is_none());
    assert_eq!(
        fixture.store.next(None)?.context("original receipt")?.event,
        original
    );
    fixture.hub.stop().await?;
    Ok(())
}

#[tokio::test]
async fn poll_ack_only_follows_durable_identity_bound_admission() -> Result<()> {
    let mut fixture = Fixture::start().await?;
    assert!(fixture.calls.try_recv().is_err());
    fixture.hub.ready.send_replace(true);
    let (first, reply) = fixture.poll().await?;
    assert!(first.get("offset").is_none());
    let inbound = json!({"message_id":10,"date":100,"chat":group(),"message_thread_id":7,
        "from":{"id":42,"is_bot":false},"text":"/status@ROOM_bot  keep 😀",
        "entities":[{"type":"bot_command","offset":0,"length":16}]});
    reply
        .send(json!([{"update_id":4,"message":inbound}]))
        .expect("poll receiver");

    let (second, reply) = fixture.poll().await?;
    let original = fixture
        .store
        .next(None)?
        .context("Cursor advanced before durable message")?;
    assert_eq!(
        original.event,
        json!({
            "kind":"message","input":{"id":"telegram:900:-100:10:original","route":route(),
                "messageId":"10","receivedAt":100000,"text":"/status  keep 😀","attachments":[]}
        })
    );
    assert_eq!(
        fixture.store.get_meta("telegram:900:offset")?.as_deref(),
        Some("5")
    );
    assert_eq!(second["offset"], 5);
    fixture
        .store
        .receipt(original.sequence, GatewayReceipt::Accepted)?;

    let approval = json!({"id":"a-original","target":"main","generation":3,"requestId":"native-request",
        "route":route(),"request":{"method":"confirm","title":"Original","message":"Proceed?"},
        "state":"pending","remoteId":null});
    assert_eq!(
        fixture
            .hub
            .effect("showApproval", &[approval.clone()])
            .await?,
        "123"
    );
    let card = fixture
        .remote
        .card
        .lock()
        .expect("card state")
        .clone()
        .context("Telegram approval card not sent")?;
    let mut wrong = card.clone();
    wrong["message_thread_id"] = json!(8);
    let action = &card["reply_markup"]["inline_keyboard"][0][0]["callback_data"];
    let callback = |id: &str, message: Value| {
        json!({
            "id":id,"from":{"id":42,"is_bot":false},"data":action,"message":message
        })
    };
    reply
        .send(json!([
            {"update_id":5,"callback_query":callback("wrong-thread",wrong)},
            {"update_id":6,"callback_query":callback("exact-original",card.clone())},
            {"update_id":7,"callback_query":callback("exact-original",card.clone())},
        ]))
        .expect("poll receiver");
    fixture.feedback("wrong-thread", true).await?;
    fixture.feedback("exact-original", false).await?;
    let (last, _outstanding) = fixture.poll().await?;
    assert_eq!(last["offset"], 8);
    assert_eq!(
        fixture.store.get_meta("telegram:900:offset")?.as_deref(),
        Some("8")
    );
    assert_eq!(fixture.store.pending()?, 1);
    assert_eq!(
        fixture
            .remote
            .card
            .lock()
            .expect("card state")
            .as_ref()
            .context("card")?["reply_markup"]["inline_keyboard"],
        json!([])
    );
    let mut approval = approval;
    approval["remoteId"] = json!("123");
    assert_eq!(
        fixture.hub.effect("showApproval", &[approval]).await?,
        "123"
    );
    assert_eq!(
        fixture
            .remote
            .card
            .lock()
            .expect("card state")
            .as_ref()
            .context("card")?["reply_markup"]["inline_keyboard"],
        json!([]),
        "queued answer must not reopen closed controls"
    );
    fixture.hub.stop().await?;
    Ok(())
}

#[test]
fn stale_telegram_cursor_cannot_ack_a_recycled_update_sequence() {
    let now = 200_000_000;
    let cases = [
        (None, None),
        (Some(""), None),
        (Some("200000001"), None),
        (Some("113600000"), None),
        (Some("113600001"), Some(900001)),
    ];
    for (observed, wanted) in cases {
        assert_eq!(fresh_offset(Some(900001), observed, now), wanted);
    }
}

#[test]
fn edited_voice_identity_matches_the_prior_gateway_and_excludes_reply_context() -> Result<()> {
    let me: Identity =
        serde_json::from_value(json!({"id":900,"is_bot":true,"username":"room_bot"}))?;
    let policy = Policy::new("42", &["-100".to_owned()])?;
    let inbound = |duration| {
        json!({
            "message_id":10,"date":100,"edit_date":102,"chat":group(),"message_thread_id":7,
            "from":{"id":42,"is_bot":false},"caption":"edited",
            "voice":{"file_id":"voice_1","file_unique_id":"unique","duration":duration}
        })
    };
    let first = ingress::normalize(&policy, &me, &serde_json::from_value(inbound(2))?)
        .context("owner voice input rejected")?;
    let mut second = inbound(3);
    second["reply_to_message"] =
        json!({"message_id":9,"date":0,"chat":group(),"message_thread_id":7});
    let second = ingress::normalize(&policy, &me, &serde_json::from_value(second)?)
        .context("owner voice input rejected after reference snapshot changed")?;
    assert_eq!(
        first.id,
        "telegram:900:-100:10:edit:102:7255006537c4167f0667ddce"
    );
    assert_eq!(first.id, second.id);
    assert_eq!(
        serde_json::to_value(second)?["attachments"][0],
        json!({
            "id":"voice_1","url":"telegram-file:voice_1","name":"voice-unique.ogg","size":null,
            "contentType":"audio/ogg","voice":true,"durationSeconds":3.0
        })
    );
    Ok(())
}
