use std::sync::atomic::Ordering;

use anyhow::{Context as _, Result, bail};
use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::store::{GatewayReceipt, answer_digest};

use super::{Call, CardFault, Fixture, route, test_hub};

fn approval() -> Value {
    json!({"id":"a-original","target":"main","generation":3,"requestId":"native-request",
        "route":route(),"request":{"method":"confirm","title":"Original","message":"Proceed?"},
        "state":"pending","remoteId":null})
}

#[tokio::test]
async fn inconclusive_http_client_error_cannot_send_a_second_approval_card() -> Result<()> {
    for body in [
        "<html>Proxy failure</html>",
        r#"{"ok":false,"description":"missing error code"}"#,
        r#"{"ok":false,"error_code":403,"description":"contradicts HTTP 400"}"#,
        r#"{"ok":false,"error_code":400,"result":true}"#,
        r#"{"ok":true,"result":true}"#,
    ] {
        let fixture = Fixture::start().await?;
        *fixture.remote.card_fault.lock().expect("card fault") = Some(CardFault {
            status: StatusCode::BAD_REQUEST,
            body,
            apply: true,
        });
        let approval = [approval()];
        let error = fixture
            .hub
            .effect("showApproval", &approval)
            .await
            .expect_err("unconfirmed first send");
        assert_eq!(error.certainty, "uncertain", "envelope: {body}");
        let retry = fixture
            .hub
            .effect("showApproval", &approval)
            .await
            .expect_err("no replacement card");
        assert_eq!(retry.certainty, "uncertain", "envelope: {body}");
        assert_eq!(fixture.remote.card_writes.load(Ordering::Relaxed), 1);
        fixture.hub.stop().await?;
    }
    Ok(())
}

#[tokio::test]
async fn authoritative_telegram_rejection_can_release_an_initial_card_attempt() -> Result<()> {
    let fixture = Fixture::start().await?;
    *fixture.remote.card_fault.lock().expect("card fault") = Some(CardFault {
        status: StatusCode::TOO_MANY_REQUESTS,
        body: r#"{"ok":false,"error_code":429,"parameters":{"retry_after":2}}"#,
        apply: false,
    });
    let approval = [approval()];
    let error = fixture
        .hub
        .effect("showApproval", &approval)
        .await
        .expect_err("rejected send");
    assert_eq!(error.certainty, "rejected");
    assert_eq!(error.retry_after_ms, Some(2000));
    assert_eq!(fixture.remote.card_writes.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.hub.effect("showApproval", &approval).await?, "123");
    assert_eq!(fixture.remote.card_writes.load(Ordering::Relaxed), 2);
    fixture.hub.stop().await?;
    Ok(())
}

#[tokio::test]
async fn callback_recovery_reads_exact_durable_answer_even_after_head_delivery() -> Result<()> {
    enum Witness {
        Exact,
        WrongPayload,
        WrongRoute,
    }
    for witness in [Witness::Exact, Witness::WrongPayload, Witness::WrongRoute] {
        let mut fixture = Fixture::start().await?;
        let approval = [approval()];
        assert_eq!(fixture.hub.effect("showApproval", &approval).await?, "123");
        let card = fixture
            .remote
            .card
            .lock()
            .expect("card state")
            .clone()
            .context("original card")?;
        let data = card["reply_markup"]["inline_keyboard"][0][0]["callback_data"]
            .as_str()
            .context("confirm action")?;
        let token = data.split(':').nth(1).context("original card token")?;
        let update = json!({"update_id":4,"callback_query":{
            "id":"crash-window","from":{"id":42,"is_bot":false},"data":data,"message":card
        }});

        fixture.hub.ready.send_replace(true);
        let (_, poll) = fixture.poll().await?;
        poll.send(json!([update.clone()])).expect("poll receiver");
        fixture.feedback("crash-window", false).await?;
        let (_, _outstanding) = fixture.poll().await?;
        let admitted = fixture
            .store
            .next(None)?
            .context("durable callback answer")?;
        fixture
            .store
            .receipt(admitted.sequence, GatewayReceipt::Accepted)?;
        fixture.hub.stop().await?;

        // Recreate the persisted cut where the actual answer insert reached
        // the head but the adapter's post-insert marker and offset did not.
        let key = format!("telegram:900:approval:{token}");
        let mut progress: Value =
            serde_json::from_str(&fixture.store.get_meta(&key)?.context("approval progress")?)?;
        assert_eq!(
            progress["binding"]["responseHash"],
            answer_digest(&admitted.event["response"])?
        );
        progress["answerAdmitted"] = json!(false);
        progress["ackAttempted"] = json!(false);
        match witness {
            Witness::Exact => {}
            Witness::WrongPayload => {
                progress["binding"]["responseHash"] =
                    json!(answer_digest(&json!({"confirmed":false}))?);
            }
            Witness::WrongRoute => {
                // Still the same physical card, but not the stored answer's
                // full source route.
                progress["binding"]["route"]["topicId"] = json!("other-topic");
            }
        }
        fixture.store.set_meta(&key, &progress.to_string())?;
        fixture.store.set_meta("telegram:900:offset", "4")?;
        fixture.hub = test_hub(&fixture.store, &fixture.base).await?;
        fixture.hub.ready.send_replace(true);
        let (first, poll) = fixture.poll().await?;
        assert_eq!(first["offset"], 4);
        poll.send(json!([update])).expect("poll receiver");

        if matches!(witness, Witness::Exact) {
            let Call::Feedback(request, reply) = fixture.call().await? else {
                bail!("Committed answer no longer counted after HEAD receipt");
            };
            assert_eq!(request, json!({"callback_query_id":"crash-window"}));
            reply.send(json!(true)).expect("feedback receiver");
        }
        let (retry, _outstanding) = fixture.poll().await?;
        let expected = if matches!(witness, Witness::Exact) {
            5
        } else {
            4
        };
        assert_eq!(retry["offset"], expected);
        assert_eq!(
            fixture.store.get_meta("telegram:900:offset")?,
            Some(expected.to_string())
        );
        assert_eq!(fixture.store.pending()?, 0, "recovery must not readmit");
        assert_eq!(fixture.remote.card_writes.load(Ordering::Relaxed), 1);
        fixture.hub.stop().await?;
    }
    Ok(())
}
