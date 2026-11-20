use super::*;

#[tokio::test]
async fn slack_modal_and_rehydrated_card_keep_their_original_route_and_generation() {
    let mut fixture = Fixture::new().await;
    let (mut peer, result) = fixture.start("ALOCAL").await;
    result.expect("matching Slack application");
    fixture.hub.ready.send(true).expect("open native readiness");
    let route = json!({
        "platform":"slack", "accountId":"TLOCAL:UBOT", "authorityId":"UOWNER", "guildId":null,
        "channelId":"DOWNER", "threadId":"1000.000001", "topicId":"original-logical-topic",
    });
    let mut approval = json!({
        "id":"original-approval", "target":"head", "generation":7, "requestId":"original-native-request",
        "route": route, "state":"pending", "remoteId":null,
        "request":{"method":"editor","title":"Review <!channel>", "prefill":"private prefill"},
    });
    let receipt = fixture
        .hub
        .effect("showApproval", &[approval.clone()])
        .await
        .expect("show card");
    assert_eq!(receipt, "2000.000001");
    approval["remoteId"] = receipt.clone();
    let posted = fixture.called("chat.postMessage").await;
    assert_eq!(posted["thread_ts"], route["threadId"]);
    assert_eq!(posted["blocks"][0]["text"]["type"], "plain_text");
    assert!(!posted.to_string().contains("private prefill"));
    let card = fixture.state.card.lock().await.clone();
    let action = card["blocks"][1]["elements"][0]["action_id"].clone();
    // Expected identity is sha256(JSON.stringify([id, generation, target, requestId, routeKey])), truncated.
    let correlation = "omp:6a38ce570f9520b4762b9fc6:open";
    assert_eq!(action, correlation);

    let mut forged = approval.clone();
    forged["generation"] = json!(8);
    assert!(fixture.hub.effect("showApproval", &[forged]).await.is_err());
    let card = fixture.state.card.lock().await.clone();
    let open = json!({
        "type":"interactive","envelope_id":"open","payload":{
            "type":"block_actions","api_app_id":"ALOCAL","team":{"id":"TLOCAL"},"user":{"id":"UOWNER"},
            "channel":{"id":"DOWNER"},"container":{"type":"message","channel_id":"DOWNER","message_ts":receipt},
            "message":card,"actions":[{"type":"button","action_ts":"2001.000001","action_id":action}],
            "trigger_id":"open-private-capability",
        },
    });
    send(&mut peer, open).await;
    assert_eq!(ack(&mut peer).await, json!({"envelope_id":"open"}));
    let opened = fixture.called("views.open").await;
    let view = &opened["view"];
    assert_eq!(
        view["blocks"][0]["element"]["initial_value"],
        "private prefill"
    );

    let make_submit = |id, metadata: Value| {
        json!({
            "type":"interactive", "envelope_id":id, "payload":{
                "type":"view_submission", "team":{"id":"TLOCAL"}, "user":{"id":"UOWNER"},
                "view":{
                    "callback_id":view["callback_id"], "id":"VMODAL","private_metadata":metadata,
                    "state":{"values":{"response":{"value":{"type":"plain_text_input","value":"  exact private reply\n"}}}},
                },
            },
        })
    };
    send(&mut peer, make_submit("forged", json!("forged-metadata"))).await;
    assert_eq!(ack(&mut peer).await, json!({"envelope_id":"forged"}));
    assert!(
        fixture
            .store
            .next(None)
            .expect("look for forged answer")
            .is_none()
    );
    send(
        &mut peer,
        make_submit("submitted", view["private_metadata"].clone()),
    )
    .await;
    assert_eq!(ack(&mut peer).await, json!({"envelope_id":"submitted"}));
    let row = fixture
        .store
        .next(None)
        .expect("read durable answer")
        .expect("admit linked modal");
    assert_eq!(
        row.event,
        json!({"kind":"answer", "approvalId":"original-approval",
        "response":{"value":"  exact private reply\n"}, "route":route})
    );
    let updated = fixture.called("chat.update").await;
    assert!(
        updated["blocks"]
            .as_array()
            .expect("Slack card blocks")
            .iter()
            .all(|block| block["type"] != "actions")
    );
    assert!(!updated.to_string().contains("exact private reply"));
    fixture.hub.stop().await.expect("stop Slack adapter");

    // A durably queued answer prevents callbacks or a second card after restart.
    let (mut replacement, result) = fixture.start("ALOCAL").await;
    result.expect("restart same Slack account");
    assert_eq!(
        fixture
            .hub
            .effect("showApproval", &[approval])
            .await
            .expect("resolve authoritative original card"),
        receipt
    );
    send(
        &mut replacement,
        make_submit("retried", view["private_metadata"].clone()),
    )
    .await;
    assert_eq!(
        ack(&mut replacement).await,
        json!({"envelope_id":"retried"})
    );
    assert_eq!(
        fixture.store.pending().expect("exact pending answer count"),
        1
    );
    fixture
        .hub
        .stop()
        .await
        .expect("stop restarted Slack adapter");
}
