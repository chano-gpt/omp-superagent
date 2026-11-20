use super::*;
use serde_json::json;

#[test]
fn start_refuses_edited_forwarded_or_preexisting_messages() {
    let config = Config::parse(&json!({
        "tokenEnv":"DISCORD", "ownerId":"22", "allowedGuildIds":["33"], "allowedChannelIds":["44"],
        "voice":{"auth":"subscription","channelId":"55","textChannelId":"44","voice":"cove","maxSessionSeconds":60}
    })).unwrap();
    let after = timestamp("2026-01-01T00:00:00.001Z").unwrap();
    let original = json!({"id":"66","author":{"id":"22"},"channel_id":"44","guild_id":"33",
        "content":"/voice start", "timestamp":"2026-01-01T00:00:01.111Z","type":0});
    let account = Id::parse("11");
    assert!(
        config
            .authorize(
                &serde_json::from_value(original.clone()).unwrap(),
                account,
                after
            )
            .is_some()
    );
    for (key, change) in [
        ("author", json!({"id":"222"})),
        ("author", json!({"id":"22","bot":true})),
        ("guild_id", json!("12")),
        ("guild_id", Value::Null),
        ("channel_id", json!("55")),
        ("webhook_id", json!("77")),
        ("edited_timestamp", json!("2026-01-01T00:00:02Z")),
        ("timestamp", json!("2026-01-01T00:00:00Z")),
        ("attachments", json!([{}])),
        ("message_reference", json!({"channel_id":"44","type":1})),
        ("message_snapshots", json!([{}])),
    ] {
        let mut message = original.clone();
        message[key] = change;
        assert!(
            config
                .authorize(&serde_json::from_value(message).unwrap(), account, after)
                .is_none(),
            "{key}"
        );
    }
}

#[test]
fn occupant_absent_from_member_cache_blocks_join_and_media() {
    let id = |s| Id::parse(s).unwrap();
    let route = Route {
        account: id("11"),
        owner: id("22"),
        guild: id("33"),
        text: id("44"),
    };
    let channel = id("55");
    let mut room = Room::default();
    assert!(!room.private_before_join(route, channel));
    let states = [
        json!({"user_id":"22","channel_id":"55","session_id":"owner"}),
        json!({"user_id":"99","channel_id":"55","session_id":"other"}),
    ];
    room.snapshot(route.guild, Some(&states));
    assert!(!room.private_before_join(route, channel));
    room.update(
        &serde_json::from_value(json!({
            "guild_id":"33","user_id":"99","channel_id":null,"session_id":"other"
        }))
        .unwrap(),
    );
    assert!(room.private_before_join(route, channel));
    room.update(
        &serde_json::from_value(json!({
            "guild_id":"33","user_id":"11","channel_id":"55","session_id":"bot"
        }))
        .unwrap(),
    );
    assert!(room.private_after_join(route, channel));
    room.update(
        &serde_json::from_value(json!({
            "guild_id":"33","user_id":"99","channel_id":"55","session_id":"other"
        }))
        .unwrap(),
    );
    assert!(!room.private_after_join(route, channel));
}
