use std::fmt;

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use serde_json::Value;

use super::{PHYSICAL_ROUTE_FIELDS, Store, get_meta, json_equal, set_meta, string};

#[derive(Debug)]
pub(crate) struct AuthorityError(pub &'static str);

impl fmt::Display for AuthorityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for AuthorityError {}

const RESTART_SOURCE: AuthorityError = AuthorityError(
    "Gateway restart requires its original completed owner command and sent acknowledgement",
);
const RESTART_CONFLICT: AuthorityError = AuthorityError("Gateway restart identity conflict");
const PURPOSE_SOURCE: AuthorityError =
    AuthorityError("Discord purpose requires an original received owner source");

impl Store {
    pub fn validate_discord_purpose(&self, request: &Value) -> Result<()> {
        let source_id = string(request, "sourceId")?;
        let db = self.db()?;
        let tx = db.unchecked_transaction()?;
        if get_meta(&tx, &format!("user-source:{source_id}"))?.as_deref() != Some("true") {
            return Err(PURPOSE_SOURCE.into());
        }
        let row = tx
            .query_row(
                "SELECT data,state FROM inputs WHERE id=?",
                [source_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((data, state)) = row else {
            return Err(PURPOSE_SOURCE.into());
        };
        if state == "cancelled" {
            return Err(PURPOSE_SOURCE.into());
        }
        let input: Value = serde_json::from_str(&data)?;
        if input["id"] != source_id {
            return Err(AuthorityError("Discord purpose source identity mismatch").into());
        }
        let source = &input["route"];
        let destination = &request["route"];
        if source["platform"] != "discord"
            || destination["platform"] != "discord"
            || PHYSICAL_ROUTE_FIELDS
                .iter()
                .any(|key| *key != "threadId" && source[*key] != destination[*key])
            || !(destination["threadId"].is_null() || destination["threadId"] == source["threadId"])
        {
            return Err(AuthorityError(
                "Discord purpose destination is unrelated to the original source",
            )
            .into());
        }
        Ok(tx.commit()?)
    }

    pub fn get_restart(&self, request_id: Option<&str>) -> Result<Option<Value>> {
        let db = self.db()?;
        get_restart(&db, request_id)
    }

    pub fn validate_restart(&self, request: &Value) -> Result<Value> {
        let db = self.db()?;
        let tx = db.unchecked_transaction()?;
        let input = validate_restart(&tx, request)?;
        tx.commit()?;
        Ok(input)
    }

    pub fn set_restart(&self, record: &Value) -> Result<()> {
        let id = string(record, "requestId")?;
        let input_id = string(record, "inputId")?;
        let source_key = format!("gateway:restart-source:{input_id}");
        let mut db = self.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous = get_restart(&tx, Some(id))?;
        let latest = get_restart(&tx, None)?;
        let source = get_meta(&tx, &source_key)?;
        if source.as_deref().is_some_and(|source| source != id)
            || previous.as_ref().is_some_and(|previous| {
                [
                    "inputId",
                    "ackOutputId",
                    "gatewayPid",
                    "headPid",
                    "sessionId",
                    "sessionFile",
                ]
                .iter()
                .any(|key| previous.get(key) != record.get(key))
            })
        {
            return Err(RESTART_CONFLICT.into());
        }
        if previous.is_none() {
            validate_restart(&tx, record)?;
        }
        let serialized = serde_json::to_string(record)?;
        set_meta(&tx, &format!("gateway:restart:{id}"), &serialized)?;
        // A late update belongs only to its request, not to the latest receipt.
        if previous.is_none()
            || latest
                .as_ref()
                .is_none_or(|latest| latest["requestId"] == id)
        {
            set_meta(&tx, "gateway:restart", &serialized)?;
        }
        set_meta(&tx, &source_key, id)?;
        tx.commit()?;
        Ok(())
    }
}

fn get_restart(db: &Connection, request_id: Option<&str>) -> Result<Option<Value>> {
    let key = match request_id {
        Some(id) => format!("gateway:restart:{id}"),
        None => "gateway:restart".to_owned(),
    };
    match get_meta(db, &key)? {
        Some(value) => Ok(serde_json::from_str(&value)?),
        None => Ok(None),
    }
}

fn validate_restart(db: &Connection, request: &Value) -> Result<Value> {
    let input_id = string(request, "inputId")?;
    let ack_id = string(request, "ackOutputId")?;
    if get_meta(db, &format!("user-source:{input_id}"))?.as_deref() != Some("true") {
        return Err(RESTART_SOURCE.into());
    }
    let source = db
        .query_row(
            "SELECT data,target,state FROM inputs WHERE id=?",
            [input_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((data, target, state)) = source else {
        return Err(RESTART_SOURCE.into());
    };
    if target.as_deref() != Some("main") || state != "done" {
        return Err(RESTART_SOURCE.into());
    }
    let input: Value = serde_json::from_str(&data)?;
    if input["id"] != input_id || input["text"].as_str().map(js_trim) != Some("/restart") {
        return Err(RESTART_SOURCE.into());
    }
    let ack = db
        .query_row(
            "SELECT source_id,route,state,remote_id FROM outputs WHERE id=?",
            [ack_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((source_id, route, state, remote_id)) = ack else {
        return Err(RESTART_SOURCE.into());
    };
    if source_id != input_id
        || state != "sent"
        || remote_id.as_deref().map(js_trim).is_none_or(str::is_empty)
        || !json_equal(&serde_json::from_str(&route)?, &input["route"], false)
    {
        return Err(RESTART_SOURCE.into());
    }
    Ok(input)
}

pub(crate) fn js_trim(value: &str) -> &str {
    value.trim_matches(|c| {
        matches!(c,
            '\u{9}'..='\u{d}' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}'
                | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'
        )
    })
}
