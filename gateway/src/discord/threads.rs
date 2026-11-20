use std::collections::HashSet;

use reqwest::Method;
use serde::Deserialize;
use serde_json::{Value, json};

use super::model::{Channel, Message, RemoteMessage, Route, snowflake, utf16};
use super::permissions::{
    CREATE_PUBLIC_THREADS, MANAGE_THREADS, READ_HISTORY, SEND_IN_THREADS, SEND_MESSAGES,
    VIEW_CHANNEL, has,
};
use super::rest::{Rest, broken, no_mentions};
use super::{Effect, EffectError, State};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Update {
    title: Option<String>,
    applied_tags: Option<Vec<String>>,
    archived: Option<bool>,
}

fn valid_title(title: &str) -> bool {
    !title.trim().is_empty() && utf16(title) <= 100
}

fn forum_tags(parent: &Channel, applied: Option<&[String]>, permissions: u64) -> Effect<()> {
    let tags = applied.unwrap_or_default();
    if !parent.is_forum() {
        return if applied.is_none() {
            Ok(())
        } else {
            Err(EffectError::rejected(
                "Discord tags can only be selected for forum/media topics",
            ))
        };
    }
    if tags.len() > 5 || tags.iter().collect::<HashSet<_>>().len() != tags.len() {
        return Err(EffectError::rejected(
            "Discord forum/media tags must be at most five unique tag IDs",
        ));
    }
    if parent.requires_tag() && tags.is_empty() {
        return Err(EffectError::rejected(
            "Discord forum/media topics require an explicitly selected tag",
        ));
    }
    for id in tags {
        let tag = parent
            .available_tags
            .iter()
            .find(|tag| &tag.id == id)
            .ok_or_else(|| {
                EffectError::rejected("Discord tag ID is not available in this forum/media channel")
            })?;
        if tag.moderated && !has(permissions, MANAGE_THREADS) {
            return Err(EffectError::rejected(
                "Discord moderated tags require ManageThreads permission",
            ));
        }
    }
    Ok(())
}

struct ThreadContext {
    channel: Channel,
    parent: Channel,
    permissions: u64,
}

impl ThreadContext {
    fn info(&self, route: &Route) -> Effect<Value> {
        let channel = &self.channel;
        let title = channel
            .name
            .as_ref()
            .ok_or_else(|| broken("Discord thread title is unavailable"))?;
        let archived = if channel.is_thread() {
            Some(
                channel
                    .thread_metadata
                    .as_ref()
                    .ok_or_else(|| broken("Discord thread archive state is unavailable"))?
                    .archived,
            )
        } else {
            None
        };
        Ok(
            json!({ "route": route.physical(), "title": title, "archived": archived,
            "requiresTag": self.parent.requires_tag(), "availableTags": self.parent.available_tags,
            "appliedTags": channel.applied_tags }),
        )
    }
}

impl State {
    pub(super) async fn can_create_thread(
        &self,
        route: &Route,
        applied: Option<&[String]>,
    ) -> Effect<bool> {
        self.assert_route(route)?;
        if route.guild_id.is_none() || route.thread_id.is_some() {
            return if applied.is_none() {
                Ok(false)
            } else {
                Err(EffectError::rejected(
                    "Discord tags can only be selected for forum/media topic creation",
                ))
            };
        }
        let parent = self.channel(route, true).await?;
        if !parent.is_thread_parent() {
            forum_tags(&parent, applied, 0)?;
            return Ok(false);
        }
        let permissions = self.permissions(&parent).await?;
        forum_tags(&parent, applied, permissions)?;
        let extra = if parent.is_forum() {
            READ_HISTORY
        } else {
            CREATE_PUBLIC_THREADS
        };
        Ok(has(
            permissions,
            VIEW_CHANNEL | SEND_MESSAGES | SEND_IN_THREADS | extra,
        ))
    }

    pub(super) async fn create_thread(
        &self,
        route: &Route,
        title: &str,
        source: Option<&str>,
        applied: Option<&[String]>,
    ) -> Effect<Value> {
        self.assert_route(route)?;
        if route.guild_id.is_none() || route.thread_id.is_some() || !valid_title(title) {
            return Err(EffectError::rejected(
                "Discord requires an authorized guild parent and a 1–100 character thread title",
            ));
        }
        let parent = self.channel(route, true).await?;
        if !parent.is_thread_parent() {
            return Err(EffectError::rejected(
                "This guild channel cannot create a public conversation thread",
            ));
        }
        let path = format!("channels/{}/threads", parent.id);
        let mut body = json!({ "name": title });
        let mut sent_starter = false;
        let endpoint = if parent.is_forum() {
            if source.is_some() {
                return Err(EffectError::rejected(
                    "Discord forum/media topics cannot reuse a source message",
                ));
            }
            let permissions = self.permissions(&parent).await?;
            forum_tags(&parent, applied, permissions)?;
            if !has(
                permissions,
                VIEW_CHANNEL | READ_HISTORY | SEND_MESSAGES | SEND_IN_THREADS,
            ) {
                return Err(EffectError::rejected(
                    "Discord bot lacks permissions to create and use a forum/media topic",
                ));
            }
            body["message"] = json!({ "content": title, "allowed_mentions": no_mentions() });
            if let Some(tags) = applied {
                body["applied_tags"] = json!(tags);
            }
            path
        } else {
            forum_tags(&parent, applied, 0)?;
            if let Some(source) = source {
                if !snowflake(source) {
                    return Err(EffectError::rejected("Invalid thread source message ID"));
                }
                let message: Message = self
                    .rest
                    .get(&format!("channels/{}/messages/{source}", parent.id))
                    .await?;
                if message.id != source || !message.matches(route) {
                    return Err(EffectError::rejected(
                        "Discord thread source does not belong to this destination",
                    ));
                }
                format!("channels/{}/messages/{source}/threads", parent.id)
            } else if parent.kind == 5 {
                let starter: RemoteMessage = Rest::response(
                    self.rest
                        .request(Method::POST, &format!("channels/{}/messages", parent.id))
                        .json(&json!({ "content": title, "allowed_mentions": no_mentions() })),
                )
                .await?;
                if !starter.belongs(route) || !snowflake(&starter.id) {
                    return Err(broken(
                        "Discord thread starter had an unexpected destination",
                    ));
                }
                sent_starter = true;
                format!("channels/{}/messages/{}/threads", parent.id, starter.id)
            } else {
                body["type"] = json!(11);
                path
            }
        };
        let returned =
            Rest::response::<Channel>(self.rest.request(Method::POST, &endpoint).json(&body)).await;
        let created = returned.map_err(|error| if sent_starter {
            broken("Discord sent the announcement starter but thread creation could not be confirmed")
        } else { error })?;
        let actual = self
            .route_for(&created)
            .map_err(|_| broken("Created Discord thread had an unexpected destination"))?;
        if actual.thread_id.is_none() || actual.parent() != route.physical() {
            return Err(broken(
                "Created Discord thread had an unexpected destination",
            ));
        }
        self.remember_channel(created).await;
        Ok(json!(actual))
    }

    async fn thread_context(&self, route: &Route) -> Effect<ThreadContext> {
        self.assert_route(route)?;
        if route.guild_id.is_none() {
            return Err(EffectError::rejected(
                "Discord thread inspection requires an authorized guild destination",
            ));
        }
        let channel = self.channel(route, true).await?;
        let parent = if channel.is_thread() {
            self.channel(&route.parent(), true).await?
        } else {
            channel.clone()
        };
        if !parent.is_thread_parent() {
            return Err(EffectError::rejected(
                "Discord destination is not a supported thread parent",
            ));
        }
        let permissions = self.permissions(&parent).await?;
        if !has(permissions, VIEW_CHANNEL | READ_HISTORY) {
            return Err(EffectError::rejected(
                "Discord bot lacks permissions to inspect this thread destination",
            ));
        }
        Ok(ThreadContext {
            channel,
            parent,
            permissions,
        })
    }

    pub(super) async fn inspect_thread(&self, route: &Route) -> Effect<Value> {
        self.thread_context(route).await?.info(route)
    }

    pub(super) async fn update_thread(&self, route: &Route, update: &Update) -> Effect<Value> {
        if route.thread_id.is_none() || route.guild_id.is_none() {
            return Err(EffectError::rejected(
                "Discord thread updates require an existing guild thread",
            ));
        }
        let mut body = json!({});
        if let Some(title) = &update.title {
            if !valid_title(title) {
                return Err(EffectError::rejected(
                    "Discord thread title must contain 1–100 characters",
                ));
            }
            body["name"] = json!(title);
        }
        if let Some(archived) = update.archived {
            body["archived"] = json!(archived);
        }
        if let Some(tags) = &update.applied_tags {
            body["applied_tags"] = json!(tags);
        }
        if update.title.is_none() && update.archived.is_none() && update.applied_tags.is_none() {
            return Err(EffectError::rejected(
                "Discord thread update needs a title, tag IDs or archived state",
            ));
        }
        let context = self.thread_context(route).await?;
        if !has(context.permissions, MANAGE_THREADS) {
            return Err(EffectError::rejected(
                "Discord thread updates require ManageThreads permission",
            ));
        }
        if update.applied_tags.is_some() {
            forum_tags(
                &context.parent,
                update.applied_tags.as_deref(),
                context.permissions,
            )?;
        }
        Rest::empty(
            self.rest
                .request(Method::PATCH, &format!("channels/{}", context.channel.id))
                .json(&body),
        )
        .await?;
        let readback_error = || {
            broken(
                "Discord thread update was sent but authoritative readback could not be confirmed",
            )
        };
        let found = self
            .thread_context(route)
            .await
            .map_err(|_| readback_error())?;
        if update
            .title
            .as_ref()
            .is_some_and(|title| found.channel.name.as_ref() != Some(title))
            || update.archived.is_some_and(|archived| {
                found
                    .channel
                    .thread_metadata
                    .as_ref()
                    .map(|meta| meta.archived)
                    != Some(archived)
            })
            || update.applied_tags.as_ref().is_some_and(|tags| {
                tags.len() != found.channel.applied_tags.len()
                    || tags
                        .iter()
                        .any(|tag| !found.channel.applied_tags.contains(tag))
            })
        {
            return Err(readback_error());
        }
        found.info(route).map_err(|_| readback_error())
    }
}
