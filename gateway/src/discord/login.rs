use std::time::Duration;

use anyhow::{Result, ensure};
use reqwest::Url;
use serde::Deserialize;
use tokio::time::{self, Instant};

use super::State;

#[derive(Deserialize)]
pub(super) struct Discovery {
    url: String,
    shards: u32,
    session_start_limit: Limits,
}

#[derive(Deserialize)]
struct Limits {
    total: u32,
    remaining: u32,
    reset_after: u64,
    max_concurrency: u32,
}

pub(super) struct Login {
    pub target: Url,
    pub limiter: IdentifyLimiter,
}

pub(super) struct IdentifyLimiter {
    total: u32,
    remaining: u32,
    reset: Instant,
    allowed: Instant,
}

impl Login {
    pub async fn discover(state: &State) -> Result<Self> {
        let discovery: Discovery = state.rest.get("gateway/bot").await?;
        let limits = discovery.session_start_limit;
        ensure!(
            discovery.shards > 0
                && limits.total > 0
                && limits.total >= limits.remaining
                && limits.max_concurrency > 0
                && limits.reset_after <= 172_800_000,
            "Discord Gateway returned an invalid shard or identify quota"
        );
        let now = Instant::now();
        Ok(Self {
            target: gateway_url(&discovery.url)?,
            limiter: IdentifyLimiter {
                total: limits.total,
                remaining: limits.remaining,
                reset: now + Duration::from_millis(limits.reset_after),
                allowed: now,
            },
        })
    }
}

impl IdentifyLimiter {
    pub async fn wait(&mut self) {
        let deadline = if self.remaining == 0 {
            self.reset.max(self.allowed)
        } else {
            self.allowed
        };
        time::sleep_until(deadline).await;
        let now = Instant::now();
        if now >= self.reset {
            self.remaining = self.total;
            self.reset = now + Duration::from_secs(24 * 60 * 60);
        }
    }

    pub fn attempted(&mut self) {
        self.remaining = self.remaining.saturating_sub(1);
        self.allowed = Instant::now() + Duration::from_secs(5);
    }
}

pub(super) fn gateway_url(value: &str) -> Result<Url> {
    let mut url =
        Url::parse(value).map_err(|_| anyhow::anyhow!("Discord Gateway URL is invalid"))?;
    let no_credentials = url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && url.path() == "/";
    let discord = url.scheme() == "wss"
        && url.port().is_none()
        && url.host_str().is_some_and(|host| {
            host == "gateway.discord.gg"
                || (host.starts_with("gateway-") && host.ends_with(".discord.gg"))
        });
    let loopback_fixture =
        cfg!(test) && url.scheme() == "ws" && url.host_str() == Some("127.0.0.1");
    ensure!(
        no_credentials && (discord || loopback_fixture),
        "Discord Gateway URL is not an approved host"
    );
    url.set_query(Some("v=10&encoding=json"));
    Ok(url)
}
