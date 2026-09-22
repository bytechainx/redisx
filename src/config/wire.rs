//! TOML / serde 侧的 wire 形态与字段映射。
//!
//! 自 `src/config.rs` 下沉而来：`NodesField` / `RedisConfigWire`（含自定义 `Deserialize`）
//! 与 `apply_wire`。`RedisConfigWire` 与 `apply_wire` 由门面（`from_toml` 路径）调用，
//! 故提为 `pub(super)`。

use std::time::Duration;

use serde::Deserialize;

use crate::error::{RedisError, RedisResult};

use super::parse::parse_mode;
use super::RedisConfig;

/// 种子节点字段：同时接受逗号分隔字符串与 TOML 数组。
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum NodesField {
    /// `"a:6379,b:6379"`。
    Csv(String),
    /// `["a:6379", "b:6379"]`。
    List(Vec<String>),
}

impl NodesField {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::Csv(csv) => csv
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
            Self::List(list) => list
                .into_iter()
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
                .collect(),
        }
    }
}

/// TOML / serde 侧的可选字段集合（不含默认值）。
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct RedisConfigWire {
    addr: Option<String>,
    nodes: Option<NodesField>,
    sentinel_master: Option<String>,
    username: Option<String>,
    password: Option<String>,
    db: Option<i64>,
    tls: Option<bool>,
    mode: Option<String>,
    connect_timeout_ms: Option<u64>,
    command_timeout_ms: Option<u64>,
    acquire_timeout_ms: Option<u64>,
    max_in_flight: Option<usize>,
    client_name: Option<String>,
    warmup_count: Option<usize>,
    tcp_keepalive_ms: Option<u64>,
    reconnect_max_delay_ms: Option<u64>,
    max_cluster_redirects: Option<u32>,
    blocking_timeout_ms: Option<u64>,
}

impl<'de> Deserialize<'de> for RedisConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = RedisConfigWire::deserialize(deserializer)?;
        let mut cfg = Self::default();
        cfg.apply_wire(&wire)
            .map_err(<D::Error as serde::de::Error>::custom)?;
        cfg.validate()
            .map_err(<D::Error as serde::de::Error>::custom)?;
        Ok(cfg)
    }
}

impl RedisConfig {
    pub(super) fn apply_wire(&mut self, wire: &RedisConfigWire) -> RedisResult<()> {
        // 凭据只能经环境变量或 builder 注入（`docs/标准.md` §2）；TOML / serde 文本源
        // 提供的非空 password 一律 fail-closed，且错误信息不回显取值。
        if wire
            .password
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
        {
            return Err(RedisError::Config(
                "不允许经 TOML / serde 提供明文 password；凭据只能经环境变量或 builder 注入"
                    .to_owned(),
            ));
        }
        if let Some(addr) = wire
            .addr
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
        {
            self.addr = addr.to_owned();
        }
        if let Some(nodes) = wire.nodes.as_ref() {
            let parsed = match nodes {
                NodesField::Csv(csv) => NodesField::Csv(csv.clone()).into_vec(),
                NodesField::List(list) => NodesField::List(list.clone()).into_vec(),
            };
            if !parsed.is_empty() {
                self.nodes = parsed;
            }
        }
        if let Some(master) = wire
            .sentinel_master
            .as_ref()
            .filter(|v| !v.trim().is_empty())
        {
            self.sentinel_master = Some(master.clone());
        }
        if let Some(username) = wire.username.as_ref() {
            self.username = if username.is_empty() {
                None
            } else {
                Some(username.clone())
            };
        }
        if let Some(db) = wire.db {
            self.db = db;
        }
        if let Some(tls) = wire.tls {
            self.tls = tls;
        }
        if let Some(mode) = wire.mode.as_ref().filter(|v| !v.trim().is_empty()) {
            self.mode = parse_mode(mode)?;
        }
        if let Some(ms) = wire.connect_timeout_ms {
            self.connect_timeout = Duration::from_millis(ms);
        }
        if let Some(ms) = wire.command_timeout_ms {
            self.command_timeout = Duration::from_millis(ms);
        }
        if let Some(ms) = wire.acquire_timeout_ms {
            self.acquire_timeout = Duration::from_millis(ms);
        }
        if let Some(n) = wire.max_in_flight {
            self.max_in_flight = n;
        }
        if let Some(name) = wire.client_name.as_ref().filter(|v| !v.trim().is_empty()) {
            self.client_name = Some(name.clone());
        }
        if let Some(n) = wire.warmup_count {
            self.warmup_count = n;
        }
        if let Some(ms) = wire.tcp_keepalive_ms {
            self.tcp_keepalive = Some(Duration::from_millis(ms));
        }
        if let Some(ms) = wire.reconnect_max_delay_ms {
            self.reconnect_max_delay = Duration::from_millis(ms);
        }
        if let Some(n) = wire.max_cluster_redirects {
            self.max_cluster_redirects = n;
        }
        if let Some(ms) = wire.blocking_timeout_ms {
            self.blocking_timeout = Duration::from_millis(ms);
        }
        Ok(())
    }
}
