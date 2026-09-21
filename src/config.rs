//! Redis 连接配置、环境变量加载与 Builder。
//!
//! 配置字段全部私有，避免明文密码随字段访问泄漏；对外只暴露 getter、[`fmt::Debug`]
//! （脱敏）与 [`RedisConfig::display_endpoint`]。构造入口：
//!
//! - [`RedisConfig::builder`]：程序化构造，全字段可覆盖；
//! - [`RedisConfig::from_env`]：`REDIS_URL` 优先，其次 `FOUNDATIONX_REDISX_*`；
//! - [`RedisConfig::from_toml`]：TOML 文本（字段名与 [`ENV_PREFIX`] 语义一一对应）；
//! - [`RedisConfig::from_url`]：`redis://` / `rediss://` URL。
//!
//! 环境变量常量见模块内 `ENV_*`。

use std::fmt;
use std::time::Duration;

use serde::Deserialize;

use crate::error::{RedisError, RedisResult};

/// 环境变量前缀（`FOUNDATIONX_REDISX_*`）。
pub const ENV_PREFIX: &str = "FOUNDATIONX_REDISX_";
/// 完整 URL 环境变量；设置后覆盖其余 `FOUNDATIONX_REDISX_*` 项。
pub const ENV_URL: &str = "REDIS_URL";
/// `host:port` 环境变量。
pub const ENV_ADDR: &str = "FOUNDATIONX_REDISX_ADDR";
/// ACL 用户名环境变量。
pub const ENV_USERNAME: &str = "FOUNDATIONX_REDISX_USERNAME";
/// 密码环境变量。
pub const ENV_PASSWORD: &str = "FOUNDATIONX_REDISX_PASSWORD";
/// 逻辑库环境变量。
pub const ENV_DB: &str = "FOUNDATIONX_REDISX_DB";
/// TLS 开关环境变量。
pub const ENV_TLS: &str = "FOUNDATIONX_REDISX_TLS";
/// 部署模式环境变量（`standalone` | `cluster` | `sentinel`）。
pub const ENV_MODE: &str = "FOUNDATIONX_REDISX_MODE";
/// 集群/哨兵种子节点环境变量（逗号分隔）。
pub const ENV_NODES: &str = "FOUNDATIONX_REDISX_NODES";
/// Sentinel 服务名环境变量。
pub const ENV_SENTINEL_MASTER: &str = "FOUNDATIONX_REDISX_SENTINEL_MASTER";
/// 建池预热 PING 次数环境变量。
pub const ENV_WARMUP: &str = "FOUNDATIONX_REDISX_WARMUP";
/// 最大 in-flight 命令数环境变量。
pub const ENV_MAX_IN_FLIGHT: &str = "FOUNDATIONX_REDISX_MAX_IN_FLIGHT";
/// 阻塞命令默认超时（毫秒）环境变量。
pub const ENV_BLOCKING_TIMEOUT_MS: &str = "FOUNDATIONX_REDISX_BLOCKING_TIMEOUT_MS";

/// 部署模式：Standalone / Cluster / Sentinel。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum RedisMode {
    /// 单机（也用 `ConnectionManager` 承载 Sentinel 发现的 master）。
    #[default]
    Standalone,
    /// Redis Cluster。
    Cluster,
    /// 哨兵：先发现 master，再以单机连接管理器连接 master。
    Sentinel,
}

/// Redis 连接配置。
///
/// 通过 [`RedisConfig::builder`] / [`RedisConfig::from_env`] / [`RedisConfig::from_toml`] /
/// [`RedisConfig::from_url`] 构造；[`fmt::Debug`] 与 [`RedisConfig::display_endpoint`] 均脱敏。
#[derive(Clone)]
pub struct RedisConfig {
    /// `host:port`，默认 `127.0.0.1:6379`。
    addr: String,
    /// 集群 / 哨兵种子节点（`host:port` 或 `redis(s)://…`）；为空时回退 `addr`。
    nodes: Vec<String>,
    /// Sentinel 服务名（`SENTINEL master <name>`）；Sentinel 模式必填。
    sentinel_master: Option<String>,
    /// ACL 用户名；`None` 表示不发送 username。
    username: Option<String>,
    /// 密码；Debug / 错误信息中脱敏。
    password: Option<String>,
    /// 逻辑库编号。
    db: i64,
    /// 是否启用 TLS（强制证书校验，拒绝 insecure）。
    tls: bool,
    /// 部署模式。
    mode: RedisMode,
    /// 建连超时。
    connect_timeout: Duration,
    /// 单命令超时。
    command_timeout: Duration,
    /// 获取 in-flight 许可（背压）的超时。
    acquire_timeout: Duration,
    /// 全局 in-flight 上限。
    max_in_flight: usize,
    /// 客户端名称（`CLIENT SETNAME`）。
    client_name: Option<String>,
    /// 建池后预热 PING 次数（0 = 关闭）。
    warmup_count: usize,
    /// TCP keepalive 间隔（配置面；由驱动/宿主应用）。
    tcp_keepalive: Option<Duration>,
    /// 重连最大退避（映射到驱动的重连退避上限）。
    reconnect_max_delay: Duration,
    /// Cluster MOVED/ASK 重定向上限。
    max_cluster_redirects: u32,
    /// 阻塞命令默认等待上限（调用方可覆盖）。
    blocking_timeout: Duration,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:6379".into(),
            nodes: Vec::new(),
            sentinel_master: None,
            username: None,
            password: None,
            db: 0,
            tls: false,
            mode: RedisMode::Standalone,
            connect_timeout: Duration::from_secs(5),
            command_timeout: Duration::from_secs(3),
            acquire_timeout: Duration::from_secs(3),
            max_in_flight: 256,
            client_name: None,
            warmup_count: 0,
            tcp_keepalive: None,
            reconnect_max_delay: Duration::from_secs(5),
            max_cluster_redirects: 16,
            blocking_timeout: Duration::from_secs(5),
        }
    }
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisConfig")
            .field("addr", &redact_seed_url(&self.addr))
            .field(
                "nodes",
                &self
                    .nodes
                    .iter()
                    .map(|n| redact_seed_url(n))
                    .collect::<Vec<_>>(),
            )
            .field("sentinel_master", &self.sentinel_master)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("db", &self.db)
            .field("tls", &self.tls)
            .field("mode", &self.mode)
            .field("connect_timeout", &self.connect_timeout)
            .field("command_timeout", &self.command_timeout)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("max_in_flight", &self.max_in_flight)
            .field("client_name", &self.client_name)
            .field("warmup_count", &self.warmup_count)
            .field("tcp_keepalive", &self.tcp_keepalive)
            .field("reconnect_max_delay", &self.reconnect_max_delay)
            .field("max_cluster_redirects", &self.max_cluster_redirects)
            .field("blocking_timeout", &self.blocking_timeout)
            .finish()
    }
}

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
struct RedisConfigWire {
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

mod builder;
mod parse;

pub use builder::RedisConfigBuilder;

use parse::{parse_host_port, parse_mode, redact_seed_url, validate_seed};

impl RedisConfig {
    /// 校验配置合法性（不建立任何网络连接）。
    ///
    /// # Errors
    ///
    /// 地址/种子非法、模式与必填项不匹配（Cluster 非 0 库、Sentinel 缺 master）、
    /// 超时为零、`max_in_flight == 0`、`max_cluster_redirects == 0` 等返回
    /// [`RedisError::Config`]。
    pub fn validate(&self) -> RedisResult<()> {
        let has_addr = !self.addr.trim().is_empty();
        let has_nodes = !self.nodes.is_empty();

        match self.mode {
            RedisMode::Standalone => {
                if !has_addr {
                    return Err(RedisError::Config("Redis 地址不能为空".to_owned()));
                }
                parse_host_port(&self.addr)?;
            }
            RedisMode::Cluster => {
                if !has_addr && !has_nodes {
                    return Err(RedisError::Config(
                        "Cluster 模式需要 addr 或 nodes 非空".to_owned(),
                    ));
                }
                if self.db != 0 {
                    return Err(RedisError::Config(
                        "Cluster 模式不支持非 0 逻辑库（Redis Cluster 无 SELECT db）".to_owned(),
                    ));
                }
                if has_addr {
                    parse_host_port(&self.addr)?;
                }
                for node in &self.nodes {
                    validate_seed(node)?;
                }
            }
            RedisMode::Sentinel => {
                match self.sentinel_master.as_deref().map(str::trim) {
                    None | Some("") => {
                        return Err(RedisError::Config(format!(
                            "Sentinel 模式需要 sentinel_master（{ENV_SENTINEL_MASTER}）"
                        )));
                    }
                    Some(_) => {}
                }
                if !has_addr && !has_nodes {
                    return Err(RedisError::Config(
                        "Sentinel 模式需要 addr 或 nodes 作为 sentinel 种子".to_owned(),
                    ));
                }
                if has_addr {
                    parse_host_port(&self.addr)?;
                }
                for node in &self.nodes {
                    validate_seed(node)?;
                }
            }
        }

        if self.db < 0 {
            return Err(RedisError::Config("Redis db 不能为负数".to_owned()));
        }
        if self.max_in_flight == 0 {
            return Err(RedisError::Config("max_in_flight 必须 ≥ 1".to_owned()));
        }
        if self.connect_timeout.is_zero()
            || self.command_timeout.is_zero()
            || self.acquire_timeout.is_zero()
            || self.reconnect_max_delay.is_zero()
            || self.blocking_timeout.is_zero()
        {
            return Err(RedisError::Config("超时时间必须 > 0".to_owned()));
        }
        if let Some(keepalive) = self.tcp_keepalive {
            if keepalive.is_zero() {
                return Err(RedisError::Config(
                    "tcp_keepalive 若设置必须 > 0".to_owned(),
                ));
            }
        }
        if self.max_cluster_redirects == 0 {
            return Err(RedisError::Config(
                "max_cluster_redirects 必须 ≥ 1".to_owned(),
            ));
        }
        Ok(())
    }

    /// 端点 `host:port`（不含凭据）。
    #[must_use]
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// 集群 / 哨兵种子节点。
    #[must_use]
    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    /// Sentinel 服务名。
    #[must_use]
    pub fn sentinel_master(&self) -> Option<&str> {
        self.sentinel_master.as_deref()
    }

    /// 逻辑库。
    #[must_use]
    pub fn db(&self) -> i64 {
        self.db
    }

    /// 是否启用 TLS。
    #[must_use]
    pub fn tls(&self) -> bool {
        self.tls
    }

    /// 部署模式。
    #[must_use]
    pub fn mode(&self) -> RedisMode {
        self.mode
    }

    /// 建连超时。
    #[must_use]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// 单命令超时。
    #[must_use]
    pub fn command_timeout(&self) -> Duration {
        self.command_timeout
    }

    /// 获取 in-flight 许可的超时。
    #[must_use]
    pub fn acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }

    /// 最大 in-flight 命令数。
    #[must_use]
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// 客户端名称。
    #[must_use]
    pub fn client_name(&self) -> Option<&str> {
        self.client_name.as_deref()
    }

    /// 建池预热 PING 次数。
    #[must_use]
    pub fn warmup_count(&self) -> usize {
        self.warmup_count
    }

    /// TCP keepalive 间隔（若配置）。
    #[must_use]
    pub fn tcp_keepalive(&self) -> Option<Duration> {
        self.tcp_keepalive
    }

    /// 重连最大退避。
    #[must_use]
    pub fn reconnect_max_delay(&self) -> Duration {
        self.reconnect_max_delay
    }

    /// Cluster 重定向次数上限。
    #[must_use]
    pub fn max_cluster_redirects(&self) -> u32 {
        self.max_cluster_redirects
    }

    /// 阻塞命令默认超时。
    #[must_use]
    pub fn blocking_timeout(&self) -> Duration {
        self.blocking_timeout
    }

    /// 逻辑 command lane 数（同 [`Self::max_in_flight`]）。
    #[must_use]
    pub fn command_lanes(&self) -> usize {
        self.max_in_flight
    }

    /// 是否配置了非空密码（不暴露明文）。
    #[must_use]
    pub fn has_password(&self) -> bool {
        self.password
            .as_ref()
            .map(|p| !p.is_empty())
            .unwrap_or(false)
    }

    /// 脱敏端点展示（日志 / 诊断用）。
    #[must_use]
    pub fn display_endpoint(&self) -> String {
        let scheme = if self.tls { "rediss" } else { "redis" };
        let user = self.username.as_deref().unwrap_or("");
        let mode_tag = match self.mode {
            RedisMode::Standalone => "",
            RedisMode::Cluster => " mode=cluster",
            RedisMode::Sentinel => " mode=sentinel",
        };
        let seeds = if self.nodes.is_empty() {
            redact_seed_url(&self.addr)
        } else {
            self.nodes
                .iter()
                .map(|seed| redact_seed_url(seed))
                .collect::<Vec<_>>()
                .join(",")
        };
        let db = self.db;
        if self.has_password() {
            if user.is_empty() {
                format!("{scheme}://***@{seeds}/{db}{mode_tag}")
            } else {
                format!("{scheme}://{user}:***@{seeds}/{db}{mode_tag}")
            }
        } else if user.is_empty() {
            format!("{scheme}://{seeds}/{db}{mode_tag}")
        } else {
            format!("{scheme}://{user}@{seeds}/{db}{mode_tag}")
        }
    }

    /// ACL 用户名（`None` 表示不发送 username）。
    #[must_use]
    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    /// 密码（建池用；禁止写入日志，故不公开读取入口）。
    #[must_use]
    pub(crate) fn password_opt(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// 构造底层 `redis::ConnectionInfo`（Standalone；含密码，禁止写入日志）。
    pub(crate) fn to_connection_info(&self) -> RedisResult<redis::ConnectionInfo> {
        let (host, port) = parse_host_port(&self.addr)?;
        Ok(redis::ConnectionInfo {
            addr: self.connection_addr(host, port),
            redis: redis::RedisConnectionInfo {
                db: self.db,
                username: self.username.clone(),
                password: self.password.clone(),
                protocol: Default::default(),
            },
        })
    }

    /// 解析种子列表：`nodes` 非空则用之，否则回退 `addr`。
    pub(crate) fn seed_nodes(&self) -> RedisResult<Vec<String>> {
        if !self.nodes.is_empty() {
            return Ok(self.nodes.clone());
        }
        if self.addr.trim().is_empty() {
            return Err(RedisError::Config(
                "Redis 种子节点为空（addr 与 nodes 均未设置）".to_owned(),
            ));
        }
        Ok(vec![self.addr.clone()])
    }

    /// 为每个种子构造 `ConnectionInfo`（共享认证 / TLS / db 策略）。
    pub(crate) fn seed_connection_infos(&self) -> RedisResult<Vec<redis::ConnectionInfo>> {
        let seeds = self.seed_nodes()?;
        let mut out = Vec::with_capacity(seeds.len());
        for seed in seeds {
            out.push(self.connection_info_for_seed(&seed)?);
        }
        Ok(out)
    }

    fn apply_wire(&mut self, wire: &RedisConfigWire) -> RedisResult<()> {
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
        if let Some(password) = wire.password.as_ref().filter(|v| !v.trim().is_empty()) {
            self.password = Some(password.clone());
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

    fn connection_addr(&self, host: String, port: u16) -> redis::ConnectionAddr {
        if self.tls {
            redis::ConnectionAddr::TcpTls {
                host,
                port,
                insecure: false,
                tls_params: None,
            }
        } else {
            redis::ConnectionAddr::Tcp(host, port)
        }
    }

    fn connection_info_for_seed(&self, seed: &str) -> RedisResult<redis::ConnectionInfo> {
        let seed = seed.trim();
        if seed.starts_with("redis://") || seed.starts_with("rediss://") {
            use redis::IntoConnectionInfo;
            let redacted = redact_seed_url(seed);
            let mut info = seed
                .into_connection_info()
                .map_err(|_| RedisError::Config(format!("非法节点 URL `{redacted}`")))?;
            if self.username.is_some() {
                info.redis.username = self.username.clone();
            }
            if self.password.is_some() {
                info.redis.password = self.password.clone();
            }
            info.redis.db = self.db;
            if self.tls {
                info.addr = match info.addr {
                    redis::ConnectionAddr::Tcp(host, port) => redis::ConnectionAddr::TcpTls {
                        host,
                        port,
                        insecure: false,
                        tls_params: None,
                    },
                    redis::ConnectionAddr::TcpTls { insecure: true, .. } => {
                        return Err(RedisError::Config(
                            "拒绝 insecure TLS 节点 URL；仅允许证书校验 TLS".to_owned(),
                        ));
                    }
                    other => other,
                };
            }
            return Ok(info);
        }

        let (host, port) = parse_host_port(seed)?;
        Ok(redis::ConnectionInfo {
            addr: self.connection_addr(host, port),
            redis: redis::RedisConnectionInfo {
                db: self.db,
                username: self.username.clone(),
                password: self.password.clone(),
                protocol: Default::default(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret() -> String {
        // 密码一律由非字面量源构造，避免硬编码凭据
        (0..12).map(|i| char::from(b'a' + (i % 26) as u8)).collect()
    }

    #[test]
    fn debug_redacts_password_but_keeps_username() {
        let secret = secret();
        let cfg = RedisConfig::builder()
            .password(secret.clone())
            .username("alice")
            .build()
            .expect("cfg");
        let debug = format!("{cfg:?}");
        assert!(debug.contains("***"), "password must be redacted: {debug}");
        assert!(!debug.contains(&secret), "leaked password: {debug}");
        assert!(debug.contains("alice"));
    }

    #[test]
    fn display_endpoint_redacts_password() {
        let cfg = RedisConfig::builder()
            .addr("10.0.0.1:6379")
            .username("u")
            .password("p".repeat(4))
            .db(2)
            .build()
            .expect("cfg");
        let endpoint = cfg.display_endpoint();
        assert!(endpoint.contains("***"), "endpoint={endpoint}");
        assert!(!endpoint.contains(":pppp@"));
        assert!(endpoint.contains("10.0.0.1:6379"));
        assert!(endpoint.contains("/2"));
    }

    #[test]
    fn node_urls_are_redacted_in_debug_endpoint_and_errors() {
        let secret = secret();
        let node = format!("redis://alice:{secret}@redis.example:6379");
        let cfg = RedisConfig::builder()
            .mode(RedisMode::Cluster)
            .nodes([node])
            .build()
            .expect("cfg");
        let debug = format!("{cfg:?}");
        let endpoint = cfg.display_endpoint();
        assert!(!debug.contains(&secret), "debug={debug}");
        assert!(!endpoint.contains(&secret), "endpoint={endpoint}");
        assert!(debug.contains("alice:***"));
        assert!(endpoint.contains("alice:***"));

        let invalid = format!("redis://alice:{secret}@[");
        let err = RedisConfig::builder()
            .mode(RedisMode::Cluster)
            .nodes([invalid])
            .build()
            .expect_err("invalid URL");
        assert!(!err.to_string().contains(&secret), "err={err}");
    }

    #[test]
    fn default_and_builder_roundtrip() {
        let cfg = RedisConfig::builder()
            .addr("127.0.0.1:6381")
            .warmup_count(3)
            .client_name("redisx-test")
            .command_lanes(4)
            .connect_timeout(Duration::from_millis(250))
            .command_timeout(Duration::from_millis(500))
            .acquire_timeout(Duration::from_millis(750))
            .blocking_timeout(Duration::from_secs(2))
            .tcp_keepalive(Duration::from_secs(30))
            .max_cluster_redirects(8)
            .max_in_flight(32)
            .build()
            .expect("cfg");
        assert_eq!(cfg.addr(), "127.0.0.1:6381");
        assert_eq!(cfg.warmup_count(), 3);
        assert_eq!(cfg.command_lanes(), 32);
        assert_eq!(cfg.max_in_flight(), 32);
        assert_eq!(cfg.blocking_timeout(), Duration::from_secs(2));
        assert_eq!(cfg.connect_timeout(), Duration::from_millis(250));
        assert_eq!(cfg.command_timeout(), Duration::from_millis(500));
        assert_eq!(cfg.acquire_timeout(), Duration::from_millis(750));
        assert_eq!(cfg.max_cluster_redirects(), 8);
        assert_eq!(cfg.client_name(), Some("redisx-test"));

        let default = RedisConfig::default();
        assert_eq!(default.mode(), RedisMode::Standalone);
        assert_eq!(default.addr(), "127.0.0.1:6379");
        assert_eq!(default.db(), 0);
        assert!(!default.tls());
        assert!(!default.has_password());
    }

    #[test]
    fn clear_optional_fields() {
        let cfg = RedisConfig::builder()
            .username("u1")
            .password(secret())
            .sentinel_master("m1")
            .tcp_keepalive(Duration::from_secs(15))
            .build()
            .expect("cfg");
        assert!(cfg.has_password());
        assert_eq!(cfg.sentinel_master(), Some("m1"));
        assert_eq!(cfg.tcp_keepalive(), Some(Duration::from_secs(15)));

        let cleared = cfg
            .clone()
            .to_builder()
            .clear_username()
            .clear_password()
            .clear_sentinel_master()
            .clear_tcp_keepalive()
            .build()
            .expect("cleared");
        assert!(!cleared.has_password());
        assert!(cleared.sentinel_master().is_none());
        assert!(cleared.tcp_keepalive().is_none());
        assert!(!cleared.display_endpoint().contains("u1"));
    }

    #[test]
    fn validate_rejects_invalid_values() {
        assert!(RedisConfig::builder().max_in_flight(0).build().is_err());
        assert!(RedisConfig::builder().db(-1).build().is_err());
        assert!(RedisConfig::builder()
            .max_cluster_redirects(0)
            .build()
            .is_err());

        for result in [
            RedisConfig::builder()
                .connect_timeout(Duration::ZERO)
                .build(),
            RedisConfig::builder()
                .command_timeout(Duration::ZERO)
                .build(),
            RedisConfig::builder()
                .acquire_timeout(Duration::ZERO)
                .build(),
            RedisConfig::builder()
                .reconnect_max_delay(Duration::ZERO)
                .build(),
            RedisConfig::builder()
                .blocking_timeout(Duration::ZERO)
                .build(),
            RedisConfig::builder().tcp_keepalive(Duration::ZERO).build(),
        ] {
            let err = result.expect_err("zero must fail");
            assert!(matches!(err, RedisError::Config(_)), "{err}");
        }
    }

    #[test]
    fn cluster_and_sentinel_rules() {
        let cluster = RedisConfig::builder()
            .mode(RedisMode::Cluster)
            .addr("127.0.0.1:7000")
            .build()
            .expect("cluster");
        assert_eq!(cluster.mode(), RedisMode::Cluster);
        assert_eq!(
            cluster.seed_nodes().expect("seeds"),
            vec!["127.0.0.1:7000".to_owned()]
        );

        let multi = RedisConfig::builder()
            .mode(RedisMode::Cluster)
            .nodes(["10.0.0.1:7000", "10.0.0.2:7000"])
            .build()
            .expect("cluster nodes");
        assert_eq!(multi.nodes().len(), 2);
        assert_eq!(multi.seed_connection_infos().expect("infos").len(), 2);

        let db_err = RedisConfig::builder()
            .mode(RedisMode::Cluster)
            .addr("127.0.0.1:7000")
            .db(1)
            .build()
            .expect_err("cluster db must be 0");
        assert!(matches!(db_err, RedisError::Config(_)));

        let sentinel_err = RedisConfig::builder()
            .mode(RedisMode::Sentinel)
            .addr("127.0.0.1:26379")
            .build()
            .expect_err("sentinel needs master");
        assert!(sentinel_err.to_string().contains("sentinel_master"));

        let sentinel = RedisConfig::builder()
            .mode(RedisMode::Sentinel)
            .nodes(["127.0.0.1:26379"])
            .sentinel_master("mymaster")
            .build()
            .expect("sentinel");
        assert_eq!(sentinel.mode(), RedisMode::Sentinel);
        assert_eq!(sentinel.sentinel_master(), Some("mymaster"));
        assert!(sentinel.display_endpoint().contains("mode=sentinel"));
    }

    #[test]
    fn tls_connection_info_is_secure() {
        let cfg = RedisConfig::builder()
            .addr("redis.example:6380")
            .tls(true)
            .build()
            .expect("cfg");
        let info = cfg.to_connection_info().expect("info");
        match info.addr {
            redis::ConnectionAddr::TcpTls {
                host,
                port,
                insecure,
                tls_params,
            } => {
                assert_eq!(host, "redis.example");
                assert_eq!(port, 6380);
                assert!(!insecure, "必须强制证书校验");
                assert!(tls_params.is_none());
            }
            other => panic!("expected TcpTls, got {other:?}"),
        }
    }

    #[test]
    fn from_url_parses_auth_and_tls() {
        let cfg = RedisConfig::from_url("redis://user:secret@127.0.0.1:6380/3").expect("url");
        assert_eq!(cfg.addr(), "127.0.0.1:6380");
        assert_eq!(cfg.db(), 3);
        assert_eq!(cfg.username(), Some("user"));
        assert_eq!(cfg.password_opt(), Some("secret"));
        assert!(!cfg.tls());
        assert_eq!(cfg.mode(), RedisMode::Standalone);

        let tls = RedisConfig::from_url("rediss://127.0.0.1:6380/0").expect("url");
        assert!(tls.tls());
        let info = tls.to_connection_info().expect("info");
        assert!(matches!(
            info.addr,
            redis::ConnectionAddr::TcpTls {
                insecure: false,
                ..
            }
        ));

        let unix = RedisConfig::from_url("unix:///tmp/redis.sock").expect_err("unix unsupported");
        assert!(matches!(unix, RedisError::Unsupported(_)));
    }

    #[test]
    fn from_toml_applies_timeouts_and_mode() {
        let cfg = RedisConfig::from_toml(
            r#"
            addr = "10.0.0.5:6380"
            nodes = ["10.0.0.6:6380", "10.0.0.7:6380"]
            mode = "cluster"
            connect_timeout_ms = 120
            command_timeout_ms = 340
            max_in_flight = 8
            warmup_count = 2
            tcp_keepalive_ms = 15000
            "#,
        )
        .expect("toml");
        assert_eq!(cfg.addr(), "10.0.0.5:6380");
        assert_eq!(cfg.mode(), RedisMode::Cluster);
        assert_eq!(cfg.nodes(), ["10.0.0.6:6380", "10.0.0.7:6380"]);
        assert_eq!(cfg.connect_timeout(), Duration::from_millis(120));
        assert_eq!(cfg.command_timeout(), Duration::from_millis(340));
        assert_eq!(cfg.max_in_flight(), 8);
        assert_eq!(cfg.warmup_count(), 2);
        assert_eq!(cfg.tcp_keepalive(), Some(Duration::from_secs(15)));
    }

    #[test]
    fn from_toml_accepts_csv_nodes_and_rejects_bad_mode() {
        let csv = RedisConfig::from_toml(
            r#"
            mode = "sentinel"
            nodes = "127.0.0.1:26379, 127.0.0.1:26380"
            sentinel_master = "mymaster"
            "#,
        )
        .expect("csv nodes");
        assert_eq!(csv.nodes().len(), 2);
        assert_eq!(csv.mode(), RedisMode::Sentinel);

        let err = RedisConfig::from_toml(r#"mode = "wat""#).expect_err("bad mode");
        assert!(matches!(err, RedisError::Config(_)));
        assert!(RedisConfig::from_toml("addr = = 1").is_err());
    }

    #[test]
    fn serde_deserialize_validates() {
        let cfg: RedisConfig =
            serde_json::from_str(r#"{"addr":"127.0.0.1:7000","mode":"cluster"}"#).expect("json");
        assert_eq!(cfg.mode(), RedisMode::Cluster);
        let err =
            serde_json::from_str::<RedisConfig>(r#"{"max_in_flight":0}"#).expect_err("zero lanes");
        assert!(err.to_string().contains("max_in_flight"));
    }
}
