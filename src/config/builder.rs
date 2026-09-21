//! [`RedisConfig`] 的外部输入构造入口与 Builder。
//!
//! 汇总 `from_toml` / `from_url` / `from_env` 三个入口、`builder` / `to_builder`
//! 派生入口，以及逐字段设置的 [`RedisConfigBuilder`]。

use std::time::Duration;

use crate::error::{RedisError, RedisResult};

use super::parse::{env_nonempty, parse_bool, parse_mode, split_nodes};
use super::{
    RedisConfig, RedisConfigWire, RedisMode, ENV_ADDR, ENV_BLOCKING_TIMEOUT_MS, ENV_DB,
    ENV_MAX_IN_FLIGHT, ENV_MODE, ENV_NODES, ENV_PASSWORD, ENV_SENTINEL_MASTER, ENV_TLS, ENV_URL,
    ENV_USERNAME, ENV_WARMUP,
};

impl RedisConfig {
    /// 创建 Builder。
    #[must_use]
    pub fn builder() -> RedisConfigBuilder {
        RedisConfigBuilder {
            inner: Self::default(),
        }
    }

    /// 基于已有配置派生出 Builder（用于覆盖个别字段后重建）。
    #[must_use]
    pub fn to_builder(self) -> RedisConfigBuilder {
        RedisConfigBuilder { inner: self }
    }

    /// 从 TOML 字符串解析并校验。
    ///
    /// 支持字段：`addr`、`nodes`（字符串或数组）、`sentinel_master`、`username`、
    /// `db`、`tls`、`mode`、`connect_timeout_ms`、`command_timeout_ms`、`acquire_timeout_ms`、
    /// `max_in_flight`、`client_name`、`warmup_count`、`tcp_keepalive_ms`、
    /// `reconnect_max_delay_ms`、`max_cluster_redirects`、`blocking_timeout_ms`。
    ///
    /// **不接受 `password`**：凭据只能经环境变量或 builder（含
    /// [`RedisConfigBuilder::password_from_provider`]）注入，TOML 提供非空 `password`
    /// 一律 fail-closed（`docs/标准.md` §2）。
    ///
    /// # Errors
    ///
    /// TOML 语法错误、字段类型不符、提供明文 `password` 或配置校验失败时返回
    /// [`RedisError::Config`]。解析错误消息不含 TOML 源码行（避免配置原文进日志）。
    pub fn from_toml(text: &str) -> RedisResult<Self> {
        // 用 `message()` 而非 `Display`：后者会带 span 与出错源码行，可能把同行凭据带进日志。
        let wire: RedisConfigWire = toml::from_str(text)
            .map_err(|e| RedisError::Config(format!("TOML 配置非法: {}", e.message())))?;
        let mut cfg = Self::default();
        cfg.apply_wire(&wire)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// 从 `redis://` / `rediss://` URL 解析配置。
    ///
    /// `rediss://` 会置位 [`RedisConfig::tls`]；`insecure` TLS 与 Unix socket 一律拒绝。
    ///
    /// # Errors
    ///
    /// URL 不可解析、使用 `insecure` TLS 或 Unix socket 时返回 [`RedisError::Config`]。
    pub fn from_url(url: &str) -> RedisResult<Self> {
        use redis::IntoConnectionInfo;

        let info = url
            .into_connection_info()
            .map_err(|e| RedisError::Config(format!("Redis URL 非法: {e}")))?;

        let (addr, tls) = match info.addr {
            redis::ConnectionAddr::Tcp(host, port) => (format!("{host}:{port}"), false),
            redis::ConnectionAddr::TcpTls {
                host,
                port,
                insecure,
                ..
            } => {
                if insecure {
                    return Err(RedisError::Config(
                        "拒绝 insecure TLS（rediss://…?insecure 或等价）；仅允许证书校验 TLS"
                            .to_owned(),
                    ));
                }
                (format!("{host}:{port}"), true)
            }
            redis::ConnectionAddr::Unix(path) => {
                return Err(RedisError::Unsupported(format!(
                    "不支持 unix socket URL: {}",
                    path.display()
                )));
            }
        };

        let mut builder = Self::builder().addr(addr).db(info.redis.db).tls(tls);
        if let Some(user) = info.redis.username {
            builder = builder.username(user);
        }
        if let Some(password) = info.redis.password {
            builder = builder.password(password);
        }
        builder.build()
    }

    /// 从环境变量加载配置并校验。
    ///
    /// 优先级：
    /// 1. `REDIS_URL`（若为非空）——覆盖地址、认证、库与 TLS；
    /// 2. 否则读取 `FOUNDATIONX_REDISX_*`（常量见 [`ENV_PREFIX`] 系列）：
    ///    `ADDR` / `USERNAME`（默认 `default`）/ `PASSWORD` / `DB` / `TLS` / `MODE` /
    ///    `NODES` / `SENTINEL_MASTER` / `WARMUP` / `MAX_IN_FLIGHT` / `BLOCKING_TIMEOUT_MS`。
    ///
    /// # Errors
    ///
    /// URL 非法、布尔/整数环境变量不可解析或校验失败时返回 [`RedisError::Config`]。
    pub fn from_env() -> RedisResult<Self> {
        if let Some(url) = env_nonempty(ENV_URL) {
            return Self::from_url(&url);
        }

        let mut builder = Self::builder();
        let addr = env_nonempty(ENV_ADDR).unwrap_or_else(|| "127.0.0.1:6379".to_owned());
        builder = builder.addr(addr);

        // 规范默认 username=default；显式空字符串表示不发送 username。
        match std::env::var(ENV_USERNAME) {
            Ok(user) if user.is_empty() => {}
            Ok(user) => builder = builder.username(user),
            Err(_) => builder = builder.username("default"),
        }

        if let Some(password) = env_nonempty(ENV_PASSWORD) {
            builder = builder.password(password);
        }
        if let Some(db) = env_nonempty(ENV_DB) {
            let db: i64 = db
                .parse()
                .map_err(|e| RedisError::Config(format!("{ENV_DB} 非法: {e}")))?;
            builder = builder.db(db);
        }
        if let Some(tls) = env_nonempty(ENV_TLS) {
            builder = builder.tls(parse_bool(&tls)?);
        }
        if let Some(mode) = env_nonempty(ENV_MODE) {
            builder = builder.mode(parse_mode(&mode)?);
        }
        if let Some(nodes) = env_nonempty(ENV_NODES) {
            builder = builder.nodes(split_nodes(&nodes));
        }
        if let Some(master) = env_nonempty(ENV_SENTINEL_MASTER) {
            builder = builder.sentinel_master(master);
        }
        if let Some(warmup) = env_nonempty(ENV_WARMUP) {
            let warmup: usize = warmup
                .parse()
                .map_err(|e| RedisError::Config(format!("{ENV_WARMUP} 非法: {e}")))?;
            builder = builder.warmup_count(warmup);
        }
        if let Some(max) = env_nonempty(ENV_MAX_IN_FLIGHT) {
            let max: usize = max
                .parse()
                .map_err(|e| RedisError::Config(format!("{ENV_MAX_IN_FLIGHT} 非法: {e}")))?;
            builder = builder.max_in_flight(max);
        }
        if let Some(ms) = env_nonempty(ENV_BLOCKING_TIMEOUT_MS) {
            let ms: u64 = ms
                .parse()
                .map_err(|e| RedisError::Config(format!("{ENV_BLOCKING_TIMEOUT_MS} 非法: {e}")))?;
            builder = builder.blocking_timeout(Duration::from_millis(ms));
        }

        builder.build()
    }
}

/// [`RedisConfig`] 的 Builder。
#[derive(Debug, Clone)]
pub struct RedisConfigBuilder {
    inner: RedisConfig,
}

impl RedisConfigBuilder {
    /// 设置 `host:port`。
    #[must_use]
    pub fn addr(mut self, addr: impl Into<String>) -> Self {
        self.inner.addr = addr.into();
        self
    }

    /// 设置集群 / 哨兵种子节点。
    #[must_use]
    pub fn nodes(mut self, nodes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.inner.nodes = nodes.into_iter().map(Into::into).collect();
        self
    }

    /// 设置 Sentinel 服务名。
    #[must_use]
    pub fn sentinel_master(mut self, name: impl Into<String>) -> Self {
        self.inner.sentinel_master = Some(name.into());
        self
    }

    /// 清除 Sentinel 服务名。
    #[must_use]
    pub fn clear_sentinel_master(mut self) -> Self {
        self.inner.sentinel_master = None;
        self
    }

    /// 设置 ACL 用户名。
    #[must_use]
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.inner.username = Some(username.into());
        self
    }

    /// 清除 ACL 用户名。
    #[must_use]
    pub fn clear_username(mut self) -> Self {
        self.inner.username = None;
        self
    }

    /// 设置密码。
    #[must_use]
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.inner.password = Some(password.into());
        self
    }

    /// 清除密码。
    #[must_use]
    pub fn clear_password(mut self) -> Self {
        self.inner.password = None;
        self
    }

    /// 从回调注入密码（返回 `None` 表示清除）。
    ///
    /// 明文仅存于内存配置；[`fmt::Debug`] 脱敏；禁止日志打印返回值。
    #[must_use]
    pub fn password_from_provider<F>(mut self, provider: F) -> Self
    where
        F: FnOnce() -> Option<String>,
    {
        self.inner.password = provider();
        self
    }

    /// 设置逻辑库。
    #[must_use]
    pub fn db(mut self, db: i64) -> Self {
        self.inner.db = db;
        self
    }

    /// 设置 TLS 开关（开启后强制证书校验，拒绝 insecure）。
    #[must_use]
    pub fn tls(mut self, tls: bool) -> Self {
        self.inner.tls = tls;
        self
    }

    /// 设置部署模式。
    #[must_use]
    pub fn mode(mut self, mode: RedisMode) -> Self {
        self.inner.mode = mode;
        self
    }

    /// 设置建连超时。
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.inner.connect_timeout = timeout;
        self
    }

    /// 设置单命令超时。
    #[must_use]
    pub fn command_timeout(mut self, timeout: Duration) -> Self {
        self.inner.command_timeout = timeout;
        self
    }

    /// 设置获取 in-flight 许可的超时。
    #[must_use]
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.inner.acquire_timeout = timeout;
        self
    }

    /// 设置最大 in-flight 命令数。
    #[must_use]
    pub fn max_in_flight(mut self, max: usize) -> Self {
        self.inner.max_in_flight = max;
        self
    }

    /// 设置逻辑 command lane 数（等价于 [`Self::max_in_flight`]）。
    #[must_use]
    pub fn command_lanes(mut self, lanes: usize) -> Self {
        self.inner.max_in_flight = lanes;
        self
    }

    /// 设置客户端名称。
    #[must_use]
    pub fn client_name(mut self, name: impl Into<String>) -> Self {
        self.inner.client_name = Some(name.into());
        self
    }

    /// 设置建池预热 PING 次数。
    #[must_use]
    pub fn warmup_count(mut self, count: usize) -> Self {
        self.inner.warmup_count = count;
        self
    }

    /// 设置 TCP keepalive 间隔。
    #[must_use]
    pub fn tcp_keepalive(mut self, interval: Duration) -> Self {
        self.inner.tcp_keepalive = Some(interval);
        self
    }

    /// 清除 TCP keepalive。
    #[must_use]
    pub fn clear_tcp_keepalive(mut self) -> Self {
        self.inner.tcp_keepalive = None;
        self
    }

    /// 设置重连最大退避。
    #[must_use]
    pub fn reconnect_max_delay(mut self, delay: Duration) -> Self {
        self.inner.reconnect_max_delay = delay;
        self
    }

    /// 设置 Cluster MOVED/ASK 重定向上限。
    #[must_use]
    pub fn max_cluster_redirects(mut self, redirects: u32) -> Self {
        self.inner.max_cluster_redirects = redirects;
        self
    }

    /// 设置阻塞命令默认超时。
    #[must_use]
    pub fn blocking_timeout(mut self, timeout: Duration) -> Self {
        self.inner.blocking_timeout = timeout;
        self
    }

    /// 校验并生成配置。
    ///
    /// # Errors
    ///
    /// 同 [`RedisConfig::validate`]。
    pub fn build(self) -> RedisResult<RedisConfig> {
        self.inner.validate()?;
        Ok(self.inner)
    }
}
