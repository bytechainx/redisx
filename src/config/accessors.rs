//! `RedisConfig` 的只读访问器。
//!
//! 自 `src/config.rs` 下沉而来：`addr` / `nodes` / `sentinel_master` / `db` / ... / `username`
//! 等只读取值方法。`RedisConfig` 的定义仍在门面 `src/config.rs`；本模块是它的子模块，
//! 故可直接读取其私有字段。

use std::time::Duration;

use crate::error::{RedisError, RedisResult};

use super::parse::{parse_host_port, redact_seed_url};
use super::{RedisConfig, RedisMode};

impl RedisConfig {
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
}
