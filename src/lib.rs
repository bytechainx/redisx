//! redisx — 生产可用的异步 Redis 适配器。
//!
//! 单一 crate 内提供三种拓扑、连接池背压、会话级超时/重试、核心数据结构与 Streams 原语，
//! 且不依赖任何私有基础设施 crate。
//!
//! ## 快速开始
//!
//! ```no_run
//! use redisx::{RedisConfig, RedisPool};
//!
//! # async fn run() -> redisx::RedisResult<()> {
//! let pool = RedisPool::connect(RedisConfig::from_url("redis://127.0.0.1:6379")?).await?;
//! pool.set("hello", b"world".to_vec()).await?;
//! let value = pool.get("hello").await?;
//! assert_eq!(value.as_deref(), Some(b"world".as_slice()));
//! pool.ping().await?;
//! pool.close(std::time::Duration::from_secs(1)).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## 公开 API 一览
//!
//! - 配置：[`RedisConfig`]（`from_env` / `from_toml` / `from_url` / `validate` / `builder`）、
//!   [`RedisConfigBuilder`]、[`RedisMode`]；密码在 `Debug` 与端点展示中脱敏。
//! - 连接池：[`RedisPool`]（`connect` / `new` / `acquire` / `ping` / `health_check` / `stats` /
//!   `metrics_snapshot` / `close`）、[`RedisPoolPermit`]、[`RedisPoolStats`]、
//!   [`RedisMetricsSnapshot`]、[`RedisHealth`]。
//! - 命令客户端：[`RedisClient`]（KV、Hash/List/Set/ZSet、Streams、事务、Lua、分布式锁）。
//! - 锁与脚本：[`RedisLock`]、[`generate_lock_token`]。
//! - Streams：[`StreamEntry`]。
//! - 事务：[`TxCmd`]。
//! - 重试：[`RetryConfig`]、[`with_retry`]，以及重试安全分类
//!   [`RedisOperation`] / [`RedisRetrySafety`] / [`RedisAtomicity`]。
//! - 错误：[`RedisError`]（`is_retryable`）、[`RedisResult`]、[`map_redis_error`]、
//!   [`map_redis_result`]。
//! - feature `pubsub`：[`RedisPubSub`]、[`RedisPubSubMessage`]。
//!
//! ## 拓扑
//!
//! | 模式 | 连接方式 | 说明 |
//! | --- | --- | --- |
//! | [`RedisMode::Standalone`] | `ConnectionManager` | 单机；自动重连 + 指数退避 |
//! | [`RedisMode::Cluster`] | `ClusterConnection` | 种子节点发现；`MOVED`/`ASK` 由驱动跟随 |
//! | [`RedisMode::Sentinel`] | 哨兵发现 master + `ConnectionManager` | 需 `sentinel_master` |
//!
//! Cluster 不支持非 0 逻辑库；Sentinel 必须提供 master 名，否则配置校验直接失败。
//!
//! ## 重试与副作用安全
//!
//! [`RedisClient::with_retry`] 只在命令的 [`RedisRetrySafety`] 为只读或幂等时进入重试环；
//! `SET`（带 TTL）、`DEL`、`PEXPIRE`、`INCR`、`PUBLISH` 等结果不明或非幂等的命令**永远**只执行
//! 一次，避免超时后重复副作用。退避为指数增长，可叠加抖动与总 deadline。

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
#![forbid(unsafe_code)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]

mod client;
mod config;
mod error;
mod error_map;
mod ext;
mod pool;
mod resilience;
mod streams;
mod structures;
mod transaction;

#[cfg(feature = "pubsub")]
mod pubsub;

pub use client::RedisClient;
pub use config::{
    RedisConfig, RedisConfigBuilder, RedisMode, ENV_ADDR, ENV_BLOCKING_TIMEOUT_MS, ENV_DB,
    ENV_MAX_IN_FLIGHT, ENV_MODE, ENV_NODES, ENV_PASSWORD, ENV_PREFIX, ENV_SENTINEL_MASTER, ENV_TLS,
    ENV_URL, ENV_USERNAME, ENV_WARMUP,
};
pub use error::{RedisError, RedisResult};
pub use error_map::{map_redis_error, map_redis_result};
pub use ext::{generate_lock_token, lock_token_matches, RedisLock};
pub use pool::{RedisHealth, RedisMetricsSnapshot, RedisPool, RedisPoolPermit, RedisPoolStats};
pub use resilience::{with_retry, RedisAtomicity, RedisOperation, RedisRetrySafety, RetryConfig};
pub use streams::StreamEntry;
pub use transaction::TxCmd;

#[cfg(feature = "pubsub")]
pub use pubsub::{RedisPubSub, RedisPubSubMessage};
