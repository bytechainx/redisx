//! `RedisPool` 的建连、构造与并发许可获取。
//!
//! 自 `src/pool.rs` 下沉而来：`connect` / `new` / `connect_from_env` 与两个私有辅助
//! （`from_parts` / `acquire_with_timeout`）。`RedisPool` / `PoolInner` 的定义仍在门面
//! `src/pool.rs`；本模块是它的子模块，故可直接读写二者的私有字段。

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::time::timeout;

use crate::config::{RedisConfig, RedisMode};
use crate::error::{RedisError, RedisResult};

use super::connect::{connect_cluster, connect_sentinel, connect_standalone};
use super::{PoolInner, RedisBackend, RedisPool, RedisPoolPermit};

impl RedisPool {
    /// 按配置建立连接（Standalone / Cluster / Sentinel）。
    ///
    /// 会执行可选的 `CLIENT SETNAME` 与 `warmup_count` 次 `PING`；二者失败均不阻断建池。
    ///
    /// # Errors
    ///
    /// 配置校验失败、连接建立失败或建连超时时返回错误。
    #[tracing::instrument(skip(config), fields(endpoint = %config.display_endpoint()))]
    pub async fn connect(config: RedisConfig) -> RedisResult<Self> {
        config.validate()?;
        let backend = match config.mode() {
            RedisMode::Standalone => connect_standalone(&config).await?,
            RedisMode::Cluster => connect_cluster(&config).await?,
            RedisMode::Sentinel => connect_sentinel(&config).await?,
        };

        if let Some(name) = config.client_name() {
            let mut conn = backend.clone();
            let _: redis::RedisResult<()> = redis::cmd("CLIENT")
                .arg("SETNAME")
                .arg(name)
                .query_async(&mut conn)
                .await;
        }

        for _ in 0..config.warmup_count() {
            let mut conn = backend.clone();
            let _: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
        }

        Ok(Self::from_parts(config, Some(backend)))
    }

    /// 同步构造：只校验配置，**不建立任何网络连接**。
    ///
    /// 该池上的数据面命令会以 [`RedisError::Connection`] 失败，直到改用
    /// [`RedisPool::connect`]；适用于「先构造配置持有者、稍后建连」的场景。
    ///
    /// # Errors
    ///
    /// 配置校验失败时返回 [`RedisError::Config`]。
    pub fn new(config: RedisConfig) -> RedisResult<Self> {
        config.validate()?;
        Ok(Self::from_parts(config, None))
    }

    /// 从环境变量连接（见 [`RedisConfig::from_env`]）。
    ///
    /// # Errors
    ///
    /// 环境变量非法或建连失败时返回错误。
    pub async fn connect_from_env() -> RedisResult<Self> {
        Self::connect(RedisConfig::from_env()?).await
    }

    fn from_parts(config: RedisConfig, backend: Option<RedisBackend>) -> Self {
        let display_endpoint = config.display_endpoint();
        let max_in_flight = config.max_in_flight();
        let command_timeout = config.command_timeout();
        let acquire_timeout = config.acquire_timeout();
        let reconnect_max_delay = config.reconnect_max_delay();
        let tcp_keepalive = config.tcp_keepalive();
        Self {
            inner: Arc::new(PoolInner {
                backend,
                command_timeout,
                acquire_timeout,
                reconnect_max_delay,
                tcp_keepalive,
                config,
                sem: Arc::new(Semaphore::new(max_in_flight)),
                max_in_flight,
                in_flight: AtomicUsize::new(0),
                waiters: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
                display_endpoint,
                commands_ok: AtomicU64::new(0),
                commands_err: AtomicU64::new(0),
                commands_timeout: AtomicU64::new(0),
                acquire_timeout_count: AtomicU64::new(0),
                rejected_closed: AtomicU64::new(0),
            }),
        }
    }

    pub(super) async fn acquire_with_timeout(
        &self,
        budget: Duration,
    ) -> RedisResult<RedisPoolPermit> {
        if self.is_closed() {
            self.inner.rejected_closed.fetch_add(1, Ordering::Relaxed);
            return Err(RedisError::Connection("redis 连接池已关闭".to_owned()));
        }
        let backend = match self.inner.backend.as_ref() {
            Some(backend) => backend.clone(),
            None => {
                return Err(RedisError::Connection(
                    "redis 连接池尚未建立连接（RedisPool::new 仅校验配置，请改用 RedisPool::connect）"
                        .to_owned(),
                ));
            }
        };
        if budget.is_zero() {
            self.inner
                .acquire_timeout_count
                .fetch_add(1, Ordering::Relaxed);
            return Err(RedisError::Timeout(
                "redis 获取 in-flight 许可预算为 0".to_owned(),
            ));
        }

        self.inner.waiters.fetch_add(1, Ordering::SeqCst);
        let acquired = timeout(budget, self.inner.sem.clone().acquire_owned()).await;
        self.inner.waiters.fetch_sub(1, Ordering::SeqCst);

        match acquired {
            Ok(Ok(permit)) => {
                if self.is_closed() {
                    drop(permit);
                    self.inner.rejected_closed.fetch_add(1, Ordering::Relaxed);
                    return Err(RedisError::Connection("redis 连接池已关闭".to_owned()));
                }
                self.inner.in_flight.fetch_add(1, Ordering::SeqCst);
                Ok(RedisPoolPermit {
                    inner: self.inner.clone(),
                    backend,
                    _permit: permit,
                })
            }
            Ok(Err(_)) => Err(RedisError::Connection("redis 背压信号量已关闭".to_owned())),
            Err(_) => {
                self.inner
                    .acquire_timeout_count
                    .fetch_add(1, Ordering::Relaxed);
                Err(RedisError::Timeout(format!(
                    "redis 获取 in-flight 许可超时（max={}）",
                    self.inner.max_in_flight
                )))
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn test_probe(driver_calls: Arc<AtomicUsize>) -> Self {
        Self::from_parts(
            RedisConfig::default(),
            Some(RedisBackend::Probe(driver_calls)),
        )
    }
}
