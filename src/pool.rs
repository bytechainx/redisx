//! Redis 资源池：三种拓扑的连接后端 + Semaphore 背压 + 低基数指标。
//!
//! - Standalone / Sentinel：`redis::aio::ConnectionManager`（自动重连）；
//! - Cluster：`redis::cluster_async::ClusterConnection`；
//! - Sentinel：先发现 master，再以 ConnectionManager 连接该 master。
//!
//! [`RedisPool`] 本身只维护连接与背压；具体命令既可以走池上的便捷方法，也可以通过
//! [`RedisPool::acquire`] 取一个 [`RedisPoolPermit`]（占用一个命令 lane）后连续执行，
//! 从而让排队、超时与指标统计保持一致。

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use redis::AsyncCommands;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::{RedisConfig, RedisMode};
use crate::error::{RedisError, RedisResult};
use crate::error_map::map_redis_result;

#[cfg(feature = "pubsub")]
use crate::pubsub::RedisPubSub;

/// 池运行时快照（低基数，可用于 readiness / 指标）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedisPoolStats {
    /// 逻辑命令 lane 数（= `max_in_flight`；未连接或已关闭时为 0）。
    pub open: usize,
    /// 正在执行的命令数。
    pub in_flight: usize,
    /// 正在等待 acquire 的调用数。
    pub waiters: usize,
}

/// 低基数累计指标（进程内；无高基数 label）。
///
/// 供宿主导出 Prometheus 或做日志采样；本 crate 不绑定任何具体可观测性 SDK。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedisMetricsSnapshot {
    /// 命令成功次数。
    pub commands_ok: u64,
    /// 命令失败次数（闭包返回 `Err`）。
    pub commands_err: u64,
    /// 命令超时次数（命令预算耗尽）。
    pub commands_timeout: u64,
    /// acquire 超时次数。
    pub acquire_timeout: u64,
    /// 因池已关闭而拒绝的次数。
    pub rejected_closed: u64,
}

/// 健康检查结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisHealth {
    /// 脱敏端点。
    pub endpoint: String,
    /// PING 往返耗时。
    pub latency: Duration,
    /// 该池对应的部署模式。
    pub mode: RedisMode,
}

// `src/pool.rs` 下沉后，`pub(crate)` 项经此转出，保持 `crate::pool::{…}` 路径不变。
pub(crate) use self::backend::{connection_manager_config, RedisBackend};

/// 共享 Redis 连接池（`Clone` 只增加引用计数）。
#[derive(Clone)]
pub struct RedisPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    /// 连接后端；`None` 表示仅通过 [`RedisPool::new`] 校验过配置，尚未建立连接。
    backend: Option<RedisBackend>,
    /// 建池时使用的配置（Pub/Sub 复用，禁止重新读取环境变量）。
    config: RedisConfig,
    sem: Arc<Semaphore>,
    max_in_flight: usize,
    in_flight: AtomicUsize,
    waiters: AtomicUsize,
    closed: AtomicBool,
    command_timeout: Duration,
    acquire_timeout: Duration,
    display_endpoint: String,
    reconnect_max_delay: Duration,
    tcp_keepalive: Option<Duration>,
    commands_ok: AtomicU64,
    commands_err: AtomicU64,
    commands_timeout: AtomicU64,
    acquire_timeout_count: AtomicU64,
    rejected_closed: AtomicU64,
}

impl std::fmt::Debug for RedisPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPool")
            .field("endpoint", &self.inner.display_endpoint)
            .field("stats", &self.stats())
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// 命令 lane 许可（RAII）：持有时占用一个 in-flight 名额，`Drop` 时归还。
///
/// 通过 [`RedisPool::acquire`] 获取。许可同时持有可用连接，因此可在同一 lane 内连续执行多次
/// 命令（每次命令仍受池的 `command_timeout` 约束）；也可以直接调用池上的便捷方法，
/// 二者共用同一套排队、超时与指标统计。
pub struct RedisPoolPermit {
    inner: Arc<PoolInner>,
    backend: RedisBackend,
    _permit: OwnedSemaphorePermit,
}

impl std::fmt::Debug for RedisPoolPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPoolPermit")
            .field("endpoint", &self.inner.display_endpoint)
            .finish_non_exhaustive()
    }
}

impl Drop for RedisPoolPermit {
    fn drop(&mut self) {
        self.inner.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl RedisPool {
    /// 建池时使用的配置（只读）。
    #[must_use]
    pub fn config(&self) -> &RedisConfig {
        &self.inner.config
    }

    /// 脱敏端点（日志 / 诊断用）。
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.inner.display_endpoint
    }

    /// 获取一个命令 lane 许可（背压凭证）。
    ///
    /// # Errors
    ///
    /// 池已关闭、尚未建连或等待许可超时时返回错误。
    pub async fn acquire(&self) -> RedisResult<RedisPoolPermit> {
        self.acquire_with_timeout(self.inner.acquire_timeout).await
    }

    /// 执行 `PING` 并返回往返耗时。
    ///
    /// # Errors
    ///
    /// 池已关闭、未建连、响应异常或超时时返回错误。
    pub async fn ping(&self) -> RedisResult<Duration> {
        let start = Instant::now();
        self.with_conn(kv::ping).await?;
        Ok(start.elapsed())
    }

    /// 结构化健康检查：`PING` 成功才返回 `Ok`。
    ///
    /// # Errors
    ///
    /// 池已关闭或 `PING` 失败时返回错误。
    pub async fn health_check(&self) -> RedisResult<RedisHealth> {
        let latency = self.ping().await?;
        Ok(RedisHealth {
            endpoint: self.endpoint().to_owned(),
            latency,
            mode: self.inner.config.mode(),
        })
    }

    /// `GET`；缺失返回 `Ok(None)`。
    ///
    /// # Errors
    ///
    /// 池状态异常、连接/协议失败或命令超时时返回错误。
    pub async fn get(&self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        self.acquire().await?.get(key).await
    }

    /// `SET`（无 TTL；转发 [`crate::client::RedisClient::set`]，重试分类经
    /// [`crate::resilience::RedisOperation::Set`] 矩阵单一来源——写入结果未知时
    /// 不自动重试）。
    ///
    /// # Errors
    ///
    /// 同 [`RedisPool::get`]。
    pub async fn set(&self, key: &str, value: Vec<u8>) -> RedisResult<()> {
        self.acquire().await?.set(key, value).await
    }

    /// `PSETEX`（毫秒精度 TTL）。
    ///
    /// # Errors
    ///
    /// TTL 非法时返回 [`RedisError::Config`]；其余同 [`RedisPool::get`]。
    pub async fn set_ex(&self, key: &str, value: Vec<u8>, ttl: Duration) -> RedisResult<()> {
        self.acquire().await?.set_ex(key, value, ttl).await
    }

    /// `DEL`；返回是否真的删除了 key。
    ///
    /// # Errors
    ///
    /// 同 [`RedisPool::get`]。
    pub async fn del(&self, key: &str) -> RedisResult<bool> {
        self.acquire().await?.del(key).await
    }

    /// `EXISTS`。
    ///
    /// # Errors
    ///
    /// 同 [`RedisPool::get`]。
    pub async fn exists(&self, key: &str) -> RedisResult<bool> {
        self.acquire().await?.exists(key).await
    }

    /// `INCRBY`（`delta = 1` 即 `INCR`）。
    ///
    /// # Errors
    ///
    /// 同 [`RedisPool::get`]。
    pub async fn incr(&self, key: &str, delta: i64) -> RedisResult<i64> {
        self.acquire().await?.incr(key, delta).await
    }

    /// `PEXPIRE`；key 不存在返回 `Ok(false)`。
    ///
    /// # Errors
    ///
    /// TTL 非法时返回 [`RedisError::Config`]；其余同 [`RedisPool::get`]。
    pub async fn expire(&self, key: &str, ttl: Duration) -> RedisResult<bool> {
        self.acquire().await?.expire(key, ttl).await
    }

    /// `PTTL`；语义见 [`RedisPoolPermit::ttl`]。
    ///
    /// # Errors
    ///
    /// key 不存在时返回 [`RedisError::Missing`]；其余同 [`RedisPool::get`]。
    pub async fn ttl(&self, key: &str) -> RedisResult<Option<Duration>> {
        self.acquire().await?.ttl(key).await
    }

    /// 当前统计。
    #[must_use]
    pub fn stats(&self) -> RedisPoolStats {
        let connected = self.inner.backend.is_some();
        let closed = self.is_closed();
        RedisPoolStats {
            open: if connected && !closed {
                self.inner.max_in_flight
            } else {
                0
            },
            in_flight: self.inner.in_flight.load(Ordering::Relaxed),
            waiters: self.inner.waiters.load(Ordering::Relaxed),
        }
    }

    /// 低基数累计指标快照。
    #[must_use]
    pub fn metrics_snapshot(&self) -> RedisMetricsSnapshot {
        RedisMetricsSnapshot {
            commands_ok: self.inner.commands_ok.load(Ordering::Relaxed),
            commands_err: self.inner.commands_err.load(Ordering::Relaxed),
            commands_timeout: self.inner.commands_timeout.load(Ordering::Relaxed),
            acquire_timeout: self.inner.acquire_timeout_count.load(Ordering::Relaxed),
            rejected_closed: self.inner.rejected_closed.load(Ordering::Relaxed),
        }
    }

    /// 配置的命令超时。
    #[must_use]
    pub fn command_timeout(&self) -> Duration {
        self.inner.command_timeout
    }

    /// 最大并发命令 lane 数。
    #[must_use]
    pub fn command_lanes(&self) -> usize {
        self.inner.max_in_flight
    }

    /// 建池时应用的连接重试最大退避。
    #[must_use]
    pub fn reconnect_max_delay(&self) -> Duration {
        self.inner.reconnect_max_delay
    }

    /// 建池时记录的 TCP keepalive 配置意图。
    ///
    /// `redis` 0.27 在建连时使用操作系统默认 keepalive；本字段保存配置意图，供宿主与
    /// 后续驱动版本对齐，并保证 `connect` 路径确实消费了该配置。
    #[must_use]
    pub fn tcp_keepalive(&self) -> Option<Duration> {
        self.inner.tcp_keepalive
    }

    /// 是否已关闭。
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Liveness：已建连且未关闭（不访问网络）。
    #[must_use]
    pub fn liveness(&self) -> bool {
        self.inner.backend.is_some() && !self.is_closed()
    }

    /// Readiness：未关闭且 `PING` 成功，返回往返耗时。
    ///
    /// # Errors
    ///
    /// 池已关闭或 `PING` 失败时返回错误。
    pub async fn readiness(&self) -> RedisResult<Duration> {
        if self.is_closed() {
            return Err(RedisError::Connection("redis 连接池已关闭".to_owned()));
        }
        self.ping().await
    }

    /// 关闭池：拒绝新请求，并在 `deadline` 内等待 in-flight 排空。
    ///
    /// # Errors
    ///
    /// `deadline` 内仍有 in-flight 命令时返回 [`RedisError::Timeout`]。
    pub async fn close(&self, deadline: Duration) -> RedisResult<()> {
        self.inner.closed.store(true, Ordering::SeqCst);
        let start = Instant::now();
        loop {
            let in_flight = self.inner.in_flight.load(Ordering::SeqCst);
            if in_flight == 0 {
                return Ok(());
            }
            if start.elapsed() >= deadline {
                return Err(RedisError::Timeout(format!(
                    "redis close 排空超时（仍有 {in_flight} 个 in-flight）"
                )));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// 使用当前配置订阅频道（feature `pubsub`）。
    ///
    /// # Errors
    ///
    /// 池已关闭或 Pub/Sub 会话建立失败时返回错误。
    #[cfg(feature = "pubsub")]
    pub async fn subscribe(
        &self,
        channels: impl IntoIterator<Item = String>,
    ) -> RedisResult<RedisPubSub> {
        if self.is_closed() {
            return Err(RedisError::Connection("redis 连接池已关闭".to_owned()));
        }
        RedisPubSub::connect_config(self.inner.config.clone(), channels).await
    }

    pub(crate) async fn with_conn<F, Fut, T>(&self, f: F) -> RedisResult<T>
    where
        F: FnOnce(RedisBackend) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        self.run(None, f).await
    }

    pub(crate) async fn with_conn_budget<F, Fut, T>(
        &self,
        command_budget: Duration,
        f: F,
    ) -> RedisResult<T>
    where
        F: FnOnce(RedisBackend) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        self.run(Some(command_budget), f).await
    }

    pub(crate) async fn with_conn_total_deadline<F, Fut, T>(
        &self,
        total: Duration,
        f: F,
    ) -> RedisResult<T>
    where
        F: FnOnce(RedisBackend) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        if total.is_zero() {
            return Err(RedisError::Timeout("redis 调用总 deadline 为 0".to_owned()));
        }
        self.run_total(Some(total), f).await
    }

    async fn run<F, Fut, T>(&self, command_budget: Option<Duration>, f: F) -> RedisResult<T>
    where
        F: FnOnce(RedisBackend) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        if let Some(budget) = command_budget {
            if budget.is_zero() {
                return Err(RedisError::Timeout("redis 命令预算为 0".to_owned()));
            }
        }
        let permit = self.acquire().await?;
        permit
            .execute_with_budget(Instant::now(), command_budget, f)
            .await
    }

    async fn run_total<F, Fut, T>(&self, total: Option<Duration>, f: F) -> RedisResult<T>
    where
        F: FnOnce(RedisBackend) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        let started_at = Instant::now();
        let acquire_budget = match total {
            Some(total) => total.min(self.inner.acquire_timeout),
            None => self.inner.acquire_timeout,
        };
        let permit = self.acquire_with_timeout(acquire_budget).await?;
        permit
            .execute_with_total_deadline(started_at, total, f)
            .await
    }
}

mod backend;
mod connect;
/// 单命令原语集合（被 `RedisPool` / `RedisPoolPermit` / `RedisClient` 共用）。
pub(crate) mod kv;
mod lifecycle;
mod permit;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RedisConfig;

    #[test]
    fn connection_manager_config_applies_reconnect_max_delay() {
        let cfg = RedisConfig::builder()
            .addr("127.0.0.1:6379")
            .reconnect_max_delay(Duration::from_millis(1234))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("cfg");
        let manager = connection_manager_config(&cfg);
        let debug = format!("{manager:?}");
        assert!(debug.contains("max_delay"), "manager={debug}");
        assert!(debug.contains("1234"), "manager={debug}");
        assert_eq!(cfg.tcp_keepalive(), Some(Duration::from_secs(30)));
        assert_eq!(cfg.reconnect_max_delay(), Duration::from_millis(1234));
    }

    #[test]
    fn new_validates_without_connecting() {
        let pool = RedisPool::new(RedisConfig::default()).expect("new");
        assert!(!pool.is_closed());
        assert!(!pool.liveness(), "未建连不算 live");
        assert_eq!(pool.stats().open, 0);
        assert_eq!(pool.command_lanes(), RedisConfig::default().max_in_flight());
        assert!(pool.endpoint().starts_with("redis://"));
        assert_eq!(
            pool.command_timeout(),
            RedisConfig::default().command_timeout()
        );
        assert_eq!(
            pool.reconnect_max_delay(),
            RedisConfig::default().reconnect_max_delay()
        );
        assert!(pool.tcp_keepalive().is_none());
        assert_eq!(pool.metrics_snapshot(), RedisMetricsSnapshot::default());

        let bad = RedisConfig::default()
            .to_builder()
            .max_in_flight(0)
            .build()
            .expect_err("max_in_flight=0 非法");
        assert!(matches!(bad, RedisError::Config(_)));
    }

    #[tokio::test]
    async fn unconnected_pool_fails_closed() {
        let pool = RedisPool::new(RedisConfig::default()).expect("new");
        let err = pool.ping().await.expect_err("未建连");
        assert!(matches!(err, RedisError::Connection(_)), "{err}");
        let err = pool.acquire().await.expect_err("未建连");
        assert!(matches!(err, RedisError::Connection(_)));
        let err = pool.health_check().await.expect_err("未建连");
        assert!(matches!(err, RedisError::Connection(_)));
    }

    #[tokio::test]
    async fn stats_and_metrics_count_probe_traffic() {
        let calls = Arc::new(AtomicUsize::new(0));
        let pool = RedisPool::test_probe(calls.clone());
        let lanes = RedisConfig::default().max_in_flight();
        assert_eq!(pool.stats().open, lanes);
        assert!(pool.liveness());

        let err = pool.ping().await.expect_err("probe 必然失败");
        assert!(matches!(err, RedisError::Connection(_)));
        assert!(calls.load(Ordering::SeqCst) >= 1);

        let snapshot = pool.metrics_snapshot();
        assert_eq!(snapshot.commands_ok, 0);
        assert_eq!(snapshot.commands_err, 1);
        assert_eq!(snapshot.commands_timeout, 0);
        assert_eq!(snapshot.acquire_timeout, 0);
        assert_eq!(snapshot.rejected_closed, 0);
        assert_eq!(pool.stats().in_flight, 0, "命令结束后应归还 lane");
    }

    #[tokio::test]
    async fn permit_holds_lane_and_releases_on_drop() {
        let pool = RedisPool::test_probe(Arc::new(AtomicUsize::new(0)));
        let permit = pool.acquire().await.expect("permit");
        assert_eq!(pool.stats().in_flight, 1);
        assert_eq!(permit.endpoint(), pool.endpoint());
        assert_eq!(
            permit.command_timeout(),
            RedisConfig::default().command_timeout()
        );
        let err = permit.get("k").await.expect_err("probe");
        assert!(matches!(err, RedisError::Connection(_)));
        drop(permit);
        assert_eq!(pool.stats().in_flight, 0);
    }

    #[tokio::test]
    async fn all_pool_commands_enter_driver() {
        let calls = Arc::new(AtomicUsize::new(0));
        let pool = RedisPool::test_probe(calls.clone());
        let _ = pool.get("k").await;
        let _ = pool.set("k", b"v".to_vec()).await;
        let _ = pool
            .set_ex("k", b"v".to_vec(), Duration::from_secs(1))
            .await;
        let _ = pool.del("k").await;
        let _ = pool.exists("k").await;
        let _ = pool.incr("k", 1).await;
        let _ = pool.expire("k", Duration::from_secs(2)).await;
        let _ = pool.ttl("k").await;
        let _ = pool.ping().await;
        let _ = pool.health_check().await;
        assert!(
            calls.load(Ordering::SeqCst) >= 10,
            "应多次进入 probe driver"
        );
    }

    #[tokio::test]
    async fn invalid_ttl_fails_before_driver() {
        let calls = Arc::new(AtomicUsize::new(0));
        let pool = RedisPool::test_probe(calls.clone());
        let err = pool
            .set_ex("k", b"v".to_vec(), Duration::ZERO)
            .await
            .expect_err("ttl=0");
        assert!(matches!(err, RedisError::Config(_)));
        let err = pool
            .expire("k", Duration::from_nanos(1))
            .await
            .expect_err("亚毫秒");
        assert!(matches!(err, RedisError::Config(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 0, "TTL 非法时不得触达 driver");
    }

    #[tokio::test]
    async fn closed_pool_rejects_and_counts() {
        let pool = RedisPool::test_probe(Arc::new(AtomicUsize::new(0)));
        pool.close(Duration::from_secs(1)).await.expect("close");
        assert!(pool.is_closed());
        assert!(!pool.liveness());
        assert_eq!(pool.stats().open, 0);

        let err = pool.ping().await.expect_err("已关闭");
        assert!(matches!(err, RedisError::Connection(_)), "{err}");
        assert!(pool.metrics_snapshot().rejected_closed >= 1);
        let err = pool.readiness().await.expect_err("已关闭");
        assert!(matches!(err, RedisError::Connection(_)));
    }

    #[tokio::test]
    async fn zero_deadlines_are_rejected_as_timeout() {
        let pool = RedisPool::test_probe(Arc::new(AtomicUsize::new(0)));
        let err = pool
            .with_conn_total_deadline(Duration::ZERO, |_| async { Ok::<(), RedisError>(()) })
            .await
            .expect_err("零总 deadline");
        assert!(matches!(err, RedisError::Timeout(_)));

        let err = pool
            .with_conn_budget(Duration::ZERO, |_| async { Ok::<(), RedisError>(()) })
            .await
            .expect_err("零预算");
        assert!(matches!(err, RedisError::Timeout(_)));
        assert_eq!(
            pool.metrics_snapshot().acquire_timeout,
            0,
            "入口拦截不计入 acquire 超时"
        );
    }

    #[tokio::test]
    async fn connect_refused_returns_error() {
        let cfg = RedisConfig::builder()
            .addr("127.0.0.1:1")
            .password("unused-password")
            .connect_timeout(Duration::from_millis(200))
            .command_timeout(Duration::from_millis(200))
            .acquire_timeout(Duration::from_millis(200))
            .build()
            .expect("cfg");
        let result = tokio::time::timeout(Duration::from_secs(5), RedisPool::connect(cfg)).await;
        match result {
            Ok(Ok(pool)) => panic!("不应连接到 127.0.0.1:1: {pool:?}"),
            Ok(Err(err)) => assert!(
                matches!(
                    err,
                    RedisError::Connection(_) | RedisError::Timeout(_) | RedisError::Transient(_)
                ),
                "{err}"
            ),
            Err(_) => {}
        }
    }

    #[tokio::test]
    async fn cluster_connect_refused_returns_error() {
        let cfg = RedisConfig::builder()
            .mode(RedisMode::Cluster)
            .nodes(["127.0.0.1:1"])
            .connect_timeout(Duration::from_millis(200))
            .command_timeout(Duration::from_millis(200))
            .acquire_timeout(Duration::from_millis(200))
            .build()
            .expect("cfg");
        let result = tokio::time::timeout(Duration::from_secs(8), RedisPool::connect(cfg)).await;
        match result {
            Ok(Ok(pool)) => panic!("不应连接到 127.0.0.1:1: {pool:?}"),
            Ok(Err(err)) => assert!(
                matches!(
                    err,
                    RedisError::Connection(_) | RedisError::Timeout(_) | RedisError::Transient(_)
                ),
                "{err}"
            ),
            Err(_) => {}
        }
    }

    #[tokio::test]
    async fn sentinel_connect_refused_returns_error() {
        let cfg = RedisConfig::builder()
            .mode(RedisMode::Sentinel)
            .nodes(["127.0.0.1:1"])
            .sentinel_master("mymaster")
            .connect_timeout(Duration::from_millis(200))
            .command_timeout(Duration::from_millis(200))
            .acquire_timeout(Duration::from_millis(200))
            .build()
            .expect("cfg");
        let result = tokio::time::timeout(Duration::from_secs(8), RedisPool::connect(cfg)).await;
        match result {
            Ok(Ok(pool)) => panic!("不应连接到 127.0.0.1:1: {pool:?}"),
            Ok(Err(err)) => {
                assert!(
                    matches!(err, RedisError::Connection(_) | RedisError::Timeout(_)),
                    "{err}"
                );
            }
            Err(_) => {}
        }
    }

    #[test]
    fn debug_outputs_do_not_leak_password() {
        let secret = String::from("s3cr3t-value");
        let cfg = RedisConfig::builder()
            .addr("127.0.0.1:6379")
            .username("alice")
            .password(secret.clone())
            .build()
            .expect("cfg");
        let pool = RedisPool::new(cfg).expect("pool");
        let debug = format!("{pool:?}");
        assert!(!debug.contains(&secret), "pool debug={debug}");
        assert!(pool.config().has_password());
    }
}
