//! 可克隆的 Redis 命令客户端（共享 [`RedisPool`]）。
//!
//! [`RedisClient`] 在池之上补齐：命令级便捷 API、可选调用级总 deadline，以及按
//! [`RedisRetrySafety`] 分类的重试路由——只有只读与幂等命令会进入重试环。

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use redis::AsyncCommands;

use crate::config::RedisConfig;
use crate::error::RedisResult;
use crate::error_map::map_redis_result;
use crate::pool::{kv, RedisBackend, RedisHealth, RedisPool};
use crate::resilience::{with_retry, RedisOperation, RedisRetrySafety, RetryConfig};

/// 生产 Redis KV 客户端。`Clone` 只共享底层池引用。
///
/// - 所有命令受池级背压（命令 lane）与 `command_timeout` 约束；
/// - 可选 [`RedisClient::with_retry`]：按 [`RedisRetrySafety`] 分类路由，副作用语义不明的
///   写命令永远不会被自动重试；
/// - 可选调用级总 deadline（[`RedisClient::with_call_deadline`]）：排队与命令共享同一预算。
#[derive(Clone, Debug)]
pub struct RedisClient {
    pool: RedisPool,
    /// 可选重试策略；`None` 时每条命令只执行一次。
    retry: Option<Arc<RetryConfig>>,
    /// 调用级总 deadline（含 acquire 排队）；`None` 时使用池的分离超时。
    call_deadline: Option<Duration>,
}

impl RedisClient {
    /// 按配置建池并返回客户端。
    ///
    /// # Errors
    ///
    /// 配置校验失败或建连失败时返回错误。
    pub async fn connect(config: RedisConfig) -> RedisResult<Self> {
        Ok(RedisPool::connect(config).await?.client())
    }

    /// 同步构造：仅校验配置，不建立网络连接。
    ///
    /// # Errors
    ///
    /// 配置校验失败时返回 [`RedisError::Config`]。
    pub fn new(config: RedisConfig) -> RedisResult<Self> {
        Ok(RedisPool::new(config)?.client())
    }

    /// 从 `redis://` / `rediss://` URL 建池。
    ///
    /// # Errors
    ///
    /// URL 非法或建连失败时返回错误。
    pub async fn connect_url(url: &str) -> RedisResult<Self> {
        Self::connect(RedisConfig::from_url(url)?).await
    }

    /// 从环境变量建池（见 [`RedisConfig::from_env`]）。
    ///
    /// # Errors
    ///
    /// 环境变量非法或建连失败时返回错误。
    pub async fn connect_from_env() -> RedisResult<Self> {
        Self::connect(RedisConfig::from_env()?).await
    }

    pub(crate) fn from_pool(pool: RedisPool) -> Self {
        Self {
            pool,
            retry: None,
            call_deadline: None,
        }
    }

    /// 启用重试：只读与幂等命令按 [`RetryConfig`] 自动重试。
    #[must_use]
    pub fn with_retry(mut self, config: RetryConfig) -> Self {
        self.retry = Some(Arc::new(config));
        self
    }

    /// 设置调用级总 deadline（排队时间计入总预算）。
    #[must_use]
    pub fn with_call_deadline(mut self, total: Duration) -> Self {
        self.call_deadline = Some(total);
        self
    }

    /// 当前重试策略（若启用）。
    #[must_use]
    pub fn retry_config(&self) -> Option<&RetryConfig> {
        self.retry.as_deref()
    }

    /// 是否已配置调用级总 deadline。
    #[must_use]
    pub fn has_call_deadline(&self) -> bool {
        self.call_deadline.is_some()
    }

    /// 所属池。
    #[must_use]
    pub fn pool(&self) -> &RedisPool {
        &self.pool
    }

    /// 建池时使用的配置。
    #[must_use]
    pub fn config(&self) -> &RedisConfig {
        self.pool.config()
    }

    /// 脱敏端点。
    #[must_use]
    pub fn endpoint(&self) -> &str {
        self.pool.endpoint()
    }

    /// `PING`。
    ///
    /// # Errors
    ///
    /// 池已关闭、未建连或响应异常时返回错误。
    pub async fn ping(&self) -> RedisResult<()> {
        self.pool.ping().await.map(|_| ())
    }

    /// 结构化健康检查。
    ///
    /// # Errors
    ///
    /// 池已关闭或 `PING` 失败时返回错误。
    pub async fn health_check(&self) -> RedisResult<RedisHealth> {
        self.pool.health_check().await
    }

    /// `GET`；缺失返回 `Ok(None)`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn get(&self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        let this = self.clone();
        let key = key.to_owned();
        self.route(RedisOperation::Get, "redis.get", move || {
            let this = this.clone();
            let key = key.clone();
            async move { this.get_once(&key).await }
        })
        .await
    }

    /// 二进制安全 `GET`（等价 [`RedisClient::get`]）。
    ///
    /// # Errors
    ///
    /// 同 [`RedisClient::get`]。
    pub async fn get_bytes(&self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        self.get(key).await
    }

    /// `SET`（无 TTL；固定值写入按幂等语义可自动重试）。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn set(&self, key: &str, value: Vec<u8>) -> RedisResult<()> {
        let this = self.clone();
        let key = key.to_owned();
        self.route_with_safety(RedisRetrySafety::Idempotent, "redis.set", move || {
            let this = this.clone();
            let key = key.clone();
            let value = value.clone();
            async move { this.set_once(&key, value).await }
        })
        .await
    }

    /// 二进制安全 `SET`（等价 [`RedisClient::set`]）。
    ///
    /// # Errors
    ///
    /// 同 [`RedisClient::set`]。
    pub async fn set_bytes(&self, key: &str, value: Vec<u8>) -> RedisResult<()> {
        self.set(key, value).await
    }

    /// `PSETEX`（毫秒精度 TTL）。
    ///
    /// 相对 TTL 写入在超时/断连后结果未知，因此**不会**被自动重试。
    ///
    /// # Errors
    ///
    /// TTL 为 0 或小于 1ms 返回 [`RedisError::Config`]；其余同 [`RedisClient::set`]。
    pub async fn set_ex(&self, key: &str, value: Vec<u8>, ttl: Duration) -> RedisResult<()> {
        let this = self.clone();
        let key = key.to_owned();
        self.route_with_safety(
            RedisRetrySafety::AmbiguousWrite,
            "redis.set_ex",
            move || {
                let this = this.clone();
                let key = key.clone();
                let value = value.clone();
                async move { this.set_ex_once(&key, value, ttl).await }
            },
        )
        .await
    }

    /// `DEL`；返回是否真的删除了 key。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn del(&self, key: &str) -> RedisResult<bool> {
        let this = self.clone();
        let key = key.to_owned();
        self.route(RedisOperation::Delete, "redis.del", move || {
            let this = this.clone();
            let key = key.clone();
            async move { this.del_once(&key).await }
        })
        .await
    }

    /// `EXISTS`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn exists(&self, key: &str) -> RedisResult<bool> {
        let this = self.clone();
        let key = key.to_owned();
        self.route(RedisOperation::Exists, "redis.exists", move || {
            let this = this.clone();
            let key = key.clone();
            async move { this.exists_once(&key).await }
        })
        .await
    }

    /// `INCRBY`（`delta = 1` 即 `INCR`）；返回自增后的值。
    ///
    /// # Errors
    ///
    /// 连接/协议失败、类型不匹配或超时时返回错误。
    pub async fn incr(&self, key: &str, delta: i64) -> RedisResult<i64> {
        let this = self.clone();
        let key = key.to_owned();
        self.route(RedisOperation::Incr, "redis.incr", move || {
            let this = this.clone();
            let key = key.clone();
            async move { this.incr_once(&key, delta).await }
        })
        .await
    }

    /// `PEXPIRE`；key 不存在返回 `Ok(false)`。
    ///
    /// # Errors
    ///
    /// TTL 非法返回 [`RedisError::Config`]；其余同 [`RedisClient::set`]。
    pub async fn expire(&self, key: &str, ttl: Duration) -> RedisResult<bool> {
        let this = self.clone();
        let key = key.to_owned();
        self.route(RedisOperation::Expire, "redis.expire", move || {
            let this = this.clone();
            let key = key.clone();
            async move { this.expire_once(&key, ttl).await }
        })
        .await
    }

    /// `PTTL`：key 不存在 → [`RedisError::Missing`]；无过期 → `Ok(None)`。
    ///
    /// # Errors
    ///
    /// key 不存在、连接/协议失败或超时时返回错误。
    pub async fn ttl(&self, key: &str) -> RedisResult<Option<Duration>> {
        let this = self.clone();
        let key = key.to_owned();
        self.route(RedisOperation::Ttl, "redis.ttl", move || {
            let this = this.clone();
            let key = key.clone();
            async move { this.ttl_once(&key).await }
        })
        .await
    }

    /// `MGET`；空入参返回空结果。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn mget(&self, keys: &[&str]) -> RedisResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let this = self.clone();
        let owned: Vec<String> = keys.iter().map(|key| (*key).to_owned()).collect();
        self.route(RedisOperation::Mget, "redis.mget", move || {
            let this = this.clone();
            let owned = owned.clone();
            async move {
                let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
                this.mget_once(&refs).await
            }
        })
        .await
    }

    /// `MSET`（无 TTL；跨 Cluster slot 不承诺原子性）。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn mset(&self, items: &[(&str, &[u8])]) -> RedisResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        let this = self.clone();
        let owned: Vec<(String, Vec<u8>)> = items
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_vec()))
            .collect();
        self.route(RedisOperation::Mset, "redis.mset", move || {
            let this = this.clone();
            let owned = owned.clone();
            async move {
                let refs: Vec<(&str, &[u8])> = owned
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_slice()))
                    .collect();
                this.mset_once(&refs).await
            }
        })
        .await
    }

    /// 在池连接上执行闭包；配置了调用级 deadline 时排队与命令共享总预算。
    pub(crate) async fn with_pool_conn<F, Fut, T>(&self, f: F) -> RedisResult<T>
    where
        F: FnOnce(RedisBackend) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        match self.call_deadline {
            Some(total) => self.pool.with_conn_total_deadline(total, f).await,
            None => self.pool.with_conn(f).await,
        }
    }

    /// 按 [`RedisOperation`] 的重试分类路由。
    async fn route<F, Fut, T>(&self, op: RedisOperation, name: &str, f: F) -> RedisResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        self.route_with_safety(op.retry_safety(), name, f).await
    }

    /// 按显式重试安全分类路由：只有只读与幂等分类会进入重试环。
    async fn route_with_safety<F, Fut, T>(
        &self,
        safety: RedisRetrySafety,
        name: &str,
        mut f: F,
    ) -> RedisResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        let retryable = matches!(
            safety,
            RedisRetrySafety::ReadOnly | RedisRetrySafety::Idempotent
        );
        match self.retry.as_deref() {
            Some(config) if retryable => with_retry(config, name, f).await,
            _ => f().await,
        }
    }

    async fn get_once(&self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        let key = key.to_owned();
        self.with_pool_conn(move |conn| async move { kv::get(conn, &key).await })
            .await
    }

    async fn set_once(&self, key: &str, value: Vec<u8>) -> RedisResult<()> {
        let key = key.to_owned();
        self.with_pool_conn(move |conn| async move { kv::set(conn, &key, value).await })
            .await
    }

    async fn set_ex_once(&self, key: &str, value: Vec<u8>, ttl: Duration) -> RedisResult<()> {
        let key = key.to_owned();
        self.with_pool_conn(move |conn| async move { kv::set_ex(conn, &key, value, ttl).await })
            .await
    }

    async fn del_once(&self, key: &str) -> RedisResult<bool> {
        let key = key.to_owned();
        self.with_pool_conn(move |conn| async move { kv::del(conn, &key).await })
            .await
    }

    async fn exists_once(&self, key: &str) -> RedisResult<bool> {
        let key = key.to_owned();
        self.with_pool_conn(move |conn| async move { kv::exists(conn, &key).await })
            .await
    }

    async fn incr_once(&self, key: &str, delta: i64) -> RedisResult<i64> {
        let key = key.to_owned();
        self.with_pool_conn(move |conn| async move { kv::incr(conn, &key, delta).await })
            .await
    }

    async fn expire_once(&self, key: &str, ttl: Duration) -> RedisResult<bool> {
        let key = key.to_owned();
        self.with_pool_conn(move |conn| async move { kv::expire(conn, &key, ttl).await })
            .await
    }

    async fn ttl_once(&self, key: &str) -> RedisResult<Option<Duration>> {
        let key = key.to_owned();
        self.with_pool_conn(move |conn| async move { kv::ttl(conn, &key).await })
            .await
    }

    async fn mget_once(&self, keys: &[&str]) -> RedisResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let owned: Vec<String> = keys.iter().map(|key| (*key).to_owned()).collect();
        self.with_pool_conn(move |mut conn| async move {
            let values: Vec<Option<Vec<u8>>> = map_redis_result(conn.get(owned).await)?;
            Ok(values)
        })
        .await
    }

    async fn mset_once(&self, items: &[(&str, &[u8])]) -> RedisResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        let owned: Vec<(String, Vec<u8>)> = items
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_vec()))
            .collect();
        self.with_pool_conn(move |mut conn| async move {
            let _: () = map_redis_result(conn.mset(&owned).await)?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RedisError;
    use crate::pool::kv::validate_ttl;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn probe(calls: Arc<AtomicUsize>) -> RedisClient {
        RedisPool::test_probe(calls).client()
    }

    #[test]
    fn ttl_validation_matrix() {
        assert!(validate_ttl(None).is_ok());
        assert!(validate_ttl(Some(Duration::from_millis(10))).is_ok());
        let err = validate_ttl(Some(Duration::ZERO)).expect_err("零 TTL");
        assert!(matches!(err, RedisError::Config(_)));
        let err = validate_ttl(Some(Duration::from_nanos(100))).expect_err("亚毫秒 TTL");
        assert!(matches!(err, RedisError::Config(_)));
    }

    #[test]
    fn retry_configuration_is_observable() {
        let client = probe(Arc::new(AtomicUsize::new(0)));
        assert!(client.retry_config().is_none());
        assert!(!client.has_call_deadline());

        let configured = client
            .clone()
            .with_retry(RetryConfig::fixed(2, Duration::from_millis(1)))
            .with_call_deadline(Duration::from_secs(1));
        assert_eq!(
            configured.retry_config().map(RetryConfig::max_attempts),
            Some(2)
        );
        assert!(configured.has_call_deadline());
        assert!(configured.endpoint().contains("redis://"));
        assert_eq!(
            configured.pool().command_lanes(),
            client.pool().command_lanes()
        );
        assert_eq!(
            configured.config().mode(),
            crate::config::RedisMode::Standalone
        );
    }

    #[tokio::test]
    async fn every_public_kv_op_enters_driver() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = probe(calls.clone());
        let _ = client.get("k").await;
        let _ = client.get_bytes("k").await;
        let _ = client.set("k", b"v".to_vec()).await;
        let _ = client.set_bytes("k", b"v".to_vec()).await;
        let _ = client
            .set_ex("k", b"v".to_vec(), Duration::from_secs(1))
            .await;
        let _ = client.del("k").await;
        let _ = client.exists("k").await;
        let _ = client.incr("k", 1).await;
        let _ = client.expire("k", Duration::from_secs(2)).await;
        let _ = client.ttl("k").await;
        let _ = client.mget(&["a", "b"]).await;
        let _ = client
            .mset(&[("a", b"1".as_slice()), ("b", b"2".as_slice())])
            .await;
        assert!(
            calls.load(Ordering::SeqCst) >= 10,
            "公共 KV 面应进入 driver"
        );
    }

    #[tokio::test]
    async fn empty_batch_ops_short_circuit() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = probe(calls.clone());
        assert!(client.mget(&[]).await.expect("空 MGET").is_empty());
        client.mset(&[]).await.expect("空 MSET");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn invalid_ttl_never_reaches_driver() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = probe(calls.clone());
        let err = client
            .set_ex("k", b"v".to_vec(), Duration::ZERO)
            .await
            .expect_err("ttl=0");
        assert!(matches!(err, RedisError::Config(_)));
        let err = client
            .expire("k", Duration::from_nanos(1))
            .await
            .expect_err("亚毫秒");
        assert!(matches!(err, RedisError::Config(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn ambiguous_write_is_attempted_once_even_with_retry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client =
            probe(calls.clone()).with_retry(RetryConfig::fixed(5, Duration::from_millis(1)));
        let _ = client
            .set_ex("k", b"v".to_vec(), Duration::from_secs(1))
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "相对 TTL 写入不得自动重试");

        let calls = Arc::new(AtomicUsize::new(0));
        let client =
            probe(calls.clone()).with_retry(RetryConfig::fixed(5, Duration::from_millis(1)));
        let _ = client.del("k").await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "DEL 不得自动重试");

        let calls = Arc::new(AtomicUsize::new(0));
        let client =
            probe(calls.clone()).with_retry(RetryConfig::fixed(5, Duration::from_millis(1)));
        let _ = client.incr("k", 1).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "INCR 不得自动重试");
    }

    #[tokio::test]
    async fn read_only_op_retries_under_retry_config() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client =
            probe(calls.clone()).with_retry(RetryConfig::fixed(3, Duration::from_millis(1)));
        let err = client.get("k").await.expect_err("probe 必然失败");
        assert!(matches!(err, RedisError::Connection(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 3, "只读命令应重试到上限");
    }

    #[tokio::test]
    async fn call_deadline_zero_fails_before_driver() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = probe(calls.clone()).with_call_deadline(Duration::ZERO);
        let err = client.get("k").await.expect_err("零总 deadline");
        assert!(matches!(
            err,
            RedisError::Timeout(_) | RedisError::Connection(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn health_check_and_ping_fail_on_unreachable_probe() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = probe(calls.clone());
        let err = client.health_check().await.expect_err("probe 失败");
        assert!(matches!(err, RedisError::Connection(_)));
        let err = client.ping().await.expect_err("probe 失败");
        assert!(matches!(err, RedisError::Connection(_)));
        assert!(calls.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn unconnected_client_fails_closed() {
        let client = RedisClient::new(RedisConfig::default()).expect("new");
        assert!(!client.pool().liveness());
        let err = client.get("k").await.expect_err("未建连");
        assert!(matches!(err, RedisError::Connection(_)));

        let invalid = RedisConfig::default().to_builder().max_in_flight(0).build();
        assert!(invalid.is_err());
    }
}
