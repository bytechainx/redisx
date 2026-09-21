//! [`RedisPoolPermit`] 的公开命令方法与内部执行器。
//!
//! 两类方法都依赖 `pool` 的私有 `PoolInner` 与 `RedisBackend`；
//! 迁到子模块后仍可访问祖先的私有项，故无需放宽可见性。

use std::future::Future;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::time::timeout;

use crate::error::{RedisError, RedisResult};

use super::kv;
use super::{RedisBackend, RedisPoolPermit};

impl RedisPoolPermit {
    /// 脱敏端点。
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.inner.display_endpoint
    }

    /// 该许可上单条命令的超时。
    #[must_use]
    pub fn command_timeout(&self) -> Duration {
        self.inner.command_timeout
    }

    /// `GET`；缺失返回 `Ok(None)`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或命令超时时返回错误。
    pub async fn get(&self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        let key = key.to_owned();
        let budget = self.inner.command_timeout;
        self.run_timed(Instant::now(), budget, move |conn| async move {
            kv::get(conn, &key).await
        })
        .await
    }

    /// `SET`（无 TTL）。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或命令超时时返回错误。
    pub async fn set(&self, key: &str, value: Vec<u8>) -> RedisResult<()> {
        let key = key.to_owned();
        let budget = self.inner.command_timeout;
        self.run_timed(Instant::now(), budget, move |conn| async move {
            kv::set(conn, &key, value).await
        })
        .await
    }

    /// `PSETEX`（毫秒精度 TTL）。
    ///
    /// # Errors
    ///
    /// TTL 为 0 或小于 1ms 返回 [`RedisError::Config`]；其余同 [`RedisPoolPermit::set`]。
    pub async fn set_ex(&self, key: &str, value: Vec<u8>, ttl: Duration) -> RedisResult<()> {
        let key = key.to_owned();
        let budget = self.inner.command_timeout;
        self.run_timed(Instant::now(), budget, move |conn| async move {
            kv::set_ex(conn, &key, value, ttl).await
        })
        .await
    }

    /// `DEL`；返回是否真的删除了 key。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或命令超时时返回错误。
    pub async fn del(&self, key: &str) -> RedisResult<bool> {
        let key = key.to_owned();
        let budget = self.inner.command_timeout;
        self.run_timed(Instant::now(), budget, move |conn| async move {
            kv::del(conn, &key).await
        })
        .await
    }

    /// `EXISTS`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或命令超时时返回错误。
    pub async fn exists(&self, key: &str) -> RedisResult<bool> {
        let key = key.to_owned();
        let budget = self.inner.command_timeout;
        self.run_timed(Instant::now(), budget, move |conn| async move {
            kv::exists(conn, &key).await
        })
        .await
    }

    /// `INCRBY`（`delta = 1` 即 `INCR`）；返回自增后的值。
    ///
    /// # Errors
    ///
    /// 连接/协议失败、类型不匹配或命令超时时返回错误。
    pub async fn incr(&self, key: &str, delta: i64) -> RedisResult<i64> {
        let key = key.to_owned();
        let budget = self.inner.command_timeout;
        self.run_timed(Instant::now(), budget, move |conn| async move {
            kv::incr(conn, &key, delta).await
        })
        .await
    }

    /// `PEXPIRE`；key 不存在返回 `Ok(false)`。
    ///
    /// # Errors
    ///
    /// TTL 为 0 或小于 1ms 返回 [`RedisError::Config`]；其余同 [`RedisPoolPermit::set`]。
    pub async fn expire(&self, key: &str, ttl: Duration) -> RedisResult<bool> {
        let key = key.to_owned();
        let budget = self.inner.command_timeout;
        self.run_timed(Instant::now(), budget, move |conn| async move {
            kv::expire(conn, &key, ttl).await
        })
        .await
    }

    /// `PTTL`。
    ///
    /// - key 不存在 → [`RedisError::Missing`]；
    /// - 无过期时间 → `Ok(None)`；
    /// - 否则 `Ok(Some(剩余时间))`。
    ///
    /// # Errors
    ///
    /// key 不存在、连接/协议失败或命令超时时返回错误。
    pub async fn ttl(&self, key: &str) -> RedisResult<Option<Duration>> {
        let key = key.to_owned();
        let budget = self.inner.command_timeout;
        self.run_timed(Instant::now(), budget, move |conn| async move {
            kv::ttl(conn, &key).await
        })
        .await
    }

    /// `PING`。
    ///
    /// # Errors
    ///
    /// 响应异常、连接失败或命令超时时返回错误。
    pub async fn ping(&self) -> RedisResult<()> {
        let budget = self.inner.command_timeout;
        self.run_timed(Instant::now(), budget, kv::ping).await
    }
}

impl RedisPoolPermit {
    /// 在显式命令预算内执行（阻塞命令用）。
    pub(crate) async fn execute_with_budget<F, Fut, T>(
        &self,
        started_at: Instant,
        command_budget: Option<Duration>,
        f: F,
    ) -> RedisResult<T>
    where
        F: FnOnce(RedisBackend) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        let budget = command_budget.unwrap_or(self.inner.command_timeout);
        self.run_timed(started_at, budget, f).await
    }

    /// 在「排队 + 命令」共享的调用级总 deadline 内执行。
    pub(crate) async fn execute_with_total_deadline<F, Fut, T>(
        &self,
        started_at: Instant,
        total: Option<Duration>,
        f: F,
    ) -> RedisResult<T>
    where
        F: FnOnce(RedisBackend) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        let budget = match total {
            Some(total) => {
                let remaining = total.saturating_sub(started_at.elapsed());
                if remaining.is_zero() {
                    self.inner.commands_timeout.fetch_add(1, Ordering::Relaxed);
                    return Err(RedisError::Timeout(
                        "redis 排队耗尽调用总 deadline（acquire 计入总预算）".to_owned(),
                    ));
                }
                remaining.min(self.inner.command_timeout)
            }
            None => self.inner.command_timeout,
        };
        self.run_timed(started_at, budget, f).await
    }

    async fn run_timed<F, Fut, T>(
        &self,
        started_at: Instant,
        budget: Duration,
        f: F,
    ) -> RedisResult<T>
    where
        F: FnOnce(RedisBackend) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        if budget.is_zero() {
            self.inner.commands_timeout.fetch_add(1, Ordering::Relaxed);
            return Err(RedisError::Timeout(format!(
                "redis 命令预算为 0（排队 {}ms）",
                started_at.elapsed().as_millis()
            )));
        }
        let result = timeout(budget, f(self.backend.clone())).await;
        match result {
            Ok(Ok(value)) => {
                self.inner.commands_ok.fetch_add(1, Ordering::Relaxed);
                Ok(value)
            }
            Ok(Err(err)) => {
                self.inner.commands_err.fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
            Err(_) => {
                self.inner.commands_timeout.fetch_add(1, Ordering::Relaxed);
                Err(RedisError::Timeout(format!(
                    "redis 命令超时（预算 {}ms，排队 {}ms）",
                    budget.as_millis(),
                    started_at.elapsed().as_millis()
                )))
            }
        }
    }
}
