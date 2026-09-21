//! 一等数据结构 API：Hash / List / Set / Sorted Set。
//!
//! 全部走 [`RedisClient`] 的生产路径（池背压 + 命令超时 + 可选调用级 deadline），
//! 不提供绕过池的 raw 旁路。

use redis::AsyncCommands;

use crate::client::RedisClient;
use crate::error::RedisResult;
use crate::error_map::map_redis_result;

impl RedisClient {
    // ── Hash ──────────────────────────────────────────────────────────────

    /// `HSET key field value`；返回是否新建字段。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn hset(&self, key: &str, field: &str, value: Vec<u8>) -> RedisResult<bool> {
        let key = key.to_owned();
        let field = field.to_owned();
        self.with_pool_conn(move |mut conn| async move {
            let added: i64 = map_redis_result(conn.hset(key, field, value).await)?;
            Ok(added > 0)
        })
        .await
    }

    /// `HGET`；字段缺失返回 `Ok(None)`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn hget(&self, key: &str, field: &str) -> RedisResult<Option<Vec<u8>>> {
        let key = key.to_owned();
        let field = field.to_owned();
        self.with_pool_conn(
            move |mut conn| async move { map_redis_result(conn.hget(key, field).await) },
        )
        .await
    }

    /// `HDEL`；返回删除字段数（`fields` 为空直接返回 0）。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn hdel(&self, key: &str, fields: &[&str]) -> RedisResult<i64> {
        if fields.is_empty() {
            return Ok(0);
        }
        let key = key.to_owned();
        let fields: Vec<String> = fields.iter().map(|field| (*field).to_owned()).collect();
        self.with_pool_conn(move |mut conn| async move {
            map_redis_result(conn.hdel(key, fields).await)
        })
        .await
    }

    /// `HGETALL` → `(field, value)` 列表。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn hgetall(&self, key: &str) -> RedisResult<Vec<(String, Vec<u8>)>> {
        let key = key.to_owned();
        self.with_pool_conn(move |mut conn| async move {
            let map: std::collections::HashMap<String, Vec<u8>> =
                map_redis_result(conn.hgetall(key).await)?;
            Ok(map.into_iter().collect())
        })
        .await
    }

    // ── List ──────────────────────────────────────────────────────────────

    /// `LPUSH`；返回推入后列表长度。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn lpush(&self, key: &str, value: Vec<u8>) -> RedisResult<i64> {
        let key = key.to_owned();
        self.with_pool_conn(move |mut conn| async move {
            map_redis_result(conn.lpush(key, value).await)
        })
        .await
    }

    /// `RPUSH`；返回推入后列表长度。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn rpush(&self, key: &str, value: Vec<u8>) -> RedisResult<i64> {
        let key = key.to_owned();
        self.with_pool_conn(move |mut conn| async move {
            map_redis_result(conn.rpush(key, value).await)
        })
        .await
    }

    /// `LPOP`；空列表返回 `Ok(None)`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn lpop(&self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        let key = key.to_owned();
        self.with_pool_conn(
            move |mut conn| async move { map_redis_result(conn.lpop(key, None).await) },
        )
        .await
    }

    /// `LRANGE start stop`（含端点）。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn lrange(&self, key: &str, start: isize, stop: isize) -> RedisResult<Vec<Vec<u8>>> {
        let key = key.to_owned();
        self.with_pool_conn(move |mut conn| async move {
            map_redis_result(conn.lrange(key, start, stop).await)
        })
        .await
    }

    /// `BLPOP` 阻塞弹出；返回 `(key, value)`，超时无元素返回 `Ok(None)`。
    ///
    /// Redis 的 `BLPOP` 超时精度为秒，因此 `timeout` 会被向上取整到至少 1 秒；阻塞预算取
    /// `command_timeout.max(timeout + 1s)`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或阻塞预算超时时返回错误。
    pub async fn blpop(
        &self,
        key: &str,
        timeout: std::time::Duration,
    ) -> RedisResult<Option<(String, Vec<u8>)>> {
        let seconds = timeout.as_secs().max(1);
        let key = key.to_owned();
        let budget = self
            .pool()
            .command_timeout()
            .max(timeout + std::time::Duration::from_secs(1));
        self.pool()
            .with_conn_budget(budget, move |mut conn| async move {
                map_redis_result(
                    redis::cmd("BLPOP")
                        .arg(&key)
                        .arg(seconds)
                        .query_async(&mut conn)
                        .await,
                )
            })
            .await
    }

    // ── Set ───────────────────────────────────────────────────────────────

    /// `SADD`；返回新增成员数。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn sadd(&self, key: &str, member: Vec<u8>) -> RedisResult<i64> {
        let key = key.to_owned();
        self.with_pool_conn(move |mut conn| async move {
            map_redis_result(conn.sadd(key, member).await)
        })
        .await
    }

    /// `SISMEMBER`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn sismember(&self, key: &str, member: &[u8]) -> RedisResult<bool> {
        let key = key.to_owned();
        let member = member.to_vec();
        self.with_pool_conn(move |mut conn| async move {
            map_redis_result(conn.sismember(key, member).await)
        })
        .await
    }

    /// `SREM`；返回移除成员数。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn srem(&self, key: &str, member: &[u8]) -> RedisResult<i64> {
        let key = key.to_owned();
        let member = member.to_vec();
        self.with_pool_conn(move |mut conn| async move {
            map_redis_result(conn.srem(key, member).await)
        })
        .await
    }

    // ── Sorted Set ────────────────────────────────────────────────────────

    /// `ZADD`；返回新增成员数。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn zadd(&self, key: &str, member: Vec<u8>, score: f64) -> RedisResult<i64> {
        let key = key.to_owned();
        self.with_pool_conn(move |mut conn| async move {
            map_redis_result(conn.zadd(key, member, score).await)
        })
        .await
    }

    /// `ZSCORE`；成员缺失返回 `Ok(None)`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn zscore(&self, key: &str, member: &[u8]) -> RedisResult<Option<f64>> {
        let key = key.to_owned();
        let member = member.to_vec();
        self.with_pool_conn(move |mut conn| async move {
            map_redis_result(conn.zscore(key, member).await)
        })
        .await
    }

    /// `ZREM`；返回移除成员数。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn zrem(&self, key: &str, member: &[u8]) -> RedisResult<i64> {
        let key = key.to_owned();
        let member = member.to_vec();
        self.with_pool_conn(move |mut conn| async move {
            map_redis_result(conn.zrem(key, member).await)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::error::RedisError;
    use crate::pool::RedisPool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn empty_hdel_short_circuits() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = RedisPool::test_probe(calls.clone()).client();
        assert_eq!(client.hdel("k", &[]).await.expect("空 fields"), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn structure_commands_enter_driver() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = RedisPool::test_probe(calls.clone()).client();
        let err = client
            .hset("k", "f", b"v".to_vec())
            .await
            .expect_err("probe");
        assert!(matches!(err, RedisError::Connection(_)), "{err}");
        let _ = client.hget("k", "f").await;
        let _ = client.hgetall("k").await;
        let _ = client.hdel("k", &["f1", "f2"]).await;
        let _ = client.lpush("k", b"v".to_vec()).await;
        let _ = client.rpush("k", b"v".to_vec()).await;
        let _ = client.lpop("k").await;
        let _ = client.lrange("k", 0, -1).await;
        let _ = client
            .blpop("k", std::time::Duration::from_millis(10))
            .await;
        let _ = client.sadd("k", b"m".to_vec()).await;
        let _ = client.sismember("k", b"m").await;
        let _ = client.srem("k", b"m").await;
        let _ = client.zadd("k", b"m".to_vec(), 1.5).await;
        let _ = client.zscore("k", b"m").await;
        let _ = client.zrem("k", b"m").await;
        assert!(
            calls.load(Ordering::SeqCst) >= 13,
            "结构命令应进入池连接路径"
        );
    }
}
