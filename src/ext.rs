//! Redis 扩展：Lua 脚本、Pipeline 批量写、带所有权校验的分布式锁。
//!
//! 锁实现为 `SET key <token> NX PX <ttl>`，释放与续租均通过 Lua 做
//! compare-and-delete / compare-and-expire，避免误删他人持有的锁。锁**不**提供
//! 「持有锁即正确」的业务保证；需要防脑裂的关键写应携带 [`RedisLock::fence`] 并在下游校验
//! 单调递增。

use std::time::Duration;

use redis::{AsyncCommands, Script};

use crate::client::RedisClient;
use crate::error::{RedisError, RedisResult};
use crate::error_map::map_redis_result;
use crate::pool::RedisBackend;

/// 持有锁时的所有权令牌 + fencing 序号。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisLock {
    key: String,
    token: String,
    fence: u64,
}

impl RedisLock {
    /// 锁键。
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 所有权令牌（禁止写入常规日志）。
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }

    /// fencing 序号（关键写应携带并校验单调递增）。
    #[must_use]
    pub fn fence(&self) -> u64 {
        self.fence
    }

    /// 常量时间校验候选令牌是否为本锁的所有者。
    ///
    /// 使用常量时间比较，避免通过响应时间侧信道逐字节猜测令牌。
    #[must_use]
    pub fn verify_token(&self, candidate: &str) -> bool {
        lock_token_matches(&self.token, candidate)
    }
}

/// 常量时间比较两个锁令牌（[`RedisLock::verify_token`] 的纯函数形式）。
///
/// 长度不同立即返回 `false`；长度相同时逐字节异或累加，耗时与首个不同字节的位置无关。
///
/// # Examples
///
/// ```
/// use redisx::lock_token_matches;
///
/// assert!(lock_token_matches("lock-abc", "lock-abc"));
/// assert!(!lock_token_matches("lock-abc", "lock-abd"));
/// assert!(!lock_token_matches("lock-abc", "lock-ab"), "长度不同直接不匹配");
/// ```
#[must_use]
pub fn lock_token_matches(expected: &str, candidate: &str) -> bool {
    constant_time_eq(expected.as_bytes(), candidate.as_bytes())
}

/// 生成锁所有权令牌。
///
/// 令牌由进程内计数器、当前时间与进程标识混合而成，无需外部 RNG 依赖；不同进程、不同时刻
/// 的令牌不会碰撞（同一纳秒内的同进程调用由计数器区分）。
#[must_use]
pub fn generate_lock_token() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|delta| delta.as_nanos())
        .unwrap_or(0);
    format!("lk-{nanos:x}-{:x}-{seq:x}", std::process::id())
}

/// 常量时间字节比较（长度不同立即返回不相等）。
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

/// 释放锁 Lua：仅 owner 可 `DEL`。
const RELEASE_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
"#;

/// 续租 Lua：仅 owner 可 `PEXPIRE`。
const EXTEND_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
"#;

impl RedisClient {
    /// 执行 Lua 脚本（固定脚本体 + KEYS/ARGV；禁止拼接不可信输入进脚本源）。
    ///
    /// 驱动侧 `Script` 会缓存 SHA 并优先 `EVALSHA`（固定脚本体 → 稳定 SHA）。
    ///
    /// # Errors
    ///
    /// 脚本体为空返回 [`RedisError::Config`]；其余为连接/协议/超时错误。
    pub async fn eval_script(
        &self,
        script: &str,
        keys: &[&str],
        args: &[&[u8]],
    ) -> RedisResult<redis::Value> {
        if script.trim().is_empty() {
            return Err(RedisError::Config("redis Lua 脚本不能为空".to_owned()));
        }
        let keys: Vec<String> = keys.iter().map(|key| (*key).to_owned()).collect();
        let args: Vec<Vec<u8>> = args.iter().map(|arg| arg.to_vec()).collect();
        let body = script.to_owned();
        self.with_pool_conn(move |mut conn: RedisBackend| async move {
            let script = Script::new(&body);
            let mut invocation = script.prepare_invoke();
            for key in &keys {
                invocation.key(key);
            }
            for arg in &args {
                invocation.arg(arg.as_slice());
            }
            map_redis_result(invocation.invoke_async(&mut conn).await)
        })
        .await
    }

    /// 先 `SCRIPT LOAD` 再按 SHA 调用，返回 `(sha, value)`。
    ///
    /// # Errors
    ///
    /// 脚本体为空返回 [`RedisError::Config`]；`SCRIPT LOAD` 或调用失败时返回对应错误。
    pub async fn script_load_and_eval(
        &self,
        script: &str,
        keys: &[&str],
        args: &[&[u8]],
    ) -> RedisResult<(String, redis::Value)> {
        if script.trim().is_empty() {
            return Err(RedisError::Config("redis Lua 脚本不能为空".to_owned()));
        }
        let body = script.to_owned();
        let sha: String = self
            .with_pool_conn(move |mut conn: RedisBackend| async move {
                let sha: String = map_redis_result(
                    redis::cmd("SCRIPT")
                        .arg("LOAD")
                        .arg(body)
                        .query_async(&mut conn)
                        .await,
                )?;
                Ok(sha)
            })
            .await?;
        let value = self.eval_sha(&sha, keys, args).await?;
        Ok((sha, value))
    }

    /// `EVALSHA`：仅接受已加载的 SHA；脚本不存在 → [`RedisError::Missing`]。
    ///
    /// # Errors
    ///
    /// SHA 为空返回 [`RedisError::Config`]；脚本未加载返回 [`RedisError::Missing`]。
    pub async fn eval_sha(
        &self,
        sha: &str,
        keys: &[&str],
        args: &[&[u8]],
    ) -> RedisResult<redis::Value> {
        if sha.trim().is_empty() {
            return Err(RedisError::Config("EVALSHA sha 不能为空".to_owned()));
        }
        let sha = sha.to_owned();
        let keys: Vec<String> = keys.iter().map(|key| (*key).to_owned()).collect();
        let args: Vec<Vec<u8>> = args.iter().map(|arg| arg.to_vec()).collect();
        self.with_pool_conn(move |mut conn: RedisBackend| async move {
            let mut cmd = redis::cmd("EVALSHA");
            cmd.arg(&sha).arg(keys.len());
            for key in &keys {
                cmd.arg(key);
            }
            for arg in &args {
                cmd.arg(arg.as_slice());
            }
            map_redis_result(cmd.query_async(&mut conn).await)
        })
        .await
    }

    /// 管道批量 `SET`（可选统一 TTL）；单次网络往返，按 `MULTI/EXEC` 执行。
    ///
    /// 跨 Cluster slot **不**承诺原子性。
    ///
    /// # Errors
    ///
    /// TTL 为 0 或亚毫秒返回 [`RedisError::Config`]；其余为连接/协议/超时错误。
    pub async fn pipeline_set(
        &self,
        items: &[(&str, Vec<u8>)],
        ttl: Option<Duration>,
    ) -> RedisResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        if let Some(ttl) = ttl {
            if ttl.is_zero() || ttl.as_millis() == 0 {
                return Err(RedisError::Config(
                    "pipeline TTL 不能为 0 或亚毫秒".to_owned(),
                ));
            }
        }
        let owned: Vec<(String, Vec<u8>)> = items
            .iter()
            .map(|(key, value)| ((*key).to_owned(), value.clone()))
            .collect();
        let ttl_ms = match ttl {
            None => None,
            Some(ttl) => Some(
                u64::try_from(ttl.as_millis())
                    .map_err(|_| RedisError::Config("TTL 过大".to_owned()))?,
            ),
        };
        self.with_pool_conn(move |mut conn: RedisBackend| async move {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for (key, value) in &owned {
                if let Some(millis) = ttl_ms {
                    pipe.cmd("PSETEX")
                        .arg(key)
                        .arg(millis)
                        .arg(value.as_slice())
                        .ignore();
                } else {
                    pipe.cmd("SET").arg(key).arg(value.as_slice()).ignore();
                }
            }
            let _: () = map_redis_result(pipe.query_async(&mut conn).await)?;
            Ok(())
        })
        .await
    }

    /// 获取分布式锁：`SET key token NX PX ttl` + fencing `INCR fence:{key}`。
    ///
    /// 竞争失败返回 [`RedisError::Conflict`]。
    ///
    /// # Errors
    ///
    /// key 为空或 TTL 非法返回 [`RedisError::Config`]；竞争失败返回
    /// [`RedisError::Conflict`]；其余为连接/协议/超时错误。
    pub async fn lock_acquire(&self, key: &str, ttl: Duration) -> RedisResult<RedisLock> {
        if key.is_empty() {
            return Err(RedisError::Config("锁 key 不能为空".to_owned()));
        }
        if ttl.is_zero() || ttl.as_millis() == 0 {
            return Err(RedisError::Config("锁 TTL 不能为 0 或亚毫秒".to_owned()));
        }
        let millis = u64::try_from(ttl.as_millis())
            .map_err(|_| RedisError::Config("锁 TTL 过大".to_owned()))?;
        let token = generate_lock_token();
        let key_owned = key.to_owned();
        let fence_key = format!("fence:{key}");

        let acquired = self
            .with_pool_conn({
                let key = key_owned.clone();
                let token = token.clone();
                move |mut conn: RedisBackend| async move {
                    let reply: Option<String> = map_redis_result(
                        redis::cmd("SET")
                            .arg(&key)
                            .arg(&token)
                            .arg("NX")
                            .arg("PX")
                            .arg(millis)
                            .query_async(&mut conn)
                            .await,
                    )?;
                    Ok(reply.is_some())
                }
            })
            .await?;

        if !acquired {
            return Err(RedisError::Conflict(format!(
                "redis 锁竞争失败: {key_owned}"
            )));
        }

        let fence: i64 = self
            .with_pool_conn(move |mut conn: RedisBackend| async move {
                map_redis_result(conn.incr(fence_key, 1_i64).await)
            })
            .await?;

        if fence < 0 {
            return Err(RedisError::Internal(format!("redis fencing 异常: {fence}")));
        }

        Ok(RedisLock {
            key: key_owned,
            token,
            fence: fence.unsigned_abs(),
        })
    }

    /// 释放锁（compare-and-delete）；非 owner 返回 `Ok(false)`。
    ///
    /// # Errors
    ///
    /// 脚本执行失败或超时时返回错误。
    pub async fn lock_release(&self, lock: &RedisLock) -> RedisResult<bool> {
        let value = self
            .eval_script(RELEASE_LUA, &[lock.key.as_str()], &[lock.token.as_bytes()])
            .await?;
        Ok(redis_value_as_i64(&value)? > 0)
    }

    /// 续租（compare-and-expire）；非 owner 返回 `Ok(false)`。
    ///
    /// # Errors
    ///
    /// TTL 非法返回 [`RedisError::Config`]；脚本执行失败或超时时返回错误。
    pub async fn lock_extend(&self, lock: &RedisLock, ttl: Duration) -> RedisResult<bool> {
        if ttl.is_zero() || ttl.as_millis() == 0 {
            return Err(RedisError::Config("续租 TTL 不能为 0 或亚毫秒".to_owned()));
        }
        let millis = u64::try_from(ttl.as_millis())
            .map_err(|_| RedisError::Config("续租 TTL 过大".to_owned()))?;
        let millis_bytes = millis.to_string().into_bytes();
        let value = self
            .eval_script(
                EXTEND_LUA,
                &[lock.key.as_str()],
                &[lock.token.as_bytes(), millis_bytes.as_slice()],
            )
            .await?;
        Ok(redis_value_as_i64(&value)? > 0)
    }
}

/// 将 Lua 返回的整数语义值解析为 `i64`。
fn redis_value_as_i64(value: &redis::Value) -> RedisResult<i64> {
    match value {
        redis::Value::Int(number) => Ok(*number),
        redis::Value::Okay => Ok(1),
        redis::Value::Nil => Ok(0),
        redis::Value::BulkString(bytes) => std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| RedisError::Internal("redis Lua 返回值无法解析为整数".to_owned())),
        redis::Value::SimpleString(text) => text
            .parse()
            .map_err(|_| RedisError::Internal(format!("redis Lua status 无法解析: {text}"))),
        _ => Err(RedisError::Internal(
            "redis Lua 返回了非整数类型".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::RedisPool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn probe() -> RedisClient {
        RedisPool::test_probe(Arc::new(AtomicUsize::new(0))).client()
    }

    #[tokio::test]
    async fn lock_ttl_and_key_are_validated() {
        let client = probe();
        let err = client
            .lock_acquire("k", Duration::ZERO)
            .await
            .expect_err("ttl=0");
        assert!(matches!(err, RedisError::Config(_)));
        let err = client
            .lock_acquire("k", Duration::from_nanos(1))
            .await
            .expect_err("亚毫秒");
        assert!(matches!(err, RedisError::Config(_)));
        let err = client
            .lock_acquire("", Duration::from_secs(1))
            .await
            .expect_err("空 key");
        assert!(matches!(err, RedisError::Config(_)));

        let lock = RedisLock {
            key: "k".into(),
            token: "t".into(),
            fence: 1,
        };
        let err = client
            .lock_extend(&lock, Duration::ZERO)
            .await
            .expect_err("续租 ttl=0");
        assert!(matches!(err, RedisError::Config(_)));
    }

    #[tokio::test]
    async fn script_arguments_are_validated() {
        let client = probe();
        let err = client
            .eval_script("  ", &[], &[])
            .await
            .expect_err("空脚本");
        assert!(matches!(err, RedisError::Config(_)));
        let err = client
            .script_load_and_eval("", &[], &[])
            .await
            .expect_err("空脚本");
        assert!(matches!(err, RedisError::Config(_)));
        let err = client.eval_sha(" ", &[], &[]).await.expect_err("空 sha");
        assert!(matches!(err, RedisError::Config(_)));
    }

    #[tokio::test]
    async fn pipeline_ttl_is_validated_and_empty_is_noop() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = RedisPool::test_probe(calls.clone()).client();
        client.pipeline_set(&[], None).await.expect("空 pipeline");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let err = client
            .pipeline_set(&[("k", b"v".to_vec())], Some(Duration::ZERO))
            .await
            .expect_err("ttl=0");
        assert!(matches!(err, RedisError::Config(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn lock_and_script_paths_reach_driver() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = RedisPool::test_probe(calls.clone()).client();
        let err = client
            .lock_acquire("lk", Duration::from_secs(1))
            .await
            .expect_err("probe");
        assert!(
            err.is_retryable() || matches!(err, RedisError::Internal(_)),
            "{err}"
        );

        let lock = RedisLock {
            key: "lk".into(),
            token: "t".into(),
            fence: 1,
        };
        // probe 返回非整数值 → 解析失败
        let _ = client.lock_release(&lock).await;
        let _ = client.lock_extend(&lock, Duration::from_secs(1)).await;
        let _ = client.eval_script("return 1", &["k"], &[b"1"]).await;
        let _ = client
            .script_load_and_eval("return 1", &["k"], &[b"1"])
            .await;
        let _ = client.eval_sha("deadbeef", &["k"], &[b"1"]).await;
        let _ = client
            .pipeline_set(&[("a", b"1".to_vec())], Some(Duration::from_secs(1)))
            .await;
        assert!(
            calls.load(Ordering::SeqCst) >= 6,
            "扩展命令应进入池连接路径"
        );
    }

    #[test]
    fn lock_accessors_and_token_verification() {
        let token = generate_lock_token();
        let lock = RedisLock {
            key: "k".into(),
            token: token.clone(),
            fence: 7,
        };
        assert_eq!(lock.key(), "k");
        assert_eq!(lock.token(), &token);
        assert_eq!(lock.fence(), 7);
        assert!(lock.verify_token(&token));
        assert!(!lock.verify_token("other"));
        assert!(!lock.verify_token(""));
        assert!(!lock.verify_token(&token[..token.len() - 1]));

        // 常量时间比较矩阵
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn generated_tokens_are_unique_and_well_formed() {
        let first = generate_lock_token();
        let second = generate_lock_token();
        assert!(first.starts_with("lk-"));
        assert!(first.contains('-'));
        assert_ne!(first, second);
        assert!(first.len() > 8);

        let other = generate_lock_token();
        assert!(!RedisLock {
            key: "k".into(),
            token: first,
            fence: 0
        }
        .verify_token(&other));
    }

    #[test]
    fn lua_value_conversion_matrix() {
        assert_eq!(redis_value_as_i64(&redis::Value::Int(42)).expect("int"), 42);
        assert_eq!(redis_value_as_i64(&redis::Value::Okay).expect("ok"), 1);
        assert_eq!(redis_value_as_i64(&redis::Value::Nil).expect("nil"), 0);
        assert_eq!(
            redis_value_as_i64(&redis::Value::BulkString(b"9".to_vec())).expect("bulk"),
            9
        );
        assert_eq!(
            redis_value_as_i64(&redis::Value::SimpleString("3".into())).expect("simple"),
            3
        );
        assert!(redis_value_as_i64(&redis::Value::BulkString(b"not-int".to_vec())).is_err());
        assert!(redis_value_as_i64(&redis::Value::SimpleString("x".into())).is_err());
        assert!(redis_value_as_i64(&redis::Value::Array(vec![])).is_err());
    }
}
