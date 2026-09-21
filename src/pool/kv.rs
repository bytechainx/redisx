//! 单命令原语集合（被 `RedisPool` / `RedisPoolPermit` / `RedisClient` 共用）。

use super::*;

/// 校验 TTL：`None` 合法；0 或小于 1ms 非法。
pub(crate) fn validate_ttl(ttl: Option<Duration>) -> RedisResult<()> {
    match ttl {
        None => Ok(()),
        Some(ttl) if ttl.is_zero() => Err(RedisError::Config(
            "TTL 不能为 0（Some(0) 视为非法）".to_owned(),
        )),
        Some(ttl) if ttl.as_millis() == 0 => {
            Err(RedisError::Config("TTL 过短（小于 1ms）".to_owned()))
        }
        Some(_) => Ok(()),
    }
}

/// TTL → 毫秒（拒绝 0 与亚毫秒）。
pub(crate) fn ttl_to_millis(ttl: Duration) -> RedisResult<u64> {
    validate_ttl(Some(ttl))?;
    u64::try_from(ttl.as_millis()).map_err(|_| RedisError::Config("TTL 过大".to_owned()))
}

/// `GET`。
pub(crate) async fn get(mut conn: RedisBackend, key: &str) -> RedisResult<Option<Vec<u8>>> {
    map_redis_result(conn.get(key).await)
}

/// `SET`（无 TTL）。
pub(crate) async fn set(mut conn: RedisBackend, key: &str, value: Vec<u8>) -> RedisResult<()> {
    let _: () = map_redis_result(conn.set(key, value).await)?;
    Ok(())
}

/// `PSETEX`。
pub(crate) async fn set_ex(
    mut conn: RedisBackend,
    key: &str,
    value: Vec<u8>,
    ttl: Duration,
) -> RedisResult<()> {
    let millis = ttl_to_millis(ttl)?;
    let _: () = map_redis_result(conn.pset_ex(key, value, millis).await)?;
    Ok(())
}

/// `DEL`。
pub(crate) async fn del(mut conn: RedisBackend, key: &str) -> RedisResult<bool> {
    let removed: i64 = map_redis_result(conn.del(key).await)?;
    Ok(removed > 0)
}

/// `EXISTS`。
pub(crate) async fn exists(mut conn: RedisBackend, key: &str) -> RedisResult<bool> {
    let found: i64 = map_redis_result(conn.exists(key).await)?;
    Ok(found > 0)
}

/// `INCRBY`。
pub(crate) async fn incr(mut conn: RedisBackend, key: &str, delta: i64) -> RedisResult<i64> {
    map_redis_result(conn.incr(key, delta).await)
}

/// `PEXPIRE`。
pub(crate) async fn expire(mut conn: RedisBackend, key: &str, ttl: Duration) -> RedisResult<bool> {
    let millis = i64::try_from(ttl_to_millis(ttl)?)
        .map_err(|_| RedisError::Config("TTL 过大".to_owned()))?;
    let changed: i64 = map_redis_result(
        redis::cmd("PEXPIRE")
            .arg(key)
            .arg(millis)
            .query_async(&mut conn)
            .await,
    )?;
    Ok(changed > 0)
}

/// `PTTL`。
pub(crate) async fn ttl(mut conn: RedisBackend, key: &str) -> RedisResult<Option<Duration>> {
    let millis: i64 = map_redis_result(redis::cmd("PTTL").arg(key).query_async(&mut conn).await)?;
    match millis {
        -2 => Err(RedisError::Missing(format!("redis key 不存在: {key}"))),
        -1 => Ok(None),
        negative if negative < 0 => {
            Err(RedisError::Internal(format!("redis PTTL 异常: {negative}")))
        }
        positive => Ok(Some(Duration::from_millis(positive.unsigned_abs()))),
    }
}

/// `PING`。
pub(crate) async fn ping(mut conn: RedisBackend) -> RedisResult<()> {
    let pong: String = map_redis_result(redis::cmd("PING").query_async(&mut conn).await)?;
    if pong.is_empty() {
        return Err(RedisError::Internal("redis PING 返回空响应".to_owned()));
    }
    Ok(())
}
