//! `redis` crate 错误 → [`RedisError`] 映射。
//!
//! 映射策略（决定上层能否自动重试）：
//!
//! | `redis::ErrorKind` | 映射结果 | 可自动重试 |
//! | --- | --- | --- |
//! | `InvalidClientConfig` / `ClientError` | [`RedisError::Config`] | 否 |
//! | `AuthenticationFailed` / `NOAUTH` / `WRONGPASS` | [`RedisError::Backend`] | 否 |
//! | `BusyLoadingError` / `TryAgain` / `LOADING` / `TRYAGAIN` | [`RedisError::Transient`] | 是 |
//! | `Moved` / `Ask` / `CrossSlot` | [`RedisError::Transient`] | 是 |
//! | `IoError` | [`RedisError::Connection`] | 是 |
//! | `ClusterDown` / `MasterDown` / `ReadOnly` / 哨兵无可用节点 | [`RedisError::Connection`] | 是 |
//! | `ExecAbortError` | [`RedisError::Conflict`] | 否 |
//! | `NoScriptError` | [`RedisError::Missing`] | 否 |
//! | `TypeError` 及其余未分类 | [`RedisError::Internal`] | 否 |

use crate::error::RedisError;

/// 将 `redis::RedisError` 映射为可分类的 [`RedisError`]。
///
/// `ResponseError` / `ExtensionError` 的语义差异只体现在错误文本中，因此对这两类额外做
/// 文本判定（`LOADING` / `TRYAGAIN` / `NOAUTH` / `WRONGPASS` / `READONLY`）。
#[must_use]
pub fn map_redis_error(err: redis::RedisError) -> RedisError {
    use redis::ErrorKind as Rk;

    let msg = err.to_string();
    match err.kind() {
        Rk::InvalidClientConfig | Rk::ClientError => {
            RedisError::Config(format!("redis 客户端参数非法: {msg}"))
        }
        Rk::AuthenticationFailed => RedisError::Backend(format!("redis 认证失败: {msg}")),
        // 服务端「尚未就绪」：等待后重试通常可恢复。
        Rk::BusyLoadingError | Rk::TryAgain => {
            RedisError::Transient(format!("redis 可恢复故障: {msg}"))
        }
        // 集群重定向 / 跨 slot：上层在有限重试后应升级为失败。
        Rk::Moved | Rk::Ask | Rk::CrossSlot => {
            RedisError::Transient(format!("redis 集群重定向/跨 slot: {msg}"))
        }
        Rk::IoError => RedisError::Connection(format!("redis I/O 失败: {msg}")),
        Rk::ClusterDown
        | Rk::MasterDown
        | Rk::ReadOnly
        | Rk::ClusterConnectionNotFound
        | Rk::EmptySentinelList
        | Rk::MasterNameNotFoundBySentinel
        | Rk::NoValidReplicasFoundBySentinel => {
            RedisError::Connection(format!("redis 节点不可用: {msg}"))
        }
        Rk::ExecAbortError => RedisError::Conflict(format!("redis 执行中止: {msg}")),
        Rk::NoScriptError => RedisError::Missing(format!("redis 脚本不存在: {msg}")),
        Rk::TypeError => RedisError::Internal(format!("redis 类型/协议错误: {msg}")),
        Rk::ResponseError | Rk::ExtensionError | Rk::NotBusy => map_textual(&msg),
        _ => RedisError::Internal(format!("redis 未分类错误: {msg}")),
    }
}

/// 依据错误文本细分「远端返回但 kind 不区分」的错误。
fn map_textual(msg: &str) -> RedisError {
    let upper = msg.to_ascii_uppercase();
    if upper.contains("LOADING") || upper.contains("TRYAGAIN") {
        RedisError::Transient(format!("redis 响应可恢复: {msg}"))
    } else if upper.contains("NOAUTH") || upper.contains("WRONGPASS") {
        RedisError::Backend(format!("redis 认证失败: {msg}"))
    } else if upper.contains("READONLY") {
        RedisError::Connection(format!("redis 只读节点: {msg}"))
    } else {
        RedisError::Internal(format!("redis 响应错误: {msg}"))
    }
}

/// 将 `redis` 结果映射为 [`crate::RedisResult`]。
///
/// # Errors
///
/// 当底层调用返回 `Err` 时，按 [`map_redis_error`] 的分类返回对应错误。
#[inline]
pub fn map_redis_result<T>(result: redis::RedisResult<T>) -> crate::RedisResult<T> {
    result.map_err(map_redis_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapped(kind: redis::ErrorKind, message: &'static str) -> RedisError {
        map_redis_error(redis::RedisError::from((kind, message)))
    }

    #[test]
    fn transient_and_retryable_kinds() {
        assert!(matches!(
            mapped(redis::ErrorKind::BusyLoadingError, "loading"),
            RedisError::Transient(_)
        ));
        assert!(matches!(
            mapped(redis::ErrorKind::TryAgain, "try again"),
            RedisError::Transient(_)
        ));
        assert!(mapped(redis::ErrorKind::BusyLoadingError, "loading").is_retryable());
        assert!(matches!(
            mapped(redis::ErrorKind::Moved, "MOVED 3999 127.0.0.1:7001"),
            RedisError::Transient(_)
        ));
        assert!(matches!(
            mapped(redis::ErrorKind::Ask, "ASK 3999 127.0.0.1:7001"),
            RedisError::Transient(_)
        ));
        assert!(matches!(
            mapped(redis::ErrorKind::CrossSlot, "CROSSSLOT"),
            RedisError::Transient(_)
        ));
    }

    #[test]
    fn connection_kinds_are_retryable() {
        for kind in [
            redis::ErrorKind::IoError,
            redis::ErrorKind::ClusterDown,
            redis::ErrorKind::MasterDown,
            redis::ErrorKind::ReadOnly,
            redis::ErrorKind::EmptySentinelList,
            redis::ErrorKind::ClusterConnectionNotFound,
            redis::ErrorKind::MasterNameNotFoundBySentinel,
            redis::ErrorKind::NoValidReplicasFoundBySentinel,
        ] {
            let err = mapped(kind, "boom");
            assert!(
                matches!(err, RedisError::Connection(_)),
                "kind={kind:?} err={err}"
            );
            assert!(err.is_retryable());
        }
    }

    #[test]
    fn config_and_auth_are_not_retryable() {
        for kind in [
            redis::ErrorKind::InvalidClientConfig,
            redis::ErrorKind::ClientError,
        ] {
            let err = mapped(kind, "bad cfg");
            assert!(matches!(err, RedisError::Config(_)), "kind={kind:?}");
            assert!(!err.is_retryable());
        }
        let auth = mapped(redis::ErrorKind::AuthenticationFailed, "auth");
        assert!(matches!(auth, RedisError::Backend(_)));
        assert!(!auth.is_retryable());
    }

    #[test]
    fn conflict_missing_type_are_not_retryable() {
        assert!(matches!(
            mapped(redis::ErrorKind::ExecAbortError, "EXECABORT"),
            RedisError::Conflict(_)
        ));
        assert!(matches!(
            mapped(redis::ErrorKind::NoScriptError, "NOSCRIPT"),
            RedisError::Missing(_)
        ));
        assert!(matches!(
            mapped(redis::ErrorKind::TypeError, "WRONGTYPE"),
            RedisError::Internal(_)
        ));
    }

    #[test]
    fn response_error_text_dispatch() {
        assert!(matches!(
            mapped(redis::ErrorKind::ResponseError, "LOADING Redis is loading"),
            RedisError::Transient(_)
        ));
        assert!(matches!(
            mapped(redis::ErrorKind::ResponseError, "TRYAGAIN later"),
            RedisError::Transient(_)
        ));
        assert!(matches!(
            mapped(
                redis::ErrorKind::ResponseError,
                "NOAUTH Authentication required"
            ),
            RedisError::Backend(_)
        ));
        assert!(matches!(
            mapped(redis::ErrorKind::ResponseError, "READONLY You can't write"),
            RedisError::Connection(_)
        ));
        assert!(matches!(
            mapped(redis::ErrorKind::ResponseError, "ERR unknown command"),
            RedisError::Internal(_)
        ));
    }

    #[test]
    fn extension_and_not_busy_text_dispatch() {
        assert!(matches!(
            mapped(redis::ErrorKind::ExtensionError, "WRONGPASS invalid"),
            RedisError::Backend(_)
        ));
        assert!(matches!(
            mapped(redis::ErrorKind::ExtensionError, "LOADING dump"),
            RedisError::Transient(_)
        ));
        assert!(matches!(
            mapped(redis::ErrorKind::ExtensionError, "EXT custom"),
            RedisError::Internal(_)
        ));
        assert!(matches!(
            mapped(redis::ErrorKind::NotBusy, "NOTBUSY"),
            RedisError::Internal(_)
        ));
    }

    #[test]
    fn result_mapping_preserves_both_paths() {
        assert_eq!(map_redis_result(Ok(7usize)).expect("ok"), 7);
        let err = redis::RedisError::from((redis::ErrorKind::IoError, "io"));
        let mapped = map_redis_result::<()>(Err(err)).expect_err("err");
        assert!(mapped.is_retryable());
        assert_eq!(mapped.label(), "connection");
    }
}
