//! 错误类型与结果别名。
//!
//! [`RedisError`] 覆盖配置、连接、远端、序列化、I/O、超时与「不支持」七类基础分类，
//! 并额外保留瞬时故障、状态冲突、目标缺失与内部错误四类语义，以便调用方按需分流。
//! 只有明确可安全重试的分类才会让 [`RedisError::is_retryable`] 返回 `true`。

/// Redis 适配器错误类型。
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RedisError {
    /// 配置非法（本地校验失败、凭证错误、URL/种子节点不可解析）。
    #[error("配置无效: {0}")]
    Config(String),
    /// 连接建立或维护失败（节点不可用、集群下线、只读节点、I/O 中断）。
    #[error("连接失败: {0}")]
    Connection(String),
    /// 远端返回业务或协议错误（认证被拒、类型错误、未分类响应等）。
    #[error("远端返回错误: {0}")]
    Backend(String),
    /// 序列化或解析失败。
    #[error("序列化失败: {0}")]
    Serialization(String),
    /// 本地网络或文件 I/O 失败。
    #[error("I/O 失败: {0}")]
    Io(#[from] std::io::Error),
    /// 操作超时（命令预算、调用总 deadline 或重试 deadline 耗尽）。
    #[error("操作超时: {0}")]
    Timeout(String),
    /// 当前能力不支持（例如 Pub/Sub 拓扑降级、Unix socket 等）。
    #[error("不支持的操作: {0}")]
    Unsupported(String),
    /// 服务端明确返回的可恢复瞬时故障（LOADING / TRYAGAIN / 集群重定向）。
    #[error("瞬时故障: {0}")]
    Transient(String),
    /// 状态冲突（锁竞争失败、事务 EXECABORT）。
    #[error("状态冲突: {0}")]
    Conflict(String),
    /// 目标不存在（key 缺失、Lua 脚本未加载）。
    #[error("目标不存在: {0}")]
    Missing(String),
    /// 未归类的内部错误（不变量被破坏、解析出非预期载荷）。
    #[error("内部错误: {0}")]
    Internal(String),
}

impl RedisError {
    /// 是否属于可安全重试的瞬时错误。
    ///
    /// 可重试分类为 [`RedisError::Connection`]（含集群重定向/节点不可用）、
    /// [`RedisError::Transient`]（LOADING / TRYAGAIN）、[`RedisError::Timeout`] 与
    /// [`RedisError::Io`]。
    ///
    /// 配置错误、认证被拒、业务错误、序列化失败、冲突、缺失与内部错误一律不可自动重试：
    /// 重试它们只会重复失败或放大副作用。
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Connection(_) | Self::Transient(_) | Self::Timeout(_) | Self::Io(_)
        )
    }

    /// 稳定的分类标签，便于日志与指标低基数打点。
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Config(_) => "config",
            Self::Connection(_) => "connection",
            Self::Backend(_) => "backend",
            Self::Serialization(_) => "serialization",
            Self::Io(_) => "io",
            Self::Timeout(_) => "timeout",
            Self::Unsupported(_) => "unsupported",
            Self::Transient(_) => "transient",
            Self::Conflict(_) => "conflict",
            Self::Missing(_) => "missing",
            Self::Internal(_) => "internal",
        }
    }
}

/// crate 专用结果别名。
pub type RedisResult<T> = Result<T, RedisError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_categories() {
        assert!(RedisError::Connection("x".into()).is_retryable());
        assert!(RedisError::Transient("x".into()).is_retryable());
        assert!(RedisError::Timeout("x".into()).is_retryable());
        assert!(RedisError::Io(std::io::Error::other("x")).is_retryable());
    }

    #[test]
    fn non_retryable_categories() {
        for err in [
            RedisError::Config("x".into()),
            RedisError::Backend("x".into()),
            RedisError::Serialization("x".into()),
            RedisError::Unsupported("x".into()),
            RedisError::Conflict("x".into()),
            RedisError::Missing("x".into()),
            RedisError::Internal("x".into()),
        ] {
            assert!(!err.is_retryable(), "不得自动重试: {err}");
        }
    }

    #[test]
    fn io_error_converts_and_labels_are_unique_in_shape() {
        let err: RedisError = std::io::Error::other("boom").into();
        assert_eq!(err.label(), "io");
        assert!(err.to_string().contains("boom"));
        assert_eq!(RedisError::Config(String::new()).label(), "config");
        assert_eq!(RedisError::Backend(String::new()).label(), "backend");
        assert_eq!(
            RedisError::Serialization(String::new()).label(),
            "serialization"
        );
        assert_eq!(
            RedisError::Unsupported(String::new()).label(),
            "unsupported"
        );
        assert_eq!(RedisError::Transient(String::new()).label(), "transient");
        assert_eq!(RedisError::Conflict(String::new()).label(), "conflict");
        assert_eq!(RedisError::Missing(String::new()).label(), "missing");
        assert_eq!(RedisError::Internal(String::new()).label(), "internal");
        assert_eq!(RedisError::Connection(String::new()).label(), "connection");
        assert_eq!(RedisError::Timeout(String::new()).label(), "timeout");
    }
}
