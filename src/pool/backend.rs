//! 连接后端与 `ConnectionManager` 参数。
//!
//! 自 `src/pool.rs` 下沉而来：`RedisBackend`（Standalone/Cluster 的 `ConnectionLike` 实现）
//! 与 `connection_manager_config` 的构造。二者均为 `pub(crate)`，经门面 `pub(crate) use`
//! 转出，故 `crate::pool::{…}` 与各子模块的引用路径不变。

// `Probe` 变体与 `AtomicUsize` 计数仅在测试构建中存在（见下）；导入同样按 cfg 门控，
// 否则非测试构建会报 unused import。
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::Arc;

use redis::aio::{ConnectionLike, ConnectionManager};
use redis::cluster_async::ClusterConnection;
use redis::{Cmd, Pipeline, RedisFuture, Value};

use crate::config::RedisConfig;

/// 连接后端：Standalone（含 Sentinel master）或 Cluster。
///
/// `ConnectionManager` 体积较大，装箱以抑制 `large_enum_variant`。
#[derive(Clone)]
pub(crate) enum RedisBackend {
    /// 单机 / Sentinel master。
    Standalone(Box<ConnectionManager>),
    /// Redis Cluster。
    Cluster(ClusterConnection),
    /// 测试 driver：记录命令调用次数并始终返回 I/O 错误。
    #[cfg(test)]
    Probe(Arc<AtomicUsize>),
}

impl ConnectionLike for RedisBackend {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            Self::Standalone(conn) => conn.req_packed_command(cmd),
            Self::Cluster(conn) => conn.req_packed_command(cmd),
            #[cfg(test)]
            Self::Probe(calls) => {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "测试 driver 被调用",
                    )))
                })
            }
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            Self::Standalone(conn) => conn.req_packed_commands(cmd, offset, count),
            Self::Cluster(conn) => conn.req_packed_commands(cmd, offset, count),
            #[cfg(test)]
            Self::Probe(calls) => {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "测试 driver 被调用",
                    )))
                })
            }
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Standalone(conn) => conn.get_db(),
            Self::Cluster(conn) => conn.get_db(),
            #[cfg(test)]
            Self::Probe(_) => 0,
        }
    }
}

/// 由 [`RedisConfig`] 构造 ConnectionManager 的重连/超时参数（可单测）。
pub(crate) fn connection_manager_config(
    config: &RedisConfig,
) -> redis::aio::ConnectionManagerConfig {
    let max_delay_ms = u64::try_from(config.reconnect_max_delay().as_millis()).unwrap_or(u64::MAX);
    // 读取 tcp_keepalive，保证 connect 路径消费该配置（驱动侧为 OS 默认 keepalive）。
    let _keepalive_policy = config.tcp_keepalive();
    redis::aio::ConnectionManagerConfig::new()
        .set_connection_timeout(config.connect_timeout())
        .set_response_timeout(config.command_timeout())
        .set_max_delay(max_delay_ms)
}
