//! 可选 Redis Pub/Sub（feature `pubsub`）：独占订阅连接，不占用命令 lane。
//!
//! 仅支持 Standalone 拓扑：Cluster 与 Sentinel 会在任何网络 I/O 之前失败，避免静默降级到
//! 错误节点、绕过池的拓扑与安全配置。
//!
//! **不提供可靠投递**：连接断开时 [`RedisPubSub::into_message_stream`] 会静默结束，
//! [`RedisPubSub::into_result_message_stream`] 会额外在流末尾产出一次
//! [`RedisError::Connection`]，调用方须重建会话。

use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use tokio::time::timeout;

use crate::config::{RedisConfig, RedisMode};
use crate::error::{RedisError, RedisResult};
use crate::error_map::map_redis_result;

/// Redis Pub/Sub 原始消息。
///
/// `channel` 与 `payload` 直接复制自 Redis 协议消息；本类型不生成业务消息 ID，也不映射到
/// 上层消息契约。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisPubSubMessage {
    /// Redis 频道原始字节。
    pub channel: Bytes,
    /// Redis 消息负载原始字节。
    pub payload: Bytes,
}

impl RedisPubSubMessage {
    fn from_redis(message: &redis::Msg) -> Self {
        Self {
            channel: Bytes::copy_from_slice(message.get_channel_name().as_bytes()),
            payload: Bytes::copy_from_slice(message.get_payload_bytes()),
        }
    }
}

/// 专用 Pub/Sub 会话。`Drop` 时底层订阅任务随之结束。
pub struct RedisPubSub {
    /// 用于 `PUBLISH` 的独立连接管理器（与订阅连接分离）。
    publish_conn: redis::aio::ConnectionManager,
    /// 已订阅频道的消息流。
    stream: Option<redis::aio::PubSubStream>,
    endpoint: String,
}

impl std::fmt::Debug for RedisPubSub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSub")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl RedisPubSub {
    /// 使用显式配置建立 Pub/Sub 并订阅给定频道。
    ///
    /// 认证、TLS 与端点全部来自该配置；本方法不会重新读取环境变量。
    ///
    /// # Errors
    ///
    /// 拓扑不受支持返回 [`RedisError::Unsupported`]；建连、订阅失败或超时返回对应错误。
    pub async fn connect_config(
        cfg: RedisConfig,
        channels: impl IntoIterator<Item = String>,
    ) -> RedisResult<Self> {
        let endpoint = cfg.display_endpoint();
        let info = pubsub_connection_info(&cfg)?;
        let client = redis::Client::open(info)
            .map_err(|e| RedisError::Connection(format!("redis PubSub 客户端创建失败: {e}")))?;

        let manager_config = redis::aio::ConnectionManagerConfig::new()
            .set_connection_timeout(cfg.connect_timeout())
            .set_response_timeout(cfg.command_timeout());
        let publish_conn = timeout(
            cfg.connect_timeout(),
            redis::aio::ConnectionManager::new_with_config(client.clone(), manager_config),
        )
        .await
        .map_err(|_| RedisError::Timeout("redis pubsub publish 连接超时".to_owned()))?
        .map_err(|e| RedisError::Connection(format!("redis pubsub publish 连接失败: {e}")))?;

        let mut pubsub = timeout(cfg.connect_timeout(), client.get_async_pubsub())
            .await
            .map_err(|_| RedisError::Timeout("redis pubsub 订阅连接超时".to_owned()))?
            .map_err(|e| RedisError::Connection(format!("redis pubsub 连接失败: {e}")))?;

        for channel in channels {
            let subscribed = timeout(cfg.command_timeout(), pubsub.subscribe(channel.as_str()))
                .await
                .map_err(|_| RedisError::Timeout("redis pubsub 订阅命令超时".to_owned()))?;
            map_redis_result(subscribed)?;
        }

        Ok(Self {
            publish_conn,
            stream: Some(pubsub.into_on_message()),
            endpoint,
        })
    }

    /// 脱敏端点。
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// `PUBLISH` 一条原始消息。
    ///
    /// # Errors
    ///
    /// 连接失败或超时时返回错误。
    pub async fn publish(&self, channel: &str, payload: &[u8]) -> RedisResult<()> {
        let mut conn = self.publish_conn.clone();
        let _: i64 = map_redis_result(
            redis::cmd("PUBLISH")
                .arg(channel)
                .arg(payload)
                .query_async(&mut conn)
                .await,
        )?;
        Ok(())
    }

    /// 取出消息流（只能调用一次）。连接断开时流静默结束，不产出错误项。
    ///
    /// # Errors
    ///
    /// 流已被取走时返回 [`RedisError::Internal`]。
    pub fn into_message_stream(mut self) -> RedisResult<BoxStream<'static, RedisPubSubMessage>> {
        let stream = self
            .stream
            .take()
            .ok_or_else(|| RedisError::Internal("PubSub stream 已被取走".to_owned()))?;
        Ok(Box::pin(
            stream.map(|message| RedisPubSubMessage::from_redis(&message)),
        ))
    }

    /// 取出 `Result` 消息流（只能调用一次）。
    ///
    /// 每条消息为 `Ok(RedisPubSubMessage)`；底层连接结束（断线 / 对端关闭）时在末尾**恰好一次**
    /// 产出 `Err`，避免静默 EOF。
    ///
    /// # Errors
    ///
    /// 流已被取走时返回 [`RedisError::Internal`]。
    pub fn into_result_message_stream(
        mut self,
    ) -> RedisResult<BoxStream<'static, RedisResult<RedisPubSubMessage>>> {
        let stream = self
            .stream
            .take()
            .ok_or_else(|| RedisError::Internal("PubSub stream 已被取走".to_owned()))?;
        let mapped = stream.map(|message| Ok(RedisPubSubMessage::from_redis(&message)));
        let with_disconnect = mapped.chain(futures_util::stream::once(async {
            Err(RedisError::Connection("redis pubsub 连接已断开".to_owned()))
        }));
        Ok(Box::pin(with_disconnect))
    }
}

/// 构造 Pub/Sub 连接信息：仅 Standalone 允许，其余拓扑 fail-closed。
fn pubsub_connection_info(cfg: &RedisConfig) -> RedisResult<redis::ConnectionInfo> {
    match cfg.mode() {
        RedisMode::Standalone => cfg.to_connection_info(),
        RedisMode::Cluster => Err(RedisError::Unsupported(
            "Redis Pub/Sub 尚不支持 Cluster；拒绝降级到 Standalone 节点".to_owned(),
        )),
        RedisMode::Sentinel => Err(RedisError::Unsupported(
            "Redis Pub/Sub 尚不支持 Sentinel master 跟随；拒绝使用静态种子节点".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn standalone_pubsub_reuses_acl_and_tls_config() {
        let cfg = RedisConfig::builder()
            .addr("redis.example:6380")
            .username("pubsub-user")
            .password(String::from_utf8(vec![b's', b'e', b'c', b'r', b'e', b't']).expect("utf8"))
            .db(4)
            .tls(true)
            .build()
            .expect("cfg");

        let info = pubsub_connection_info(&cfg).expect("standalone info");
        assert_eq!(info.redis.username.as_deref(), Some("pubsub-user"));
        assert!(info.redis.password.is_some());
        assert_eq!(info.redis.db, 4);
        assert!(matches!(
            info.addr,
            redis::ConnectionAddr::TcpTls {
                insecure: false,
                ..
            }
        ));
    }

    #[test]
    fn cluster_pubsub_fails_closed_before_connect() {
        let cfg = RedisConfig::builder()
            .mode(RedisMode::Cluster)
            .nodes(["127.0.0.1:7000"])
            .build()
            .expect("cfg");
        let err = pubsub_connection_info(&cfg).expect_err("cluster 不得降级");
        assert!(matches!(err, RedisError::Unsupported(_)));
        assert!(err.to_string().contains("Cluster"));
    }

    #[test]
    fn sentinel_pubsub_fails_closed_before_connect() {
        let cfg = RedisConfig::builder()
            .mode(RedisMode::Sentinel)
            .nodes(["127.0.0.1:26379"])
            .sentinel_master("mymaster")
            .build()
            .expect("cfg");
        let err = pubsub_connection_info(&cfg).expect_err("sentinel 不得用种子当 master");
        assert!(matches!(err, RedisError::Unsupported(_)));
        assert!(err.to_string().contains("Sentinel"));
    }

    #[test]
    fn pubsub_deadlines_come_from_same_config() {
        let connect = Duration::from_millis(17);
        let command = Duration::from_millis(23);
        let cfg = RedisConfig::builder()
            .connect_timeout(connect)
            .command_timeout(command)
            .build()
            .expect("cfg");
        assert_eq!(cfg.connect_timeout(), connect);
        assert_eq!(cfg.command_timeout(), command);
        assert!(pubsub_connection_info(&cfg).is_ok());
    }

    #[test]
    fn raw_message_keeps_channel_and_payload_bytes() {
        let message = RedisPubSubMessage {
            channel: Bytes::from_static(b"prices.raw"),
            payload: Bytes::from_static(&[0, 1, 255]),
        };
        assert_eq!(message.channel.as_ref(), b"prices.raw");
        assert_eq!(message.payload.as_ref(), &[0, 1, 255]);
        assert_eq!(message.clone(), message);
    }

    #[test]
    fn stream_entry_points_are_named_on_type() {
        fn _message_stream(
            _: fn(RedisPubSub) -> RedisResult<BoxStream<'static, RedisPubSubMessage>>,
        ) {
        }
        fn _result_stream(
            _: fn(RedisPubSub) -> RedisResult<BoxStream<'static, RedisResult<RedisPubSubMessage>>>,
        ) {
        }
        _message_stream(RedisPubSub::into_message_stream);
        _result_stream(RedisPubSub::into_result_message_stream);
    }

    #[tokio::test]
    async fn connect_to_unreachable_endpoint_fails() {
        let cfg = RedisConfig::builder()
            .addr("127.0.0.1:1")
            .connect_timeout(Duration::from_millis(200))
            .command_timeout(Duration::from_millis(200))
            .build()
            .expect("cfg");
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            RedisPubSub::connect_config(cfg, ["ch".to_owned()]),
        )
        .await;
        match result {
            Ok(Ok(pubsub)) => panic!("不应连接到 127.0.0.1:1: {pubsub:?}"),
            Ok(Err(err)) => assert!(
                matches!(err, RedisError::Connection(_) | RedisError::Timeout(_)),
                "{err}"
            ),
            Err(_) => {}
        }
    }
}
