//! 按 [`RedisConfig`](crate::RedisConfig) 建连：Standalone / Cluster / Sentinel。

use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::sentinel::{Sentinel, SentinelNodeConnectionInfo};
use redis::TlsMode;
use tokio::time::timeout;

use crate::config::RedisConfig;
use crate::error::{RedisError, RedisResult};

use super::connection_manager_config;
use super::RedisBackend;

pub(super) async fn connect_standalone(config: &RedisConfig) -> RedisResult<RedisBackend> {
    let info = config.to_connection_info()?;
    let client = redis::Client::open(info)
        .map_err(|e| RedisError::Connection(format!("redis 客户端构建失败: {e}")))?;
    let manager_config = connection_manager_config(config);

    let conn = timeout(
        config.connect_timeout(),
        ConnectionManager::new_with_config(client, manager_config),
    )
    .await
    .map_err(|_| RedisError::Timeout("redis 连接超时".to_owned()))?
    .map_err(|e| RedisError::Connection(format!("redis 连接失败: {e}")))?;

    Ok(RedisBackend::Standalone(Box::new(conn)))
}

pub(super) async fn connect_cluster(config: &RedisConfig) -> RedisResult<RedisBackend> {
    let infos = config.seed_connection_infos()?;
    let mut builder = ClusterClient::builder(infos);
    let max_wait_ms = u64::try_from(config.reconnect_max_delay().as_millis()).unwrap_or(u64::MAX);
    let _keepalive_policy = config.tcp_keepalive();
    builder = builder
        .connection_timeout(config.connect_timeout())
        .response_timeout(config.command_timeout())
        .retries(config.max_cluster_redirects())
        .max_retry_wait(max_wait_ms);
    if config.tls() {
        builder = builder.tls(TlsMode::Secure);
    }
    if let Some(password) = config.password_opt() {
        builder = builder.password(password.to_owned());
    }
    if let Some(username) = config.username() {
        builder = builder.username(username.to_owned());
    }

    let client = builder
        .build()
        .map_err(|e| RedisError::Connection(format!("redis cluster 客户端构建失败: {e}")))?;

    let conn = timeout(config.connect_timeout(), client.get_async_connection())
        .await
        .map_err(|_| RedisError::Timeout("redis cluster 连接超时".to_owned()))?
        .map_err(|e| RedisError::Connection(format!("redis cluster 连接失败: {e}")))?;

    Ok(RedisBackend::Cluster(conn))
}

pub(super) async fn connect_sentinel(config: &RedisConfig) -> RedisResult<RedisBackend> {
    let master_name = config
        .sentinel_master()
        .ok_or_else(|| RedisError::Config("Sentinel 模式缺少 sentinel_master".to_owned()))?
        .to_owned();

    let sentinel_infos = config.seed_connection_infos()?;
    let mut sentinel = Sentinel::build(sentinel_infos)
        .map_err(|e| RedisError::Connection(format!("redis sentinel 客户端构建失败: {e}")))?;

    let node_info = SentinelNodeConnectionInfo {
        tls_mode: if config.tls() {
            Some(TlsMode::Secure)
        } else {
            None
        },
        redis_connection_info: Some(redis::RedisConnectionInfo {
            db: config.db(),
            username: config.username().map(str::to_owned),
            password: config.password_opt().map(str::to_owned),
            protocol: Default::default(),
        }),
    };

    let discover = async {
        sentinel
            .async_master_for(&master_name, Some(&node_info))
            .await
            .map_err(|e| RedisError::Connection(format!("redis sentinel 发现 master 失败: {e}")))
    };

    let client = timeout(config.connect_timeout(), discover)
        .await
        .map_err(|_| RedisError::Timeout("redis sentinel 发现 master 超时".to_owned()))??;

    let manager_config = connection_manager_config(config);
    let conn = timeout(
        config.connect_timeout(),
        ConnectionManager::new_with_config(client, manager_config),
    )
    .await
    .map_err(|_| RedisError::Timeout("redis sentinel master 连接超时".to_owned()))?
    .map_err(|e| RedisError::Connection(format!("redis sentinel master 连接失败: {e}")))?;

    Ok(RedisBackend::Standalone(Box::new(conn)))
}
