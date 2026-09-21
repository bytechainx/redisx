//! 公共 API 表面集成测试：类型存在性、`Send + Sync` 约束、默认 feature 导出。

use std::time::Duration;

use redisx::{
    map_redis_error, map_redis_result, with_retry, RedisAtomicity, RedisClient, RedisConfig,
    RedisConfigBuilder, RedisError, RedisHealth, RedisLock, RedisMetricsSnapshot, RedisMode,
    RedisOperation, RedisPool, RedisPoolPermit, RedisPoolStats, RedisRetrySafety, RetryConfig,
    StreamEntry, TxCmd,
};

fn assert_send_sync<T: Send + Sync>() {}
fn assert_clone<T: Clone>() {}
fn assert_debug<T: std::fmt::Debug>() {}

#[test]
fn public_types_exist_and_are_thread_safe() {
    assert_send_sync::<RedisPool>();
    assert_send_sync::<RedisPoolPermit>();
    assert_send_sync::<RedisClient>();
    assert_send_sync::<RedisConfig>();
    assert_send_sync::<RedisConfigBuilder>();
    assert_send_sync::<RedisError>();
    assert_send_sync::<RedisLock>();
    assert_send_sync::<StreamEntry>();
    assert_send_sync::<TxCmd>();
    assert_send_sync::<RetryConfig>();
    assert_send_sync::<RedisPoolStats>();
    assert_send_sync::<RedisMetricsSnapshot>();
    assert_send_sync::<RedisHealth>();
    assert_send_sync::<RedisOperation>();
    assert_send_sync::<RedisRetrySafety>();
    assert_send_sync::<RedisAtomicity>();

    assert_clone::<RedisPool>();
    assert_clone::<RedisClient>();
    assert_clone::<RedisConfig>();
    assert_clone::<RedisConfigBuilder>();
    assert_clone::<RedisLock>();
    assert_clone::<StreamEntry>();
    assert_clone::<TxCmd>();
    assert_clone::<RetryConfig>();

    assert_debug::<RedisPool>();
    assert_debug::<RedisClient>();
    assert_debug::<RedisConfig>();
    assert_debug::<RedisError>();
    assert_debug::<RedisLock>();
}

#[test]
fn pool_and_client_are_shareable_across_threads() {
    let pool = RedisPool::new(RedisConfig::default()).expect("pool");
    let handles: Vec<_> = (0..4)
        .map(|index| {
            let pool = pool.clone();
            std::thread::spawn(move || {
                // 未建连的池在任意线程上都以 Connection 错误 fail-closed
                let client = pool.client();
                let stats = client.pool().stats();
                (index, stats.open, pool.endpoint().to_owned())
            })
        })
        .collect();
    for handle in handles {
        let (_, open, endpoint) = handle.join().expect("join");
        assert_eq!(open, 0);
        assert!(endpoint.starts_with("redis://"));
    }
}

#[test]
fn configs_survive_thread_boundaries() {
    let config = RedisConfig::builder()
        .addr("127.0.0.1:6380")
        .db(2)
        .build()
        .expect("cfg");
    let moved = std::thread::spawn(move || config.addr().to_owned())
        .join()
        .expect("join");
    assert_eq!(moved, "127.0.0.1:6380");
    assert!(RedisClient::new(RedisConfig::default()).is_ok());
}

#[test]
fn new_only_validates_and_never_connects() {
    let pool = RedisPool::new(RedisConfig::default()).expect("pool");
    assert!(!pool.liveness());
    assert_eq!(pool.stats().open, 0);
    assert_eq!(pool.stats().in_flight, 0);
    assert_eq!(pool.stats().waiters, 0);
    assert_eq!(pool.metrics_snapshot(), RedisMetricsSnapshot::default());
    assert!(!pool.is_closed());

    let client = RedisClient::new(RedisConfig::default()).expect("client");
    assert!(!client.pool().liveness());
    assert!(client.retry_config().is_none());
    assert!(!client.has_call_deadline());
}

#[test]
fn documented_selectors_are_public() {
    // 编译期点名：这些单元入口必须对外可用
    let cases: [RedisOperation; 10] = [
        RedisOperation::Get,
        RedisOperation::Set,
        RedisOperation::Delete,
        RedisOperation::Exists,
        RedisOperation::Expire,
        RedisOperation::Ttl,
        RedisOperation::Mget,
        RedisOperation::Mset,
        RedisOperation::Incr,
        RedisOperation::Publish,
    ];
    for operation in cases {
        let safety = operation.retry_safety();
        assert_eq!(
            operation.allows_automatic_retry(),
            safety != RedisRetrySafety::AmbiguousWrite
                && safety != RedisRetrySafety::NeverAutomatic
        );
        let _ = operation.atomicity();
    }
    let _: fn(redis::RedisError) -> RedisError = map_redis_error;
    assert_eq!(map_redis_result(Ok(7usize)).expect("ok"), 7);
}

#[tokio::test]
async fn public_async_entrypoints_are_usable() {
    let pool = RedisPool::new(RedisConfig::default()).expect("pool");
    assert!(pool.ping().await.is_err());
    assert!(pool.health_check().await.is_err());
    assert!(pool.get("k").await.is_err());
    assert!(pool.acquire().await.is_err());

    let client = pool.client();
    assert!(client.ping().await.is_err());
    assert!(client.get("k").await.is_err());
    assert!(client.hget("k", "f").await.is_err());
    assert!(client.xlen("s").await.is_err());
    assert!(client
        .multi_exec(&[TxCmd::set("k", b"v".to_vec())])
        .await
        .is_err());

    let result = with_retry(
        &RetryConfig::fixed(1, Duration::from_millis(1)),
        "api.surface",
        || async { Ok::<_, RedisError>(1_u8) },
    )
    .await;
    assert_eq!(result.expect("retry"), 1);
}

#[tokio::test]
async fn closed_pool_rejects_every_entrypoint() {
    let pool = RedisPool::new(RedisConfig::default()).expect("pool");
    pool.close(Duration::from_secs(1)).await.expect("close");
    assert!(pool.is_closed());
    assert!(!pool.liveness());
    let err = pool.readiness().await.expect_err("已关闭");
    assert!(matches!(err, RedisError::Connection(_)));
    assert!(pool.get("k").await.is_err());
    assert!(pool.set("k", b"v".to_vec()).await.is_err());
    assert_eq!(pool.stats().open, 0);
}

#[cfg(feature = "pubsub")]
#[test]
fn pubsub_feature_types_are_exported() {
    fn assert_public<T: 'static>() {}
    assert_public::<redisx::RedisPubSub>();
    assert_public::<redisx::RedisPubSubMessage>();

    let message = redisx::RedisPubSubMessage {
        channel: bytes::Bytes::from_static(b"ch"),
        payload: bytes::Bytes::from_static(b"payload"),
    };
    assert_eq!(message.channel.as_ref(), b"ch");
    assert_eq!(message.payload.as_ref(), b"payload");
}

#[test]
fn mode_enum_is_public_and_copy() {
    let modes = [
        RedisMode::Standalone,
        RedisMode::Cluster,
        RedisMode::Sentinel,
    ];
    for mode in modes {
        let copy = mode;
        assert_eq!(format!("{mode:?}"), format!("{copy:?}"));
    }
    assert_eq!(RedisMode::default(), RedisMode::Standalone);
}
