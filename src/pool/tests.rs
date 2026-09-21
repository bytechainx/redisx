//! `pool` 模块单元测试。

use super::*;
use crate::config::RedisConfig;

#[test]
fn connection_manager_config_applies_reconnect_max_delay() {
    let cfg = RedisConfig::builder()
        .addr("127.0.0.1:6379")
        .reconnect_max_delay(Duration::from_millis(1234))
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .expect("cfg");
    let manager = connection_manager_config(&cfg);
    let debug = format!("{manager:?}");
    assert!(debug.contains("max_delay"), "manager={debug}");
    assert!(debug.contains("1234"), "manager={debug}");
    assert_eq!(cfg.tcp_keepalive(), Some(Duration::from_secs(30)));
    assert_eq!(cfg.reconnect_max_delay(), Duration::from_millis(1234));
}

#[test]
fn new_validates_without_connecting() {
    let pool = RedisPool::new(RedisConfig::default()).expect("new");
    assert!(!pool.is_closed());
    assert!(!pool.liveness(), "未建连不算 live");
    assert_eq!(pool.stats().open, 0);
    assert_eq!(pool.command_lanes(), RedisConfig::default().max_in_flight());
    assert!(pool.endpoint().starts_with("redis://"));
    assert_eq!(
        pool.command_timeout(),
        RedisConfig::default().command_timeout()
    );
    assert_eq!(
        pool.reconnect_max_delay(),
        RedisConfig::default().reconnect_max_delay()
    );
    assert!(pool.tcp_keepalive().is_none());
    assert_eq!(pool.metrics_snapshot(), RedisMetricsSnapshot::default());

    let bad = RedisConfig::default()
        .to_builder()
        .max_in_flight(0)
        .build()
        .expect_err("max_in_flight=0 非法");
    assert!(matches!(bad, RedisError::Config(_)));
}

#[tokio::test]
async fn unconnected_pool_fails_closed() {
    let pool = RedisPool::new(RedisConfig::default()).expect("new");
    let err = pool.ping().await.expect_err("未建连");
    assert!(matches!(err, RedisError::Connection(_)), "{err}");
    let err = pool.acquire().await.expect_err("未建连");
    assert!(matches!(err, RedisError::Connection(_)));
    let err = pool.health_check().await.expect_err("未建连");
    assert!(matches!(err, RedisError::Connection(_)));
}

#[tokio::test]
async fn stats_and_metrics_count_probe_traffic() {
    let calls = Arc::new(AtomicUsize::new(0));
    let pool = RedisPool::test_probe(calls.clone());
    let lanes = RedisConfig::default().max_in_flight();
    assert_eq!(pool.stats().open, lanes);
    assert!(pool.liveness());

    let err = pool.ping().await.expect_err("probe 必然失败");
    assert!(matches!(err, RedisError::Connection(_)));
    assert!(calls.load(Ordering::SeqCst) >= 1);

    let snapshot = pool.metrics_snapshot();
    assert_eq!(snapshot.commands_ok, 0);
    assert_eq!(snapshot.commands_err, 1);
    assert_eq!(snapshot.commands_timeout, 0);
    assert_eq!(snapshot.acquire_timeout, 0);
    assert_eq!(snapshot.rejected_closed, 0);
    assert_eq!(pool.stats().in_flight, 0, "命令结束后应归还 lane");
}

#[tokio::test]
async fn permit_holds_lane_and_releases_on_drop() {
    let pool = RedisPool::test_probe(Arc::new(AtomicUsize::new(0)));
    let permit = pool.acquire().await.expect("permit");
    assert_eq!(pool.stats().in_flight, 1);
    assert_eq!(permit.endpoint(), pool.endpoint());
    assert_eq!(
        permit.command_timeout(),
        RedisConfig::default().command_timeout()
    );
    let err = permit.get("k").await.expect_err("probe");
    assert!(matches!(err, RedisError::Connection(_)));
    drop(permit);
    assert_eq!(pool.stats().in_flight, 0);
}

#[tokio::test]
async fn all_pool_commands_enter_driver() {
    let calls = Arc::new(AtomicUsize::new(0));
    let pool = RedisPool::test_probe(calls.clone());
    let _ = pool.get("k").await;
    let _ = pool.set("k", b"v".to_vec()).await;
    let _ = pool
        .set_ex("k", b"v".to_vec(), Duration::from_secs(1))
        .await;
    let _ = pool.del("k").await;
    let _ = pool.exists("k").await;
    let _ = pool.incr("k", 1).await;
    let _ = pool.expire("k", Duration::from_secs(2)).await;
    let _ = pool.ttl("k").await;
    let _ = pool.ping().await;
    let _ = pool.health_check().await;
    assert!(
        calls.load(Ordering::SeqCst) >= 10,
        "应多次进入 probe driver"
    );
}

#[tokio::test]
async fn invalid_ttl_fails_before_driver() {
    let calls = Arc::new(AtomicUsize::new(0));
    let pool = RedisPool::test_probe(calls.clone());
    let err = pool
        .set_ex("k", b"v".to_vec(), Duration::ZERO)
        .await
        .expect_err("ttl=0");
    assert!(matches!(err, RedisError::Config(_)));
    let err = pool
        .expire("k", Duration::from_nanos(1))
        .await
        .expect_err("亚毫秒");
    assert!(matches!(err, RedisError::Config(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 0, "TTL 非法时不得触达 driver");
}

#[tokio::test]
async fn closed_pool_rejects_and_counts() {
    let pool = RedisPool::test_probe(Arc::new(AtomicUsize::new(0)));
    pool.close(Duration::from_secs(1)).await.expect("close");
    assert!(pool.is_closed());
    assert!(!pool.liveness());
    assert_eq!(pool.stats().open, 0);

    let err = pool.ping().await.expect_err("已关闭");
    assert!(matches!(err, RedisError::Connection(_)), "{err}");
    assert!(pool.metrics_snapshot().rejected_closed >= 1);
    let err = pool.readiness().await.expect_err("已关闭");
    assert!(matches!(err, RedisError::Connection(_)));
}

#[tokio::test]
async fn zero_deadlines_are_rejected_as_timeout() {
    let pool = RedisPool::test_probe(Arc::new(AtomicUsize::new(0)));
    let err = pool
        .with_conn_total_deadline(Duration::ZERO, |_| async { Ok::<(), RedisError>(()) })
        .await
        .expect_err("零总 deadline");
    assert!(matches!(err, RedisError::Timeout(_)));

    let err = pool
        .with_conn_budget(Duration::ZERO, |_| async { Ok::<(), RedisError>(()) })
        .await
        .expect_err("零预算");
    assert!(matches!(err, RedisError::Timeout(_)));
    assert_eq!(
        pool.metrics_snapshot().acquire_timeout,
        0,
        "入口拦截不计入 acquire 超时"
    );
}

#[tokio::test]
async fn connect_refused_returns_error() {
    let cfg = RedisConfig::builder()
        .addr("127.0.0.1:1")
        .password("unused-password")
        .connect_timeout(Duration::from_millis(200))
        .command_timeout(Duration::from_millis(200))
        .acquire_timeout(Duration::from_millis(200))
        .build()
        .expect("cfg");
    let result = tokio::time::timeout(Duration::from_secs(5), RedisPool::connect(cfg)).await;
    match result {
        Ok(Ok(pool)) => panic!("不应连接到 127.0.0.1:1: {pool:?}"),
        Ok(Err(err)) => assert!(
            matches!(
                err,
                RedisError::Connection(_) | RedisError::Timeout(_) | RedisError::Transient(_)
            ),
            "{err}"
        ),
        Err(_) => {}
    }
}

#[tokio::test]
async fn cluster_connect_refused_returns_error() {
    let cfg = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .nodes(["127.0.0.1:1"])
        .connect_timeout(Duration::from_millis(200))
        .command_timeout(Duration::from_millis(200))
        .acquire_timeout(Duration::from_millis(200))
        .build()
        .expect("cfg");
    let result = tokio::time::timeout(Duration::from_secs(8), RedisPool::connect(cfg)).await;
    match result {
        Ok(Ok(pool)) => panic!("不应连接到 127.0.0.1:1: {pool:?}"),
        Ok(Err(err)) => assert!(
            matches!(
                err,
                RedisError::Connection(_) | RedisError::Timeout(_) | RedisError::Transient(_)
            ),
            "{err}"
        ),
        Err(_) => {}
    }
}

#[tokio::test]
async fn sentinel_connect_refused_returns_error() {
    let cfg = RedisConfig::builder()
        .mode(RedisMode::Sentinel)
        .nodes(["127.0.0.1:1"])
        .sentinel_master("mymaster")
        .connect_timeout(Duration::from_millis(200))
        .command_timeout(Duration::from_millis(200))
        .acquire_timeout(Duration::from_millis(200))
        .build()
        .expect("cfg");
    let result = tokio::time::timeout(Duration::from_secs(8), RedisPool::connect(cfg)).await;
    match result {
        Ok(Ok(pool)) => panic!("不应连接到 127.0.0.1:1: {pool:?}"),
        Ok(Err(err)) => {
            assert!(
                matches!(err, RedisError::Connection(_) | RedisError::Timeout(_)),
                "{err}"
            );
        }
        Err(_) => {}
    }
}

#[test]
fn ttl_validation_and_conversion() {
    assert!(kv::validate_ttl(None).is_ok());
    assert!(kv::validate_ttl(Some(Duration::from_millis(1))).is_ok());
    let zero = kv::validate_ttl(Some(Duration::ZERO)).expect_err("零");
    assert!(matches!(zero, RedisError::Config(_)));
    let sub = kv::validate_ttl(Some(Duration::from_nanos(100))).expect_err("亚毫秒");
    assert!(matches!(sub, RedisError::Config(_)));

    assert_eq!(kv::ttl_to_millis(Duration::from_secs(2)).expect("ms"), 2000);
    assert_eq!(kv::ttl_to_millis(Duration::from_millis(1)).expect("ms"), 1);
    assert!(kv::ttl_to_millis(Duration::from_nanos(500)).is_err());
}

#[test]
fn debug_outputs_do_not_leak_password() {
    let secret = String::from("s3cr3t-value");
    let cfg = RedisConfig::builder()
        .addr("127.0.0.1:6379")
        .username("alice")
        .password(secret.clone())
        .build()
        .expect("cfg");
    let pool = RedisPool::new(cfg).expect("pool");
    let debug = format!("{pool:?}");
    assert!(!debug.contains(&secret), "pool debug={debug}");
    assert!(pool.config().has_password());
}
