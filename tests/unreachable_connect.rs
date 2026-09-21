//! 失败路径集成测试：对不可达地址（`127.0.0.1:1`，必然拒绝）建连与 `ping` 必须失败。
//!
//! 全部超时压到 1 秒级以内，保证测试不会因为等待网络而变慢。

use std::time::Duration;

use redisx::{RedisClient, RedisConfig, RedisPool, RedisResult};

/// 指向必然拒绝连接的端口的配置，超时均在 1 秒内。
fn unreachable_config() -> RedisConfig {
    RedisConfig::builder()
        .addr("127.0.0.1:1")
        .db(0)
        .connect_timeout(Duration::from_millis(200))
        .command_timeout(Duration::from_millis(200))
        .acquire_timeout(Duration::from_millis(200))
        .reconnect_max_delay(Duration::from_millis(200))
        .build()
        .expect("配置本身合法")
}

#[tokio::test]
async fn ping_fails_for_unreachable_address() {
    let started = std::time::Instant::now();

    // 建连阶段即失败：对端拒绝连接，驱动不会伪造一个可用池
    let outcome: RedisResult<()> = async {
        let pool = RedisPool::connect(unreachable_config()).await?;
        // 若建连意外成功（例如本机 1 端口被占用），ping 也必须立刻失败
        pool.ping().await?;
        Ok(())
    }
    .await;

    let err = outcome.expect_err("对不可达地址 ping 必须返回 Err");
    assert!(
        matches!(
            err,
            redisx::RedisError::Connection(_) | redisx::RedisError::Timeout(_)
        ),
        "err={err}"
    );
    assert!(err.is_retryable(), "连接类失败应可重试: {err}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "失败路径不得长时间阻塞: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn client_connect_url_fails_fast_for_unreachable_address() {
    let started = std::time::Instant::now();
    // 从 URL 解析配置，再把超时压到 1 秒级以内（默认值为 5s，不利于快速失败）
    let config = RedisConfig::from_url("redis://127.0.0.1:1")
        .expect("URL 本身合法")
        .to_builder()
        .connect_timeout(Duration::from_millis(200))
        .command_timeout(Duration::from_millis(200))
        .acquire_timeout(Duration::from_millis(200))
        .build()
        .expect("配置合法");

    let outcome = tokio::time::timeout(Duration::from_secs(3), RedisClient::connect(config)).await;
    match outcome {
        Ok(Ok(client)) => {
            // 极端情况下（端口被占用）也必须能在 ping 上观察到失败
            assert!(client.ping().await.is_err());
        }
        Ok(Err(err)) => assert!(
            matches!(
                err,
                redisx::RedisError::Connection(_) | redisx::RedisError::Timeout(_)
            ),
            "err={err}"
        ),
        Err(_) => panic!("建连失败路径不得超时挂起"),
    }
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "失败路径不得长时间阻塞: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn health_check_and_operations_fail_before_reaching_a_server() {
    // 未建连的池：任何数据面调用都必须 fail-closed，且不产生网络等待
    let pool = RedisPool::new(unreachable_config()).expect("仅校验配置");
    assert_eq!(pool.stats().open, 0);
    assert!(pool.health_check().await.is_err());
    assert!(pool.set("k", b"v".to_vec()).await.is_err());
    assert!(pool.get("k").await.is_err());

    // 建连失败后 ping/health_check 同样返回 Err
    let client = RedisClient::new(unreachable_config()).expect("仅校验配置");
    assert!(client.ping().await.is_err());
    assert!(client.health_check().await.is_err());
}
