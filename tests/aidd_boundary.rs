#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! AIDD 对抗 / 边界用例（特性 002）。
//!
//! 候选由 AI 生成，逐条人工复核后仅保留「结论=保留」项；丢弃项登记于 PR 描述。
//! 全部离线，不依赖真实 Redis。
//!
//! // AIDD: toml_password_stays_redacted | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 Debug 与 display_endpoint 一律脱敏 | 结论=保留
//! // AIDD: insecure_tls_and_unix_socket_rejected | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 拒绝 insecure TLS / Unix socket | 结论=保留
//! // AIDD: node_url_credentials_redacted_in_error | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 错误信息不回显凭据 | 结论=保留
//! // AIDD: lock_token_uniqueness_and_constant_time_compare | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 锁令牌常时比较、不误删他人锁 | 结论=保留
//! // AIDD: pipeline_ttl_zero_rejected_and_empty_noop | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 阻塞点超时/参数校验 | 结论=保留
//! // AIDD: retry_safety_blocks_side_effect_commands | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §4 非幂等命令永不自动重试 | 结论=保留
//! // AIDD: close_is_idempotent_and_rejects_after | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 close(timeout) 优雅排空且拒绝新请求 | 结论=保留
//! // AIDD: weird_keys_do_not_panic_without_connection | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 不可信输入不得 panic | 结论=保留

use std::time::Duration;

use redisx::{
    generate_lock_token, lock_token_matches, RedisConfig, RedisError, RedisMode, RedisOperation,
    RedisPool, RedisRetrySafety,
};

/// 只校验配置、不建连的池（数据面命令以 [`RedisError::Connection`] 失败）。
fn disconnected_client() -> redisx::RedisClient {
    RedisPool::new(RedisConfig::default())
        .expect("仅校验配置")
        .client()
}

/// 边界：即使密码来自 TOML，`Debug` 与端点展示也必须脱敏。
///
/// 已知偏差（2026-09-22 实测并已上报）：`docs/标准.md` §2 声明 `from_toml()` 拒绝明文密码，
/// 但实现仍接受该字段——故本用例不评价「是否接受」，只钉住「不得泄漏」这条。
#[test]
fn toml_password_stays_redacted() {
    let secret = String::from("toml-plaintext-secret");
    let toml = format!("addr = \"127.0.0.1:6379\"\npassword = \"{secret}\"\n");
    let config = RedisConfig::from_toml(&toml).expect("当前实现接受该字段");
    assert!(config.has_password());
    let debug = format!("{config:?}");
    assert!(!debug.contains(&secret), "Debug 泄漏密码: {debug}");
    assert!(!config.display_endpoint().contains(&secret));
}

/// 边界：`rediss://…#insecure` 与 Unix socket 一律 fail-closed。
#[test]
fn insecure_tls_and_unix_socket_rejected() {
    let insecure =
        RedisConfig::from_url("rediss://127.0.0.1:6380/#insecure").expect_err("insecure TLS");
    assert!(matches!(insecure, RedisError::Config(_)), "{insecure}");

    let unix = RedisConfig::from_url("unix:///tmp/redis.sock").expect_err("unix socket");
    assert!(matches!(unix, RedisError::Unsupported(_)), "{unix}");

    // 合法 rediss 强制证书校验（拒绝 insecure）。
    let tls = RedisConfig::from_url("rediss://127.0.0.1:6380/0").expect("rediss");
    assert!(tls.tls());
}

/// 边界：Cluster 种子 URL 内嵌凭据——校验失败信息不得回显密码。
#[test]
fn node_url_credentials_redacted_in_error() {
    let secret = String::from("node-secret-value");
    let node = format!("redis://alice:{secret}@");
    let error = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .nodes([node])
        .build()
        .expect_err("畸形节点 URL 必须拒绝");
    assert!(
        !error.to_string().contains(&secret),
        "错误回显凭据: {error}"
    );
    assert!(!format!("{error:?}").contains(&secret));
}

/// 边界：锁令牌唯一 + 常时比较的长度/差异行为。
#[test]
fn lock_token_uniqueness_and_constant_time_compare() {
    let first = generate_lock_token();
    let second = generate_lock_token();
    assert!(first.starts_with("lk-"));
    assert_ne!(first, second);
    assert!(first.len() > 8);

    assert!(lock_token_matches(&first, &first));
    assert!(!lock_token_matches(&first, "other"));
    assert!(!lock_token_matches(&first, ""));
    // 长度不同的前缀不得被误判为相等。
    assert!(!lock_token_matches(&first, &first[..first.len() - 1]));
    assert!(lock_token_matches("", ""));
}

/// 边界：管道写 TTL 为 0 / 亚毫秒必须拒绝；空输入是 no-op（不触碰连接）。
#[tokio::test]
async fn pipeline_ttl_zero_rejected_and_empty_noop() {
    let client = disconnected_client();
    client
        .pipeline_set(&[], None)
        .await
        .expect("空 pipeline 应为 no-op");
    for ttl in [Duration::ZERO, Duration::from_nanos(1)] {
        let error = client
            .pipeline_set(&[("k", b"v".to_vec())], Some(ttl))
            .await
            .expect_err("非法 TTL 必须拒绝");
        assert!(matches!(error, RedisError::Config(_)), "{error}");
    }
}

/// 边界：非幂等 / 结果不明的命令永不被自动重试（避免超时后重复副作用）。
#[test]
fn retry_safety_blocks_side_effect_commands() {
    for op in [
        RedisOperation::Set,
        RedisOperation::Delete,
        RedisOperation::Expire,
        RedisOperation::Incr,
        RedisOperation::Publish,
    ] {
        assert!(!op.allows_automatic_retry(), "{op:?} 不得自动重试");
    }
    assert_eq!(
        RedisOperation::Publish.retry_safety(),
        RedisRetrySafety::NeverAutomatic
    );
    assert!(RedisOperation::Get.allows_automatic_retry());
}

/// 边界：重复 `close()` 幂等；关闭后所有入口持续拒绝。
#[tokio::test]
async fn close_is_idempotent_and_rejects_after() {
    let pool = RedisPool::new(RedisConfig::default()).expect("仅校验配置");
    pool.close(Duration::from_millis(100))
        .await
        .expect("首次 close 成功");
    pool.close(Duration::from_millis(100))
        .await
        .expect("重复 close 仍成功");
    assert!(pool.is_closed());
    assert!(!pool.liveness());
    assert!(pool.readiness().await.is_err());
    assert!(pool.set("k", b"v".to_vec()).await.is_err());
}

/// 边界：含控制字符 / Unicode / 超长的 key 在未建连时返回连接错误而非 panic。
#[tokio::test]
async fn weird_keys_do_not_panic_without_connection() {
    let client = disconnected_client();
    for key in [
        "k\n\r\x00\x1b[31m",
        "数据库．查询\u{0301}",
        &"x".repeat(100_000),
        "",
    ] {
        let error = client
            .set(key, b"v".to_vec())
            .await
            .expect_err("未建连必须失败");
        assert!(matches!(error, RedisError::Connection(_)), "{error}");
    }
}
