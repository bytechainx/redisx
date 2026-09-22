#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! SDD 规格对照（特性 002）：把 `docs/标准.md` 的章节条款转成可执行断言。
//!
//! // SPEC-MAP: S-1 | 1. 定位 | assert_positioning
//! // SPEC-MAP: S-2 | 2. 配置治理 | assert_config_governance
//! // SPEC-MAP: S-3 | 3. 连接与背压 | assert_connection_backpressure
//! // SPEC-MAP: S-4 | 4. 重试与副作用安全 | assert_retry_and_side_effect_safety
//! // SPEC-MAP: S-5 | 5. 验收 | assert_acceptance

use std::time::Duration;

use redisx::{
    with_retry, RedisAtomicity, RedisConfig, RedisError, RedisMode, RedisOperation, RedisPool,
    RedisRetrySafety, RetryConfig, ENV_PREFIX,
};

/// S-1：单一 crate 覆盖 Standalone / Cluster / Sentinel 三拓扑，零内部耦合。
#[test]
fn assert_positioning() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RedisPool>();
    assert_send_sync::<RedisConfig>();
    assert_send_sync::<RetryConfig>();

    // 三种拓扑都是同一配置类型的取值，不是三套独立适配器。
    assert_eq!(RedisConfig::default().mode(), RedisMode::Standalone);
    assert_eq!(
        RedisConfig::builder()
            .mode(RedisMode::Cluster)
            .addr("127.0.0.1:7000")
            .build()
            .expect("cluster")
            .mode(),
        RedisMode::Cluster
    );
    assert_eq!(
        RedisConfig::builder()
            .mode(RedisMode::Sentinel)
            .nodes(["127.0.0.1:26379"])
            .sentinel_master("mymaster")
            .build()
            .expect("sentinel")
            .mode(),
        RedisMode::Sentinel
    );
}

/// S-2：配置治理——builder / from_env / from_toml / from_url 四入口 + `ENV_*` 常量 +
/// 校验 fail-fast + 凭据只经 env / builder 注入（TOML 拒绝明文 password）+ 脱敏。
#[test]
fn assert_config_governance() {
    assert_eq!(ENV_PREFIX, "FOUNDATIONX_REDISX_");
    for key in [
        redisx::ENV_ADDR,
        redisx::ENV_PASSWORD,
        redisx::ENV_DB,
        redisx::ENV_MODE,
    ] {
        assert!(key.starts_with(ENV_PREFIX), "key={key}");
    }

    // 四入口均产出可校验的 RedisConfig：builder / from_url / from_toml 字段一致。
    let from_builder = RedisConfig::builder()
        .addr("127.0.0.1:6380")
        .db(3)
        .command_timeout(Duration::from_millis(500))
        .build()
        .expect("builder");
    let from_url = RedisConfig::from_url("redis://user:pw@127.0.0.1:6380/3").expect("from_url");
    assert_eq!(from_url.addr(), from_builder.addr());
    assert_eq!(from_url.db(), from_builder.db());
    let from_toml =
        RedisConfig::from_toml("addr = \"127.0.0.1:6380\"\ndb = 3\ncommand_timeout_ms = 500\n")
            .expect("from_toml");
    assert_eq!(from_toml.addr(), from_builder.addr());
    assert_eq!(from_toml.db(), from_builder.db());
    assert_eq!(from_toml.command_timeout(), from_builder.command_timeout());

    // Sentinel 必填、Cluster 非 0 库非法（validate fail-fast）。
    assert!(RedisConfig::builder()
        .mode(RedisMode::Sentinel)
        .addr("127.0.0.1:26379")
        .build()
        .is_err());
    assert!(RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .addr("127.0.0.1:7000")
        .db(2)
        .build()
        .is_err());

    // 密码只经 builder / env 注入；Debug 与端点展示一律脱敏。
    let with_password = RedisConfig::builder()
        .addr("127.0.0.1:6380")
        .username("alice")
        .password("s3cret-value")
        .build()
        .expect("配置");
    assert!(with_password.has_password());
    let debug = format!("{with_password:?}");
    assert!(debug.contains("***"));
    assert!(!debug.contains("s3cret-value"));
    let endpoint = with_password.display_endpoint();
    assert!(endpoint.contains("***"));
    assert!(!endpoint.contains("s3cret-value"));

    // 凭据只经 env / builder 注入：TOML 提供非空 password 必须 fail-closed。
    let from_toml_with_password =
        RedisConfig::from_toml("addr = \"127.0.0.1:6380\"\npassword = \"s3cret-value\"\n");
    assert!(
        matches!(from_toml_with_password, Err(RedisError::Config(_))),
        "TOML 明文 password 必须被拒绝: {from_toml_with_password:?}"
    );
}

/// S-3：连接与背压——有界 in-flight、各处超时、close 排空、结构化可观测数据。
#[tokio::test]
async fn assert_connection_backpressure() {
    let config = RedisConfig::default();
    assert_eq!(config.max_in_flight(), 256);
    assert_eq!(config.command_lanes(), config.max_in_flight());
    for timeout in [
        config.connect_timeout(),
        config.command_timeout(),
        config.acquire_timeout(),
        config.blocking_timeout(),
    ] {
        assert!(!timeout.is_zero(), "所有外部调用都必须有超时");
    }
    // 无界排队被禁止：acl 上限必须 >= 1。
    assert!(RedisConfig::builder().max_in_flight(0).build().is_err());

    // 结构化可观测数据（未建连时不虚构数据）。
    let pool = RedisPool::new(config).expect("仅校验配置");
    let stats = pool.stats();
    assert_eq!(stats.open, 0, "未建连时无可用 lane");
    let metrics = pool.metrics_snapshot();
    assert_eq!(metrics.commands_ok + metrics.commands_err, 0);
    assert!(!pool.liveness());

    // close(timeout) 在空闲池上立即成功，并拒绝后续请求。
    pool.close(Duration::from_millis(200))
        .await
        .expect("空闲池 close 应立即成功");
    assert!(pool.is_closed());
    assert!(pool.ping().await.is_err(), "关闭后不得 ping 成功");
}

/// S-4：重试与副作用安全——命令分类、只对只读/幂等自动重试、指数退避 + 抖动 + deadline、
/// 错误分类。
#[tokio::test]
async fn assert_retry_and_side_effect_safety() {
    // 命令重试安全分类。
    assert_eq!(
        RedisOperation::Get.retry_safety(),
        RedisRetrySafety::ReadOnly
    );
    // MSET 与 SET 同为固定值写入：分类统一为 AmbiguousWrite（不自动重试），
    // 符合规格 S-4「结果不明的命令永远只执行一次」的保守原则
    for ambiguous in [
        RedisOperation::Set,
        RedisOperation::Delete,
        RedisOperation::Expire,
        RedisOperation::Mset,
    ] {
        assert_eq!(
            ambiguous.retry_safety(),
            RedisRetrySafety::AmbiguousWrite,
            "{ambiguous:?}"
        );
        assert!(
            !ambiguous.allows_automatic_retry(),
            "{ambiguous:?} 不得自动重试"
        );
    }
    for never in [RedisOperation::Incr, RedisOperation::Publish] {
        assert_eq!(never.retry_safety(), RedisRetrySafety::NeverAutomatic);
        assert!(!never.allows_automatic_retry());
    }
    assert!(RedisOperation::Get.allows_automatic_retry());
    assert_eq!(
        RedisOperation::Get.atomicity(),
        RedisAtomicity::SingleCommand
    );
    assert_eq!(
        RedisOperation::Mget.atomicity(),
        RedisAtomicity::MultiKeySingleSlot
    );

    // 指数退避单调且封顶；抖动有界。
    let retry = RetryConfig::exponential(5, Duration::from_millis(50), Duration::from_secs(2));
    assert!(retry.backoff_for(1) <= retry.backoff_for(2));
    assert!(retry.backoff_for(9) <= Duration::from_secs(2));
    // 抖动落在 [0.5 × backoff, backoff]（见 `RetryConfig::jittered` 契约），且同参可复现。
    let jittered = RetryConfig::jittered(Duration::from_millis(100), 42);
    assert!(jittered <= Duration::from_millis(100));
    assert!(jittered >= Duration::from_millis(50));
    assert_eq!(
        jittered,
        RetryConfig::jittered(Duration::from_millis(100), 42)
    );

    // 只对可重试错误进入重试环；业务拒绝只尝试一次。
    let mut retryable_calls = 0_u32;
    let config = RetryConfig::fixed(3, Duration::ZERO).without_jitter();
    let error = with_retry(&config, "redis.probe", || {
        retryable_calls += 1;
        async { Err::<u8, _>(RedisError::Transient("try again".into())) }
    })
    .await
    .expect_err("瞬时故障应重试后仍失败");
    assert!(matches!(error, RedisError::Transient(_)));
    assert_eq!(retryable_calls, 3, "max_attempts=3 应尝试 3 次");

    let mut permanent_calls = 0_u32;
    let error = with_retry(&config, "redis.probe", || {
        permanent_calls += 1;
        async { Err::<u8, _>(RedisError::Conflict("dup".into())) }
    })
    .await
    .expect_err("业务冲突应直接返回");
    assert!(matches!(error, RedisError::Conflict(_)));
    assert_eq!(permanent_calls, 1, "非可重试错误只尝试一次");
}

/// S-5：验收——三件套命令可一次性执行，测试与基准均离线。
#[test]
fn assert_acceptance() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    assert!(
        std::path::Path::new(manifest)
            .join("benches/hot_path.rs")
            .is_file(),
        "微基准应随 crate 提供"
    );
    assert!(
        std::path::Path::new(manifest)
            .join("docs/标准.md")
            .is_file(),
        "标准文档应随 crate 提供"
    );
    // 离线约束：默认配置指向 127.0.0.1:6379，用例不依赖真实服务。
    assert_eq!(RedisConfig::default().addr(), "127.0.0.1:6379");
}
