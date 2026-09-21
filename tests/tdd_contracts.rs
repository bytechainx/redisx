#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! TDD 行为契约（特性 002）。
//!
//! 入口集合 = `specs/002-public-api-compliance-and-test-tiers/contracts/public-api-contract.md`
//! 登记的 12 个 redisx 入口。契约中的 `RedisClient::lock` 对应实现里的分布式锁族
//! `lock_acquire` / `lock_release` / `lock_extend`（表内保留契约登记名以对齐检查器 V14）。
//!
//! 全部用例离线：失败路径使用未建连的池（`RedisPool::new` 只校验配置）与必然拒绝连接的
//! `127.0.0.1:1`，不依赖真实 Redis 服务。
//!
//! // TDD-PROBE: RedisConfig::from_env | 变异：忽略 REDIS_URL 优先级恒读散字段 | 红=from_env_defaults_and_url_override | 绿=from_env_defaults_and_url_override
//! // TDD-PROBE: RedisConfig::from_toml | 变异：放行非法 mode 字面量 | 红=from_toml_parses_and_rejects_invalid | 绿=from_toml_parses_and_rejects_invalid
//! // TDD-PROBE: RedisConfig::from_toml | 变异：wire 采纳非空 password（明文凭据入配置） | 红=from_toml_rejects_plaintext_password | 绿=from_toml_rejects_plaintext_password
//! // TDD-PROBE: RedisConfig::validate | 变异：去掉 Cluster 非 0 库与 Sentinel 缺 master 拦截 | 红=validate_topology_rules | 绿=validate_topology_rules
//! // TDD-PROBE: RedisClient::connect_from_env | 变异：connect_from_env 忽略 addr 恒连本机默认 | 红=connect_from_env_refused_is_retryable | 绿=connect_from_env_refused_is_retryable
//! // TDD-PROBE: RedisClient::set | 变异：未建连时写命令返回 Ok | 红=set_get_del_fail_closed_without_connection | 绿=set_get_del_fail_closed_without_connection
//! // TDD-PROBE: RedisClient::get | 变异：未建连时读命令返回 Ok(None) | 红=set_get_del_fail_closed_without_connection | 绿=set_get_del_fail_closed_without_connection
//! // TDD-PROBE: RedisClient::del | 变异：未建连时删除返回 Ok(false) | 红=set_get_del_fail_closed_without_connection | 绿=set_get_del_fail_closed_without_connection
//! // TDD-PROBE: RedisClient::lock | 变异：lock_acquire 接受零 TTL | 红=lock_acquire_rejects_zero_ttl | 绿=lock_acquire_rejects_zero_ttl
//! // TDD-PROBE: RedisPool::connect | 变异：connect 不可达时返回未建连的池 | 红=pool_connect_refused_errors | 绿=pool_connect_refused_errors
//! // TDD-PROBE: RedisPool::ping | 变异：ping 未建连时返回 Ok | 红=pool_ping_and_health_fail_closed | 绿=pool_ping_and_health_fail_closed
//! // TDD-PROBE: RedisPool::health_check | 变异：health_check 未探活即返回快照 | 红=pool_ping_and_health_fail_closed | 绿=pool_ping_and_health_fail_closed
//! // TDD-PROBE: RedisError::is_retryable | 变异：Conflict 计入可重试 | 红=error_is_retryable_classification | 绿=error_is_retryable_classification

use std::sync::Mutex;
use std::time::Duration;

use redisx::{
    RedisClient, RedisConfig, RedisError, RedisMode, RedisPool, ENV_ADDR, ENV_BLOCKING_TIMEOUT_MS,
    ENV_DB, ENV_MAX_IN_FLIGHT, ENV_MODE, ENV_NODES, ENV_PASSWORD, ENV_SENTINEL_MASTER, ENV_TLS,
    ENV_URL, ENV_USERNAME, ENV_WARMUP,
};

/// 环境变量是进程级共享状态；本文件内串行化修改，避免并行用例互相干扰。
static ENV_LOCK: Mutex<()> = Mutex::new(());

const MANAGED_ENV: &[&str] = &[
    ENV_URL,
    ENV_ADDR,
    ENV_USERNAME,
    ENV_PASSWORD,
    ENV_DB,
    ENV_TLS,
    ENV_MODE,
    ENV_NODES,
    ENV_SENTINEL_MASTER,
    ENV_WARMUP,
    ENV_MAX_IN_FLIGHT,
    ENV_BLOCKING_TIMEOUT_MS,
];

struct EnvGuard<'a> {
    _lock: std::sync::MutexGuard<'a, ()>,
}

impl<'a> EnvGuard<'a> {
    fn new(vars: &[(&str, &str)]) -> Self {
        let lock = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for key in MANAGED_ENV {
            std::env::remove_var(key);
        }
        for (key, value) in vars {
            std::env::set_var(key, value);
        }
        Self { _lock: lock }
    }
}

impl Drop for EnvGuard<'_> {
    fn drop(&mut self) {
        for key in MANAGED_ENV {
            std::env::remove_var(key);
        }
    }
}

/// 必然拒绝连接的地址（`127.0.0.1:1`）。
fn refused_config() -> RedisConfig {
    RedisConfig::builder()
        .addr("127.0.0.1:1")
        .connect_timeout(Duration::from_millis(500))
        .command_timeout(Duration::from_millis(500))
        .acquire_timeout(Duration::from_millis(500))
        .build()
        .expect("不可达配置本身应合法")
}

/// 只校验配置、不建连的客户端（数据面命令会以 [`RedisError::Connection`] 失败）。
fn disconnected_client() -> RedisClient {
    RedisPool::new(RedisConfig::default())
        .expect("仅校验配置")
        .client()
}

/// `RedisConfig::from_env`：`REDIS_URL` 优先；非法取值 fail-closed；默认 username=default。
#[test]
fn from_env_defaults_and_url_override() {
    {
        let _guard = EnvGuard::new(&[]);
        let config = RedisConfig::from_env().expect("默认");
        assert_eq!(config.addr(), "127.0.0.1:6379");
        assert_eq!(config.db(), 0);
        assert!(!config.tls());
        assert_eq!(config.mode(), RedisMode::Standalone);
        assert_eq!(config.username(), Some("default"));
    }
    {
        // REDIS_URL 覆盖散字段。
        let _guard = EnvGuard::new(&[
            (ENV_ADDR, "10.9.9.9:6390"),
            (ENV_URL, "redis://override:pw@127.0.0.1:6380/2"),
        ]);
        let config = RedisConfig::from_env().expect("URL 优先");
        assert_eq!(config.addr(), "127.0.0.1:6380");
        assert_eq!(config.db(), 2);
        assert!(config.has_password());
        assert!(!format!("{config:?}").contains("pw"), "密码不得外泄");
    }
    {
        // 非法取值必须 fail-closed。
        let _guard = EnvGuard::new(&[(ENV_DB, "not-a-number")]);
        assert!(RedisConfig::from_env().is_err());
        std::env::set_var(ENV_DB, "0");
        std::env::set_var(ENV_TLS, "maybe");
        assert!(RedisConfig::from_env().is_err());
    }
}

/// `RedisConfig::from_toml`：字段与时间/模式正确落位；非法 mode / 语法错误 fail-closed。
#[test]
fn from_toml_parses_and_rejects_invalid() {
    let config = RedisConfig::from_toml(
        r#"
        addr = "10.0.0.5:6380"
        nodes = ["10.0.0.6:6380", "10.0.0.7:6380"]
        mode = "cluster"
        connect_timeout_ms = 120
        command_timeout_ms = 340
        max_in_flight = 8
        warmup_count = 2
        "#,
    )
    .expect("TOML");
    assert_eq!(config.addr(), "10.0.0.5:6380");
    assert_eq!(config.mode(), RedisMode::Cluster);
    assert_eq!(config.nodes().len(), 2);
    assert_eq!(config.connect_timeout(), Duration::from_millis(120));
    assert_eq!(config.command_timeout(), Duration::from_millis(340));
    assert_eq!(config.max_in_flight(), 8);
    assert_eq!(config.warmup_count(), 2);

    let bad_mode = RedisConfig::from_toml(r#"mode = "wat""#).expect_err("非法 mode 必须拒绝");
    assert!(matches!(bad_mode, RedisError::Config(_)));
    assert!(
        RedisConfig::from_toml("addr = = 1").is_err(),
        "语法错误必须拒绝"
    );
}

/// `RedisConfig::from_toml` 必须拒绝非空 `password`——凭据只能经 env / builder 注入
/// （`docs/标准.md` §2），TOML 通道不得把明文凭据带进配置。
#[test]
fn from_toml_rejects_plaintext_password() {
    let toml = "addr = \"127.0.0.1:6379\"\npassword = \"plaintext-secret\"\n";
    let error = RedisConfig::from_toml(toml).expect_err("TOML 明文 password 必须被拒绝");
    assert!(matches!(error, RedisError::Config(_)), "{error}");
    // 拒绝信息不得回显凭据取值。
    assert!(!error.to_string().contains("plaintext-secret"), "{error}");

    // 空白 password 等同未提供（不视为凭据走私）。
    RedisConfig::from_toml("addr = \"127.0.0.1:6379\"\npassword = \"   \"\n")
        .expect("空白 password 不应触发拒绝");
}

/// `RedisConfig::validate`：Cluster 非 0 逻辑库与 Sentinel 缺 master 一律拒绝。
#[test]
fn validate_topology_rules() {
    let cluster_db = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .addr("127.0.0.1:7000")
        .db(1)
        .build()
        .expect_err("Cluster 不支持非 0 库");
    assert!(matches!(cluster_db, RedisError::Config(_)));

    let sentinel_missing = RedisConfig::builder()
        .mode(RedisMode::Sentinel)
        .addr("127.0.0.1:26379")
        .build()
        .expect_err("Sentinel 缺 master");
    assert!(sentinel_missing.to_string().contains("sentinel_master"));

    RedisConfig::builder()
        .mode(RedisMode::Sentinel)
        .nodes(["127.0.0.1:26379"])
        .sentinel_master("mymaster")
        .build()
        .expect("满足条件后放行");

    for zero in [
        RedisConfig::builder()
            .connect_timeout(Duration::ZERO)
            .build(),
        RedisConfig::builder()
            .command_timeout(Duration::ZERO)
            .build(),
        RedisConfig::builder()
            .acquire_timeout(Duration::ZERO)
            .build(),
        RedisConfig::builder().max_in_flight(0).build(),
        RedisConfig::builder().db(-1).build(),
    ] {
        assert!(matches!(zero, Err(RedisError::Config(_))));
    }
}

/// `RedisClient::connect_from_env`：按 env 地址建连；不可达必须报错且可重试。
#[tokio::test]
async fn connect_from_env_refused_is_retryable() {
    let _guard = EnvGuard::new(&[(ENV_ADDR, "127.0.0.1:1")]);
    let result =
        tokio::time::timeout(Duration::from_secs(15), RedisClient::connect_from_env()).await;
    let error = result
        .expect("connect_from_env 必须受内部超时约束")
        .expect_err("不可达地址不得建连成功");
    assert!(error.is_retryable(), "连接失败应可重试: {error}");
}

/// `RedisClient::{set,get,del}`：未建连时三者都必须 fail-closed，不得伪装成功。
#[tokio::test]
async fn set_get_del_fail_closed_without_connection() {
    let client = disconnected_client();

    let set_error = client
        .set("k", b"v".to_vec())
        .await
        .expect_err("未建连不得写入成功");
    assert!(
        matches!(set_error, RedisError::Connection(_)),
        "{set_error}"
    );

    let get_error = client.get("k").await.expect_err("未建连不得读到 None");
    assert!(
        matches!(get_error, RedisError::Connection(_)),
        "{get_error}"
    );

    let del_error = client.del("k").await.expect_err("未建连不得删除成功");
    assert!(
        matches!(del_error, RedisError::Connection(_)),
        "{del_error}"
    );
}

/// `RedisClient::lock`（实现为 `lock_acquire`）：空 key 与零 TTL 是本地配置错误，不碰网络。
#[tokio::test]
async fn lock_acquire_rejects_zero_ttl() {
    let client = disconnected_client();
    let zero = client
        .lock_acquire("lk", Duration::ZERO)
        .await
        .expect_err("零 TTL 必须拒绝");
    assert!(matches!(zero, RedisError::Config(_)), "{zero}");

    let sub_ms = client
        .lock_acquire("lk", Duration::from_nanos(1))
        .await
        .expect_err("亚毫秒 TTL 必须拒绝");
    assert!(matches!(sub_ms, RedisError::Config(_)), "{sub_ms}");

    let empty_key = client
        .lock_acquire("", Duration::from_secs(1))
        .await
        .expect_err("空 key 必须拒绝");
    assert!(matches!(empty_key, RedisError::Config(_)), "{empty_key}");
}

/// `RedisPool::connect`：不可达地址必须返回错误，而不是「未建连的池」。
#[tokio::test]
async fn pool_connect_refused_errors() {
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        RedisPool::connect(refused_config()),
    )
    .await
    .expect("connect 必须受内部超时约束");
    let error = result.expect_err("不可达地址不得建池成功");
    assert!(error.is_retryable() || matches!(error, RedisError::Connection(_)));
}

/// `RedisPool::{ping,health_check}`：未建连时探活与健康检查都必须报错。
#[tokio::test]
async fn pool_ping_and_health_fail_closed() {
    let pool = RedisPool::new(RedisConfig::default()).expect("仅校验配置");
    assert!(!pool.liveness(), "未建连时 liveness 应为 false");

    let ping_error = pool.ping().await.expect_err("未建连不得 ping 成功");
    assert!(
        matches!(ping_error, RedisError::Connection(_)),
        "{ping_error}"
    );

    let health_error = pool
        .health_check()
        .await
        .expect_err("未建连不得返回健康快照");
    assert!(
        matches!(health_error, RedisError::Connection(_)),
        "{health_error}"
    );
}

/// `RedisError::is_retryable`：只有连接 / 瞬时 / 超时 / I/O 可自动重试。
#[test]
fn error_is_retryable_classification() {
    for retryable in [
        RedisError::Connection(String::new()),
        RedisError::Transient(String::new()),
        RedisError::Timeout(String::new()),
        RedisError::Io(std::io::Error::other("io")),
    ] {
        assert!(retryable.is_retryable(), "应可重试: {retryable}");
    }
    for permanent in [
        RedisError::Config(String::new()),
        RedisError::Backend(String::new()),
        RedisError::Serialization(String::new()),
        RedisError::Unsupported(String::new()),
        RedisError::Conflict(String::new()),
        RedisError::Missing(String::new()),
        RedisError::Internal(String::new()),
    ] {
        assert!(!permanent.is_retryable(), "不得自动重试: {permanent}");
    }
}
