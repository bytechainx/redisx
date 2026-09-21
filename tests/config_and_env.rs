//! 配置集成测试：校验正/反用例、环境变量加载、密码脱敏、URL 与模式推断、TOML 解析。

use std::time::Duration;

use redisx::{RedisConfig, RedisError, RedisMode};

const SECRET: &str = "unused-test-credential";

/// 与本 crate 相关的全部环境变量，便于在单个测试内独占操作（避免并行测试互相干扰）。
const REDISX_ENV_KEYS: [&str; 12] = [
    redisx::ENV_URL,
    redisx::ENV_ADDR,
    redisx::ENV_USERNAME,
    redisx::ENV_PASSWORD,
    redisx::ENV_DB,
    redisx::ENV_TLS,
    redisx::ENV_MODE,
    redisx::ENV_NODES,
    redisx::ENV_SENTINEL_MASTER,
    redisx::ENV_WARMUP,
    redisx::ENV_MAX_IN_FLIGHT,
    redisx::ENV_BLOCKING_TIMEOUT_MS,
];

fn clear_env() {
    for key in REDISX_ENV_KEYS {
        std::env::remove_var(key);
    }
}

#[test]
fn env_prefix_is_documented_and_stable() {
    assert_eq!(redisx::ENV_PREFIX, "FOUNDATIONX_REDISX_");
    for key in [
        redisx::ENV_ADDR,
        redisx::ENV_USERNAME,
        redisx::ENV_PASSWORD,
        redisx::ENV_DB,
        redisx::ENV_TLS,
        redisx::ENV_MODE,
        redisx::ENV_NODES,
        redisx::ENV_SENTINEL_MASTER,
        redisx::ENV_WARMUP,
        redisx::ENV_MAX_IN_FLIGHT,
        redisx::ENV_BLOCKING_TIMEOUT_MS,
    ] {
        assert!(key.starts_with(redisx::ENV_PREFIX), "key={key}");
    }
    assert_eq!(
        redisx::ENV_URL,
        "REDIS_URL",
        "REDIS_URL 是标准约定，不带前缀"
    );
}

#[test]
fn validate_accepts_defaults_and_rejects_invalid_values() {
    RedisConfig::default().validate().expect("默认配置合法");
    RedisConfig::builder().build().expect("builder 默认合法");

    assert!(RedisConfig::default()
        .to_builder()
        .addr("")
        .build()
        .is_err());
    assert!(RedisConfig::default()
        .to_builder()
        .addr("127.0.0.1:not-a-port")
        .build()
        .is_err());
    assert!(RedisConfig::default()
        .to_builder()
        .addr("[::1].build")
        .build()
        .is_err());
    assert!(RedisConfig::default()
        .to_builder()
        .max_in_flight(0)
        .build()
        .is_err());
    assert!(RedisConfig::default().to_builder().db(-1).build().is_err());
    assert!(RedisConfig::default()
        .to_builder()
        .max_cluster_redirects(0)
        .build()
        .is_err());
    assert!(RedisConfig::default()
        .to_builder()
        .connect_timeout(Duration::ZERO)
        .build()
        .is_err());
    assert!(RedisConfig::default()
        .to_builder()
        .command_timeout(Duration::ZERO)
        .build()
        .is_err());
    assert!(RedisConfig::default()
        .to_builder()
        .acquire_timeout(Duration::ZERO)
        .build()
        .is_err());
    assert!(RedisConfig::default()
        .to_builder()
        .blocking_timeout(Duration::ZERO)
        .build()
        .is_err());
    assert!(RedisConfig::default()
        .to_builder()
        .reconnect_max_delay(Duration::ZERO)
        .build()
        .is_err());
    assert!(RedisConfig::default()
        .to_builder()
        .tcp_keepalive(Duration::ZERO)
        .build()
        .is_err());

    for err in [
        RedisConfig::default()
            .to_builder()
            .max_in_flight(0)
            .build()
            .expect_err("lanes"),
        RedisConfig::default()
            .to_builder()
            .db(-1)
            .build()
            .expect_err("db"),
        RedisConfig::default()
            .to_builder()
            .connect_timeout(Duration::ZERO)
            .build()
            .expect_err("t"),
    ] {
        assert!(matches!(err, RedisError::Config(_)), "{err}");
    }

    // 合法边界：显式端口、IPv6、最小 lane 数
    let ok = RedisConfig::builder()
        .addr("[::1]:6380")
        .max_in_flight(1)
        .command_lanes(1)
        .tcp_keepalive(Duration::from_secs(1))
        .build()
        .expect("边界配置合法");
    assert_eq!(ok.max_in_flight(), 1);
}

#[test]
fn topology_specific_rules() {
    // Cluster：非 0 逻辑库非法
    let err = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .nodes(["127.0.0.1:7000"])
        .db(3)
        .build()
        .expect_err("cluster 不支持 db != 0");
    assert!(err.to_string().contains("逻辑库"));

    // Cluster：种子非法 URL 非法（且错误信息不含密码）
    let node = format!("redis://alice:{SECRET}@");
    let err = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .nodes([node])
        .build()
        .expect_err("非法节点 URL");
    assert!(!err.to_string().contains(SECRET), "err={err}");

    // Sentinel：缺少 master 名非法
    let err = RedisConfig::builder()
        .mode(RedisMode::Sentinel)
        .nodes(["127.0.0.1:26379"])
        .build()
        .expect_err("缺少 sentinel_master");
    assert!(err.to_string().contains("sentinel_master"));

    // Sentinel：满足条件后合法，且模式进入端点展示
    let cfg = RedisConfig::builder()
        .mode(RedisMode::Sentinel)
        .nodes(["127.0.0.1:26379"])
        .sentinel_master("mymaster")
        .build()
        .expect("sentinel");
    assert_eq!(cfg.mode(), RedisMode::Sentinel);
    assert_eq!(cfg.sentinel_master(), Some("mymaster"));
    assert!(cfg.display_endpoint().contains("mode=sentinel"));

    // Standalone：tls 强制证书校验
    let tls = RedisConfig::builder()
        .addr("redis.example:6380")
        .tls(true)
        .build()
        .expect("tls");
    assert!(tls.tls());
    assert!(tls.display_endpoint().starts_with("rediss://"));
}

#[test]
fn mode_is_inferred_from_url_and_text() {
    // rediss:// 推断 TLS + standalone
    let cfg = RedisConfig::from_url("rediss://user:pass@127.0.0.1:6380/4").expect("url");
    assert_eq!(cfg.mode(), RedisMode::Standalone);
    assert!(cfg.tls());
    assert_eq!(cfg.db(), 4);
    assert_eq!(cfg.addr(), "127.0.0.1:6380");

    // 文本模式（大小写不敏感、single 视为 standalone）
    for (text, expected) in [
        ("standalone", RedisMode::Standalone),
        ("single", RedisMode::Standalone),
        ("CLUSTER", RedisMode::Cluster),
        ("Sentinel", RedisMode::Sentinel),
    ] {
        let toml = match expected {
            RedisMode::Sentinel => {
                format!("mode = \"{text}\"\naddr = \"127.0.0.1:26379\"\nsentinel_master = \"m\"\n")
            }
            _ => format!("mode = \"{text}\"\naddr = \"127.0.0.1:6379\"\n"),
        };
        let cfg = RedisConfig::from_toml(&toml).expect("toml mode");
        assert_eq!(cfg.mode(), expected, "text={text}");
    }

    // insecure TLS（URL 片段 #insecure）与 unix socket fail-closed
    let insecure =
        RedisConfig::from_url("rediss://127.0.0.1:6380/#insecure").expect_err("insecure");
    assert!(matches!(insecure, RedisError::Config(_)), "{insecure}");
    let unix = RedisConfig::from_url("unix:///tmp/redis.sock").expect_err("unix");
    assert!(matches!(unix, RedisError::Unsupported(_)), "{unix}");
    let bad_mode = RedisConfig::from_toml("mode = \"wat\"").expect_err("bad mode");
    assert!(matches!(bad_mode, RedisError::Config(_)));
}

#[test]
fn toml_roundtrip_including_csv_and_array_nodes() {
    let from_array = RedisConfig::from_toml(
        r#"
        mode = "cluster"
        nodes = ["10.0.0.1:7000", "10.0.0.2:7000"]
        connect_timeout_ms = 111
        command_timeout_ms = 222
        acquire_timeout_ms = 333
        max_in_flight = 7
        warmup_count = 2
        blocking_timeout_ms = 444
        reconnect_max_delay_ms = 555
        max_cluster_redirects = 6
        tcp_keepalive_ms = 15000
        client_name = "redisx-config-test"
        "#,
    )
    .expect("toml array nodes");
    assert_eq!(from_array.nodes(), ["10.0.0.1:7000", "10.0.0.2:7000"]);
    assert_eq!(from_array.mode(), RedisMode::Cluster);
    assert_eq!(from_array.connect_timeout(), Duration::from_millis(111));
    assert_eq!(from_array.command_timeout(), Duration::from_millis(222));
    assert_eq!(from_array.acquire_timeout(), Duration::from_millis(333));
    assert_eq!(from_array.max_in_flight(), 7);
    assert_eq!(from_array.command_lanes(), 7);
    assert_eq!(from_array.warmup_count(), 2);
    assert_eq!(from_array.blocking_timeout(), Duration::from_millis(444));
    assert_eq!(from_array.reconnect_max_delay(), Duration::from_millis(555));
    assert_eq!(from_array.max_cluster_redirects(), 6);
    assert_eq!(from_array.tcp_keepalive(), Some(Duration::from_secs(15)));
    assert_eq!(from_array.client_name(), Some("redisx-config-test"));

    let from_csv = RedisConfig::from_toml(
        r#"
        mode = "sentinel"
        nodes = "127.0.0.1:26379, 127.0.0.1:26380"
        sentinel_master = "mymaster"
        "#,
    )
    .expect("toml csv nodes");
    assert_eq!(from_csv.nodes().len(), 2);
    assert_eq!(from_csv.sentinel_master(), Some("mymaster"));

    assert!(RedisConfig::from_toml("not = toml =").is_err());
}

#[test]
fn password_is_redacted_everywhere() {
    let secret = String::from(SECRET);
    let cfg = RedisConfig::builder()
        .addr("10.0.0.9:6379")
        .username("alice")
        .password(secret.clone())
        .db(1)
        .build()
        .expect("cfg");

    let debug = format!("{cfg:?}");
    assert!(debug.contains("***"), "debug={debug}");
    assert!(!debug.contains(&secret), "Debug 泄漏密码: {debug}");
    assert!(debug.contains("alice"));

    let endpoint = cfg.display_endpoint();
    assert!(endpoint.contains("alice:***@"), "endpoint={endpoint}");
    assert!(!endpoint.contains(&secret), "端点泄漏密码: {endpoint}");
    assert!(cfg.has_password());

    // 种子 URL 内的凭据同样脱敏
    let node = format!("redis://bob:{secret}@redis.example:6379");
    let cluster = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .nodes([node])
        .build()
        .expect("cluster");
    let debug = format!("{cluster:?}");
    let endpoint = cluster.display_endpoint();
    assert!(!debug.contains(&secret), "debug={debug}");
    assert!(!endpoint.contains(&secret), "endpoint={endpoint}");
    assert!(debug.contains("bob:***"));

    // 未设置密码时不做多余标记
    let plain = RedisConfig::builder()
        .addr("10.0.0.9:6379")
        .build()
        .expect("plain");
    assert!(!plain.has_password());
    assert!(!plain.display_endpoint().contains("***"));
}

#[test]
fn from_env_reads_prefixed_variables() {
    // 环境变量操作集中在单个测试内执行，避免并行测试互相干扰
    clear_env();
    let defaulted = RedisConfig::from_env().expect("默认");
    assert_eq!(defaulted.addr(), "127.0.0.1:6379");
    assert_eq!(defaulted.db(), 0);
    assert!(!defaulted.tls());
    assert_eq!(defaulted.mode(), RedisMode::Standalone);
    // username 规范默认 default
    assert_eq!(defaulted.username(), Some("default"));

    // 显式空用户名表示不发送 username
    std::env::set_var(redisx::ENV_USERNAME, "");
    let without = RedisConfig::from_env().expect("空用户名");
    assert_eq!(without.username(), None);
    assert!(!without.display_endpoint().contains('@'));

    std::env::set_var(redisx::ENV_ADDR, "10.1.2.3:6390");
    std::env::set_var(redisx::ENV_DB, "5");
    std::env::set_var(redisx::ENV_TLS, "yes");
    std::env::set_var(redisx::ENV_MODE, "cluster");
    std::env::set_var(redisx::ENV_NODES, "10.1.2.4:7000, 10.1.2.5:7000");
    std::env::set_var(redisx::ENV_USERNAME, "acl-user");
    std::env::set_var(redisx::ENV_PASSWORD, SECRET);
    std::env::set_var(redisx::ENV_WARMUP, "3");
    std::env::set_var(redisx::ENV_MAX_IN_FLIGHT, "12");
    std::env::set_var(redisx::ENV_BLOCKING_TIMEOUT_MS, "1500");

    // Cluster 模式不允许非 0 db，因此这里期望校验失败（证明 env 值确实生效）
    let cluster_err = RedisConfig::from_env().expect_err("cluster + db=5 必须失败");
    assert!(
        matches!(cluster_err, RedisError::Config(_)),
        "{cluster_err}"
    );

    std::env::set_var(redisx::ENV_DB, "0");
    let cfg = RedisConfig::from_env().expect("from_env");
    assert_eq!(cfg.addr(), "10.1.2.3:6390");
    assert_eq!(cfg.mode(), RedisMode::Cluster);
    assert_eq!(cfg.nodes(), ["10.1.2.4:7000", "10.1.2.5:7000"]);
    assert_eq!(cfg.username(), Some("acl-user"));
    assert_eq!(cfg.db(), 0);
    assert!(cfg.tls());
    assert!(cfg.has_password());
    assert!(!format!("{cfg:?}").contains(SECRET));
    assert_eq!(cfg.warmup_count(), 3);
    assert_eq!(cfg.max_in_flight(), 12);
    assert_eq!(cfg.blocking_timeout(), Duration::from_millis(1500));

    // REDIS_URL 覆盖其余环境变量
    std::env::set_var(redisx::ENV_URL, "redis://override:pass@127.0.0.1:6380/2");
    let url_cfg = RedisConfig::from_env().expect("REDIS_URL");
    assert_eq!(url_cfg.addr(), "127.0.0.1:6380");
    assert_eq!(url_cfg.db(), 2);
    assert_eq!(url_cfg.mode(), RedisMode::Standalone);
    assert!(!url_cfg.tls());

    // 非法取值必须 fail-closed
    clear_env();
    std::env::set_var(redisx::ENV_DB, "not-a-number");
    assert!(RedisConfig::from_env().is_err());
    std::env::set_var(redisx::ENV_DB, "0");
    std::env::set_var(redisx::ENV_TLS, "maybe");
    assert!(RedisConfig::from_env().is_err());
    std::env::set_var(redisx::ENV_TLS, "false");
    std::env::set_var(redisx::ENV_MODE, "nope");
    assert!(RedisConfig::from_env().is_err());
    std::env::set_var(redisx::ENV_MODE, "standalone");
    std::env::set_var(redisx::ENV_MAX_IN_FLIGHT, "0");
    assert!(RedisConfig::from_env().is_err());

    clear_env();
    let _ = RedisConfig::from_env().expect("清理后仍可用");
}

#[test]
fn deserialize_applies_validation() {
    let cfg: RedisConfig =
        serde_json::from_str(r#"{"addr":"127.0.0.1:7000","mode":"cluster","max_in_flight":9}"#)
            .expect("json");
    assert_eq!(cfg.mode(), RedisMode::Cluster);
    assert_eq!(cfg.max_in_flight(), 9);

    let err = serde_json::from_str::<RedisConfig>(r#"{"max_in_flight":0}"#).expect_err("非法");
    assert!(err.to_string().contains("max_in_flight"));
    let err = serde_json::from_str::<RedisConfig>(r#"{"mode":"nope"}"#).expect_err("非法模式");
    assert!(err.to_string().contains("模式"));
}
