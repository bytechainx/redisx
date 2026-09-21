//! `config` 模块单元测试。

use super::*;

// 低层解析辅助现居 `config::parse`；`super::*` 未覆盖的三个在此显式引入。
use super::parse::{parse_bool, split_nodes};

fn secret() -> String {
    // 密码一律由非字面量源构造，避免硬编码凭据
    (0..12).map(|i| char::from(b'a' + (i % 26) as u8)).collect()
}

#[test]
fn debug_redacts_password_but_keeps_username() {
    let secret = secret();
    let cfg = RedisConfig::builder()
        .password(secret.clone())
        .username("alice")
        .build()
        .expect("cfg");
    let debug = format!("{cfg:?}");
    assert!(debug.contains("***"), "password must be redacted: {debug}");
    assert!(!debug.contains(&secret), "leaked password: {debug}");
    assert!(debug.contains("alice"));
}

#[test]
fn display_endpoint_redacts_password() {
    let cfg = RedisConfig::builder()
        .addr("10.0.0.1:6379")
        .username("u")
        .password("p".repeat(4))
        .db(2)
        .build()
        .expect("cfg");
    let endpoint = cfg.display_endpoint();
    assert!(endpoint.contains("***"), "endpoint={endpoint}");
    assert!(!endpoint.contains(":pppp@"));
    assert!(endpoint.contains("10.0.0.1:6379"));
    assert!(endpoint.contains("/2"));
}

#[test]
fn node_urls_are_redacted_in_debug_endpoint_and_errors() {
    let secret = secret();
    let node = format!("redis://alice:{secret}@redis.example:6379");
    let cfg = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .nodes([node])
        .build()
        .expect("cfg");
    let debug = format!("{cfg:?}");
    let endpoint = cfg.display_endpoint();
    assert!(!debug.contains(&secret), "debug={debug}");
    assert!(!endpoint.contains(&secret), "endpoint={endpoint}");
    assert!(debug.contains("alice:***"));
    assert!(endpoint.contains("alice:***"));

    let invalid = format!("redis://alice:{secret}@[");
    let err = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .nodes([invalid])
        .build()
        .expect_err("invalid URL");
    assert!(!err.to_string().contains(&secret), "err={err}");
}

#[test]
fn default_and_builder_roundtrip() {
    let cfg = RedisConfig::builder()
        .addr("127.0.0.1:6381")
        .warmup_count(3)
        .client_name("redisx-test")
        .command_lanes(4)
        .connect_timeout(Duration::from_millis(250))
        .command_timeout(Duration::from_millis(500))
        .acquire_timeout(Duration::from_millis(750))
        .blocking_timeout(Duration::from_secs(2))
        .tcp_keepalive(Duration::from_secs(30))
        .max_cluster_redirects(8)
        .max_in_flight(32)
        .build()
        .expect("cfg");
    assert_eq!(cfg.addr(), "127.0.0.1:6381");
    assert_eq!(cfg.warmup_count(), 3);
    assert_eq!(cfg.command_lanes(), 32);
    assert_eq!(cfg.max_in_flight(), 32);
    assert_eq!(cfg.blocking_timeout(), Duration::from_secs(2));
    assert_eq!(cfg.connect_timeout(), Duration::from_millis(250));
    assert_eq!(cfg.command_timeout(), Duration::from_millis(500));
    assert_eq!(cfg.acquire_timeout(), Duration::from_millis(750));
    assert_eq!(cfg.max_cluster_redirects(), 8);
    assert_eq!(cfg.client_name(), Some("redisx-test"));

    let default = RedisConfig::default();
    assert_eq!(default.mode(), RedisMode::Standalone);
    assert_eq!(default.addr(), "127.0.0.1:6379");
    assert_eq!(default.db(), 0);
    assert!(!default.tls());
    assert!(!default.has_password());
}

#[test]
fn clear_optional_fields() {
    let cfg = RedisConfig::builder()
        .username("u1")
        .password(secret())
        .sentinel_master("m1")
        .tcp_keepalive(Duration::from_secs(15))
        .build()
        .expect("cfg");
    assert!(cfg.has_password());
    assert_eq!(cfg.sentinel_master(), Some("m1"));
    assert_eq!(cfg.tcp_keepalive(), Some(Duration::from_secs(15)));

    let cleared = cfg
        .clone()
        .to_builder()
        .clear_username()
        .clear_password()
        .clear_sentinel_master()
        .clear_tcp_keepalive()
        .build()
        .expect("cleared");
    assert!(!cleared.has_password());
    assert!(cleared.sentinel_master().is_none());
    assert!(cleared.tcp_keepalive().is_none());
    assert!(!cleared.display_endpoint().contains("u1"));
}

#[test]
fn validate_rejects_invalid_values() {
    assert!(RedisConfig::builder().max_in_flight(0).build().is_err());
    assert!(RedisConfig::builder().db(-1).build().is_err());
    assert!(RedisConfig::builder()
        .max_cluster_redirects(0)
        .build()
        .is_err());

    for result in [
        RedisConfig::builder()
            .connect_timeout(Duration::ZERO)
            .build(),
        RedisConfig::builder()
            .command_timeout(Duration::ZERO)
            .build(),
        RedisConfig::builder()
            .acquire_timeout(Duration::ZERO)
            .build(),
        RedisConfig::builder()
            .reconnect_max_delay(Duration::ZERO)
            .build(),
        RedisConfig::builder()
            .blocking_timeout(Duration::ZERO)
            .build(),
        RedisConfig::builder().tcp_keepalive(Duration::ZERO).build(),
    ] {
        let err = result.expect_err("zero must fail");
        assert!(matches!(err, RedisError::Config(_)), "{err}");
    }
}

#[test]
fn cluster_and_sentinel_rules() {
    let cluster = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .addr("127.0.0.1:7000")
        .build()
        .expect("cluster");
    assert_eq!(cluster.mode(), RedisMode::Cluster);
    assert_eq!(
        cluster.seed_nodes().expect("seeds"),
        vec!["127.0.0.1:7000".to_owned()]
    );

    let multi = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .nodes(["10.0.0.1:7000", "10.0.0.2:7000"])
        .build()
        .expect("cluster nodes");
    assert_eq!(multi.nodes().len(), 2);
    assert_eq!(multi.seed_connection_infos().expect("infos").len(), 2);

    let db_err = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .addr("127.0.0.1:7000")
        .db(1)
        .build()
        .expect_err("cluster db must be 0");
    assert!(matches!(db_err, RedisError::Config(_)));

    let sentinel_err = RedisConfig::builder()
        .mode(RedisMode::Sentinel)
        .addr("127.0.0.1:26379")
        .build()
        .expect_err("sentinel needs master");
    assert!(sentinel_err.to_string().contains("sentinel_master"));

    let sentinel = RedisConfig::builder()
        .mode(RedisMode::Sentinel)
        .nodes(["127.0.0.1:26379"])
        .sentinel_master("mymaster")
        .build()
        .expect("sentinel");
    assert_eq!(sentinel.mode(), RedisMode::Sentinel);
    assert_eq!(sentinel.sentinel_master(), Some("mymaster"));
    assert!(sentinel.display_endpoint().contains("mode=sentinel"));
}

#[test]
fn tls_connection_info_is_secure() {
    let cfg = RedisConfig::builder()
        .addr("redis.example:6380")
        .tls(true)
        .build()
        .expect("cfg");
    let info = cfg.to_connection_info().expect("info");
    match info.addr {
        redis::ConnectionAddr::TcpTls {
            host,
            port,
            insecure,
            tls_params,
        } => {
            assert_eq!(host, "redis.example");
            assert_eq!(port, 6380);
            assert!(!insecure, "必须强制证书校验");
            assert!(tls_params.is_none());
        }
        other => panic!("expected TcpTls, got {other:?}"),
    }
}

#[test]
fn from_url_parses_auth_and_tls() {
    let cfg = RedisConfig::from_url("redis://user:secret@127.0.0.1:6380/3").expect("url");
    assert_eq!(cfg.addr(), "127.0.0.1:6380");
    assert_eq!(cfg.db(), 3);
    assert_eq!(cfg.username(), Some("user"));
    assert_eq!(cfg.password_opt(), Some("secret"));
    assert!(!cfg.tls());
    assert_eq!(cfg.mode(), RedisMode::Standalone);

    let tls = RedisConfig::from_url("rediss://127.0.0.1:6380/0").expect("url");
    assert!(tls.tls());
    let info = tls.to_connection_info().expect("info");
    assert!(matches!(
        info.addr,
        redis::ConnectionAddr::TcpTls {
            insecure: false,
            ..
        }
    ));

    let unix = RedisConfig::from_url("unix:///tmp/redis.sock").expect_err("unix unsupported");
    assert!(matches!(unix, RedisError::Unsupported(_)));
}

#[test]
fn from_toml_applies_timeouts_and_mode() {
    let cfg = RedisConfig::from_toml(
        r#"
            addr = "10.0.0.5:6380"
            nodes = ["10.0.0.6:6380", "10.0.0.7:6380"]
            mode = "cluster"
            connect_timeout_ms = 120
            command_timeout_ms = 340
            max_in_flight = 8
            warmup_count = 2
            tcp_keepalive_ms = 15000
            "#,
    )
    .expect("toml");
    assert_eq!(cfg.addr(), "10.0.0.5:6380");
    assert_eq!(cfg.mode(), RedisMode::Cluster);
    assert_eq!(cfg.nodes(), ["10.0.0.6:6380", "10.0.0.7:6380"]);
    assert_eq!(cfg.connect_timeout(), Duration::from_millis(120));
    assert_eq!(cfg.command_timeout(), Duration::from_millis(340));
    assert_eq!(cfg.max_in_flight(), 8);
    assert_eq!(cfg.warmup_count(), 2);
    assert_eq!(cfg.tcp_keepalive(), Some(Duration::from_secs(15)));
}

#[test]
fn from_toml_accepts_csv_nodes_and_rejects_bad_mode() {
    let csv = RedisConfig::from_toml(
        r#"
            mode = "sentinel"
            nodes = "127.0.0.1:26379, 127.0.0.1:26380"
            sentinel_master = "mymaster"
            "#,
    )
    .expect("csv nodes");
    assert_eq!(csv.nodes().len(), 2);
    assert_eq!(csv.mode(), RedisMode::Sentinel);

    let err = RedisConfig::from_toml(r#"mode = "wat""#).expect_err("bad mode");
    assert!(matches!(err, RedisError::Config(_)));
    assert!(RedisConfig::from_toml("addr = = 1").is_err());
}

#[test]
fn serde_deserialize_validates() {
    let cfg: RedisConfig =
        serde_json::from_str(r#"{"addr":"127.0.0.1:7000","mode":"cluster"}"#).expect("json");
    assert_eq!(cfg.mode(), RedisMode::Cluster);
    let err =
        serde_json::from_str::<RedisConfig>(r#"{"max_in_flight":0}"#).expect_err("zero lanes");
    assert!(err.to_string().contains("max_in_flight"));
}

#[test]
fn helpers_parse_and_validate() {
    assert_eq!(
        parse_host_port("localhost").expect("port"),
        ("localhost".to_owned(), 6379)
    );
    assert_eq!(
        parse_host_port("[::1]:6380").expect("ipv6"),
        ("::1".to_owned(), 6380)
    );
    assert!(parse_host_port("10.1.2.3:not-a-port").is_err());

    assert!(validate_seed("127.0.0.1:6379").is_ok());
    assert!(validate_seed("redis://127.0.0.1:6379").is_ok());
    assert!(validate_seed("").is_err());
    assert!(validate_seed("redis://").is_err());

    assert!(parse_bool("ON").expect("on"));
    assert!(!parse_bool("0").expect("off"));
    assert!(parse_bool("maybe").is_err());

    assert_eq!(parse_mode("cluster").expect("cluster"), RedisMode::Cluster);
    assert_eq!(
        parse_mode("SENTINEL").expect("sentinel"),
        RedisMode::Sentinel
    );
    assert_eq!(parse_mode("single").expect("single"), RedisMode::Standalone);
    assert!(parse_mode("wat").is_err());

    assert_eq!(
        split_nodes(" a:1 , ,b:2 "),
        vec!["a:1".to_owned(), "b:2".to_owned()]
    );
}

#[test]
fn redaction_helper_matrix() {
    assert_eq!(redact_seed_url("redis://u:p@h:1"), "redis://u:***@h:1");
    assert_eq!(redact_seed_url("redis://p@h:1"), "redis://***@h:1");
    assert_eq!(redact_seed_url("h:1"), "h:1");
}
