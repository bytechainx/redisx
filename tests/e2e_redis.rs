#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! E2E（redisx）：离线 fail-closed + 真连公开面全程。
//!
//! 真连凭据只从进程环境 `FOUNDATIONX_REDISX_*` / `REDIS_URL` 读取。
//! 本机注入：工作区根 `.config/redisx.env`（禁止入库）。
//!
//! ```bash
//! set -a; . /home/workspace/bytechainx/.config/redisx.env; set +a
//! cd /home/workspace/bytechainx/.worktrees/redisx/e2e-public-api
//! CARGO_TARGET_DIR=/home/workspace/bytechainx/.cargo/wt/redisx \
//!   cargo test --test e2e_redis -- --include-ignored --test-threads=1
//! ```

use std::collections::BTreeSet;
use std::io::{Error as IoError, ErrorKind};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use redisx::{
    generate_lock_token, lock_token_matches, map_redis_error, map_redis_result, with_retry,
    RedisAtomicity, RedisClient, RedisConfig, RedisError, RedisHealth, RedisMetricsSnapshot,
    RedisMode, RedisOperation, RedisPool, RedisPoolStats, RedisPubSub, RedisPubSubMessage,
    RedisResult, RedisRetrySafety, RetryConfig, StreamEntry, TxCmd, ENV_ADDR,
    ENV_BLOCKING_TIMEOUT_MS, ENV_DB, ENV_MAX_IN_FLIGHT, ENV_MODE, ENV_NODES, ENV_PASSWORD,
    ENV_PREFIX, ENV_SENTINEL_MASTER, ENV_TLS, ENV_URL, ENV_USERNAME, ENV_WARMUP,
};

const E2E_MANIFEST: &[(&str, &str)] = &[
    ("type", "RedisAtomicity"),
    ("variant", "RedisAtomicity::MultiKeySingleSlot"),
    ("variant", "RedisAtomicity::None"),
    ("variant", "RedisAtomicity::SingleCommand"),
    ("type", "RedisError"),
    ("variant", "RedisError::Backend"),
    ("variant", "RedisError::Config"),
    ("variant", "RedisError::Conflict"),
    ("variant", "RedisError::Connection"),
    ("variant", "RedisError::Internal"),
    ("variant", "RedisError::Io"),
    ("variant", "RedisError::Missing"),
    ("variant", "RedisError::Serialization"),
    ("variant", "RedisError::Timeout"),
    ("variant", "RedisError::Transient"),
    ("variant", "RedisError::Unsupported"),
    ("fn", "RedisError::is_retryable"),
    ("fn", "RedisError::label"),
    ("type", "RedisMode"),
    ("variant", "RedisMode::Cluster"),
    ("variant", "RedisMode::Sentinel"),
    ("variant", "RedisMode::Standalone"),
    ("type", "RedisOperation"),
    ("variant", "RedisOperation::Delete"),
    ("variant", "RedisOperation::Exists"),
    ("variant", "RedisOperation::Expire"),
    ("variant", "RedisOperation::Get"),
    ("variant", "RedisOperation::Incr"),
    ("variant", "RedisOperation::Mget"),
    ("variant", "RedisOperation::Mset"),
    ("variant", "RedisOperation::Publish"),
    ("variant", "RedisOperation::Set"),
    ("variant", "RedisOperation::Ttl"),
    ("fn", "RedisOperation::allows_automatic_retry"),
    ("fn", "RedisOperation::atomicity"),
    ("fn", "RedisOperation::retry_safety"),
    ("type", "RedisRetrySafety"),
    ("variant", "RedisRetrySafety::AmbiguousWrite"),
    ("variant", "RedisRetrySafety::Idempotent"),
    ("variant", "RedisRetrySafety::NeverAutomatic"),
    ("variant", "RedisRetrySafety::ReadOnly"),
    ("type", "TxCmd"),
    ("variant", "TxCmd::Del"),
    ("variant", "TxCmd::Incr"),
    ("variant", "TxCmd::Set"),
    ("fn", "TxCmd::command_name"),
    ("fn", "TxCmd::del"),
    ("fn", "TxCmd::incr"),
    ("fn", "TxCmd::key"),
    ("fn", "TxCmd::set"),
    ("type", "RedisClient"),
    ("fn", "RedisClient::blpop"),
    ("fn", "RedisClient::hdel"),
    ("fn", "RedisClient::hget"),
    ("fn", "RedisClient::hgetall"),
    ("fn", "RedisClient::hset"),
    ("fn", "RedisClient::lpop"),
    ("fn", "RedisClient::lpush"),
    ("fn", "RedisClient::lrange"),
    ("fn", "RedisClient::rpush"),
    ("fn", "RedisClient::sadd"),
    ("fn", "RedisClient::sismember"),
    ("fn", "RedisClient::srem"),
    ("fn", "RedisClient::zadd"),
    ("fn", "RedisClient::zrem"),
    ("fn", "RedisClient::zscore"),
    ("fn", "RedisClient::config"),
    ("fn", "RedisClient::connect"),
    ("fn", "RedisClient::connect_from_env"),
    ("fn", "RedisClient::connect_url"),
    ("fn", "RedisClient::del"),
    ("fn", "RedisClient::endpoint"),
    ("fn", "RedisClient::exists"),
    ("fn", "RedisClient::expire"),
    ("fn", "RedisClient::get"),
    ("fn", "RedisClient::get_bytes"),
    ("fn", "RedisClient::has_call_deadline"),
    ("fn", "RedisClient::health_check"),
    ("fn", "RedisClient::incr"),
    ("fn", "RedisClient::mget"),
    ("fn", "RedisClient::mset"),
    ("fn", "RedisClient::new"),
    ("fn", "RedisClient::ping"),
    ("fn", "RedisClient::pool"),
    ("fn", "RedisClient::retry_config"),
    ("fn", "RedisClient::set"),
    ("fn", "RedisClient::set_bytes"),
    ("fn", "RedisClient::set_ex"),
    ("fn", "RedisClient::ttl"),
    ("fn", "RedisClient::with_call_deadline"),
    ("fn", "RedisClient::with_retry"),
    ("fn", "RedisClient::eval_script"),
    ("fn", "RedisClient::eval_sha"),
    ("fn", "RedisClient::lock_acquire"),
    ("fn", "RedisClient::lock_extend"),
    ("fn", "RedisClient::lock_release"),
    ("fn", "RedisClient::pipeline_set"),
    ("fn", "RedisClient::script_load_and_eval"),
    ("fn", "RedisClient::multi_exec"),
    ("fn", "RedisClient::multi_set"),
    ("fn", "RedisClient::xack"),
    ("fn", "RedisClient::xadd"),
    ("fn", "RedisClient::xadd_with_id"),
    ("fn", "RedisClient::xdel"),
    ("fn", "RedisClient::xlen"),
    ("fn", "RedisClient::xrange"),
    ("fn", "RedisClient::xread"),
    ("fn", "RedisClient::xread_block"),
    ("type", "RedisConfig"),
    ("fn", "RedisConfig::acquire_timeout"),
    ("fn", "RedisConfig::addr"),
    ("fn", "RedisConfig::blocking_timeout"),
    ("fn", "RedisConfig::client_name"),
    ("fn", "RedisConfig::command_lanes"),
    ("fn", "RedisConfig::command_timeout"),
    ("fn", "RedisConfig::connect_timeout"),
    ("fn", "RedisConfig::db"),
    ("fn", "RedisConfig::display_endpoint"),
    ("fn", "RedisConfig::has_password"),
    ("fn", "RedisConfig::max_cluster_redirects"),
    ("fn", "RedisConfig::max_in_flight"),
    ("fn", "RedisConfig::mode"),
    ("fn", "RedisConfig::nodes"),
    ("fn", "RedisConfig::reconnect_max_delay"),
    ("fn", "RedisConfig::sentinel_master"),
    ("fn", "RedisConfig::tcp_keepalive"),
    ("fn", "RedisConfig::tls"),
    ("fn", "RedisConfig::username"),
    ("fn", "RedisConfig::warmup_count"),
    ("fn", "RedisConfig::builder"),
    ("fn", "RedisConfig::from_env"),
    ("fn", "RedisConfig::from_toml"),
    ("fn", "RedisConfig::from_url"),
    ("fn", "RedisConfig::to_builder"),
    ("fn", "RedisConfig::validate"),
    ("type", "RedisConfigBuilder"),
    ("fn", "RedisConfigBuilder::acquire_timeout"),
    ("fn", "RedisConfigBuilder::addr"),
    ("fn", "RedisConfigBuilder::blocking_timeout"),
    ("fn", "RedisConfigBuilder::build"),
    ("fn", "RedisConfigBuilder::clear_password"),
    ("fn", "RedisConfigBuilder::clear_sentinel_master"),
    ("fn", "RedisConfigBuilder::clear_tcp_keepalive"),
    ("fn", "RedisConfigBuilder::clear_username"),
    ("fn", "RedisConfigBuilder::client_name"),
    ("fn", "RedisConfigBuilder::command_lanes"),
    ("fn", "RedisConfigBuilder::command_timeout"),
    ("fn", "RedisConfigBuilder::connect_timeout"),
    ("fn", "RedisConfigBuilder::db"),
    ("fn", "RedisConfigBuilder::max_cluster_redirects"),
    ("fn", "RedisConfigBuilder::max_in_flight"),
    ("fn", "RedisConfigBuilder::mode"),
    ("fn", "RedisConfigBuilder::nodes"),
    ("fn", "RedisConfigBuilder::password"),
    ("fn", "RedisConfigBuilder::password_from_provider"),
    ("fn", "RedisConfigBuilder::reconnect_max_delay"),
    ("fn", "RedisConfigBuilder::sentinel_master"),
    ("fn", "RedisConfigBuilder::tcp_keepalive"),
    ("fn", "RedisConfigBuilder::tls"),
    ("fn", "RedisConfigBuilder::username"),
    ("fn", "RedisConfigBuilder::warmup_count"),
    ("type", "RedisHealth"),
    ("field", "RedisHealth::endpoint"),
    ("field", "RedisHealth::latency"),
    ("field", "RedisHealth::mode"),
    ("type", "RedisLock"),
    ("fn", "RedisLock::fence"),
    ("fn", "RedisLock::key"),
    ("fn", "RedisLock::token"),
    ("fn", "RedisLock::verify_token"),
    ("type", "RedisMetricsSnapshot"),
    ("field", "RedisMetricsSnapshot::acquire_timeout"),
    ("field", "RedisMetricsSnapshot::commands_err"),
    ("field", "RedisMetricsSnapshot::commands_ok"),
    ("field", "RedisMetricsSnapshot::commands_timeout"),
    ("field", "RedisMetricsSnapshot::rejected_closed"),
    ("type", "RedisPool"),
    ("fn", "RedisPool::acquire"),
    ("fn", "RedisPool::close"),
    ("fn", "RedisPool::command_lanes"),
    ("fn", "RedisPool::command_timeout"),
    ("fn", "RedisPool::config"),
    ("fn", "RedisPool::del"),
    ("fn", "RedisPool::endpoint"),
    ("fn", "RedisPool::exists"),
    ("fn", "RedisPool::expire"),
    ("fn", "RedisPool::get"),
    ("fn", "RedisPool::health_check"),
    ("fn", "RedisPool::incr"),
    ("fn", "RedisPool::is_closed"),
    ("fn", "RedisPool::liveness"),
    ("fn", "RedisPool::metrics_snapshot"),
    ("fn", "RedisPool::ping"),
    ("fn", "RedisPool::readiness"),
    ("fn", "RedisPool::reconnect_max_delay"),
    ("fn", "RedisPool::set"),
    ("fn", "RedisPool::set_ex"),
    ("fn", "RedisPool::stats"),
    ("fn", "RedisPool::subscribe"),
    ("fn", "RedisPool::tcp_keepalive"),
    ("fn", "RedisPool::ttl"),
    ("fn", "RedisPool::client"),
    ("fn", "RedisPool::connect"),
    ("fn", "RedisPool::connect_from_env"),
    ("fn", "RedisPool::new"),
    ("type", "RedisPoolPermit"),
    ("fn", "RedisPoolPermit::command_timeout"),
    ("fn", "RedisPoolPermit::del"),
    ("fn", "RedisPoolPermit::endpoint"),
    ("fn", "RedisPoolPermit::exists"),
    ("fn", "RedisPoolPermit::expire"),
    ("fn", "RedisPoolPermit::get"),
    ("fn", "RedisPoolPermit::incr"),
    ("fn", "RedisPoolPermit::ping"),
    ("fn", "RedisPoolPermit::set"),
    ("fn", "RedisPoolPermit::set_ex"),
    ("fn", "RedisPoolPermit::ttl"),
    ("type", "RedisPoolStats"),
    ("field", "RedisPoolStats::in_flight"),
    ("field", "RedisPoolStats::open"),
    ("field", "RedisPoolStats::waiters"),
    ("type", "RedisPubSub"),
    ("fn", "RedisPubSub::connect_config"),
    ("fn", "RedisPubSub::endpoint"),
    ("fn", "RedisPubSub::into_message_stream"),
    ("fn", "RedisPubSub::into_result_message_stream"),
    ("fn", "RedisPubSub::publish"),
    ("type", "RedisPubSubMessage"),
    ("field", "RedisPubSubMessage::channel"),
    ("field", "RedisPubSubMessage::payload"),
    ("type", "RetryConfig"),
    ("fn", "RetryConfig::backoff_for"),
    ("fn", "RetryConfig::deadline"),
    ("fn", "RetryConfig::exponential"),
    ("fn", "RetryConfig::fixed"),
    ("fn", "RetryConfig::initial_backoff"),
    ("fn", "RetryConfig::jitter"),
    ("fn", "RetryConfig::jittered"),
    ("fn", "RetryConfig::max_attempts"),
    ("fn", "RetryConfig::max_backoff"),
    ("fn", "RetryConfig::multiplier"),
    ("fn", "RetryConfig::with_deadline"),
    ("fn", "RetryConfig::with_multiplier"),
    ("fn", "RetryConfig::without_jitter"),
    ("type", "StreamEntry"),
    ("field", "StreamEntry::fields"),
    ("field", "StreamEntry::id"),
    ("fn", "StreamEntry::field"),
    ("const", "ENV_ADDR"),
    ("const", "ENV_BLOCKING_TIMEOUT_MS"),
    ("const", "ENV_DB"),
    ("const", "ENV_MAX_IN_FLIGHT"),
    ("const", "ENV_MODE"),
    ("const", "ENV_NODES"),
    ("const", "ENV_PASSWORD"),
    ("const", "ENV_PREFIX"),
    ("const", "ENV_SENTINEL_MASTER"),
    ("const", "ENV_TLS"),
    ("const", "ENV_URL"),
    ("const", "ENV_USERNAME"),
    ("const", "ENV_WARMUP"),
    ("fn", "generate_lock_token"),
    ("fn", "lock_token_matches"),
    ("fn", "map_redis_error"),
    ("fn", "map_redis_result"),
    ("fn", "with_retry"),
    ("type", "RedisResult"),
];

mod cover {
    use std::collections::BTreeSet;
    use std::sync::{Mutex, OnceLock};

    static EXECUTED: OnceLock<Mutex<BTreeSet<(&'static str, &'static str)>>> = OnceLock::new();

    fn log() -> &'static Mutex<BTreeSet<(&'static str, &'static str)>> {
        EXECUTED.get_or_init(|| Mutex::new(BTreeSet::new()))
    }

    pub fn hit(kind: &'static str, id: &'static str) {
        assert!(
            super::E2E_MANIFEST
                .iter()
                .any(|(k, i)| *k == kind && *i == id),
            "登记了清单外的公开条目：{kind} {id}"
        );
        log().lock().expect("覆盖登记表锁中毒").insert((kind, id));
    }

    pub fn executed() -> BTreeSet<(&'static str, &'static str)> {
        log().lock().expect("覆盖登记表锁中毒").clone()
    }
}

fn hit(kind: &'static str, id: &'static str) {
    cover::hit(kind, id);
}

fn assert_manifest_wellformed() {
    let mut seen: BTreeSet<(&str, &str)> = BTreeSet::new();
    for (kind, id) in E2E_MANIFEST {
        assert!(
            matches!(*kind, "fn" | "type" | "field" | "const" | "variant"),
            "未知条目类别 {kind}"
        );
        assert!(seen.insert((kind, id)), "清单重复：{kind} {id}");
    }
}

fn assert_coverage_complete() {
    let declared: BTreeSet<(&str, &str)> = E2E_MANIFEST.iter().copied().collect();
    let executed = cover::executed();
    let missing: Vec<_> = declared.difference(&executed).collect();
    let ghost: Vec<_> = executed.difference(&declared).collect();
    assert!(missing.is_empty(), "声明未执行：{missing:?}");
    assert!(ghost.is_empty(), "执行未声明：{ghost:?}");
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("e2e:redisx:{prefix}:{}:{nanos}", std::process::id())
}

fn phase_constants() {
    let pairs = [
        ("ENV_PREFIX", ENV_PREFIX),
        ("ENV_URL", ENV_URL),
        ("ENV_ADDR", ENV_ADDR),
        ("ENV_USERNAME", ENV_USERNAME),
        ("ENV_PASSWORD", ENV_PASSWORD),
        ("ENV_DB", ENV_DB),
        ("ENV_TLS", ENV_TLS),
        ("ENV_MODE", ENV_MODE),
        ("ENV_NODES", ENV_NODES),
        ("ENV_SENTINEL_MASTER", ENV_SENTINEL_MASTER),
        ("ENV_WARMUP", ENV_WARMUP),
        ("ENV_MAX_IN_FLIGHT", ENV_MAX_IN_FLIGHT),
        ("ENV_BLOCKING_TIMEOUT_MS", ENV_BLOCKING_TIMEOUT_MS),
    ];
    for (id, value) in pairs {
        hit("const", id);
        assert!(!value.is_empty(), "{id}");
    }
    assert_eq!(ENV_PREFIX, "FOUNDATIONX_REDISX_");
    assert_eq!(ENV_URL, "REDIS_URL");
}

fn phase_value_types() {
    hit("type", "RedisMode");
    hit("variant", "RedisMode::Standalone");
    hit("variant", "RedisMode::Cluster");
    hit("variant", "RedisMode::Sentinel");
    let _ = [
        RedisMode::Standalone,
        RedisMode::Cluster,
        RedisMode::Sentinel,
    ];

    hit("type", "RedisRetrySafety");
    let _ = [
        RedisRetrySafety::ReadOnly,
        RedisRetrySafety::Idempotent,
        RedisRetrySafety::AmbiguousWrite,
        RedisRetrySafety::NeverAutomatic,
    ];
    hit("variant", "RedisRetrySafety::ReadOnly");
    hit("variant", "RedisRetrySafety::Idempotent");
    hit("variant", "RedisRetrySafety::AmbiguousWrite");
    hit("variant", "RedisRetrySafety::NeverAutomatic");

    hit("type", "RedisAtomicity");
    let _ = [
        RedisAtomicity::SingleCommand,
        RedisAtomicity::MultiKeySingleSlot,
        RedisAtomicity::None,
    ];
    hit("variant", "RedisAtomicity::SingleCommand");
    hit("variant", "RedisAtomicity::MultiKeySingleSlot");
    hit("variant", "RedisAtomicity::None");

    hit("type", "RedisOperation");
    for (op, var) in [
        (RedisOperation::Get, "RedisOperation::Get"),
        (RedisOperation::Set, "RedisOperation::Set"),
        (RedisOperation::Delete, "RedisOperation::Delete"),
        (RedisOperation::Exists, "RedisOperation::Exists"),
        (RedisOperation::Expire, "RedisOperation::Expire"),
        (RedisOperation::Ttl, "RedisOperation::Ttl"),
        (RedisOperation::Mget, "RedisOperation::Mget"),
        (RedisOperation::Mset, "RedisOperation::Mset"),
        (RedisOperation::Incr, "RedisOperation::Incr"),
        (RedisOperation::Publish, "RedisOperation::Publish"),
    ] {
        hit("variant", var);
        let _ = op.retry_safety();
        let _ = op.atomicity();
        let _ = op.allows_automatic_retry();
    }
    hit("fn", "RedisOperation::retry_safety");
    hit("fn", "RedisOperation::atomicity");
    hit("fn", "RedisOperation::allows_automatic_retry");

    hit("type", "TxCmd");
    let set = TxCmd::set("k", b"v".to_vec());
    hit("fn", "TxCmd::set");
    hit("variant", "TxCmd::Set");
    hit("fn", "TxCmd::command_name");
    hit("fn", "TxCmd::key");
    assert_eq!(set.command_name(), "SET");
    assert_eq!(set.key(), "k");
    let del = TxCmd::del("k");
    hit("fn", "TxCmd::del");
    hit("variant", "TxCmd::Del");
    assert_eq!(del.command_name(), "DEL");
    let incr = TxCmd::incr("k");
    hit("fn", "TxCmd::incr");
    hit("variant", "TxCmd::Incr");
    assert_eq!(incr.command_name(), "INCR");

    hit("type", "StreamEntry");
    let entry = StreamEntry {
        id: "1-0".into(),
        fields: vec![("f".into(), b"v".to_vec())],
    };
    hit("field", "StreamEntry::id");
    hit("field", "StreamEntry::fields");
    hit("fn", "StreamEntry::field");
    assert_eq!(entry.id, "1-0");
    assert_eq!(entry.field("f"), Some(b"v".as_slice()));

    hit("type", "RedisHealth");
    let health = RedisHealth {
        endpoint: "127.0.0.1:1".into(),
        latency: Duration::from_millis(1),
        mode: RedisMode::Standalone,
    };
    hit("field", "RedisHealth::endpoint");
    hit("field", "RedisHealth::latency");
    hit("field", "RedisHealth::mode");
    assert_eq!(health.mode, RedisMode::Standalone);

    hit("type", "RedisPoolStats");
    let stats = RedisPoolStats {
        open: 1,
        in_flight: 0,
        waiters: 0,
    };
    hit("field", "RedisPoolStats::open");
    hit("field", "RedisPoolStats::in_flight");
    hit("field", "RedisPoolStats::waiters");
    assert_eq!(stats.open, 1);

    hit("type", "RedisMetricsSnapshot");
    let snap = RedisMetricsSnapshot {
        commands_ok: 0,
        commands_err: 0,
        commands_timeout: 0,
        acquire_timeout: 0,
        rejected_closed: 0,
    };
    hit("field", "RedisMetricsSnapshot::commands_ok");
    hit("field", "RedisMetricsSnapshot::commands_err");
    hit("field", "RedisMetricsSnapshot::commands_timeout");
    hit("field", "RedisMetricsSnapshot::acquire_timeout");
    hit("field", "RedisMetricsSnapshot::rejected_closed");
    assert_eq!(snap.commands_ok, 0);

    hit("type", "RedisPubSubMessage");
    let msg = RedisPubSubMessage {
        channel: bytes::Bytes::from_static(b"c"),
        payload: bytes::Bytes::from_static(b"p"),
    };
    hit("field", "RedisPubSubMessage::channel");
    hit("field", "RedisPubSubMessage::payload");
    assert_eq!(&msg.channel[..], b"c");

    hit("type", "RedisError");
    hit("type", "RedisResult");
    let _: RedisResult<()> = Ok(());
    let errors = [
        RedisError::Config("c".into()),
        RedisError::Connection("n".into()),
        RedisError::Backend("b".into()),
        RedisError::Serialization("s".into()),
        RedisError::Io(IoError::new(ErrorKind::Other, "io")),
        RedisError::Timeout("t".into()),
        RedisError::Unsupported("u".into()),
        RedisError::Transient("tr".into()),
        RedisError::Conflict("cf".into()),
        RedisError::Missing("m".into()),
        RedisError::Internal("i".into()),
    ];
    for (err, var) in errors.iter().zip([
        "RedisError::Config",
        "RedisError::Connection",
        "RedisError::Backend",
        "RedisError::Serialization",
        "RedisError::Io",
        "RedisError::Timeout",
        "RedisError::Unsupported",
        "RedisError::Transient",
        "RedisError::Conflict",
        "RedisError::Missing",
        "RedisError::Internal",
    ]) {
        hit("variant", var);
        let _ = err.is_retryable();
        let _ = err.label();
    }
    hit("fn", "RedisError::is_retryable");
    hit("fn", "RedisError::label");
    assert!(RedisError::Timeout("t".into()).is_retryable());
    assert!(!RedisError::Config("c".into()).is_retryable());

    hit("fn", "generate_lock_token");
    hit("fn", "lock_token_matches");
    let tok = generate_lock_token();
    assert!(lock_token_matches(&tok, &tok));
    assert!(!lock_token_matches(&tok, "nope"));

    let mapped = map_redis_error(redis::RedisError::from((redis::ErrorKind::IoError, "e2e")));
    hit("fn", "map_redis_error");
    assert!(mapped.is_retryable());
    hit("fn", "map_redis_result");
    map_redis_result::<()>(Ok(())).expect("ok");
    map_redis_result::<()>(Err(redis::RedisError::from((
        redis::ErrorKind::IoError,
        "e2e",
    ))))
    .expect_err("err");
}

fn phase_retry() {
    hit("type", "RetryConfig");
    let fixed = RetryConfig::fixed(2, Duration::from_millis(1)).without_jitter();
    hit("fn", "RetryConfig::fixed");
    hit("fn", "RetryConfig::without_jitter");
    let exp = RetryConfig::exponential(3, Duration::from_millis(1), Duration::from_millis(8))
        .with_multiplier(2.0)
        .with_deadline(Duration::from_secs(1));
    hit("fn", "RetryConfig::exponential");
    hit("fn", "RetryConfig::with_multiplier");
    hit("fn", "RetryConfig::with_deadline");
    let _ = fixed.max_attempts();
    let _ = fixed.initial_backoff();
    let _ = fixed.max_backoff();
    let _ = fixed.multiplier();
    let _ = fixed.jitter();
    let _ = exp.deadline();
    let _ = fixed.backoff_for(1);
    let _ = RetryConfig::jittered(Duration::from_millis(4), 1);
    hit("fn", "RetryConfig::max_attempts");
    hit("fn", "RetryConfig::initial_backoff");
    hit("fn", "RetryConfig::max_backoff");
    hit("fn", "RetryConfig::multiplier");
    hit("fn", "RetryConfig::jitter");
    hit("fn", "RetryConfig::deadline");
    hit("fn", "RetryConfig::backoff_for");
    hit("fn", "RetryConfig::jittered");
}

async fn phase_with_retry() {
    hit("fn", "with_retry");
    let cfg = RetryConfig::fixed(2, Duration::from_millis(1)).without_jitter();
    let mut n = 0;
    let out = with_retry(&cfg, "e2e", || {
        n += 1;
        async move {
            if n == 1 {
                Err(RedisError::Timeout("once".into()))
            } else {
                Ok(7_u8)
            }
        }
    })
    .await
    .expect("retry then ok");
    assert_eq!(out, 7);
}

fn phase_config_offline() {
    hit("type", "RedisConfig");
    hit("type", "RedisConfigBuilder");
    let toml = r#"
addr = "127.0.0.1:1"
connect_timeout_ms = 200
command_timeout_ms = 200
acquire_timeout_ms = 200
max_in_flight = 2
"#;
    let cfg = RedisConfig::from_toml(toml).expect("无密码 TOML");
    hit("fn", "RedisConfig::from_toml");
    cfg.validate().expect("loopback 合法");
    hit("fn", "RedisConfig::validate");

    RedisConfig::from_toml("password = \"x\"\naddr = \"127.0.0.1:1\"\n")
        .expect_err("TOML 禁明文密码");

    let url_cfg = RedisConfig::from_url("redis://127.0.0.1:1/0").expect("url");
    hit("fn", "RedisConfig::from_url");
    let _ = url_cfg.validate();

    let built = RedisConfig::builder()
        .addr("127.0.0.1:1")
        .username("default")
        .password("secret")
        .password_from_provider(|| Some("secret2".into()))
        .clear_password()
        .clear_username()
        .db(0)
        .tls(false)
        .mode(RedisMode::Standalone)
        .nodes(Vec::<String>::new())
        .sentinel_master("m")
        .clear_sentinel_master()
        .client_name("e2e")
        .connect_timeout(Duration::from_millis(200))
        .command_timeout(Duration::from_millis(200))
        .acquire_timeout(Duration::from_millis(200))
        .blocking_timeout(Duration::from_millis(200))
        .max_in_flight(2)
        .command_lanes(2)
        .warmup_count(0)
        .max_cluster_redirects(3)
        .reconnect_max_delay(Duration::from_millis(50))
        .tcp_keepalive(Duration::from_secs(30))
        .clear_tcp_keepalive()
        .build()
        .expect("builder");
    hit("fn", "RedisConfig::builder");
    hit("fn", "RedisConfigBuilder::addr");
    hit("fn", "RedisConfigBuilder::username");
    hit("fn", "RedisConfigBuilder::password");
    hit("fn", "RedisConfigBuilder::password_from_provider");
    hit("fn", "RedisConfigBuilder::clear_password");
    hit("fn", "RedisConfigBuilder::clear_username");
    hit("fn", "RedisConfigBuilder::db");
    hit("fn", "RedisConfigBuilder::tls");
    hit("fn", "RedisConfigBuilder::mode");
    hit("fn", "RedisConfigBuilder::nodes");
    hit("fn", "RedisConfigBuilder::sentinel_master");
    hit("fn", "RedisConfigBuilder::clear_sentinel_master");
    hit("fn", "RedisConfigBuilder::client_name");
    hit("fn", "RedisConfigBuilder::connect_timeout");
    hit("fn", "RedisConfigBuilder::command_timeout");
    hit("fn", "RedisConfigBuilder::acquire_timeout");
    hit("fn", "RedisConfigBuilder::blocking_timeout");
    hit("fn", "RedisConfigBuilder::max_in_flight");
    hit("fn", "RedisConfigBuilder::command_lanes");
    hit("fn", "RedisConfigBuilder::warmup_count");
    hit("fn", "RedisConfigBuilder::max_cluster_redirects");
    hit("fn", "RedisConfigBuilder::reconnect_max_delay");
    hit("fn", "RedisConfigBuilder::tcp_keepalive");
    hit("fn", "RedisConfigBuilder::clear_tcp_keepalive");
    hit("fn", "RedisConfigBuilder::build");

    let _ = built.addr();
    let _ = built.username();
    let _ = built.db();
    let _ = built.tls();
    let _ = built.mode();
    let _ = built.nodes();
    let _ = built.sentinel_master();
    let _ = built.client_name();
    let _ = built.connect_timeout();
    let _ = built.command_timeout();
    let _ = built.acquire_timeout();
    let _ = built.blocking_timeout();
    let _ = built.max_in_flight();
    let _ = built.command_lanes();
    let _ = built.warmup_count();
    let _ = built.max_cluster_redirects();
    let _ = built.reconnect_max_delay();
    let _ = built.tcp_keepalive();
    let _ = built.has_password();
    let _ = built.display_endpoint();
    hit("fn", "RedisConfig::addr");
    hit("fn", "RedisConfig::username");
    hit("fn", "RedisConfig::db");
    hit("fn", "RedisConfig::tls");
    hit("fn", "RedisConfig::mode");
    hit("fn", "RedisConfig::nodes");
    hit("fn", "RedisConfig::sentinel_master");
    hit("fn", "RedisConfig::client_name");
    hit("fn", "RedisConfig::connect_timeout");
    hit("fn", "RedisConfig::command_timeout");
    hit("fn", "RedisConfig::acquire_timeout");
    hit("fn", "RedisConfig::blocking_timeout");
    hit("fn", "RedisConfig::max_in_flight");
    hit("fn", "RedisConfig::command_lanes");
    hit("fn", "RedisConfig::warmup_count");
    hit("fn", "RedisConfig::max_cluster_redirects");
    hit("fn", "RedisConfig::reconnect_max_delay");
    hit("fn", "RedisConfig::tcp_keepalive");
    hit("fn", "RedisConfig::has_password");
    hit("fn", "RedisConfig::display_endpoint");

    let again = built.to_builder().build().expect("to_builder");
    hit("fn", "RedisConfig::to_builder");
    let _ = again.display_endpoint();

    RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .db(1)
        .build()
        .expect_err("cluster 非 0 库");
    RedisConfig::builder()
        .mode(RedisMode::Sentinel)
        .build()
        .expect_err("sentinel 缺 master");
}

async fn phase_offline_connect() {
    let cfg = RedisConfig::builder()
        .addr("127.0.0.1:1")
        .connect_timeout(Duration::from_millis(200))
        .command_timeout(Duration::from_millis(200))
        .acquire_timeout(Duration::from_millis(200))
        .warmup_count(0)
        .build()
        .expect("cfg");

    hit("type", "RedisPool");
    hit("type", "RedisClient");
    hit("type", "RedisPoolPermit");
    hit("type", "RedisPubSub");
    hit("type", "RedisLock");

    let started = Instant::now();
    let err = RedisPool::connect(cfg.clone()).await.expect_err("不可达");
    hit("fn", "RedisPool::connect");
    assert!(err.is_retryable(), "{err:?}");
    assert!(started.elapsed() < Duration::from_secs(10));

    RedisClient::connect(cfg.clone())
        .await
        .expect_err("client connect 不可达");
    hit("fn", "RedisClient::connect");
    RedisClient::connect_url("redis://127.0.0.1:1/0")
        .await
        .expect_err("connect_url");
    hit("fn", "RedisClient::connect_url");

    let pool = RedisPool::new(cfg.clone()).expect("new 不联网");
    hit("fn", "RedisPool::new");
    let client = RedisClient::new(cfg).expect("client new");
    hit("fn", "RedisClient::new");
    let _ = client.pool();
    let _ = client.config();
    let _ = client.endpoint();
    hit("fn", "RedisClient::pool");
    hit("fn", "RedisClient::config");
    hit("fn", "RedisClient::endpoint");
    let client = client
        .with_retry(RetryConfig::fixed(1, Duration::from_millis(1)))
        .with_call_deadline(Duration::from_secs(1));
    hit("fn", "RedisClient::with_retry");
    hit("fn", "RedisClient::with_call_deadline");
    assert!(client.retry_config().is_some());
    assert!(client.has_call_deadline());
    hit("fn", "RedisClient::retry_config");
    hit("fn", "RedisClient::has_call_deadline");

    let _ = pool.config();
    let _ = pool.endpoint();
    let _ = pool.command_timeout();
    let _ = pool.command_lanes();
    let _ = pool.reconnect_max_delay();
    let _ = pool.tcp_keepalive();
    let _ = pool.is_closed();
    let _ = pool.liveness();
    let _ = pool.stats();
    let _ = pool.metrics_snapshot();
    hit("fn", "RedisPool::config");
    hit("fn", "RedisPool::endpoint");
    hit("fn", "RedisPool::command_timeout");
    hit("fn", "RedisPool::command_lanes");
    hit("fn", "RedisPool::reconnect_max_delay");
    hit("fn", "RedisPool::tcp_keepalive");
    hit("fn", "RedisPool::is_closed");
    hit("fn", "RedisPool::liveness");
    hit("fn", "RedisPool::stats");
    hit("fn", "RedisPool::metrics_snapshot");
    hit("fn", "RedisPool::client");
    let _ = pool.client();

    pool.ping().await.expect_err("未连接 ping");
    hit("fn", "RedisPool::ping");
    pool.get("k").await.expect_err("未连接 get");
    hit("fn", "RedisPool::get");
    pool.set("k", b"v".to_vec()).await.expect_err("未连接 set");
    hit("fn", "RedisPool::set");
    pool.set_ex("k", b"v".to_vec(), Duration::from_secs(1))
        .await
        .expect_err("未连接 set_ex");
    hit("fn", "RedisPool::set_ex");
    pool.del("k").await.expect_err("未连接 del");
    hit("fn", "RedisPool::del");
    pool.exists("k").await.expect_err("未连接 exists");
    hit("fn", "RedisPool::exists");
    pool.expire("k", Duration::from_secs(1))
        .await
        .expect_err("未连接 expire");
    hit("fn", "RedisPool::expire");
    pool.ttl("k").await.expect_err("未连接 ttl");
    hit("fn", "RedisPool::ttl");
    pool.incr("k", 1).await.expect_err("未连接 incr");
    hit("fn", "RedisPool::incr");
    pool.health_check().await.expect_err("未连接 health");
    hit("fn", "RedisPool::health_check");
    pool.readiness().await.expect_err("未连接 ready");
    hit("fn", "RedisPool::readiness");
    pool.acquire().await.expect_err("未连接 acquire");
    hit("fn", "RedisPool::acquire");

    let cluster = RedisConfig::builder()
        .mode(RedisMode::Cluster)
        .nodes(vec!["127.0.0.1:1".to_string()])
        .db(0)
        .connect_timeout(Duration::from_millis(200))
        .build()
        .expect("cluster cfg");
    RedisPubSub::connect_config(cluster, ["e2e".into()])
        .await
        .expect_err("cluster pubsub fail-closed");
    hit("fn", "RedisPubSub::connect_config");

    pool.close(Duration::from_millis(200)).await.expect("close");
    hit("fn", "RedisPool::close");
}

#[tokio::test]
async fn offline_config_and_unreachable_connect() {
    assert_manifest_wellformed();
    phase_constants();
    phase_value_types();
    phase_retry();
    phase_with_retry().await;
    phase_config_offline();
    phase_offline_connect().await;
}

/// 完整真连：002 入口 + 公开面其余命令。键名唯一化并删除。
#[tokio::test]
#[ignore = "需要真实 Redis 与 FOUNDATIONX_REDISX_* / REDIS_URL"]
async fn e2e_live_full_journey() {
    assert_manifest_wellformed();
    phase_constants();
    phase_value_types();
    phase_retry();
    phase_with_retry().await;
    phase_config_offline();
    phase_offline_connect().await;

    let cfg = RedisConfig::from_env().expect("已注入 FOUNDATIONX_REDISX_* 或 REDIS_URL");
    hit("fn", "RedisConfig::from_env");
    cfg.validate().expect("from_env 可校验");

    let pool = RedisPool::connect(cfg.clone()).await.expect("pool connect");
    assert!(pool.liveness());
    pool.ping().await.expect("pool ping");
    let health = pool.health_check().await.expect("health");
    assert_eq!(health.mode, cfg.mode());
    pool.readiness().await.expect("ready");

    let prefix = unique("k");
    let kv = format!("{prefix}:kv");
    let hash = format!("{prefix}:h");
    let list = format!("{prefix}:l");
    let setk = format!("{prefix}:s");
    let zset = format!("{prefix}:z");
    let stream = format!("{prefix}:x");
    let lockk = format!("{prefix}:lock");
    let incrk = format!("{prefix}:i");
    let pipe_a = format!("{prefix}:p1");
    let pipe_b = format!("{prefix}:p2");
    let txk = format!("{prefix}:tx");
    let channel = unique("ch").replace(':', ".");

    pool.set(&kv, b"v1".to_vec()).await.expect("pool set");
    assert_eq!(
        pool.get(&kv).await.expect("pool get").as_deref(),
        Some(b"v1".as_slice())
    );
    pool.set_ex(&kv, b"v2".to_vec(), Duration::from_secs(30))
        .await
        .expect("pool set_ex");
    pool.expire(&kv, Duration::from_secs(30))
        .await
        .expect("expire");
    let _ = pool.ttl(&kv).await.expect("ttl");
    assert!(pool.exists(&kv).await.expect("exists"));
    pool.incr(&incrk, 1).await.expect("pool incr");

    let permit = pool.acquire().await.expect("acquire");
    hit("type", "RedisPoolPermit");
    permit.ping().await.expect("permit ping");
    hit("fn", "RedisPoolPermit::ping");
    permit.set(&kv, b"v3".to_vec()).await.expect("permit set");
    hit("fn", "RedisPoolPermit::set");
    permit
        .set_ex(&kv, b"v4".to_vec(), Duration::from_secs(30))
        .await
        .expect("permit set_ex");
    hit("fn", "RedisPoolPermit::set_ex");
    let _ = permit.get(&kv).await.expect("permit get");
    hit("fn", "RedisPoolPermit::get");
    permit
        .expire(&kv, Duration::from_secs(30))
        .await
        .expect("p exp");
    hit("fn", "RedisPoolPermit::expire");
    let _ = permit.ttl(&kv).await.expect("p ttl");
    hit("fn", "RedisPoolPermit::ttl");
    let _ = permit.exists(&kv).await.expect("p exists");
    hit("fn", "RedisPoolPermit::exists");
    permit.incr(&incrk, 1).await.expect("p incr");
    hit("fn", "RedisPoolPermit::incr");
    let _ = permit.command_timeout();
    let _ = permit.endpoint();
    hit("fn", "RedisPoolPermit::command_timeout");
    hit("fn", "RedisPoolPermit::endpoint");
    let gone = format!("{prefix}:gone");
    permit
        .set(&gone, b"x".to_vec())
        .await
        .expect("permit set gone");
    assert!(permit.del(&gone).await.expect("permit del"));
    assert!(!permit.exists(&gone).await.expect("permit gone gone"));
    hit("fn", "RedisPoolPermit::del");
    drop(permit);

    let client = RedisPool::connect_from_env()
        .await
        .expect("pool from_env")
        .client();
    hit("fn", "RedisPool::connect_from_env");
    let client2 = RedisClient::connect_from_env()
        .await
        .expect("client from_env");
    hit("fn", "RedisClient::connect_from_env");
    client2.ping().await.expect("c2 ping");
    drop(client2);

    client.ping().await.expect("client ping");
    hit("fn", "RedisClient::ping");
    client.health_check().await.expect("client health");
    hit("fn", "RedisClient::health_check");
    client.set(&kv, b"v5".to_vec()).await.expect("set");
    hit("fn", "RedisClient::set");
    assert_eq!(
        client.get(&kv).await.expect("get after set").as_deref(),
        Some(b"v5".as_slice())
    );
    hit("fn", "RedisClient::get");
    client
        .set_bytes(&kv, b"v6".to_vec())
        .await
        .expect("set_bytes");
    hit("fn", "RedisClient::set_bytes");
    assert_eq!(
        client.get_bytes(&kv).await.expect("get_bytes").as_deref(),
        Some(b"v6".as_slice())
    );
    hit("fn", "RedisClient::get_bytes");
    client
        .set_ex(&kv, b"v7".to_vec(), Duration::from_secs(30))
        .await
        .expect("set_ex");
    hit("fn", "RedisClient::set_ex");
    assert_eq!(
        client.get(&kv).await.expect("get after set_ex").as_deref(),
        Some(b"v7".as_slice())
    );
    client.mset(&[(&kv, b"v8".as_slice())]).await.expect("mset");
    hit("fn", "RedisClient::mset");
    let mget = client.mget(&[&kv]).await.expect("mget");
    hit("fn", "RedisClient::mget");
    assert_eq!(mget[0].as_deref(), Some(b"v8".as_slice()));
    assert!(client.exists(&kv).await.expect("exists"));
    hit("fn", "RedisClient::exists");
    client
        .expire(&kv, Duration::from_secs(30))
        .await
        .expect("expire");
    hit("fn", "RedisClient::expire");
    let _ = client.ttl(&kv).await.expect("ttl");
    hit("fn", "RedisClient::ttl");
    assert_eq!(client.incr(&incrk, 1).await.expect("incr"), 3);
    hit("fn", "RedisClient::incr");

    assert!(client.hset(&hash, "f", b"1".to_vec()).await.expect("hset"));
    hit("fn", "RedisClient::hset");
    assert_eq!(
        client.hget(&hash, "f").await.expect("hget").as_deref(),
        Some(b"1".as_slice())
    );
    hit("fn", "RedisClient::hget");
    assert!(!client.hgetall(&hash).await.expect("hgetall").is_empty());
    hit("fn", "RedisClient::hgetall");
    assert_eq!(client.hdel(&hash, &["f"]).await.expect("hdel"), 1);
    hit("fn", "RedisClient::hdel");

    client.lpush(&list, b"a".to_vec()).await.expect("lpush");
    hit("fn", "RedisClient::lpush");
    client.rpush(&list, b"b".to_vec()).await.expect("rpush");
    hit("fn", "RedisClient::rpush");
    let _ = client.lrange(&list, 0, -1).await.expect("lrange");
    hit("fn", "RedisClient::lrange");
    let _ = client.lpop(&list).await.expect("lpop");
    hit("fn", "RedisClient::lpop");
    let _ = client
        .blpop(&list, Duration::from_millis(50))
        .await
        .expect("blpop");
    hit("fn", "RedisClient::blpop");

    assert_eq!(client.sadd(&setk, b"m".to_vec()).await.expect("sadd"), 1);
    hit("fn", "RedisClient::sadd");
    assert!(client.sismember(&setk, b"m").await.expect("sismember"));
    hit("fn", "RedisClient::sismember");
    assert_eq!(client.srem(&setk, b"m").await.expect("srem"), 1);
    hit("fn", "RedisClient::srem");
    assert!(!client
        .sismember(&setk, b"m")
        .await
        .expect("sismember after srem"));

    assert_eq!(
        client.zadd(&zset, b"m".to_vec(), 1.0).await.expect("zadd"),
        1
    );
    hit("fn", "RedisClient::zadd");
    assert_eq!(client.zscore(&zset, b"m").await.expect("zscore"), Some(1.0));
    hit("fn", "RedisClient::zscore");
    assert_eq!(client.zrem(&zset, b"m").await.expect("zrem"), 1);
    hit("fn", "RedisClient::zrem");

    let sid = client
        .xadd(&stream, &[("f", b"1".as_slice())])
        .await
        .expect("xadd");
    hit("fn", "RedisClient::xadd");
    client
        .xadd_with_id(&stream, "*", &[("f", b"2".as_slice())])
        .await
        .expect("xadd_id");
    hit("fn", "RedisClient::xadd_with_id");
    assert!(client.xlen(&stream).await.expect("xlen") >= 2);
    hit("fn", "RedisClient::xlen");
    assert!(!client
        .xrange(&stream, "-", "+", None)
        .await
        .expect("xrange")
        .is_empty());
    hit("fn", "RedisClient::xrange");
    assert!(!client
        .xread(&stream, "0-0", None)
        .await
        .expect("xread")
        .is_empty());
    hit("fn", "RedisClient::xread");
    assert!(client
        .xread_block(&stream, "$", Duration::from_millis(50), None)
        .await
        .expect("xread_block")
        .is_empty());
    hit("fn", "RedisClient::xread_block");
    assert_eq!(
        client
            .xack(&stream, "e2e-no-group", &[&sid])
            .await
            .expect("无组 XACK 仍返回计数"),
        0,
        "无消费组不得当成已 ack"
    );
    hit("fn", "RedisClient::xack");
    assert_eq!(client.xdel(&stream, &[&sid]).await.expect("xdel"), 1);
    hit("fn", "RedisClient::xdel");

    client
        .pipeline_set(&[(&pipe_a, b"1".to_vec()), (&pipe_b, b"2".to_vec())], None)
        .await
        .expect("pipeline");
    hit("fn", "RedisClient::pipeline_set");
    assert_eq!(
        client.get(&pipe_a).await.expect("pipe get").as_deref(),
        Some(b"1".as_slice())
    );
    client
        .multi_set(&[(&txk, b"t".as_slice())])
        .await
        .expect("multi_set");
    hit("fn", "RedisClient::multi_set");
    assert_eq!(
        client.get(&txk).await.expect("multi_set get").as_deref(),
        Some(b"t".as_slice())
    );
    client
        .multi_exec(&[TxCmd::set(&txk, b"t2".to_vec()), TxCmd::incr(&incrk)])
        .await
        .expect("multi_exec");
    hit("fn", "RedisClient::multi_exec");
    assert_eq!(
        client.get(&txk).await.expect("multi_exec get").as_deref(),
        Some(b"t2".as_slice())
    );

    let pong = client
        .eval_script("return redis.call('PING')", &[], &[])
        .await
        .expect("eval");
    hit("fn", "RedisClient::eval_script");
    let pong_dbg = format!("{pong:?}").to_ascii_lowercase();
    assert!(pong_dbg.contains("pong"), "{pong:?}");
    let (sha, loaded) = client
        .script_load_and_eval("return 1", &[], &[])
        .await
        .expect("load");
    hit("fn", "RedisClient::script_load_and_eval");
    assert!(matches!(loaded, redis::Value::Int(1)));
    let again = client.eval_sha(&sha, &[], &[]).await.expect("eval_sha");
    hit("fn", "RedisClient::eval_sha");
    assert!(matches!(again, redis::Value::Int(1)));

    let lock = client
        .lock_acquire(&lockk, Duration::from_secs(15))
        .await
        .expect("lock");
    hit("fn", "RedisClient::lock_acquire");
    hit("type", "RedisLock");
    assert_eq!(lock.key(), lockk);
    assert!(!lock.token().is_empty());
    assert!(lock.fence() >= 1);
    assert!(lock.verify_token(lock.token()));
    hit("fn", "RedisLock::key");
    hit("fn", "RedisLock::token");
    hit("fn", "RedisLock::fence");
    hit("fn", "RedisLock::verify_token");
    assert!(client.exists(&lockk).await.expect("lock key"));
    assert!(client
        .lock_extend(&lock, Duration::from_secs(15))
        .await
        .expect("extend"));
    hit("fn", "RedisClient::lock_extend");
    assert!(client.lock_release(&lock).await.expect("release"));
    hit("fn", "RedisClient::lock_release");
    assert!(!client.exists(&lockk).await.expect("lock released"));
    assert!(!client.lock_release(&lock).await.expect("second release"));
    let fence_key = format!("fence:{lockk}");
    assert!(client.del(&fence_key).await.expect("del fence"));

    let sub = pool.subscribe([channel.clone()]).await.expect("subscribe");
    hit("fn", "RedisPool::subscribe");
    let _ = sub.endpoint();
    hit("fn", "RedisPubSub::endpoint");
    let mut sub_stream = sub.into_message_stream().expect("message stream");
    hit("fn", "RedisPubSub::into_message_stream");
    let publisher = RedisPubSub::connect_config(cfg.clone(), [format!("{channel}.ctl")])
        .await
        .expect("publisher");
    publisher.publish(&channel, b"hi").await.expect("publish");
    hit("fn", "RedisPubSub::publish");
    let msg = tokio::time::timeout(Duration::from_secs(5), sub_stream.next())
        .await
        .expect("pubsub 超时")
        .expect("pubsub 无消息");
    assert_eq!(&msg.payload[..], b"hi");
    assert_eq!(&msg.channel[..], channel.as_bytes());

    let ch2 = format!("{channel}.2");
    let sub2 = RedisPubSub::connect_config(cfg.clone(), [ch2.clone()])
        .await
        .expect("pubsub connect");
    let mut rstream = sub2.into_result_message_stream().expect("result stream");
    hit("fn", "RedisPubSub::into_result_message_stream");
    publisher.publish(&ch2, b"hi2").await.expect("publish2");
    let msg2 = tokio::time::timeout(Duration::from_secs(5), rstream.next())
        .await
        .expect("result stream 超时")
        .expect("result stream 无消息")
        .expect("result stream err");
    assert_eq!(&msg2.payload[..], b"hi2");

    let leftover = [
        kv.as_str(),
        hash.as_str(),
        list.as_str(),
        setk.as_str(),
        zset.as_str(),
        stream.as_str(),
        lockk.as_str(),
        incrk.as_str(),
        pipe_a.as_str(),
        pipe_b.as_str(),
        txk.as_str(),
        fence_key.as_str(),
    ];
    for key in leftover {
        let _ = client.del(key).await;
    }
    hit("fn", "RedisClient::del");
    assert!(!client.exists(&kv).await.expect("kv deleted"));

    pool.close(Duration::from_secs(2)).await.expect("close");
    assert!(pool.is_closed());
    pool.ping().await.expect_err("close 后拒");

    assert_coverage_complete();
}
