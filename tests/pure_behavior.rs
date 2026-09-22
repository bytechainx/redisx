#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 纯函数行为集成测试：错误映射与可重试判定、重试退避计算、锁令牌生成/校验、
//! `TxCmd` 构造、Stream 条目查询、重试安全分类。

use std::time::Duration;

use redisx::{
    generate_lock_token, lock_token_matches, map_redis_error, map_redis_result, with_retry,
    RedisAtomicity, RedisError, RedisOperation, RedisRetrySafety, RetryConfig, StreamEntry, TxCmd,
};

fn mapped(kind: redis::ErrorKind, message: &'static str) -> RedisError {
    map_redis_error(redis::RedisError::from((kind, message)))
}

#[test]
fn error_mapping_classifies_and_marks_retryability() {
    // 可重试：连接类 / I/O / 瞬时故障 / 超时
    for (kind, message) in [
        (redis::ErrorKind::IoError, "broken pipe"),
        (redis::ErrorKind::ClusterDown, "CLUSTERDOWN"),
        (redis::ErrorKind::MasterDown, "MASTERDOWN"),
        (redis::ErrorKind::ReadOnly, "READONLY"),
        (redis::ErrorKind::EmptySentinelList, "no sentinels"),
        (redis::ErrorKind::BusyLoadingError, "LOADING"),
        (redis::ErrorKind::TryAgain, "TRYAGAIN"),
        (redis::ErrorKind::Moved, "MOVED 3999 127.0.0.1:7001"),
        (redis::ErrorKind::Ask, "ASK 3999 127.0.0.1:7001"),
        (redis::ErrorKind::CrossSlot, "CROSSSLOT"),
    ] {
        let err = mapped(kind, message);
        assert!(err.is_retryable(), "kind={kind:?} err={err}");
    }

    // 不可重试：配置 / 认证 / 冲突 / 缺失 / 内部
    for (kind, message) in [
        (redis::ErrorKind::InvalidClientConfig, "bad config"),
        (redis::ErrorKind::ClientError, "client error"),
        (redis::ErrorKind::AuthenticationFailed, "WRONGPASS"),
        (redis::ErrorKind::ExecAbortError, "EXECABORT"),
        (redis::ErrorKind::NoScriptError, "NOSCRIPT"),
        (redis::ErrorKind::TypeError, "WRONGTYPE"),
    ] {
        let err = mapped(kind, message);
        assert!(!err.is_retryable(), "kind={kind:?} err={err}");
    }

    // 文本细分：LOADING/TRYAGAIN 可重试，NOAUTH/WRONGPASS 不可重试，READONLY 归连接类
    assert!(mapped(redis::ErrorKind::ResponseError, "LOADING Redis is loading").is_retryable());
    assert!(mapped(redis::ErrorKind::ResponseError, "TRYAGAIN later").is_retryable());
    assert!(!mapped(
        redis::ErrorKind::ResponseError,
        "NOAUTH Authentication required"
    )
    .is_retryable());
    assert!(!mapped(redis::ErrorKind::ResponseError, "WRONGPASS invalid").is_retryable());
    assert!(mapped(redis::ErrorKind::ResponseError, "READONLY You can't write").is_retryable());

    // 分类标签稳定，便于低基数打点
    assert_eq!(
        mapped(redis::ErrorKind::IoError, "io").label(),
        "connection"
    );
    assert_eq!(
        mapped(redis::ErrorKind::ClusterDown, "down").label(),
        "connection"
    );
    assert_eq!(
        mapped(redis::ErrorKind::TryAgain, "retry").label(),
        "transient"
    );
    assert_eq!(
        mapped(redis::ErrorKind::Moved, "moved").label(),
        "transient"
    );
    assert_eq!(
        mapped(redis::ErrorKind::InvalidClientConfig, "cfg").label(),
        "config"
    );
    assert_eq!(
        mapped(redis::ErrorKind::AuthenticationFailed, "auth").label(),
        "backend"
    );
    assert_eq!(
        mapped(redis::ErrorKind::ExecAbortError, "abort").label(),
        "conflict"
    );
    assert_eq!(
        mapped(redis::ErrorKind::NoScriptError, "noscript").label(),
        "missing"
    );
    assert_eq!(
        mapped(redis::ErrorKind::TypeError, "type").label(),
        "internal"
    );

    // Nothing 泄漏到 Ok 路径
    assert_eq!(map_redis_result(Ok(7usize)).expect("ok"), 7);
    let err = map_redis_result::<()>(Err(redis::RedisError::from((
        redis::ErrorKind::IoError,
        "io",
    ))))
    .expect_err("err");
    assert!(err.is_retryable());
}

#[test]
fn retry_backoff_is_exponential_capped_and_deterministic() {
    let config = RetryConfig::exponential(5, Duration::from_millis(10), Duration::from_millis(80));
    assert_eq!(config.max_attempts(), 5);
    assert_eq!(config.backoff_for(0), Duration::ZERO);
    assert_eq!(config.backoff_for(1), Duration::from_millis(10));
    assert_eq!(config.backoff_for(2), Duration::from_millis(20));
    assert_eq!(config.backoff_for(3), Duration::from_millis(40));
    assert_eq!(config.backoff_for(4), Duration::from_millis(80));
    assert_eq!(
        config.backoff_for(u32::MAX),
        Duration::from_millis(80),
        "退避必须封顶"
    );

    // 固定间隔策略
    let fixed = RetryConfig::fixed(3, Duration::from_millis(25));
    assert_eq!(fixed.multiplier(), 1.0);
    assert_eq!(fixed.backoff_for(1), Duration::from_millis(25));
    assert_eq!(fixed.backoff_for(10), Duration::from_millis(25));

    // 自定义倍率与非法倍率回落
    let custom = RetryConfig::exponential(4, Duration::from_millis(10), Duration::from_secs(30))
        .with_multiplier(3.0);
    assert_eq!(custom.backoff_for(2), Duration::from_millis(30));
    assert_eq!(custom.backoff_for(3), Duration::from_millis(90));
    for bad in [0.0_f64, -2.0, f64::NAN, f64::INFINITY] {
        assert_eq!(
            custom.clone().with_multiplier(bad).multiplier(),
            1.0,
            "bad={bad}"
        );
    }

    // 抖动可复现、不放大退避、下界 0.5×
    let backoff = Duration::from_millis(1000);
    for seed in [0_u64, 1, 42, u64::MAX] {
        let first = RetryConfig::jittered(backoff, seed);
        assert_eq!(first, RetryConfig::jittered(backoff, seed));
        assert!(first <= backoff, "抖动不得放大退避: {first:?}");
        assert!(
            first >= Duration::from_millis(500),
            "抖动下界 0.5×: {first:?}"
        );
    }
    assert_ne!(
        RetryConfig::jittered(backoff, 1),
        RetryConfig::jittered(backoff, 2)
    );

    // accessor 与默认值
    let default = RetryConfig::default();
    assert_eq!(default.max_attempts(), 3);
    assert_eq!(default.initial_backoff(), Duration::from_millis(50));
    assert_eq!(default.max_backoff(), Duration::from_secs(2));
    assert!(default.jitter());
    assert!(default.deadline().is_none());
    assert_eq!(
        default
            .clone()
            .with_deadline(Duration::from_secs(1))
            .deadline(),
        Some(Duration::from_secs(1))
    );
    assert!(!default.without_jitter().jitter());
}

#[tokio::test]
async fn retry_only_retries_retryable_errors_within_budget() {
    let config = RetryConfig::fixed(3, Duration::from_millis(1)).without_jitter();

    // 可重试错误会重试到成功
    let mut attempts = 0;
    let value = with_retry(&config, "test.retry", || {
        attempts += 1;
        let current = attempts;
        async move {
            if current < 3 {
                Err(RedisError::Transient("loading".to_owned()))
            } else {
                Ok(current)
            }
        }
    })
    .await
    .expect("第三次成功");
    assert_eq!(value, 3);

    // 不可重试错误只执行一次
    let mut calls = 0;
    let err = with_retry::<(), _, _>(&config, "test.invalid", || {
        calls += 1;
        async { Err(RedisError::Config("bad".to_owned())) }
    })
    .await
    .expect_err("不可重试");
    assert!(matches!(err, RedisError::Config(_)));
    assert_eq!(calls, 1);

    // 预算耗尽返回最后一次错误
    let mut exhausted = 0;
    let err = with_retry::<(), _, _>(&config, "test.exhaust", || {
        exhausted += 1;
        async { Err(RedisError::Connection("refused".to_owned())) }
    })
    .await
    .expect_err("耗尽");
    assert!(matches!(err, RedisError::Connection(_)));
    assert_eq!(exhausted, 3);

    // deadline 约束总耗时
    let bounded = RetryConfig::fixed(50, Duration::from_millis(30))
        .without_jitter()
        .with_deadline(Duration::from_millis(40));
    let mut bounded_calls = 0;
    let err = with_retry::<(), _, _>(&bounded, "test.deadline", || {
        bounded_calls += 1;
        async { Err(RedisError::Timeout("slow".to_owned())) }
    })
    .await
    .expect_err("deadline 耗尽");
    assert!(matches!(err, RedisError::Timeout(_)));
    assert!(bounded_calls <= 3, "calls={bounded_calls}");
}

#[test]
fn retry_safety_and_atomicity_contracts() {
    let read_only = [
        RedisOperation::Get,
        RedisOperation::Exists,
        RedisOperation::Ttl,
        RedisOperation::Mget,
    ];
    for operation in read_only {
        assert_eq!(
            operation.retry_safety(),
            RedisRetrySafety::ReadOnly,
            "op={operation:?}"
        );
        assert!(operation.allows_automatic_retry(), "op={operation:?}");
    }
    for operation in [
        RedisOperation::Get,
        RedisOperation::Exists,
        RedisOperation::Ttl,
    ] {
        assert_eq!(
            operation.atomicity(),
            RedisAtomicity::SingleCommand,
            "op={operation:?}"
        );
    }
    assert_eq!(
        RedisOperation::Mget.atomicity(),
        RedisAtomicity::MultiKeySingleSlot
    );
    assert_eq!(
        RedisOperation::Mset.atomicity(),
        RedisAtomicity::MultiKeySingleSlot
    );
    // MSET 与 SET 同为固定值写入：分类统一为 AmbiguousWrite（不自动重试）；
    // atomicity 仍为 MultiKeySingleSlot，故不并入下方 SingleCommand 列表
    assert_eq!(
        RedisOperation::Mset.retry_safety(),
        RedisRetrySafety::AmbiguousWrite
    );
    assert!(!RedisOperation::Mset.allows_automatic_retry());

    for operation in [
        RedisOperation::Set,
        RedisOperation::Delete,
        RedisOperation::Expire,
    ] {
        assert_eq!(
            operation.retry_safety(),
            RedisRetrySafety::AmbiguousWrite,
            "op={operation:?}"
        );
        assert!(!operation.allows_automatic_retry(), "op={operation:?}");
        assert_eq!(operation.atomicity(), RedisAtomicity::SingleCommand);
    }
    for operation in [RedisOperation::Incr, RedisOperation::Publish] {
        assert_eq!(
            operation.retry_safety(),
            RedisRetrySafety::NeverAutomatic,
            "op={operation:?}"
        );
        assert!(!operation.allows_automatic_retry(), "op={operation:?}");
    }
    assert_eq!(RedisOperation::Publish.atomicity(), RedisAtomicity::None);
    assert_eq!(
        RedisOperation::Incr.atomicity(),
        RedisAtomicity::SingleCommand
    );
}

#[test]
fn transaction_command_construction() {
    let set = TxCmd::set("k", b"v".to_vec());
    assert_eq!(
        set,
        TxCmd::Set {
            key: "k".to_owned(),
            value: b"v".to_vec()
        }
    );
    assert_eq!(set.command_name(), "SET");
    assert_eq!(set.key(), "k");

    let del = TxCmd::del("gone");
    assert_eq!(
        del,
        TxCmd::Del {
            key: "gone".to_owned()
        }
    );
    assert_eq!(del.command_name(), "DEL");
    assert_eq!(del.key(), "gone");

    let incr = TxCmd::incr("counter");
    assert_eq!(
        incr,
        TxCmd::Incr {
            key: "counter".to_owned()
        }
    );
    assert_eq!(incr.command_name(), "INCR");
    assert_eq!(incr.key(), "counter");

    // 不可变借用后再 clone，说明描述结构是值语义
    let batch = [set.clone(), del, incr];
    let cloned = batch.to_vec();
    assert_eq!(cloned, batch);
    assert_eq!(
        batch.iter().map(TxCmd::command_name).collect::<Vec<_>>(),
        ["SET", "DEL", "INCR"]
    );
}

#[test]
fn lock_tokens_are_generated_and_verified() {
    let first = generate_lock_token();
    let second = generate_lock_token();
    assert!(first.starts_with("lk-"));
    assert!(first.contains('-'));
    assert_ne!(first, second, "令牌必须唯一");
    assert!(first.len() > 8);

    // 常量时间校验：相等 / 不等 / 截断 / 加长 / 空串
    assert!(lock_token_matches(&first, &first));
    assert!(!lock_token_matches(&first, &second));
    assert!(!lock_token_matches(&first, ""));
    assert!(
        !lock_token_matches(&first, &first[..first.len() - 1]),
        "截断令牌必须失败"
    );
    assert!(
        !lock_token_matches(&first, &format!("{first}x")),
        "加长令牌必须失败"
    );
    assert!(lock_token_matches("", ""));
}

#[test]
fn stream_entry_field_lookup() {
    let entry = StreamEntry {
        id: "1700000000000-0".to_owned(),
        fields: vec![
            ("kind".to_owned(), b"trade".to_vec()),
            ("payload".to_owned(), vec![0, 1, 255]),
        ],
    };
    assert_eq!(entry.id, "1700000000000-0");
    assert_eq!(entry.field("kind"), Some(b"trade".as_slice()));
    assert_eq!(entry.field("payload"), Some([0_u8, 1, 255].as_slice()));
    assert_eq!(entry.field("missing"), None);
}
