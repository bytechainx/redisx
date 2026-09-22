#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! live 真连服端到端测试（redisx，覆盖全部公开接口）。
//!
//! 全部用例 `#[ignore]`，默认不跑（CI 行为不变）。本地显式运行：
//!
//! ```bash
//! set -a; source /home/workspace/sre/secrets/env/redisx.env; set +a
//! cd /home/workspace/bytechainx/redisx
//! CARGO_TARGET_DIR=/home/workspace/bytechainx/.cargo/target \
//!   cargo test --test live_redis -- --ignored --test-threads=1
//! ```
//!
//! **凭据只从环境变量读取，绝不硬编码**；失败信息只报所需变量前缀，不回显取值。
//!
//! 覆盖面（按公开接口分组）：
//!
//! - `RedisConfig` / `RedisConfigBuilder` / `ENV_*` 常量 / `RedisMode`；
//! - `RedisPool`：`connect` / `new`（离线）fail-closed 见 public_api.rs / `connect_from_env` /
//!   `client` / `acquire` / `ping` / `health_check` / `readiness` / `stats` / `metrics_snapshot` /
//!   池级 KV / `close` / `subscribe`；
//! - `RedisPoolPermit`：`endpoint` / `command_timeout` 与全部 permit 级命令；
//! - `RedisClient`：四个构造入口、`with_retry` / `with_call_deadline` 系观察器、
//!   KV（含 `get_bytes` / `set_bytes` / `mget` / `mset`）、Hash / List / Set / ZSet、
//!   Streams（含 `xack` 经消费组）、事务（`multi_exec` / `multi_set`）、
//!   Lua（`eval_script` / `script_load_and_eval` / `eval_sha`）、`pipeline_set`、分布式锁；
//! - `RedisPubSub` / `RedisPubSubMessage`（feature `pubsub`，默认开启）；
//! - 纯项（`RetryConfig` 计算函数、`RedisOperation` 分类矩阵、`RedisError::label` /
//!   `is_retryable`、`generate_lock_token` / `lock_token_matches`、`TxCmd` / `StreamEntry`
//!   访问器、`map_redis_error` 的 NOSCRIPT→Missing 映射）在 live 用例内顺带断言；
//!   其余纯项（`validate` / getter / Debug 脱敏等）由 `src/` 内联单测与
//!   `tests/{public_api,config_and_env,pure_behavior}.rs` 覆盖。
//!
//! 键名纪律：所有 key / 频道一律用唯一前缀 `bytechainx:e2e:<pid>:<nanos>:`，
//! 用例尾尽力 DEL 清理；绝不 FLUSHDB / FLUSHALL、绝不动他人 key。

use std::panic::AssertUnwindSafe;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{FutureExt, StreamExt};

use redisx::{
    generate_lock_token, lock_token_matches, RedisAtomicity, RedisClient, RedisConfig, RedisError,
    RedisOperation, RedisPool, RedisPubSub, RedisRetrySafety, RetryConfig, TxCmd, ENV_ADDR, ENV_DB,
    ENV_PASSWORD, ENV_PREFIX, ENV_TLS, ENV_URL, ENV_USERNAME,
};

/// live 用例整体超时上限（含建连、往返与清理）。
const LIVE_TIMEOUT: Duration = Duration::from_secs(60);

/// 进程内唯一后缀：`进程号 + 纳秒时间戳`，避免并发/重复运行的键冲突。
fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|delta| delta.as_nanos())
        .unwrap_or(0);
    format!("{}_{}", std::process::id(), nanos)
}

/// 唯一化 key（不触碰业务既有数据）。
fn live_key(name: &str) -> String {
    format!("bytechainx:e2e:{}:{name}", unique_suffix())
}

/// 唯一化 Pub/Sub 频道名。
fn live_channel(name: &str) -> String {
    format!("bytechainx:e2e:{}:chan:{name}", unique_suffix())
}

/// 测试尾统一清理：尽力删除全部用过的 key（失败不阻断收尾断言）。
async fn cleanup_keys(client: &RedisClient, keys: &[String]) {
    for key in keys {
        let _ = client.del(key).await;
    }
}

/// 建连 + 结构化探活 + 指标 + close 收尾。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_connect_ping_health_and_close() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        // 纯项：ENV_* 常量点名。
        assert_eq!(ENV_PREFIX, "FOUNDATIONX_REDISX_");
        assert_eq!(ENV_URL, "REDIS_URL");
        assert_eq!(ENV_ADDR, "FOUNDATIONX_REDISX_ADDR");
        assert_eq!(ENV_USERNAME, "FOUNDATIONX_REDISX_USERNAME");
        assert_eq!(ENV_PASSWORD, "FOUNDATIONX_REDISX_PASSWORD");
        assert_eq!(ENV_DB, "FOUNDATIONX_REDISX_DB");
        assert_eq!(ENV_TLS, "FOUNDATIONX_REDISX_TLS");

        // 建连成功断言（E2）：connect_from_env 读环境变量建池。
        let pool = RedisPool::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });
        assert!(!pool.endpoint().is_empty(), "脱敏端点不得为空");
        assert!(pool.liveness(), "建连后应判定为 live");

        // 结构化探活（E3）：ping 返回往返耗时。
        let latency = pool.ping().await.expect("ping 应成功");
        assert!(latency <= LIVE_TIMEOUT, "ping 延迟异常: {latency:?}");

        // readiness：未关闭且 PING 成功。
        let ready = pool.readiness().await.expect("readiness 应成功");
        assert!(ready <= LIVE_TIMEOUT, "readiness 延迟异常: {ready:?}");

        // 结构化健康检查（E3）：端点、延迟、模式。
        let health = pool.health_check().await.expect("health_check 应成功");
        assert!(!health.endpoint.is_empty());
        assert!(health.latency <= LIVE_TIMEOUT);
        assert_eq!(health.mode, redisx::RedisMode::Standalone);
        assert_eq!(health.mode, pool.config().mode());

        // 池快照：已建连时 lane 数等于配置上限。
        let stats = pool.stats();
        assert_eq!(stats.open, pool.config().max_in_flight());
        assert_eq!(stats.in_flight, 0);
        assert_eq!(stats.waiters, 0);

        // 指标：命令成功计数随真实往返增长。
        assert!(
            pool.metrics_snapshot().commands_ok >= 2,
            "ping/readiness 后 commands_ok 应 ≥ 2"
        );

        // 配置访问器（纯项顺带断言）。
        assert_eq!(pool.command_lanes(), pool.config().max_in_flight());
        assert!(pool.command_timeout() > Duration::ZERO);
        assert!(pool.reconnect_max_delay() > Duration::ZERO);

        // close 收尾（E5）：优雅排空后拒绝新请求。
        pool.close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
        assert!(pool.is_closed());
        assert!(!pool.liveness());
        assert_eq!(pool.stats().open, 0);
        assert!(pool.ping().await.is_err(), "关闭后 ping 必须失败");
        assert!(pool.readiness().await.is_err(), "关闭后 readiness 必须失败");
        assert!(pool.metrics_snapshot().rejected_closed >= 1);
    })
    .await
    .expect("live 用例不得超时");
}

/// 唯一化 key 的数据面往返 + TTL + 批量 + 强制清理。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_kv_roundtrip_ttl_and_cleanup() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        let client = RedisClient::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });
        let key = live_key("kv");
        let ttl_key = live_key("ttl");
        let counter = live_key("counter");
        let batch_1 = live_key("batch:1");
        let batch_2 = live_key("batch:2");
        let missing = live_key("missing");
        let keys = [
            key.clone(),
            ttl_key.clone(),
            counter.clone(),
            batch_1.clone(),
            batch_2.clone(),
        ];

        // 用例主体包进 catch_unwind：即使中途 panic 也能在收尾强制清理（E4）。
        let outcome = AssertUnwindSafe(async {
            client
                .set(&key, b"live-value".to_vec())
                .await
                .expect("SET 应成功");
            let value = client.get(&key).await.expect("GET 应成功");
            assert_eq!(value.as_deref(), Some(b"live-value".as_slice()));

            // 二进制安全等价入口。
            client
                .set_bytes(&key, b"bytes-\x00\xff".to_vec())
                .await
                .expect("SET_BYTES 应成功");
            let value = client.get_bytes(&key).await.expect("GET_BYTES 应成功");
            assert_eq!(value.as_deref(), Some(b"bytes-\x00\xff".as_slice()));

            // EXISTS。
            assert!(client.exists(&key).await.expect("EXISTS 应成功"));
            assert!(!client.exists(&missing).await.expect("EXISTS 缺失键应为 false"));

            // PTTL：无过期键 → None；缺失键 → Missing（fail-closed）。
            assert_eq!(client.ttl(&key).await.expect("无 TTL 键应返回 None"), None);
            let err = client
                .ttl(&missing)
                .await
                .expect_err("缺失键 PTTL 应报 Missing");
            assert!(matches!(err, RedisError::Missing(_)), "{err}");
            assert_eq!(err.label(), "missing");
            assert!(!err.is_retryable());

            // TTL 往返：PSETEX 写入后 PTTL 应为正且不超过设定值。
            client
                .set_ex(&ttl_key, b"ttl-value".to_vec(), Duration::from_secs(60))
                .await
                .expect("SETEX 应成功");
            let ttl = client.ttl(&ttl_key).await.expect("TTL 应可读");
            let ttl = ttl.expect("带 TTL 的键应有剩余时间");
            assert!(
                ttl > Duration::ZERO && ttl <= Duration::from_secs(60),
                "{ttl:?}"
            );

            // PEXPIRE：已有键 true；缺失键 false。
            assert!(
                client
                    .expire(&key, Duration::from_secs(120))
                    .await
                    .expect("PEXPIRE 应成功")
            );
            assert!(
                !client
                    .expire(&missing, Duration::from_secs(60))
                    .await
                    .expect("缺失键 PEXPIRE 应返回 false")
            );

            // INCRBY。
            assert_eq!(client.incr(&counter, 5).await.expect("INCR 应成功"), 5);
            assert_eq!(client.incr(&counter, 5).await.expect("INCR 应成功"), 10);

            // MSET / MGET（含缺失槽位）。
            client
                .mset(&[(&batch_1, b"one".as_slice()), (&batch_2, b"two".as_slice())])
                .await
                .expect("MSET 应成功");
            let values = client
                .mget(&[&batch_1, &missing, &batch_2])
                .await
                .expect("MGET 应成功");
            assert_eq!(values[0].as_deref(), Some(b"one".as_slice()));
            assert_eq!(values[1], None);
            assert_eq!(values[2].as_deref(), Some(b"two".as_slice()));
            // 空批短路（不触网）。
            assert!(client.mget(&[]).await.expect("空 MGET").is_empty());
            client.mset(&[]).await.expect("空 MSET");

            // TTL 非法 fail-closed（客户端校验，不触达服务端）。
            let err = client
                .set_ex(&key, b"v".to_vec(), Duration::ZERO)
                .await
                .expect_err("零 TTL 必须拒绝");
            assert!(matches!(err, RedisError::Config(_)));
            let err = client
                .expire(&key, Duration::from_nanos(1))
                .await
                .expect_err("亚毫秒 TTL 必须拒绝");
            assert!(matches!(err, RedisError::Config(_)));

            // 客户端观察器与便捷探活。
            assert!(client.endpoint().starts_with("redis://"));
            assert_eq!(client.config().mode(), redisx::RedisMode::Standalone);
            assert!(client.pool().liveness());
            client.ping().await.expect("client ping 应成功");
            let health = client.health_check().await.expect("health_check 应成功");
            assert_eq!(health.mode, redisx::RedisMode::Standalone);

            Ok::<(), RedisError>(())
        })
        .catch_unwind()
        .await;

        // 无论往返成败（含 panic）都强制清理（E4）。
        cleanup_keys(&client, &keys).await;

        outcome
            .expect("用例主体不得 panic")
            .expect("数据面往返应成功");
        assert_eq!(client.get(&key).await.expect("清理后 GET 应成功"), None);
        assert!(!client.exists(&counter).await.expect("清理后 EXISTS 应成功"));

        // close 收尾（E5）。
        client
            .pool()
            .close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
        assert!(client.pool().is_closed());
    })
    .await
    .expect("live 用例不得超时");
}

/// 分布式锁的取 / 续 / 放往返与竞争失败路径。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_lock_acquire_release_extend_roundtrip() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        let client = RedisClient::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });
        let key = live_key("lock");
        let fence_key = format!("fence:{key}");

        // 纯项：令牌生成与常量时间比较。
        let token_a = generate_lock_token();
        let token_b = generate_lock_token();
        assert_ne!(token_a, token_b);
        assert!(lock_token_matches(&token_a, &token_a));
        assert!(!lock_token_matches(&token_a, &token_b));
        assert!(!lock_token_matches(&token_a, &token_a[..token_a.len() - 1]));

        // 客户端校验 fail-closed（不触网）。
        let err = client
            .lock_acquire("", Duration::from_secs(30))
            .await
            .expect_err("空锁 key 必须拒绝");
        assert!(matches!(err, RedisError::Config(_)));

        let outcome = AssertUnwindSafe(async {
            let lock = client
                .lock_acquire(&key, Duration::from_secs(30))
                .await
                .expect("取锁应成功");
            assert_eq!(lock.key(), key.as_str());
            assert!(lock.verify_token(lock.token()), "owner 令牌自校验应通过");
            assert!(!lock.verify_token("not-the-owner-token"), "他人令牌不得通过");
            assert!(lock.fence() >= 1, "fencing 序号应从 1 起");

            // 竞争失败：同 key 二次取锁应 Conflict。
            let conflict = client
                .lock_acquire(&key, Duration::from_secs(30))
                .await
                .expect_err("竞争失败应返回 Conflict");
            assert!(matches!(conflict, RedisError::Conflict(_)), "{conflict}");
            assert_eq!(conflict.label(), "conflict");
            assert!(!conflict.is_retryable());

            // 续期：owner 可续；释放：owner 可放；再释放返回 false（不再是 owner）。
            assert!(
                client
                    .lock_extend(&lock, Duration::from_secs(60))
                    .await
                    .expect("续期应成功"),
                "owner 续期应返回 true"
            );
            assert!(
                client
                    .lock_release(&lock)
                    .await
                    .expect("释放应成功"),
                "owner 释放应返回 true"
            );
            assert!(
                !client
                    .lock_release(&lock)
                    .await
                    .expect("二次释放应成功返回"),
                "非 owner 释放应返回 false"
            );

            // 释放走 compare-and-delete：锁键此时应已不存在。
            assert!(
                !client.exists(&key).await.expect("EXISTS 应成功"),
                "释放后锁键不应仍存在"
            );
            Ok::<(), RedisError>(())
        })
        .catch_unwind()
        .await;

        // 强制清理锁键与 fencing 计数键（E4；含 panic 路径）。
        let del_lock = client.del(&key).await;
        let del_fence = client.del(&fence_key).await;

        outcome
            .expect("用例主体不得 panic")
            .expect("锁往返应成功");
        assert!(del_lock.is_ok() && del_fence.is_ok(), "清理应可执行");

        client
            .pool()
            .close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
        assert!(client.pool().is_closed());
    })
    .await
    .expect("live 用例不得超时");
}

/// 池构造路径（client_name / warmup）、permit 生命周期与池级命令。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_pool_construct_permit_and_lifecycle() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        // to_builder 派生 + CLIENT SETNAME + warmup PING（建池路径真实执行）。
        let cfg = RedisConfig::from_env()
            .expect("from_env 应成功")
            .to_builder()
            .client_name("bytechainx-e2e-live")
            .warmup_count(2)
            .build()
            .expect("builder 派生配置应合法");
        assert_eq!(cfg.client_name(), Some("bytechainx-e2e-live"));
        assert_eq!(cfg.warmup_count(), 2);

        let pool = RedisPool::connect(cfg)
            .await
            .expect("RedisPool::connect 应成功");
        assert!(pool.liveness());
        assert_eq!(pool.config().client_name(), Some("bytechainx-e2e-live"));

        let kv_key = live_key("pool-kv");
        let ex_key = live_key("pool-ex");
        let counter = live_key("pool-counter");
        let permit_key = live_key("permit-kv");
        let permit_ex = live_key("permit-ex");
        let missing = live_key("missing");
        let keys = [
            kv_key.clone(),
            ex_key.clone(),
            counter.clone(),
            permit_key.clone(),
            permit_ex.clone(),
        ];

        let outcome = AssertUnwindSafe(async {
            // 池级便捷命令。
            pool.set(&kv_key, b"pool".to_vec())
                .await
                .expect("池级 SET 应成功");
            assert_eq!(
                pool.get(&kv_key).await.expect("池级 GET 应成功"),
                Some(b"pool".to_vec())
            );
            assert!(pool.exists(&kv_key).await.expect("池级 EXISTS 应成功"));
            assert_eq!(pool.incr(&counter, 3).await.expect("池级 INCR 应成功"), 3);
            pool.set_ex(&ex_key, b"ex".to_vec(), Duration::from_secs(60))
                .await
                .expect("池级 PSETEX 应成功");
            assert!(pool.ttl(&ex_key).await.expect("池级 PTTL 应成功").is_some());
            assert!(pool
                .expire(&counter, Duration::from_secs(60))
                .await
                .expect("池级 PEXPIRE 应成功"));
            assert!(
                pool.ttl(&counter)
                    .await
                    .expect("池级 PTTL 应成功")
                    .is_some(),
                "PEXPIRE 生效后 counter 应带剩余时间"
            );

            // permit：占用 lane → 连续命令 → Drop 归还。
            {
                let permit = pool.acquire().await.expect("acquire 应成功");
                assert_eq!(permit.endpoint(), pool.endpoint());
                assert_eq!(permit.command_timeout(), pool.command_timeout());
                assert_eq!(
                    pool.stats().in_flight,
                    1,
                    "持有 permit 期间应占用 1 个 lane"
                );

                permit
                    .set(&permit_key, b"permit".to_vec())
                    .await
                    .expect("permit SET 应成功");
                assert_eq!(
                    permit.get(&permit_key).await.expect("permit GET 应成功"),
                    Some(b"permit".to_vec())
                );
                assert!(permit
                    .exists(&permit_key)
                    .await
                    .expect("permit EXISTS 应成功"));
                permit
                    .set_ex(&permit_ex, b"permit-ex".to_vec(), Duration::from_secs(60))
                    .await
                    .expect("permit PSETEX 应成功");
                assert!(permit
                    .expire(&permit_key, Duration::from_secs(120))
                    .await
                    .expect("permit PEXPIRE 应成功"));
                assert!(permit
                    .ttl(&permit_key)
                    .await
                    .expect("permit PTTL 应成功")
                    .is_some());
                assert_eq!(
                    permit.incr(&counter, 2).await.expect("permit INCR 应成功"),
                    5
                );
                permit.ping().await.expect("permit PING 应成功");
                let err = permit
                    .ttl(&missing)
                    .await
                    .expect_err("permit PTTL 缺失键应报 Missing");
                assert!(matches!(err, RedisError::Missing(_)), "{err}");
            }
            assert_eq!(pool.stats().in_flight, 0, "permit Drop 后应归还 lane");
            assert!(pool.metrics_snapshot().commands_ok >= 5);

            Ok::<(), RedisError>(())
        })
        .catch_unwind()
        .await;

        cleanup_keys(&client_of(&pool), &keys).await;
        outcome
            .expect("用例主体不得 panic")
            .expect("池与 permit 往返应成功");

        // close：拒绝新请求并计数。
        pool.close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
        assert!(pool.is_closed());
        assert!(pool.acquire().await.is_err(), "关闭后 acquire 必须失败");
        assert!(pool.get(&kv_key).await.is_err(), "关闭后池级命令必须失败");
        assert!(pool.metrics_snapshot().rejected_closed >= 1);
    })
    .await
    .expect("live 用例不得超时");
}

/// Hash / List（含 BLPOP）/ Set / ZSet 全量命令往返。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_structures_hash_list_set_zset() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        let client = RedisClient::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });
        let hash = live_key("hash");
        let list = live_key("list");
        let blpop_list = live_key("blpop");
        let set = live_key("set");
        let zset = live_key("zset");
        let keys = [hash.clone(), list.clone(), blpop_list.clone(), set.clone(), zset.clone()];

        let outcome = AssertUnwindSafe(async {
            // ── Hash ──
            assert!(
                client
                    .hset(&hash, "f1", b"v1".to_vec())
                    .await
                    .expect("HSET 应成功")
            );
            assert!(
                !client
                    .hset(&hash, "f1", b"v1-again".to_vec())
                    .await
                    .expect("重复 HSET 应返回 false")
            );
            assert_eq!(
                client.hget(&hash, "f1").await.expect("HGET 应成功"),
                Some(b"v1-again".to_vec())
            );
            assert_eq!(
                client.hget(&hash, "missing").await.expect("HGET 缺失字段应为 None"),
                None
            );
            assert!(
                client
                    .hset(&hash, "f2", b"v2".to_vec())
                    .await
                    .expect("HSET f2 应成功")
            );
            let all = client.hgetall(&hash).await.expect("HGETALL 应成功");
            assert_eq!(all.len(), 2);
            assert!(
                all.contains(&("f1".to_owned(), b"v1-again".to_vec()))
                    && all.contains(&("f2".to_owned(), b"v2".to_vec()))
            );
            assert_eq!(
                client
                    .hdel(&hash, &["f1", "f2"])
                    .await
                    .expect("HDEL 应成功"),
                2
            );
            assert_eq!(
                client.hdel(&hash, &[]).await.expect("空 HDEL 应短路"),
                0
            );
            assert!(client.hgetall(&hash).await.expect("HGETALL 空键应为空").is_empty());

            // ── List ──
            assert_eq!(
                client
                    .lpush(&list, b"head".to_vec())
                    .await
                    .expect("LPUSH 应成功"),
                1
            );
            assert_eq!(
                client
                    .rpush(&list, b"tail".to_vec())
                    .await
                    .expect("RPUSH 应成功"),
                2
            );
            assert_eq!(
                client
                    .lpush(&list, b"new-head".to_vec())
                    .await
                    .expect("LPUSH 应成功"),
                3
            );
            let range = client.lrange(&list, 0, -1).await.expect("LRANGE 应成功");
            assert_eq!(range.len(), 3);
            assert_eq!(range[0].as_slice(), b"new-head");
            assert_eq!(range[2].as_slice(), b"tail");
            assert_eq!(
                client.lpop(&list).await.expect("LPOP 应成功"),
                Some(b"new-head".to_vec())
            );
            assert_eq!(
                client.lpop(&list).await.expect("LPOP 应成功"),
                Some(b"head".to_vec())
            );
            // BLPOP：先推后取立即返回 (key, value)。
            assert_eq!(
                client
                    .rpush(&blpop_list, b"blocked".to_vec())
                    .await
                    .expect("RPUSH 应成功"),
                1
            );
            let popped = client
                .blpop(&blpop_list, Duration::from_millis(50))
                .await
                .expect("BLPOP 应成功");
            let (popped_key, popped_value) = popped.expect("已有元素应立即弹出");
            assert_eq!(popped_key, blpop_list);
            assert_eq!(popped_value, b"blocked".to_vec());
            // BLPOP：空列表阻塞到期返回 None（秒级向上取整，约等 1s）。
            let none = client
                .blpop(&blpop_list, Duration::from_millis(10))
                .await
                .expect("空 BLPOP 应成功返回");
            assert!(none.is_none(), "空列表阻塞到期应返回 None");

            // ── Set ──
            assert_eq!(
                client.sadd(&set, b"m1".to_vec()).await.expect("SADD 应成功"),
                1
            );
            assert_eq!(
                client.sadd(&set, b"m1".to_vec()).await.expect("重复 SADD 应为 0"),
                0
            );
            assert!(
                client
                    .sismember(&set, b"m1")
                    .await
                    .expect("SISMEMBER 应成功")
            );
            assert!(
                !client
                    .sismember(&set, b"m2")
                    .await
                    .expect("SISMEMBER 缺失应为 false")
            );
            assert_eq!(
                client.srem(&set, b"m1").await.expect("SREM 应成功"),
                1
            );
            assert_eq!(
                client.srem(&set, b"m1").await.expect("重复 SREM 应为 0"),
                0
            );

            // ── ZSet ──
            assert_eq!(
                client
                    .zadd(&zset, b"member".to_vec(), 1.5)
                    .await
                    .expect("ZADD 应成功"),
                1
            );
            assert_eq!(
                client
                    .zadd(&zset, b"member".to_vec(), 2.5)
                    .await
                    .expect("重复 ZADD 应为 0"),
                0
            );
            assert_eq!(
                client
                    .zscore(&zset, b"member")
                    .await
                    .expect("ZSCORE 应成功"),
                Some(2.5)
            );
            assert_eq!(
                client
                    .zscore(&zset, b"missing")
                    .await
                    .expect("ZSCORE 缺失成员应为 None"),
                None
            );
            assert_eq!(
                client.zrem(&zset, b"member").await.expect("ZREM 应成功"),
                1
            );
            assert_eq!(
                client.zrem(&zset, b"member").await.expect("重复 ZREM 应为 0"),
                0
            );

            Ok::<(), RedisError>(())
        })
        .catch_unwind()
        .await;

        cleanup_keys(&client, &keys).await;
        outcome
            .expect("用例主体不得 panic")
            .expect("数据结构往返应成功");

        client
            .pool()
            .close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
    })
    .await
    .expect("live 用例不得超时");
}

/// Streams：xadd / xadd_with_id / xlen / xrange / xread / xread_block / xdel / xack（经消费组）。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_streams_xadd_xread_xdel_xack() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        let client = RedisClient::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });
        let stream_key = live_key("stream");
        let group = "e2e-group";

        // 参数校验 fail-closed（不触网）。
        let err = client
            .xadd(&stream_key, &[])
            .await
            .expect_err("空 fields 必须拒绝");
        assert!(matches!(err, RedisError::Config(_)));
        let err = client
            .xadd_with_id(&stream_key, "  ", &[("f", b"v")])
            .await
            .expect_err("空 id 必须拒绝");
        assert!(matches!(err, RedisError::Config(_)));

        let outcome = AssertUnwindSafe(async {
            // 显式 ID 写入（回放路径）与服务端 ID 写入。
            assert_eq!(
                client
                    .xadd_with_id(&stream_key, "1-1", &[("kind", b"first")])
                    .await
                    .expect("XADD 指定 ID 应成功"),
                "1-1"
            );
            let second = client
                .xadd(&stream_key, &[("kind", b"second")])
                .await
                .expect("XADD 应成功");
            assert!(second.as_str() > "1-1", "服务端 ID 应大于显式 ID: {second}");
            assert_eq!(client.xlen(&stream_key).await.expect("XLEN 应成功"), 2);

            // XRANGE 全量 + COUNT 截断 + StreamEntry::field（纯项）。
            let all = client
                .xrange(&stream_key, "-", "+", None)
                .await
                .expect("XRANGE 应成功");
            assert_eq!(all.len(), 2);
            assert_eq!(all[0].id, "1-1");
            assert_eq!(all[0].field("kind"), Some(b"first".as_slice()));
            assert_eq!(all[0].field("missing"), None);
            assert_eq!(
                client
                    .xrange(&stream_key, "-", "+", Some(1))
                    .await
                    .expect("XRANGE COUNT 应成功")
                    .len(),
                1
            );

            // XREAD 从头读 + 阻塞读。
            let head = client
                .xread(&stream_key, "0-0", Some(10))
                .await
                .expect("XREAD 应成功");
            assert_eq!(head.len(), 2);
            assert_eq!(head[1].id, second);
            let got = client
                .xread_block(&stream_key, "0-0", Duration::from_millis(50), Some(10))
                .await
                .expect("XREAD BLOCK（有积压）应立即返回");
            assert_eq!(got.len(), 2);
            let none = client
                .xread_block(&stream_key, &second, Duration::from_millis(50), Some(10))
                .await
                .expect("XREAD BLOCK 阻塞到期应成功");
            assert!(none.is_empty(), "无新消息阻塞到期应返回空");

            // XDEL。
            assert_eq!(
                client.xdel(&stream_key, &["1-1"]).await.expect("XDEL 应成功"),
                1
            );
            assert_eq!(
                client.xdel(&stream_key, &[]).await.expect("空 XDEL 应短路"),
                0
            );
            assert_eq!(client.xlen(&stream_key).await.expect("XLEN 应成功"), 1);

            // 消费组经 eval_script 建立（crate 不提供 XGROUP），XREADGROUP 入 PEL 后 XACK。
            let create_group = "return redis.call('XGROUP','CREATE',KEYS[1],ARGV[1],'0')";
            client
                .eval_script(create_group, &[&stream_key], &[group.as_bytes()])
                .await
                .expect("XGROUP CREATE 应成功");
            let read_group = "return redis.call('XREADGROUP','GROUP',ARGV[1],ARGV[2],'COUNT',10,'STREAMS',KEYS[1],'>')";
            client
                .eval_script(
                    read_group,
                    &[&stream_key],
                    &[group.as_bytes(), b"e2e-consumer"],
                )
                .await
                .expect("XREADGROUP 应成功");
            assert_eq!(
                client
                    .xack(&stream_key, group, &[&second])
                    .await
                    .expect("XACK 应成功"),
                1
            );
            assert_eq!(
                client
                    .xack(&stream_key, group, &[])
                    .await
                    .expect("空 XACK 应短路"),
                0
            );
            let err = client
                .xack(&stream_key, " ", &["1-0"])
                .await
                .expect_err("空消费组名必须拒绝");
            assert!(matches!(err, RedisError::Config(_)));

            Ok::<(), RedisError>(())
        })
        .catch_unwind()
        .await;

        // DEL 连同消费组与流一并清理（E4；含 panic 路径）。
        let del_stream = client.del(&stream_key).await;
        outcome
            .expect("用例主体不得 panic")
            .expect("streams 往返应成功");
        assert!(del_stream.is_ok());

        client
            .pool()
            .close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
    })
    .await
    .expect("live 用例不得超时");
}

/// 事务（multi_exec / multi_set）与 pipeline_set。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_transaction_and_pipeline() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        let client = RedisClient::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });
        let tx_set = live_key("tx-set");
        let tx_counter = live_key("tx-counter");
        let tx_del = live_key("tx-del");
        let ms_1 = live_key("multiset:1");
        let ms_2 = live_key("multiset:2");
        let pl_1 = live_key("pipe:1");
        let pl_2 = live_key("pipe:2");
        let pl_ttl = live_key("pipe:ttl");
        let keys = [
            tx_set.clone(),
            tx_counter.clone(),
            tx_del.clone(),
            ms_1.clone(),
            ms_2.clone(),
            pl_1.clone(),
            pl_2.clone(),
            pl_ttl.clone(),
        ];

        // 纯项：TxCmd 构造器与访问器。
        assert_eq!(TxCmd::set("a", b"b".to_vec()).command_name(), "SET");
        assert_eq!(TxCmd::del("a").command_name(), "DEL");
        assert_eq!(TxCmd::incr("a").command_name(), "INCR");
        assert_eq!(TxCmd::set("a", b"b".to_vec()).key(), "a");
        assert_eq!(TxCmd::del("a").key(), "a");
        assert_eq!(TxCmd::incr("a").key(), "a");

        // 空事务 fail-closed（不触网）。
        let err = client
            .multi_exec(&[])
            .await
            .expect_err("空事务必须拒绝");
        assert!(matches!(err, RedisError::Config(_)));

        let outcome = AssertUnwindSafe(async {
            // 预置 tx_del，事务内 DEL。
            client
                .set(&tx_del, b"stale".to_vec())
                .await
                .expect("预置应成功");

            let results = client
                .multi_exec(&[
                    TxCmd::set(tx_set.clone(), b"tx".to_vec()),
                    TxCmd::incr(tx_counter.clone()),
                    TxCmd::del(tx_del.clone()),
                ])
                .await
                .expect("MULTI/EXEC 应成功");
            assert_eq!(results.len(), 3, "EXEC 应返回逐命令结果");
            assert_eq!(
                client.get(&tx_set).await.expect("事务 SET 后 GET 应成功"),
                Some(b"tx".to_vec())
            );
            assert_eq!(
                client
                    .get(&tx_counter)
                    .await
                    .expect("事务 INCR 后 GET 应成功"),
                Some(b"1".to_vec())
            );
            assert_eq!(
                client.get(&tx_del).await.expect("事务 DEL 后 GET 应为 None"),
                None
            );

            // multi_set 便捷入口与空入参短路。
            client
                .multi_set(&[(&ms_1, b"m1".as_slice()), (&ms_2, b"m2".as_slice())])
                .await
                .expect("multi_set 应成功");
            assert_eq!(
                client.get(&ms_1).await.expect("GET 应成功"),
                Some(b"m1".to_vec())
            );
            assert_eq!(
                client.get(&ms_2).await.expect("GET 应成功"),
                Some(b"m2".to_vec())
            );
            client.multi_set(&[]).await.expect("空 multi_set 应短路");

            // pipeline 无 TTL。
            client
                .pipeline_set(
                    &[(&pl_1, b"p1".to_vec()), (&pl_2, b"p2".to_vec())],
                    None,
                )
                .await
                .expect("pipeline_set 应成功");
            assert_eq!(
                client.get(&pl_1).await.expect("GET 应成功"),
                Some(b"p1".to_vec())
            );
            assert_eq!(
                client.get(&pl_2).await.expect("GET 应成功"),
                Some(b"p2".to_vec())
            );
            assert_eq!(
                client.ttl(&pl_1).await.expect("无 TTL 断言应成功"),
                None,
                "无 TTL pipeline 写入不应带过期"
            );

            // pipeline 统一 TTL。
            client
                .pipeline_set(&[(&pl_ttl, b"pt".to_vec())], Some(Duration::from_secs(60)))
                .await
                .expect("pipeline_set（TTL）应成功");
            assert_eq!(
                client.get(&pl_ttl).await.expect("GET 应成功"),
                Some(b"pt".to_vec())
            );
            let ttl = client
                .ttl(&pl_ttl)
                .await
                .expect("TTL 应可读")
                .expect("统一 TTL 写入应有剩余时间");
            assert!(ttl > Duration::ZERO && ttl <= Duration::from_secs(60), "{ttl:?}");

            // TTL 校验 fail-closed（不触网）。
            let err = client
                .pipeline_set(&[(&pl_1, b"x".to_vec())], Some(Duration::ZERO))
                .await
                .expect_err("pipeline 零 TTL 必须拒绝");
            assert!(matches!(err, RedisError::Config(_)));

            Ok::<(), RedisError>(())
        })
        .catch_unwind()
        .await;

        cleanup_keys(&client, &keys).await;
        outcome
            .expect("用例主体不得 panic")
            .expect("事务与 pipeline 往返应成功");

        client
            .pool()
            .close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
    })
    .await
    .expect("live 用例不得超时");
}

/// Lua：eval_script / script_load_and_eval / eval_sha（含 NOSCRIPT→Missing fail-closed）。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_lua_scripts_eval_sha() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        let client = RedisClient::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });
        let script_key = live_key("lua");

        // 空脚本 fail-closed（不触网）。
        let err = client
            .eval_script("  ", &[], &[])
            .await
            .expect_err("空脚本必须拒绝");
        assert!(matches!(err, RedisError::Config(_)));
        let err = client
            .script_load_and_eval("", &[], &[])
            .await
            .expect_err("空脚本必须拒绝");
        assert!(matches!(err, RedisError::Config(_)));
        let err = client
            .eval_sha(" ", &[], &[])
            .await
            .expect_err("空 sha 必须拒绝");
        assert!(matches!(err, RedisError::Config(_)));

        let outcome = AssertUnwindSafe(async {
            // EVAL：ARGV 透传（BulkString）。
            let value = client
                .eval_script("return ARGV[1]", &[], &[b"hello-lua"])
                .await
                .expect("EVAL 应成功");
            match value {
                redis::Value::BulkString(bytes) => {
                    assert_eq!(bytes, b"hello-lua".to_vec());
                }
                other => panic!("EVAL 返回应为 BulkString: {other:?}"),
            }

            // EVAL：KEYS 写入 + 整数返回。
            let value = client
                .eval_script(
                    "redis.call('SET',KEYS[1],ARGV[1]); return 42",
                    &[&script_key],
                    &[b"script-set"],
                )
                .await
                .expect("EVAL 写入应成功");
            assert!(matches!(value, redis::Value::Int(42)));
            assert_eq!(
                client.get(&script_key).await.expect("GET 应成功"),
                Some(b"script-set".to_vec())
            );

            // SCRIPT LOAD + EVALSHA（两段式）。
            let (sha, value) = client
                .script_load_and_eval("return 7", &[], &[])
                .await
                .expect("SCRIPT LOAD + EVALSHA 应成功");
            assert_eq!(sha.len(), 40, "SHA-1 应为 40 个十六进制字符: {sha}");
            assert!(sha.chars().all(|c| c.is_ascii_hexdigit()));
            assert!(matches!(value, redis::Value::Int(7)));

            // eval_sha 复用已加载 SHA。
            let value = client
                .eval_sha(&sha, &[], &[])
                .await
                .expect("EVALSHA 应成功");
            assert!(matches!(value, redis::Value::Int(7)));

            // 未知 SHA → NOSCRIPT → Missing（map_redis_error 的 fail-closed 映射）。
            let bogus = "0".repeat(40);
            let err = client
                .eval_sha(&bogus, &[], &[])
                .await
                .expect_err("未知 SHA 应报 NOSCRIPT");
            assert!(matches!(err, RedisError::Missing(_)), "{err}");
            assert_eq!(err.label(), "missing");
            assert!(!err.is_retryable());

            Ok::<(), RedisError>(())
        })
        .catch_unwind()
        .await;

        cleanup_keys(&client, &[script_key]).await;
        outcome
            .expect("用例主体不得 panic")
            .expect("Lua 往返应成功");

        client
            .pool()
            .close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
    })
    .await
    .expect("live 用例不得超时");
}

/// Pub/Sub：pool.subscribe / RedisPubSub::connect_config / publish / 两种消息流。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_pubsub_roundtrip() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        let pool = RedisPool::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });

        // 会话一：pool::subscribe + into_result_message_stream。
        let channel_1 = live_channel("result");
        let session = pool
            .subscribe([channel_1.clone()])
            .await
            .expect("subscribe 应成功");
        assert!(!session.endpoint().is_empty());
        session
            .publish(&channel_1, b"payload-result-stream")
            .await
            .expect("PUBLISH 应成功");
        let mut stream = session
            .into_result_message_stream()
            .expect("into_result_message_stream 应成功");
        let message = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("接收消息不得超时")
            .expect("应收到一条消息")
            .expect("消息应为 Ok");
        assert_eq!(message.channel.as_ref(), channel_1.as_bytes());
        assert_eq!(message.payload.as_ref(), b"payload-result-stream");
        drop(stream);

        // 会话二：RedisPubSub::connect_config + into_message_stream。
        let cfg = RedisConfig::from_env().expect("from_env 应成功");
        let channel_2 = live_channel("message");
        let session_2 = RedisPubSub::connect_config(cfg, [channel_2.clone()])
            .await
            .expect("connect_config 应成功");
        session_2
            .publish(&channel_2, b"payload-message-stream")
            .await
            .expect("PUBLISH 应成功");
        let mut stream_2 = session_2
            .into_message_stream()
            .expect("into_message_stream 应成功");
        let message = tokio::time::timeout(Duration::from_secs(5), stream_2.next())
            .await
            .expect("接收消息不得超时")
            .expect("应收到一条消息");
        assert_eq!(message.channel.as_ref(), channel_2.as_bytes());
        assert_eq!(message.payload.as_ref(), b"payload-message-stream");
        drop(stream_2);

        // 纯项：RedisPubSubMessage 字段保持原始字节。
        let raw = redisx::RedisPubSubMessage {
            channel: bytes::Bytes::from_static(b"chan"),
            payload: bytes::Bytes::from_static(&[0, 1, 255]),
        };
        assert_eq!(raw.channel.as_ref(), b"chan");
        assert_eq!(raw.payload.as_ref(), &[0, 1, 255]);
        assert_eq!(raw.clone(), raw);

        pool.close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
    })
    .await
    .expect("live 用例不得超时");
}

/// 配置入口：from_url（connect_url）与 from_toml（+ password_from_provider 注入）。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_config_entrypoints_from_url_from_toml() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        let addr = std::env::var(ENV_ADDR).unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
        let username = std::env::var(ENV_USERNAME).unwrap_or_else(|_| "default".to_owned());
        let password = std::env::var(ENV_PASSWORD).unwrap_or_default();
        let db: i64 = std::env::var(ENV_DB)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let tls = std::env::var(ENV_TLS)
            .map(|value| value == "true" || value == "1")
            .unwrap_or(false);

        // from_url：由环境变量拼 URL（百分号编码凭据），connect_url 建池。
        let scheme = if tls { "rediss" } else { "redis" };
        let userinfo = if password.is_empty() {
            if username.is_empty() {
                String::new()
            } else {
                format!("{}@", percent_encode(&username))
            }
        } else {
            format!(
                "{}:{}@",
                percent_encode(&username),
                percent_encode(&password)
            )
        };
        let url = format!("{scheme}://{userinfo}{addr}/{db}");
        let client = RedisClient::connect_url(&url)
            .await
            .unwrap_or_else(|error| panic!("connect_url 建池失败（URL 不回显凭据）: {error}"));

        // from_toml：字段与 ENV 语义一一对应；密码只能经 builder 注入。
        let toml_text = format!("addr = \"{addr}\"\nusername = \"{username}\"\ndb = {db}\n");
        let cfg = RedisConfig::from_toml(&toml_text).expect("from_toml 应成功");
        assert_eq!(cfg.addr(), addr);
        assert_eq!(cfg.db(), db);
        assert_eq!(cfg.mode(), redisx::RedisMode::Standalone);
        let cfg = cfg
            .to_builder()
            .password_from_provider(|| {
                let password = std::env::var(ENV_PASSWORD).unwrap_or_default();
                if password.is_empty() {
                    None
                } else {
                    Some(password)
                }
            })
            .build()
            .expect("注入凭据后配置应合法");
        let client_2 = RedisClient::connect(cfg)
            .await
            .expect("from_toml 配置建池应成功");

        let url_key = live_key("from-url");
        let toml_key = live_key("from-toml");
        let outcome = AssertUnwindSafe(async {
            client.ping().await.expect("connect_url 后 ping 应成功");
            client
                .set(&url_key, b"u".to_vec())
                .await
                .expect("SET 应成功");
            assert_eq!(
                client.get(&url_key).await.expect("GET 应成功"),
                Some(b"u".to_vec())
            );
            client_2.ping().await.expect("ping 应成功");
            client_2
                .set(&toml_key, b"t".to_vec())
                .await
                .expect("SET 应成功");
            assert_eq!(
                client_2.get(&toml_key).await.expect("GET 应成功"),
                Some(b"t".to_vec())
            );
            Ok::<(), RedisError>(())
        })
        .catch_unwind()
        .await;

        // 强制清理（E4；含 panic 路径）。
        let del_url = client.del(&url_key).await;
        let del_toml = client_2.del(&toml_key).await;
        outcome
            .expect("用例主体不得 panic")
            .expect("配置入口往返应成功");
        assert!(del_url.is_ok() && del_toml.is_ok(), "清理应可执行");

        // from_toml 明文密码 fail-closed（离线即拒，不触网）。
        let err = RedisConfig::from_toml("addr = \"127.0.0.1:6379\"\npassword = \"x\"\n")
            .expect_err("TOML 明文密码必须拒绝");
        assert!(matches!(err, RedisError::Config(_)));

        client
            .pool()
            .close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
        client_2
            .pool()
            .close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
    })
    .await
    .expect("live 用例不得超时");
}

/// 重试策略与调用级 deadline：观察器、真实命令行为与零预算 fail-closed。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_retry_policy_and_call_deadline() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        let client = RedisClient::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });
        let key = live_key("retry");

        // 纯项：RetryConfig 构造、访问器与确定性计算函数。
        let retry = RetryConfig::fixed(3, Duration::from_millis(10)).without_jitter();
        assert_eq!(retry.max_attempts(), 3);
        assert_eq!(retry.initial_backoff(), Duration::from_millis(10));
        assert_eq!(retry.max_backoff(), Duration::from_millis(10));
        assert!(!retry.jitter());
        assert!(retry.deadline().is_none());
        assert_eq!(retry.backoff_for(1), Duration::from_millis(10));
        assert_eq!(retry.backoff_for(0), Duration::ZERO);
        let jittered = RetryConfig::jittered(Duration::from_millis(1000), 7);
        assert!(jittered <= Duration::from_millis(1000));
        assert!(jittered >= Duration::from_millis(500));
        let exponential = RetryConfig::exponential(4, Duration::from_millis(10), Duration::from_millis(40));
        assert_eq!(exponential.multiplier(), 2.0);
        assert_eq!(exponential.backoff_for(3), Duration::from_millis(40));

        let outcome = AssertUnwindSafe(async {
            // 挂到客户端：观察器可读，ReadOnly 命令在重试环内真实执行成功。
            let configured = client.clone().with_retry(retry.clone());
            assert_eq!(
                configured
                    .retry_config()
                    .map(|config| config.max_attempts()),
                Some(3)
            );
            assert!(!configured.has_call_deadline());
            configured
                .set(&key, b"retry".to_vec())
                .await
                .expect("SET 应成功");
            assert_eq!(
                configured.get(&key).await.expect("GET（重试环内）应成功"),
                Some(b"retry".to_vec())
            );

            // 调用级 deadline：宽松预算成功；零预算 fail-closed。
            let deadline_client = client
                .clone()
                .with_call_deadline(Duration::from_secs(10));
            assert!(deadline_client.has_call_deadline());
            assert_eq!(
                deadline_client.get(&key).await.expect("GET 应成功"),
                Some(b"retry".to_vec())
            );
            let zero_client = client.clone().with_call_deadline(Duration::ZERO);
            let err = zero_client
                .get(&key)
                .await
                .expect_err("零总 deadline 必须拒绝");
            assert!(matches!(err, RedisError::Timeout(_)), "{err}");

            // with_retry 自由函数（真实执行）。
            let value = redisx::with_retry(&retry, "live.echo", || async {
                Ok::<_, RedisError>(1_u8)
            })
            .await
            .expect("with_retry 应成功");
            assert_eq!(value, 1);

            Ok::<(), RedisError>(())
        })
        .catch_unwind()
        .await;

        // 强制清理（E4；含 panic 路径）。
        cleanup_keys(&client, &[key]).await;
        outcome
            .expect("用例主体不得 panic")
            .expect("重试与 deadline 往返应成功");

        // 纯项：RedisOperation 分类矩阵点名。
        assert_eq!(
            RedisOperation::Get.retry_safety(),
            RedisRetrySafety::ReadOnly
        );
        assert!(RedisOperation::Get.allows_automatic_retry());
        assert_eq!(
            RedisOperation::Set.retry_safety(),
            RedisRetrySafety::AmbiguousWrite
        );
        assert!(!RedisOperation::Set.allows_automatic_retry());
        assert_eq!(
            RedisOperation::Incr.retry_safety(),
            RedisRetrySafety::NeverAutomatic
        );
        assert!(!RedisOperation::Publish.allows_automatic_retry());
        assert_eq!(
            RedisOperation::Mget.atomicity(),
            RedisAtomicity::MultiKeySingleSlot
        );
        assert_eq!(
            RedisOperation::Publish.atomicity(),
            RedisAtomicity::None
        );

        client
            .pool()
            .close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
    })
    .await
    .expect("live 用例不得超时");
}

/// URL userinfo 百分号编码（凭据含保留字符时保持 URL 可解析）。
fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(byte));
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// 由池派生客户端（清理辅助用）。
fn client_of(pool: &RedisPool) -> RedisClient {
    pool.client()
}
