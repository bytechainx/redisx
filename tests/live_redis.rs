#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! live 真连服测试（特性 002，redisx）。
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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redisx::{RedisClient, RedisPool};

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

/// 唯一化 key 前缀（不触碰业务既有数据）。
fn live_key(name: &str) -> String {
    format!("redisx:live:{name}:{}", unique_suffix())
}

/// 建连 + 结构化探活 + close 收尾。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_connect_ping_health_and_close() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        // 建连成功断言（E2）：connect_from_env 读环境变量建池。
        let pool = RedisPool::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });
        assert!(!pool.endpoint().is_empty(), "脱敏端点不得为空");

        // 结构化探活（E3）：ping 返回往返耗时。
        let latency = pool.ping().await.expect("ping 应成功");
        assert!(latency <= LIVE_TIMEOUT, "ping 延迟异常: {latency:?}");

        // 结构化健康检查（E3）：端点、延迟、模式。
        let health = pool.health_check().await.expect("health_check 应成功");
        assert!(!health.endpoint.is_empty());
        assert!(health.latency <= LIVE_TIMEOUT);
        assert_eq!(health.mode, pool.config().mode());

        // 池快照：已建连时 lane 数等于配置上限。
        let stats = pool.stats();
        assert_eq!(stats.open, pool.config().max_in_flight());

        // close 收尾（E5）：优雅排空后拒绝新请求。
        pool.close(Duration::from_secs(5))
            .await
            .expect("close 应成功");
        assert!(pool.is_closed());
        assert!(pool.ping().await.is_err(), "关闭后 ping 必须失败");
    })
    .await
    .expect("live 用例不得超时");
}

/// 唯一化 key 的数据面往返 + TTL + 强制清理。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_kv_roundtrip_ttl_and_cleanup() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        // 建连成功断言（E2）。
        let client = RedisClient::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });
        // 唯一化 key（E4）：进程号 + 纳秒时间戳，绝不触碰既有业务键。
        let key = live_key("kv");
        let ttl_key = live_key("ttl");

        let outcome = async {
            client
                .set(&key, b"live-value".to_vec())
                .await
                .expect("SET 应成功");
            let value = client.get(&key).await.expect("GET 应成功");
            assert_eq!(value.as_deref(), Some(b"live-value".as_slice()));

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
            Ok::<(), redisx::RedisError>(())
        }
        .await;

        // 无论往返成败都强制清理（E4）。
        let del_main = client.del(&key).await;
        let del_ttl = client.del(&ttl_key).await;

        outcome.expect("数据面往返应成功");
        assert!(del_main.expect("DEL 主键应成功"), "主键应被删除");
        assert!(del_ttl.expect("DEL TTL 键应成功"), "TTL 键应被删除");

        // 断言清理生效：删除后 GET 返回 None。
        assert_eq!(client.get(&key).await.expect("清理后 GET 应成功"), None);

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

/// 分布式锁的取 / 续 / 放往返：覆盖 `lock_acquire` / `lock_extend` / `lock_release`
/// 三入口的真实行为（离线无法构造 `RedisLock`，故这三条行为只能在此验证）。
#[tokio::test]
#[ignore = "需要真实 Redis 实例与 FOUNDATIONX_REDISX_* / REDIS_URL 环境变量"]
async fn live_lock_acquire_release_extend_roundtrip() {
    tokio::time::timeout(LIVE_TIMEOUT, async {
        let client = RedisClient::connect_from_env().await.unwrap_or_else(|error| {
            panic!("建池失败，请确认 FOUNDATIONX_REDISX_ADDR / USERNAME / PASSWORD / DB 与 live 服务可达: {error}")
        });
        // 唯一化锁键与 fencing 键（E4）。
        let key = live_key("lock");
        let fence_key = format!("fence:{key}");

        let lock = client
            .lock_acquire(&key, Duration::from_secs(30))
            .await
            .expect("取锁应成功");
        assert!(lock.verify_token(lock.token()), "owner 令牌自校验应通过");
        assert!(!lock.verify_token("not-the-owner-token"), "他人令牌不得通过");
        assert!(lock.fence() >= 1, "fencing 序号应从 1 起");

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

        // 释放走 compare-and-delete：锁键此时应已不存在；再清理 fencing 计数键。
        assert!(
            !client.exists(&key).await.expect("EXISTS 应成功"),
            "释放后锁键不应仍存在"
        );
        assert!(
            client.del(&fence_key).await.expect("清理 fencing 键"),
            "fencing 键应已删除"
        );

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
