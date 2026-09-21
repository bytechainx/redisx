# redisx 公开 API

**版本 / 角色**：`redisx 0.1.0` · 生产可用的异步 Redis 适配器（三种拓扑 + 连接池背压 + 核心数据结构 + Streams 原语）

## 公开消费面

| 面 | 类型 / 函数 | 说明 |
|----|-------------|------|
| 配置 | `RedisConfig`、`RedisConfigBuilder`、`RedisMode`、`ENV_*` 常量 | `from_env` / `from_toml` / `from_url` / `validate` / `builder`；密码在 `Debug` 与 `display_endpoint()` 中脱敏 |
| 连接池 | `RedisPool`、`RedisPoolPermit`、`RedisPoolStats`、`RedisMetricsSnapshot`、`RedisHealth` | `connect` / `new` / `acquire` / `ping` / `health_check` / `stats` / `metrics_snapshot` / `close`；`max_in_flight` + `acquire_timeout` 有界背压 |
| 命令客户端 | `RedisClient` | KV、Hash/List/Set/ZSet、Streams、事务、Lua、分布式锁 |
| 锁与脚本 | `RedisLock`、`generate_lock_token`、`lock_token_matches` | 带 token 校验的分布式锁 |
| Streams | `StreamEntry` | 流条目 |
| 事务 | `TxCmd` | MULTI/EXEC 事务命令 |
| 重试 | `RetryConfig`、`with_retry`、`RedisOperation`、`RedisRetrySafety`、`RedisAtomicity` | 指数退避 + 抖动 + 总 deadline；按副作用分类准入 |
| 错误 | `RedisError`（`is_retryable`）、`RedisResult`、`map_redis_error`、`map_redis_result` | thiserror 枚举 + 上游错误映射 |
| feature `pubsub`（默认开启） | `RedisPubSub`、`RedisPubSubMessage` | 原始 Pub/Sub 会话 |

## 最小用法

```rust,no_run
use redisx::{RedisConfig, RedisPool};

# async fn run() -> redisx::RedisResult<()> {
let pool = RedisPool::connect(RedisConfig::from_url("redis://127.0.0.1:6379")?).await?;
pool.set("hello", b"world".to_vec()).await?;
let value = pool.get("hello").await?;
assert_eq!(value.as_deref(), Some(b"world".as_slice()));
pool.ping().await?;
pool.close(std::time::Duration::from_secs(1)).await?;
# Ok(())
# }
```

## 拓扑

| 模式 | 连接方式 | 说明 |
| --- | --- | --- |
| `RedisMode::Standalone` | `ConnectionManager` | 单机；自动重连 + 指数退避 |
| `RedisMode::Cluster` | `ClusterConnection` | 种子节点发现；`MOVED`/`ASK` 由驱动跟随 |
| `RedisMode::Sentinel` | 哨兵发现 master + `ConnectionManager` | 必须提供 `sentinel_master`，否则 `validate()` 失败 |

Cluster 不支持非 0 逻辑库。

## 重试与副作用安全

`RedisClient::with_retry` 只在命令的 `RedisRetrySafety` 为只读（`ReadOnly`）或幂等（`Idempotent`）时进入重试环；`SET`（带 TTL）、`DEL`、`PEXPIRE`、`INCR`、`PUBLISH` 等结果不明（`AmbiguousWrite`）或非幂等（`NeverAutomatic`）的命令**永远**只执行一次，避免超时后重复副作用。`RedisOperation::retry_safety()` / `atomicity()` 为 `const fn`，可在编译期归类。

## 能力边界

本 crate 提供 Redis 协议内的连接、命令、池化与重试治理；**不提供**Redis 服务端、缓存语义层（如 stale-while-revalidate）、跨 shard 事务或二级索引。TLS 经 rustls（webpki 根证书），不依赖 OpenSSL。
