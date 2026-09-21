# redisx

生产可用的异步 Redis 适配器：单 crate 提供 Standalone / Cluster / Sentinel 三种拓扑、
连接池背压、会话级超时与可配置重试、KV 与数据结构命令、Streams 原语、`MULTI/EXEC` 事务、
Lua 脚本与带所有权校验的分布式锁，并可选提供原始 Pub/Sub。

- 零私有依赖：只用 crates.io 公开依赖（`redis` 0.27 + `tokio`）。
- 副作用安全优先：结果不明的写命令**永远**不会被自动重试。
- 明确的错误分类：`RedisError` 覆盖配置 / 连接 / 远端 / 序列化 / I/O / 超时 / 不支持，
  并提供 `is_retryable()`。
- 敏感信息保护：密码在 `Debug` 与端点展示中一律脱敏为 `***`。

## 安装

```bash
cargo add redisx
```

TLS 使用 rustls（webpki 根证书）；如需关闭默认的 Pub/Sub 支持：

```bash
cargo add redisx --no-default-features
```

## 最小可运行示例

```rust,no_run
use std::time::Duration;

use redisx::{RedisConfig, RedisPool, RedisResult};

#[tokio::main]
async fn main() -> RedisResult<()> {
    // 也可以用 RedisConfig::from_env() / from_toml(...) / builder()
    let config = RedisConfig::from_url("redis://127.0.0.1:6379")?;

    let pool = RedisPool::connect(config).await?;

    pool.set("hello", b"world".to_vec()).await?;
    let value = pool.get("hello").await?;
    assert_eq!(value.as_deref(), Some(b"world".as_slice()));

    pool.set_ex("session:1", b"payload".to_vec(), Duration::from_secs(60)).await?;
    let rtt = pool.ping().await?;
    println!("PING 往返 {:?}，端点 {}", rtt, pool.endpoint());

    // 需要重试时用客户端：只有只读/幂等命令会进入重试环
    let client = pool.client().with_retry(redisx::RetryConfig::default());
    let _ = client.get("hello").await?;

    pool.close(Duration::from_secs(1)).await?;
    Ok(())
}
```

## 三种拓扑

| 模式 | 连接方式 | 关键配置 | 说明 |
| --- | --- | --- | --- |
| `RedisMode::Standalone` | `ConnectionManager` | `addr` | 单机；断线自动重连，重连退避上限由 `reconnect_max_delay` 控制 |
| `RedisMode::Cluster` | `ClusterConnection` | `nodes` 或 `addr` | 种子节点发现拓扑，`MOVED`/`ASK` 由驱动跟随；`max_cluster_redirects` 限制重定向次数；**不支持非 0 逻辑库** |
| `RedisMode::Sentinel` | 哨兵发现 master + `ConnectionManager` | `nodes` + `sentinel_master` | 先向哨兵查询 master 地址，再以单机方式连接该 master；缺少 `sentinel_master` 时配置校验直接失败 |

```rust,no_run
use redisx::{RedisConfig, RedisMode, RedisPool};

async fn connect_cluster() -> redisx::RedisResult<RedisPool> {
    RedisPool::connect(
        RedisConfig::builder()
            .mode(RedisMode::Cluster)
            .nodes(["10.0.0.1:7000", "10.0.0.2:7000"])
            .connect_timeout(std::time::Duration::from_secs(2))
            .build()?,
    )
    .await
}
```

## Feature

| feature | 默认 | 说明 |
| --- | --- | --- |
| `pubsub` | ✅ | 提供 `RedisPubSub` / `RedisPubSubMessage`：独占订阅连接 + 独立 publish 连接，仅支持 Standalone（Cluster/Sentinel 在建连前 fail-closed），**不提供可靠投递** |

## 配置项

`RedisConfig` 可用 `builder()`、`from_url()`、`from_toml()` 或 `from_env()` 构造；TOML 键与
环境变量一一对应。

| TOML 键 | 环境变量 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `addr` | `FOUNDATIONX_REDISX_ADDR` | string | `127.0.0.1:6379` | 端点或种子 |
| `nodes` | `FOUNDATIONX_REDISX_NODES` | string / array | 空 | Cluster / Sentinel 种子，逗号分隔或 TOML 数组 |
| `sentinel_master` | `FOUNDATIONX_REDISX_SENTINEL_MASTER` | string | 无 | Sentinel 服务名（Sentinel 模式必填） |
| `username` | `FOUNDATIONX_REDISX_USERNAME` | string | `from_env` 默认 `default` | ACL 用户名 |
| `password` | `FOUNDATIONX_REDISX_PASSWORD` | string | 无 | 密码；`Debug` 脱敏 |
| `db` | `FOUNDATIONX_REDISX_DB` | integer | `0` | 逻辑库；Cluster 必须为 0 |
| `tls` | `FOUNDATIONX_REDISX_TLS` | boolean | `false` | 启用后强制证书校验，拒绝 insecure |
| `mode` | `FOUNDATIONX_REDISX_MODE` | string | `standalone` | `standalone` / `cluster` / `sentinel`（`single` 等价 standalone） |
| `connect_timeout_ms` | — | integer | `5000` | 建连超时 |
| `command_timeout_ms` | — | integer | `3000` | 单命令超时 |
| `acquire_timeout_ms` | — | integer | `3000` | 获取命令 lane 的超时 |
| `max_in_flight` | `FOUNDATIONX_REDISX_MAX_IN_FLIGHT` | integer | `256` | 最大并发命令数（命令 lane） |
| `client_name` | — | string | 无 | `CLIENT SETNAME` |
| `warmup_count` | `FOUNDATIONX_REDISX_WARMUP` | integer | `0` | 建池后预热 `PING` 次数 |
| `tcp_keepalive_ms` | — | integer | 无 | TCP keepalive 间隔 |
| `reconnect_max_delay_ms` | — | integer | `5000` | 重连退避上限 |
| `max_cluster_redirects` | — | integer | `16` | Cluster 重定向上限 |
| `blocking_timeout_ms` | `FOUNDATIONX_REDISX_BLOCKING_TIMEOUT_MS` | integer | `5000` | 阻塞命令默认等待上限 |
| — | `REDIS_URL` | string | 无 | 若设置则覆盖以上全部（`redis://` / `rediss://`） |

`RedisConfig::validate()`（构建时自动调用）会拒绝：空地址、Cluster 非 0 库、Sentinel 缺
master、负 `db`、`max_in_flight == 0`、任何为 0 的超时、`max_cluster_redirects == 0`、
insecure TLS 与 Unix socket URL。

## 重试语义

`RetryConfig` 支持指数退避、可选抖动与总 deadline：

```rust,no_run
use std::time::Duration;

use redisx::{RedisClient, RedisConfig, RetryConfig};

async fn connect_with_retry() -> redisx::RedisResult<RedisClient> {
    let client = RedisClient::connect(RedisConfig::from_env()?).await?;
    Ok(client.with_retry(
        RetryConfig::exponential(4, Duration::from_millis(50), Duration::from_secs(2))
            .with_deadline(Duration::from_secs(5)),
    ))
}
```

只有 `RedisRetrySafety::ReadOnly`（`GET` / `EXISTS` / `PTTL` / `MGET`）与
`RedisRetrySafety::Idempotent`（`SET` 无 TTL、`MSET`）会重试；
`DEL` / `PEXPIRE` / `SET`+TTL 属 `AmbiguousWrite`，`INCR` / `PUBLISH` 属 `NeverAutomatic`，
一律只执行一次。

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
