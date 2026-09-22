# redisx Agent 指南

> 本文件为 AI Agent 在本仓库工作时的入口指南。

## 项目定位

生产可用的异步 Redis 适配器：单一 crate 内提供三种拓扑（Standalone / Cluster / Sentinel）、连接池背压、会话级超时/重试、核心数据结构与 Streams 原语。

## 技术栈

- Rust edition 2021, rust-version 1.75
- 关键依赖: `redis` 0.27（tokio-comp / connection-manager / cluster-async / sentinel / rustls TLS）、`tokio`、`thiserror`、`serde`、`serde_json`、`toml`、`tracing`、`url`、`bytes`、`futures-*`
- feature `pubsub`（默认开启）：原始 Pub/Sub 会话（`RedisPubSub` / `RedisPubSubMessage`）
- 零内部耦合，不依赖 kernel/contracts 等私有 crate

## 代码结构

```text
src/
├── lib.rs        # 入口：模块声明 + 受控 re-export
├── client.rs     # RedisClient：KV、Hash/List/Set/ZSet、Streams、事务、Lua、分布式锁
├── config.rs     # RedisConfig 门面：RedisMode / RedisConfig 定义 + ENV_* 常量、
│                 # Default/Debug、validate、连接信息构造、内联测试
├── config/
│   ├── accessors.rs # RedisConfig 的只读访问器（addr/nodes/db/tls/mode/... 与 timeout 系列）
│   ├── builder.rs   # RedisConfigBuilder（链式覆盖）
│   ├── parse.rs     # 解析与校验辅助（host:port、mode、seed 脱敏）
│   └── wire.rs      # TOML / serde wire 形态与 apply_wire 字段映射
├── error.rs      # RedisError（is_retryable）/ RedisResult
├── error_map.rs  # map_redis_error / map_redis_result 错误映射
├── ext.rs        # RedisLock / generate_lock_token / lock_token_matches
├── pool.rs       # RedisPool 门面：RedisPoolStats/MetricsSnapshot/Health、RedisPoolPermit、
│                 # 契约方法 + 内联测试
├── pool/
│   ├── backend.rs   # RedisBackend（ConnectionLike 实现）与 connection_manager_config
│   ├── connect.rs   # connect_standalone / connect_cluster / connect_sentinel
│   ├── kv.rs        # 单命令原语集合（被 Pool / Permit / Client 共用）
│   ├── lifecycle.rs # connect / new / connect_from_env 与 from_parts / acquire_with_timeout
│   └── permit.rs    # RedisPoolPermit 的执行与 deadline 语义
├── pubsub.rs     # RedisPubSub / RedisPubSubMessage（feature = "pubsub"）
├── resilience.rs # RetryConfig / with_retry / RedisOperation / RedisRetrySafety / RedisAtomicity
├── streams.rs    # StreamEntry
├── structures.rs # 核心数据结构命令封装
└── transaction.rs# TxCmd 事务命令

tests/            # config_and_env.rs · public_api.rs · pure_behavior.rs · unreachable_connect.rs
benches/          # hot_path.rs（harness = false，离线基准）
docs/             # API.md · 标准.md
```

## 设计约定（改代码前必读）

- **拓扑边界**：Cluster 不支持非 0 逻辑库；Sentinel 必须提供 `sentinel_master`，否则 `validate()` 直接失败。
- **重试安全分类**：`with_retry` 只在命令的 `RedisRetrySafety` 为只读或幂等时进入重试环；`SET`（带 TTL）、`DEL`、`PEXPIRE`、`INCR`、`PUBLISH` 等结果不明或非幂等命令永远只执行一次。新增命令时必须先归类 `RedisOperation`。
- **秘密脱敏**：密码在 `Debug` 与 `display_endpoint()` 中脱敏；`from_toml()` 拒绝明文密码，密码只能经 env 或 builder 注入。
- **有界背压**：连接池以 `max_in_flight` + `acquire_timeout` 限流，禁止引入无界队列。
- **离线可测**：单元测试与 bench 不依赖真实 Redis；连接行为测试用不可达地址验证失败路径（`tests/unreachable_connect.rs`）。

## 开发约定

- 注释与文档使用简体中文；标识符保持英文
- 错误类型：thiserror 枚举 + `#[non_exhaustive]` + `pub type RedisResult<T>`；保留 `is_retryable` 分类
- 配置：`RedisConfig` 结构体 + `builder()`/`from_env()`/`from_toml()`/`from_url()` + `validate()` + fail-fast
- 禁止裸 `unwrap()`（库代码）/ 无注释 `expect()`；测试模块经 `#![cfg_attr(test, allow(...))]` 放宽
- 异步代码使用 tokio，禁止在 async 中做阻塞 I/O；外部调用必须有 timeout
- `#![forbid(unsafe_code)]`、`#![deny(missing_docs)]` 已开启：新增 pub 项必须带中文 `///` 文档

## 门禁三件套（P0）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

基准（离线，不需要 Redis 服务，可选）：

```bash
cargo bench            # 完整 50_000 次迭代
cargo bench -- --quick # 快速 1_000 次迭代
```

## 相关文档

- 组织 Rust 规范：`~/org-config/rulesets/rust/RULES.md`
- API 文档：`docs/API.md`
- 标准与验收：`docs/标准.md`
- 术语与领域语言：`CONTEXT.md`
- 贡献指南：`CONTRIBUTING.md`
- 变更记录：`CHANGELOG.md`
- 基准测试：`benches/hot_path.rs`
