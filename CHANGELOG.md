# Changelog — redisx

本文件记录 `redisx` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/platform/drivers/redis` 抽取而来（抽取时点为 `0.3.27`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

### 变更

- **`RedisOperation::Mset` 重试分类由 `Idempotent` 改为 `AmbiguousWrite`**：
  MSET 与 SET 同为固定值写入，写入结果未知（超时/断连）时自动重试可能覆盖
  并发写入者的中间值；统一为保守分类后矩阵内不再有同类命令分类漂移，
  `RedisClient::mset()` 不再自动重试（Wave 2 审查 follow-up）。
- **`RedisClient::set()` 不再走硬编码 Idempotent 重试捷径**，重试分类改经
  `RedisOperation::Set` 安全矩阵单一来源（`AmbiguousWrite`）：SET 结果未知
  （超时/断连）时不再自动重试，与 `RedisPool::set()` 及矩阵路径行为一致
  （对抗审查 P1-2）。变更前 client 路径会静默自动重试，存在调用方不知情下
  覆盖并发写入者中间值的风险。迁移：确需 SET 自动重试的调用方请自行判定
  幂等性并显式包装重试。

## [0.1.2] - 2026-09-22

### 新增

- 三类合规测试（特性 002）：
  - `tests/tdd_contracts.rs`：公开接口契约全部 12 个入口的行为契约与 `// TDD-PROBE:` 红绿表（变异探测见 PR 描述）；
  - `tests/sdd_spec.rs`：`docs/标准.md` §1–§5 章节的 `// SPEC-MAP:` 1:1 可执行对照；
  - `tests/aidd_boundary.rs`：8 条对抗/边界用例与 `// AIDD:` 人工复核表。
- `tests/live_redis.rs`：真实 Redis 的 live 用例（建连 / 结构化探活 / 唯一名 key 的 SET-GET-DEL 与 TTL 往返 + 清理 / close 收尾），默认 `#[ignore]`，凭据只读环境变量，运行方式见 `scripts/live/README.md`。

### 变更

- **内部结构改写（公开 API 与可观察契约均不变）**：按 `docs/module-rules.md` §5.5 的手法，把
  `src/pool.rs` 与 `src/config.rs` 两处超长门面继续下沉 ——
  池侧：连接后端与 `ConnectionManager` 参数 → `src/pool/backend.rs`、建连/构造/并发许可获取 →
  `src/pool/lifecycle.rs`；配置侧：只读访问器 → `src/config/accessors.rs`、
  wire 形态与字段映射 → `src/config/wire.rs`。两个门面只保留模块文档、类型定义、`Debug`/`Default`、
  其余方法（含 `validate` / `close` / `subscribe` / 连接信息构造）与**原有内联测试**。
  `RedisBackend` / `connection_manager_config` 经门面 `pub(crate) use` 转出，故
  `src/client.rs` 与各 `pool/*.rs` 的引用路径不变。**可见性调整仅限 crate 内部**：
  `acquire_with_timeout`、`apply_wire`、`RedisConfigWire` 提为 `pub(super)`。
  `src/pool.rs` 生产段 **665 → 441**、`src/config.rs` 生产段 **662 → 327**。
  动机：`module-rules` 是元仓库必需检查，且它审计各仓**默认分支**，故当两处距
  `MR-STRUCT-007` 的 800 行 ERROR 阈值只剩 135 / 138 行时，任一仓的任意改动都可能卡住
  元仓库的全部 PR。属**纯搬移**（行多重集比对确认零代码行丢失），全部 137 项测试与
  doctest 结果不变。

## [0.1.1] - 2026-09-22

### 修正

- `RedisConfig::from_toml` 现在拒绝非空 `password`：原行为会把 TOML 明文凭据带进配置，
  与 `docs/标准.md` §2「凭据只能经环境变量或 builder 注入」不符。属**实现向契约靠拢**的行为收紧
  （PATCH）；`Debug` 与 `display_endpoint()` 的脱敏不变。
- `from_toml` 的 TOML 解析错误消息改用 `toml::de::Error::message()`，不再包含出错源码行
  （原 `Display` 会把 `password = "…"` 整行带进 `RedisError::Config`，与「密码永不进入日志」相悖）。

## [0.1.0] - 2026-09-21

### 新增

- 以单一 crate 形式提供三种拓扑：`RedisMode::{Standalone, Cluster, Sentinel}`，
  分别走 `ConnectionManager`、`ClusterConnection` 与哨兵发现 + `ConnectionManager`。
- 配置 `RedisConfig` / `RedisConfigBuilder`：`from_env` / `from_toml` / `from_url` /
  `builder` 四入口等价，构造期 `validate()` fail-fast，公开 `ENV_*` 常量。
- 连接池 `RedisPool`（`connect` / `new` / `acquire` / `ping` / `health_check` / `stats` /
  `metrics_snapshot` / `close`）与 `RedisPoolPermit` / `RedisPoolStats` /
  `RedisMetricsSnapshot` / `RedisHealth`；以 `max_in_flight` + `acquire_timeout`
  构成有界背压，`close(timeout)` 优雅排空在途命令。
- 命令客户端 `RedisClient`：KV、Hash/List/Set/ZSet、Streams、`MULTI/EXEC` 事务、Lua 脚本、
  分布式锁。
- 分布式锁 `RedisLock` 与 `generate_lock_token` / `lock_token_matches`：带 token 所有权校验。
- Streams 原语 `StreamEntry` 与事务命令 `TxCmd`。
- 重试 `RetryConfig` / `with_retry`：指数退避 + 抖动 + 总 deadline，并新增副作用安全分类
  `RedisOperation` / `RedisRetrySafety` / `RedisAtomicity`（`retry_safety()` / `atomicity()`
  为 `const fn`）。
- 统一错误 `RedisError`（含 `is_retryable()`）/ `RedisResult` 及上游错误映射
  `map_redis_error` / `map_redis_result`。
- 可选 feature `pubsub`（本版本**默认开启**）：原始 Pub/Sub 会话 `RedisPubSub` /
  `RedisPubSubMessage`。

### 变更

- 相对 `xhyper.rs` 源模块的破坏性变更：
  - 移除对内部 crate `kernel`、`resiliencx` 的依赖；错误模型下沉为 crate 内 `src/error.rs`
    的 `RedisError` / `RedisResult`。
  - 重试从外部可靠性框架改为 crate 内 `src/resilience.rs` 自实现，并新增按副作用准入的
    安全分类（`RedisRetrySafety` 等）。
  - `pubsub` 由源模块的非默认 feature 改为**默认开启**；源模块的 `runtime-tokio` / `live`
    feature 未保留。
  - 基准从源模块的 `kv_hot_path` / `api_matrix` 收敛为单个 `benches/hot_path.rs`。
- 不再随 crate 提供源模块的 `selfcheck` 与 `time_storage` 模块。

### 说明

- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用；`cargo package`
  只作元数据完整性校验。
- 提供的是 Redis 协议内的连接、命令、池化与重试治理；**不提供** Redis 服务端、
  缓存语义层（如 stale-while-revalidate）、跨 shard 事务或二级索引。
- `Cluster` 不支持非 0 逻辑库；`Sentinel` 必须提供 `sentinel_master`，否则 `validate()` 失败。
- TLS 经 rustls（webpki 根证书），不依赖 OpenSSL。
