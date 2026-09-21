# CONTRIBUTING.md — 贡献指南（redisx）

本文件面向贡献者，汇总本地门禁与提交约定。
AI Agent 的工作约定另见 [`AGENTS.md`](./AGENTS.md)；术语与领域语言见 [`CONTEXT.md`](./CONTEXT.md)。

## 开发流程

- 本仓库是**独立的单 crate 仓库**，不依赖 `xhyper.rs` 主工程及其内部 crate（`kernel` /
  `contracts` 等），只依赖 crates.io 公开包。
- substantial 变更走 feature branch → PR → review → merge，**禁止直接 push `main`**。
- `main` 已启用分支保护：要求 PR + 必需检查 `fmt / clippy / test`，
  `required_approving_review_count = 0`（单人也能合并），禁止强推与删除。
- 合并方式固定为 **create a merge commit**。注意仓库设置是
  `merge_commit_title = MERGE_MESSAGE` + `merge_commit_message = PR_TITLE`，因此
  `gh pr merge` 必须显式传 `--subject` 与 `--body`，否则会产出通用
  `Merge pull request #N from …` 标题。
- 提交信息遵循 Conventional Commits（`feat:` / `fix:` / `docs:` / `ci:` / `chore:` /
  `refactor:`），描述用简体中文。

## 本地门禁（P0 三件套）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

本仓库唯一可选 feature 为 `pubsub`（**默认开启**），上述命令原样执行即可（`--all-targets`
已覆盖 tests 与 benches）。全部测试离线运行，不依赖真实 Redis 服务；连接失败路径统一用
不可达地址验证。

元数据完整性门禁（**不发布 crates.io**，此命令只校验打包元数据）：

```bash
cargo package --no-verify --allow-dirty
```

## 复用口径（不发布 crates.io）

- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用。
- 文档与元数据中不得出现「可独立发布」「可直接 `cargo publish`」等表述，
  也不得放置 crates.io / docs.rs 徽章与外链。
- `Cargo.toml` 的 `documentation` 指向 `https://github.com/bytechainx/redisx#readme`。
- 消费方引入方式（README「安装」小节为准）：

  ```toml
  [dependencies]
  redisx = { git = "https://github.com/bytechainx/redisx" }
  ```

## 开发约定

- 注释、文档、错误消息使用**简体中文**；标识符保持英文。
- 错误类型：`thiserror` 枚举 + `#[non_exhaustive]` + `pub type RedisResult<T>` 别名，
  并保留 `is_retryable()` 分类。
- 不在库代码里裸 `unwrap()`（`[lints.clippy]` 已 `deny` `unwrap_used` / `expect_used` / `panic`，
  测试模块经 `#![cfg_attr(test, allow(...))]` 放宽）。
- 所有 `pub` 项必须有中文 `///` 文档（`missing_docs` 已 `deny`），`unsafe_code` 已 `forbid`。
- 集成测试**必须离线运行**，不触碰真实网络。
- **拓扑边界**：`Cluster` 不支持非 0 逻辑库；`Sentinel` 必须提供 `sentinel_master`，
  否则 `validate()` 直接失败。
- **重试安全分类必填**：新增命令必须先归类 `RedisOperation`，经 `retry_safety()` 得到
  `ReadOnly` / `Idempotent` / `AmbiguousWrite` / `NeverAutomatic`；结果不明或非幂等命令
  永远只执行一次。
- **秘密脱敏**：密码在 `Debug` 与 `display_endpoint()` 中脱敏；`from_toml()` 拒绝明文密码，
  密码只能经环境变量或 builder（含 `password_from_provider`）注入。
- **有界背压**：连接池以 `max_in_flight` + `acquire_timeout` 限流，禁止引入无界队列；
  所有外部调用必须有 timeout。
- edition 2021，MSRV `rust-version = "1.75"`（改动依赖时同步核对）。

## 提交前自检清单

- [ ] `cargo fmt --all -- --check` 通过
- [ ] `cargo clippy --all-targets -- -D warnings` 通过
- [ ] `cargo test --all-targets` 通过
- [ ] `cargo package --no-verify --allow-dirty` 通过
- [ ] 新增 `pub` 项都有中文 `///` 文档
- [ ] 文档中无「可独立发布」/ crates.io / docs.rs 表述
- [ ] 新增命令已在 `RedisOperation` 中归类，未引入自动重试结果不明的写命令
- [ ] 未引入无界队列，新增外部调用都带 timeout
