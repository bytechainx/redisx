# redisx 候选改动清单（2026-11-15）

> 来源：workspace `specs/repos/infra/redisx/tasks.md` T101。只产出清单，不改代码。
> 现行行为正本：`docs/标准.md`（自称 v0.1.0）+ `docs/API.md` + 源码；Cargo `[package] version` = `0.1.2`。
> 基线：`origin/main` `f7b912a9`。

## 候选项

| # | 候选改动 | 依据 | 影响面 | 建议 |
|---|---------|------|--------|------|
| C1 | `AGENTS.md` 技术栈行 `rust-version 1.75` 与 `Cargo.toml` `rust-version = "1.88"` 不一致 | `Cargo.toml:5`；MSRV 由 `0c73513` 上调；`AGENTS.md:11` | 文档漂移，误导贡献者按旧 MSRV 开发 | 改为 1.88，随下一文档 MR 合入 |
| C2 | `docs/标准.md` 文首 `redisx` v0.1.0 与 Cargo `0.1.2` 差额无说明 | `docs/标准.md:3`；`Cargo.toml:3`；workspace 侧已按「文首自称版本 ≠ Cargo 版本」登记（`STATUS.md` 诚实表） | 读者可能误读版本；workspace 已容忍，不阻塞 | 二选一：文首补注「文档版本」语义，或随版本治理约定对齐；需 crate Owner 裁定口径 |
| C3 | `docs/标准.md` §5 测试清单只列 4 个离线测试文件，未提 `aidd_boundary.rs` / `sdd_spec.rs` / `tdd_contracts.rs` / `e2e_redis.rs` / `live_redis.rs` | `docs/标准.md:37`；`tests/` 实有 9 个测试目标 | 「至少覆盖」语义下不算违约，但分层全貌不完整 | 可选：补一句分层索引指向各测试文件职责，不改验收口径 |
| C4 | 依赖漏洞：`cargo deny`（advisories）报错多条 2026-11 新增 RUSTSEC（rustls 0346–0349、rustls-webpki 0382、tokio 0386、webpki-roots 0381），PR #17 的 `cargo deny` job 失败而 `main` 2026-10-09 的同锁文件 CI 为 success——系公告库随时间更新所致，非本次文档改动引入 | PR #17 check `cargo deny` 失败日志；`gh run list --branch main` | 任何新 PR 的 `cargo deny` 门禁都会红，直至升级依赖或登记例外 | **已处置**：PR #18 以 `cargo update -p` 最小升级（rustls 0.23.46、rustls-webpki 0.103.17、tokio 1.54.1、webpki-roots 1.0.12），三项 CI 全绿，待 Owner 评审合入；合入后本 PR 需同步 main 复绿 |

## 已核对无候选

- 依赖清单：`docs/标准.md` 文首 11 项依赖与 `Cargo.toml [dependencies]` 一致（`thiserror = "2"`、`toml = "0.8"` 等主版本相符）。
- 拓扑边界、重试安全分类、秘密脱敏、有界背压、离线可测：抽查 `src/config.rs` / `src/pool.rs` / `src/resilience.rs` 结构与 `docs/标准.md` §2–§4 一致，无新缺口证据。
- E2E 层：`tests/e2e_redis.rs` 已在 `origin/main`（workspace P24 判据满足）。

## 边界

- 本清单不构成实现授权；C1–C3 的采纳与排期由 crate Owner 经 MR 流程裁定（对应 workspace `tasks.md` T102 / T103）。
- 未执行 `cargo` 门禁；本清单为静态对照产物。
