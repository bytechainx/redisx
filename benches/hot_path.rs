#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! redisx 热路径基准测试。
//!
//! 覆盖「配置构建 → 校验 → 端点展示脱敏 → 重试安全分类」的离线路径，
//! 不连接真实 Redis 服务。运行：`cargo bench`（加 `-- --quick` 缩减迭代数）。
use std::hint::black_box;
use std::time::Instant;

use redisx::{RedisConfig, RedisOperation, RedisRetrySafety};

fn iters() -> u32 {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--test") {
        // cargo test --all-targets 会以测试模式运行本二进制；只做冒烟。
        10
    } else if args.iter().any(|a| a == "--quick") {
        1_000
    } else {
        50_000
    }
}

fn main() {
    let n = iters();

    // 预热
    for _ in 0..3 {
        let config = RedisConfig::builder()
            .addr("127.0.0.1:6379")
            .build()
            .expect("build 应成功");
        black_box(config.validate().is_ok());
    }

    let config = RedisConfig::builder()
        .addr("127.0.0.1:6379")
        .client_name("redisx-bench")
        .build()
        .expect("build 应成功");

    let start = Instant::now();
    for i in 0..n {
        // 配置构建 + 校验（fail-fast 路径）
        let built = RedisConfig::builder()
            .addr("127.0.0.1:6379")
            .build()
            .expect("build 应成功");
        built.validate().expect("validate 应成功");
        black_box(built.addr());
        black_box(built.mode());
        // 端点展示（含脱敏路径）
        black_box(config.display_endpoint());
        black_box(config.has_password());
        // 重试安全分类（with_retry 的准入判定）
        black_box(RedisOperation::Get.retry_safety() == RedisRetrySafety::ReadOnly);
        black_box(i);
    }
    let elapsed = start.elapsed();
    println!(
        "bench_redisx_hot_path: iters={n} total={elapsed:?} per_iter={:?}",
        elapsed / n
    );
}
