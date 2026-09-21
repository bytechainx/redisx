//! 重试策略与命令重试安全分类（本 crate 自实现，不依赖外部可靠性框架）。
//!
//! - [`RetryConfig`]：指数退避 + 可选抖动 + 总 deadline；
//!   [`RetryConfig::backoff_for`] 与 [`RetryConfig::jittered`] 为纯函数，便于离线验证。
//! - [`with_retry`]：仅当 [`RedisError::is_retryable`] 为真且未超预算时重试。
//! - [`RedisOperation`] / [`RedisRetrySafety`] / [`RedisAtomicity`]：描述每个公开命令
//!   「能否自动重试」与「原子性边界」，[`RedisClient`](crate::RedisClient) 据此决定是否
//!   把命令放进重试环——副作用语义不明的写命令永远不会被自动重试。

use std::future::Future;
use std::time::{Duration, Instant};

use crate::error::{RedisError, RedisResult};

/// 命令的重试副作用分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedisRetrySafety {
    /// 只读命令；瞬时失败后可自动重试。
    ReadOnly,
    /// 固定输入的幂等写入；瞬时失败后可自动重试。
    Idempotent,
    /// 写命令的响应可能丢失；自动重试可能重复副作用，只能由调用方显式选择。
    AmbiguousWrite,
    /// 自动重试会破坏合同（例如 Pub/Sub 可能重复投递）。
    NeverAutomatic,
}

/// 命令的原子性边界。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedisAtomicity {
    /// 单条命令；服务端执行原子，但客户端超时不代表命令未生效。
    SingleCommand,
    /// 多 key 单条命令；仅在 Standalone 或 Cluster 同一 hash slot 内成立。
    MultiKeySingleSlot,
    /// 无可靠投递或事务原子性保证。
    None,
}

/// 公开命令枚举（用于查询重试与原子性合同）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedisOperation {
    /// `GET`。
    Get,
    /// `SET` / `PSETEX`。
    Set,
    /// `DEL`。
    Delete,
    /// `EXISTS`。
    Exists,
    /// `PEXPIRE`。
    Expire,
    /// `PTTL`。
    Ttl,
    /// `MGET`。
    Mget,
    /// `MSET`。
    Mset,
    /// `INCR` / `INCRBY`。
    Incr,
    /// `PUBLISH`。
    Publish,
}

impl RedisOperation {
    /// 该命令的自动重试安全分类。
    #[must_use]
    pub const fn retry_safety(self) -> RedisRetrySafety {
        match self {
            Self::Get | Self::Exists | Self::Ttl | Self::Mget => RedisRetrySafety::ReadOnly,
            Self::Mset => RedisRetrySafety::Idempotent,
            Self::Set | Self::Delete | Self::Expire => RedisRetrySafety::AmbiguousWrite,
            Self::Incr | Self::Publish => RedisRetrySafety::NeverAutomatic,
        }
    }

    /// 该命令的 Redis 服务端原子性边界。
    #[must_use]
    pub const fn atomicity(self) -> RedisAtomicity {
        match self {
            Self::Mget | Self::Mset => RedisAtomicity::MultiKeySingleSlot,
            Self::Publish => RedisAtomicity::None,
            Self::Get
            | Self::Set
            | Self::Delete
            | Self::Exists
            | Self::Expire
            | Self::Ttl
            | Self::Incr => RedisAtomicity::SingleCommand,
        }
    }

    /// 是否允许在配置重试后自动重试（只有只读与幂等写入允许）。
    #[must_use]
    pub const fn allows_automatic_retry(self) -> bool {
        matches!(
            self.retry_safety(),
            RedisRetrySafety::ReadOnly | RedisRetrySafety::Idempotent
        )
    }
}

/// 重试策略：指数退避 + 抖动 + 总 deadline。
///
/// 字段私有，通过 [`RetryConfig::fixed`] / [`RetryConfig::exponential`] 构造并用
/// `with_*` 方法微调；`max_attempts` 始终 ≥ 1。
#[derive(Debug, Clone, PartialEq)]
pub struct RetryConfig {
    max_attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    multiplier: f64,
    jitter: bool,
    deadline: Option<Duration>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self::exponential(3, Duration::from_millis(50), Duration::from_secs(2))
    }
}

impl RetryConfig {
    /// 固定间隔重试（`delay` 为每次失败后的等待）。
    #[must_use]
    pub fn fixed(max_attempts: u32, delay: Duration) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            initial_backoff: delay,
            max_backoff: delay,
            multiplier: 1.0,
            jitter: true,
            deadline: None,
        }
    }

    /// 指数退避重试：第 n 次失败后等待 `initial * multiplier^(n-1)`，上限 `max_backoff`。
    #[must_use]
    pub fn exponential(max_attempts: u32, initial: Duration, max_backoff: Duration) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            initial_backoff: initial.min(max_backoff),
            max_backoff,
            multiplier: 2.0,
            jitter: true,
            deadline: None,
        }
    }

    /// 设置退避倍率（非有限值或 ≤ 1 时保持 1.0，即固定间隔）。
    #[must_use]
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = if multiplier.is_finite() && multiplier > 1.0 {
            multiplier
        } else {
            1.0
        };
        self
    }

    /// 设置总重试 deadline（含首次尝试；耗尽后以 [`RedisError::Timeout`] 失败）。
    #[must_use]
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// 关闭抖动（退避完全确定，便于断言与调试）。
    #[must_use]
    pub fn without_jitter(mut self) -> Self {
        self.jitter = false;
        self
    }

    /// 最大尝试次数（含首次）。
    #[must_use]
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// 首次退避。
    #[must_use]
    pub fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    /// 退避上限。
    #[must_use]
    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    /// 退避倍率。
    #[must_use]
    pub fn multiplier(&self) -> f64 {
        self.multiplier
    }

    /// 是否启用抖动。
    #[must_use]
    pub fn jitter(&self) -> bool {
        self.jitter
    }

    /// 总 deadline（若配置）。
    #[must_use]
    pub fn deadline(&self) -> Option<Duration> {
        self.deadline
    }

    /// 第 `attempt` 次尝试失败后的确定性退避（不含抖动）。
    ///
    /// `attempt = 0` 返回零；`attempt = 1` 返回 `initial_backoff`；之后按倍率增长并以
    /// `max_backoff` 截断。
    #[must_use]
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }
        let cap = self.max_backoff.as_secs_f64();
        let mut delay = self.initial_backoff;
        let mut index = 1;
        while index < attempt {
            let scaled = delay.as_secs_f64() * self.multiplier;
            if !scaled.is_finite() || scaled >= cap {
                return self.max_backoff;
            }
            delay = Duration::from_secs_f64(scaled.max(0.0));
            index += 1;
        }
        delay.min(self.max_backoff)
    }

    /// 在确定性退避上叠加确定性抖动，结果落在 `[0.5 × backoff, backoff]`。
    ///
    /// 抖动因子只由 `seed` 决定，因此同一 `(backoff, seed)` 永远得到同一结果，可离线断言。
    #[must_use]
    pub fn jittered(backoff: Duration, seed: u64) -> Duration {
        // splitmix64：无需外部 RNG 依赖的确定性扰动
        let mut x = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^= x >> 31;
        let permille = (x % 1001) as u32;
        let factor = 0.5 + f64::from(permille) / 2000.0;
        backoff.mul_f64(factor)
    }

    /// 是否还应继续重试：尝试次数未用尽且未超出 deadline。
    #[must_use]
    fn should_continue(&self, attempt: u32, elapsed: Duration) -> bool {
        if attempt >= self.max_attempts {
            return false;
        }
        match self.deadline {
            Some(deadline) => elapsed < deadline,
            None => true,
        }
    }
}

/// 按 [`RetryConfig`] 执行命令，仅对可重试错误重试。
///
/// 语义要点：
/// - `max_attempts` 含首次尝试；
/// - 错误不可重试（[`RedisError::is_retryable`] 为假）时立刻返回，不做无意义等待；
/// - 配置了 deadline 时，等待被裁剪到剩余预算，预算耗尽返回 [`RedisError::Timeout`]；
/// - 抖动关闭时退避完全确定，便于测试。
///
/// # Errors
///
/// 命令在预算内成功前，返回最后一次失败的错误；deadline 耗尽时返回
/// [`RedisError::Timeout`]。
pub async fn with_retry<T, F, Fut>(config: &RetryConfig, op: &str, mut f: F) -> RedisResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = RedisResult<T>>,
{
    let start = Instant::now();
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match f().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if !err.is_retryable() {
                    tracing::debug!(op, attempt, error = %err, "命令错误不可重试，直接返回");
                    return Err(err);
                }
                if !config.should_continue(attempt, start.elapsed()) {
                    tracing::debug!(op, attempt, "重试预算耗尽");
                    return Err(err);
                }
                let mut backoff = config.backoff_for(attempt);
                if config.jitter {
                    backoff = RetryConfig::jittered(backoff, jitter_seed(attempt, op));
                }
                if let Some(deadline) = config.deadline {
                    let remaining = deadline.saturating_sub(start.elapsed());
                    if remaining.is_zero() {
                        return Err(RedisError::Timeout(format!(
                            "{op} 重试超出总 deadline（已尝试 {attempt} 次）"
                        )));
                    }
                    backoff = backoff.min(remaining);
                }
                tracing::debug!(op, attempt, delay_ms = backoff.as_millis(), "退避后重试");
                if !backoff.is_zero() {
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

/// 以「操作名 + 尝试序号」派生抖动种子，保证同参数下可复现。
#[must_use]
fn jitter_seed(attempt: u32, op: &str) -> u64 {
    let mut seed = u64::from(attempt);
    for byte in op.as_bytes() {
        seed = seed.wrapping_mul(31).wrapping_add(u64::from(*byte));
    }
    seed
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn backoff_is_exponential_and_capped() {
        let cfg = RetryConfig::exponential(5, Duration::from_millis(10), Duration::from_millis(40));
        assert_eq!(cfg.backoff_for(0), Duration::ZERO);
        assert_eq!(cfg.backoff_for(1), Duration::from_millis(10));
        assert_eq!(cfg.backoff_for(2), Duration::from_millis(20));
        assert_eq!(cfg.backoff_for(3), Duration::from_millis(40));
        assert_eq!(cfg.backoff_for(4), Duration::from_millis(40));
        assert_eq!(cfg.backoff_for(64), Duration::from_millis(40));
    }

    #[test]
    fn fixed_backoff_is_constant() {
        let cfg = RetryConfig::fixed(3, Duration::from_millis(7));
        assert_eq!(cfg.multiplier(), 1.0);
        assert_eq!(cfg.backoff_for(1), Duration::from_millis(7));
        assert_eq!(cfg.backoff_for(9), Duration::from_millis(7));
    }

    #[test]
    fn custom_multiplier_and_invalid_multiplier() {
        let cfg = RetryConfig::exponential(4, Duration::from_millis(10), Duration::from_secs(10))
            .with_multiplier(3.0);
        assert_eq!(cfg.backoff_for(1), Duration::from_millis(10));
        assert_eq!(cfg.backoff_for(2), Duration::from_millis(30));
        assert_eq!(cfg.backoff_for(3), Duration::from_millis(90));

        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert_eq!(
                cfg.clone().with_multiplier(bad).multiplier(),
                1.0,
                "bad={bad}"
            );
        }
    }

    #[test]
    fn jitter_is_deterministic_and_bounded() {
        let backoff = Duration::from_millis(1000);
        for seed in [0_u64, 1, 7, 42, u64::MAX] {
            let first = RetryConfig::jittered(backoff, seed);
            let second = RetryConfig::jittered(backoff, seed);
            assert_eq!(first, second, "seed={seed} 必须可复现");
            assert!(first <= backoff, "抖动不得放大退避: {first:?}");
            assert!(
                first >= Duration::from_millis(500),
                "抖动下限 0.5×: {first:?}"
            );
        }
        assert_ne!(
            RetryConfig::jittered(backoff, 1),
            RetryConfig::jittered(backoff, 2)
        );
    }

    #[test]
    fn jitter_seed_varies_with_operation() {
        let backoff = Duration::from_millis(1000);
        assert_ne!(
            RetryConfig::jittered(backoff, jitter_seed(1, "redis.get")),
            RetryConfig::jittered(backoff, jitter_seed(1, "redis.mget"))
        );
        assert_eq!(jitter_seed(2, "redis.get"), jitter_seed(2, "redis.get"));
        assert_ne!(jitter_seed(1, "redis.get"), jitter_seed(2, "redis.get"));
    }

    #[tokio::test]
    async fn retries_transient_until_success() {
        let cfg = RetryConfig::fixed(3, Duration::from_millis(1)).without_jitter();
        let calls = AtomicU32::new(0);
        let value = with_retry(&cfg, "redis.get", || {
            let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if attempt < 3 {
                    Err(RedisError::Transient("loading".to_owned()))
                } else {
                    Ok(attempt)
                }
            }
        })
        .await
        .expect("第三次成功");
        assert_eq!(value, 3);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn non_retryable_error_is_attempted_once() {
        let cfg = RetryConfig::fixed(5, Duration::from_millis(1)).without_jitter();
        let calls = AtomicU32::new(0);
        let err = with_retry::<(), _, _>(&cfg, "redis.set", || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(RedisError::Config("bad".to_owned())) }
        })
        .await
        .expect_err("不可重试");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(err, RedisError::Config(_)));
    }

    #[tokio::test]
    async fn attempts_are_bounded() {
        let cfg = RetryConfig::fixed(2, Duration::from_millis(1)).without_jitter();
        let calls = AtomicU32::new(0);
        let err = with_retry::<(), _, _>(&cfg, "redis.get", || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(RedisError::Connection("refused".to_owned())) }
        })
        .await
        .expect_err("耗尽尝试");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(matches!(err, RedisError::Connection(_)));
    }

    #[tokio::test]
    async fn zero_attempts_fails_before_driver() {
        let cfg = RetryConfig::fixed(0, Duration::from_millis(1));
        assert_eq!(cfg.max_attempts(), 1, "max_attempts 至少为 1");
        let calls = AtomicU32::new(0);
        let value = with_retry(&cfg, "redis.get", || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, RedisError>(1) }
        })
        .await
        .expect("单次成功");
        assert_eq!(value, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deadline_bounds_total_retry_time() {
        let cfg = RetryConfig::fixed(100, Duration::from_millis(50))
            .without_jitter()
            .with_deadline(Duration::from_millis(60));
        let calls = AtomicU32::new(0);
        let err = with_retry::<(), _, _>(&cfg, "redis.get", || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(RedisError::Timeout("slow".to_owned())) }
        })
        .await
        .expect_err("deadline 耗尽");
        assert!(matches!(
            err,
            RedisError::Timeout(_) | RedisError::Connection(_)
        ));
        assert!(
            calls.load(Ordering::SeqCst) <= 3,
            "calls={}",
            calls.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn operation_contract_matrix() {
        for op in [
            RedisOperation::Get,
            RedisOperation::Exists,
            RedisOperation::Ttl,
            RedisOperation::Mget,
        ] {
            assert_eq!(op.retry_safety(), RedisRetrySafety::ReadOnly, "op={op:?}");
            assert!(op.allows_automatic_retry(), "op={op:?}");
        }
        assert_eq!(
            RedisOperation::Mset.retry_safety(),
            RedisRetrySafety::Idempotent
        );
        assert!(RedisOperation::Mset.allows_automatic_retry());

        for op in [
            RedisOperation::Set,
            RedisOperation::Delete,
            RedisOperation::Expire,
            RedisOperation::Incr,
            RedisOperation::Publish,
        ] {
            assert!(!op.allows_automatic_retry(), "op={op:?} 不得自动重试");
        }
        assert_eq!(
            RedisOperation::Set.retry_safety(),
            RedisRetrySafety::AmbiguousWrite
        );
        assert_eq!(
            RedisOperation::Incr.retry_safety(),
            RedisRetrySafety::NeverAutomatic
        );
        assert_eq!(
            RedisOperation::Publish.retry_safety(),
            RedisRetrySafety::NeverAutomatic
        );
    }

    #[test]
    fn atomicity_contract_matrix() {
        assert_eq!(
            RedisOperation::Get.atomicity(),
            RedisAtomicity::SingleCommand
        );
        assert_eq!(
            RedisOperation::Set.atomicity(),
            RedisAtomicity::SingleCommand
        );
        assert_eq!(
            RedisOperation::Mset.atomicity(),
            RedisAtomicity::MultiKeySingleSlot
        );
        assert_eq!(
            RedisOperation::Mget.atomicity(),
            RedisAtomicity::MultiKeySingleSlot
        );
        assert_eq!(RedisOperation::Publish.atomicity(), RedisAtomicity::None);
    }

    #[test]
    fn default_and_accessors() {
        let cfg = RetryConfig::default();
        assert_eq!(cfg.max_attempts(), 3);
        assert_eq!(cfg.initial_backoff(), Duration::from_millis(50));
        assert_eq!(cfg.max_backoff(), Duration::from_secs(2));
        assert!(cfg.jitter());
        assert!(cfg.deadline().is_none());
        assert!(!cfg.clone().without_jitter().jitter());
        assert_eq!(
            cfg.with_deadline(Duration::from_secs(1)).deadline(),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            RetryConfig::fixed(1, Duration::ZERO).backoff_for(1),
            Duration::ZERO
        );
    }
}
