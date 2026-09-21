//! `MULTI`/`EXEC` 事务命令封装。
//!
//! 事务内命令以 Pipeline 的 atomic 模式一次性提交，服务端保证 `EXEC` 内的命令连续执行且不被
//! 其它客户端命令插入。Cluster 跨 slot **不**承诺原子性，调用方须保证相关 key 落在同一 hash
//! slot（可用 hash tag）。

use crate::client::RedisClient;
use crate::error::{RedisError, RedisResult};
use crate::error_map::map_redis_result;

/// 事务内排队的一条命令。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TxCmd {
    /// `SET key value`（无 TTL）。
    Set {
        /// 键。
        key: String,
        /// 值。
        value: Vec<u8>,
    },
    /// `DEL key`。
    Del {
        /// 键。
        key: String,
    },
    /// `INCR key`。
    Incr {
        /// 键。
        key: String,
    },
}

impl TxCmd {
    /// 构造 `SET key value`。
    #[must_use]
    pub fn set(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self::Set {
            key: key.into(),
            value: value.into(),
        }
    }

    /// 构造 `DEL key`。
    #[must_use]
    pub fn del(key: impl Into<String>) -> Self {
        Self::Del { key: key.into() }
    }

    /// 构造 `INCR key`。
    #[must_use]
    pub fn incr(key: impl Into<String>) -> Self {
        Self::Incr { key: key.into() }
    }

    /// 该命令对应的 Redis 命令名。
    #[must_use]
    pub const fn command_name(&self) -> &'static str {
        match self {
            Self::Set { .. } => "SET",
            Self::Del { .. } => "DEL",
            Self::Incr { .. } => "INCR",
        }
    }

    /// 该命令涉及的 key。
    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::Set { key, .. } | Self::Del { key } | Self::Incr { key } => key,
        }
    }
}

impl RedisClient {
    /// `MULTI` → 排队命令 → `EXEC`；返回各命令的原始 [`redis::Value`]。
    ///
    /// # Errors
    ///
    /// `cmds` 为空返回 [`RedisError::Config`]；其余为连接/协议/超时错误
    /// （`EXECABORT` 映射为 [`RedisError::Conflict`]）。
    pub async fn multi_exec(&self, cmds: &[TxCmd]) -> RedisResult<Vec<redis::Value>> {
        if cmds.is_empty() {
            return Err(RedisError::Config("MULTI/EXEC 至少需要一条命令".to_owned()));
        }
        let cmds = cmds.to_vec();
        self.with_pool_conn(move |mut conn| async move {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for command in &cmds {
                match command {
                    TxCmd::Set { key, value } => {
                        pipe.cmd("SET").arg(key).arg(value.as_slice());
                    }
                    TxCmd::Del { key } => {
                        pipe.cmd("DEL").arg(key);
                    }
                    TxCmd::Incr { key } => {
                        pipe.cmd("INCR").arg(key);
                    }
                }
            }
            map_redis_result(pipe.query_async(&mut conn).await)
        })
        .await
    }

    /// 便捷入口：事务内 `SET` 多个 key（无 TTL）并 `EXEC`；入参为空时直接返回。
    ///
    /// # Errors
    ///
    /// 同 [`RedisClient::multi_exec`]。
    pub async fn multi_set(&self, items: &[(&str, &[u8])]) -> RedisResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        let cmds: Vec<TxCmd> = items
            .iter()
            .map(|(key, value)| TxCmd::set(*key, (*value).to_vec()))
            .collect();
        self.multi_exec(&cmds).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::RedisPool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn tx_cmd_constructors_and_accessors() {
        let set = TxCmd::set("k", b"v".to_vec());
        assert_eq!(
            set,
            TxCmd::Set {
                key: "k".to_owned(),
                value: b"v".to_vec()
            }
        );
        assert_eq!(set.command_name(), "SET");
        assert_eq!(set.key(), "k");

        let del = TxCmd::del("k");
        assert_eq!(del.command_name(), "DEL");
        assert_eq!(del.key(), "k");

        let incr = TxCmd::incr(String::from("counter"));
        assert_eq!(incr.command_name(), "INCR");
        assert_eq!(incr.key(), "counter");

        assert_eq!(TxCmd::set("k", b"v".to_vec()).clone(), set);
    }

    #[tokio::test]
    async fn multi_exec_requires_commands() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = RedisPool::test_probe(calls.clone()).client();
        let err = client.multi_exec(&[]).await.expect_err("空事务");
        assert!(matches!(err, RedisError::Config(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn multi_set_empty_is_noop_and_non_empty_reaches_driver() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = RedisPool::test_probe(calls.clone()).client();
        client.multi_set(&[]).await.expect("空 MSET");
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let err = client
            .multi_exec(&[TxCmd::set("k", b"v".to_vec())])
            .await
            .expect_err("probe");
        assert!(matches!(err, RedisError::Connection(_)), "{err}");

        let err = client
            .multi_exec(&[
                TxCmd::del("a"),
                TxCmd::incr("b"),
                TxCmd::set("c", b"1".to_vec()),
            ])
            .await
            .expect_err("probe");
        assert!(matches!(err, RedisError::Connection(_)));

        let _ = client.multi_set(&[("a", b"1".as_slice())]).await;
        assert!(calls.load(Ordering::SeqCst) >= 3);
    }
}
