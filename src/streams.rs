//! Redis Streams 一等 API（可靠消息通道；Pub/Sub 的可达性**不**由本模块保证）。
//!
//! 覆盖 `XADD` / `XLEN` / `XRANGE` / `XREAD` / `XREAD BLOCK` / `XDEL` / `XACK`。
//! 消费组管理（`XGROUP` / `XREADGROUP`）未在本 crate 内提供，因此 `XACK` 需由调用方先用其他
//! 手段建立消费组后使用。

use std::time::Duration;

use crate::client::RedisClient;
use crate::error::{RedisError, RedisResult};
use crate::error_map::map_redis_result;

/// Stream 条目。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    /// Redis stream ID（形如 `1710000000000-0`）。
    pub id: String,
    /// field → value（value 保持原始字节）。
    pub fields: Vec<(String, Vec<u8>)>,
}

impl StreamEntry {
    /// 按字段名取第一个匹配值。
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&[u8]> {
        self.fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_slice())
    }
}

impl RedisClient {
    /// `XADD key * field value …`；返回服务端生成的 stream ID。
    ///
    /// # Errors
    ///
    /// `fields` 为空返回 [`RedisError::Config`]；其余为连接/协议/超时错误。
    pub async fn xadd(&self, key: &str, fields: &[(&str, &[u8])]) -> RedisResult<String> {
        self.xadd_with_id(key, "*", fields).await
    }

    /// `XADD key <id> field value …`（测试 / 回放用；`id` 须为合法 stream ID 或 `*`）。
    ///
    /// # Errors
    ///
    /// `fields` 为空或 `id` 为空白返回 [`RedisError::Config`]；其余为连接/协议/超时错误。
    pub async fn xadd_with_id(
        &self,
        key: &str,
        id: &str,
        fields: &[(&str, &[u8])],
    ) -> RedisResult<String> {
        if fields.is_empty() {
            return Err(RedisError::Config("XADD 至少需要一个 field".to_owned()));
        }
        if id.trim().is_empty() {
            return Err(RedisError::Config("XADD id 不能为空".to_owned()));
        }
        let key = key.to_owned();
        let id = id.to_owned();
        let owned: Vec<(String, Vec<u8>)> = fields
            .iter()
            .map(|(field, value)| ((*field).to_owned(), (*value).to_vec()))
            .collect();
        self.with_pool_conn(move |mut conn| async move {
            let mut cmd = redis::cmd("XADD");
            cmd.arg(&key).arg(&id);
            for (field, value) in &owned {
                cmd.arg(field).arg(value.as_slice());
            }
            map_redis_result(cmd.query_async(&mut conn).await)
        })
        .await
    }

    /// `XLEN`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn xlen(&self, key: &str) -> RedisResult<i64> {
        let key = key.to_owned();
        self.with_pool_conn(move |mut conn| async move {
            map_redis_result(redis::cmd("XLEN").arg(&key).query_async(&mut conn).await)
        })
        .await
    }

    /// `XRANGE key start end [COUNT n]`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> RedisResult<Vec<StreamEntry>> {
        let key = key.to_owned();
        let start = start.to_owned();
        let end = end.to_owned();
        self.with_pool_conn(move |mut conn| async move {
            let mut cmd = redis::cmd("XRANGE");
            cmd.arg(&key).arg(&start).arg(&end);
            if let Some(count) = count {
                cmd.arg("COUNT").arg(count);
            }
            let raw: XrangeReply = map_redis_result(cmd.query_async(&mut conn).await)?;
            Ok(raw
                .into_iter()
                .map(|(id, fields)| StreamEntry { id, fields })
                .collect())
        })
        .await
    }

    /// `XREAD [COUNT n] STREAMS key id`（单流）；无新消息返回空 `Vec`。
    ///
    /// 阻塞读取请用 [`RedisClient::xread_block`]。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn xread(
        &self,
        key: &str,
        last_id: &str,
        count: Option<usize>,
    ) -> RedisResult<Vec<StreamEntry>> {
        let key = key.to_owned();
        let last_id = last_id.to_owned();
        self.with_pool_conn(move |mut conn| async move {
            let mut cmd = redis::cmd("XREAD");
            if let Some(count) = count {
                cmd.arg("COUNT").arg(count);
            }
            cmd.arg("STREAMS").arg(&key).arg(&last_id);
            parse_xread_single(&mut conn, cmd).await
        })
        .await
    }

    /// `XREAD BLOCK <ms> [COUNT n] STREAMS key id`（单流）。
    ///
    /// 使用阻塞命令预算 `command_timeout.max(block + 1s)`，避免与常规命令超时冲突；
    /// 阻塞到期无消息返回空 `Vec`。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或阻塞预算超时时返回错误。
    pub async fn xread_block(
        &self,
        key: &str,
        last_id: &str,
        block: Duration,
        count: Option<usize>,
    ) -> RedisResult<Vec<StreamEntry>> {
        let millis = u64::try_from(block.as_millis()).unwrap_or(u64::MAX).max(1);
        let key = key.to_owned();
        let last_id = last_id.to_owned();
        let budget = self
            .pool()
            .command_timeout()
            .max(block + Duration::from_secs(1));
        self.pool()
            .with_conn_budget(budget, move |mut conn| async move {
                let mut cmd = redis::cmd("XREAD");
                cmd.arg("BLOCK").arg(millis);
                if let Some(count) = count {
                    cmd.arg("COUNT").arg(count);
                }
                cmd.arg("STREAMS").arg(&key).arg(&last_id);
                parse_xread_single(&mut conn, cmd).await
            })
            .await
    }

    /// `XDEL`；返回删除条目数（`ids` 为空直接返回 0，不访问网络）。
    ///
    /// # Errors
    ///
    /// 连接/协议失败或超时时返回错误。
    pub async fn xdel(&self, key: &str, ids: &[&str]) -> RedisResult<i64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let key = key.to_owned();
        let ids: Vec<String> = ids.iter().map(|id| (*id).to_owned()).collect();
        self.with_pool_conn(move |mut conn| async move {
            let mut cmd = redis::cmd("XDEL");
            cmd.arg(&key);
            for id in &ids {
                cmd.arg(id);
            }
            map_redis_result(cmd.query_async(&mut conn).await)
        })
        .await
    }

    /// `XACK key group id …`；返回成功确认的条目数（`ids` 为空直接返回 0）。
    ///
    /// 需要 `group` 已由 `XGROUP CREATE` 建立（本 crate 不提供消费组管理）。
    ///
    /// # Errors
    ///
    /// 消费组不存在、连接/协议失败或超时时返回错误。
    pub async fn xack(&self, key: &str, group: &str, ids: &[&str]) -> RedisResult<i64> {
        if ids.is_empty() {
            return Ok(0);
        }
        if group.trim().is_empty() {
            return Err(RedisError::Config("XACK 消费组名不能为空".to_owned()));
        }
        let key = key.to_owned();
        let group = group.to_owned();
        let ids: Vec<String> = ids.iter().map(|id| (*id).to_owned()).collect();
        self.with_pool_conn(move |mut conn| async move {
            let mut cmd = redis::cmd("XACK");
            cmd.arg(&key).arg(&group);
            for id in &ids {
                cmd.arg(id);
            }
            map_redis_result(cmd.query_async(&mut conn).await)
        })
        .await
    }
}

type XreadField = (String, Vec<u8>);
type XreadEntry = (String, Vec<XreadField>);
type XreadStream = (String, Vec<XreadEntry>);
type XreadReply = Vec<XreadStream>;
/// `XRANGE` 原始应答：`[(id, [(field, value)])]`。
type XrangeReply = Vec<XreadEntry>;

/// 解析单流 `XREAD` 应答：`[[stream, [[id, [f, v, …]], …]]]` 或 `Nil`。
async fn parse_xread_single(
    conn: &mut impl redis::aio::ConnectionLike,
    cmd: redis::Cmd,
) -> RedisResult<Vec<StreamEntry>> {
    let raw: Option<XreadReply> = map_redis_result(cmd.query_async(conn).await)?;
    let Some(streams) = raw else {
        return Ok(Vec::new());
    };
    let mut entries = Vec::new();
    for (_stream, items) in streams {
        for (id, fields) in items {
            entries.push(StreamEntry { id, fields });
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::RedisPool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn probe(calls: Arc<AtomicUsize>) -> RedisClient {
        RedisPool::test_probe(calls).client()
    }

    #[tokio::test]
    async fn xadd_requires_fields_and_id() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = probe(calls.clone());
        let err = client.xadd("s", &[]).await.expect_err("空 fields");
        assert!(matches!(err, RedisError::Config(_)));
        let err = client
            .xadd_with_id("s", "1-0", &[])
            .await
            .expect_err("空 fields");
        assert!(matches!(err, RedisError::Config(_)));
        let err = client
            .xadd_with_id("s", "  ", &[("f", b"v")])
            .await
            .expect_err("空 id");
        assert!(matches!(err, RedisError::Config(_)));
        let err = client
            .xadd_with_id("s", "", &[("f", b"v")])
            .await
            .expect_err("空 id");
        assert!(matches!(err, RedisError::Config(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 0, "参数非法不得触达 driver");
    }

    #[tokio::test]
    async fn empty_id_lists_short_circuit() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = probe(calls.clone());
        assert_eq!(client.xdel("s", &[]).await.expect("空 ids"), 0);
        assert_eq!(client.xack("s", "g", &[]).await.expect("空 ids"), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let err = client.xack("s", " ", &["1-0"]).await.expect_err("空消费组");
        assert!(matches!(err, RedisError::Config(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stream_commands_enter_driver() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = probe(calls.clone());
        let _ = client.xadd("s", &[("f", b"v")]).await;
        let _ = client
            .xadd_with_id("s", "1700000000000-0", &[("f", b"v")])
            .await;
        let _ = client.xlen("s").await;
        let _ = client.xrange("s", "-", "+", Some(2)).await;
        let _ = client.xread("s", "0-0", Some(1)).await;
        let _ = client
            .xread_block("s", "0-0", Duration::from_millis(20), Some(1))
            .await;
        let _ = client.xdel("s", &["1-0"]).await;
        let _ = client.xack("s", "g", &["1-0"]).await;
        assert!(
            calls.load(Ordering::SeqCst) >= 8,
            "stream 命令应进入池连接路径"
        );
    }

    #[test]
    fn stream_entry_field_lookup() {
        let entry = StreamEntry {
            id: "1-0".to_owned(),
            fields: vec![
                ("a".to_owned(), b"1".to_vec()),
                ("b".to_owned(), vec![0, 255]),
            ],
        };
        assert_eq!(entry.field("a"), Some(b"1".as_slice()));
        assert_eq!(entry.field("b"), Some([0_u8, 255].as_slice()));
        assert_eq!(entry.field("missing"), None);
        assert_eq!(entry.id, "1-0");
    }
}
