//! 配置低层解析辅助：地址 / 种子 / 布尔 / 部署模式解析与凭据脱敏。
//!
//! 仅供 `config` 及其子模块使用，故可见性为 `pub(super)`。

use super::RedisMode;
use crate::error::{RedisError, RedisResult};

/// 脱敏可能含凭据的种子 URL（`redis://user:pass@host` → `redis://user:***@host`）。
#[must_use]
pub(super) fn redact_seed_url(raw: &str) -> String {
    let s = raw.trim();
    if let Some(scheme_end) = s.find("://") {
        let rest = &s[scheme_end + 3..];
        if let Some(at) = rest.rfind('@') {
            let creds = &rest[..at];
            let host = &rest[at + 1..];
            if let Some(colon) = creds.find(':') {
                let user = &creds[..colon];
                return format!("{}://{}:***@{}", &s[..scheme_end], user, host);
            }
            return format!("{}://***@{}", &s[..scheme_end], host);
        }
    }
    s.to_owned()
}

/// 读取非空环境变量；未设置或全空白返回 `None`。
pub(super) fn env_nonempty(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

/// 解析 `host:port`（缺省端口 6379，支持 `[ipv6]:port`）。
pub(super) fn parse_host_port(addr: &str) -> RedisResult<(String, u16)> {
    let addr = addr.trim();
    if let Some(rest) = addr.strip_prefix('[') {
        let (host, port_part) = rest
            .split_once("]:")
            .ok_or_else(|| RedisError::Config(format!("非法 IPv6 地址: {addr}")))?;
        let port: u16 = port_part
            .parse()
            .map_err(|e| RedisError::Config(format!("非法端口: {e}")))?;
        return Ok((host.to_owned(), port));
    }
    match addr.rsplit_once(':') {
        Some((host, port_part)) if !host.is_empty() => {
            let port: u16 = port_part
                .parse()
                .map_err(|e| RedisError::Config(format!("非法端口: {e}")))?;
            Ok((host.to_owned(), port))
        }
        _ => Ok((addr.to_owned(), 6379)),
    }
}

/// 校验种子节点：非空、URL 可解析、且不使用 insecure TLS。
pub(super) fn validate_seed(seed: &str) -> RedisResult<()> {
    let seed = seed.trim();
    if seed.is_empty() {
        return Err(RedisError::Config("种子节点不能为空字符串".to_owned()));
    }
    if seed.starts_with("redis://") || seed.starts_with("rediss://") {
        let redacted = redact_seed_url(seed);
        let parsed = url::Url::parse(seed)
            .map_err(|e| RedisError::Config(format!("非法节点 URL `{redacted}`: {e}")))?;
        if parsed.host_str().unwrap_or("").is_empty() {
            return Err(RedisError::Config(format!(
                "节点 URL 缺少主机名 `{redacted}`"
            )));
        }
        use redis::IntoConnectionInfo;
        let info = seed
            .into_connection_info()
            .map_err(|_| RedisError::Config(format!("非法节点 URL `{redacted}`")))?;
        if let redis::ConnectionAddr::TcpTls { insecure: true, .. } = info.addr {
            return Err(RedisError::Config("拒绝 insecure TLS 节点 URL".to_owned()));
        }
        return Ok(());
    }
    parse_host_port(seed).map(|_| ())
}

/// 逗号分隔节点列表 → 去空白、去空项。
pub(super) fn split_nodes(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// 解析宽松布尔字面量。
pub(super) fn parse_bool(value: &str) -> RedisResult<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(RedisError::Config(format!(
            "布尔环境变量非法: {other}（期望 true/false）"
        ))),
    }
}

/// 解析部署模式（不区分大小写）。
pub(super) fn parse_mode(value: &str) -> RedisResult<RedisMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "standalone" | "single" => Ok(RedisMode::Standalone),
        "cluster" => Ok(RedisMode::Cluster),
        "sentinel" => Ok(RedisMode::Sentinel),
        other => Err(RedisError::Config(format!(
            "未知 Redis 模式 `{other}`（期望 standalone|cluster|sentinel）"
        ))),
    }
}
