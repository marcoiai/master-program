use crate::mesh::envelope::ProtocolEnvelope;
use crate::mesh::identity::{PeerIdentity, TransportEndpoint};
use crate::mesh::transport::{MeshTransport, TransportCapacity, TransportError, TransportReceipt};
use std::io::{Read, Write};
use std::time::Duration;

#[derive(Debug, Default)]
pub struct LocalHttpTransport;

impl MeshTransport for LocalHttpTransport {
    fn kind(&self) -> &'static str {
        "local_http"
    }

    fn start(&self) -> Result<(), TransportError> {
        Ok(())
    }

    fn stop(&self) -> Result<(), TransportError> {
        Ok(())
    }

    fn send(
        &self,
        envelope: &ProtocolEnvelope,
        target_peer: &PeerIdentity,
    ) -> Result<TransportReceipt, TransportError> {
        let Some(url) = target_peer.url.as_deref() else {
            return Err(TransportError::new(
                "peer_transport_unavailable",
                "local_http peer has no url",
            ));
        };
        let body = serde_json::json!({
            "message": envelope.payload.get("message").and_then(|value| value.as_str()).unwrap_or("ping"),
            "source": envelope.source_node_id,
            "messageId": envelope.message_id,
        })
        .to_string();
        post_peer_ping(url, &body)
            .map(|_| TransportReceipt {
                transport: self.kind().to_string(),
            })
            .map_err(|error| TransportError::new("transport_send_failed", error))
    }

    fn status(&self) -> TransportEndpoint {
        TransportEndpoint {
            kind: self.kind().to_string(),
            status: "available".to_string(),
            url: None,
            metadata: serde_json::Value::Null,
        }
    }

    fn capacity(&self) -> TransportCapacity {
        TransportCapacity::local_http()
    }

    fn capabilities(&self) -> Vec<String> {
        vec!["mesh.control".to_string(), "core.ping".to_string()]
    }
}

pub fn parse_http_url(url: &str) -> Result<(String, u16, String), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| "only_http_supported_in_mesh_v0".to_string())?;
    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, format!("/{path}")),
        None => (rest, "/".to_string()),
    };
    if authority.trim().is_empty() {
        return Err("missing_host".to_string());
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => {
            let parsed_port = port
                .parse::<u16>()
                .map_err(|_| "invalid_port".to_string())?;
            (host.to_string(), parsed_port)
        }
        None => (authority.to_string(), 80),
    };
    Ok((host, port, path))
}

fn post_peer_ping(base_url: &str, body: &str) -> Result<(), String> {
    let body = http_request(base_url, "POST", "/v1/ping", Some(body))?;
    if body.is_empty() {
        Err("empty_response".to_string())
    } else {
        Ok(())
    }
}

pub fn get_peer_json(base_url: &str, endpoint: &str) -> Result<serde_json::Value, String> {
    let body = http_request(base_url, "GET", endpoint, None)?;
    serde_json::from_str(&body).map_err(|error| format!("invalid_peer_json: {error}"))
}

fn http_request(
    base_url: &str,
    method: &str,
    endpoint: &str,
    body: Option<&str>,
) -> Result<String, String> {
    let (host, port, base_path) = parse_http_url(base_url)?;
    let path = joined_path(&base_path, endpoint);
    let mut stream = std::net::TcpStream::connect((host.as_str(), port))
        .map_err(|error| format!("connect_failed: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(4)))
        .map_err(|error| format!("read_timeout_failed: {error}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(4)))
        .map_err(|error| format!("write_timeout_failed: {error}"))?;

    let body = body.unwrap_or("");
    let request = if method == "GET" {
        format!(
            "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
        )
    } else {
        format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    };
    stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("write_failed: {error}"))?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| format!("read_failed: {error}"))?;
    if !(response.starts_with("HTTP/1.1 2") || response.starts_with("HTTP/1.0 2")) {
        let status = response.lines().next().unwrap_or("empty_response");
        return Err(format!("peer_http_error: {status}"));
    }

    match response.split_once("\r\n\r\n") {
        Some((_, body)) => Ok(body.to_string()),
        None => Ok(String::new()),
    }
}

fn joined_path(base_path: &str, endpoint: &str) -> String {
    let endpoint = if endpoint.starts_with('/') {
        endpoint.to_string()
    } else {
        format!("/{endpoint}")
    };
    if base_path == "/" {
        endpoint
    } else {
        format!("{}{}", base_path.trim_end_matches('/'), endpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_http_url_with_default_port() {
        assert_eq!(
            parse_http_url("http://localhost").unwrap(),
            ("localhost".to_string(), 80, "/".to_string())
        );
    }

    #[test]
    fn joins_base_paths() {
        assert_eq!(joined_path("/", "/v1/mesh/node"), "/v1/mesh/node");
        assert_eq!(
            joined_path("/node-a", "/v1/mesh/node"),
            "/node-a/v1/mesh/node"
        );
    }
}
