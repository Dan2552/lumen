use crate::controllers::file_controller;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener as StdTcpListener};
use tauri::{AppHandle, Manager};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const DEFAULT_COMPANION_PORT: u16 = 44741;
const MAX_REQUEST_BYTES: usize = 512 * 1024;
const COMPANION_PAGE_HTML: &str = include_str!("../templates/companion/index.html");

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
    body: Vec<u8>,
}

struct HttpResponse {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl HttpResponse {
    fn html(status: u16, body: &'static str) -> Self {
        Self {
            status,
            content_type: "text/html; charset=utf-8",
            body: body.as_bytes().to_vec(),
        }
    }

    fn text(status: u16, message: String) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: message.into_bytes(),
        }
    }

    fn json<T: Serialize>(status: u16, value: &T) -> Self {
        match serde_json::to_vec(value) {
            Ok(body) => Self {
                status,
                content_type: "application/json; charset=utf-8",
                body,
            },
            Err(error) => Self::text(500, format!("json encode failed: {error}")),
        }
    }

    fn empty(status: u16) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CompanionNavigateRequest {
    path: String,
    #[serde(default)]
    window: Option<String>,
}

fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        _ => "Unknown",
    }
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(10 + (value - b'a')),
        b'A'..=b'F' => Some(10 + (value - b'A')),
        _ => None,
    }
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = hex_value(bytes[index + 1]);
                let low = hex_value(bytes[index + 2]);
                if let (Some(high), Some(low)) = (high, low) {
                    decoded.push((high << 4) | low);
                    index += 3;
                } else {
                    decoded.push(bytes[index]);
                    index += 1;
                }
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).to_string()
}

fn parse_query(raw: &str) -> HashMap<String, String> {
    let mut query = HashMap::new();
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair
            .split_once('=')
            .map(|(key, value)| (percent_decode(key), percent_decode(value)))
            .unwrap_or_else(|| (percent_decode(pair), String::new()));
        query.insert(key, value);
    }
    query
}

fn parse_target(raw: &str) -> (String, HashMap<String, String>) {
    match raw.split_once('?') {
        Some((path, query)) => (path.to_string(), parse_query(query)),
        None => (raw.to_string(), HashMap::new()),
    }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    let needle = b"\r\n\r\n";
    buffer
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|index| index + needle.len())
}

fn parse_content_length(headers: &str) -> Result<usize, String> {
    for line in headers.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("content-length") {
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| "invalid content-length".to_string())?;
            return Ok(parsed);
        }
    }
    Ok(0)
}

async fn read_request(stream: &mut TcpStream) -> Result<HttpRequest, String> {
    let mut buffer = Vec::new();
    let mut temp = [0u8; 4096];
    let mut header_end = None;

    while header_end.is_none() {
        let read = stream
            .read(&mut temp)
            .await
            .map_err(|error| format!("read failed: {error}"))?;
        if read == 0 {
            return Err("connection closed before request headers".to_string());
        }
        buffer.extend_from_slice(&temp[..read]);
        if buffer.len() > MAX_REQUEST_BYTES {
            return Err("request too large".to_string());
        }
        header_end = find_header_end(&buffer);
    }

    let header_end = header_end.expect("header_end checked");
    let header_text = std::str::from_utf8(&buffer[..header_end])
        .map_err(|_| "request headers are not valid UTF-8".to_string())?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| "missing request line".to_string())?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| "missing method".to_string())?
        .to_string();
    let target = request_parts
        .next()
        .ok_or_else(|| "missing request target".to_string())?
        .to_string();
    let content_length = parse_content_length(header_text)?;
    if content_length > MAX_REQUEST_BYTES {
        return Err("request body too large".to_string());
    }
    let expected_total = header_end
        .checked_add(content_length)
        .ok_or_else(|| "invalid request size".to_string())?;
    while buffer.len() < expected_total {
        let read = stream
            .read(&mut temp)
            .await
            .map_err(|error| format!("read failed: {error}"))?;
        if read == 0 {
            return Err("connection closed while reading request body".to_string());
        }
        buffer.extend_from_slice(&temp[..read]);
        if buffer.len() > MAX_REQUEST_BYTES {
            return Err("request too large".to_string());
        }
    }

    let body = buffer[header_end..expected_total].to_vec();
    let (path, query) = parse_target(&target);
    Ok(HttpRequest {
        method,
        path,
        query,
        body,
    })
}

fn resolve_window_label(raw: Option<&str>) -> Result<String, String> {
    let label = raw.unwrap_or("main").trim();
    if !file_controller::is_companion_window_label(label) {
        return Err(format!("unsupported window label: {label}"));
    }
    Ok(label.to_string())
}

fn map_state_error(error: String) -> HttpResponse {
    if error.contains("failed to lock tabs state") {
        HttpResponse::text(500, error)
    } else {
        HttpResponse::text(400, error)
    }
}

async fn route_request(request: HttpRequest, app: &AppHandle) -> HttpResponse {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") | ("GET", "/companion") => HttpResponse::html(200, COMPANION_PAGE_HTML),
        ("GET", "/favicon.ico") => HttpResponse::empty(204),
        ("GET", "/api/state") => {
            let window_label =
                match resolve_window_label(request.query.get("window").map(String::as_str)) {
                    Ok(label) => label,
                    Err(error) => return HttpResponse::text(400, error),
                };
            let tabs_state = app.state::<file_controller::FileTabsState>();
            match file_controller::companion_snapshot(tabs_state.inner(), &window_label) {
                Ok(snapshot) => HttpResponse::json(200, &snapshot),
                Err(error) => map_state_error(error),
            }
        }
        ("POST", "/api/navigate") => {
            let payload = match serde_json::from_slice::<CompanionNavigateRequest>(&request.body) {
                Ok(payload) => payload,
                Err(error) => {
                    return HttpResponse::text(400, format!("invalid JSON body: {error}"))
                }
            };
            if payload.path.trim().is_empty() {
                return HttpResponse::text(400, "path cannot be empty".to_string());
            }
            let window_label = match resolve_window_label(payload.window.as_deref()) {
                Ok(label) => label,
                Err(error) => return HttpResponse::text(400, error),
            };
            let tabs_state = app.state::<file_controller::FileTabsState>();
            match file_controller::companion_navigate(
                tabs_state.inner(),
                &window_label,
                payload.path,
            ) {
                Ok(snapshot) => {
                    file_controller::refresh_browser_window(app, &window_label);
                    HttpResponse::json(200, &snapshot)
                }
                Err(error) => map_state_error(error),
            }
        }
        ("POST", "/api/state") | ("GET", "/api/navigate") => {
            HttpResponse::text(405, "method not allowed".to_string())
        }
        _ => HttpResponse::text(404, "not found".to_string()),
    }
}

async fn write_response(stream: &mut TcpStream, response: HttpResponse) -> Result<(), String> {
    let headers = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        response.status,
        status_text(response.status),
        response.content_type,
        response.body.len()
    );
    stream
        .write_all(headers.as_bytes())
        .await
        .map_err(|error| format!("write headers failed: {error}"))?;
    if !response.body.is_empty() {
        stream
            .write_all(&response.body)
            .await
            .map_err(|error| format!("write body failed: {error}"))?;
    }
    stream
        .shutdown()
        .await
        .map_err(|error| format!("shutdown failed: {error}"))?;
    Ok(())
}

async fn handle_connection(mut stream: TcpStream, app: AppHandle) -> Result<(), String> {
    let request = match read_request(&mut stream).await {
        Ok(request) => request,
        Err(error) => {
            let response = if error.contains("too large") {
                HttpResponse::text(413, error)
            } else {
                HttpResponse::text(400, error)
            };
            let _ = write_response(&mut stream, response).await;
            return Ok(());
        }
    };
    let response = route_request(request, &app).await;
    write_response(&mut stream, response).await
}

fn bind_listener() -> Result<StdTcpListener, String> {
    let preferred_port = std::env::var("LUMEN_COMPANION_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_COMPANION_PORT);
    let preferred = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, preferred_port);
    match StdTcpListener::bind(preferred) {
        Ok(listener) => Ok(listener),
        Err(preferred_error) => {
            let fallback = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0);
            let listener = StdTcpListener::bind(fallback).map_err(|fallback_error| {
                format!(
                    "failed to bind companion server on preferred port {preferred_port} ({preferred_error}) \
and failed fallback bind ({fallback_error})"
                )
            })?;
            eprintln!(
                "lumen companion: preferred port {preferred_port} unavailable ({preferred_error}); \
using an ephemeral port instead"
            );
            Ok(listener)
        }
    }
}

pub fn start(app: AppHandle) -> Result<(), String> {
    let listener = bind_listener()?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("failed to set companion listener nonblocking mode: {error}"))?;
    let listener = tokio::net::TcpListener::from_std(listener)
        .map_err(|error| format!("failed to create tokio companion listener: {error}"))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("failed to read companion listener address: {error}"))?;

    println!(
        "lumen companion: http://127.0.0.1:{}/companion",
        address.port()
    );
    println!(
        "lumen companion: http://<this-device-lan-ip>:{}/companion",
        address.port()
    );

    tauri::async_runtime::spawn(async move {
        let listener = listener;
        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(result) => result,
                Err(error) => {
                    eprintln!("lumen companion accept failed: {error}");
                    continue;
                }
            };
            let app = app.clone();
            tauri::async_runtime::spawn(async move {
                if let Err(error) = handle_connection(stream, app).await {
                    eprintln!("lumen companion request failed: {error}");
                }
            });
        }
    });

    Ok(())
}
