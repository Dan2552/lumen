use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Mutex;
use tauri::State;

#[derive(Default)]
pub struct ShellControlState {
    socket_path: Mutex<Option<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ShellControlRequest {
    Cd { path: String },
    Exec { command: String },
    Ping,
}

#[derive(Debug, Serialize, Deserialize)]
struct ShellControlResponse {
    ok: bool,
    #[serde(default)]
    message: Option<String>,
}

fn normalize_socket_path(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("socket path cannot be empty".to_string());
    }
    Ok(trimmed.to_string())
}

fn active_socket_path(state: &State<'_, ShellControlState>) -> Result<Option<String>, String> {
    state
        .socket_path
        .lock()
        .map(|value| value.clone())
        .map_err(|_| "failed to lock shell control state".to_string())
}

fn set_active_socket_path(
    state: &State<'_, ShellControlState>,
    socket_path: Option<String>,
) -> Result<(), String> {
    let mut guard = state
        .socket_path
        .lock()
        .map_err(|_| "failed to lock shell control state".to_string())?;
    *guard = socket_path;
    Ok(())
}

#[cfg(unix)]
fn discover_live_shell_socket_path() -> Option<String> {
    use std::cmp::Reverse;
    use std::fs;
    use std::os::unix::fs::FileTypeExt;
    use std::time::UNIX_EPOCH;

    fn candidate_dirs() -> Vec<PathBuf> {
        let mut result = Vec::new();
        let mut push_unique = |value: PathBuf| {
            if !result.iter().any(|existing| existing == &value) {
                result.push(value);
            }
        };
        push_unique(std::env::temp_dir());
        push_unique(PathBuf::from("/tmp"));
        result
    }

    let mut candidates: Vec<(u128, String)> = Vec::new();
    for dir in candidate_dirs() {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let is_named_socket = name == "lumen-shell-current.sock"
                || (name.starts_with("lumen-shell-") && name.ends_with(".sock"));
            if !is_named_socket {
                continue;
            }
            let Ok(metadata) = fs::metadata(&path) else {
                continue;
            };
            if !metadata.file_type().is_socket() {
                continue;
            }
            let modified_ms = metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
                .map(|value| value.as_millis())
                .unwrap_or(0);
            candidates.push((modified_ms, path.to_string_lossy().to_string()));
        }
    }
    candidates.sort_by_key(|(modified_ms, _)| Reverse(*modified_ms));

    for (_, path) in candidates {
        if send_shell_control_request(&path, &ShellControlRequest::Ping).is_ok() {
            return Some(path);
        }
    }
    None
}

#[cfg(not(unix))]
fn discover_live_shell_socket_path() -> Option<String> {
    None
}

fn send_request_to_active(
    state: &State<'_, ShellControlState>,
    request: ShellControlRequest,
) -> Result<bool, String> {
    let current = active_socket_path(state)?;

    if let Some(path) = current {
        match send_shell_control_request(&path, &request) {
            Ok(()) => return Ok(true),
            Err(initial_error) => {
                if let Some(discovered) = discover_live_shell_socket_path() {
                    if discovered != path {
                        if send_shell_control_request(&discovered, &request).is_ok() {
                            let _ = set_active_socket_path(state, Some(discovered));
                            return Ok(true);
                        }
                    }
                }
                let _ = set_active_socket_path(state, None);
                return Err(initial_error);
            }
        }
    }

    if let Some(discovered) = discover_live_shell_socket_path() {
        send_shell_control_request(&discovered, &request)?;
        let _ = set_active_socket_path(state, Some(discovered));
        return Ok(true);
    }

    Ok(false)
}

#[cfg(unix)]
fn send_shell_control_request(
    socket_path: &str,
    request: &ShellControlRequest,
) -> Result<(), String> {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    let mut stream = UnixStream::connect(socket_path).map_err(|error| {
        format!("failed to connect to shell control socket `{socket_path}`: {error}")
    })?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));

    let payload = serde_json::to_vec(request)
        .map_err(|error| format!("failed to serialize shell control request: {error}"))?;
    stream
        .write_all(&payload)
        .map_err(|error| format!("failed to send shell control request: {error}"))?;
    stream
        .write_all(b"\n")
        .map_err(|error| format!("failed to finish shell control request: {error}"))?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| format!("failed to read shell control response: {error}"))?;
    if line.trim().is_empty() {
        return Err("shell control socket returned an empty response".to_string());
    }

    let response: ShellControlResponse = serde_json::from_str(line.trim())
        .map_err(|error| format!("invalid shell control response: {error}"))?;
    if response.ok {
        Ok(())
    } else {
        Err(response
            .message
            .unwrap_or_else(|| "shell command failed".to_string()))
    }
}

#[cfg(not(unix))]
fn send_shell_control_request(_socket_path: &str, _request: &ShellControlRequest) -> Result<(), String> {
    Err("shell control is only supported on unix targets".to_string())
}

#[tauri::command]
pub fn shell_control_get_socket_path(state: State<'_, ShellControlState>) -> Option<String> {
    state
        .socket_path
        .lock()
        .ok()
        .and_then(|value| value.clone())
}

#[tauri::command]
pub fn shell_control_get_or_discover_socket_path(
    state: State<'_, ShellControlState>,
) -> Option<String> {
    if let Ok(Some(path)) = active_socket_path(&state) {
        if send_shell_control_request(&path, &ShellControlRequest::Ping).is_ok() {
            return Some(path);
        }
    }
    let discovered = discover_live_shell_socket_path();
    let _ = set_active_socket_path(&state, discovered.clone());
    discovered
}

#[tauri::command]
pub fn shell_control_set_socket_path(
    state: State<'_, ShellControlState>,
    socket_path: Option<String>,
) -> Result<Option<String>, String> {
    let normalized = match socket_path {
        Some(value) => Some(normalize_socket_path(&value)?),
        None => None,
    };
    set_active_socket_path(&state, normalized.clone())?;
    Ok(normalized)
}

#[tauri::command]
pub fn shell_control_cd(
    state: State<'_, ShellControlState>,
    path: String,
) -> Result<bool, String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err("path cannot be empty".to_string());
    }
    send_request_to_active(
        &state,
        ShellControlRequest::Cd {
            path: trimmed.to_string(),
        },
    )
}

#[tauri::command]
pub fn shell_control_exec(
    state: State<'_, ShellControlState>,
    command: String,
) -> Result<bool, String> {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return Err("command cannot be empty".to_string());
    }
    send_request_to_active(
        &state,
        ShellControlRequest::Exec {
            command: trimmed.to_string(),
        },
    )
}

#[tauri::command]
pub fn shell_control_ping(state: State<'_, ShellControlState>) -> Result<bool, String> {
    send_request_to_active(&state, ShellControlRequest::Ping)
}
