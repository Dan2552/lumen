use super::ContextCommand;
use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

pub fn commands_for_directory(path: &Path) -> Vec<ContextCommand> {
    if !path.is_dir() {
        return Vec::new();
    }
    let bin_dir = path.join("bin");
    if !bin_dir.is_dir() {
        return Vec::new();
    }

    let Ok(entries) = fs::read_dir(&bin_dir) else {
        return Vec::new();
    };

    let mut scripts = BTreeSet::new();
    for entry in entries.flatten() {
        let file_path = entry.path();
        if !file_path.is_file() {
            continue;
        }
        let Some(name) = file_path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.starts_with('.') || !is_safe_script_name(name) {
            continue;
        }
        if !is_probably_script(&file_path, name) {
            continue;
        }
        scripts.insert(name.to_string());
    }

    scripts
        .into_iter()
        .map(|name| ContextCommand {
            id: format!("bin:{name}"),
            label: format!("Bin: ./bin/{name}"),
            command: format!("./bin/{name}"),
        })
        .collect()
}

fn is_safe_script_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
}

fn is_probably_script(path: &Path, name: &str) -> bool {
    if is_executable(path) || has_shebang(path) {
        return true;
    }
    let ext = Path::new(name)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    matches!(
        ext.as_str(),
        "sh" | "bash"
            | "zsh"
            | "fish"
            | "py"
            | "rb"
            | "pl"
            | "php"
            | "js"
            | "ts"
            | "tsx"
            | "mjs"
            | "cjs"
            | "lua"
            | "ps1"
            | "bat"
            | "cmd"
    )
}

fn has_shebang(path: &Path) -> bool {
    let Ok(mut file) = fs::File::open(path) else {
        return false;
    };
    let mut prefix = [0_u8; 2];
    let Ok(read) = file.read(&mut prefix) else {
        return false;
    };
    read == 2 && prefix == [b'#', b'!']
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    fs::metadata(path)
        .map(|meta| meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(_path: &Path) -> bool {
    false
}
