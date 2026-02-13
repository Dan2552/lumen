use super::ContextCommand;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

pub fn commands_for_directory(path: &Path) -> Vec<ContextCommand> {
    if !path.is_dir() {
        return Vec::new();
    }

    let has_cargo_toml = path.join("Cargo.toml").is_file();
    let has_src_tauri = path.join("src-tauri").is_dir();
    if !has_cargo_toml && !has_src_tauri {
        return Vec::new();
    }

    let mut commands = Vec::new();
    if has_cargo_toml {
        commands.push(ContextCommand {
            id: "rust:cargo-run".to_string(),
            label: "Rust: cargo run".to_string(),
            command: "cargo run".to_string(),
        });
        commands.push(ContextCommand {
            id: "rust:cargo-test".to_string(),
            label: "Rust: cargo test".to_string(),
            command: "cargo test".to_string(),
        });
        for alias in parse_cargo_aliases(path) {
            commands.push(ContextCommand {
                id: format!("rust:cargo-alias:{}", normalize_alias_id(&alias)),
                label: format!("Rust: cargo {alias}"),
                command: format!("cargo {alias}"),
            });
        }
    }
    if has_src_tauri {
        commands.push(ContextCommand {
            id: "rust:cargo-tauri-dev".to_string(),
            label: "Rust: cargo tauri dev".to_string(),
            command: "cargo tauri dev".to_string(),
        });
        commands.push(ContextCommand {
            id: "rust:cargo-tauri-build".to_string(),
            label: "Rust: cargo tauri build".to_string(),
            command: "cargo tauri build".to_string(),
        });
    }
    commands
}

fn parse_cargo_aliases(path: &Path) -> Vec<String> {
    let mut aliases = BTreeSet::new();
    for candidate in [path.join(".cargo/config.toml"), path.join(".cargo/config")] {
        if !candidate.is_file() {
            continue;
        }
        if let Ok(content) = fs::read_to_string(&candidate) {
            parse_aliases_from_config(&content, &mut aliases);
        }
    }
    aliases.into_iter().collect()
}

fn parse_aliases_from_config(content: &str, aliases: &mut BTreeSet<String>) {
    let mut in_alias_section = false;
    for raw_line in content.lines() {
        let line_without_comment = strip_toml_comment(raw_line);
        let line = line_without_comment.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            let section = line[1..line.len().saturating_sub(1)].trim();
            in_alias_section = section == "alias";
            continue;
        }
        if !in_alias_section {
            continue;
        }
        let Some((raw_key, _raw_value)) = line.split_once('=') else {
            continue;
        };
        let key = unquote_toml_key(raw_key.trim());
        if !is_valid_alias_key(&key) {
            continue;
        }
        aliases.insert(key);
    }
}

fn strip_toml_comment(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut in_single = false;
    let mut in_double = false;
    let mut escape_next = false;
    for ch in value.chars() {
        if in_double && !escape_next && ch == '\\' {
            escape_next = true;
            result.push(ch);
            continue;
        }
        if !escape_next {
            if ch == '"' && !in_single {
                in_double = !in_double;
            } else if ch == '\'' && !in_double {
                in_single = !in_single;
            } else if ch == '#' && !in_single && !in_double {
                break;
            }
        }
        escape_next = false;
        result.push(ch);
    }
    result
}

fn unquote_toml_key(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        if (trimmed.starts_with('"') && trimmed.ends_with('"'))
            || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

fn is_valid_alias_key(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(char::is_whitespace)
}

fn normalize_alias_id(value: &str) -> String {
    let normalized: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let trimmed = normalized.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "alias".to_string()
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_aliases_from_alias_section() {
        let mut aliases = BTreeSet::new();
        parse_aliases_from_config(
            r#"
            [alias]
            t = "test"
            xtask = ["run", "-p", "xtask", "--"]
            "#,
            &mut aliases,
        );
        assert!(aliases.contains("t"));
        assert!(aliases.contains("xtask"));
    }

    #[test]
    fn ignores_comments_and_other_sections() {
        let mut aliases = BTreeSet::new();
        parse_aliases_from_config(
            r#"
            [build]
            target = "x86_64-apple-darwin"

            [alias]
            "qa-check" = "clippy" # inline comment
            "#,
            &mut aliases,
        );
        assert_eq!(aliases.len(), 1);
        assert!(aliases.contains("qa-check"));
    }
}
