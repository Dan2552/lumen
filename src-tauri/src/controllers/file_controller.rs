use serde::Serialize;
use std::{
    collections::BTreeMap,
    env, fs,
    io::{ErrorKind, Write},
    io::Read,
    path::{Path, PathBuf},
    process::Command,
    sync::Mutex,
    time::UNIX_EPOCH,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use tauri::{
    menu::MenuBuilder, AppHandle, LogicalPosition, Manager, PhysicalPosition, PhysicalSize,
    Runtime, State, Window,
};
use tera::Context;

#[derive(Debug, Serialize)]
struct FileEntry {
    name: String,
    path: String,
    is_dir: bool,
    icon: &'static str,
}

#[derive(Debug, Serialize)]
struct Column {
    title: String,
    path: String,
    sort_mode: String,
    entries: Vec<FileEntry>,
    selected_path: String,
}

#[derive(Debug, Serialize, Clone)]
struct PreviewModel {
    kind: String,
    title: String,
    subtitle: String,
    path: String,
    icon: String,
    image_data_url: Option<String>,
    pdf_path: Option<String>,
    glb_path: Option<String>,
    text_head: Option<String>,
    note: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ModelPreviewData {
    mime_type: String,
    base64: String,
}

#[derive(Debug, Serialize, Clone)]
struct TabView {
    id: String,
    title: String,
    is_active: bool,
}

#[derive(Debug, Clone)]
#[derive(serde::Serialize, serde::Deserialize)]
struct TabState {
    id: u64,
    #[serde(default)]
    root_path: Option<String>,
    #[serde(default)]
    focus_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum DirectorySortMode {
    Alphabetical,
    RecentFirst,
}

impl Default for DirectorySortMode {
    fn default() -> Self {
        Self::Alphabetical
    }
}

impl DirectorySortMode {
    fn from_input(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "alphabetical" => Some(Self::Alphabetical),
            "recent_first" | "recent" | "updated" => Some(Self::RecentFirst),
            _ => None,
        }
    }
}

#[derive(Debug)]
#[derive(serde::Serialize, serde::Deserialize)]
struct TabsModel {
    #[serde(default)]
    tabs: Vec<TabState>,
    #[serde(default = "default_active_id")]
    active_id: u64,
    #[serde(default = "default_next_id")]
    next_id: u64,
    #[serde(default)]
    directory_sorts: BTreeMap<String, DirectorySortMode>,
    #[serde(default)]
    window: Option<WindowGeometry>,
}

#[derive(Debug, Clone)]
#[derive(serde::Serialize, serde::Deserialize)]
struct WindowGeometry {
    x: i32,
    y: i32,
    width: u32,
    height: u32,
}

fn default_active_id() -> u64 {
    1
}

fn default_next_id() -> u64 {
    2
}

#[derive(Debug)]
struct SortableEntry {
    name_key: String,
    modified_ms: u128,
    entry: FileEntry,
}

fn list_directory(path: &PathBuf, sort_mode: DirectorySortMode) -> Vec<FileEntry> {
    let Ok(entries) = fs::read_dir(path) else {
        return Vec::new();
    };

    let mut items: Vec<SortableEntry> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let entry_path = entry.path();
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy().to_string();
            if name.starts_with('.') {
                return None;
            }
            let metadata = entry.metadata().ok()?;
            let is_dir = metadata.is_dir();
            let modified_ms = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_millis())
                .unwrap_or(0);

            Some(SortableEntry {
                name_key: name.to_lowercase(),
                modified_ms,
                entry: FileEntry {
                    icon: icon_for_entry(&name, is_dir),
                    name,
                    path: entry_path.to_string_lossy().to_string(),
                    is_dir,
                },
            })
        })
        .collect();

    match sort_mode {
        DirectorySortMode::Alphabetical => {
            items.sort_by(|a, b| {
                b.entry
                    .is_dir
                    .cmp(&a.entry.is_dir)
                    .then_with(|| a.name_key.cmp(&b.name_key))
            });
        }
        DirectorySortMode::RecentFirst => {
            items.sort_by(|a, b| {
                b.entry
                    .is_dir
                    .cmp(&a.entry.is_dir)
                    .then_with(|| b.modified_ms.cmp(&a.modified_ms))
                    .then_with(|| a.name_key.cmp(&b.name_key))
            });
        }
    }

    items.into_iter().map(|item| item.entry).collect()
}

fn icon_for_entry(name: &str, is_dir: bool) -> &'static str {
    if is_dir {
        return "folder";
    }

    let ext = Path::new(name)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase());

    match ext.as_deref() {
        Some("png") | Some("jpg") | Some("jpeg") | Some("gif") | Some("webp") | Some("svg")
        | Some("bmp") | Some("ico") | Some("tif") | Some("tiff") | Some("heic") | Some("avif") => {
            "file-image"
        }
        Some("mp4") | Some("mov") | Some("mkv") | Some("avi") | Some("webm") | Some("m4v") => {
            "file-video"
        }
        Some("mp3") | Some("wav") | Some("flac") | Some("m4a") | Some("aac") | Some("ogg") => {
            "file-audio"
        }
        Some("zip") | Some("rar") | Some("7z") | Some("tar") | Some("gz") | Some("bz2")
        | Some("xz") => "file-archive",
        Some("rs") | Some("js") | Some("ts") | Some("jsx") | Some("tsx") | Some("py")
        | Some("go") | Some("java") | Some("c") | Some("cpp") | Some("h") | Some("hpp")
        | Some("cs") | Some("php") | Some("rb") | Some("swift") | Some("kt") | Some("lua")
        | Some("sh") | Some("zsh") | Some("bash") | Some("html") | Some("css") | Some("scss")
        | Some("xml") => "file-code",
        Some("json") => "file-json",
        Some("toml") | Some("yaml") | Some("yml") | Some("md") | Some("txt") | Some("log")
        | Some("pdf") | Some("doc") | Some("docx") | Some("rtf") | Some("odt") => "file-text",
        Some("csv") | Some("tsv") | Some("xls") | Some("xlsx") => "file-spreadsheet",
        _ => "file",
    }
}

fn image_mime_from_ext(ext: &str) -> Option<&'static str> {
    match ext {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "svg" => Some("image/svg+xml"),
        "bmp" => Some("image/bmp"),
        "ico" => Some("image/x-icon"),
        "avif" => Some("image/avif"),
        "heic" => Some("image/heic"),
        _ => None,
    }
}

fn is_affinity_ext(ext: &str) -> bool {
    matches!(ext, "afdesign" | "afphoto" | "afpub")
}

fn parse_embedded_png(bytes: &[u8], start: usize) -> Option<(usize, u32, u32)> {
    const PNG_SIGNATURE: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];
    if start + PNG_SIGNATURE.len() > bytes.len() {
        return None;
    }
    if bytes[start..start + PNG_SIGNATURE.len()] != PNG_SIGNATURE {
        return None;
    }

    let mut cursor = start + PNG_SIGNATURE.len();
    let mut width: Option<u32> = None;
    let mut height: Option<u32> = None;

    loop {
        if cursor + 8 > bytes.len() {
            return None;
        }
        let chunk_len = u32::from_be_bytes([
            bytes[cursor],
            bytes[cursor + 1],
            bytes[cursor + 2],
            bytes[cursor + 3],
        ]) as usize;
        let chunk_type = &bytes[cursor + 4..cursor + 8];
        let data_start = cursor + 8;
        let data_end = data_start.checked_add(chunk_len)?;
        let crc_end = data_end.checked_add(4)?;
        if crc_end > bytes.len() {
            return None;
        }

        if chunk_type == b"IHDR" && chunk_len >= 13 {
            width = Some(u32::from_be_bytes([
                bytes[data_start],
                bytes[data_start + 1],
                bytes[data_start + 2],
                bytes[data_start + 3],
            ]));
            height = Some(u32::from_be_bytes([
                bytes[data_start + 4],
                bytes[data_start + 5],
                bytes[data_start + 6],
                bytes[data_start + 7],
            ]));
        }

        if chunk_type == b"IEND" {
            return Some((crc_end, width.unwrap_or(0), height.unwrap_or(0)));
        }

        cursor = crc_end;
    }
}

fn extract_affinity_thumbnail_png(path: &Path) -> Option<Vec<u8>> {
    const MAX_AFFINITY_SCAN_BYTES: usize = 100 * 1024 * 1024;
    let file = fs::File::open(path).ok()?;
    let mut bytes = Vec::new();
    file.take(MAX_AFFINITY_SCAN_BYTES as u64)
        .read_to_end(&mut bytes)
        .ok()?;

    let mut best: Option<(u64, usize, usize)> = None; // (area, start, end)
    let mut index = 0usize;
    while index + 8 <= bytes.len() {
        if bytes[index] == 137
            && bytes[index + 1] == 80
            && bytes[index + 2] == 78
            && bytes[index + 3] == 71
            && bytes[index + 4] == 13
            && bytes[index + 5] == 10
            && bytes[index + 6] == 26
            && bytes[index + 7] == 10
        {
            if let Some((end, width, height)) = parse_embedded_png(&bytes, index) {
                let area = width as u64 * height as u64;
                match best {
                    Some((best_area, _, _)) if best_area >= area => {}
                    _ => {
                        best = Some((area, index, end));
                    }
                }
                index = end;
                continue;
            }
        }
        index += 1;
    }

    best.map(|(_, start, end)| bytes[start..end].to_vec())
}

fn mime_type_for_sidecar(path: &Path) -> &'static str {
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "bin" => "application/octet-stream",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "bmp" => "image/bmp",
        "ico" => "image/x-icon",
        "tif" | "tiff" => "image/tiff",
        "ktx2" => "image/ktx2",
        "ktx" => "image/ktx",
        _ => "application/octet-stream",
    }
}

fn is_external_or_data_uri(uri: &str) -> bool {
    let lower = uri.to_ascii_lowercase();
    lower.starts_with("data:")
        || lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("blob:")
}

fn inline_gltf_sidecars(home: &Path, gltf_path: &Path) -> Result<Vec<u8>, String> {
    let parent = gltf_path
        .parent()
        .ok_or_else(|| "gltf has no parent directory".to_string())?;
    let raw = fs::read_to_string(gltf_path).map_err(|error| error.to_string())?;
    let mut gltf: serde_json::Value = serde_json::from_str(&raw).map_err(|error| error.to_string())?;

    if let Some(buffers) = gltf.get_mut("buffers").and_then(|value| value.as_array_mut()) {
        for buffer in buffers {
            let Some(uri_value) = buffer.get_mut("uri") else {
                continue;
            };
            let Some(uri) = uri_value.as_str() else {
                continue;
            };
            if is_external_or_data_uri(uri) {
                continue;
            }
            let source_path = parent.join(uri);
            let canonical = source_path.canonicalize().map_err(|error| error.to_string())?;
            if !canonical.starts_with(home) {
                return Err("gltf sidecar is outside allowed root".to_string());
            }
            let bytes = fs::read(&canonical).map_err(|error| error.to_string())?;
            let mime = mime_type_for_sidecar(&canonical);
            *uri_value = serde_json::Value::String(format!("data:{mime};base64,{}", STANDARD.encode(bytes)));
        }
    }

    if let Some(images) = gltf.get_mut("images").and_then(|value| value.as_array_mut()) {
        for image in images {
            let Some(uri_value) = image.get_mut("uri") else {
                continue;
            };
            let Some(uri) = uri_value.as_str() else {
                continue;
            };
            if is_external_or_data_uri(uri) {
                continue;
            }
            let source_path = parent.join(uri);
            let canonical = source_path.canonicalize().map_err(|error| error.to_string())?;
            if !canonical.starts_with(home) {
                return Err("gltf sidecar is outside allowed root".to_string());
            }
            let bytes = fs::read(&canonical).map_err(|error| error.to_string())?;
            let mime = mime_type_for_sidecar(&canonical);
            *uri_value = serde_json::Value::String(format!("data:{mime};base64,{}", STANDARD.encode(bytes)));
        }
    }

    serde_json::to_vec(&gltf).map_err(|error| error.to_string())
}

fn is_previewable_text_ext(ext: &str) -> bool {
    matches!(
        ext,
        "txt"
            | "md"
            | "gd"
            | "gdshader"
            | "tscn"
            | "tres"
            | "godot"
            | "cfg"
            | "ini"
            | "rs"
            | "js"
            | "ts"
            | "jsx"
            | "tsx"
            | "json"
            | "toml"
            | "yaml"
            | "yml"
            | "html"
            | "css"
            | "scss"
            | "xml"
            | "sh"
            | "zsh"
            | "bash"
            | "py"
            | "go"
            | "java"
            | "c"
            | "cpp"
            | "h"
            | "hpp"
            | "csv"
            | "log"
    )
}

fn build_preview(focus_path: Option<&Path>) -> Option<PreviewModel> {
    let path = focus_path?;
    if !path.is_file() {
        return None;
    }

    let file_name = label_from_path(path);
    let subtitle = path.to_string_lossy().to_string();
    let icon = icon_for_entry(&file_name, false).to_string();
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();

    if is_affinity_ext(&ext) {
        if let Some(png) = extract_affinity_thumbnail_png(path) {
            let data_url = format!("data:image/png;base64,{}", STANDARD.encode(png));
            return Some(PreviewModel {
                kind: "image".to_string(),
                title: file_name,
                subtitle,
                path: path.to_string_lossy().to_string(),
                icon,
                image_data_url: Some(data_url),
                pdf_path: None,
                glb_path: None,
                text_head: None,
                note: Some("Embedded Affinity thumbnail preview.".to_string()),
            });
        }
        return Some(PreviewModel {
            kind: "unknown".to_string(),
            title: file_name,
            subtitle,
            path: path.to_string_lossy().to_string(),
            icon,
            image_data_url: None,
            pdf_path: None,
            glb_path: None,
            text_head: None,
            note: Some("No embedded Affinity thumbnail found.".to_string()),
        });
    }

    if let Some(mime) = image_mime_from_ext(&ext) {
        const MAX_IMAGE_PREVIEW_BYTES: u64 = 64 * 1024 * 1024;
        let file_size = fs::metadata(path).ok().map(|meta| meta.len()).unwrap_or(0);
        if file_size > MAX_IMAGE_PREVIEW_BYTES {
            return Some(PreviewModel {
                kind: "image".to_string(),
                title: file_name,
                subtitle,
                path: path.to_string_lossy().to_string(),
                icon,
                image_data_url: None,
                pdf_path: None,
                glb_path: None,
                text_head: None,
                note: Some("Image is too large for inline preview (max 64MB).".to_string()),
            });
        }

        let bytes = fs::read(path).ok()?;
        let data_url = format!("data:{mime};base64,{}", STANDARD.encode(bytes));
        return Some(PreviewModel {
            kind: "image".to_string(),
            title: file_name,
            subtitle,
            path: path.to_string_lossy().to_string(),
            icon,
            image_data_url: Some(data_url),
            pdf_path: None,
            glb_path: None,
            text_head: None,
            note: None,
        });
    }

    if ext == "pdf" {
        return Some(PreviewModel {
            kind: "pdf".to_string(),
            title: file_name,
            subtitle,
            path: path.to_string_lossy().to_string(),
            icon,
            image_data_url: None,
            pdf_path: Some(path.to_string_lossy().to_string()),
            glb_path: None,
            text_head: None,
            note: None,
        });
    }

    if ext == "glb" || ext == "gltf" {
        return Some(PreviewModel {
            kind: "glb".to_string(),
            title: file_name,
            subtitle,
            path: path.to_string_lossy().to_string(),
            icon,
            image_data_url: None,
            pdf_path: None,
            glb_path: Some(path.to_string_lossy().to_string()),
            text_head: None,
            note: None,
        });
    }

    if is_previewable_text_ext(&ext) {
        const MAX_TEXT_PREVIEW_BYTES: usize = 8 * 1024 * 1024;
        let file = fs::File::open(path).ok()?;
        let file_size = fs::metadata(path).ok().map(|meta| meta.len()).unwrap_or(0);
        let mut buffer = Vec::new();
        let mut limited = file.take(MAX_TEXT_PREVIEW_BYTES as u64);
        limited.read_to_end(&mut buffer).ok()?;
        let text = String::from_utf8_lossy(&buffer).to_string();
        return Some(PreviewModel {
            kind: "text".to_string(),
            title: file_name,
            subtitle,
            path: path.to_string_lossy().to_string(),
            icon,
            image_data_url: None,
            pdf_path: None,
            glb_path: None,
            text_head: Some(text),
            note: if file_size > MAX_TEXT_PREVIEW_BYTES as u64 {
                Some("Showing the first 8MB.".to_string())
            } else {
                None
            },
        });
    }

    Some(PreviewModel {
        kind: "unknown".to_string(),
        title: file_name,
        subtitle,
        path: path.to_string_lossy().to_string(),
        icon,
        image_data_url: None,
        pdf_path: None,
        glb_path: None,
        text_head: None,
        note: Some("No preview available for this file type yet.".to_string()),
    })
}

fn label_from_path(path: &Path) -> String {
    path.file_name()
        .map(|value| value.to_string_lossy().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| path.to_string_lossy().to_string())
}

fn components_under_home(home: &Path, focus: &Path) -> Vec<String> {
    focus
        .strip_prefix(home)
        .ok()
        .map(|relative| {
            relative
                .components()
                .filter_map(|component| {
                    let value = component.as_os_str().to_string_lossy().to_string();
                    if value.is_empty() {
                        None
                    } else {
                        Some(value)
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn is_directory(path: &Path) -> bool {
    fs::metadata(path).map(|meta| meta.is_dir()).unwrap_or(false)
}

fn build_columns(
    home: &Path,
    focus_path: Option<&Path>,
    directory_sorts: &BTreeMap<String, DirectorySortMode>,
) -> Vec<Column> {
    let components = focus_path
        .filter(|path| path.starts_with(home))
        .map(|path| components_under_home(home, path))
        .unwrap_or_default();
    let mut columns = Vec::new();
    let mut parent_dir = home.to_path_buf();
    let mut depth = 0usize;

    loop {
        let sort_mode = directory_sorts
            .get(&parent_dir.to_string_lossy().to_string())
            .copied()
            .unwrap_or(DirectorySortMode::Alphabetical);
        let entries = list_directory(&parent_dir, sort_mode);
        let selected_path = if let Some(component) = components.get(depth) {
            let candidate = parent_dir.join(component);
            let candidate_str = candidate.to_string_lossy().to_string();
            if entries.iter().any(|entry| entry.path == candidate_str) {
                candidate_str
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        columns.push(Column {
            title: label_from_path(&parent_dir),
            path: parent_dir.to_string_lossy().to_string(),
            sort_mode: match sort_mode {
                DirectorySortMode::Alphabetical => "alphabetical".to_string(),
                DirectorySortMode::RecentFirst => "recent_first".to_string(),
            },
            entries,
            selected_path: selected_path.clone(),
        });

        if selected_path.is_empty() || !is_directory(Path::new(&selected_path)) {
            break;
        }

        parent_dir = PathBuf::from(selected_path);
        depth += 1;
    }

    columns
}

fn copy_path_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    let metadata = fs::metadata(src)?;
    if metadata.is_dir() {
        fs::create_dir_all(dst)?;
        for entry in fs::read_dir(src)? {
            let entry = entry?;
            let child_src = entry.path();
            let child_dst = dst.join(entry.file_name());
            copy_path_recursive(&child_src, &child_dst)?;
        }
        Ok(())
    } else {
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(src, dst)?;
        Ok(())
    }
}

fn unique_destination_path(target_dir: &Path, source_path: &Path) -> PathBuf {
    let fallback_name = "item".to_string();
    let file_name = source_path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_string())
        .unwrap_or(fallback_name);
    let candidate = target_dir.join(&file_name);
    if !candidate.exists() {
        return candidate;
    }

    let source_is_dir = source_path.is_dir();
    if source_is_dir {
        for index in 1..10_000 {
            let suffix = if index == 1 {
                " copy".to_string()
            } else {
                format!(" copy {index}")
            };
            let next = target_dir.join(format!("{file_name}{suffix}"));
            if !next.exists() {
                return next;
            }
        }
        return target_dir.join(format!("{file_name} copy"));
    }

    let source = Path::new(&file_name);
    let stem = source
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("file");
    let ext = source.extension().and_then(|value| value.to_str());
    for index in 1..10_000 {
        let suffix = if index == 1 {
            " copy".to_string()
        } else {
            format!(" copy {index}")
        };
        let next_name = if let Some(ext) = ext {
            format!("{stem}{suffix}.{ext}")
        } else {
            format!("{stem}{suffix}")
        };
        let next = target_dir.join(next_name);
        if !next.exists() {
            return next;
        }
    }

    target_dir.join(file_name)
}

fn move_path_with_fallback(src: &Path, dst: &Path) -> std::io::Result<()> {
    match fs::rename(src, dst) {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == ErrorKind::CrossesDevices => {
            copy_path_recursive(src, dst)?;
            if src.is_dir() {
                fs::remove_dir_all(src)?;
            } else {
                fs::remove_file(src)?;
            }
            Ok(())
        }
        Err(err) => Err(err),
    }
}

fn perform_drop_operation(
    home: &Path,
    target_dir: &Path,
    source_paths: &[String],
    operation: &str,
) -> Result<(), String> {
    if !target_dir.starts_with(home) || !target_dir.is_dir() {
        return Err("invalid target directory".to_string());
    }
    if source_paths.is_empty() {
        return Ok(());
    }

    for source in source_paths {
        let src = PathBuf::from(source);
        if !src.exists() {
            continue;
        }
        let destination = unique_destination_path(target_dir, &src);
        if operation == "move" {
            move_path_with_fallback(&src, &destination).map_err(|error| error.to_string())?;
        } else {
            copy_path_recursive(&src, &destination).map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

fn resolve_home_scoped_path(home: &Path, path: &str) -> Result<PathBuf, String> {
    let candidate = PathBuf::from(path);
    if !candidate.is_absolute() {
        return Err("path must be absolute".to_string());
    }
    if !candidate.starts_with(home) {
        return Err("path is outside allowed root".to_string());
    }
    Ok(candidate)
}

fn validate_rename_name(new_name: &str) -> Result<String, String> {
    let trimmed = new_name.trim();
    if trimmed.is_empty() {
        return Err("new name cannot be empty".to_string());
    }
    if trimmed == "." || trimmed == ".." {
        return Err("invalid name".to_string());
    }
    if trimmed.contains('/') {
        return Err("new name cannot contain '/'".to_string());
    }
    Ok(trimmed.to_string())
}

fn move_to_trash(path: &Path) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        let trash_dir = home_directory().join(".Trash");
        fs::create_dir_all(&trash_dir).map_err(|error| error.to_string())?;
        let destination = unique_destination_path(&trash_dir, path);
        move_path_with_fallback(path, &destination).map_err(|error| error.to_string())
    }
    #[cfg(not(target_os = "macos"))]
    {
        if path.is_dir() {
            fs::remove_dir_all(path).map_err(|error| error.to_string())
        } else {
            fs::remove_file(path).map_err(|error| error.to_string())
        }
    }
}

fn remove_permanently(path: &Path) -> Result<(), String> {
    if path.is_dir() {
        fs::remove_dir_all(path).map_err(|error| error.to_string())
    } else {
        fs::remove_file(path).map_err(|error| error.to_string())
    }
}

fn set_active_focus_after_path_change(model: &mut TabsModel, home: &Path, preferred: Option<&Path>, source: &Path) {
    let fallback = source.parent().unwrap_or(home);
    let next = preferred.filter(|path| path.exists()).unwrap_or(fallback);
    let normalized = normalize_tab_path(home, Some(&next.to_string_lossy()));
    if let Some(tab) = active_tab_mut(model) {
        tab.focus_path = Some(normalized.to_string_lossy().to_string());
    }
}

fn home_directory() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("/"))
}

fn tabs_state_path() -> PathBuf {
    home_directory().join(".lumen").join("state.json")
}

fn load_tabs_model_from_disk() -> Option<TabsModel> {
    let path = tabs_state_path();
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str::<TabsModel>(&raw).ok()
}

fn persist_tabs_model(model: &TabsModel) {
    let path = tabs_state_path();
    if let Some(parent) = path.parent() {
        if fs::create_dir_all(parent).is_err() {
            return;
        }
    }

    let Ok(json) = serde_json::to_vec_pretty(model) else {
        return;
    };
    let tmp_path = path.with_extension("json.tmp");
    let Ok(mut file) = fs::File::create(&tmp_path) else {
        return;
    };
    if file.write_all(&json).is_err() || file.flush().is_err() {
        let _ = fs::remove_file(&tmp_path);
        return;
    }
    let _ = fs::rename(tmp_path, path);
}

pub struct FileTabsState {
    tabs: Mutex<TabsModel>,
}

impl Default for FileTabsState {
    fn default() -> Self {
        let model = load_tabs_model_from_disk().unwrap_or(TabsModel {
            tabs: Vec::new(),
            active_id: 1,
            next_id: 2,
            directory_sorts: BTreeMap::new(),
            window: None,
        });
        Self {
            tabs: Mutex::new(model),
        }
    }
}

fn normalize_tab_path(home: &Path, input: Option<&str>) -> PathBuf {
    let raw = input
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| home.to_path_buf());

    if raw.starts_with(home) {
        raw
    } else {
        home.to_path_buf()
    }
}

fn active_tab_root_path(tab: &TabState, home: &Path) -> PathBuf {
    tab.root_path
        .as_deref()
        .map(PathBuf::from)
        .filter(|path| path.starts_with(home) && path.is_dir())
        .unwrap_or_else(|| home.to_path_buf())
}

fn relative_path_from_root(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .ok()
        .map(|relative| {
            let value = relative.to_string_lossy().to_string();
            if value.is_empty() { ".".to_string() } else { value }
        })
        .unwrap_or_else(|| path.to_string_lossy().to_string())
}

fn ensure_tabs(model: &mut TabsModel, home: &Path) {
    model.directory_sorts.retain(|path, mode| {
        PathBuf::from(path).starts_with(home) && *mode != DirectorySortMode::Alphabetical
    });

    for tab in &mut model.tabs {
        if let Some(root) = tab.root_path.clone() {
            let normalized = normalize_tab_path(home, Some(&root));
            if normalized == home {
                tab.root_path = None;
            } else {
                tab.root_path = Some(normalized.to_string_lossy().to_string());
            }
        }
        let tab_root = active_tab_root_path(tab, home);
        if let Some(path) = tab.focus_path.clone() {
            let normalized = normalize_tab_path(home, Some(&path));
            if normalized.starts_with(&tab_root) {
                tab.focus_path = Some(normalized.to_string_lossy().to_string());
            } else {
                tab.focus_path = None;
            }
        }
    }

    if model.next_id == 0 {
        model.next_id = 1;
    }
    if let Some(max_id) = model.tabs.iter().map(|tab| tab.id).max() {
        if model.next_id <= max_id {
            model.next_id = max_id + 1;
        }
    }

    if model.tabs.is_empty() {
        model.tabs.push(TabState {
            id: 1,
            root_path: None,
            focus_path: None,
        });
        model.active_id = 1;
        model.next_id = 2;
    }
}

pub fn restore_main_window_state<R: Runtime>(app: &AppHandle<R>) {
    let window = match app.get_webview_window("main") {
        Some(window) => window,
        None => return,
    };

    let saved_geometry = {
        let state = app.state::<FileTabsState>();
        let Ok(model) = state.tabs.lock() else {
            return;
        };
        model.window.clone()
    };

    if let Some(geometry) = saved_geometry {
        let _ = window.set_size(PhysicalSize::new(geometry.width, geometry.height));
        let _ = window.set_position(PhysicalPosition::new(geometry.x, geometry.y));
    }
}

pub fn persist_main_window_state<R: Runtime>(window: &Window<R>) {
    let Ok(position) = window.inner_position() else {
        return;
    };
    let Ok(size) = window.inner_size() else {
        return;
    };

    let state = window.state::<FileTabsState>();
    let Ok(mut model) = state.tabs.lock() else {
        return;
    };
    model.window = Some(WindowGeometry {
        x: position.x,
        y: position.y,
        width: size.width,
        height: size.height,
    });
    persist_tabs_model(&model);
}

fn active_tab_mut(model: &mut TabsModel) -> Option<&mut TabState> {
    let index = model
        .tabs
        .iter()
        .position(|tab| tab.id == model.active_id)
        .unwrap_or(0);
    model.tabs.get_mut(index)
}

fn render_view(model: &TabsModel) -> String {
    let mut context = Context::new();
    let home = home_directory();
    let active_tab = model
        .tabs
        .iter()
        .find(|tab| tab.id == model.active_id)
        .or_else(|| model.tabs.first());
    let active_root_path = active_tab
        .map(|tab| active_tab_root_path(tab, &home))
        .unwrap_or_else(|| home.clone());
    let active_focus_path = active_tab
        .and_then(|tab| tab.focus_path.clone())
        .map(PathBuf::from)
        .filter(|path| path.starts_with(&active_root_path));
    let columns = build_columns(
        &active_root_path,
        active_focus_path.as_deref(),
        &model.directory_sorts,
    );
    let preview = build_preview(active_focus_path.as_deref());
    let active_path_for_new = active_focus_path
        .as_ref()
        .map(|path| path.to_string_lossy().to_string())
        .unwrap_or_else(|| active_root_path.to_string_lossy().to_string());
    let tabs: Vec<TabView> = model
        .tabs
        .iter()
        .map(|tab| TabView {
            id: tab.id.to_string(),
            title: tab
                .focus_path
                .as_deref()
                .map(Path::new)
                .map(label_from_path)
                .unwrap_or_else(|| "New Tab".to_string()),
            is_active: tab.id == model.active_id,
        })
        .collect();

    context.insert("root_name", &label_from_path(&active_root_path));
    context.insert("root_path", &active_root_path.to_string_lossy().to_string());
    context.insert("columns", &columns);
    context.insert("tabs", &tabs);
    context.insert("active_path", &active_path_for_new);
    context.insert("active_tab_id", &model.active_id.to_string());
    context.insert("preview", &preview);

    // context.insert("user_needs_to_setup_biometrics", &user_needs_to_setup_biometrics);
    render!("files/index", &context)
}

pub struct FileContextMenuState {
    pub pending: Mutex<Option<PendingContextTarget>>,
}

#[derive(Debug, Clone)]
pub struct PendingContextTarget {
    pub path: String,
    pub relative_path: String,
}

impl Default for FileContextMenuState {
    fn default() -> Self {
        Self {
            pending: Mutex::new(None),
        }
    }
}

#[tauri::command]
pub fn index(state: State<'_, FileTabsState>) -> String {
    let home = home_directory();
    let mut model = match state.tabs.lock() {
        Ok(model) => model,
        Err(_) => return String::new(),
    };
    ensure_tabs(&mut model, &home);
    persist_tabs_model(&model);
    render_view(&model)
}

#[tauri::command]
pub fn navigate(state: State<'_, FileTabsState>, path: String) -> String {
    let home = home_directory();
    let mut model = match state.tabs.lock() {
        Ok(model) => model,
        Err(_) => return String::new(),
    };
    ensure_tabs(&mut model, &home);

    let normalized = normalize_tab_path(&home, Some(&path));
    if let Some(tab) = active_tab_mut(&mut model) {
        let root = active_tab_root_path(tab, &home);
        if normalized.starts_with(&root) {
            tab.focus_path = Some(normalized.to_string_lossy().to_string());
        }
    }

    persist_tabs_model(&model);
    render_view(&model)
}

#[tauri::command]
pub fn activate_tab(state: State<'_, FileTabsState>, tab_id: String) -> String {
    let home = home_directory();
    let mut model = match state.tabs.lock() {
        Ok(model) => model,
        Err(_) => return String::new(),
    };
    ensure_tabs(&mut model, &home);

    if let Ok(parsed_id) = tab_id.parse::<u64>() {
        if model.tabs.iter().any(|tab| tab.id == parsed_id) {
            model.active_id = parsed_id;
        }
    }

    persist_tabs_model(&model);
    render_view(&model)
}

#[tauri::command]
pub fn new_tab(state: State<'_, FileTabsState>, _path: String) -> String {
    let home = home_directory();
    let mut model = match state.tabs.lock() {
        Ok(model) => model,
        Err(_) => return String::new(),
    };
    ensure_tabs(&mut model, &home);

    let id = model.next_id;
    model.next_id += 1;
    model.tabs.push(TabState {
        id,
        root_path: None,
        focus_path: None,
    });
    model.active_id = id;

    persist_tabs_model(&model);
    render_view(&model)
}

#[tauri::command]
pub fn close_tab(state: State<'_, FileTabsState>, tab_id: String) -> String {
    let home = home_directory();
    let mut model = match state.tabs.lock() {
        Ok(model) => model,
        Err(_) => return String::new(),
    };
    ensure_tabs(&mut model, &home);

    let Ok(parsed_id) = tab_id.parse::<u64>() else {
        return render_view(&model);
    };

    let Some(index) = model.tabs.iter().position(|tab| tab.id == parsed_id) else {
        return render_view(&model);
    };

    if model.tabs.len() == 1 {
        model.tabs[0].root_path = None;
        model.tabs[0].focus_path = None;
        model.active_id = model.tabs[0].id;
        persist_tabs_model(&model);
        return render_view(&model);
    }

    model.tabs.remove(index);

    if model.active_id == parsed_id {
        let new_index = if index == 0 { 0 } else { index - 1 };
        model.active_id = model.tabs[new_index].id;
    } else if !model.tabs.iter().any(|tab| tab.id == model.active_id) {
        model.active_id = model.tabs[0].id;
    }

    persist_tabs_model(&model);
    render_view(&model)
}

#[tauri::command]
pub fn drop_files_into_directory(
    state: State<'_, FileTabsState>,
    target_dir: String,
    source_paths: Vec<String>,
    operation: String,
) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let op = operation.to_ascii_lowercase();
    if op != "copy" && op != "move" {
        return Err("unsupported operation".to_string());
    }

    let target = normalize_tab_path(&home, Some(&target_dir));
    perform_drop_operation(&home, &target, &source_paths, &op)?;

    if let Some(tab) = active_tab_mut(&mut model) {
        let root = active_tab_root_path(tab, &home);
        if target.starts_with(&root) {
            tab.focus_path = Some(target.to_string_lossy().to_string());
        }
    }
    persist_tabs_model(&model);
    Ok(render_view(&model))
}

#[tauri::command]
pub fn show_file_context_menu(
    window: Window,
    state: State<'_, FileContextMenuState>,
    tabs_state: State<'_, FileTabsState>,
    path: String,
    is_dir: bool,
    x: f64,
    y: f64,
) -> Result<(), String> {
    let home = home_directory();
    let mut model = tabs_state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);
    let active_tab = model
        .tabs
        .iter()
        .find(|tab| tab.id == model.active_id)
        .or_else(|| model.tabs.first())
        .ok_or_else(|| "missing active tab".to_string())?;
    let root = active_tab_root_path(active_tab, &home);
    drop(model);

    let path_buf = PathBuf::from(&path);
    let relative_path = relative_path_from_root(&path_buf, &root);

    let mut pending = state
        .pending
        .lock()
        .map_err(|_| "failed to lock context menu state".to_string())?;
    *pending = Some(PendingContextTarget {
        path,
        relative_path,
    });
    drop(pending);

    let mut builder = MenuBuilder::new(&window);
    if is_dir {
        builder = builder
            .text("ctx_new_dir", "Create directory")
            .text("ctx_new_file", "Create file")
            .separator()
            .text("ctx_set_tab_root", "Set as current tab root")
            .separator()
            .text("ctx_open_warp", "Open in Warp")
            .text("ctx_open_zed", "Open in Zed")
            .separator();
    } else {
        builder = builder.text("ctx_open_zed", "Open in Zed").separator();
    }
    let menu = builder
        .text("copy_absolute_path", "Copy absolute path")
        .text("copy_relative_path", "Copy relative path")
        .separator()
        .text("ctx_rename", "Rename")
        .text("ctx_trash", "Trash")
        .text("ctx_delete", "Delete")
        .build()
        .map_err(|error| error.to_string())?;

    window
        .popup_menu_at(&menu, LogicalPosition::new(x, y))
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub fn rename_path(
    state: State<'_, FileTabsState>,
    path: String,
    new_name: String,
) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let source = resolve_home_scoped_path(&home, &path)?;
    if source == home {
        return Err("cannot rename root directory".to_string());
    }
    if !source.exists() {
        return Err("path does not exist".to_string());
    }
    let name = validate_rename_name(&new_name)?;
    let parent = source
        .parent()
        .ok_or_else(|| "path has no parent".to_string())?;
    let destination = parent.join(name);
    if destination.exists() && destination != source {
        return Err("destination already exists".to_string());
    }
    if destination != source {
        fs::rename(&source, &destination).map_err(|error| error.to_string())?;
    }
    set_active_focus_after_path_change(&mut model, &home, Some(&destination), &source);
    persist_tabs_model(&model);
    Ok(render_view(&model))
}

#[tauri::command]
pub fn trash_path(state: State<'_, FileTabsState>, path: String) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let source = resolve_home_scoped_path(&home, &path)?;
    if source == home {
        return Err("cannot trash root directory".to_string());
    }
    if !source.exists() {
        return Err("path does not exist".to_string());
    }
    move_to_trash(&source)?;
    set_active_focus_after_path_change(&mut model, &home, None, &source);
    persist_tabs_model(&model);
    Ok(render_view(&model))
}

#[tauri::command]
pub fn delete_path(state: State<'_, FileTabsState>, path: String) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let source = resolve_home_scoped_path(&home, &path)?;
    if source == home {
        return Err("cannot delete root directory".to_string());
    }
    if !source.exists() {
        return Err("path does not exist".to_string());
    }
    remove_permanently(&source)?;
    set_active_focus_after_path_change(&mut model, &home, None, &source);
    persist_tabs_model(&model);
    Ok(render_view(&model))
}

#[tauri::command]
pub fn create_directory(
    state: State<'_, FileTabsState>,
    parent_path: String,
    name: String,
) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let parent = resolve_home_scoped_path(&home, &parent_path)?;
    if !parent.is_dir() {
        return Err("parent path is not a directory".to_string());
    }
    let clean_name = validate_rename_name(&name)?;
    let target = parent.join(clean_name);
    if target.exists() {
        return Err("destination already exists".to_string());
    }
    fs::create_dir(&target).map_err(|error| error.to_string())?;
    set_active_focus_after_path_change(&mut model, &home, Some(&target), &parent);
    persist_tabs_model(&model);
    Ok(render_view(&model))
}

#[tauri::command]
pub fn create_file(
    state: State<'_, FileTabsState>,
    parent_path: String,
    name: String,
) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let parent = resolve_home_scoped_path(&home, &parent_path)?;
    if !parent.is_dir() {
        return Err("parent path is not a directory".to_string());
    }
    let mut clean_name = validate_rename_name(&name)?;
    if !clean_name.to_ascii_lowercase().ends_with(".txt") {
        clean_name.push_str(".txt");
    }
    let target = parent.join(clean_name);
    if target.exists() {
        return Err("destination already exists".to_string());
    }
    fs::File::create(&target).map_err(|error| error.to_string())?;
    set_active_focus_after_path_change(&mut model, &home, Some(&target), &parent);
    persist_tabs_model(&model);
    Ok(render_view(&model))
}

#[tauri::command]
pub fn set_directory_sort(
    state: State<'_, FileTabsState>,
    path: String,
    mode: String,
) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let directory = resolve_home_scoped_path(&home, &path)?;
    if !directory.is_dir() {
        return Err("path is not a directory".to_string());
    }
    let parsed_mode =
        DirectorySortMode::from_input(&mode).ok_or_else(|| "invalid sort mode".to_string())?;
    let key = directory.to_string_lossy().to_string();
    if parsed_mode == DirectorySortMode::Alphabetical {
        model.directory_sorts.remove(&key);
    } else {
        model.directory_sorts.insert(key, parsed_mode);
    }

    persist_tabs_model(&model);
    Ok(render_view(&model))
}

fn open_path_with_application(app_name: &str, path: &Path) -> Result<(), String> {
    let status = Command::new("open")
        .arg("-a")
        .arg(app_name)
        .arg(path)
        .status()
        .map_err(|error| error.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("failed to launch {app_name}"))
    }
}

#[tauri::command]
pub async fn load_pdf_preview_data(path: String) -> Result<String, String> {
    let home = home_directory();
    let target = resolve_home_scoped_path(&home, &path)?;
    if !target.is_file() {
        return Err("path is not a file".to_string());
    }
    let ext = target
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    if ext != "pdf" {
        return Err("path is not a pdf".to_string());
    }

    const MAX_PDF_PREVIEW_BYTES: u64 = 100 * 1024 * 1024;
    let metadata = fs::metadata(&target).map_err(|error| error.to_string())?;
    if metadata.len() > MAX_PDF_PREVIEW_BYTES {
        return Err("PDF is too large for inline preview.".to_string());
    }

    let bytes = tokio::fs::read(&target)
        .await
        .map_err(|error| error.to_string())?;
    Ok(STANDARD.encode(bytes))
}

#[tauri::command]
pub async fn load_glb_preview_data(path: String) -> Result<ModelPreviewData, String> {
    let home = home_directory();
    let target = resolve_home_scoped_path(&home, &path)?;
    if !target.is_file() {
        return Err("path is not a file".to_string());
    }
    let ext = target
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    if ext != "glb" && ext != "gltf" {
        return Err("path is not a glb/gltf".to_string());
    }

    const MAX_GLB_PREVIEW_BYTES: u64 = 100 * 1024 * 1024;
    let metadata = fs::metadata(&target).map_err(|error| error.to_string())?;
    if metadata.len() > MAX_GLB_PREVIEW_BYTES {
        return Err("GLB is too large for inline preview.".to_string());
    }

    let bytes = if ext == "gltf" {
        inline_gltf_sidecars(&home, &target)?
    } else {
        tokio::fs::read(&target)
            .await
            .map_err(|error| error.to_string())?
    };
    let mime_type = if ext == "gltf" {
        "model/gltf+json".to_string()
    } else {
        "model/gltf-binary".to_string()
    };
    Ok(ModelPreviewData {
        mime_type,
        base64: STANDARD.encode(bytes),
    })
}

#[tauri::command]
pub fn open_in_zed(path: String) -> Result<(), String> {
    let home = home_directory();
    let target = resolve_home_scoped_path(&home, &path)?;
    open_path_with_application("Zed", &target)
}

#[tauri::command]
pub fn open_in_warp(path: String) -> Result<(), String> {
    let home = home_directory();
    let target = resolve_home_scoped_path(&home, &path)?;
    if !target.is_dir() {
        return Err("warp can only open directories".to_string());
    }
    open_path_with_application("Warp", &target)
}

#[tauri::command]
pub fn set_tab_root(state: State<'_, FileTabsState>, path: String) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let target = resolve_home_scoped_path(&home, &path)?;
    if !target.is_dir() {
        return Err("tab root must be a directory".to_string());
    }
    if let Some(tab) = active_tab_mut(&mut model) {
        if target == home {
            tab.root_path = None;
        } else {
            tab.root_path = Some(target.to_string_lossy().to_string());
        }
        if let Some(focus) = tab.focus_path.clone().map(PathBuf::from) {
            if !focus.starts_with(&target) {
                tab.focus_path = None;
            }
        }
    }
    persist_tabs_model(&model);
    Ok(render_view(&model))
}
