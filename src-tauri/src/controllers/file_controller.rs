use base64::{engine::general_purpose::STANDARD, Engine};
use flate2::read::GzDecoder;
use pulldown_cmark::{html, CowStr, Event, Options, Parser, Tag};
use serde::Serialize;
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet, HashMap},
    env, fs,
    hash::{Hash, Hasher},
    io::Read,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    time::UNIX_EPOCH,
};
use tauri::{
    menu::MenuBuilder, AppHandle, LogicalPosition, Manager, PhysicalPosition, PhysicalSize,
    Runtime, State, Window,
};
use tera::Context;
use zip::ZipArchive;

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
    affinity_image_data_urls: Option<Vec<String>>,
    pdf_path: Option<String>,
    glb_path: Option<String>,
    video_path: Option<String>,
    zip_entries: Option<Vec<ZipEntryItem>>,
    text_head: Option<String>,
    note: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
struct ZipEntryItem {
    name: String,
    is_dir: bool,
    size: u64,
    icon: &'static str,
}

#[derive(Debug, Serialize)]
pub struct ZipEntryPreviewData {
    kind: String,
    title: String,
    subtitle: String,
    image_data_url: Option<String>,
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

#[derive(Debug, Serialize, Clone)]
struct SearchResultItem {
    path: String,
    relative_path: String,
    name: String,
    icon: &'static str,
}

#[derive(Debug, Serialize)]
pub struct PathContextCapabilities {
    is_dir: bool,
    show_default_open: bool,
    show_github_desktop: bool,
}

#[derive(Debug)]
struct SearchModel {
    generation: u64,
    query: String,
    root_path: String,
    running: bool,
    scanned: u64,
    results: Vec<SearchResultItem>,
}

pub struct FileSearchState {
    search: Arc<Mutex<SearchModel>>,
}

impl Default for FileSearchState {
    fn default() -> Self {
        Self {
            search: Arc::new(Mutex::new(SearchModel {
                generation: 0,
                query: String::new(),
                root_path: String::new(),
                running: false,
                scanned: 0,
                results: Vec::new(),
            })),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TabState {
    id: u64,
    #[serde(default)]
    root_path: Option<String>,
    #[serde(default)]
    focus_path: Option<String>,
    #[serde(default)]
    show_hidden: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

fn list_directory(
    path: &PathBuf,
    sort_mode: DirectorySortMode,
    show_hidden: bool,
) -> Vec<FileEntry> {
    let Ok(entries) = fs::read_dir(path) else {
        return Vec::new();
    };

    let mut items: Vec<SortableEntry> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let entry_path = entry.path();
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy().to_string();
            if !show_hidden && name.starts_with('.') {
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
                b.modified_ms
                    .cmp(&a.modified_ms)
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

fn video_mime_from_ext(ext: &str) -> Option<&'static str> {
    match ext {
        "mp4" => Some("video/mp4"),
        "mov" => Some("video/quicktime"),
        "m4v" => Some("video/x-m4v"),
        "webm" => Some("video/webm"),
        _ => None,
    }
}

fn is_affinity_ext(ext: &str) -> bool {
    matches!(ext, "afdesign" | "afphoto" | "afpub")
}

fn read_u32_with_endian(bytes: &[u8], little_endian: bool) -> Option<u32> {
    if bytes.len() < 4 {
        return None;
    }
    let array = [bytes[0], bytes[1], bytes[2], bytes[3]];
    Some(if little_endian {
        u32::from_le_bytes(array)
    } else {
        u32::from_be_bytes(array)
    })
}

fn encode_rgba_png(width: u32, height: u32, rgba: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut encoder = png::Encoder::new(&mut out, width, height);
    encoder.set_color(png::ColorType::Rgba);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header().ok()?;
    writer.write_image_data(rgba).ok()?;
    drop(writer);
    Some(out)
}

fn push_unique_preview_png(
    previews: &mut Vec<Vec<u8>>,
    seen: &mut std::collections::HashSet<u64>,
    png: Vec<u8>,
    max_count: usize,
) {
    if previews.len() >= max_count {
        return;
    }
    let mut hasher = DefaultHasher::new();
    png.hash(&mut hasher);
    let digest = hasher.finish();
    if seen.insert(digest) {
        previews.push(png);
    }
}

fn decode_blend_test_payload(data: &[u8], little_endian: bool) -> Vec<Vec<u8>> {
    const MAX_BLEND_PREVIEWS: usize = 24;
    let mut previews = Vec::new();
    let mut seen = std::collections::HashSet::new();

    let mut png_at = 0usize;
    while png_at + 8 <= data.len() {
        if let Some((png_end, _, _)) = parse_embedded_png(data, png_at) {
            push_unique_preview_png(
                &mut previews,
                &mut seen,
                data[png_at..png_end].to_vec(),
                MAX_BLEND_PREVIEWS,
            );
            if previews.len() >= MAX_BLEND_PREVIEWS {
                return previews;
            }
            png_at = png_end;
            continue;
        }
        png_at += 1;
    }

    if !previews.is_empty() {
        return previews;
    }

    if data.len() < 8 {
        return previews;
    }
    let endianness_candidates = [little_endian, !little_endian];
    for candidate_endian in endianness_candidates {
        let Some(width) = read_u32_with_endian(&data[0..4], candidate_endian) else {
            continue;
        };
        let Some(height) = read_u32_with_endian(&data[4..8], candidate_endian) else {
            continue;
        };
        if width == 0 || height == 0 || width > 8192 || height > 8192 {
            continue;
        }
        let Some(pixel_count) = (width as usize).checked_mul(height as usize) else {
            continue;
        };
        let Some(raw_len) = pixel_count.checked_mul(4) else {
            continue;
        };
        let Some(pixels_end) = 8usize.checked_add(raw_len) else {
            continue;
        };
        if pixels_end > data.len() {
            continue;
        }
        let raw = &data[8..pixels_end];
        let mut rgba = vec![0u8; raw_len];
        for y in 0..height as usize {
            let src_y = height as usize - 1 - y;
            for x in 0..width as usize {
                let src_i = (src_y * width as usize + x) * 4;
                let dst_i = (y * width as usize + x) * 4;
                let b = raw[src_i];
                let g = raw[src_i + 1];
                let r = raw[src_i + 2];
                let a = raw[src_i + 3];
                rgba[dst_i] = r;
                rgba[dst_i + 1] = g;
                rgba[dst_i + 2] = b;
                rgba[dst_i + 3] = a;
            }
        }
        if let Some(png) = encode_rgba_png(width, height, &rgba) {
            push_unique_preview_png(&mut previews, &mut seen, png, MAX_BLEND_PREVIEWS);
        }
        if previews.len() >= MAX_BLEND_PREVIEWS {
            break;
        }
    }
    previews
}

fn extract_blend_preview_from_bytes_with_mode(
    bytes: &[u8],
    align_blocks_to_4: bool,
) -> Vec<Vec<u8>> {
    const MAX_BLEND_PREVIEWS: usize = 24;
    let mut previews = Vec::new();
    let mut seen = std::collections::HashSet::new();

    if bytes.len() < 12 || &bytes[0..7] != b"BLENDER" {
        return previews;
    }

    let pointer_size = match bytes[7] {
        b'_' => 4usize,
        b'-' => 8usize,
        _ => return previews,
    };
    let little_endian = match bytes[8] {
        b'v' => true,
        b'V' => false,
        _ => return previews,
    };
    let block_header_len = 4 + 4 + pointer_size + 4 + 4;
    let mut offset = 12usize;

    while offset + block_header_len <= bytes.len() {
        let code = &bytes[offset..offset + 4];
        if code == b"ENDB" {
            break;
        }
        let size = match read_u32_with_endian(&bytes[offset + 4..offset + 8], little_endian) {
            Some(value) => value as usize,
            None => break,
        };
        let data_start = offset + block_header_len;
        let Some(data_end) = data_start.checked_add(size) else {
            break;
        };
        if data_end > bytes.len() {
            break;
        }

        if code == b"TEST" {
            let data = &bytes[data_start..data_end];
            for png in decode_blend_test_payload(data, little_endian) {
                push_unique_preview_png(&mut previews, &mut seen, png, MAX_BLEND_PREVIEWS);
                if previews.len() >= MAX_BLEND_PREVIEWS {
                    return previews;
                }
            }
        }

        let next_offset = if align_blocks_to_4 {
            data_end.saturating_add(3) & !3
        } else {
            data_end
        };
        if next_offset <= offset {
            break;
        }
        offset = next_offset;
    }

    // Fallback: scan for raw TEST marker payloads even if block parsing misses edge cases.
    let mut idx = 12usize;
    while idx + 4 < bytes.len() && previews.len() < MAX_BLEND_PREVIEWS {
        if &bytes[idx..idx + 4] == b"TEST" {
            for payload_start in [idx + 4, idx + 8, idx + 12] {
                if payload_start >= bytes.len() || previews.len() >= MAX_BLEND_PREVIEWS {
                    continue;
                }
                let data = &bytes[payload_start..];
                for png in decode_blend_test_payload(data, little_endian) {
                    push_unique_preview_png(&mut previews, &mut seen, png, MAX_BLEND_PREVIEWS);
                    if previews.len() >= MAX_BLEND_PREVIEWS {
                        break;
                    }
                }
            }
        }
        idx += 1;
    }

    previews
}

fn extract_blend_preview_from_bytes(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut previews = extract_blend_preview_from_bytes_with_mode(bytes, false);
    if previews.is_empty() {
        previews = extract_blend_preview_from_bytes_with_mode(bytes, true);
    }
    previews
}

fn maybe_gzip_decompress(bytes: &[u8]) -> Option<Vec<u8>> {
    if bytes.len() < 2 || bytes[0] != 0x1f || bytes[1] != 0x8b {
        return None;
    }
    let mut decoder = GzDecoder::new(bytes);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).ok()?;
    Some(out)
}

fn extract_blend_preview_pngs(path: &Path) -> Vec<Vec<u8>> {
    const MAX_BLEND_SCAN_BYTES: usize = 512 * 1024 * 1024;
    let Ok(file) = fs::File::open(path) else {
        return Vec::new();
    };
    let mut bytes = Vec::new();
    if file
        .take(MAX_BLEND_SCAN_BYTES as u64)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return Vec::new();
    }

    let previews = extract_blend_preview_from_bytes(&bytes);
    if !previews.is_empty() {
        return previews;
    }
    if let Some(inflated) = maybe_gzip_decompress(&bytes) {
        return extract_blend_preview_from_bytes(&inflated);
    }
    Vec::new()
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

fn extract_affinity_embedded_pngs(path: &Path) -> Option<Vec<Vec<u8>>> {
    const MAX_AFFINITY_SCAN_BYTES: usize = 100 * 1024 * 1024;
    const MAX_EMBEDDED_PNGS: usize = 24;
    let file = fs::File::open(path).ok()?;
    let mut bytes = Vec::new();
    file.take(MAX_AFFINITY_SCAN_BYTES as u64)
        .read_to_end(&mut bytes)
        .ok()?;

    let mut found: Vec<(u64, usize, usize)> = Vec::new(); // (area, start, end)
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
                found.push((area, index, end));
                index = end;
                continue;
            }
        }
        index += 1;
    }

    if found.is_empty() {
        return None;
    }

    found.sort_by(|a, b| a.0.cmp(&b.0));
    found.dedup_by(|a, b| a.1 == b.1 && a.2 == b.2);
    if found.len() > MAX_EMBEDDED_PNGS {
        found.truncate(MAX_EMBEDDED_PNGS);
    }

    Some(
        found
            .into_iter()
            .map(|(_, start, end)| bytes[start..end].to_vec())
            .collect(),
    )
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
    let mut gltf: serde_json::Value =
        serde_json::from_str(&raw).map_err(|error| error.to_string())?;

    if let Some(buffers) = gltf
        .get_mut("buffers")
        .and_then(|value| value.as_array_mut())
    {
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
            let canonical = source_path
                .canonicalize()
                .map_err(|error| error.to_string())?;
            if !canonical.starts_with(home) {
                return Err("gltf sidecar is outside allowed root".to_string());
            }
            let bytes = fs::read(&canonical).map_err(|error| error.to_string())?;
            let mime = mime_type_for_sidecar(&canonical);
            *uri_value =
                serde_json::Value::String(format!("data:{mime};base64,{}", STANDARD.encode(bytes)));
        }
    }

    if let Some(images) = gltf
        .get_mut("images")
        .and_then(|value| value.as_array_mut())
    {
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
            let canonical = source_path
                .canonicalize()
                .map_err(|error| error.to_string())?;
            if !canonical.starts_with(home) {
                return Err("gltf sidecar is outside allowed root".to_string());
            }
            let bytes = fs::read(&canonical).map_err(|error| error.to_string())?;
            let mime = mime_type_for_sidecar(&canonical);
            *uri_value =
                serde_json::Value::String(format!("data:{mime};base64,{}", STANDARD.encode(bytes)));
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

fn markdown_image_data_url(markdown_path: &Path, destination: &str) -> Option<String> {
    let raw = destination.trim();
    if raw.is_empty() || raw.starts_with('#') {
        return None;
    }
    if raw.starts_with("//") {
        return None;
    }
    if raw
        .chars()
        .position(|ch| ch == ':')
        .map(|index| {
            let prefix = &raw[..index];
            !prefix.is_empty()
                && prefix
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
        })
        .unwrap_or(false)
    {
        return None;
    }

    let path_part = raw
        .split_once('?')
        .map(|(value, _)| value)
        .unwrap_or(raw)
        .split_once('#')
        .map(|(value, _)| value)
        .unwrap_or(raw);

    let source_path = {
        let candidate = Path::new(path_part);
        if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            markdown_path
                .parent()
                .map(|parent| parent.join(candidate))
                .unwrap_or_else(|| candidate.to_path_buf())
        }
    };

    let ext = source_path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    let mime = image_mime_from_ext(&ext)?;
    let bytes = fs::read(&source_path).ok()?;
    Some(format!("data:{mime};base64,{}", STANDARD.encode(bytes)))
}

fn render_markdown_preview_html(markdown: &str, markdown_path: &Path) -> String {
    let options = Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TABLES
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_FOOTNOTES;
    let parser = Parser::new_ext(markdown, options).map(|event| match event {
        Event::Start(Tag::Image {
            link_type,
            dest_url,
            title,
            id,
        }) => {
            let next_dest: CowStr<'_> = markdown_image_data_url(markdown_path, dest_url.as_ref())
                .map(CowStr::from)
                .unwrap_or(dest_url);
            Event::Start(Tag::Image {
                link_type,
                dest_url: next_dest,
                title,
                id,
            })
        }
        _ => event,
    });
    let mut html_out = String::new();
    html::push_html(&mut html_out, parser);
    html_out
}

fn is_probably_plain_text(path: &Path) -> bool {
    const SAMPLE_BYTES: usize = 8192;
    let Ok(file) = fs::File::open(path) else {
        return false;
    };
    let mut buffer = Vec::new();
    let mut limited = file.take(SAMPLE_BYTES as u64);
    if limited.read_to_end(&mut buffer).is_err() {
        return false;
    }
    if buffer.is_empty() {
        return true;
    }
    is_probably_plain_text_bytes(&buffer)
}

fn is_probably_plain_text_bytes(buffer: &[u8]) -> bool {
    if buffer.is_empty() {
        return true;
    }
    if buffer.contains(&0) {
        return false;
    }
    let control_bytes = buffer
        .iter()
        .filter(|&&byte| (byte < 0x20 && byte != b'\n' && byte != b'\r' && byte != b'\t') || byte == 0x7f)
        .count();
    // Allow a small amount of control bytes for odd files, but reject mostly-binary content.
    (control_bytes as f64) / (buffer.len() as f64) <= 0.01
}

fn list_zip_entries(path: &Path) -> Result<Vec<ZipEntryItem>, String> {
    const MAX_ZIP_ENTRIES: usize = 5000;
    let file = fs::File::open(path).map_err(|error| error.to_string())?;
    let mut archive = ZipArchive::new(file).map_err(|error| error.to_string())?;
    let mut entries = Vec::new();

    for index in 0..archive.len().min(MAX_ZIP_ENTRIES) {
        let entry = archive.by_index(index).map_err(|error| error.to_string())?;
        let raw_name = entry.name().trim_end_matches('/').to_string();
        if raw_name.is_empty() {
            continue;
        }
        let is_dir = entry.is_dir();
        let base_name = Path::new(&raw_name)
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or(raw_name.as_str());
        let icon = if is_dir {
            "folder"
        } else {
            icon_for_entry(base_name, false)
        };
        entries.push(ZipEntryItem {
            name: raw_name,
            is_dir,
            size: entry.size(),
            icon,
        });
    }

    entries.sort_by(|a, b| {
        b.is_dir
            .cmp(&a.is_dir)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
    });

    Ok(entries)
}

fn load_zip_entry_preview(path: &Path, entry_name: &str) -> Result<ZipEntryPreviewData, String> {
    let file = fs::File::open(path).map_err(|error| error.to_string())?;
    let mut archive = ZipArchive::new(file).map_err(|error| error.to_string())?;
    let mut entry = archive
        .by_name(entry_name)
        .map_err(|error| error.to_string())?;

    let title = Path::new(entry_name)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(entry_name)
        .to_string();
    let subtitle = entry_name.to_string();

    if entry.is_dir() {
        return Ok(ZipEntryPreviewData {
            kind: "unknown".to_string(),
            title,
            subtitle,
            image_data_url: None,
            text_head: None,
            note: Some("Directory entry.".to_string()),
        });
    }

    let ext = Path::new(entry_name)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    let entry_size = entry.size();

    if let Some(mime) = image_mime_from_ext(&ext) {
        const MAX_ZIP_IMAGE_PREVIEW_BYTES: u64 = 64 * 1024 * 1024;
        if entry_size > MAX_ZIP_IMAGE_PREVIEW_BYTES {
            return Ok(ZipEntryPreviewData {
                kind: "unknown".to_string(),
                title,
                subtitle,
                image_data_url: None,
                text_head: None,
                note: Some("Image entry is too large for inline preview (max 64MB).".to_string()),
            });
        }
        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        return Ok(ZipEntryPreviewData {
            kind: "image".to_string(),
            title,
            subtitle,
            image_data_url: Some(format!("data:{mime};base64,{}", STANDARD.encode(bytes))),
            text_head: None,
            note: None,
        });
    }

    let allow_text = is_previewable_text_ext(&ext) || ext.is_empty();
    if allow_text {
        const MAX_ZIP_TEXT_PREVIEW_BYTES: usize = 8 * 1024 * 1024;
        let mut bytes = Vec::new();
        let mut limited = entry.take(MAX_ZIP_TEXT_PREVIEW_BYTES as u64);
        limited
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        if is_previewable_text_ext(&ext) || is_probably_plain_text_bytes(&bytes) {
            return Ok(ZipEntryPreviewData {
                kind: "text".to_string(),
                title,
                subtitle,
                image_data_url: None,
                text_head: Some(String::from_utf8_lossy(&bytes).to_string()),
                note: if entry_size > MAX_ZIP_TEXT_PREVIEW_BYTES as u64 {
                    Some("Showing the first 8MB.".to_string())
                } else {
                    None
                },
            });
        }
    }

    Ok(ZipEntryPreviewData {
        kind: "unknown".to_string(),
        title,
        subtitle,
        image_data_url: None,
        text_head: None,
        note: Some("No inline preview for this ZIP entry type yet.".to_string()),
    })
}

fn should_show_default_open_for_file(path: &Path) -> bool {
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    if ext == "godot" {
        return true;
    }
    if ext == "html" || ext == "htm" {
        return true;
    }
    !is_previewable_text_ext(&ext)
}

fn is_git_repository_dir(path: &Path) -> bool {
    if !path.is_dir() {
        return false;
    }
    path.join(".git").exists()
}

fn find_git_repository_ancestor(path: &Path) -> Option<PathBuf> {
    let mut current = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent()?.to_path_buf()
    };
    loop {
        if is_git_repository_dir(&current) {
            return Some(current);
        }
        if !current.pop() {
            break;
        }
    }
    None
}

fn resolve_github_desktop_repo_for_path(path: &Path, tab_root: &Path) -> Option<PathBuf> {
    if !path.is_dir() {
        return None;
    }
    if let Some(repo) = find_git_repository_ancestor(path) {
        return Some(repo);
    }
    if path.starts_with(tab_root) && is_git_repository_dir(tab_root) {
        return Some(tab_root.to_path_buf());
    }
    None
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

    if ext == "blend" {
        let pngs = extract_blend_preview_pngs(path);
        if !pngs.is_empty() {
            let mut data_urls: Vec<String> = pngs
                .into_iter()
                .map(|png| format!("data:image/png;base64,{}", STANDARD.encode(png)))
                .collect();
            let first = data_urls.first().cloned();
            return Some(PreviewModel {
                kind: "blend".to_string(),
                title: file_name,
                subtitle,
                path: path.to_string_lossy().to_string(),
                icon,
                image_data_url: first,
                affinity_image_data_urls: Some(std::mem::take(&mut data_urls)),
                pdf_path: None,
                glb_path: None,
                video_path: None,
                zip_entries: None,
                text_head: None,
                note: Some("Embedded Blender thumbnails.".to_string()),
            });
        }
        return Some(PreviewModel {
            kind: "unknown".to_string(),
            title: file_name,
            subtitle,
            path: path.to_string_lossy().to_string(),
            icon,
            image_data_url: None,
            affinity_image_data_urls: None,
            pdf_path: None,
            glb_path: None,
            video_path: None,
            zip_entries: None,
            text_head: None,
            note: Some("No embedded Blender thumbnail found.".to_string()),
        });
    }

    if is_affinity_ext(&ext) {
        if let Some(pngs) = extract_affinity_embedded_pngs(path) {
            let mut data_urls: Vec<String> = pngs
                .into_iter()
                .map(|png| format!("data:image/png;base64,{}", STANDARD.encode(png)))
                .collect();
            let first = data_urls.first().cloned();
            return Some(PreviewModel {
                kind: "affinity".to_string(),
                title: file_name,
                subtitle,
                path: path.to_string_lossy().to_string(),
                icon,
                image_data_url: first,
                affinity_image_data_urls: Some(std::mem::take(&mut data_urls)),
                pdf_path: None,
                glb_path: None,
                video_path: None,
                zip_entries: None,
                text_head: None,
                note: Some("Embedded Affinity thumbnails.".to_string()),
            });
        }
        return Some(PreviewModel {
            kind: "unknown".to_string(),
            title: file_name,
            subtitle,
            path: path.to_string_lossy().to_string(),
            icon,
            image_data_url: None,
            affinity_image_data_urls: None,
            pdf_path: None,
            glb_path: None,
            video_path: None,
            zip_entries: None,
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
                affinity_image_data_urls: None,
                pdf_path: None,
                glb_path: None,
                video_path: None,
                zip_entries: None,
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
            affinity_image_data_urls: None,
            pdf_path: None,
            glb_path: None,
            video_path: None,
            zip_entries: None,
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
            affinity_image_data_urls: None,
            pdf_path: Some(path.to_string_lossy().to_string()),
            glb_path: None,
            video_path: None,
            zip_entries: None,
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
            affinity_image_data_urls: None,
            pdf_path: None,
            glb_path: Some(path.to_string_lossy().to_string()),
            video_path: None,
            zip_entries: None,
            text_head: None,
            note: None,
        });
    }

    if video_mime_from_ext(&ext).is_some() {
        return Some(PreviewModel {
            kind: "video".to_string(),
            title: file_name,
            subtitle,
            path: path.to_string_lossy().to_string(),
            icon,
            image_data_url: None,
            affinity_image_data_urls: None,
            pdf_path: None,
            glb_path: None,
            video_path: Some(path.to_string_lossy().to_string()),
            zip_entries: None,
            text_head: None,
            note: None,
        });
    }

    if ext == "zip" {
        let entries = list_zip_entries(path).ok();
        let empty = entries.as_ref().map(|value| value.is_empty()).unwrap_or(true);
        return Some(PreviewModel {
            kind: "zip".to_string(),
            title: file_name,
            subtitle,
            path: path.to_string_lossy().to_string(),
            icon,
            image_data_url: None,
            affinity_image_data_urls: None,
            pdf_path: None,
            glb_path: None,
            video_path: None,
            zip_entries: entries,
            text_head: None,
            note: if empty {
                Some("ZIP archive is empty.".to_string())
            } else {
                None
            },
        });
    }

    let preview_as_text = is_previewable_text_ext(&ext) || (ext.is_empty() && is_probably_plain_text(path));
    if preview_as_text {
        const MAX_TEXT_PREVIEW_BYTES: usize = 8 * 1024 * 1024;
        let file = fs::File::open(path).ok()?;
        let file_size = fs::metadata(path).ok().map(|meta| meta.len()).unwrap_or(0);
        let mut buffer = Vec::new();
        let mut limited = file.take(MAX_TEXT_PREVIEW_BYTES as u64);
        limited.read_to_end(&mut buffer).ok()?;
        let text = String::from_utf8_lossy(&buffer).to_string();
        let is_markdown = ext == "md";
        let content = if is_markdown {
            render_markdown_preview_html(&text, path)
        } else {
            text
        };
        return Some(PreviewModel {
            kind: if is_markdown {
                "markdown".to_string()
            } else {
                "text".to_string()
            },
            title: file_name,
            subtitle,
            path: path.to_string_lossy().to_string(),
            icon,
            image_data_url: None,
            affinity_image_data_urls: None,
            pdf_path: None,
            glb_path: None,
            video_path: None,
            zip_entries: None,
            text_head: Some(content),
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
        affinity_image_data_urls: None,
        pdf_path: None,
        glb_path: None,
        video_path: None,
        zip_entries: None,
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
    fs::metadata(path)
        .map(|meta| meta.is_dir())
        .unwrap_or(false)
}

fn has_hidden_component_between(root: &Path, target: &Path) -> bool {
    target
        .strip_prefix(root)
        .ok()
        .map(|relative| {
            relative.components().any(|component| {
                component
                    .as_os_str()
                    .to_str()
                    .map(|value| value.starts_with('.') && value != "." && value != "..")
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn build_columns(
    home: &Path,
    focus_path: Option<&Path>,
    directory_sorts: &BTreeMap<String, DirectorySortMode>,
    show_hidden: bool,
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
        let entries = list_directory(&parent_dir, sort_mode, show_hidden);
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

fn resolve_location_input_path(home: &Path, path: &str) -> Result<PathBuf, String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err("path cannot be empty".to_string());
    }

    let candidate = if trimmed == "~" {
        home.to_path_buf()
    } else if let Some(rest) = trimmed.strip_prefix("~/") {
        home.join(rest)
    } else {
        let raw = PathBuf::from(trimmed);
        if raw.is_absolute() {
            raw
        } else {
            home.join(raw)
        }
    };

    if !candidate.is_absolute() {
        return Err("path must resolve to an absolute path".to_string());
    }
    if !candidate.exists() {
        return Err("Path does not exist.".to_string());
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

fn set_active_focus_after_path_change(
    model: &mut TabsModel,
    home: &Path,
    preferred: Option<&Path>,
    source: &Path,
) {
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

    if raw.is_absolute() {
        raw
    } else {
        home.to_path_buf()
    }
}

fn active_tab_root_path(tab: &TabState, home: &Path) -> PathBuf {
    tab.root_path
        .as_deref()
        .map(PathBuf::from)
        .filter(|path| path.is_absolute() && path.is_dir())
        .unwrap_or_else(|| home.to_path_buf())
}

fn root_display_name(root: &Path, disambiguate: bool) -> String {
    let base = label_from_path(root);
    if !disambiguate {
        return base;
    }
    if let Some(parent) = root.parent() {
        let parent_name = label_from_path(parent);
        if !parent_name.is_empty() && parent_name != base {
            return format!("{parent_name}/{base}");
        }
    }
    base
}

fn tab_title(tab: &TabState, home: &Path, disambiguate_root: bool) -> String {
    let root = active_tab_root_path(tab, home);
    let root_name = root_display_name(&root, disambiguate_root);
    let focus = tab
        .focus_path
        .as_deref()
        .map(PathBuf::from)
        .filter(|path| path.starts_with(&root) && path != &root);

    if let Some(focus_path) = focus {
        format!("{root_name} - {}", label_from_path(&focus_path))
    } else {
        root_name
    }
}

fn relative_path_from_root(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .ok()
        .map(|relative| {
            let value = relative.to_string_lossy().to_string();
            if value.is_empty() {
                ".".to_string()
            } else {
                value
            }
        })
        .unwrap_or_else(|| path.to_string_lossy().to_string())
}

fn fuzzy_score(haystack: &str, needle: &str) -> Option<i64> {
    if needle.is_empty() {
        return Some(0);
    }
    let mut score = 0i64;
    let mut last_index: Option<usize> = None;
    let mut search_from = 0usize;
    let hay_chars: Vec<char> = haystack.chars().collect();
    let needle_chars: Vec<char> = needle.chars().collect();

    for &needle_char in &needle_chars {
        let mut found = None;
        for index in search_from..hay_chars.len() {
            if hay_chars[index] == needle_char {
                found = Some(index);
                break;
            }
        }
        let index = found?;
        score += 8;
        if let Some(last) = last_index {
            if index == last + 1 {
                score += 14;
            } else {
                let gap = (index - last - 1) as i64;
                score -= gap.min(12);
            }
        } else if index == 0
            || matches!(
                hay_chars.get(index.saturating_sub(1)),
                Some('/' | '_' | '-' | ' ' | '.')
            )
        {
            score += 18;
        }
        last_index = Some(index);
        search_from = index + 1;
    }

    Some(score - (hay_chars.len() as i64 / 3))
}

fn push_ranked_result(
    ranked: &mut Vec<(i64, SearchResultItem)>,
    score: i64,
    item: SearchResultItem,
    max_results: usize,
) {
    ranked.push((score, item));
    if ranked.len() > max_results * 4 {
        ranked.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| a.1.relative_path.cmp(&b.1.relative_path))
        });
        ranked.truncate(max_results);
    }
}

fn sorted_ranked_results(
    ranked: &[(i64, SearchResultItem)],
    max_results: usize,
) -> Vec<SearchResultItem> {
    let mut items = ranked.to_vec();
    items.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| a.1.relative_path.cmp(&b.1.relative_path))
    });
    items.truncate(max_results);
    items.into_iter().map(|(_, item)| item).collect()
}

fn run_fuzzy_search_worker(
    shared: Arc<Mutex<SearchModel>>,
    generation: u64,
    root: PathBuf,
    query: String,
    show_hidden: bool,
) {
    const MAX_RESULTS: usize = 200;
    const FLUSH_EVERY: u64 = 40;
    let query_lower = query.to_ascii_lowercase();
    let mut stack = vec![root.clone()];
    let mut scanned = 0u64;
    let mut ranked: Vec<(i64, SearchResultItem)> = Vec::new();

    while let Some(directory) = stack.pop() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if !show_hidden && name.starts_with('.') {
                continue;
            }
            scanned += 1;
            let metadata = match entry.metadata() {
                Ok(value) => value,
                Err(_) => continue,
            };
            let is_dir = metadata.is_dir();
            if is_dir {
                stack.push(path.clone());
            }
            let relative = path
                .strip_prefix(&root)
                .ok()
                .map(|value| value.to_string_lossy().to_string())
                .unwrap_or_else(|| path.to_string_lossy().to_string());
            let relative_lower = relative.to_ascii_lowercase();
            let name_lower = name.to_ascii_lowercase();
            let name_score = fuzzy_score(&name_lower, &query_lower);
            let path_score = fuzzy_score(&relative_lower, &query_lower);
            let combined = match (name_score, path_score) {
                (Some(a), Some(b)) => Some(a.max(b + 10)),
                (Some(a), None) => Some(a + 8),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            if let Some(score) = combined {
                let icon = icon_for_entry(&name, is_dir);
                push_ranked_result(
                    &mut ranked,
                    score,
                    SearchResultItem {
                        path: path.to_string_lossy().to_string(),
                        relative_path: relative,
                        name,
                        icon,
                    },
                    MAX_RESULTS,
                );
            }

            if scanned % FLUSH_EVERY == 0 {
                let Ok(mut model) = shared.lock() else {
                    return;
                };
                if model.generation != generation {
                    return;
                }
                model.scanned = scanned;
                model.results = sorted_ranked_results(&ranked, MAX_RESULTS);
            }
        }
    }

    let Ok(mut model) = shared.lock() else {
        return;
    };
    if model.generation != generation {
        return;
    }
    model.scanned = scanned;
    model.running = false;
    model.results = sorted_ranked_results(&ranked, MAX_RESULTS);
}

fn ensure_tabs(model: &mut TabsModel, home: &Path) {
    model.directory_sorts.retain(|path, mode| {
        PathBuf::from(path).is_absolute() && *mode != DirectorySortMode::Alphabetical
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
            show_hidden: false,
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
    let active_show_hidden = active_tab.map(|tab| tab.show_hidden).unwrap_or(false);
    let columns = build_columns(
        &active_root_path,
        active_focus_path.as_deref(),
        &model.directory_sorts,
        active_show_hidden,
    );
    let preview = build_preview(active_focus_path.as_deref());
    let active_path_for_new = active_focus_path
        .as_ref()
        .map(|path| path.to_string_lossy().to_string())
        .unwrap_or_else(|| active_root_path.to_string_lossy().to_string());
    let mut root_basename_counts: HashMap<String, usize> = HashMap::new();
    for tab in &model.tabs {
        let root = active_tab_root_path(tab, &home);
        let key = label_from_path(&root);
        *root_basename_counts.entry(key).or_insert(0) += 1;
    }
    let tabs: Vec<TabView> = model
        .tabs
        .iter()
        .map(|tab| {
            let root = active_tab_root_path(tab, &home);
            let basename = label_from_path(&root);
            let disambiguate_root = root_basename_counts.get(&basename).copied().unwrap_or(0) > 1;
            TabView {
                id: tab.id.to_string(),
                title: tab_title(tab, &home, disambiguate_root),
                is_active: tab.id == model.active_id,
            }
        })
        .collect();

    context.insert("root_name", &label_from_path(&active_root_path));
    context.insert("root_path", &active_root_path.to_string_lossy().to_string());
    context.insert("show_hidden", &active_show_hidden);
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
    pub paths: Vec<String>,
    pub relative_paths: Vec<String>,
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
pub fn validate_location_path(path: String) -> Result<String, String> {
    let home = home_directory();
    let target = resolve_location_input_path(&home, &path)?;
    Ok(target.to_string_lossy().to_string())
}

#[tauri::command]
pub fn go_to_location(state: State<'_, FileTabsState>, path: String) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let target = resolve_location_input_path(&home, &path)?;

    if let Some(tab) = active_tab_mut(&mut model) {
        let current_root = active_tab_root_path(tab, &home);
        if !target.starts_with(&current_root) {
            tab.root_path = Some("/".to_string());
        }
        let effective_root = active_tab_root_path(tab, &home);
        if has_hidden_component_between(&effective_root, &target) {
            tab.show_hidden = true;
        }
        if target.starts_with(&effective_root) {
            tab.focus_path = Some(target.to_string_lossy().to_string());
        }
    }

    persist_tabs_model(&model);
    Ok(render_view(&model))
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
        show_hidden: false,
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
pub fn reorder_tabs(state: State<'_, FileTabsState>, tab_ids: String) -> String {
    let home = home_directory();
    let mut model = match state.tabs.lock() {
        Ok(model) => model,
        Err(_) => return String::new(),
    };
    ensure_tabs(&mut model, &home);

    let requested: Vec<u64> = tab_ids
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter_map(|value| value.parse::<u64>().ok())
        .collect();
    if requested.is_empty() {
        return render_view(&model);
    }

    let mut remaining = std::mem::take(&mut model.tabs);
    let mut reordered: Vec<TabState> = Vec::with_capacity(remaining.len());

    for id in requested {
        if let Some(index) = remaining.iter().position(|tab| tab.id == id) {
            reordered.push(remaining.remove(index));
        }
    }
    reordered.extend(remaining);
    model.tabs = reordered;

    if !model.tabs.iter().any(|tab| tab.id == model.active_id) {
        model.active_id = model.tabs.first().map(|tab| tab.id).unwrap_or(1);
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
    selection_count: usize,
    selected_paths: Option<Vec<String>>,
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
    let is_multi_selection = selection_count > 1;
    let show_default_open = should_show_default_open_for_file(&path_buf);
    let show_github_desktop =
        is_dir && resolve_github_desktop_repo_for_path(&path_buf, &root).is_some();

    let mut paths = if is_multi_selection {
        selected_paths.unwrap_or_default()
    } else {
        vec![path.clone()]
    };
    paths.retain(|value| !value.trim().is_empty());
    if paths.is_empty() {
        paths.push(path.clone());
    }
    if !paths.iter().any(|value| value == &path) {
        paths.insert(0, path.clone());
    }
    let relative_paths = paths
        .iter()
        .map(|value| relative_path_from_root(&PathBuf::from(value), &root))
        .collect::<Vec<_>>();

    let mut pending = state
        .pending
        .lock()
        .map_err(|_| "failed to lock context menu state".to_string())?;
    *pending = Some(PendingContextTarget {
        paths,
        relative_paths,
    });
    drop(pending);

    let mut builder = MenuBuilder::new(&window);
    if is_multi_selection {
        let menu = builder
            .text("copy_absolute_path", "Copy absolute path")
            .text("copy_relative_path", "Copy relative path")
            .separator()
            .text("ctx_trash", "Trash")
            .text("ctx_delete", "Delete")
            .build()
            .map_err(|error| error.to_string())?;
        return window
            .popup_menu_at(&menu, LogicalPosition::new(x, y))
            .map_err(|error| error.to_string());
    }
    if is_dir {
        builder = builder
            .text("ctx_new_dir", "Create directory")
            .text("ctx_new_file", "Create file")
            .separator()
            .text("ctx_set_tab_root", "Set as current tab root")
            .separator()
            .text("ctx_open_finder", "Open in Finder")
            .text("ctx_open_warp", "Open in Warp")
            .text("ctx_open_zed", "Open in Zed");
        if show_github_desktop {
            builder = builder.text("ctx_open_github_desktop", "Open in GitHub Desktop");
        }
        builder = builder.separator();
    } else {
        if show_default_open {
            builder = builder.text("ctx_open_default", "Open");
        }
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

fn parse_paths_json_arg(paths_json: &str) -> Result<Vec<String>, String> {
    let parsed: Vec<String> =
        serde_json::from_str(paths_json).map_err(|_| "invalid paths payload".to_string())?;
    let mut unique = BTreeSet::new();
    for path in parsed {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            unique.insert(trimmed.to_string());
        }
    }
    Ok(unique.into_iter().collect())
}

fn directory_listing_signature(path: &Path, show_hidden: bool) -> u64 {
    let mut hasher = DefaultHasher::new();
    path.to_string_lossy().to_string().hash(&mut hasher);

    let mut rows: Vec<(String, bool, u128)> = Vec::new();
    match fs::read_dir(path) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if !show_hidden && name.starts_with('.') {
                    continue;
                }
                let metadata = match entry.metadata() {
                    Ok(metadata) => metadata,
                    Err(_) => continue,
                };
                let modified_ms = metadata
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                    .map(|duration| duration.as_millis())
                    .unwrap_or(0);
                rows.push((name, metadata.is_dir(), modified_ms));
            }
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            rows.len().hash(&mut hasher);
            for (name, is_dir, modified_ms) in rows {
                name.hash(&mut hasher);
                is_dir.hash(&mut hasher);
                modified_ms.hash(&mut hasher);
            }
        }
        Err(error) => {
            "read_dir_error".hash(&mut hasher);
            format!("{error}").hash(&mut hasher);
        }
    }

    hasher.finish()
}

#[tauri::command]
pub fn visible_directories_signature(
    tabs_state: State<'_, FileTabsState>,
    paths_json: String,
) -> Result<String, String> {
    let home = home_directory();
    let mut model = tabs_state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);
    let show_hidden = model
        .tabs
        .iter()
        .find(|tab| tab.id == model.active_id)
        .or_else(|| model.tabs.first())
        .map(|tab| tab.show_hidden)
        .unwrap_or(false);
    drop(model);

    let paths = parse_paths_json_arg(&paths_json)?;
    let mut hasher = DefaultHasher::new();
    for path in paths {
        let path_buf = PathBuf::from(&path);
        if !path_buf.is_absolute() || !path_buf.is_dir() {
            continue;
        }
        directory_listing_signature(&path_buf, show_hidden).hash(&mut hasher);
    }
    Ok(format!("{:016x}", hasher.finish()))
}

#[tauri::command]
pub fn trash_paths(state: State<'_, FileTabsState>, paths_json: String) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let paths = parse_paths_json_arg(&paths_json)?;
    for path in &paths {
        let source = resolve_home_scoped_path(&home, path)?;
        if source == home {
            return Err("cannot trash root directory".to_string());
        }
        if !source.exists() {
            return Err("path does not exist".to_string());
        }
        move_to_trash(&source)?;
        set_active_focus_after_path_change(&mut model, &home, None, &source);
    }
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
pub fn delete_paths(state: State<'_, FileTabsState>, paths_json: String) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let paths = parse_paths_json_arg(&paths_json)?;
    for path in &paths {
        let source = resolve_home_scoped_path(&home, path)?;
        if source == home {
            return Err("cannot delete root directory".to_string());
        }
        if !source.exists() {
            return Err("path does not exist".to_string());
        }
        remove_permanently(&source)?;
        set_active_focus_after_path_change(&mut model, &home, None, &source);
    }
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
    let clean_name = validate_rename_name(&name)?;
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

#[tauri::command]
pub fn set_tab_show_hidden(
    state: State<'_, FileTabsState>,
    show_hidden: String,
) -> Result<String, String> {
    let home = home_directory();
    let mut model = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);

    let parsed = match show_hidden.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => true,
        "false" | "0" | "no" | "off" => false,
        _ => return Err("invalid show_hidden value".to_string()),
    };

    if let Some(tab) = active_tab_mut(&mut model) {
        tab.show_hidden = parsed;
    }
    persist_tabs_model(&model);
    Ok(render_view(&model))
}

#[tauri::command]
pub fn fuzzy_search_start(
    tabs_state: State<'_, FileTabsState>,
    search_state: State<'_, FileSearchState>,
    query: String,
) -> Result<(), String> {
    let home = home_directory();
    let mut tabs = tabs_state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut tabs, &home);
    let active_tab = tabs
        .tabs
        .iter()
        .find(|tab| tab.id == tabs.active_id)
        .or_else(|| tabs.tabs.first())
        .ok_or_else(|| "missing active tab".to_string())?;
    let root = active_tab_root_path(active_tab, &home);
    let show_hidden = active_tab.show_hidden;
    drop(tabs);

    let trimmed = query.trim().to_string();
    let shared = search_state.search.clone();
    let generation = {
        let mut search = shared
            .lock()
            .map_err(|_| "failed to lock search state".to_string())?;
        search.generation = search.generation.saturating_add(1);
        search.query = trimmed.clone();
        search.root_path = root.to_string_lossy().to_string();
        search.running = !trimmed.is_empty();
        search.scanned = 0;
        search.results.clear();
        search.generation
    };

    if trimmed.is_empty() {
        return Ok(());
    }

    std::thread::spawn(move || {
        run_fuzzy_search_worker(shared, generation, root, trimmed, show_hidden);
    });
    Ok(())
}

#[tauri::command]
pub fn fuzzy_search_cancel(search_state: State<'_, FileSearchState>) -> Result<(), String> {
    let mut search = search_state
        .search
        .lock()
        .map_err(|_| "failed to lock search state".to_string())?;
    search.generation = search.generation.saturating_add(1);
    search.query.clear();
    search.running = false;
    search.scanned = 0;
    search.results.clear();
    Ok(())
}

#[tauri::command]
pub fn fuzzy_search_results(search_state: State<'_, FileSearchState>) -> String {
    let (query, running, scanned, results) = {
        let Ok(search) = search_state.search.lock() else {
            return String::new();
        };
        (
            search.query.clone(),
            search.running,
            search.scanned,
            search.results.clone(),
        )
    };

    let mut context = Context::new();
    context.insert("query", &query);
    context.insert("running", &running);
    context.insert("scanned", &scanned);
    context.insert("results", &results);
    render!("files/_search_results", &context)
}

#[tauri::command]
pub fn fuzzy_search_preview(path: String) -> String {
    let home = home_directory();
    let resolved = match resolve_home_scoped_path(&home, &path) {
        Ok(path) => path,
        Err(_) => {
            let mut context = Context::new();
            context.insert("preview", &Option::<PreviewModel>::None);
            return render!("files/_search_preview", &context);
        }
    };
    let preview = build_preview(Some(&resolved));
    let mut context = Context::new();
    context.insert("preview", &preview);
    render!("files/_search_preview", &context)
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
pub fn open_in_default(path: String) -> Result<(), String> {
    let home = home_directory();
    let target = resolve_home_scoped_path(&home, &path)?;
    let status = Command::new("open")
        .arg(&target)
        .status()
        .map_err(|error| error.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err("failed to open path".to_string())
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
pub async fn load_video_preview_data(path: String) -> Result<ModelPreviewData, String> {
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
    let Some(mime_type) = video_mime_from_ext(&ext) else {
        return Err("path is not a supported video".to_string());
    };

    const MAX_VIDEO_PREVIEW_BYTES: u64 = 100 * 1024 * 1024;
    let metadata = fs::metadata(&target).map_err(|error| error.to_string())?;
    if metadata.len() > MAX_VIDEO_PREVIEW_BYTES {
        return Err("Video is too large for inline preview.".to_string());
    }

    let bytes = tokio::fs::read(&target)
        .await
        .map_err(|error| error.to_string())?;
    Ok(ModelPreviewData {
        mime_type: mime_type.to_string(),
        base64: STANDARD.encode(bytes),
    })
}

#[tauri::command]
pub async fn load_zip_entry_preview_data(
    path: String,
    entry_name: String,
) -> Result<ZipEntryPreviewData, String> {
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
    if ext != "zip" {
        return Err("path is not a zip".to_string());
    }
    if entry_name.trim().is_empty() {
        return Err("entry name is empty".to_string());
    }

    load_zip_entry_preview(&target, &entry_name)
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
pub fn open_in_finder(path: String) -> Result<(), String> {
    let home = home_directory();
    let target = resolve_home_scoped_path(&home, &path)?;
    if !target.is_dir() {
        return Err("finder can only open directories".to_string());
    }
    open_path_with_application("Finder", &target)
}

#[tauri::command]
pub fn open_in_github_desktop(
    path: String,
    tabs_state: State<'_, FileTabsState>,
) -> Result<(), String> {
    let tab_root = active_tab_root_for_state(&tabs_state)?;
    open_in_github_desktop_with_tab_root(path, tab_root)
}

pub fn open_in_github_desktop_from_app(app: &AppHandle, path: String) -> Result<(), String> {
    let tabs_state = app.state::<FileTabsState>();
    let tab_root = active_tab_root_for_state(&tabs_state)?;
    open_in_github_desktop_with_tab_root(path, tab_root)
}

fn active_tab_root_for_state(tabs_state: &State<'_, FileTabsState>) -> Result<PathBuf, String> {
    let home = home_directory();
    let mut model = tabs_state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);
    Ok(model
        .tabs
        .iter()
        .find(|tab| tab.id == model.active_id)
        .or_else(|| model.tabs.first())
        .map(|tab| active_tab_root_path(tab, &home))
        .unwrap_or_else(|| home.clone()))
}

fn open_in_github_desktop_with_tab_root(path: String, tab_root: PathBuf) -> Result<(), String> {
    let home = home_directory();
    let target = resolve_home_scoped_path(&home, &path)?;
    if !target.is_dir() {
        return Err("github desktop can only open directories".to_string());
    }
    let repo = resolve_github_desktop_repo_for_path(&target, &tab_root)
        .ok_or_else(|| "no git repository found for directory".to_string())?;
    open_path_with_application("GitHub Desktop", &repo)
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

#[tauri::command]
pub fn path_context_capabilities(
    path: String,
    tabs_state: State<'_, FileTabsState>,
) -> Result<PathContextCapabilities, String> {
    let home = home_directory();
    let mut model = tabs_state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    ensure_tabs(&mut model, &home);
    let tab_root = model
        .tabs
        .iter()
        .find(|tab| tab.id == model.active_id)
        .or_else(|| model.tabs.first())
        .map(|tab| active_tab_root_path(tab, &home))
        .unwrap_or_else(|| home.clone());
    drop(model);

    let target = resolve_home_scoped_path(&home, &path)?;
    let is_dir = target.is_dir();
    Ok(PathContextCapabilities {
        is_dir,
        show_default_open: !is_dir && should_show_default_open_for_file(&target),
        show_github_desktop: is_dir
            && resolve_github_desktop_repo_for_path(&target, &tab_root).is_some(),
    })
}
