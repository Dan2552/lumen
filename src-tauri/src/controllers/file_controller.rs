use base64::{engine::general_purpose::STANDARD, Engine};
use flate2::read::GzDecoder;
#[cfg(target_os = "macos")]
use objc2::{msg_send, runtime::AnyObject};
#[cfg(target_os = "macos")]
use objc2_app_kit::{NSWindow, NSWindowButton};
#[cfg(target_os = "macos")]
use objc2_foundation::{NSRect, NSString};
use pulldown_cmark::{html, CowStr, Event, Options, Parser, Tag};
use serde::Serialize;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet, HashMap, VecDeque},
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
    menu::MenuBuilder, AppHandle, Emitter, LogicalPosition, Manager, Runtime, State, WebviewUrl,
    WebviewWindowBuilder, Window,
};
use tera::Context;
use zip::ZipArchive;

#[path = "file_controller/context/mod.rs"]
mod context;

#[derive(Debug, Serialize, Clone)]
struct FileEntry {
    name: String,
    path: String,
    is_dir: bool,
    icon: &'static str,
}

#[derive(Debug, Serialize, Clone)]
struct Column {
    title: String,
    path: String,
    sort_mode: String,
    entries: Vec<FileEntry>,
    selected_path: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct CompanionEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub icon: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct CompanionColumn {
    pub title: String,
    pub path: String,
    pub selected_path: String,
    pub entries: Vec<CompanionEntry>,
}

#[derive(Debug, Serialize, Clone)]
pub struct CompanionTab {
    pub id: String,
    pub title: String,
    pub is_active: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct CompanionSnapshot {
    pub window_label: String,
    pub home_path: String,
    pub root_path: String,
    pub active_path: String,
    pub active_tab_id: String,
    pub show_hidden: bool,
    pub tabs: Vec<CompanionTab>,
    pub columns: Vec<CompanionColumn>,
    pub revision: String,
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
    context_commands: Vec<context::ContextCommand>,
}

#[derive(Debug, Serialize)]
pub struct GoToDirectorySuggestion {
    name: String,
    value: String,
}

#[derive(Debug, Clone)]
struct SearchModel {
    generation: u64,
    query: String,
    root_path: String,
    running: bool,
    scanned: u64,
    results: Vec<SearchResultItem>,
}

pub struct FileSearchState {
    search: Arc<Mutex<HashMap<String, SearchModel>>>,
}

impl Default for FileSearchState {
    fn default() -> Self {
        Self {
            search: Arc::new(Mutex::new(HashMap::new())),
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TabsModel {
    #[serde(default)]
    tabs: Vec<TabState>,
    #[serde(default = "default_active_id")]
    active_id: u64,
    #[serde(default = "default_next_id")]
    next_id: u64,
    #[serde(default)]
    window: Option<WindowGeometry>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Default)]
struct TabsStore {
    #[serde(default)]
    windows: BTreeMap<String, TabsModel>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SettingsStore {
    #[serde(default)]
    directory_sorts: BTreeMap<String, DirectorySortMode>,
    #[serde(default = "default_theme_base_color")]
    theme_base_color: String,
    #[serde(default = "default_theme_highlight_color")]
    theme_highlight_color: String,
    #[serde(default = "default_theme_lightness_offset")]
    theme_lightness_offset: i32,
}

impl Default for SettingsStore {
    fn default() -> Self {
        Self {
            directory_sorts: BTreeMap::new(),
            theme_base_color: default_theme_base_color(),
            theme_highlight_color: default_theme_highlight_color(),
            theme_lightness_offset: default_theme_lightness_offset(),
        }
    }
}

#[derive(Debug, serde::Deserialize, Default)]
struct LegacyTabsModelSettings {
    #[serde(default)]
    directory_sorts: BTreeMap<String, DirectorySortMode>,
}

#[derive(Debug, serde::Deserialize, Default)]
struct LegacyTabsStoreSettings {
    #[serde(default)]
    windows: BTreeMap<String, LegacyTabsModelSettings>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WindowGeometry {
    x: i32,
    y: i32,
    width: u32,
    height: u32,
}

const MAC_TRAFFIC_LIGHT_X: f64 = 12.0;
const MAC_TRAFFIC_LIGHT_Y: f64 = 22.0;
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
struct MacLogicalPosition {
    x: f64,
    y: f64,
}

fn default_active_id() -> u64 {
    1
}

fn default_next_id() -> u64 {
    2
}

fn default_tabs_model() -> TabsModel {
    TabsModel {
        tabs: Vec::new(),
        active_id: default_active_id(),
        next_id: default_next_id(),
        window: None,
    }
}

fn default_search_model() -> SearchModel {
    SearchModel {
        generation: 0,
        query: String::new(),
        root_path: String::new(),
        running: false,
        scanned: 0,
        results: Vec::new(),
    }
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
    home: &Path,
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
            if !show_hidden && is_hidden_component(path, &name, home) {
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

fn rewrite_html_img_sources(fragment: &str, markdown_path: &Path) -> String {
    fn eq_ascii_ignore_case(a: u8, b: u8) -> bool {
        a.eq_ignore_ascii_case(&b)
    }

    fn find_img_src_value_range(tag: &str) -> Option<(usize, usize)> {
        let bytes = tag.as_bytes();
        if bytes.len() < 7 {
            return None;
        }
        let mut i = 0usize;
        while i + 3 <= bytes.len() {
            if eq_ascii_ignore_case(bytes[i], b's')
                && eq_ascii_ignore_case(bytes[i + 1], b'r')
                && eq_ascii_ignore_case(bytes[i + 2], b'c')
            {
                let prev_ok = i == 0
                    || bytes[i - 1].is_ascii_whitespace()
                    || bytes[i - 1] == b'<'
                    || bytes[i - 1] == b'/';
                if !prev_ok {
                    i += 1;
                    continue;
                }
                let mut j = i + 3;
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j >= bytes.len() || bytes[j] != b'=' {
                    i += 1;
                    continue;
                }
                j += 1;
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j >= bytes.len() {
                    return None;
                }
                let quote = bytes[j];
                if quote == b'"' || quote == b'\'' {
                    let value_start = j + 1;
                    let mut k = value_start;
                    while k < bytes.len() {
                        if bytes[k] == quote {
                            return Some((value_start, k));
                        }
                        k += 1;
                    }
                    return None;
                }
                let value_start = j;
                let mut k = value_start;
                while k < bytes.len() && !bytes[k].is_ascii_whitespace() && bytes[k] != b'>' {
                    k += 1;
                }
                return Some((value_start, k));
            }
            i += 1;
        }
        None
    }

    let mut output = String::with_capacity(fragment.len());
    let mut cursor = 0usize;

    while let Some(rel_img_start) = fragment[cursor..].to_ascii_lowercase().find("<img") {
        let img_start = cursor + rel_img_start;
        output.push_str(&fragment[cursor..img_start]);

        let Some(rel_img_end) = fragment[img_start..].find('>') else {
            output.push_str(&fragment[img_start..]);
            return output;
        };
        let img_end = img_start + rel_img_end + 1;
        let tag = &fragment[img_start..img_end];

        let mut rewritten = None::<String>;
        if let Some((src_value_start, src_value_end)) = find_img_src_value_range(tag) {
            let raw_src = &tag[src_value_start..src_value_end];
            if let Some(data_url) = markdown_image_data_url(markdown_path, raw_src) {
                let mut next_tag = String::with_capacity(tag.len() + data_url.len());
                next_tag.push_str(&tag[..src_value_start]);
                next_tag.push_str(&data_url);
                next_tag.push_str(&tag[src_value_end..]);
                rewritten = Some(next_tag);
            }
        }

        output.push_str(rewritten.as_deref().unwrap_or(tag));
        cursor = img_end;
    }

    output.push_str(&fragment[cursor..]);
    output
}

fn render_markdown_preview_html(markdown: &str, markdown_path: &Path) -> String {
    let options = Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TABLES
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_FOOTNOTES;
    let parser = Parser::new_ext(markdown, options).map(|event| match event {
        Event::Html(value) => Event::Html(CowStr::from(rewrite_html_img_sources(
            &value,
            markdown_path,
        ))),
        Event::InlineHtml(value) => Event::InlineHtml(CowStr::from(rewrite_html_img_sources(
            &value,
            markdown_path,
        ))),
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
    rewrite_html_img_sources(&html_out, markdown_path)
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
        .filter(|&&byte| {
            (byte < 0x20 && byte != b'\n' && byte != b'\r' && byte != b'\t') || byte == 0x7f
        })
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

fn is_macos_application_bundle(path: &Path) -> bool {
    if !cfg!(target_os = "macos") || !path.is_dir() {
        return false;
    }
    let is_app_dir = path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.eq_ignore_ascii_case("app"))
        .unwrap_or(false);
    if !is_app_dir {
        return false;
    }
    path.join("Contents").join("Info.plist").is_file()
}

fn should_show_default_open_for_path(path: &Path, is_dir: bool) -> bool {
    if is_macos_application_bundle(path) {
        return true;
    }
    if is_dir {
        return false;
    }
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
        let empty = entries
            .as_ref()
            .map(|value| value.is_empty())
            .unwrap_or(true);
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

    let preview_as_text =
        is_previewable_text_ext(&ext) || (ext.is_empty() && is_probably_plain_text(path));
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
    let home = home_directory();
    let Ok(relative) = target.strip_prefix(root) else {
        return false;
    };
    let mut parent = root.to_path_buf();
    for component in relative.components() {
        let Some(value) = component.as_os_str().to_str() else {
            continue;
        };
        if is_hidden_component(&parent, value, &home) {
            return true;
        }
        parent.push(component.as_os_str());
    }
    false
}

fn is_hidden_component(parent: &Path, value: &str, home: &Path) -> bool {
    if value == "." || value == ".." {
        return false;
    }
    if value.starts_with('.') {
        return true;
    }
    if value == "Library" && parent == home {
        return true;
    }
    let pictures = home.join("Pictures");
    if parent == pictures
        && (value == "Photo Booth Library" || value == "Photos Library.photoslibrary")
    {
        return true;
    }
    parent == home && value == "Music"
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
        let entries = list_directory(&parent_dir, sort_mode, show_hidden, home);
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

fn resolve_absolute_existing_path(path: &str) -> Result<PathBuf, String> {
    let candidate = PathBuf::from(path.trim());
    if !candidate.is_absolute() {
        return Err("path must be absolute".to_string());
    }
    if !candidate.exists() {
        return Err("path does not exist".to_string());
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

fn split_location_parent_and_segment(input: &str) -> (String, String) {
    if input.is_empty() {
        return (String::new(), String::new());
    }
    if input.ends_with('/') {
        return (input.trim_end_matches('/').to_string(), String::new());
    }
    if let Some(index) = input.rfind('/') {
        return (input[..index].to_string(), input[index + 1..].to_string());
    }
    (String::new(), input.to_string())
}

fn location_suggestion_context(home: &Path, input: &str) -> (PathBuf, String, String) {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed == "~" {
        return (home.to_path_buf(), "~/".to_string(), String::new());
    }

    if let Some(rest) = trimmed.strip_prefix("~/") {
        let (parent_rel, segment) = split_location_parent_and_segment(rest);
        let parent_dir = if parent_rel.is_empty() {
            home.to_path_buf()
        } else {
            home.join(&parent_rel)
        };
        let parent_input = if parent_rel.is_empty() {
            "~/".to_string()
        } else {
            format!("~/{parent_rel}/")
        };
        return (parent_dir, parent_input, segment);
    }

    if trimmed.starts_with('/') {
        if trimmed == "/" {
            return (PathBuf::from("/"), "/".to_string(), String::new());
        }
        let (parent_raw, segment) = split_location_parent_and_segment(trimmed);
        let parent_path = if parent_raw.is_empty() {
            "/".to_string()
        } else {
            parent_raw
        };
        let parent_input = if parent_path == "/" {
            "/".to_string()
        } else {
            format!("{parent_path}/")
        };
        return (PathBuf::from(parent_path), parent_input, segment);
    }

    let (parent_rel, segment) = split_location_parent_and_segment(trimmed);
    let parent_dir = if parent_rel.is_empty() {
        home.to_path_buf()
    } else {
        home.join(&parent_rel)
    };
    let parent_input = if parent_rel.is_empty() {
        String::new()
    } else {
        format!("{parent_rel}/")
    };
    (parent_dir, parent_input, segment)
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

fn settings_state_path() -> PathBuf {
    home_directory().join(".lumen").join("settings.json")
}

fn default_theme_base_color() -> String {
    "#444F66".to_string()
}

fn default_theme_highlight_color() -> String {
    "#2F86E9".to_string()
}

fn default_theme_lightness_offset() -> i32 {
    0
}

fn normalize_hex_color(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let raw = trimmed.strip_prefix('#').unwrap_or(trimmed);
    let expanded = match raw.len() {
        3 => {
            let mut output = String::with_capacity(6);
            for ch in raw.chars() {
                if !ch.is_ascii_hexdigit() {
                    return None;
                }
                output.push(ch);
                output.push(ch);
            }
            output
        }
        6 => {
            if !raw.chars().all(|ch| ch.is_ascii_hexdigit()) {
                return None;
            }
            raw.to_string()
        }
        _ => return None,
    };
    Some(format!("#{}", expanded.to_ascii_uppercase()))
}

fn normalize_directory_sorts(directory_sorts: &mut BTreeMap<String, DirectorySortMode>) {
    directory_sorts.retain(|path, mode| {
        PathBuf::from(path).is_absolute() && *mode != DirectorySortMode::Alphabetical
    });
}

fn normalize_theme_base_color(theme_base_color: &mut String) {
    if let Some(normalized) = normalize_hex_color(theme_base_color) {
        *theme_base_color = normalized;
    } else {
        *theme_base_color = default_theme_base_color();
    }
}

fn normalize_theme_highlight_color(theme_highlight_color: &mut String) {
    if let Some(normalized) = normalize_hex_color(theme_highlight_color) {
        *theme_highlight_color = normalized;
    } else {
        *theme_highlight_color = default_theme_highlight_color();
    }
}

fn normalize_theme_lightness_offset(theme_lightness_offset: &mut i32) {
    *theme_lightness_offset = (*theme_lightness_offset).clamp(-40, 40);
}

fn normalize_settings_store(settings: &mut SettingsStore) {
    normalize_directory_sorts(&mut settings.directory_sorts);
    normalize_theme_base_color(&mut settings.theme_base_color);
    normalize_theme_highlight_color(&mut settings.theme_highlight_color);
    normalize_theme_lightness_offset(&mut settings.theme_lightness_offset);
}

fn legacy_directory_sorts_from_tabs_state() -> BTreeMap<String, DirectorySortMode> {
    let raw = match fs::read_to_string(tabs_state_path()) {
        Ok(raw) => raw,
        Err(_) => return BTreeMap::new(),
    };
    let mut directory_sorts = BTreeMap::new();
    if let Ok(store) = serde_json::from_str::<LegacyTabsStoreSettings>(&raw) {
        for model in store.windows.values() {
            directory_sorts.extend(model.directory_sorts.clone());
        }
    }
    if directory_sorts.is_empty() {
        if let Ok(model) = serde_json::from_str::<LegacyTabsModelSettings>(&raw) {
            directory_sorts.extend(model.directory_sorts);
        }
    }
    normalize_directory_sorts(&mut directory_sorts);
    directory_sorts
}

fn load_tabs_store_from_disk() -> Option<TabsStore> {
    let path = tabs_state_path();
    let raw = fs::read_to_string(path).ok()?;
    if let Ok(store) = serde_json::from_str::<TabsStore>(&raw) {
        if !store.windows.is_empty() {
            return Some(store);
        }
    }
    let legacy = serde_json::from_str::<TabsModel>(&raw).ok()?;
    let mut windows = BTreeMap::new();
    windows.insert("main".to_string(), legacy);
    Some(TabsStore { windows })
}

fn load_settings_store_from_disk() -> Option<SettingsStore> {
    let path = settings_state_path();
    let raw = fs::read_to_string(path).ok()?;
    let mut value = serde_json::from_str::<serde_json::Value>(&raw).ok()?;
    if let Some(object) = value.as_object_mut() {
        let has_highlight_color = object
            .get("theme_highlight_color")
            .and_then(|value| value.as_str())
            .and_then(normalize_hex_color)
            .is_some();
        if !has_highlight_color {
            if let Some(base_color) = object
                .get("theme_base_color")
                .and_then(|value| value.as_str())
                .and_then(normalize_hex_color)
            {
                object.insert(
                    "theme_highlight_color".to_string(),
                    serde_json::Value::String(base_color),
                );
            }
        }
    }
    serde_json::from_value::<SettingsStore>(value).ok()
}

fn persist_tabs_store(store: &TabsStore) {
    let path = tabs_state_path();
    if let Some(parent) = path.parent() {
        if fs::create_dir_all(parent).is_err() {
            return;
        }
    }

    let Ok(json) = serde_json::to_vec_pretty(store) else {
        return;
    };
    if let Ok(existing) = fs::read(&path) {
        if existing == json {
            return;
        }
    }
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

fn persist_settings_store(store: &SettingsStore) {
    let path = settings_state_path();
    if let Some(parent) = path.parent() {
        if fs::create_dir_all(parent).is_err() {
            return;
        }
    }

    let Ok(json) = serde_json::to_vec_pretty(store) else {
        return;
    };
    if let Ok(existing) = fs::read(&path) {
        if existing == json {
            return;
        }
    }
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
    tabs: Mutex<TabsStore>,
    settings: Mutex<SettingsStore>,
}

impl Default for FileTabsState {
    fn default() -> Self {
        let home = home_directory();
        let mut store = load_tabs_store_from_disk().unwrap_or_default();
        if store.windows.is_empty() {
            store
                .windows
                .insert("main".to_string(), default_tabs_model());
        }
        if !store.windows.contains_key("main") {
            if let Some(first_label) = store.windows.keys().next().cloned() {
                if let Some(model) = store.windows.remove(&first_label) {
                    store.windows.insert("main".to_string(), model);
                }
            }
        }
        if !store.windows.contains_key("main") {
            store
                .windows
                .insert("main".to_string(), default_tabs_model());
        }
        for model in store.windows.values_mut() {
            ensure_tabs(model, &home);
        }
        let loaded_settings = load_settings_store_from_disk();
        let should_migrate_settings = loaded_settings.is_none();
        let mut settings = loaded_settings.unwrap_or_default();
        normalize_settings_store(&mut settings);
        if should_migrate_settings && settings.directory_sorts.is_empty() {
            settings.directory_sorts = legacy_directory_sorts_from_tabs_state();
            normalize_settings_store(&mut settings);
            if !settings.directory_sorts.is_empty() {
                persist_settings_store(&settings);
            }
        }
        Self {
            tabs: Mutex::new(store),
            settings: Mutex::new(settings),
        }
    }
}

fn ensure_window_model<'a>(store: &'a mut TabsStore, window_label: &str) -> &'a mut TabsModel {
    store
        .windows
        .entry(window_label.to_string())
        .or_insert_with(default_tabs_model)
}

fn ensure_window_tabs_model<'a>(
    store: &'a mut TabsStore,
    window_label: &str,
    home: &Path,
) -> &'a mut TabsModel {
    let model = ensure_window_model(store, window_label);
    ensure_tabs(model, home);
    model
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
    shared: Arc<Mutex<HashMap<String, SearchModel>>>,
    window_label: String,
    generation: u64,
    root: PathBuf,
    query: String,
    show_hidden: bool,
) {
    const MAX_RESULTS: usize = 200;
    const FLUSH_EVERY: u64 = 40;
    let query_lower = query.to_ascii_lowercase();
    let mut queue = VecDeque::from([root.clone()]);
    let home = home_directory();
    let mut scanned = 0u64;
    let mut ranked: Vec<(i64, SearchResultItem)> = Vec::new();

    while let Some(directory) = queue.pop_front() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let is_hidden = is_hidden_component(&directory, &name, &home);
            if !show_hidden && is_hidden {
                continue;
            }
            scanned += 1;
            let metadata = match entry.metadata() {
                Ok(value) => value,
                Err(_) => continue,
            };
            let is_dir = metadata.is_dir();
            if is_dir {
                queue.push_back(path.clone());
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
                let Ok(mut searches) = shared.lock() else {
                    return;
                };
                let Some(model) = searches.get_mut(&window_label) else {
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

    let Ok(mut searches) = shared.lock() else {
        return;
    };
    let Some(model) = searches.get_mut(&window_label) else {
        return;
    };
    if model.generation != generation {
        return;
    }
    model.scanned = scanned;
    model.running = false;
    model.results = sorted_ranked_results(&ranked, MAX_RESULTS);
}

fn search_model_mut<'a>(
    searches: &'a mut HashMap<String, SearchModel>,
    window_label: &str,
) -> &'a mut SearchModel {
    searches
        .entry(window_label.to_string())
        .or_insert_with(default_search_model)
}

fn ensure_tabs(model: &mut TabsModel, home: &Path) {
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

pub fn ensure_main_window<R: Runtime>(app: &AppHandle<R>) -> Result<(), tauri::Error> {
    let _ = ensure_browser_window(app, "main")?;
    Ok(())
}

pub fn restore_saved_additional_windows<R: Runtime>(app: &AppHandle<R>) {
    let labels = {
        let state = app.state::<FileTabsState>();
        let Ok(store) = state.tabs.lock() else {
            return;
        };
        store
            .windows
            .keys()
            .filter(|label| label.as_str() != "main")
            .cloned()
            .collect::<Vec<_>>()
    };

    for label in labels {
        let _ = ensure_browser_window(app, &label);
    }
}

fn saved_geometry_for_label<R: Runtime>(
    app: &AppHandle<R>,
    window_label: &str,
) -> Option<WindowGeometry> {
    let state = app.state::<FileTabsState>();
    let Ok(store) = state.tabs.lock() else {
        return None;
    };
    store
        .windows
        .get(window_label)
        .and_then(|model| model.window.clone())
}

fn is_browser_window_label(label: &str) -> bool {
    label == "main" || label.starts_with("main-")
}

pub fn is_companion_window_label(label: &str) -> bool {
    is_browser_window_label(label)
}

pub fn refresh_browser_window<R: Runtime>(app: &AppHandle<R>, window_label: &str) {
    if !is_browser_window_label(window_label) {
        return;
    }
    let Some(window) = app.get_webview_window(window_label) else {
        return;
    };
    let _ = window.eval(
        "(function(){\
            if(!window.htmx){return;}\
            window.htmx.ajax('GET','command/index',{target:'#main-root',swap:'outerHTML'});\
        })();",
    );
}

pub fn browser_window_count<R: Runtime>(app: &AppHandle<R>) -> usize {
    app.webview_windows()
        .keys()
        .filter(|label| is_browser_window_label(label))
        .count()
}

pub fn remove_window_state<R: Runtime>(app: &AppHandle<R>, window_label: &str) {
    if !is_browser_window_label(window_label) {
        return;
    }
    let state = app.state::<FileTabsState>();
    let Ok(mut store) = state.tabs.lock() else {
        return;
    };
    store.windows.remove(window_label);
    if store.windows.is_empty() {
        store
            .windows
            .insert("main".to_string(), default_tabs_model());
    }
    persist_tabs_store(&store);
}

pub fn persist_window_state<R: Runtime>(window: &Window<R>) {
    let label = window.label().to_string();
    if !is_browser_window_label(&label) {
        return;
    }
    let Ok(position) = window.inner_position() else {
        return;
    };
    let Ok(size) = window.inner_size() else {
        return;
    };

    let state = window.state::<FileTabsState>();
    let Ok(mut store) = state.tabs.lock() else {
        return;
    };
    let model = ensure_window_model(&mut store, &label);
    model.window = Some(WindowGeometry {
        x: position.x,
        y: position.y,
        width: size.width,
        height: size.height,
    });
    persist_tabs_store(&store);
}

pub fn open_new_window<R: Runtime>(app: &AppHandle<R>) -> Result<(), String> {
    let label = next_window_label(app);
    {
        let state = app.state::<FileTabsState>();
        let mut store = state
            .tabs
            .lock()
            .map_err(|_| "failed to lock tabs state".to_string())?;
        store.windows.insert(label.clone(), default_tabs_model());
        persist_tabs_store(&store);
    }
    if let Err(error) = ensure_browser_window(app, &label) {
        let state = app.state::<FileTabsState>();
        if let Ok(mut store) = state.tabs.lock() {
            store.windows.remove(&label);
            persist_tabs_store(&store);
        }
        return Err(error.to_string());
    }
    Ok(())
}

pub fn open_appearance_window<R: Runtime>(app: &AppHandle<R>) -> Result<(), String> {
    let label = "appearance-settings";
    if let Some(window) = app.get_webview_window(label) {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
        return Ok(());
    }

    let builder = WebviewWindowBuilder::new(app, label, WebviewUrl::App("appearance.html".into()))
        .title("Settings")
        .inner_size(440.0, 220.0)
        .min_inner_size(420.0, 200.0)
        .decorations(true)
        .resizable(false)
        .accept_first_mouse(true);
    let window = builder.build().map_err(|error| error.to_string())?;
    let _ = window.set_focus();
    Ok(())
}

fn ensure_browser_window<R: Runtime>(
    app: &AppHandle<R>,
    window_label: &str,
) -> Result<tauri::WebviewWindow<R>, tauri::Error> {
    if let Some(window) = app.get_webview_window(window_label) {
        return Ok(window);
    }
    let saved_geometry = saved_geometry_for_label(app, window_label);
    let mut builder =
        WebviewWindowBuilder::new(app, window_label, WebviewUrl::App("index.html".into()))
            .title("Lumen")
            .inner_size(800.0, 680.0);
    if let Some(geometry) = saved_geometry {
        builder = builder
            .inner_size(geometry.width as f64, geometry.height as f64)
            .position(geometry.x as f64, geometry.y as f64);
    }
    #[cfg(target_os = "macos")]
    {
        builder = builder
            .title_bar_style(tauri::TitleBarStyle::Overlay)
            .traffic_light_position(LogicalPosition::new(
                MAC_TRAFFIC_LIGHT_X,
                MAC_TRAFFIC_LIGHT_Y,
            ))
            .hidden_title(true);
    }
    let window = builder.build()?;
    #[cfg(target_os = "macos")]
    {
        let _ = reapply_macos_traffic_lights_webview(&window);
    }
    Ok(window)
}

#[cfg(target_os = "macos")]
unsafe fn layout_window_traffic_lights(
    ns_window: *mut NSWindow,
    point: MacLogicalPosition,
    space_between: Option<f64>,
) {
    let (x, y) = (point.x, point.y);

    let close_button: *mut AnyObject =
        unsafe { msg_send![ns_window, standardWindowButton: NSWindowButton::CloseButton] };
    let minimize_button: *mut AnyObject =
        unsafe { msg_send![ns_window, standardWindowButton: NSWindowButton::MiniaturizeButton] };
    let zoom_button: *mut AnyObject =
        unsafe { msg_send![ns_window, standardWindowButton: NSWindowButton::ZoomButton] };
    if close_button.is_null() || minimize_button.is_null() || zoom_button.is_null() {
        return;
    }

    let titlebar_container: *mut AnyObject = unsafe {
        let view: *mut AnyObject = msg_send![close_button, superview];
        msg_send![view, superview]
    };
    if titlebar_container.is_null() {
        return;
    }

    let close_frame: NSRect = unsafe { msg_send![close_button, frame] };
    let mini_frame: NSRect = unsafe { msg_send![minimize_button, frame] };
    let space_between = space_between.unwrap_or_else(|| mini_frame.origin.x - close_frame.origin.x);

    let window_frame: NSRect = unsafe { msg_send![ns_window, frame] };
    let mut titlebar_frame: NSRect = unsafe { msg_send![titlebar_container, frame] };
    titlebar_frame.size.height = close_frame.size.height + y;
    titlebar_frame.origin.y = window_frame.size.height - titlebar_frame.size.height;
    let _: () = unsafe { msg_send![titlebar_container, setFrame: titlebar_frame] };

    for (index, button) in [close_button, minimize_button, zoom_button]
        .iter()
        .enumerate()
    {
        let mut frame: NSRect = unsafe { msg_send![*button, frame] };
        frame.origin.x = x + (index as f64 * space_between);
        frame.origin.y = (titlebar_frame.size.height - frame.size.height) / 2.0;
        let _: () = unsafe { msg_send![*button, setFrame: frame] };
    }
}

#[cfg(target_os = "macos")]
fn reapply_macos_traffic_lights_webview<R: Runtime>(
    window: &tauri::WebviewWindow<R>,
) -> Result<(), String> {
    let ns_window_ptr = window.ns_window().map_err(|error| error.to_string())?;
    let ns_window_ptr = ns_window_ptr as usize;
    window
        .run_on_main_thread(move || {
            // SAFETY: `ns_window_ptr` is obtained from Tauri for this live window and used only on
            // the main thread where AppKit view operations are valid.
            let ns_window = ns_window_ptr as *mut NSWindow;
            unsafe {
                layout_window_traffic_lights(
                    ns_window,
                    MacLogicalPosition {
                        x: MAC_TRAFFIC_LIGHT_X,
                        y: MAC_TRAFFIC_LIGHT_Y,
                    },
                    None,
                );
            }
        })
        .map_err(|error| error.to_string())
}

#[cfg(target_os = "macos")]
fn set_macos_title_and_relayout<R: Runtime>(window: &Window<R>, title: &str) -> Result<(), String> {
    let ns_window_ptr = window.ns_window().map_err(|error| error.to_string())?;
    let ns_window_ptr = ns_window_ptr as usize;
    let title = title.to_string();
    window
        .run_on_main_thread(move || {
            // SAFETY: `ns_window_ptr` is obtained from Tauri for this live window and used only on
            // the main thread where AppKit view operations are valid.
            let ns_window = ns_window_ptr as *mut NSWindow;
            unsafe {
                let ns_title = NSString::from_str(&title);
                let _: () = msg_send![ns_window, setTitle: &*ns_title];
                layout_window_traffic_lights(
                    ns_window,
                    MacLogicalPosition {
                        x: MAC_TRAFFIC_LIGHT_X,
                        y: MAC_TRAFFIC_LIGHT_Y + 4.0,
                    },
                    None,
                );
            }
        })
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub fn set_window_title(window: Window, title: String) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        set_macos_title_and_relayout(&window, &title)
    }
    #[cfg(not(target_os = "macos"))]
    {
        window
            .set_title(&title)
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

fn next_window_label<R: Runtime>(app: &AppHandle<R>) -> String {
    let mut index = 2u64;
    let stored_labels = {
        let state = app.state::<FileTabsState>();
        state
            .tabs
            .lock()
            .map(|store| store.windows.keys().cloned().collect::<BTreeSet<_>>())
            .unwrap_or_default()
    };
    loop {
        let candidate = format!("main-{index}");
        if app.get_webview_window(&candidate).is_none() && !stored_labels.contains(&candidate) {
            return candidate;
        }
        index = index.saturating_add(1);
    }
}

fn active_tab_mut(model: &mut TabsModel) -> Option<&mut TabState> {
    let index = model
        .tabs
        .iter()
        .position(|tab| tab.id == model.active_id)
        .unwrap_or(0);
    model.tabs.get_mut(index)
}

fn settings_snapshot(
    state: &FileTabsState,
) -> (BTreeMap<String, DirectorySortMode>, String, String, i32) {
    state
        .settings
        .lock()
        .map(|settings| {
            (
                settings.directory_sorts.clone(),
                settings.theme_base_color.clone(),
                settings.theme_highlight_color.clone(),
                settings.theme_lightness_offset,
            )
        })
        .unwrap_or_else(|_| {
            (
                BTreeMap::new(),
                default_theme_base_color(),
                default_theme_highlight_color(),
                default_theme_lightness_offset(),
            )
        })
}

fn render_view_for_state(model: &TabsModel, state: &FileTabsState) -> String {
    let (directory_sorts, theme_base_color, theme_highlight_color, theme_lightness_offset) =
        settings_snapshot(state);
    render_view(
        model,
        &directory_sorts,
        &theme_base_color,
        &theme_highlight_color,
        theme_lightness_offset,
    )
}

fn render_view(
    model: &TabsModel,
    directory_sorts: &BTreeMap<String, DirectorySortMode>,
    theme_base_color: &str,
    theme_highlight_color: &str,
    theme_lightness_offset: i32,
) -> String {
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
    let active_root_above_home = active_root_path != home && home.starts_with(&active_root_path);
    let active_root_under_home = active_root_path.starts_with(&home);
    let active_focus_path = active_tab
        .and_then(|tab| tab.focus_path.clone())
        .map(PathBuf::from)
        .filter(|path| path.starts_with(&active_root_path));
    let active_show_hidden = active_tab.map(|tab| tab.show_hidden).unwrap_or(false);
    let columns = build_columns(
        &active_root_path,
        active_focus_path.as_deref(),
        directory_sorts,
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
    context.insert("home_path", &home.to_string_lossy().to_string());
    context.insert("root_path", &active_root_path.to_string_lossy().to_string());
    context.insert("active_root_above_home", &active_root_above_home);
    context.insert("active_root_under_home", &active_root_under_home);
    context.insert("show_hidden", &active_show_hidden);
    context.insert("columns", &columns);
    context.insert("tabs", &tabs);
    context.insert("active_path", &active_path_for_new);
    context.insert("active_tab_id", &model.active_id.to_string());
    context.insert("preview", &preview);
    context.insert("theme_base_color", theme_base_color);
    context.insert("theme_highlight_color", theme_highlight_color);
    context.insert("theme_lightness_offset", &theme_lightness_offset);

    // context.insert("user_needs_to_setup_biometrics", &user_needs_to_setup_biometrics);
    render!("files/index", &context)
}

fn apply_navigate_to_model(model: &mut TabsModel, home: &Path, path: &str) {
    let normalized = normalize_tab_path(home, Some(path));
    if let Some(tab) = active_tab_mut(model) {
        let root = active_tab_root_path(tab, home);
        if normalized.starts_with(&root) {
            tab.focus_path = Some(normalized.to_string_lossy().to_string());
        }
    }
}

fn companion_tabs(model: &TabsModel, home: &Path) -> Vec<CompanionTab> {
    let mut root_basename_counts: HashMap<String, usize> = HashMap::new();
    for tab in &model.tabs {
        let root = active_tab_root_path(tab, home);
        let key = label_from_path(&root);
        *root_basename_counts.entry(key).or_insert(0) += 1;
    }
    model
        .tabs
        .iter()
        .map(|tab| {
            let root = active_tab_root_path(tab, home);
            let basename = label_from_path(&root);
            let disambiguate_root = root_basename_counts.get(&basename).copied().unwrap_or(0) > 1;
            CompanionTab {
                id: tab.id.to_string(),
                title: tab_title(tab, home, disambiguate_root),
                is_active: tab.id == model.active_id,
            }
        })
        .collect()
}

fn companion_columns(columns: &[Column]) -> Vec<CompanionColumn> {
    columns
        .iter()
        .map(|column| CompanionColumn {
            title: column.title.clone(),
            path: column.path.clone(),
            selected_path: column.selected_path.clone(),
            entries: column
                .entries
                .iter()
                .map(|entry| CompanionEntry {
                    name: entry.name.clone(),
                    path: entry.path.clone(),
                    is_dir: entry.is_dir,
                    icon: entry.icon.to_string(),
                })
                .collect(),
        })
        .collect()
}

fn companion_revision(
    model: &TabsModel,
    columns: &[Column],
    active_root_path: &Path,
    active_path: &str,
    show_hidden: bool,
) -> String {
    let mut hasher = DefaultHasher::new();
    model.active_id.hash(&mut hasher);
    for tab in &model.tabs {
        tab.id.hash(&mut hasher);
        tab.root_path.hash(&mut hasher);
        tab.focus_path.hash(&mut hasher);
        tab.show_hidden.hash(&mut hasher);
    }
    active_root_path
        .to_string_lossy()
        .to_string()
        .hash(&mut hasher);
    active_path.hash(&mut hasher);
    show_hidden.hash(&mut hasher);
    for column in columns {
        column.path.hash(&mut hasher);
        column.selected_path.hash(&mut hasher);
        for entry in &column.entries {
            entry.path.hash(&mut hasher);
            entry.name.hash(&mut hasher);
            entry.is_dir.hash(&mut hasher);
        }
    }
    format!("{:016x}", hasher.finish())
}

fn companion_snapshot_from_model(
    model: &TabsModel,
    state: &FileTabsState,
    window_label: &str,
) -> CompanionSnapshot {
    let (directory_sorts, _, _, _) = settings_snapshot(state);
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
        &directory_sorts,
        active_show_hidden,
    );
    let active_path = active_focus_path
        .as_ref()
        .map(|path| path.to_string_lossy().to_string())
        .unwrap_or_else(|| active_root_path.to_string_lossy().to_string());
    let active_tab_id = active_tab
        .map(|tab| tab.id.to_string())
        .unwrap_or_else(|| "1".to_string());
    let revision = companion_revision(
        model,
        &columns,
        &active_root_path,
        &active_path,
        active_show_hidden,
    );

    CompanionSnapshot {
        window_label: window_label.to_string(),
        home_path: home.to_string_lossy().to_string(),
        root_path: active_root_path.to_string_lossy().to_string(),
        active_path,
        active_tab_id,
        show_hidden: active_show_hidden,
        tabs: companion_tabs(model, &home),
        columns: companion_columns(&columns),
        revision,
    }
}

pub fn companion_snapshot(
    state: &FileTabsState,
    window_label: &str,
) -> Result<CompanionSnapshot, String> {
    let home = home_directory();
    if !is_browser_window_label(window_label) {
        return Err("unsupported window label".to_string());
    }
    let model = {
        let mut store = state
            .tabs
            .lock()
            .map_err(|_| "failed to lock tabs state".to_string())?;
        ensure_window_tabs_model(&mut store, window_label, &home).clone()
    };
    Ok(companion_snapshot_from_model(&model, state, window_label))
}

pub fn companion_navigate(
    state: &FileTabsState,
    window_label: &str,
    path: String,
) -> Result<CompanionSnapshot, String> {
    let home = home_directory();
    if !is_browser_window_label(window_label) {
        return Err("unsupported window label".to_string());
    }
    let model = {
        let mut store = state
            .tabs
            .lock()
            .map_err(|_| "failed to lock tabs state".to_string())?;
        let model = ensure_window_tabs_model(&mut store, window_label, &home);
        apply_navigate_to_model(model, &home, &path);
        let next = model.clone();
        persist_tabs_store(&store);
        next
    };
    Ok(companion_snapshot_from_model(&model, state, window_label))
}

pub struct FileContextMenuState {
    pub pending: Mutex<Option<PendingContextTarget>>,
}

#[derive(Debug, Clone)]
pub struct PendingContextTarget {
    pub window_label: String,
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
pub fn index(window: Window, state: State<'_, FileTabsState>) -> String {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = match state.tabs.lock() {
        Ok(store) => store,
        Err(_) => return String::new(),
    };
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    rendered
}

#[tauri::command]
pub fn navigate(window: Window, state: State<'_, FileTabsState>, path: String) -> String {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = match state.tabs.lock() {
        Ok(store) => store,
        Err(_) => return String::new(),
    };
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        apply_navigate_to_model(model, &home, &path);

        render_view_for_state(model, state.inner())
    };

    persist_tabs_store(&store);
    rendered
}

#[tauri::command]
pub fn validate_location_path(path: String) -> Result<String, String> {
    let home = home_directory();
    let target = resolve_location_input_path(&home, &path)?;
    Ok(target.to_string_lossy().to_string())
}

#[tauri::command]
pub fn goto_directory_suggestions(path: String) -> Vec<GoToDirectorySuggestion> {
    const MAX_RESULTS: usize = 24;

    let home = home_directory();
    let (parent_dir, parent_input, segment) = location_suggestion_context(&home, &path);
    if !parent_dir.is_dir() {
        return Vec::new();
    }

    let segment_lower = segment.to_ascii_lowercase();
    let show_hidden = segment.starts_with('.');
    let mut ranked: Vec<(i64, String)> = Vec::new();
    let Ok(entries) = fs::read_dir(&parent_dir) else {
        return Vec::new();
    };

    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().to_string();
        if name.is_empty() {
            continue;
        }
        if !show_hidden && is_hidden_component(&parent_dir, &name, &home) {
            continue;
        }

        let name_lower = name.to_ascii_lowercase();
        let mut score = match fuzzy_score(&name_lower, &segment_lower) {
            Some(value) => value,
            None => continue,
        };
        if !segment_lower.is_empty() && name_lower.starts_with(&segment_lower) {
            score += 200;
        }
        ranked.push((score, name));
    }

    ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    ranked.truncate(MAX_RESULTS);
    ranked
        .into_iter()
        .map(|(_, name)| GoToDirectorySuggestion {
            value: format!("{parent_input}{name}/"),
            name,
        })
        .collect()
}

#[tauri::command]
pub fn new_window(app: AppHandle) -> Result<(), String> {
    open_new_window(&app)
}

#[tauri::command]
pub fn open_appearance_window_command(app: AppHandle) -> Result<(), String> {
    open_appearance_window(&app)
}

#[tauri::command]
pub fn go_to_location(
    window: Window,
    state: State<'_, FileTabsState>,
    path: String,
) -> Result<String, String> {
    let home = home_directory();
    let target = resolve_location_input_path(&home, &path)?;
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);

        if let Some(tab) = active_tab_mut(model) {
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

        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
}

#[tauri::command]
pub fn activate_tab(window: Window, state: State<'_, FileTabsState>, tab_id: String) -> String {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = match state.tabs.lock() {
        Ok(store) => store,
        Err(_) => return String::new(),
    };
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        if let Ok(parsed_id) = tab_id.parse::<u64>() {
            if model.tabs.iter().any(|tab| tab.id == parsed_id) {
                model.active_id = parsed_id;
            }
        }

        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    rendered
}

#[tauri::command]
pub fn new_tab(window: Window, state: State<'_, FileTabsState>, _path: String) -> String {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = match state.tabs.lock() {
        Ok(store) => store,
        Err(_) => return String::new(),
    };
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        let id = model.next_id;
        model.next_id += 1;
        model.tabs.push(TabState {
            id,
            root_path: None,
            focus_path: None,
            show_hidden: false,
        });
        model.active_id = id;
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    rendered
}

fn close_tab_in_model(model: &mut TabsModel, tab_id: u64) {
    let Some(index) = model.tabs.iter().position(|tab| tab.id == tab_id) else {
        return;
    };
    if model.tabs.len() == 1 {
        model.tabs[0].root_path = None;
        model.tabs[0].focus_path = None;
        model.active_id = model.tabs[0].id;
        return;
    }
    model.tabs.remove(index);
    if model.active_id == tab_id {
        let new_index = if index == 0 { 0 } else { index - 1 };
        model.active_id = model.tabs[new_index].id;
    } else if !model.tabs.iter().any(|tab| tab.id == model.active_id) {
        model.active_id = model.tabs[0].id;
    }
}

#[tauri::command]
pub fn close_tab(window: Window, state: State<'_, FileTabsState>, tab_id: String) -> String {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = match state.tabs.lock() {
        Ok(store) => store,
        Err(_) => return String::new(),
    };
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        if let Ok(parsed_id) = tab_id.parse::<u64>() {
            close_tab_in_model(model, parsed_id);
        }
        render_view_for_state(model, state.inner())
    };

    persist_tabs_store(&store);
    rendered
}

#[tauri::command]
pub fn close_tab_or_window(
    window: Window,
    state: State<'_, FileTabsState>,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    let should_close_window = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        model.tabs.len() <= 1
    };
    if should_close_window {
        drop(store);
        window.close().map_err(|error| error.to_string())?;
        return Ok(String::new());
    }
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        let active_id = model.active_id;
        close_tab_in_model(model, active_id);
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
}

#[tauri::command]
pub fn reorder_tabs(window: Window, state: State<'_, FileTabsState>, tab_ids: String) -> String {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = match state.tabs.lock() {
        Ok(store) => store,
        Err(_) => return String::new(),
    };
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        let requested: Vec<u64> = tab_ids
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .filter_map(|value| value.parse::<u64>().ok())
            .collect();
        if !requested.is_empty() {
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
        }
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    rendered
}

#[tauri::command]
pub fn drop_files_into_directory(
    window: Window,
    state: State<'_, FileTabsState>,
    target_dir: String,
    source_paths: Vec<String>,
    operation: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;

    let op = operation.to_ascii_lowercase();
    if op != "copy" && op != "move" {
        return Err("unsupported operation".to_string());
    }

    let target = normalize_tab_path(&home, Some(&target_dir));
    perform_drop_operation(&home, &target, &source_paths, &op)?;

    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        if let Some(tab) = active_tab_mut(model) {
            let root = active_tab_root_path(tab, &home);
            if target.starts_with(&root) {
                tab.focus_path = Some(target.to_string_lossy().to_string());
            }
        }
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
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
    let window_label = window.label().to_string();
    let mut store = tabs_state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    let model = ensure_window_tabs_model(&mut store, &window_label, &home);
    let active_tab = model
        .tabs
        .iter()
        .find(|tab| tab.id == model.active_id)
        .or_else(|| model.tabs.first())
        .ok_or_else(|| "missing active tab".to_string())?;
    let root = active_tab_root_path(active_tab, &home);
    drop(store);

    let path_buf = PathBuf::from(&path);
    let is_multi_selection = selection_count > 1;
    let show_default_open = should_show_default_open_for_path(&path_buf, is_dir);
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
        window_label,
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
            .separator();
        if show_default_open {
            builder = builder.text("ctx_open_default", "Open");
        }
        builder = builder
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
    window: Window,
    state: State<'_, FileTabsState>,
    path: String,
    new_name: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;

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
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        set_active_focus_after_path_change(model, &home, Some(&destination), &source);
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
}

#[tauri::command]
pub fn trash_path(
    window: Window,
    state: State<'_, FileTabsState>,
    path: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;

    let source = resolve_home_scoped_path(&home, &path)?;
    if source == home {
        return Err("cannot trash root directory".to_string());
    }
    if !source.exists() {
        return Err("path does not exist".to_string());
    }
    move_to_trash(&source)?;
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        set_active_focus_after_path_change(model, &home, None, &source);
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
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

fn directory_listing_signature(path: &Path, show_hidden: bool, home: &Path) -> u64 {
    let mut hasher = DefaultHasher::new();
    path.to_string_lossy().to_string().hash(&mut hasher);

    let mut rows: Vec<(String, bool, u128)> = Vec::new();
    match fs::read_dir(path) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if !show_hidden && is_hidden_component(path, &name, home) {
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
    window: Window,
    tabs_state: State<'_, FileTabsState>,
    paths_json: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = tabs_state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    let model = ensure_window_tabs_model(&mut store, &window_label, &home);
    let show_hidden = model
        .tabs
        .iter()
        .find(|tab| tab.id == model.active_id)
        .or_else(|| model.tabs.first())
        .map(|tab| tab.show_hidden)
        .unwrap_or(false);
    drop(store);

    let paths = parse_paths_json_arg(&paths_json)?;
    let mut hasher = DefaultHasher::new();
    for path in paths {
        let path_buf = PathBuf::from(&path);
        if !path_buf.is_absolute() || !path_buf.is_dir() {
            continue;
        }
        directory_listing_signature(&path_buf, show_hidden, &home).hash(&mut hasher);
    }
    Ok(format!("{:016x}", hasher.finish()))
}

#[tauri::command]
pub fn trash_paths(
    window: Window,
    state: State<'_, FileTabsState>,
    paths_json: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;

    let paths = parse_paths_json_arg(&paths_json)?;
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        for path in &paths {
            let source = resolve_home_scoped_path(&home, path)?;
            if source == home {
                return Err("cannot trash root directory".to_string());
            }
            if !source.exists() {
                return Err("path does not exist".to_string());
            }
            move_to_trash(&source)?;
            set_active_focus_after_path_change(model, &home, None, &source);
        }
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
}

#[tauri::command]
pub fn delete_path(
    window: Window,
    state: State<'_, FileTabsState>,
    path: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;

    let source = resolve_home_scoped_path(&home, &path)?;
    if source == home {
        return Err("cannot delete root directory".to_string());
    }
    if !source.exists() {
        return Err("path does not exist".to_string());
    }
    remove_permanently(&source)?;
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        set_active_focus_after_path_change(model, &home, None, &source);
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
}

#[tauri::command]
pub fn delete_paths(
    window: Window,
    state: State<'_, FileTabsState>,
    paths_json: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;

    let paths = parse_paths_json_arg(&paths_json)?;
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        for path in &paths {
            let source = resolve_home_scoped_path(&home, path)?;
            if source == home {
                return Err("cannot delete root directory".to_string());
            }
            if !source.exists() {
                return Err("path does not exist".to_string());
            }
            remove_permanently(&source)?;
            set_active_focus_after_path_change(model, &home, None, &source);
        }
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
}

#[tauri::command]
pub fn create_directory(
    window: Window,
    state: State<'_, FileTabsState>,
    parent_path: String,
    name: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;

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
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        set_active_focus_after_path_change(model, &home, Some(&target), &parent);
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
}

#[tauri::command]
pub fn create_file(
    window: Window,
    state: State<'_, FileTabsState>,
    parent_path: String,
    name: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;

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
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        set_active_focus_after_path_change(model, &home, Some(&target), &parent);
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
}

#[tauri::command]
pub fn set_directory_sort(
    window: Window,
    state: State<'_, FileTabsState>,
    path: String,
    mode: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;

    let directory = resolve_home_scoped_path(&home, &path)?;
    if !directory.is_dir() {
        return Err("path is not a directory".to_string());
    }
    let parsed_mode =
        DirectorySortMode::from_input(&mode).ok_or_else(|| "invalid sort mode".to_string())?;
    let mut settings = state
        .settings
        .lock()
        .map_err(|_| "failed to lock settings state".to_string())?;
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        let key = directory.to_string_lossy().to_string();
        if parsed_mode == DirectorySortMode::Alphabetical {
            settings.directory_sorts.remove(&key);
        } else {
            settings.directory_sorts.insert(key, parsed_mode);
        }
        normalize_settings_store(&mut settings);
        render_view(
            model,
            &settings.directory_sorts,
            &settings.theme_base_color,
            &settings.theme_highlight_color,
            settings.theme_lightness_offset,
        )
    };

    persist_tabs_store(&store);
    persist_settings_store(&settings);
    Ok(rendered)
}

#[tauri::command]
pub fn get_theme_base_color(state: State<'_, FileTabsState>) -> String {
    state
        .settings
        .lock()
        .map(|settings| settings.theme_base_color.clone())
        .unwrap_or_else(|_| default_theme_base_color())
}

#[tauri::command]
pub fn set_theme_base_color(
    app: AppHandle,
    state: State<'_, FileTabsState>,
    color: String,
) -> Result<String, String> {
    let normalized = normalize_hex_color(&color)
        .ok_or_else(|| "invalid color, expected #RRGGBB or #RGB".to_string())?;
    let mut settings = state
        .settings
        .lock()
        .map_err(|_| "failed to lock settings state".to_string())?;
    settings.theme_base_color = normalized.clone();
    normalize_settings_store(&mut settings);
    persist_settings_store(&settings);
    let _ = app.emit(
        "theme-base-color-changed",
        settings.theme_base_color.clone(),
    );
    Ok(settings.theme_base_color.clone())
}

#[tauri::command]
pub fn get_theme_highlight_color(state: State<'_, FileTabsState>) -> String {
    state
        .settings
        .lock()
        .map(|settings| settings.theme_highlight_color.clone())
        .unwrap_or_else(|_| default_theme_highlight_color())
}

#[tauri::command]
pub fn set_theme_highlight_color(
    app: AppHandle,
    state: State<'_, FileTabsState>,
    color: String,
) -> Result<String, String> {
    let normalized = normalize_hex_color(&color)
        .ok_or_else(|| "invalid color, expected #RRGGBB or #RGB".to_string())?;
    let mut settings = state
        .settings
        .lock()
        .map_err(|_| "failed to lock settings state".to_string())?;
    settings.theme_highlight_color = normalized.clone();
    normalize_settings_store(&mut settings);
    persist_settings_store(&settings);
    let _ = app.emit(
        "theme-highlight-color-changed",
        settings.theme_highlight_color.clone(),
    );
    Ok(settings.theme_highlight_color.clone())
}

#[tauri::command]
pub fn get_theme_lightness_offset(state: State<'_, FileTabsState>) -> i32 {
    state
        .settings
        .lock()
        .map(|settings| settings.theme_lightness_offset)
        .unwrap_or_else(|_| default_theme_lightness_offset())
}

#[tauri::command]
pub fn set_theme_lightness_offset(
    app: AppHandle,
    state: State<'_, FileTabsState>,
    offset: i32,
) -> Result<i32, String> {
    let mut settings = state
        .settings
        .lock()
        .map_err(|_| "failed to lock settings state".to_string())?;
    settings.theme_lightness_offset = offset;
    normalize_settings_store(&mut settings);
    persist_settings_store(&settings);
    let _ = app.emit(
        "theme-lightness-offset-changed",
        settings.theme_lightness_offset,
    );
    Ok(settings.theme_lightness_offset)
}

#[tauri::command]
pub fn set_tab_show_hidden(
    window: Window,
    state: State<'_, FileTabsState>,
    show_hidden: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;

    let parsed = match show_hidden.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => true,
        "false" | "0" | "no" | "off" => false,
        _ => return Err("invalid show_hidden value".to_string()),
    };

    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        if let Some(tab) = active_tab_mut(model) {
            tab.show_hidden = parsed;
        }
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
}

#[tauri::command]
pub fn fuzzy_search_start(
    window: Window,
    tabs_state: State<'_, FileTabsState>,
    search_state: State<'_, FileSearchState>,
    query: String,
) -> Result<(), String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let (root, show_hidden) = {
        let mut store = tabs_state
            .tabs
            .lock()
            .map_err(|_| "failed to lock tabs state".to_string())?;
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        let active_tab = model
            .tabs
            .iter()
            .find(|tab| tab.id == model.active_id)
            .or_else(|| model.tabs.first())
            .ok_or_else(|| "missing active tab".to_string())?;
        (
            active_tab_root_path(active_tab, &home),
            active_tab.show_hidden,
        )
    };

    let trimmed = query.trim().to_string();
    let shared = search_state.search.clone();
    let generation = {
        let mut searches = shared
            .lock()
            .map_err(|_| "failed to lock search state".to_string())?;
        let search = search_model_mut(&mut searches, &window_label);
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

    let worker_window_label = window_label.clone();
    std::thread::spawn(move || {
        run_fuzzy_search_worker(
            shared,
            worker_window_label,
            generation,
            root,
            trimmed,
            show_hidden,
        );
    });
    Ok(())
}

#[tauri::command]
pub fn fuzzy_search_cancel(
    window: Window,
    search_state: State<'_, FileSearchState>,
) -> Result<(), String> {
    let window_label = window.label().to_string();
    let mut searches = search_state
        .search
        .lock()
        .map_err(|_| "failed to lock search state".to_string())?;
    let search = search_model_mut(&mut searches, &window_label);
    search.generation = search.generation.saturating_add(1);
    search.query.clear();
    search.running = false;
    search.scanned = 0;
    search.results.clear();
    Ok(())
}

#[tauri::command]
pub fn fuzzy_search_results(window: Window, search_state: State<'_, FileSearchState>) -> String {
    let window_label = window.label().to_string();
    let (query, running, scanned, results) = {
        let Ok(mut searches) = search_state.search.lock() else {
            return String::new();
        };
        let search = search_model_mut(&mut searches, &window_label);
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

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn applescript_string_literal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            _ => escaped.push(ch),
        }
    }
    escaped.push('"');
    escaped
}

fn run_terminal_command_in_directory(directory: &Path, command: &str) -> Result<(), String> {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return Err("command cannot be empty".to_string());
    }
    let directory_value = directory.to_string_lossy().to_string();
    let shell_command = format!("cd {} && {}", shell_single_quote(&directory_value), trimmed);

    #[cfg(target_os = "macos")]
    {
        let script = format!(
            "tell application \"Terminal\" to do script {}",
            applescript_string_literal(&shell_command)
        );
        let status = Command::new("osascript")
            .arg("-e")
            .arg("tell application \"Terminal\" to activate")
            .arg("-e")
            .arg(script)
            .status()
            .map_err(|error| error.to_string())?;
        if status.success() {
            return Ok(());
        }
        return Err("failed to run command in Terminal".to_string());
    }

    #[cfg(not(target_os = "macos"))]
    {
        let status = Command::new("sh")
            .arg("-lc")
            .arg(&shell_command)
            .current_dir(directory)
            .status()
            .map_err(|error| error.to_string())?;
        if status.success() {
            return Ok(());
        }
        return Err("command failed".to_string());
    }
}

fn resolve_bin_script_path_for_command(
    directory: &Path,
    command: &context::ContextCommand,
) -> Option<PathBuf> {
    if !command.id.starts_with("bin:") {
        return None;
    }
    let trimmed = command.command.trim();
    let relative = trimmed.strip_prefix("./")?;
    let mut components = Path::new(relative).components();
    let first = components.next()?;
    let second = components.next()?;
    if components.next().is_some() {
        return None;
    }
    let first = first.as_os_str().to_str()?;
    let second = second.as_os_str().to_str()?;
    if first != "bin" || second.is_empty() {
        return None;
    }
    Some(directory.join(first).join(second))
}

fn ensure_executable_if_needed(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        let metadata = fs::metadata(path).map_err(|error| error.to_string())?;
        let mut permissions = metadata.permissions();
        let current_mode = permissions.mode();
        let executable_mode = current_mode | 0o111;
        if executable_mode != current_mode {
            permissions.set_mode(executable_mode);
            fs::set_permissions(path, permissions).map_err(|error| error.to_string())?;
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

#[tauri::command]
pub fn open_in_default(path: String) -> Result<(), String> {
    let target = resolve_absolute_existing_path(&path)?;
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
pub async fn load_markdown_image_data(
    markdown_path: String,
    image_src: String,
) -> Result<String, String> {
    let path = PathBuf::from(markdown_path);
    markdown_image_data_url(&path, &image_src)
        .ok_or_else(|| "Failed to resolve markdown image source.".to_string())
}

#[tauri::command]
pub async fn load_local_image_data_url(path: String) -> Result<String, String> {
    let source_path = PathBuf::from(path);
    let ext = source_path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    let mime = image_mime_from_ext(&ext)
        .ok_or_else(|| "Unsupported image type for inline markdown preview.".to_string())?;
    let bytes = fs::read(&source_path).map_err(|error| error.to_string())?;
    Ok(format!("data:{mime};base64,{}", STANDARD.encode(bytes)))
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
    window: Window,
    path: String,
    tabs_state: State<'_, FileTabsState>,
) -> Result<(), String> {
    let tab_root = active_tab_root_for_state(&tabs_state, window.label())?;
    open_in_github_desktop_with_tab_root(path, tab_root)
}

#[tauri::command]
pub fn run_context_terminal_command(path: String, command_id: String) -> Result<(), String> {
    let target = resolve_absolute_existing_path(&path)?;
    if !target.is_dir() {
        return Err("command path must be a directory".to_string());
    }
    let command_key = command_id.trim();
    if command_key.is_empty() {
        return Err("command id cannot be empty".to_string());
    }
    let command = context::commands_for_directory(&target)
        .into_iter()
        .find(|entry| entry.id == command_key)
        .ok_or_else(|| "command is not available for this directory".to_string())?;
    if let Some(script_path) = resolve_bin_script_path_for_command(&target, &command) {
        ensure_executable_if_needed(&script_path)?;
    }
    run_terminal_command_in_directory(&target, &command.command)
}

fn active_tab_root_for_state(
    tabs_state: &State<'_, FileTabsState>,
    window_label: &str,
) -> Result<PathBuf, String> {
    let home = home_directory();
    let mut store = tabs_state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    let model = ensure_window_tabs_model(&mut store, window_label, &home);
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
pub fn set_tab_root(
    window: Window,
    state: State<'_, FileTabsState>,
    path: String,
) -> Result<String, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;

    let target = PathBuf::from(path.trim());
    if !target.is_absolute() {
        return Err("path must be absolute".to_string());
    }
    if !target.is_dir() {
        return Err("tab root must be a directory".to_string());
    }
    let rendered = {
        let model = ensure_window_tabs_model(&mut store, &window_label, &home);
        if let Some(tab) = active_tab_mut(model) {
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
        render_view_for_state(model, state.inner())
    };
    persist_tabs_store(&store);
    Ok(rendered)
}

#[tauri::command]
pub fn path_context_capabilities(
    window: Window,
    path: String,
    tabs_state: State<'_, FileTabsState>,
) -> Result<PathContextCapabilities, String> {
    let home = home_directory();
    let window_label = window.label().to_string();
    let mut store = tabs_state
        .tabs
        .lock()
        .map_err(|_| "failed to lock tabs state".to_string())?;
    let model = ensure_window_tabs_model(&mut store, &window_label, &home);
    let tab_root = model
        .tabs
        .iter()
        .find(|tab| tab.id == model.active_id)
        .or_else(|| model.tabs.first())
        .map(|tab| active_tab_root_path(tab, &home))
        .unwrap_or_else(|| home.clone());
    drop(store);

    let target = resolve_absolute_existing_path(&path)?;
    let is_dir = target.is_dir();
    let context_commands = if is_dir {
        context::commands_for_directory(&target)
    } else {
        Vec::new()
    };
    Ok(PathContextCapabilities {
        is_dir,
        show_default_open: should_show_default_open_for_path(&target, is_dir),
        show_github_desktop: is_dir
            && resolve_github_desktop_repo_for_path(&target, &tab_root).is_some(),
        context_commands,
    })
}
