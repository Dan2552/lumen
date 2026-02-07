use serde::Serialize;
use std::{
    env, fs,
    io::Write,
    io::Read,
    path::{Path, PathBuf},
    sync::Mutex,
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
    entries: Vec<FileEntry>,
    selected_path: String,
}

#[derive(Debug, Serialize, Clone)]
struct PreviewModel {
    kind: String,
    title: String,
    subtitle: String,
    image_data_url: Option<String>,
    text_head: Option<String>,
    note: Option<String>,
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
    focus_path: Option<String>,
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
    entry: FileEntry,
}

fn list_directory(path: &PathBuf) -> Vec<FileEntry> {
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

            Some(SortableEntry {
                name_key: name.to_lowercase(),
                entry: FileEntry {
                    icon: icon_for_entry(&name, is_dir),
                    name,
                    path: entry_path.to_string_lossy().to_string(),
                    is_dir,
                },
            })
        })
        .collect();

    items.sort_by(|a, b| {
        b.entry
            .is_dir
            .cmp(&a.entry.is_dir)
            .then_with(|| a.name_key.cmp(&b.name_key))
    });

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

fn is_previewable_text_ext(ext: &str) -> bool {
    matches!(
        ext,
        "txt"
            | "md"
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
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();

    if let Some(mime) = image_mime_from_ext(&ext) {
        const MAX_IMAGE_PREVIEW_BYTES: u64 = 8 * 1024 * 1024;
        let file_size = fs::metadata(path).ok().map(|meta| meta.len()).unwrap_or(0);
        if file_size > MAX_IMAGE_PREVIEW_BYTES {
            return Some(PreviewModel {
                kind: "image".to_string(),
                title: file_name,
                subtitle,
                image_data_url: None,
                text_head: None,
                note: Some("Image is too large for inline preview.".to_string()),
            });
        }

        let bytes = fs::read(path).ok()?;
        let data_url = format!("data:{mime};base64,{}", STANDARD.encode(bytes));
        return Some(PreviewModel {
            kind: "image".to_string(),
            title: file_name,
            subtitle,
            image_data_url: Some(data_url),
            text_head: None,
            note: None,
        });
    }

    if is_previewable_text_ext(&ext) {
        const MAX_TEXT_PREVIEW_BYTES: usize = 16 * 1024;
        let mut file = fs::File::open(path).ok()?;
        let mut buffer = vec![0u8; MAX_TEXT_PREVIEW_BYTES];
        let read = file.read(&mut buffer).ok()?;
        buffer.truncate(read);
        let text = String::from_utf8_lossy(&buffer).to_string();
        return Some(PreviewModel {
            kind: "text".to_string(),
            title: file_name,
            subtitle,
            image_data_url: None,
            text_head: Some(text),
            note: Some("Showing the first 16KB.".to_string()),
        });
    }

    Some(PreviewModel {
        kind: "unknown".to_string(),
        title: file_name,
        subtitle,
        image_data_url: None,
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

fn build_columns(home: &Path, focus_path: Option<&Path>) -> Vec<Column> {
    let components = focus_path
        .filter(|path| path.starts_with(home))
        .map(|path| components_under_home(home, path))
        .unwrap_or_default();
    let mut columns = Vec::new();
    let mut parent_dir = home.to_path_buf();
    let mut depth = 0usize;

    loop {
        let entries = list_directory(&parent_dir);
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

fn ensure_tabs(model: &mut TabsModel, home: &Path) {
    for tab in &mut model.tabs {
        if let Some(path) = tab.focus_path.clone() {
            let normalized = normalize_tab_path(home, Some(&path));
            tab.focus_path = Some(normalized.to_string_lossy().to_string());
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
    let active_focus_path = active_tab
        .and_then(|tab| tab.focus_path.clone())
        .map(PathBuf::from);
    let columns = build_columns(&home, active_focus_path.as_deref());
    let preview = build_preview(active_focus_path.as_deref());
    let active_path_for_new = active_focus_path
        .as_ref()
        .map(|path| path.to_string_lossy().to_string())
        .unwrap_or_else(|| home.to_string_lossy().to_string());
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

    context.insert("root_name", &label_from_path(&home));
    context.insert("root_path", &home.to_string_lossy().to_string());
    context.insert("columns", &columns);
    context.insert("tabs", &tabs);
    context.insert("active_path", &active_path_for_new);
    context.insert("active_tab_id", &model.active_id.to_string());
    context.insert("preview", &preview);

    // context.insert("user_needs_to_setup_biometrics", &user_needs_to_setup_biometrics);
    render!("files/index", &context)
}

pub struct FileContextMenuState {
    pub pending_path: Mutex<Option<String>>,
}

impl Default for FileContextMenuState {
    fn default() -> Self {
        Self {
            pending_path: Mutex::new(None),
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
        tab.focus_path = Some(normalized.to_string_lossy().to_string());
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
pub fn show_file_context_menu(
    window: Window,
    state: State<'_, FileContextMenuState>,
    path: String,
    x: f64,
    y: f64,
) -> Result<(), String> {
    let mut pending = state
        .pending_path
        .lock()
        .map_err(|_| "failed to lock context menu state".to_string())?;
    *pending = Some(path);
    drop(pending);

    let menu = MenuBuilder::new(&window)
        .text("copy_path", "Copy path")
        .build()
        .map_err(|error| error.to_string())?;

    window
        .popup_menu_at(&menu, LogicalPosition::new(x, y))
        .map_err(|error| error.to_string())
}
