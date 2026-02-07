mod config;
mod display_path;
#[macro_use]
mod templates;
mod controllers {
    pub mod file_controller;
}
use controllers::file_controller;
use std::collections::BTreeSet;
use std::sync::Mutex;
use serde::Serialize;
use tauri::{
    AppHandle, Emitter, Manager, PhysicalPosition, Position, State, WebviewUrl,
    WebviewWindowBuilder,
};
use tauri_plugin_clipboard_manager::ClipboardExt;

#[tauri::command]
fn root(state: State<'_, file_controller::FileTabsState>) -> String {
    file_controller::index(state)
}

#[derive(Default)]
struct HoldState {
    paths: Mutex<Vec<String>>,
    dragging_window_labels: Mutex<BTreeSet<String>>,
}

#[derive(Serialize, Clone)]
struct FilesContextActionEvent {
    action: String,
    path: String,
}

fn ensure_hold_window(app: &AppHandle) -> Result<(), tauri::Error> {
    if app.get_webview_window("hold").is_some() {
        return Ok(());
    }

    WebviewWindowBuilder::new(app, "hold", WebviewUrl::App("hold.html".into()))
        .title("Hold")
        .inner_size(260.0, 34.0)
        .min_inner_size(220.0, 34.0)
        .decorations(false)
        .resizable(false)
        .always_on_top(true)
        .skip_taskbar(true)
        .build()?;

    if let Some(window) = app.get_webview_window("hold") {
        if let Ok(Some(monitor)) = app.primary_monitor() {
            let monitor_size = monitor.size();
            let monitor_pos = monitor.position();
            let width: i32 = window
                .outer_size()
                .map(|size| size.width as i32)
                .unwrap_or(260);
            let x = monitor_pos.x + ((monitor_size.width as i32 - width) / 2);
            let y = monitor_pos.y + 6;
            let _ = window.set_position(Position::Physical(PhysicalPosition::new(x, y)));
        }
        let _ = window.hide();
    }

    Ok(())
}

fn should_show_hold_window(app: &AppHandle) -> bool {
    let state = app.state::<HoldState>();
    let has_items = state.paths.lock().map(|paths| !paths.is_empty()).unwrap_or(false);
    let has_drag_hover = state
        .dragging_window_labels
        .lock()
        .map(|labels| !labels.is_empty())
        .unwrap_or(false);
    has_items || has_drag_hover
}

fn update_hold_window_visibility(app: &AppHandle) {
    let Some(window) = app.get_webview_window("hold") else {
        return;
    };
    if should_show_hold_window(app) {
        let _ = window.show();
    } else {
        let _ = window.hide();
    }
}

#[tauri::command]
fn is_alt_pressed() -> bool {
    #[cfg(target_os = "macos")]
    unsafe {
        use objc::{class, msg_send, sel, sel_impl};
        // NSEventModifierFlagOption
        const OPTION_FLAG: u64 = 1 << 19;
        let flags: u64 = msg_send![class!(NSEvent), modifierFlags];
        return (flags & OPTION_FLAG) != 0;
    }

    #[cfg(not(target_os = "macos"))]
    {
        false
    }
}

#[tauri::command]
fn hold_get_items(state: State<'_, HoldState>) -> Vec<String> {
    state.paths.lock().map(|paths| paths.clone()).unwrap_or_default()
}

#[tauri::command]
fn hold_set_items(app: AppHandle, state: State<'_, HoldState>, paths: Vec<String>) -> Vec<String> {
    let Ok(mut current) = state.paths.lock() else {
        return Vec::new();
    };
    let mut unique = BTreeSet::new();
    for path in paths {
        if !path.is_empty() {
            unique.insert(path);
        }
    }
    *current = unique.into_iter().collect();
    let result = current.clone();
    drop(current);
    update_hold_window_visibility(&app);
    result
}

#[tauri::command]
fn hold_add_items(app: AppHandle, state: State<'_, HoldState>, paths: Vec<String>) -> Vec<String> {
    let Ok(mut current) = state.paths.lock() else {
        return Vec::new();
    };
    let mut unique: BTreeSet<String> = current.iter().cloned().collect();
    for path in paths {
        if !path.is_empty() {
            unique.insert(path);
        }
    }
    *current = unique.into_iter().collect();
    let result = current.clone();
    drop(current);
    update_hold_window_visibility(&app);
    result
}

#[tauri::command]
fn hold_clear_items(app: AppHandle, state: State<'_, HoldState>) {
    if let Ok(mut current) = state.paths.lock() {
        current.clear();
    }
    update_hold_window_visibility(&app);
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(file_controller::FileTabsState::default())
        .manage(file_controller::FileContextMenuState::default())
        .manage(HoldState::default())
        .setup(|app| {
            file_controller::restore_main_window_state(&app.handle());
            let _ = ensure_hold_window(&app.handle());
            update_hold_window_visibility(&app.handle());
            Ok(())
        })
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_drag::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .on_window_event(|window, event| match event {
            tauri::WindowEvent::Resized(_) | tauri::WindowEvent::Moved(_) => {
                if window.label() == "main" {
                    file_controller::persist_main_window_state(window);
                }
            }
            tauri::WindowEvent::DragDrop(drag_event) => {
                let app = window.app_handle();
                let state = app.state::<HoldState>();
                if let Ok(mut labels) = state.dragging_window_labels.lock() {
                    match drag_event {
                        tauri::DragDropEvent::Enter { .. } => {
                            labels.insert(window.label().to_string());
                        }
                        tauri::DragDropEvent::Leave | tauri::DragDropEvent::Drop { .. } => {
                            labels.remove(window.label());
                        }
                        tauri::DragDropEvent::Over { .. } => {}
                        _ => {}
                    }
                }
                update_hold_window_visibility(&app);
            }
            _ => {}
        })
        .on_menu_event(|app, event| {
            let path_to_copy = {
                let state = app.state::<file_controller::FileContextMenuState>();
                state.pending.lock().ok().and_then(|pending| pending.clone())
            };
            let Some(pending) = path_to_copy else {
                return;
            };
            let path = pending.path;
            let relative_path = pending.relative_path;

            let menu_id = event.id().as_ref();
            if menu_id == "copy_absolute_path" {
                let _ = app.clipboard().write_text(path);
                return;
            }
            if menu_id == "copy_relative_path" {
                let _ = app.clipboard().write_text(relative_path);
                return;
            }

            if menu_id == "ctx_rename"
                || menu_id == "ctx_trash"
                || menu_id == "ctx_delete"
                || menu_id == "ctx_new_dir"
                || menu_id == "ctx_new_file"
                || menu_id == "ctx_set_tab_root"
                || menu_id == "ctx_open_zed"
                || menu_id == "ctx_open_warp"
            {
                let action = match menu_id {
                    "ctx_rename" => "rename",
                    "ctx_trash" => "trash",
                    "ctx_delete" => "delete",
                    "ctx_new_dir" => "new_dir",
                    "ctx_new_file" => "new_file",
                    "ctx_set_tab_root" => "set_tab_root",
                    "ctx_open_zed" => "open_zed",
                    "ctx_open_warp" => "open_warp",
                    _ => return,
                };
                let _ = app.emit(
                    "files-context-action",
                    FilesContextActionEvent {
                        action: action.to_string(),
                        path,
                    },
                );
                return;
            }
        })
        .invoke_handler(tauri::generate_handler![
            root,
            is_alt_pressed,
            hold_get_items,
            hold_set_items,
            hold_add_items,
            hold_clear_items,
            file_controller::index,
            file_controller::navigate,
            file_controller::activate_tab,
            file_controller::new_tab,
            file_controller::close_tab,
            file_controller::drop_files_into_directory,
            file_controller::show_file_context_menu,
            file_controller::rename_path,
            file_controller::trash_path,
            file_controller::delete_path,
            file_controller::create_directory,
            file_controller::create_file,
            file_controller::set_tab_root,
            file_controller::open_in_zed,
            file_controller::open_in_warp,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
