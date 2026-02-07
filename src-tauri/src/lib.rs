mod config;
mod display_path;
#[macro_use]
mod templates;
mod controllers {
    pub mod file_controller;
}
use controllers::file_controller;
use tauri::{Manager, State};
use tauri_plugin_clipboard_manager::ClipboardExt;

#[tauri::command]
fn root(state: State<'_, file_controller::FileTabsState>) -> String {
    file_controller::index(state)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(file_controller::FileTabsState::default())
        .manage(file_controller::FileContextMenuState::default())
        .setup(|app| {
            file_controller::restore_main_window_state(&app.handle());
            Ok(())
        })
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .on_window_event(|window, event| match event {
            tauri::WindowEvent::Resized(_) | tauri::WindowEvent::Moved(_) => {
                if window.label() == "main" {
                    file_controller::persist_main_window_state(window);
                }
            }
            _ => {}
        })
        .on_menu_event(|app, event| {
            if event.id() == "copy_path" {
                let path_to_copy = {
                    let state = app.state::<file_controller::FileContextMenuState>();
                    state.pending_path.lock().ok().and_then(|pending| pending.clone())
                };

                if let Some(path) = path_to_copy {
                    let _ = app.clipboard().write_text(path);
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            root,
            file_controller::index,
            file_controller::navigate,
            file_controller::activate_tab,
            file_controller::new_tab,
            file_controller::close_tab,
            file_controller::show_file_context_menu,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
