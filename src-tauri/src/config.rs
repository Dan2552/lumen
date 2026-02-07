// pub fn get_vault_path() -> Option<String> {
//     let home = std::env::var("HOME")
//         .expect("HOME environment variable not set");
//     let config_path = format!("{}/.citadel/vault_path", home);
//     let vault_path = std::fs::read_to_string(&config_path).ok();

//     if let Some(vault_path) = vault_path {
//         if Vault::exists(&vault_path) {
//             return Some(vault_path);
//         }
//     }

//     None
// }

// pub fn set_vault_path(path: &str) {
//     let home = std::env::var("HOME")
//         .expect("HOME environment variable not set");
//     let dir = format!("{}/.citadel", home);
//     std::fs::create_dir_all(&dir).unwrap();
//     let config_path = format!("{}/vault_path", dir);
//     std::fs::write(config_path, path).unwrap();
// }

// pub fn unset_vault_path() {
//     let home = std::env::var("HOME")
//         .expect("HOME environment variable not set");
//     let config_path = format!("{}/.citadel/vault_path", home);

//     if !std::path::Path::new(&config_path).exists() {
//         return;
//     }

//     std::fs::remove_file(config_path).unwrap();
// }

// pub fn get_override_shim_path() -> Option<String> {
//     let home = std::env::var("HOME")
//         .expect("HOME environment variable not set");
//     let config_path = format!("{}/.citadel/override_shim_path", home);
//     std::fs::read_to_string(&config_path).ok()
// }

// pub fn get_override_chrome_extension_id() -> Option<String> {
//     let home = std::env::var("HOME")
//         .expect("HOME environment variable not set");
//     let config_path = format!("{}/.citadel/override_chrome_extension_id", home);
//     std::fs::read_to_string(&config_path).ok()
// }
