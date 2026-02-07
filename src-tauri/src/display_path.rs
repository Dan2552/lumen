// pub fn full_to_display(path: &str) -> String {
//     let home = std::env::var("HOME").unwrap_or("".to_string());
//     if path.starts_with(&home) && home.len() > 0 {
//         return path.replacen(&home, "~", 1);
//     } else {
//         return path.to_string();
//     }
// }

// pub fn display_to_full(path: &str) -> String {
//     let home = std::env::var("HOME").unwrap_or("".to_string());
//     if path.starts_with("~") {
//         return path.replacen("~", &home, 1);
//     } else {
//         return path.to_string();
//     }
// }
