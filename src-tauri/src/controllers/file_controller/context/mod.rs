pub mod bin;
pub mod ruby;
pub mod rust;

use serde::Serialize;
use std::collections::BTreeSet;
use std::path::Path;

#[derive(Debug, Serialize, Clone)]
pub struct ContextCommand {
    pub id: String,
    pub label: String,
    pub command: String,
}

pub fn commands_for_directory(path: &Path) -> Vec<ContextCommand> {
    let mut commands = Vec::new();
    commands.extend(bin::commands_for_directory(path));
    commands.extend(rust::commands_for_directory(path));
    commands.extend(ruby::commands_for_directory(path));

    let mut seen = BTreeSet::new();
    commands
        .into_iter()
        .filter(|command| seen.insert(command.id.clone()))
        .collect()
}
