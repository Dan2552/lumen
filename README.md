# Lumen

Lumen is a desktop file browser built with Tauri, with

* Finder-style multi-column navigation
* File previews
* Tabs
* Drag and drop support
* Text-editor-alike fuzzy file search
* A persistent state so you can pick up where you left off (stored in `~/.lumen/state.json`)

![Lumen screenshot](docs/screenshot.png)

![Lumen screenshot fuzzy search](docs/fuzzy.png)

## Build And Run

### 1. Install prerequisites

- Rust (via [rustup](https://rustup.rs/))
- Tauri CLI:
  - `cargo install tauri-cli`

On macOS you should also have Xcode Command Line Tools installed:

- `xcode-select --install`

### 2. Development build

From the project root:

```bash
cargo tauri dev
```

### 3. Release build

From the project root:

```bash
cargo tauri build
```

Build outputs are generated under:

`src-tauri/target/release/bundle/`
