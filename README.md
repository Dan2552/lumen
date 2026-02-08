# Lumen

Lumen is a desktop file browser built with Tauri, with Finder-style multi-column navigation, previews, tabs, and context actions. As well as text editor fuzzy file search.

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
