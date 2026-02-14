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

## Companion Web UI

On app startup, Lumen now also starts an HTTP companion server so another device on your LAN can control the active desktop window state.

- Default URL: `http://127.0.0.1:44741/companion`
- LAN URL: `http://<this-device-ip>:44741/companion`
- Optional port override: set `LUMEN_COMPANION_PORT`

Companion behavior:

- Portrait: shows 1 column (current depth)
- Landscape: shows 2 columns
- Companion navigation updates the desktop window immediately
- Desktop updates are polled by the companion and reflected in ~1 second
