#!/usr/bin/env bash
# Regenerate icons from 1024x1024.png using ImageMagick
# - Resizes all recognizable PNGs in this directory based on their filenames
# - Rebuilds macOS .icns via iconutil
# - Rebuilds .ico with multiple sizes
#
# Requirements:
# - ImageMagick (convert)
# - macOS iconutil (for .icns)
#
# Usage:
#   ./generate_image.sh
#
# Notes:
# - This script overwrites existing generated files.
# - It only resizes files whose target dimensions can be inferred from the filename.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${DIR}/1024x1024.png"

if [[ ! -f "$SRC" ]]; then
  echo "Error: Source image not found: $SRC"
  exit 1
fi

# Helper: resize from SRC to WxH and write to OUT
resize_to() {
  local w="$1"
  local h="$2"
  local out="$3"
  mkdir -p "$(dirname "$out")"
  echo "Generating $(basename "$out") (${w}x${h})"
  magick "$SRC" -resize "${w}x${h}" -alpha on -background none -define png:color-type=6 "$out"
}

# Parse size from filename patterns and generate the target
process_png() {
  local file="$1"
  local base="$(basename "$file")"

  # Match e.g. 128x128.png, 32x32.png
  if [[ "$base" =~ ^([0-9]+)x([0-9]+)\.png$ ]]; then
    local w="${BASH_REMATCH[1]}"
    local h="${BASH_REMATCH[2]}"
    resize_to "$w" "$h" "$file"
    return 0
  fi

  # Match e.g. 128x128@2x.png
  if [[ "$base" =~ ^([0-9]+)x([0-9]+)@2x\.png$ ]]; then
    local w=$(( ${BASH_REMATCH[1]} * 2 ))
    local h=$(( ${BASH_REMATCH[2]} * 2 ))
    resize_to "$w" "$h" "$file"
    return 0
  fi

  # Match e.g. Square150x150Logo.png (Windows tile assets)
  if [[ "$base" =~ ^Square([0-9]+)x([0-9]+)Logo\.png$ ]]; then
    local w="${BASH_REMATCH[1]}"
    local h="${BASH_REMATCH[2]}"
    resize_to "$w" "$h" "$file"
    return 0
  fi

  # StoreLogo.png (common default 50x50)
  if [[ "$base" == "StoreLogo.png" ]]; then
    local w="50"
    local h="50"
    resize_to "$w" "$h" "$file"
    return 0
  fi

  # icon.png (commonly 1024x1024 or app-defined main icon)
  if [[ "$base" == "icon.png" ]]; then
    # Use full size source as-is
    cp -f "$SRC" "$file"
    echo "Copied icon.png from source (1024x1024)"
    return 0
  fi

  # If we get here, we couldn't infer a size; skip.
  echo "Skipping unrecognized PNG filename: $base"
  return 1
}

# 1) Regenerate all recognizable PNGs in this directory (except the source).
echo "Regenerating PNG assets from $(basename "$SRC") ..."
shopt -s nullglob
for png in "${DIR}"/*.png; do
  if [[ "$png" == "$SRC" ]]; then
    continue
  fi
  process_png "$png" || true
done

# 2) Rebuild macOS .icns
# Create icon.iconset with the standard sizes and then build icon.icns.
ICONSET_DIR="${DIR}/icon.iconset"
ICNS_OUT="${DIR}/icon.icns"

echo "Rebuilding macOS .icns ..."
rm -rf "$ICONSET_DIR"
mkdir -p "$ICONSET_DIR"

# Standard macOS iconset sizes
sizes=(16 32 64 128 256 512 1024)
for sz in "${sizes[@]}"; do
  # 1x
  magick "$SRC" -resize "${sz}x${sz}" -alpha on -background none -define png:color-type=6 "${ICONSET_DIR}/icon_${sz}x${sz}.png"
  # 2x
  magick "$SRC" -resize "$((sz*2))x$((sz*2))" -alpha on -background none -define png:color-type=6 "${ICONSET_DIR}/icon_${sz}x${sz}@2x.png"
done

# Build the .icns (requires iconutil on macOS)
if command -v iconutil >/dev/null 2>&1; then
  iconutil -c icns "$ICONSET_DIR" -o "$ICNS_OUT"
  echo "Built $(basename "$ICNS_OUT")"
else
  echo "Warning: iconutil not found; skipping .icns build. The iconset has been generated at: $ICONSET_DIR"
fi

# 3) Rebuild .ico with multiple sizes
ICO_OUT="${DIR}/icon.ico"
echo "Rebuilding .ico ..."
# Use ImageMagick's auto-resize to embed standard sizes in a single .ico
# Includes common sizes: 16, 24, 32, 48, 64, 128, 256
magick "$SRC" -alpha on -define icon:auto-resize=16,24,32,48,64,128,256 "$ICO_OUT"
echo "Built $(basename "$ICO_OUT")"

echo "Done."