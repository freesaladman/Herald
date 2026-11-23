#!/bin/sh
# Installs or updates Herald on macOS from the latest GitHub release.
#   curl -fsSL https://raw.githubusercontent.com/freesaladman/Herald/main/scripts/install.sh | sh
# HERALD_VERSION (e.g. v0.1.0) picks a release. HERALD_APP_DIR (default /Applications, or
# ~/Applications when that is not writable) and HERALD_BIN_DIR (default ~/.local/bin) pick
# where the app and the herald CLI link go.
set -eu

REPO="freesaladman/Herald"

fail() {
    echo "herald install: $*" >&2
    exit 1
}

[ "$(uname -s)" = "Darwin" ] || fail "this script installs the macOS app; on Windows use install.ps1"

case "$(uname -m)" in
    arm64) arch=aarch64 ;;
    x86_64)
        # An x86_64 shell translated on Apple Silicon still gets the native build.
        if [ "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" = "1" ]; then
            arch=aarch64
        else
            arch=x64
        fi
        ;;
    *) fail "unsupported CPU: $(uname -m)" ;;
esac

tag="${HERALD_VERSION:-}"
if [ -z "$tag" ]; then
    tag=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" |
        sed -n 's/^ *"tag_name": *"\([^"]*\)".*/\1/p' | head -n 1)
    [ -n "$tag" ] || fail "could not find the latest release"
fi
asset="Herald_${tag#v}_${arch}.app.tar.gz"
base="https://github.com/$REPO/releases/download/$tag"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Downloading Herald $tag ($arch)..."
curl -fsSL -o "$tmp/$asset" "$base/$asset" || fail "download failed: $base/$asset"
curl -fsSL -o "$tmp/$asset.sha256" "$base/$asset.sha256" || fail "download failed: $base/$asset.sha256"
expected=$(awk '{ print $1; exit }' "$tmp/$asset.sha256")
actual=$(shasum -a 256 "$tmp/$asset" | awk '{ print $1 }')
[ "$expected" = "$actual" ] || fail "checksum mismatch for $asset"

tar -xzf "$tmp/$asset" -C "$tmp"
[ -d "$tmp/Herald.app" ] || fail "the archive has no Herald.app"

app_dir="${HERALD_APP_DIR:-/Applications}"
if [ -z "${HERALD_APP_DIR:-}" ] && [ ! -w "$app_dir" ]; then
    app_dir="$HOME/Applications"
fi
mkdir -p "$app_dir"
app="$app_dir/Herald.app"

# The app ignores a normal quit request outside its tray menu, so stop it directly.
pkill -x herald-app >/dev/null 2>&1 && sleep 1 || true
rm -rf "$app"
mv "$tmp/Herald.app" "$app"
xattr -dr com.apple.quarantine "$app" 2>/dev/null || true

bin_dir="${HERALD_BIN_DIR:-$HOME/.local/bin}"
mkdir -p "$bin_dir"
ln -sf "$app/Contents/MacOS/herald" "$bin_dir/herald"

echo "Installed $app"
echo "Linked the herald CLI at $bin_dir/herald"
case ":$PATH:" in
    *":$bin_dir:"*) ;;
    *) echo "Add $bin_dir to your PATH to use it: echo 'export PATH=\"$bin_dir:\$PATH\"' >> ~/.zprofile" ;;
esac

open "$app"
echo "Herald is running in the menu bar."
