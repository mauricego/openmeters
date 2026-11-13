#!/bin/sh
set -eu

repo_root=$(CDPATH= cd "$(dirname "$0")/../.." && pwd)
app_path=${1:-"$repo_root/target/OpenMeters.app"}
version=$(awk '/^\[package\]/{package=1;next} /^\[/{package=0} package && /^version[[:space:]]*=/{gsub(/"/, ""); print $3; exit}' "$repo_root/Cargo.toml")

cargo build --manifest-path "$repo_root/Cargo.toml" --release
mkdir -p "$app_path/Contents/MacOS"
cp "$repo_root/target/release/openmeters" "$app_path/Contents/MacOS/OpenMeters"
cp "$repo_root/packaging/macos/Info.plist" "$app_path/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString $version" "$app_path/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleVersion $version" "$app_path/Contents/Info.plist"
codesign --force --deep --sign - "$app_path"

printf 'Created %s\n' "$app_path"
