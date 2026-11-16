#!/usr/bin/env bash
# Build size_probe and size_baseline for a target with the size profile and print a Markdown
# row: target, features, the probe, the baseline and what storage adds.
# Usage: scripts/size_report.sh [TARGET] [--no-vendored]
set -euo pipefail
target=${1:-}
features=(--features sync)
label="vendored OpenSSL"
if [ "${2:-}" = "--no-vendored" ]; then
  features=(--no-default-features --features sync)
  label="system crypto"
fi
args=(--locked --profile size --example size_probe --example size_baseline "${features[@]}")
dir=target
if [ -n "$target" ]; then args+=(--target "$target"); dir="target/$target"; fi
cargo build "${args[@]}" >&2
ext=""
case "$target" in *windows*) ext=".exe" ;; esac
[ -z "$target" ] && [ "${OS:-}" = "Windows_NT" ] && ext=".exe"
size() { wc -c <"$dir/size/examples/$1$ext" | tr -d ' '; }
probe=$(size size_probe); base=$(size size_baseline)
kib() { awk -v b="$1" 'BEGIN { printf "%.0f KiB", b / 1024 }'; }
echo "| ${target:-$(rustc -vV | sed -n 's/^host: //p')} | $label | $(kib "$probe") | $(kib "$base") | $(kib $((probe - base))) |"
