#!/bin/bash
set -euo pipefail

input=$(cat)
file=$(echo "$input" | jq -r '.path // empty')

if [[ -z "$file" || ! -f "$file" || "$file" != *.go ]]; then
  exit 0
fi

if command -v gofumpt &>/dev/null; then
  gofumpt -w "$file"
elif command -v gofmt &>/dev/null; then
  gofmt -s -w "$file"
fi

exit 0
