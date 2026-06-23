#!/usr/bin/env bash

set -euo pipefail

# Run govulncheck at each Go module root.
# Root module covers: apiserver, kubectl-plugin, apiserversdk
# ray-operator has its own module.
module_roots=". ray-operator"

for dir in $module_roots; do
  echo "Running govulncheck in ${dir}..."
  pushd "$dir"
  govulncheck ./...
  popd
done
