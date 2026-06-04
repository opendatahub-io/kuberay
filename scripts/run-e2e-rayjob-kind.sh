#!/usr/bin/env bash
# Run PR-subset RayJob e2e tests on Kind, one test function per invocation.
# Intended for 2-CPU / 7GB runners where running all tests in a single go test
# overloads the cluster (TestRayJobWithClusterSelector keeps a RayCluster alive
# while t.Parallel subtests allow other top-level tests to run concurrently).
#
# Usage (from repo root or any directory):
#   ./scripts/run-e2e-rayjob-kind.sh
#   ./scripts/run-e2e-rayjob-kind.sh --test TestRayJob
#   SKIP_DEPLOY=1 ./scripts/run-e2e-rayjob-kind.sh
#
# Environment variables:
#   KIND_CLUSTER_NAME   Kind cluster name (default: kind)
#   IMG                 Operator image (default: kuberay/operator:nightly)
#   ENGINE              docker or podman (default: docker)
#   SKIP_BUILD          Skip operator image build when set to 1
#   SKIP_DEPLOY         Skip Kind setup and operator deploy when set to 1
#   KUBERAY_TEST_*      Passed through to go test (see ray-operator/test/support)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RAY_OPERATOR="${REPO_ROOT}/ray-operator"
SUPPORT_GO="${RAY_OPERATOR}/test/support/support.go"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
IMG="${IMG:-kuberay/operator:nightly}"
ENGINE="${ENGINE:-docker}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_DEPLOY="${SKIP_DEPLOY:-0}"

export KUBERAY_TEST_TIMEOUT_SHORT="${KUBERAY_TEST_TIMEOUT_SHORT:-5m}"
export KUBERAY_TEST_TIMEOUT_MEDIUM="${KUBERAY_TEST_TIMEOUT_MEDIUM:-12m}"
export KUBERAY_TEST_TIMEOUT_LONG="${KUBERAY_TEST_TIMEOUT_LONG:-15m}"
export KUBERAY_TEST_RAY_IMAGE="${KUBERAY_TEST_RAY_IMAGE:-rayproject/ray:2.52.1}"

PR_RAYJOB_TESTS=(
  TestRayJob
  TestRayJobSuspend
  TestRayJobLightWeightMode
  TestRayJobWithClusterSelector
)

SELECTED_TEST=""
JSON_OUTPUT=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --test)
      SELECTED_TEST="$2"
      shift 2
      ;;
    --skip-deploy)
      SKIP_DEPLOY=1
      shift
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --json)
      JSON_OUTPUT=1
      shift
      ;;
    -h|--help)
      sed -n '2,20p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

restore_support_go() {
  if [[ -n "${SUPPORT_BACKUP:-}" && -f "${SUPPORT_BACKUP}" ]]; then
    mv -f "${SUPPORT_BACKUP}" "${SUPPORT_GO}"
  fi
}

require_kind_cluster() {
  if ! kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
    echo "Kind cluster '${KIND_CLUSTER_NAME}' not found. Create one with:" >&2
    echo "  kind create cluster --name ${KIND_CLUSTER_NAME} --image kindest/node:v1.29.0" >&2
    exit 1
  fi
}

deploy_operator() {
  require_kind_cluster

  if [[ "${SKIP_BUILD}" != "1" ]]; then
    echo "--- Building operator image ${IMG} ---"
    make -C "${RAY_OPERATOR}" docker-build IMG="${IMG}" ENGINE="${ENGINE}"
  fi

  echo "--- Loading image into Kind (${KIND_CLUSTER_NAME}) ---"
  kind load docker-image "${IMG}" --name "${KIND_CLUSTER_NAME}"

  echo "--- Deploying operator ---"
  make -C "${RAY_OPERATOR}" deploy IMG="${IMG}"
  kubectl wait --timeout=90s --for=condition=Available=true deployment -n default kuberay-operator
}

patch_test_resources() {
  echo "--- Patching e2e resource requests (scenario: pr) ---"
  SUPPORT_BACKUP="${SUPPORT_GO}.run-e2e-rayjob-kind.bak"
  cp "${SUPPORT_GO}" "${SUPPORT_BACKUP}"
  trap restore_support_go EXIT

  (
    cd "${SCRIPT_DIR}"
    go build -o update-resources update-resources.go
    ./update-resources -scenario pr "${SUPPORT_GO}"
  )
}

run_tests() {
  local tests=()
  if [[ -n "${SELECTED_TEST}" ]]; then
    tests=("${SELECTED_TEST}")
  else
    tests=("${PR_RAYJOB_TESTS[@]}")
  fi

  cd "${RAY_OPERATOR}"
  for test_name in "${tests[@]}"; do
    echo "--- Running ${test_name} ---"
    if [[ "${JSON_OUTPUT}" == "1" ]]; then
      go test -timeout 120m -p 1 -parallel 1 -count=1 -v \
        -run "^${test_name}$" \
        ./test/e2erayjob -json 2>&1 | tee "./gotest-${test_name}.log" | gotestfmt
    else
      go test -timeout 120m -p 1 -parallel 1 -count=1 -v \
        -run "^${test_name}$" \
        ./test/e2erayjob
    fi
  done
}

main() {
  if [[ "${SKIP_DEPLOY}" != "1" ]]; then
    deploy_operator
  else
    require_kind_cluster
  fi

  patch_test_resources
  run_tests
  echo "--- All selected RayJob e2e tests passed ---"
}

main
