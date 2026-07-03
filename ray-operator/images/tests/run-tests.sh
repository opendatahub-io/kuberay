#!/bin/bash

set -o allexport
# shellcheck disable=SC1091
source .env-odh
set +o allexport

# Create results directory if it doesn't exist
mkdir -p results

# Initialize variables
PARSED_TEST_TAGS=""
EXTRA_ARGS=()

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -testTier=VALUE    Specify test tier(s): Tier1, Smoke"
    echo "  -h, -help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -testTier=Tier1"
    echo "  $0 -testTier=Smoke"
    echo "  $0 -testTier=Tier1,Smoke"
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -testTier=*)
            PARSED_TEST_TAGS="${1#*=}"
            shift
            ;;
        -h|-help|--help)
            show_usage
            ;;
        *)
            # Collect unknown arguments to pass to gotestsum
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

# Set TEST_TAGS only from command line arguments
TEST_TAGS="$PARSED_TEST_TAGS"

# If no TEST_TAGS provided via command line, show usage and exit
if [[ -z "$TEST_TAGS" ]]; then
    echo "Info: No test tier specified."
    echo ""
    show_usage
fi

REGEX_PARTS=()
INCLUDE_TIER1=false
TEST_PACKAGES=("./test/e2e")
TEST_TIMEOUT="30m"

IFS=',' read -ra TIER_LIST <<< "$TEST_TAGS"
for TIER in "${TIER_LIST[@]}"; do
    # trim leading/trailing whitespace
    TIER="${TIER#"${TIER%%[![:space:]]*}"}"
    TIER="${TIER%"${TIER##*[![:space:]]}"}"
    [[ -z "$TIER" ]] && continue
    case "$TIER" in
        Tier1)
            echo "Adding Tier1 kuberay e2e tests (including autoscaler)"
            INCLUDE_TIER1=true
            REGEX_PARTS+=("TestRayJobWithClusterSelector|TestRayJob|TestRayJobSuspend|TestRayJobLightWeightMode|TestRayClusterAutoscaler")
            ;;
        Smoke)
            echo "Adding Smoke kuberay e2e tests (authentication validation)"
            REGEX_PARTS+=("TestRayClusterAuthentication")
            ;;
        Sanity)
            echo "Warning: 'Sanity' tier is no longer supported. Use 'Tier1' or 'Smoke'."
            echo ""
            show_usage
            ;;
        *)
            echo "Info: Invalid test tier '$TIER'. Supported tiers: Tier1, Smoke."
            echo ""
            show_usage
            ;;
    esac
done

if [[ ${#REGEX_PARTS[@]} -eq 0 ]]; then
    echo "Error: No valid test tiers resolved."
    show_usage
fi

if [[ "$INCLUDE_TIER1" == true ]]; then
    TEST_PACKAGES+=("./test/e2eautoscaler")
    TEST_TIMEOUT="60m"
fi

TEST_RUN_REGEX="^($(IFS='|'; echo "${REGEX_PARTS[*]}"))$"
echo "Running e2e tests matching: $TEST_RUN_REGEX"
echo "Running packages: ${TEST_PACKAGES[*]}"
echo "Test timeout: $TEST_TIMEOUT"

# Run tests with junit XML output
gotestsum --format standard-verbose --junitfile results/xunit_report.xml --junitfile-testsuite-name short --junitfile-testcase-classname relative -- -timeout "$TEST_TIMEOUT" -run "$TEST_RUN_REGEX" "${TEST_PACKAGES[@]}" -p 1 -parallel 1 "${EXTRA_ARGS[@]}"
