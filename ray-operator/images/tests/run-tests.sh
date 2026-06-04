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

# Tier definitions: each tier maps to Go package paths and a test-name regex fragment.
declare -A TIER_PACKAGES TIER_REGEX

TIER_PACKAGES[Tier1]="./test/e2erayjob"
TIER_REGEX[Tier1]="TestRayJobWithClusterSelector|TestRayJob|TestRayJobSuspend|TestRayJobLightWeightMode"

TIER_PACKAGES[Smoke]="./test/e2e"
TIER_REGEX[Smoke]="TestRayClusterAuthOptions"

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
    echo ""
    echo "Each tier runs tests from explicit package paths; combined tiers produce one JUnit report."
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
TEST_PACKAGES=()
SELECTED_TIERS=()

IFS=',' read -ra TIER_LIST <<< "$TEST_TAGS"
for TIER in "${TIER_LIST[@]}"; do
    # trim leading/trailing whitespace
    TIER="${TIER#"${TIER%%[![:space:]]*}"}"
    TIER="${TIER%"${TIER##*[![:space:]]}"}"
    [[ -z "$TIER" ]] && continue
    case "$TIER" in
        Tier1|Smoke)
            echo "Adding $TIER kuberay e2e tests (packages: ${TIER_PACKAGES[$TIER]})"
            REGEX_PARTS+=("${TIER_REGEX[$TIER]}")
            SELECTED_TIERS+=("$TIER")
            read -ra tier_pkgs <<< "${TIER_PACKAGES[$TIER]}"
            for pkg in "${tier_pkgs[@]}"; do
                [[ -z "$pkg" ]] && continue
                already_added=false
                for existing in "${TEST_PACKAGES[@]}"; do
                    if [[ "$existing" == "$pkg" ]]; then
                        already_added=true
                        break
                    fi
                done
                if [[ "$already_added" == false ]]; then
                    TEST_PACKAGES+=("$pkg")
                fi
            done
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

if [[ ${#TEST_PACKAGES[@]} -eq 0 ]]; then
    echo "Error: No test packages resolved for selected tiers."
    show_usage
fi

TEST_RUN_REGEX="^($(IFS='|'; echo "${REGEX_PARTS[*]}"))$"

echo "Selected tiers: ${SELECTED_TIERS[*]}"
echo "Test packages: ${TEST_PACKAGES[*]}"
echo "Running e2e tests matching: $TEST_RUN_REGEX"

# Run tests with junit XML output (single invocation across all packages)
gotestsum --format standard-verbose --junitfile results/xunit_report.xml --junitfile-testsuite-name short --junitfile-testcase-classname relative -- -timeout 30m -run "$TEST_RUN_REGEX" "${TEST_PACKAGES[@]}" -p 1 -parallel 1 "${EXTRA_ARGS[@]}"
