#!/bin/bash

set -o allexport
# shellcheck disable=SC1091
source .env-odh
set +o allexport

# Create results directory if it doesn't exist
mkdir -p results

TEST_RUN_REGEX=""

if [[ "$TEST_TAGS" == *"Sanity"* && "$TEST_TAGS" == *"Tier1"* ]]; then
    echo "Running Sanity and Tier1 kuberay e2e tests"
    TEST_RUN_REGEX="^(TestRayJobWithClusterSelector|TestRayJob|TestRayJobSuspend|TestRayJobLightWeightMode)$"
elif [[ "$TEST_TAGS" == *"Sanity"* ]]; then
    echo "Running Sanity kuberay e2e tests"
    TEST_RUN_REGEX="^TestRayJobWithClusterSelector$"
elif [[ "$TEST_TAGS" == *"Tier1"* ]]; then
    echo "Running Tier1 kuberay tests"
    TEST_RUN_REGEX="^(TestRayJob|TestRayJobSuspend|TestRayJobLightWeightMode)$"
else
    echo "No valid TEST_TAGS provided (Sanity or Tier1). Exiting."
    exit 1
fi

# Run tests with junit XML output
gotestsum --format testname --junitfile results/xunit_report.xml -- -run "$TEST_RUN_REGEX" ./test/e2erayjob -p 1 -parallel 1 "$@"
