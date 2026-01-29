#!/bin/bash
# post-migration-check.sh
# Compares RayCluster state after migration to verify:
# 1. CodeFlare-specific items were removed
# 2. Custom configurations were preserved
#
# Usage: ./post-migration-check.sh [namespace] [pre-migration-dir] [output-dir]

set -e

NAMESPACE="${1:-test-migration}"
PRE_DIR="${2:-/tmp/migration-check/pre}"
OUTPUT_DIR="${3:-/tmp/migration-check/post}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=============================================="
echo "Post-Migration Check for RayClusters"
echo "Namespace: $NAMESPACE"
echo "Pre-migration data: $PRE_DIR"
echo "Output directory: $OUTPUT_DIR"
echo "Timestamp: $TIMESTAMP"
echo "=============================================="

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Verify pre-migration data exists
if [ ! -d "$PRE_DIR" ]; then
    echo -e "${RED}ERROR: Pre-migration directory '$PRE_DIR' does not exist${NC}"
    echo "Please run pre-migration-check.sh first"
    exit 1
fi

# Track overall test results
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0
WARNINGS=0

# Function to log check result
log_check() {
    local status=$1
    local message=$2
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    
    case $status in
        "PASS")
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
            echo -e "${GREEN}[PASS]${NC} $message"
            ;;
        "FAIL")
            FAILED_CHECKS=$((FAILED_CHECKS + 1))
            echo -e "${RED}[FAIL]${NC} $message"
            ;;
        "WARN")
            WARNINGS=$((WARNINGS + 1))
            echo -e "${YELLOW}[WARN]${NC} $message"
            ;;
    esac
}

# CodeFlare items that should be REMOVED
CODEFLARE_INITCONTAINERS=("create-cert")
CODEFLARE_CONTAINERS=("oauth-proxy")
CODEFLARE_ENV_VARS=("MY_POD_IP" "RAY_USE_TLS" "RAY_TLS_SERVER_CERT" "RAY_TLS_SERVER_KEY" "RAY_TLS_CA_CERT")
CODEFLARE_VOLUMES=("ca-vol" "server-cert" "proxy-tls-secret")
CODEFLARE_VOLUME_MOUNTS=("ca-vol" "server-cert")
CODEFLARE_FINALIZER="ray.openshift.ai/oauth-finalizer"

# SDK items that should be PRESERVED (do not treat as CodeFlare items)
# Note: ODH CA volumes/mounts should be preserved
SDK_PRESERVED_VOLUMES=("odh-trusted-ca-cert" "odh-ca-cert")
SDK_PRESERVED_MOUNTS=("odh-trusted-ca-cert" "odh-ca-cert")

# Container names used by SDK (both should be checked for env vars)
SDK_HEAD_CONTAINER="ray-head"
SDK_WORKER_CONTAINER="machine-learning"
# Legacy worker container name (upstream kuberay)
LEGACY_WORKER_CONTAINER="ray-worker"

# Get list of RayClusters from pre-migration data
RAYCLUSTERS=$(ls "$PRE_DIR"/*.json 2>/dev/null | xargs -n1 basename 2>/dev/null | sed 's/\.json$//' | grep -v "resources" || echo "")

if [ -z "$RAYCLUSTERS" ]; then
    echo -e "${RED}ERROR: No pre-migration RayCluster data found in '$PRE_DIR'${NC}"
    exit 1
fi

echo ""
echo "RayClusters to verify: $RAYCLUSTERS"
echo ""

# Function to check if item exists in JSON array
json_array_contains() {
    local json=$1
    local item=$2
    echo "$json" | jq -e "index(\"$item\")" &>/dev/null
}

# Function to verify RayCluster after migration
verify_raycluster() {
    local name=$1
    local pre_file="$PRE_DIR/${name}.json"
    local post_file="$OUTPUT_DIR/${name}.json"
    local report_file="$OUTPUT_DIR/${name}-report.txt"
    
    echo ""
    echo "=============================================="
    echo "Verifying RayCluster: $name"
    echo "=============================================="
    
    # Get current state
    if ! kubectl get raycluster "$name" -n "$NAMESPACE" -o json > "$post_file" 2>/dev/null; then
        log_check "FAIL" "RayCluster '$name' not found in namespace '$NAMESPACE'"
        return 1
    fi
    
    # Start report
    {
        echo "Migration Verification Report: $name"
        echo "Timestamp: $TIMESTAMP"
        echo "=============================================="
        echo ""
    } > "$report_file"
    
    # Load pre and post JSON
    local pre_json=$(cat "$pre_file")
    local post_json=$(cat "$post_file")
    
    echo ""
    echo "--- Checking CodeFlare Finalizer ---"
    
    # Check finalizer removal
    local had_finalizer=$(echo "$pre_json" | jq -r ".metadata.finalizers // [] | index(\"$CODEFLARE_FINALIZER\") != null")
    local has_finalizer=$(echo "$post_json" | jq -r ".metadata.finalizers // [] | index(\"$CODEFLARE_FINALIZER\") != null")
    
    if [ "$had_finalizer" = "true" ]; then
        if [ "$has_finalizer" = "false" ]; then
            log_check "PASS" "CodeFlare finalizer was removed"
        else
            log_check "FAIL" "CodeFlare finalizer should have been removed but still exists"
        fi
    else
        log_check "PASS" "No CodeFlare finalizer to remove (not a CodeFlare-managed cluster)"
    fi
    
    echo ""
    echo "--- Checking enableIngress ---"
    
    # Check enableIngress
    local pre_ingress=$(echo "$pre_json" | jq -r ".spec.headGroupSpec.enableIngress // false")
    local post_ingress=$(echo "$post_json" | jq -r ".spec.headGroupSpec.enableIngress // false")
    
    if [ "$had_finalizer" = "true" ] && [ "$pre_ingress" = "true" ]; then
        if [ "$post_ingress" = "false" ]; then
            log_check "PASS" "enableIngress was set to false"
        else
            log_check "FAIL" "enableIngress should have been set to false"
        fi
    else
        log_check "PASS" "enableIngress check not applicable"
    fi
    
    echo ""
    echo "--- Checking Head Group ---"
    
    # Check head group initContainers
    local pre_head_init=$(echo "$pre_json" | jq -r '[.spec.headGroupSpec.template.spec.initContainers[].name] // []')
    local post_head_init=$(echo "$post_json" | jq -r '[.spec.headGroupSpec.template.spec.initContainers[].name] // []')
    
    for init in "${CODEFLARE_INITCONTAINERS[@]}"; do
        if echo "$pre_head_init" | jq -e "index(\"$init\")" &>/dev/null; then
            if echo "$post_head_init" | jq -e "index(\"$init\")" &>/dev/null; then
                log_check "FAIL" "Head initContainer '$init' should have been removed"
            else
                log_check "PASS" "Head initContainer '$init' was removed"
            fi
        fi
    done
    
    # Check that custom initContainers are preserved
    local custom_init=$(echo "$pre_head_init" | jq -r '.[]' | grep -v -E "^(create-cert)$" || true)
    for init in $custom_init; do
        if echo "$post_head_init" | jq -e "index(\"$init\")" &>/dev/null; then
            log_check "PASS" "Custom head initContainer '$init' was preserved"
        else
            log_check "FAIL" "Custom head initContainer '$init' should have been preserved but was removed"
        fi
    done
    
    # Check head group containers
    local pre_head_containers=$(echo "$pre_json" | jq -r '[.spec.headGroupSpec.template.spec.containers[].name] // []')
    local post_head_containers=$(echo "$post_json" | jq -r '[.spec.headGroupSpec.template.spec.containers[].name] // []')
    
    for container in "${CODEFLARE_CONTAINERS[@]}"; do
        if echo "$pre_head_containers" | jq -e "index(\"$container\")" &>/dev/null; then
            if echo "$post_head_containers" | jq -e "index(\"$container\")" &>/dev/null; then
                log_check "FAIL" "Head container '$container' should have been removed"
            else
                log_check "PASS" "Head container '$container' was removed"
            fi
        fi
    done
    
    # Check that custom containers are preserved
    local custom_containers=$(echo "$pre_head_containers" | jq -r '.[]' | grep -v -E "^(oauth-proxy)$" || true)
    for container in $custom_containers; do
        if echo "$post_head_containers" | jq -e "index(\"$container\")" &>/dev/null; then
            log_check "PASS" "Custom head container '$container' was preserved"
        else
            log_check "FAIL" "Custom head container '$container' should have been preserved but was removed"
        fi
    done
    
    # Check head group env vars (for ray-head container)
    echo ""
    echo "--- Checking Head Env Vars ---"
    
    local pre_head_env=$(echo "$pre_json" | jq -r '[.spec.headGroupSpec.template.spec.containers[] | select(.name=="ray-head") | .env[].name] // []')
    local post_head_env=$(echo "$post_json" | jq -r '[.spec.headGroupSpec.template.spec.containers[] | select(.name=="ray-head") | .env[].name] // []')
    
    for env in "${CODEFLARE_ENV_VARS[@]}"; do
        if echo "$pre_head_env" | jq -e "index(\"$env\")" &>/dev/null; then
            if echo "$post_head_env" | jq -e "index(\"$env\")" &>/dev/null; then
                log_check "FAIL" "Head env var '$env' should have been removed"
            else
                log_check "PASS" "Head env var '$env' was removed"
            fi
        fi
    done
    
    # Check custom env vars preserved
    local custom_env=$(echo "$pre_head_env" | jq -r '.[]' | grep -v -E "^(MY_POD_IP|RAY_USE_TLS|RAY_TLS_SERVER_CERT|RAY_TLS_SERVER_KEY|RAY_TLS_CA_CERT)$" || true)
    for env in $custom_env; do
        if echo "$post_head_env" | jq -e "index(\"$env\")" &>/dev/null; then
            log_check "PASS" "Custom head env var '$env' was preserved"
        else
            log_check "FAIL" "Custom head env var '$env' should have been preserved but was removed"
        fi
    done
    
    # Check head group volumes
    echo ""
    echo "--- Checking Head Volumes ---"
    
    local pre_head_volumes=$(echo "$pre_json" | jq -r '[.spec.headGroupSpec.template.spec.volumes[].name] // []')
    local post_head_volumes=$(echo "$post_json" | jq -r '[.spec.headGroupSpec.template.spec.volumes[].name] // []')
    
    for vol in "${CODEFLARE_VOLUMES[@]}"; do
        if echo "$pre_head_volumes" | jq -e "index(\"$vol\")" &>/dev/null; then
            if echo "$post_head_volumes" | jq -e "index(\"$vol\")" &>/dev/null; then
                log_check "FAIL" "Head volume '$vol' should have been removed"
            else
                log_check "PASS" "Head volume '$vol' was removed"
            fi
        fi
    done
    
    # Check custom volumes preserved
    local custom_vol=$(echo "$pre_head_volumes" | jq -r '.[]' | grep -v -E "^(ca-vol|server-cert|proxy-tls-secret)$" || true)
    for vol in $custom_vol; do
        if echo "$post_head_volumes" | jq -e "index(\"$vol\")" &>/dev/null; then
            log_check "PASS" "Custom head volume '$vol' was preserved"
        else
            log_check "FAIL" "Custom head volume '$vol' should have been preserved but was removed"
        fi
    done
    
    # Check head ServiceAccountName
    echo ""
    echo "--- Checking Head ServiceAccountName ---"
    
    local pre_head_sa=$(echo "$pre_json" | jq -r '.spec.headGroupSpec.template.spec.serviceAccountName // ""')
    local post_head_sa=$(echo "$post_json" | jq -r '.spec.headGroupSpec.template.spec.serviceAccountName // ""')
    
    if [[ "$pre_head_sa" == *"-oauth-proxy"* ]]; then
        if [ -z "$post_head_sa" ]; then
            log_check "PASS" "CodeFlare ServiceAccountName '$pre_head_sa' was cleared"
        else
            log_check "FAIL" "CodeFlare ServiceAccountName '$pre_head_sa' should have been cleared"
        fi
    elif [ -n "$pre_head_sa" ]; then
        if [ "$pre_head_sa" = "$post_head_sa" ]; then
            log_check "PASS" "Custom ServiceAccountName '$pre_head_sa' was preserved"
        else
            log_check "FAIL" "Custom ServiceAccountName '$pre_head_sa' should have been preserved (now: '$post_head_sa')"
        fi
    fi
    
    # Check worker groups
    echo ""
    echo "--- Checking Worker Groups ---"
    
    local num_workers=$(echo "$pre_json" | jq '.spec.workerGroupSpecs | length // 0')
    
    for i in $(seq 0 $((num_workers - 1))); do
        echo ""
        echo "Worker Group $i:"
        
        # Worker initContainers
        local pre_worker_init=$(echo "$pre_json" | jq -r "[.spec.workerGroupSpecs[$i].template.spec.initContainers[].name] // []")
        local post_worker_init=$(echo "$post_json" | jq -r "[.spec.workerGroupSpecs[$i].template.spec.initContainers[].name] // []")
        
        for init in "${CODEFLARE_INITCONTAINERS[@]}"; do
            if echo "$pre_worker_init" | jq -e "index(\"$init\")" &>/dev/null; then
                if echo "$post_worker_init" | jq -e "index(\"$init\")" &>/dev/null; then
                    log_check "FAIL" "Worker[$i] initContainer '$init' should have been removed"
                else
                    log_check "PASS" "Worker[$i] initContainer '$init' was removed"
                fi
            fi
        done
        
        # Custom worker initContainers
        local custom_worker_init=$(echo "$pre_worker_init" | jq -r '.[]' | grep -v -E "^(create-cert)$" || true)
        for init in $custom_worker_init; do
            if echo "$post_worker_init" | jq -e "index(\"$init\")" &>/dev/null; then
                log_check "PASS" "Custom worker[$i] initContainer '$init' was preserved"
            else
                log_check "FAIL" "Custom worker[$i] initContainer '$init' should have been preserved"
            fi
        done
        
        # Worker env vars (check both "machine-learning" SDK name and "ray-worker" legacy name)
        local pre_worker_env=$(echo "$pre_json" | jq -r "[.spec.workerGroupSpecs[$i].template.spec.containers[] | select(.name==\"machine-learning\" or .name==\"ray-worker\") | .env[].name] // []")
        local post_worker_env=$(echo "$post_json" | jq -r "[.spec.workerGroupSpecs[$i].template.spec.containers[] | select(.name==\"machine-learning\" or .name==\"ray-worker\") | .env[].name] // []")
        
        for env in "${CODEFLARE_ENV_VARS[@]}"; do
            if echo "$pre_worker_env" | jq -e "index(\"$env\")" &>/dev/null; then
                if echo "$post_worker_env" | jq -e "index(\"$env\")" &>/dev/null; then
                    log_check "FAIL" "Worker[$i] env var '$env' should have been removed"
                else
                    log_check "PASS" "Worker[$i] env var '$env' was removed"
                fi
            fi
        done
        
        # Custom worker env vars
        local custom_worker_env=$(echo "$pre_worker_env" | jq -r '.[]' | grep -v -E "^(MY_POD_IP|RAY_USE_TLS|RAY_TLS_SERVER_CERT|RAY_TLS_SERVER_KEY|RAY_TLS_CA_CERT)$" || true)
        for env in $custom_worker_env; do
            if echo "$post_worker_env" | jq -e "index(\"$env\")" &>/dev/null; then
                log_check "PASS" "Custom worker[$i] env var '$env' was preserved"
            else
                log_check "FAIL" "Custom worker[$i] env var '$env' should have been preserved"
            fi
        done
        
        # Worker ServiceAccountName
        local pre_worker_sa=$(echo "$pre_json" | jq -r ".spec.workerGroupSpecs[$i].template.spec.serviceAccountName // \"\"")
        local post_worker_sa=$(echo "$post_json" | jq -r ".spec.workerGroupSpecs[$i].template.spec.serviceAccountName // \"\"")
        
        if [[ "$pre_worker_sa" == *"-oauth-proxy"* ]]; then
            if [ -z "$post_worker_sa" ]; then
                log_check "PASS" "Worker[$i] CodeFlare ServiceAccountName was cleared"
            else
                log_check "FAIL" "Worker[$i] CodeFlare ServiceAccountName should have been cleared"
            fi
        elif [ -n "$pre_worker_sa" ]; then
            if [ "$pre_worker_sa" = "$post_worker_sa" ]; then
                log_check "PASS" "Worker[$i] custom ServiceAccountName '$pre_worker_sa' was preserved"
            else
                log_check "FAIL" "Worker[$i] custom ServiceAccountName '$pre_worker_sa' should have been preserved"
            fi
        fi
    done
    
    # Check annotation added
    echo ""
    echo "--- Checking Migration Annotation ---"
    
    if [ "$had_finalizer" = "true" ]; then
        local has_annotation=$(echo "$post_json" | jq -r '.metadata.annotations["odh.ray.io/secure-trusted-network"] // ""')
        if [ "$has_annotation" = "true" ]; then
            log_check "PASS" "Migration annotation 'odh.ray.io/secure-trusted-network' was added"
        else
            log_check "WARN" "Migration annotation 'odh.ray.io/secure-trusted-network' not found (may be expected)"
        fi
    fi
    
    echo ""
    echo "Report saved to: $report_file"
}

# Function to check related resources cleanup
check_related_resources_cleanup() {
    local name=$1
    
    echo ""
    echo "--- Checking Related Resources Cleanup ---"
    
    # Check for CodeFlare CA secrets
    local ca_secrets=$(kubectl get secrets -n "$NAMESPACE" -o name 2>/dev/null | grep -E "$name.*ca-secret" || true)
    if [ -z "$ca_secrets" ]; then
        log_check "PASS" "CodeFlare CA secrets were cleaned up"
    else
        log_check "WARN" "Some CodeFlare CA secrets may still exist: $ca_secrets"
    fi
    
    # Check for OAuth proxy secrets
    local proxy_secrets=$(kubectl get secrets -n "$NAMESPACE" -o name 2>/dev/null | grep -E "$name.*proxy-tls" || true)
    if [ -z "$proxy_secrets" ]; then
        log_check "PASS" "OAuth proxy TLS secrets were cleaned up"
    else
        log_check "WARN" "Some OAuth proxy TLS secrets may still exist: $proxy_secrets"
    fi
    
    # Check for CodeFlare ServiceAccounts
    local oauth_sa=$(kubectl get serviceaccounts -n "$NAMESPACE" -o name 2>/dev/null | grep -E "$name.*oauth-proxy" || true)
    if [ -z "$oauth_sa" ]; then
        log_check "PASS" "OAuth proxy ServiceAccounts were cleaned up"
    else
        log_check "WARN" "Some OAuth proxy ServiceAccounts may still exist: $oauth_sa"
    fi
    
    # Check for CodeFlare ClusterRoleBindings
    local crb=$(kubectl get clusterrolebindings -o name 2>/dev/null | grep -E "$name.*oauth" || true)
    if [ -z "$crb" ]; then
        log_check "PASS" "OAuth ClusterRoleBindings were cleaned up"
    else
        log_check "WARN" "Some OAuth ClusterRoleBindings may still exist: $crb"
    fi
}

# Process each RayCluster
for rc in $RAYCLUSTERS; do
    verify_raycluster "$rc"
    check_related_resources_cleanup "$rc"
done

# Summary
echo ""
echo "=============================================="
echo "Migration Verification Summary"
echo "=============================================="
echo ""
echo "Total checks: $TOTAL_CHECKS"
echo -e "${GREEN}Passed: $PASSED_CHECKS${NC}"
echo -e "${RED}Failed: $FAILED_CHECKS${NC}"
echo -e "${YELLOW}Warnings: $WARNINGS${NC}"
echo ""

# Create summary file
SUMMARY="$OUTPUT_DIR/summary.txt"
{
    echo "Post-Migration Verification Summary"
    echo "Timestamp: $TIMESTAMP"
    echo "Namespace: $NAMESPACE"
    echo ""
    echo "Total checks: $TOTAL_CHECKS"
    echo "Passed: $PASSED_CHECKS"
    echo "Failed: $FAILED_CHECKS"
    echo "Warnings: $WARNINGS"
    echo ""
    echo "RayClusters verified:"
    for rc in $RAYCLUSTERS; do
        echo "  - $rc"
    done
} > "$SUMMARY"

echo "Results saved to: $OUTPUT_DIR"
echo ""

# Exit with error if any checks failed
if [ $FAILED_CHECKS -gt 0 ]; then
    echo -e "${RED}Migration verification FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}Migration verification PASSED${NC}"
    exit 0
fi
