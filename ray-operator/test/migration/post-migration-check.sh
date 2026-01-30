#!/bin/bash
# post-migration-check.sh
# Verifies RayCluster state after migration:
# 1. CodeFlare-specific items are NOT present (removed)
# 2. Migration annotations are set correctly
# 3. External CodeFlare resources are cleaned up
#
# Usage: ./post-migration-check.sh [namespace]

set -e

NAMESPACE="${1:-test-migration}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=============================================="
echo "Post-Migration Check for RayClusters"
echo "Namespace: $NAMESPACE"
echo "Timestamp: $TIMESTAMP"
echo "=============================================="

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

# Function to log info (no check count)
log_info() {
    local message=$1
    echo -e "       ${message}"
}

# CodeFlare items that should be REMOVED after migration
CODEFLARE_INITCONTAINERS=("create-cert")
CODEFLARE_CONTAINERS=("oauth-proxy")
CODEFLARE_ENV_VARS=("MY_POD_IP" "RAY_USE_TLS" "RAY_TLS_SERVER_CERT" "RAY_TLS_SERVER_KEY" "RAY_TLS_CA_CERT")
CODEFLARE_VOLUMES=("ca-vol" "server-cert" "proxy-tls-secret")
CODEFLARE_VOLUME_MOUNTS=("ca-vol" "server-cert")
CODEFLARE_FINALIZER="ray.openshift.ai/oauth-finalizer"

# Get list of RayClusters in the namespace
RAYCLUSTERS=$(kubectl get rayclusters -n "$NAMESPACE" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")

if [ -z "$RAYCLUSTERS" ]; then
    echo -e "${YELLOW}WARNING: No RayClusters found in namespace '$NAMESPACE'${NC}"
    exit 0
fi

echo ""
echo "RayClusters to verify: $RAYCLUSTERS"
echo ""

# Function to verify RayCluster after migration
verify_raycluster() {
    local name=$1
    
    echo ""
    echo "=============================================="
    echo "Verifying RayCluster: $name"
    echo "=============================================="
    
    # Get current state
    local post_json=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o json 2>/dev/null)
    if [ -z "$post_json" ]; then
        log_check "FAIL" "RayCluster '$name' not found in namespace '$NAMESPACE'"
        return 1
    fi
    
    echo ""
    echo "--- Checking CodeFlare Finalizer ---"
    
    # Check finalizer is removed
    local finalizers=$(echo "$post_json" | jq -r '.metadata.finalizers // [] | join(", ")')
    local has_cf_finalizer=$(echo "$post_json" | jq -r ".metadata.finalizers // [] | index(\"$CODEFLARE_FINALIZER\") != null")
    
    log_info "Current Finalizers: [${finalizers:-(none)}]"
    
    if [ "$has_cf_finalizer" = "true" ]; then
        log_check "FAIL" "CodeFlare finalizer still present (migration not complete)"
    else
        log_check "PASS" "CodeFlare finalizer not present"
    fi
    
    echo ""
    echo "--- Checking Head Group (CodeFlare items should be ABSENT) ---"
    
    # Check head initContainers
    local head_init=$(echo "$post_json" | jq -r '[.spec.headGroupSpec.template.spec.initContainers[].name] // []')
    local head_init_str=$(echo "$head_init" | jq -r 'join(", ")')
    
    echo ""
    echo "InitContainers: [${head_init_str:-(none)}]"
    
    for init in "${CODEFLARE_INITCONTAINERS[@]}"; do
        if echo "$head_init" | jq -e "index(\"$init\")" &>/dev/null; then
            log_check "FAIL" "CodeFlare initContainer '$init' still present"
        else
            log_check "PASS" "CodeFlare initContainer '$init' not present"
        fi
    done
    
    # Check head containers
    local head_containers=$(echo "$post_json" | jq -r '[.spec.headGroupSpec.template.spec.containers[].name] // []')
    local head_containers_str=$(echo "$head_containers" | jq -r 'join(", ")')
    
    echo ""
    echo "Containers: [${head_containers_str:-(none)}]"
    
    for container in "${CODEFLARE_CONTAINERS[@]}"; do
        if echo "$head_containers" | jq -e "index(\"$container\")" &>/dev/null; then
            log_check "FAIL" "CodeFlare container '$container' still present"
        else
            log_check "PASS" "CodeFlare container '$container' not present"
        fi
    done
    
    # Check head env vars
    local head_env=$(echo "$post_json" | jq -r '[.spec.headGroupSpec.template.spec.containers[] | select(.name=="ray-head") | .env[].name] // []')
    local head_env_str=$(echo "$head_env" | jq -r 'join(", ")')
    
    echo ""
    echo "Head Env Vars: [${head_env_str:-(none)}]"
    
    for env in "${CODEFLARE_ENV_VARS[@]}"; do
        if echo "$head_env" | jq -e "index(\"$env\")" &>/dev/null; then
            log_check "FAIL" "CodeFlare env var '$env' still present"
        else
            log_check "PASS" "CodeFlare env var '$env' not present"
        fi
    done
    
    # Check head volumes
    local head_volumes=$(echo "$post_json" | jq -r '[.spec.headGroupSpec.template.spec.volumes[].name] // []')
    local head_volumes_str=$(echo "$head_volumes" | jq -r 'join(", ")')
    
    echo ""
    echo "Head Volumes: [${head_volumes_str:-(none)}]"
    
    for vol in "${CODEFLARE_VOLUMES[@]}"; do
        if echo "$head_volumes" | jq -e "index(\"$vol\")" &>/dev/null; then
            log_check "FAIL" "CodeFlare volume '$vol' still present"
        else
            log_check "PASS" "CodeFlare volume '$vol' not present"
        fi
    done
    
    # Check head volume mounts
    local head_mounts=$(echo "$post_json" | jq -r '[.spec.headGroupSpec.template.spec.containers[] | select(.name=="ray-head") | .volumeMounts[].name] // []')
    local head_mounts_str=$(echo "$head_mounts" | jq -r 'join(", ")')
    
    echo ""
    echo "Head Volume Mounts: [${head_mounts_str:-(none)}]"
    
    for mount in "${CODEFLARE_VOLUME_MOUNTS[@]}"; do
        if echo "$head_mounts" | jq -e "index(\"$mount\")" &>/dev/null; then
            log_check "FAIL" "CodeFlare volume mount '$mount' still present"
        else
            log_check "PASS" "CodeFlare volume mount '$mount' not present"
        fi
    done
    
    # Check head ServiceAccountName
    local head_sa=$(echo "$post_json" | jq -r '.spec.headGroupSpec.template.spec.serviceAccountName // ""')
    
    echo ""
    echo "Head ServiceAccountName: ${head_sa:-(not set)}"
    
    if [[ "$head_sa" == *"-oauth-proxy"* ]]; then
        log_check "FAIL" "CodeFlare ServiceAccountName pattern '*-oauth-proxy' still present"
    else
        log_check "PASS" "No CodeFlare ServiceAccountName pattern"
    fi
    
    # Check worker groups
    echo ""
    echo "--- Checking Worker Groups ---"
    
    local num_workers=$(echo "$post_json" | jq '.spec.workerGroupSpecs | length // 0')
    log_info "Number of worker groups: $num_workers"
    
    for i in $(seq 0 $((num_workers - 1))); do
        local group_name=$(echo "$post_json" | jq -r ".spec.workerGroupSpecs[$i].groupName // \"worker-$i\"")
        echo ""
        echo "Worker Group $i ($group_name):"
        
        # Worker initContainers
        local worker_init=$(echo "$post_json" | jq -r "[.spec.workerGroupSpecs[$i].template.spec.initContainers[].name] // []")
        local worker_init_str=$(echo "$worker_init" | jq -r 'join(", ")')
        
        echo "  InitContainers: [${worker_init_str:-(none)}]"
        
        for init in "${CODEFLARE_INITCONTAINERS[@]}"; do
            if echo "$worker_init" | jq -e "index(\"$init\")" &>/dev/null; then
                log_check "FAIL" "Worker[$i] CodeFlare initContainer '$init' still present"
            else
                log_check "PASS" "Worker[$i] CodeFlare initContainer '$init' not present"
            fi
        done
        
        # Worker env vars
        local worker_env=$(echo "$post_json" | jq -r "[.spec.workerGroupSpecs[$i].template.spec.containers[] | select(.name==\"machine-learning\" or .name==\"ray-worker\") | .env[].name] // []")
        local worker_env_str=$(echo "$worker_env" | jq -r 'join(", ")')
        
        echo "  Env Vars: [${worker_env_str:-(none)}]"
        
        for env in "${CODEFLARE_ENV_VARS[@]}"; do
            if echo "$worker_env" | jq -e "index(\"$env\")" &>/dev/null; then
                log_check "FAIL" "Worker[$i] CodeFlare env var '$env' still present"
            else
                log_check "PASS" "Worker[$i] CodeFlare env var '$env' not present"
            fi
        done
        
        # Worker volumes
        local worker_volumes=$(echo "$post_json" | jq -r "[.spec.workerGroupSpecs[$i].template.spec.volumes[].name] // []")
        local worker_volumes_str=$(echo "$worker_volumes" | jq -r 'join(", ")')
        
        echo "  Volumes: [${worker_volumes_str:-(none)}]"
        
        for vol in "${CODEFLARE_VOLUMES[@]}"; do
            [ "$vol" = "proxy-tls-secret" ] && continue  # head only
            if echo "$worker_volumes" | jq -e "index(\"$vol\")" &>/dev/null; then
                log_check "FAIL" "Worker[$i] CodeFlare volume '$vol' still present"
            else
                log_check "PASS" "Worker[$i] CodeFlare volume '$vol' not present"
            fi
        done
    done
    
    # Check annotations
    echo ""
    echo "--- Checking Annotations ---"
    
    local secure_network=$(echo "$post_json" | jq -r '.metadata.annotations["odh.ray.io/secure-trusted-network"] // "(not set)"')
    local suspend_annotation=$(echo "$post_json" | jq -r '.metadata.annotations["migration.ray.io/suspended-for-migration"] // "(not set)"')
    local cf_version=$(echo "$post_json" | jq -r '.metadata.annotations["ray.openshift.ai/version"] // "(not set)"')
    local suspend_state=$(echo "$post_json" | jq -r '.spec.suspend // false')
    
    log_info "odh.ray.io/secure-trusted-network: $secure_network"
    log_info "ray.openshift.ai/version: $cf_version"
    log_info "migration.ray.io/suspended-for-migration: $suspend_annotation"
    log_info "spec.suspend: $suspend_state"
    
    if [ "$secure_network" = "true" ]; then
        log_check "PASS" "Secure trusted network annotation is set"
    else
        log_check "WARN" "Secure trusted network annotation not set (may be expected)"
    fi
    
    if [ "$cf_version" != "(not set)" ]; then
        log_check "FAIL" "CodeFlare version annotation still present (should be removed)"
    else
        log_check "PASS" "CodeFlare version annotation removed"
    fi
    
    if [ "$suspend_annotation" != "(not set)" ]; then
        log_check "WARN" "Migration suspend annotation still present (mid-migration?)"
    else
        log_check "PASS" "Migration suspend annotation removed"
    fi
    
    if [ "$suspend_state" = "true" ]; then
        log_check "WARN" "Cluster is suspended (may be in mid-migration)"
    else
        log_check "PASS" "Cluster is not suspended"
    fi
}

# Function to check related resources cleanup
check_related_resources_cleanup() {
    local name=$1
    
    echo ""
    echo "--- Checking Related Resources (CodeFlare resources should be ABSENT) ---"
    echo ""
    
    # Check for CodeFlare CA secrets (pattern: ca-secret-{cluster-name})
    # Note: KubeRay creates {cluster-name}-ca-secret-{uid} which is different
    local ca_secrets=$(kubectl get secrets -n "$NAMESPACE" -o name 2>/dev/null | grep -E "^secret/ca-secret-$name$" || true)
    echo "CodeFlare CA Secrets: ${ca_secrets:-(none)}"
    if [ -z "$ca_secrets" ]; then
        log_check "PASS" "No CodeFlare CA secrets"
    else
        log_check "WARN" "CodeFlare CA secrets may still exist"
    fi
    
    # Check for OAuth proxy TLS secrets
    local proxy_secrets=$(kubectl get secrets -n "$NAMESPACE" -o name 2>/dev/null | grep -E "$name.*proxy-tls-secret" || true)
    echo "Proxy TLS Secrets: ${proxy_secrets:-(none)}"
    if [ -z "$proxy_secrets" ]; then
        log_check "PASS" "No OAuth proxy TLS secrets"
    else
        log_check "WARN" "OAuth proxy TLS secrets may still exist"
    fi
    
    # Check for OAuth config secrets
    local oauth_config=$(kubectl get secrets -n "$NAMESPACE" -o name 2>/dev/null | grep -E "$name.*oauth-config" || true)
    echo "OAuth Config Secrets: ${oauth_config:-(none)}"
    if [ -z "$oauth_config" ]; then
        log_check "PASS" "No OAuth config secrets"
    else
        log_check "WARN" "OAuth config secrets may still exist"
    fi
    
    # Check for CodeFlare ServiceAccounts (must have CodeFlare label)
    # Note: KubeRay also creates {cluster}-oauth-proxy-sa but it won't have the CodeFlare label
    local oauth_sa=$(kubectl get serviceaccounts -n "$NAMESPACE" -l "ray.openshift.ai/cluster-name=$name" -o name 2>/dev/null || true)
    echo "CodeFlare OAuth ServiceAccounts: ${oauth_sa:-(none)}"
    if [ -z "$oauth_sa" ]; then
        log_check "PASS" "No CodeFlare OAuth proxy ServiceAccounts"
    else
        log_check "WARN" "CodeFlare OAuth proxy ServiceAccounts may still exist"
    fi
    
    # Show KubeRay-created ServiceAccount (expected to exist)
    local kuberay_sa=$(kubectl get serviceaccounts -n "$NAMESPACE" -o name 2>/dev/null | grep -E "$name.*oauth-proxy-sa$" || true)
    if [ -n "$kuberay_sa" ]; then
        log_info "KubeRay Auth ServiceAccount (expected): $kuberay_sa"
    fi
    
    # Check for OAuth Services
    local oauth_svc=$(kubectl get services -n "$NAMESPACE" -o name 2>/dev/null | grep -E "$name.*-oauth$|$name-oauth" || true)
    echo "OAuth Services: ${oauth_svc:-(none)}"
    if [ -z "$oauth_svc" ]; then
        log_check "PASS" "No OAuth Services"
    else
        log_check "WARN" "OAuth Services may still exist"
    fi
    
    # Check for CodeFlare ClusterRoleBindings
    local crb=$(kubectl get clusterrolebindings -o name 2>/dev/null | grep -E "$name.*-$NAMESPACE.*auth|$name.*auth" || true)
    echo "OAuth ClusterRoleBindings: ${crb:-(none)}"
    if [ -z "$crb" ]; then
        log_check "PASS" "No OAuth ClusterRoleBindings"
    else
        log_check "WARN" "OAuth ClusterRoleBindings may still exist"
    fi
    
    # Check for CodeFlare Routes
    local routes=$(kubectl get routes -n "$NAMESPACE" -o name 2>/dev/null | grep -E "ray-dashboard-$name|rayclient-$name" || true)
    echo "CodeFlare Routes: ${routes:-(none)}"
    if [ -z "$routes" ]; then
        log_check "PASS" "No CodeFlare Routes"
    else
        log_check "WARN" "CodeFlare Routes may still exist"
    fi
    
    # Check for CodeFlare NetworkPolicies (must have CodeFlare label)
    # Note: KubeRay creates {cluster}-head and {cluster}-workers but without CodeFlare label
    local netpols=$(kubectl get networkpolicies -n "$NAMESPACE" -l "ray.openshift.ai/cluster-name=$name" -o name 2>/dev/null || true)
    echo "CodeFlare NetworkPolicies: ${netpols:-(none)}"
    if [ -z "$netpols" ]; then
        log_check "PASS" "No CodeFlare NetworkPolicies"
    else
        log_check "WARN" "CodeFlare NetworkPolicies may still exist"
    fi
    
    # Show KubeRay-created NetworkPolicies (expected to exist)
    local kuberay_netpols=$(kubectl get networkpolicies -n "$NAMESPACE" -l "ray.io/cluster=$name" -o name 2>/dev/null || true)
    if [ -n "$kuberay_netpols" ]; then
        log_info "KubeRay NetworkPolicies (expected): $(echo $kuberay_netpols | tr '\n' ' ')"
    fi
    
    # Show KubeRay resources
    echo ""
    echo "--- KubeRay Resources (should exist after migration) ---"
    
    # Check mTLS Certificates (cert-manager)
    local head_cert=$(kubectl get certificate "ray-head-cert-$name" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    local worker_cert=$(kubectl get certificate "ray-worker-cert-$name" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    local ca_cert=$(kubectl get certificate "ray-ca-certificate-$name" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    
    echo "mTLS Certificates:"
    if [ "$ca_cert" = "True" ]; then
        log_check "PASS" "CA Certificate ready (ray-ca-certificate-$name)"
    elif [ -z "$ca_cert" ]; then
        log_check "WARN" "CA Certificate not found (ray-ca-certificate-$name)"
    else
        log_check "FAIL" "CA Certificate not ready (ray-ca-certificate-$name)"
    fi
    
    if [ "$head_cert" = "True" ]; then
        log_check "PASS" "Head Certificate ready (ray-head-cert-$name)"
    elif [ -z "$head_cert" ]; then
        log_check "WARN" "Head Certificate not found (ray-head-cert-$name)"
    else
        log_check "FAIL" "Head Certificate not ready (ray-head-cert-$name)"
    fi
    
    if [ "$worker_cert" = "True" ]; then
        log_check "PASS" "Worker Certificate ready (ray-worker-cert-$name)"
    elif [ -z "$worker_cert" ]; then
        log_check "WARN" "Worker Certificate not found (ray-worker-cert-$name)"
    else
        log_check "FAIL" "Worker Certificate not ready (ray-worker-cert-$name)"
    fi
    
    # Check mTLS Secrets
    local head_secret=$(kubectl get secret "ray-head-secret-$name" -n "$NAMESPACE" -o name 2>/dev/null || echo "")
    local worker_secret=$(kubectl get secret "ray-worker-secret-$name" -n "$NAMESPACE" -o name 2>/dev/null || echo "")
    
    echo ""
    echo "mTLS Secrets:"
    if [ -n "$head_secret" ]; then
        log_check "PASS" "Head TLS secret exists (ray-head-secret-$name)"
    else
        log_check "FAIL" "Head TLS secret missing (ray-head-secret-$name)"
    fi
    
    if [ -n "$worker_secret" ]; then
        log_check "PASS" "Worker TLS secret exists (ray-worker-secret-$name)"
    else
        log_check "FAIL" "Worker TLS secret missing (ray-worker-secret-$name)"
    fi
    
    # Check Issuers
    local ca_issuer=$(kubectl get issuer "ray-ca-issuer-$name" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    echo ""
    echo "Issuers:"
    if [ "$ca_issuer" = "True" ]; then
        log_check "PASS" "CA Issuer ready (ray-ca-issuer-$name)"
    elif [ -z "$ca_issuer" ]; then
        log_check "WARN" "CA Issuer not found (ray-ca-issuer-$name)"
    else
        log_check "FAIL" "CA Issuer not ready (ray-ca-issuer-$name)"
    fi
    
    echo ""
    local kuberay_secrets=$(kubectl get secrets -n "$NAMESPACE" -l "ray.io/cluster=$name" -o name 2>/dev/null || true)
    echo "All KubeRay Secrets: ${kuberay_secrets:-(none)}"
    
    local kuberay_netpols=$(kubectl get networkpolicies -n "$NAMESPACE" -l "ray.io/cluster=$name" -o name 2>/dev/null || true)
    echo "KubeRay NetworkPolicies: ${kuberay_netpols:-(none)}"
    
    local pods=$(kubectl get pods -n "$NAMESPACE" -l "ray.io/cluster=$name" -o jsonpath='{range .items[*]}{.metadata.name}{" ("}{.status.phase}{")"}{"\n"}{end}' 2>/dev/null || true)
    echo "Pods: ${pods:-(none)}"
}

# Process each RayCluster
for rc in $RAYCLUSTERS; do
    verify_raycluster "$rc"
    check_related_resources_cleanup "$rc"
done

# Summary
echo ""
echo "############################################"
echo "# SUMMARY"
echo "############################################"
echo ""
echo "Timestamp: $TIMESTAMP"
echo "Namespace: $NAMESPACE"
echo ""
echo "RayClusters verified:"
for rc in $RAYCLUSTERS; do
    echo "  - $rc"
done
echo ""
echo "=============================================="
echo "Total checks: $TOTAL_CHECKS"
echo -e "${GREEN}Passed: $PASSED_CHECKS${NC}"
echo -e "${RED}Failed: $FAILED_CHECKS${NC}"
echo -e "${YELLOW}Warnings: $WARNINGS${NC}"
echo "=============================================="
echo ""

# Exit with error if any checks failed
if [ $FAILED_CHECKS -gt 0 ]; then
    echo -e "${RED}Migration verification FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}Migration verification PASSED${NC}"
    exit 0
fi
