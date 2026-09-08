#!/bin/bash
# pre-migration-check.sh
# Displays the current state of RayClusters to verify CodeFlare-operator configuration
#
# Usage: ./pre-migration-check.sh [namespace]
#
# This script checks for CodeFlare-operator injected items:
# - oauth-proxy sidecar container
# - create-cert initContainer
# - TLS environment variables (RAY_USE_TLS, etc.)
# - ca-vol, server-cert, proxy-tls-secret volumes
# - CodeFlare finalizer
# - Related external resources

set -e

NAMESPACE="${1:-test-migration}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

echo "=============================================="
echo "Pre-Migration Check for RayClusters"
echo "Namespace: $NAMESPACE"
echo "Timestamp: $TIMESTAMP"
echo "=============================================="

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" &>/dev/null; then
    echo "ERROR: Namespace '$NAMESPACE' does not exist"
    exit 1
fi

# Get list of RayClusters
RAYCLUSTERS=$(kubectl get rayclusters -n "$NAMESPACE" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")

if [ -z "$RAYCLUSTERS" ]; then
    echo "WARNING: No RayClusters found in namespace '$NAMESPACE'"
    exit 0
fi

echo ""
echo "Found RayClusters: $RAYCLUSTERS"
echo ""

# Function to extract and display RayCluster details
extract_raycluster_details() {
    local name=$1
    
    echo ""
    echo "Processing RayCluster: $name"
    echo ""
    
    {
        echo "RayCluster: $name"
        echo "Captured at: $TIMESTAMP"
        echo "=============================================="
        echo ""
        
        # ============================================
        # CODEFLARE-SPECIFIC ITEMS CHECK
        # ============================================
        echo "=== CodeFlare-Operator Detection ==="
        echo ""
        
        # Check for CodeFlare finalizer
        local all_finalizers=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.metadata.finalizers[*]}' 2>/dev/null || echo "")
        local has_cf_finalizer=$(echo "$all_finalizers" | grep -c "ray.openshift.ai/oauth-finalizer" || echo "0")
        echo "CodeFlare Finalizer (ray.openshift.ai/oauth-finalizer): $([ "$has_cf_finalizer" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  All Finalizers: ${all_finalizers:-(none)}"
        
        # Check for CodeFlare version annotation
        local cf_version=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.metadata.annotations.ray\.openshift\.ai/version}' 2>/dev/null || echo "")
        local has_cf_version=$([ -n "$cf_version" ] && echo "1" || echo "0")
        echo "CodeFlare Version Annotation (ray.openshift.ai/version): $([ "$has_cf_version" -gt 0 ] && echo "PRESENT ✓ (value: $cf_version)" || echo "NOT PRESENT")"
        echo ""
        
        echo "--- Head Group CodeFlare Items ---"
        
        # Check for oauth-proxy sidecar
        local head_containers=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.spec.headGroupSpec.template.spec.containers[*].name}' 2>/dev/null || echo "")
        local has_oauth_proxy=$(echo "$head_containers" | grep -c "oauth-proxy" || echo "0")
        echo "OAuth Proxy Sidecar: $([ "$has_oauth_proxy" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  All Head Containers: ${head_containers:-(none)}"
        
        # Check for create-cert initContainer (head)
        local head_init=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.spec.headGroupSpec.template.spec.initContainers[*].name}' 2>/dev/null || echo "")
        local has_create_cert_head=$(echo "$head_init" | grep -c "create-cert" || echo "0")
        echo "create-cert InitContainer: $([ "$has_create_cert_head" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  All Head InitContainers: ${head_init:-(none)}"
        
        # Check for CodeFlare TLS env vars
        local head_env=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.spec.headGroupSpec.template.spec.containers[0].env[*].name}' 2>/dev/null || echo "")
        local has_ray_use_tls=$(echo "$head_env" | grep -c "RAY_USE_TLS" || echo "0")
        local has_my_pod_ip=$(echo "$head_env" | grep -c "MY_POD_IP" || echo "0")
        local has_tls_cert=$(echo "$head_env" | grep -c "RAY_TLS_SERVER_CERT" || echo "0")
        local has_tls_key=$(echo "$head_env" | grep -c "RAY_TLS_SERVER_KEY" || echo "0")
        local has_tls_ca=$(echo "$head_env" | grep -c "RAY_TLS_CA_CERT" || echo "0")
        echo "CodeFlare Env Vars:"
        echo "  MY_POD_IP: $([ "$has_my_pod_ip" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  RAY_USE_TLS: $([ "$has_ray_use_tls" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  RAY_TLS_SERVER_CERT: $([ "$has_tls_cert" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  RAY_TLS_SERVER_KEY: $([ "$has_tls_key" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  RAY_TLS_CA_CERT: $([ "$has_tls_ca" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  All Head Env Vars: ${head_env:-(none)}"
        
        # Check for CodeFlare volumes
        local head_volumes=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.spec.headGroupSpec.template.spec.volumes[*].name}' 2>/dev/null || echo "")
        local has_ca_vol=$(echo "$head_volumes" | grep -c "ca-vol" || echo "0")
        local has_server_cert=$(echo "$head_volumes" | grep -c "server-cert" || echo "0")
        local has_proxy_tls=$(echo "$head_volumes" | grep -c "proxy-tls-secret" || echo "0")
        echo "CodeFlare Volumes:"
        echo "  ca-vol: $([ "$has_ca_vol" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  server-cert: $([ "$has_server_cert" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  proxy-tls-secret: $([ "$has_proxy_tls" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  All Head Volumes: ${head_volumes:-(none)}"
        
        # Check for CodeFlare volume mounts
        local head_mounts=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{range .spec.headGroupSpec.template.spec.containers[?(@.name=="ray-head")].volumeMounts[*]}{.name}{" "}{end}' 2>/dev/null || echo "")
        local has_ca_vol_mount=$(echo "$head_mounts" | grep -c "ca-vol" || echo "0")
        local has_server_cert_mount=$(echo "$head_mounts" | grep -c "server-cert" || echo "0")
        echo "CodeFlare Volume Mounts:"
        echo "  ca-vol: $([ "$has_ca_vol_mount" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  server-cert: $([ "$has_server_cert_mount" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
        echo "  All Head Mounts: ${head_mounts:-(none)}"
        
        # Check for CodeFlare ServiceAccountName
        local sa_name=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.spec.headGroupSpec.template.spec.serviceAccountName}' 2>/dev/null || echo "")
        local is_oauth_sa=$(echo "$sa_name" | grep -c "oauth-proxy" || echo "0")
        echo "ServiceAccountName: ${sa_name:-(not set)} $([ "$is_oauth_sa" -gt 0 ] && echo "(CODEFLARE ✓)" || echo "")"
        echo ""
        
        # Check worker groups for CodeFlare items
        local num_workers=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.spec.workerGroupSpecs}' 2>/dev/null | jq 'length' 2>/dev/null || echo "0")
        if [ "$num_workers" -gt 0 ]; then
            echo "--- Worker Groups CodeFlare Items ---"
            for wi in $(seq 0 $((num_workers - 1))); do
                echo "Worker Group $wi:"
                
                local worker_init=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{.spec.workerGroupSpecs[$wi].template.spec.initContainers[*].name}" 2>/dev/null || echo "")
                local has_create_cert_w=$(echo "$worker_init" | grep -c "create-cert" || echo "0")
                echo "  create-cert InitContainer: $([ "$has_create_cert_w" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
                echo "    All Worker InitContainers: ${worker_init:-(none)}"
                
                local worker_env=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{.spec.workerGroupSpecs[$wi].template.spec.containers[0].env[*].name}" 2>/dev/null || echo "")
                local has_ray_tls_w=$(echo "$worker_env" | grep -c "RAY_USE_TLS" || echo "0")
                echo "  RAY_USE_TLS: $([ "$has_ray_tls_w" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
                echo "    All Worker Env Vars: ${worker_env:-(none)}"
                
                local worker_volumes=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{.spec.workerGroupSpecs[$wi].template.spec.volumes[*].name}" 2>/dev/null || echo "")
                local has_ca_vol_w=$(echo "$worker_volumes" | grep -c "ca-vol" || echo "0")
                echo "  ca-vol Volume: $([ "$has_ca_vol_w" -gt 0 ] && echo "PRESENT ✓" || echo "NOT PRESENT")"
                echo "    All Worker Volumes: ${worker_volumes:-(none)}"
            done
        fi
        echo ""
        echo "----------------------------------------------"
        echo ""
        
        # Finalizers
        echo "=== Finalizers ==="
        kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.metadata.finalizers[*]}' 2>/dev/null || echo "(none)"
        echo ""
        echo ""
        
        # Annotations
        echo "=== Annotations ==="
        kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.metadata.annotations}' 2>/dev/null | jq -r 'to_entries | .[] | "\(.key): \(.value)"' 2>/dev/null || echo "(none)"
        echo ""
        
        # Head Group
        echo "=== Head Group ==="
        echo "EnableIngress: $(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.spec.headGroupSpec.enableIngress}' 2>/dev/null || echo 'not set')"
        echo ""
        
        echo "--- Head ServiceAccountName ---"
        kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.spec.headGroupSpec.template.spec.serviceAccountName}' 2>/dev/null || echo "(not set)"
        echo ""
        echo ""
        
        echo "--- Head InitContainers ---"
        kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{range .spec.headGroupSpec.template.spec.initContainers[*]}{.name}{"\n"}{end}' 2>/dev/null || echo "(none)"
        echo ""
        
        echo "--- Head Containers ---"
        kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{range .spec.headGroupSpec.template.spec.containers[*]}{.name}{"\n"}{end}' 2>/dev/null || echo "(none)"
        echo ""
        
        echo "--- Head Container Env Vars (ray-head) ---"
        kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{range .spec.headGroupSpec.template.spec.containers[?(@.name=="ray-head")].env[*]}{.name}={.value}{"\n"}{end}' 2>/dev/null || echo "(none)"
        echo ""
        
        echo "--- Head Container Volume Mounts (ray-head) ---"
        kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{range .spec.headGroupSpec.template.spec.containers[?(@.name=="ray-head")].volumeMounts[*]}{.name} -> {.mountPath}{"\n"}{end}' 2>/dev/null || echo "(none)"
        echo ""
        
        echo "--- Head Volumes ---"
        kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{range .spec.headGroupSpec.template.spec.volumes[*]}{.name}{"\n"}{end}' 2>/dev/null || echo "(none)"
        echo ""
        
        # Worker Groups
        echo "=== Worker Groups ==="
        local num_workers=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath='{.spec.workerGroupSpecs}' 2>/dev/null | jq 'length' 2>/dev/null || echo "0")
        echo "Number of worker groups: $num_workers"
        echo ""
        
        for i in $(seq 0 $((num_workers - 1))); do
            echo "--- Worker Group $i ---"
            echo "GroupName: $(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{.spec.workerGroupSpecs[$i].groupName}" 2>/dev/null)"
            echo ""
            
            echo "ServiceAccountName:"
            kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{.spec.workerGroupSpecs[$i].template.spec.serviceAccountName}" 2>/dev/null || echo "(not set)"
            echo ""
            echo ""
            
            echo "InitContainers:"
            kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{range .spec.workerGroupSpecs[$i].template.spec.initContainers[*]}{.name}{\"\\n\"}{end}" 2>/dev/null || echo "(none)"
            echo ""
            
            echo "Containers:"
            kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{range .spec.workerGroupSpecs[$i].template.spec.containers[*]}{.name}{\"\\n\"}{end}" 2>/dev/null || echo "(none)"
            echo ""
            
            echo "Worker Container Env Vars (machine-learning or ray-worker):"
            # SDK uses "machine-learning", upstream uses "ray-worker"
            local worker_env=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{range .spec.workerGroupSpecs[$i].template.spec.containers[?(@.name==\"machine-learning\")].env[*]}{.name}={.value}{\"\\n\"}{end}" 2>/dev/null)
            if [ -z "$worker_env" ]; then
                worker_env=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{range .spec.workerGroupSpecs[$i].template.spec.containers[?(@.name==\"ray-worker\")].env[*]}{.name}={.value}{\"\\n\"}{end}" 2>/dev/null)
            fi
            echo "${worker_env:-(none)}"
            echo ""
            
            echo "Worker Container Volume Mounts (machine-learning or ray-worker):"
            local worker_mounts=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{range .spec.workerGroupSpecs[$i].template.spec.containers[?(@.name==\"machine-learning\")].volumeMounts[*]}{.name} -> {.mountPath}{\"\\n\"}{end}" 2>/dev/null)
            if [ -z "$worker_mounts" ]; then
                worker_mounts=$(kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{range .spec.workerGroupSpecs[$i].template.spec.containers[?(@.name==\"ray-worker\")].volumeMounts[*]}{.name} -> {.mountPath}{\"\\n\"}{end}" 2>/dev/null)
            fi
            echo "${worker_mounts:-(none)}"
            echo ""
            
            echo "Volumes:"
            kubectl get raycluster "$name" -n "$NAMESPACE" -o jsonpath="{range .spec.workerGroupSpecs[$i].template.spec.volumes[*]}{.name}{\"\\n\"}{end}" 2>/dev/null || echo "(none)"
            echo ""
        done
        
    }
}

# Function to display related resources
capture_related_resources() {
    local name=$1
    
    echo ""
    echo "=== Related Resources for: $name ==="
    echo ""
    
    # Services
    echo "Services (matching $name or oauth):"
    local services=$(kubectl get services -n "$NAMESPACE" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -E "(${name}|oauth)" || echo "(none)")
    echo "  $services"
    echo ""
    
    # Secrets
    echo "Secrets (CodeFlare-related patterns):"
    # CodeFlare patterns: ca-secret-{cluster}, {cluster}-oauth-config, {cluster}-proxy-tls-secret
    local secrets=$(kubectl get secrets -n "$NAMESPACE" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -E "^(ca-secret-${name}|${name}-oauth-config|${name}-proxy-tls-secret)$" 2>/dev/null || echo "(none)")
    echo "  $secrets"
    echo ""
    
    # NetworkPolicies
    echo "NetworkPolicies (matching $name):"
    local netpols=$(kubectl get networkpolicies -n "$NAMESPACE" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -E "${name}" || echo "(none)")
    echo "  $netpols"
    echo ""
    
    # ServiceAccounts (CodeFlare-created with label)
    echo "ServiceAccounts (CodeFlare-created with ray.openshift.ai/cluster-name label):"
    local sas=$(kubectl get serviceaccounts -n "$NAMESPACE" -l "ray.openshift.ai/cluster-name=$name" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || echo "(none)")
    echo "  $sas"
    echo ""
    
    # ClusterRoleBindings
    echo "ClusterRoleBindings (matching $name):"
    local crbs=$(kubectl get clusterrolebindings -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -E "(${name}.*auth|${name})" 2>/dev/null || echo "(none)")
    echo "  $crbs"
    echo ""
    
    # Routes
    echo "Routes (ray-dashboard/rayclient patterns):"
    local routes=$(kubectl get routes -n "$NAMESPACE" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -E "(ray-dashboard-${name}|rayclient-${name})" 2>/dev/null || echo "(none)")
    echo "  $routes"
    echo ""
    
    # Pods
    echo "Pods (ray.io/cluster=$name):"
    local pods=$(kubectl get pods -n "$NAMESPACE" -l "ray.io/cluster=$name" -o jsonpath='{range .items[*]}{.metadata.name}{" ("}{.status.phase}{")"}{"\n"}{end}' 2>/dev/null || echo "(none)")
    echo "  $pods"
}

# Process each RayCluster
for rc in $RAYCLUSTERS; do
    echo ""
    echo "----------------------------------------------"
    extract_raycluster_details "$rc"
    capture_related_resources "$rc"
done

echo ""
echo ""
echo "############################################"
echo "# SUMMARY"
echo "############################################"
echo ""
echo "Timestamp: $TIMESTAMP"
echo "Namespace: $NAMESPACE"
echo ""
echo "RayClusters checked:"
for rc in $RAYCLUSTERS; do
    echo "  - $rc"
done
echo ""
echo "=============================================="
echo "CodeFlare-Operator Detection Summary:"
echo "=============================================="
echo ""

for rc in $RAYCLUSTERS; do
    echo "--- $rc ---"
    
    # Check for CodeFlare finalizer
    has_cf_finalizer=$(kubectl get raycluster "$rc" -n "$NAMESPACE" -o jsonpath='{.metadata.finalizers[*]}' 2>/dev/null | grep -c "ray.openshift.ai/oauth-finalizer" || echo "0")
    
    # Check for oauth-proxy sidecar
    has_oauth_proxy=$(kubectl get raycluster "$rc" -n "$NAMESPACE" -o jsonpath='{.spec.headGroupSpec.template.spec.containers[*].name}' 2>/dev/null | grep -c "oauth-proxy" || echo "0")
    
    # Check for create-cert initContainer
    has_create_cert=$(kubectl get raycluster "$rc" -n "$NAMESPACE" -o jsonpath='{.spec.headGroupSpec.template.spec.initContainers[*].name}' 2>/dev/null | grep -c "create-cert" || echo "0")
    
    # Check for RAY_USE_TLS env var
    has_ray_tls=$(kubectl get raycluster "$rc" -n "$NAMESPACE" -o jsonpath='{.spec.headGroupSpec.template.spec.containers[0].env[*].name}' 2>/dev/null | grep -c "RAY_USE_TLS" || echo "0")
    
    # Check for ca-vol volume
    has_ca_vol=$(kubectl get raycluster "$rc" -n "$NAMESPACE" -o jsonpath='{.spec.headGroupSpec.template.spec.volumes[*].name}' 2>/dev/null | grep -c "ca-vol" || echo "0")
    
    # Check for CodeFlare version annotation
    has_cf_version=$(kubectl get raycluster "$rc" -n "$NAMESPACE" -o jsonpath='{.metadata.annotations.ray\.openshift\.ai/version}' 2>/dev/null | grep -c "." || echo "0")
    
    # Count CodeFlare items found
    cf_count=0
    [ "$has_cf_finalizer" -gt 0 ] && ((cf_count++)) || true
    [ "$has_oauth_proxy" -gt 0 ] && ((cf_count++)) || true
    [ "$has_create_cert" -gt 0 ] && ((cf_count++)) || true
    [ "$has_ray_tls" -gt 0 ] && ((cf_count++)) || true
    [ "$has_ca_vol" -gt 0 ] && ((cf_count++)) || true
    [ "$has_cf_version" -gt 0 ] && ((cf_count++)) || true
    
    if [ "$cf_count" -gt 0 ]; then
        echo "  STATUS: CODEFLARE-MANAGED (${cf_count}/6 indicators found)"
        echo "  Items found:"
        [ "$has_cf_finalizer" -gt 0 ] && echo "    ✓ ray.openshift.ai/oauth-finalizer"
        [ "$has_cf_version" -gt 0 ] && echo "    ✓ ray.openshift.ai/version annotation"
        [ "$has_oauth_proxy" -gt 0 ] && echo "    ✓ oauth-proxy sidecar"
        [ "$has_create_cert" -gt 0 ] && echo "    ✓ create-cert initContainer"
        [ "$has_ray_tls" -gt 0 ] && echo "    ✓ RAY_USE_TLS env vars"
        [ "$has_ca_vol" -gt 0 ] && echo "    ✓ ca-vol/server-cert volumes"
    else
        echo "  STATUS: NOT CODEFLARE-MANAGED (migration will skip)"
    fi
    echo ""
done

echo "=============================================="
echo "Pre-migration check complete!"
echo "=============================================="
