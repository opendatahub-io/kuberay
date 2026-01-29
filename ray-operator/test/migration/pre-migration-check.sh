#!/bin/bash
# pre-migration-check.sh
# Captures the state of RayClusters before migration for later comparison
#
# Usage: ./pre-migration-check.sh [namespace] [output-dir]
#
# This script captures:
# - RayCluster CR specs (excluding status)
# - Container names, env vars, volume mounts
# - InitContainer names
# - ServiceAccountNames
# - Volumes
# - Related resources (Services, Secrets, NetworkPolicies, etc.)

set -e

NAMESPACE="${1:-test-migration}"
OUTPUT_DIR="${2:-/tmp/migration-check/pre}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

echo "=============================================="
echo "Pre-Migration Check for RayClusters"
echo "Namespace: $NAMESPACE"
echo "Output directory: $OUTPUT_DIR"
echo "Timestamp: $TIMESTAMP"
echo "=============================================="

# Create output directory
mkdir -p "$OUTPUT_DIR"

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

# Function to extract and save RayCluster details
extract_raycluster_details() {
    local name=$1
    local outfile="$OUTPUT_DIR/${name}.json"
    
    echo "Processing RayCluster: $name"
    
    # Get full RayCluster spec
    kubectl get raycluster "$name" -n "$NAMESPACE" -o json > "$outfile"
    
    # Create summary file
    local summary="$OUTPUT_DIR/${name}-summary.txt"
    {
        echo "RayCluster: $name"
        echo "Captured at: $TIMESTAMP"
        echo "=============================================="
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
        
    } > "$summary"
    
    echo "  Saved to: $outfile"
    echo "  Summary: $summary"
}

# Function to capture related resources
capture_related_resources() {
    local name=$1
    local resource_dir="$OUTPUT_DIR/${name}-resources"
    mkdir -p "$resource_dir"
    
    echo ""
    echo "Capturing related resources for: $name"
    
    # Services
    echo "  - Services..."
    kubectl get services -n "$NAMESPACE" -l "ray.io/cluster=$name" -o json > "$resource_dir/services.json" 2>/dev/null || echo '{"items":[]}' > "$resource_dir/services.json"
    
    # Secrets (list names only for security)
    echo "  - Secrets..."
    kubectl get secrets -n "$NAMESPACE" -l "ray.io/cluster=$name" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' > "$resource_dir/secrets.txt" 2>/dev/null || echo "(none)" > "$resource_dir/secrets.txt"
    
    # Also capture CodeFlare CA secrets
    kubectl get secrets -n "$NAMESPACE" -o name 2>/dev/null | grep -E "(ca-secret|proxy-tls)" >> "$resource_dir/secrets.txt" 2>/dev/null || true
    
    # NetworkPolicies
    echo "  - NetworkPolicies..."
    kubectl get networkpolicies -n "$NAMESPACE" -l "ray.io/cluster=$name" -o json > "$resource_dir/networkpolicies.json" 2>/dev/null || echo '{"items":[]}' > "$resource_dir/networkpolicies.json"
    
    # Also capture any CodeFlare-created NetworkPolicies
    kubectl get networkpolicies -n "$NAMESPACE" -o json 2>/dev/null | jq "[.items[] | select(.metadata.name | contains(\"$name\"))]" > "$resource_dir/networkpolicies-all.json" 2>/dev/null || echo '[]' > "$resource_dir/networkpolicies-all.json"
    
    # ServiceAccounts
    echo "  - ServiceAccounts..."
    kubectl get serviceaccounts -n "$NAMESPACE" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -E "(oauth-proxy|$name)" > "$resource_dir/serviceaccounts.txt" 2>/dev/null || echo "(none)" > "$resource_dir/serviceaccounts.txt"
    
    # ClusterRoleBindings (cluster-scoped)
    echo "  - ClusterRoleBindings..."
    kubectl get clusterrolebindings -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep "$name" > "$resource_dir/clusterrolebindings.txt" 2>/dev/null || echo "(none)" > "$resource_dir/clusterrolebindings.txt"
    
    # Routes (OpenShift)
    echo "  - Routes..."
    kubectl get routes -n "$NAMESPACE" -l "ray.io/cluster=$name" -o json > "$resource_dir/routes.json" 2>/dev/null || echo '{"items":[]}' > "$resource_dir/routes.json"
    
    # Pods
    echo "  - Pods..."
    kubectl get pods -n "$NAMESPACE" -l "ray.io/cluster=$name" -o json > "$resource_dir/pods.json" 2>/dev/null || echo '{"items":[]}' > "$resource_dir/pods.json"
    
    echo "  Resources saved to: $resource_dir/"
}

# Process each RayCluster
for rc in $RAYCLUSTERS; do
    echo ""
    echo "----------------------------------------------"
    extract_raycluster_details "$rc"
    capture_related_resources "$rc"
done

# Create manifest file
MANIFEST="$OUTPUT_DIR/manifest.txt"
{
    echo "Pre-Migration Check Manifest"
    echo "Timestamp: $TIMESTAMP"
    echo "Namespace: $NAMESPACE"
    echo ""
    echo "RayClusters captured:"
    for rc in $RAYCLUSTERS; do
        echo "  - $rc"
    done
} > "$MANIFEST"

echo ""
echo "=============================================="
echo "Pre-migration check complete!"
echo "Output saved to: $OUTPUT_DIR"
echo ""
echo "Files created:"
ls -la "$OUTPUT_DIR"
echo "=============================================="
