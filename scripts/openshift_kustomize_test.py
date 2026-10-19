"""Regression tests for the OpenShift KubeRay manifests."""

from pathlib import Path
import os
import shutil
import subprocess

import yaml


REPO_ROOT = Path(__file__).absolute().parent.parent


def kustomize_command():
    """Return an available Kustomize command for local and CI environments."""
    configured_binary = os.environ.get("KUSTOMIZE_BIN")
    if configured_binary:
        return [configured_binary, "build"]

    local_binary = REPO_ROOT / "ray-operator/bin/kustomize"
    if local_binary.is_file() and os.access(str(local_binary), os.X_OK):
        return [str(local_binary), "build"]

    kubectl = shutil.which("kubectl")
    if kubectl:
        return [kubectl, "kustomize"]

    raise RuntimeError("Neither Kustomize nor kubectl kustomize is available")


def render_openshift_overlay():
    """Render the OpenShift overlay and return its Kubernetes objects."""
    result = subprocess.run(
        kustomize_command() + [str(REPO_ROOT / "ray-operator/config/openshift")],
        check=True,
        capture_output=True,
        text=True,
    )
    return [obj for obj in yaml.safe_load_all(result.stdout) if obj]


def test_kuberay_operator_selector_preserves_rhoai_351_labels():
    """The selector must stay compatible with the RHOAI 3.5.1 Deployment."""
    deployment = next(
        obj
        for obj in render_openshift_overlay()
        if obj.get("kind") == "Deployment"
        and obj.get("metadata", {}).get("name") == "kuberay-operator"
    )

    expected_labels = {
        "app.kubernetes.io/name": "kuberay",
        "app.kubernetes.io/component": "kuberay-operator",
        "app.kubernetes.io/part-of": "ray",
        "app.opendatahub.io/ray": "true",
    }

    assert deployment["spec"]["selector"]["matchLabels"] == expected_labels
    assert deployment["spec"]["template"]["metadata"]["labels"] == expected_labels
