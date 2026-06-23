---
paths:
  - ray-operator/**
---

# ray-operator

Core Kubernetes operator managing RayCluster, RayJob, RayService, and RayCronJob custom resources.
Built with Kubebuilder v3 and controller-runtime.

## Key Abstractions

- **Reconcilers** (`controllers/ray/*_controller.go`): one per CR kind, implement the controller-runtime `Reconcile` loop
- **Common builders** (`controllers/ray/common/`): construct K8s objects
  (Pods, Services, Jobs, RBAC) from CR specs — shared across controllers
- **Batch scheduler interface** (`controllers/ray/batchscheduler/interface/`):
  pluggable gang schedulers (Volcano, YuniKorn, Kai, scheduler-plugins)
- **Webhooks** (`pkg/webhooks/v1/`): admission validation and defaulting, registered when `ENABLE_WEBHOOKS=true`
- **Feature gates** (`pkg/features/`): runtime toggles for experimental features (RayCronJob, RayJobDeletionPolicy, etc.)

## Source of Truth for CRDs

The Go type definitions in `apis/ray/v1/*_types.go` are the single source of truth.
Everything else is derived:

- CRD YAML: `make manifests` -> `config/crd/bases/`
- Client code: `make generate` -> `pkg/client/`
- Helm CRDs: `make helm` -> `helm-chart/kuberay-operator/crds/`
- API docs: `make api-docs` -> `docs/reference/api.md`

After changing `*_types.go`, run `make sync` to regenerate all derived artifacts.

## Generated Files — Do Not Edit

Files with `DO NOT EDIT` headers are auto-generated. Key patterns:

- `apis/**/zz_generated.deepcopy.go` (controller-gen)
- `pkg/client/**` (k8s code-generator: clientset, informers, listers, applyconfiguration)
- `config/crd/bases/*.yaml` (controller-gen)
- `*_mock.go` (MockGen)

## Import Boundaries

- `apis/` must NOT import from `controllers/` or `pkg/webhooks/` — types stay dependency-free
- `pkg/webhooks/` must NOT import from `controllers/` — webhooks depend only on API types and utilities
- `controllers/ray/common/` must NOT import specific `*_controller.go` files — shared builders are not coupled to one controller

## Commands

- `make test` — unit tests
- `make test-e2e` — end-to-end tests (requires kind cluster)
- `make manifests` — regenerate CRDs, RBAC, webhook YAML
- `make generate` — regenerate deepcopy and client code
- `make sync` — manifests + generate + helm + api-docs
