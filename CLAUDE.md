# KubeRay

Kubernetes operator for deploying and managing Ray applications. Includes RayCluster, RayJob, and RayService
custom resources. Go-based operator using Kubebuilder framework.

## Repository Structure

| Directory | Description |
|---|---|
| `ray-operator/` | Core operator managing Ray custom resources |
| `apiserver/` | Optional REST API server for KubeRay resources (Alpha) |
| `apiserversdk/` | Go SDK for KubeRay API server |
| `proto/` | Protocol buffer definitions for API |
| `helm-chart/` | Helm charts for KubeRay deployment |
| `kubectl-plugin/` | kubectl ray plugin (Beta) |
| `config/` | Operator configuration and CRD manifests |
| `scripts/` | Build and utility scripts |

### Where to Make Changes

| Task | Location |
|---|---|
| CRD types (RayCluster, RayJob, RayService) | `ray-operator/apis/ray/v1/` |
| Controller reconciliation logic | `ray-operator/controllers/ray/` |
| Webhooks (validation and defaulting) | `ray-operator/pkg/webhooks/v1/` |
| E2e tests | `ray-operator/test/e2e/`, `ray-operator/test/e2eautoscaler/`, `ray-operator/test/e2eupgrade/`, `ray-operator/test/e2erayservice/` |
| Unit tests | Adjacent `*_test.go` or `*_unit_test.go` next to source |
| E2e test helpers | `ray-operator/test/support/` |
| Kustomize overlays (OpenShift/midstream) | `ray-operator/config/openshift/` |
| Kustomize base (CRDs, RBAC, webhook) | `ray-operator/config/default/`, `ray-operator/config/crd/`, `ray-operator/config/rbac/`, `ray-operator/config/webhook/` |
| Generated clients (clientset, apply configs) | `ray-operator/pkg/client/` |
| CI workflows | `.github/workflows/` |
| Codegen scripts | `ray-operator/hack/`, then `make manifests generate` |
| Midstream Makefile (e2e image builds) | Root `Makefile` |
| Pre-commit config | `.pre-commit-config.yaml` |
| Linter config | `.golangci.yml` |

## Build and Test Commands

```sh
# Run all unit tests (from ray-operator/)
cd ray-operator && make test

# Run specific unit test file
cd ray-operator && make test WHAT=./controllers/ray/...

# Run e2e tests (requires cluster)
cd ray-operator && make test-e2e

# Run apiserver unit tests
cd apiserver && make test

# Build operator binary
cd ray-operator && make build

# Build Docker images
cd ray-operator && make docker-build

# Install CRDs
cd ray-operator && make install

# Deploy operator
cd ray-operator && make deploy
```

## Single-File Commands

```sh
# Lint a single file
golangci-lint run path/to/file.go

# Lint with auto-fix
golangci-lint run --fix path/to/file.go

# Run all linters via script
./scripts/lint.sh

# Format Go code
gofmt -s -w path/to/file.go
goimports -w path/to/file.go

# Run pre-commit hooks manually
pre-commit run --all-files
```

## Coding Conventions

### Go Style

- **Go version**: 1.24+
- **Linter**: golangci-lint (configured in `.golangci.yml`)
- **Formatting**: gofmt with simplify (-s), goimports
- **Import order**: standard library, third-party, `github.com/ray-project/kuberay`
- **Naming**: standard Go conventions (camelCase for unexported, PascalCase for exported)
- **Error handling**: explicit error returns, no panic in production code
- **Comments**: exported symbols must have doc comments

### Linting Rules

Enabled linters (via `.golangci.yml`):

- gofmt (with simplify)
- goimports (local prefix: github.com/ray-project/kuberay)
- revive (Go style guide)
- gosec (security)
- misspell
- ginkgolinter (for test files)

Key conventions:

- Use context as first parameter
- Avoid naked returns in long functions
- Check error returns explicitly
- No TODO without explanation
- Exported functions require doc comments

### Testing

- **Framework**: Go testing + Ginkgo for e2e tests
- **Coverage**: Required for new code
- **Test location**: `*_test.go` files alongside source or in `test/e2e/`
- **Mocking**: Use interfaces and generated mocks
- **E2E tests**: Require Kubernetes cluster (kind recommended)

### Pre-Commit Hooks

Enforced via `.pre-commit-config.yaml`:

- trailing-whitespace, end-of-file-fixer
- YAML, JSON validation
- golangci-lint
- shellcheck for shell scripts
- gitleaks (secret detection)
- markdownlint
- CRD schema validation
- Helm chart validation

### Kubernetes API Patterns

- **CRDs**: RayCluster, RayJob, RayService defined in `ray-operator/apis/ray/v1`
- **Controllers**: Use controller-runtime reconcile pattern
- **Status updates**: Use status subresource, update status separately
- **Finalizers**: Required for cleanup logic
- **RBAC**: Defined in `config/rbac/`
- **Webhooks**: Validation and defaulting in `ray-operator/controllers/ray/common/`

### Project Modules

This is a multi-module repo with three main Go modules:

- Root module (workspace coordination)
- `ray-operator/` (operator implementation)
- `apiserver/` (API server implementation)
- `scripts/` (utility scripts)

When making changes, run `go mod tidy` in the relevant module directory.

## Pattern References

Real examples for the most common change types. Follow these patterns, not descriptions.

### Adding a new CRD field

See `AuthenticationReady` in `ray-operator/apis/ray/v1/raycluster_types.go`.
Pattern: add a typed constant or struct field with a godoc comment, wire it into
the controller, then add an e2e test. After changing types, regenerate:

```sh
cd ray-operator && make manifests generate
```

### Adding or modifying a controller reconciler

See `RayClusterReconciler.Reconcile` in `ray-operator/controllers/ray/raycluster_controller.go`.
The thin `Reconcile` method fetches the CR and delegates to `rayClusterReconcile`,
which calls sub-reconcilers (`reconcilePods`, `reconcileHeadService`, etc.).
New reconciliation logic goes in a sub-reconciler, not inline in the top-level method.

### Adding an e2e test

See `TestRayJob` in `ray-operator/test/e2e/rayjob_test.go`.
Pattern: `test := With(t)` for support helpers, `test.NewTestNamespace()` for isolation,
apply resources via `test.Client().Core()...Apply(...)`, assert with `Eventually` +
Gomega. Support helpers live in `ray-operator/test/support/`.

### Midstream carry patch

See `CARRY: task(RHOAIENG-59432): fix RoleBinding for autoscaling`.
Pattern: `CARRY:` prefix, Jira key in subject, scoped to the minimum diff. Carries are
upstream-unmergeable fixes specific to RHOAI/ODH. They touch controller logic, RBAC,
and corresponding unit tests.

### Kustomize overlay or webhook change

- **Webhook**: `ray-operator/pkg/webhooks/v1/raycluster_mutating_webhook.go` — OpenShift-
  specific defaults (e.g. enforce `EnableSecureTrustedNetwork`, disable `EnableIngress`).
  Unit tests are adjacent: `raycluster_mutating_webhook_unit_test.go`.
- **Kustomize**: `ray-operator/config/openshift/kustomization.yaml` — composes `../default`,
  adds `webhook.yaml`, and applies patches like `webhook-deployment-patch.yaml` and
  `kuberay-operator-image-patch.yaml`.

## Documentation

User-facing docs: <https://docs.ray.io/en/latest/cluster/kubernetes/>
Developer docs: See DEVELOPMENT.md in ray-operator/ and apiserver/
