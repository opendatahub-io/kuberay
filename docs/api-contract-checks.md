# API contract checks

Pull requests targeting **`dev`** run static API contract checks from
[`red-hat-data-services/api-contract-tests`](https://github.com/red-hat-data-services/api-contract-tests)
([RHOAIENG-103190](https://redhat.atlassian.net/browse/RHOAIENG-103190)). No cluster is required.
Every job uses **`mode: enforce`**: breaking changes fail the pull request.

## What runs on each pull request

Workflow [`.github/workflows/api-contract.yml`](../.github/workflows/api-contract.yml) adds these jobs:

| Job | What it checks |
|-----|----------------|
| `api-contract` (matrix ×6) | Swagger 2.0 specs under `proto/` vs baseline **v1.6.2** (`560baadd…`) |
| `crd-contract` | `ray-operator/config/crd/bases/*.yaml` vs baseline **dev** (`b02c607…`); see baselines below |
| `api-contract-coverage` | Every OpenAPI file and CRD manifest is a contract, generator input, or listed exclusion |

Contracts are declared in [`.rhoai/contracts.yaml`](../.rhoai/contracts.yaml).

## Baselines

- **OpenAPI:** newest GA tag **v1.6.2** at `560baadd3a5c4268e6c021f6cfe36591a7b2551e`.
- **CRDs:** `dev` at `b02c607ea69f1095416a4a31500beaff08f80463` when this gate was added.
  v1.6.2 fails the CRD check (CRDs changed since release). Record the baseline exception on
  [RHOAIENG-103190](https://redhat.atlassian.net/browse/RHOAIENG-103190) if reviewers require v1.6.2 for
  all artifacts.

Moving a baseline forward needs approval from someone other than the author. Use full 40-character SHAs only.

## Run locally

Use the same tool versions as the workflow (`oasdiff` **1.33.0**, `yq` **4.45.1**). The reusable workflows
call api-contract-tests at the **`latest`** tag.

```sh
git clone https://github.com/red-hat-data-services/api-contract-tests.git /tmp/api-contract-tests
git -C /tmp/api-contract-tests fetch --tags && git -C /tmp/api-contract-tests checkout latest

go install github.com/oasdiff/oasdiff@v1.33.0
go build -C /tmp/api-contract-tests/cmd/swagger2openapi -o bin/swagger2openapi .

export PATH="$(go env GOPATH)/bin:$PATH"
CT=/tmp/api-contract-tests

# One OpenAPI contract (repeat for each contract-id in the workflow matrix)
python3 "$CT/cmd/contract-tests/cli.py" check --repo . \
  --contract-set .rhoai/contracts.yaml --contract-id kuberay-api \
  --baseline-ref 560baadd3a5c4268e6c021f6cfe36591a7b2551e \
  --swagger2openapi ./bin/swagger2openapi --output-dir /tmp/contract-report

python3 "$CT/cmd/contract-tests/cli.py" crd-check --repo . \
  --crd-glob 'ray-operator/config/crd/bases/*.yaml' \
  --baseline-ref b02c607ea69f1095416a4a31500beaff08f80463 \
  --output-dir /tmp/contract-report-crd

python3 "$CT/cmd/contract-tests/cli.py" coverage --repo . \
  --contract-set .rhoai/contracts.yaml --yq-version 4.45.1 \
  --crd-glob 'ray-operator/config/crd/bases/*.yaml' \
  --crd-exclude 'helm-chart/kuberay-operator/crds/*.yaml'
```

Downstream-only: keep `.rhoai/` and `.github/workflows/api-contract.yml` when syncing from
[ray-project/kuberay](https://github.com/ray-project/kuberay).
