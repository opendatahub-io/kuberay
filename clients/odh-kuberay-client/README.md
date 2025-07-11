# Overview

This python client library provide APIs to handle `raycluster` and `rayjobs` from your python application.

## NOTICE

This python client is derived from the ray-project/kuberay python client and is not the upstream version of this client.

## Prerequisites

It is assumed that your `k8s cluster in already setup`. Your kubectl configuration is expected to be
in  `~/.kube/config` if you are running the code directly from you terminal.

It is also expected that the `kuberay operator` is installed.
[Installation instructions are here][quick-start]

## Quick Install (TestPyPI)

Development versions are published to TestPyPI on every push/PR:

```bash
pip install -i https://test.pypi.org/simple/ odh-kuberay-client
```

## How to set up TestPyPI publishing in GitHub Actions

1. Create an account at <https://test.pypi.org/account/register/>
2. Go to Account Settings → API tokens → Add API token ("Upload packages")
3. Copy the token (starts with `pypi-...`)
4. In your GitHub repo, go to Settings → Secrets → Actions → New repository secret
5. Name: `TEST_PYPI_API_TOKEN`, Value: (paste your token)
6. That's it! Every push/PR will build and publish to TestPyPI if the secret is set.

### Automatic Publishing on PRs

When you create a PR targeting the `dev` branch:

- The workflow automatically builds the Python client
- Publishes it to TestPyPI with a unique version (e.g., `0.1.0.dev123` for PR #123)
- Comments on the PR with installation instructions
- Each new commit to the PR triggers a new build

This allows testing changes before merging!

## Installation

### From TestPyPI (Recommended)

Development versions are automatically published to TestPyPI on every PR and push:

```bash
# Install latest development version
pip install -i https://test.pypi.org/simple/ odh-kuberay-client

# Or install with PyPI for dependencies
pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ odh-kuberay-client

# Install specific version (e.g., from PR #123)
pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ odh-kuberay-client==0.1.0.dev123
```

🔗 **Browse versions:** <https://test.pypi.org/project/odh-kuberay-client/>

### From Pull Requests

You can install directly from any PR branch:

```bash
# Install from a PR (replace with actual PR branch)
pip install git+https://github.com/{username}/kuberay.git@{branch-name}#subdirectory=clients/odh-kuberay-client
```

### From Source (Development)

```bash
git clone https://github.com/opendatahub-io/kuberay.git
cd kuberay/clients/odh-kuberay-client
pip install -e .
```

## Usage

There are multiple levels of using the API with increasing levels of complexity.

### director

This is the easiest form of using the API to create rayclusters with predefined cluster sizes

```python
my_kuberay_api = kuberay_cluster_api.RayClusterApi()

my_cluster_director = kuberay_cluster_builder.Director()

cluster0 = my_cluster_director.build_small_cluster(name="new-cluster0")

if cluster0:
    my_kuberay_api.create_ray_cluster(body=cluster0)
```

the director create the cluster definition, and the `cluster_api` acts as the HTTP client sending
the create (post) request to the k8s api-server

### cluster_builder

The builder allows you to build the cluster piece by piece. You can customize the cluster more.

```python
cluster1 = (
        my_cluster_builder.build_meta(name="new-cluster1")
        .build_head()
        .build_worker(group_name="workers", replicas=3)
        .get_cluster()
    )

if not my_cluster_builder.succeeded:
    return

my_kuberay_api.create_ray_cluster(body=cluster1)
```

### cluster_utils

`cluster_utils` gives you even more options to modify your cluster definition, add/remove worker
groups, change replicas in a worker group, duplicate a worker group, etc.

```python
my_Cluster_utils = kuberay_cluster_utils.ClusterUtils()

cluster_to_patch, succeeded = my_Cluster_utils.update_worker_group_replicas(
    cluster2, group_name="workers", max_replicas=4, min_replicas=1, replicas=2
)

if succeeded:
    my_kuberay_api.patch_ray_cluster(
        name=cluster_to_patch["metadata"]["name"], ray_patch=cluster_to_patch
    )
```

### cluster_api

Finally, the `cluster_api` is the one you always use to implement your cluster change in k8s. You can
use it with raw `JSON` if you wish. The `director/cluster_builder/cluster_utils` are just tools to
shield the user from using raw `JSON`.

## 🛠️ Local Development

To develop and test the Python client locally:

### 1. **Clone the repository**

```bash
git clone https://github.com/opendatahub-io/kuberay.git
cd kuberay/clients/odh-kuberay-client
```

### 2. **Install dependencies in editable mode**

This allows you to edit the code and immediately see changes without reinstalling.

```bash
pip install -U pip setuptools
pip install -e .
```

### 3. **Run tests**

```bash
python -m unittest discover python_client_test/
```

### 4. **Uninstall the local package**

If you want to remove the local development install:

```bash
pip uninstall odh-kuberay-client
```

### 5. **Build and Test Distribution Locally**

To build the package as it would be for PyPI:

```bash
# Clean previous builds
rm -rf dist/ build/ *.egg-info

# Build the package
python -m build
```

You can then install the built wheel or tarball:

```bash
pip install dist/odh_kuberay_client-*.whl
```

### 6. **Publishing to TestPyPI**

To test your package before releasing to PyPI:

#### Local Dev Prerequisites

1. **Create a TestPyPI account** at <https://test.pypi.org/account/register/>
2. **Generate an API token**:
   - Go to Account Settings → API tokens → Add API token
   - Select scope: "Entire account" or project-specific
   - Copy the token (starts with `pypi-`)
3. **Set up authentication** (choose one method):
   - **Environment variable (recommended)**:

     ```bash
     export TWINE_USERNAME=__token__
     export TWINE_PASSWORD=your-testpypi-token
     ```

   - **Or use `.pypirc` file** in your home directory
   - **Or enter credentials when prompted**

#### Upload to TestPyPI

```bash
# Install twine if not already installed
pip install --upgrade twine

# Upload to TestPyPI (requires account at https://test.pypi.org)
twine upload --repository testpypi dist/*
```

**Note**: TestPyPI is separate from PyPI - you need different accounts and tokens for each.

## Code Organization

```text
clients/
└── odh-kuberay-client
    ├── LICENSE
    ├── README.md
    ├── examples
    │   ├── complete-example.py
    │   ├── use-builder.py
    │   ├── use-director.py
    │   ├── use-raw-config_map_with-api.py
    │   ├── use-raw-with-api.py
    │   └── use-utils.py
    ├── pyproject.toml
    ├── odh_kuberay_client
    │   ├── __init__.py
    │   ├── constants.py
    │   ├── kuberay_cluster_api.py
    │   ├── kuberay_job_api.py
    │   └── utils
    │       ├── __init__.py
    │       ├── kuberay_cluster_builder.py
    │       └── kuberay_cluster_utils.py
    ├── python_client_test
    │   ├── README.md
    │   ├── test_cluster_api.py
    │   ├── test_job_api.py
    │   ├── test_director.py
    │   └── test_utils.py
    └── setup.cfg
```

## For developers

make sure you have installed setuptool

`pip install -U pip setuptools`

### run the pip command

from the directory `path/to/kuberay/clients/odh-kuberay-client`

`pip install -e .`

### to uninstall the module run

`pip uninstall odh-kuberay-client`

### For testing run

 `python -m unittest discover 'path/to/kuberay/clients/odh-kuberay-client/python_client_test/'`

[quick-start]: https://github.com/opendatahub-io/kuberay#quick-start
