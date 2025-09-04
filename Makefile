# Image tag for image containing e2e tests
E2E_TEST_IMAGE_VERSION ?= latest
E2E_TEST_IMAGE ?= quay.io/opendatahub/kuberay-tests:${E2E_TEST_IMAGE_VERSION}

# Build the test image
.PHONY: build-test-image
build-test-image:
	@echo "Building test image: $(E2E_TEST_IMAGE)"
	# Comment out t.Parallel() calls in the test file
	@if [ "$$(uname)" = "Darwin" ]; then \
		sed -i '' 's/t\.Parallel()/\/\/ t.Parallel()/g' ray-operator/test/e2erayjob/rayjob_cluster_selector_test.go; \
	else \
		sed -i 's/t\.Parallel()/\/\/ t.Parallel()/g' ray-operator/test/e2erayjob/rayjob_cluster_selector_test.go; \
	fi
	# Update resource requirements in support.go using Go AST manipulation
	@echo "Updating resource requirements using Go AST manipulation..."
	@cd scripts && go build -o update-resources update-resources.go
	@scripts/update-resources ray-operator/test/support/support.go
	@echo "Resource requirements updated successfully using Go AST."
	# Build the Docker image using podman
	podman build -f ray-operator/images/tests/Dockerfile -t $(E2E_TEST_IMAGE) .
	# Confirm that the resources/ limits were updated
	@echo "=== Contents of support.go (resource values) ==="
	@grep -n 'resource\.MustParse(' ray-operator/test/support/support.go | grep -E '"(500m|1000m|2000m|1G|3G|6G|10G)"'
	@echo "=== Contents of rayjob_cluster_selector_test.go (t.Parallel comments) ==="
	@grep -n 't\.Parallel()' ray-operator/test/e2erayjob/rayjob_cluster_selector_test.go

# Push the test image
.PHONY: push-test-image
push-test-image:
	@echo "Pushing test image: $(E2E_TEST_IMAGE)"
	podman push $(E2E_TEST_IMAGE)
