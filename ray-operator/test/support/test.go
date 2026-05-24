package support

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// Option is a function that modifies a value of type T
// This pattern allows for flexible configuration of objects with optional settings
type Option[T any] func(T)

// Test represents a test that can be run in parallel with other tests
// This interface defines the minimum set of methods required for test isolation
type Test interface {
	// T returns the testing.T instance for this test
	T() *testing.T

	// Ctx returns the context for this test
	// The context is automatically cancelled when the test completes
	Ctx() context.Context

	// Client returns the Kubernetes client for this test
	// Each test gets its own client to avoid interference
	Client() Client

	// OutputDir returns the output directory for test artifacts
	// Each test gets its own directory to avoid conflicts
	OutputDir() string

	// NewTestNamespace creates a new namespace for this test with a unique name
	// The namespace is automatically cleaned up when the test completes
	// Options can be provided to customize the namespace
	NewTestNamespace(opts ...Option[*corev1.Namespace]) *corev1.Namespace
}
