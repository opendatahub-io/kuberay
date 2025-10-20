package support

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ParallelTest represents a test that can be run in parallel with other tests
type ParallelTest struct {
	t             *testing.T
	ctx           context.Context
	client        Client
	cleanup       []func()
	beforeEachFns []func(namespace *corev1.Namespace) error
}

// WithParallel creates a new ParallelTest that is safe to run in parallel with other tests
func WithParallel(t *testing.T) *ParallelTest {
	t.Helper()
	t.Parallel() // Mark test as parallel

	ctx := context.Background()
	if deadline, ok := t.Deadline(); ok {
		withDeadline, cancel := context.WithDeadline(ctx, deadline)
		t.Cleanup(cancel)
		ctx = withDeadline
	}

	pt := &ParallelTest{
		t:             t,
		ctx:           ctx,
		cleanup:       make([]func(), 0),
		beforeEachFns: make([]func(namespace *corev1.Namespace) error, 0),
	}

	// Create a new client for this test
	c, err := newTestClient()
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	pt.client = c

	// Register cleanup to run in reverse order
	t.Cleanup(func() {
		for i := len(pt.cleanup) - 1; i >= 0; i-- {
			pt.cleanup[i]()
		}
	})

	return pt
}

// NewTestNamespace creates a new namespace for this test with a unique name
func (pt *ParallelTest) NewTestNamespace(opts ...Option[*corev1.Namespace]) *corev1.Namespace {
	pt.t.Helper()

	name := fmt.Sprintf("test-%s", pt.t.Name())
	name = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, name)
	if len(name) > 63 {
		name = name[:63]
	}
	name = strings.TrimSuffix(name, "-")

	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}

	for _, opt := range opts {
		opt(namespace)
	}

	namespace, err := pt.client.Core().CoreV1().Namespaces().Create(pt.ctx, namespace, metav1.CreateOptions{})
	if err != nil {
		pt.t.Fatalf("Error creating namespace: %v", err)
	}

	for _, fn := range pt.beforeEachFns {
		if err := fn(namespace); err != nil {
			pt.t.Fatalf("BeforeEach failed: %v", err)
		}
	}

	pt.cleanup = append(pt.cleanup, func() {
		storeAllPodLogs(pt, namespace)
		storeEvents(pt, namespace)

		err := pt.client.Core().CoreV1().Namespaces().Delete(pt.ctx, namespace.Name, metav1.DeleteOptions{})
		if err != nil {
			pt.t.Errorf("Error deleting namespace: %v", err)
		}
	})

	return namespace
}

// NewConfigMap creates a new ConfigMap in the given namespace
func (pt *ParallelTest) NewConfigMap(namespace string, name string, data map[string]string) *corev1.ConfigMap {
	pt.t.Helper()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: data,
	}

	cm, err := pt.client.Core().CoreV1().ConfigMaps(namespace).Create(pt.ctx, cm, metav1.CreateOptions{})
	if err != nil {
		pt.t.Fatalf("Error creating ConfigMap: %v", err)
	}

	pt.cleanup = append(pt.cleanup, func() {
		err := pt.client.Core().CoreV1().ConfigMaps(namespace).Delete(pt.ctx, name, metav1.DeleteOptions{})
		if err != nil {
			pt.t.Errorf("Error deleting ConfigMap: %v", err)
		}
	})

	return cm
}

// Gomega returns a new Gomega instance for this test
func (pt *ParallelTest) Gomega() *gomega.WithT {
	return gomega.NewWithT(pt.t)
}

// T returns the testing.T instance
func (pt *ParallelTest) T() *testing.T {
	return pt.t
}

// Ctx returns the context for this test
func (pt *ParallelTest) Ctx() context.Context {
	return pt.ctx
}

// Client returns the Kubernetes client for this test
func (pt *ParallelTest) Client() Client {
	return pt.client
}

// OutputDir returns the output directory for test artifacts
func (pt *ParallelTest) OutputDir() string {
	return filepath.Join("test", "output", pt.t.Name())
}

// BeforeEach registers a function to be run before each test case
// The function will be run after namespace creation but before the test starts
func (pt *ParallelTest) BeforeEach(fn func(namespace *corev1.Namespace) error) *ParallelTest {
	pt.t.Helper()
	pt.beforeEachFns = append(pt.beforeEachFns, fn)
	return pt
}

// testContext holds test-specific data that needs to be shared between BeforeEach and the test
type testContext struct {
	configMap *corev1.ConfigMap
	namespace *corev1.Namespace
}

// WithNamespace creates a new namespace with a unique name based on the test name
// This is a convenience wrapper around BeforeEach for creating and cleaning up namespaces
func (pt *ParallelTest) WithNamespace() *corev1.Namespace {
	name := fmt.Sprintf("test-%s", pt.t.Name())
	name = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, name)
	if len(name) > 63 {
		name = name[:63]
	}
	name = strings.TrimSuffix(name, "-")

	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}

	namespace, err := pt.client.Core().CoreV1().Namespaces().Create(pt.ctx, namespace, metav1.CreateOptions{})
	if err != nil {
		pt.t.Fatalf("Error creating namespace: %v", err)
	}

	pt.cleanup = append(pt.cleanup, func() {
		storeAllPodLogs(pt, namespace)
		storeEvents(pt, namespace)

		err := pt.client.Core().CoreV1().Namespaces().Delete(pt.ctx, namespace.Name, metav1.DeleteOptions{})
		if err != nil {
			pt.t.Errorf("Error deleting namespace: %v", err)
		}
	})

	return namespace
}

// WithConfigMaps creates ConfigMaps in the test namespace with the given files
// This is a convenience wrapper around BeforeEach for the common case of creating ConfigMaps
func (pt *ParallelTest) WithConfigMaps(fileNames ...string) *corev1.ConfigMap {
	var ctx testContext
	pt.BeforeEach(func(namespace *corev1.Namespace) error {
		name := fmt.Sprintf("test-%s", pt.t.Name())
		name = strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
				return r
			}
			return '-'
		}, name)
		if len(name) > 63 {
			name = name[:63]
		}
		name = strings.TrimSuffix(name, "-")

		configMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace.Name,
			},
			Data: make(map[string]string),
		}

		for _, fileName := range fileNames {
			data, err := os.ReadFile(filepath.Join("test", "data", fileName))
			if err != nil {
				pt.t.Fatalf("Error reading file %s: %v", fileName, err)
			}
			configMap.Data[fileName] = string(data)
		}

		configMap, err := pt.client.Core().CoreV1().ConfigMaps(namespace.Name).Create(pt.ctx, configMap, metav1.CreateOptions{})
		if err != nil {
			pt.t.Fatalf("Error creating ConfigMap: %v", err)
		}

		pt.cleanup = append(pt.cleanup, func() {
			err := pt.client.Core().CoreV1().ConfigMaps(namespace.Name).Delete(pt.ctx, configMap.Name, metav1.DeleteOptions{})
			if err != nil {
				pt.t.Errorf("Error deleting ConfigMap: %v", err)
			}
		})

		ctx.configMap = configMap
		return nil
	})
	return ctx.configMap
}

// WithResourceQuota creates a resource quota in the test namespace
// This is a convenience wrapper around BeforeEach for the common case of creating resource quotas
func (pt *ParallelTest) WithResourceQuota(name string, cpu string, memory string) {
	pt.BeforeEach(func(namespace *corev1.Namespace) error {
		quota := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace.Name,
			},
			Spec: corev1.ResourceQuotaSpec{
				Hard: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(cpu),
					corev1.ResourceMemory: resource.MustParse(memory),
				},
			},
		}

		quota, err := pt.client.Core().CoreV1().ResourceQuotas(namespace.Name).Create(pt.ctx, quota, metav1.CreateOptions{})
		if err != nil {
			pt.t.Fatalf("Error creating resource quota: %v", err)
		}

		pt.cleanup = append(pt.cleanup, func() {
			err := pt.client.Core().CoreV1().ResourceQuotas(namespace.Name).Delete(pt.ctx, quota.Name, metav1.DeleteOptions{})
			if err != nil {
				pt.t.Errorf("Error deleting resource quota: %v", err)
			}
		})

		return nil
	})
}

// WithFinalizers adds finalizers to the given pod templates
// This is a convenience wrapper around BeforeEach for the common case of adding finalizers
func (pt *ParallelTest) WithFinalizers(finalizer string) {
	pt.BeforeEach(func(namespace *corev1.Namespace) error {
		pt.cleanup = append(pt.cleanup, func() {
			pods, err := pt.client.Core().CoreV1().Pods(namespace.Name).List(pt.ctx, metav1.ListOptions{})
			if err != nil {
				pt.t.Errorf("Error listing pods: %v", err)
				return
			}
			for _, pod := range pods.Items {
				patchBytes := []byte(`{"metadata":{"finalizers":[]}}`)
				_, err := pt.client.Core().CoreV1().Pods(namespace.Name).Patch(pt.ctx, pod.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
				if err != nil {
					pt.t.Errorf("Error removing finalizer from pod %s/%s: %v", namespace.Name, pod.Name, err)
				}
			}
		})
		return nil
	})
}
