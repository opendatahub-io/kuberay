package v1

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionv1beta1 "k8s.io/api/admission/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	//+kubebuilder:scaffold:imports
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

// mockOpenShiftRESTMapper is a mock RESTMapper that simulates OpenShift environment
// by successfully returning a RESTMapping for the Route resource
type mockOpenShiftRESTMapper struct {
	meta.RESTMapper
}

func (m *mockOpenShiftRESTMapper) RESTMapping(_ schema.GroupKind, _ ...string) (*meta.RESTMapping, error) {
	// Simulate OpenShift by returning success for Route resource
	return &meta.RESTMapping{}, nil
}

var (
	cfg       *rest.Config
	k8sClient client.Client
	testEnv   *envtest.Environment
	ctx       context.Context
	cancel    context.CancelFunc
)

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Webhook Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: false,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{filepath.Join("..", "..", "..", "config", "webhook")},
		},
	}

	var err error
	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	scheme := runtime.NewScheme()
	err = rayv1.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())

	err = admissionv1beta1.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())

	//+kubebuilder:scaffold:scheme

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// start webhook server using Manager
	webhookInstallOptions := &testEnv.WebhookInstallOptions
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		WebhookServer: webhook.NewServer(webhook.Options{
			Host:    webhookInstallOptions.LocalServingHost,
			Port:    webhookInstallOptions.LocalServingPort,
			CertDir: webhookInstallOptions.LocalServingCertDir,
		}),
		LeaderElection: false,
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	})
	Expect(err).NotTo(HaveOccurred())

	err = SetupRayClusterDefaulterWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	err = SetupRayClusterValidatorWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	//+kubebuilder:scaffold:webhook

	go func() {
		defer GinkgoRecover()
		err = mgr.Start(ctx)
		Expect(err).NotTo(HaveOccurred())
	}()

	// wait for the webhook server to get ready
	dialer := &net.Dialer{Timeout: time.Second}
	addrPort := fmt.Sprintf("%s:%d", webhookInstallOptions.LocalServingHost, webhookInstallOptions.LocalServingPort)
	Eventually(func() error {
		conn, err := tls.DialWithDialer(dialer,
			"tcp",
			addrPort,
			&tls.Config{InsecureSkipVerify: true}, //nolint:gosec // Allow InsecureSkipVerify because we are connecting to our own webhook server.
		)
		if err != nil {
			return err
		}
		conn.Close()
		return nil
	}).Should(Succeed())
})

var _ = Describe("RayCluster mutating webhook", func() {
	Context("when running on non-OpenShift (kind)", func() {
		It("should set the secure-trusted-network annotation to false", func() {
			name := fmt.Sprintf("test-raycluster-%d", rand.IntnRange(1000, 9000))
			rayCluster := rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      name,
					// No annotations set initially
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						RayStartParams: map[string]string{},
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "ray-head",
										Image: "rayproject/ray:latest",
									},
								},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{},
				},
			}

			err := k8sClient.Create(context.TODO(), &rayCluster)
			Expect(err).NotTo(HaveOccurred())

			// Verify the annotation was set to "false" by the mutating webhook (not OpenShift)
			created := &rayv1.RayCluster{}
			err = k8sClient.Get(context.TODO(), client.ObjectKey{Name: name, Namespace: "default"}, created)
			Expect(err).NotTo(HaveOccurred())
			Expect(created.Annotations).NotTo(BeNil())
			Expect(created.Annotations["odh.ray.io/secure-trusted-network"]).To(Equal("false"))

			// Cleanup
			err = k8sClient.Delete(context.TODO(), created)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("when running on OpenShift", func() {
		It("should set the secure-trusted-network annotation to true", func() {
			name := fmt.Sprintf("test-raycluster-%d", rand.IntnRange(1000, 9000))
			rayCluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      name,
					// No annotations set initially
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						RayStartParams: map[string]string{},
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "ray-head",
										Image: "rayproject/ray:latest",
									},
								},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{},
				},
			}

			// Create a mock RESTMapper that simulates OpenShift (returns Route resource)
			mockRESTMapper := &mockOpenShiftRESTMapper{}
			defaulter := &RayClusterDefaulter{
				RESTMapper: mockRESTMapper,
			}

			// Call the Default method directly
			err := defaulter.Default(context.TODO(), rayCluster)
			Expect(err).NotTo(HaveOccurred())

			// Verify the annotation was set to "true"
			Expect(rayCluster.Annotations).NotTo(BeNil())
			Expect(rayCluster.Annotations["odh.ray.io/secure-trusted-network"]).To(Equal("true"))
		})

		It("should set enableIngress to false on OpenShift when not explicitly set", func() {
			name := fmt.Sprintf("test-raycluster-%d", rand.IntnRange(1000, 9000))
			rayCluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      name,
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						// EnableIngress not set (nil)
						RayStartParams: map[string]string{},
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "ray-head",
										Image: "rayproject/ray:latest",
									},
								},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{},
				},
			}

			// Create a mock RESTMapper that simulates OpenShift
			mockRESTMapper := &mockOpenShiftRESTMapper{}
			defaulter := &RayClusterDefaulter{
				RESTMapper: mockRESTMapper,
			}

			// Call the Default method directly
			err := defaulter.Default(context.TODO(), rayCluster)
			Expect(err).NotTo(HaveOccurred())

			// Verify enableIngress was set to false
			Expect(rayCluster.Spec.HeadGroupSpec.EnableIngress).NotTo(BeNil())
			Expect(*rayCluster.Spec.HeadGroupSpec.EnableIngress).To(BeFalse(),
				"EnableIngress should be set to false to enforce Gateway-only access")
		})

		It("should enforce enableIngress: false on OpenShift even when user sets true", func() {
			name := fmt.Sprintf("test-raycluster-%d", rand.IntnRange(1000, 9000))
			rayCluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      name,
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						EnableIngress:  ptr.To(true), // User explicitly sets to true
						RayStartParams: map[string]string{},
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "ray-head",
										Image: "rayproject/ray:latest",
									},
								},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{},
				},
			}

			// Create a mock RESTMapper that simulates OpenShift
			mockRESTMapper := &mockOpenShiftRESTMapper{}
			defaulter := &RayClusterDefaulter{
				RESTMapper: mockRESTMapper,
			}

			// Call the Default method directly
			err := defaulter.Default(context.TODO(), rayCluster)
			Expect(err).NotTo(HaveOccurred())

			// Verify enableIngress is forced to false (strict enforcement)
			Expect(rayCluster.Spec.HeadGroupSpec.EnableIngress).NotTo(BeNil())
			Expect(*rayCluster.Spec.HeadGroupSpec.EnableIngress).To(BeFalse(),
				"EnableIngress should be forced to false to enforce Gateway-only access (strict enforcement)")
		})
	})
})

var _ = Describe("RayCluster validating webhook", func() {
	Context("when name is invalid", func() {
		It("should return error", func() {
			rayCluster := rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "invalid.name",
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						RayStartParams: map[string]string{},
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "ray-head",
										Image: "rayproject/ray:latest",
									},
								},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{},
				},
			}

			err := k8sClient.Create(context.TODO(), &rayCluster)
			Expect(err).To(HaveOccurred())

			Expect(err.Error()).To(ContainSubstring("RayCluster.ray.io \"invalid.name\" is invalid: metadata.name:"))
		})
	})

	Context("when groupNames are not unique", func() {
		var name, namespace string
		var rayCluster rayv1.RayCluster

		BeforeEach(func() {
			namespace = "default"
			name = fmt.Sprintf("test-raycluster-%d", rand.IntnRange(1000, 9000))
		})

		It("should return error", func() {
			rayCluster = rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						RayStartParams: map[string]string{},
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "ray-head",
										Image: "rayproject/ray:latest",
									},
								},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
						{
							GroupName:      "group1",
							RayStartParams: map[string]string{},
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "ray-worker",
											Image: "rayproject/ray:latest",
										},
									},
								},
							},
						},
						{
							GroupName:      "group1",
							RayStartParams: map[string]string{},
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "ray-worker",
											Image: "rayproject/ray:latest",
										},
									},
								},
							},
						},
					},
				},
			}

			err := k8sClient.Create(context.TODO(), &rayCluster)
			Expect(err).To(HaveOccurred())

			Expect(err.Error()).To(ContainSubstring("worker group names must be unique"))
		})
	})
})

var _ = AfterSuite(func() {
	cancel()
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})
