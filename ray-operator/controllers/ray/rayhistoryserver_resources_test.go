package ray

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1" //nolint:staticcheck // RayHistoryServer is intentionally introduced in the v1alpha1 API.
)

func TestHistoryServerResourcesSecureExposure(t *testing.T) {
	hs := historyServerTestCR()
	hs.Spec.Storage.S3.Endpoint = "https://s3.example.test:9443"
	input := historyServerInputs{Namespace: "operator", Image: "history:tested", ProxyImage: "proxy:tested", TLSSecretName: "serving-cert", Revision: "revision-1", ProxyTLSArgs: []string{"--tls-min-version=VersionTLS12"}, Env: []corev1.EnvVar{{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "source"}, Key: "AWS_SECRET_ACCESS_KEY"}}}}}
	objects, err := historyServerResources(hs, input)
	require.NoError(t, err)
	require.Len(t, objects, 6)
	for _, object := range objects {
		switch obj := object.(type) {
		case *corev1.Service:
			require.Equal(t, corev1.ServiceTypeClusterIP, obj.Spec.Type)
			require.Len(t, obj.Spec.Ports, 1)
			require.EqualValues(t, 443, obj.Spec.Ports[0].Port)
			require.Equal(t, "https", obj.Spec.Ports[0].TargetPort.StrVal)
		case *appsv1.Deployment:
			pod := obj.Spec.Template.Spec
			require.Equal(t, "revision-1", obj.Spec.Template.Annotations["ray.io/history-server-revision"])
			require.True(t, *pod.SecurityContext.RunAsNonRoot)
			require.Nil(t, pod.SecurityContext.RunAsUser)
			require.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, pod.SecurityContext.SeccompProfile.Type)
			require.Len(t, pod.Containers, 2)
			server, proxy := pod.Containers[0], pod.Containers[1]
			require.Equal(t, input.Env, server.Env)
			require.Empty(t, server.EnvFrom)
			require.Contains(t, server.Args, "--enable-live-clusters=false")
			require.Equal(t, "/readz", server.ReadinessProbe.HTTPGet.Path)
			require.Equal(t, "/livez", server.LivenessProbe.HTTPGet.Path)
			require.Contains(t, proxy.Args, "--upstream=http://127.0.0.1:8080/")
			require.Contains(t, proxy.Args, "--tls-cert-file=/etc/tls/tls.crt")
			require.Contains(t, proxy.Args, input.ProxyTLSArgs[0])
			for _, container := range pod.Containers {
				require.False(t, *container.SecurityContext.AllowPrivilegeEscalation)
				require.True(t, *container.SecurityContext.ReadOnlyRootFilesystem)
				require.Contains(t, container.SecurityContext.Capabilities.Drop, corev1.Capability("ALL"))
				for _, mount := range container.VolumeMounts {
					require.Empty(t, mount.SubPath)
				}
			}
		case *networkingv1.NetworkPolicy:
			require.Len(t, obj.Spec.Ingress, 1)
			require.Len(t, obj.Spec.Ingress[0].Ports, 1)
			require.EqualValues(t, 8443, obj.Spec.Ingress[0].Ports[0].Port.IntVal)
			ports := []int32{}
			for _, port := range obj.Spec.Egress[0].Ports {
				ports = append(ports, port.Port.IntVal)
			}
			require.Contains(t, ports, int32(9443))
			require.Contains(t, ports, int32(6443))
			require.Contains(t, ports, int32(5353))
			require.NotContains(t, ports, int32(8080))
		case *corev1.ConfigMap:
			require.Contains(t, obj.Data["config.yaml"], "verb: access")
			require.Contains(t, obj.Data["config.yaml"], "resource: rayhistoryservers")
			require.Contains(t, obj.Data["config.yaml"], "apiGroup: ray.io")
			require.Contains(t, obj.Data["config.yaml"], "resourceName: default")
			require.NotContains(t, obj.Data["config.yaml"], "group: ray.io")
			require.NotContains(t, obj.Data["config.yaml"], "name: default")
			require.NotContains(t, obj.Data["config.yaml"], "namespace:")
		case *rbacv1.ClusterRoleBinding:
			require.Equal(t, "system:auth-delegator", obj.RoleRef.Name)
			require.Empty(t, obj.Namespace)
			require.Equal(t, "operator", obj.Subjects[0].Namespace)
		}
	}
}

func TestHistoryServerResourceOverrides(t *testing.T) {
	hs := historyServerTestCR()
	hs.Spec.Deployment = &rayv1alpha1.HistoryServerDeployment{Replicas: new(int32(3)), Resources: &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")}}}
	hs.Spec.Security = &rayv1alpha1.HistoryServerSecurity{CABundle: []byte("public-ca")}
	input := historyServerInputs{Namespace: "operator", Image: "history:tested", ProxyImage: "proxy:tested", TLSSecretName: "cert"}
	objects, err := historyServerResources(hs, input)
	require.NoError(t, err)
	for _, object := range objects {
		switch obj := object.(type) {
		case *appsv1.Deployment:
			require.EqualValues(t, 3, *obj.Spec.Replicas)
			require.Equal(t, *hs.Spec.Deployment.Resources, obj.Spec.Template.Spec.Containers[0].Resources)
		case *corev1.ConfigMap:
			require.Equal(t, "public-ca", obj.Data["ca.crt"])
		}
	}
	hs.Spec.Deployment.Replicas = new(int32(0))
	_, err = historyServerResources(hs, input)
	require.Error(t, err)
}
