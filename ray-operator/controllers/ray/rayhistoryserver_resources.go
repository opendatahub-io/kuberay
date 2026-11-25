package ray

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1" //nolint:staticcheck // RayHistoryServer is intentionally introduced in the v1alpha1 API.
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

type historyServerInputs struct {
	Namespace, Image, ProxyImage, TLSSecretName, Revision string
	ProxyTLSArgs                                          []string
	Env                                                   []corev1.EnvVar
	Volumes                                               []corev1.Volume
	VolumeMounts                                          []corev1.VolumeMount
}

func historyServerResources(hs *rayv1alpha1.RayHistoryServer, in historyServerInputs) ([]client.Object, error) {
	if in.Namespace == "" || in.Image == "" || in.ProxyImage == "" || in.TLSSecretName == "" {
		return nil, fmt.Errorf("History Server namespace, images and TLS Secret must be configured")
	}
	const name = "ray-history-server"
	labels := func() map[string]string {
		return map[string]string{"app.kubernetes.io/name": name, "app.kubernetes.io/managed-by": "kuberay-operator"}
	}
	metadata := func(objectName string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: objectName, Namespace: in.Namespace, Labels: labels()}
	}
	config := &corev1.ConfigMap{ObjectMeta: metadata(name + "-config"), Data: map[string]string{
		// Custom access permission avoids treating ordinary CR viewers as archive
		// administrators. No user bindings or namespace-tenant claims are made.
		"config.yaml": "authorization:\n  resourceAttributes:\n    group: ray.io\n    resource: rayhistoryservers\n    name: default\n    verb: access\n",
	}}
	if hs.Spec.Security != nil && len(hs.Spec.Security.CABundle) > 0 {
		config.Data["ca.crt"] = string(hs.Spec.Security.CABundle)
	}
	security := func() *corev1.SecurityContext {
		return &corev1.SecurityContext{AllowPrivilegeEscalation: new(false), ReadOnlyRootFilesystem: new(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	}
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
	}
	replicas := int32(1)
	if hs.Spec.Deployment != nil {
		if hs.Spec.Deployment.Replicas != nil {
			replicas = *hs.Spec.Deployment.Replicas
		}
		if hs.Spec.Deployment.Resources != nil {
			resources = *hs.Spec.Deployment.Resources.DeepCopy()
		}
	}
	if replicas < 1 || replicas > 3 {
		return nil, fmt.Errorf("History Server replicas must be between 1 and 3")
	}
	server := corev1.Container{
		Name: "history-server", Image: in.Image, ImagePullPolicy: corev1.PullIfNotPresent,
		Args: []string{"--enable-live-clusters=false"}, Env: in.Env, Resources: resources, SecurityContext: security(),
		Ports:          []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
		VolumeMounts:   append([]corev1.VolumeMount{{Name: "tmp", MountPath: "/tmp"}}, in.VolumeMounts...),
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readz", Port: intstr.FromInt32(8080)}}, PeriodSeconds: 10, TimeoutSeconds: 5},
		LivenessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/livez", Port: intstr.FromInt32(8080)}}, InitialDelaySeconds: 30, PeriodSeconds: 20, TimeoutSeconds: 5},
	}
	proxy := corev1.Container{
		Name: "kube-rbac-proxy", Image: in.ProxyImage, ImagePullPolicy: corev1.PullIfNotPresent,
		Args:         append([]string{"--secure-listen-address=0.0.0.0:8443", "--upstream=http://127.0.0.1:8080/", "--config-file=/etc/kube-rbac-proxy/config.yaml", "--tls-cert-file=/etc/tls/tls.crt", "--tls-private-key-file=/etc/tls/tls.key", "--logtostderr=true"}, in.ProxyTLSArgs...),
		Ports:        []corev1.ContainerPort{{Name: "https", ContainerPort: 8443}},
		VolumeMounts: []corev1.VolumeMount{{Name: "proxy-config", MountPath: "/etc/kube-rbac-proxy", ReadOnly: true}, {Name: "serving-tls", MountPath: "/etc/tls", ReadOnly: true}},
		Resources:    utils.StandardProxyResources(), SecurityContext: security(),
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(8443)}}, PeriodSeconds: 10},
	}
	volumes := append([]corev1.Volume{
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "proxy-config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: config.Name}, Items: []corev1.KeyToPath{{Key: "config.yaml", Path: "config.yaml"}}}}},
		{Name: "serving-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: in.TLSSecretName}}},
	}, in.Volumes...)
	deployment := &appsv1.Deployment{ObjectMeta: metadata(name), Spec: appsv1.DeploymentSpec{
		Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: labels()},
		Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels(), Annotations: map[string]string{"ray.io/history-server-revision": in.Revision}}, Spec: corev1.PodSpec{
			ServiceAccountName: name,
			SecurityContext:    &corev1.PodSecurityContext{RunAsNonRoot: new(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers:         []corev1.Container{server, proxy}, Volumes: volumes,
		}},
	}}
	service := &corev1.Service{ObjectMeta: metadata(name), Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: labels(), Ports: []corev1.ServicePort{{Name: "https", Port: 443, TargetPort: intstr.FromString("https"), Protocol: corev1.ProtocolTCP}}}}
	ports, err := historyServerEgressPorts(hs)
	if err != nil {
		return nil, err
	}
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metadata(name), Spec: networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: labels()}, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
		Ingress: []networkingv1.NetworkPolicyIngressRule{{Ports: []networkingv1.NetworkPolicyPort{{Protocol: ptr.To(corev1.ProtocolTCP), Port: new(intstr.FromInt32(8443))}}}},
		Egress:  []networkingv1.NetworkPolicyEgressRule{{Ports: ports}},
	}}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-auth-delegator", Labels: labels()},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "system:auth-delegator"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: name, Namespace: in.Namespace}},
	}
	// Policy/auth must exist before publishing the Service or creating Pods.
	return []client.Object{&corev1.ServiceAccount{ObjectMeta: metadata(name)}, config, binding, policy, service, deployment}, nil
}

func historyServerEgressPorts(hs *rayv1alpha1.RayHistoryServer) ([]networkingv1.NetworkPolicyPort, error) {
	ports := map[int]bool{443: true, 6443: true} // storage HTTPS and API-server DNAT
	endpoints := []string{}
	if hs.Spec.Storage != nil {
		if hs.Spec.Storage.S3 != nil {
			endpoints = append(endpoints, hs.Spec.Storage.S3.Endpoint)
		}
		if hs.Spec.Storage.AzureBlob != nil {
			endpoints = append(endpoints, hs.Spec.Storage.AzureBlob.AccountURL)
		}
	}
	for _, endpoint := range endpoints {
		if endpoint == "" {
			continue
		}
		if err := historyServerHTTPSURL(endpoint); err != nil {
			return nil, err
		}
		u, _ := url.Parse(endpoint)
		if u.Port() == "" {
			continue
		}
		port, err := strconv.Atoi(u.Port())
		if err != nil || port < 1 || port > 65535 {
			return nil, fmt.Errorf("backend endpoint has an invalid port")
		}
		ports[port] = true
	}
	sorted := make([]int, 0, len(ports))
	for port := range ports {
		sorted = append(sorted, port)
	}
	sort.Ints(sorted)
	result := []networkingv1.NetworkPolicyPort{{Protocol: ptr.To(corev1.ProtocolUDP), Port: new(intstr.FromInt32(53))}, {Protocol: ptr.To(corev1.ProtocolTCP), Port: new(intstr.FromInt32(53))}}
	// OpenShift DNS commonly translates Service port 53 to pod port 5353.
	result = append(result, networkingv1.NetworkPolicyPort{Protocol: ptr.To(corev1.ProtocolUDP), Port: new(intstr.FromInt32(5353))}, networkingv1.NetworkPolicyPort{Protocol: ptr.To(corev1.ProtocolTCP), Port: new(intstr.FromInt32(5353))})
	for _, port := range sorted {
		result = append(result, networkingv1.NetworkPolicyPort{Protocol: ptr.To(corev1.ProtocolTCP), Port: new(intstr.FromInt(port))})
	}
	return result, nil
}
