package ray

import (
	"context"
	"fmt"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

const (
	podMonitorOptOutAnnotation = "odh.ray.io/cluster-podmonitor"
	podMonitorScrapeLabel      = "monitoring.opendatahub.io/scrape"
	podMonitorRequeueInterval  = time.Minute
)

// PodMonitorController manages optional monitoring independently of RayCluster readiness.
// CARRY: RHAI-23206 - downstream scrape-by-default integration with OpenShift UWM.
type PodMonitorController struct {
	client.Client
	APIReader client.Reader
	Recorder  events.EventRecorder
}

// NewPodMonitorController uses direct reads to avoid starting an informer for an optional CRD.
func NewPodMonitorController(mgr ctrl.Manager) *PodMonitorController {
	return &PodMonitorController{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Recorder:  mgr.GetEventRecorder("podmonitor-controller"),
	}
}

// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=podmonitors,verbs=get;create;update;delete
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters;rayjobs,verbs=get;list;watch

// Reconcile creates, repairs or removes only this RayCluster's owned PodMonitor.
func (r *PodMonitorController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cluster := &rayv1.RayCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if utils.ManagedByExternalController(cluster.Spec.ManagedBy) != nil {
		return ctrl.Result{}, nil
	}
	if err := r.reconcilePodMonitor(ctx, cluster); err != nil {
		logger := ctrl.LoggerFrom(ctx)
		if meta.IsNoMatchError(err) || errors.IsNotFound(err) {
			logger.V(1).Info("PodMonitor API is unavailable; skipping monitoring", "error", err)
		} else {
			logger.Error(err, "Unable to reconcile PodMonitor; RayCluster reconciliation is unaffected")
			r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "PodMonitorReconcileFailed", string(utils.ReconcileAction), "%s", err)
		}
	}

	// Periodic reconciliation repairs drift, observes RayJob annotation changes and
	// recovers when monitoring is installed later, without an optional-CRD watch.
	// shortcut: missing-API discovery is per cluster; share backoff if API load warrants it.
	return ctrl.Result{RequeueAfter: podMonitorRequeueInterval}, nil
}

func (r *PodMonitorController) reconcilePodMonitor(ctx context.Context, cluster *rayv1.RayCluster) error {
	// Finalizers can delay cluster deletion; stop scraping without waiting for GC.
	disabled := cluster.DeletionTimestamp != nil || !features.Enabled(features.RayClusterPodMonitor)
	if !disabled {
		disabled = cluster.Annotations[podMonitorOptOutAnnotation] == "disabled"
		if owner := podMonitorRayJobOwner(cluster); owner != nil {
			job := &rayv1.RayJob{}
			err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: owner.Name}, job)
			if err != nil && !errors.IsNotFound(err) {
				return err
			}
			if err == nil && job.UID == owner.UID {
				// RayJob annotations are copied at cluster creation. Use the live
				// parent value so removing an opt-out does not leave a stale copy.
				disabled = job.Annotations[podMonitorOptOutAnnotation] == "disabled"
			}
		}
	}

	monitor := &monitoringv1.PodMonitor{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-podmonitor"}
	err := r.APIReader.Get(ctx, key, monitor)
	if errors.IsNotFound(err) {
		if disabled {
			return nil
		}
		desired := desiredPodMonitor(cluster)
		if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme(), controllerutil.WithBlockOwnerDeletion(false)); err != nil {
			return err
		}
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if !metav1.IsControlledBy(monitor, cluster) {
		message := fmt.Sprintf("PodMonitor %s already exists and is not owned by this RayCluster; skipping", key)
		ctrl.LoggerFrom(ctx).Info(message)
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "PodMonitorOwnershipConflict", string(utils.ReconcileAction), "%s", message)
		return nil
	}
	// Optional monitoring must not hold up foreground RayCluster deletion.
	if owner := metav1.GetControllerOfNoCopy(monitor); ptr.Deref(owner.BlockOwnerDeletion, false) {
		owner.BlockOwnerDeletion = new(false)
		if err := r.Update(ctx, monitor); err != nil {
			return err
		}
	}
	if disabled {
		// Preconditions prevent deleting a replacement object created after our read.
		return client.IgnoreNotFound(r.Delete(ctx, monitor, client.Preconditions{UID: &monitor.UID, ResourceVersion: &monitor.ResourceVersion}))
	}

	desired := desiredPodMonitor(cluster)
	if equality.Semantic.DeepEqual(monitor.Spec, desired.Spec) && monitor.Labels[podMonitorScrapeLabel] == "false" {
		return nil
	}
	monitor.Spec = desired.Spec
	if monitor.Labels == nil {
		monitor.Labels = make(map[string]string)
	}
	monitor.Labels[podMonitorScrapeLabel] = "false"
	return r.Update(ctx, monitor)
}

func desiredPodMonitor(cluster *rayv1.RayCluster) *monitoringv1.PodMonitor {
	endpoint := monitoringv1.PodMetricsEndpoint{
		Port:   ptr.To(utils.MetricsPortName),
		Path:   "/metrics",
		Scheme: ptr.To(monitoringv1.Scheme("http")),
	}
	if owner := podMonitorRayJobOwner(cluster); owner != nil {
		// The RayJob owns this entire cluster; all its head and worker targets
		// share that lineage. Origin labels are not automatically copied to Pods.
		endpoint.RelabelConfigs = []monitoringv1.RelabelConfig{{
			TargetLabel: "rayjob",
			Replacement: new(owner.Name),
			Action:      "replace",
		}}
	}
	return &monitoringv1.PodMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-podmonitor",
			Namespace: cluster.Namespace,
			Labels:    map[string]string{podMonitorScrapeLabel: "false"},
		},
		Spec: monitoringv1.PodMonitorSpec{
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{utils.RayClusterLabelKey: cluster.Name},
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      utils.RayNodeTypeLabelKey,
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{string(rayv1.HeadNode), string(rayv1.WorkerNode)},
				}},
			},
			PodTargetLabels: []string{
				utils.RayClusterLabelKey,
				utils.RayNodeTypeLabelKey,
				utils.RayNodeGroupLabelKey,
			},
			PodMetricsEndpoints: []monitoringv1.PodMetricsEndpoint{endpoint},
		},
	}
}

func podMonitorRayJobOwner(cluster *rayv1.RayCluster) *metav1.OwnerReference {
	owner := metav1.GetControllerOf(cluster)
	if owner != nil && owner.Kind == "RayJob" &&
		(owner.APIVersion == rayv1.GroupVersion.String() || owner.APIVersion == "ray.io/v1alpha1") {
		return owner
	}
	return nil
}

// SetupWithManager deliberately watches only required Ray APIs. An Owns watch
// on PodMonitors would prevent the manager from starting when their CRD is absent.
func (r *PodMonitorController) SetupWithManager(mgr ctrl.Manager, reconcileConcurrency int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("podmonitor").
		For(&rayv1.RayCluster{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: reconcileConcurrency}).
		Complete(r)
}
