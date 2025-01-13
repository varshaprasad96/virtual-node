package controllers

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// nodePingController handles periodic status updates for virtual nodes.
type nodePingController struct {
	Client       client.Client
	PingInterval time.Duration
}

// NewNodePingController creates a new nodePingController.
func NewNodePingController(mgr manager.Manager, pingInterval time.Duration) error {
	reconciler := &nodePingController{
		Client:       mgr.GetClient(),
		PingInterval: pingInterval,
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named("node-ping-controller").
		For(&corev1.Node{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			// Filter only virtual nodes by label
			node, ok := obj.(*corev1.Node)
			if !ok {
				return false
			}
			val, exists := node.Labels[virtualNodeLabelKey]
			return exists && val == virtualNodeLabelVal
		})).
		Complete(reconciler)
}

// Reconcile updates the status of virtual nodes periodically.
func (r *nodePingController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling virtual node", "nodeName", req.Name)

	// Fetch the virtual node
	node := &corev1.Node{}
	if err := r.Client.Get(ctx, req.NamespacedName, node); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return reconcile.Result{}, err
		}
		logger.Info("Node not found, skipping", "nodeName", req.Name)
		return reconcile.Result{}, nil
	}

	// Update the node conditions
	now := metav1.Now()
	node.Status.Conditions = []corev1.NodeCondition{
		{
			Type:               corev1.NodeReady,
			Status:             corev1.ConditionTrue,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
			Reason:             "VirtualNodeReady",
			Message:            "Virtual node is ready",
		},
		{
			Type:               corev1.NodeMemoryPressure,
			Status:             corev1.ConditionFalse,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
			Reason:             "NoMemoryPressure",
			Message:            "No memory pressure detected",
		},
		{
			Type:               corev1.NodeDiskPressure,
			Status:             corev1.ConditionFalse,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
			Reason:             "NoDiskPressure",
			Message:            "No disk pressure detected",
		},
		{
			Type:               corev1.NodePIDPressure,
			Status:             corev1.ConditionFalse,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
			Reason:             "NoPIDPressure",
			Message:            "No PID pressure detected",
		},
	}

	if err := r.Client.Status().Update(ctx, node); err != nil {
		logger.Error(err, "Failed to update node status", "nodeName", req.Name)
		return reconcile.Result{}, err
	}

	logger.Info("Successfully updated virtual node status", "nodeName", req.Name)
	return reconcile.Result{RequeueAfter: r.PingInterval}, nil
}
