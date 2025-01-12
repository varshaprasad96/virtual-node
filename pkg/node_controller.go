package controllers

import (
	"context"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	virtualNodeLabelKey = "type"
	virtualNodeLabelVal = "virtualNode"
)

// VirtualNodeReconciler manages virtual nodes and their leases
type VirtualNodeReconciler struct {
	client        client.Client
	leaseDuration int32
	renewInterval time.Duration
	clock         clock.Clock
}

// Reconcile ensures a virtual node is created and has an active lease.
func (r *VirtualNodeReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the node object
	node := &corev1.Node{}
	if err := r.client.Get(ctx, req.NamespacedName, node); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return reconcile.Result{}, err
		}
		logger.Info("Node not found, skipping", "nodeName", req.Name)
		return reconcile.Result{}, nil
	}

	// Check if it's a virtual node by label
	if val, ok := node.Labels[virtualNodeLabelKey]; !ok || val != virtualNodeLabelVal {
		logger.Info("Skipping non-virtual node", "nodeName", node.Name)
		return reconcile.Result{}, nil
	}

	// Ensure lease exists for the virtual node
	err := r.ensureLease(ctx, node)
	if err != nil {
		logger.Error(err, "Failed to ensure lease", "nodeName", node.Name)
		return reconcile.Result{}, err
	}

	logger.Info("Successfully reconciled virtual node", "nodeName", node.Name)
	return reconcile.Result{RequeueAfter: r.renewInterval}, nil
}

// ensureLease ensures a lease is created and periodically renewed for the virtual node
func (r *VirtualNodeReconciler) ensureLease(ctx context.Context, node *corev1.Node) error {
	logger := log.FromContext(ctx)

	lease := &coordinationv1.Lease{}
	err := r.client.Get(ctx, client.ObjectKey{Name: node.Name, Namespace: corev1.NamespaceNodeLease}, lease)
	if apierrors.IsNotFound(err) {
		// Lease does not exist, create a new one
		lease = &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      node.Name,
				Namespace: corev1.NamespaceNodeLease,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: corev1.SchemeGroupVersion.String(),
						Kind:       "Node",
						Name:       node.Name,
						UID:        node.UID,
					},
				},
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &node.Name,
				LeaseDurationSeconds: &r.leaseDuration,
				RenewTime:            &metav1.MicroTime{Time: r.clock.Now()},
			},
		}
		if err := r.client.Create(ctx, lease); err != nil {
			return fmt.Errorf("failed to create lease: %w", err)
		}
		logger.Info("Successfully created lease", "nodeName", node.Name)
	} else if err != nil {
		return err
	}

	// Update the lease to renew it
	lease.Spec.RenewTime = &metav1.MicroTime{Time: r.clock.Now()}
	if err := r.client.Update(ctx, lease); err != nil {
		return fmt.Errorf("failed to update lease: %w", err)
	}

	logger.Info("Successfully renewed lease", "nodeName", node.Name)
	return nil
}

// SetupWithManager sets up the controller with the Manager and watches for Node resources.
func (r *VirtualNodeReconciler) SetupWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			node, ok := obj.(*corev1.Node)
			if !ok {
				return false
			}
			// Filter only virtual nodes based on label
			val, exists := node.Labels[virtualNodeLabelKey]
			return exists && val == virtualNodeLabelVal
		})).
		Complete(r)
}
