package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

type VirtualNodeReconciler struct {
	client           client.Client
	mgr              manager.Manager
	nodeName         string
	leaseClient      coordinationclient.LeaseInterface
	leaseDuration    int32
	pingInterval     time.Duration
	statusInterval   time.Duration
	nodeStatusUpdate chan *corev1.Node
	serverNode       *corev1.Node
	nodeLock         sync.Mutex
}

func (r *VirtualNodeReconciler) SetupWithManager(mgr manager.Manager) error {
	r.client = mgr.GetClient()
	r.mgr = mgr
	r.leaseClient = coordinationclient.NewForConfigOrDie(mgr.GetConfig()).Leases(corev1.NamespaceNodeLease)
	r.leaseDuration = 40
	r.pingInterval = 10 * time.Second
	r.statusInterval = 1 * time.Minute
	r.nodeStatusUpdate = make(chan *corev1.Node, 1)
	return nil
}

func (r *VirtualNodeReconciler) Start(ctx context.Context) error {
	if err := r.ensureLease(ctx); err != nil {
		return fmt.Errorf("failed to ensure lease: %w", err)
	}

	// Start lease controller
	go r.runLeaseController(ctx)

	// Start status update controller
	go r.runStatusUpdateController(ctx)

	fmt.Println("Virtual node is ready")
	<-ctx.Done()
	return nil
}

func (r *VirtualNodeReconciler) ensureLease(ctx context.Context) error {
	_, err := r.leaseClient.Get(ctx, r.nodeName, metav1.GetOptions{})
	if err == nil {
		return nil // Lease already exists
	}

	if errors.IsNotFound(err) {
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      r.nodeName,
				Namespace: corev1.NamespaceNodeLease,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       ptr.To(r.nodeName),
				LeaseDurationSeconds: ptr.To(r.leaseDuration),
			},
		}
		_, err = r.leaseClient.Create(ctx, lease, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create lease: %w", err)
		}
		fmt.Println("Successfully created lease")
		return nil
	}

	return fmt.Errorf("failed to get lease: %w", err)
}

func (r *VirtualNodeReconciler) runLeaseController(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(float64(r.leaseDuration) * 0.25 * float64(time.Second)))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.renewLease(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (r *VirtualNodeReconciler) renewLease(ctx context.Context) {
	retry.OnError(retry.DefaultRetry, func(error) bool { return true }, func() error {
		lease, err := r.leaseClient.Get(ctx, r.nodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}
		_, err = r.leaseClient.Update(ctx, lease, metav1.UpdateOptions{})
		if err != nil {
			fmt.Printf("Failed to renew lease: %v\n", err)
		}
		return err
	})
}

func (r *VirtualNodeReconciler) runStatusUpdateController(ctx context.Context) {
	ticker := time.NewTicker(r.statusInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.updateNodeStatus(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (r *VirtualNodeReconciler) updateNodeStatus(ctx context.Context) {
	r.nodeLock.Lock()
	node := r.serverNode.DeepCopy()
	r.nodeLock.Unlock()

	updateNodeStatusHeartbeat(node)

	retry.OnError(retry.DefaultRetry, func(error) bool { return true }, func() error {
		existingNode := &corev1.Node{}
		err := r.client.Get(ctx, client.ObjectKey{Name: r.nodeName}, existingNode)
		if err != nil {
			return err
		}
		existingNode.Status = node.Status
		return r.client.Status().Update(ctx, existingNode)
	})
}

func updateNodeStatusHeartbeat(n *corev1.Node) {
	now := metav1.NewTime(time.Now())
	for i := range n.Status.Conditions {
		n.Status.Conditions[i].LastHeartbeatTime = now
	}
}

func main() {
	kubeconfig := ctrl.GetConfigOrDie()
	mgr, err := manager.New(kubeconfig, manager.Options{})
	if err != nil {
		fmt.Printf("Failed to create manager: %v\n", err)
		return
	}

	reconciler := &VirtualNodeReconciler{
		nodeName: "virtual-node-example",
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		fmt.Printf("Failed to set up reconciler with manager: %v\n", err)
		return
	}

	signalHandler := signals.SetupSignalHandler()

	go func() {
		if err := reconciler.Start(signalHandler); err != nil {
			fmt.Printf("Error running virtual node: %v\n", err)
		}
	}()

	if err := mgr.Start(signalHandler); err != nil {
		fmt.Printf("Error starting manager: %v\n", err)
	}
}
