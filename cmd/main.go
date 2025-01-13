package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	nodeController "github/varshaprasad96/virtual-node/pkg"
	"k8s.io/utils/clock"
)

func main() {
	var leaseDuration int
	var renewInterval time.Duration

	flag.IntVar(&leaseDuration, "lease-duration", 40, "Duration of the node lease in seconds")
	flag.DurationVar(&renewInterval, "renew-interval", 10*time.Second, "Interval at which to renew the node lease")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{})
	if err != nil {
		ctrl.Log.Error(err, "Unable to start manager")
		os.Exit(1)
	}

	// Start the nodePingController with a 10-second ping interval
	if err := nodeController.NewNodePingController(mgr, 10*time.Second); err != nil {
		fmt.Printf("Failed to create node ping controller: %v\n", err)
		os.Exit(1)
	}

	reconciler := &nodeController.VirtualNodeReconciler{
		Client:        mgr.GetClient(),
		LeaseDuration: int32(leaseDuration),
		RenewInterval: renewInterval,
		Clock:         clock.RealClock{},
	}

	if err := reconciler.SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "Unable to create controller", "controller", "VirtualNode")
		os.Exit(1)
	}

	ctrl.Log.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.Error(err, "Problem running manager")
		os.Exit(1)
	}
	fmt.Println("Running the virtual node successfully")
}
