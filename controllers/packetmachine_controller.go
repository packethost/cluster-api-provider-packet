/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/packethost/packngo"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	infrastructurev1alpha3 "sigs.k8s.io/cluster-api-provider-packet/api/v1alpha3"
	packet "sigs.k8s.io/cluster-api-provider-packet/pkg/cloud/packet"
	"sigs.k8s.io/cluster-api-provider-packet/pkg/cloud/packet/scope"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1alpha3"
	capierrors "sigs.k8s.io/cluster-api/errors"
	"sigs.k8s.io/cluster-api/util"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	force = true
)

// PacketMachineReconciler reconciles a PacketMachine object
type PacketMachineReconciler struct {
	client.Client
	Log          logr.Logger
	Recorder     record.EventRecorder
	Scheme       *runtime.Scheme
	PacketClient *packet.PacketClient
}

// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=packetmachines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=packetmachines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines;machines/status,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets;,verbs=get;list;watch

func (r *PacketMachineReconciler) Reconcile(req ctrl.Request) (_ ctrl.Result, reterr error) {
	ctx := context.Background()
	logger := r.Log.WithValues("packetmachine", req.NamespacedName)

	// your logic here
	packetmachine := &infrastructurev1alpha3.PacketMachine{}
	if err := r.Get(ctx, req.NamespacedName, packetmachine); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger = logger.WithName(packetmachine.APIVersion)

	// Fetch the Machine.
	machine, err := util.GetOwnerMachine(ctx, r.Client, packetmachine.ObjectMeta)
	if err != nil {
		return ctrl.Result{}, err
	}
	if machine == nil {
		logger.Info("Machine Controller has not yet set OwnerRef")
		return ctrl.Result{}, nil
	}

	logger = logger.WithValues("machine", machine.Name)

	// Fetch the Cluster.
	cluster, err := util.GetClusterFromMetadata(ctx, r.Client, machine.ObjectMeta)
	if err != nil {
		logger.Info("Machine is missing cluster label or cluster does not exist")
		return ctrl.Result{}, nil
	}

	logger = logger.WithValues("cluster", cluster.Name)

	if util.IsPaused(cluster, machine) {
		logger.Info("PacketMachine or linked Cluster is marked as paused. Won't reconcile")
		return ctrl.Result{}, nil
	}

	packetcluster := &infrastructurev1alpha3.PacketCluster{}
	packetclusterNamespacedName := client.ObjectKey{
		Namespace: packetmachine.Namespace,
		Name:      cluster.Spec.InfrastructureRef.Name,
	}
	if err := r.Get(ctx, packetclusterNamespacedName, packetcluster); err != nil {
		logger.Info("PacketCluster is not available yet")
		return ctrl.Result{}, nil
	}

	logger = logger.WithValues("packetcluster", packetcluster.Name)

	// Create the cluster scope
	clusterScope, err := scope.NewClusterScope(scope.ClusterScopeParams{
		Client:        r.Client,
		Logger:        logger,
		Cluster:       cluster,
		PacketCluster: packetcluster,
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	// Create the machine scope
	machineScope, err := scope.NewMachineScope(scope.MachineScopeParams{
		Logger:        logger,
		Client:        r.Client,
		Cluster:       cluster,
		Machine:       machine,
		PacketCluster: packetcluster,
		PacketMachine: packetmachine,
	})
	if err != nil {
		return ctrl.Result{}, errors.Errorf("failed to create scope: %+v", err)
	}

	// Always close the scope when exiting this function so we can persist any PacketMachine changes.
	defer func() {
		if err := machineScope.Close(); err != nil && reterr == nil {
			reterr = err
		}
	}()

	// Handle deleted machines
	if !packetmachine.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, machineScope, clusterScope, logger)
	}

	return r.reconcile(ctx, machineScope, clusterScope, logger)
}

func (r *PacketMachineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&infrastructurev1alpha3.PacketMachine{}).
		Watches(
			&source.Kind{Type: &clusterv1.Machine{}},
			&handler.EnqueueRequestsFromMapFunc{
				ToRequests: util.MachineToInfrastructureMapFunc(infrastructurev1alpha3.GroupVersion.WithKind("PacketMachine")),
			},
		).
		Complete(r)
}

func (r *PacketMachineReconciler) reconcile(ctx context.Context, machineScope *scope.MachineScope, clusterScope *scope.ClusterScope, logger logr.Logger) (ctrl.Result, error) {
	logger.Info("Reconciling PacketMachine")

	packetmachine := machineScope.PacketMachine
	// If the PacketMachine is in an error state, return early.
	if packetmachine.Status.ErrorReason != nil || packetmachine.Status.ErrorMessage != nil {
		machineScope.Info("Error state detected, skipping reconciliation")
		return ctrl.Result{}, nil
	}

	// If the PacketMachine doesn't have our finalizer, add it.
	controllerutil.AddFinalizer(packetmachine, infrastructurev1alpha3.MachineFinalizer)

	if !machineScope.Cluster.Status.InfrastructureReady {
		machineScope.Info("Cluster infrastructure is not ready yet")
		return ctrl.Result{}, nil
	}

	// Make sure bootstrap data secret is available and populated.
	if machineScope.Machine.Spec.Bootstrap.DataSecretName == nil {
		machineScope.Info("Bootstrap data secret is not yet available")
		return ctrl.Result{}, nil
	}

	providerID := machineScope.GetInstanceID()
	defaultTags := defaultTags(machineScope.Namespace(), machineScope.Name(), clusterScope.Name())

	var (
		dev                  *packngo.Device
		addrs                []corev1.NodeAddress
		err                  error
		controlPlaneEndpoint packngo.IPAddressReservation
	)

	if providerID != "" {
		// If we already have a providerID, then retrieve the device using the
		// providerID. This means that the Machine has already been created
		// and we successfully recorded the providerID.
		dev, err = r.PacketClient.GetDevice(providerID)
		if err != nil {
			var perr *packngo.ErrorResponse
			if errors.As(err, &perr) && perr.Response != nil && perr.Response.StatusCode == http.StatusNotFound {
				errs := fmt.Errorf("device does not exist: %s", providerID)

				machineScope.SetErrorReason(capierrors.UpdateMachineError)
				machineScope.SetErrorMessage(errs)
				return ctrl.Result{}, errs
			}

			return ctrl.Result{}, err
		}
	}

	if dev == nil {
		// We don't yet have a providerID, check to see if we've already
		// created a device by using the tags that we assign to devices
		// on creation.
		dev, err = r.PacketClient.GetDeviceByTags(
			machineScope.PacketCluster.Spec.ProjectID,
			defaultTags,
		)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	if dev == nil {
		// We weren't able to find a device by either providerID or by tags,
		// so we need to create a new device.
		createDeviceReq := packet.CreateDeviceRequest{
			ExtraTags:    defaultTags,
			MachineScope: machineScope,
		}

		// when the node is a control plan we should check if the elastic ip
		// for this cluster is not assigned. If it is free we can prepare the
		// current node to use it.
		if machineScope.IsControlPlane() {
			controlPlaneEndpoint, _ = r.PacketClient.GetIPByClusterIdentifier(
				clusterScope.Namespace(),
				clusterScope.Name(),
				clusterScope.PacketCluster.Spec.ProjectID)
			if len(controlPlaneEndpoint.Assignments) == 0 {
				a := corev1.NodeAddress{
					Type:    corev1.NodeExternalIP,
					Address: controlPlaneEndpoint.Address,
				}
				addrs = append(addrs, a)
			}
			createDeviceReq.ControlPlaneEndpoint = controlPlaneEndpoint.Address
		}

		dev, err = r.PacketClient.NewDevice(createDeviceReq)

		switch {
		// TODO: find a better way than parsing the error messages for this.
		case err != nil && strings.Contains(err.Error(), " no available hardware reservations "):
			// Do not treat an error indicating there are no hardware
			// reservations available as fatal, we should continue to retry
			// device creation until a reservation is available
			return ctrl.Result{}, fmt.Errorf("failed to create machine %s: %w", machineScope.Name(), err)
		case err != nil:
			errs := fmt.Errorf("failed to create machine %s: %w", machineScope.Name(), err)
			machineScope.SetErrorReason(capierrors.CreateMachineError)
			machineScope.SetErrorMessage(errs)
			return ctrl.Result{}, errs
		}
	}

	// we do not need to set this as packet://<id> because SetProviderID() does the formatting for us
	machineScope.SetProviderID(dev.ID)
	machineScope.SetInstanceStatus(infrastructurev1alpha3.PacketResourceStatus(dev.State))

	deviceAddr, err := r.PacketClient.GetDeviceAddresses(dev)
	if err != nil {
		machineScope.SetErrorMessage(errors.New("failed to getting device addresses"))
		return ctrl.Result{}, err
	}

	machineScope.SetAddresses(append(addrs, deviceAddr...))

	// Proceed to reconcile the PacketMachine state.
	var result reconcile.Result

	switch infrastructurev1alpha3.PacketResourceStatus(dev.State) {
	case infrastructurev1alpha3.PacketResourceStatusNew, infrastructurev1alpha3.PacketResourceStatusQueued, infrastructurev1alpha3.PacketResourceStatusProvisioning:
		machineScope.Info("Machine instance is pending", "instance-id", machineScope.GetInstanceID())
		result = ctrl.Result{RequeueAfter: 10 * time.Second}
	case infrastructurev1alpha3.PacketResourceStatusRunning:
		machineScope.Info("Machine instance is active", "instance-id", machineScope.GetInstanceID())

		// This logic is here because an elastic ip can be assigned only an
		// active node. It needs to be a control plane and the IP should not be
		// assigned to anything at this point.
		controlPlaneEndpoint, _ = r.PacketClient.GetIPByClusterIdentifier(
			clusterScope.Namespace(),
			clusterScope.Name(),
			clusterScope.PacketCluster.Spec.ProjectID)
		if len(controlPlaneEndpoint.Assignments) == 0 && machineScope.IsControlPlane() {
			if _, _, err := r.PacketClient.DeviceIPs.Assign(dev.ID, &packngo.AddressStruct{
				Address: controlPlaneEndpoint.Address,
			}); err != nil {
				r.Log.Error(err, "err assigining elastic ip to control plane. retrying...")
				return ctrl.Result{RequeueAfter: time.Second * 20}, nil
			}
		}
		machineScope.SetReady()
		result = ctrl.Result{}
	default:
		machineScope.SetErrorReason(capierrors.UpdateMachineError)
		machineScope.SetErrorMessage(errors.Errorf("Instance status %q is unexpected", dev.State))
		result = ctrl.Result{}
	}

	return result, nil
}

func (r *PacketMachineReconciler) reconcileDelete(ctx context.Context, machineScope *scope.MachineScope, clusterScope *scope.ClusterScope, logger logr.Logger) (ctrl.Result, error) {
	logger.Info("Deleting machine")

	packetmachine := machineScope.PacketMachine
	providerID := machineScope.GetInstanceID()
	defaultTags := defaultTags(machineScope.Namespace(), machineScope.Name(), clusterScope.Name())

	if providerID == "" {
		logger.Info("no provider ID provided, checking for device by tags")

		device, err := r.PacketClient.GetDeviceByTags(machineScope.PacketCluster.Spec.ProjectID, defaultTags)
		if err != nil {
			return ctrl.Result{}, err
		}

		if device != nil {
			if err := r.deleteDevice(device.ID, force); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to delete the machine: %w", err)
			}
		}

		controllerutil.RemoveFinalizer(packetmachine, infrastructurev1alpha3.MachineFinalizer)
		return ctrl.Result{}, nil
	}

	device, err := r.PacketClient.GetDevice(providerID)
	if err != nil {
		var perr *packngo.ErrorResponse
		if errors.As(err, &perr) && perr.Response != nil && perr.Response.StatusCode == http.StatusNotFound {
			// When the server does not exist we do not have anything left to do.
			// Probably somebody manually deleted the server from the UI or via API.
			logger.Info("Server not found, nothing left to do")
			controllerutil.RemoveFinalizer(packetmachine, infrastructurev1alpha3.MachineFinalizer)
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, fmt.Errorf("error retrieving machine status %s: %w", packetmachine.Name, err)
	}

	// We should never get there but this is a safetly check
	if device == nil {
		controllerutil.RemoveFinalizer(packetmachine, infrastructurev1alpha3.MachineFinalizer)
		return ctrl.Result{}, fmt.Errorf("machine does not exist: %s", packetmachine.Name)
	}

	if err := r.deleteDevice(device.ID, force); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to delete the machine: %v", err)
	}

	controllerutil.RemoveFinalizer(packetmachine, infrastructurev1alpha3.MachineFinalizer)
	return ctrl.Result{}, nil
}

func (r *PacketMachineReconciler) deleteDevice(id string, force bool) error {
	if _, err := r.PacketClient.Devices.Delete(id, force); err != nil {
		return err
	}

	return nil
}

func defaultTags(namespace, name, clusterName string) []string {
	return []string{
		packet.GenerateClusterTag(clusterName),
		packet.GenerateMachineNameTag(name),
		packet.GenerateNamespaceTag(namespace),
	}
}
