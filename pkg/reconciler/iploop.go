package reconciler

import (
	"context"
	"fmt"
	"net"
	"strings"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/k8snetworkplumbingwg/whereabouts/pkg/allocate"
	whereaboutsv1alpha1 "github.com/k8snetworkplumbingwg/whereabouts/pkg/api/whereabouts.cni.cncf.io/v1alpha1"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/logging"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/storage"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/storage/kubernetes"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/types"
)

type ReconcileLooper struct {
	k8sClient              kubernetes.Client
	liveWhereaboutsPods    map[string]podWrapper
	orphanedIPs            []OrphanedIPReservations
	orphanedClusterWideIPs []whereaboutsv1alpha1.OverlappingRangeIPReservation
}

type OrphanedIPReservations struct {
	Pool        storage.IPPool
	Allocations []types.IPReservation
}

func NewReconcileLooperWithClient(k8sClient *kubernetes.Client) (*ReconcileLooper, error) {
	ipPools, err := k8sClient.ListIPPools()
	if err != nil {
		return nil, logging.Errorf("failed to retrieve all IP pools: %v", err)
	}

	pods, err := k8sClient.ListPods()
	if err != nil {
		return nil, err
	}

	whereaboutsPodRefs := getPodRefsServedByWhereabouts(ipPools)
	looper := &ReconcileLooper{
		k8sClient:           *k8sClient,
		liveWhereaboutsPods: indexPods(pods, whereaboutsPodRefs),
	}

	if err := looper.findOrphanedIPsPerPool(ipPools); err != nil {
		return nil, err
	}

	if err := looper.findClusterWideIPReservations(); err != nil {
		return nil, err
	}
	return looper, nil
}

func (rl *ReconcileLooper) findOrphanedIPsPerPool(ipPools []storage.IPPool) error {
	for _, pool := range ipPools {
		orphanIP := OrphanedIPReservations{
			Pool: pool,
		}
		for _, ipReservation := range pool.Allocations() {
			logging.Debugf("the IP reservation: %s", ipReservation)
			// Short-circuit for reservations with DeletionTimestamp past TTL
			// These are already marked for deletion, so add them directly to orphaned list
			if ipReservation.DeletionTimestamp != 0 {
				if allocate.IsReservationPastTTL(&ipReservation, pool.GetTTL()) {
					logging.Debugf("IP reservation has expired DeletionTimestamp, marking as orphaned: %s", ipReservation)
					orphanIP.Allocations = append(orphanIP.Allocations, ipReservation)
					continue
				}
				logging.Debugf("IP reservation has DeletionTimestamp but not past TTL: %s", ipReservation)
			} else {
				if ipReservation.PodRef == "" {
					_ = logging.Errorf("pod ref missing for Allocations: %s", ipReservation)
					continue
				}
				if rl.isOrphanedIP(ipReservation.PodRef, ipReservation.PodUID, ipReservation.IP.String()) {
					logging.Debugf("pod ref %s is not listed in the live pods list", ipReservation.PodRef)
					orphanIP.Allocations = append(orphanIP.Allocations, ipReservation)
				}
			}
		}

		if len(orphanIP.Allocations) > 0 {
			rl.orphanedIPs = append(rl.orphanedIPs, orphanIP)
		}
	}

	return nil
}

func (rl *ReconcileLooper) isOrphanedIP(podRef, podUID, ip string) bool {
	livePod, exists := rl.liveWhereaboutsPods[podRef]
	if !exists {
		// Since we use informer cache listers, could be very rare case where IPPool has reservation
		// but the Pod lister isn't in sync. Worst thing to do is cleanup wrong IP.
		// For sanity, refetch the Pod once
		refreshedPod, err := rl.refreshPod(podRef)
		if err != nil {
			logging.Debugf("client error refreshing Pod, will try next reconcile loop: %v", err)
			return false
		}
		if refreshedPod == nil {
			// No client error, and Pod not found after direct GET to API server. Can confirm orphaned
			logging.Debugf("Pod %s not found in live pod list, IP %s is orphaned", podRef, ip)
			return true
		}
		livePod = *refreshedPod
	}

	// At this point PodRef does not exist, NOR does it have Deletiontimestamp set.
	// This is a true orphaned Pod(node or kubelet died, for example, and Pod was cleaned up).
	// If the PodUID mismatches, its an orphaned Pod. If the reservation is legacy (no UID),
	// we don't care about pending Pod check either - that was only to handle race condition
	// where reconciler runs immediately after IPPool is updated by IPAM, but Pod isn't annotated
	// by kubelet with the multus annotation

	if podUID != "" {
		if string(livePod.uid) != podUID {
			// UIDs don't match - this is a different pod with the same name
			// For example a StatefulSet created on new node
			logging.Debugf("Pod %s exists but UID %s doesn't match reservation UID %s, orphaned",
				podRef, livePod.uid, podUID)
			return true
		}

		logging.Debugf("Pod %s with matching UID %s found, IP %s is not orphaned",
			podRef, podUID, ip)
		return false
	}

	return !isIpOnPod(&livePod, podRef, ip)
}

func (rl *ReconcileLooper) refreshPod(podRef string) (*podWrapper, error) {
	namespace, podName := splitPodRef(podRef)
	if namespace == "" || podName == "" {
		logging.Errorf("Invalid podRef format: %s", podRef)
		return nil, fmt.Errorf("invalid podRef format: %s", podRef)
	}

	pod, err := rl.k8sClient.GetPod(namespace, podName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logging.Debugf("Pod not found while refreshing: %s", podRef)
			return nil, nil
		}
		logging.Errorf("Failed to refresh Pod %s: %s\n", podRef, err)
		return nil, err
	}

	wrappedPod := wrapPod(*pod)
	logging.Debugf("Got refreshed pod: %v", wrappedPod)
	return wrappedPod, nil
}

func splitPodRef(podRef string) (string, string) {
	namespacedName := strings.Split(podRef, "/")
	if len(namespacedName) != 2 {
		logging.Errorf("Failed to split podRef %s", podRef)
		return "", ""
	}

	return namespacedName[0], namespacedName[1]
}

func composePodRef(pod v1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.GetNamespace(), pod.GetName())
}

func (rl *ReconcileLooper) ReconcileIPPools() ([]net.IP, error) {
	findAllocationIndex := func(reservation types.IPReservation, reservations []types.IPReservation) int {
		for idx, r := range reservations {
			if r.PodRef == reservation.PodRef && r.IP.Equal(reservation.IP) {
				return idx
			}
		}
		return -1
	}

	var totalCleanedUpIps []net.IP
	for _, orphanedIP := range rl.orphanedIPs {
		currentIPReservations := orphanedIP.Pool.Allocations()
		originalReservations := make([]types.IPReservation, len(currentIPReservations))
		copy(originalReservations, currentIPReservations)

		// Process orphaned allocation peer pool
		var cleanedUpIpsPerPool []net.IP
		for _, allocation := range orphanedIP.Allocations {
			idx := findAllocationIndex(allocation, currentIPReservations)
			if idx < 0 {
				// Should never happen
				logging.Debugf("Failed to find allocation for pod ref: %s and IP: %s", allocation.PodRef, allocation.IP.String())
				continue
			}

			var deallocatedIP net.IP
			currentIPReservations, deallocatedIP = allocate.DeallocateIPForIndex(currentIPReservations, idx, orphanedIP.Pool.GetTTL())

			cleanedUpIpsPerPool = append(cleanedUpIpsPerPool, deallocatedIP)
		}

		if len(originalReservations) != len(currentIPReservations) {
			logging.Debugf("Going to update the reserve list to: %+v", currentIPReservations)

			ctx, cancel := context.WithTimeout(context.Background(), storage.RequestTimeout)
			if err := orphanedIP.Pool.Update(ctx, currentIPReservations); err != nil {
				cancel()
				return nil, logging.Errorf("failed to update the reservation list: %v", err)
			}

			cancel()
			totalCleanedUpIps = append(totalCleanedUpIps, cleanedUpIpsPerPool...)
		}
	}

	return totalCleanedUpIps, nil
}

func (rl *ReconcileLooper) findClusterWideIPReservations() error {
	clusterWideIPReservations, err := rl.k8sClient.ListOverlappingIPs()
	if err != nil {
		return logging.Errorf("failed to list all OverLappingIPs: %v", err)
	}

	for _, clusterWideIPReservation := range clusterWideIPReservations {
		ip := clusterWideIPReservation.GetName()
		// De-normalize the IP
		// In the UpdateOverlappingRangeAllocation function, the IP address is created with a "normalized" name to comply with the k8s api.
		// We must denormalize here in order to properly look up the IP address in the regular format, which pods use.
		denormalizedip := strings.ReplaceAll(ip, "-", ":")

		podRef := clusterWideIPReservation.Spec.PodRef

		if rl.isOrphanedIP(podRef, "", denormalizedip) {
			logging.Debugf("pod ref %s is not listed in the live pods list", podRef)
			rl.orphanedClusterWideIPs = append(rl.orphanedClusterWideIPs, clusterWideIPReservation)
		}
	}

	return nil
}

func (rl *ReconcileLooper) ReconcileOverlappingIPAddresses() error {
	var failedReconciledClusterWideIPs []string

	for _, overlappingIPStruct := range rl.orphanedClusterWideIPs {
		if err := rl.k8sClient.DeleteOverlappingIP(&overlappingIPStruct); err != nil {
			logging.Errorf("failed to remove cluster wide IP: %s", overlappingIPStruct.GetName())
			failedReconciledClusterWideIPs = append(failedReconciledClusterWideIPs, overlappingIPStruct.GetName())
			continue
		}
		logging.Verbosef("removed stale overlappingIP allocation [%s]", overlappingIPStruct.GetName())
	}

	if len(failedReconciledClusterWideIPs) != 0 {
		return logging.Errorf("could not reconcile cluster wide IPs: %v", failedReconciledClusterWideIPs)
	}
	return nil
}
