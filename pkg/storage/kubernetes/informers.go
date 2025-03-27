package kubernetes

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	v1corelisters "k8s.io/client-go/listers/core/v1"

	"github.com/k8snetworkplumbingwg/whereabouts/pkg/api/whereabouts.cni.cncf.io/v1alpha1"
	wbinformers "github.com/k8snetworkplumbingwg/whereabouts/pkg/client/informers/externalversions"
	wblister "github.com/k8snetworkplumbingwg/whereabouts/pkg/client/listers/whereabouts.cni.cncf.io/v1alpha1"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/logging"
)

// InitializeInformers sets up the shared informers for Pods and IPPools
// It returns the listers which can be used to access the cache
func InitializeInformers(client *Client, resyncPeriod time.Duration, stopCh <-chan struct{}) error {
	k8sInformerFactory := informers.NewSharedInformerFactory(client.clientSet, resyncPeriod)
	podInformer := k8sInformerFactory.Core().V1().Pods()
	client.podLister = podInformer.Lister()

	wbInformerFactory := wbinformers.NewSharedInformerFactory(client.client, resyncPeriod)
	ipPoolInformer := wbInformerFactory.Whereabouts().V1alpha1().IPPools()
	client.ipPoolLister = ipPoolInformer.Lister()

	k8sInformerFactory.Start(stopCh)
	wbInformerFactory.Start(stopCh)

	logging.Verbosef("Waiting for informer caches to sync")
	ret := k8sInformerFactory.WaitForCacheSync(stopCh)
	for res, ok := range ret {
		if !ok {
			return fmt.Errorf("informer cache failed to sync resource %s", res)
		}
	}

	ret = wbInformerFactory.WaitForCacheSync(stopCh)
	for res, ok := range ret {
		if !ok {
			return fmt.Errorf("informer cache failed to sync resource %s", res)
		}
	}
	logging.Verbosef("Informer caches synced successfully")
	return nil
}

// ListPodsFromInformer lists all pods using the informer cache from the client
func ListPodsFromInformer(podLister v1corelisters.PodLister) ([]*v1.Pod, error) {
	pods, err := podLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	return pods, nil
}

// ListIPPoolsFromInformer lists all IPPools using the informer cache from the client
func ListIPPoolsFromInformer(ipPoolLister wblister.IPPoolLister) ([]*v1alpha1.IPPool, error) {
	return ipPoolLister.List(labels.Everything())
}
