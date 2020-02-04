package executor

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"strconv"
	"time"

	"github.com/golang/glog"

	"github.com/turbonomic/kubeturbo/pkg/action/util"
	api "k8s.io/api/core/v1"

	podutil "github.com/turbonomic/kubeturbo/pkg/discovery/util"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
)

type ReScheduler struct {
	TurboK8sActionExecutor
	sccAllowedSet map[string]struct{}
}

func NewReScheduler(ae TurboK8sActionExecutor, sccAllowedSet map[string]struct{}) *ReScheduler {
	return &ReScheduler{
		TurboK8sActionExecutor: ae,
		sccAllowedSet:          sccAllowedSet,
	}
}

// Execute executes the move action. The error message will be shown in UI.
func (r *ReScheduler) Execute(input *TurboActionExecutorInput) (*TurboActionExecutorOutput, error) {
	actionItem := input.ActionItem
	pod := input.Pod

	//1. get target Pod and new hosting Node
	node, dstCluster, err := r.getPodNode(actionItem)
	if err != nil {
		glog.Errorf("Failed to execute pod move: %v.", err)
		return &TurboActionExecutorOutput{}, err
	}

	if dstCluster != "" {
		if r.kubefedDynClient == nil {
			return &TurboActionExecutorOutput{}, fmt.Errorf("Got multi-cluster action but Kubefed client is null")
		}
		// Cross cluster move
		res := schema.GroupVersionResource{
			Group:    "turbo.kubefed.io",
			Version:  "v1alpha1",
			Resource: "actions",
		}
		action := &unstructured.Unstructured{}
		name := "kubeturbo-action-" + strconv.FormatInt(time.Now().UnixNano(), 32)
		SetBasicMetaFields(action, res, "Action", name, r.kubefedNamespace)
		targetRef := make(map[string]interface{})
		targetRef["kind"] = "pod"
		targetRef["name"] = pod.Name
		targetRef["namespace"] = pod.Namespace
		err := unstructured.SetNestedMap(action.Object, targetRef, "spec", "targetRef")
		if err != nil {
			glog.Errorf("Failed to execute pod move: %v.", err)
			return &TurboActionExecutorOutput{}, err
		}
		clusters := make(map[string]interface{})
		clusters["source"] = r.thisClusterName
		clusters["destination"] = dstCluster
		err = unstructured.SetNestedMap(action.Object, clusters, "spec", "clusters")
		if err != nil {
			glog.Errorf("Failed to execute pod move: %v.", err)
			return &TurboActionExecutorOutput{}, err
		}
		err = unstructured.SetNestedField(action.Object, node.Name, "spec", "targetNode")
		if err != nil {
			glog.Errorf("Failed to execute pod move: %v.", err)
			return &TurboActionExecutorOutput{}, err
		}

		_, err = r.kubefedDynClient.Resource(res).Namespace(r.kubefedNamespace).Create(action, metav1.CreateOptions{})
		return &TurboActionExecutorOutput{}, err

		// TODO: clean up above code and wait for the status update before returning success
	}

	//2. move pod to the node and check move status
	npod, err := r.reSchedule(pod, node)
	if err != nil {
		glog.Errorf("Failed to execute pod move: %v.", err)
		return &TurboActionExecutorOutput{}, err
	}

	return &TurboActionExecutorOutput{
		Succeeded: true,
		OldPod:    pod,
		NewPod:    npod,
	}, nil
}

// get k8s.node of the new hosting node
func (r *ReScheduler) getNode(action *proto.ActionItemDTO) (*api.Node, string, error) {
	//1. check host entity
	hostSE := action.GetNewSE()
	if hostSE == nil {
		err := fmt.Errorf("New host entity is empty")
		glog.Errorf("%v.", err)
		return nil, "", err
	}

	//2. check entity type
	etype := hostSE.GetEntityType()
	if etype != proto.EntityDTO_VIRTUAL_MACHINE && etype != proto.EntityDTO_PHYSICAL_MACHINE {
		err := fmt.Errorf("The move destination [%v] is neither a VM nor a PM", etype)
		glog.Errorf("%v.", err)
		return nil, "", err
	}

	//3. get node from properties
	node, err := util.GetNodeFromProperties(r.kubeClient, hostSE.GetEntityProperties())
	if err == nil {
		glog.V(2).Infof("Get node(%v) from properties.", node.Name)
		return node, "", nil
	}

	//5. get node by UUID
	node, err = util.GetNodebyUUID(r.kubeClient, hostSE.GetId())
	if err == nil {
		glog.V(2).Infof("Get node(%v) by UUID(%v).", node.Name, hostSE.GetId())
		return node, "", nil
	}

	nodeUid := hostSE.GetId()
	if r.kubefedDynClient != nil {
		// Possibly a multi-cluster action
		glog.V(2).Infof("Checking the node by UUID(%v) from federated clusters.", hostSE.GetId())
		kfClient := r.kubefedDynClient
		res := schema.GroupVersionResource{
			Group:    "core.kubefed.io",
			Version:  "v1beta1",
			Resource: "kubefedclusters",
		}

		clusterList, err := kfClient.Resource(res).Namespace(r.kubefedNamespace).List(metav1.ListOptions{})
		if err == nil {
			for _, cluster := range clusterList.Items {
				clusterName := cluster.GetName()
				nodelist, found, err := unstructured.NestedSlice(cluster.Object, "status", "nodeList")
				if err != nil {
					glog.V(2).Infof("Error retrieving nodelist from federated cluster %s: %v.", clusterName, err)
					continue
				}
				if !found {
					glog.V(2).Infof("Nodelist not updated for federated cluster %s.", clusterName)
					continue
				}

				for idx := range nodelist {
					node := nodelist[idx].(map[string]interface{})
					uid := node["uid"]
					if uid.(string) == nodeUid {
						typedNode := &api.Node{}
						nodeName := node["name"].(string)
						typedNode.Name = nodeName
						glog.V(2).Infof("Get node(%v) from federated Cluster(%v).", nodeName, clusterName)
						return typedNode, clusterName, nil
					}
				}
			}
		} else {
			glog.V(2).Infof("Error retrieving clusterList from federated control plane: %v", err)
		}
	}

	//4. get node by displayName
	node, err = util.GetNodebyName(r.kubeClient, hostSE.GetDisplayName())
	if err == nil {
		glog.V(2).Infof("Get node(%v) by displayName.", node.Name)
		return node, "", nil
	}

	//6. get node by IP
	vmIPs := getVMIps(hostSE)
	if len(vmIPs) > 0 {
		node, err = util.GetNodebyIP(r.kubeClient, vmIPs)
		if err == nil {
			glog.V(2).Infof("Get node(%v) by IP.", hostSE.GetDisplayName())
			return node, "", nil
		}
		err = fmt.Errorf("Failed to get node %s by IP %+v: %v",
			hostSE.GetDisplayName(), vmIPs, err)
	} else {
		err = fmt.Errorf("Failed to get node %s: IPs are empty",
			hostSE.GetDisplayName())
	}
	glog.Errorf("%v.", err)
	return nil, "", err
}

// get kubernetes pod, and the new hosting kubernetes node
func (r *ReScheduler) getPodNode(action *proto.ActionItemDTO) (*api.Node, string, error) {
	glog.V(4).Infof("MoveActionItem: %++v", action)
	// Check and find the new hosting node for the pod.
	return r.getNode(action)
}

// Check whether the action should be executed.
// TODO: find a reliable way to check node's status; current checking has no actual effect.
func (r *ReScheduler) preActionCheck(pod *api.Pod, node *api.Node) error {
	fullName := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

	// Check if the pod privilege is supported
	if !util.SupportPrivilegePod(pod, r.sccAllowedSet) {
		err := fmt.Errorf("Pod %s has unsupported SCC", fullName)
		glog.Errorf("%v.", err)
		return err
	}

	//1. If Pod is terminated, then no need to move it.
	// if pod.Status.Phase != api.PodRunning {
	if pod.Status.Phase == api.PodSucceeded {
		glog.Errorf("Move action should be aborted: original pod termiated:%v phase:%v", fullName, pod.Status.Phase)
	}

	//2. if Node is out of condition
	conditions := node.Status.Conditions
	if conditions == nil || len(conditions) < 1 {
		glog.Warningf("Move action: pod[%v]'s new host(%v) condition is unknown", fullName, node.Name)
		return nil
	}

	for _, cond := range conditions {
		if cond.Status != api.ConditionTrue {
			glog.Warningf("Move action: pod[%v]'s new host(%v) in bad condition: %v", fullName, node.Name, cond.Type)
		}
	}

	return nil
}

func (r *ReScheduler) reSchedule(pod *api.Pod, node *api.Node) (*api.Pod, error) {
	//1. do some check
	if err := r.preActionCheck(pod, node); err != nil {
		glog.Errorf("Move action aborted: %v.", err)
		return nil, err
	}

	nodeName := node.Name
	fullName := util.BuildIdentifier(pod.Namespace, pod.Name)
	// if the pod is already on the target node, then simply return success.
	if pod.Spec.NodeName == nodeName {
		err := fmt.Errorf("Pod [%v] is already on host [%v]", fullName, nodeName)
		glog.V(2).Infof("Move action aborted: %v.", err)
		return nil, err
	}

	parentKind, parentName, err := podutil.GetPodParentInfo(pod)
	if err != nil {
		err = fmt.Errorf("Cannot get parent info of pod [%v]: %v", fullName, err)
		glog.Errorf("Move action aborted: %v.", err)
		return nil, err
	}

	if !util.SupportedParent(parentKind) {
		err = fmt.Errorf("The object kind [%v] of [%s] is not supported", parentKind, parentName)
		glog.Errorf("Move action aborted: %v.", err)
		return nil, err
	}

	//2. move
	return movePod(r.kubeClient, pod, nodeName, defaultRetryMore)
}

func getVMIps(entity *proto.EntityDTO) []string {
	result := []string{}

	if entity.GetEntityType() != proto.EntityDTO_VIRTUAL_MACHINE {
		glog.Errorf("Hosting node is a not virtual machine: %++v", entity.GetEntityType())
		return result
	}

	vmData := entity.GetVirtualMachineData()
	if vmData == nil {
		err := fmt.Errorf("Missing virtualMachineData[%v] in targetSE", entity.GetDisplayName())
		glog.Error(err.Error())
		return result
	}

	if len(vmData.GetIpAddress()) < 1 {
		glog.Warningf("Machine IPs are empty: %++v", vmData)
	}

	return vmData.GetIpAddress()
}

func SetBasicMetaFields(resource *unstructured.Unstructured, gvr schema.GroupVersionResource, kind, name, namespace string) {
	resource.SetKind(kind)
	gv := gvr.GroupVersion()
	resource.SetAPIVersion(gv.String())
	resource.SetName(name)
	resource.SetNamespace(namespace)
}
