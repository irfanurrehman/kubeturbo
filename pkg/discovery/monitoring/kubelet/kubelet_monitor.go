package kubelet

import (
	"errors"
	"sync"

	api "k8s.io/api/core/v1"
	stats "k8s.io/kubernetes/pkg/kubelet/apis/stats/v1alpha1"

	"github.com/turbonomic/kubeturbo/pkg/discovery/metrics"
	"github.com/turbonomic/kubeturbo/pkg/discovery/monitoring/types"
	"github.com/turbonomic/kubeturbo/pkg/discovery/task"
	"github.com/turbonomic/kubeturbo/pkg/discovery/util"
	"k8s.io/client-go/kubernetes"

	"github.com/golang/glog"
	"github.com/turbonomic/kubeturbo/pkg/kubeclient"
)

// KubeletMonitor is a resource monitoring worker.
type KubeletMonitor struct {
	nodeList []*api.Node

	kubeletClient *kubeclient.KubeletClient

	// Backup k8s client for node cpufrequency
	kubeClient *kubernetes.Clientset

	metricSink *metrics.EntityMetricSink

	stopCh chan struct{}

	wg sync.WaitGroup
}

func NewKubeletMonitor(config *KubeletMonitorConfig) (*KubeletMonitor, error) {
	return &KubeletMonitor{
		kubeletClient: config.kubeletClient,
		kubeClient:    config.kubeClient,
		metricSink:    metrics.NewEntityMetricSink(),
		stopCh:        make(chan struct{}, 1),
	}, nil
}

func (m *KubeletMonitor) reset() {
	m.metricSink = metrics.NewEntityMetricSink()
	m.stopCh = make(chan struct{}, 1)
}

func (m *KubeletMonitor) GetMonitoringSource() types.MonitoringSource {
	return types.KubeletSource
}

func (m *KubeletMonitor) ReceiveTask(task *task.Task) {
	m.reset()

	m.nodeList = task.NodeList()
}

func (m *KubeletMonitor) Stop() {
	m.stopCh <- struct{}{}
}

func (m *KubeletMonitor) Do() *metrics.EntityMetricSink {
	glog.V(4).Infof("%s has started task.", m.GetMonitoringSource())
	err := m.RetrieveResourceStat()
	if err != nil {
		glog.Errorf("Failed to execute task: %s", err)
	}
	glog.V(4).Infof("%s monitor has finished task.", m.GetMonitoringSource())
	return m.metricSink
}

// Start to retrieve resource stats for the received list of nodes.
func (m *KubeletMonitor) RetrieveResourceStat() error {
	defer func() {
		close(m.stopCh)
	}()
	if m.nodeList == nil || len(m.nodeList) == 0 {
		return errors.New("Invalid nodeList or empty nodeList. Finish Immediately...")
	}
	m.wg.Add(len(m.nodeList))

	for _, node := range m.nodeList {
		go func(n *api.Node) {
			defer m.wg.Done()
			select {
			case <-m.stopCh:
				return
			default:
				m.scrapeKubelet(n)
			}
		}(node)
	}

	m.wg.Wait()

	return nil
}

// Retrieve resource metrics for the given node.
func (m *KubeletMonitor) scrapeKubelet(node *api.Node) {
	kc := m.kubeletClient

	nodefreq, err := kc.GetNodeCpuFrequency(node)
	if err != nil {
		glog.Errorf("Failed to get resource metrics (cpufreq) from %s: %s", node.Name, err)
		return
	}
	m.parseNodeCpuFreq(node, nodefreq)

	ip, err := util.GetNodeIPForMonitor(node, types.KubeletSource)
	if err != nil {
		glog.Errorf("Failed to get resource metrics summary from %s: %s", node.Name, err)
		return
	}
	// get summary information about the given node and the pods running on it.
	summary, err := kc.GetSummary(ip)
	if err != nil {
		glog.Errorf("Failed to get resource metrics summary from %s: %s", node.Name, err)
		return
	}
	// Indicate that we have used the cache last time we've asked for some of the info.
	if kc.HasCacheBeenUsed(ip) {
		cacheUsedMetric := metrics.NewEntityStateMetric(metrics.NodeType, util.NodeKeyFunc(node), "NodeCacheUsed", 1)
		m.metricSink.AddNewMetricEntries(cacheUsedMetric)
	}

	m.parseNodeStats(summary.Node)
	m.parsePodStats(summary.Pods)

	glog.V(4).Infof("Finished scrape node %s.", node.Name)
}

func (m *KubeletMonitor) parseNodeCpuFreq(node *api.Node, cpuFrequencyMHz float64) {
	glog.V(4).Infof("node-%s cpuFrequency = %.2fMHz", node.Name, cpuFrequencyMHz)
	cpuFrequencyMetric := metrics.NewEntityStateMetric(metrics.NodeType, util.NodeKeyFunc(node), metrics.CpuFrequency, cpuFrequencyMHz)
	m.metricSink.AddNewMetricEntries(cpuFrequencyMetric)
}

// Parse node stats and put it into sink.
func (m *KubeletMonitor) parseNodeStats(nodeStats stats.NodeStats) {
	var cpuUsageMiliCore, memoryWorkingSetKiloBytes, rootfsCapacity, rootfsUsed float64
	// cpu
	if nodeStats.CPU.UsageNanoCores != nil {
		cpuUsageMiliCore = util.MetricNanoToMili(float64(*nodeStats.CPU.UsageNanoCores))
	}
	if nodeStats.Memory.WorkingSetBytes != nil {
		memoryWorkingSetKiloBytes = util.Base2BytesToKilobytes(float64(*nodeStats.Memory.WorkingSetBytes))
	}
	if nodeStats.Fs.CapacityBytes != nil {
		rootfsCapacity = util.Base2BytesToMegabytes(float64(*nodeStats.Fs.CapacityBytes))
	}
	if nodeStats.Fs.UsedBytes != nil {
		// OpsMgr server expects the reported size in megabytes
		rootfsUsed = util.Base2BytesToMegabytes(float64(*nodeStats.Fs.UsedBytes))
	}

	key := util.NodeStatsKeyFunc(nodeStats)
	nodeName := nodeStats.NodeName
	glog.V(4).Infof("CPU usage of node %s is %.3f Milicore", nodeName, cpuUsageMiliCore)
	glog.V(4).Infof("Memory working set of node %s is %.3f KB", nodeName, memoryWorkingSetKiloBytes)
	glog.V(4).Infof("Root File System size for node %s is %.3f Megabytes", nodeName, rootfsCapacity)
	glog.V(4).Infof("Root File System used for node %s is %.3f Megabytes", nodeName, rootfsUsed)
	m.genUsedMetrics(metrics.NodeType, key, cpuUsageMiliCore, memoryWorkingSetKiloBytes)
	m.genFSMetrics(metrics.NodeType, key, rootfsCapacity, rootfsUsed)
}

// Parse pod stats for every pod and put them into sink.
func (m *KubeletMonitor) parsePodStats(podStats []stats.PodStats) {
	for i := range podStats {
		pod := &(podStats[i])
		cpuUsed, memUsed := m.parseContainerStats(pod)
		key := util.PodMetricId(&(pod.PodRef))

		ephemeralFsCapacity, ephemeralFsUsed := float64(0), float64(0)
		if pod.EphemeralStorage != nil {
			if pod.EphemeralStorage.CapacityBytes != nil {
				ephemeralFsCapacity = util.Base2BytesToMegabytes(float64(*pod.EphemeralStorage.CapacityBytes))
			}
			if pod.EphemeralStorage.UsedBytes != nil {
				// OpsMgr server expects the reported size in megabytes
				ephemeralFsUsed = util.Base2BytesToMegabytes(float64(*pod.EphemeralStorage.UsedBytes))
			}
		} else {
			glog.V(4).Infof("Ephemeral fs status is not available for pod %v", key)
		}

		glog.V(4).Infof("Cpu usage of pod %s is %.3f Milicore", key, cpuUsed)
		glog.V(4).Infof("Memory usage of pod %s is %.3f Kb", key, memUsed)
		glog.V(4).Infof("Ephemeral fs capacity for pod %s is %.3f Megabytes", key, ephemeralFsCapacity)
		glog.V(4).Infof("Ephemeral fs used for pod %s is %.3f Megabytes", key, ephemeralFsUsed)

		m.genUsedMetrics(metrics.PodType, key, cpuUsed, memUsed)
		m.genNumConsumersUsedMetrics(metrics.PodType, key)
		m.genFSMetrics(metrics.PodType, key, ephemeralFsCapacity, ephemeralFsUsed)
	}
}

func (m *KubeletMonitor) parseContainerStats(pod *stats.PodStats) (float64, float64) {

	totalUsedCPU := float64(0.0)
	totalUsedMem := float64(0.0)

	podMId := util.PodMetricId(&(pod.PodRef))
	containers := pod.Containers

	for i := range containers {
		container := &containers[i]
		if container.CPU == nil || container.CPU.UsageNanoCores == nil {
			continue
		}
		if container.Memory == nil || container.Memory.WorkingSetBytes == nil {
			continue
		}

		cpuUsed := util.MetricNanoToMili(float64(*container.CPU.UsageNanoCores))
		memUsed := util.Base2BytesToKilobytes(float64(*container.Memory.WorkingSetBytes))

		totalUsedCPU += cpuUsed
		totalUsedMem += memUsed

		//1. container Used
		containerMId := util.ContainerMetricId(podMId, container.Name)
		// Generate used metrics for VCPU and VMemory commodities
		m.genUsedMetrics(metrics.ContainerType, containerMId, cpuUsed, memUsed)
		// Generate used metrics for VCPURequest and VMemRequest commodities
		m.genRequestUsedMetrics(metrics.ContainerType, containerMId, cpuUsed, memUsed)

		glog.V(4).Infof("container[%s-%s] cpu/memory/cpuRequest/memoryRequest usage:%.3f, %.3f, %.3f, %.3f",
			pod.PodRef.Name, container.Name, cpuUsed, memUsed, cpuUsed, memUsed)

		//2. app Used
		appMId := util.ApplicationMetricId(containerMId)
		m.genUsedMetrics(metrics.ApplicationType, appMId, cpuUsed, memUsed)
	}

	return totalUsedCPU, totalUsedMem
}

func (m *KubeletMonitor) genUsedMetrics(etype metrics.DiscoveredEntityType, key string, cpuMili, memory float64) {
	cpuMetric := metrics.NewEntityResourceMetric(etype, key, metrics.CPUMili, metrics.Used, cpuMili)
	memMetric := metrics.NewEntityResourceMetric(etype, key, metrics.Memory, metrics.Used, memory)
	m.metricSink.AddNewMetricEntries(cpuMetric, memMetric)
}

// genRequestUsedMetrics generates used metrics for VCPURequest and VMemRequest commodity
func (m *KubeletMonitor) genRequestUsedMetrics(etype metrics.DiscoveredEntityType, key string, cpu, memory float64) {
	cpuRequestMetric := metrics.NewEntityResourceMetric(etype, key, metrics.CPURequest, metrics.Used, cpu)
	memRequestMetric := metrics.NewEntityResourceMetric(etype, key, metrics.MemoryRequest, metrics.Used, memory)
	m.metricSink.AddNewMetricEntries(cpuRequestMetric, memRequestMetric)
}

func (m *KubeletMonitor) genNumConsumersUsedMetrics(etype metrics.DiscoveredEntityType, key string) {
	// Each pod consumes one from numConsumers / Total available is node allocatable pod number
	numConsumersMetric := metrics.NewEntityResourceMetric(etype, key, metrics.NumPods, metrics.Used, 1)
	m.metricSink.AddNewMetricEntries(numConsumersMetric)
}

func (m *KubeletMonitor) genFSMetrics(etype metrics.DiscoveredEntityType, key string, capacity, used float64) {
	capacityMetric := metrics.NewEntityResourceMetric(etype, key, metrics.VStorage, metrics.Capacity, capacity)
	usedMetric := metrics.NewEntityResourceMetric(etype, key, metrics.VStorage, metrics.Used, used)
	m.metricSink.AddNewMetricEntries(capacityMetric, usedMetric)
}
