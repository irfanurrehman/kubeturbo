package dtofactory

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/turbonomic/kubeturbo/pkg/discovery/metrics"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
)

var (
	pod1  = "pod1"
	node1 = "node1"

	metricsSink = metrics.NewEntityMetricSink()

	cpuUsed_pod1 = metrics.NewEntityResourceMetric(metrics.PodType, pod1, metrics.CPU, metrics.Used, 1.0)
	memUsed_pod1 = metrics.NewEntityResourceMetric(metrics.PodType, pod1, metrics.Memory, metrics.Used, 8010812.000000/8)

	cpuCap_pod1 = metrics.NewEntityResourceMetric(metrics.PodType, pod1, metrics.CPU, metrics.Capacity, 2.0)
	memCap_pod1 = metrics.NewEntityResourceMetric(metrics.PodType, pod1, metrics.Memory, metrics.Capacity, 8010812.000000/4)

	cpuCap_node1  = metrics.NewEntityResourceMetric(metrics.NodeType, node1, metrics.CPU, metrics.Capacity, 4.0)
	cpuUsed_node1 = metrics.NewEntityResourceMetric(metrics.NodeType, node1, metrics.CPU, metrics.Used, 2.0)

	memCap_node1  = metrics.NewEntityResourceMetric(metrics.NodeType, node1, metrics.Memory, metrics.Capacity, 8010812.000000)
	memUsed_node1 = metrics.NewEntityResourceMetric(metrics.NodeType, node1, metrics.Memory, metrics.Used, 8010812.000000/4)

	cpuFrequency = 2048.0
	cpuConverter = NewConverter().Set(
		func(input float64) float64 {
			return input * cpuFrequency
		},
		metrics.CPU)
)

func TestBuildCPUSold(t *testing.T) {
	metricsSink = metrics.NewEntityMetricSink()
	metricsSink.AddNewMetricEntries(cpuUsed_pod1, cpuCap_pod1)

	dtoBuilder := &generalBuilder{
		metricsSink: metricsSink,
	}

	eType := metrics.PodType
	commSold, err := dtoBuilder.getSoldResourceCommodityWithKey(eType, pod1, metrics.CPU, "", cpuConverter, nil)
	fmt.Printf("%++v\n", commSold)
	fmt.Printf("%++v\n", err)
	assert.Nil(t, err)
	assert.NotNil(t, commSold)
	assert.Equal(t, proto.CommodityDTO_VCPU, commSold.GetCommodityType())
	capacityValue := cpuConverter.Convert(metrics.CPU, cpuCap_pod1.GetValue().(float64))
	usedValue := cpuConverter.Convert(metrics.CPU, cpuUsed_pod1.GetValue().(float64))
	assert.Equal(t, usedValue, commSold.GetUsed())
	assert.Equal(t, capacityValue, commSold.GetCapacity())
}

func TestBuildMemSold(t *testing.T) {
	metricsSink = metrics.NewEntityMetricSink()
	metricsSink.AddNewMetricEntries(memUsed_pod1, memCap_pod1)

	dtoBuilder := &generalBuilder{
		metricsSink: metricsSink,
	}

	eType := metrics.PodType
	commSold, err := dtoBuilder.getSoldResourceCommodityWithKey(eType, pod1, metrics.Memory, "", nil, nil)
	fmt.Printf("%++v\n", commSold)
	fmt.Printf("%++v\n", err)
	assert.NotNil(t, commSold)
	assert.Equal(t, proto.CommodityDTO_VMEM, commSold.GetCommodityType())
	assert.Nil(t, err)
	assert.Equal(t, memUsed_pod1.GetValue().(float64), commSold.GetUsed())
	assert.Equal(t, memCap_pod1.GetValue().(float64), commSold.GetCapacity())
}

func TestBuildUnsupportedResource(t *testing.T) {
	metricsSink = metrics.NewEntityMetricSink()

	dtoBuilder := &generalBuilder{
		metricsSink: metricsSink,
	}

	eType := metrics.PodType
	commSold, _ := dtoBuilder.getSoldResourceCommodityWithKey(eType, pod1, metrics.MemoryRequest, "", nil, nil)
	assert.Nil(t, commSold)
}

func TestBuildCPUSoldWithMissingCap(t *testing.T) {
	eType := metrics.PodType
	// Missing CPU Capacity
	metricsSink = metrics.NewEntityMetricSink()
	metricsSink.AddNewMetricEntries(cpuUsed_pod1)

	dtoBuilder := &generalBuilder{
		metricsSink: metricsSink,
	}
	commSold, err := dtoBuilder.getSoldResourceCommodityWithKey(eType, pod1, metrics.CPU, "", cpuConverter, nil)
	assert.Nil(t, commSold)
	assert.NotNil(t, err)
}

func TestBuildCPUSoldWithMissingUsed(t *testing.T) {
	eType := metrics.PodType
	metricsSink = metrics.NewEntityMetricSink()
	metricsSink.AddNewMetricEntries(cpuCap_pod1)

	dtoBuilder := &generalBuilder{
		metricsSink: metricsSink,
	}
	commSold, err := dtoBuilder.getSoldResourceCommodityWithKey(eType, pod1, metrics.CPU, "", cpuConverter, nil)
	assert.Nil(t, commSold)
	assert.NotNil(t, err)
}

func TestBuildCPUSoldWithMissingConverter(t *testing.T) {
	eType := metrics.PodType
	metricsSink = metrics.NewEntityMetricSink()
	metricsSink.AddNewMetricEntries(cpuUsed_pod1, cpuCap_pod1)
	dtoBuilder := &generalBuilder{
		metricsSink: metricsSink,
	}
	commSold, err := dtoBuilder.getSoldResourceCommodityWithKey(eType, pod1, metrics.CPU, "", nil, nil)
	assert.Nil(t, commSold)
	assert.NotNil(t, err)
}

func TestBuildCommSoldWithKey(t *testing.T) {
	metricsSink = metrics.NewEntityMetricSink()
	metricsSink.AddNewMetricEntries(memUsed_pod1, memCap_pod1)

	dtoBuilder := &generalBuilder{
		metricsSink: metricsSink,
	}

	eType := metrics.PodType
	commSold, err := dtoBuilder.getSoldResourceCommodityWithKey(eType, pod1, metrics.Memory, "key", nil, nil)
	assert.Nil(t, err)
	assert.NotNil(t, commSold)
	assert.Equal(t, proto.CommodityDTO_VMEM, commSold.GetCommodityType())
	assert.Equal(t, "key", commSold.GetKey())
}

func TestBuildCommSold(t *testing.T) {
	metricsSink = metrics.NewEntityMetricSink()
	metricsSink.AddNewMetricEntries(cpuUsed_pod1, cpuCap_pod1, memCap_pod1, memUsed_pod1)

	dtoBuilder := &generalBuilder{
		metricsSink: metricsSink,
	}

	eType := metrics.PodType
	resourceTypesList := []metrics.ResourceType{metrics.CPU, metrics.Memory}
	commSoldList := dtoBuilder.getResourceCommoditiesSold(eType, pod1, resourceTypesList, cpuConverter, nil)
	commMap := make(map[proto.CommodityDTO_CommodityType]*proto.CommodityDTO)

	for _, commSold := range commSoldList {
		commMap[commSold.GetCommodityType()] = commSold
	}

	for _, rType := range resourceTypesList {
		cType, _ := rTypeMapping[rType]
		commSold, _ := commMap[cType]
		assert.NotNil(t, commSold)
		fmt.Printf("%++v\n", commSold)
	}
}

func TestBuildCommBought(t *testing.T) {
	metricsSink = metrics.NewEntityMetricSink()
	metricsSink.AddNewMetricEntries(cpuUsed_pod1, memUsed_pod1)

	dtoBuilder := &generalBuilder{
		metricsSink: metricsSink,
	}

	eType := metrics.PodType
	resourceTypesList := []metrics.ResourceType{metrics.CPU, metrics.Memory}
	commBoughtList := dtoBuilder.getResourceCommoditiesBought(eType, pod1, resourceTypesList, cpuConverter, nil)
	commMap := make(map[proto.CommodityDTO_CommodityType]*proto.CommodityDTO)

	for _, commBought := range commBoughtList {
		commMap[commBought.GetCommodityType()] = commBought
	}

	for _, rType := range resourceTypesList {
		cType, _ := rTypeMapping[rType]
		commBought, _ := commMap[cType]
		assert.NotNil(t, commBought)
		fmt.Printf("%++v\n", commBought)
	}
}

func TestMetricValueWithMultiplePoints(t *testing.T) {
	metricsSink = metrics.NewEntityMetricSink().WithMaxMetricPointsSize(3)
	containerId := "container"
	cpuUsedMetric1 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.CPU, metrics.Used,
		[]metrics.Point{{
			Value:     2,
			Timestamp: 1,
		}})
	cpuUsedMetric2 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.CPU, metrics.Used,
		[]metrics.Point{{
			Value:     4,
			Timestamp: 2,
		}})
	cpuUsedMetric3 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.CPU, metrics.Used,
		[]metrics.Point{{
			Value:     3,
			Timestamp: 3,
		}})
	metricsSink.AddNewMetricEntries(cpuUsedMetric1)
	metricsSink.UpdateMetricEntry(cpuUsedMetric2)
	metricsSink.UpdateMetricEntry(cpuUsedMetric3)

	dtoBuilder := &generalBuilder{
		metricsSink: metricsSink,
	}

	metricValue, _ := dtoBuilder.metricValue(metrics.ContainerType, containerId, metrics.CPU, metrics.Used, nil)
	assert.EqualValues(t, 3, int(metricValue.Avg))
	assert.EqualValues(t, 4, int(metricValue.Peak))
}

func TestMetricValueWithThrottlingCumulativePoints(t *testing.T) {
	metricsSink = metrics.NewEntityMetricSink().WithMaxMetricPointsSize(11)
	containerId := "container-throttling"
	cpuUsedMetric1 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used,
		[]metrics.ThrottlingCumulative{{
			Throttled: 1,
			Total:     5,
			Timestamp: 1,
		}})
	cpuUsedMetric2 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used,
		[]metrics.ThrottlingCumulative{{
			Throttled: 3,
			Total:     8,
			Timestamp: 2,
		}})
	cpuUsedMetric3 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used,
		[]metrics.ThrottlingCumulative{{
			Throttled: 5,
			Total:     10,
			Timestamp: 3,
		}})
	cpuUsedMetric4 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used,
		[]metrics.ThrottlingCumulative{{
			Throttled: 6,
			Total:     15,
			Timestamp: 4,
		}})
	cpuUsedMetric5 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used,
		[]metrics.ThrottlingCumulative{{
			Throttled: 11,
			Total:     25,
			Timestamp: 5,
		}})
	metricsSink.AddNewMetricEntries(cpuUsedMetric1)
	metricsSink.UpdateMetricEntry(cpuUsedMetric2)
	metricsSink.UpdateMetricEntry(cpuUsedMetric3)
	metricsSink.UpdateMetricEntry(cpuUsedMetric4)
	metricsSink.UpdateMetricEntry(cpuUsedMetric5)

	dtoBuilder := &generalBuilder{
		metricsSink: metricsSink,
	}
	metricValue, _ := dtoBuilder.metricValue(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used, nil)
	// throttled   = (3-1) (5-3)  (6-5)   (11-6)
	// total   =     (8-5) (10-8) (15-10) (20-15)
	// Avg = (11-1)*100/(25-5) = 50
	// Peak = (5-3)*100/(10-8) = 100
	assert.EqualValues(t, 50, int(metricValue.Avg))
	assert.EqualValues(t, 100, int(metricValue.Peak))

	// ------------------------------------------

	// Adding more metrics where the counter is reset in continuation to previous 5
	cpuUsedMetric6 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used,
		[]metrics.ThrottlingCumulative{{
			Throttled: 0,
			Total:     0,
			Timestamp: 6,
		}})

	cpuUsedMetric7 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used,
		[]metrics.ThrottlingCumulative{{
			Throttled: 1,
			Total:     5,
			Timestamp: 7,
		}})

	cpuUsedMetric8 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used,
		[]metrics.ThrottlingCumulative{{
			Throttled: 3,
			Total:     8,
			Timestamp: 8,
		}})

	// Counter set to an old value again on this sample
	cpuUsedMetric9 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used,
		[]metrics.ThrottlingCumulative{{
			Throttled: 1,
			Total:     5,
			Timestamp: 9,
		}})
	cpuUsedMetric10 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used,
		[]metrics.ThrottlingCumulative{{
			Throttled: 3,
			Total:     8,
			Timestamp: 10,
		}})
	cpuUsedMetric11 := metrics.NewEntityResourceMetric(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used,
		[]metrics.ThrottlingCumulative{{
			Throttled: 1,
			Total:     5,
			Timestamp: 10,
		}})

	metricsSink.UpdateMetricEntry(cpuUsedMetric6)
	metricsSink.UpdateMetricEntry(cpuUsedMetric7)
	metricsSink.UpdateMetricEntry(cpuUsedMetric8)
	metricsSink.UpdateMetricEntry(cpuUsedMetric9)
	metricsSink.UpdateMetricEntry(cpuUsedMetric10)
	metricsSink.UpdateMetricEntry(cpuUsedMetric11)

	metricValue, _ = dtoBuilder.metricValue(metrics.ContainerType, containerId, metrics.VCPUThrottling, metrics.Used, nil)

	// time                  t1   t2    t3     t4      t5                  t6     t7    t8               t9     t10    t11
	// throttled samples =   1    3     5      6       11                  0      1     3                1       3      1
	// total samples =       5    8     10     15      25                  0      5     8                5       8      5
	// diff                   d1    d2     d3      d4          d5              d6    d7       d8              d9       d10
	// throttled =           (3-1) (5-3)  (6-5)   (11-6)  (reset/ignore 0-11) (1-0) (3-1) (reset/ignore 1-3) (3-1)  (reset/ignore 1-3)
	// total =               (8-5) (10-8) (15-10) (25-15) (reset/ignore 0-25) (5-0) (8-5) (reset/ignore 5-8) (8-5)  (reset/ignore 5-8)
	// Window                |_____________w1___________|                     |___w2____|                    |_w3_|

	// Avg = ((11-1)+(3-0)+(3-1))*100/((25-5)+(8-0)+(8-5)) = 48.387
	// Peak = (5-3)*100/(10-8) = 100
	assert.EqualValues(t, 48, int(metricValue.Avg))
	assert.EqualValues(t, 100, int(metricValue.Peak))
}
