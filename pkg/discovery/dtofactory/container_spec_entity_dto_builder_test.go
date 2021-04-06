package dtofactory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/turbonomic/kubeturbo/pkg/discovery/metrics"
	"github.com/turbonomic/kubeturbo/pkg/discovery/repository"
	"github.com/turbonomic/kubeturbo/pkg/discovery/worker/aggregation"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
)

func Test_containerSpecDTOBuilder_getCommoditiesSold(t *testing.T) {
	namespace := "namespace"
	controllerUID := "controllerUID"
	containerSpecName := "containerSpecName"
	containerSpecId := "containerSpecId"
	containerSpecMetrics := repository.ContainerSpecMetrics{
		Namespace:         namespace,
		ControllerUID:     controllerUID,
		ContainerSpecName: containerSpecName,
		ContainerSpecId:   containerSpecId,
		ContainerReplicas: 2,
		ContainerMetrics: map[metrics.ResourceType]*repository.ContainerMetrics{
			metrics.CPU: {
				Capacity: []float64{3.0, 4.0},
				Used: []metrics.Point{
					createContainerMetricPoint(1.0, 1),
					createContainerMetricPoint(3.0, 2),
				},
			},
			metrics.Memory: {
				Capacity: []float64{3.0, 4.0},
				Used: []metrics.Point{
					createContainerMetricPoint(1.0, 1),
					createContainerMetricPoint(3.0, 2),
				},
			},
			metrics.MemoryRequest: {
				Capacity: []float64{3.0, 4.0},
				Used: []metrics.Point{
					createContainerMetricPoint(1.0, 1),
					createContainerMetricPoint(3.0, 2),
				},
			},
			metrics.VCPUThrottling: {
				Capacity: []float64{100, 100},
				Used: [][]metrics.ThrottlingCumulative{
					createContainerMetricCumulativeThrottling(1, 4, 2, 5, 4, 10, 1, 2, 3),
					createContainerMetricCumulativeThrottling(2, 8, 4, 12, 8, 20, 1, 2, 3),
				},
			},
		},
	}

	builder := &containerSpecDTOBuilder{
		containerSpecMetricsMap:            map[string]*repository.ContainerSpecMetrics{containerSpecId: &containerSpecMetrics},
		containerUtilizationDataAggregator: aggregation.ContainerUtilizationDataAggregators[aggregation.DefaultContainerUtilizationDataAggStrategy],
		containerUsageDataAggregator:       aggregation.ContainerUsageDataAggregators[aggregation.DefaultContainerUsageDataAggStrategy],
	}
	commodityDTOs, err := builder.getCommoditiesSold(&containerSpecMetrics)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(commodityDTOs))
	for _, commodityDTO := range commodityDTOs {
		assert.Equal(t, true, *commodityDTO.Active)
		assert.Equal(t, true, *commodityDTO.Resizable)
		// Parse values to int to avoid tolerance of float values
		if commodityDTO.GetCommodityType() == proto.CommodityDTO_VCPU_THROTTLING {
			assert.Equal(t, 50, int(*commodityDTO.Used))
			assert.Equal(t, 100, int(*commodityDTO.Peak))
			assert.Equal(t, 100, int(*commodityDTO.Capacity))
		} else {
			assert.Equal(t, 2, int(*commodityDTO.Used))
			assert.Equal(t, 3, int(*commodityDTO.Peak))
			assert.Equal(t, 4, int(*commodityDTO.Capacity))
			assert.Equal(t, 2, len(commodityDTO.UtilizationData.Point))
		}
	}
}

func createContainerMetricPoint(value float64, timestamp int64) metrics.Point {
	return metrics.Point{
		Value:     value,
		Timestamp: timestamp,
	}
}

func createContainerMetricCumulativeThrottling(thr1, tot1, thr2, tot2, thr3, tot3 float64, t1, t2, t3 int64) []metrics.ThrottlingCumulative {
	return []metrics.ThrottlingCumulative{
		{
			Throttled: thr1,
			Total:     tot1,
			Timestamp: t1,
		},
		{
			Throttled: thr2,
			Total:     tot2,
			Timestamp: t2,
		},
		{
			Throttled: thr3,
			Total:     tot3,
			Timestamp: t3,
		},
	}
}
