// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"math/rand"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/common/model"

	dto "github.com/prometheus/client_model/go"
)

type MockedDiskMetricStore struct {
	GetMetricFamiliesCallback    func() []*dto.MetricFamily
	GetMetricFamiliesMapCallback func() GroupingKeyToMetricGroup
	HealthyCallback              func() error
	ReadyCallback                func() error
	ShutdownCallback             func() error
	SubmitWriteRequestCallback   func(wr WriteRequest)
}

func (m *MockedDiskMetricStore) GetMetricFamilies() []*dto.MetricFamily {
	return m.GetMetricFamiliesCallback()
}

func (m *MockedDiskMetricStore) GetMetricFamiliesMap() GroupingKeyToMetricGroup {
	return m.GetMetricFamiliesMapCallback()
}

func (m *MockedDiskMetricStore) Healthy() error {
	return m.HealthyCallback()
}

func (m *MockedDiskMetricStore) Ready() error {
	return m.ReadyCallback()
}

func (m *MockedDiskMetricStore) Shutdown() error {
	return m.ShutdownCallback()
}

func (m *MockedDiskMetricStore) SubmitWriteRequest(wr WriteRequest) {
	m.SubmitWriteRequestCallback(wr)
}

var _ MetricStore = &MockedDiskMetricStore{}

type MetricTestRequest struct {
	Key uint64
	WriteRequest
	StoredMetrics GroupingKeyToMetricGroup
}

func generateMetricFamily() *dto.MetricFamily {
	return &dto.MetricFamily{
		Name: proto.String("some_metric"),
		Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{
			&dto.Metric{
				Label: []*dto.LabelPair{
					&dto.LabelPair{
						Name:  proto.String("some-label"),
						Value: proto.String("with-value"),
					},
				},
				Counter: &dto.Counter{
					Value: proto.Float64(rand.Float64() * float64(rand.Int())),
				},
			},
			&dto.Metric{
				Label: []*dto.LabelPair{
					&dto.LabelPair{
						Name:  proto.String("some-label"),
						Value: proto.String("with-another-value"),
					},
				},
				Counter: &dto.Counter{
					Value: proto.Float64(rand.Float64() * float64(rand.Int())),
				},
			},
		},
	}
}

func generateMetricRequests() []*MetricTestRequest {
	var metricRequests []*MetricTestRequest

	length := rand.Intn(100)

	for i := 0; i < length; i++ {
		labels := map[string]string{
			"a label": "with value",
		}
		key := model.LabelsToSignature(labels)

		metricRequests = append(metricRequests, &MetricTestRequest{
			Key: key,
			WriteRequest: WriteRequest{
				Labels: labels,
				MetricFamilies: map[string]*dto.MetricFamily{
					"some_metric": generateMetricFamily(),
				},
			},
			StoredMetrics: GroupingKeyToMetricGroup{
				key: MetricGroup{
					Labels: labels,
					Metrics: NameToTimestampedMetricFamilyMap{
						"some_metric": TimestampedMetricFamily{
							Timestamp:            time.Now(),
							GobbableMetricFamily: (*GobbableMetricFamily)(generateMetricFamily()),
						},
					},
				},
			},
		})
	}

	return metricRequests
}

func TestAggregation(t *testing.T) {
	metrics := generateMetricRequests()
	index := 0
	tested := 0

	var values []*dto.Metric

	dms := &MockedDiskMetricStore{
		GetMetricFamiliesMapCallback: func() GroupingKeyToMetricGroup {
			next := metrics[index].WriteRequest.MetricFamilies["some_metric"]
			values = make([]*dto.Metric, len(next.Metric))

			for i := 0; i < len(next.Metric); i++ {
				switch next.GetType() {
				case dto.MetricType_COUNTER:
					values[i] = &dto.Metric{
						Counter: &dto.Counter{
							Value: proto.Float64(next.Metric[i].Counter.GetValue()),
						},
					}
				}
			}

			return metrics[index].StoredMetrics
		},
		SubmitWriteRequestCallback: func(wr WriteRequest) {
			prev := (*dto.MetricFamily)(metrics[index].StoredMetrics[metrics[index].Key].Metrics["some_metric"].GobbableMetricFamily).Metric
			next := metrics[index].WriteRequest.MetricFamilies["some_metric"].Metric
			submitted := wr.MetricFamilies["some_metric"].Metric

			for i := 0; i < len(values); i++ {
				switch wr.MetricFamilies["some_metric"].GetType() {
				case dto.MetricType_COUNTER:
					if submitted[i].Counter.GetValue() != next[i].Counter.GetValue() &&
						submitted[i].Counter.GetValue() != prev[i].Counter.GetValue()+values[i].Counter.GetValue() {
						t.Errorf(
							"Submitted counter write request %d didn't match, %f != %f + %f",
							index,
							submitted[i].Counter.GetValue(),
							prev[i].Counter.GetValue(),
							next[i].Counter.GetValue(),
						)
					}
				}

				tested += 1
			}

			index += 1
		},
		ShutdownCallback: func() error {
			return nil
		},
	}

	adms := AtomicDiskMetricStore{
		dms:        dms,
		writeQueue: make(chan WriteRequest, writeQueueCapacity),
		drain:      make(chan bool),
		done:       make(chan bool),
	}

	go adms.loop()

	for i := 0; i < len(metrics); i++ {
		adms.SubmitWriteRequest(metrics[i].WriteRequest)
	}

	adms.Shutdown()

	if index != len(metrics) && tested != len(metrics)*2 {
		t.Error("Not all write requests were handled")
	}
}
