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
	"encoding/base64"
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
	Name string
	Key  uint64
	WriteRequest
	StoredMetrics GroupingKeyToMetricGroup
}

func generateCounterMetricFamily(name string) *dto.MetricFamily {
	return &dto.MetricFamily{
		Name: proto.String(name),
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

func generateHistogramMetricFamily(name string) *dto.MetricFamily {
	return &dto.MetricFamily{
		Name: proto.String(name),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{
			&dto.Metric{
				Label: []*dto.LabelPair{
					&dto.LabelPair{
						Name:  proto.String("some-label"),
						Value: proto.String("with-value"),
					},
				},
				Histogram: &dto.Histogram{
					SampleCount: proto.Uint64(rand.Uint64()),
					SampleSum:   proto.Float64(rand.Float64() * float64(rand.Int())),
					Bucket: []*dto.Bucket{
						&dto.Bucket{
							CumulativeCount: proto.Uint64(rand.Uint64()),
							UpperBound:      proto.Float64(rand.Float64() * float64(rand.Int())),
						},
					},
				},
			},
		},
	}
}

func generateMetricRequests() []*MetricTestRequest {
	var metricRequests []*MetricTestRequest

	length := rand.Intn(30)

	for i := 0; i < length; i++ {
		rand.Seed(int64(i))

		name := randStr(10)
		labels := map[string]string{
			randStr(10): randStr(20),
		}
		key := model.LabelsToSignature(labels)

		var generator func(string) *dto.MetricFamily

		if rand.Float32() < 0.5 {
			generator = generateCounterMetricFamily
		} else {
			generator = generateHistogramMetricFamily
		}

		metricRequests = append(metricRequests, &MetricTestRequest{
			Name: name,
			Key:  key,
			WriteRequest: WriteRequest{
				Labels: labels,
				MetricFamilies: map[string]*dto.MetricFamily{
					name: generator(name),
				},
			},
			StoredMetrics: GroupingKeyToMetricGroup{
				key: MetricGroup{
					Labels: labels,
					Metrics: NameToTimestampedMetricFamilyMap{
						name: TimestampedMetricFamily{
							Timestamp:            time.Now(),
							GobbableMetricFamily: (*GobbableMetricFamily)(generator(name)),
						},
					},
				},
			},
		})
	}

	return metricRequests
}

func randStr(length int) string {
	buf := make([]byte, length)
	rand.Read(buf)
	str := base64.StdEncoding.EncodeToString(buf)
	return str[:length]
}

func TestAggregation(t *testing.T) {
	metrics := generateMetricRequests()
	index := 0
	values := make(map[string][]*dto.Metric)

	dms := &MockedDiskMetricStore{
		GetMetricFamiliesMapCallback: func() GroupingKeyToMetricGroup {
			name := metrics[index].Name
			next := metrics[index].WriteRequest.MetricFamilies[name]
			values[name] = make([]*dto.Metric, len(next.Metric))

			for i := 0; i < len(next.Metric); i++ {
				switch next.GetType() {
				case dto.MetricType_COUNTER:
					values[name][i] = &dto.Metric{
						Counter: &dto.Counter{
							Value: proto.Float64(next.Metric[i].Counter.GetValue()),
						},
					}
				case dto.MetricType_HISTOGRAM:
					buckets := make([]*dto.Bucket, len(next.Metric[i].Histogram.Bucket))

					for ii := 0; ii < len(buckets); ii++ {
						buckets[ii] = &dto.Bucket{
							CumulativeCount: proto.Uint64(next.Metric[i].Histogram.Bucket[ii].GetCumulativeCount()),
							UpperBound:      proto.Float64(next.Metric[i].Histogram.Bucket[ii].GetUpperBound()),
						}
					}

					values[name][i] = &dto.Metric{
						Histogram: &dto.Histogram{
							SampleCount: proto.Uint64(next.Metric[i].Histogram.GetSampleCount()),
							SampleSum:   proto.Float64(next.Metric[i].Histogram.GetSampleSum()),
							Bucket:      buckets,
						},
					}
				}
			}

			return metrics[index].StoredMetrics
		},
		SubmitWriteRequestCallback: func(wr WriteRequest) {
			name := metrics[index].Name
			next := values[name]

			prev := (*dto.MetricFamily)(metrics[index].StoredMetrics[metrics[index].Key].Metrics[name].GobbableMetricFamily).Metric
			submitted := wr.MetricFamilies[name].Metric

			for i := 0; i < len(next); i++ {
				switch wr.MetricFamilies[name].GetType() {
				case dto.MetricType_COUNTER:
					if submitted[i].Counter.GetValue() != prev[i].Counter.GetValue()+next[i].Counter.GetValue() {
						t.Errorf(
							"Submitted counter write request didn't match, %f != %f + %f",
							submitted[i].Counter.GetValue(),
							prev[i].Counter.GetValue(),
							next[i].Counter.GetValue(),
						)
					}
				case dto.MetricType_HISTOGRAM:
					if submitted[i].Histogram.GetSampleCount() != prev[i].Histogram.GetSampleCount()+next[i].Histogram.GetSampleCount() {
						t.Errorf("Submitted histogram sample counter write request didn't match")
					}

					if submitted[i].Histogram.GetSampleSum() != prev[i].Histogram.GetSampleSum()+next[i].Histogram.GetSampleSum() {
						t.Errorf("Submitted histogram sample sum write request didn't match")
					}

					for ii := 0; ii < len(next[i].Histogram.Bucket); ii++ {
						if submitted[i].Histogram.Bucket[ii].GetCumulativeCount() != prev[i].Histogram.Bucket[ii].GetCumulativeCount()+next[i].Histogram.Bucket[ii].GetCumulativeCount() {
							t.Errorf("Submitted histogram bucket counter write request didn't match")
						}

						upperBound := prev[i].Histogram.Bucket[ii].GetUpperBound()

						if upperBound < next[i].Histogram.Bucket[ii].GetUpperBound() {
							upperBound = next[i].Histogram.Bucket[ii].GetUpperBound()
						}

						if submitted[i].Histogram.Bucket[ii].GetUpperBound() != upperBound {
							t.Errorf("Submitted histogram bucket upper bound write request didn't match")
						}
					}
				}
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

	if index != len(metrics) {
		t.Errorf("Not all write requests were handled, %d != %d", index, len(metrics))
	}
}
