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
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	dto "github.com/prometheus/client_model/go"
)

type AtomicDiskMetricStore struct {
	dms        MetricStore
	lock       sync.RWMutex
	writeQueue chan WriteRequest
	drain      chan bool
	done       chan bool
}

func NewAtomicDiskMetricStore(
	persistenceFile string,
	persistenceInterval time.Duration,
	gaptherPredefinedHelpFrom prometheus.Gatherer,
	logger log.Logger,
) *AtomicDiskMetricStore {
	adms := &AtomicDiskMetricStore{
		dms: NewDiskMetricStore(
			persistenceFile,
			persistenceInterval,
			gaptherPredefinedHelpFrom,
			logger,
		),
		writeQueue: make(chan WriteRequest, writeQueueCapacity),
		drain:      make(chan bool),
		done:       make(chan bool),
	}

	go adms.loop()

	return adms
}

func (adms *AtomicDiskMetricStore) GetMetricFamilies() []*dto.MetricFamily {
	return adms.dms.GetMetricFamilies()
}

func (adms *AtomicDiskMetricStore) GetMetricFamiliesMap() GroupingKeyToMetricGroup {
	return adms.dms.GetMetricFamiliesMap()
}

func (adms *AtomicDiskMetricStore) Healthy() error {
	return adms.dms.Healthy()
}

func (adms *AtomicDiskMetricStore) Ready() error {
	return adms.dms.Ready()
}

func (adms *AtomicDiskMetricStore) Shutdown() error {
	close(adms.drain)
	<-adms.done
	return adms.dms.Shutdown()
}

func (adms *AtomicDiskMetricStore) SubmitWriteRequest(req WriteRequest) {
	adms.writeQueue <- req
}

func (adms *AtomicDiskMetricStore) loop() {
	for {
		select {
		case wr := <-adms.writeQueue:
			adms.processWriteRequest(wr)
		case <-adms.drain:
			for {
				select {
				case wr := <-adms.writeQueue:
					adms.processWriteRequest(wr)
				default:
					adms.done <- true
					return
				}
			}
		}
	}
}

func (adms *AtomicDiskMetricStore) processWriteRequest(wr WriteRequest) {
	adms.lock.Lock()
	defer adms.lock.Unlock()
	defer adms.dms.SubmitWriteRequest(wr)

	if wr.MetricFamilies == nil {
		return
	}

	groupkey := model.LabelsToSignature(wr.Labels)

	for name, mf := range wr.MetricFamilies {
		group, ok := adms.dms.GetMetricFamiliesMap()[groupkey]

		if !ok {
			continue
		}

		switch mf.GetType() {
		case dto.MetricType_COUNTER:
			incrementCounter(name, &group, mf)
		}
	}
}

func getMetricSignature(metric *dto.Metric) uint64 {
	labels := make(map[string]string)

	for _, pair := range metric.GetLabel() {
		labels[pair.GetName()] = pair.GetValue()
	}

	return model.LabelsToSignature(labels)
}

func getStoredMetric(name string, group *MetricGroup, metric *dto.Metric) *dto.Metric {
	sign := getMetricSignature(metric)

	mf := (*dto.MetricFamily)(group.Metrics[name].GobbableMetricFamily)

	for _, sm := range mf.GetMetric() {
		if sign == getMetricSignature(sm) {
			return sm
		}
	}

	return nil
}

func incrementCounter(name string, group *MetricGroup, mf *dto.MetricFamily) {
	for _, metric := range mf.GetMetric() {
		storedMetric := getStoredMetric(name, group, metric)

		if storedMetric != nil && metric.GetCounter() != nil {
			*metric.GetCounter().Value += storedMetric.GetCounter().GetValue()
		}
	}
}
