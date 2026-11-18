/*
Copyright 2022 The KEDA Authors

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

package fallback

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	v2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/scale"
	"k8s.io/metrics/pkg/apis/external_metrics"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/metricscollector"
	"github.com/kedacore/keda/v2/pkg/scaling/resolver"
	"github.com/kedacore/keda/v2/pkg/util"
)

var log = logf.Log.WithName("fallback")

// ScaledObjectHandler encapsulates ScaledObject with necessary clients and locks for performing fallback operations safely in concurrent code
type ScaledObjectHandler struct {
	Ctx          context.Context
	KubeClient   runtimeclient.Client
	ScaleClient  scale.ScalesGetter
	UpdateLock   *sync.RWMutex
	ScaledObject *kedav1alpha1.ScaledObject
}

// UpdateHealthStatus safely serializes updates to ScaledObject's health status and ships them to kube-api.
// It keeps the metric-name keyed behavior for callers that operate on one metric per trigger.
func (h *ScaledObjectHandler) UpdateHealthStatus(metricName string, healthStatus kedav1alpha1.HealthStatus) {
	h.updateHealthStatus(metricName, metricName, healthStatus)
}

// updateHealthStatus updates a trigger-scoped health entry. healthKey identifies the trigger whose
// counter is updated; multiple triggers may expose the same metric name, so their counters stay
// independent.
func (h *ScaledObjectHandler) updateHealthStatus(healthKey string, triggerName string, healthStatus kedav1alpha1.HealthStatus) {
	h.UpdateLock.Lock()
	defer h.UpdateLock.Unlock()

	original := h.ScaledObject.DeepCopy()
	if h.ScaledObject.Status.Health == nil {
		h.ScaledObject.Status.Health = make(map[string]kedav1alpha1.HealthStatus)
	}
	healthStatus.TriggerName = triggerName
	if healthStatus.NumberOfFailures == nil || *healthStatus.NumberOfFailures == 0 {
		healthStatus.LastError = ""
		healthStatus.LastFailureTime = nil
	}
	h.ScaledObject.Status.Health[healthKey] = healthStatus
	h.updateStatus(original)
}

type fallbackStatusUpdate struct {
	triggerName     string
	active          bool
	currentReplicas *int32
	reason          string
}

// IncrementFailure increments the metric-name keyed failure count and persists the change to kube-api.
func (h *ScaledObjectHandler) IncrementFailure(metricName string) kedav1alpha1.HealthStatus {
	return h.incrementFailure(metricName, metricName, nil, nil)
}

// incrementFailure increments the consecutive failure count for a specific trigger.
func (h *ScaledObjectHandler) incrementFailure(healthKey string, triggerName string, cause error, fallbackUpdate *fallbackStatusUpdate) kedav1alpha1.HealthStatus {
	h.UpdateLock.Lock()
	defer h.UpdateLock.Unlock()

	original := h.ScaledObject.DeepCopy()
	if h.ScaledObject.Status.Health == nil {
		h.ScaledObject.Status.Health = make(map[string]kedav1alpha1.HealthStatus)
	}
	healthStatus := h.ScaledObject.Status.Health[healthKey]
	if healthStatus.NumberOfFailures == nil {
		healthStatus.NumberOfFailures = new(int32(0))
	}
	*healthStatus.NumberOfFailures++
	healthStatus.Status = kedav1alpha1.HealthStatusFailing
	healthStatus.TriggerName = triggerName
	if cause != nil {
		healthStatus.LastError = cause.Error()
	}
	now := metav1.Now()
	healthStatus.LastFailureTime = &now
	h.ScaledObject.Status.Health[healthKey] = healthStatus
	if fallbackUpdate != nil {
		h.applyFallbackStatusLocked(fallbackUpdate)
	}
	h.updateStatus(original)
	return healthStatus
}

// UpdateFallbackStatus records which triggers are currently falling back, the observed replica count
// and the error that caused the fallback. It is a no-op when the resulting status would not change.
func (h *ScaledObjectHandler) UpdateFallbackStatus(triggerName string, active bool, currentReplicas *int32, reason string) {
	h.UpdateLock.Lock()
	defer h.UpdateLock.Unlock()

	if !active && h.ScaledObject.Status.Fallback == nil {
		return
	}

	original := h.ScaledObject.DeepCopy()
	h.applyFallbackStatusLocked(&fallbackStatusUpdate{
		triggerName:     triggerName,
		active:          active,
		currentReplicas: currentReplicas,
		reason:          reason,
	})
	h.updateStatus(original)
}

// applyFallbackStatusLocked applies a fallback status change. It must be called with h.UpdateLock held.
func (h *ScaledObjectHandler) applyFallbackStatusLocked(update *fallbackStatusUpdate) {
	triggerName := update.triggerName
	active := update.active
	currentReplicas := update.currentReplicas
	reason := update.reason

	if h.ScaledObject.Status.Fallback == nil {
		h.ScaledObject.Status.Fallback = &kedav1alpha1.FallbackStatus{}
	}
	fallbackStatus := h.ScaledObject.Status.Fallback

	if active {
		fallbackStatus.Active = true
		fallbackStatus.Reason = reason
		if triggerName != "" && !containsString(fallbackStatus.Triggers, triggerName) {
			fallbackStatus.Triggers = append(fallbackStatus.Triggers, triggerName)
			sort.Strings(fallbackStatus.Triggers)
		}
		if currentReplicas != nil {
			replicas := *currentReplicas
			fallbackStatus.CurrentReplicas = &replicas
		} else {
			fallbackStatus.CurrentReplicas = nil
		}
	} else {
		fallbackStatus.Triggers = removeString(fallbackStatus.Triggers, triggerName)
		if !fallbackExceededThreshold(h.ScaledObject.Status, h.ScaledObject.Spec.Fallback.FailureThreshold) {
			fallbackStatus.Active = false
			fallbackStatus.Triggers = nil
			fallbackStatus.CurrentReplicas = nil
			fallbackStatus.Reason = ""
		}
	}
}

func containsString(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func removeString(values []string, value string) []string {
	result := values[:0]
	for _, item := range values {
		if item != value {
			result = append(result, item)
		}
	}
	return result
}

// updateStatus recomputes the fallback condition and patches the status to kube-api if it changed.
// It must be called with h.UpdateLock held; original is a snapshot taken before the health mutation
// so the merge patch carries both the health and condition diffs.
func (h *ScaledObjectHandler) updateStatus(original *kedav1alpha1.ScaledObject) {
	if !isFallbackEnabled(h.ScaledObject) || !HasValidFallback(h.ScaledObject) {
		log.V(1).Info("Fallback is not enabled, hence skipping the health update to the scaledobject", "scaledObject.Namespace", h.ScaledObject.Namespace, "scaledObject.Name", h.ScaledObject.Name)
		return
	}

	if fallbackExceededThreshold(h.ScaledObject.Status, h.ScaledObject.Spec.Fallback.FailureThreshold) {
		h.ScaledObject.Status.Conditions.SetFallbackCondition(metav1.ConditionTrue, "FallbackExists", h.fallbackConditionMessage())
	} else {
		h.ScaledObject.Status.Conditions.SetFallbackCondition(metav1.ConditionFalse, "NoFallbackFound", "No fallbacks are active on this scaled object")
	}

	// Update status only if it has changed
	if equality.Semantic.DeepEqual(original.Status, h.ScaledObject.Status) {
		return
	}

	patch := runtimeclient.MergeFrom(original)
	if err := h.KubeClient.Status().Patch(h.Ctx, h.ScaledObject, patch); err != nil {
		log.Error(err, "failed to patch ScaledObjects Status", "scaledObject.Namespace", h.ScaledObject.Namespace, "scaledObject.Name", h.ScaledObject.Name)
	}
}

func (h *ScaledObjectHandler) fallbackConditionMessage() string {
	triggerNames := make([]string, 0, len(h.ScaledObject.Status.Health))
	for healthKey, health := range h.ScaledObject.Status.Health {
		if health.Status != kedav1alpha1.HealthStatusFailing || health.NumberOfFailures == nil {
			continue
		}
		if *health.NumberOfFailures <= h.ScaledObject.Spec.Fallback.FailureThreshold {
			continue
		}
		name := health.TriggerName
		if name == "" {
			name = healthKey
		}
		triggerNames = append(triggerNames, name)
	}
	sort.Strings(triggerNames)

	message := "At least one trigger is falling back on this scaled object"
	if len(triggerNames) > 0 {
		message = fmt.Sprintf("Fallback is active for trigger(s): %s", strings.Join(triggerNames, ", "))
	}
	if h.ScaledObject.Status.Fallback != nil && h.ScaledObject.Status.Fallback.CurrentReplicas != nil {
		message = fmt.Sprintf("%s; current replicas: %d", message, *h.ScaledObject.Status.Fallback.CurrentReplicas)
	}
	return message
}

func isFallbackEnabled(scaledObject *kedav1alpha1.ScaledObject) bool {
	return scaledObject.Spec.Fallback != nil
}

func GetMetricsWithFallback(soh ScaledObjectHandler, metrics []external_metrics.ExternalMetricValue, suppressedError error, metricName string, metricSpec v2.MetricSpec) ([]external_metrics.ExternalMetricValue, bool, error) {
	return GetMetricsWithFallbackForTrigger(soh, metrics, suppressedError, metricName, metricName, metricName, metricSpec)
}

// GetMetricsWithFallbackForTrigger applies fallback for a single trigger. healthKey must uniquely
// identify the trigger, while triggerName is the user-facing name used in status, events and metrics.
func GetMetricsWithFallbackForTrigger(soh ScaledObjectHandler, metrics []external_metrics.ExternalMetricValue, suppressedError error, triggerName string, healthKey string, metricName string, metricSpec v2.MetricSpec) ([]external_metrics.ExternalMetricValue, bool, error) {
	if triggerName == "" {
		triggerName = metricName
	}
	if healthKey == "" {
		healthKey = triggerName
	}

	soh.UpdateLock.RLock()
	scaledObject := soh.ScaledObject.DeepCopy()
	soh.UpdateLock.RUnlock()

	// Health is only tracked for ScaledObjects with a valid fallback configured. For all others we
	// must leave the status untouched, so the enabled/valid checks have to happen before any mutation.
	fallbackConfigured := isFallbackEnabled(scaledObject) && HasValidFallback(scaledObject)

	// A trigger that returns no metrics at all is not a healthy reading when fallback is configured.
	// Treat it like any other trigger error so the failure counter, status and events explain why.
	if suppressedError == nil && fallbackConfigured && len(metrics) == 0 {
		suppressedError = fmt.Errorf("trigger %q returned no metrics", triggerName)
	}

	if suppressedError == nil {
		if fallbackConfigured {
			soh.updateHealthStatus(healthKey, triggerName, kedav1alpha1.HealthStatus{
				NumberOfFailures: new(int32(0)),
				Status:           kedav1alpha1.HealthStatusHappy,
			})
			soh.UpdateFallbackStatus(triggerName, false, nil, "")
		}
		metricscollector.RecordFallback(scaledObject.Namespace, scaledObject.Name, triggerName, false, 0)
		return metrics, false, nil
	}

	switch {
	case !isFallbackEnabled(scaledObject):
		return nil, false, suppressedError
	case !HasValidFallback(scaledObject):
		log.Info("Failed to validate ScaledObject Spec. Please check that parameters are positive integers", "scaledObject.Namespace", scaledObject.Namespace, "scaledObject.Name", scaledObject.Name)
		return nil, false, suppressedError
	}

	previousFailures := int32(0)
	if previous, ok := scaledObject.Status.Health[healthKey]; ok && previous.NumberOfFailures != nil {
		previousFailures = *previous.NumberOfFailures
	}
	willExceedThreshold := previousFailures+1 > scaledObject.Spec.Fallback.FailureThreshold

	var currentReplicas int32
	var currentReplicasPtr *int32
	var currentReplicasErr error
	if willExceedThreshold && scaledObject.Spec.Fallback.Behavior != kedav1alpha1.FallbackBehaviorScalingModifiers {
		currentReplicas, currentReplicasErr = resolver.GetCurrentReplicas(soh.Ctx, soh.KubeClient, soh.ScaleClient, soh.ScaledObject)
		if currentReplicasErr == nil {
			currentReplicasPtr = new(currentReplicas)
		}
	}

	var fallbackUpdate *fallbackStatusUpdate
	if willExceedThreshold {
		fallbackUpdate = &fallbackStatusUpdate{
			triggerName:     triggerName,
			active:          true,
			currentReplicas: currentReplicasPtr,
			reason:          suppressedError.Error(),
		}
	}

	healthStatus := soh.incrementFailure(healthKey, triggerName, suppressedError, fallbackUpdate)

	if *healthStatus.NumberOfFailures > scaledObject.Spec.Fallback.FailureThreshold {
		return handleFallbackThresholdExceeded(soh, scaledObject, triggerName, metricName, metricSpec,
			suppressedError, fallbackUpdate, currentReplicas, currentReplicasPtr, currentReplicasErr)
	}

	if fallbackUpdate != nil {
		soh.UpdateFallbackStatus(triggerName, false, nil, "")
	}
	metricscollector.RecordFallback(scaledObject.Namespace, scaledObject.Name, triggerName, false, 0)
	return nil, false, suppressedError
}

func handleFallbackThresholdExceeded(soh ScaledObjectHandler, scaledObject *kedav1alpha1.ScaledObject, triggerName string, metricName string,
	metricSpec v2.MetricSpec, suppressedError error, fallbackUpdate *fallbackStatusUpdate, currentReplicas int32,
	currentReplicasPtr *int32, currentReplicasErr error) ([]external_metrics.ExternalMetricValue, bool, error) {
	if scaledObject.Spec.Fallback.Behavior == kedav1alpha1.FallbackBehaviorScalingModifiers {
		if fallbackUpdate == nil {
			soh.UpdateFallbackStatus(triggerName, true, nil, suppressedError.Error())
		}
		metricscollector.RecordFallback(scaledObject.Namespace, scaledObject.Name, triggerName, true, 0)
		// scaling modifier expression engine will treat this as "nil" for the "??" operator
		placeholderMetric := external_metrics.ExternalMetricValue{
			MetricName: metricName,
			Value:      *resource.NewQuantity(-1, resource.DecimalSI),
			Timestamp:  metav1.Now(),
		}
		return []external_metrics.ExternalMetricValue{placeholderMetric}, false, nil
	}

	// Record the current replica count for the fallback status, event and metric. For static
	// fallback the replica count is informational only; for the other behaviors it is required.
	if scaledObject.Spec.Fallback.Behavior != kedav1alpha1.FallbackBehaviorStatic && currentReplicasErr == nil && fallbackUpdate == nil {
		currentReplicas, currentReplicasErr = resolver.GetCurrentReplicas(soh.Ctx, soh.KubeClient, soh.ScaleClient, soh.ScaledObject)
		if currentReplicasErr == nil {
			currentReplicasPtr = new(currentReplicas)
		}
	}
	if currentReplicasErr != nil {
		log.Error(currentReplicasErr, "failed to read current replicas for fallback", "scaledObject.Namespace", scaledObject.Namespace, "scaledObject.Name", scaledObject.Name, "trigger", triggerName, "reason", suppressedError.Error())
		if scaledObject.Spec.Fallback.Behavior != kedav1alpha1.FallbackBehaviorStatic {
			if fallbackUpdate == nil {
				soh.UpdateFallbackStatus(triggerName, true, nil, suppressedError.Error())
			}
			metricscollector.RecordFallback(scaledObject.Namespace, scaledObject.Name, triggerName, false, 0)
			return nil, false, suppressedError
		}
	}

	fallbackMetrics := doFallback(soh, metricSpec, metricName, currentReplicas, suppressedError)
	if fallbackMetrics == nil {
		if fallbackUpdate != nil {
			soh.UpdateFallbackStatus(triggerName, false, nil, "")
		}
		return nil, false, fmt.Errorf("error performing fallback")
	}
	if fallbackUpdate == nil {
		soh.UpdateFallbackStatus(triggerName, true, currentReplicasPtr, suppressedError.Error())
	}
	metricscollector.RecordFallback(scaledObject.Namespace, scaledObject.Name, triggerName, true, currentReplicas)
	return fallbackMetrics, true, nil
}

func fallbackExceededThreshold(status kedav1alpha1.ScaledObjectStatus, failureThreshold int32) bool {
	for _, element := range status.Health {
		if element.Status == kedav1alpha1.HealthStatusFailing && element.NumberOfFailures != nil && *element.NumberOfFailures > failureThreshold {
			return true
		}
	}

	return false
}

func HasValidFallback(scaledObject *kedav1alpha1.ScaledObject) bool {
	modifierChecking := true
	if scaledObject.IsUsingModifiers() {
		value, err := strconv.ParseInt(scaledObject.Spec.Advanced.ScalingModifiers.Target, 10, 64)
		modifierChecking = err == nil && value > 0
	}
	return scaledObject.Spec.Fallback.FailureThreshold >= 0 &&
		scaledObject.Spec.Fallback.Replicas >= 0 &&
		modifierChecking
}

// Similar to how it's done in HPA's code: https://github.com/kubernetes/kubernetes/blob/091f87c10bc3532041b77a783a5f832de5506dc8/pkg/controller/podautoscaler/replica_calculator.go#L323
func getReadyReplicasCount(soh ScaledObjectHandler) (int32, error) {
	scaledObject := soh.ScaledObject
	if scaledObject == nil || scaledObject.Spec.ScaleTargetRef == nil || scaledObject.Status.ScaleTargetGVKR == nil {
		return -1, fmt.Errorf("")
	}

	scale, err := soh.ScaleClient.Scales(scaledObject.Namespace).Get(soh.Ctx, scaledObject.Status.ScaleTargetGVKR.GroupResource(), scaledObject.Spec.ScaleTargetRef.Name, metav1.GetOptions{})
	if err != nil {
		return -1, err
	}

	parsedSelector, err := labels.Parse(scale.Status.Selector)
	if err != nil {
		return -1, err
	}

	podList := v1.PodList{}
	err = soh.KubeClient.List(soh.Ctx, &podList, runtimeclient.InNamespace(scaledObject.Namespace), runtimeclient.MatchingLabelsSelector{Selector: parsedSelector})
	if err != nil {
		return -1, err
	}

	var readyPodCount int32
	for _, pod := range podList.Items {
		if pod.Status.Phase == v1.PodRunning && util.IsPodReady(&pod) {
			readyPodCount++
		}
	}

	return readyPodCount, nil
}

func doFallback(soh ScaledObjectHandler, metricSpec v2.MetricSpec, metricName string, currentReplicas int32, suppressedError error) []external_metrics.ExternalMetricValue {
	scaledObject := soh.ScaledObject
	fallbackBehavior := scaledObject.Spec.Fallback.Behavior
	fallbackReplicas := int64(scaledObject.Spec.Fallback.Replicas)
	var replicas float64

	switch fallbackBehavior {
	case kedav1alpha1.FallbackBehaviorStatic:
		replicas = float64(fallbackReplicas)
	case kedav1alpha1.FallbackBehaviorCurrentReplicas:
		replicas = float64(currentReplicas)
	case kedav1alpha1.FallbackBehaviorCurrentReplicasIfHigher:
		currentReplicasCount := int64(currentReplicas)
		if currentReplicasCount > fallbackReplicas {
			replicas = float64(currentReplicasCount)
		} else {
			replicas = float64(fallbackReplicas)
		}
	case kedav1alpha1.FallbackBehaviorCurrentReplicasIfLower:
		currentReplicasCount := int64(currentReplicas)
		if currentReplicasCount < fallbackReplicas {
			replicas = float64(currentReplicasCount)
		} else {
			replicas = float64(fallbackReplicas)
		}
	case kedav1alpha1.FallbackBehaviorScalingModifiers:
		return nil
	default:
		replicas = float64(fallbackReplicas)
	}

	// If the metricType is Value, we get the number of readyReplicas, and divide replicas by it.
	if (!scaledObject.IsUsingModifiers() && metricSpec.External.Target.Type == v2.ValueMetricType) ||
		(scaledObject.IsUsingModifiers() && scaledObject.Spec.Advanced.ScalingModifiers.MetricType == v2.ValueMetricType) {
		readyReplicas, err := getReadyReplicasCount(soh)
		if err != nil {
			log.Error(err, "failed to do fallback for metric of type Value", "scaledObject.Namespace", scaledObject.Namespace, "scaledObject.Name", scaledObject.Name, "metricName", metricName)
			return nil
		}
		if readyReplicas == 0 {
			log.Error(fmt.Errorf("readyReplicas is zero, cannot do fallback"), "failed to do fallback for metric of type Value", "scaledObject.Namespace", scaledObject.Namespace, "scaledObject.Name", scaledObject.Name, "metricName", metricName)
			return nil
		}
		replicas /= float64(readyReplicas)
	}

	var normalisationValue float64
	if !scaledObject.IsUsingModifiers() {
		switch metricSpec.External.Target.Type {
		case v2.AverageValueMetricType:
			normalisationValue = metricSpec.External.Target.AverageValue.AsApproximateFloat64()
		case v2.ValueMetricType:
			normalisationValue = metricSpec.External.Target.Value.AsApproximateFloat64()
		}
	} else {
		value, _ := strconv.ParseFloat(scaledObject.Spec.Advanced.ScalingModifiers.Target, 64)
		normalisationValue = value
		metricName = kedav1alpha1.CompositeMetricName
	}

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewMilliQuantity(int64(normalisationValue*1000*replicas), resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}
	fallbackMetrics := []external_metrics.ExternalMetricValue{metric}

	log.Info("Suppressing error, using fallback metrics",
		"scaledObject.Namespace", scaledObject.Namespace,
		"scaledObject.Name", scaledObject.Name,
		"suppressedError", suppressedError,
		"fallback.behavior", fallbackBehavior,
		"fallback.replicas", fallbackReplicas,
		"workload.currentReplicas", currentReplicas)
	return fallbackMetrics
}
