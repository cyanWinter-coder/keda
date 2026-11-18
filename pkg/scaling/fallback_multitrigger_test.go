/*
Copyright 2026 The KEDA Authors

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

package scaling

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	v2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/metrics/pkg/apis/external_metrics"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/mock/mock_client"
	mock_scalers "github.com/kedacore/keda/v2/pkg/mock/mock_scaler"
	"github.com/kedacore/keda/v2/pkg/mock/mock_scaling/mock_executor"
	"github.com/kedacore/keda/v2/pkg/scalers"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	"github.com/kedacore/keda/v2/pkg/scaling/cache"
	"github.com/kedacore/keda/v2/pkg/scaling/cache/metricscache"
)

func TestGetScaledObjectState_MultipleTriggersFallbackAndRecovery(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_client.NewMockClient(ctrl)
	statusWriter := mock_client.NewMockStatusWriter(ctrl)
	recorder := events.NewFakeRecorder(10)

	fallingTriggerFailing := atomic.NewBool(true)

	failingScaler := mock_scalers.NewMockScaler(ctrl)
	failingConfig := scalersconfig.ScalerConfig{TriggerName: "trigger-a", TriggerType: "fake-a", TriggerIndex: 0}
	failingScaler.EXPECT().GetMetricSpecForScaling(gomock.Any()).Return([]v2.MetricSpec{createMetricSpec(10, "metric-a")}).AnyTimes()
	failingScaler.EXPECT().GetMetricsAndActivity(gomock.Any(), "metric-a").DoAndReturn(
		func(context.Context, string) ([]external_metrics.ExternalMetricValue, bool, error) {
			if fallingTriggerFailing.Load() {
				return nil, false, errors.New("trigger-a is unavailable")
			}
			return []external_metrics.ExternalMetricValue{scalers.GenerateMetricInMili("metric-a", 10)}, true, nil
		}).AnyTimes()
	failingScaler.EXPECT().Close(gomock.Any()).AnyTimes()

	activeScaler := mock_scalers.NewMockScaler(ctrl)
	activeConfig := scalersconfig.ScalerConfig{TriggerName: "trigger-b", TriggerType: "fake-b", TriggerIndex: 1}
	activeScaler.EXPECT().GetMetricSpecForScaling(gomock.Any()).Return([]v2.MetricSpec{createMetricSpec(10, "metric-b")}).AnyTimes()
	activeScaler.EXPECT().GetMetricsAndActivity(gomock.Any(), "metric-b").
		Return([]external_metrics.ExternalMetricValue{scalers.GenerateMetricInMili("metric-b", 10)}, true, nil).AnyTimes()
	activeScaler.EXPECT().Close(gomock.Any()).AnyTimes()

	minReplicas := int32(0)
	scaledObject := &kedav1alpha1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: testNamespaceGlobal},
		Spec: kedav1alpha1.ScaledObjectSpec{
			ScaleTargetRef:  &kedav1alpha1.ScaleTarget{Name: "test"},
			MinReplicaCount: &minReplicas,
			Fallback: &kedav1alpha1.Fallback{
				FailureThreshold: 2,
				Replicas:         3,
			},
			Triggers: []kedav1alpha1.ScaleTriggers{
				{Name: "trigger-a", Type: "fake-a"},
				{Name: "trigger-b", Type: "fake-b"},
			},
		},
		Status: kedav1alpha1.ScaledObjectStatus{
			ScaleTargetGVKR: &kedav1alpha1.GroupVersionKindResource{Group: "apps", Kind: "Deployment"},
			Health: map[string]kedav1alpha1.HealthStatus{
				"trigger-a": {NumberOfFailures: new(int32(2)), Status: kedav1alpha1.HealthStatusFailing},
				"trigger-b": {NumberOfFailures: new(int32(0)), Status: kedav1alpha1.HealthStatusHappy},
			},
			Conditions: *kedav1alpha1.GetInitializedConditions(),
		},
	}

	scalerCache := &cache.ScalersCache{
		ScaledObject: scaledObject,
		Scalers: []cache.ScalerBuilder{
			{
				Scaler:       failingScaler,
				ScalerConfig: failingConfig,
				Factory: func(context.Context) (scalers.Scaler, *scalersconfig.ScalerConfig, error) {
					return failingScaler, &failingConfig, nil
				},
			},
			{
				Scaler:       activeScaler,
				ScalerConfig: activeConfig,
				Factory: func(context.Context) (scalers.Scaler, *scalersconfig.ScalerConfig, error) {
					return activeScaler, &activeConfig, nil
				},
			},
		},
		Recorder: recorder,
	}

	sh := &scaleHandler{
		client:                   mockClient,
		scaleLoopContexts:        &sync.Map{},
		scaleExecutor:            mock_executor.NewMockScaleExecutor(ctrl),
		globalHTTPTimeout:        time.Second,
		recorder:                 recorder,
		scalerCaches:             map[string]*cache.ScalersCache{scaledObject.GenerateIdentifier(): scalerCache},
		scalerCachesLock:         &sync.RWMutex{},
		scaledObjectsMetricCache: metricscache.NewMetricsCache(),
		rawMetricsSubscriptions:  map[string]*RawMetricSubscriptions{},
		metricToSubscriptions:    map[metricMeta][]*RawMetricSubscriptions{},
		subsLock:                 &sync.RWMutex{},
	}

	statusWriter.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockClient.EXPECT().Status().Return(statusWriter).AnyTimes()
	replicas := int32(3)
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.AssignableToTypeOf(&appsv1.Deployment{})).
		DoAndReturn(func(_ context.Context, _ types.NamespacedName, deployment *appsv1.Deployment, _ ...any) error {
			deployment.Spec.Replicas = &replicas
			return nil
		}).AnyTimes()

	// trigger-a is exactly at the threshold; the next failure activates fallback while trigger-b
	// remains active.
	isActive, isError, _, activeTriggers, isFallbackActive, err := sh.getScaledObjectState(context.Background(), scaledObject)
	require.NoError(t, err)
	assert.True(t, isActive)
	assert.False(t, isError)
	assert.True(t, isFallbackActive)
	assert.ElementsMatch(t, []string{"metric-a", "metric-b"}, activeTriggers)
	assert.Equal(t, int32(3), *scaledObject.Status.Health["trigger-a"].NumberOfFailures)
	assert.Equal(t, int32(0), *scaledObject.Status.Health["trigger-b"].NumberOfFailures)
	require.NotNil(t, scaledObject.Status.Fallback)
	assert.True(t, scaledObject.Status.Fallback.Active)
	assert.Equal(t, []string{"trigger-a"}, scaledObject.Status.Fallback.Triggers)
	require.NotNil(t, scaledObject.Status.Fallback.CurrentReplicas)
	assert.Equal(t, int32(3), *scaledObject.Status.Fallback.CurrentReplicas)
	fallbackCondition := scaledObject.Status.Conditions.GetFallbackCondition()
	assert.True(t, fallbackCondition.IsTrue())

	// A second failing poll keeps the same fallback and replica count instead of oscillating.
	isActive, isError, _, _, isFallbackActive, err = sh.getScaledObjectState(context.Background(), scaledObject)
	require.NoError(t, err)
	assert.True(t, isActive)
	assert.False(t, isError)
	assert.True(t, isFallbackActive)
	assert.Equal(t, int32(4), *scaledObject.Status.Health["trigger-a"].NumberOfFailures)
	assert.Equal(t, []string{"trigger-a"}, scaledObject.Status.Fallback.Triggers)
	assert.Equal(t, int32(3), *scaledObject.Status.Fallback.CurrentReplicas)

	// After recovery, trigger-a's counter is cleared, fallback ends and trigger-b stays active.
	fallingTriggerFailing.Store(false)
	isActive, isError, _, activeTriggers, isFallbackActive, err = sh.getScaledObjectState(context.Background(), scaledObject)
	require.NoError(t, err)
	assert.True(t, isActive)
	assert.False(t, isError)
	assert.False(t, isFallbackActive)
	assert.ElementsMatch(t, []string{"metric-a", "metric-b"}, activeTriggers)
	assert.Equal(t, int32(0), *scaledObject.Status.Health["trigger-a"].NumberOfFailures)
	assert.Equal(t, kedav1alpha1.HealthStatusHappy, scaledObject.Status.Health["trigger-a"].Status)
	assert.False(t, scaledObject.Status.Fallback.Active)
	fallbackCondition = scaledObject.Status.Conditions.GetFallbackCondition()
	assert.True(t, fallbackCondition.IsFalse())
}

func TestFallbackEventMessagesIncludeTriggerAndReplicas(t *testing.T) {
	replicas := int32(4)
	scaledObject := &kedav1alpha1.ScaledObject{
		Status: kedav1alpha1.ScaledObjectStatus{
			Fallback: &kedav1alpha1.FallbackStatus{
				Active:          true,
				Triggers:        []string{"trigger-a", "trigger-b"},
				CurrentReplicas: &replicas,
				Reason:          "trigger-a is unavailable",
			},
		},
	}

	activated := fallbackActivatedMessage(scaledObject)
	assert.Contains(t, activated, "trigger-a, trigger-b")
	assert.Contains(t, activated, "current replicas: 4")
	assert.Contains(t, activated, "trigger-a is unavailable")

	deactivated := fallbackDeactivatedMessage(scaledObject)
	assert.Contains(t, deactivated, "current replicas: 4")
}
