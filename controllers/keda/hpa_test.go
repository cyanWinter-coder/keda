/*
Copyright 2021 The KEDA Authors

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

package keda

import (
	"context"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	v2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/mock/mock_client"
	mock_scalers "github.com/kedacore/keda/v2/pkg/mock/mock_scaler"
	"github.com/kedacore/keda/v2/pkg/mock/mock_scaling"
	"github.com/kedacore/keda/v2/pkg/scalers"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	"github.com/kedacore/keda/v2/pkg/scaling/cache"
)

var _ = Describe("hpa", func() {
	var (
		reconciler   ScaledObjectReconciler
		scaleHandler *mock_scaling.MockScaleHandler
		client       *mock_client.MockClient
		statusWriter *mock_client.MockStatusWriter
		scaler       *mock_scalers.MockScaler
		logger       logr.Logger
		ctrl         *gomock.Controller
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		client = mock_client.NewMockClient(ctrl)
		scaleHandler = mock_scaling.NewMockScaleHandler(ctrl)
		scaler = mock_scalers.NewMockScaler(ctrl)
		statusWriter = mock_client.NewMockStatusWriter(ctrl)
		logger = logr.Discard()
		reconciler = ScaledObjectReconciler{
			Client:       client,
			ScaleHandler: scaleHandler,
		}
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	It("should remove deleted metric from health status", func() {
		numberOfFailures := int32(87)
		health := make(map[string]v1alpha1.HealthStatus)
		health["another metric name"] = v1alpha1.HealthStatus{
			NumberOfFailures: &numberOfFailures,
			Status:           v1alpha1.HealthStatusFailing,
		}

		scaledObject := setupTest(health, scaler, scaleHandler)

		var capturedScaledObject v1alpha1.ScaledObject
		client.EXPECT().Status().Return(statusWriter)
		statusWriter.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(arg any, scaledObject *v1alpha1.ScaledObject, anotherArg any, opts ...any) {
			capturedScaledObject = *scaledObject
		})

		_, err := reconciler.getScaledObjectMetricSpecs(context.Background(), logger, scaledObject)

		Expect(err).ToNot(HaveOccurred())
		Expect(capturedScaledObject.Status.Health).To(BeEmpty())
	})

	It("should return error when scalers return empty metric specs", func() {
		scaledObject := &v1alpha1.ScaledObject{
			ObjectMeta: v1.ObjectMeta{
				Name:      "test-scaled-object",
				Namespace: "test-namespace",
			},
		}

		emptyScaler := mock_scalers.NewMockScaler(ctrl)
		scalersCache := cache.ScalersCache{
			Scalers: []cache.ScalerBuilder{{
				Scaler: emptyScaler,
				Factory: func(context.Context) (scalers.Scaler, *scalersconfig.ScalerConfig, error) {
					return emptyScaler, &scalersconfig.ScalerConfig{}, nil
				},
			}},
			Recorder: nil,
		}

		ctx := context.Background()
		emptyScaler.EXPECT().GetMetricSpecForScaling(ctx).Return([]v2.MetricSpec{})
		scaleHandler.EXPECT().GetScalersCache(ctx, gomock.Eq(scaledObject)).Return(&scalersCache, nil)

		specs, err := reconciler.getScaledObjectMetricSpecs(ctx, logger, scaledObject)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no metric specs returned from scalers"))
		Expect(specs).To(BeNil())
	})

	It("should return error when scalers return nil metric specs", func() {
		scaledObject := &v1alpha1.ScaledObject{
			ObjectMeta: v1.ObjectMeta{
				Name:      "test-scaled-object",
				Namespace: "test-namespace",
			},
		}

		emptyScaler := mock_scalers.NewMockScaler(ctrl)
		scalersCache := cache.ScalersCache{
			Scalers: []cache.ScalerBuilder{{
				Scaler: emptyScaler,
				Factory: func(context.Context) (scalers.Scaler, *scalersconfig.ScalerConfig, error) {
					return emptyScaler, &scalersconfig.ScalerConfig{}, nil
				},
			}},
			Recorder: nil,
		}

		ctx := context.Background()
		emptyScaler.EXPECT().GetMetricSpecForScaling(ctx).Return(nil)
		scaleHandler.EXPECT().GetScalersCache(ctx, gomock.Eq(scaledObject)).Return(&scalersCache, nil)

		specs, err := reconciler.getScaledObjectMetricSpecs(ctx, logger, scaledObject)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no metric specs returned from scalers"))
		Expect(specs).To(BeNil())
	})

	It("should not remove existing metric from health status", func() {
		numberOfFailures := int32(87)
		health := make(map[string]v1alpha1.HealthStatus)
		health["another metric name"] = v1alpha1.HealthStatus{
			NumberOfFailures: &numberOfFailures,
			Status:           v1alpha1.HealthStatusFailing,
		}

		health["some metric name"] = v1alpha1.HealthStatus{
			NumberOfFailures: &numberOfFailures,
			Status:           v1alpha1.HealthStatusFailing,
		}

		scaledObject := setupTest(health, scaler, scaleHandler)

		var capturedScaledObject v1alpha1.ScaledObject
		client.EXPECT().Status().Return(statusWriter)
		statusWriter.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(arg any, scaledObject *v1alpha1.ScaledObject, anotherArg any, opts ...any) {
			capturedScaledObject = *scaledObject
		})

		_, err := reconciler.getScaledObjectMetricSpecs(context.Background(), logger, scaledObject)

		expectedHealth := make(map[string]v1alpha1.HealthStatus)
		expectedHealth["some metric name"] = v1alpha1.HealthStatus{
			NumberOfFailures: &numberOfFailures,
			Status:           v1alpha1.HealthStatusFailing,
		}

		Expect(err).ToNot(HaveOccurred())
		Expect(capturedScaledObject.Status.Health).To(HaveLen(1))
		Expect(capturedScaledObject.Status.Health).To(Equal(expectedHealth))
	})

})

func setupTest(health map[string]v1alpha1.HealthStatus, scaler *mock_scalers.MockScaler, scaleHandler *mock_scaling.MockScaleHandler) *v1alpha1.ScaledObject {
	scaledObject := &v1alpha1.ScaledObject{
		ObjectMeta: v1.ObjectMeta{
			Name: "some scaled object name",
		},
		Status: v1alpha1.ScaledObjectStatus{
			Health: health,
		},
	}

	scalersCache := cache.ScalersCache{
		Scalers: []cache.ScalerBuilder{{
			Scaler: scaler,
			Factory: func(context.Context) (scalers.Scaler, *scalersconfig.ScalerConfig, error) {
				return scaler, &scalersconfig.ScalerConfig{}, nil
			},
		}},
		Recorder: nil,
	}
	metricSpec := v2.MetricSpec{
		External: &v2.ExternalMetricSource{
			Metric: v2.MetricIdentifier{
				Name: "some metric name",
			},
		},
	}
	metricSpecs := []v2.MetricSpec{metricSpec}
	ctx := context.Background()
	scaler.EXPECT().GetMetricSpecForScaling(ctx).Return(metricSpecs)
	scaleHandler.EXPECT().GetScalersCache(context.Background(), gomock.Eq(scaledObject)).Return(&scalersCache, nil)

	return scaledObject
}

var _ = Describe("updateHPAWithConflictRetry", func() {
	var (
		reconciler ScaledObjectReconciler
		client     *mock_client.MockClient
		logger     logr.Logger
		ctrl       *gomock.Controller
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		client = mock_client.NewMockClient(ctrl)
		logger = logr.Discard()
		reconciler = ScaledObjectReconciler{Client: client}
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	namespacedName := types.NamespacedName{Name: "keda-hpa-test", Namespace: "test-ns"}
	conflict := func() error {
		return apierrors.NewConflict(schema.GroupResource{Group: "autoscaling", Resource: "horizontalpodautoscalers"}, "keda-hpa-test",
			context.DeadlineExceeded)
	}

	It("retries against a fresh object when the first update conflicts", func() {
		// first read -> update conflicts; second read -> update succeeds
		client.EXPECT().Get(gomock.Any(), namespacedName, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ types.NamespacedName, hpa *v2.HorizontalPodAutoscaler, _ ...any) error {
				hpa.ObjectMeta = v1.ObjectMeta{Name: "keda-hpa-test", Namespace: "test-ns", ResourceVersion: "100"}
				hpa.Labels = map[string]string{"stale": "true"}
				return nil
			})
		client.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).Return(conflict())
		client.EXPECT().Get(gomock.Any(), namespacedName, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ types.NamespacedName, hpa *v2.HorizontalPodAutoscaler, _ ...any) error {
				hpa.ObjectMeta = v1.ObjectMeta{Name: "keda-hpa-test", Namespace: "test-ns", ResourceVersion: "101"}
				hpa.Labels = map[string]string{"concurrent": "write"}
				return nil
			})
		client.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, hpa *v2.HorizontalPodAutoscaler, _ ...any) error {
				// the retried write is based on the fresh resourceVersion and carries the desired labels
				Expect(hpa.ResourceVersion).To(Equal("101"))
				Expect(hpa.Labels).To(Equal(map[string]string{"desired": "label"}))
				return nil
			})

		err := reconciler.updateHPAWithConflictRetry(context.Background(), logger, namespacedName,
			func(hpa *v2.HorizontalPodAutoscaler) {
				hpa.Labels = map[string]string{"desired": "label"}
			})

		Expect(err).ToNot(HaveOccurred())
	})

	It("returns a non-conflict error without retrying", func() {
		client.EXPECT().Get(gomock.Any(), namespacedName, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ types.NamespacedName, hpa *v2.HorizontalPodAutoscaler, _ ...any) error {
				hpa.ObjectMeta = v1.ObjectMeta{Name: "keda-hpa-test", Namespace: "test-ns", ResourceVersion: "100"}
				return nil
			})
		internalErr := apierrors.NewInternalError(context.DeadlineExceeded)
		client.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).Return(internalErr)

		err := reconciler.updateHPAWithConflictRetry(context.Background(), logger, namespacedName,
			func(hpa *v2.HorizontalPodAutoscaler) {
				hpa.Labels = map[string]string{"desired": "label"}
			})

		Expect(apierrors.IsInternalError(err)).To(BeTrue())
		Expect(apierrors.IsConflict(err)).To(BeFalse())
	})

	It("surfaces a read error without attempting an update", func() {
		notFound := apierrors.NewNotFound(schema.GroupResource{Group: "autoscaling", Resource: "horizontalpodautoscalers"}, "keda-hpa-test")
		client.EXPECT().Get(gomock.Any(), namespacedName, gomock.Any()).Return(notFound)

		called := false
		err := reconciler.updateHPAWithConflictRetry(context.Background(), logger, namespacedName,
			func(hpa *v2.HorizontalPodAutoscaler) {
				called = true
			})

		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		Expect(called).To(BeFalse(), "the apply function must not run when the HPA cannot be read")
	})

	It("re-applies owner references after a conflict so ownership transfer is not lost", func() {
		desiredOwnerRefs := []v1.OwnerReference{{
			APIVersion: "keda.sh/v1alpha1",
			Kind:       "ScaledObject",
			Name:       "owning-so",
			UID:        "abc-123",
			Controller: ptr.To(true),
		}}

		// the existing HPA is owned by a foreign controller; the retried read returns a fresh copy
		// that still carries that foreign owner, and the apply must replace it with the desired refs
		foreignOwnerRefs := []v1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       "foreign-owner",
			UID:        "foreign-uid",
		}}

		client.EXPECT().Get(gomock.Any(), namespacedName, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ types.NamespacedName, hpa *v2.HorizontalPodAutoscaler, _ ...any) error {
				hpa.ObjectMeta = v1.ObjectMeta{
					Name: "keda-hpa-test", Namespace: "test-ns", ResourceVersion: "100",
					OwnerReferences: foreignOwnerRefs,
				}
				return nil
			})
		client.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).Return(conflict())
		client.EXPECT().Get(gomock.Any(), namespacedName, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ types.NamespacedName, hpa *v2.HorizontalPodAutoscaler, _ ...any) error {
				hpa.ObjectMeta = v1.ObjectMeta{
					Name: "keda-hpa-test", Namespace: "test-ns", ResourceVersion: "101",
					OwnerReferences: foreignOwnerRefs,
				}
				return nil
			})
		client.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, hpa *v2.HorizontalPodAutoscaler, _ ...any) error {
				Expect(hpa.ResourceVersion).To(Equal("101"))
				Expect(hpa.OwnerReferences).To(Equal(desiredOwnerRefs),
					"the retried write must carry the ScaledObject owner reference rather than the foreign owner")
				return nil
			})

		err := reconciler.updateHPAWithConflictRetry(context.Background(), logger, namespacedName,
			func(hpa *v2.HorizontalPodAutoscaler) {
				hpa.OwnerReferences = desiredOwnerRefs
			})

		Expect(err).ToNot(HaveOccurred())
	})
})
