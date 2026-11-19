# Scaling decisions, concurrency and status synchronization

This document describes how KEDA computes a scale decision for a `ScaledObject`, how the
controller, the scale loop and the external metrics adapter coordinate when metric data is
missing or several actors try to scale at the same time, and which guarantees protect the
desired replica count and the `ScaledObject`/HPA status from being overwritten.

## The two decision paths

A `ScaledObject` is evaluated along two independent paths that must stay consistent:

1. **Scale loop** (`pkg/scaling/scale_handler.go`, `getScaledObjectState` / `checkScalers`).
   Every `pollingInterval` each trigger's scaler is queried for its metrics and activity. The
   aggregated state drives `ScaleExecutor.RequestScale`, which only performs scale-from-zero and
   scale-to-zero (plus min-replica reconciliation); replica counts above zero are owned by the HPA.
2. **External metrics adapter** (`GetScaledObjectMetrics`). The HPA controller queries KEDA for
   the metric values and computes the actual replica target while the workload is running.

The decision is computed **centrally in the controller/scale-handler layer**, not inside the
scaler resolver. The resolver package (`pkg/scaling/resolver`) only resolves scale targets,
secrets and authentication; it never makes a scale decision. Keeping the decision in one place
means the scale loop and the metrics adapter interpret the same metric observation the same way
(fallback, activity, modifiers), and the HPA-facing and polling-facing paths cannot drift.

## Missing metrics

A trigger source can answer successfully without returning any metric value, for example an empty
queue. That response is treated as *missing data*, not as an error:

- In the scale loop the trigger is **inactive** and `isScaledObjectError` stays `false`, so the
  scalers cache is not invalidated, no fallback is engaged and no spurious scale action or
  `Ready=False` condition is produced.
- In the metrics adapter, when every observed trigger returns an empty (error-free) list, KEDA
  returns an explicit `no matching metrics found` error instead of an empty
  `ExternalMetricValueList`. The HPA controller then records `FailedGetExternalMetric` rather than
  scaling on a fabricated zero-valued metric.

An actual scaler error takes a different path: it sets the error flag, invalidates the scalers
cache so scalers are rebuilt on the next tick, and is handed to the fallback logic
(`pkg/fallback`) when a fallback is configured. Within the failure threshold the error is still
reported to the HPA directly; only after the threshold is exceeded do fallback metrics replace it.

## Concurrency in the scale loop

Each `ScaledObject` owns a per-object `sync.Mutex` created in `HandleScalableObject` and shared by
the polling loop (`startScaleLoop`) and every external-push scaler goroutine
(`startPushScalers`). Both enter the scaling decision through `checkScalers` / `RequestScale`,
which take that mutex, so for a given `ScaledObject`:

- a push-scaler activation can never interleave with a polling tick, and
- `RequestScale` (including the read-modify-write of the scale subresource) is never entered
  concurrently with itself.

Status updates are additionally safe across goroutines: `handleResult` re-reads the object,
applies only the delta against the snapshot the executor actually observed, and patches the
status subresource with conflict retry. A push scaler therefore never overwrites a concurrent
polling-loop update of another trigger's activity.

## Optimistic concurrency on the scale subresource

`ScaleExecutor.updateScaleOnScaleTarget` performs a read-modify-write on the workload's `/scale`
subresource. The HPA controller, a user or another controller can update the same scale object
between the read and the write. The update is therefore wrapped in an explicit conflict-retry
loop:

- a `Conflict` (stale `resourceVersion`) causes a fresh read, after which KEDA re-asserts its own
  desired replica count against the new version;
- if the fresh read already carries the desired replicas, no second write is issued;
- any other error (timeout, API failure, ...) is returned immediately and is **not** retried,
  preserving the previous single-attempt behavior for those paths;
- when the retry budget is exhausted the last conflict is surfaced rather than swallowed.

This prevents a stale KEDA write from clobbering a concurrent actor's desired replica count.

## HPA updates

`ScaledObjectReconciler.updateHPAIfNeeded` reconciles the HPA spec, labels and annotations from
the `ScaledObject`. Each section is persisted through `updateHPAWithConflictRetry`, which
re-reads the HPA on a `Conflict`, re-applies the ScaledObject-owned fields to the fresh object and
retries. The ScaledObject's controller owner reference is part of that desired state and is
re-applied on every attempt, so an HPA ownership transfer still completes after a conflict. Only
ScaledObject-owned fields are set in the apply step, so retrying against a newer
`resourceVersion` never resurrects a stale value, and concurrent changes to unrelated fields are
preserved. Non-conflict errors fail fast and requeue the reconcile.

The controller-runtime workqueue already serializes reconcile attempts for a single
`ScaledObject`; this guard covers the remaining window in which an actor outside KEDA modifies the
HPA between the reconciler's read and its write.

## Status synchronization

- ScaledObject status (`conditions`, `lastActiveTime`, trigger activity, paused replica count,
  health) is written via strategic merge patches against a freshly fetched object, with conflict
  retry, so concurrent writers merge rather than overwrite each other.
- HPA health is mirrored into the dedicated `HPAActive` condition; when the HPA cannot be observed
  (no HPA yet, transient read error, no `ScalingActive` condition) the condition is left
  untouched rather than reset, so the last observed state persists.
- The HPA's own status is never written by KEDA; KEDA only manages the HPA spec and the
  `ScaledObject` status, keeping the two interfaces compatible and independently reconcilable.
