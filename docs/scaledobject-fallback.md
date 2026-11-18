# ScaledObject fallback and scale-to-zero behavior

KEDA tracks trigger health independently. A transient error from one trigger must not put the
whole ScaledObject into fallback, and it must never override another trigger that is still active.

## Failure threshold

Each trigger has its own consecutive failure counter in `status.health`. The counter is keyed by
the trigger name (or by a stable generated key for unnamed triggers) and is reset to zero as soon
as that trigger returns a healthy metric.

Fallback becomes active only when the counter is **greater than** `spec.fallback.failureThreshold`.
If the threshold is `3`, the first three consecutive failures return an error to the metric source;
the fourth failure activates fallback. While the counter is at or below the threshold, KEDA keeps
the current replica count instead of scaling the target to zero.

`status.health` also exposes:

- `triggerName`: the trigger that owns the counter.
- `lastError`: the most recent error returned by that trigger.
- `lastFailureTime`: when the counter was last incremented.

## Multiple triggers

Active state is evaluated for every trigger and combined with OR semantics:

- If any trigger is active, the ScaleTarget is never treated as inactive, even if another trigger
  is failing or falling back.
- If a trigger is failing but its failure threshold has not been exceeded, the ScaleTarget keeps
  its current replica count while the other triggers remain authoritative.
- Fallback metrics are produced only for the trigger that exceeded its threshold. Healthy trigger
  metrics continue to be evaluated normally, so the HPA uses the highest requested replica count.

When a trigger recovers, its counter and `lastError` are cleared. If no other trigger is above its
threshold, the `Fallback` condition returns to `False` and normal scaling resumes.

## Status, events and metrics

While fallback is active, `status.fallback` records:

- `active`: whether fallback is currently in use.
- `triggers`: the trigger names that exceeded their threshold.
- `currentReplicas`: the ScaleTarget replica count observed when fallback was evaluated.
- `reason`: the trigger error that caused fallback.

The `Fallback` condition uses the same trigger names and current replica count in its message.

Kubernetes events are emitted only when fallback changes state:

- `ScaledObjectFallbackActivated`: includes the failing trigger(s), current replicas and the error
  reason when available.
- `ScaledObjectFallbackDeactivated`: emitted once after all fallback counters have recovered.

Prometheus and OpenTelemetry expose the fallback state per trigger:

- `keda_scaled_object_fallback_active{namespace,scaledObject,trigger}`: `1` while the trigger is
  falling back, otherwise `0`.
- `keda_scaled_object_fallback_current_replicas{namespace,scaledObject,trigger}`: the observed
  ScaleTarget replica count.

## Error handling

An empty metric result, authentication failure or unreadable trigger state is treated as a trigger
failure and recorded with the trigger name and error reason. KEDA does not silently convert an
unknown error into a scale-to-zero decision: while a trigger error is present, the current replica
count is held until the trigger recovers or fallback is explicitly configured and activated.

The canonical user documentation is maintained in
[`kedacore/keda-docs`](https://github.com/kedacore/keda-docs); this file documents the behavior
implemented in this repository.
