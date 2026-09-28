package goservicedrain

import "errors"

// Sentinel errors returned by the coordinator. Callers should use errors.Is.
var (
	// ErrInstanceExists is returned when registering an instance id that is
	// already registered.
	ErrInstanceExists = errors.New("goservicedrain: instance already registered")
	// ErrInstanceUnknown is returned when an operation references an instance
	// that has never been registered.
	ErrInstanceUnknown = errors.New("goservicedrain: instance not registered")
	// ErrInstanceDraining is returned by AcquireSession while an instance is in
	// the draining state: new sessions must not be handed out.
	ErrInstanceDraining = errors.New("goservicedrain: instance is draining")
	// ErrInstanceDrained is returned by operations (e.g. AcquireSession) that
	// only make sense while an instance is still accepting or gracefully
	// draining traffic, but the instance has already completed draining.
	ErrInstanceDrained = errors.New("goservicedrain: instance already drained")
	// ErrLeaseUnknown is returned when a session/lease id does not exist for
	// the instance, has already been closed or expired, or belongs to an
	// older drain version.
	ErrLeaseUnknown = errors.New("goservicedrain: unknown or stale session lease")
	// ErrVersionMismatch is returned when an operation carries a version that
	// does not match the instance's current version. Stale close/heartbeat
	// requests and stale drain cancellations are rejected this way.
	ErrVersionMismatch = errors.New("goservicedrain: version does not match current instance version")
	// ErrNoDrainInProgress is returned by CancelDrain when the instance is not
	// currently draining (active, or drain already finished).
	ErrNoDrainInProgress = errors.New("goservicedrain: no drain in progress")
	// ErrInvalidArgument is returned for zero or negative durations and other
	// malformed inputs.
	ErrInvalidArgument = errors.New("goservicedrain: invalid argument")
)
