package goservicedrain

import "time"

// State is the lifecycle state of a registered service instance.
type State string

const (
	// StateActive means the instance is registered and may acquire new
	// sessions.
	StateActive State = "active"
	// StateDraining means a drain has been initiated: new sessions are
	// rejected, but sessions established before (or adopted by) the current
	// drain version may finish before the drain deadline.
	StateDraining State = "draining"
	// StateDrained means the drain has completed, either because all sessions
	// ended before the deadline or because remaining sessions were forcibly
	// terminated at the deadline. A drained instance never accepts traffic
	// again.
	StateDrained State = "drained"
)

// Completion reasons recorded on a drain completion notification.
const (
	// ReasonDrained means all active sessions ended naturally before the
	// force deadline.
	ReasonDrained = "drained"
	// ReasonForced means the force deadline was reached and remaining
	// sessions were terminated.
	ReasonForced = "forced"
)

// Lease is a time-bounded claim on one new session. The caller must present
// SessionID and Version on every later heartbeat and close; messages that do
// not match the instance's current version are ignored.
type Lease struct {
	// SessionID uniquely identifies the acquired session.
	SessionID string
	// InstanceID is the instance that owns the session.
	InstanceID string
	// Version is the instance version under which the session was acquired.
	// It equals the instance's current version while the instance is active.
	Version int64
	// Deadline is the lease expiration time.
	Deadline time.Time
}

// SessionInfo describes one currently active session.
type SessionInfo struct {
	SessionID string
	// Version is the instance version the session currently belongs to. It is
	// pinned when the lease is granted and updated to the new version when a
	// drain adopts in-flight sessions.
	Version  int64
	Deadline time.Time
}

// DrainInfo describes an in-progress drain. It is nil while no drain is
// active.
type DrainInfo struct {
	// Version is the monotonically increasing version created by BeginDrain.
	Version int64
	// Deadline is the force-termination deadline for this drain.
	Deadline time.Time
}

// TerminatedSession records a session that was still open when the force
// deadline arrived and had to be terminated.
type TerminatedSession struct {
	SessionID string
	Version   int64
}

// Completion is the single drain-completion notification produced per drain.
// Exactly one Completion is recorded even when the last natural close and the
// force-timeout processing race with each other.
type Completion struct {
	InstanceID string
	// DrainID equals the drain version, uniquely identifying each drain.
	DrainID int64
	// Reason is ReasonDrained or ReasonForced.
	Reason string
	// Terminated lists sessions forcibly terminated at the deadline. Empty
	// for natural drains.
	Terminated []TerminatedSession
	At         time.Time
}

// Status is a point-in-time snapshot of an instance. Slices are copies and
// safe to retain after the call returns.
type Status struct {
	InstanceID     string
	State          State
	Version        int64
	ActiveSessions int
	Drain          *DrainInfo
	Sessions       []SessionInfo
}
