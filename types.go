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

// Drain task states recorded in DrainTaskInfo.State. A drain task is created
// by BeginDrain and is never deleted; it only transitions forward.
const (
	// TaskDraining means the drain is in progress: the instance rejects new
	// sessions while pre-drain sessions finish or transfer.
	TaskDraining = "draining"
	// TaskDrained means every session ended (closed, expired or transferred)
	// before the force deadline.
	TaskDrained = "drained"
	// TaskTimedOut means the force deadline was reached with sessions still
	// open; each leftover connection is recorded individually in
	// DrainTaskInfo.Terminated.
	TaskTimedOut = "timed_out"
	// TaskCancelled means the drain was cancelled while still in progress.
	// The task record (including its connection snapshot) is retained.
	TaskCancelled = "cancelled"
)

// Per-connection termination reasons recorded on TerminatedSession.
const (
	// ReasonForceTimeout means the connection was still open when the
	// drain's maximum wait window elapsed and had to be terminated.
	ReasonForceTimeout = "force_timeout"
)

// Reasons reported in Status.BlockedReason while an instance cannot accept
// new sessions. Empty when the instance is accepting.
const (
	// BlockedDraining means a drain is in progress.
	BlockedDraining = "draining"
	// BlockedDrained means the instance has finished draining and never
	// accepts traffic again.
	BlockedDrained = "drained"
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

// SessionSnapshot captures one in-flight connection at the moment a drain
// task is created.
type SessionSnapshot struct {
	SessionID string
	Version   int64
	Deadline  time.Time
}

// TransferRecord describes a session that was explicitly moved off the
// instance instead of being closed in place.
type TransferRecord struct {
	SessionID string
	Version   int64
	// Target is the caller-supplied destination the session was moved to.
	Target string
	At     time.Time
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
	// Reason explains why this individual connection was terminated
	// (currently always ReasonForceTimeout).
	Reason string
}

// DrainTaskInfo is the recorded history of one drain task. Tasks are kept
// forever, including cancelled ones, so an instance can never fall back to
// plain "available" while losing the fact that a drain happened.
type DrainTaskInfo struct {
	// DrainID is the client-supplied idempotency key of the drain.
	DrainID string
	// Version is the instance version the drain created.
	Version int64
	// State is one of TaskDraining/TaskDrained/TaskTimedOut/TaskCancelled.
	State string
	// StartedAt is when the drain task was created.
	StartedAt time.Time
	// MaxWait is the maximum wait window granted to in-flight sessions.
	MaxWait time.Duration
	// Deadline is StartedAt + MaxWait: the force-termination time.
	Deadline time.Time
	// FinishedAt is when the task reached a terminal state; zero while
	// still draining.
	FinishedAt time.Time
	// Snapshot lists the connections that were in flight when the drain
	// began.
	Snapshot []SessionSnapshot
	// Terminated lists, per connection, what was still unfinished when the
	// wait window elapsed, each with its own reason.
	Terminated []TerminatedSession
	// Transfers lists sessions explicitly moved off the instance during
	// this drain.
	Transfers []TransferRecord
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
	// Accepting is true exactly when the instance may hand out new sessions
	// (state active). It is always consistent with State and Drains.
	Accepting bool
	// BlockedReason explains why new sessions are rejected
	// (BlockedDraining/BlockedDrained); empty while Accepting is true.
	BlockedReason string
	Drain         *DrainInfo
	Sessions      []SessionInfo
	// Drains is the full drain task history of the instance, oldest first,
	// including cancelled and finished tasks.
	Drains []DrainTaskInfo
	// Transfers lists every session moved off this instance, oldest first.
	Transfers []TransferRecord
}
