// Package goservicedrain coordinates graceful draining ("摘流") of service
// instances.
//
// An instance progresses through three states:
//
//	active -> draining -> drained
//
// Every BeginDrain produces a new, monotonically increasing version and stops
// the instance from acquiring new sessions; sessions already established are
// adopted by the new version and may finish before the force deadline.
// Each drain is recorded as a task (start time, maximum wait window and the
// connection snapshot at creation) that is kept forever, even after a
// cancel. Accepted connections can only end (close/expiry/forced
// termination, each recorded) or move into an explicit transferred state.
// BeginDrain is idempotent on its client-supplied drain id.
// Heartbeats, closes and lease expirations only affect a session when the
// carried version matches the instance's current version and the lease still
// exists, so duplicate and out-of-order messages from an old drain version
// can never move the new generation's counters or resurrect a drained
// instance. All state-changing facts are written to an append-only Journal
// before being applied to memory, which makes coordinator state fully
// recoverable by replay.
package goservicedrain

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// session is an in-memory active (not yet closed/expired/terminated) session.
type session struct {
	id       string
	version  int64
	deadline time.Time
}

// instance is the in-memory state of one registered service instance.
type instance struct {
	id            string
	state         State
	version       int64
	sessions      map[string]*session
	drainDeadline time.Time // zero unless state == draining
	tasks         []*drainTask
	transfers     []TransferRecord
}

// drainTask is the recorded history of one drain. Tasks are never removed:
// even a cancelled drain keeps its snapshot so the instance can never revert
// to plain "available" while losing the drain record.
type drainTask struct {
	id         string
	instanceID string
	version    int64
	startedAt  time.Time
	maxWait    time.Duration
	deadline   time.Time
	state      string
	snapshot   []SessionSnapshot
	terminated []TerminatedSession
	transfers  []TransferRecord
	finishedAt time.Time
}

// openTask returns the instance's in-progress drain task, or nil.
func (inst *instance) openTask() *drainTask {
	for i := len(inst.tasks) - 1; i >= 0; i-- {
		if inst.tasks[i].state == TaskDraining {
			return inst.tasks[i]
		}
	}
	return nil
}

// Coordinator serializes all drain coordination for a set of instances. Its
// zero value is not usable; create one with NewCoordinator, NewEphemeral or
// RecoverCoordinator.
type Coordinator struct {
	mu          sync.Mutex
	clock       Clock
	journal     Journal
	seq         int64
	counter     uint64
	insts       map[string]*instance
	tasksByID   map[string]*drainTask
	completions []Completion
}

// NewCoordinator returns a Coordinator whose changes are persisted to journal
// and seeded with whatever the journal already contains.
func NewCoordinator(journal Journal) (*Coordinator, error) {
	c := &Coordinator{
		clock:     SystemClock(),
		journal:   journal,
		insts:     make(map[string]*instance),
		tasksByID: make(map[string]*drainTask),
	}
	if err := c.replay(); err != nil {
		return nil, err
	}
	return c, nil
}

// NewEphemeral returns an in-memory Coordinator (non-persistent) driven by
// clock. It is mainly useful in tests.
func NewEphemeral(clock Clock) *Coordinator {
	if clock == nil {
		clock = SystemClock()
	}
	c := &Coordinator{
		clock:     clock,
		journal:   newMemJournal(),
		insts:     make(map[string]*instance),
		tasksByID: make(map[string]*drainTask),
	}
	return c
}

// RecoverCoordinator opens the file-backed journal at path and rebuilds the
// coordinator from it. The returned coordinator keeps appending to the same
// journal.
func RecoverCoordinator(path string) (*Coordinator, *FileJournal, error) {
	j, err := OpenFileJournal(path)
	if err != nil {
		return nil, nil, err
	}
	c, err := NewCoordinator(j)
	if err != nil {
		_ = j.Close()
		return nil, nil, err
	}
	return c, j, nil
}

// journaledChange is one persisted fact together with its in-memory mutation.
// The apply step runs only after the whole batch has been durably appended.
type journaledChange struct {
	kind    string
	payload any
	apply   func()
}

// commit durably appends a batch of changes and applies them in order. The
// caller must hold c.mu.
func (c *Coordinator) commit(changes ...journaledChange) error {
	now := c.clock.Now()
	events := make([]Event, len(changes))
	for i, ch := range changes {
		c.seq++
		events[i] = Event{Seq: c.seq, Kind: ch.kind, At: now, Payload: ch.payload}
	}
	if err := c.journal.Append(events); err != nil {
		return err
	}
	for _, ch := range changes {
		if ch.apply != nil {
			ch.apply()
		}
	}
	return nil
}

func (c *Coordinator) instance(id string) (*instance, error) {
	inst, ok := c.insts[id]
	if !ok {
		return nil, ErrInstanceUnknown
	}
	return inst, nil
}

func (c *Coordinator) newSessionID() string {
	c.counter++
	return fmt.Sprintf("s-%d", c.counter)
}

// Register adds a new instance in the active state at version 0.
func (c *Coordinator) Register(instanceID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.insts[instanceID]; ok {
		return ErrInstanceExists
	}
	return c.commit(journaledChange{
		kind:    kindRegistered,
		payload: &evRegistered{InstanceID: instanceID, Version: 0},
		apply: func() {
			c.insts[instanceID] = &instance{
				id:       instanceID,
				state:    StateActive,
				version:  0,
				sessions: make(map[string]*session),
			}
		},
	})
}

// AcquireSession hands a new time-bounded session lease to an active
// instance. Draining instances are rejected immediately and drained instances
// are rejected permanently.
func (c *Coordinator) AcquireSession(instanceID string, ttl time.Duration) (Lease, error) {
	if ttl <= 0 {
		return Lease{}, ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	inst, err := c.instance(instanceID)
	if err != nil {
		return Lease{}, err
	}
	switch inst.state {
	case StateDraining:
		return Lease{}, ErrInstanceDraining
	case StateDrained:
		return Lease{}, ErrInstanceDrained
	}

	sessionID := c.newSessionID()
	deadline := c.clock.Now().Add(ttl)
	lease := Lease{
		SessionID:  sessionID,
		InstanceID: instanceID,
		Version:    inst.version,
		Deadline:   deadline,
	}
	err = c.commit(journaledChange{
		kind: kindLeaseAcquired,
		payload: &evLeaseAcquired{
			InstanceID: instanceID,
			SessionID:  sessionID,
			Version:    inst.version,
			Deadline:   deadline,
		},
		apply: func() {
			inst.sessions[sessionID] = &session{
				id:       sessionID,
				version:  inst.version,
				deadline: deadline,
			}
		},
	})
	if err != nil {
		return Lease{}, err
	}
	return lease, nil
}

// lookupSession returns the active session only if its lease exists and the
// carried version matches the instance's current version. A missing lease
// yields ErrLeaseUnknown (covers duplicates and already-ended sessions); a
// live lease with a stale version yields ErrVersionMismatch.
func lookupSession(inst *instance, sessionID string, version int64) (*session, error) {
	sess, ok := inst.sessions[sessionID]
	if !ok {
		return nil, ErrLeaseUnknown
	}
	if sess.version != version {
		return nil, ErrVersionMismatch
	}
	return sess, nil
}

// Heartbeat extends a session lease. It is a no-op for the active counter when
// the session is unknown or the version is stale.
func (c *Coordinator) Heartbeat(instanceID, sessionID string, version int64, ttl time.Duration) (time.Time, error) {
	if ttl <= 0 {
		return time.Time{}, ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	inst, err := c.instance(instanceID)
	if err != nil {
		return time.Time{}, err
	}
	sess, err := lookupSession(inst, sessionID, version)
	if err != nil {
		return time.Time{}, err
	}

	deadline := c.clock.Now().Add(ttl)
	err = c.commit(journaledChange{
		kind: kindHeartbeat,
		payload: &evHeartbeat{
			InstanceID: instanceID,
			SessionID:  sessionID,
			Version:    version,
			Deadline:   deadline,
		},
		apply: func() {
			sess.deadline = deadline
		},
	})
	if err != nil {
		return time.Time{}, err
	}
	return deadline, nil
}

// CloseSession ends one session. Only a close whose version matches the
// instance's current version decrements the active count; duplicate,
// reordered or stale closes are rejected without touching anything. When the
// last session of a draining instance closes, the drain completes exactly
// once in the same committed batch.
func (c *Coordinator) CloseSession(instanceID, sessionID string, version int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	inst, err := c.instance(instanceID)
	if err != nil {
		return err
	}
	if _, err := lookupSession(inst, sessionID, version); err != nil {
		return err
	}

	changes := []journaledChange{{
		kind: kindSessionClosed,
		payload: &evSessionEnded{
			InstanceID: instanceID,
			SessionID:  sessionID,
			Version:    version,
		},
		apply: func() {
			delete(inst.sessions, sessionID)
		},
	}}

	// A draining instance whose last session is closing completes naturally.
	// The state guard makes a racing second close/timeout unable to complete
	// the drain twice.
	if inst.state == StateDraining && len(inst.sessions) == 1 {
		changes = append(changes, c.naturalCompletionChange(inst))
	}
	return c.commit(changes...)
}

// TransferSession moves one accepted session off the instance into an
// explicit transferred state instead of closing it in place. The transfer is
// recorded on the instance and on the in-progress drain task (if any), and
// the session lease ends immediately: later heartbeats or closes for it are
// rejected as unknown. Version rules match CloseSession. When the last
// session of a draining instance is transferred, the drain completes
// naturally in the same committed batch.
func (c *Coordinator) TransferSession(instanceID, sessionID string, version int64, target string) error {
	if target == "" {
		return ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	inst, err := c.instance(instanceID)
	if err != nil {
		return err
	}
	if _, err := lookupSession(inst, sessionID, version); err != nil {
		return err
	}

	rec := TransferRecord{
		SessionID: sessionID,
		Version:   version,
		Target:    target,
		At:        c.clock.Now(),
	}
	changes := []journaledChange{{
		kind: kindSessionMoved,
		payload: &evSessionTransferred{
			InstanceID: instanceID,
			SessionID:  sessionID,
			Version:    version,
			Target:     target,
		},
		apply: func() {
			delete(inst.sessions, sessionID)
			inst.transfers = append(inst.transfers, rec)
			if task := inst.openTask(); task != nil {
				task.transfers = append(task.transfers, rec)
			}
		},
	}}
	if inst.state == StateDraining && len(inst.sessions) == 1 {
		changes = append(changes, c.naturalCompletionChange(inst))
	}
	return c.commit(changes...)
}

// naturalCompletionChange builds the drain_completed change (reason drained)
// and its state transition. The caller must hold c.mu and have already
// established that the session map will be empty once the batch applies.
func (c *Coordinator) naturalCompletionChange(inst *instance) journaledChange {
	at := c.clock.Now()
	drainID := inst.version
	payload := &evDrainCompleted{
		InstanceID: inst.id,
		DrainID:    drainID,
		Reason:     ReasonDrained,
		At:         at,
	}
	return journaledChange{
		kind:    kindDrainCompleted,
		payload: payload,
		apply: func() {
			c.applyCompletion(inst, Completion{
				InstanceID: inst.id,
				DrainID:    drainID,
				Reason:     ReasonDrained,
				At:         at,
			})
			if task := inst.openTask(); task != nil {
				task.state = TaskDrained
				task.finishedAt = at
			}
		},
	}
}

// applyCompletion transitions an instance to drained and records its single
// completion notification. Must run from a committed change's apply step.
func (c *Coordinator) applyCompletion(inst *instance, comp Completion) {
	inst.state = StateDrained
	inst.drainDeadline = time.Time{}
	c.completions = append(c.completions, comp)
}

// BeginDrain initiates draining: it allocates the next version, stops new
// session acquisition immediately and adopts every in-flight session under
// the new version so that carries of the old version become stale. Sessions
// may finish naturally until now+forceAfter; at that point ProcessTimeouts
// terminates the rest. With no active sessions the drain completes at once.
//
// drainID is a client-supplied idempotency key: resubmitting the same
// drainID with the same instance and wait window returns the original
// version and deadline without creating a new drain, even if the first
// drain has already finished. Reusing the drainID with a different instance
// or wait window fails with ErrDrainConflict.
//
// Every call records a drain task (start time, wait window, connection
// snapshot) that is kept for the life of the instance, including after a
// cancel.
func (c *Coordinator) BeginDrain(instanceID, drainID string, forceAfter time.Duration) (version int64, deadline time.Time, err error) {
	if drainID == "" || forceAfter <= 0 {
		return 0, time.Time{}, ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if task, ok := c.tasksByID[drainID]; ok {
		if task.instanceID != instanceID || task.maxWait != forceAfter {
			return 0, time.Time{}, ErrDrainConflict
		}
		return task.version, task.deadline, nil
	}

	inst, err := c.instance(instanceID)
	if err != nil {
		return 0, time.Time{}, err
	}
	switch inst.state {
	case StateDraining:
		return inst.version, inst.drainDeadline, ErrInstanceDraining
	case StateDrained:
		return 0, time.Time{}, ErrInstanceDrained
	}

	newVersion := inst.version + 1
	startedAt := c.clock.Now()
	deadline = startedAt.Add(forceAfter)
	adopted := make(map[string]int64, len(inst.sessions))
	ids := make([]string, 0, len(inst.sessions))
	for id := range inst.sessions {
		adopted[id] = newVersion
		ids = append(ids, id)
	}
	sort.Strings(ids) // deterministic journal content
	snapshot := make([]SessionSnapshot, 0, len(ids))
	for _, id := range ids {
		snapshot = append(snapshot, SessionSnapshot{
			SessionID: id,
			Version:   newVersion,
			Deadline:  inst.sessions[id].deadline,
		})
	}
	task := &drainTask{
		id:         drainID,
		instanceID: instanceID,
		version:    newVersion,
		startedAt:  startedAt,
		maxWait:    forceAfter,
		deadline:   deadline,
		state:      TaskDraining,
		snapshot:   snapshot,
	}

	changes := []journaledChange{{
		kind: kindDrainBegan,
		payload: &evDrainBegan{
			InstanceID:      instanceID,
			DrainID:         drainID,
			Version:         newVersion,
			StartedAt:       startedAt,
			MaxWait:         forceAfter,
			Deadline:        deadline,
			SessionVersions: adopted,
			Snapshot:        snapshot,
		},
		apply: func() {
			inst.version = newVersion
			inst.state = StateDraining
			inst.drainDeadline = deadline
			for _, sess := range inst.sessions {
				sess.version = newVersion
			}
			inst.tasks = append(inst.tasks, task)
			c.tasksByID[drainID] = task
		},
	}}
	if len(ids) == 0 {
		changes = append(changes, c.naturalCompletionChange(inst))
	}
	if err := c.commit(changes...); err != nil {
		return 0, time.Time{}, err
	}
	return newVersion, deadline, nil
}

// CancelDrain aborts an in-progress drain and resumes accepting new
// sessions. The caller must present the current drain version; a late cancel
// carrying an old version is rejected (ErrVersionMismatch), as are cancels
// against an instance that is active (ErrNoDrainInProgress) or already
// drained (ErrInstanceDrained) — a completed or timed-out drain can never be
// resurrected by a stale cancel. The cancelled drain task, including its
// connection snapshot, stays in the instance's drain history.
func (c *Coordinator) CancelDrain(instanceID string, version int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	inst, err := c.instance(instanceID)
	if err != nil {
		return err
	}
	switch inst.state {
	case StateActive:
		return ErrNoDrainInProgress
	case StateDrained:
		return ErrInstanceDrained
	}
	if inst.version != version {
		return ErrVersionMismatch
	}
	cancelledAt := c.clock.Now()
	return c.commit(journaledChange{
		kind:    kindDrainCanceled,
		payload: &evDrainCanceled{InstanceID: instanceID, Version: version},
		apply: func() {
			inst.state = StateActive
			inst.drainDeadline = time.Time{}
			if task := inst.openTask(); task != nil {
				task.state = TaskCancelled
				task.finishedAt = cancelledAt
			}
		},
	})
}

// ProcessTimeouts advances all deadline-driven work using the coordinator's
// clock: expired session leases are dropped (possibly completing a drain),
// and draining instances past their force deadline terminate every remaining
// session and complete with reason "forced". Call it from a timer, or use
// Advance with a FakeClock in tests.
func (c *Coordinator) ProcessTimeouts() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.advanceLocked(c.clock.Now())
}

// Advance drives timeout processing as if the current time were now. It does
// not mutate the configured Clock; it is primarily intended for tests using a
// fixed clock.
func (c *Coordinator) Advance(now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.advanceLocked(now)
}

func (c *Coordinator) advanceLocked(now time.Time) error {
	var changes []journaledChange

	// Deterministic iteration order keeps the journal stable.
	ids := make([]string, 0, len(c.insts))
	for id := range c.insts {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		inst := c.insts[id]

		// 1. Drop leases whose own deadline has passed. Sessions are iterated
		//    in id order; the mutations run later from apply closures.
		var expired []string
		for sid := range inst.sessions {
			if !inst.sessions[sid].deadline.After(now) {
				expired = append(expired, sid)
			}
		}
		sort.Strings(expired)
		for _, sid := range expired {
			sess := inst.sessions[sid]
			sid, v := sid, sess.version
			changes = append(changes, journaledChange{
				kind: kindSessionExpired,
				payload: &evSessionEnded{
					InstanceID: inst.id,
					SessionID:  sid,
					Version:    v,
				},
				apply: func() {
					delete(inst.sessions, sid)
				},
			})
		}

		if inst.state != StateDraining {
			continue
		}

		// 2. Every session is gone after the lease-expiry removals above ->
		//    the drain completes naturally.
		if len(inst.sessions)-len(expired) == 0 {
			changes = append(changes, c.naturalCompletionChange(inst))
			continue
		}

		// 3. Force deadline reached -> terminate everything still open.
		if !inst.drainDeadline.After(now) {
			var killed []string
			var records []TerminatedSession
			for sid := range inst.sessions {
				// Sessions expiring at/before `now` were already scheduled for
				// removal in step 1; the rest are force-terminated.
				isExpired := false
				for _, e := range expired {
					if e == sid {
						isExpired = true
						break
					}
				}
				if isExpired {
					continue
				}
				killed = append(killed, sid)
			}
			sort.Strings(killed)
			for _, sid := range killed {
				records = append(records, TerminatedSession{
					SessionID: sid,
					Version:   inst.sessions[sid].version,
					Reason:    ReasonForceTimeout,
				})
			}

			at := now
			drainID := inst.version
			killedCopy := append([]TerminatedSession(nil), records...)
			changes = append(changes,
				journaledChange{
					kind: kindSessionsKilled,
					payload: &evSessionsKilled{
						InstanceID: inst.id,
						Version:    drainID,
						SessionIDs: killed,
					},
					apply: func() {
						for _, sid := range killed {
							delete(inst.sessions, sid)
						}
					},
				},
				journaledChange{
					kind: kindDrainCompleted,
					payload: &evDrainCompleted{
						InstanceID: inst.id,
						DrainID:    drainID,
						Reason:     ReasonForced,
						Terminated: killedCopy,
						At:         at,
					},
					apply: func() {
						c.applyCompletion(inst, Completion{
							InstanceID: inst.id,
							DrainID:    drainID,
							Reason:     ReasonForced,
							Terminated: killedCopy,
							At:         at,
						})
						if task := inst.openTask(); task != nil {
							task.state = TaskTimedOut
							task.terminated = append([]TerminatedSession(nil), killedCopy...)
							task.finishedAt = at
						}
					},
				},
			)
		}
	}

	if len(changes) == 0 {
		return nil
	}
	return c.commit(changes...)
}

// Status returns a point-in-time snapshot of an instance.
func (c *Coordinator) Status(instanceID string) (Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	inst, err := c.instance(instanceID)
	if err != nil {
		return Status{}, err
	}

	sessions := make([]SessionInfo, 0, len(inst.sessions))
	for _, sess := range inst.sessions {
		sessions = append(sessions, SessionInfo{
			SessionID: sess.id,
			Version:   sess.version,
			Deadline:  sess.deadline,
		})
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].SessionID < sessions[j].SessionID
	})

	st := Status{
		InstanceID:     inst.id,
		State:          inst.state,
		Version:        inst.version,
		ActiveSessions: len(inst.sessions),
		Accepting:      inst.state == StateActive,
		Sessions:       sessions,
	}
	switch inst.state {
	case StateDraining:
		st.Drain = &DrainInfo{Version: inst.version, Deadline: inst.drainDeadline}
		st.BlockedReason = BlockedDraining
	case StateDrained:
		st.BlockedReason = BlockedDrained
	}
	st.Drains = make([]DrainTaskInfo, 0, len(inst.tasks))
	for _, task := range inst.tasks {
		st.Drains = append(st.Drains, DrainTaskInfo{
			DrainID:    task.id,
			Version:    task.version,
			State:      task.state,
			StartedAt:  task.startedAt,
			MaxWait:    task.maxWait,
			Deadline:   task.deadline,
			FinishedAt: task.finishedAt,
			Snapshot:   append([]SessionSnapshot(nil), task.snapshot...),
			Terminated: append([]TerminatedSession(nil), task.terminated...),
			Transfers:  append([]TransferRecord(nil), task.transfers...),
		})
	}
	st.Transfers = append([]TransferRecord(nil), inst.transfers...)
	return st, nil
}

// Completions returns a copy of every drain completion recorded so far (at
// most one per drain), oldest first.
func (c *Coordinator) Completions() []Completion {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Completion, len(c.completions))
	for i, comp := range c.completions {
		if comp.Terminated != nil {
			comp.Terminated = append([]TerminatedSession(nil), comp.Terminated...)
		}
		out[i] = comp
	}
	return out
}

// replay rebuilds all in-memory state from the journal.
func (c *Coordinator) replay() error {
	return c.journal.Replay(func(e Event) error {
		c.seq = e.Seq
		switch p := e.Payload.(type) {
		case *evRegistered:
			if _, exists := c.insts[p.InstanceID]; exists {
				return &CorruptJournalError{Seq: e.Seq, Reason: "duplicate registration"}
			}
			c.insts[p.InstanceID] = &instance{
				id:       p.InstanceID,
				state:    StateActive,
				version:  p.Version,
				sessions: make(map[string]*session),
			}

		case *evLeaseAcquired:
			inst, err := c.instance(p.InstanceID)
			if err != nil {
				return &CorruptJournalError{Seq: e.Seq, Reason: err.Error()}
			}
			inst.sessions[p.SessionID] = &session{
				id:       p.SessionID,
				version:  p.Version,
				deadline: p.Deadline,
			}
			c.counter++

		case *evHeartbeat:
			inst, err := c.instance(p.InstanceID)
			if err != nil {
				return &CorruptJournalError{Seq: e.Seq, Reason: err.Error()}
			}
			sess, ok := inst.sessions[p.SessionID]
			if !ok {
				return &CorruptJournalError{Seq: e.Seq, Reason: "heartbeat for unknown session"}
			}
			sess.deadline = p.Deadline

		case *evSessionEnded:
			inst, err := c.instance(p.InstanceID)
			if err != nil {
				return &CorruptJournalError{Seq: e.Seq, Reason: err.Error()}
			}
			delete(inst.sessions, p.SessionID)

		case *evDrainBegan:
			inst, err := c.instance(p.InstanceID)
			if err != nil {
				return &CorruptJournalError{Seq: e.Seq, Reason: err.Error()}
			}
			inst.version = p.Version
			inst.state = StateDraining
			inst.drainDeadline = p.Deadline
			for sid, sess := range inst.sessions {
				if v, ok := p.SessionVersions[sid]; ok {
					sess.version = v
				}
			}
			task := &drainTask{
				id:         p.DrainID,
				instanceID: p.InstanceID,
				version:    p.Version,
				startedAt:  p.StartedAt,
				maxWait:    p.MaxWait,
				deadline:   p.Deadline,
				state:      TaskDraining,
				snapshot:   append([]SessionSnapshot(nil), p.Snapshot...),
			}
			inst.tasks = append(inst.tasks, task)
			c.tasksByID[p.DrainID] = task

		case *evDrainCanceled:
			inst, err := c.instance(p.InstanceID)
			if err != nil {
				return &CorruptJournalError{Seq: e.Seq, Reason: err.Error()}
			}
			inst.state = StateActive
			inst.drainDeadline = time.Time{}
			if task := inst.openTask(); task != nil && task.version == p.Version {
				task.state = TaskCancelled
				task.finishedAt = e.At
			}

		case *evSessionTransferred:
			inst, err := c.instance(p.InstanceID)
			if err != nil {
				return &CorruptJournalError{Seq: e.Seq, Reason: err.Error()}
			}
			delete(inst.sessions, p.SessionID)
			rec := TransferRecord{
				SessionID: p.SessionID,
				Version:   p.Version,
				Target:    p.Target,
				At:        e.At,
			}
			inst.transfers = append(inst.transfers, rec)
			if task := inst.openTask(); task != nil {
				task.transfers = append(task.transfers, rec)
			}

		case *evSessionsKilled:
			inst, err := c.instance(p.InstanceID)
			if err != nil {
				return &CorruptJournalError{Seq: e.Seq, Reason: err.Error()}
			}
			for _, sid := range p.SessionIDs {
				delete(inst.sessions, sid)
			}

		case *evDrainCompleted:
			inst, err := c.instance(p.InstanceID)
			if err != nil {
				return &CorruptJournalError{Seq: e.Seq, Reason: err.Error()}
			}
			inst.state = StateDrained
			inst.drainDeadline = time.Time{}
			terminated := append([]TerminatedSession(nil), p.Terminated...)
			c.completions = append(c.completions, Completion{
				InstanceID: p.InstanceID,
				DrainID:    p.DrainID,
				Reason:     p.Reason,
				Terminated: terminated,
				At:         p.At,
			})
			if task := inst.openTask(); task != nil && task.version == p.DrainID {
				if p.Reason == ReasonForced {
					task.state = TaskTimedOut
					task.terminated = terminated
				} else {
					task.state = TaskDrained
				}
				task.finishedAt = p.At
			}

		default:
			return &CorruptJournalError{Seq: e.Seq, Reason: "unhandled event payload"}
		}
		return nil
	})
}
