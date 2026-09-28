package goservicedrain

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestCoordinator(t *testing.T) (*Coordinator, *FakeClock) {
	t.Helper()
	clock := NewFakeClock(time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC))
	return NewEphemeral(clock), clock
}

func TestRegisterAndUnknownInstance(t *testing.T) {
	c, _ := newTestCoordinator(t)

	if err := c.Register("inst-1"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := c.Register("inst-1"); !errors.Is(err, ErrInstanceExists) {
		t.Fatalf("duplicate Register = %v, want ErrInstanceExists", err)
	}
	if _, err := c.Status("ghost"); !errors.Is(err, ErrInstanceUnknown) {
		t.Fatalf("Status ghost = %v, want ErrInstanceUnknown", err)
	}
	if _, err := c.Heartbeat("ghost", "s", 0, time.Second); !errors.Is(err, ErrInstanceUnknown) {
		t.Fatalf("Heartbeat ghost = %v, want ErrInstanceUnknown", err)
	}
}

func TestAcquireHeartbeatCloseLifecycle(t *testing.T) {
	c, clock := newTestCoordinator(t)
	if err := c.Register("inst"); err != nil {
		t.Fatal(err)
	}

	lease, err := c.AcquireSession("inst", 10*time.Second)
	if err != nil {
		t.Fatalf("AcquireSession: %v", err)
	}
	if lease.Version != 0 || lease.InstanceID != "inst" || lease.SessionID == "" {
		t.Fatalf("unexpected lease: %+v", lease)
	}
	wantDeadline := clock.Now().Add(10 * time.Second)
	if !lease.Deadline.Equal(wantDeadline) {
		t.Fatalf("deadline = %v, want %v", lease.Deadline, wantDeadline)
	}

	st, _ := c.Status("inst")
	if st.State != StateActive || st.ActiveSessions != 1 || st.Version != 0 {
		t.Fatalf("status = %+v", st)
	}

	clock.Advance(9 * time.Second)
	newDeadline, err := c.Heartbeat("inst", lease.SessionID, lease.Version, 10*time.Second)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !newDeadline.Equal(clock.Now().Add(10 * time.Second)) {
		t.Fatalf("heartbeat deadline = %v", newDeadline)
	}

	// A second heartbeat is also valid (duplicate delivery must not error on a
	// live lease), and pushes the deadline again.
	if _, err := c.Heartbeat("inst", lease.SessionID, lease.Version, 5*time.Second); err != nil {
		t.Fatalf("duplicate Heartbeat: %v", err)
	}

	if err := c.CloseSession("inst", lease.SessionID, lease.Version); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	st, _ = c.Status("inst")
	if st.ActiveSessions != 0 {
		t.Fatalf("active after close = %d, want 0", st.ActiveSessions)
	}

	// Duplicate close: count must stay zero.
	if err := c.CloseSession("inst", lease.SessionID, lease.Version); !errors.Is(err, ErrLeaseUnknown) {
		t.Fatalf("duplicate close = %v, want ErrLeaseUnknown", err)
	}
	st, _ = c.Status("inst")
	if st.ActiveSessions != 0 || st.State != StateActive {
		t.Fatalf("state after duplicate close = %+v", st)
	}
}

func TestInvalidArguments(t *testing.T) {
	c, _ := newTestCoordinator(t)
	if err := c.Register("inst"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AcquireSession("inst", 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ttl=0: %v", err)
	}
	if _, _, err := c.BeginDrain("inst", 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("forceAfter=0: %v", err)
	}
	lease, _ := c.AcquireSession("inst", time.Second)
	if _, err := c.Heartbeat("inst", lease.SessionID, lease.Version, -1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("heartbeat ttl=-1: %v", err)
	}
}

// During draining no new sessions are handed out; existing sessions adopted by
// the new version keep running; the instance drains when they all close.
func TestGracefulDrainNaturalCompletion(t *testing.T) {
	c, clock := newTestCoordinator(t)
	_ = c.Register("inst")

	l1, _ := c.AcquireSession("inst", time.Minute)
	l2, _ := c.AcquireSession("inst", time.Minute)

	version, deadline, err := c.BeginDrain("inst", 30*time.Second)
	if err != nil {
		t.Fatalf("BeginDrain: %v", err)
	}
	if version != 1 {
		t.Fatalf("drain version = %d, want 1", version)
	}
	if !deadline.Equal(clock.Now().Add(30 * time.Second)) {
		t.Fatalf("deadline = %v", deadline)
	}

	st, _ := c.Status("inst")
	if st.State != StateDraining || st.ActiveSessions != 2 || st.Drain == nil || st.Drain.Version != 1 {
		t.Fatalf("status after BeginDrain = %+v", st)
	}
	for _, s := range st.Sessions {
		if s.Version != 1 {
			t.Fatalf("session %s not adopted to version 1: %+v", s.SessionID, s)
		}
	}

	// New traffic is refused immediately.
	if _, err := c.AcquireSession("inst", time.Minute); !errors.Is(err, ErrInstanceDraining) {
		t.Fatalf("AcquireSession while draining = %v, want ErrInstanceDraining", err)
	}

	// Old-version close for an adopted session must not touch the count.
	if err := c.CloseSession("inst", l1.SessionID, 0); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("stale close = %v, want ErrVersionMismatch", err)
	}
	st, _ = c.Status("inst")
	if st.ActiveSessions != 2 {
		t.Fatalf("stale close changed count: %+v", st)
	}
	// Old-version heartbeat likewise.
	if _, err := c.Heartbeat("inst", l1.SessionID, 0, time.Minute); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("stale heartbeat = %v, want ErrVersionMismatch", err)
	}

	// Closes carrying the current version decrement; the first one must not
	// complete the drain yet.
	if err := c.CloseSession("inst", l1.SessionID, version); err != nil {
		t.Fatalf("CloseSession l1: %v", err)
	}
	st, _ = c.Status("inst")
	if st.State != StateDraining || st.ActiveSessions != 1 {
		t.Fatalf("after first close: %+v", st)
	}
	if comps := c.Completions(); len(comps) != 0 {
		t.Fatalf("completion too early: %+v", comps)
	}

	// Last close completes the drain exactly once.
	if err := c.CloseSession("inst", l2.SessionID, version); err != nil {
		t.Fatalf("CloseSession l2: %v", err)
	}
	st, _ = c.Status("inst")
	if st.State != StateDrained || st.ActiveSessions != 0 {
		t.Fatalf("after last close: %+v", st)
	}
	comps := c.Completions()
	if len(comps) != 1 {
		t.Fatalf("completions = %d, want 1: %+v", len(comps), comps)
	}
	if comps[0].Reason != ReasonDrained || comps[0].DrainID != 1 {
		t.Fatalf("completion = %+v", comps[0])
	}

	// A drained instance never accepts traffic again.
	if _, err := c.AcquireSession("inst", time.Minute); !errors.Is(err, ErrInstanceDrained) {
		t.Fatalf("AcquireSession after drained = %v, want ErrInstanceDrained", err)
	}
	// Late close after completion cannot resurrect or decrement anything.
	if err := c.CloseSession("inst", l2.SessionID, version); !errors.Is(err, ErrLeaseUnknown) {
		t.Fatalf("late close = %v, want ErrLeaseUnknown", err)
	}
	if comps := c.Completions(); len(comps) != 1 {
		t.Fatalf("completions after late close = %d", len(comps))
	}
}

func TestBeginDrainWithNoSessionsCompletesImmediately(t *testing.T) {
	c, _ := newTestCoordinator(t)
	_ = c.Register("inst")
	version, _, err := c.BeginDrain("inst", time.Minute)
	if err != nil {
		t.Fatalf("BeginDrain: %v", err)
	}
	if version != 1 {
		t.Fatalf("version = %d", version)
	}
	st, _ := c.Status("inst")
	if st.State != StateDrained {
		t.Fatalf("state = %s, want drained", st.State)
	}
	comps := c.Completions()
	if len(comps) != 1 || comps[0].Reason != ReasonDrained {
		t.Fatalf("completions = %+v", comps)
	}
}

func TestForceTerminationAtDeadline(t *testing.T) {
	c, clock := newTestCoordinator(t)
	_ = c.Register("inst")
	l1, _ := c.AcquireSession("inst", 10*time.Minute) // lease outlives force deadline
	l2, _ := c.AcquireSession("inst", 5*time.Second)  // lease expires on its own
	version, deadline, _ := c.BeginDrain("inst", time.Minute)

	// Before the force deadline, nothing is forced.
	clock.Advance(59 * time.Second)
	if err := c.ProcessTimeouts(); err != nil {
		t.Fatalf("ProcessTimeouts: %v", err)
	}
	st, _ := c.Status("inst")
	if st.State != StateDraining {
		t.Fatalf("state before deadline = %s", st.State)
	}
	if got := len(st.Sessions); got != 1 {
		t.Fatalf("sessions before deadline = %d, want 1 (l2 lease-expired)", got)
	}

	// Exactly at the force deadline the remaining session is terminated.
	clock.Advance(time.Second)
	if !clock.Now().Equal(deadline) {
		t.Fatalf("clock %v, want deadline %v", clock.Now(), deadline)
	}
	if err := c.ProcessTimeouts(); err != nil {
		t.Fatalf("ProcessTimeouts at deadline: %v", err)
	}
	st, _ = c.Status("inst")
	if st.State != StateDrained || st.ActiveSessions != 0 {
		t.Fatalf("state after deadline = %+v", st)
	}
	comps := c.Completions()
	if len(comps) != 1 {
		t.Fatalf("completions = %+v", comps)
	}
	comp := comps[0]
	if comp.Reason != ReasonForced || comp.DrainID != version {
		t.Fatalf("completion = %+v", comp)
	}
	if len(comp.Terminated) != 1 || comp.Terminated[0].SessionID != l1.SessionID {
		t.Fatalf("terminated = %+v, want only %s", comp.Terminated, l1.SessionID)
	}
	// The lease-expired session must not be double-counted as terminated.
	for _, term := range comp.Terminated {
		if term.SessionID == l2.SessionID {
			t.Fatalf("lease-expired session %s recorded as terminated", l2.SessionID)
		}
	}

	// Re-running timeout processing produces nothing new.
	if err := c.ProcessTimeouts(); err != nil {
		t.Fatalf("second ProcessTimeouts: %v", err)
	}
	if len(c.Completions()) != 1 {
		t.Fatalf("completions after re-run = %d", len(c.Completions()))
	}
	// Late close for the killed session is ignored.
	if err := c.CloseSession("inst", l1.SessionID, version); !errors.Is(err, ErrLeaseUnknown) {
		t.Fatalf("close killed session = %v", err)
	}
	// Late cancel cannot resurrect a forced drain.
	if err := c.CancelDrain("inst", version); !errors.Is(err, ErrInstanceDrained) {
		t.Fatalf("cancel after forced = %v, want ErrInstanceDrained", err)
	}
}

func TestLeaseExpiryCompletesDrainNaturally(t *testing.T) {
	c, clock := newTestCoordinator(t)
	_ = c.Register("inst")
	l, _ := c.AcquireSession("inst", 10*time.Second)
	_, _, _ = c.BeginDrain("inst", time.Minute)

	clock.Advance(10 * time.Second)
	if err := c.ProcessTimeouts(); err != nil {
		t.Fatalf("ProcessTimeouts: %v", err)
	}
	st, _ := c.Status("inst")
	if st.State != StateDrained || st.ActiveSessions != 0 {
		t.Fatalf("state = %+v", st)
	}
	comps := c.Completions()
	if len(comps) != 1 || comps[0].Reason != ReasonDrained || len(comps[0].Terminated) != 0 {
		t.Fatalf("completion = %+v", comps)
	}
	// A late close for the expired lease changes nothing.
	if err := c.CloseSession("inst", l.SessionID, 1); !errors.Is(err, ErrLeaseUnknown) {
		t.Fatalf("close expired = %v", err)
	}
	if len(c.Completions()) != 1 {
		t.Fatal("duplicate completion after late close")
	}
}

func TestHeartbeatPreventsExpiry(t *testing.T) {
	c, clock := newTestCoordinator(t)
	_ = c.Register("inst")
	l, _ := c.AcquireSession("inst", 10*time.Second)

	clock.Advance(8 * time.Second)
	if _, err := c.Heartbeat("inst", l.SessionID, 0, 10*time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	clock.Advance(9 * time.Second)
	if err := c.ProcessTimeouts(); err != nil {
		t.Fatalf("ProcessTimeouts: %v", err)
	}
	st, _ := c.Status("inst")
	if st.ActiveSessions != 1 {
		t.Fatalf("session expired despite heartbeat: %+v", st)
	}
}

func TestCancelDrain(t *testing.T) {
	c, _ := newTestCoordinator(t)
	_ = c.Register("inst")
	l, _ := c.AcquireSession("inst", time.Minute)

	version, _, err := c.BeginDrain("inst", time.Minute)
	if err != nil {
		t.Fatalf("BeginDrain: %v", err)
	}

	// Wrong version on cancel is rejected.
	if err := c.CancelDrain("inst", version-1); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("cancel with old version = %v, want ErrVersionMismatch", err)
	}
	st, _ := c.Status("inst")
	if st.State != StateDraining {
		t.Fatalf("state after rejected cancel = %s", st.State)
	}

	if err := c.CancelDrain("inst", version); err != nil {
		t.Fatalf("CancelDrain: %v", err)
	}
	st, _ = c.Status("inst")
	if st.State != StateActive || st.Version != version || st.Drain != nil {
		t.Fatalf("state after cancel = %+v", st)
	}
	if st.ActiveSessions != 1 {
		t.Fatalf("sessions after cancel = %d", st.ActiveSessions)
	}

	// Traffic flows again, under the current version.
	newLease, err := c.AcquireSession("inst", time.Minute)
	if err != nil {
		t.Fatalf("AcquireSession after cancel: %v", err)
	}
	if newLease.Version != version {
		t.Fatalf("new lease version = %d, want %d", newLease.Version, version)
	}

	// Messages carrying the pre-drain version remain stale.
	if err := c.CloseSession("inst", l.SessionID, 0); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("old-version close after cancel = %v, want ErrVersionMismatch", err)
	}

	// Cancel with no drain in progress.
	if err := c.CancelDrain("inst", version); !errors.Is(err, ErrNoDrainInProgress) {
		t.Fatalf("cancel while active = %v, want ErrNoDrainInProgress", err)
	}
}

// A drain completed naturally cannot be canceled, and a cancel for an old
// drain must not undo a newer drain.
func TestLateCancelCannotUndoNewerDrain(t *testing.T) {
	c, _ := newTestCoordinator(t)
	_ = c.Register("inst")
	l, _ := c.AcquireSession("inst", time.Minute)

	// Drain v1, cancel, drain v2, complete naturally.
	v1, _, _ := c.BeginDrain("inst", time.Minute)
	if err := c.CancelDrain("inst", v1); err != nil {
		t.Fatalf("CancelDrain v1: %v", err)
	}
	if err := c.CloseSession("inst", l.SessionID, v1); err != nil { // v1 after cancel == current
		t.Fatalf("CloseSession: %v", err)
	}
	if _, _, err := c.BeginDrain("inst", time.Minute); err != nil {
		t.Fatalf("BeginDrain v2: %v", err)
	}
	// no sessions -> completes immediately and permanently
	st, _ := c.Status("inst")
	if st.State != StateDrained || st.Version != 2 {
		t.Fatalf("status = %+v", st)
	}

	if err := c.CancelDrain("inst", v1); !errors.Is(err, ErrInstanceDrained) {
		t.Fatalf("late cancel v1 = %v, want ErrInstanceDrained", err)
	}
	if err := c.CancelDrain("inst", 2); !errors.Is(err, ErrInstanceDrained) {
		t.Fatalf("cancel v2 = %v, want ErrInstanceDrained", err)
	}
	if _, err := c.AcquireSession("inst", time.Second); !errors.Is(err, ErrInstanceDrained) {
		t.Fatalf("AcquireSession = %v, want ErrInstanceDrained", err)
	}
}

// Redrain after cancel keeps incrementing versions and invalidates old
// generations' messages.
func TestRedrainVersionAdvances(t *testing.T) {
	c, _ := newTestCoordinator(t)
	_ = c.Register("inst")
	l, _ := c.AcquireSession("inst", time.Minute) // acquired under v0

	v1, _, _ := c.BeginDrain("inst", time.Minute) // adopts session to v1
	if err := c.CancelDrain("inst", v1); err != nil {
		t.Fatal(err)
	}
	v2, _, err := c.BeginDrain("inst", time.Minute) // adopts to v2
	if err != nil {
		t.Fatalf("second BeginDrain: %v", err)
	}
	if v2 != 2 {
		t.Fatalf("second drain version = %d, want 2", v2)
	}
	st, _ := c.Status("inst")
	if st.Sessions[0].Version != 2 {
		t.Fatalf("session version = %d, want 2", st.Sessions[0].Version)
	}
	for _, stale := range []int64{0, v1} {
		if err := c.CloseSession("inst", l.SessionID, stale); !errors.Is(err, ErrVersionMismatch) {
			t.Fatalf("close v%d = %v, want ErrVersionMismatch", stale, err)
		}
	}
	if err := c.CloseSession("inst", l.SessionID, v2); err != nil {
		t.Fatalf("close current version: %v", err)
	}
	if comps := c.Completions(); len(comps) != 1 || comps[0].DrainID != v2 {
		t.Fatalf("completions = %+v", comps)
	}
}

// The last natural close and force-timeout processing must never complete the
// drain twice or expose a negative active count, regardless of interleaving.
func TestConcurrentCloseVersusForceTimeout(t *testing.T) {
	const iterations = 200
	for iter := 0; iter < iterations; iter++ {
		clock := NewFakeClock(time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC))
		c := NewEphemeral(clock)
		id := fmt.Sprintf("inst-%d", iter)
		if err := c.Register(id); err != nil {
			t.Fatal(err)
		}

		const n = 8
		leases := make([]Lease, n)
		for i := range leases {
			l, err := c.AcquireSession(id, 10*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			leases[i] = l
		}
		version, deadline, err := c.BeginDrain(id, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		clock.Set(deadline) // force deadline is now due

		var wg sync.WaitGroup
		// Closers: each session closed twice (duplicate delivery) plus stale
		// versions, all concurrently with timeout processing.
		for _, l := range leases {
			l := l
			for _, v := range []int64{version, version, version - 1} {
				wg.Add(1)
				go func(v int64) {
					defer wg.Done()
					_ = c.CloseSession(id, l.SessionID, v)
				}(v)
			}
		}
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = c.ProcessTimeouts()
			}()
		}
		wg.Wait()

		st, err := c.Status(id)
		if err != nil {
			t.Fatal(err)
		}
		if st.State != StateDrained {
			t.Fatalf("iter %d: state = %s, want drained", iter, st.State)
		}
		if st.ActiveSessions != 0 || st.ActiveSessions < 0 {
			t.Fatalf("iter %d: active sessions = %d", iter, st.ActiveSessions)
		}
		comps := c.Completions()
		if len(comps) != 1 {
			t.Fatalf("iter %d: completions = %d, want exactly 1", iter, len(comps))
		}
		switch comps[0].Reason {
		case ReasonDrained, ReasonForced:
		default:
			t.Fatalf("iter %d: reason = %q", iter, comps[0].Reason)
		}
		// Every session ends up either naturally closed or force-terminated,
		// never both, and the terminated list contains no duplicates.
		seen := map[string]bool{}
		for _, term := range comps[0].Terminated {
			if seen[term.SessionID] {
				t.Fatalf("iter %d: session %s terminated twice", iter, term.SessionID)
			}
			seen[term.SessionID] = true
		}
		if reason := comps[0].Reason; reason == ReasonForced && len(seen) > n {
			t.Fatalf("iter %d: more terminated sessions (%d) than acquired (%d)", iter, len(seen), n)
		}
	}
}

// Mixed concurrent lifecycle operations must keep counters consistent.
func TestConcurrentMixedOperations(t *testing.T) {
	c, _ := newTestCoordinator(t)
	_ = c.Register("inst")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			_, _ = c.AcquireSession("inst", time.Minute)
		}()
		go func() {
			defer wg.Done()
			st, err := c.Status("inst")
			if err == nil && (st.ActiveSessions < 0 || len(st.Sessions) != st.ActiveSessions) {
				t.Errorf("inconsistent status: %+v", st)
			}
		}()
		go func() {
			defer wg.Done()
			_ = c.ProcessTimeouts()
		}()
		go func() {
			defer wg.Done()
			_, _ = c.Heartbeat("inst", "missing", 0, time.Second)
		}()
	}
	wg.Wait()
	st, _ := c.Status("inst")
	if len(st.Sessions) != st.ActiveSessions {
		t.Fatalf("counter mismatch: %d vs %d", st.ActiveSessions, len(st.Sessions))
	}
}

func TestFileJournalRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.log")

	c1, j1, err := RecoverCoordinator(path)
	if err != nil {
		t.Fatalf("RecoverCoordinator: %v", err)
	}
	_ = c1.Register("inst")
	l1, _ := c1.AcquireSession("inst", 10*time.Minute)
	l2, _ := c1.AcquireSession("inst", 10*time.Second)
	v1, deadline, _ := c1.BeginDrain("inst", time.Minute)
	// Heartbeat l1 under the adopted version.
	if _, err := c1.Heartbeat("inst", l1.SessionID, v1, time.Hour); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	// l2's own lease expires; drain must not complete because l1 remains.
	if err := c1.Advance(deadline.Add(-time.Second)); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	st1, _ := c1.Status("inst")
	if st1.State != StateDraining || st1.ActiveSessions != 1 {
		t.Fatalf("pre-close status = %+v", st1)
	}
	if err := c1.CloseSession("inst", l1.SessionID, v1); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if err := j1.Close(); err != nil {
		t.Fatalf("journal close: %v", err)
	}

	// Rebuild from the same journal and verify the final state and the single
	// completion.
	c2, j2, err := RecoverCoordinator(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer j2.Close()

	st2, err := c2.Status("inst")
	if err != nil {
		t.Fatalf("Status after recovery: %v", err)
	}
	if st2.State != StateDrained || st2.Version != 1 || st2.ActiveSessions != 0 {
		t.Fatalf("recovered status = %+v", st2)
	}
	comps := c2.Completions()
	if len(comps) != 1 || comps[0].Reason != ReasonDrained || comps[0].DrainID != 1 {
		t.Fatalf("recovered completions = %+v", comps)
	}
	// l2 must not appear terminated; it expired naturally.
	if len(comps[0].Terminated) != 0 {
		t.Fatalf("unexpected terminated sessions: %+v", comps[0].Terminated)
	}

	// New session ids must not collide with ids minted before the crash.
	l3, err := c2.AcquireSession("inst", time.Second)
	if err == nil {
		t.Fatalf("AcquireSession on drained instance unexpectedly succeeded: %+v", l3)
	}

	// A second instance in the same coordinator keeps working after recovery.
	if err := c2.Register("inst-2"); err != nil {
		t.Fatalf("Register inst-2: %v", err)
	}
	l, err := c2.AcquireSession("inst-2", time.Minute)
	if err != nil {
		t.Fatalf("AcquireSession inst-2: %v", err)
	}
	if l.SessionID == l1.SessionID || l.SessionID == l2.SessionID {
		t.Fatalf("session id collision after recovery: %s", l.SessionID)
	}
}

func TestFileJournalReplayForcedDrain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.log")

	func() {
		clock := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		j, err := OpenFileJournal(path)
		if err != nil {
			t.Fatal(err)
		}
		c, err := NewCoordinator(j)
		if err != nil {
			t.Fatal(err)
		}
		c.clock = clock
		defer j.Close()
		_ = c.Register("inst")
		_, _ = c.AcquireSession("inst", 24*time.Hour) // outlives the force deadline
		_, _, _ = c.BeginDrain("inst", time.Minute)
		if err := c.ProcessTimeouts(); err != nil { // before deadline: nothing
			t.Fatal(err)
		}
		st, _ := c.Status("inst")
		if st.State != StateDraining {
			t.Fatalf("state = %s", st.State)
		}
		clock.Advance(time.Minute)
		if err := c.ProcessTimeouts(); err != nil {
			t.Fatal(err)
		}
	}()

	c, j, err := RecoverCoordinator(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	st, _ := c.Status("inst")
	if st.State != StateDrained || st.ActiveSessions != 0 {
		t.Fatalf("recovered state = %+v", st)
	}
	comps := c.Completions()
	if len(comps) != 1 || comps[0].Reason != ReasonForced || len(comps[0].Terminated) != 1 {
		t.Fatalf("recovered completion = %+v", comps)
	}
}

func TestCancelThenReopenJournal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.log")

	c1, j1, err := RecoverCoordinator(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = c1.Register("inst")
	_, _ = c1.AcquireSession("inst", time.Hour)
	v, _, _ := c1.BeginDrain("inst", time.Hour)
	if err := c1.CancelDrain("inst", v); err != nil {
		t.Fatal(err)
	}
	if err := j1.Close(); err != nil {
		t.Fatal(err)
	}

	c2, j2, err := RecoverCoordinator(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	st, _ := c2.Status("inst")
	if st.State != StateActive || st.Version != 1 || st.ActiveSessions != 1 {
		t.Fatalf("recovered status = %+v", st)
	}
	if len(c2.Completions()) != 0 {
		t.Fatal("canceled drain must not produce a completion")
	}
	if _, err := c2.AcquireSession("inst", time.Hour); err != nil {
		t.Fatalf("AcquireSession after recovery: %v", err)
	}
}
