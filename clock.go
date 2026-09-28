package goservicedrain

import (
	"sync"
	"time"
)

// Clock abstracts time acquisition so that deadline-driven behavior is
// deterministic in tests.
type Clock interface {
	Now() time.Time
}

// systemClock reports the wall-clock time.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// SystemClock returns a Clock backed by time.Now.
func SystemClock() Clock { return systemClock{} }

// FakeClock is a manually advanced Clock used by tests and by callers that want
// to drive timeout processing with their own time source. Advance moves the
// clock forward (negative durations are rejected).
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock returns a FakeClock initialized at t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d. It panics on a negative duration.
func (c *FakeClock) Advance(d time.Duration) {
	if d < 0 {
		panic("goservicedrain: cannot advance FakeClock backwards")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set moves the clock to t. Earlier timestamps are rejected.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.Before(c.now) {
		panic("goservicedrain: cannot move FakeClock backwards")
	}
	c.now = t
}
