package goservicedrain

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

// Journal persists the sequence of facts produced by the coordinator:
// session/version changes and drain lifecycle transitions. Events are
// append-only; coordinator state is fully reconstructed by replaying the
// journal, so no separate state snapshot is needed.
type Journal interface {
	// Append atomically appends a batch of events.
	Append(events []Event) error
	// Replay invokes fn for every previously persisted event, in order.
	Replay(fn func(Event) error) error
}

// Event kinds persisted in the journal.
const (
	kindRegistered     = "instance_registered"
	kindLeaseAcquired  = "lease_acquired"
	kindHeartbeat      = "session_heartbeat"
	kindSessionClosed  = "session_closed"
	kindSessionExpired = "session_expired"
	kindDrainBegan     = "drain_began"
	kindDrainCanceled  = "drain_canceled"
	kindSessionsKilled = "sessions_terminated"
	kindDrainCompleted = "drain_completed"
)

// Event is one persisted fact. Payload is one of the unexported *Ev* types
// below.
type Event struct {
	// Seq is the 1-based position of the event in the journal.
	Seq     int64
	Kind    string
	At      time.Time
	Payload any
}

type evRegistered struct {
	InstanceID string
	Version    int64
}

type evLeaseAcquired struct {
	InstanceID string
	SessionID  string
	Version    int64
	Deadline   time.Time
}

type evHeartbeat struct {
	InstanceID string
	SessionID  string
	Version    int64
	Deadline   time.Time
}

type evSessionEnded struct {
	InstanceID string
	SessionID  string
	Version    int64
}

type evDrainBegan struct {
	InstanceID string
	Version    int64
	Deadline   time.Time
	// SessionVersions pins every in-flight session to the new drain version
	// at adoption time.
	SessionVersions map[string]int64
}

type evDrainCanceled struct {
	InstanceID string
	Version    int64
}

type evSessionsKilled struct {
	InstanceID string
	Version    int64
	SessionIDs []string
}

type evDrainCompleted struct {
	InstanceID string
	DrainID    int64
	Reason     string
	Terminated []TerminatedSession
	At         time.Time
}

// payloadByKind constructs an empty payload value of the kind recorded in an
// envelope.
func payloadByKind(kind string) any {
	switch kind {
	case kindRegistered:
		return new(evRegistered)
	case kindLeaseAcquired:
		return new(evLeaseAcquired)
	case kindHeartbeat:
		return new(evHeartbeat)
	case kindSessionClosed, kindSessionExpired:
		return new(evSessionEnded)
	case kindDrainBegan:
		return new(evDrainBegan)
	case kindDrainCanceled:
		return new(evDrainCanceled)
	case kindSessionsKilled:
		return new(evSessionsKilled)
	case kindDrainCompleted:
		return new(evDrainCompleted)
	default:
		return nil
	}
}

type envelope struct {
	Seq     int64           `json:"seq"`
	Kind    string          `json:"kind"`
	At      time.Time       `json:"at"`
	Payload json.RawMessage `json:"payload"`
}

// memJournal keeps events in memory; useful for tests and ephemeral
// coordinators.
type memJournal struct {
	mu     sync.Mutex
	events []Event
}

func newMemJournal() *memJournal { return &memJournal{} }

func (j *memJournal) Append(events []Event) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.events = append(j.events, events...)
	return nil
}

func (j *memJournal) Replay(fn func(Event) error) error {
	j.mu.Lock()
	events := make([]Event, len(j.events))
	copy(events, j.events)
	j.mu.Unlock()
	for _, e := range events {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// FileJournal is an append-only newline-delimited JSON journal on disk. Each
// Append batch is buffered and flushed to stable storage (fsync) as a unit.
type FileJournal struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// OpenFileJournal opens (creating if needed) the journal at path for append
// and replay. An existing journal is not truncated.
func OpenFileJournal(path string) (*FileJournal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &FileJournal{f: f, w: bufio.NewWriter(f)}, nil
}

// Append serializes the batch as one JSON object per line, flushes the buffer
// and fsyncs the file so the batch survives a crash.
func (j *FileJournal) Append(events []Event) error {
	if len(events) == 0 {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, e := range events {
		raw, err := json.Marshal(e.Payload)
		if err != nil {
			return err
		}
		env := envelope{Seq: e.Seq, Kind: e.Kind, At: e.At, Payload: raw}
		line, err := json.Marshal(env)
		if err != nil {
			return err
		}
		if _, err := j.w.Write(line); err != nil {
			return err
		}
		if err := j.w.WriteByte('\n'); err != nil {
			return err
		}
	}
	if err := j.w.Flush(); err != nil {
		return err
	}
	return j.f.Sync()
}

// Replay reads the journal from the beginning and invokes fn per event.
func (j *FileJournal) Replay(fn func(Event) error) error {
	j.mu.Lock()
	if err := j.w.Flush(); err != nil {
		j.mu.Unlock()
		return err
	}
	j.mu.Unlock()

	f, err := os.Open(j.f.Name())
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(bufio.NewReader(f))
	for {
		var env envelope
		if err := dec.Decode(&env); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		payload := payloadByKind(env.Kind)
		if payload == nil {
			return &CorruptJournalError{Seq: env.Seq, Reason: "unknown event kind " + env.Kind}
		}
		if err := json.Unmarshal(env.Payload, payload); err != nil {
			return &CorruptJournalError{Seq: env.Seq, Reason: err.Error()}
		}
		if err := fn(Event{Seq: env.Seq, Kind: env.Kind, At: env.At, Payload: payload}); err != nil {
			return err
		}
	}
}

// Close flushes pending writes and releases the file handle.
func (j *FileJournal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.w.Flush(); err != nil {
		_ = j.f.Close()
		return err
	}
	return j.f.Close()
}

// CorruptJournalError indicates that a journal line could not be decoded.
type CorruptJournalError struct {
	Seq    int64
	Reason string
}

func (e *CorruptJournalError) Error() string {
	return "goservicedrain: corrupt journal at seq " +
		strconv.FormatInt(e.Seq, 10) + ": " + e.Reason
}
