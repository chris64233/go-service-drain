package goservicedrain

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Coordinator 协调服务实例的平滑摘流。
//
// 所有状态变更都在同一把互斥锁内串行完成，并且先持久化后提交，
// 因此“最后一次关闭”与“超时推进”并发时也只会完成一次摘流，
// 活动会话数不会出现负数。
type Coordinator struct {
	mu    sync.Mutex
	store Store
	snap  *Snapshot

	// notify 镜像 snap.Completions，容量较大；持久化的 Completions 才是可靠记录。
	notify chan Completion
}

// NewCoordinator 从 store 恢复一个协调器。
func NewCoordinator(ctx context.Context, store Store) (*Coordinator, error) {
	snap, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	normalizeSnapshot(snap)
	c := &Coordinator{
		store:  store,
		snap:   snap,
		notify: make(chan Completion, 1024),
	}
	return c, nil
}

// Completions 返回摘流完成通知通道。每轮摘流最多写入一条。
// 通道为尽力投递；可靠的完整记录请用 CompletionLog。
func (c *Coordinator) Completions() <-chan Completion {
	return c.notify
}

// CompletionLog 返回已持久化的摘流完成记录（不会因重启丢失，也不会重复）。
func (c *Coordinator) CompletionLog() []Completion {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Completion, len(c.snap.Completions))
	copy(out, c.snap.Completions)
	return out
}

// VersionLog 返回持久化的版本变化记录。
func (c *Coordinator) VersionLog() []VersionEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]VersionEvent, len(c.snap.VersionLog))
	copy(out, c.snap.VersionLog)
	return out
}

func (c *Coordinator) persistLocked(ctx context.Context) error {
	if err := c.store.Save(ctx, c.snap); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) nextID(prefix string) string {
	id := c.snap.NextSeq
	c.snap.NextSeq++
	return fmt.Sprintf("%s-%d", prefix, id)
}

func (c *Coordinator) getInstance(id string) (*Instance, error) {
	in, ok := c.snap.Instances[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
	}
	return in, nil
}

// Register 登记实例，初始为正常接流，版本纪元为 0。
func (c *Coordinator) Register(ctx context.Context, instanceID string, now time.Time) error {
	if instanceID == "" {
		return ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.snap.Instances[instanceID]; ok {
		return fmt.Errorf("%w: %s", ErrAlreadyRegistered, instanceID)
	}
	c.snap.Instances[instanceID] = &Instance{
		ID:       instanceID,
		State:    StateServing,
		Version:  0,
		Sessions: map[string]*Session{},
	}
	return c.persistLocked(ctx)
}

// AcquireSession 为已登记、且正在接流的实例领取一个带期限的会话租约。
// 摘流中的实例立即拒绝新会话（ErrDraining）。
// 返回会话 ID 与租约 ID；后续心跳/关闭需携带二者以及当前版本。
func (c *Coordinator) AcquireSession(ctx context.Context, instanceID string, now time.Time, ttl time.Duration) (sessionID, leaseID string, version int64, err error) {
	if ttl <= 0 {
		return "", "", 0, ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	in, err := c.getInstance(instanceID)
	if err != nil {
		return "", "", 0, err
	}
	if in.State == StateDrained {
		return "", "", 0, fmt.Errorf("%w: instance %s", ErrDrainCompleted, instanceID)
	}
	if in.State == StateDraining {
		return "", "", 0, fmt.Errorf("%w: instance %s", ErrDraining, instanceID)
	}

	sessionID = c.nextID("sess")
	leaseID = c.nextID("lease")
	in.Sessions[sessionID] = &Session{
		ID:        sessionID,
		LeaseID:   leaseID,
		Version:   in.Version,
		State:     SessionOpen,
		Deadline:  now.Add(ttl),
		CreatedAt: now,
	}
	in.ActiveCount++
	if err := c.persistLocked(ctx); err != nil {
		return "", "", 0, err
	}
	return sessionID, leaseID, in.Version, nil
}

// Heartbeat 会话心跳续期。版本或租约不匹配、会话已结束、实例已摘除时一律拒绝，
// 重复/乱序心跳不会改变任何计数。
func (c *Coordinator) Heartbeat(ctx context.Context, instanceID, sessionID, leaseID string, version int64, now time.Time, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	in, err := c.getInstance(instanceID)
	if err != nil {
		return err
	}
	sess, err := c.openSession(in, sessionID, leaseID, version)
	if err != nil {
		return err
	}

	deadline := now.Add(ttl)
	// 心跳不能把会话续到强制截止时间之后。
	if in.State == StateDraining && in.GraceDeadline != nil && deadline.After(*in.GraceDeadline) {
		deadline = *in.GraceDeadline
	}
	sess.Deadline = deadline
	return c.persistLocked(ctx)
}

// CloseSession 正常关闭会话。仅当版本与租约都匹配、且会话仍处于活动状态时，
// 活动会话数才减一。重复关闭返回 ErrSessionNotOpen 且不改变计数。
// 若这是最后一个活动会话且实例正在摘流，则当场完成摘流。
func (c *Coordinator) CloseSession(ctx context.Context, instanceID, sessionID, leaseID string, version int64, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	in, err := c.getInstance(instanceID)
	if err != nil {
		return err
	}
	sess, err := c.openSession(in, sessionID, leaseID, version)
	if err != nil {
		return err
	}

	closed := now
	sess.State = SessionClosed
	sess.ClosedAt = &closed
	in.ActiveCount--
	if in.ActiveCount < 0 {
		// openSession 已保证只有 open→非open 才会走到这里，理论上不可达。
		in.ActiveCount = 0
	}

	if in.State == StateDraining && in.ActiveCount == 0 {
		if err := c.completeDrainLocked(ctx, in, now, false); err != nil {
			return err
		}
	}
	return c.persistLocked(ctx)
}

// StartDrain 发起摘流：版本递增、立即停止领取新会话，存量会话在 gracePeriod 内继续。
// 已经摘流完成（终态）的实例不能再次摘流；摘流中的实例需先取消再重新发起。
// 返回新的版本号与强制截止时间。
func (c *Coordinator) StartDrain(ctx context.Context, instanceID string, now time.Time, gracePeriod time.Duration) (version int64, deadline time.Time, err error) {
	if gracePeriod <= 0 {
		return 0, time.Time{}, ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	in, err := c.getInstance(instanceID)
	if err != nil {
		return 0, time.Time{}, err
	}
	if in.State == StateDrained {
		return 0, time.Time{}, fmt.Errorf("%w: instance %s", ErrDrainCompleted, instanceID)
	}
	if in.State == StateDraining {
		return 0, time.Time{}, fmt.Errorf("%w: instance %s", ErrDraining, instanceID)
	}

	in.Version++
	in.State = StateDraining
	in.DrainStartedAt = &now
	dl := now.Add(gracePeriod)
	in.GraceDeadline = &dl
	in.ForceTerminated = 0
	in.CompletedAt = nil

	// 把存量活动会话重标到当前摘流版本：它们的关闭/心跳携带新版本才生效，
	// 而上一版本纪元的迟到请求会因版本不匹配被拒绝。
	for _, sess := range in.Sessions {
		if sess.State == SessionOpen {
			sess.Version = in.Version
		}
	}

	c.snap.VersionLog = append(c.snap.VersionLog, VersionEvent{
		InstanceID: instanceID,
		Version:    in.Version,
		Type:       ChangeDrainStarted,
		At:         now,
	})
	if err := c.persistLocked(ctx); err != nil {
		return 0, time.Time{}, err
	}

	// 极端情况下没有存量会话，立即完成摘流。
	if in.ActiveCount == 0 {
		if err := c.completeDrainLocked(ctx, in, now, false); err != nil {
			return 0, time.Time{}, err
		}
	}
	return in.Version, dl, nil
}

// CancelDrain 在截止前取消摘流并恢复接流。必须携带当前版本；
// 已完成或已进入强制终止的摘流不能被迟到取消恢复。
func (c *Coordinator) CancelDrain(ctx context.Context, instanceID string, version int64, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	in, err := c.getInstance(instanceID)
	if err != nil {
		return err
	}
	switch {
	case in.State == StateDrained:
		return fmt.Errorf("%w: instance %s", ErrDrainCompleted, instanceID)
	case in.State != StateDraining:
		return fmt.Errorf("%w: instance %s", ErrNotDraining, instanceID)
	case version != in.Version:
		return fmt.Errorf("%w: got %d want %d", ErrVersionMismatch, version, in.Version)
	}

	in.State = StateServing
	in.DrainStartedAt = nil
	in.GraceDeadline = nil
	in.ForceTerminated = 0
	c.snap.VersionLog = append(c.snap.VersionLog, VersionEvent{
		InstanceID: instanceID,
		Version:    in.Version,
		Type:       ChangeDrainCanceled,
		At:         now,
	})
	return c.persistLocked(ctx)
}

// Advance 推进逻辑时钟：回收租约过期会话，并处理摘流的归零完成与强制截止。
// now 单调由调用方保证（生产环境传 time.Now）。可对全部实例调用。
func (c *Coordinator) Advance(ctx context.Context, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	changed := false
	for _, in := range c.snap.Instances {
		if in.State == StateDrained {
			continue
		}
		for _, sess := range in.Sessions {
			if sess.State == SessionOpen && !sess.Deadline.After(now) {
				sess.State = SessionExpired
				t := now
				sess.ClosedAt = &t
				in.ActiveCount--
				changed = true
			}
		}
		if in.State != StateDraining {
			continue
		}
		switch {
		case in.ActiveCount == 0:
			if err := c.completeDrainLocked(ctx, in, now, false); err != nil {
				return err
			}
			changed = true
		case in.GraceDeadline != nil && !in.GraceDeadline.After(now):
			if err := c.forceTerminateLocked(ctx, in, now); err != nil {
				return err
			}
			changed = true
		}
	}
	if changed {
		return c.persistLocked(ctx)
	}
	return nil
}

// forceTerminateLocked 到达强制截止时间：终止所有仍活动的会话并完成摘流。
func (c *Coordinator) forceTerminateLocked(ctx context.Context, in *Instance, now time.Time) error {
	for _, sess := range in.Sessions {
		if sess.State != SessionOpen {
			continue
		}
		sess.State = SessionTerminated
		t := now
		sess.ClosedAt = &t
		in.ActiveCount--
		in.ForceTerminated++
	}
	if in.ActiveCount < 0 {
		in.ActiveCount = 0
	}
	return c.completeDrainLocked(ctx, in, now, true)
}

// completeDrainLocked 把实例置为摘流完成终态，并只生成一条完成通知。
// 调用方必须持有锁；状态机保证整个摘流轮次只进入一次。
func (c *Coordinator) completeDrainLocked(ctx context.Context, in *Instance, now time.Time, forced bool) error {
	if in.State == StateDrained {
		// 并发的关闭与超时推进在锁内串行，先到者已完成，后者直接返回。
		return nil
	}
	in.State = StateDrained
	completed := now
	in.CompletedAt = &completed

	cmp := Completion{
		InstanceID:  in.ID,
		Version:     in.Version,
		CompletedAt: now,
		Terminated:  in.ForceTerminated,
		Forced:      forced,
	}
	c.snap.Completions = append(c.snap.Completions, cmp)
	if err := c.persistLocked(ctx); err != nil {
		return err
	}
	// 持久化成功后再尽力通知；通道镜像，绝不重复生成。
	select {
	case c.notify <- cmp:
	default:
	}
	return nil
}

// openSession 是关闭/心跳共用的护栏：实例终态、版本、租约、会话状态逐一校验。
func (c *Coordinator) openSession(in *Instance, sessionID, leaseID string, version int64) (*Session, error) {
	if in.State == StateDrained {
		return nil, fmt.Errorf("%w: instance %s", ErrDrainCompleted, in.ID)
	}
	sess, ok := in.Sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSessionNotFound, sessionID)
	}
	if version != in.Version || version != sess.Version {
		return nil, fmt.Errorf("%w: got %d current %d", ErrVersionMismatch, version, in.Version)
	}
	if leaseID != sess.LeaseID {
		return nil, fmt.Errorf("%w: session %s", ErrLeaseMismatch, sessionID)
	}
	if sess.State != SessionOpen {
		return nil, fmt.Errorf("%w: session %s is %s", ErrSessionNotOpen, sessionID, sess.State)
	}
	return sess, nil
}

// Status 查询实例当前状态。
func (c *Coordinator) Status(ctx context.Context, instanceID string) (Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	in, err := c.getInstance(instanceID)
	if err != nil {
		return Status{}, err
	}
	return in.status(), nil
}
