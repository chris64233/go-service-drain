package goservicedrain

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	c, err := NewCoordinator(context.Background(), NewMemoryStore())
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}
	return c
}

func at(min int, sec int) time.Time {
	return time.Date(2026, 9, 28, 10, min, sec, 0, time.UTC)
}

func mustAcquire(t *testing.T, c *Coordinator, inst string, now time.Time, ttl time.Duration) (string, string, int64) {
	t.Helper()
	sid, lid, ver, err := c.AcquireSession(context.Background(), inst, now, ttl)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	return sid, lid, ver
}

func drainVersion(t *testing.T, c *Coordinator, inst string) int64 {
	t.Helper()
	st, err := c.Status(context.Background(), inst)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	return st.Version
}

// 1. 基本流程：登记 -> 领取 -> 摘流 -> 关闭最后一个会话 -> 自动完成。
func TestBasicDrainCompletesOnZeroSessions(t *testing.T) {
	c := newTestCoordinator(t)
	now := at(0, 0)

	if err := c.Register(context.Background(), "inst-1", now); err != nil {
		t.Fatal(err)
	}
	s1, l1, v0 := mustAcquire(t, c, "inst-1", now, time.Minute)
	if v0 != 0 {
		t.Fatalf("initial version = %d, want 0", v0)
	}

	ver, deadline, err := c.StartDrain(context.Background(), "inst-1", now.Add(time.Second), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ver != 1 || !deadline.Equal(now.Add(31*time.Second)) {
		t.Fatalf("drain ver=%d deadline=%v", ver, deadline)
	}

	// 摘流后立即拒绝新会话。
	if _, _, _, err := c.AcquireSession(context.Background(), "inst-1", now.Add(2*time.Second), time.Minute); !errors.Is(err, ErrDraining) {
		t.Fatalf("acquire during drain err=%v, want ErrDraining", err)
	}

	st, _ := c.Status(context.Background(), "inst-1")
	if st.State != StateDraining || st.ActiveCount != 1 {
		t.Fatalf("status = %+v", st)
	}

	// 用摘流后的当前版本关闭。
	if err := c.CloseSession(context.Background(), "inst-1", s1, l1, ver, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}

	st, _ = c.Status(context.Background(), "inst-1")
	if st.State != StateDrained || st.ActiveCount != 0 {
		t.Fatalf("status after close = %+v", st)
	}
	log := c.CompletionLog()
	if len(log) != 1 || log[0].Forced || log[0].Version != 1 {
		t.Fatalf("completion log = %+v", log)
	}

	// 终态实例不能再摘流，也不能领取。
	if _, _, err := c.StartDrain(context.Background(), "inst-1", now, time.Minute); !errors.Is(err, ErrDrainCompleted) {
		t.Fatalf("redrain err=%v, want ErrDrainCompleted", err)
	}
	if _, _, _, err := c.AcquireSession(context.Background(), "inst-1", now, time.Minute); !errors.Is(err, ErrDrainCompleted) {
		t.Fatalf("acquire drained err=%v", err)
	}
}

// 2. 乱序/重复的关闭与心跳：旧版本、错租约、重复关闭都不能改变计数。
func TestStaleAndDuplicateRequests(t *testing.T) {
	c := newTestCoordinator(t)
	now := at(1, 0)
	ctx := context.Background()
	_ = c.Register(ctx, "i", now)

	s1, l1, _ := mustAcquire(t, c, "i", now, time.Minute)
	s2, l2, _ := mustAcquire(t, c, "i", now, time.Minute)

	ver, _, _ := c.StartDrain(ctx, "i", now.Add(time.Second), time.Minute)
	if ver != 1 {
		t.Fatalf("ver = %d", ver)
	}

	// 旧版本（v0）的迟到关闭：拒绝，计数不变。
	if err := c.CloseSession(ctx, "i", s1, l1, 0, now.Add(2*time.Second)); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("stale close err=%v", err)
	}
	if st, _ := c.Status(ctx, "i"); st.ActiveCount != 2 {
		t.Fatalf("active = %d, want 2", st.ActiveCount)
	}

	// 错误租约：拒绝。
	if err := c.CloseSession(ctx, "i", s1, l2, ver, now.Add(2*time.Second)); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("wrong lease err=%v", err)
	}

	// 不存在的会话。
	if err := c.CloseSession(ctx, "i", "nope", l1, ver, now.Add(2*time.Second)); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing session err=%v", err)
	}

	// 正确关闭 s1。
	if err := c.CloseSession(ctx, "i", s1, l1, ver, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	// 重复关闭：拒绝且计数不变（仍为 1，不能出现 0 或负数）。
	err := c.CloseSession(ctx, "i", s1, l1, ver, now.Add(4*time.Second))
	if !errors.Is(err, ErrSessionNotOpen) {
		t.Fatalf("duplicate close err=%v", err)
	}
	st, _ := c.Status(ctx, "i")
	if st.ActiveCount != 1 || st.State != StateDraining {
		t.Fatalf("status = %+v", st)
	}

	// 旧版本心跳对 s2 无效；正确版本心跳可续期。
	if err := c.Heartbeat(ctx, "i", s2, l2, 0, now.Add(5*time.Second), time.Minute); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("stale heartbeat err=%v", err)
	}
	if err := c.Heartbeat(ctx, "i", s2, l2, ver, now.Add(5*time.Second), time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	// 关闭后的 s1 心跳同样拒绝。
	if err := c.Heartbeat(ctx, "i", s1, l1, ver, now.Add(5*time.Second), time.Minute); !errors.Is(err, ErrSessionNotOpen) {
		t.Fatalf("heartbeat closed err=%v", err)
	}
}

// 取消摘流后旧摘流轮次的迟到关闭不能影响新版本纪元的计数；
// 重新摘流后旧版本的迟到请求也不能让实例重新接流。
func TestCancelAndRedrainStaleRequests(t *testing.T) {
	c := newTestCoordinator(t)
	now := at(2, 0)
	ctx := context.Background()
	_ = c.Register(ctx, "i", now)

	s1, l1, _ := mustAcquire(t, c, "i", now, 10*time.Minute)
	v1, _, _ := c.StartDrain(ctx, "i", now, time.Minute) // v1

	// 携带错误版本取消：拒绝。
	if err := c.CancelDrain(ctx, "i", v1-1, now); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("cancel wrong version err=%v", err)
	}
	if err := c.CancelDrain(ctx, "i", v1, now.Add(time.Second)); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	st, _ := c.Status(ctx, "i")
	if st.State != StateServing || st.Draining {
		t.Fatalf("after cancel status = %+v", st)
	}

	// 取消后恢复接流：可以领取新会话，版本保持 v1。
	s2, l2, vAcq := mustAcquire(t, c, "i", now.Add(2*time.Second), 10*time.Minute)
	if vAcq != 1 {
		t.Fatalf("acquired version %d, want 1", vAcq)
	}

	// 旧摘流版本 v1 的迟到关闭此时与当前版本恰好相同但会话仍 open，属于合法关闭。
	// 重新摘流 -> v2 后，v1 的迟到请求必须全部被拒。
	v2, _, _ := c.StartDrain(ctx, "i", now.Add(3*time.Second), time.Minute)
	if v2 != 2 {
		t.Fatalf("redrain version = %d, want 2", v2)
	}
	if err := c.CloseSession(ctx, "i", s2, l2, v1, now.Add(4*time.Second)); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("v1 close after v2 drain err=%v", err)
	}
	if err := c.CancelDrain(ctx, "i", v1, now.Add(4*time.Second)); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("v1 cancel after v2 drain err=%v", err)
	}
	if st, _ := c.Status(ctx, "i"); st.State != StateDraining || st.ActiveCount != 2 {
		t.Fatalf("status = %+v", st)
	}
	// 用当前版本取消 v2 摘流，s1 仍可正常关闭。
	if err := c.CancelDrain(ctx, "i", v2, now.Add(5*time.Second)); err != nil {
		t.Fatalf("cancel v2: %v", err)
	}
	if err := c.CloseSession(ctx, "i", s1, l1, v2, now.Add(6*time.Second)); err != nil {
		t.Fatalf("close s1: %v", err)
	}
	if st, _ := c.Status(ctx, "i"); st.ActiveCount != 1 {
		t.Fatalf("active = %d, want 1", st.ActiveCount)
	}
}

// 3a. 租约过期由 Advance 回收；之后的关闭/心跳不能再次减计数。
func TestLeaseExpiry(t *testing.T) {
	c := newTestCoordinator(t)
	now := at(3, 0)
	ctx := context.Background()
	_ = c.Register(ctx, "i", now)
	s1, l1, _ := mustAcquire(t, c, "i", now, time.Minute)
	s2, l2, _ := mustAcquire(t, c, "i", now, 5*time.Minute)

	if err := c.Advance(ctx, now.Add(61*time.Second)); err != nil {
		t.Fatal(err)
	}
	st, _ := c.Status(ctx, "i")
	if st.State != StateServing || st.ActiveCount != 1 {
		t.Fatalf("status = %+v", st)
	}
	// 已过期会话的迟到关闭：拒绝，计数保持 1。
	if err := c.CloseSession(ctx, "i", s1, l1, 0, now.Add(2*time.Minute)); !errors.Is(err, ErrSessionNotOpen) {
		t.Fatalf("close expired err=%v", err)
	}
	if st, _ := c.Status(ctx, "i"); st.ActiveCount != 1 {
		t.Fatalf("active = %d, want 1", st.ActiveCount)
	}
	// 未过期会话不受影响。
	if err := c.Heartbeat(ctx, "i", s2, l2, 0, now.Add(2*time.Minute), time.Minute); err != nil {
		t.Fatalf("heartbeat s2: %v", err)
	}
}

// 3b. 强制截止：未关闭会话被终止、计数归零、只产生一条通知、终态不可恢复。
func TestForceDeadlineTermination(t *testing.T) {
	c := newTestCoordinator(t)
	now := at(4, 0)
	ctx := context.Background()
	_ = c.Register(ctx, "i", now)
	s1, l1, _ := mustAcquire(t, c, "i", now, time.Hour)
	s2, l2, _ := mustAcquire(t, c, "i", now, time.Hour)

	ver, dl, _ := c.StartDrain(ctx, "i", now, time.Minute)

	// 截止前取消：允许。截止后再取消：拒绝。
	if err := c.Advance(ctx, dl.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	st, _ := c.Status(ctx, "i")
	if st.State != StateDrained || st.ActiveCount != 0 || st.ForceTerminated != 2 {
		t.Fatalf("status = %+v", st)
	}
	log := c.CompletionLog()
	if len(log) != 1 || !log[0].Forced || log[0].Terminated != 2 || log[0].Version != ver {
		t.Fatalf("completion = %+v", log)
	}
	// 通知通道也只有一条。
	select {
	case n := <-c.Completions():
		if !n.Forced || n.Terminated != 2 {
			t.Fatalf("notify = %+v", n)
		}
	default:
		t.Fatal("missing completion notification")
	}
	select {
	case <-c.Completions():
		t.Fatal("duplicate notification")
	default:
	}

	// 迟到取消不能让已摘除实例重新接流。
	if err := c.CancelDrain(ctx, "i", ver, dl.Add(2*time.Second)); !errors.Is(err, ErrDrainCompleted) {
		t.Fatalf("cancel after force err=%v", err)
	}
	// 迟到关闭不能减少计数（计数已是 0，必须拒绝而非变成 -1）。
	if err := c.CloseSession(ctx, "i", s1, l1, ver, dl.Add(3*time.Second)); !errors.Is(err, ErrDrainCompleted) {
		t.Fatalf("close after force err=%v", err)
	}
	if err := c.CloseSession(ctx, "i", s2, l2, ver, dl.Add(3*time.Second)); !errors.Is(err, ErrDrainCompleted) {
		t.Fatalf("close after force err=%v", err)
	}
	if st, _ := c.Status(ctx, "i"); st.ActiveCount != 0 {
		t.Fatalf("active = %d, want 0", st.ActiveCount)
	}
	// 再推进一次时间也不能重复完成。
	if err := c.Advance(ctx, dl.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if len(c.CompletionLog()) != 1 {
		t.Fatalf("completion log = %d, want 1", len(c.CompletionLog()))
	}
}

// 3c. 最后一次关闭与超时推进并发：只完成一次、无负数。
func TestConcurrentCloseAndAdvance(t *testing.T) {
	c := newTestCoordinator(t)
	now := at(5, 0)
	ctx := context.Background()
	_ = c.Register(ctx, "i", now)

	const n = 200
	type s struct{ id, lease string }
	sessions := make([]s, n)
	for i := 0; i < n; i++ {
		sid, lid, _ := mustAcquire(t, c, "i", now, time.Minute)
		sessions[i] = s{sid, lid}
	}
	ver, _, _ := c.StartDrain(ctx, "i", now, time.Minute)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			_ = c.CloseSession(ctx, "i", sessions[i].id, sessions[i].lease, ver, now.Add(30*time.Second))
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = c.Advance(ctx, now.Add(30*time.Second))
	}()
	wg.Wait()

	st, _ := c.Status(ctx, "i")
	if st.State != StateDrained {
		t.Fatalf("state = %s, want drained", st.State)
	}
	if st.ActiveCount != 0 {
		t.Fatalf("active = %d, want 0", st.ActiveCount)
	}
	if len(c.CompletionLog()) != 1 {
		t.Fatalf("completions = %d, want exactly 1", len(c.CompletionLog()))
	}
}

// 空摘流（无活动会话时发起）立即完成，也只产生一条通知。
func TestEmptyDrainCompletesImmediately(t *testing.T) {
	c := newTestCoordinator(t)
	ctx := context.Background()
	now := at(6, 0)
	_ = c.Register(ctx, "i", now)
	ver, _, err := c.StartDrain(ctx, "i", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ver != 1 {
		t.Fatalf("ver = %d", ver)
	}
	st, _ := c.Status(ctx, "i")
	if st.State != StateDrained || len(c.CompletionLog()) != 1 {
		t.Fatalf("status=%+v log=%d", st, len(c.CompletionLog()))
	}
	// 已完成不能取消。
	if err := c.CancelDrain(ctx, "i", ver, now); !errors.Is(err, ErrDrainCompleted) {
		t.Fatalf("cancel completed err=%v", err)
	}
}

// 非摘流状态下取消返回 ErrNotDraining。
func TestCancelWhenServing(t *testing.T) {
	c := newTestCoordinator(t)
	ctx := context.Background()
	_ = c.Register(ctx, "i", at(7, 0))
	if err := c.CancelDrain(ctx, "i", 0, at(7, 0)); !errors.Is(err, ErrNotDraining) {
		t.Fatalf("err=%v, want ErrNotDraining", err)
	}
}

// 未登记实例的操作返回 ErrInstanceNotFound。
func TestUnknownInstance(t *testing.T) {
	c := newTestCoordinator(t)
	ctx := context.Background()
	now := at(8, 0)
	if _, err := c.Status(ctx, "ghost"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("status err=%v", err)
	}
	if err := c.Register(ctx, "a", now); err != nil {
		t.Fatal(err)
	}
	if err := c.Register(ctx, "a", now); !errors.Is(err, ErrAlreadyRegistered) {
		t.Fatalf("dup register err=%v", err)
	}
}

// 心跳续期不能超过强制截止时间；截止推进后被终止。
func TestHeartbeatCappedByGraceDeadline(t *testing.T) {
	c := newTestCoordinator(t)
	ctx := context.Background()
	now := at(9, 0)
	_ = c.Register(ctx, "i", now)
	s1, l1, _ := mustAcquire(t, c, "i", now, time.Minute)
	_, dl, _ := c.StartDrain(ctx, "i", now, 2*time.Minute)
	if err := c.Heartbeat(ctx, "i", s1, l1, 1, now.Add(time.Second), time.Hour); err != nil {
		t.Fatal(err)
	}
	// 推进到租约原 deadline 之后但 grace deadline 之前：会话因被截到 grace 而过期，
	// 随后摘流归零完成（非强制，terminated=0）。
	if err := c.Advance(ctx, dl); err != nil {
		t.Fatal(err)
	}
	st, _ := c.Status(ctx, "i")
	if st.State != StateDrained || st.ForceTerminated != 0 {
		t.Fatalf("status = %+v", st)
	}
}

// 6. 文件持久化：重启后状态、版本日志、完成记录都恢复，且不重发完成。
func TestFileStoreRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	ctx := context.Background()
	now := at(10, 0)

	store := NewFileStore(path)
	c, err := NewCoordinator(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Register(ctx, "i", now)
	s1, l1, _ := mustAcquire(t, c, "i", now, time.Hour)
	v1, _, _ := c.StartDrain(ctx, "i", now, time.Minute)
	// 另一个实例在到期时被强制终止。
	_ = c.Register(ctx, "j", now)
	sj, lj, _ := mustAcquire(t, c, "j", now, time.Hour)
	_, dlJ, _ := c.StartDrain(ctx, "j", now, time.Minute)
	if err := c.CloseSession(ctx, "i", s1, l1, v1, now.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := c.Advance(ctx, dlJ.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	// 重新加载。
	c2, err := NewCoordinator(ctx, NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	si, _ := c2.Status(ctx, "i")
	if si.State != StateDrained || si.ActiveCount != 0 {
		t.Fatalf("i = %+v", si)
	}
	sj2, _ := c2.Status(ctx, "j")
	if sj2.State != StateDrained || sj2.ForceTerminated != 1 {
		t.Fatalf("j = %+v", sj2)
	}
	if len(c2.CompletionLog()) != 2 {
		t.Fatalf("completions = %d, want 2", len(c2.CompletionLog()))
	}
	if len(c2.VersionLog()) != 2 {
		t.Fatalf("version log = %d, want 2", len(c2.VersionLog()))
	}
	// 重启后迟到请求依旧被拒。
	if err := c2.CloseSession(ctx, "j", sj, lj, 1, now.Add(time.Hour)); !errors.Is(err, ErrDrainCompleted) {
		t.Fatalf("stale close after restart err=%v", err)
	}
	// 恢复后推进时间不应重复产生完成记录。
	if err := c2.Advance(ctx, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if len(c2.CompletionLog()) != 2 {
		t.Fatalf("completions after re-advance = %d", len(c2.CompletionLog()))
	}
}

// 版本日志同时记录发起与取消。
func TestVersionLogContents(t *testing.T) {
	c := newTestCoordinator(t)
	ctx := context.Background()
	now := at(11, 0)
	_ = c.Register(ctx, "i", now)
	mustAcquire(t, c, "i", now, time.Hour) // 有活动会话，摘流不会立即完成
	_, _, _ = c.StartDrain(ctx, "i", now, time.Minute)
	_ = c.CancelDrain(ctx, "i", 1, now.Add(time.Second))
	_, _, _ = c.StartDrain(ctx, "i", now.Add(2*time.Second), time.Minute)

	log := c.VersionLog()
	want := []struct {
		ver int64
		t   VersionChangeType
	}{{1, ChangeDrainStarted}, {1, ChangeDrainCanceled}, {2, ChangeDrainStarted}}
	if len(log) != len(want) {
		t.Fatalf("log len = %d, want %d (%+v)", len(log), len(want), log)
	}
	for i, w := range want {
		if log[i].Version != w.ver || log[i].Type != w.t {
			t.Fatalf("log[%d] = %+v, want ver=%d type=%s", i, log[i], w.ver, w.t)
		}
	}
}
