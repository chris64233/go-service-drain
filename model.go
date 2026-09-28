package goservicedrain

import "time"

// InstanceState 实例的生命周期状态。
type InstanceState string

const (
	// StateServing 正常接流：可以领取新会话。
	StateServing InstanceState = "serving"
	// StateDraining 摘流中：停止领取新会话，存量会话在截止时间前可继续完成。
	StateDraining InstanceState = "draining"
	// StateDrained 摘流完成：终态，不能被取消恢复，也不再接受会话操作。
	StateDrained InstanceState = "drained"
)

// SessionState 会话租约的状态。
type SessionState string

const (
	// SessionOpen 活动中，计入活动会话数。
	SessionOpen SessionState = "open"
	// SessionClosed 被正常关闭。
	SessionClosed SessionState = "closed"
	// SessionExpired 心跳租约过期，由协调器推进时间时回收。
	SessionExpired SessionState = "expired"
	// SessionTerminated 到达强制截止时间后仍未关闭，被强制终止。
	SessionTerminated SessionState = "terminated"
)

// VersionChangeType 版本变化类型。
type VersionChangeType string

const (
	// ChangeDrainStarted 发起摘流，版本递增。
	ChangeDrainStarted VersionChangeType = "drain_started"
	// ChangeDrainCanceled 摘流在截止前被取消，版本不变但记录一次变化。
	ChangeDrainCanceled VersionChangeType = "drain_canceled"
)

// Session 表示一个带期限的会话租约。
type Session struct {
	ID        string       `json:"id"`
	LeaseID   string       `json:"lease_id"`
	Version   int64        `json:"version"` // 领取/摘流重标时所属的版本纪元
	State     SessionState `json:"state"`
	Deadline  time.Time    `json:"deadline"` // 租约截止时间，心跳可续期
	CreatedAt time.Time    `json:"created_at"`
	ClosedAt  *time.Time   `json:"closed_at,omitempty"`
}

// VersionEvent 记录一次与版本相关的变化，随快照持久化。
type VersionEvent struct {
	InstanceID string            `json:"instance_id"`
	Version    int64             `json:"version"`
	Type       VersionChangeType `json:"type"`
	At         time.Time         `json:"at"`
}

// Completion 是摘流完成通知。每一轮摘流只产生一条。
type Completion struct {
	InstanceID  string    `json:"instance_id"`
	Version     int64     `json:"version"`
	CompletedAt time.Time `json:"completed_at"`
	// Terminated 到达强制截止时间仍未关闭、被强制终止的会话数。
	Terminated int `json:"terminated"`
	// Forced 为 true 表示因强制截止完成；false 表示活动会话自然归零完成。
	Forced bool `json:"forced"`
}

// Instance 是实例的持久化状态。
type Instance struct {
	ID          string              `json:"id"`
	State       InstanceState       `json:"state"`
	Version     int64               `json:"version"` // 当前版本纪元，每次发起摘流递增
	ActiveCount int                 `json:"active_count"`
	Sessions    map[string]*Session `json:"sessions,omitempty"`

	// 摘流轮次信息，仅 State == StateDraining/Drained 时有效。
	DrainStartedAt  *time.Time `json:"drain_started_at,omitempty"`
	GraceDeadline   *time.Time `json:"grace_deadline,omitempty"` // 强制截止时间
	ForceTerminated int        `json:"force_terminated,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
}

// Status 是状态查询的只读视图。
type Status struct {
	InstanceID      string
	State           InstanceState
	Version         int64
	ActiveCount     int
	Draining        bool
	DrainStartedAt  time.Time
	GraceDeadline   time.Time
	ForceTerminated int
	CompletedAt     time.Time
}

func (in *Instance) status() Status {
	s := Status{
		InstanceID:      in.ID,
		State:           in.State,
		Version:         in.Version,
		ActiveCount:     in.ActiveCount,
		Draining:        in.State == StateDraining,
		ForceTerminated: in.ForceTerminated,
	}
	if in.DrainStartedAt != nil {
		s.DrainStartedAt = *in.DrainStartedAt
	}
	if in.GraceDeadline != nil {
		s.GraceDeadline = *in.GraceDeadline
	}
	if in.CompletedAt != nil {
		s.CompletedAt = *in.CompletedAt
	}
	return s
}

// Snapshot 是持久化的完整状态。
type Snapshot struct {
	Instances map[string]*Instance `json:"instances"`
	// VersionLog 持久化版本变化历史。
	VersionLog []VersionEvent `json:"version_log,omitempty"`
	// Completions 已发出的摘流完成通知（保证重启后不重发、可回放）。
	Completions []Completion `json:"completions,omitempty"`
	NextSeq     int64        `json:"next_seq"`
}
