package goservicedrain

import "errors"

// 协调器返回的哨兵错误。调用方使用 errors.Is 判定。
var (
	// ErrInstanceNotFound 实例未登记。
	ErrInstanceNotFound = errors.New("instance not found")
	// ErrAlreadyRegistered 实例重复登记。
	ErrAlreadyRegistered = errors.New("instance already registered")
	// ErrDraining 实例正在摘流，不能领取新会话。
	ErrDraining = errors.New("instance is draining")
	// ErrSessionNotFound 会话不存在。
	ErrSessionNotFound = errors.New("session not found")
	// ErrLeaseMismatch 租约标识不匹配，拒绝操作。
	ErrLeaseMismatch = errors.New("lease id mismatch")
	// ErrVersionMismatch 操作携带的版本与实例当前版本不一致（旧摘流轮次的迟到请求）。
	ErrVersionMismatch = errors.New("version mismatch: stale request from an older drain epoch")
	// ErrSessionNotOpen 会话已经关闭、过期或被强制终止，重复请求不得再次改变计数。
	ErrSessionNotOpen = errors.New("session is not open")
	// ErrNotDraining 实例当前没有进行中的摘流（已完成、已取消或从未发起）。
	ErrNotDraining = errors.New("instance is not draining")
	// ErrDrainCompleted 摘流已完成，迟到的取消或会话操作不能改变终态。
	ErrDrainCompleted = errors.New("drain already completed")
	// ErrInvalidArgument 参数非法。
	ErrInvalidArgument = errors.New("invalid argument")
)
