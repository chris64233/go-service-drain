# go-service-drain

服务实例平滑摘流（graceful drain）协调器：实例登记后领取**带期限的会话租约**，发起摘流时生成**单调递增版本**并立即停止接新流，已建立的会话在截止时间前继续完成；活动会话归零自动完成摘流，到达强制截止时间后仍未关闭的会话被终止。关闭、心跳、租约过期等消息允许重复或乱序到达，只有匹配当前版本与有效租约的操作才能改变活动会话计数。

## 状态机

每个实例在登记后经历三个状态：

```
                BeginDrain(v+1)                 活动会话全部关闭 / 租约过期
   ┌────────┐ ───────────────────▶ ┌──────────┐ ─────────────────────────────▶ ┌─────────┐
   │ active │                      │ draining │   或到达强制截止时间（终止剩余会话）│ drained │
   └────────┘ ◀─────────────────── └──────────┘                                  └─────────┘
                  CancelDrain(v)        （终止态：永不接流、不可取消）
```

| 状态 | 能否领取新会话 | 含义 |
| --- | --- | --- |
| `active` | 可以 | 正常接流，当前版本下持有活动会话 |
| `draining` | 不可以（`ErrInstanceDraining`） | 摘流进行中；存量会话被新版本"认领"，可在强制截止时间前自然关闭 |
| `drained` | 永远不可以（`ErrInstanceDrained`） | 摘流已完成（自然完成或强制终止），迟到取消不能恢复 |

### 版本（generation）与消息有效性

- 实例登记时版本为 `0`；每次 `BeginDrain` 生成 `v+1`，每次 `CancelDrain` 不回退版本（重新发起摘流继续递增），保证版本号全局单调。
- 发起摘流时，实例上所有在途会话的归属版本被原子改写为新版本。此后携带旧版本的关闭/心跳一律返回 `ErrVersionMismatch`，**既不会减少新版本计数，也不会延长旧租约**。
- 会话操作（心跳/关闭）必须同时满足：①租约仍存在（未关闭/未过期/未终止）；②携带版本等于实例当前版本。重复消息因租约已删除返回 `ErrLeaseUnknown`，不产生任何计数变化，因此计数不可能为负。

### 摘流完成与并发安全

- 活动会话数归零的瞬间，关闭事件与 `drain_completed`（`reason=drained`）在**同一个持久化批次**内提交。
- 到达强制截止时间后推进超时：尚未关闭的会话写入 `sessions_terminated` 记录，随后产生且仅产生一条 `drain_completed`（`reason=forced`）通知；租约自身先到期的会话记为自然过期，不会被重复记为终止。
- 最后一次自然关闭与超时推进并发时，互斥锁 + 状态守卫保证：只完成一次、无负数计数、终止列表无重复。`Completions()` 可读取全部完成通知（每次摘流至多一条）。
- `CancelDrain` 必须携带当前版本：`active` 状态返回 `ErrNoDrainInProgress`，版本不符返回 `ErrVersionMismatch`，已 `drained`（含强制终止完成）返回 `ErrInstanceDrained`——迟到取消无法让已摘除实例重新接流。

## API

| 方法 | 说明 |
| --- | --- |
| `Register(instanceID)` | 登记实例（active，版本 0） |
| `AcquireSession(instanceID, ttl)` | 领取带期限的会话租约，返回 `Lease{SessionID, Version, Deadline}` |
| `Heartbeat(instanceID, sessionID, version, ttl)` | 续租；版本/租约不匹配则拒绝 |
| `CloseSession(instanceID, sessionID, version)` | 关闭会话；可能同时自然完成摘流 |
| `BeginDrain(instanceID, forceAfter)` | 发起摘流，返回新版本与强制截止时间；无会话时立即完成 |
| `CancelDrain(instanceID, version)` | 截止前取消摘流并恢复接流 |
| `ProcessTimeouts()` | 按协调器时钟推进：租约过期、强制终止 |
| `Status(instanceID)` | 查询状态、版本、活动会话数及租约明细 |

## 持久化

所有改变状态的事实先追加写入只增的事件日志（Journal），再应用到内存；内存状态完全由重放恢复，无需快照：

```
instance_registered / lease_acquired / session_heartbeat
session_closed / session_expired
drain_began（含被认领会话→新版本的映射）/ drain_canceled
sessions_terminated / drain_completed
```

- `FileJournal`：每行一个 JSON 事件，按批次缓冲并 `fsync` 落盘；`RecoverCoordinator(path)` 打开日志、重放后继续追加。
- `NewEphemeral(clock)`：纯内存实现，配合 `FakeClock` 可确定性地驱动截止时间。

生产使用示例：

```go
coord, journal, err := goservicedrain.RecoverCoordinator("/var/lib/drain/journal.log")
if err != nil { log.Fatal(err) }
defer journal.Close()

if err := coord.Register("10.0.0.1:8080"); err != nil { log.Fatal(err) }

lease, err := coord.AcquireSession("10.0.0.1:8080", 30*time.Second)
// ... 业务会话期间周期性 Heartbeat(instance, lease.SessionID, lease.Version, ttl)

// 运维触发摘流：立即不接新流，60s 后强制终止
version, deadline, err := coord.BeginDrain("10.0.0.1:8080", 60*time.Second)

// 定时推进（或由独立 goroutine ticker 调用）
if err := coord.ProcessTimeouts(); err != nil { log.Print(err) }

// 截止前撤销摘流（需携带当前版本）
_ = coord.CancelDrain("10.0.0.1:8080", version)
```

## 测试

```
go test -race -count=1 ./...
```

覆盖：正常生命周期、重复/乱序/旧版本消息隔离（含关闭后迟到心跳、活跃实例租约过期）、租约过期自然完成、强制截止终止且只通知一次、取消与重复摘流版本递增、文件日志重放恢复，以及 200 轮「最后关闭 × 超时推进」并发压力（`-race`）。
