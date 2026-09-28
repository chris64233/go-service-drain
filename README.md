# go-service-drain

服务实例平滑摘流（graceful drain）协调器：实例登记后可以领取带期限的会话租约；
发起摘流会递增版本并立即停止接新会话，已建立的会话在强制截止时间前可继续完成；
活动会话归零或到达截止时间后摘流完成，完成通知每轮恰好一条。

## 状态机

### 实例状态

```
            Register
              │
              ▼
        ┌──────────┐  StartDrain(版本+1)   ┌──────────┐
        │ serving  │ ───────────────────▶ │ draining │
        │ (接流中)  │ ◀─────────────────── │ (摘流中)  │
        └──────────┘    CancelDrain(版本匹配，截止前) └──────────┘
              │                                  │
              │                                  ├─ 活动会话归零 ─┐
              │                                  ├─ 租约过期归零  ├─▶ 完成（终态）
              │                                  └─ 到达 grace    │
              │                                     截止时间强制终止┘
              └──────────────────────────────────────┘
                         (drained，不可取消/不可再摘流)
```

- `serving`：正常接流，可领取新会话。
- `draining`：`StartDrain` 后进入，版本号递增，立即拒绝新会话；存量会话可继续。
- `drained`：摘流完成终态。活动会话自然归零（含租约过期回收）时正常完成；
  到达 `graceDeadline` 仍有会话时，这些会话被标记为 `terminated`（强制终止）后完成。
  终态不能被迟到的取消恢复，也不能再次摘流或领取会话。

### 会话租约状态

```
open ──CloseSession(版本+租约匹配)──▶ closed
open ──Advance: now >= deadline────▶ expired   （租约过期，被回收）
open ──Advance: now >= graceDeadline─▶ terminated（强制截止仍未关闭）
```

只有 `open → 非 open` 的迁移会让活动会话数减一，因此计数不会出现负数。

## 版本纪元（epoch）与乱序防护

- 每次 `StartDrain` 都让实例版本 `version` 加一；发起摘流时，所有存量活动会话
  被重标到新版本。
- `Heartbeat` / `CloseSession` / `CancelDrain` 必须携带实例**当前**版本，
  同时关闭/心跳还要携带领取时返回的租约 ID。
- 旧摘流轮次的迟到请求（旧版本号）一律返回 `ErrVersionMismatch`，
  不能减少新一轮摘流的计数，也不能把已摘除的实例恢复接流。
- 租约不匹配返回 `ErrLeaseMismatch`；重复或迟到的关闭/心跳命中
  非 `open` 会话时返回 `ErrSessionNotOpen`，不再改变计数。

## 取消摘流

- 截止前允许 `CancelDrain(instanceID, currentVersion)`：状态回到 `serving`、
  恢复领取新会话。取消不递增版本，但会在版本日志中记录 `drain_canceled`。
- 必须携带当前版本；版本不符、未在摘流中、或摘流已完成（含已进入强制终止）
  时分别返回 `ErrVersionMismatch` / `ErrNotDraining` / `ErrDrainCompleted`。

## 完成通知

- 活动会话归零的“最后一次关闭”与 `Advance` 超时推进可能并发；所有状态迁移在
  同一把互斥锁内串行、先持久化后提交，因此每轮摘流**恰好完成一次**，
  只产生一条 `Completion`（`forced=true` 表示强制截止完成，`Terminated`
  为被终止会话数）。
- 通知有两路：`Completions()` 通道（尽力投递，带缓冲）和
  `CompletionLog()`（随快照持久化，重启不丢、不重）。

## 操作 API

| 方法 | 说明 |
| --- | --- |
| `Register(instanceID, now)` | 登记实例，初始 `serving`、版本 0 |
| `AcquireSession(instanceID, now, ttl)` | 领取带期限的会话租约，返回 sessionID/leaseID/version；摘流中返回 `ErrDraining` |
| `Heartbeat(instanceID, sessionID, leaseID, version, now, ttl)` | 心跳续期，不能超过强制截止时间 |
| `CloseSession(instanceID, sessionID, leaseID, version, now)` | 正常关闭；若归零则当场完成摘流 |
| `StartDrain(instanceID, now, gracePeriod)` | 版本+1、停止接流，返回新版本与强制截止时间 |
| `CancelDrain(instanceID, version, now)` | 截止前携带当前版本取消，恢复接流 |
| `Advance(now)` | 推进逻辑时钟：回收过期租约、归零完成、强制截止终止 |
| `Status(instanceID)` | 查询状态、版本、活动数、截止时间等 |

## 持久化

- `Store` 接口：`Load` / `Save`；提供 `MemoryStore`（测试）与 `FileStore`
  （JSON，临时文件 + rename 原子落盘）。
- 快照包含全部实例会话（状态、租约、截止时间）、活动计数、版本日志
  （`drain_started` / `drain_canceled`）与摘流完成记录；重启后终态、
  版本护栏和“通知只发一次”的语义都保持不变。

## 测试

```sh
go test -race ./...
```

覆盖：基本摘流归零、重复/乱序/旧版本请求、取消后重新摘流的版本护栏、
租约过期、强制终止与单条通知、关闭与超时并发（200 会话竞态）、
空摘流立即完成、心跳截止截断、文件持久化恢复等。
