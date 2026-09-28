# go-data-export

用于承载用户数据导出、分片生成与文件生命周期管理相关的 Go 服务代码。

开发环境：Go 1.23.0。

运行测试：

    go test ./...

## 概述

本服务提供**可从故障中恢复、保证一致快照语义**的用户数据导出：

- 创建任务时固定**数据快照水位**与**待生成分片集合**，之后不可变更；
- 工作者凭**租约**（含单调递增的围栏令牌）领取分片、上传文件并提交摘要回执；
- 全部分片回执校验通过后，系统在**同一事务内**一次性生成不可变清单并写入**唯一一条**完成通知（outbox）；
- 取消、过期、分片完成互相竞争时，任务**只有一个终态**；
- 过期清理不会移除仍被有效清单引用的对象。

## 核心概念

| 概念 | 说明 |
| --- | --- |
| `Task` | 一次导出任务。创建时固定 `Watermark`（快照水位）、`ShardCount`（分片集合）与 `ExecutionVersion`（执行版本）。状态机：`RUNNING → SUCCEEDED / CANCELLED`，`SUCCEEDED → EXPIRED`（清理后）。终态唯一且不可逆。 |
| `Shard` | 任务创建时固定的一个待生成分片。状态：`PENDING → LEASED → ACCEPTED`。 |
| `Lease` | 工作者领取分片获得的租约凭证，携带 `LeaseID` 与单调递增的 `FencingToken`。租约过期后分片可被重新领取，旧租约的回执会被围栏令牌拒绝。 |
| `Receipt` | 工作者上传分片后提交的摘要回执，按 `ReceiptID` 幂等。 |
| `Manifest` | 全部分片校验通过后一次性生成的不可变清单，摘要由水位、执行版本与全部分片摘要确定性计算。 |
| `OutboxEvent` | 与状态变更同事务写入的完成通知，每个任务至多一条，由外部派发器投递后调用 `MarkOutboxDispatched`。 |

## 接口

```go
svc := dataexport.NewService(store, objects, dataexport.Options{
    LeaseTTL:    time.Minute,   // 分片租约时长
    DownloadTTL: 24 * time.Hour, // 完成后清单可下载时长
})
```

| 方法 | 语义 |
| --- | --- |
| `CreateTask` | 固定快照水位与分片集合。同一外部请求号重放且范围一致时返回原任务（幂等）；同号改变范围或分片数报 `TASK_CONFLICT`。 |
| `ClaimShard` | 领取一个待生成分片，返回带围栏令牌的租约。任务已终结时报 `TASK_NOT_RUNNING`（取消后停止新领取）；无可领取分片报 `NO_SHARD_AVAILABLE`。 |
| `SubmitReceipt` | 校验执行版本、快照水位与租约后接受回执。重放同一回执返回原结果；旧租约 / 过期租约 / 版本或水位不匹配的回执被拒绝；与已接受内容不一致的回执报 `RECEIPT_CONFLICT`，不得覆盖。最后一个分片被接受时，同事务生成清单与完成通知。 |
| `CancelTask` | 取消任务（幂等）。与分片完成竞争时只有一个终态；已完成任务报 `TASK_NOT_RUNNING`。 |
| `GetManifest` | 读取清单。任务未完成报 `MANIFEST_NOT_READY`；超过下载期限报 `MANIFEST_EXPIRED`。 |
| `CleanupExpired` | 把下载期已过的任务迁移到 `EXPIRED` 并删除其对象；仍被有效清单引用的对象（如内容寻址去重共享的对象）一律保留。 |
| `PendingOutboxEvents` / `MarkOutboxDispatched` | 读取未派发的完成通知 / 标记已派发。 |
| `Recover` | 重启后修复崩溃残留：重新终结"分片齐但未完成"的任务、重新物化丢失的清单对象。幂等，可反复调用。 |

## 一致性设计

**事务化状态变更。** 所有"检查状态 + 多步写入"（接受回执 → 生成清单 → 写 outbox；取消；过期迁移）都在 `Store.Update` 的单个事务内完成，并发事务串行化。因此：

- 多个工作者同时完成最后几个分片，只会有一个事务观察到"全部接受且任务仍运行"，清单与完成通知**恰好一份**；
- 取消与最后回执竞争时，只有一个事务能把任务带离 `RUNNING`，**终态唯一**。

**幂等。** 任务按外部请求号去重（范围不一致即冲突）；回执按 `ReceiptID` 去重，重放返回原结果，同号不同内容报冲突；已接受分片不接受不一致摘要的覆盖。

**故障恢复。** 状态先落库、清单对象后物化。若在两者之间崩溃，`Recover`（或下次 `GetManifest`）会按持久化状态确定性重建清单对象；`Recover` 也会重新终结因崩溃停留在 `RUNNING` 但分片已齐的任务。清单摘要由水位与分片摘要确定性计算，重建结果与原清单一致。

**对象生命周期。** 清理分两步：先在事务内把过期任务迁移到 `EXPIRED` 并收集候选对象，再删除候选中**未被任何仍有效清单引用**的对象。已取消任务的残留上传对象也会被清理。

## 错误类型

所有错误均为 `*dataexport.Error`，携带稳定的 `Code`，用 `IsCode(err, code)` 判断：

| Code | 含义 |
| --- | --- |
| `INVALID_ARGUMENT` | 请求参数非法 |
| `TASK_NOT_FOUND` / `SHARD_NOT_FOUND` | 任务 / 分片不存在 |
| `TASK_CONFLICT` | 同一外部请求号提交了不同的范围或分片数 |
| `TASK_NOT_RUNNING` | 任务已终结，拒绝领取 / 回执 / 取消 |
| `NO_SHARD_AVAILABLE` | 当前无可领取分片 |
| `LEASE_MISMATCH` / `LEASE_EXPIRED` | 旧租约（围栏令牌不符）/ 租约已过期 |
| `VERSION_MISMATCH` / `WATERMARK_MISMATCH` | 回执的执行版本 / 快照水位与任务固定值不一致 |
| `RECEIPT_CONFLICT` | 回执与已接受内容冲突，拒绝覆盖 |
| `MANIFEST_NOT_READY` / `MANIFEST_EXPIRED` | 清单未生成 / 下载期已过 |
| `OBJECT_STORE_FAILURE` | 对象存储读写失败 |

## 持久化与对象存储抽象

- `Store`：持久化状态（任务、分片、回执、清单、outbox）的事务抽象。`MemStore` 是内存实现，以 copy-on-write 提供事务与串行化语义；接入真实数据库时实现同一接口即可，领域逻辑不变。
- `ObjectStore`：分片文件与清单对象的存储抽象（`MemObjectStore` 为内存实现）。对象按不可变内容写入，同一键重复写相同内容必须幂等。

## 测试

`service_test.go` 覆盖：创建幂等与冲突、租约与围栏令牌、回执校验 / 重放 / 防覆盖、并发完成最后分片只产生一份清单与一条通知、取消与完成的竞争终态唯一、过期下载拒绝、清理保护被引用对象、崩溃恢复与 outbox 派发流程。测试使用可注入的时钟与 ID 生成器，并以 `-race` 验证并发安全。
