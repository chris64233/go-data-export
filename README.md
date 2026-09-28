# go-data-export

从故障中恢复、并保证**一致快照语义**的用户数据导出服务（Go 1.23，零第三方依赖）。

工作者模型：创建任务时固定快照水位与待生成分片集合；多个工作者凭**租约**并行领取
分片、上传对象并提交**摘要回执**；所有分片都属于同一水位且摘要校验通过后，系统
**一次性**生成不可变清单并发出一条完成通知。取消、过期、分片完成并发竞争时只能有
一个终态。

## 一致性模型（设计要点）

### 1. 快照水位与幂等创建

- 创建任务时同时固定：**快照水位 `watermark`**、**待生成分片集合**（每片绑定该水位
  与该水位上的期望摘要 SHA-256）、处理截止时间。
- **外部请求号 `X-Request-Id` 与导出范围 `scope` 共同决定幂等结果**：
  - 同号 + 完全相同的范围重复请求 → 返回同一个任务，不产生任何副作用；
  - 同号但范围（用户 / 资源 / 起止边界）变化 → `request_conflict`（HTTP 409），
    原任务不受影响。
- 分片规格在创建后不可变，因此"摘要校验通过"即等价于"对象内容属于该快照水位"。

### 2. 租约与三重校验的回执

- 工作者领取分片获得带 `expires_at` 的租约与单调递增的 `generation`（代际）。
  - 租约到期的分片可被重新领取，代际 +1，旧租约立即失效（容错：工作者宕机）。
  - 任务**取消后停止一切新领取**。
- 回执必须同时匹配：
  1. **任务当前执行版本** `execution_version`（取消 / 过期会使版本递增），不符 →
     `execution_version_mismatch`；
  2. **分片当前租约**：代际不符 / 非本人 → `lease_stale`，租约到期 →
     `lease_expired`；
  3. **对象摘要**：对象存储服务端计算的 SHA-256 必须等于创建时固定的期望摘要，
     不符 → `digest_mismatch`；对象不存在 → `object_not_found`。
- **重放同一回执返回原结果**（`accepted=false`，回原回执与清单），不产生重复通知。
- **旧租约 / 不同摘要 / 不同对象的回执不能覆盖已接受内容** →
  `shard_already_completed`。

### 3. 单清单、单终态

- 最后一个分片被接受时，在**同一个事务**内：复核所有分片属于同一快照水位且摘要
  齐备 → 一次性写入**不可变清单**、置任务 `completed`、写入一条
  `task.completed` outbox 通知。
- 多个工作者并发提交最后分片时，存储事务的乐观并发控制保证只有一个提交能观察到
  "全部完成"——**只有一个清单、一条完成通知**（清单重复写入会直接 panic，作为
  不变量的最后防线）。
- 取消 / 过期 / 完成互相竞争同样只有一个终态：后提交方收到 `task_terminal`。
- outbox 事件 ID 确定性生成（`<task_id>/<event_type>`），通知至少投递一次，
  消费端按事件 ID 幂等。

### 4. 过期、下载与清理

- 任务超过处理截止时间仍未完成 → 过期扫描终结为 `expired`（一条
  `task.expired` 通知）；终结后在途回执一律拒绝。
- 清单有独立**保留期**：超过保留期后读取清单 / 下载对象 →
  `manifest_expired`（HTTP 410），过期下载被拒绝。
- 清理只删除：保留期已过清单引用的对象、以及已终结但永不入清单任务的孤儿回执
  对象。**任何仍被保留期内有效清单引用的对象都不会被删除**（计入
  `skipped_referenced`）；pending 任务的在途对象、无法关联归属的对象保持不动。

## 代码结构

| 文件 | 内容 |
| --- | --- |
| `model.go` | 任务 / 分片 / 租约 / 回执 / 清单 / outbox 事件模型 |
| `errors.go` | 稳定错误码、哨兵错误与 `errors.Is` 支持 |
| `object_store.go` | S3 风格对象存储抽象（服务端计算 SHA-256） |
| `store.go` | `PersistentStore` 接口 + 可串行化语义的内存实现（快照 + 乐观版本号重试） |
| `service.go` | 全部业务规则（创建 / 领取 / 回执 / 取消 / 过期 / 清单 / 清理） |
| `outbox.go` | outbox 轮询投递器（至少一次，故障重启不丢事件） |
| `http.go` | REST 接口与错误码 → HTTP 状态码映射 |
| `cmd/dataexportd/main.go` | 组装服务的可运行示例 |

生产环境只需把 `PersistentStore` 换为数据库实现（任务表 / 分片表 / outbox 表 +
`request_id` 唯一索引，事务提交语义与内存实现一致），`ObjectStore` 换为 S3/OSS。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/v1/tasks` | 创建任务（幂等头 `X-Request-Id`） |
| `GET` | `/v1/tasks/{id}` | 读取任务状态 |
| `POST` | `/v1/tasks/{id}/cancel` | 取消任务（幂等） |
| `POST` | `/v1/tasks/{id}/claims` | 工作者领取分片（`X-Worker-Id`） |
| `POST` | `/v1/tasks/{id}/shards/{idx}/receipts` | 提交摘要回执 |
| `GET` | `/v1/tasks/{id}/manifest` | 读取不可变清单 |
| `GET` | `/v1/tasks/{id}/shards/{idx}/object` | 下载分片对象（保留期内） |
| `POST` | `/v1/maintenance/expire` | 过期扫描 |
| `POST` | `/v1/maintenance/cleanup` | 过期对象清理（可带 `candidate_keys`） |

典型工作流程：

```bash
# 1) 创建任务（watermark 与每片期望摘要在创建时固定）
curl -sS -X POST localhost:8080/v1/tasks \
  -H 'X-Request-Id: req-20260101-0001' -H 'Content-Type: application/json' \
  -d '{
    "scope": {"user_id":"u-1","resource":"messages","start":100,"end":200},
    "watermark": 42,
    "shards": [
      {"index":0,"watermark":42,"expected_digest":"<sha256-hex>"},
      {"index":1,"watermark":42,"expected_digest":"<sha256-hex>"}
    ]
  }'

# 2) 工作者领取分片
curl -sS -X POST localhost:8080/v1/tasks/<task-id>/claims -H 'X-Worker-Id: worker-7'

# 3) 工作者把对象上传到对象存储后，提交回执（回带执行版本与租约代际）
curl -sS -X POST localhost:8080/v1/tasks/<task-id>/shards/0/receipts \
  -H 'X-Worker-Id: worker-7' -H 'Content-Type: application/json' \
  -d '{"execution_version":1,"lease_generation":1,"object_key":"objects/<task-id>/shard-0.bin"}'

# 4) 最后一片回执返回 completed=true 后读取清单
curl -sS localhost:8080/v1/tasks/<task-id>/manifest
```

错误响应统一为：

```json
{"error": {"code": "lease_stale", "message": "lease is stale: ..."}}
```

主要错误码：`invalid_argument`、`not_found`、`request_conflict`、
`execution_version_mismatch`、`lease_stale`、`lease_expired`、
`no_shard_available`、`digest_mismatch`、`snapshot_watermark_mismatch`、
`object_not_found`、`shard_already_completed`、`task_canceled`、`task_expired`、
`task_completed`、`task_terminal`、`manifest_not_found`、`manifest_expired`。

## 作为库使用

```go
store   := dataexport.NewMemoryStore(clock.Now)
objects := dataexport.NewObjectStore()
svc     := dataexport.NewService(store, objects, dataexport.DefaultConfig(), clock.Now)

task, _ := svc.CreateTask(dataexport.CreateTaskInput{
    RequestID: "req-1",
    Scope:     dataexport.Scope{UserID: "u-1"},
    Watermark: 42,
    Shards:    []dataexport.ShardSpec{{Index: 0, Watermark: 42, ExpectedDigest: hex}},
})
claim, _ := svc.ClaimShardOf(task.ID, "worker-1")
key := "objects/" + task.ID + "/shard-0.bin"
objects.PutObject(key, content, clock.Now()) // 返回服务端计算的 SHA-256
res, err := svc.SubmitReceipt(dataexport.ReceiptInput{
    TaskID: task.ID, Index: claim.Index,
    ExecutionVersion: claim.ExecutionVersion,
    LeaseGeneration:  claim.Lease.Generation,
    WorkerID:         "worker-1", ObjectKey: key,
})
// res.Completed == true 时 res.Manifest 即不可变清单
```

## 开发

开发环境：Go 1.23.0。

```bash
go test -race ./...        # 全部测试（含并发竞争、取消/完成竞速、租约与清理）
go test -run TestHTTP ./...
go vet ./...
```

测试覆盖：请求幂等与同号改范围冲突、分片水位校验、租约领取/超时重领/旧租拒绝、
执行版本不符、摘要不符与对象缺失、回执重放与不可覆盖、16 工作者并发完成只有一个
清单与一条通知、30 轮取消×完成竞速只有一个终态、取消后停领、任务过期、清单过期
拒绝下载、清理保护有效清单引用对象 / 不删在途与未知对象、outbox 失败重试与 HTTP
端到端状态码。
