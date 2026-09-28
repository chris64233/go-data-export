package dataexport

import "time"

// TaskStatus 描述导出任务的生命周期状态。
type TaskStatus string

const (
	// StatusPending 任务已创建，等待 / 正在进行分片导出。
	StatusPending TaskStatus = "pending"
	// StatusCompleted 所有分片属于同一快照水位且摘要校验通过，清单已不可变地生成。
	StatusCompleted TaskStatus = "completed"
	// StatusCanceled 任务被用户或系统取消，停止新领取，永不再生成清单。
	StatusCanceled TaskStatus = "canceled"
	// StatusExpired 任务超过处理截止时间仍未完成，进入过期终态。
	StatusExpired TaskStatus = "expired"
)

// Terminal 报告状态是否为不可再迁移的终态。
func (s TaskStatus) Terminal() bool {
	return s == StatusCompleted || s == StatusCanceled || s == StatusExpired
}

// Scope 描述一次导出的数据范围。RequestID 与 Scope 一起决定幂等结果：
// 同一 RequestID 重复提交相同 Scope 返回同一个任务；Scope 不同则报冲突。
type Scope struct {
	// UserID 被导出数据的归属用户。
	UserID string `json:"user_id"`
	// Resource 导出的资源类别（如 "messages"、"files"）；空串表示用户全量数据。
	Resource string `json:"resource,omitempty"`
	// Start / End 是导出范围的逻辑边界（例如数据版本号或毫秒时间戳）。
	Start int64 `json:"start,omitempty"`
	End   int64 `json:"end,omitempty"`
}

func (s Scope) equals(o Scope) bool {
	return s.UserID == o.UserID && s.Resource == o.Resource && s.Start == o.Start && s.End == o.End
}

// ShardSpec 是任务创建时固定下来的待生成分片。
type ShardSpec struct {
	// Index 是分片在任务内的稳定序号（从 0 起）。
	Index int `json:"index"`
	// Watermark 是该分片必须对应的快照水位；任务要求所有分片等于任务水位。
	Watermark int64 `json:"watermark"`
	// ExpectedDigest 是该分片在快照水位上导出内容的期望摘要（十六进制）。
	// 回执接受时会与对象存储中对象的实际摘要逐字节比对。
	ExpectedDigest string `json:"expected_digest"`
	// Size 是该分片在快照水位上的期望字节数，仅用于清单与校验提示。
	Size int64 `json:"size,omitempty"`
}

// ShardStatus 描述单个分片的状态。
type ShardStatus string

const (
	// ShardPending 分片尚未被任何工作者完成。
	ShardPending ShardStatus = "pending"
	// ShardDone 分片回执已被接受，内容不可再被覆盖。
	ShardDone ShardStatus = "done"
)

// Shard 是单个分片的持久化状态。
type Shard struct {
	TaskID string      `json:"task_id"`
	Index  int         `json:"index"`
	Status ShardStatus `json:"status"`
	// 以下规格字段在任务创建时由 ShardSpec 固定、永不可变：
	// 它们把分片绑定到任务的快照水位，使回执接受与清单生成可以独立复核。
	Watermark      int64    `json:"watermark"`
	ExpectedDigest string   `json:"expected_digest"`
	ExpectedSize   int64    `json:"expected_size,omitempty"`
	Lease          *Lease   `json:"lease,omitempty"`
	Receipt        *Receipt `json:"receipt,omitempty"`
}

// Lease 是工作者领取分片时获得的租约。
type Lease struct {
	// Generation 是租约代际，每次领取单调递增；回执必须携带当前代际。
	Generation int64 `json:"generation"`
	// WorkerID 持有租约的工作者。
	WorkerID string `json:"worker_id"`
	// ExpiresAt 租约到期时刻；到期后分片可以被其他工作者重新领取。
	ExpiresAt time.Time `json:"expires_at"`
}

// activeAt 报告租约在时刻 now 是否仍然有效。
func (l *Lease) activeAt(now time.Time) bool {
	return l != nil && now.Before(l.ExpiresAt)
}

// Receipt 是工作者上传分片内容后提交的摘要回执（持久化形态）。
type Receipt struct {
	// ExecutionVersion 回执必须匹配任务当前执行版本。
	ExecutionVersion int64 `json:"execution_version"`
	// LeaseGeneration 回执必须匹配分片当前租约代际。
	LeaseGeneration int64 `json:"lease_generation"`
	// WorkerID 提交回执的工作者。
	WorkerID string `json:"worker_id"`
	// ObjectKey 上传对象在对象存储中的键。
	ObjectKey string `json:"object_key"`
	// Digest 上传对象的实际摘要（十六进制），必须与 ShardSpec.ExpectedDigest 一致。
	Digest string `json:"digest"`
	// Size 上传对象字节数。
	Size int64 `json:"size"`
	// AcceptedAt 回执被接受的时刻。
	AcceptedAt time.Time `json:"accepted_at"`
}

// Task 是导出任务的持久化聚合根。
type Task struct {
	ID string `json:"id"`
	// RequestID 是外部请求幂等号。
	RequestID string `json:"request_id"`
	Scope     Scope  `json:"scope"`

	// Watermark 是创建任务时固定的快照水位。所有分片必须属于该水位，
	// 任务完成后生成的清单也不可变地绑定该水位。
	Watermark int64 `json:"watermark"`
	// ExecutionVersion 是任务当前执行版本；任何导致执行轮次失效的操作
	// （本实现中为取消）都会使其递增，旧版本的回执一律拒绝。
	ExecutionVersion int64 `json:"execution_version"`

	Status TaskStatus `json:"status"`

	// Deadline 是任务处理截止时间；超过仍未完成则可被过期扫描终结。
	Deadline time.Time `json:"deadline"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// CompletedAt / CanceledAt / ExpiredAt 记录各终态落定时刻。
	CompletedAt time.Time `json:"completed_at,omitempty"`
	CanceledAt  time.Time `json:"canceled_at,omitempty"`
	ExpiredAt   time.Time `json:"expired_at,omitempty"`
}

// ManifestEntry 是清单中的一个分片条目。
type ManifestEntry struct {
	Index     int    `json:"index"`
	ObjectKey string `json:"object_key"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	// WorkerID 记录实际产出该分片的工作者，便于审计。
	WorkerID string `json:"worker_id"`
}

// Manifest 是任务完成时一次性生成的不可变清单。
type Manifest struct {
	TaskID string `json:"task_id"`
	// Watermark 清单绑定的快照水位，与任务水位一致。
	Watermark int64           `json:"watermark"`
	Entries   []ManifestEntry `json:"entries"`
	CreatedAt time.Time       `json:"created_at"`
	// RetainUntil 清单（及其对象）的可下载保留截止时间；过期后下载被拒绝、对象可清理。
	RetainUntil time.Time `json:"retain_until"`
}

// expiredAt 报告清单在时刻 now 是否已超过保留期。
func (m *Manifest) expiredAt(now time.Time) bool {
	return now.After(m.RetainUntil)
}

// OutboxEvent 是与状态变更在同一事务中写入的出站通知（transactional outbox）。
// 投递由独立的 OutboxDispatcher 至少一次地完成；同 EventID 的事件内容永不改变，
// 投递端可据此去重。
type OutboxEvent struct {
	EventID string `json:"event_id"`
	TaskID  string `json:"task_id"`
	// Type 目前为 "task.completed" / "task.canceled" / "task.expired"。
	Type      string            `json:"type"`
	Payload   map[string]string `json:"payload,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
}

// Outbox 事件类型。
const (
	EventTaskCompleted = "task.completed"
	EventTaskCanceled  = "task.canceled"
	EventTaskExpired   = "task.expired"
)
