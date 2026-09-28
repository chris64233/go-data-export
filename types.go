package dataexport

import "time"

// ExportRange 是导出范围，按用户 ID 游标闭区间表示。
// 外部请求号 + 导出范围共同决定任务幂等结果。
type ExportRange struct {
	FromID string
	ToID   string
}

// TaskStatus 是任务的终态机状态。终态（Succeeded/Cancelled/Expired）唯一且不可逆。
type TaskStatus string

const (
	TaskStatusRunning   TaskStatus = "RUNNING"
	TaskStatusSucceeded TaskStatus = "SUCCEEDED"
	TaskStatusCancelled TaskStatus = "CANCELLED"
	TaskStatusExpired   TaskStatus = "EXPIRED" // 已完成但下载期已过，对象已被清理
)

// Task 是一次导出任务。创建时固定快照水位与分片集合，之后不可变更。
type Task struct {
	ID                string
	ExternalRequestID string
	Range             ExportRange
	Watermark         int64 // 创建时固定的数据快照水位
	ShardCount        int
	ExecutionVersion  int64 // 任务执行版本，回执必须匹配
	Status            TaskStatus
	ManifestKey       string
	CreatedAt         time.Time
	CompletedAt       *time.Time
	DownloadExpiresAt *time.Time
}

// ShardStatus 是分片状态。
type ShardStatus string

const (
	ShardStatusPending  ShardStatus = "PENDING"
	ShardStatusLeased   ShardStatus = "LEASED"
	ShardStatusAccepted ShardStatus = "ACCEPTED"
)

// Shard 是任务创建时固定的一个待生成分片。
type Shard struct {
	TaskID         string
	Index          int
	Status         ShardStatus
	LeaseID        string
	FencingToken   int64 // 每次成功领取单调递增，用于拒绝旧租约回执
	LeaseExpiresAt time.Time
	Digest         string
	ObjectKey      string
	ReceiptID      string
	AcceptedAt     *time.Time
}

// Lease 是工作者领取分片后获得的租约凭证。
type Lease struct {
	TaskID           string
	ShardIndex       int
	LeaseID          string
	FencingToken     int64
	ExecutionVersion int64
	Watermark        int64
	ExpiresAt        time.Time
}

// Receipt 是已接受回执的持久化记录，用于幂等重放。
type Receipt struct {
	ReceiptID     string
	TaskID        string
	ShardIndex    int
	Digest        string
	ObjectKey     string
	TaskCompleted bool // 接受该回执时任务是否因此完成
	AcceptedAt    time.Time
}

// ManifestFile 是清单中的一个分片文件条目。
type ManifestFile struct {
	ShardIndex int
	ObjectKey  string
	Digest     string
}

// Manifest 是全部分片校验通过后一次性生成的不可变清单。
type Manifest struct {
	TaskID           string
	Watermark        int64
	ExecutionVersion int64
	Files            []ManifestFile
	Digest           string // 清单内容摘要，由水位、版本与全部分片摘要确定性计算
	ObjectKey        string
	CreatedAt        time.Time
	ExpiresAt        time.Time
}

// OutboxEventType 是 outbox 事件类型。
type OutboxEventType string

const (
	// OutboxEventTaskCompleted 是任务完成通知，每个任务至多产生一条。
	OutboxEventTaskCompleted OutboxEventType = "export.task_completed"
)

// OutboxEvent 与状态变更在同一事务内写入，保证完成通知不丢不重。
type OutboxEvent struct {
	ID           string
	Type         OutboxEventType
	TaskID       string
	ManifestKey  string
	ManifestHash string
	Watermark    int64
	FileCount    int
	CreatedAt    time.Time
	DispatchedAt *time.Time
}
