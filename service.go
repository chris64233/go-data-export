package dataexport

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Options 是服务的可调参数，均带默认值。
type Options struct {
	LeaseTTL    time.Duration // 分片租约时长，默认 1 分钟
	DownloadTTL time.Duration // 完成后清单可下载时长，默认 24 小时

	// 以下为可注入的依赖，便于测试与故障演练。
	Now               func() time.Time // 时钟，默认 time.Now
	NewID             func() string    // ID 生成器，默认随机 hex
	SnapshotWatermark func() int64     // 创建任务时固定的快照水位，默认 UnixNano
}

func (o *Options) withDefaults() {
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = time.Minute
	}
	if o.DownloadTTL <= 0 {
		o.DownloadTTL = 24 * time.Hour
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.NewID == nil {
		o.NewID = func() string {
			var b [16]byte
			_, _ = rand.Read(b[:])
			return hex.EncodeToString(b[:])
		}
	}
	if o.SnapshotWatermark == nil {
		o.SnapshotWatermark = func() int64 { return o.Now().UnixNano() }
	}
}

// Service 提供一致快照语义的导出任务管理。
type Service struct {
	store   Store
	objects ObjectStore
	opts    Options
}

func NewService(store Store, objects ObjectStore, opts Options) *Service {
	opts.withDefaults()
	return &Service{store: store, objects: objects, opts: opts}
}

// CreateTaskRequest 是创建导出任务的请求。
type CreateTaskRequest struct {
	ExternalRequestID string      // 外部请求号，幂等键
	Range             ExportRange // 导出范围，与请求号共同决定幂等结果
	ShardCount        int         // 分片数，创建后固定
	Watermark         int64       // 可选；为 0 时由服务固定当前快照水位
}

// CreateTaskResult 是创建结果。Created=false 表示命中幂等重放。
type CreateTaskResult struct {
	Task    *Task
	Created bool
}

// CreateTask 创建导出任务，固定快照水位与分片集合。
// 同一外部请求号重放且范围一致时返回原任务；范围或分片数不同则报 TASK_CONFLICT。
func (s *Service) CreateTask(ctx context.Context, req CreateTaskRequest) (*CreateTaskResult, error) {
	if req.ExternalRequestID == "" {
		return nil, errorf(CodeInvalidArgument, "external request id is required")
	}
	if req.Range.FromID == "" || req.Range.ToID == "" || req.Range.FromID > req.Range.ToID {
		return nil, errorf(CodeInvalidArgument, "invalid export range %+v", req.Range)
	}
	if req.ShardCount <= 0 {
		return nil, errorf(CodeInvalidArgument, "shard count must be positive")
	}

	result := &CreateTaskResult{}
	err := s.store.Update(ctx, func(tx *Tx) error {
		if existing := tx.GetTaskByRequestID(req.ExternalRequestID); existing != nil {
			if existing.Range != req.Range || existing.ShardCount != req.ShardCount {
				return errorf(CodeTaskConflict,
					"request id %q already used with range %+v and %d shards",
					req.ExternalRequestID, existing.Range, existing.ShardCount)
			}
			result.Task = existing
			result.Created = false
			return nil
		}

		watermark := req.Watermark
		if watermark == 0 {
			watermark = s.opts.SnapshotWatermark()
		}
		now := s.opts.Now()
		task := &Task{
			ID:                s.opts.NewID(),
			ExternalRequestID: req.ExternalRequestID,
			Range:             req.Range,
			Watermark:         watermark,
			ShardCount:        req.ShardCount,
			ExecutionVersion:  1,
			Status:            TaskStatusRunning,
			CreatedAt:         now,
		}
		tx.PutTask(task)
		for i := 0; i < req.ShardCount; i++ {
			tx.PutShard(&Shard{TaskID: task.ID, Index: i, Status: ShardStatusPending})
		}
		result.Task = task
		result.Created = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetTask 读取任务当前状态。
func (s *Service) GetTask(ctx context.Context, taskID string) (*Task, error) {
	var task *Task
	err := s.store.View(ctx, func(tx *Tx) error {
		task = tx.GetTask(taskID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, errorf(CodeTaskNotFound, "task %q not found", taskID)
	}
	return task, nil
}

// ClaimShard 为工作者领取一个待生成分片，返回带围栏令牌的租约。
// 已取消或已终结的任务拒绝新领取；没有可领取分片时返回 NO_SHARD_AVAILABLE。
func (s *Service) ClaimShard(ctx context.Context, taskID, workerID string) (*Lease, error) {
	var lease *Lease
	err := s.store.Update(ctx, func(tx *Tx) error {
		task := tx.GetTask(taskID)
		if task == nil {
			return errorf(CodeTaskNotFound, "task %q not found", taskID)
		}
		if task.Status != TaskStatusRunning {
			return errorf(CodeTaskNotRunning, "task %q is %s, no new claims", taskID, task.Status)
		}

		now := s.opts.Now()
		for _, shard := range tx.ListShards(taskID) {
			claimable := shard.Status == ShardStatusPending ||
				(shard.Status == ShardStatusLeased && !shard.LeaseExpiresAt.After(now))
			if !claimable {
				continue
			}
			shard.Status = ShardStatusLeased
			shard.LeaseID = s.opts.NewID()
			shard.FencingToken++
			shard.LeaseExpiresAt = now.Add(s.opts.LeaseTTL)
			tx.PutShard(shard)
			lease = &Lease{
				TaskID:           taskID,
				ShardIndex:       shard.Index,
				LeaseID:          shard.LeaseID,
				FencingToken:     shard.FencingToken,
				ExecutionVersion: task.ExecutionVersion,
				Watermark:        task.Watermark,
				ExpiresAt:        shard.LeaseExpiresAt,
			}
			return nil
		}
		return errorf(CodeNoShardAvailable, "task %q has no claimable shard", taskID)
	})
	if err != nil {
		return nil, err
	}
	return lease, nil
}

// SubmitReceiptRequest 是工作者上传的分片摘要回执。
type SubmitReceiptRequest struct {
	TaskID           string
	ShardIndex       int
	ReceiptID        string // 工作者侧幂等键
	LeaseID          string // 必须匹配分片当前租约
	FencingToken     int64
	ExecutionVersion int64  // 必须匹配任务当前执行版本
	Watermark        int64  // 必须匹配任务固定快照水位
	Digest           string // 分片内容摘要
	ObjectKey        string // 分片对象键
}

// ReceiptResult 是回执处理结果。Duplicate=true 表示这是对已接受回执的重放。
type ReceiptResult struct {
	ReceiptID     string
	ShardIndex    int
	Duplicate     bool
	TaskCompleted bool // 接受该回执是否使任务完成（重放时返回原值）
}

// SubmitReceipt 接受分片摘要回执。
//
// 幂等语义：同一 ReceiptID 重放相同内容返回原结果；同一 ReceiptID 提交
// 不同内容、或对已接受分片提交不一致摘要，均报 RECEIPT_CONFLICT 且不覆盖
// 已接受内容。旧租约（LEASE_MISMATCH）、过期租约（LEASE_EXPIRED）、
// 版本或水位不匹配的回执一律拒绝。
//
// 当这是最后一个分片时，在同一事务内生成不可变清单并写入唯一一条完成通知。
func (s *Service) SubmitReceipt(ctx context.Context, req SubmitReceiptRequest) (*ReceiptResult, error) {
	if req.ReceiptID == "" || req.Digest == "" || req.ObjectKey == "" {
		return nil, errorf(CodeInvalidArgument, "receipt id, digest and object key are required")
	}

	result := &ReceiptResult{ReceiptID: req.ReceiptID, ShardIndex: req.ShardIndex}
	finalize := false
	err := s.store.Update(ctx, func(tx *Tx) error {
		task := tx.GetTask(req.TaskID)
		if task == nil {
			return errorf(CodeTaskNotFound, "task %q not found", req.TaskID)
		}

		// 1. 回执号幂等：重放返回原结果，改内容报冲突。
		if prev := tx.GetReceipt(req.TaskID, req.ReceiptID); prev != nil {
			if prev.ShardIndex == req.ShardIndex && prev.Digest == req.Digest && prev.ObjectKey == req.ObjectKey {
				result.Duplicate = true
				result.TaskCompleted = prev.TaskCompleted
				return nil
			}
			return errorf(CodeReceiptConflict, "receipt %q already accepted with different content", req.ReceiptID)
		}

		// 2. 执行版本与快照水位必须匹配任务当前固定值。
		if req.ExecutionVersion != task.ExecutionVersion {
			return errorf(CodeVersionMismatch, "receipt version %d != task version %d", req.ExecutionVersion, task.ExecutionVersion)
		}
		if req.Watermark != task.Watermark {
			return errorf(CodeWatermarkMismatch, "receipt watermark %d != task watermark %d", req.Watermark, task.Watermark)
		}

		shard := tx.GetShard(req.TaskID, req.ShardIndex)
		if shard == nil {
			return errorf(CodeShardNotFound, "shard %d not found in task %q", req.ShardIndex, req.TaskID)
		}

		// 3. 已接受分片：内容一致按幂等成功返回，不一致拒绝覆盖。
		if shard.Status == ShardStatusAccepted {
			if shard.Digest == req.Digest && shard.ObjectKey == req.ObjectKey {
				result.Duplicate = true
				result.TaskCompleted = task.Status == TaskStatusSucceeded
				return nil
			}
			return errorf(CodeReceiptConflict, "shard %d already accepted, refusing to overwrite", req.ShardIndex)
		}

		// 4. 任务必须仍在运行，否则拒绝新内容（取消后不再接受回执）。
		if task.Status != TaskStatusRunning {
			return errorf(CodeTaskNotRunning, "task %q is %s", req.TaskID, task.Status)
		}

		// 5. 租约校验：必须是分片当前租约且未过期。
		if shard.Status != ShardStatusLeased || shard.LeaseID != req.LeaseID || shard.FencingToken != req.FencingToken {
			return errorf(CodeLeaseMismatch, "stale lease for shard %d", req.ShardIndex)
		}
		now := s.opts.Now()
		if !shard.LeaseExpiresAt.After(now) {
			return errorf(CodeLeaseExpired, "lease %q expired", req.LeaseID)
		}

		// 6. 接受回执。
		shard.Status = ShardStatusAccepted
		shard.Digest = req.Digest
		shard.ObjectKey = req.ObjectKey
		shard.ReceiptID = req.ReceiptID
		shard.AcceptedAt = &now
		tx.PutShard(shard)

		// 7. 若这是最后一个分片，同事务生成清单与完成通知（至多一次）。
		if allShardsAccepted(tx, req.TaskID, task.ShardCount) {
			if err := s.finalizeTx(tx, task, now); err != nil {
				return err
			}
			finalize = true
			result.TaskCompleted = true
		}
		tx.PutReceipt(&Receipt{
			ReceiptID:     req.ReceiptID,
			TaskID:        req.TaskID,
			ShardIndex:    req.ShardIndex,
			Digest:        req.Digest,
			ObjectKey:     req.ObjectKey,
			TaskCompleted: result.TaskCompleted,
			AcceptedAt:    now,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if finalize {
		// 清单对象在事务提交后物化；失败不丢状态，由 Recover 补齐。
		_ = s.materializeManifest(ctx, req.TaskID)
	}
	return result, nil
}

// allShardsAccepted 判断任务的全部分片是否都已接受。调用方须持有事务。
func allShardsAccepted(tx *Tx, taskID string, shardCount int) bool {
	shards := tx.ListShards(taskID)
	if len(shards) != shardCount {
		return false
	}
	for _, sh := range shards {
		if sh.Status != ShardStatusAccepted {
			return false
		}
	}
	return true
}

// finalizeTx 在事务内把任务推进到 SUCCEEDED：生成不可变清单并追加唯一一条
// 完成通知。仅在任务仍处于 RUNNING 时生效，保证与取消/过期竞争时终态唯一。
func (s *Service) finalizeTx(tx *Tx, task *Task, now time.Time) error {
	if task.Status != TaskStatusRunning {
		return nil
	}
	shards := tx.ListShards(task.ID)
	files := make([]ManifestFile, 0, len(shards))
	for _, sh := range shards {
		files = append(files, ManifestFile{ShardIndex: sh.Index, ObjectKey: sh.ObjectKey, Digest: sh.Digest})
	}
	expiresAt := now.Add(s.opts.DownloadTTL)
	manifest := &Manifest{
		TaskID:           task.ID,
		Watermark:        task.Watermark,
		ExecutionVersion: task.ExecutionVersion,
		Files:            files,
		Digest:           manifestDigest(task, files),
		ObjectKey:        fmt.Sprintf("exports/%s/manifest.json", task.ID),
		CreatedAt:        now,
		ExpiresAt:        expiresAt,
	}
	tx.PutManifest(manifest)

	task.Status = TaskStatusSucceeded
	task.CompletedAt = &now
	task.DownloadExpiresAt = &expiresAt
	task.ManifestKey = manifest.ObjectKey
	tx.PutTask(task)

	tx.AppendOutbox(&OutboxEvent{
		ID:           s.opts.NewID(),
		Type:         OutboxEventTaskCompleted,
		TaskID:       task.ID,
		ManifestKey:  manifest.ObjectKey,
		ManifestHash: manifest.Digest,
		Watermark:    task.Watermark,
		FileCount:    len(files),
		CreatedAt:    now,
	})
	return nil
}

// manifestDigest 由水位、执行版本与全部分片摘要确定性计算清单摘要。
func manifestDigest(task *Task, files []ManifestFile) string {
	h := sha256.New()
	fmt.Fprintf(h, "task=%s watermark=%d version=%d\n", task.ID, task.Watermark, task.ExecutionVersion)
	for _, f := range files {
		fmt.Fprintf(h, "%d:%s:%s\n", f.ShardIndex, f.ObjectKey, f.Digest)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// materializeManifest 把清单写入对象存储（幂等）。提交后崩溃导致的缺失
// 由 Recover 或下次 GetManifest 自愈。
func (s *Service) materializeManifest(ctx context.Context, taskID string) error {
	var manifest *Manifest
	err := s.store.View(ctx, func(tx *Tx) error {
		manifest = tx.GetManifest(taskID)
		return nil
	})
	if err != nil {
		return err
	}
	if manifest == nil {
		return errorf(CodeManifestNotReady, "task %q has no manifest", taskID)
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := s.objects.Put(ctx, manifest.ObjectKey, data); err != nil {
		return errorf(CodeObjectStore, "write manifest object: %v", err)
	}
	return nil
}

// CancelTask 取消任务。与分片完成竞争时只有一个终态：
// 任务仍运行则置为 CANCELLED 并停止新领取；已完成则报 TASK_NOT_RUNNING；
// 重复取消幂等成功。
func (s *Service) CancelTask(ctx context.Context, taskID string) error {
	return s.store.Update(ctx, func(tx *Tx) error {
		task := tx.GetTask(taskID)
		if task == nil {
			return errorf(CodeTaskNotFound, "task %q not found", taskID)
		}
		switch task.Status {
		case TaskStatusCancelled:
			return nil
		case TaskStatusRunning:
			task.Status = TaskStatusCancelled
			tx.PutTask(task)
			return nil
		default:
			return errorf(CodeTaskNotRunning, "task %q already %s", taskID, task.Status)
		}
	})
}

// GetManifest 读取任务清单。任务未完成报 MANIFEST_NOT_READY；
// 超过下载期限报 MANIFEST_EXPIRED。
func (s *Service) GetManifest(ctx context.Context, taskID string) (*Manifest, error) {
	var manifest *Manifest
	var task *Task
	err := s.store.View(ctx, func(tx *Tx) error {
		task = tx.GetTask(taskID)
		manifest = tx.GetManifest(taskID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, errorf(CodeTaskNotFound, "task %q not found", taskID)
	}
	if manifest == nil {
		return nil, errorf(CodeManifestNotReady, "task %q is %s", taskID, task.Status)
	}
	now := s.opts.Now()
	if task.Status == TaskStatusExpired || !manifest.ExpiresAt.After(now) {
		return nil, errorf(CodeManifestExpired, "manifest of task %q expired at %s", taskID, manifest.ExpiresAt)
	}
	// 自愈：清单对象缺失（如提交后崩溃）时重新物化。
	if !s.objects.Exists(ctx, manifest.ObjectKey) {
		if err := s.materializeManifest(ctx, taskID); err != nil {
			return nil, err
		}
	}
	return manifest, nil
}

// CleanupReport 是一次过期清理的结果。
type CleanupReport struct {
	ExpiredTasks   []string // 本次转为 EXPIRED 的任务
	DeletedObjects []string // 本次删除的对象键
}

// CleanupExpired 清理下载期已过的任务对象。
//
// 终态唯一性：与完成/取消一样在事务内做状态迁移。对象删除遵循
// "不得移除仍由有效清单引用的对象"：先收集所有未过期清单引用的对象键，
// 过期/取消任务的对象若仍被有效清单引用则保留。
func (s *Service) CleanupExpired(ctx context.Context) (*CleanupReport, error) {
	report := &CleanupReport{}
	var candidates []string
	err := s.store.Update(ctx, func(tx *Tx) error {
		now := s.opts.Now()

		// 仍有效的清单引用的全部对象键（含清单本身与分片文件）。
		validRefs := map[string]bool{}
		for _, t := range tx.ListTasks() {
			if t.Status != TaskStatusSucceeded || t.DownloadExpiresAt == nil || !t.DownloadExpiresAt.After(now) {
				continue
			}
			if m := tx.GetManifest(t.ID); m != nil {
				validRefs[m.ObjectKey] = true
				for _, f := range m.Files {
					validRefs[f.ObjectKey] = true
				}
			}
		}

		collect := func(taskID string) {
			if m := tx.GetManifest(taskID); m != nil {
				if !validRefs[m.ObjectKey] {
					candidates = append(candidates, m.ObjectKey)
				}
				for _, f := range m.Files {
					if !validRefs[f.ObjectKey] {
						candidates = append(candidates, f.ObjectKey)
					}
				}
				return
			}
			// 未完成的任务没有清单，直接按分片记录清理已上传对象。
			for _, sh := range tx.ListShards(taskID) {
				if sh.ObjectKey != "" && !validRefs[sh.ObjectKey] {
					candidates = append(candidates, sh.ObjectKey)
				}
			}
		}

		for _, t := range tx.ListTasks() {
			switch t.Status {
			case TaskStatusSucceeded:
				if t.DownloadExpiresAt != nil && !t.DownloadExpiresAt.After(now) {
					t.Status = TaskStatusExpired
					tx.PutTask(t)
					report.ExpiredTasks = append(report.ExpiredTasks, t.ID)
					collect(t.ID)
				}
			case TaskStatusCancelled:
				collect(t.ID)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(candidates)
	seen := map[string]bool{}
	for _, key := range candidates {
		if seen[key] {
			continue
		}
		seen[key] = true
		if err := s.objects.Delete(ctx, key); err != nil {
			return report, errorf(CodeObjectStore, "delete %q: %v", key, err)
		}
		report.DeletedObjects = append(report.DeletedObjects, key)
	}
	return report, nil
}

// PendingOutboxEvents 按写入顺序返回尚未派发的 outbox 事件。
func (s *Service) PendingOutboxEvents(ctx context.Context) ([]*OutboxEvent, error) {
	var out []*OutboxEvent
	err := s.store.View(ctx, func(tx *Tx) error {
		for _, e := range tx.ListOutbox() {
			if e.DispatchedAt == nil {
				out = append(out, e)
			}
		}
		return nil
	})
	return out, err
}

// MarkOutboxDispatched 标记 outbox 事件已派发（幂等）。
func (s *Service) MarkOutboxDispatched(ctx context.Context, ids []string) error {
	return s.store.Update(ctx, func(tx *Tx) error {
		now := s.opts.Now()
		for _, e := range tx.ListOutbox() {
			for _, id := range ids {
				if e.ID == id && e.DispatchedAt == nil {
					e.DispatchedAt = &now
					tx.PutOutboxEvent(e)
				}
			}
		}
		return nil
	})
}

// Recover 在重启后修复崩溃残留：
//  1. 所有分片已接受但仍处于 RUNNING 的任务（完成事务未提交）——重新终结；
//  2. 已成功但清单对象缺失的任务（提交后、物化前崩溃）——重新物化。
//
// 两种修复都幂等，可反复调用。
func (s *Service) Recover(ctx context.Context) error {
	var toFinalize []string
	var toMaterialize []string
	err := s.store.View(ctx, func(tx *Tx) error {
		for _, t := range tx.ListTasks() {
			switch t.Status {
			case TaskStatusRunning:
				if allShardsAccepted(tx, t.ID, t.ShardCount) {
					toFinalize = append(toFinalize, t.ID)
				}
			case TaskStatusSucceeded:
				if t.ManifestKey != "" && !s.objects.Exists(ctx, t.ManifestKey) {
					toMaterialize = append(toMaterialize, t.ID)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, taskID := range toFinalize {
		err := s.store.Update(ctx, func(tx *Tx) error {
			task := tx.GetTask(taskID)
			if task == nil || task.Status != TaskStatusRunning {
				return nil // 已被并发流程终结
			}
			if !allShardsAccepted(tx, taskID, task.ShardCount) {
				return nil
			}
			return s.finalizeTx(tx, task, s.opts.Now())
		})
		if err != nil {
			return err
		}
		toMaterialize = append(toMaterialize, taskID)
	}
	for _, taskID := range toMaterialize {
		if err := s.materializeManifest(ctx, taskID); err != nil {
			return err
		}
	}
	return nil
}
