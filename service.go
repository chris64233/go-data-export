// Service 是全部业务规则的承载者：快照水位固定、请求幂等与冲突、租约驱动的分片
// 领取、执行版本/租约/摘要三重校验的回执幂等接受、单清单单终态竞争、取消与过期、
// 保留期感知的清单读取与对象清理。所有方法对并发调用安全。

package dataexport

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"time"
)

// Config 控制任务的时间预算。
type Config struct {
	// LeaseTTL 每次领取分片获得的租约时长。
	LeaseTTL time.Duration
	// TaskTTL 任务从创建起的处理截止时间；超时未完成可被过期扫描终结。
	TaskTTL time.Duration
	// ManifestRetention 完成清单的可下载保留时长；过期后拒绝下载、对象可清理。
	ManifestRetention time.Duration
}

// DefaultConfig 给出一组偏保守的默认时间预算。
func DefaultConfig() Config {
	return Config{
		LeaseTTL:          2 * time.Minute,
		TaskTTL:           24 * time.Hour,
		ManifestRetention: 7 * 24 * time.Hour,
	}
}

// Service 是导出服务的入口，所有方法对并发调用安全。
type Service struct {
	store   PersistentStore
	objects *ObjectStore
	cfg     Config
	now     func() time.Time
}

// NewService 组装服务。store 与 objects 必须非空；now 为 nil 时使用墙钟，
// 测试可注入可控时钟。
func NewService(store PersistentStore, objects *ObjectStore, cfg Config, now func() time.Time) *Service {
	if cfg.LeaseTTL <= 0 || cfg.TaskTTL <= 0 || cfg.ManifestRetention <= 0 {
		panic("dataexport: Config durations must all be positive")
	}
	if now == nil {
		now = time.Now
	}
	return &Service{store: store, objects: objects, cfg: cfg, now: now}
}

// CreateTaskInput 是创建任务的入参。
type CreateTaskInput struct {
	// RequestID 是调用方提供的外部幂等号。
	RequestID string
	Scope     Scope
	// Watermark 是本次导出固定的快照水位（例如数据库版本号 / 逻辑时间戳）。
	Watermark int64
	// Shards 是待生成分片集合；每个分片的 Watermark 必须等于任务水位。
	Shards []ShardSpec
}

// Claim 结果。
type Claim struct {
	TaskID string `json:"task_id"`
	Index  int    `json:"index"`
	Lease  Lease  `json:"lease"`
	// ExecutionVersion 是工作者提交回执时必须回带的当前执行版本。
	ExecutionVersion int64 `json:"execution_version"`
}

// ReceiptInput 是工作者上传对象后提交回执的入参。
type ReceiptInput struct {
	TaskID string
	Index  int
	// ExecutionVersion 必须等于任务当前执行版本。
	ExecutionVersion int64
	// LeaseGeneration 必须等于分片当前租约代际。
	LeaseGeneration int64
	WorkerID        string
	ObjectKey       string
}

// ReceiptResult 是回执接受（或重放命中）后的结果。
type ReceiptResult struct {
	// Accepted 为 true 表示本次调用首次接受了回执；false 表示重放命中，
	// 返回的是此前已接受回执的原结果。
	Accepted bool    `json:"accepted"`
	Receipt  Receipt `json:"receipt"`
	// Completed 在本次接受恰好促成任务完成时为 true，Manifest 非空。
	Completed bool      `json:"completed"`
	Manifest  *Manifest `json:"manifest,omitempty"`
}

func newID(prefix string) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

// CreateTask 创建导出任务。
//
// 幂等与冲突规则：RequestID + Scope 共同决定结果。
//   - 同一 RequestID 以完全相同的 Scope（与水位、分片集合无关，二者由 Scope 派生）
//     重复请求，返回原先创建的同一个任务（幂等重放）；
//   - 同一 RequestID 改变 Scope（含 UserID / Resource / 起止边界）报
//     ErrRequestConflict；
//   - 新 RequestID 创建新任务，创建瞬间固定快照水位与待生成分片集合。
func (s *Service) CreateTask(in CreateTaskInput) (Task, error) {
	if in.RequestID == "" {
		return Task{}, errorf(ErrCodeInvalidArgument, "request_id is required")
	}
	if in.Scope.UserID == "" {
		return Task{}, errorf(ErrCodeInvalidArgument, "scope.user_id is required")
	}
	if in.Watermark <= 0 {
		return Task{}, errorf(ErrCodeInvalidArgument, "watermark must be positive")
	}
	if len(in.Shards) == 0 {
		return Task{}, errorf(ErrCodeInvalidArgument, "at least one shard is required")
	}
	seen := map[int]bool{}
	for _, sh := range in.Shards {
		if seen[sh.Index] {
			return Task{}, errorf(ErrCodeInvalidArgument, "duplicate shard index %d", sh.Index)
		}
		seen[sh.Index] = true
		if sh.Watermark != in.Watermark {
			return Task{}, withDetails(errorf(ErrCodeSnapshotMismatch,
				"shard %d watermark %d does not equal task watermark %d",
				sh.Index, sh.Watermark, in.Watermark),
				map[string]string{"shard_index": fmt.Sprintf("%d", sh.Index)})
		}
		if sh.ExpectedDigest == "" {
			return Task{}, errorf(ErrCodeInvalidArgument, "shard %d expected_digest is required", sh.Index)
		}
	}

	var created Task
	err := s.store.Update(func(tx *Tx) error {
		if existingID, ok := tx.FindRequestID(in.RequestID); ok {
			existing, ok := tx.GetTask(existingID)
			if !ok {
				return errorf(ErrCodeNotFound, "task %q bound to request id is missing", existingID)
			}
			if !existing.Scope.equals(in.Scope) {
				return ErrRequestConflict
			}
			created = existing
			return nil // 幂等重放：不产生任何写入。
		}

		now := tx.Now()
		t := Task{
			ID:               newID("task"),
			RequestID:        in.RequestID,
			Scope:            in.Scope,
			Watermark:        in.Watermark,
			ExecutionVersion: 1,
			Status:           StatusPending,
			Deadline:         now.Add(s.cfg.TaskTTL),
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		tx.CreateTask(t, in.Shards)
		created = t
		return nil
	})
	if err != nil {
		return Task{}, err
	}
	return created, nil
}

// GetTask 读取任务状态。
func (s *Service) GetTask(id string) (Task, error) {
	var out Task
	err := s.store.View(func(v StateView) error {
		t, ok := v.GetTask(id)
		if !ok {
			return errorf(ErrCodeNotFound, "task %q not found", id)
		}
		out = t
		return nil
	})
	return out, err
}

// ClaimShard 为 worker 领取一个当前可执行的分片并颁发租约。
//
// 可领取 = 分片未完成，且没有未到期的有效租约（新任务分片或租约已超时的分片）。
// 任务取消后停止一切新领取；任务已完成 / 过期同样拒绝。
// 领取会使该分片租约代际 +1，旧租约立即失效。
// 无可领取分片时返回 ErrNoShardAvailable。
func (s *Service) ClaimShard(workerID string) (Claim, error) {
	if workerID == "" {
		return Claim{}, errorf(ErrCodeInvalidArgument, "worker_id is required")
	}
	var claim Claim
	err := s.store.Update(func(tx *Tx) error {
		now := tx.Now()
		// 选择策略：按 (创建顺序, 任务ID, 分片序号) 确定顺序，使并发领取的结果可预测；
		// 真正的互斥由事务的版本校验保证。
		var candidate *Shard
		var candidateTask Task
	scan:
		for _, t := range sortedTasks(tx) {
			if t.Status != StatusPending {
				continue // 取消后停止新领取；完成 / 过期任务也不可领取。
			}
			if now.After(t.Deadline) {
				continue // 已超过处理截止，等待过期扫描终结，不再发放租约。
			}
			for _, sh := range sortedShards(tx, t.ID) {
				if sh.Status == ShardDone {
					continue
				}
				if sh.Lease != nil && sh.Lease.activeAt(now) {
					continue
				}
				candidate = &sh
				candidateTask = t
				break scan
			}
		}
		if candidate == nil {
			return ErrNoShardAvailable
		}

		gen := int64(1)
		if candidate.Lease != nil {
			gen = candidate.Lease.Generation + 1
		}
		candidate.Lease = &Lease{
			Generation: gen,
			WorkerID:   workerID,
			ExpiresAt:  now.Add(s.cfg.LeaseTTL),
		}
		tx.PutShard(*candidate)
		tx.PutTask(candidateTask)
		claim = Claim{
			TaskID:           candidateTask.ID,
			Index:            candidate.Index,
			ExecutionVersion: candidateTask.ExecutionVersion,
			Lease:            *candidate.Lease,
		}
		return nil
	})
	if err != nil {
		return Claim{}, err
	}
	return claim, nil
}

// ClaimShardOf 在已知任务 ID 时领取该任务下的分片；语义与 ClaimShard 相同，
// 便于工作者按任务拉取。任务不存在返回 ErrNotFound，任务取消返回 ErrTaskCanceled。
func (s *Service) ClaimShardOf(taskID, workerID string) (Claim, error) {
	if taskID == "" || workerID == "" {
		return Claim{}, errorf(ErrCodeInvalidArgument, "task_id and worker_id are required")
	}
	var claim Claim
	err := s.store.Update(func(tx *Tx) error {
		t, err := tx.MustGetTask(taskID)
		if err != nil {
			return err
		}
		now := tx.Now()
		switch {
		case t.Status == StatusCanceled:
			return ErrTaskCanceled
		case t.Status == StatusExpired:
			return ErrTaskExpired
		case t.Status == StatusCompleted:
			return ErrTaskCompleted
		case now.After(t.Deadline):
			return ErrTaskExpired
		}

		var candidate *Shard
		for _, sh := range sortedShards(tx, taskID) {
			sh := sh
			if sh.Status == ShardDone {
				continue
			}
			if sh.Lease != nil && sh.Lease.activeAt(now) {
				continue
			}
			candidate = &sh
			break
		}
		if candidate == nil {
			return ErrNoShardAvailable
		}
		gen := int64(1)
		if candidate.Lease != nil {
			gen = candidate.Lease.Generation + 1
		}
		candidate.Lease = &Lease{Generation: gen, WorkerID: workerID, ExpiresAt: now.Add(s.cfg.LeaseTTL)}
		tx.PutShard(*candidate)
		tx.PutTask(t)
		claim = Claim{TaskID: taskID, Index: candidate.Index, ExecutionVersion: t.ExecutionVersion, Lease: *candidate.Lease}
		return nil
	})
	return claim, err
}

// SubmitReceipt 接受工作者的分片上传摘要回执。
//
// 接受条件（任一不满足即拒绝，且绝不覆盖已接受内容）：
//  1. 任务处于 pending；已取消 -> ErrTaskCanceled，已完成 -> ErrTaskCompleted，
//     已过期 -> ErrTaskExpired；
//  2. 回执 ExecutionVersion == 任务当前 ExecutionVersion，否则 ErrVersionMismatch；
//  3. 分片存在当前租约且 WorkerID 匹配、未到期：租约代际不符 -> ErrLeaseStale，
//     租约到期 -> ErrLeaseExpired，无租约 -> ErrLeaseStale；
//  4. 引用对象存在（ErrObjectNotFound），且其服务端摘要与大小分别等于
//     ShardSpec.ExpectedDigest（ErrDigestMismatch）——该期望摘要在任务创建时
//     绑定快照水位，因此摘要通过即证明对象内容属于该水位；
//  5. 分片此前未接受过回执。
//
// 幂等：若分片已有接受回执，则仅当本次回执的 (执行版本, 租约代际, 工作者, 对象键)
// 与原回执完全一致时按重放返回原结果——重放读取的是已持久化的回执记录，即使对象
// 此后已过保留期被清理也照常返回；旧租约 / 不同对象 / 不同工作者一律
// ErrShardAlreadyCompleted，不覆盖。
//
// 当最后一个分片被接受时，在同一事务内：复核全部分片属于同一快照水位且摘要齐备，
// 一次性生成不可变清单、置任务为 completed，并写入 task.completed outbox 通知。
// 多个工作者并发提交最后分片时，事务串行化保证只有一个提交能观察到"全部完成"，
// 因而只有一个清单、一条完成通知。
func (s *Service) SubmitReceipt(in ReceiptInput) (ReceiptResult, error) {
	if in.TaskID == "" || in.WorkerID == "" || in.ObjectKey == "" {
		return ReceiptResult{}, errorf(ErrCodeInvalidArgument, "task_id, worker_id and object_key are required")
	}
	if in.ExecutionVersion <= 0 || in.LeaseGeneration <= 0 {
		return ReceiptResult{}, errorf(ErrCodeInvalidArgument, "execution_version and lease_generation must be positive")
	}

	var result ReceiptResult
	err := s.store.Update(func(tx *Tx) error {
		t, err := tx.MustGetTask(in.TaskID)
		if err != nil {
			return err
		}
		sh, err := tx.MustGetShard(in.TaskID, in.Index)
		if err != nil {
			return err
		}

		// 已接受过回执：无论任务此后进入何种终态、对象是否已过保留期被清理，
		// 逐字节相同的回执重放永远返回原结果（回执幂等先于一切，且不依赖对象
		// 仍然存在——重放的是已持久化的回执记录本身）。旧租约 / 不同摘要 /
		// 不同对象一律 ErrShardAlreadyCompleted，不覆盖。
		if sh.Status == ShardDone && sh.Receipt != nil {
			old := sh.Receipt
			same := in.ExecutionVersion == old.ExecutionVersion &&
				in.LeaseGeneration == old.LeaseGeneration &&
				in.WorkerID == old.WorkerID &&
				in.ObjectKey == old.ObjectKey
			if !same {
				return ErrShardAlreadyCompleted
			}
			result = ReceiptResult{Accepted: false, Receipt: *old}
			if m, ok := tx.GetManifest(in.TaskID); ok {
				mc := m
				result.Completed = true
				result.Manifest = &mc
			}
			return nil
		}

		// 分片尚未接受回执：任务终态 / 截止时间检查。
		now := tx.Now()
		switch t.Status {
		case StatusCanceled:
			return ErrTaskCanceled
		case StatusExpired:
			return ErrTaskExpired
		case StatusCompleted:
			// 理论上不可达：完成必然意味着所有分片已有回执。
			return ErrTaskCompleted
		case StatusPending:
			if now.After(t.Deadline) {
				return ErrTaskExpired
			}
		}

		if in.ExecutionVersion != t.ExecutionVersion {
			return ErrVersionMismatch
		}
		if sh.Lease == nil || in.LeaseGeneration != sh.Lease.Generation {
			return ErrLeaseStale
		}
		if sh.Lease.WorkerID != in.WorkerID {
			return ErrLeaseStale
		}
		if !sh.Lease.activeAt(now) {
			return ErrLeaseExpired
		}

		// 首次接受路径才访问对象存储（真实系统对应 HEAD Object）：对象必须存在，
		// 且服务端摘要必须匹配分片创建时固定在快照水位上的期望摘要。
		// 该读取是只读操作，事务因乐观冲突重放时重复执行无副作用。
		digest, size, err := s.objects.StatObject(in.ObjectKey)
		if err != nil {
			return err
		}
		expected, ok := shardSpecAt(tx, in.TaskID, in.Index)
		if !ok {
			return errorf(ErrCodeNotFound, "spec for shard %d not found", in.Index)
		}
		if expected.Watermark != t.Watermark {
			return ErrSnapshotMismatch
		}
		if digest != expected.ExpectedDigest {
			return withDetails(ErrDigestMismatch, map[string]string{
				"expected": expected.ExpectedDigest,
				"actual":   digest,
			})
		}

		receipt := Receipt{
			ExecutionVersion: in.ExecutionVersion,
			LeaseGeneration:  in.LeaseGeneration,
			WorkerID:         in.WorkerID,
			ObjectKey:        in.ObjectKey,
			Digest:           digest,
			Size:             size,
			AcceptedAt:       now,
		}
		sh.Status = ShardDone
		sh.Receipt = &receipt
		tx.PutShard(sh)

		result = ReceiptResult{Accepted: true, Receipt: receipt}

		// 判断是否所有分片都已完成；若是，则独占性地生成清单与终态。
		all := sortedShards(tx, in.TaskID)
		allDone := true
		for _, x := range all {
			if x.Status != ShardDone {
				allDone = false
				break
			}
		}
		if !allDone {
			tx.PutTask(t)
			return nil
		}

		// 走到这里时 t.Status 必为 pending（新回执只在 pending 下接受）。
		// 完成前最后一次快照水位一致性复核：所有分片规格都必须属于任务水位。
		entries := make([]ManifestEntry, 0, len(all))
		for _, x := range all {
			spec, ok := shardSpecAt(tx, in.TaskID, x.Index)
			if !ok || spec.Watermark != t.Watermark || x.Receipt == nil || x.Receipt.Digest != spec.ExpectedDigest {
				return ErrSnapshotMismatch
			}
			entries = append(entries, ManifestEntry{
				Index:     x.Index,
				ObjectKey: x.Receipt.ObjectKey,
				Digest:    x.Receipt.Digest,
				Size:      x.Receipt.Size,
				WorkerID:  x.Receipt.WorkerID,
			})
		}

		manifest := Manifest{
			TaskID:      in.TaskID,
			Watermark:   t.Watermark,
			Entries:     entries,
			CreatedAt:   now,
			RetainUntil: now.Add(s.cfg.ManifestRetention),
		}
		tx.PutManifest(manifest) // 重复写入会 panic：单清单不变量的最后防线。

		t.Status = StatusCompleted
		t.CompletedAt = now
		tx.PutTask(t)

		tx.EnqueueOutbox(OutboxEvent{
			EventID: in.TaskID + "/" + EventTaskCompleted,
			TaskID:  in.TaskID,
			Type:    EventTaskCompleted,
			Payload: map[string]string{
				"watermark": fmt.Sprintf("%d", t.Watermark),
				"shards":    fmt.Sprintf("%d", len(entries)),
			},
		})

		mc := manifest
		result.Completed = true
		result.Manifest = &mc
		return nil
	})
	if err != nil {
		return ReceiptResult{}, err
	}
	return result, nil
}

// CancelTask 取消任务。取消是幂等的：重复取消返回当前状态；
// 任务若已经处于 completed / expired 终态，则返回 ErrTaskTerminal——
// 取消、过期、分片完成互相竞争时，终态由先提交的事务唯一确定，之后不可更改。
// 取消成功时在同一事务写入 task.canceled outbox 通知，且此后停止新领取。
func (s *Service) CancelTask(taskID, reason string) (Task, error) {
	var out Task
	err := s.store.Update(func(tx *Tx) error {
		t, err := tx.MustGetTask(taskID)
		if err != nil {
			return err
		}
		if t.Status == StatusCanceled {
			out = t
			return nil
		}
		if t.Status.Terminal() {
			return ErrTaskTerminal
		}
		now := tx.Now()
		t.Status = StatusCanceled
		t.CanceledAt = now
		// 执行版本递增：所有在途工作者手里的旧版本回执将被拒绝。
		t.ExecutionVersion++
		tx.PutTask(t)
		tx.EnqueueOutbox(OutboxEvent{
			EventID: taskID + "/" + EventTaskCanceled,
			TaskID:  taskID,
			Type:    EventTaskCanceled,
			Payload: map[string]string{"reason": reason},
		})
		out = t
		return nil
	})
	return out, err
}

// ExpireDueTasks 扫描所有 pending 且已超过处理截止时间的任务，将其终结为 expired。
// 与"最后一个分片完成"竞争时同样只有一个终态：若完成先提交，本任务不再被选中；
// 若过期先提交，在途回执会因任务终态 / 版本问题被拒绝。每个过期任务写入一条
// task.expired outbox 通知。返回本次终结的任务 ID 列表。
func (s *Service) ExpireDueTasks() ([]string, error) {
	var expired []string
	err := s.store.Update(func(tx *Tx) error {
		now := tx.Now()
		for _, t := range sortedTasks(tx) {
			if t.Status != StatusPending || !now.After(t.Deadline) {
				continue
			}
			t.Status = StatusExpired
			t.ExpiredAt = now
			t.ExecutionVersion++
			tx.PutTask(t)
			tx.EnqueueOutbox(OutboxEvent{
				EventID: t.ID + "/" + EventTaskExpired,
				TaskID:  t.ID,
				Type:    EventTaskExpired,
				Payload: map[string]string{"deadline": t.Deadline.UTC().Format(time.RFC3339Nano)},
			})
			expired = append(expired, t.ID)
		}
		return nil
	})
	return expired, err
}

// GetManifest 读取不可变清单。清单不存在（任务未完成）返回 ErrManifestNotFound；
// 清单超过保留期返回 ErrManifestExpired——过期下载必须被拒绝。
func (s *Service) GetManifest(taskID string) (Manifest, error) {
	var out Manifest
	err := s.store.View(func(v StateView) error {
		if _, ok := v.GetTask(taskID); !ok {
			return errorf(ErrCodeNotFound, "task %q not found", taskID)
		}
		m, ok := v.GetManifest(taskID)
		if !ok {
			return ErrManifestNotFound
		}
		if m.expiredAt(s.now()) {
			return ErrManifestExpired
		}
		out = m
		return nil
	})
	return out, err
}

// GetShardObject 在下载窗口内返回某个已完成分片的对象内容（含清单存在性与
// 保留期校验：任务未完成 / 清单已过期都拒绝下载）。
func (s *Service) GetShardObject(taskID string, index int) ([]byte, error) {
	var key string
	err := s.store.View(func(v StateView) error {
		if _, ok := v.GetTask(taskID); !ok {
			return errorf(ErrCodeNotFound, "task %q not found", taskID)
		}
		m, ok := v.GetManifest(taskID)
		if !ok {
			return ErrManifestNotFound
		}
		if m.expiredAt(s.now()) {
			return ErrManifestExpired
		}
		for _, e := range m.Entries {
			if e.Index == index {
				key = e.ObjectKey
				return nil
			}
		}
		return errorf(ErrCodeNotFound, "shard %d not found in manifest", index)
	})
	if err != nil {
		return nil, err
	}
	data, _, _, err := s.objects.GetObject(key)
	return data, err
}

// CleanupResult 是过期清理的结果。
type CleanupResult struct {
	// DeletedObjects 被实际删除的对象键。
	DeletedObjects []string
	// ExpiredManifests 被判定为超过保留期的任务 ID。
	ExpiredManifests []string
	// SkippedReferenced 记录因仍被有效清单引用而未删除的对象键
	// （清理过程不得移除仍由有效清单引用的对象）。
	SkippedReferenced []string
}

// CleanupExpired 执行过期清理：
//   - 已超过保留期的清单：删除其分片对象；但若同一对象仍被另一个保留期内的有效
//     清单引用，则跳过并计入 SkippedReferenced（清理过程不得移除有效清单引用的对象）；
//   - 已取消 / 已过期任务上已接受回执、却永远不会进入清单的孤儿对象，一并删除；
//   - pending 任务在途工作者可能正在引用的对象一律不动。
//
// 对于"已上传但从未被回执接受"的对象（状态层完全无记录），使用
// CleanupExpiredWithCandidates 传入对象存储列出的候选键进行清理。
func (s *Service) CleanupExpired() (CleanupResult, error) {
	return s.cleanup(nil)
}

// CleanupExpiredWithCandidates 与 CleanupExpired 相同，但额外接收一批候选对象键
// （例如按对象前缀从对象存储列出的分片对象）。候选对象仅在能关联到"保留期已过期
// 清单所属任务"或"已终结且无清单任务"时删除；仍被有效清单引用的跳过；
// 无法关联到任何任务、或属于 pending 任务的候选对象保持不动（保守策略，
// 绝不删除在途对象或他人对象）。
func (s *Service) CleanupExpiredWithCandidates(candidateKeys []string) (CleanupResult, error) {
	return s.cleanup(candidateKeys)
}

func (s *Service) cleanup(extraCandidates []string) (CleanupResult, error) {
	var res CleanupResult

	deleteSet := map[string]bool{}
	skipSet := map[string]bool{}
	expiredManifestSet := map[string]bool{}

	err := s.store.View(func(v StateView) error {
		now := s.now()

		// protected: 被任意保留期内有效清单引用的对象，任何情况下都不删。
		protected := map[string]bool{}
		// deletable: 由过期清单条目、或终结无清单任务的回执所指向的对象。
		deletable := map[string]bool{}

		for _, t := range sortedTasksView(v) {
			m, hasManifest := v.GetManifest(t.ID)
			switch {
			case hasManifest && m.expiredAt(now):
				expiredManifestSet[t.ID] = true
				for _, e := range m.Entries {
					deletable[e.ObjectKey] = true
				}
			case hasManifest:
				for _, e := range m.Entries {
					protected[e.ObjectKey] = true
				}
			case t.Status.Terminal():
				// canceled / expired 且永不会再有清单：已接受回执的对象是孤儿。
				for _, sh := range v.GetShards(t.ID) {
					if sh.Receipt != nil {
						deletable[sh.Receipt.ObjectKey] = true
					}
				}
			}
		}

		// 候选键只能在"对象归属可判定"时参与清理。归属判定：
		// 出现在任一清单条目、或终结任务的已接受回执中。
		knownOwner := map[string]bool{}
		for _, t := range sortedTasksView(v) {
			m, hasManifest := v.GetManifest(t.ID)
			if hasManifest {
				for _, e := range m.Entries {
					knownOwner[e.ObjectKey] = true
				}
				continue
			}
			if t.Status.Terminal() {
				for _, sh := range v.GetShards(t.ID) {
					if sh.Receipt != nil {
						knownOwner[sh.Receipt.ObjectKey] = true
					}
				}
			}
		}
		for _, k := range extraCandidates {
			if knownOwner[k] {
				deletable[k] = true
			}
			// 归属未知（pending 在途对象 / 他人对象）：保持不动。
		}

		for k := range deletable {
			if protected[k] {
				skipSet[k] = true
				continue
			}
			deleteSet[k] = true
		}
		return nil
	})
	if err != nil {
		return res, err
	}

	for k := range deleteSet {
		if s.objects.DeleteObject(k) {
			res.DeletedObjects = append(res.DeletedObjects, k)
		}
	}
	for k := range skipSet {
		res.SkippedReferenced = append(res.SkippedReferenced, k)
	}
	for id := range expiredManifestSet {
		res.ExpiredManifests = append(res.ExpiredManifests, id)
	}
	sort.Strings(res.DeletedObjects)
	sort.Strings(res.SkippedReferenced)
	sort.Strings(res.ExpiredManifests)
	return res, nil
}

// ---- 事务内排序辅助（保证选择顺序确定，便于测试与公平性）----

func sortedTasks(tx *Tx) []Task {
	out := make([]Task, 0, len(tx.tasks))
	for _, t := range tx.tasks {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func sortedTasksView(v StateView) []Task {
	if tx, ok := v.(*Tx); ok {
		return sortedTasks(tx)
	}
	return nil
}

func sortedShards(tx *Tx, taskID string) []Shard {
	out := tx.GetShards(taskID)
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

// shardSpecAt 返回分片在任务创建时固定的不可变规格（快照水位与期望摘要）。
// 规格随分片行持久化，因此这里就是分片自身的字段。
func shardSpecAt(tx *Tx, taskID string, index int) (ShardSpec, bool) {
	sh, ok := tx.shards[taskID][index]
	if !ok {
		return ShardSpec{}, false
	}
	return ShardSpec{
		Index:          sh.Index,
		Watermark:      sh.Watermark,
		ExpectedDigest: sh.ExpectedDigest,
		Size:           sh.ExpectedSize,
	}, true
}
