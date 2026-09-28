package dataexport

import (
	"sync"
	"time"
)

// PersistentStore 是任务状态与 outbox 的持久化抽象。
//
// 生产环境应基于支持事务的数据库实现（一张任务表、一张分片表、一张 outbox 表，
// 以及 request_id 唯一索引）。本仓库提供进程内的 MemoryStore，其 Update 事务提供
// 可串行化语义并内置乐观并发重试，因此 Service 的全部并发推理可以直接在单存储上
// 被测试验证（多个 goroutine 抢最后分片 / 取消与完成竞争等）。
type PersistentStore interface {
	// View 在只读事务中访问状态快照。fn 不得修改传入的 StateView。
	View(fn func(StateView) error) error
	// Update 在读写事务中修改状态。fn 返回错误则事务回滚（返回该错误）；
	// 发生乐观并发冲突时整个 fn 会以新的版本号重新执行。
	Update(fn func(tx *Tx) error) error
}

// StateView 是事务内可见的只读状态。
type StateView interface {
	Version() int64
	GetTask(id string) (Task, bool)
	GetShards(taskID string) []Shard
	GetManifest(taskID string) (Manifest, bool)
	// FindRequestID 返回外部请求号当前绑定的任务 ID（若存在）。
	FindRequestID(requestID string) (string, bool)
	// Outbox 以入队顺序返回尚未被标记投递的事件快照。
	PendingOutbox() []OutboxEvent
}

// Tx 是读写事务句柄。
type Tx struct {
	version int64
	now     time.Time

	tasks     map[string]*Task
	shards    map[string]map[int]*Shard
	manifests map[string]*Manifest
	requests  map[string]string // requestID -> taskID
	outbox    []OutboxEvent
}

// Version 返回事务基于的状态版本（乐观并发控制）。
func (tx *Tx) Version() int64 { return tx.version }

// Now 返回事务开始时注入的逻辑时钟时刻；同一事务内所有时间戳一致。
func (tx *Tx) Now() time.Time { return tx.now }

func (tx *Tx) GetTask(id string) (Task, bool) {
	t, ok := tx.tasks[id]
	if !ok {
		return Task{}, false
	}
	return *t, true
}

// MustGetTask 返回任务，不存在时包装为 ErrNotFound（便于在事务中直接 return）。
func (tx *Tx) MustGetTask(id string) (Task, error) {
	t, ok := tx.GetTask(id)
	if !ok {
		return Task{}, errorf(ErrCodeNotFound, "task %q not found", id)
	}
	return t, nil
}

func (tx *Tx) GetShards(taskID string) []Shard {
	byIndex := tx.shards[taskID]
	out := make([]Shard, 0, len(byIndex))
	for _, sh := range byIndex {
		out = append(out, *sh)
	}
	return out
}

// MustGetShard 返回指定分片。
func (tx *Tx) MustGetShard(taskID string, index int) (Shard, error) {
	byIndex := tx.shards[taskID]
	sh, ok := byIndex[index]
	if !ok {
		return Shard{}, errorf(ErrCodeNotFound, "shard %d of task %q not found", index, taskID)
	}
	return *sh, nil
}

func (tx *Tx) GetManifest(taskID string) (Manifest, bool) {
	m, ok := tx.manifests[taskID]
	if !ok {
		return Manifest{}, false
	}
	return *m, true
}

func (tx *Tx) FindRequestID(requestID string) (string, bool) {
	id, ok := tx.requests[requestID]
	return id, ok
}

func (tx *Tx) PendingOutbox() []OutboxEvent {
	out := make([]OutboxEvent, len(tx.outbox))
	copy(out, tx.outbox)
	return out
}

// ---- 变更操作（仅在 Update 事务内有效）----

// PutTask 新建或更新任务。
func (tx *Tx) PutTask(t Task) {
	cp := t
	cp.UpdatedAt = tx.now
	tx.tasks[t.ID] = &cp
}

// CreateTask 以初始分片集合一次性创建任务，并绑定外部请求号。
// 调用方必须自行保证 requestID 尚未绑定其他任务。
func (tx *Tx) CreateTask(t Task, shardSpecs []ShardSpec) {
	t.CreatedAt = tx.now
	t.UpdatedAt = tx.now
	if t.ExecutionVersion == 0 {
		t.ExecutionVersion = 1
	}
	cp := t
	tx.tasks[t.ID] = &cp

	byIndex := make(map[int]*Shard, len(shardSpecs))
	for i := range shardSpecs {
		spec := shardSpecs[i]
		byIndex[spec.Index] = &Shard{
			TaskID:         t.ID,
			Index:          spec.Index,
			Status:         ShardPending,
			Watermark:      spec.Watermark,
			ExpectedDigest: spec.ExpectedDigest,
			ExpectedSize:   spec.Size,
		}
	}
	tx.shards[t.ID] = byIndex
	tx.requests[t.RequestID] = t.ID
}

// PutShard 更新分片状态。
func (tx *Tx) PutShard(s Shard) {
	byIndex, ok := tx.shards[s.TaskID]
	if !ok {
		byIndex = make(map[int]*Shard)
		tx.shards[s.TaskID] = byIndex
	}
	cp := s
	byIndex[s.Index] = &cp
}

// PutManifest 不可变地写入清单。每个任务至多写入一次：重复写入会 panic，
// 因为这意味着 Service 层的"只生成一个清单"不变量被破坏。
func (tx *Tx) PutManifest(m Manifest) {
	if _, exists := tx.manifests[m.TaskID]; exists {
		panic("dataexport: manifest for task " + m.TaskID + " is immutable and already exists")
	}
	cp := m
	cp.Entries = append([]ManifestEntry(nil), m.Entries...)
	tx.manifests[m.TaskID] = &cp
}

// EnqueueOutbox 在当前事务中追加一条出站事件（与状态修改原子提交）。
func (tx *Tx) EnqueueOutbox(e OutboxEvent) {
	if e.CreatedAt.IsZero() {
		e.CreatedAt = tx.now
	}
	tx.outbox = append(tx.outbox, e)
}

// MemoryStore 是 PersistentStore 的进程内实现。
//
// 它用一把互斥锁串行化所有提交，并通过版本号在 Update 的 fn 执行期间检测并发写：
// fn 基于某一版本读取并计算，提交时若版本已前进则放弃并重试。这与真实数据库
// "BEGIN ... SELECT ... COMMIT" 在可串行化隔离级别下的行为等价，足以驱动全部
// 并发正确性测试。
type MemoryStore struct {
	mu        sync.Mutex
	version   int64
	tasks     map[string]*Task
	shards    map[string]map[int]*Shard
	manifests map[string]*Manifest
	requests  map[string]string
	outbox    []OutboxEvent
	// delivered 记录已被 OutboxDispatcher 标记完成的事件 ID（保留事件用于审计）。
	delivered map[string]bool
	now       func() time.Time
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore(now func() time.Time) *MemoryStore {
	if now == nil {
		now = time.Now
	}
	return &MemoryStore{
		tasks:     map[string]*Task{},
		shards:    map[string]map[int]*Shard{},
		manifests: map[string]*Manifest{},
		requests:  map[string]string{},
		delivered: map[string]bool{},
		now:       now,
	}
}

// snapshot 在锁内深拷贝全量状态，供事务执行使用。
func (s *MemoryStore) snapshot() (int64, map[string]*Task, map[string]map[int]*Shard, map[string]*Manifest, map[string]string, []OutboxEvent) {
	tasks := make(map[string]*Task, len(s.tasks))
	for k, v := range s.tasks {
		cp := *v
		tasks[k] = &cp
	}
	shards := make(map[string]map[int]*Shard, len(s.shards))
	for tid, m := range s.shards {
		nm := make(map[int]*Shard, len(m))
		for i, v := range m {
			cp := *v
			if v.Lease != nil {
				l := *v.Lease
				cp.Lease = &l
			}
			if v.Receipt != nil {
				r := *v.Receipt
				cp.Receipt = &r
			}
			nm[i] = &cp
		}
		shards[tid] = nm
	}
	manifests := make(map[string]*Manifest, len(s.manifests))
	for k, v := range s.manifests {
		cp := *v
		cp.Entries = append([]ManifestEntry(nil), v.Entries...)
		manifests[k] = &cp
	}
	requests := make(map[string]string, len(s.requests))
	for k, v := range s.requests {
		requests[k] = v
	}
	outbox := make([]OutboxEvent, len(s.outbox))
	copy(outbox, s.outbox)
	return s.version, tasks, shards, manifests, requests, outbox
}

// View 实现 PersistentStore。
func (s *MemoryStore) View(fn func(StateView) error) error {
	s.mu.Lock()
	version, tasks, shards, manifests, requests, outbox := s.snapshot()
	s.mu.Unlock()

	tx := &Tx{
		version:   version,
		now:       s.now(),
		tasks:     tasks,
		shards:    shards,
		manifests: manifests,
		requests:  requests,
		outbox:    outbox,
	}
	return fn(tx)
}

// Update 实现 PersistentStore。流程为：锁内取深拷贝快照与版本号 -> 释放锁执行
// fn（期间对 ObjectStore 等外部资源的访问不被阻塞）-> 重新加锁校验版本号，
// 若期间已有其他事务提交则丢弃工作集并重跑 fn，等价于可串行化隔离级别下的
// "UPDATE ... WHERE version=?" 乐观并发模式。
//
// 因为 fn 可能被重放，其中不得产生不可逆的外部副作用；outbox 事件 ID 必须
// 确定性生成（Service 中使用 taskID+"/"+eventType），保证重放不会制造重复事件。
func (s *MemoryStore) Update(fn func(tx *Tx) error) error {
	for {
		s.mu.Lock()
		baseVersion, tasks, shards, manifests, requests, outbox := s.snapshot()
		s.mu.Unlock()

		tx := &Tx{
			version:   baseVersion,
			now:       s.now(),
			tasks:     tasks,
			shards:    shards,
			manifests: manifests,
			requests:  requests,
			outbox:    outbox,
		}
		if err := fn(tx); err != nil {
			return err
		}

		s.mu.Lock()
		if s.version != baseVersion {
			// 执行期间有其他事务提交，丢弃本次结果并重试。
			s.mu.Unlock()
			continue
		}
		s.commitLocked(tx)
		s.version++
		s.mu.Unlock()
		return nil
	}
}

// commitLocked 在持锁状态下用事务内的工作集替换全局状态。
// 工作集本身来自深拷贝，因此直接替换即可安全发布。
func (s *MemoryStore) commitLocked(tx *Tx) {
	s.tasks = tx.tasks
	s.shards = tx.shards
	s.manifests = tx.manifests
	s.requests = tx.requests
	// outbox 只追加；直接采用事务视图（其起点包含了提交时的全部历史）。
	s.outbox = tx.outbox
}

// MarkOutboxDelivered 由 OutboxDispatcher 调用，记录事件已成功投递。
// 事件本身保留在 outbox 中以便审计；PendingOutbox 只返回未投递项。
func (s *MemoryStore) MarkOutboxDelivered(eventID string) {
	s.mu.Lock()
	s.delivered[eventID] = true
	s.mu.Unlock()
}

// PendingOutboxEvents 返回尚未投递的 outbox 事件（供 dispatcher 使用）。
func (s *MemoryStore) PendingOutboxEvents() []OutboxEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]OutboxEvent, 0, len(s.outbox))
	for _, e := range s.outbox {
		if !s.delivered[e.EventID] {
			out = append(out, e)
		}
	}
	return out
}
