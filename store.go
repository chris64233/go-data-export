package dataexport

import (
	"context"
	"sort"
	"sync"
)

// Store 是持久化状态抽象。Update 中的函数在事务内执行：
// 要么全部提交，要么全部回滚，且并发 Update 之间串行化。
// 所有"检查状态 + 多步写入"的竞态（完成/取消/过期）都依赖这一语义。
type Store interface {
	Update(ctx context.Context, fn func(tx *Tx) error) error
	View(ctx context.Context, fn func(tx *Tx) error) error
}

// db 是全部持久化状态。
type db struct {
	tasks       map[string]*Task
	taskByReqID map[string]string              // 外部请求号 -> 任务 ID
	shards      map[string]map[int]*Shard      // 任务 ID -> 分片序号 -> 分片
	receipts    map[string]map[string]*Receipt // 任务 ID -> 回执 ID -> 回执
	manifests   map[string]*Manifest           // 任务 ID -> 清单
	outbox      map[string]*OutboxEvent
	outboxOrder []string
}

func newDB() *db {
	return &db{
		tasks:       map[string]*Task{},
		taskByReqID: map[string]string{},
		shards:      map[string]map[int]*Shard{},
		receipts:    map[string]map[string]*Receipt{},
		manifests:   map[string]*Manifest{},
		outbox:      map[string]*OutboxEvent{},
	}
}

func cloneTask(t *Task) *Task {
	if t == nil {
		return nil
	}
	c := *t
	if t.CompletedAt != nil {
		v := *t.CompletedAt
		c.CompletedAt = &v
	}
	if t.DownloadExpiresAt != nil {
		v := *t.DownloadExpiresAt
		c.DownloadExpiresAt = &v
	}
	return &c
}

func cloneShard(s *Shard) *Shard {
	if s == nil {
		return nil
	}
	c := *s
	if s.AcceptedAt != nil {
		v := *s.AcceptedAt
		c.AcceptedAt = &v
	}
	return &c
}

func cloneReceipt(r *Receipt) *Receipt {
	if r == nil {
		return nil
	}
	c := *r
	return &c
}

func cloneManifest(m *Manifest) *Manifest {
	if m == nil {
		return nil
	}
	c := *m
	c.Files = append([]ManifestFile(nil), m.Files...)
	return &c
}

func cloneOutboxEvent(e *OutboxEvent) *OutboxEvent {
	if e == nil {
		return nil
	}
	c := *e
	if e.DispatchedAt != nil {
		v := *e.DispatchedAt
		c.DispatchedAt = &v
	}
	return &c
}

// clone 深拷贝整个库，用于 copy-on-write 事务。
func (d *db) clone() *db {
	c := newDB()
	for k, v := range d.tasks {
		c.tasks[k] = cloneTask(v)
	}
	for k, v := range d.taskByReqID {
		c.taskByReqID[k] = v
	}
	for k, m := range d.shards {
		cm := make(map[int]*Shard, len(m))
		for i, s := range m {
			cm[i] = cloneShard(s)
		}
		c.shards[k] = cm
	}
	for k, m := range d.receipts {
		cm := make(map[string]*Receipt, len(m))
		for i, r := range m {
			cm[i] = cloneReceipt(r)
		}
		c.receipts[k] = cm
	}
	for k, v := range d.manifests {
		c.manifests[k] = cloneManifest(v)
	}
	for k, v := range d.outbox {
		c.outbox[k] = cloneOutboxEvent(v)
	}
	c.outboxOrder = append([]string(nil), d.outboxOrder...)
	return c
}

// Tx 是事务句柄，只能在 Store.Update / Store.View 的回调内使用。
// 读返回的是事务内私有副本，调用方不得保留指针跨事务使用。
type Tx struct {
	d *db
}

func (tx *Tx) GetTask(id string) *Task {
	return cloneTask(tx.d.tasks[id])
}

func (tx *Tx) GetTaskByRequestID(requestID string) *Task {
	id, ok := tx.d.taskByReqID[requestID]
	if !ok {
		return nil
	}
	return cloneTask(tx.d.tasks[id])
}

func (tx *Tx) PutTask(t *Task) {
	tx.d.tasks[t.ID] = cloneTask(t)
	tx.d.taskByReqID[t.ExternalRequestID] = t.ID
}

func (tx *Tx) ListTasks() []*Task {
	out := make([]*Task, 0, len(tx.d.tasks))
	for _, t := range tx.d.tasks {
		out = append(out, cloneTask(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (tx *Tx) GetShard(taskID string, index int) *Shard {
	return cloneShard(tx.d.shards[taskID][index])
}

func (tx *Tx) PutShard(s *Shard) {
	m := tx.d.shards[s.TaskID]
	if m == nil {
		m = map[int]*Shard{}
		tx.d.shards[s.TaskID] = m
	}
	m[s.Index] = cloneShard(s)
}

// ListShards 按分片序号升序返回任务的全部分片。
func (tx *Tx) ListShards(taskID string) []*Shard {
	m := tx.d.shards[taskID]
	out := make([]*Shard, 0, len(m))
	for _, s := range m {
		out = append(out, cloneShard(s))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

func (tx *Tx) GetReceipt(taskID, receiptID string) *Receipt {
	return cloneReceipt(tx.d.receipts[taskID][receiptID])
}

func (tx *Tx) PutReceipt(r *Receipt) {
	m := tx.d.receipts[r.TaskID]
	if m == nil {
		m = map[string]*Receipt{}
		tx.d.receipts[r.TaskID] = m
	}
	m[r.ReceiptID] = cloneReceipt(r)
}

func (tx *Tx) GetManifest(taskID string) *Manifest {
	return cloneManifest(tx.d.manifests[taskID])
}

func (tx *Tx) PutManifest(m *Manifest) {
	tx.d.manifests[m.TaskID] = cloneManifest(m)
}

func (tx *Tx) AppendOutbox(e *OutboxEvent) {
	tx.d.outbox[e.ID] = cloneOutboxEvent(e)
	tx.d.outboxOrder = append(tx.d.outboxOrder, e.ID)
}

// ListOutbox 按写入顺序返回全部 outbox 事件。
func (tx *Tx) ListOutbox() []*OutboxEvent {
	out := make([]*OutboxEvent, 0, len(tx.d.outboxOrder))
	for _, id := range tx.d.outboxOrder {
		out = append(out, cloneOutboxEvent(tx.d.outbox[id]))
	}
	return out
}

func (tx *Tx) PutOutboxEvent(e *OutboxEvent) {
	tx.d.outbox[e.ID] = cloneOutboxEvent(e)
}

// MemStore 是 Store 的事务内存实现：Update 在互斥锁内对库的
// 深拷贝执行，成功后整体换入，失败则丢弃，天然具备回滚与串行化语义。
type MemStore struct {
	mu sync.Mutex
	d  *db
}

func NewMemStore() *MemStore {
	return &MemStore{d: newDB()}
}

func (s *MemStore) Update(_ context.Context, fn func(tx *Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	work := s.d.clone()
	if err := fn(&Tx{d: work}); err != nil {
		return err
	}
	s.d = work
	return nil
}

func (s *MemStore) View(_ context.Context, fn func(tx *Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(&Tx{d: s.d.clone()})
}
