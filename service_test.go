package dataexport_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	dataexport "github.com/chris64233/go-data-export"
)

// ---- 测试基础设施 ----

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	return c.t
}

type env struct {
	clock   *fakeClock
	store   *dataexport.MemoryStore
	objects *dataexport.ObjectStore
	svc     *dataexport.Service
}

func newEnv(t *testing.T, cfg dataexport.Config) *env {
	t.Helper()
	clock := newFakeClock()
	objects := dataexport.NewObjectStore()
	store := dataexport.NewMemoryStore(clock.Now)
	if cfg == (dataexport.Config{}) {
		cfg = dataexport.Config{
			LeaseTTL:          time.Hour,
			TaskTTL:           24 * time.Hour,
			ManifestRetention: 7 * 24 * time.Hour,
		}
	}
	return &env{
		clock:   clock,
		store:   store,
		objects: objects,
		svc:     dataexport.NewService(store, objects, cfg, clock.Now),
	}
}

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func scope() dataexport.Scope {
	return dataexport.Scope{UserID: "user-1", Resource: "messages", Start: 100, End: 200}
}

// createTaskWithContents 以每段内容构造期望摘要并创建任务，返回任务 ID 与内容切片。
func createTaskWithContents(t *testing.T, e *env, requestID string, contents ...[]byte) (string, [][]byte) {
	t.Helper()
	specs := make([]dataexport.ShardSpec, len(contents))
	for i, c := range contents {
		specs[i] = dataexport.ShardSpec{Index: i, Watermark: 42, ExpectedDigest: digestOf(c)}
	}
	task, err := e.svc.CreateTask(dataexport.CreateTaskInput{
		RequestID: requestID,
		Scope:     scope(),
		Watermark: 42,
		Shards:    specs,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	return task.ID, contents
}

func upload(t *testing.T, e *env, taskID string, idx int, content []byte) string {
	t.Helper()
	key := fmt.Sprintf("objects/%s/shard-%d.bin", taskID, idx)
	e.objects.PutObject(key, content, e.clock.Now())
	return key
}

// claimAndSubmit 领取指定任务的下一个分片，上传 content 并提交回执。
func claimAndSubmit(t *testing.T, e *env, taskID, worker string, idx int, content []byte) dataexport.ReceiptResult {
	t.Helper()
	c, err := e.svc.ClaimShardOf(taskID, worker)
	if err != nil {
		t.Fatalf("ClaimShardOf: %v", err)
	}
	if c.Index != idx {
		t.Fatalf("claimed shard %d, want %d", c.Index, idx)
	}
	key := upload(t, e, taskID, idx, content)
	res, err := e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID:           taskID,
		Index:            idx,
		ExecutionVersion: c.ExecutionVersion,
		LeaseGeneration:  c.Lease.Generation,
		WorkerID:         worker,
		ObjectKey:        key,
	})
	if err != nil {
		t.Fatalf("SubmitReceipt: %v", err)
	}
	return res
}

func completeTask(t *testing.T, e *env, taskID string, contents [][]byte) dataexport.Manifest {
	t.Helper()
	var last dataexport.ReceiptResult
	for i, c := range contents {
		last = claimAndSubmit(t, e, taskID, fmt.Sprintf("w-%d", i), i, c)
	}
	if !last.Completed || last.Manifest == nil {
		t.Fatalf("final receipt did not complete task")
	}
	if len(last.Manifest.Entries) != len(contents) {
		t.Fatalf("manifest has %d entries, want %d", len(last.Manifest.Entries), len(contents))
	}
	return *last.Manifest
}

// ---- 1. 创建：幂等、冲突、水位固定 ----

func TestCreateTaskIdempotent(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	in := dataexport.CreateTaskInput{
		RequestID: "req-1",
		Scope:     scope(),
		Watermark: 42,
		Shards:    []dataexport.ShardSpec{{Index: 0, Watermark: 42, ExpectedDigest: "aa"}},
	}
	t1, err := e.svc.CreateTask(in)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	t2, err := e.svc.CreateTask(in)
	if err != nil {
		t.Fatalf("CreateTask replay: %v", err)
	}
	if t1.ID != t2.ID {
		t.Fatalf("idempotent re-create produced different task id: %s vs %s", t1.ID, t2.ID)
	}
	if t2.Watermark != 42 || t2.ExecutionVersion != 1 {
		t.Fatalf("unexpected task: %+v", t2)
	}
}

func TestCreateTaskSameRequestIDDifferentScopeConflict(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	t1, err := e.svc.CreateTask(dataexport.CreateTaskInput{
		RequestID: "req-1", Scope: scope(), Watermark: 1,
		Shards: []dataexport.ShardSpec{{Index: 0, Watermark: 1, ExpectedDigest: "aa"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	changed := scope()
	changed.End = 999 // 同号改变范围
	_, err = e.svc.CreateTask(dataexport.CreateTaskInput{
		RequestID: "req-1", Scope: changed, Watermark: 1,
		Shards: []dataexport.ShardSpec{{Index: 0, Watermark: 1, ExpectedDigest: "aa"}},
	})
	if !errors.Is(err, dataexport.ErrRequestConflict) {
		t.Fatalf("want ErrRequestConflict, got %v", err)
	}

	// 冲突不得影响原任务。
	got, err := e.svc.GetTask(t1.ID)
	if err != nil || got.Scope.End != 200 {
		t.Fatalf("original task mutated after conflict: %+v err=%v", got, err)
	}
}

func TestCreateTaskValidation(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	base := func() dataexport.CreateTaskInput {
		return dataexport.CreateTaskInput{
			RequestID: "req-1", Scope: scope(), Watermark: 1,
			Shards: []dataexport.ShardSpec{{Index: 0, Watermark: 1, ExpectedDigest: "aa"}},
		}
	}

	cases := []struct {
		name string
		mut  func(*dataexport.CreateTaskInput)
		want error
	}{
		{"missing request id", func(in *dataexport.CreateTaskInput) { in.RequestID = "" }, dataexport.ErrInvalidArgument},
		{"missing user", func(in *dataexport.CreateTaskInput) { in.Scope.UserID = "" }, dataexport.ErrInvalidArgument},
		{"bad watermark", func(in *dataexport.CreateTaskInput) { in.Watermark = 0 }, dataexport.ErrInvalidArgument},
		{"no shards", func(in *dataexport.CreateTaskInput) { in.Shards = nil }, dataexport.ErrInvalidArgument},
		{"duplicate index", func(in *dataexport.CreateTaskInput) {
			in.Shards = []dataexport.ShardSpec{
				{Index: 0, Watermark: 1, ExpectedDigest: "aa"},
				{Index: 0, Watermark: 1, ExpectedDigest: "bb"},
			}
		}, dataexport.ErrInvalidArgument},
		{"shard watermark mismatch", func(in *dataexport.CreateTaskInput) {
			in.Shards[0].Watermark = 2
		}, dataexport.ErrSnapshotMismatch},
		{"missing digest", func(in *dataexport.CreateTaskInput) {
			in.Shards[0].ExpectedDigest = ""
		}, dataexport.ErrInvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := base()
			tc.mut(&in)
			if _, err := e.svc.CreateTask(in); !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

func TestGetTaskNotFound(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	if _, err := e.svc.GetTask("nope"); !errors.Is(err, dataexport.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// ---- 2. 领取 / 回执 / 清单 主流程 ----

func TestHappyPathManifestAndDownload(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	contents := [][]byte{[]byte("shard-0"), []byte("shard-1"), []byte("shard-2")}
	taskID, _ := createTaskWithContents(t, e, "req-1", contents...)
	m := completeTask(t, e, taskID, contents)

	if m.Watermark != 42 {
		t.Fatalf("manifest watermark = %d, want 42", m.Watermark)
	}
	for i, c := range contents {
		if m.Entries[i].Index != i || m.Entries[i].Digest != digestOf(c) {
			t.Fatalf("bad manifest entry %d: %+v", i, m.Entries[i])
		}
	}

	// 恰好一条完成通知，确定性 EventID。
	pending := e.store.PendingOutboxEvents()
	if len(pending) != 1 || pending[0].Type != dataexport.EventTaskCompleted ||
		pending[0].EventID != taskID+"/task.completed" {
		t.Fatalf("unexpected outbox: %+v", pending)
	}

	got, err := e.svc.GetManifest(taskID)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if len(got.Entries) != 3 {
		t.Fatalf("manifest entries = %d", len(got.Entries))
	}
	for i, c := range contents {
		data, err := e.svc.GetShardObject(taskID, i)
		if err != nil || !bytes.Equal(data, c) {
			t.Fatalf("GetShardObject(%d) = %q, %v", i, data, err)
		}
	}
}

func TestClaimDistributesShards(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	taskID, _ := createTaskWithContents(t, e, "req-1", []byte("a"), []byte("b"), []byte("c"))

	got := map[int]string{}
	for i := 0; i < 3; i++ {
		c, err := e.svc.ClaimShard(fmt.Sprintf("worker-%d", i))
		if err != nil {
			t.Fatalf("ClaimShard: %v", err)
		}
		if c.TaskID != taskID {
			t.Fatalf("claimed task %s, want %s", c.TaskID, taskID)
		}
		got[c.Index] = c.Lease.WorkerID
		if c.ExecutionVersion != 1 || c.Lease.Generation != 1 {
			t.Fatalf("unexpected claim: %+v", c)
		}
	}
	if len(got) != 3 {
		t.Fatalf("claims = %v, want 3 distinct shards", got)
	}
	// 第四次无分片可领。
	if _, err := e.svc.ClaimShard("worker-x"); !errors.Is(err, dataexport.ErrNoShardAvailable) {
		t.Fatalf("want ErrNoShardAvailable, got %v", err)
	}
}

func TestReceiptReplayReturnsOriginalResult(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	contents := [][]byte{[]byte("only")}
	taskID, _ := createTaskWithContents(t, e, "req-1", contents...)

	c, err := e.svc.ClaimShardOf(taskID, "w1")
	if err != nil {
		t.Fatal(err)
	}
	key := upload(t, e, taskID, 0, contents[0])
	in := dataexport.ReceiptInput{
		TaskID: taskID, Index: 0,
		ExecutionVersion: c.ExecutionVersion, LeaseGeneration: c.Lease.Generation,
		WorkerID: "w1", ObjectKey: key,
	}
	first, err := e.svc.SubmitReceipt(in)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Accepted || !first.Completed {
		t.Fatalf("first receipt = %+v, want accepted+completed", first)
	}
	// 同回执重放：返回原结果，不再产生通知。
	replay, err := e.svc.SubmitReceipt(in)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Accepted {
		t.Fatalf("replay must report Accepted=false")
	}
	if !replay.Completed || replay.Manifest == nil || replay.Receipt.ObjectKey != key {
		t.Fatalf("replay result = %+v", replay)
	}
	if len(e.store.PendingOutboxEvents()) != 1 {
		t.Fatalf("outbox should still contain exactly 1 event, got %d", len(e.store.PendingOutboxEvents()))
	}
	// 取消之后重放同一回执，仍然返回原结果。
	if _, err := e.svc.CancelTask(taskID, "x"); !errors.Is(err, dataexport.ErrTaskTerminal) {
		t.Fatalf("canceling completed task: want ErrTaskTerminal, got %v", err)
	}
	replay2, err := e.svc.SubmitReceipt(in)
	if err != nil || replay2.Accepted || replay2.Manifest == nil {
		t.Fatalf("replay after terminal = %+v, %v", replay2, err)
	}
}

// ---- 3. 旧租约 / 错误版本 / 摘要 不得覆盖 ----

func TestOldLeaseReceiptRejected(t *testing.T) {
	e := newEnv(t, dataexport.Config{
		LeaseTTL:          time.Minute,
		TaskTTL:           time.Hour,
		ManifestRetention: time.Hour,
	})
	content := []byte("hello")
	taskID, _ := createTaskWithContents(t, e, "req-1", content)

	c1, err := e.svc.ClaimShardOf(taskID, "w1")
	if err != nil {
		t.Fatal(err)
	}

	key := upload(t, e, taskID, 0, content)

	// 租约到期后，w1 持原代际提交 -> ErrLeaseExpired。
	e.clock.Advance(2 * time.Minute)
	_, err = e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: c1.ExecutionVersion,
		LeaseGeneration: c1.Lease.Generation, WorkerID: "w1", ObjectKey: key,
	})
	if !errors.Is(err, dataexport.ErrLeaseExpired) {
		t.Fatalf("expired lease receipt: want ErrLeaseExpired, got %v", err)
	}

	// w2 重新领取：租约代际 +1，w1 的旧租约立即成为 stale。
	c2, err := e.svc.ClaimShardOf(taskID, "w2")
	if err != nil {
		t.Fatal(err)
	}
	if c2.Lease.Generation != 2 {
		t.Fatalf("generation = %d, want 2", c2.Lease.Generation)
	}
	_, err = e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: c1.ExecutionVersion,
		LeaseGeneration: c1.Lease.Generation, WorkerID: "w1", ObjectKey: key,
	})
	if !errors.Is(err, dataexport.ErrLeaseStale) {
		t.Fatalf("old lease receipt: want ErrLeaseStale, got %v", err)
	}

	// 新租约持有者的合法回执被接受。
	res, err := e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: c2.ExecutionVersion,
		LeaseGeneration: c2.Lease.Generation, WorkerID: "w2", ObjectKey: key,
	})
	if err != nil || !res.Accepted {
		t.Fatalf("valid receipt = %+v, %v", res, err)
	}

	// 已接受后，旧租约 / 不同对象的回执不能覆盖。
	otherKey := fmt.Sprintf("objects/%s/shard-0-other.bin", taskID) // 同内容、不同键
	e.objects.PutObject(otherKey, content, e.clock.Now())
	cases := []dataexport.ReceiptInput{
		{TaskID: taskID, Index: 0, ExecutionVersion: 1, LeaseGeneration: 1, WorkerID: "w1", ObjectKey: key},
		{TaskID: taskID, Index: 0, ExecutionVersion: 1, LeaseGeneration: 2, WorkerID: "w2", ObjectKey: otherKey},
		{TaskID: taskID, Index: 0, ExecutionVersion: 1, LeaseGeneration: 2, WorkerID: "someone-else", ObjectKey: key},
	}
	for i, in := range cases {
		if _, err := e.svc.SubmitReceipt(in); !errors.Is(err, dataexport.ErrShardAlreadyCompleted) {
			t.Fatalf("case %d: want ErrShardAlreadyCompleted, got %v", i, err)
		}
	}
}

func TestWrongExecutionVersionRejected(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	taskID, _ := createTaskWithContents(t, e, "req-1", []byte("x"))
	c, err := e.svc.ClaimShardOf(taskID, "w1")
	if err != nil {
		t.Fatal(err)
	}
	key := upload(t, e, taskID, 0, []byte("x"))
	_, err = e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: 99,
		LeaseGeneration: c.Lease.Generation, WorkerID: "w1", ObjectKey: key,
	})
	if !errors.Is(err, dataexport.ErrVersionMismatch) {
		t.Fatalf("want ErrVersionMismatch, got %v", err)
	}
}

func TestLeaseExpiredReceiptRejectedAndReclaim(t *testing.T) {
	e := newEnv(t, dataexport.Config{
		LeaseTTL:          time.Minute,
		TaskTTL:           time.Hour,
		ManifestRetention: time.Hour,
	})
	taskID, _ := createTaskWithContents(t, e, "req-1", []byte("x"))
	c1, err := e.svc.ClaimShardOf(taskID, "w1")
	if err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(2 * time.Minute)

	key := upload(t, e, taskID, 0, []byte("x"))
	_, err = e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: c1.ExecutionVersion,
		LeaseGeneration: c1.Lease.Generation, WorkerID: "w1", ObjectKey: key,
	})
	if !errors.Is(err, dataexport.ErrLeaseExpired) {
		t.Fatalf("want ErrLeaseExpired, got %v", err)
	}

	// 过期分片可被重新领取，新租约回执成功。
	c2, err := e.svc.ClaimShardOf(taskID, "w2")
	if err != nil || c2.Lease.Generation != 2 {
		t.Fatalf("reclaim = %+v, %v", c2, err)
	}
	res, err := e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: c2.ExecutionVersion,
		LeaseGeneration: c2.Lease.Generation, WorkerID: "w2", ObjectKey: key,
	})
	if err != nil || !res.Completed {
		t.Fatalf("receipt after reclaim = %+v, %v", res, err)
	}
}

func TestDigestMismatchAndMissingObject(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	taskID, _ := createTaskWithContents(t, e, "req-1", []byte("expected"))
	c, err := e.svc.ClaimShardOf(taskID, "w1")
	if err != nil {
		t.Fatal(err)
	}

	// 对象不存在。
	_, err = e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: c.ExecutionVersion,
		LeaseGeneration: c.Lease.Generation, WorkerID: "w1", ObjectKey: "missing",
	})
	if !errors.Is(err, dataexport.ErrObjectNotFound) {
		t.Fatalf("want ErrObjectNotFound, got %v", err)
	}

	// 摘要不匹配：内容与快照水位上的期望摘要不同。
	badKey := upload(t, e, taskID, 0, []byte("different content"))
	_, err = e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: c.ExecutionVersion,
		LeaseGeneration: c.Lease.Generation, WorkerID: "w1", ObjectKey: badKey,
	})
	if !errors.Is(err, dataexport.ErrDigestMismatch) {
		t.Fatalf("want ErrDigestMismatch, got %v", err)
	}

	// 失败后分片仍可重新提交正确回执。
	goodKey := upload(t, e, taskID, 0, []byte("expected"))
	res, err := e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: c.ExecutionVersion,
		LeaseGeneration: c.Lease.Generation, WorkerID: "w1", ObjectKey: goodKey,
	})
	if err != nil || !res.Completed {
		t.Fatalf("correct receipt after failures = %+v, %v", res, err)
	}
}

// ---- 4. 并发：单清单、单终态 ----

func TestConcurrentCompletionSingleManifest(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	const n = 16
	contents := make([][]byte, n)
	specs := make([]dataexport.ShardSpec, n)
	for i := 0; i < n; i++ {
		contents[i] = []byte(fmt.Sprintf("shard-%d", i))
		specs[i] = dataexport.ShardSpec{Index: i, Watermark: 7, ExpectedDigest: digestOf(contents[i])}
	}
	task, err := e.svc.CreateTask(dataexport.CreateTaskInput{
		RequestID: "req-conc", Scope: scope(), Watermark: 7, Shards: specs,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 每个工作者领取一个分片（领取本身串行化，索引必然互不相同）。
	type work struct {
		claim dataexport.Claim
		key   string
	}
	works := make([]work, n)
	got := map[int]bool{}
	for i := 0; i < n; i++ {
		c, err := e.svc.ClaimShardOf(task.ID, fmt.Sprintf("w-%d", i))
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if got[c.Index] {
			t.Fatalf("shard %d claimed twice", c.Index)
		}
		got[c.Index] = true
		works[i] = work{claim: c, key: upload(t, e, task.ID, c.Index, contents[c.Index])}
	}

	// 并发提交所有回执，只有最后一个提交者能完成任务。
	var wg sync.WaitGroup
	start := make(chan struct{})
	var completed int64
	var mu sync.Mutex
	for _, wk := range works {
		wk := wk
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := e.svc.SubmitReceipt(dataexport.ReceiptInput{
				TaskID: task.ID, Index: wk.claim.Index,
				ExecutionVersion: wk.claim.ExecutionVersion,
				LeaseGeneration:  wk.claim.Lease.Generation,
				WorkerID:         wk.claim.Lease.WorkerID, ObjectKey: wk.key,
			})
			if err != nil {
				t.Errorf("receipt: %v", err)
				return
			}
			if res.Completed {
				mu.Lock()
				completed++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if completed != 1 {
		t.Fatalf("completion observed by %d receipts, want exactly 1", completed)
	}
	gotTask, err := e.svc.GetTask(task.ID)
	if err != nil || gotTask.Status != dataexport.StatusCompleted {
		t.Fatalf("task = %+v, %v", gotTask, err)
	}
	m, err := e.svc.GetManifest(task.ID)
	if err != nil || len(m.Entries) != n {
		t.Fatalf("manifest = %+v, %v", m, err)
	}
	events := e.store.PendingOutboxEvents()
	if len(events) != 1 || events[0].Type != dataexport.EventTaskCompleted {
		t.Fatalf("outbox = %+v, want single completed event", events)
	}
}

func TestConcurrentClaimsExactlyShardCount(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	const n = 30
	contents := make([][]byte, n)
	for i := range contents {
		contents[i] = []byte(fmt.Sprintf("c-%d", i))
	}
	taskID, _ := createTaskWithContents(t, e, "req-1", contents...)

	var wg sync.WaitGroup
	start := make(chan struct{})
	var mu sync.Mutex
	claims := map[int]int{}
	var noAvail int
	for i := 0; i < n*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			c, err := e.svc.ClaimShard("w")
			if errors.Is(err, dataexport.ErrNoShardAvailable) {
				mu.Lock()
				noAvail++
				mu.Unlock()
				return
			}
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			mu.Lock()
			claims[c.Index]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if len(claims) != n {
		t.Fatalf("%d distinct shards claimed, want %d", len(claims), n)
	}
	for idx, count := range claims {
		if count != 1 {
			t.Fatalf("shard %d claimed %d times, want exactly 1", idx, count)
		}
	}
	if noAvail != n {
		t.Fatalf("%d no-available results, want %d", noAvail, n)
	}
	_ = taskID
}

// TestCancelRacesCompletion 重复制造"最后回执 vs 取消"竞争，断言每次只有一个终态。
func TestCancelRacesCompletion(t *testing.T) {
	for iter := 0; iter < 30; iter++ {
		e := newEnv(t, dataexport.Config{})
		taskID, _ := createTaskWithContents(t, e, fmt.Sprintf("req-%d", iter), []byte("a"), []byte("b"))

		// 先完成分片 0。
		claimAndSubmit(t, e, taskID, "w0", 0, []byte("a"))
		// 分片 1 已被领取，对象已上传，回执与取消同时竞争。
		c, err := e.svc.ClaimShardOf(taskID, "w1")
		if err != nil {
			t.Fatal(err)
		}
		key := upload(t, e, taskID, 1, []byte("b"))

		start := make(chan struct{})
		var wg sync.WaitGroup
		var cancelErr, receiptErr error
		var receiptRes dataexport.ReceiptResult
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, cancelErr = e.svc.CancelTask(taskID, "race")
		}()
		go func() {
			defer wg.Done()
			<-start
			receiptRes, receiptErr = e.svc.SubmitReceipt(dataexport.ReceiptInput{
				TaskID: taskID, Index: 1, ExecutionVersion: c.ExecutionVersion,
				LeaseGeneration: c.Lease.Generation, WorkerID: "w1", ObjectKey: key,
			})
		}()
		close(start)
		wg.Wait()

		task, err := e.svc.GetTask(taskID)
		if err != nil {
			t.Fatal(err)
		}
		events := e.store.PendingOutboxEvents()
		terminalEvents := 0
		for _, ev := range events {
			if ev.Type == dataexport.EventTaskCompleted || ev.Type == dataexport.EventTaskCanceled {
				terminalEvents++
			}
		}
		if terminalEvents != 1 {
			t.Fatalf("iter %d: %d terminal events, want 1 (events=%v)", iter, terminalEvents, events)
		}

		switch task.Status {
		case dataexport.StatusCompleted:
			if receiptErr != nil || !receiptRes.Completed {
				t.Fatalf("iter %d: completed but receipt err=%v res=%+v", iter, receiptErr, receiptRes)
			}
			if cancelErr == nil || !errors.Is(cancelErr, dataexport.ErrTaskTerminal) {
				t.Fatalf("iter %d: completed task cancel result = %v", iter, cancelErr)
			}
		case dataexport.StatusCanceled:
			if receiptErr == nil || !errors.Is(receiptErr, dataexport.ErrTaskCanceled) {
				t.Fatalf("iter %d: canceled task but receipt err=%v", iter, receiptErr)
			}
			if cancelErr != nil {
				t.Fatalf("iter %d: cancel err=%v", iter, cancelErr)
			}
		default:
			t.Fatalf("iter %d: unexpected status %q", iter, task.Status)
		}
		if !task.Status.Terminal() {
			t.Fatalf("iter %d: task not terminal", iter)
		}
	}
}

// ---- 5. 取消语义 ----

func TestCancelStopsClaimsAndIsIdempotent(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	taskID, _ := createTaskWithContents(t, e, "req-1", []byte("a"), []byte("b"))
	if _, err := e.svc.ClaimShardOf(taskID, "w1"); err != nil {
		t.Fatal(err)
	}

	canceled, err := e.svc.CancelTask(taskID, "user-requested")
	if err != nil || canceled.Status != dataexport.StatusCanceled || canceled.ExecutionVersion != 2 {
		t.Fatalf("cancel = %+v, %v", canceled, err)
	}

	// 取消后停止新领取。
	if _, err := e.svc.ClaimShardOf(taskID, "w2"); !errors.Is(err, dataexport.ErrTaskCanceled) {
		t.Fatalf("claim after cancel: want ErrTaskCanceled, got %v", err)
	}
	// 全局领取也不会再选中该任务（没有其他任务 => 无分片可领）。
	if _, err := e.svc.ClaimShard("w2"); !errors.Is(err, dataexport.ErrNoShardAvailable) {
		t.Fatalf("global claim after cancel: want ErrNoShardAvailable, got %v", err)
	}

	// 取消幂等：再次取消返回同一任务，不产生第二条通知。
	again, err := e.svc.CancelTask(taskID, "again")
	if err != nil || again.Status != dataexport.StatusCanceled {
		t.Fatalf("second cancel = %+v, %v", again, err)
	}
	events := e.store.PendingOutboxEvents()
	if len(events) != 1 || events[0].Type != dataexport.EventTaskCanceled {
		t.Fatalf("outbox = %+v, want single canceled event", events)
	}
}

func TestReceiptAfterCancelRejected(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	taskID, _ := createTaskWithContents(t, e, "req-1", []byte("a"))
	c, err := e.svc.ClaimShardOf(taskID, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.CancelTask(taskID, "n/a"); err != nil {
		t.Fatal(err)
	}
	key := upload(t, e, taskID, 0, []byte("a"))
	// 即使带着递增后的新版本，终态也先拒绝。
	_, err = e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: c.ExecutionVersion + 1,
		LeaseGeneration: c.Lease.Generation, WorkerID: "w1", ObjectKey: key,
	})
	if !errors.Is(err, dataexport.ErrTaskCanceled) {
		t.Fatalf("want ErrTaskCanceled, got %v", err)
	}
}

// ---- 6. 过期 ----

func TestExpireDueTasks(t *testing.T) {
	e := newEnv(t, dataexport.Config{
		LeaseTTL:          time.Hour,
		TaskTTL:           time.Hour,
		ManifestRetention: 24 * time.Hour,
	})
	taskID, _ := createTaskWithContents(t, e, "req-1", []byte("a"))
	c, err := e.svc.ClaimShardOf(taskID, "w1")
	if err != nil {
		t.Fatal(err)
	}

	e.clock.Advance(2 * time.Hour)
	if _, err := e.svc.ClaimShardOf(taskID, "w2"); !errors.Is(err, dataexport.ErrTaskExpired) {
		t.Fatalf("claim past deadline: want ErrTaskExpired, got %v", err)
	}

	ids, err := e.svc.ExpireDueTasks()
	if err != nil || len(ids) != 1 || ids[0] != taskID {
		t.Fatalf("ExpireDueTasks = %v, %v", ids, err)
	}
	task, _ := e.svc.GetTask(taskID)
	if task.Status != dataexport.StatusExpired {
		t.Fatalf("status = %s", task.Status)
	}
	events := e.store.PendingOutboxEvents()
	if len(events) != 1 || events[0].Type != dataexport.EventTaskExpired {
		t.Fatalf("outbox = %+v", events)
	}
	// 重复扫描不得重复终结 / 重复通知。
	if ids, _ := e.svc.ExpireDueTasks(); len(ids) != 0 {
		t.Fatalf("second expire scan = %v, want empty", ids)
	}
	// 过期后在途回执被拒绝、清单不存在。
	key := upload(t, e, taskID, 0, []byte("a"))
	_, err = e.svc.SubmitReceipt(dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: c.ExecutionVersion,
		LeaseGeneration: c.Lease.Generation, WorkerID: "w1", ObjectKey: key,
	})
	if !errors.Is(err, dataexport.ErrTaskExpired) {
		t.Fatalf("receipt after expiry: want ErrTaskExpired, got %v", err)
	}
	if _, err := e.svc.GetManifest(taskID); !errors.Is(err, dataexport.ErrManifestNotFound) {
		t.Fatalf("manifest after expiry: want ErrManifestNotFound, got %v", err)
	}
}

func TestCompletionBeforeDeadlineNotExpired(t *testing.T) {
	e := newEnv(t, dataexport.Config{
		LeaseTTL:          time.Hour,
		TaskTTL:           time.Hour,
		ManifestRetention: 24 * time.Hour,
	})
	taskID, contents := createTaskWithContents(t, e, "req-1", []byte("a"))
	completeTask(t, e, taskID, contents)
	e.clock.Advance(2 * time.Hour)
	if ids, _ := e.svc.ExpireDueTasks(); len(ids) != 0 {
		t.Fatalf("completed task expired: %v", ids)
	}
}

// ---- 7. 清单过期下载拒绝 ----

func TestManifestExpiredRejectsDownload(t *testing.T) {
	e := newEnv(t, dataexport.Config{
		LeaseTTL:          time.Hour,
		TaskTTL:           24 * time.Hour,
		ManifestRetention: time.Hour,
	})
	taskID, contents := createTaskWithContents(t, e, "req-1", []byte("payload"))
	completeTask(t, e, taskID, contents)

	if _, err := e.svc.GetManifest(taskID); err != nil {
		t.Fatalf("manifest within retention: %v", err)
	}
	e.clock.Advance(2 * time.Hour)
	if _, err := e.svc.GetManifest(taskID); !errors.Is(err, dataexport.ErrManifestExpired) {
		t.Fatalf("want ErrManifestExpired, got %v", err)
	}
	if _, err := e.svc.GetShardObject(taskID, 0); !errors.Is(err, dataexport.ErrManifestExpired) {
		t.Fatalf("download after retention: want ErrManifestExpired, got %v", err)
	}
}

// ---- 8. 清理：过期对象删除、有效清单引用保护、在途保护 ----

func TestCleanupDeletesExpiredManifestObjects(t *testing.T) {
	e := newEnv(t, dataexport.Config{
		LeaseTTL:          time.Hour,
		TaskTTL:           24 * time.Hour,
		ManifestRetention: time.Hour,
	})
	idA, cA := createTaskWithContents(t, e, "req-a", []byte("a1"), []byte("a2"))
	idB, cB := createTaskWithContents(t, e, "req-b", []byte("b1"))
	completeTask(t, e, idA, cA)
	e.clock.Advance(30 * time.Minute)
	completeTask(t, e, idB, cB)
	e.clock.Advance(45 * time.Minute) // A 过期（75min>60min），B 仍在保留期（45min<60min）

	// B 的对象必须保留；A 的对象被删。
	for _, key := range e.objects.Keys() {
		t.Logf("object before cleanup: %s", key)
	}
	res, err := e.svc.CleanupExpired()
	if err != nil {
		t.Fatal(err)
	}
	if len(res.ExpiredManifests) != 1 || res.ExpiredManifests[0] != idA {
		t.Fatalf("expired manifests = %v, want [%s]", res.ExpiredManifests, idA)
	}
	if len(res.DeletedObjects) != 2 {
		t.Fatalf("deleted = %v, want 2 objects", res.DeletedObjects)
	}
	if _, err := e.svc.GetShardObject(idB, 0); err != nil {
		t.Fatalf("valid manifest object must remain downloadable: %v", err)
	}
	if _, err := e.svc.GetManifest(idA); !errors.Is(err, dataexport.ErrManifestExpired) {
		t.Fatalf("A manifest: want expired, got %v", err)
	}
}

func TestCleanupProtectsObjectReferencedByValidManifest(t *testing.T) {
	e := newEnv(t, dataexport.Config{
		LeaseTTL:          time.Hour,
		TaskTTL:           24 * time.Hour,
		ManifestRetention: time.Hour,
	})

	// sharedKey 同时被两个清单引用：A（先过期）与 B（仍在保留期）。
	shared := []byte("shared-content")
	sharedKey := "objects/shared.bin"
	e.objects.PutObject(sharedKey, shared, e.clock.Now())
	// dOnlyKey 只被先过期的 A 同期任务 D 引用，应被实际删除。
	dOnly := []byte("d-only")
	dOnlyKey := "objects/d-only.bin"
	e.objects.PutObject(dOnlyKey, dOnly, e.clock.Now())

	completeOneShardTask := func(reqID, key string, content []byte) string {
		task, err := e.svc.CreateTask(dataexport.CreateTaskInput{
			RequestID: reqID, Scope: scope(), Watermark: 42,
			Shards: []dataexport.ShardSpec{{Index: 0, Watermark: 42, ExpectedDigest: digestOf(content)}},
		})
		if err != nil {
			t.Fatal(err)
		}
		c, err := e.svc.ClaimShardOf(task.ID, "w")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.svc.SubmitReceipt(dataexport.ReceiptInput{
			TaskID: task.ID, Index: 0, ExecutionVersion: c.ExecutionVersion,
			LeaseGeneration: c.Lease.Generation, WorkerID: "w", ObjectKey: key,
		}); err != nil {
			t.Fatal(err)
		}
		return task.ID
	}

	idA := completeOneShardTask("req-a", sharedKey, shared) // t=0 完成
	idD := completeOneShardTask("req-d", dOnlyKey, dOnly)   // t=0 完成
	e.clock.Advance(30 * time.Minute)
	idB := completeOneShardTask("req-b", sharedKey, shared) // t=30m 完成
	e.clock.Advance(45 * time.Minute)                       // t=75m：A、D 过期；B 仍在 1h 保留期内

	res, err := e.svc.CleanupExpired()
	if err != nil {
		t.Fatal(err)
	}
	// A、D 的清单都被判定过期。
	if len(res.ExpiredManifests) != 2 {
		t.Fatalf("expired manifests = %v, want 2", res.ExpiredManifests)
	}
	for _, want := range []string{idA, idD} {
		found := false
		for _, got := range res.ExpiredManifests {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("expired manifests = %v, want to contain %s", res.ExpiredManifests, want)
		}
	}
	// D 的独占对象被删除。
	if len(res.DeletedObjects) != 1 || res.DeletedObjects[0] != dOnlyKey {
		t.Fatalf("deleted = %v, want only %s", res.DeletedObjects, dOnlyKey)
	}
	// A 引用的共享对象因 B 的有效清单而被显式跳过、仍然存在。
	if len(res.SkippedReferenced) != 1 || res.SkippedReferenced[0] != sharedKey {
		t.Fatalf("skipped = %v, want [%s]", res.SkippedReferenced, sharedKey)
	}
	if e.objects.HasObject(dOnlyKey) {
		t.Fatalf("d-only object should have been deleted")
	}
	if !e.objects.HasObject(sharedKey) {
		t.Fatalf("shared object referenced by valid manifest must exist")
	}
	// 通过 B 的有效清单仍可下载共享对象。
	if data, err := e.svc.GetShardObject(idB, 0); err != nil || !bytes.Equal(data, shared) {
		t.Fatalf("download shared via valid manifest: data=%q err=%v", data, err)
	}
	_ = idA
}

func TestCleanupDoesNotTouchInFlightOrUnknownObjects(t *testing.T) {
	e := newEnv(t, dataexport.Config{
		LeaseTTL:          time.Hour,
		TaskTTL:           24 * time.Hour,
		ManifestRetention: time.Hour,
	})
	idCanceled, cc := createTaskWithContents(t, e, "req-c", []byte("done"), []byte("pending"))
	// 分片 0 已接受，分片 1 仍在途时取消：分片 0 的对象成为终结任务孤儿。
	claimAndSubmit(t, e, idCanceled, "w0", 0, cc[0])
	if _, err := e.svc.CancelTask(idCanceled, "give up"); err != nil {
		t.Fatal(err)
	}

	// 一个 pending 任务，工作者已上传对象但尚未提交回执（在途对象）。
	idPending, _ := createTaskWithContents(t, e, "req-p", []byte("inflight"))
	c, err := e.svc.ClaimShardOf(idPending, "w")
	if err != nil {
		t.Fatal(err)
	}
	inflightKey := upload(t, e, idPending, 0, []byte("inflight"))
	_ = c

	// 一个完全未知的对象键（无法关联任何任务）。
	unknownKey := "objects/foreign/unknown.bin"
	e.objects.PutObject(unknownKey, []byte("xxx"), e.clock.Now())

	res, err := e.svc.CleanupExpiredWithCandidates(
		append(e.objects.Keys(), inflightKey))
	if err != nil {
		t.Fatal(err)
	}
	// 取消任务的孤儿对象被清理。
	var orphanDeleted bool
	for _, k := range res.DeletedObjects {
		if strings.Contains(k, idCanceled) {
			orphanDeleted = true
		}
	}
	if !orphanDeleted {
		t.Fatalf("canceled-task orphan not deleted: %v", res.DeletedObjects)
	}
	// 在途对象与未知对象保留。
	if !e.objects.HasObject(inflightKey) {
		t.Fatalf("in-flight object of pending task must not be deleted")
	}
	if !e.objects.HasObject(unknownKey) {
		t.Fatalf("unknown object must not be deleted")
	}
}

func TestReceiptReplaySurvivesObjectCleanup(t *testing.T) {
	e := newEnv(t, dataexport.Config{
		LeaseTTL:          time.Hour,
		TaskTTL:           24 * time.Hour,
		ManifestRetention: time.Hour,
	})
	content := []byte("replay-me")
	taskID, _ := createTaskWithContents(t, e, "req-1", content)
	c, err := e.svc.ClaimShardOf(taskID, "w1")
	if err != nil {
		t.Fatal(err)
	}
	key := upload(t, e, taskID, 0, content)
	in := dataexport.ReceiptInput{
		TaskID: taskID, Index: 0, ExecutionVersion: c.ExecutionVersion,
		LeaseGeneration: c.Lease.Generation, WorkerID: "w1", ObjectKey: key,
	}
	if _, err := e.svc.SubmitReceipt(in); err != nil {
		t.Fatal(err)
	}

	// 保留期过后清理删除了对象。
	e.clock.Advance(2 * time.Hour)
	res, err := e.svc.CleanupExpired()
	if err != nil || len(res.DeletedObjects) != 1 {
		t.Fatalf("cleanup = %+v, %v", res, err)
	}
	if e.objects.HasObject(key) {
		t.Fatalf("object should have been deleted")
	}

	// 重放同一回执：不依赖对象仍存在，仍返回原结果。
	replay, err := e.svc.SubmitReceipt(in)
	if err != nil {
		t.Fatalf("replay after cleanup: %v", err)
	}
	if replay.Accepted || replay.Receipt.ObjectKey != key || replay.Receipt.Digest != digestOf(content) {
		t.Fatalf("replay result = %+v", replay)
	}
	if !replay.Completed || replay.Manifest == nil {
		t.Fatalf("replay must still surface original manifest: %+v", replay)
	}
}

// ---- 9. Outbox 投递 ----

type recordingNotifier struct {
	mu      sync.Mutex
	events  []dataexport.OutboxEvent
	failFor int // 前 failFor 次调用返回错误
	calls   int
}

func (n *recordingNotifier) Notify(_ context.Context, e dataexport.OutboxEvent) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls++
	if n.calls <= n.failFor {
		return errors.New("notifier temporarily unavailable")
	}
	n.events = append(n.events, e)
	return nil
}

func TestOutboxDispatcherAtLeastOnce(t *testing.T) {
	e := newEnv(t, dataexport.Config{})
	n := &recordingNotifier{failFor: 2}
	d := dataexport.NewOutboxDispatcher(e.store, n, time.Millisecond)

	id, contents := createTaskWithContents(t, e, "req-1", []byte("a"), []byte("b"))
	completeTask(t, e, id, contents)
	id2, _ := createTaskWithContents(t, e, "req-2", []byte("c"))
	if _, err := e.svc.CancelTask(id2, "test"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)
	defer cancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if d.PendingCount() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	d.Stop()

	if d.PendingCount() != 0 {
		t.Fatalf("%d events still pending after dispatch", d.PendingCount())
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.calls < 3 {
		t.Fatalf("notifier calls = %d, expected retries (>=3)", n.calls)
	}
	types := map[string]int{}
	for _, ev := range n.events {
		types[ev.Type]++
	}
	if types[dataexport.EventTaskCompleted] != 1 || types[dataexport.EventTaskCanceled] != 1 {
		t.Fatalf("delivered events = %v", n.events)
	}
}

// ---- 10. HTTP 接口 ----

func TestHTTPEndpoints(t *testing.T) {
	e := newEnv(t, dataexport.Config{
		LeaseTTL:          time.Hour,
		TaskTTL:           24 * time.Hour,
		ManifestRetention: time.Hour,
	})
	srv := httptest.NewServer(dataexport.NewHTTPHandler(e.svc))
	defer srv.Close()

	content := []byte("http-shard")
	body := map[string]any{
		"scope":     map[string]any{"user_id": "u1", "resource": "messages", "start": 1, "end": 2},
		"watermark": 9,
		"shards":    []map[string]any{{"index": 0, "watermark": 9, "expected_digest": digestOf(content)}},
	}
	raw, _ := json.Marshal(body)

	post := func(method, url string, reqBody any, headers map[string]string) (int, map[string]any, []byte) {
		var rdr io.Reader
		if reqBody != nil {
			b, err := json.Marshal(reqBody)
			if err != nil {
				t.Fatal(err)
			}
			rdr = bytes.NewReader(b)
		}
		req, err := http.NewRequest(method, url, rdr)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data := bytes.NewBuffer(nil)
		_, _ = data.ReadFrom(resp.Body)
		var parsed map[string]any
		_ = json.Unmarshal(data.Bytes(), &parsed)
		return resp.StatusCode, parsed, data.Bytes()
	}

	// 创建（幂等头）。
	status, created, _ := post(http.MethodPost, srv.URL+"/v1/tasks", json.RawMessage(raw),
		map[string]string{"X-Request-Id": "req-http", "Content-Type": "application/json"})
	if status != http.StatusCreated {
		t.Fatalf("create status = %d", status)
	}
	taskID := created["id"].(string)

	// 同号不同范围 => 409。
	conflictBody := map[string]any{
		"scope":     map[string]any{"user_id": "u1", "resource": "messages", "start": 1, "end": 999},
		"watermark": 9,
		"shards":    []map[string]any{{"index": 0, "watermark": 9, "expected_digest": digestOf(content)}},
	}
	status, parsed, _ := post(http.MethodPost, srv.URL+"/v1/tasks", conflictBody,
		map[string]string{"X-Request-Id": "req-http", "Content-Type": "application/json"})
	if status != http.StatusConflict || parsed["error"].(map[string]any)["code"] != string(dataexport.ErrCodeRequestConflict) {
		t.Fatalf("conflict status=%d body=%v", status, parsed)
	}

	// 领取。
	status, claimJSON, _ := post(http.MethodPost, srv.URL+"/v1/tasks/"+taskID+"/claims", nil,
		map[string]string{"X-Worker-Id": "w1"})
	if status != http.StatusOK {
		t.Fatalf("claim status = %d", status)
	}
	key := upload(t, e, taskID, 0, content)

	// 回执。
	receiptBody := map[string]any{
		"execution_version": int64(claimJSON["execution_version"].(float64)),
		"lease_generation":  int64(claimJSON["lease"].(map[string]any)["generation"].(float64)),
		"object_key":        key,
	}
	status, res, _ := post(http.MethodPost, srv.URL+"/v1/tasks/"+taskID+"/shards/0/receipts", receiptBody,
		map[string]string{"X-Worker-Id": "w1", "Content-Type": "application/json"})
	if status != http.StatusOK || res["completed"] != true {
		t.Fatalf("receipt status=%d body=%v", status, res)
	}

	// 读取清单。
	resp, err := http.Get(srv.URL + "/v1/tasks/" + taskID + "/manifest")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("manifest status = %d", resp.StatusCode)
	}

	// 下载对象。
	resp2, err := http.Get(srv.URL + "/v1/tasks/" + taskID + "/shards/0/object")
	if err != nil {
		t.Fatal(err)
	}
	data2 := bytes.NewBuffer(nil)
	_, _ = data2.ReadFrom(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK || !bytes.Equal(data2.Bytes(), content) {
		t.Fatalf("object status=%d body=%q", resp2.StatusCode, data2.Bytes())
	}

	// 清单过期 => 410。
	e.clock.Advance(2 * time.Hour)
	status, parsed, _ = post(http.MethodGet, srv.URL+"/v1/tasks/"+taskID+"/manifest", nil, nil)
	if status != http.StatusGone || parsed["error"].(map[string]any)["code"] != string(dataexport.ErrCodeManifestExpired) {
		t.Fatalf("expired manifest status=%d body=%v", status, parsed)
	}

	// 取消不存在的任务 => 404。
	status, _, _ = post(http.MethodPost, srv.URL+"/v1/tasks/nope/cancel", map[string]any{}, nil)
	if status != http.StatusNotFound {
		t.Fatalf("cancel missing status = %d, want 404", status)
	}
}
