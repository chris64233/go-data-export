package dataexport

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testClock 是可手动推进的时钟。
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type fixture struct {
	svc     *Service
	store   *MemStore
	objects *MemObjectStore
	clock   *testClock
	seq     int
}

func newFixture() *fixture {
	f := &fixture{
		store:   NewMemStore(),
		objects: NewMemObjectStore(),
		clock:   newTestClock(),
	}
	f.svc = NewService(f.store, f.objects, Options{
		LeaseTTL:    time.Minute,
		DownloadTTL: time.Hour,
		Now:         f.clock.Now,
		NewID: func() string {
			f.seq++
			return fmt.Sprintf("id-%d", f.seq)
		},
		SnapshotWatermark: func() int64 { return 777 },
	})
	return f
}

func (f *fixture) createTask(t *testing.T, reqID string, shards int) *Task {
	t.Helper()
	res, err := f.svc.CreateTask(context.Background(), CreateTaskRequest{
		ExternalRequestID: reqID,
		Range:             ExportRange{FromID: "u-000", ToID: "u-999"},
		ShardCount:        shards,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	return res.Task
}

// claimAndSubmit 领取一个分片并提交回执。
func (f *fixture) claimAndSubmit(t *testing.T, taskID, worker, receiptID string) *ReceiptResult {
	t.Helper()
	lease, err := f.svc.ClaimShard(context.Background(), taskID, worker)
	if err != nil {
		t.Fatalf("ClaimShard: %v", err)
	}
	res, err := f.svc.SubmitReceipt(context.Background(), SubmitReceiptRequest{
		TaskID:           taskID,
		ShardIndex:       lease.ShardIndex,
		ReceiptID:        receiptID,
		LeaseID:          lease.LeaseID,
		FencingToken:     lease.FencingToken,
		ExecutionVersion: lease.ExecutionVersion,
		Watermark:        lease.Watermark,
		Digest:           fmt.Sprintf("digest-shard-%d", lease.ShardIndex),
		ObjectKey:        fmt.Sprintf("exports/%s/part-%04d.bin", taskID, lease.ShardIndex),
	})
	if err != nil {
		t.Fatalf("SubmitReceipt: %v", err)
	}
	return res
}

func TestCreateTask_IdempotencyAndConflict(t *testing.T) {
	f := newFixture()
	ctx := context.Background()

	req := CreateTaskRequest{
		ExternalRequestID: "req-1",
		Range:             ExportRange{FromID: "u-000", ToID: "u-999"},
		ShardCount:        4,
	}
	first, err := f.svc.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	if !first.Created {
		t.Fatal("first create should report Created=true")
	}
	if first.Task.Watermark != 777 {
		t.Fatalf("watermark not pinned at creation: %d", first.Task.Watermark)
	}

	// 同号同范围：幂等返回原任务。
	second, err := f.svc.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("replay create: %v", err)
	}
	if second.Created || second.Task.ID != first.Task.ID {
		t.Fatalf("replay should return original task, got %+v", second)
	}

	// 同号改范围：必须冲突。
	req.Range.ToID = "u-500"
	if _, err := f.svc.CreateTask(ctx, req); !IsCode(err, CodeTaskConflict) {
		t.Fatalf("changed range must conflict, got %v", err)
	}

	// 同号改分片数：同样冲突。
	req.Range.ToID = "u-999"
	req.ShardCount = 8
	if _, err := f.svc.CreateTask(ctx, req); !IsCode(err, CodeTaskConflict) {
		t.Fatalf("changed shard count must conflict, got %v", err)
	}

	// 创建时固定的分片集合。
	task, err := f.svc.GetTask(ctx, first.Task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if task.ShardCount != 4 || task.Status != TaskStatusRunning || task.ExecutionVersion != 1 {
		t.Fatalf("unexpected task: %+v", task)
	}
}

func TestClaimShard_LeaseAndFencing(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	task := f.createTask(t, "req-1", 1)

	lease1, err := f.svc.ClaimShard(ctx, task.ID, "w1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if lease1.Watermark != task.Watermark || lease1.ExecutionVersion != 1 {
		t.Fatalf("lease must carry pinned watermark and version: %+v", lease1)
	}

	// 租约未过期时同一分片不可重复领取。
	if _, err := f.svc.ClaimShard(ctx, task.ID, "w2"); !IsCode(err, CodeNoShardAvailable) {
		t.Fatalf("second claim must fail, got %v", err)
	}

	// 租约过期后可被重新领取，围栏令牌递增。
	f.clock.Advance(2 * time.Minute)
	lease2, err := f.svc.ClaimShard(ctx, task.ID, "w2")
	if err != nil {
		t.Fatalf("re-claim after expiry: %v", err)
	}
	if lease2.FencingToken != lease1.FencingToken+1 || lease2.LeaseID == lease1.LeaseID {
		t.Fatalf("fencing token must increase, got %+v vs %+v", lease2, lease1)
	}

	// 旧租约提交回执：拒绝。
	_, err = f.svc.SubmitReceipt(ctx, SubmitReceiptRequest{
		TaskID: task.ID, ShardIndex: 0, ReceiptID: "r-old",
		LeaseID: lease1.LeaseID, FencingToken: lease1.FencingToken,
		ExecutionVersion: 1, Watermark: task.Watermark,
		Digest: "d", ObjectKey: "k",
	})
	if !IsCode(err, CodeLeaseMismatch) {
		t.Fatalf("stale lease receipt must be rejected, got %v", err)
	}

	// 当前租约但已过期：拒绝。
	f.clock.Advance(2 * time.Minute)
	_, err = f.svc.SubmitReceipt(ctx, SubmitReceiptRequest{
		TaskID: task.ID, ShardIndex: 0, ReceiptID: "r-exp",
		LeaseID: lease2.LeaseID, FencingToken: lease2.FencingToken,
		ExecutionVersion: 1, Watermark: task.Watermark,
		Digest: "d", ObjectKey: "k",
	})
	if !IsCode(err, CodeLeaseExpired) {
		t.Fatalf("expired lease receipt must be rejected, got %v", err)
	}
}

func TestSubmitReceipt_Validation(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	task := f.createTask(t, "req-1", 2)
	lease, err := f.svc.ClaimShard(ctx, task.ID, "w1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	base := SubmitReceiptRequest{
		TaskID: task.ID, ShardIndex: lease.ShardIndex, ReceiptID: "r-1",
		LeaseID: lease.LeaseID, FencingToken: lease.FencingToken,
		ExecutionVersion: lease.ExecutionVersion, Watermark: lease.Watermark,
		Digest: "d", ObjectKey: "k",
	}

	// 执行版本不匹配。
	bad := base
	bad.ExecutionVersion = 99
	if _, err := f.svc.SubmitReceipt(ctx, bad); !IsCode(err, CodeVersionMismatch) {
		t.Fatalf("version mismatch expected, got %v", err)
	}

	// 快照水位不匹配：分片不属于该快照。
	bad = base
	bad.Watermark = 123
	if _, err := f.svc.SubmitReceipt(ctx, bad); !IsCode(err, CodeWatermarkMismatch) {
		t.Fatalf("watermark mismatch expected, got %v", err)
	}

	// 分片不存在。
	bad = base
	bad.ShardIndex = 42
	if _, err := f.svc.SubmitReceipt(ctx, bad); !IsCode(err, CodeShardNotFound) {
		t.Fatalf("shard not found expected, got %v", err)
	}

	// 任务不存在。
	bad = base
	bad.TaskID = "nope"
	if _, err := f.svc.SubmitReceipt(ctx, bad); !IsCode(err, CodeTaskNotFound) {
		t.Fatalf("task not found expected, got %v", err)
	}
}

func TestSubmitReceipt_ReplayAndNoOverwrite(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	task := f.createTask(t, "req-1", 1)
	lease, err := f.svc.ClaimShard(ctx, task.ID, "w1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	req := SubmitReceiptRequest{
		TaskID: task.ID, ShardIndex: lease.ShardIndex, ReceiptID: "r-1",
		LeaseID: lease.LeaseID, FencingToken: lease.FencingToken,
		ExecutionVersion: lease.ExecutionVersion, Watermark: lease.Watermark,
		Digest: "digest-a", ObjectKey: "obj-a",
	}

	first, err := f.svc.SubmitReceipt(ctx, req)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if first.Duplicate || !first.TaskCompleted {
		t.Fatalf("first receipt should complete the task: %+v", first)
	}

	// 重放同一回执：返回原结果，即使任务已完成。
	replay, err := f.svc.SubmitReceipt(ctx, req)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Duplicate || !replay.TaskCompleted {
		t.Fatalf("replay must return original result: %+v", replay)
	}

	// 同一回执号、不同内容：冲突。
	conflicting := req
	conflicting.Digest = "digest-b"
	if _, err := f.svc.SubmitReceipt(ctx, conflicting); !IsCode(err, CodeReceiptConflict) {
		t.Fatalf("receipt id reuse with different content must conflict, got %v", err)
	}

	// 新回执号、不同摘要：不得覆盖已接受内容。
	overwrite := req
	overwrite.ReceiptID = "r-2"
	overwrite.Digest = "digest-c"
	overwrite.ObjectKey = "obj-c"
	if _, err := f.svc.SubmitReceipt(ctx, overwrite); !IsCode(err, CodeReceiptConflict) {
		t.Fatalf("overwrite of accepted shard must be rejected, got %v", err)
	}

	// 已接受内容未被覆盖。
	manifest, err := f.svc.GetManifest(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if manifest.Files[0].Digest != "digest-a" || manifest.Files[0].ObjectKey != "obj-a" {
		t.Fatalf("accepted content was overwritten: %+v", manifest.Files[0])
	}
}

func TestFinalize_ConcurrentLastShards_SingleManifest(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	const shards = 8
	task := f.createTask(t, "req-1", shards)

	// 预先领取全部分片。
	leases := make([]*Lease, shards)
	for i := 0; i < shards; i++ {
		l, err := f.svc.ClaimShard(ctx, task.ID, fmt.Sprintf("w-%d", i))
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		leases[i] = l
	}

	// 并发提交全部回执，多个工作者会同时"完成最后一个分片"。
	var wg sync.WaitGroup
	errs := make([]error, shards)
	completed := make([]bool, shards)
	for i := 0; i < shards; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := f.svc.SubmitReceipt(ctx, SubmitReceiptRequest{
				TaskID: task.ID, ShardIndex: leases[i].ShardIndex, ReceiptID: fmt.Sprintf("r-%d", i),
				LeaseID: leases[i].LeaseID, FencingToken: leases[i].FencingToken,
				ExecutionVersion: leases[i].ExecutionVersion, Watermark: leases[i].Watermark,
				Digest:    fmt.Sprintf("d-%d", i),
				ObjectKey: fmt.Sprintf("k-%d", i),
			})
			errs[i] = err
			if err == nil {
				completed[i] = res.TaskCompleted
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("receipt %d: %v", i, err)
		}
	}

	// 恰好一个回执观察到"任务因我完成"。
	count := 0
	for _, c := range completed {
		if c {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("exactly one receipt should finalize the task, got %d", count)
	}

	// 恰好一条完成通知。
	events, err := f.svc.PendingOutboxEvents(ctx)
	if err != nil {
		t.Fatalf("outbox: %v", err)
	}
	if len(events) != 1 || events[0].Type != OutboxEventTaskCompleted || events[0].FileCount != shards {
		t.Fatalf("exactly one completion event expected, got %+v", events)
	}

	// 清单不可变且对象已物化。
	manifest, err := f.svc.GetManifest(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if len(manifest.Files) != shards || manifest.Watermark != task.Watermark {
		t.Fatalf("bad manifest: %+v", manifest)
	}
	if !f.objects.Exists(ctx, manifest.ObjectKey) {
		t.Fatal("manifest object must be materialized")
	}
	if events[0].ManifestHash != manifest.Digest {
		t.Fatal("outbox event must reference the manifest digest")
	}
}

func TestCancel_RacesWithCompletion_SingleTerminalState(t *testing.T) {
	// 反复跑取消与最后回执的竞争，验证终态唯一。
	for round := 0; round < 50; round++ {
		f := newFixture()
		ctx := context.Background()
		task := f.createTask(t, "req-1", 1)
		lease, err := f.svc.ClaimShard(ctx, task.ID, "w1")
		if err != nil {
			t.Fatalf("claim: %v", err)
		}

		var wg sync.WaitGroup
		var receiptErr, cancelErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, receiptErr = f.svc.SubmitReceipt(ctx, SubmitReceiptRequest{
				TaskID: task.ID, ShardIndex: 0, ReceiptID: "r-1",
				LeaseID: lease.LeaseID, FencingToken: lease.FencingToken,
				ExecutionVersion: lease.ExecutionVersion, Watermark: lease.Watermark,
				Digest: "d", ObjectKey: "k",
			})
		}()
		go func() {
			defer wg.Done()
			cancelErr = f.svc.CancelTask(ctx, task.ID)
		}()
		wg.Wait()

		final, err := f.svc.GetTask(ctx, task.ID)
		if err != nil {
			t.Fatalf("GetTask: %v", err)
		}
		switch final.Status {
		case TaskStatusSucceeded:
			if receiptErr != nil {
				t.Fatalf("round %d: succeeded but receipt failed: %v", round, receiptErr)
			}
			if !IsCode(cancelErr, CodeTaskNotRunning) {
				t.Fatalf("round %d: cancel after completion must be rejected, got %v", round, cancelErr)
			}
		case TaskStatusCancelled:
			if cancelErr != nil {
				t.Fatalf("round %d: cancelled but cancel failed: %v", round, cancelErr)
			}
			if !IsCode(receiptErr, CodeTaskNotRunning) {
				t.Fatalf("round %d: receipt after cancel must be rejected, got %v", round, receiptErr)
			}
			// 取消后停止新领取。
			if _, err := f.svc.ClaimShard(ctx, task.ID, "w2"); !IsCode(err, CodeTaskNotRunning) {
				t.Fatalf("round %d: claim after cancel must be rejected, got %v", round, err)
			}
		default:
			t.Fatalf("round %d: unexpected status %s", round, final.Status)
		}

		// 终态唯一：至多一条完成通知。
		events, err := f.svc.PendingOutboxEvents(ctx)
		if err != nil {
			t.Fatalf("outbox: %v", err)
		}
		if final.Status == TaskStatusSucceeded && len(events) != 1 {
			t.Fatalf("round %d: succeeded task must have exactly one event", round)
		}
		if final.Status == TaskStatusCancelled && len(events) != 0 {
			t.Fatalf("round %d: cancelled task must have no event", round)
		}
	}
}

func TestCancel_Idempotent(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	task := f.createTask(t, "req-1", 2)

	if err := f.svc.CancelTask(ctx, task.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := f.svc.CancelTask(ctx, task.ID); err != nil {
		t.Fatalf("re-cancel must be idempotent: %v", err)
	}
	if err := f.svc.CancelTask(ctx, "missing"); !IsCode(err, CodeTaskNotFound) {
		t.Fatalf("cancel missing task: %v", err)
	}
}

func TestManifest_ExpiryAndDownloadRejection(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	task := f.createTask(t, "req-1", 1)
	f.claimAndSubmit(t, task.ID, "w1", "r-1")

	if _, err := f.svc.GetManifest(ctx, task.ID); err != nil {
		t.Fatalf("manifest must be downloadable: %v", err)
	}

	// 超过下载期限：拒绝下载。
	f.clock.Advance(2 * time.Hour)
	if _, err := f.svc.GetManifest(ctx, task.ID); !IsCode(err, CodeManifestExpired) {
		t.Fatalf("expired download must be rejected, got %v", err)
	}

	// 未完成任务读清单。
	other := f.createTask(t, "req-2", 1)
	if _, err := f.svc.GetManifest(ctx, other.ID); !IsCode(err, CodeManifestNotReady) {
		t.Fatalf("unfinished task manifest must not be ready, got %v", err)
	}
}

func TestCleanupExpired_ProtectsReferencedObjects(t *testing.T) {
	f := newFixture()
	ctx := context.Background()

	// 任务 A：短下载期，即将过期。
	taskA := f.createTask(t, "req-a", 2)
	leaseA0, _ := f.svc.ClaimShard(ctx, taskA.ID, "w1")
	leaseA1, _ := f.svc.ClaimShard(ctx, taskA.ID, "w1")
	// 分片 0 的对象键与任务 B 共享（模拟内容寻址去重）。
	sharedKey := "shared/dedup-object.bin"
	submit := func(taskID string, lease *Lease, receiptID, key string) {
		t.Helper()
		// 模拟工作者先把分片文件上传到对象存储，再提交回执。
		if err := f.objects.Put(ctx, key, []byte("data-"+key)); err != nil {
			t.Fatal(err)
		}
		_, err := f.svc.SubmitReceipt(ctx, SubmitReceiptRequest{
			TaskID: taskID, ShardIndex: lease.ShardIndex, ReceiptID: receiptID,
			LeaseID: lease.LeaseID, FencingToken: lease.FencingToken,
			ExecutionVersion: lease.ExecutionVersion, Watermark: lease.Watermark,
			Digest: "d-" + key, ObjectKey: key,
		})
		if err != nil {
			t.Fatalf("submit %s: %v", receiptID, err)
		}
	}
	submit(taskA.ID, leaseA0, "r-a0", sharedKey)
	submit(taskA.ID, leaseA1, "r-a1", "exports/a/part-1.bin")

	// 半小时后任务 B 才完成，因此 A 过期时 B 的清单仍然有效。
	f.clock.Advance(30 * time.Minute)
	taskB := f.createTask(t, "req-b", 1)
	leaseB, _ := f.svc.ClaimShard(ctx, taskB.ID, "w1")
	submit(taskB.ID, leaseB, "r-b0", sharedKey)

	// 任务 C：已取消，留有已上传对象。
	taskC := f.createTask(t, "req-c", 1)
	leaseC, _ := f.svc.ClaimShard(ctx, taskC.ID, "w1")
	orphanKey := "exports/c/orphan.bin"
	if err := f.objects.Put(ctx, orphanKey, []byte("partial")); err != nil {
		t.Fatal(err)
	}
	_ = leaseC
	if err := f.svc.CancelTask(ctx, taskC.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// 让 A 过期（A 完成于 t=0、1 小时下载期；B 完成于 t=30min，此刻仍有效）。
	f.clock.Advance(45 * time.Minute)
	report, err := f.svc.CleanupExpired(ctx)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if len(report.ExpiredTasks) != 1 || report.ExpiredTasks[0] != taskA.ID {
		t.Fatalf("only task A should expire: %+v", report)
	}

	// 仍被有效清单（任务 B）引用的共享对象必须保留。
	if !f.objects.Exists(ctx, sharedKey) {
		t.Fatal("object referenced by a valid manifest must not be removed")
	}
	// A 独有的对象与清单已删除。
	if f.objects.Exists(ctx, "exports/a/part-1.bin") {
		t.Fatal("expired task's own object must be deleted")
	}
	if f.objects.Exists(ctx, fmt.Sprintf("exports/%s/manifest.json", taskA.ID)) {
		t.Fatal("expired manifest object must be deleted")
	}
	// B 的清单与对象完好。
	if _, err := f.svc.GetManifest(ctx, taskB.ID); err != nil {
		t.Fatalf("valid manifest must survive cleanup: %v", err)
	}
	// 过期任务下载被拒绝。
	if _, err := f.svc.GetManifest(ctx, taskA.ID); !IsCode(err, CodeManifestExpired) {
		t.Fatalf("expired task download must be rejected, got %v", err)
	}
}

func TestRecover_AfterCrash(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	task := f.createTask(t, "req-1", 2)
	f.claimAndSubmit(t, task.ID, "w1", "r-1")
	res := f.claimAndSubmit(t, task.ID, "w2", "r-2")
	if !res.TaskCompleted {
		t.Fatal("task should be completed")
	}

	// 模拟崩溃：状态已提交但清单对象丢失。
	manifest, err := f.svc.GetManifest(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if err := f.objects.Delete(ctx, manifest.ObjectKey); err != nil {
		t.Fatal(err)
	}

	if err := f.svc.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if !f.objects.Exists(ctx, manifest.ObjectKey) {
		t.Fatal("Recover must re-materialize the manifest object")
	}

	// Recover 幂等，且不产生重复的完成通知。
	if err := f.svc.Recover(ctx); err != nil {
		t.Fatalf("Recover again: %v", err)
	}
	events, err := f.svc.PendingOutboxEvents(ctx)
	if err != nil {
		t.Fatalf("outbox: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("Recover must not duplicate completion events, got %d", len(events))
	}
}

func TestOutbox_DispatchFlow(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	task := f.createTask(t, "req-1", 1)
	f.claimAndSubmit(t, task.ID, "w1", "r-1")

	events, err := f.svc.PendingOutboxEvents(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("one pending event expected, got %d", len(events))
	}
	if err := f.svc.MarkOutboxDispatched(ctx, []string{events[0].ID}); err != nil {
		t.Fatalf("mark dispatched: %v", err)
	}
	events, err = f.svc.PendingOutboxEvents(ctx)
	if err != nil {
		t.Fatalf("pending after dispatch: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("no pending events expected, got %d", len(events))
	}
}
