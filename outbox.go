package dataexport

import (
	"context"
	"sync"
	"time"
)

// Notifier 把 outbox 事件投递到外部世界（消息总线 / Webhook 等）。
// 实现必须是并发安全的；同 EventID 的事件可能被重复投递（至少一次语义），
// 接收端需按 EventID 幂等。
type Notifier interface {
	Notify(ctx context.Context, e OutboxEvent) error
}

// NotifierFunc 让普通函数满足 Notifier。
type NotifierFunc func(ctx context.Context, e OutboxEvent) error

// Notify 实现 Notifier。
func (f NotifierFunc) Notify(ctx context.Context, e OutboxEvent) error { return f(ctx, e) }

// OutboxDispatcher 轮询存储中的未投递 outbox 事件并交给 Notifier。
// 它把"状态与通知的原子写入"（事务内 outbox）与"实际外部投递"解耦：
// 服务崩溃重启后事件仍在 outbox 中，投递至少发生一次；确定性 EventID
// （taskID/事件类型）保证每个任务每种终态事件在逻辑上唯一。
type OutboxDispatcher struct {
	store    *MemoryStore
	notifier Notifier
	interval time.Duration
	now      func() time.Time

	stop chan struct{}
	done chan struct{}
	mu   sync.Mutex
	// delivered 缓存本进程已确认的事件 ID，避免对同一个 notifier 重复调用；
	// 跨进程的去重仍由接收端依据 EventID 完成。
	delivered map[string]bool
}

// NewOutboxDispatcher 创建投递器。interval 为轮询间隔。
func NewOutboxDispatcher(store *MemoryStore, notifier Notifier, interval time.Duration) *OutboxDispatcher {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	return &OutboxDispatcher{
		store:     store,
		notifier:  notifier,
		interval:  interval,
		now:       time.Now,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		delivered: map[string]bool{},
	}
}

// Run 阻塞运行直到 ctx 取消或 Stop 被调用，处理完当前批次后退出。
func (d *OutboxDispatcher) Run(ctx context.Context) {
	defer close(d.done)
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		d.drain(ctx)
		select {
		case <-ctx.Done():
			return
		case <-d.stop:
			return
		case <-ticker.C:
		}
	}
}

// Stop 通知 Run 退出并等待其结束。
func (d *OutboxDispatcher) Stop() {
	d.mu.Lock()
	select {
	case <-d.stop:
	default:
		close(d.stop)
	}
	d.mu.Unlock()
	<-d.done
}

// drain 投递一批当前待处理事件。Notifier 返回错误的事件保留未标记，下轮重试。
func (d *OutboxDispatcher) drain(ctx context.Context) {
	for _, e := range d.store.PendingOutboxEvents() {
		if d.delivered[e.EventID] {
			d.store.MarkOutboxDelivered(e.EventID)
			continue
		}
		if err := d.notifier.Notify(ctx, e); err != nil {
			// 投递失败：停止本批，等待下轮重试（保留至少一次语义）。
			return
		}
		d.delivered[e.EventID] = true
		d.store.MarkOutboxDelivered(e.EventID)
	}
}

// PendingCount 返回尚未被标记投递的事件数（测试 / 可观测性使用）。
func (d *OutboxDispatcher) PendingCount() int {
	return len(d.store.PendingOutboxEvents())
}
