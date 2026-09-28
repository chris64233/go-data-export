// Command dataexportd 组装并启动数据导出服务：
// HTTP 接口 + outbox 投递器 + 周期性过期扫描 / 清理。
//
// 这是面向接线方式的参考实现（存储与对象存储均为进程内实现）；生产部署替换为
// 数据库版 PersistentStore 与 S3/OSS 版对象存储即可，业务代码无需改动。
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	dataexport "github.com/chris64233/go-data-export"
)

func main() {
	cfg := dataexport.DefaultConfig()
	store := dataexport.NewMemoryStore(time.Now)
	objects := dataexport.NewObjectStore()
	svc := dataexport.NewService(store, objects, cfg, time.Now)

	// outbox：这里的 Notifier 仅记录日志；实际接入消息总线 / Webhook。
	notifier := dataexport.NotifierFunc(func(_ context.Context, e dataexport.OutboxEvent) error {
		log.Printf("outbox event: id=%s task=%s type=%s payload=%v", e.EventID, e.TaskID, e.Type, e.Payload)
		return nil
	})
	dispatcher := dataexport.NewOutboxDispatcher(store, notifier, time.Second)

	handler := dataexport.NewHTTPHandler(svc)
	srv := &http.Server{
		Addr:              ":8080",
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go dispatcher.Run(ctx)

	// 维护循环：周期性终结过期任务、清理过保留期的对象。
	maintDone := make(chan struct{})
	go func() {
		defer close(maintDone)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ids, err := svc.ExpireDueTasks(); err != nil {
					log.Printf("expire scan error: %v", err)
				} else if len(ids) > 0 {
					log.Printf("expired tasks: %v", ids)
				}
				if res, err := svc.CleanupExpired(); err != nil {
					log.Printf("cleanup error: %v", err)
				} else if len(res.DeletedObjects) > 0 {
					log.Printf("cleanup deleted=%d skipped=%d", len(res.DeletedObjects), len(res.SkippedReferenced))
				}
			}
		}
	}()

	go func() {
		log.Printf("dataexportd listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	dispatcher.Stop()
	<-maintDone
	os.Exit(0)
}
