package dataexport

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// ObjectStore 模拟分片上传内容的对象存储（S3 风格）。
//
// 真实部署中替换为 S3/OSS 等实现即可：对象以 ObjectKey 寻址，摘要在服务端计算。
// Store 是并发安全的；GetObject 在对象不存在时返回 ErrObjectNotFound。
type ObjectStore struct {
	mu      sync.RWMutex
	objects map[string]*storedObject
}

type storedObject struct {
	data      []byte
	digest    string
	size      int64
	createdAt time.Time
	// refs 由存储层在清理流程中查询，这里仅保留对象本身信息；
	// 引用关系由 PersistentStore 的清单负责判定。
}

// NewObjectStore 创建一个空的对象存储。
func NewObjectStore() *ObjectStore {
	return &ObjectStore{objects: make(map[string]*storedObject)}
}

// PutObject 保存对象内容并返回服务端计算出的 SHA-256 摘要（十六进制）。
// 同一 key 重复写入会覆盖（测试 / 重试上传场景），但已被接受回执引用的 key
// 不会在正常流程中被再次使用——每个分片接受时对象键唯一。
func (s *ObjectStore) PutObject(key string, data []byte, now time.Time) (digest string, size int64) {
	sum := sha256.Sum256(data)
	digest = hex.EncodeToString(sum[:])
	s.mu.Lock()
	s.objects[key] = &storedObject{data: append([]byte(nil), data...), digest: digest, size: int64(len(data)), createdAt: now}
	s.mu.Unlock()
	return digest, int64(len(data))
}

// GetObject 返回对象内容与元数据。
func (s *ObjectStore) GetObject(key string) (data []byte, digest string, size int64, err error) {
	s.mu.RLock()
	o, ok := s.objects[key]
	s.mu.RUnlock()
	if !ok {
		return nil, "", 0, ErrObjectNotFound
	}
	return append([]byte(nil), o.data...), o.digest, o.size, nil
}

// StatObject 只返回摘要与大小，不拷贝内容（回执校验路径使用）。
func (s *ObjectStore) StatObject(key string) (digest string, size int64, err error) {
	s.mu.RLock()
	o, ok := s.objects[key]
	s.mu.RUnlock()
	if !ok {
		return "", 0, ErrObjectNotFound
	}
	return o.digest, o.size, nil
}

// DeleteObject 删除对象，返回是否实际删除了对象。
func (s *ObjectStore) DeleteObject(key string) bool {
	s.mu.Lock()
	_, ok := s.objects[key]
	delete(s.objects, key)
	s.mu.Unlock()
	return ok
}

// HasObject 报告对象是否存在（测试与清理校验使用）。
func (s *ObjectStore) HasObject(key string) bool {
	s.mu.RLock()
	_, ok := s.objects[key]
	s.mu.RUnlock()
	return ok
}

// Keys 返回当前所有对象键的快照（主要用于测试与清理审计）。
func (s *ObjectStore) Keys() []string {
	s.mu.RLock()
	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	return keys
}
