package dataexport

import (
	"context"
	"errors"
	"sort"
	"sync"
)

// ErrObjectNotFound 表示对象存储中不存在该键。
var ErrObjectNotFound = errors.New("object not found")

// ObjectStore 是分片文件与清单对象的存储抽象。
// 对象按不可变内容写入（同一键重复 Put 相同内容必须幂等）。
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) bool
	Keys(ctx context.Context) []string
}

// MemObjectStore 是 ObjectStore 的内存实现。
type MemObjectStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func NewMemObjectStore() *MemObjectStore {
	return &MemObjectStore{data: map[string][]byte{}}
}

func (s *MemObjectStore) Put(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = append([]byte(nil), data...)
	return nil
}

func (s *MemObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return append([]byte(nil), v...), nil
}

func (s *MemObjectStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}

func (s *MemObjectStore) Exists(_ context.Context, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	return ok
}

func (s *MemObjectStore) Keys(_ context.Context) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.data))
	for k := range s.data {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
