package goservicedrain

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store 持久化协调器快照。
// Save 必须在状态提交前调用成功；实现需保证快照原子可见。
type Store interface {
	Load(ctx context.Context) (*Snapshot, error)
	Save(ctx context.Context, snap *Snapshot) error
}

// MemoryStore 内存存储，主要用于测试。
type MemoryStore struct {
	mu   sync.Mutex
	snap *Snapshot
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (m *MemoryStore) Load(ctx context.Context) (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snap == nil {
		return newSnapshot(), nil
	}
	return cloneSnapshot(m.snap), nil
}

func (m *MemoryStore) Save(ctx context.Context, snap *Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snap = cloneSnapshot(snap)
	return nil
}

// FileStore 将快照以 JSON 原子写入文件：先写临时文件再 rename，避免半截文件。
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore 在 path 位置持久化快照。
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (f *FileStore) Load(ctx context.Context) (*Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return newSnapshot(), nil
		}
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	if len(data) == 0 {
		return newSnapshot(), nil
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("decode snapshot %s: %w", f.path, err)
	}
	normalizeSnapshot(&snap)
	return &snap, nil
}

func (f *FileStore) Save(ctx context.Context, snap *Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("commit snapshot: %w", err)
	}
	return nil
}

func newSnapshot() *Snapshot {
	s := &Snapshot{
		Instances: map[string]*Instance{},
		NextSeq:   1,
	}
	return s
}

// normalizeSnapshot 兼容老数据/空 map。
func normalizeSnapshot(s *Snapshot) {
	if s.Instances == nil {
		s.Instances = map[string]*Instance{}
	}
	for _, in := range s.Instances {
		if in.Sessions == nil {
			in.Sessions = map[string]*Session{}
		}
	}
	if s.NextSeq == 0 {
		s.NextSeq = 1
	}
}

func cloneSnapshot(s *Snapshot) *Snapshot {
	data, err := json.Marshal(s)
	if err != nil {
		// Snapshot 只含可 JSON 序列化的字段，不应发生。
		panic(fmt.Sprintf("snapshot clone: %v", err))
	}
	var out Snapshot
	if err := json.Unmarshal(data, &out); err != nil {
		panic(fmt.Sprintf("snapshot clone: %v", err))
	}
	normalizeSnapshot(&out)
	return &out
}
