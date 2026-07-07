package timeline

import (
	"sync/atomic"
	"testing"
	"time"
)

// closableStorage is a minimal CacheStorage that also implements Close(),
// used to verify CacheBase.Close() propagates to a closable storage backend.
type closableStorage[Data any, Key any] struct {
	closed atomic.Bool
}

func (s *closableStorage[Data, Key]) Load(key Key) (*CacheState[Data], error) { return nil, nil }
func (s *closableStorage[Data, Key]) Save(key Key, state *CacheState[Data], updated []*CacheStateSegment[Data]) error {
	return nil
}
func (s *closableStorage[Data, Key]) Add(key Key, periodStart, periodEnd time.Time, data []Data) (CacheData[Data], error) {
	return nil, nil
}
func (s *closableStorage[Data, Key]) Close() { s.closed.Store(true) }

// TestCacheBase_Close_PropagatesToStorage guards the fix for the leak where
// CacheBase.Close() was an empty body and never reached a closable storage
// (sqliteCacheStorage.Close runs PRAGMA optimize + sqlDB.Close on every conn —
// skipped, so DB handles/WAL leaked for the process lifetime).
func TestCacheBase_Close_PropagatesToStorage(t *testing.T) {
	st := &closableStorage[struct{}, int64]{}
	c := NewCacheBase[struct{}, int64](CacheBaseOptions[struct{}, int64]{
		Storage: st,
	})

	if st.closed.Load() {
		t.Fatal("precondition: storage must not be closed before CacheBase.Close()")
	}

	c.Close()

	if !st.closed.Load() {
		t.Fatal("CacheBase.Close() did not propagate to the closable storage backend")
	}
}

// TestCacheBase_Close_NonClosableStorageNoop verifies Close() is a safe no-op
// for storage backends that do not implement Close (e.g. in-memory).
func TestCacheBase_Close_NonClosableStorageNoop(t *testing.T) {
	c := NewCacheBase[struct{}, int64](CacheBaseOptions[struct{}, int64]{
		Storage: &memoryCacheStorage[struct{}, int64]{},
	})
	c.Close() // must not panic
}
