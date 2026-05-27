package timeline

import (
	"path/filepath"
	"sync"

	"github.com/gofrs/flock"
	"github.com/pkg/errors"
)

// fileKeyLocker hands out per-key advisory file locks backed by flock(2).
//
// It is used to serialize the cache's check->fetch->store sequence for a single
// key ACROSS PROCESSES (within a process the cache already serializes via its
// in-memory Fetch mutex). Each key gets a dedicated sidecar "<key>.lock" file in
// the cache directory — never the backing .db, to avoid fighting sqlite's own
// locking. Different keys use different lock files and never block each other.
//
// A fresh *flock.Flock (and thus a fresh file descriptor) is opened per
// acquisition. On Linux a flock placed via one open file description blocks one
// placed via another, so this serializes even two goroutines of the same
// process; the OS releases the lock automatically if the process dies, so there
// are no stale locks to recover.
type fileKeyLocker struct {
	dir string
}

func newFileKeyLocker(dir string) *fileKeyLocker {
	return &fileKeyLocker{dir: dir}
}

// LockKey blocks until the exclusive advisory lock for keyStr is held and
// returns an unlock func (safe to call once, typically via defer).
func (l *fileKeyLocker) LockKey(keyStr string) (unlock func(), err error) {
	lockPath := filepath.Join(l.dir, keyStr+".lock")

	fl := flock.New(lockPath)
	if err := fl.Lock(); err != nil {
		return nil, errors.Wrapf(err, "failed to acquire file lock %v", lockPath)
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			_ = fl.Unlock()
		})
	}, nil
}
