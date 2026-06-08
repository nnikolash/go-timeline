package timeline_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	timeline "github.com/nnikolash/go-timeline"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newLockingSqliteCache builds a SqliteCache with CrossProcessLocking enabled,
// serving from a fixed dataset and counting source calls. Two instances over
// the same cacheDir simulate two processes: independent in-memory indexes,
// shared on-disk files and lock files.
func newLockingSqliteCache(t *testing.T, cacheDir string, data []TimelineData, sourceCalls *atomic.Int32) timeline.Cache[TimelineData, TimelineDataKey] {
	t.Helper()

	c, err := timeline.NewSqliteCache[TimelineData, TimelineDataKey, int64](timeline.SqliteCacheOptions[TimelineData, TimelineDataKey, int64]{
		CacheDir:            cacheDir,
		CrossProcessLocking: true,
		GetTimestamp:        func(d *TimelineData) time.Time { return d.Timestamp },
		GetID:               func(d *TimelineData) int64 { return d.ID },
		GetFromSource: func(_ TimelineDataKey, periodStart, periodEnd time.Time, _, _ *TimelineData, _ interface{}) (timeline.CacheStateSegment[TimelineData], error) {
			sourceCalls.Add(1)
			res := timeline.CacheStateSegment[TimelineData]{PeriodStart: periodStart, PeriodEnd: periodEnd, Data: []TimelineData{}}
			for _, v := range data {
				if v.Timestamp.Before(periodStart) || v.Timestamp.After(periodEnd) {
					continue
				}
				res.Data = append(res.Data, v)
			}
			return res, nil
		},
	})
	require.NoError(t, err)

	return c
}

// TestCache_Sqlite_WALEnabled verifies WAL is turned on unconditionally (even
// without CrossProcessLocking), so lock-free reads can coexist with writes.
func TestCache_Sqlite_WALEnabled(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	t0 := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	storage := map[string][]TimelineData{"key_1": {{Timestamp: t0, Value: "1", ID: 1}}}

	cache, _, _ := newSqliteCache(t, cacheDir, storage, nil, nil)
	k := TimelineDataKey{"key", 1}
	_, err := cache.Get(k, t0, t0, nil)
	require.NoError(t, err)
	cache.Close()

	dbs, err := filepath.Glob(filepath.Join(cacheDir, "*.db"))
	require.NoError(t, err)
	require.NotEmpty(t, dbs, "no sqlite db file created")

	db, err := gorm.Open(sqlite.Open(dbs[0]), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)

	var mode string
	require.NoError(t, db.Raw("PRAGMA journal_mode").Scan(&mode).Error)
	require.Equal(t, "wal", strings.ToLower(mode))
}

// TestCache_CrossProcessLock_LockFileNameMatchesDataFile asserts that the
// per-key cross-process lock file is named after the db file it protects:
// "<db file name>.lock" (sqlite_timeline_cache_<key>.db.lock), so lock and data
// share a prefix and sit next to each other in a directory listing. Before the
// fix the lock was the bare "<key>.lock", which looked unrelated to its db file.
func TestCache_CrossProcessLock_LockFileNameMatchesDataFile(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	key := TimelineDataKey{"key", 1}

	t0 := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	data := []TimelineData{{Timestamp: t0, Value: "1", ID: 1}}

	var sourceCalls atomic.Int32
	cache := newLockingSqliteCache(t, cacheDir, data, &sourceCalls)
	_, err := cache.Get(key, t0, t0, nil)
	require.NoError(t, err)
	cache.Close()

	dbs, err := filepath.Glob(filepath.Join(cacheDir, "sqlite_timeline_cache_*.db"))
	require.NoError(t, err)
	require.Len(t, dbs, 1, "expected exactly one per-key db file")
	dbName := filepath.Base(dbs[0])

	// The per-key lock must be exactly "<db file name>.lock".
	wantLock := dbName + ".lock"
	_, err = os.Stat(filepath.Join(cacheDir, wantLock))
	require.NoError(t, err, "expected lock file %q next to db file %q", wantLock, dbName)

	// No bare "<key>.lock" (db prefix missing) must exist anymore: every per-key
	// lock must start with the same prefix as the db files it guards.
	locks, err := filepath.Glob(filepath.Join(cacheDir, "*.lock"))
	require.NoError(t, err)
	for _, lp := range locks {
		base := filepath.Base(lp)
		require.True(t, strings.HasPrefix(base, "sqlite_timeline_cache"),
			"lock file %q does not share the cache prefix of its data files", base)
	}
}

// TestCache_CrossProcessLock_ReloadsIndexInsteadOfRefetching is the core test:
// an instance with a stale in-memory index must, under the cross-process lock,
// reload the on-disk index and serve a period another instance stored — rather
// than re-fetching it from source.
func TestCache_CrossProcessLock_ReloadsIndexInsteadOfRefetching(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	key := TimelineDataKey{"key", 1}

	tBase := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	mk := func(dayOffset int, id int64) TimelineData {
		return TimelineData{Timestamp: tBase.AddDate(0, 0, dayOffset), Value: fmt.Sprint(id), ID: id}
	}
	// P1 = days [0;2], P2 = days [10;12] — disjoint, so they stay two segments.
	data := []TimelineData{
		mk(0, 1), mk(1, 2), mk(2, 3),
		mk(10, 4), mk(11, 5), mk(12, 6),
	}
	p1Start, p1End := tBase, tBase.AddDate(0, 0, 2)
	p2Start, p2End := tBase.AddDate(0, 0, 10), tBase.AddDate(0, 0, 12)

	vals := func(res []TimelineData) []string {
		return Map(res, func(e TimelineData, _ int) string { return e.Value })
	}

	var sourceCalls atomic.Int32

	// Instance B warms P1 while the DB has only P1 → its in-memory index knows P1 only.
	instB := newLockingSqliteCache(t, cacheDir, data, &sourceCalls)
	resB1, err := instB.Get(key, p1Start, p1End, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2", "3"}, vals(resB1))
	require.EqualValues(t, 1, sourceCalls.Load())

	// Instance A (fresh; loads index {P1}) fetches P2 and writes it to the shared DB.
	instA := newLockingSqliteCache(t, cacheDir, data, &sourceCalls)
	resA, err := instA.Get(key, p2Start, p2End, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"4", "5", "6"}, vals(resA))
	require.EqualValues(t, 2, sourceCalls.Load())

	// Instance B requests P2. Its in-memory index is stale (knows only P1).
	// Under the lock it must reload, find P2 on disk, and serve it — no re-fetch.
	resB2, err := instB.Get(key, p2Start, p2End, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"4", "5", "6"}, vals(resB2))
	require.EqualValues(t, 2, sourceCalls.Load(),
		"instance B must reload the index under the lock instead of re-fetching P2")
}

// TestCache_CrossProcessLock_ConcurrentInstances is a smoke test: two instances
// sharing the same dir hammer the same key concurrently. The per-key lock must
// serialize their fetch+store (no "database is locked", no corruption) and
// every Get must return the correct data regardless of who fetched it.
func TestCache_CrossProcessLock_ConcurrentInstances(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	key := TimelineDataKey{"key", 1}

	tBase := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	data := make([]TimelineData, 0, 30)
	for i := 0; i < 30; i++ {
		data = append(data, TimelineData{Timestamp: tBase.AddDate(0, 0, i), Value: fmt.Sprint(i), ID: int64(i)})
	}
	pStart, pEnd := tBase, tBase.AddDate(0, 0, 29)
	want := Map(data, func(e TimelineData, _ int) string { return e.Value })

	var sourceCalls atomic.Int32
	instA := newLockingSqliteCache(t, cacheDir, data, &sourceCalls)
	instB := newLockingSqliteCache(t, cacheDir, data, &sourceCalls)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		c := instA
		if i%2 == 1 {
			c = instB
		}
		wg.Add(1)
		go func(c timeline.Cache[TimelineData, TimelineDataKey]) {
			defer wg.Done()
			res, err := c.Get(key, pStart, pEnd, nil)
			if err != nil {
				errs <- err
				return
			}
			if got := Map(res, func(e TimelineData, _ int) string { return e.Value }); !reflect.DeepEqual(got, want) {
				errs <- fmt.Errorf("got %v, want %v", got, want)
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
