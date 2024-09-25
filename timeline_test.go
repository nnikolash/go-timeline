package timeline_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	timeline "github.com/nnikolash/go-timeline"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type TimelineDataKey struct {
	Param1 string
	Param2 int
}

type TimelineData struct {
	Timestamp time.Time
	Value     string
	ID        int64
}

func newCache(
	construct func(
		getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
		getTimestamp func(d *TimelineData) time.Time,
	) timeline.Cache[TimelineData, TimelineDataKey],
	storage map[string][]TimelineData,
	onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, data []TimelineData),
	getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker) {
	var fetchFromStorageAllowed atomic.Bool
	fetchFromStorageAllowed.Store(true)
	fetchLock := &sync.Mutex{}

	var c timeline.Cache[TimelineData, TimelineDataKey]

	if getFromSource == nil {
		getFromSource = func(key TimelineDataKey, periodStart, periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, extra interface{}) (timeline.CacheFetchResult[TimelineData], error) {
			if !fetchFromStorageAllowed.Load() {
				return timeline.CacheFetchResult[TimelineData]{}, errors.New("fetch from storage is not allowed in test")
			}

			fetchLock.Lock()
			defer fetchLock.Unlock()

			keyStr := fmt.Sprintf("%s_%d", key.Param1, key.Param2)
			storage, ok := storage[keyStr]
			if !ok {
				return timeline.CacheFetchResult[TimelineData]{}, errors.Errorf("wrong key %v", key)
			}

			res := timeline.CacheFetchResult[TimelineData]{
				PeriodStart: periodStart,
				PeriodEnd:   periodEnd,
				Data:        []TimelineData{},
			}
			for _, v := range storage {
				if v.Timestamp.After(periodEnd) {
					break
				}
				if !v.Timestamp.Before(periodStart) {
					res.Data = append(res.Data, v)
				}
			}

			if onUpdated != nil {
				onUpdated(c, key, periodStart, periodEnd, closertFromStart, closerFromEnd, res.Data)
			}

			return res, nil
		}
	}

	getTimestamp := func(d *TimelineData) time.Time {
		return d.Timestamp
	}

	c = construct(getFromSource, getTimestamp)

	return c, &fetchFromStorageAllowed, fetchLock
}

func newMemoryCache(
	t *testing.T,
	storage map[string][]TimelineData,
	onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, data []TimelineData),
	getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker) {
	return newCache(
		func(
			getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
			getTimestamp func(d *TimelineData) time.Time,
		) timeline.Cache[TimelineData, TimelineDataKey] {
			return timeline.NewMemoryCache[TimelineData, TimelineDataKey](timeline.MemoryCacheOptions[TimelineData, TimelineDataKey]{
				GetTimestamp:  getTimestamp,
				GetFromSource: getFromSource,
			})
		},
		storage,
		onUpdated,
		getFromSource,
	)
}

func newFileCache(
	t *testing.T,
	cacheDir string,
	storage map[string][]TimelineData,
	onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, data []TimelineData),
	getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker) {
	return newCache(
		func(
			getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
			getTimestamp func(d *TimelineData) time.Time,
		) timeline.Cache[TimelineData, TimelineDataKey] {
			c, err := timeline.NewFileCache[TimelineData, TimelineDataKey](timeline.FileCacheOptions[TimelineData, TimelineDataKey]{
				CacheDir:      cacheDir,
				GetTimestamp:  getTimestamp,
				GetFromSource: getFromSource,
			})
			require.NoError(t, err)

			return c
		},
		storage,
		onUpdated,
		getFromSource,
	)
}

func newFileCacheInTmpDir(
	t *testing.T,
	storage map[string][]TimelineData,
	onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, data []TimelineData),
	getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker) {
	return newFileCache(t, t.TempDir(), storage, onUpdated, getFromSource)
}

func newSqliteCacheInTmpDir(
	t *testing.T,
	storage map[string][]TimelineData,
	onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, data []TimelineData),
	getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker) {
	return newSqliteCache(t, t.TempDir(), storage, onUpdated, getFromSource)
}

func newSqliteCache(
	t *testing.T,
	cacheDir string,
	storage map[string][]TimelineData,
	onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, data []TimelineData),
	getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker) {
	return newCache(
		func(
			getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
			getTimestamp func(d *TimelineData) time.Time,
		) timeline.Cache[TimelineData, TimelineDataKey] {
			c, err := timeline.NewSqliteCache[TimelineData, TimelineDataKey, int64](timeline.SqliteCacheOptions[TimelineData, TimelineDataKey, int64]{
				CacheDir:      cacheDir,
				GetTimestamp:  getTimestamp,
				GetFromSource: getFromSource,
				GetID:         func(d *TimelineData) int64 { return d.ID },
			})
			require.NoError(t, err)

			return c
		},
		storage,
		onUpdated,
		getFromSource,
	)
}

func newTwolayerCacheInTmpDir(
	t *testing.T,
	storage map[string][]TimelineData,
	onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, data []TimelineData),
	getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker) {
	return newTwolayerCache(t, t.TempDir(), storage, onUpdated, getFromSource)
}

func newTwolayerCache(
	t *testing.T,
	cacheDir string,
	storage map[string][]TimelineData,
	onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, data []TimelineData),
	getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker) {
	return newCache(
		func(
			getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
			getTimestamp func(d *TimelineData) time.Time,
		) timeline.Cache[TimelineData, TimelineDataKey] {
			c, err := timeline.NewMemoryAndSqliteCache[TimelineData, TimelineDataKey, int64](
				timeline.MemoryAndSqliteCacheOptions[TimelineData, TimelineDataKey, int64]{
					SqliteCacheOptions: timeline.SqliteCacheOptions[TimelineData, TimelineDataKey, int64]{
						CacheDir:      cacheDir,
						GetTimestamp:  getTimestamp,
						GetFromSource: getFromSource,
						GetID:         func(d *TimelineData) int64 { return d.ID },
					},
					MinFirstLayerFetchPeriod: 0,
				})
			require.NoError(t, err)

			return c
		},
		storage,
		onUpdated,
		getFromSource,
	)
}

type testCache func(
	t *testing.T,
	storage map[string][]TimelineData,
	onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, data []TimelineData),
	getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker)

var testCaches = map[string]testCache{
	"MemoryCache": newMemoryCache,
	"FileCache":   newFileCacheInTmpDir,
	"SqliteCache": newSqliteCacheInTmpDir,
	"TwoLayer":    newTwolayerCacheInTmpDir,
}

type testPersistentCache func(
	t *testing.T,
	cacheDir string,
	storage map[string][]TimelineData,
	onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, data []TimelineData),
	getFromSource timeline.CacheSource[TimelineData, TimelineDataKey],
) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker)

var testPesistentCaches = map[string]testPersistentCache{
	"FileCache":   newFileCache,
	"SqliteCache": newSqliteCache,
}

var timelineCacheDataStorage = map[string][]TimelineData{
	"key_1": {
		{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), "1", 1},
		{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), "1", 2},
		{time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), "2", 3},
		{time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), "2", 4},
		{time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), "3", 5},
		{time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), "3", 6},
		{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "4", 7},
		{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "4", 8},
		{time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC), "5", 9},
		{time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC), "5", 10},
		{time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC), "6", 11},
		{time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC), "6", 12},
		{time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC), "7", 13},
		{time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC), "7", 14},
		{time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC), "8", 15},
		{time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC), "8", 16},
		{time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC), "9", 17},
		{time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC), "9", 18},
		{time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC), "10", 19},
		{time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC), "10", 20},
	},
	"key_2": {
		{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), "10", 1},
		{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), "10", 2},
		{time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), "20", 3},
		{time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), "20", 4},
		{time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), "30", 5},
		{time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), "30", 6},
		{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "40", 7},
		{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "40", 8},
		{time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC), "50", 9},
		{time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC), "50", 10},
		{time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC), "60", 11},
		{time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC), "60", 12},
		{time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC), "70", 13},
		{time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC), "70", 14},
		{time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC), "80", 15},
		{time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC), "80", 16},
		{time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC), "90", 17},
		{time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC), "90", 18},
		{time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC), "100", 19},
		{time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC), "100", 20},
	},
}

func duplicate(v []string) []string {
	d := make([]string, 0, 2*len(v))
	for _, s := range v {
		d = append(d, s, s)
	}
	return d
}

func TestCache_Basic(t *testing.T) {
	t.Parallel()

	for name, newCache := range testCaches {
		t.Run(name, func(t *testing.T) {
			var expectedClosestFromStart *TimelineData
			var expectedClosestFromEnd *TimelineData

			cache, fetchFromStorageAllowed, _ := newCache(t, timelineCacheDataStorage,
				func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time, closestFromStart, closestFromEnd *TimelineData, data []TimelineData) {

					require.Equal(t, expectedClosestFromStart, closestFromStart)
					require.Equal(t, expectedClosestFromEnd, closestFromEnd)
				}, nil)

			fetchFromStorageAllowed.Store(false)

			_, err := cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.Error(t, err)

			fetchFromStorageAllowed.Store(true)

			_, wasCached, err := cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.False(t, wasCached)

			_, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.False(t, wasCached)

			entries, err := cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"2", "3", "4"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			_, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.False(t, wasCached)

			fetchFromStorageAllowed.Store(false)

			entries, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.True(t, wasCached)
			require.Equal(t, duplicate([]string{"2", "3", "4"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			_, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.False(t, wasCached)

			fetchFromStorageAllowed.Store(true)

			expectedClosestFromStart = &TimelineData{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "4", 8}

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"8", "9", "10"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			fetchFromStorageAllowed.Store(false)

			_, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.Error(t, err)

			fetchFromStorageAllowed.Store(true)

			expectedClosestFromEnd = &TimelineData{time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC), "8", 15}

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"2", "3", "4", "5", "6", "7", "8", "9", "10"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			fetchFromStorageAllowed.Store(false)

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"2", "3", "4", "5", "6", "7", "8", "9", "10"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			fetchFromStorageAllowed.Store(true)

			expectedClosestFromStart = nil
			expectedClosestFromEnd = nil

			entries, err = cache.Get(
				TimelineDataKey{"key", 2},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"20", "30", "40", "50", "60", "70", "80", "90", "100"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))
		})
	}
}

func TestCache_CacheReturnsMore(t *testing.T) {
	t.Parallel()

	for name, newCache := range testCaches {
		t.Run(name, func(t *testing.T) {
			fetchFromStorageAllowed := true

			getFromSource := func(key TimelineDataKey, periodStart, periodEnd time.Time, _, _ *TimelineData, extra interface{}) (timeline.CacheFetchResult[TimelineData], error) {
				if !fetchFromStorageAllowed {
					return timeline.CacheFetchResult[TimelineData]{}, errors.New("fetch from storage is not allowed in test")
				}

				keyStr := fmt.Sprintf("%s_%d", key.Param1, key.Param2)
				data, ok := timelineCacheDataStorage[keyStr]
				if !ok {
					return timeline.CacheFetchResult[TimelineData]{}, errors.Errorf("wrong key %v", key)
				}

				return timeline.CacheFetchResult[TimelineData]{
					PeriodStart: data[0].Timestamp,
					PeriodEnd:   data[len(data)-1].Timestamp,
					Data:        data,
				}, nil
			}

			cache, _, _ := newCache(t, timelineCacheDataStorage, nil, getFromSource)

			fetchFromStorageAllowed = false
			_, err := cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.Error(t, err)

			fetchFromStorageAllowed = true
			entries, err := cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"2", "3", "4"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))
		})
	}
}

func TestCache_MultiThreadingLock(t *testing.T) {
	//t.Parallel()

	for name, newCache := range testCaches {
		t.Run(name, func(t *testing.T) {
			var timesFetchCalled atomic.Int32

			cache, fetchFromStorageAllowed, fetchLock := newCache(t, timelineCacheDataStorage,
				func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart,
					periodEnd time.Time, closertFromStart, closerFromEnd *TimelineData, data []TimelineData) {
					timesFetchCalled.Add(1)

					_, found, err := c.GetCached(
						TimelineDataKey{"key", 1},
						time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
						time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
					)
					require.NoError(t, err)
					require.False(t, found)
				}, nil)

			g := errgroup.Group{}
			defer func() {
				require.NoError(t, g.Wait())
			}()

			fetchLock.Lock()

			g.Go(func() error {
				t.Helper()

				entries, err := cache.Get(
					TimelineDataKey{"key", 1},
					time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
					time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
					nil,
				)
				require.NoError(t, err)
				require.Equal(t, duplicate([]string{"2", "3", "4"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

				return nil
			})

			time.Sleep(100 * time.Millisecond)

			fetchFromStorageAllowed.Store(false)

			g.Go(func() error {
				t.Helper()

				entries, err := cache.Get(
					TimelineDataKey{"key", 1},
					time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
					time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
					nil,
				)
				require.NoError(t, err)
				require.Equal(t, duplicate([]string{"2", "3", "4"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

				return nil
			})

			time.Sleep(100 * time.Millisecond)

			fetchLock.Unlock()

			time.Sleep(100 * time.Millisecond)

			require.Equal(t, int32(1), timesFetchCalled.Load())
		})
	}
}

func TestCache_Continuous(t *testing.T) {
	t.Parallel()

	var storage = map[string][]TimelineData{
		"key_1": {
			{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), "1", 4},
			{time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), "2", 3},
			{time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC).Add(time.Nanosecond), "3", 2},
			{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "4", 1},
		},
	}

	for name, newCache := range testCaches {
		t.Run(name, func(t *testing.T) {
			cache, _, _ := newCache(t, storage, nil, nil)

			entries, err := cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, []string{"1", "2"}, Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			_, exists, err := cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC).Add(time.Nanosecond),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.False(t, exists)

			_, exists, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC).Add(time.Nanosecond),
			)
			require.NoError(t, err)
			require.False(t, exists)

			_, exists, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC).Add(time.Nanosecond),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.False(t, exists)

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC).Add(time.Nanosecond),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, []string{"3", "4"}, Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			entries, exists, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, []string{"1", "2", "3", "4"}, Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			entries, exists, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Nanosecond),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, []string{"2", "3", "4"}, Map(entries, func(e TimelineData, _ int) string { return e.Value }))
		})
	}
}

// func TestTImelineCache_LastEntryUpdated(t *testing.T) {
// 	var storage = map[string][]TimelineData{
// 		"key_1": {
// 			{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), "1"},
// 			{time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), "2"},
// 			{time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC).Add(time.Nanosecond), "3"},
// 			{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "4"},
// 		},
// 	}

// 	cache, _, _ := newCache(storage, nil)
// }

func TestCache_Restoring(t *testing.T) {
	t.Parallel()

	var dataStorage = map[string][]TimelineData{
		"key_1": {
			{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), "1", 1},
			{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), "1", 2},
			{time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), "2", 3},
			{time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), "2", 4},
			{time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), "3", 5},
			{time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), "3", 6},
			{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "4", 7},
			{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "4", 8},
			{time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC), "8", 15},
			{time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC), "8", 16},
			{time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC), "9", 17},
			{time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC), "9", 18},
			{time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC), "10", 19},
			{time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC), "10", 20},
			{time.Date(2021, 1, 11, 0, 0, 0, 0, time.UTC), "11", 21},
			{time.Date(2021, 1, 11, 0, 0, 0, 0, time.UTC), "11", 22},
			{time.Date(2021, 1, 12, 0, 0, 0, 0, time.UTC), "12", 23},
			{time.Date(2021, 1, 12, 0, 0, 0, 0, time.UTC), "12", 24},
			{time.Date(2021, 1, 13, 0, 0, 0, 0, time.UTC), "13", 25},
			{time.Date(2021, 1, 13, 0, 0, 0, 0, time.UTC), "13", 26},
		},
	}

	for name, newCacheFull := range testPesistentCaches {

		newCache := func(t *testing.T, cacheDir string, onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time,
			closertFromStart, closerFromEnd *TimelineData, data []TimelineData)) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker) {

			return newCacheFull(t, cacheDir, dataStorage, onUpdated, nil)
		}

		t.Run(name, func(t *testing.T) {
			cacheDir := t.TempDir()

			cache, _, _ := newCache(t, cacheDir, nil)

			entries, err := cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"2", "3", "4"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			entries, wasCached, err := cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.True(t, wasCached)
			require.Equal(t, duplicate([]string{"2", "3", "4"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 11, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 12, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"11", "12"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			_, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.False(t, wasCached)

			var expectedClosestFromStart *TimelineData
			var expectedClosestFromEnd *TimelineData

			cache, fetchFromStorageAllowed, _ := newCache(t,
				cacheDir,
				func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time,
					closestFromStart, closestFromEnd *TimelineData, data []TimelineData) {
					require.Equal(t, expectedClosestFromStart, closestFromStart)
					require.Equal(t, expectedClosestFromEnd, closestFromEnd)
				},
			)

			fetchFromStorageAllowed.Store(false)

			entries, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.True(t, wasCached)
			require.Equal(t, duplicate([]string{"2", "3", "4"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			entries, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.True(t, wasCached)
			require.Equal(t, duplicate([]string{}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			_, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.False(t, wasCached)

			_, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.False(t, wasCached)

			_, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.False(t, wasCached)

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 11, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 12, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"11", "12"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"2", "3", "4"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			_, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.Error(t, err)

			fetchFromStorageAllowed.Store(true)

			expectedClosestFromStart = &TimelineData{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "4", 8}
			expectedClosestFromEnd = &TimelineData{time.Date(2021, 1, 11, 0, 0, 0, 0, time.UTC), "11", 21}

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"8", "9"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			expectedClosestFromStart = &TimelineData{time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC), "9", 18}
			expectedClosestFromEnd = &TimelineData{time.Date(2021, 1, 11, 0, 0, 0, 0, time.UTC), "11", 21}

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"10"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			_, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.False(t, wasCached)

			expectedClosestFromStart = &TimelineData{time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC), "4", 8}
			expectedClosestFromEnd = &TimelineData{time.Date(2021, 1, 8, 0, 0, 0, 0, time.UTC), "8", 15}

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			entries, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.True(t, wasCached)
			require.Equal(t, duplicate([]string{}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			entries, wasCached, err = cache.GetCached(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.True(t, wasCached)
			require.Equal(t, duplicate([]string{}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			cache, fetchFromStorageAllowed, _ = newCache(t, cacheDir, nil)

			fetchFromStorageAllowed.Store(false)

			_, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 13, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.Error(t, err)

			_, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 12, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.Error(t, err)

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"2", "3", "4"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			_, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.Error(t, err)

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"8", "9"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			_, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.Error(t, err)

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"10"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

			entries, err = cache.Get(
				TimelineDataKey{"key", 1},
				time.Date(2021, 1, 11, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 12, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, duplicate([]string{"11", "12"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))
		})
	}
}
