package timeline_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	timeline "github.com/nnikolash/go-timeline"
	"github.com/stretchr/testify/require"
)

// Most of the tests are in timeline_test.go

func TestCache_TwoLayer(t *testing.T) {
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

	newCache := func(t *testing.T, cacheDir string, onUpdated func(c timeline.Cache[TimelineData, TimelineDataKey], key TimelineDataKey, periodStart, periodEnd time.Time,
		closertFromStart, closerFromEnd *TimelineData, data []TimelineData)) (timeline.Cache[TimelineData, TimelineDataKey], *atomic.Bool, sync.Locker) {

		return newTwolayerCache(t, cacheDir, dataStorage, onUpdated, nil)
	}

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

	_, wasCached, err := cache.GetCached(
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

	entries, err = cache.Get(
		TimelineDataKey{"key", 1},
		time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, duplicate([]string{"2", "3", "4"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

	entries, err = cache.Get(
		TimelineDataKey{"key", 1},
		time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC),
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, duplicate([]string{}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

	_, err = cache.Get(
		TimelineDataKey{"key", 1},
		time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
		nil,
	)
	require.Error(t, err)

	_, err = cache.Get(
		TimelineDataKey{"key", 1},
		time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
		nil,
	)
	require.Error(t, err)

	_, err = cache.Get(
		TimelineDataKey{"key", 1},
		time.Date(2021, 1, 7, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 9, 0, 0, 0, 0, time.UTC),
		nil,
	)
	require.Error(t, err)

	entries, err = cache.Get(
		TimelineDataKey{"key", 1},
		time.Date(2021, 1, 11, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 12, 0, 0, 0, 0, time.UTC),
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, duplicate([]string{"11", "12"}), Map(entries, func(e TimelineData, _ int) string { return e.Value }))

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
}
