package timeline_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Most of the tests are in timeline_test.go

func TestCache_SqliteTimezone(t *testing.T) {
	t.Parallel()

	var UTC0 = time.FixedZone("UTC+0", 0)
	var UTC2 = time.FixedZone("UTC+2", 2*60*60)
	var UTC5 = time.FixedZone("UTC+5", 5*60*60)
	var UTC7 = time.FixedZone("UTC+7", 7*60*60)
	var UTC8 = time.FixedZone("UTC+8", 8*60*60)
	var UTC9 = time.FixedZone("UTC+9", 9*60*60)
	var UTC12 = time.FixedZone("UTC+12", 12*60*60)

	var dataStorage = map[string][]TimelineData{
		"key_1": {
			{time.Date(2021, 1, 1, 0, 0, 0, 0, UTC7), "1", 1},
			{time.Date(2021, 2, 2, 0, 0, 0, 0, UTC0), "2", 3},
			{time.Date(2021, 3, 3, 0, 0, 0, 0, UTC12), "3", 5},
			{time.Date(2021, 4, 4, 0, 0, 0, 0, UTC8), "4", 7},
			{time.Date(2021, 5, 8, 0, 0, 0, 0, UTC2), "8", 15},
			{time.Date(2021, 6, 9, 0, 0, 0, 0, UTC5), "9", 17},
			{time.Date(2021, 7, 10, 0, 0, 0, 0, UTC0), "10", 19},
			{time.Date(2021, 8, 11, 0, 0, 0, 0, UTC12), "11", 21},
			{time.Date(2021, 9, 12, 0, 0, 0, 0, UTC7), "12", 23},
			{time.Date(2021, 10, 13, 0, 0, 0, 0, UTC5), "13", 26},
		},
	}

	cacheDir := t.TempDir()

	for i := 0; i < 2; i++ {
		cache, fetchFromStorageAllowed, _ := newSqliteCache(t, cacheDir, dataStorage, nil, nil)

		if i != 0 {
			fetchFromStorageAllowed.Store(false)
		}

		k1 := TimelineDataKey{"key", 1}

		res, err := cache.Get(k1, time.Date(2021, 2, 2, 0, 0, 0, 0, UTC0), time.Date(2021, 4, 4, 0, 0, 0, 0, UTC8), nil)
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4"}, Map(res, func(e TimelineData, _ int) string { return e.Value }))

		res, err = cache.Get(k1, time.Date(2021, 2, 2, 0, 0, 0, 0, UTC0), time.Date(2021, 4, 4, 0, 0, 0, 0, UTC7), nil)
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4"}, Map(res, func(e TimelineData, _ int) string { return e.Value }))

		res, err = cache.Get(k1, time.Date(2021, 2, 2, 0, 0, 0, 0, UTC0), time.Date(2021, 4, 4, 0, 0, 0, 0, UTC9), nil)
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3"}, Map(res, func(e TimelineData, _ int) string { return e.Value }))

		res, err = cache.Get(k1, time.Date(2021, 5, 8, 0, 0, 0, 0, UTC2), time.Date(2021, 12, 12, 0, 0, 0, 0, UTC7), nil)
		require.NoError(t, err)
		require.Equal(t, []string{"8", "9", "10", "11", "12", "13"}, Map(res, func(e TimelineData, _ int) string { return e.Value }))

		res, err = cache.Get(k1, time.Date(2021, 5, 8, 0, 0, 0, 0, UTC0), time.Date(2021, 12, 12, 0, 0, 0, 0, UTC7), nil)
		require.NoError(t, err)
		require.Equal(t, []string{"9", "10", "11", "12", "13"}, Map(res, func(e TimelineData, _ int) string { return e.Value }))

		res, err = cache.Get(k1, time.Date(2021, 5, 8, 0, 0, 0, 0, UTC5), time.Date(2021, 12, 12, 0, 0, 0, 0, UTC7), nil)
		require.NoError(t, err)
		require.Equal(t, []string{"8", "9", "10", "11", "12", "13"}, Map(res, func(e TimelineData, _ int) string { return e.Value }))
	}
}
