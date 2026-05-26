package timeline_test

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	timeline "github.com/nnikolash/go-timeline"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// This file contains tests for:
//   - non-UTC TZ behavior across all cache implementations
//   - persistence (save/load) with non-UTC TZ inputs
//   - source contract violations (verifyPeriodData should reject)
//   - source contract: bigger-period overhead returns
//   - degenerate inputs

// ---------------------------------------------------------------------------
// Shared TZ helpers
// ---------------------------------------------------------------------------

var (
	tzCEST = time.FixedZone("CEST", 2*60*60)
	tzUTCp5 = time.FixedZone("UTC+5", 5*60*60)
)

// tzDataStorage contains entries with timestamps in various non-UTC zones.
// The "absolute" instant for each entry is the same as it would be in UTC
// at the corresponding "day at midnight UTC" — but we store them in
// non-UTC representations to exercise TZ-mixing paths.
func tzDataStorage() map[string][]TimelineData {
	d := func(day int, value string, id int64, tz *time.Location) TimelineData {
		// Build the timestamp in UTC first, then convert to the target TZ
		// so the absolute time is identical to "2021-01-{day} 00:00 UTC".
		ts := time.Date(2021, 1, day, 0, 0, 0, 0, time.UTC).In(tz)
		return TimelineData{Timestamp: ts, Value: value, ID: id}
	}
	return map[string][]TimelineData{
		"key_1": {
			d(1, "1", 1, tzCEST),
			d(2, "2", 2, time.UTC),
			d(3, "3", 3, tzUTCp5),
			d(4, "4", 4, tzCEST),
			d(5, "5", 5, time.UTC),
			d(6, "6", 6, tzUTCp5),
			d(7, "7", 7, tzCEST),
			d(8, "8", 8, time.UTC),
			d(9, "9", 9, tzUTCp5),
			d(10, "10", 10, tzCEST),
		},
	}
}

// ---------------------------------------------------------------------------
// 1. Non-UTC TZ behavior across all cache implementations
// ---------------------------------------------------------------------------

func TestCache_NonUTC_TZ_AllCaches(t *testing.T) {
	t.Parallel()

	storage := tzDataStorage()

	for name, newCache := range testCaches {
		t.Run(name, func(t *testing.T) {
			cache, _, _ := newCache(t, storage, nil, nil)
			key := TimelineDataKey{"key", 1}

			// Request periodStart/periodEnd in CEST — same absolute time as
			// the underlying day-boundaries in UTC.
			periodStart := time.Date(2021, 1, 2, 2, 0, 0, 0, tzCEST) // = 2021-01-02 00:00 UTC
			periodEnd := time.Date(2021, 1, 4, 2, 0, 0, 0, tzCEST)   // = 2021-01-04 00:00 UTC

			entries, err := cache.Get(key, periodStart, periodEnd, nil)
			require.NoError(t, err)
			require.Equal(t,
				[]string{"2", "3", "4"},
				Map(entries, func(e TimelineData, _ int) string { return e.Value }),
			)

			// Now request same period expressed in different TZ — must hit cache.
			periodStartUTC := time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC)
			periodEndUTC := time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC)

			cached, wasCached, err := cache.GetCached(key, periodStartUTC, periodEndUTC)
			require.NoError(t, err)
			require.True(t, wasCached, "same absolute period in different TZ must be cached")
			require.Equal(t,
				[]string{"2", "3", "4"},
				Map(cached, func(e TimelineData, _ int) string { return e.Value }),
			)

			// Same period expressed in UTC+5
			periodStartP5 := time.Date(2021, 1, 2, 5, 0, 0, 0, tzUTCp5)
			periodEndP5 := time.Date(2021, 1, 4, 5, 0, 0, 0, tzUTCp5)

			cached, wasCached, err = cache.GetCached(key, periodStartP5, periodEndP5)
			require.NoError(t, err)
			require.True(t, wasCached)
			require.Equal(t,
				[]string{"2", "3", "4"},
				Map(cached, func(e TimelineData, _ int) string { return e.Value }),
			)
		})
	}
}

// ---------------------------------------------------------------------------
// 2. Persistence with non-UTC TZ
// ---------------------------------------------------------------------------

func TestCache_Restoring_NonUTC_TZ(t *testing.T) {
	t.Parallel()

	storage := tzDataStorage()

	for name, newCacheFull := range testPesistentCaches {
		t.Run(name, func(t *testing.T) {
			cacheDir := t.TempDir()
			key := TimelineDataKey{"key", 1}

			// First lifetime: insert via Get in CEST.
			cache, _, _ := newCacheFull(t, cacheDir, storage, nil, nil)
			periodStart := time.Date(2021, 1, 2, 2, 0, 0, 0, tzCEST)
			periodEnd := time.Date(2021, 1, 4, 2, 0, 0, 0, tzCEST)

			entries, err := cache.Get(key, periodStart, periodEnd, nil)
			require.NoError(t, err)
			require.Equal(t,
				[]string{"2", "3", "4"},
				Map(entries, func(e TimelineData, _ int) string { return e.Value }),
			)

			cache.Close()

			// Second lifetime: restore, query the same absolute period in UTC
			// and in UTC+5. Should be cached without going to source.
			cache, fetchFromStorageAllowed, _ := newCacheFull(t, cacheDir, storage, nil, nil)
			fetchFromStorageAllowed.Store(false)

			cached, wasCached, err := cache.GetCached(
				key,
				time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC),
			)
			require.NoError(t, err)
			require.True(t, wasCached, "restored period must be hit in UTC representation")
			require.Equal(t,
				[]string{"2", "3", "4"},
				Map(cached, func(e TimelineData, _ int) string { return e.Value }),
			)

			cached, wasCached, err = cache.GetCached(
				key,
				time.Date(2021, 1, 2, 5, 0, 0, 0, tzUTCp5),
				time.Date(2021, 1, 4, 5, 0, 0, 0, tzUTCp5),
			)
			require.NoError(t, err)
			require.True(t, wasCached, "restored period must be hit in UTC+5 representation")
			require.Equal(t,
				[]string{"2", "3", "4"},
				Map(cached, func(e TimelineData, _ int) string { return e.Value }),
			)

			// Cache miss for not-loaded portion still works after restore.
			fetchFromStorageAllowed.Store(true)
			entries, err = cache.Get(
				key,
				time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 6, 0, 0, 0, 0, time.UTC),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t,
				[]string{"5", "6"},
				Map(entries, func(e TimelineData, _ int) string { return e.Value }),
			)
		})
	}
}

// ---------------------------------------------------------------------------
// 3. Source contract violations — verifyPeriodData must reject
// ---------------------------------------------------------------------------

// makeBadSource returns a source that always answers with the same
// hard-coded segment regardless of the request.
func makeBadSource(seg timeline.CacheStateSegment[TimelineData]) timeline.CacheSource[TimelineData, TimelineDataKey] {
	return func(_ TimelineDataKey, _ time.Time, _ time.Time, _, _ *TimelineData, _ interface{}) (timeline.CacheStateSegment[TimelineData], error) {
		return seg, nil
	}
}

func TestCache_SourceContract_FirstElemBeforePeriodStart(t *testing.T) {
	t.Parallel()

	// Source returns a candle 7 minutes earlier than declared PeriodStart.
	// This is the exact downstream-bug shape from PROBLEMS_AND_RECOMMENDATIONS.md.
	periodStart := time.Date(2024, 5, 25, 11, 37, 29, 0, tzCEST)
	periodEnd := time.Date(2024, 5, 25, 12, 0, 0, 0, tzCEST)
	candle := TimelineData{
		Timestamp: time.Date(2024, 5, 25, 9, 30, 0, 0, time.UTC), // = 11:30 CEST, < periodStart
		Value:     "bad",
		ID:        1,
	}
	bad := timeline.CacheStateSegment[TimelineData]{
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		Data:        []TimelineData{candle},
	}

	for name, newCache := range testCaches {
		t.Run(name, func(t *testing.T) {
			cache, _, _ := newCache(t, nil, nil, makeBadSource(bad))

			_, err := cache.Get(TimelineDataKey{"key", 1}, periodStart, periodEnd, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "firstElemT")
			require.Contains(t, err.Error(), "periodStart")
		})
	}
}

func TestCache_SourceContract_LastElemAfterPeriodEnd(t *testing.T) {
	t.Parallel()

	periodStart := time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC)
	bad := timeline.CacheStateSegment[TimelineData]{
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		Data: []TimelineData{
			{Timestamp: periodStart, Value: "a", ID: 1},
			{Timestamp: periodEnd.Add(time.Hour), Value: "b", ID: 2}, // > periodEnd
		},
	}

	for name, newCache := range testCaches {
		t.Run(name, func(t *testing.T) {
			cache, _, _ := newCache(t, nil, nil, makeBadSource(bad))

			_, err := cache.Get(TimelineDataKey{"key", 1}, periodStart, periodEnd, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "lastElemT")
			require.Contains(t, err.Error(), "periodEnd")
		})
	}
}

func TestCache_SourceContract_DataNotSorted(t *testing.T) {
	t.Parallel()

	periodStart := time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC)
	bad := timeline.CacheStateSegment[TimelineData]{
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		Data: []TimelineData{
			{Timestamp: time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), Value: "later", ID: 1},
			{Timestamp: time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), Value: "earlier", ID: 2},
		},
	}

	for name, newCache := range testCaches {
		t.Run(name, func(t *testing.T) {
			cache, _, _ := newCache(t, nil, nil, makeBadSource(bad))

			_, err := cache.Get(TimelineDataKey{"key", 1}, periodStart, periodEnd, nil)
			require.Error(t, err)
			require.True(t,
				strings.Contains(err.Error(), "not sorted") || strings.Contains(err.Error(), "lastElemT") || strings.Contains(err.Error(), "firstElemT"),
				"expected sortedness/order violation in error, got: %v", err,
			)
		})
	}
}

func TestCache_SourceContract_SmallerPeriodThanRequested(t *testing.T) {
	t.Parallel()

	periodStart := time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC)
	// Source returns a *smaller* PeriodStart, declaring less than requested.
	bad := timeline.CacheStateSegment[TimelineData]{
		PeriodStart: periodStart.Add(time.Hour), // > requested periodStart  -- VIOLATION
		PeriodEnd:   periodEnd,
		Data:        nil,
	}

	for name, newCache := range testCaches {
		t.Run(name, func(t *testing.T) {
			cache, _, _ := newCache(t, nil, nil, makeBadSource(bad))

			_, err := cache.Get(TimelineDataKey{"key", 1}, periodStart, periodEnd, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "does not contain requested period")
		})
	}
}

// ---------------------------------------------------------------------------
// 4. Source contract — overhead-fetch done CORRECTLY
// ---------------------------------------------------------------------------

// Demonstrates the right way to do "overhead-fetch for indicators":
// source returns bigger period than requested AND extends PeriodStart back
// to match the earliest returned element timestamp.
//
// This is the contract downstream needs to follow to avoid the bug in
// PROBLEMS_AND_RECOMMENDATIONS.md.
func TestCache_SourceContract_OverheadFetch_DoneRight(t *testing.T) {
	t.Parallel()

	requestedStart := time.Date(2024, 5, 25, 11, 37, 29, 0, tzCEST)
	requestedEnd := time.Date(2024, 5, 25, 12, 0, 0, 0, tzCEST)

	// Overhead candle at 11:30 CEST = 09:30 UTC.
	overheadCandle := TimelineData{
		Timestamp: time.Date(2024, 5, 25, 9, 30, 0, 0, time.UTC),
		Value:     "overhead",
		ID:        1,
	}
	insideCandle := TimelineData{
		// 09:45 UTC = 11:45 CEST, which lies inside [11:37:29 CEST, 12:00 CEST].
		Timestamp: time.Date(2024, 5, 25, 9, 45, 0, 0, time.UTC),
		Value:     "inside",
		ID:        2,
	}

	source := func(_ TimelineDataKey, _, _ time.Time, _, _ *TimelineData, _ interface{}) (timeline.CacheStateSegment[TimelineData], error) {
		return timeline.CacheStateSegment[TimelineData]{
			// PeriodStart EXTENDED back to match earliest element timestamp.
			PeriodStart: overheadCandle.Timestamp,
			PeriodEnd:   requestedEnd,
			Data:        []TimelineData{overheadCandle, insideCandle},
		}, nil
	}

	for name, newCache := range testCaches {
		t.Run(name, func(t *testing.T) {
			cache, _, _ := newCache(t, nil, nil, source)

			entries, err := cache.Get(TimelineDataKey{"key", 1}, requestedStart, requestedEnd, nil)
			require.NoError(t, err, "source extending PeriodStart back is the supported overhead-fetch pattern")
			// Only entries within the requested window are returned.
			require.Equal(t,
				[]string{"inside"},
				Map(entries, func(e TimelineData, _ int) string { return e.Value }),
			)
		})
	}
}

// ---------------------------------------------------------------------------
// 5. Degenerate inputs
// ---------------------------------------------------------------------------

func TestCache_Degenerate_PeriodStartAfterPeriodEnd(t *testing.T) {
	t.Parallel()

	periodStart := time.Date(2021, 1, 4, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC)

	// Use a non-failing source so it has the opportunity to be invoked,
	// then we check that an invalid range is rejected somewhere on the way.
	source := func(_ TimelineDataKey, ps, pe time.Time, _, _ *TimelineData, _ interface{}) (timeline.CacheStateSegment[TimelineData], error) {
		// If a buggy version of the library calls source with reversed
		// bounds, surface an error to differentiate it from "no data".
		if ps.After(pe) {
			return timeline.CacheStateSegment[TimelineData]{}, errors.New("source got reversed period")
		}
		return timeline.CacheStateSegment[TimelineData]{PeriodStart: ps, PeriodEnd: pe}, nil
	}

	for name, newCache := range testCaches {
		t.Run(name, func(t *testing.T) {
			cache, _, _ := newCache(t, nil, nil, source)

			// Per current implementation the library either errors or
			// returns empty data — both are acceptable; what we don't
			// want is a panic or stuck/inconsistent cache state.
			_, err := cache.Get(TimelineDataKey{"key", 1}, periodStart, periodEnd, nil)
			require.Error(t, err, "reversed period must not silently succeed")
		})
	}
}

func TestCache_Degenerate_GetCachedAll_ZeroInputs(t *testing.T) {
	t.Parallel()

	storage := tzDataStorage()
	cache, _, _ := newMemoryCache(t, storage, nil, nil)

	type call struct {
		name                                                            string
		requiredStart, requiredEnd, minStart, maxEnd time.Time
	}
	now := time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC)
	tests := []call{
		{"requiredStart zero", time.Time{}, now, now, now},
		{"requiredEnd zero", now, time.Time{}, now, now},
		{"minStart zero", now, now, time.Time{}, now},
		{"maxEnd zero", now, now, now, time.Time{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mc, ok := cache.(interface {
				GetCachedAll(TimelineDataKey, time.Time, time.Time, time.Time, time.Time) (timeline.CacheStateSegment[TimelineData], bool, error)
			})
			require.True(t, ok)

			_, _, err := mc.GetCachedAll(
				TimelineDataKey{"key", 1},
				tc.requiredStart, tc.requiredEnd, tc.minStart, tc.maxEnd,
			)
			require.Error(t, err)
			require.Contains(t, err.Error(), "is zero")
		})
	}
}

// Helper to keep gosec/staticcheck quiet about unused imports if the file
// is edited later.
var _ = []interface{}{
	sync.Mutex{},
	atomic.Bool{},
}
