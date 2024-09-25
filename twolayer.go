package timeline

import (
	"time"
)

type MemoryAndSqliteCacheOptions[Data any, Key any, ID comparable] struct {
	SqliteCacheOptions[Data, Key, ID]
	MinFirstLayerFetchPeriod time.Duration
}

func NewMemoryAndSqliteCache[Data any, Key any, ID comparable](opts MemoryAndSqliteCacheOptions[Data, Key, ID]) (*MemoryAndSqliteCache[Data, Key, ID], error) {
	sqliteCache, err := NewSqliteCache[Data, Key, ID](opts.SqliteCacheOptions)
	if err != nil {
		return nil, err
	}

	c := &MemoryAndSqliteCache[Data, Key, ID]{
		minFirstLayerFetchPeriod: opts.MinFirstLayerFetchPeriod,
		sqliteCache:              sqliteCache,
	}

	c.MemoryCache = *NewMemoryCache[Data, Key](MemoryCacheOptions[Data, Key]{
		KeyToStr:             opts.KeyToStr,
		GetTimestamp:         opts.GetTimestamp,
		GetFromSource:        c.getFromSecondLayer,
		SkipDataVerification: opts.SkipDataVerification,
	})

	return c, nil
}

type MemoryAndSqliteCache[Data any, Key any, ID comparable] struct {
	MemoryCache[Data, Key]
	sqliteCache              *SqliteCache[Data, Key, ID]
	minFirstLayerFetchPeriod time.Duration
}

var _ Cache[struct{}, int64] = &MemoryAndSqliteCache[struct{}, int64, string]{}

func (c *MemoryAndSqliteCache[Data, Key, ID]) getFromSecondLayer(key Key, periodStart, periodEnd time.Time, closestFromStart, closestFromEnd *Data, extra interface{}) (CacheFetchResult[Data], error) {
	forwardFetchPeriodStart := periodStart
	forwardFetchPeriodEnd := periodEnd
	requestedPeriodDuration := periodEnd.Sub(periodStart)

	if requestedPeriodDuration < c.minFirstLayerFetchPeriod {
		requestedPeriodCenter := periodStart.Add(requestedPeriodDuration / 2)
		forwardFetchPeriodStart = requestedPeriodCenter.Add(-c.minFirstLayerFetchPeriod / 2)
		forwardFetchPeriodEnd = requestedPeriodCenter.Add(c.minFirstLayerFetchPeriod / 2)
	}

	cachedInSecondLayer, isCached, err := c.sqliteCache.GetCachedAll(key, periodStart, periodEnd, forwardFetchPeriodStart, forwardFetchPeriodEnd)
	if err != nil {
		return CacheFetchResult[Data]{}, err
	}
	if isCached {
		return cachedInSecondLayer, nil
	}

	// TODO: GetFresh instead of Get
	// TODO: GetAll(s, e, min,max) instead of Get
	data, err := c.sqliteCache.Get(key, periodStart, periodEnd, extra)
	if err != nil {
		return CacheFetchResult[Data]{}, err
	}

	return CacheFetchResult[Data]{
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		Data:        data,
	}, nil
}
