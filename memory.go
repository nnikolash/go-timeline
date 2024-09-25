package timeline

import (
	"time"

	sparse "github.com/nnikolash/go-sparse"
)

type MemoryCacheOptions[Data any, Key any] struct {
	KeyToStr             func(Key) string
	GetTimestamp         func(d *Data) time.Time
	GetFromSource        CacheSource[Data, Key]
	Load                 func(key Key) (*MemoryCacheState[Data], error)
	Save                 func(key Key, state *CacheState[Data], updated []*CacheFetchResult[Data]) error
	SkipDataVerification bool
}

type MemoryCacheState[Data any] struct {
	Entries []*CacheFetchResult[Data]
}

// TODO: last element of each period may be incomplete.
// E.g, first we fetched candles [2024-01-01; 2024-02-01], then - [2024-03-01; 2024-04-01].
// Candle at 2024-02-01 will be incomplete because it was most likely updated after that date.
// And we still will return that incomplete value if period [2024-01-01; 2024-02-01] requested.
// But this is very minor difference, so I decided for now to ignore it.

func NewMemoryCache[Data any, Key any](opts MemoryCacheOptions[Data, Key]) *MemoryCache[Data, Key] {
	return &MemoryCache[Data, Key]{
		CacheBase: *NewCacheBase[Data, Key](CacheBaseOptions[Data, Key]{
			KeyToStr:      opts.KeyToStr,
			GetTimestamp:  opts.GetTimestamp,
			GetFromSource: opts.GetFromSource,
			Storage: &memoryCacheStorage[Data, Key]{
				getTimestamp: opts.GetTimestamp,
				load:         opts.Load,
				save:         opts.Save,
			},
			SkipDataVerification: opts.SkipDataVerification,
		}),
	}
}

type MemoryCache[Data any, Key any] struct {
	CacheBase[Data, Key]
}

var _ Cache[struct{}, int64] = &MemoryCache[struct{}, int64]{}

type memoryCacheStorage[Data any, Key any] struct {
	getTimestamp func(d *Data) time.Time
	load         func(key Key) (*MemoryCacheState[Data], error)
	save         func(key Key, state *CacheState[Data], updated []*CacheFetchResult[Data]) error
}

var _ CacheStorage[struct{}, int64] = &memoryCacheStorage[struct{}, int64]{}

func (c *memoryCacheStorage[Data, Key]) Load(key Key) (*CacheState[Data], error) {
	if c.load == nil {
		return nil, nil
	}

	cache, err := c.load(key)
	if err != nil {
		return nil, err
	}

	if cache == nil {
		return nil, nil
	}

	stateEntries := make([]*sparse.SeriesEntryFields[Data, time.Time], 0, len(cache.Entries))

	for _, entry := range cache.Entries {
		d, err := c.Add(key, entry.PeriodStart, entry.PeriodEnd, entry.Data)
		if err != nil {
			panic(err)
		}

		entry := sparse.SeriesEntryFields[Data, time.Time]{
			PeriodBounds: sparse.PeriodBounds[time.Time]{
				PeriodStart: entry.PeriodStart,
				PeriodEnd:   entry.PeriodEnd,
			},
			Data:  d,
			Empty: len(entry.Data) == 0,
		}

		stateEntries = append(stateEntries, &entry)
	}

	return &CacheState[Data]{Entries: stateEntries}, nil
}

func (c *memoryCacheStorage[Data, Key]) Save(key Key, state *CacheState[Data], updated []*CacheFetchResult[Data]) error {
	if c.save == nil {
		return nil
	}

	return c.save(key, state, updated)
}

func (c *memoryCacheStorage[Data, Key]) Add(key Key, periodStart, periodEnd time.Time, data []Data) (CacheData[Data], error) {
	return sparse.NewArrayData(c.getTimestamp, IdxCmp, periodStart, periodEnd, data)
}
