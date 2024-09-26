package timeline

import (
	"time"

	sparse "github.com/nnikolash/go-sparse"
)

// Save cache to persistent storage
type SaveCacheFunc[Data any, Key any] func(key Key, state *CacheState[Data], updated []*CacheFetchResult[Data]) error

// Load cache from persistent storage
type LoadCaheFunc[Data any, Key any] func(key Key) (*MemoryCacheState[Data], error)

type MemoryCacheOptions[Data any, Key any] struct {
	GetTimestamp         func(d *Data) time.Time  // (Required)
	GetFromSource        CacheSource[Data, Key]   // (Required) Fetch data from source
	KeyToStr             func(Key) string         // (Optional)
	Save                 SaveCacheFunc[Data, Key] // (Optional) Save to persistent storage
	Load                 LoadCaheFunc[Data, Key]  // (Optional) Load from persistent storage
	SkipDataVerification bool                     // (Optional) Set this to true if you are sure your data is well-ordered
}

type MemoryCacheState[Data any] struct {
	Entries []*CacheFetchResult[Data]
}

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
