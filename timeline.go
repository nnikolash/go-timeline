package timeline

import (
	"sync"
	"time"

	"github.com/nnikolash/go-sparse"
	"github.com/pkg/errors"
)

type CacheFetchResult[Data any] struct {
	PeriodStart time.Time
	PeriodEnd   time.Time
	Data        []Data
}

type CacheState[Data any] sparse.SeriesState[Data, time.Time]

type PeriodBounds = sparse.PeriodBounds[time.Time]

type CacheSource[Data any, Key any] func(key Key, periodStart, periodEnd time.Time, closestFromStart, closestFromEnd *Data, extra interface{}) (CacheFetchResult[Data], error)

type CacheData[Data any] sparse.SeriesData[Data, time.Time]

// type CacheEntry[Data any] struct {
// 	PeriodStart time.Time
// 	PeriodEnd   time.Time
// 	Data        CacheData[Data]
// }

//type CacheEntry[Data any] sparse.SeriesEntryFields[Data, time.Time]

type TimePeriodBounds[Data any] struct {
	sparse.PeriodBounds[time.Time]
	First *Data
	Last  *Data
}

type Cache[Data any, Key any] interface {
	Get(key Key, periodStart, periodEnd time.Time, extra interface{}) ([]Data, error)
	GetCached(key Key, periodStart, periodEnd time.Time) ([]Data, bool, error)
	GetCachedAll(key Key, requiredPeriodStart, requiredPeriodEnd, minPeriodStart, maxPeriodEnd time.Time) (CacheFetchResult[Data], bool, error)
	GetCachedPeriodClosestFromStart(key Key, point time.Time, nonEmpty bool) (*TimePeriodBounds[Data], error)
	GetCachedPeriodClosestFromEnd(key Key, point time.Time, nonEmpty bool) (*TimePeriodBounds[Data], error)
	Close()
}

type CacheBaseOptions[Data any, Key any] struct {
	KeyToStr             func(Key) string
	GetTimestamp         func(d *Data) time.Time
	GetFromSource        CacheSource[Data, Key]
	Storage              CacheStorage[Data, Key]
	SkipDataVerification bool
}

// TODO: last element of each period may be incomplete.
// E.g, first we fetched candles [2024-01-01; 2024-02-01], then - [2024-03-01; 2024-04-01].
// Candle at 2024-02-01 will be incomplete because it was most likely updated after that date.
// And we still will return that incomplete value if period [2024-01-01; 2024-02-01] requested.
// But this is very minor difference, so I decided for now to ignore it.

func NewCacheBase[Data any, Key any](opts CacheBaseOptions[Data, Key]) *CacheBase[Data, Key] {
	if opts.KeyToStr == nil {
		opts.KeyToStr = CreateConvertorToString[Key]()
	}

	return &CacheBase[Data, Key]{
		opts:          opts,
		entriesPerKey: make(map[string]*sparseSeriesT[Data]),
		m:             &sync.RWMutex{},
	}
}

type CacheBase[Data any, Key any] struct {
	opts          CacheBaseOptions[Data, Key]
	entriesPerKey map[string]*sparseSeriesT[Data]
	m             *sync.RWMutex
}

var _ Cache[struct{}, int64] = &CacheBase[struct{}, int64]{}

func (c *CacheBase[Data, Key]) Get(key Key, periodStart, periodEnd time.Time, extra interface{}) ([]Data, error) {
	res, _, err := c.get(key, periodStart, periodEnd, time.Time{}, time.Time{}, true, extra)
	return res.Data, err
}

func (c *CacheBase[Data, Key]) GetCached(key Key, periodStart, periodEnd time.Time) ([]Data, bool, error) {
	res, isCached, err := c.get(key, periodStart, periodEnd, time.Time{}, time.Time{}, false, nil)
	return res.Data, isCached, err
}

func (c *CacheBase[Data, Key]) GetCachedAll(key Key, requiredPeriodStart, requiredPeriodEnd, minPeriodStart, maxPeriodEnd time.Time) (CacheFetchResult[Data], bool, error) {
	if requiredPeriodStart.IsZero() {
		return CacheFetchResult[Data]{}, false, errors.New("requiredPeriodStart is zero")
	}
	if requiredPeriodEnd.IsZero() {
		return CacheFetchResult[Data]{}, false, errors.New("requiredPeriodEnd is zero")
	}
	if minPeriodStart.IsZero() {
		return CacheFetchResult[Data]{}, false, errors.New("minPeriodStart is zero")
	}
	if maxPeriodEnd.IsZero() {
		return CacheFetchResult[Data]{}, false, errors.New("maxPeriodEnd is zero")
	}

	return c.get(key, requiredPeriodStart, requiredPeriodEnd, minPeriodStart, maxPeriodEnd, false, nil)
}

func (c *CacheBase[Data, Key]) get(key Key, periodStart, periodEnd, minPeriodStart, maxPeriodEnd time.Time, fetchAllowed bool, extra interface{}) (CacheFetchResult[Data], bool, error) {
	keyStr := c.opts.KeyToStr(key)

	c.m.RLock()
	series := c.entriesPerKey[keyStr]
	c.m.RUnlock()

	if series == nil {
		var err error
		series, err = c.initKeyEntriesStorage(keyStr, key)
		if err != nil {
			return CacheFetchResult[Data]{}, false, errors.Wrapf(err, "failed to initialize entries storage for '%v'", keyStr)
		}
	}

	if len(series.Entries()) == 0 && !fetchAllowed {
		return CacheFetchResult[Data]{}, false, nil
	}

	var err error

	if minPeriodStart.IsZero() && maxPeriodEnd.IsZero() {
		var data []Data

		if data, err = c.getCachedData(series, periodStart, periodEnd); err == nil {
			return CacheFetchResult[Data]{PeriodStart: periodStart, PeriodEnd: periodEnd, Data: data}, true, nil
		}
	} else {
		if fetchAllowed {
			return CacheFetchResult[Data]{}, false, errors.New("not implemented: point is not allowed to be set when fetchAllowed is true")
		}

		var fetched CacheFetchResult[Data]

		if fetched, err = c.getCachedAllData(series, periodStart, periodEnd, minPeriodStart, maxPeriodEnd); err == nil {
			return fetched, true, nil
		}
	}

	var missingPeriodErr *sparse.MissingPeriodError[time.Time]

	if !errors.As(err, &missingPeriodErr) {
		return CacheFetchResult[Data]{}, false, errors.Wrapf(err, "failed to fetch from sparse storage entries of '%v' for period [%v; %v]", keyStr, periodStart, periodEnd)
	}
	if !fetchAllowed {
		return CacheFetchResult[Data]{}, false, nil
	}

	// TODO: further optimization can be done by fetching only missing periods
	data, err := c.loadDataFromSourceIntoCache(key, series, periodStart, periodEnd, extra)
	if err != nil {
		return CacheFetchResult[Data]{}, false, err
	}

	return CacheFetchResult[Data]{
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		Data:        data,
	}, false, nil
}

func (c *CacheBase[Data, Key]) GetCachedPeriodClosestFromStart(key Key, point time.Time, nonEmpty bool) (*TimePeriodBounds[Data], error) {
	// TODO: can be also optimized by writing special implementation for GetCachedOrClosestFromStart

	keyStr := c.opts.KeyToStr(key)

	c.m.RLock()
	series := c.entriesPerKey[keyStr]
	c.m.RUnlock()

	if series == nil {
		return nil, nil
	}

	series.Access.RLock()
	defer series.Access.RUnlock()

	entry := series.GetPeriodClosestFromStart(point, nonEmpty)
	if entry == nil {
		return nil, nil
	}

	res := &TimePeriodBounds[Data]{
		PeriodBounds: sparse.PeriodBounds[time.Time]{PeriodStart: entry.PeriodStart, PeriodEnd: entry.PeriodEnd},
	}
	if !entry.Empty {
		var err error
		res.First, err = entry.First()
		if err != nil {
			return nil, err
		}

		res.Last, err = entry.Last()
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (c *CacheBase[Data, Key]) GetCachedPeriodClosestFromEnd(key Key, point time.Time, nonEmpty bool) (*TimePeriodBounds[Data], error) {
	// TODO: can be also optimized by writing special implementation for GetCachedOrClosestFromEnd

	keyStr := c.opts.KeyToStr(key)

	c.m.RLock()
	series := c.entriesPerKey[keyStr]
	c.m.RUnlock()

	if series == nil {
		return nil, nil
	}

	series.Access.RLock()
	defer series.Access.RUnlock()

	entry := series.GetPeriodClosestFromEnd(point, nonEmpty)
	if entry == nil {
		return nil, nil
	}

	res := &TimePeriodBounds[Data]{
		PeriodBounds: sparse.PeriodBounds[time.Time]{PeriodStart: entry.PeriodStart, PeriodEnd: entry.PeriodEnd}}
	if !entry.Empty {
		var err error
		res.First, err = entry.First()
		if err != nil {
			return nil, err
		}

		res.Last, err = entry.Last()
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

var IdxCmp = time.Time.Compare

func newSparseTimeSeries[Data any](getTimestamp func(d *Data) time.Time, dataFactory sparse.SeriesDataFactory[Data, time.Time]) *sparse.Series[Data, time.Time] {
	return sparse.NewSeries[Data, time.Time](
		dataFactory,
		getTimestamp,
		IdxCmp,
		isTimeContinuous,
	)
}

func (c *CacheBase[Data, Key]) initKeyEntriesStorage(keyStr string, key Key) (*sparseSeriesT[Data], error) {
	c.m.Lock()
	defer c.m.Unlock()

	existingKeyEntries := c.entriesPerKey[keyStr]
	if existingKeyEntries != nil {
		return existingKeyEntries, nil
	}

	newSeries := &sparseSeriesT[Data]{
		Series: *newSparseTimeSeries(
			c.opts.GetTimestamp,
			func(
				getIdx func(data *Data) time.Time,
				idxCmp func(idx1, idx2 time.Time) int,
				periodStart, periodEnd time.Time, data []Data,
			) (sparse.SeriesData[Data, time.Time], error) {
				return c.opts.Storage.Add(key, periodStart, periodEnd, data)
			},
		),
		Access: sync.RWMutex{},
		Fetch:  sync.Mutex{},
	}

	if err := c.loadCache(key, newSeries); err != nil {
		return nil, errors.Wrapf(err, "failed to load cache for '%v'", keyStr)
	}
	c.entriesPerKey[keyStr] = newSeries

	return newSeries, nil
}

func (c *CacheBase[Data, Key]) loadCache(key Key, series *sparseSeriesT[Data]) error {
	state, err := c.opts.Storage.Load(key)
	if err != nil {
		return errors.Wrapf(err, "failed to load cache for '%v'", c.opts.KeyToStr(key))
	}

	if state == nil {
		return nil
	}

	prevPeriodEnd := time.Time{}
	for i, entry := range state.Entries {
		if err := c.verifyPeriodData(entry.PeriodStart, entry.PeriodEnd, nil, prevPeriodEnd); err != nil {
			return errors.Wrapf(err, "loaded cache of '%v' failed verification at index %v", c.opts.KeyToStr(key), i)
		}

		prevPeriodEnd = entry.PeriodEnd
	}

	if err := series.Restore((*sparse.SeriesState[Data, time.Time])(state)); err != nil {
		return errors.Wrapf(err, "failed to add period of '%v' to cache", c.opts.KeyToStr(key))
	}

	return nil
}

func (c *CacheBase[Data, Key]) verifyPeriodData(periodStart, periodEnd time.Time, periodData []Data, prevPeriodEnd time.Time) error {
	if c.opts.SkipDataVerification {
		return nil
	}

	if periodStart.After(periodEnd) {
		return errors.Errorf("corrupted loaded cache: periodStart > periodEnd: %v > %v", periodStart, periodEnd)
	}
	if !prevPeriodEnd.IsZero() && periodStart.Before(prevPeriodEnd) {
		return errors.Errorf("corrupted loaded cache for period [%v; %v]: periods are not sorted: prevPeriodEnd > currentPeriodStart: %v > %v",
			periodStart, periodEnd, prevPeriodEnd, periodStart)
	}

	if len(periodData) != 0 {
		firstElem := &periodData[0]
		lastElem := &periodData[len(periodData)-1]

		if c.opts.GetTimestamp(firstElem).After(c.opts.GetTimestamp(lastElem)) {
			return errors.Errorf("corrupted loaded cache for period [%v; %v]: data is not sorted: firstElemT > lastElemT: %v > %v",
				periodStart, periodEnd, c.opts.GetTimestamp(firstElem), c.opts.GetTimestamp(lastElem))
		}

		if c.opts.GetTimestamp(firstElem).Before(periodStart) {
			return errors.Errorf("corrupted loaded cache for period [%v; %v]: data is not sorted: firstElemT < periodStart: %v < %v",
				periodStart, periodEnd, c.opts.GetTimestamp(firstElem), periodStart)
		}
		if c.opts.GetTimestamp(lastElem).After(periodEnd) {
			return errors.Errorf("corrupted loaded cache for period [%v; %v]: data is not sorted: lastElemT > periodEnd: %v > %v",
				periodStart, periodEnd, c.opts.GetTimestamp(lastElem), periodEnd)
		}

		var prevDataT time.Time
		for i := 0; i < len(periodData); i++ {
			d := &periodData[i]
			dt := c.opts.GetTimestamp(d)

			if !prevDataT.IsZero() && dt.Before(prevDataT) {
				return errors.Errorf("corrupted loaded cache: data is not sorted: prevDataT > currentDataT: %v > %v, i = %v", prevDataT, dt, i)
			}

			prevDataT = dt
		}
	}

	return nil
}

func (c *CacheBase[Data, Key]) getCachedData(series *sparseSeriesT[Data], periodStart, periodEnd time.Time) ([]Data, error) {
	series.Access.RLock()
	defer series.Access.RUnlock()

	return series.Get(periodStart, periodEnd)
}

func (c *CacheBase[Data, Key]) getCachedAllData(series *sparseSeriesT[Data], requiredPeriodStart, requiredPeriodEnd, minPeriodStart, maxPeriodEnd time.Time) (CacheFetchResult[Data], error) {
	series.Access.RLock()
	defer series.Access.RUnlock()

	period := series.GetPeriod(requiredPeriodStart, requiredPeriodEnd)
	if period == nil {
		// TODO: this is not cool
		return CacheFetchResult[Data]{}, &sparse.MissingPeriodError[time.Time]{PeriodStart: requiredPeriodStart, PeriodEnd: requiredPeriodEnd}
	}

	assert(!period.PeriodStart.After(requiredPeriodStart) && !period.PeriodEnd.Before(requiredPeriodEnd),
		"period is out of range: period = [ %v ; %v ], range = [ %v ; %v ]",
		period.PeriodStart, period.PeriodEnd, requiredPeriodStart, requiredPeriodEnd,
	)

	fetchedPeriodStart, fetchedPeriodEnd, data, err := period.GetAllInRange(minPeriodStart, maxPeriodEnd)
	if err != nil {
		return CacheFetchResult[Data]{}, err
	}

	return CacheFetchResult[Data]{
		PeriodStart: fetchedPeriodStart,
		PeriodEnd:   fetchedPeriodEnd,
		Data:        data,
	}, nil
}

func (c *CacheBase[Data, Key]) loadDataFromSourceIntoCache(key Key, series *sparseSeriesT[Data], periodStart, periodEnd time.Time, extra interface{}) ([]Data, error) {
	series.Fetch.Lock()
	defer series.Fetch.Unlock()

	if data, err := c.getCachedData(series, periodStart, periodEnd); err == nil {
		return data, nil
	}

	var err error

	fetchPeriodStart := periodStart
	closestFromStart := series.GetPeriodClosestFromStart(periodStart, true)
	var closestFromStartData *Data
	if closestFromStart != nil {
		closestFromStartData, err = closestFromStart.Last()
		if err != nil {
			return nil, err
		}

		if closestFromStart.PeriodEnd.After(periodStart) {
			fetchPeriodStart = closestFromStart.PeriodEnd
		}
	}

	fetchPeriodEnd := periodEnd
	var closestFromEndData *Data
	closestFromEnd := series.GetPeriodClosestFromEnd(periodEnd, true)
	if closestFromEnd != nil {
		closestFromEndData, err = closestFromEnd.First()
		if err != nil {
			return nil, err
		}

		if closestFromEnd.PeriodStart.Before(periodEnd) {
			fetchPeriodEnd = closestFromEnd.PeriodStart
		}
	}

	entryFromSource, err := c.fetchEntriesFromSource(key, fetchPeriodStart, fetchPeriodEnd, closestFromStartData, closestFromEndData, extra)
	if err != nil {
		return nil, err
	}

	// Using Get with original bounds, because new data may contain bigger period than requested.
	return c.addAndGetCacheEntries(key, series, entryFromSource, periodStart, periodEnd)
}

func (c *CacheBase[Data, Key]) addAndGetCacheEntries(key Key, series *sparseSeriesT[Data],
	addedEntry CacheFetchResult[Data], getPeriodStart, getPeriodEnd time.Time) ([]Data, error) {

	series.Access.Lock()
	defer series.Access.Unlock()

	series.AddPeriod(addedEntry.PeriodStart, addedEntry.PeriodEnd, addedEntry.Data)

	allEntries := series.GetAllEntries()
	allEntriesFields := make([]*sparse.SeriesEntryFields[Data, time.Time], 0, len(allEntries))

	for _, entry := range allEntries {
		allEntriesFields = append(allEntriesFields, &entry.SeriesEntryFields)
	}

	if err := c.opts.Storage.Save(key, &CacheState[Data]{Entries: allEntriesFields}, []*CacheFetchResult[Data]{&addedEntry}); err != nil {
		return nil, err
	}

	res, err := series.Get(getPeriodStart, getPeriodEnd)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *CacheBase[Data, Key]) fetchEntriesFromSource(key Key, periodStart, periodEnd time.Time, closestFromStart, closestFromEnd *Data, extra interface{}) (CacheFetchResult[Data], error) {
	if closestFromStart != nil {
		assert(!periodStart.Before(c.opts.GetTimestamp(closestFromStart)), "%v: periodStart < closestFromStart: %v < %v", key, periodStart, c.opts.GetTimestamp(closestFromStart))
	}
	if closestFromEnd != nil {
		assert(!periodEnd.After(c.opts.GetTimestamp(closestFromEnd)), "periodEnd > closestFromEnd: %v > %v", periodEnd, c.opts.GetTimestamp(closestFromEnd))
	}

	entryFromSource, err := c.opts.GetFromSource(key, periodStart, periodEnd, closestFromStart, closestFromEnd, extra)
	if err != nil {
		return CacheFetchResult[Data]{}, errors.Wrapf(err, "failed to fetch from source entries of '%v' for period [%v; %v]", c.opts.KeyToStr(key), periodStart, periodEnd)
	}
	if entryFromSource.PeriodStart.IsZero() || entryFromSource.PeriodEnd.IsZero() {
		return CacheFetchResult[Data]{}, errors.Errorf("fetched entries of '%v' for period [%v; %v] have zero period bounds: [%v; %v]",
			c.opts.KeyToStr(key), periodStart, periodEnd, entryFromSource.PeriodStart, entryFromSource.PeriodEnd)
	}
	if entryFromSource.PeriodStart.After(periodStart) || entryFromSource.PeriodEnd.Before(periodEnd) {
		return CacheFetchResult[Data]{}, errors.Errorf("fetched entries of '%v' for period [%v; %v] does not contain requested period [%v; %v]", c.opts.KeyToStr(key),
			entryFromSource.PeriodStart, entryFromSource.PeriodEnd, periodStart, periodEnd)
	}

	if err := c.verifyPeriodData(entryFromSource.PeriodStart, entryFromSource.PeriodEnd, entryFromSource.Data, time.Time{}); err != nil {
		return CacheFetchResult[Data]{}, errors.Wrapf(err, "fetched entries of '%v' for period [%v; %v] failed verification", c.opts.KeyToStr(key), periodStart, periodEnd)
	}

	return entryFromSource, nil
}

func (c *CacheBase[Data, Key]) Close() {
}

type sparseSeriesT[Data any] struct {
	sparse.Series[Data, time.Time]
	Access sync.RWMutex
	Fetch  sync.Mutex // This is used mainly to not block cache getten upon fetching. But also improves performance.
	//TODO: can be optimizer more by having fetch lock per period
}

func isTimeContinuous(smaller, bigger time.Time) bool {
	diff := bigger.Sub(smaller)
	return diff >= 0 && diff <= time.Nanosecond
}

type CacheStorage[Data any, Key any] interface {
	Load(key Key) (*CacheState[Data], error)
	Save(key Key, state *CacheState[Data], updated []*CacheFetchResult[Data]) error
	Add(key Key, periodStart, periodEnd time.Time, data []Data) (CacheData[Data], error)
}
