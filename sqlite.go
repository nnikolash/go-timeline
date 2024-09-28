package timeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/glebarez/sqlite"
	sparse "github.com/nnikolash/go-sparse"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type SqliteCacheOptions[Data any, Key any, ID comparable] struct {
	CacheDir             string                  // (Required) Directory to store cache files
	GetTimestamp         func(d *Data) time.Time // (Required) Get timestamp of data entry
	GetID                func(d *Data) ID        // (Required) Get ID of data entry. Needed to properly order data in DB if timestamp is not unique.
	GetFromSource        CacheSource[Data, Key]  // (Required) Fetch data from source
	KeyToStr             func(Key) string        // (Optional) Convert key to string
	SkipDataVerification bool
}

func NewSqliteCache[Data any, Key any, ID comparable](opts SqliteCacheOptions[Data, Key, ID]) (*SqliteCache[Data, Key, ID], error) {
	if opts.KeyToStr == nil {
		opts.KeyToStr = CreateConvertorToString[Key]()
	}

	if err := os.MkdirAll(opts.CacheDir, 0755); err != nil && !os.IsExist(err) {
		return nil, errors.Wrapf(err, "failed to create cache directory %v", opts.CacheDir)
	}

	return &SqliteCache[Data, Key, ID]{
		CacheBase: *NewCacheBase[Data, Key](CacheBaseOptions[Data, Key]{
			KeyToStr:      opts.KeyToStr,
			GetTimestamp:  opts.GetTimestamp,
			GetFromSource: opts.GetFromSource,
			Storage: &sqliteCacheStorage[Data, Key, ID]{
				cacheDir:     opts.CacheDir,
				getID:        opts.GetID,
				getTimestamp: opts.GetTimestamp,
				keyToStr:     opts.KeyToStr,
				connections:  make(map[string]*gorm.DB),
			},
			SkipDataVerification: opts.SkipDataVerification,
		}),
	}, nil
}

type SqliteCache[Data any, Key any, ID comparable] struct {
	CacheBase[Data, Key]
}

var _ Cache[struct{}, int64] = &SqliteCache[struct{}, int64, string]{}

type sqliteCacheStorage[Data any, Key any, ID comparable] struct {
	cacheDir     string
	getID        func(d *Data) ID
	getTimestamp func(d *Data) time.Time
	keyToStr     func(Key) string
	connections  map[string]*gorm.DB
}

var _ CacheStorage[struct{}, int64] = &sqliteCacheStorage[struct{}, int64, string]{}

func (c *sqliteCacheStorage[Data, Key, ID]) Load(key Key) (*CacheState[Data], error) {
	segments, err := c.getAllSegmentsFromDB(key)
	if err != nil {
		return nil, err
	}

	stateSegments := make([]*sparse.SeriesSegmentFields[Data, time.Time], 0, len(segments))

	for _, segment := range segments {
		stateSegments = append(stateSegments, &sparse.SeriesSegmentFields[Data, time.Time]{
			PeriodBounds: sparse.PeriodBounds[time.Time]{
				PeriodStart: segment.Start,
				PeriodEnd:   segment.End,
			},
			Data:  newSqliteCacheData[Data](key, c),
			Empty: segment.IsEmpty,
		})
	}
	return &CacheState[Data]{
		Segments: stateSegments,
	}, nil
}

func (c *sqliteCacheStorage[Data, Key, ID]) Save(key Key, state *CacheState[Data], updated []*CacheStateSegment[Data]) error {
	segments := make([]dbSegmentT, 0, len(state.Segments))
	for _, segment := range state.Segments {
		segments = append(segments, dbSegmentT{
			Start:   segment.PeriodStart,
			End:     segment.PeriodEnd,
			IsEmpty: segment.Empty,
		})
	}
	return c.saveSegmentsToDB(key, segments)
}

func (c *sqliteCacheStorage[Data, Key, ID]) Add(key Key, periodStart, periodEnd time.Time, data []Data) (CacheData[Data], error) {
	if err := c.saveDataToDB(key, data); err != nil {
		return nil, err
	}

	return newSqliteCacheData[Data](key, c), nil
}

func (c *sqliteCacheStorage[Data, Key, ID]) cacheFilePath(key Key) string {
	return path.Join(c.cacheDir, fmt.Sprintf("sqlite_timeline_cache_%v.db", key))
}

func (c *sqliteCacheStorage[Data, Key, ID]) getConn(key Key) (*gorm.DB, error) {
	keyStr := c.keyToStr(key)

	db, connOpen := c.connections[keyStr]
	if connOpen {
		return db, nil
	}

	dbFilePath := c.cacheFilePath(key)

	var err error
	db, err = gorm.Open(sqlite.Open(dbFilePath), &gorm.Config{
		Logger: &loggerWithFilename{
			l:        logger.Default.LogMode(logger.Warn),
			filename: dbFilePath,
		},
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open db file %v", c.cacheFilePath(key))
	}

	if err := db.AutoMigrate(&dbSegmentT{}, &dbDataT[ID]{}); err != nil {
		return nil, errors.Wrapf(err, "failed to migrate db schema")
	}

	c.connections[keyStr] = db // TODO: pool with ttl

	return db, nil
}

func (c *sqliteCacheStorage[Data, Key, ID]) dbRequest(key Key, request func(db *gorm.DB) error) error {
	db, err := c.getConn(key)
	if err != nil {
		return err
	}

	return db.Transaction(func(tx *gorm.DB) error {
		if err := request(tx); err != nil {
			return errors.Wrapf(err, "failed to execute db request")
		}

		return nil
	})
}

type dbSegmentT struct {
	Start   time.Time `gorm:"index"`
	End     time.Time
	IsEmpty bool
}

func (c *sqliteCacheStorage[Data, Key, ID]) getAllSegmentsFromDB(key Key) ([]dbSegmentT, error) {
	var segments []dbSegmentT
	if err := c.dbRequest(key, func(db *gorm.DB) error {
		return db.Order("start ASC").Find(&segments).Error
	}); err != nil {
		return nil, err
	}

	return segments, nil
}

func (c *sqliteCacheStorage[Data, Key, ID]) saveSegmentsToDB(key Key, segments []dbSegmentT) error {
	// TODO: what is there is more than 999 segments? We cant use batching here...

	for i := range segments {
		segments[i].Start = segments[i].Start.UTC()
		segments[i].End = segments[i].End.UTC()
	}

	return c.dbRequest(key, func(db *gorm.DB) error {
		if err := db.Where("1 = 1").Delete(&dbSegmentT{}).Error; err != nil {
			return errors.Wrapf(err, "failed to delete segments from db for key %v", key)
		}

		if err := db.Save(segments).Error; err != nil {
			return errors.Wrapf(err, "failed to save segments to db for key %v", key)
		}

		return nil
	})
}

func (c *sqliteCacheStorage[Data, Key, ID]) getRangeFromDB(key Key, from, to time.Time) ([]Data, error) {
	var data []dbDataT[ID]
	if err := c.dbRequest(key, func(db *gorm.DB) error {
		return db.Where("date >= ? AND date <= ?", from.UTC(), to.UTC()).Order("date ASC, id asc").Find(&data).Error
	}); err != nil {
		return nil, err
	}

	decoded := make([]Data, len(data))
	for i := range data {
		if err := json.Unmarshal(data[i].Content, &decoded[i]); err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal data content")
		}
	}

	return decoded, nil
}

func (c *sqliteCacheStorage[Data, Key, ID]) getOneFromDB(key Key, getLast bool, query interface{}, args ...interface{}) (*Data, error) {
	order := "ASC"
	if getLast {
		order = "DESC"
	}

	var data []dbDataT[ID]
	if err := c.dbRequest(key, func(db *gorm.DB) error {
		return db.Where(query, args...).Order("date " + order + ", id " + order).Limit(1).Find(&data).Error
	}); err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	if len(data) > 1 {
		return nil, errors.Errorf("unexpected number of db entries: %v", len(data))
	}

	var decoded Data
	if err := json.Unmarshal(data[0].Content, &decoded); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal data content")
	}

	return &decoded, nil
}

func (c *sqliteCacheStorage[Data, Key, ID]) getFirstFromDB(key Key, t time.Time) (*Data, error) {
	return c.getOneFromDB(key, false, "date >= ?", t.UTC())
}

func (c *sqliteCacheStorage[Data, Key, ID]) getLastFromDB(key Key, t time.Time) (*Data, error) {
	return c.getOneFromDB(key, true, "date <= ?", t.UTC())
}

func (c *sqliteCacheStorage[Data, Key, ID]) saveDataToDB(key Key, data []Data) error {
	encoded := make([]dbDataT[ID], 0, len(data))
	for i := range data {
		j, err := json.Marshal(data[i])
		if err != nil {
			return errors.Wrapf(err, "failed to marshal data content")
		}

		encoded = append(encoded, dbDataT[ID]{
			ID:      c.getID(&data[i]),
			Date:    c.getTimestamp(&data[i]).UTC(),
			Content: j,
		})
	}

	const batchSize = 500

	for i := 0; i < len(encoded); i += batchSize {
		batch := encoded[i:min(i+batchSize, len(encoded))]
		if err := c.dbRequest(key, func(db *gorm.DB) error {
			return db.Save(batch).Error
		}); err != nil {
			return err
		}
	}

	return nil
}

func (c *sqliteCacheStorage[Data, Key, ID]) Close() {
	for _, db := range c.connections {
		sqlDB, err := db.DB()
		if err != nil {
			continue
		}

		if _, err := sqlDB.Exec("PRAGMA optimize"); err != nil {
			log.Printf("ERROR: Failed to optimize db %v: %v", db.Name(), err)
		}

		sqlDB.Close()
	}

}

type dbDataT[ID comparable] struct {
	ID      ID        `gorm:"primaryKey;autoIncrement:false;index:sort,priority:2"`
	Date    time.Time `gorm:"index:sort,priority:1"`
	Content []byte
}

func newSqliteCacheData[Data any, Key any, ID comparable](key Key, storage *sqliteCacheStorage[Data, Key, ID]) *sqliteCacheData[Data, Key, ID] {
	return &sqliteCacheData[Data, Key, ID]{
		key:     key,
		storage: storage,
	}
}

type sqliteCacheData[Data any, Key any, ID comparable] struct {
	storage *sqliteCacheStorage[Data, Key, ID]
	key     Key
}

var _ CacheData[struct{}] = &sqliteCacheData[struct{}, string, int64]{}

func (d *sqliteCacheData[Data, Key, ID]) Get(periodStart, periodEnd time.Time) ([]Data, error) {
	return d.storage.getRangeFromDB(d.key, periodStart, periodEnd)
}

func (d *sqliteCacheData[Data, Key, ID]) GetEndOpen(periodStart, periodEnd time.Time) ([]Data, error) {
	return d.storage.getRangeFromDB(d.key, periodStart, periodEnd.Add(time.Nanosecond))
}

func (d *sqliteCacheData[Data, Key, ID]) Merge(data []Data) error {
	if len(data) == 0 {
		return nil
	}

	if err := d.storage.saveDataToDB(d.key, data); err != nil {
		return err
	}

	return nil
}

func (d *sqliteCacheData[Data, Key, ID]) First(idx time.Time) (*Data, error) {
	return d.storage.getFirstFromDB(d.key, idx)
}

func (d *sqliteCacheData[Data, Key, ID]) Last(idx time.Time) (*Data, error) {
	return d.storage.getLastFromDB(d.key, idx)
}

type loggerWithFilename struct {
	l        logger.Interface
	filename string
}

var _ logger.Interface = &loggerWithFilename{}

func (l *loggerWithFilename) LogMode(level logger.LogLevel) logger.Interface {
	return &loggerWithFilename{
		l:        l.l.LogMode(level),
		filename: l.filename,
	}
}

func (l *loggerWithFilename) Info(ctx context.Context, msg string, data ...interface{}) {
	l.l.Info(ctx, fmt.Sprintf("%v: %v", l.filename, msg), data...)
}

func (l *loggerWithFilename) Warn(ctx context.Context, msg string, data ...interface{}) {
	// TODO: wtf? why is it not used?
	l.l.Warn(ctx, fmt.Sprintf("%v: %v", l.filename, msg), data...)
}

func (l *loggerWithFilename) Error(ctx context.Context, msg string, data ...interface{}) {
	l.l.Error(ctx, fmt.Sprintf("%v: %v", l.filename, msg), data...)
}

func (l *loggerWithFilename) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	l.l.Trace(ctx, begin, fc, err)
}
