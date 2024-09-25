package timeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
)

type FileCacheOptions[Data any, Key any] struct {
	CacheDir             string
	KeyToStr             func(Key) string
	GetTimestamp         func(d *Data) time.Time
	GetFromSource        CacheSource[Data, Key]
	SkipDataVerification bool
}

func NewFileCache[Data any, Key any](opts FileCacheOptions[Data, Key]) (*FileCache[Data, Key], error) {
	if opts.CacheDir == "" {
		return nil, errors.New("cache directory is not set")
	}

	if err := os.MkdirAll(opts.CacheDir, 0755); err != nil && !os.IsExist(err) {
		return nil, errors.Wrapf(err, "failed to create cache directory %v", opts.CacheDir)
	}

	keyToStr := opts.KeyToStr
	if keyToStr == nil {
		keyToStr = CreateConvertorToString[Key]()
	}

	c := &FileCache[Data, Key]{
		cacheDir: opts.CacheDir,
		keyToStr: keyToStr,
	}

	c.MemoryCache = *NewMemoryCache[Data, Key](MemoryCacheOptions[Data, Key]{
		KeyToStr:             opts.KeyToStr,
		GetTimestamp:         opts.GetTimestamp,
		GetFromSource:        opts.GetFromSource,
		Load:                 c.loadCacheFromFile,
		Save:                 c.saveCacheToFile,
		SkipDataVerification: opts.SkipDataVerification,
	})

	return c, nil
}

type FileCache[Data any, Key any] struct {
	cacheDir string
	keyToStr func(Key) string
	MemoryCache[Data, Key]
}

var _ Cache[struct{}, int64] = &FileCache[struct{}, int64]{}

func (c *FileCache[Data, Key]) cacheFilePath(key Key) string {
	return path.Join(c.cacheDir, fmt.Sprintf("cache_%v.json", c.keyToStr(key)))
}

func (c *FileCache[Data, Key]) loadCacheFromFile(key Key) (*MemoryCacheState[Data], error) {
	cacheFilePath := c.cacheFilePath(key)
	cacheBytes, err := os.ReadFile(cacheFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	if len(cacheBytes) == 0 {
		return nil, nil
	}

	var dump []*CacheFetchResult[Data]
	if err := json.Unmarshal(cacheBytes, &dump); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal cache from file %v: %v", cacheFilePath, string(cacheBytes))
	}

	return &MemoryCacheState[Data]{
		Entries: dump,
	}, nil
}

func (c *FileCache[Data, Key]) saveCacheToFile(key Key, state *CacheState[Data], _ []*CacheFetchResult[Data]) error {
	var dump []*CacheFetchResult[Data]
	for _, entry := range state.Entries {
		data, err := entry.Data.Get(entry.PeriodStart, entry.PeriodEnd)
		if err != nil {
			return err
		}

		dump = append(dump, &CacheFetchResult[Data]{
			PeriodStart: entry.PeriodStart,
			PeriodEnd:   entry.PeriodEnd,
			Data:        data,
		})
	}

	cacheFilePath := c.cacheFilePath(key)

	cacheBytes, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return errors.Wrapf(err, "failed to marshal cache to file %v", cacheFilePath)
	}

	if err := os.WriteFile(cacheFilePath, cacheBytes, 0644); err != nil {
		return errors.Wrapf(err, "failed to write cache to file %v", cacheFilePath)
	}

	return nil
}
