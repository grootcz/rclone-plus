package dbmeta

import (
	"path/filepath"
	"sync"

	"gorm.io/gorm"
)

type OriginCache struct {
	db     *gorm.DB
	rwLock sync.RWMutex
	tracer *TracePlugin
}

var originCache *OriginCache
var originCacheOnce sync.Once

func GetOriginCache() *OriginCache {
	originCacheOnce.Do(func() {
		originCache = &OriginCache{}
	})

	return originCache
}

func (oc *OriginCache) Init(path string) error {
	file := filepath.Join(path, DatabaseFileOfOriginCache)
	db, tracer, err := OpenDB(file)
	if err != nil {
		return err
	}

	oc.db = db
	oc.tracer = tracer

	err = oc.createTable()
	if err != nil {
		return err
	}

	return nil
}

const DatabaseFileOfOriginCache = "origin_cache.db"
