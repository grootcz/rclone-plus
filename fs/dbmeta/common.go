package dbmeta

import (
	"github.com/rclone/rclone/fs"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	ErrorOfAlreadyExist   = "already exists"
	ErrorOfUniqueKeyExist = "UNIQUE constraint failed"
)

const (
	SqliteFileBrokenErrorInfoTypeOne = "unsupported file format"
	SqliteFileBrokenErrorInfoTypeTwo = "file is not a database"
)

var dbTracer *TracePlugin

func OpenDB(path string) (*gorm.DB, *TracePlugin, error) {
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		fs.Errorf(nil, "open db file %s failed: %s", path, err)
		return nil, nil, err
	}

	dbTracer = new(TracePlugin)
	err = dbTracer.Initialize(db)
	if err != nil {
		fs.Errorf(nil, "set db tracer failed: %s", err)
		return nil, nil, err
	}

	return db, dbTracer, nil
}
