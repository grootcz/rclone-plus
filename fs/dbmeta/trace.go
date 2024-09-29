package dbmeta

import (
	"time"

	"github.com/rclone/rclone/fs"
	"gorm.io/gorm"
	"gorm.io/gorm/utils"
)

type SqlTraceInfo struct {
	Timestamp   int64   `json:"timestamp"`     // time, format like：2006-01-02 15:04:05
	Stack       string  `json:"stack"`         // file address and line number
	SQL         string  `json:"sql"`           // SQL statement
	Rows        int64   `json:"rows_affected"` // Number of rows affected
	CostSeconds float64 `json:"cost_seconds"`  // Execution time (in seconds)
	ErrorStr    string  `json:"error_str"`
}

var (
	cst *time.Location
)

const (
	callBackBeforeName = "trace:before"
	callBackAfterName  = "trace:after"
	startTime          = "_start_time"
)

type TracePlugin struct{}

func (op *TracePlugin) Name() string {
	return "tracePlugin"
}

func (op *TracePlugin) Initialize(db *gorm.DB) (err error) {
	//create
	if err = db.Callback().Create().Before("gorm:before_create").Register("create"+callBackBeforeName, before); err != nil {
		return err
	}
	if err = db.Callback().Create().After("gorm:after_create").Register("create"+callBackAfterName, after); err != nil {
		return err
	}

	//query
	if err = db.Callback().Query().Before("gorm:before_query").Register("query"+callBackBeforeName, before); err != nil {
		return err
	}
	if err = db.Callback().Query().After("gorm:after_query").Register("query"+callBackAfterName, after); err != nil {
		return err
	}

	//update
	if err = db.Callback().Update().Before("gorm:before_update").Register("update"+callBackBeforeName, before); err != nil {
		return err
	}
	if err = db.Callback().Update().After("gorm:after_update").Register("update"+callBackAfterName, after); err != nil {
		return err
	}

	//delete
	if err = db.Callback().Delete().Before("gorm:before_delete").Register("delete"+callBackBeforeName, before); err != nil {
		return err
	}
	if err = db.Callback().Delete().After("gorm:after_delete").Register("delete"+callBackAfterName, after); err != nil {
		return err
	}

	//row
	if err = db.Callback().Row().Before("gorm:before_row").Register("row"+callBackBeforeName, before); err != nil {
		return err
	}
	if err = db.Callback().Row().After("gorm:after_row").Register("row"+callBackAfterName, after); err != nil {
		return err
	}

	//raw
	if err = db.Callback().Raw().Before("gorm:before_raw").Register("raw"+callBackBeforeName, before); err != nil {
		return err
	}
	if err = db.Callback().Raw().After("gorm:after_raw").Register("raw"+callBackAfterName, after); err != nil {
		return err
	}

	return
}

func before(db *gorm.DB) {
	db.InstanceSet(startTime, time.Now())
	return
}

func after(db *gorm.DB) {
	_ts, isExist := db.InstanceGet(startTime)
	if !isExist {
		return
	}

	ts, ok := _ts.(time.Time)
	if !ok {
		return
	}

	sql := db.Dialector.Explain(db.Statement.SQL.String(), db.Statement.Vars...)

	sqlInfo := new(SqlTraceInfo)
	sqlInfo.Timestamp = time.Now().Unix()
	sqlInfo.SQL = sql
	sqlInfo.Stack = utils.FileWithLineNum()
	sqlInfo.Rows = db.Statement.RowsAffected
	sqlInfo.CostSeconds = time.Since(ts).Seconds()

	fs.Debugf(nil, "db trace : %+v", sqlInfo)

	return
}

var _ gorm.Plugin = &TracePlugin{}
