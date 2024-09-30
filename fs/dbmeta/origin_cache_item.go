package dbmeta

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/ranges"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Info is same as vfscache item info
type Info struct {
	ModTime     time.Time     // last time file was modified
	ATime       time.Time     // last time file was accessed
	Size        int64         // size of the file
	Rs          ranges.Ranges // which parts of the file are present
	Fingerprint string        // fingerprint of remote object
	Dirty       bool          // set if the backing file has been modified
}

type Item struct {
	ParentPath string       `gorm:"column:parent_path" json:"parent_path"`
	Name       string       `gorm:"column:name" json:"name"`
	Type       fs.EntryType `gorm:"column:type" json:"type"`
	Size       int64        `gorm:"column:size" json:"size"`
	State      int32        `gorm:"column:state" json:"state"`
	Sha1       string       `gorm:"column:sha1" json:"sha1"`
	CreatedAt  int64        `gorm:"autoCreateTime;column:created_at" json:"created_at"`
	UpdatedAt  int64        `gorm:"autoUpdateTime;column:updated_at" json:"updated_at"`
	Info       []byte       `gorm:"column:info" json:"info"`
	//Opts            RemoteObjOptions `gorm:"-" json:"-"`
}

const TableNameOfOriginCacheItem = "item_list"

func (oc *OriginCache) createTable() error {
	sqlStr := "CREATE TABLE IF NOT EXISTS `item_list` (" +
		"`parent_path` NCHAR(10240) NOT NULL DEFAULT ''," +
		"`name` NCHAR(10240) NOT NULL DEFAULT ''," +
		"`type`        INT          NOT NULL DEFAULT 0," +
		"`size`        INT          NOT NULL DEFAULT 0," +
		"`state`       INT          NOT NULL DEFAULT 0," +
		"`sha1`        NCHAR(1024)  NOT NULL DEFAULT ''," +
		"`created_at`  INT          NOT NULL DEFAULT 0," +
		"`updated_at`  INT          NOT NULL DEFAULT 0," +
		"`info`        BLOB         NULL" +
		");"
	result := oc.db.Exec(sqlStr)
	if result.Error != nil {
		fs.Errorf(nil, "create table item_list error = %s", result.Error)
		return result.Error
	}

	uIdxStr := `CREATE UNIQUE INDEX idx_u_item ON item_list (parent_path, name)`
	result = oc.db.Exec(uIdxStr)
	if result.Error != nil {
		if strings.Contains(result.Error.Error(), ErrorOfAlreadyExist) {
			return nil
		}

		fs.Errorf(nil, "create table item_list unique index error = %s", result.Error)
		return result.Error
	}

	return nil
}

func (oc *OriginCache) FilterName(name string, operator string) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where(fmt.Sprintf("name %s ?", operator), name)
	}
}

func (oc *OriginCache) FilterParentPath(path string, operator string) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where(fmt.Sprintf("name %s ?", operator), path)
	}
}

func (oc *OriginCache) basicFilter(filterList ...func(*gorm.DB) *gorm.DB) *gorm.DB {
	if filterList == nil || len(filterList) == 0 {
		return oc.db
	} else {
		return oc.db.Scopes(filterList...)
	}
}

func (oc *OriginCache) UpsertItem(ctx context.Context, item Item) error {
	ctxTemp, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var info Info
	if item.Type == fs.EntryObject {
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		err := json.Unmarshal(item.Info, &info)
		if err != nil {
			fs.Errorf(nil, "unmarshal info=%s error = %s", string(item.Info), err.Error())
			return err
		}
	}

	oc.rwLock.Lock()
	defer oc.rwLock.Unlock()

	result := oc.db.Table(TableNameOfOriginCacheItem).WithContext(ctxTemp).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "parent_path"}, {Name: "name"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"updated_at": time.Now().Unix(),
			"sha1":       info.Fingerprint,
			"size":       info.Size,
			"info":       item.Info,
		}),
	}).Create(&item)
	if result.Error != nil {
		fs.Errorf(nil, "create item=%+v error = %s", item, result.Error)
		return result.Error
	}

	return nil
}

func (oc *OriginCache) DeleteItem(ctx context.Context, name string) error {
	ctxTemp, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	parentDir, itemName := filepath.Split(name)

	oc.rwLock.Lock()
	defer oc.rwLock.Unlock()

	result := oc.db.Table(TableNameOfOriginCacheItem).
		Where("parent_path=? and name=?", parentDir, itemName).
		WithContext(ctxTemp).Delete(&Item{})
	if result.Error != nil {
		fs.Errorf(nil, "delete item=%s error = %s", name, result.Error)
		return result.Error
	}

	return nil
}

func (oc *OriginCache) ModifyItem(ctx context.Context, name string, infoMap map[string]interface{}) error {
	ctxTemp, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	parentDir, itemName := filepath.Split(name)

	oc.rwLock.Lock()
	defer oc.rwLock.Unlock()

	result := oc.db.Table(TableNameOfOriginCacheItem).
		Where("parent_path=? and name=?", parentDir, itemName).
		WithContext(ctxTemp).Updates(infoMap)
	if result.Error != nil {
		fs.Errorf(nil, "update item=%s error = %s", name, result.Error)
		return result.Error
	}

	return nil
}

func (oc *OriginCache) RenameItem(ctx context.Context, oldName string, newName string) error {
	ctxTemp, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	oldParentDir, oldItemName := filepath.Split(oldName)
	newParentDir, newItemName := filepath.Split(newName)

	oc.rwLock.Lock()
	defer oc.rwLock.Unlock()

	result := oc.db.Table(TableNameOfOriginCacheItem).
		Where("parent_path=? and name=?", oldParentDir, oldItemName).
		WithContext(ctxTemp).Updates(map[string]interface{}{
		"parent_path": newParentDir,
		"name":        newItemName,
		//"updated_at":  time.Now().Unix(),
	})
	if result.Error != nil {
		if strings.Contains(result.Error.Error(), ErrorOfUniqueKeyExist) {
			err := oc.db.Transaction(func(tx *gorm.DB) error {
				var sqlMeta Item
				rd := tx.Table(TableNameOfOriginCacheItem).
					Where("parent_path=? and name=?", newParentDir, newItemName).
					Delete(&sqlMeta)
				if rd.Error != nil {
					fs.Errorf(nil, "error = %s", result.Error)
					return rd.Error
				}

				ru := tx.Table(TableNameOfOriginCacheItem).
					Where("parent_path=? and name=?", oldParentDir, oldItemName).
					Updates(map[string]interface{}{
						"parent_path": newParentDir,
						"name":        newItemName,
						//"updated_at":  time.Now().Unix(),
					})
				if ru.Error != nil {
					fs.Errorf(nil, "error = %s", result.Error)
					return rd.Error
				}

				return nil
			})
			if err != nil {
				return err
			}

			return nil
		}

		fs.Errorf(nil, "update item %s => %s error = %s", oldName, newName, result.Error)
		return result.Error
	}

	return nil
}

func (oc *OriginCache) GetItem(ctx context.Context, name string) (Item, error) {
	ctxTemp, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	parentDir, itemName := filepath.Split(name)
	var item Item

	oc.rwLock.RLock()
	defer oc.rwLock.RUnlock()

	result := oc.db.Table(TableNameOfOriginCacheItem).
		Where("parent_path=? and name=?", parentDir, itemName).
		WithContext(ctxTemp).First(&item)
	if result.Error != nil {
		//fs.Errorf(nil, "get item=%s error = %s", name, result.Error)
		return item, result.Error
	}

	return item, nil
}

func (oc *OriginCache) GetAllItem(ctx context.Context) ([]Item, error) {
	ctxTemp, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	allList := make([]Item, 0, 10000)
	TempList := make([]Item, 0, 1000)

	oc.rwLock.RLock()
	defer oc.rwLock.RUnlock()

	result := oc.db.Table(TableNameOfOriginCacheItem).
		WithContext(ctxTemp).FindInBatches(&TempList, 1000, func(tx *gorm.DB, batch int) error {
		allList = append(allList, TempList...)
		return nil
	})
	if result.Error != nil {
		fs.Errorf(nil, "get item_meta error = %s", result.Error)
		return allList, result.Error
	}

	return allList, nil
}

func (oc *OriginCache) DeleteAllItem(ctx context.Context) error {
	ctxTemp, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	oc.rwLock.Lock()
	defer oc.rwLock.Unlock()

	result := oc.db.Table(TableNameOfOriginCacheItem).
		WithContext(ctxTemp).Delete(&Item{})
	if result.Error != nil {
		fs.Errorf(nil, "delete item_meta error = %s", result.Error)
		return result.Error
	}

	return nil
}
