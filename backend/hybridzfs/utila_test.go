package hybridzfs_test

import (
	"testing"

	"github.com/rclone/rclone/backend/hybridzfs"
)

func Test_IsPartialFile(t *testing.T) {
	name := "新建 文本文档.txt.efe454dd.partial1"
	t.Logf("%t\n", hybridzfs.IsPartialFile(name))
}
