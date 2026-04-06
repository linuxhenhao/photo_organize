//go:build linux

package metadata

import (
	"os"
	"syscall"
	"time"
)

func GetStatBirthTime(fi os.FileInfo) (time.Time, bool) {
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return time.Time{}, false
	}
	// Btim is birth time on Linux
	return time.Unix(stat.Ctim.Sec, stat.Ctim.Nsec), true
}
