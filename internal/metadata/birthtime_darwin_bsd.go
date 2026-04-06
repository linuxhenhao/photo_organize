//go:build darwin || freebsd || openbsd || netbsd

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
	// Birthtimespec is birth time on macOS and BSD systems
	return time.Unix(stat.Birthtimespec.Sec, stat.Birthtimespec.Nsec), true
}
