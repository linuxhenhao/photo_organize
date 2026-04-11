//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !windows

package metadata

import (
	"os"
	"time"
)

// Fallback for systems where specific birth time syscall isn't implemented here.
func GetStatBirthTime(fi os.FileInfo) (time.Time, bool) {
	return time.Time{}, false // Indicate birth time couldn't be fetched
}
