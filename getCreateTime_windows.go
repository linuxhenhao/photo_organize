//go:build windows

package main

import (
	"os"
	"syscall"
	"time"
)

func getStatBirthTime(fi os.FileInfo) (time.Time, bool) {
	d, ok := fi.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return time.Time{}, false
	}
	// CreationTime is a FILETIME structure.
	// Convert FILETIME to Unix time.
	return time.Unix(0, d.CreationTime.Nanoseconds()), true
}
