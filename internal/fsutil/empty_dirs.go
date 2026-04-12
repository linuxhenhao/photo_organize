package fsutil

import (
	"os"
	"path/filepath"
	"strings"
)

// RemoveEmptyParentDirs removes empty directories starting at startDir and walking
// upward until stopDir. stopDir itself is never removed.
func RemoveEmptyParentDirs(startDir, stopDir string) error {
	if startDir == "" || stopDir == "" {
		return nil
	}

	current, err := filepath.Abs(startDir)
	if err != nil {
		return err
	}
	root, err := filepath.Abs(stopDir)
	if err != nil {
		return err
	}

	for {
		rel, err := filepath.Rel(root, current)
		if err != nil {
			return err
		}
		if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return nil
		}

		entries, err := os.ReadDir(current)
		if os.IsNotExist(err) {
			parent := filepath.Dir(current)
			if parent == current {
				return nil
			}
			current = parent
			continue
		}
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return nil
		}
		if err := os.Remove(current); err != nil && !os.IsNotExist(err) {
			return err
		}

		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
		current = parent
	}
}
