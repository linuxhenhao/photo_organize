package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <image-path>\n", filepath.Base(os.Args[0]))
		os.Exit(2)
	}

	path := os.Args[1]

	names, err := listXattrs(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list xattr: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("path: %s\n", path)
	fmt.Printf("xattrs (%d):\n", len(names))
	for _, name := range names {
		value, err := getXattr(path, name)
		if err != nil {
			fmt.Printf("  %s = <error: %v>\n", name, err)
			continue
		}
		fmt.Printf("  %s = %q\n", name, value)
	}

	dir, err := getXattr(path, "user.thumb.dir")
	if err != nil {
		fmt.Fprintf(os.Stderr, "read user.thumb.dir: %v\n", err)
		os.Exit(1)
	}
	id, err := getXattr(path, "user.thumb.id")
	if err != nil {
		fmt.Fprintf(os.Stderr, "read user.thumb.id: %v\n", err)
		os.Exit(1)
	}

	stem := id
	if idx := strings.IndexByte(stem, '-'); idx >= 0 {
		stem = stem[:idx]
	}

	fmt.Println("derived thumbnail candidates:")
	for _, suffix := range []string{
		"_1600_40.webp",
		"_1600_40.jpg",
		"_640_40.webp",
		"_640_40.jpg",
		"_320_40.webp",
		"_320_40.jpg",
		"_mini.webp",
		"_mini.jpg",
	} {
		candidate := filepath.Join(dir, stem+suffix)
		if _, err := os.Stat(candidate); err == nil {
			fmt.Printf("  [exists] %s\n", candidate)
			continue
		} else if errors.Is(err, os.ErrNotExist) {
			fmt.Printf("  [missing] %s\n", candidate)
			continue
		} else {
			fmt.Printf("  [error=%v] %s\n", err, candidate)
		}
	}
}

func listXattrs(path string) ([]string, error) {
	size, err := unix.Listxattr(path, nil)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}

	buf := make([]byte, size)
	n, err := unix.Listxattr(path, buf)
	if err != nil {
		return nil, err
	}

	var names []string
	start := 0
	for i, b := range buf[:n] {
		if b != 0 {
			continue
		}
		if i > start {
			names = append(names, string(buf[start:i]))
		}
		start = i + 1
	}
	return names, nil
}

func getXattr(path, name string) (string, error) {
	size, err := unix.Getxattr(path, name, nil)
	if err != nil {
		return "", err
	}
	if size == 0 {
		return "", nil
	}

	buf := make([]byte, size)
	n, err := unix.Getxattr(path, name, buf)
	if err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}
