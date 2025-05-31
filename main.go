package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/rwcarlsen/goexif/exif"
)

const (
	statsFileName = "import_stats.txt"
	workerCount   = 8
	bufferSize    = 100
)

// Simplified to just track file paths, metadata will be read in workers
type fileInfo string

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: photo_organize <source_dirs...> <repo_path>")
	}
	sourceDirs := os.Args[1 : len(os.Args)-1]
	repoPath := os.Args[len(os.Args)-1]

	processed := loadProcessedFiles(statsFileName)

	files := collectFiles(sourceDirs, processed)
	if len(files) == 0 {
		log.Println("No files to process")
		return
	}

	statsChan := make(chan string, bufferSize)
	var wg sync.WaitGroup

	// Start stats writer
	wg.Add(1)
	go statsWriter(statsChan, &wg, statsFileName)

	// Setup worker pool
	fileChan := make(chan fileInfo, workerCount*2)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(fileChan, repoPath, statsChan, &wg)
	}

	// Send files to workers
	for _, f := range files {
		fileChan <- f
	}
	close(fileChan)

	// Wait for all workers and stats writer
	wg.Wait()
}

func collectFiles(sourceDirs []string, processed map[string]bool) []fileInfo {
	var files []fileInfo

	for _, dir := range sourceDirs {
		_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			if processed[path] {
				return nil
			}

			// Just collect file paths - metadata will be read in workers
			files = append(files, fileInfo(path))
			return nil
		})
	}

	return files
}

func getCreateTime(path string, info os.FileInfo) time.Time {
	// Try EXIF metadata first
	f, err := os.Open(path)
	if err == nil {
		ex, err := exif.Decode(f)
		_ = f.Close() // Close early to release file handle
		if err == nil {
			date, err := ex.DateTime()
			if err == nil {
				return date
			}
		}
	}

	// Fallback to OS-specific file creation time
	stat := info.Sys().(*syscall.Stat_t)

	var createTime time.Time
	switch runtime.GOOS {
	case "darwin": // macOS
		// 正确将 stat.TimeSpec 转换为 time.Time
		createTime = time.Unix(stat.Birthtimespec.Sec, stat.Birthtimespec.Nsec)
	case "linux": // Linux
		// 注意：Linux 系统没有标准的创建时间，这里的 ctime 是文件状态更改时间
		createTime = time.Unix(stat.Ctimespec.Sec, stat.Ctimespec.Nsec)
	default:
		createTime = info.ModTime() // Fallback to modification time
	}
	return createTime
}

func worker(files <-chan fileInfo, repoPath string, statsChan chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	for fPath := range files {
		// Get file info from OS
		info, err := os.Stat(string(fPath))
		if err != nil {
			log.Printf("Error getting stats for %s: %v", fPath, err)
			continue
		}

		// Get create time (now happens in worker)
		createTime := getCreateTime(string(fPath), info)

		// Process file
		destPath := getDestPath(createTime, repoPath, string(fPath))
		if err := copyFile(string(fPath), destPath); err != nil {
			log.Printf("Error copying %s: %v", fPath, err)
			continue
		}
		statsChan <- string(fPath)
	}
}

func getDestPath(t time.Time, repo, src string) string {
	year, month, day := t.Date()
	base := filepath.Base(src)
	dir := filepath.Join(repo, fmt.Sprintf("%d", year), fmt.Sprintf("%02d", int(month)), fmt.Sprintf("%02d", day))
	_ = os.MkdirAll(dir, 0755)
	return filepath.Join(dir, base)
}

func copyFile(src, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Sync()
}

func loadProcessedFiles(path string) map[string]bool {
	processed := make(map[string]bool)
	f, err := os.Open(path)
	if err != nil {
		return processed
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		processed[scanner.Text()] = true
	}
	return processed
}

func statsWriter(ch <-chan string, wg *sync.WaitGroup, path string) {
	defer wg.Done()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Error opening stats file: %v", err)
	}
	defer f.Close()

	buffer := make([]string, 0, bufferSize)
	flushTicker := time.NewTicker(5 * time.Second)

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt)

	for {
		select {
		case file, ok := <-ch:
			if !ok {
				flushBuffer(f, buffer)
				return
			}
			buffer = append(buffer, file)
			if len(buffer) >= bufferSize {
				flushBuffer(f, buffer)
				buffer = buffer[:0]
			}
		case <-flushTicker.C:
			if len(buffer) > 0 {
				flushBuffer(f, buffer)
				buffer = buffer[:0]
			}
		case <-exit:
			flushBuffer(f, buffer)
			log.Println("Gracefully shutting down...")
			return
		}
	}
}

func flushBuffer(f *os.File, buffer []string) {
	for _, line := range buffer {
		if _, err := f.WriteString(line + "\n"); err != nil {
			log.Printf("Error writing to stats file: %v", err)
		}
	}
	_ = f.Sync()
}
