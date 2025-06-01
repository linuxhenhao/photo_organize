package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
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

type Source int

const (
	SourceExif    = iota
	SourceCTime   // ctime
	SourceModTime // mod time
	SourceFname   // filename
)

var (
	ErrNotOverwrite = errors.New("target exist and not overwrite")
	ErrBadStr       = errors.New("bad str")
)

type Stat struct {
	SourcePath string
	TargetPath string
	Source     Source
}

func (s Stat) String() string {
	return fmt.Sprintf("<%s><%s><%d>", s.SourcePath, s.TargetPath, s.Source)
}

func (s *Stat) LoadString(str string) error {
	l := len(str)

	parse := func(start int, end int, str string) (string, int, error) {
		if start >= end {
			return "", 0, io.EOF
		}
		if str[start] != '<' {
			return "", 0, ErrBadStr
		}
		for i := start + 1; i < end; i++ {
			if str[i] == '>' {
				return str[start+1 : i], i + 1, nil
			}
		}
		return "", 0, ErrBadStr
	}
	start, end := 0, l
	seg, next, err := parse(start, end, str)
	if err != nil {
		return err
	}
	s.SourcePath = seg
	seg, next, err = parse(next, end, str)
	if err != nil {
		return err
	}
	s.TargetPath = seg
	seg, _, err = parse(next, end, str)
	if err != nil {
		return err
	}
	got, err := strconv.ParseInt(seg, 10, 32)
	if err != nil {
		return err
	}
	s.Source = Source(got)
	return nil
}

// extractDateFromFilename uses regex to find and parse date patterns in filenames
func extractDateFromFilename(filename string) (time.Time, bool) {
	// Common date regex patterns: yyyy-mm-dd, yyyy/mm/dd, yyyymmdd, yyyy_mm_dd, yyyy
	patterns := []string{
		`(\d{4})-(\d{2})-(\d{2})`, // yyyy-mm-dd
		`(\d{4})/(\d{2})/(\d{2})`, // yyyy/mm/dd
		`(\d{4})(\d{2})(\d{2})`,   // yyyymmdd
		`(\d{4})_(\d{2})_(\d{2})`, // yyyy_mm_dd
	}

	minDate := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	currentDate := time.Now().UTC().Truncate(24 * time.Hour) // Truncate to midnight
	currentYear := currentDate.Year()
	minYear := minDate.Year()

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindStringSubmatch(filename)
		if len(matches) < 2 {
			continue
		}

		var year, month, day int
		switch len(matches) {
		case 2: // yyyy
			if _, err := strconv.Atoi(matches[1]); err != nil {
				continue
			}
			year, _ = strconv.Atoi(matches[1])
			month = 1
			day = 1
		case 4: // yyyy-mm-dd, yyyy/mm/dd, yyyymmdd, yyyy_mm_dd
			yearStr, monthStr, dayStr := matches[1], matches[2], matches[3]
			year, _ = strconv.Atoi(yearStr)
			month, _ = strconv.Atoi(monthStr)
			day, _ = strconv.Atoi(dayStr)
		default:
			continue
		}

		// Validate individual date components
		if year < minYear || year > currentYear {
			continue
		}
		if month < 1 || month > 12 {
			continue
		}
		if day < 1 || day > 31 {
			continue
		}

		// Create candidate date
		parsedTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)

		// Validate date range (2000-01-01 to today)
		if parsedTime.Before(minDate) || parsedTime.After(currentDate) {
			continue
		}

		return parsedTime, true
	}

	return time.Time{}, false
}

// Simplified to just track file paths, metadata will be read in workers
type fileInfo string

func main() {
	var overwrite bool
	flag.BoolVar(&overwrite, "overwrite", false, "Overwrite existing files in the repository")
	flag.Parse()

	if len(flag.Args()) < 2 {
		log.Fatal("Usage: photo_organize [-overwrite] <source_dirs...> <repo_path>")
	}
	sourceDirs := flag.Args()[:len(flag.Args())-1]
	repoPath, err := filepath.Abs(flag.Args()[len(flag.Args())-1])
	if err != nil {
		panic(fmt.Sprintf("Error getting absolute path for %s: %v", repoPath, err))
	}

	processed := loadProcessedFiles(statsFileName)

	files := collectFiles(sourceDirs)
	if len(files) == 0 {
		log.Println("No files to process")
		return
	}

	statsChan := make(chan Stat, bufferSize)
	var statswg sync.WaitGroup
	var wg sync.WaitGroup

	// Start stats writer
	statswg.Add(1)
	go statsWriter(statsChan, &statswg, statsFileName)

	// Setup worker pool
	fileChan := make(chan fileInfo, workerCount*2)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(fileChan, repoPath, statsChan, processed, overwrite, &wg)
	}

	// Send files to workers
	for _, f := range files {
		fileChan <- f
	}
	close(fileChan)

	// Wait for all workers
	wg.Wait()
	close(statsChan)
	// wait for stats writer
	statswg.Wait()
}

func collectFiles(sourceDirs []string) []fileInfo {
	var files []fileInfo

	for _, dir := range sourceDirs {
		_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			absPath, _ := filepath.Abs(path)
			// Just collect file paths - metadata will be read in workers
			files = append(files, fileInfo(absPath))
			return nil
		})
	}

	return files
}

func getCreateTime(path string, info os.FileInfo) (time.Time, Source) {
	// Try EXIF metadata first
	f, err := os.Open(path)
	if err == nil {
		ex, err := exif.Decode(f)
		_ = f.Close() // Close early to release file handle
		if err == nil {
			date, err := ex.DateTime()
			if err == nil {
				return date, SourceExif
			}
		}
	}
	// Try regex-based filename date extraction
	parsedTime, ok := extractDateFromFilename(filepath.Base(path))
	if ok {
		return parsedTime, SourceFname
	}
	// 尝试转换为 syscall.Stat_t 获取更底层信息
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		// Linux 系统中 ctime 是 inode 更改时间，并非真正的创建时间
		// 但在没有创建时间的情况下，这是最接近的替代值
		return time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec)), SourceCTime
	}
	return info.ModTime(), SourceModTime
}

func worker(files <-chan fileInfo, repoPath string, statsChan chan<- Stat, processed map[string]bool, overwrite bool, wg *sync.WaitGroup) {
	defer wg.Done()
	for fPath := range files {
		// Get file info from OS
		if processed[string(fPath)] {
			continue
		}
		info, err := os.Stat(string(fPath))
		if err != nil {
			log.Printf("Error getting stats for %s: %v", fPath, err)
			continue
		}

		// Get create time (now happens in worker)
		createTime, source := getCreateTime(string(fPath), info)

		// Process file
		destPath := getDestPath(createTime, repoPath, string(fPath))
		if err := copyFile(string(fPath), destPath, overwrite); err != nil {
			log.Printf("Error copying %s: %v", fPath, err)
			if !errors.Is(err, ErrNotOverwrite) {
				continue
			}
			// not overwrite should send stat
		}
		statsChan <- Stat{
			SourcePath: string(fPath),
			TargetPath: destPath,
			Source:     source,
		}
	}
}

func getDestPath(t time.Time, repo, src string) string {
	year, month, day := t.Date()
	base := filepath.Base(src)
	dir := filepath.Join(repo, fmt.Sprintf("%d", year), fmt.Sprintf("%02d", int(month)), fmt.Sprintf("%02d", day))
	_ = os.MkdirAll(dir, 0755)
	return filepath.Join(dir, base)
}

func copyFile(src, dest string, overwrite bool) error {
	// Check if destination exists
	if _, err := os.Stat(dest); err == nil {
		if !overwrite {
			return ErrNotOverwrite
		}
	} else if !os.IsNotExist(err) {
		// Other error checking
		return fmt.Errorf("error checking destination file %s: %v", dest, err)
	}

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
		stat := &Stat{}
		err := stat.LoadString(scanner.Text())
		if err != nil {
			log.Printf("load stats err: %v\n", err)
			continue
		}
		processed[stat.SourcePath] = true
	}
	return processed
}

func statsWriter(ch <-chan Stat, wg *sync.WaitGroup, path string) {
	defer wg.Done()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Error opening stats file: %v", err)
	}
	defer f.Close()

	buffer := make([]Stat, 0, bufferSize)
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

func flushBuffer(f *os.File, buffer []Stat) {
	for _, s := range buffer {
		if _, err := f.WriteString(s.String()); err != nil {
			log.Printf("Error writing to stats file: %v", err)
		}
	}
	_ = f.Sync()
}
