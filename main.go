package main

import (
	"bytes"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/murmur3"
	_ "modernc.org/sqlite"
)

const (
	workers   = 10
	batchSize = 200
)

var (
	dbPath     string
	sourceDirs []string
	destDir    string
)

func main() {
	scanCmd := flag.NewFlagSet("scan", flag.ExitOnError)
	scanCmd.StringVar(&dbPath, "db", "photos.db", "sqlite database path")
	scanCmd.Parse(os.Args[2:])

	importCmd := flag.NewFlagSet("import", flag.ExitOnError)
	importCmd.StringVar(&dbPath, "db", "photos.db", "sqlite database path")
	importCmd.StringVar(&destDir, "dest", "", "destination directory")
	importCmd.Parse(os.Args[2:])

	switch os.Args[1] {
	case "scan":
		handleScan()
	case "import":
		handleImport()
	default:
		log.Fatal("invalid command")
	}
}

func handleScan() {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`PRAGMA journal_mode = WAL;`)
	if err != nil {
		log.Fatal(err)
	}

	// 创建工作池
	files := make(chan string, 100)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range files {
				processFile(db, file)
			}
		}()
	}

	// 遍历目录
	for _, dir := range sourceDirs {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				files <- path
			}
			return nil
		})
	}

	close(files)
	wg.Wait()

	// 在文件遍历完成后添加
	updateHashes(db)
	assignGroupIDs(db)
}

func processFile(db *sql.DB, path string) {
	stat, err := os.Stat(path)
	if err != nil {
		log.Printf("获取文件状态失败: %v", err)
		return
	}

	// 获取创建时间
	createTime, err := getCreateTime(path)
	if err != nil {
		log.Printf("获取创建时间失败: %v", err)
		createTime = stat.ModTime()
	}

	_, err = db.Exec(`INSERT OR IGNORE INTO photos(source_path, size, create_time) VALUES(?, ?, ?)`,
		path, stat.Size(), createTime.Format(time.RFC3339))
	if err != nil {
		log.Printf("数据库写入失败: %v", err)
	}
}

// 新增文件名时间提取函数
func extractTimeFromFilename(path string) (time.Time, error) {
	base := filepath.Base(path)

	// 拆分后的正则模式
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(\d{4})[-](\d{2})[-](\d{2})`), // yyyy-mm-dd
		regexp.MustCompile(`(\d{4})[_](\d{2})[_](\d{2})`), // yyyy_mm_dd
		regexp.MustCompile(`(\d{4})(\d{2})(\d{2})`),       // yyyymmdd
	}

	// 按优先级匹配不同格式
	for _, re := range patterns {
		matches := re.FindAllStringSubmatch(base, -1)
		for _, m := range matches {
			if len(m) > 3 {
				// 修正：matches 是二维切片，这里应该使用 m 变量获取匹配结果
				year, _ := strconv.Atoi(m[1])
				month, _ := strconv.Atoi(m[2])
				day, _ := strconv.Atoi(m[3])

				currentYear := time.Now().Year()
				if year < 2009 || year > currentYear {
					log.Printf("无效年份 %d (范围:2009-%d)", year, currentYear)
					continue
				}
				if month >= 1 && month <= 12 && day >= 1 && day <= 31 {
					return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
				}
			}
		}
	}
	return time.Time{}, errors.New("未找到有效日期格式")
}

// 修改后的getCreateTime函数
func getCreateTime(path string) (time.Time, error) {
	cmd := exec.Command("exiftool", "-m", "-d", "%Y-%m-%dT%H:%M:%S%z",
		"-CreateDate", "-DateTimeOriginal", "-MediaCreateDate", "-TrackCreateDate",
		"-FileModifyDate", "-T", "-fast", path)
	output, err := cmd.Output()
	if err == nil {
		fields := strings.Split(string(bytes.TrimSpace(output)), "\t")
		for _, field := range fields[:5] {
			if t, err := time.Parse(time.RFC3339, field); err == nil {
				return t, nil
			}
		}
	}

	// 全部EXIF字段无效时使用文件修改时间
	// 尝试获取syscall.Stat_t结构体
	// 新增文件名解析
	if t, err := extractTimeFromFilename(path); err == nil {
		return t, nil
	}

	// 尝试获取syscall.Stat_t结构体
	// 修正变量名为stat
	stat, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	if stat, ok := stat.Sys().(*syscall.Stat_t); ok {
		return time.Unix(int64(stat.Ctim.Sec), stat.Ctim.NSec), nil
	}

	// 保留原有回退逻辑
	return stat.ModTime(), nil
}

func handleImport() {
	// TODO: 实现导入逻辑
}

func updateHashes(db *sql.DB) {
	tx, _ := db.Begin()
	defer tx.Rollback()

	rows, _ := tx.Query(`
		SELECT size, source_path
		FROM photos 
		WHERE mmh3_hash = '' 
		GROUP BY size 
		HAVING COUNT(*) > 1
	`)

	batch := 0
	stmt, _ := tx.Prepare(`UPDATE photos SET mmh3_hash = ? WHERE source_path = ?`)
	defer stmt.Close()

	for rows.Next() {
		var size int64
		var path string
		rows.Scan(&size, &path)

		hash, err := calculateHash(path)
		if err != nil {
			log.Printf("哈希计算失败[%s]: %v", path, err)
			continue
		}

		if _, err = stmt.Exec(hash, path); err != nil {
			log.Printf("更新失败[%s]: %v", path, err)
		}

		batch++
		if batch%200 == 0 {
			tx.Commit()
			tx, _ = db.Begin()
			stmt, _ = tx.Prepare(`UPDATE photos SET mmh3_hash = ? WHERE source_path = ?`)
		}
	}
	tx.Commit()
}

func calculateHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := murmur3.New64()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hasher.Sum64()), nil
}

// 添加分组逻辑
func assignGroupIDs(db *sql.DB) {
	tx, _ := db.Begin()
	defer tx.Rollback()

	_, err := tx.Exec(`UPDATE photos SET group_id = 0 WHERE mmh3_hash = ''`)
	if err != nil {
		log.Fatal(err)
	}

	rows, _ := tx.Query(`SELECT DISTINCT mmh3_hash FROM photos WHERE mmh3_hash != '' ORDER BY mmh3_hash`)
	groupID := 1
	for rows.Next() {
		var hash string
		rows.Scan(&hash)
		_, err = tx.Exec(`UPDATE photos SET group_id = ? WHERE mmh3_hash = ?`, groupID, hash)
		if err != nil {
			log.Fatal(err)
		}
		groupID++
	}
	tx.Commit()
}
