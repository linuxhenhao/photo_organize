package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
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

func getCreateTime(path string) (time.Time, error) {
	cmd := exec.Command("exiftool", "-d", "%Y-%m-%dT%H:%M:%S%z", "-CreateDate", "-T", path)
	output, err := cmd.Output()
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, string(bytes.TrimSpace(output)))
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
