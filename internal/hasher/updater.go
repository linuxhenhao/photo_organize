package hasher

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
)

const hashWorkers = 10

type hashJob struct {
	path      string
	calcMmh3  bool
	calcPhash bool
	mimeType  string
}

type hashResult struct {
	path  string
	mmh3  string
	dhash string
	err   error
}

// UpdateHashes calculates and updates hashes for files concurrently.
func UpdateHashes(db *sql.DB) error {
	rows, err := db.Query(`
        SELECT source_path, mmh3_hash, dhash, mime_type
        FROM photos
        WHERE mmh3_hash = '' OR dhash = '' OR dhash IS NULL;
    `)
	if err != nil {
		return fmt.Errorf("failed to query for files needing hash update: %w", err)
	}

	var jobsList []hashJob
	for rows.Next() {
		var path, mmh3, dhashStr, mimeType string
		if err := rows.Scan(&path, &mmh3, &dhashStr, &mimeType); err != nil {
			log.Printf("Failed to scan path for hash update: %v", err)
			continue
		}
		jobsList = append(jobsList, hashJob{
			path:      path,
			calcMmh3:  mmh3 == "",
			calcPhash: dhashStr == "",
			mimeType:  strings.ToLower(mimeType),
		})
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over files needing hash update: %w", err)
	}

	if len(jobsList) == 0 {
		return nil
	}

	log.Printf("Found %d files to process for hash calculation...\n", len(jobsList))

	jobs := make(chan hashJob, len(jobsList))
	results := make(chan hashResult, len(jobsList))
	var wg sync.WaitGroup

	for i := 0; i < hashWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				res := hashResult{path: job.path}
				if job.calcMmh3 {
					h, err := CalculateHash(job.path)
					if err != nil {
						res.err = err
					}
					res.mmh3 = h
				}
				if job.calcPhash && res.err == nil {
					if CanVisualHash(job.path, job.mimeType) {
						p, err := CalculateDHash(job.path)
						if err != nil {
							res.dhash = "UNSUPPORTED"
						} else {
							res.dhash = DHashToString(p)
						}
					} else {
						res.dhash = "NOT_IMAGE"
					}
				}
				results <- res
			}
		}()
	}

	for _, j := range jobsList {
		jobs <- j
	}
	close(jobs)

	// Single goroutine to write to the database
	go func() {
		wg.Wait()
		close(results)
	}()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	processedCount := 0
	for result := range results {
		if result.err != nil {
			log.Printf("Hash calculation failed for [%s]: %v. Skipping update.", result.path, result.err)
			continue
		}

		if result.mmh3 != "" && result.dhash != "" {
			_, err = tx.Exec(`UPDATE photos SET mmh3_hash = ?, dhash = ? WHERE source_path = ?`, result.mmh3, result.dhash, result.path)
		} else if result.mmh3 != "" {
			_, err = tx.Exec(`UPDATE photos SET mmh3_hash = ? WHERE source_path = ?`, result.mmh3, result.path)
		} else if result.dhash != "" {
			_, err = tx.Exec(`UPDATE photos SET dhash = ? WHERE source_path = ?`, result.dhash, result.path)
		}

		if err != nil {
			log.Printf("Failed to update db for [%s]: %v", result.path, err)
		}

		processedCount++
		if processedCount%200 == 0 {
			log.Printf("Updated hashes for %d files...\n", processedCount)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// AssignGroupIDs groups identical files by assigning the same group_id based on mmh3_hash and size
func AssignGroupIDs(db *sql.DB) error {
	rows, err := db.Query(`
        SELECT DISTINCT mmh3_hash
        FROM photos
        WHERE mmh3_hash != '';
    `)
	if err != nil {
		return fmt.Errorf("failed to select distinct hashes: %w", err)
	}
	defer rows.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	groupID := 1
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			log.Printf("Failed to scan hash: %v", err)
			continue
		}

		_, err := tx.Exec(`UPDATE photos SET group_id = ? WHERE mmh3_hash = ?`, groupID, hash)
		if err != nil {
			log.Printf("Failed to update group_id %d for hash %s: %v", groupID, hash, err)
			continue
		}
		groupID++
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error processing distinct hashes: %w", err)
	}

	return nil
}
