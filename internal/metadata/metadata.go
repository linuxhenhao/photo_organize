package metadata

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	projectexiftool "github.com/linuxhenhao/photo_organize/internal/exiftool"
)

// validateDate checks if the given year, month, and day form a valid date
func validateDate(year, month, day int, path string) (bool, error) {
	currentYear := time.Now().Year()
	if year < 1900 || year > currentYear+5 { // Allow a bit into the future for camera clock issues
		return false, fmt.Errorf("unlikely year %d for file [%s]. Range: 1900-%d", year, path, currentYear+5)
	}
	if month < 1 || month > 12 {
		return false, fmt.Errorf("invalid month %d", month)
	}
	if day < 1 || day > 31 {
		return false, fmt.Errorf("invalid day %d", day)
	}
	return true, nil
}

// ExtractTimeFromFilename attempts to regex a standard date string out of a path
func ExtractTimeFromFilename(path string) (time.Time, error) {
	base := filepath.Base(path)
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(\d{4})-(\d{2})-(\d{2})`), // YYYY-MM-DD
		regexp.MustCompile(`(\d{4})_(\d{2})_(\d{2})`), // YYYY_MM_DD
		regexp.MustCompile(`(\d{4})(\d{2})(\d{2})`),   // YYYYMMDD
	}

	for _, re := range patterns {
		matches := re.FindStringSubmatch(base)
		if len(matches) >= 4 {
			year, _ := strconv.Atoi(matches[1])
			month, _ := strconv.Atoi(matches[2])
			day, _ := strconv.Atoi(matches[3])

			if valid, err := validateDate(year, month, day, path); valid {
				return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local), nil
			} else {
				log.Printf("Date validation failed in filename: %v", err)
				continue
			}
		}
	}

	dirPath := filepath.Dir(path)
	dirComponents := strings.Split(dirPath, string(filepath.Separator))

	if len(dirComponents) >= 3 {
		start := len(dirComponents) - 3
		year, yearErr := strconv.Atoi(dirComponents[start])
		month, monthErr := strconv.Atoi(dirComponents[start+1])
		day, dayErr := strconv.Atoi(dirComponents[start+2])

		if yearErr == nil && monthErr == nil && dayErr == nil {
			if valid, err := validateDate(year, month, day, path); valid {
				return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local), nil
			} else {
				log.Printf("Date validation failed in directory path: %v", err)
			}
		}
	}
	return time.Time{}, errors.New("no valid date format found in filename or directory path")
}

// GetMetadata tries to get file creation time and MIME type using a single exiftool pass.
// Priority for time: exiftool, filename, stat birth time, stat mod time.
func GetMetadata(path string, fi os.FileInfo) (time.Time, string, error) {
	var mimeType string

	pool, err := projectexiftool.SharedPool()
	if err == nil {
		results, queryErr := pool.Extract([]string{path}, []string{
			"MIMEType",
			"CreateDate",
			"DateTimeOriginal",
			"MediaCreateDate",
			"TrackCreateDate",
			"SubSecCreateDate",
			"SubSecDateTimeOriginal",
		}, projectexiftool.QueryOptions{
			Fast:              true,
			IgnoreMinorErrors: true,
			DateFormat:        "%Y-%m-%dT%H:%M:%S",
		})
		if queryErr == nil && len(results) == 1 {
			if value, ok := results[0].GetString("MIMEType"); ok && value != "-" {
				mimeType = strings.TrimSpace(value)
			}

			for _, key := range []string{
				"CreateDate",
				"DateTimeOriginal",
				"MediaCreateDate",
				"TrackCreateDate",
				"SubSecCreateDate",
				"SubSecDateTimeOriginal",
			} {
				value, ok := results[0].GetString(key)
				if !ok || value == "" || value == "-" {
					continue
				}
				if t, errParseLocal := time.ParseInLocation("2006-01-02T15:04:05", value, time.Local); errParseLocal == nil {
					return t, mimeType, nil
				}
			}
		} else if queryErr != nil {
			log.Printf("exiftool pool query failed for [%s]: %v. Trying other methods.", path, queryErr)
		}
	} else {
		log.Printf("exiftool pool unavailable for [%s]: %v. Trying other methods.", path, err)
	}

	if t, err := ExtractTimeFromFilename(path); err == nil {
		return t, mimeType, nil
	}

	if t, ok := GetStatBirthTime(fi); ok {
		return t, mimeType, nil
	}

	return time.Time{}, mimeType, errors.New("no valid date format found in filename or directory OS path")
}
