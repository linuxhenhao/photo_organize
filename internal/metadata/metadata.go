package metadata

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
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

	cmd := exec.Command("exiftool", "-m", "-d", "%Y-%m-%dT%H:%M:%S",
		"-CreateDate", "-DateTimeOriginal", "-MediaCreateDate", "-TrackCreateDate",
		"-SubSecCreateDate", "-SubSecDateTimeOriginal", "-MIMEType",
		"-T", "-fast", path)
	output, err := cmd.Output()

	if err == nil {
		fields := strings.Split(string(bytes.TrimSpace(output)), "\t")
		
		if len(fields) > 0 {
			mimeType = strings.TrimSpace(fields[len(fields)-1])
			if mimeType == "-" {
				mimeType = ""
			}
		}

		for i, field := range fields {
			if i == len(fields)-1 {
				continue
			}
			if field == "-" || field == "" {
				continue
			}
			layouts := []string{
				"2006-01-02T15:04:05",
			}
			for _, layout := range layouts {
				if t, errParseLocal := time.ParseInLocation(layout, field, time.Local); errParseLocal == nil {
					return t, mimeType, nil
				}
			}
		}
	} else {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("exiftool command failed for [%s] with exit code %d. Stderr: %s. Trying other methods.", path, exitErr.ExitCode(), string(exitErr.Stderr))
		} else if errors.Is(err, exec.ErrNotFound) {
			log.Printf("exiftool command not found. Ensure it's installed and in PATH. Skipping exiftool for [%s].", path)
		} else {
			log.Printf("exiftool execution failed for [%s]: %v. Trying other methods.", path, err)
		}
	}

	if t, err := ExtractTimeFromFilename(path); err == nil {
		return t, mimeType, nil
	}

	if t, ok := GetStatBirthTime(fi); ok {
		return t, mimeType, nil
	}

	return time.Time{}, mimeType, errors.New("no valid date format found in filename or directory OS path")
}
