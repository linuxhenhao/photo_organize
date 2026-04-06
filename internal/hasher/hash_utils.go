package hasher

import (
	"bytes"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png" // Basic support for now, can extend to others if needed
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/corona10/goimagehash"
	"github.com/twmb/murmur3"
)

// IsImageForPHash checks if the file is an image using exiftool's MIME type detection.
func IsImageForPHash(path string) bool {
	cmd := exec.Command("exiftool", "-MIMEType", "-s3", "-fast", path)
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	mime := strings.ToLower(strings.TrimSpace(string(out)))
	return strings.HasPrefix(mime, "image/")
}

// extractThumbnail attempts to extract the embedded thumbnail using exiftool.
func extractThumbnail(path string) ([]byte, error) {
	cmd := exec.Command("exiftool", "-b", "-ThumbnailImage", "-q", path)
	return cmd.Output()
}

// calculatePHash computes the dHash for an image, preferring its fast-to-extract thumbnail.
func CalculatePHash(path string) (uint64, error) {
	var img image.Image
	var err error

	// Try extracting thumbnail first
	thumbBytes, thumbErr := extractThumbnail(path)
	if thumbErr == nil && len(thumbBytes) > 0 {
		img, _, err = image.Decode(bytes.NewReader(thumbBytes))
	}

	// If thumbnail fails or doesn't exist, fallback to original file
	if img == nil {
		file, openErr := os.Open(path)
		if openErr != nil {
			return 0, fmt.Errorf("failed to open file for phash [%s]: %w", path, openErr)
		}
		defer file.Close()
		img, _, err = image.Decode(file)
		if err != nil {
			return 0, fmt.Errorf("failed to decode image for phash [%s]: %w", path, err)
		}
	}

	hash, err := goimagehash.DifferenceHash(img)
	if err != nil {
		return 0, err
	}
	return hash.GetHash(), nil
}

// PHashToString formats a uint64 phash as a 16-character hex string.
func PHashToString(hash uint64) string {
	return fmt.Sprintf("%016x", hash)
}

// StringToPHash parses a 16-character hex string back to a uint64 phash.
func StringToPHash(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseUint(s, 16, 64)
}

// CalculateHash generates the murmur3 mmh3_hash for a file
func CalculateHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file [%s] for hashing: %w", path, err)
	}
	defer file.Close()

	hasher := murmur3.New64()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("failed to copy file content to hasher for [%s]: %w", path, err)
	}
	return fmt.Sprintf("%x", hasher.Sum64()), nil
}
