package hasher

import (
	"bytes"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png" // Basic support for now, can extend to others if needed
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/corona10/goimagehash"
	projectexiftool "github.com/linuxhenhao/photo_organize/internal/exiftool"
	"github.com/twmb/murmur3"
	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"
)

func mimeTypeWithExiftool(path string) (string, error) {
	pool, err := projectexiftool.SharedPool()
	if err != nil {
		return "", err
	}

	results, err := pool.Extract([]string{path}, []string{"MIMEType"}, projectexiftool.QueryOptions{
		Fast:              true,
		IgnoreMinorErrors: true,
	})
	if err != nil {
		return "", err
	}
	if len(results) != 1 {
		return "", fmt.Errorf("unexpected exiftool result count for %s: %d", path, len(results))
	}

	mimeType, ok := results[0].GetString("MIMEType")
	if !ok {
		return "", fmt.Errorf("MIMEType missing for %s", path)
	}
	return strings.ToLower(strings.TrimSpace(mimeType)), nil
}

// CanVisualHash reports whether a file should participate in image-only visual hashing.
func CanVisualHash(path string, mimeType string) bool {
	if mimeType != "" {
		return strings.HasPrefix(strings.ToLower(strings.TrimSpace(mimeType)), "image/")
	}

	if detectedMime, err := mimeTypeWithExiftool(path); err == nil && detectedMime != "" {
		return strings.HasPrefix(detectedMime, "image/")
	}

	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()

	_, _, err = image.DecodeConfig(file)
	return err == nil
}

// IsImageForPHash checks if the file should participate in visual hashing.
func IsImageForPHash(path string) bool {
	return CanVisualHash(path, "")
}

// extractThumbnail attempts to extract the embedded thumbnail using exiftool.
func extractThumbnail(path string) ([]byte, error) {
	pool, err := projectexiftool.SharedPool()
	if err != nil {
		return nil, err
	}

	results, err := pool.Extract([]string{path}, []string{
		"PreviewImage",
		"JpgFromRaw",
		"ThumbnailImage",
	}, projectexiftool.QueryOptions{
		Binary:            true,
		IgnoreMinorErrors: true,
	})
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("unexpected exiftool result count for %s: %d", path, len(results))
	}

	for _, key := range []string{"PreviewImage", "JpgFromRaw", "ThumbnailImage"} {
		data, ok, err := results[0].GetBytes(key)
		if err != nil {
			return nil, err
		}
		if ok && len(data) > 0 {
			return data, nil
		}
	}

	return nil, fmt.Errorf("no embedded preview found for %s", path)
}

func decodeImageForHash(path string) (image.Image, error) {
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
			return nil, fmt.Errorf("failed to open file for phash [%s]: %w", path, openErr)
		}
		defer file.Close()
		img, _, err = image.Decode(file)
		if err != nil {
			return nil, fmt.Errorf("failed to decode image for phash [%s]: %w", path, err)
		}
	}

	return img, nil
}

// CalculatePHash computes the fast dHash stored in cache.db's `phash` column
// for BK-tree candidate lookup.
func CalculatePHash(path string) (uint64, error) {
	img, err := decodeImageForHash(path)
	if err != nil {
		return 0, err
	}

	hash, err := goimagehash.DifferenceHash(img)
	if err != nil {
		return 0, err
	}
	return hash.GetHash(), nil
}

// CalculateFullPerceptionHash computes the stronger full perception hash.
// Callers should use the full name to avoid confusion with the cached `phash`
// column, which currently stores dHash values.
func CalculateFullPerceptionHash(path string) (uint64, error) {
	img, err := decodeImageForHash(path)
	if err != nil {
		return 0, err
	}

	hash, err := goimagehash.PerceptionHash(img)
	if err != nil {
		return 0, err
	}
	return hash.GetHash(), nil
}

// CalculatePerceptionHash is kept as a compatibility alias for callers that
// still use the old name.
func CalculatePerceptionHash(path string) (uint64, error) {
	return CalculateFullPerceptionHash(path)
}

// CalculateColorSignature samples a coarse RGB grid for color-aware duplicate confirmation.
func CalculateColorSignature(path string) ([]uint8, error) {
	img, err := decodeImageForHash(path)
	if err != nil {
		return nil, err
	}

	bounds := img.Bounds()
	if bounds.Dx() == 0 || bounds.Dy() == 0 {
		return nil, fmt.Errorf("invalid image bounds for %s", path)
	}

	const gridSize = 4
	signature := make([]uint8, 0, gridSize*gridSize*3)
	for gy := 0; gy < gridSize; gy++ {
		y0 := bounds.Min.Y + gy*bounds.Dy()/gridSize
		y1 := bounds.Min.Y + (gy+1)*bounds.Dy()/gridSize
		if y1 <= y0 {
			y1 = y0 + 1
		}
		for gx := 0; gx < gridSize; gx++ {
			x0 := bounds.Min.X + gx*bounds.Dx()/gridSize
			x1 := bounds.Min.X + (gx+1)*bounds.Dx()/gridSize
			if x1 <= x0 {
				x1 = x0 + 1
			}

			var rSum, gSum, bSum, count uint64
			for y := y0; y < y1; y++ {
				for x := x0; x < x1; x++ {
					r, g, b, _ := img.At(x, y).RGBA()
					rSum += uint64(r >> 8)
					gSum += uint64(g >> 8)
					bSum += uint64(b >> 8)
					count++
				}
			}

			signature = append(signature,
				uint8(rSum/count),
				uint8(gSum/count),
				uint8(bSum/count),
			)
		}
	}

	return signature, nil
}

// ColorSignatureDistance reports the mean absolute channel difference between two signatures.
func ColorSignatureDistance(a []uint8, b []uint8) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return math.Inf(1)
	}

	var sum float64
	for i := range a {
		sum += math.Abs(float64(a[i]) - float64(b[i]))
	}
	return sum / float64(len(a))
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
