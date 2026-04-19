package hasher

import (
	"image"
	"image/color"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/image/tiff"
)

func TestCalculateHash(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_hash_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "test.txt")
	content := []byte("test content")
	err = os.WriteFile(filePath, content, 0644)
	require.NoError(t, err)

	// Since we are using murmur3, "test content" will have a specific deterministic hash.
	// We just want to check it runs without error and returns non-empty.
	hash1, err := CalculateHash(filePath)
	require.NoError(t, err)
	require.NotEmpty(t, hash1)

	// ensure consistency
	hash2, err := CalculateHash(filePath)
	require.NoError(t, err)
	require.Equal(t, hash1, hash2)
}

func TestCalculatePHashSupportsTIFF(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_phash_tiff_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "preview.tiff")
	img := image.NewRGBA(image.Rect(0, 0, 32, 32))
	for y := 0; y < 32; y++ {
		for x := 0; x < 32; x++ {
			img.Set(x, y, color.RGBA{
				R: uint8(x * 8),
				G: uint8(y * 8),
				B: uint8((x + y) * 4),
				A: 255,
			})
		}
	}

	file, err := os.Create(filePath)
	require.NoError(t, err)
	err = tiff.Encode(file, img, nil)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	hash, err := CalculatePHash(filePath)
	require.NoError(t, err)
	require.NotZero(t, hash)
}

func TestCalculatePHashSupportsARWFixture(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "test_data", "source", "DSC00903.ARW")
	if _, err := os.Stat(fixturePath); err != nil {
		t.Fatalf("ARW fixture missing: %v", err)
	}

	hash, err := CalculatePHash(fixturePath)
	require.NoError(t, err)
	require.NotZero(t, hash)
}
