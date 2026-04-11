package dedupe

import (
	"image"
	"image/color"
	"image/jpeg"
	"os"
	"path/filepath"
	"testing"

	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/stretchr/testify/require"
)

func writePatternJPEG(t *testing.T, path string, width int, height int, quality int) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			rx := float64(x) / float64(width)
			ry := float64(y) / float64(height)
			img.Set(x, y, color.RGBA{
				R: uint8(255 * rx),
				G: uint8(255 * ry),
				B: uint8(255 * (1 - rx*ry)),
				A: 255,
			})
		}
	}

	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	require.NoError(t, jpeg.Encode(file, img, &jpeg.Options{Quality: quality}))
}

func TestEvaluateThumbnailMatchConfirmsRepoThumbnail(t *testing.T) {
	masterPath := filepath.Clean(filepath.Join("..", "..", "test_data", "source_mock", "img_2023_05_01.jpg"))
	thumbPath := filepath.Clean(filepath.Join("..", "..", "test_data", "source_mock_thumbs", "thumb_2023_05_01.jpg"))

	masterMeta := metadata.ExtractImageMetaJson(masterPath)
	thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
	thumbStat, err := os.Stat(thumbPath)
	require.NoError(t, err)
	masterStat, err := os.Stat(masterPath)
	require.NoError(t, err)

	decision, err := EvaluateThumbnailMatch(thumbPath, thumbMeta, thumbStat.Size(), masterPath, masterMeta, masterStat.Size())
	require.NoError(t, err)
	require.True(t, decision.Confirmed)
	require.False(t, decision.PreferCandidate)
}

func TestEvaluateThumbnailMatchRejectsAspectRatioMismatch(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dedupe_thumbnail_ratio_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	squarePath := filepath.Join(tempDir, "square.jpg")
	widePath := filepath.Join(tempDir, "wide.jpg")
	writePatternJPEG(t, squarePath, 128, 128, 90)
	writePatternJPEG(t, widePath, 128, 64, 90)

	squareMeta := metadata.ExtractImageMetaJson(squarePath)
	wideMeta := metadata.ExtractImageMetaJson(widePath)
	squareStat, err := os.Stat(squarePath)
	require.NoError(t, err)
	wideStat, err := os.Stat(widePath)
	require.NoError(t, err)

	decision, err := EvaluateThumbnailMatch(widePath, wideMeta, wideStat.Size(), squarePath, squareMeta, squareStat.Size())
	require.NoError(t, err)
	require.False(t, decision.Confirmed)
}

func TestComparePreferencePrefersHigherResolutionOverFileSize(t *testing.T) {
	decision := comparePreference(
		metadata.MediaMeta{Width: 128, Height: 128},
		900,
		metadata.MediaMeta{Width: 64, Height: 64},
		1500,
	)
	require.Equal(t, 1, decision)
}
