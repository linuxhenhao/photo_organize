package dedupe

import (
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"os"
	"path/filepath"
	"testing"

	"github.com/linuxhenhao/photo_organize/internal/hasher"
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

func TestCandidateSearchDistanceCoversRepoMockThumbnails(t *testing.T) {
	maxDistance := 0

	for i := 1; i <= 5; i++ {
		masterPath := filepath.Clean(filepath.Join("..", "..", "test_data", "source_mock", fmt.Sprintf("img_2023_05_%02d.jpg", i)))
		thumbPath := filepath.Clean(filepath.Join("..", "..", "test_data", "source_mock_thumbs", fmt.Sprintf("thumb_2023_05_%02d.jpg", i)))

		masterHash, err := hasher.CalculatePHash(masterPath)
		require.NoError(t, err)
		thumbHash, err := hasher.CalculatePHash(thumbPath)
		require.NoError(t, err)

		distance := hasher.HammingDistance(masterHash, thumbHash)
		if distance > maxDistance {
			maxDistance = distance
		}
		require.LessOrEqual(t, distance, CandidateSearchDistance, "pair %d should be reachable in stage-1 recall", i)
	}

	require.Greater(t, maxDistance, 12, "repo fixtures should guard against regressing to the old 12-bit cutoff")
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
	decision := ComparePreference(
		"candidate.jpg",
		metadata.MediaMeta{Width: 128, Height: 128},
		900,
		"existing.jpg",
		metadata.MediaMeta{Width: 64, Height: 64},
		1500,
	)
	require.Equal(t, 1, decision)
}

func TestComparePreferencePrefersRAWWhenResolutionMatches(t *testing.T) {
	decision := ComparePreference(
		"candidate.CR2",
		metadata.MediaMeta{Width: 6000, Height: 4000},
		2_000_000,
		"existing.jpg",
		metadata.MediaMeta{Width: 6000, Height: 4000},
		8_000_000,
	)
	require.Equal(t, 1, decision)
}

func TestComparePreferencePrefersRAWWhenResolutionIsWithinTolerance(t *testing.T) {
	decision := ComparePreference(
		"candidate.CR2",
		metadata.MediaMeta{Width: 5184, Height: 3456},
		21_531_849,
		"existing.jpg",
		metadata.MediaMeta{Width: 5208, Height: 3476},
		1_807_441,
	)
	require.Equal(t, 1, decision)
}

func TestComparePreferenceStillPrefersHigherResolutionOverRAWOutsideTolerance(t *testing.T) {
	decision := ComparePreference(
		"candidate.CR2",
		metadata.MediaMeta{Width: 4000, Height: 3000},
		20_000_000,
		"existing.jpg",
		metadata.MediaMeta{Width: 6000, Height: 4000},
		8_000_000,
	)
	require.Equal(t, -1, decision)
}
