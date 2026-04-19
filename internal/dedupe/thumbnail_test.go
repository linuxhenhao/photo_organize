package dedupe

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	_ "image/gif"
	"image/jpeg"
	"os"
	"path/filepath"
	"testing"

	projectexiftool "github.com/linuxhenhao/photo_organize/internal/exiftool"
	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/stretchr/testify/require"
	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"
)

func writePatternJPEG(t *testing.T, path string, width int, height int, quality int) {
	writePatternJPEGVariant(t, path, width, height, quality, 0)
}

func writePatternJPEGVariant(t *testing.T, path string, width int, height int, quality int, variant int) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			rx := float64(x) / float64(max(width-1, 1))
			ry := float64(y) / float64(max(height-1, 1))
			baseR := uint8(255 * rx)
			baseG := uint8(255 * ry)
			baseB := uint8(255 * (1 - rx*ry))
			if ((x/9)+(y/9)+variant)%2 == 0 {
				baseR = 255 - baseR/2
				baseB /= 2
			}
			circleX := width / 3
			circleY := height / 2
			if variant%2 == 1 {
				circleX = (2 * width) / 3
			}
			if variant >= 2 {
				circleY = height / 3
			}
			if (x-circleX)*(x-circleX)+(y-circleY)*(y-circleY) < (min(width, height)/6)*(min(width, height)/6) {
				baseG = 255
			}
			img.Set(x, y, color.RGBA{R: baseR, G: baseG, B: baseB, A: 255})
		}
	}

	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	require.NoError(t, jpeg.Encode(file, img, &jpeg.Options{Quality: quality}))
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func writeRawPreviewJPEG(t *testing.T, rawPath string, outputPath string) {
	t.Helper()

	pool, err := projectexiftool.SharedPool()
	require.NoError(t, err)

	results, err := pool.Extract([]string{rawPath}, []string{
		"PreviewImage",
		"JpgFromRaw",
		"ThumbnailImage",
	}, projectexiftool.QueryOptions{
		Binary:            true,
		IgnoreMinorErrors: true,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)

	var preview []byte
	for _, key := range []string{"PreviewImage", "JpgFromRaw", "ThumbnailImage"} {
		data, ok, err := results[0].GetBytes(key)
		require.NoError(t, err)
		if ok && len(data) > 0 {
			preview = data
			break
		}
	}
	require.NotEmpty(t, preview, "expected embedded preview in %s", rawPath)

	img, _, err := image.Decode(bytes.NewReader(preview))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(outputPath), 0755))

	file, err := os.Create(outputPath)
	require.NoError(t, err)
	defer file.Close()

	require.NoError(t, jpeg.Encode(file, img, &jpeg.Options{Quality: 82}))
}

func TestClassifyDerivativeConfirmsRealRawThumbnail(t *testing.T) {
	parentPath := filepath.Clean(filepath.Join("..", "..", "test_data", "source1", "DSC01075.ARW"))
	childPath := filepath.Clean(filepath.Join("..", "..", "test_data", "source1", "DSC01075.thumb.jpg"))

	parentMeta := metadata.ExtractImageMetaJson(parentPath)
	childMeta := metadata.ExtractImageMetaJson(childPath)
	childStat, err := os.Stat(childPath)
	require.NoError(t, err)
	parentStat, err := os.Stat(parentPath)
	require.NoError(t, err)

	decision, err := ClassifyDerivative(childPath, childMeta, childStat.Size(), parentPath, parentMeta, parentStat.Size())
	require.NoError(t, err)
	require.True(t, decision.Confirmed)
	require.Equal(t, DerivativeVariant, decision.Kind)
}

func TestClassifyDerivativeConfirmsCR2EmbeddedPreviewDerivative(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dedupe_cr2_preview_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	parentPath := filepath.Clean(filepath.Join("..", "..", "test_data", "source", "IMG_5798.CR2"))
	childPath := filepath.Join(tempDir, "IMG_5798.preview.jpg")
	writeRawPreviewJPEG(t, parentPath, childPath)

	parentMeta := metadata.ExtractImageMetaJson(parentPath)
	childMeta := metadata.ExtractImageMetaJson(childPath)
	childStat, err := os.Stat(childPath)
	require.NoError(t, err)
	parentStat, err := os.Stat(parentPath)
	require.NoError(t, err)

	decision, err := ClassifyDerivative(childPath, childMeta, childStat.Size(), parentPath, parentMeta, parentStat.Size())
	require.NoError(t, err)
	require.True(t, decision.Confirmed)
	require.Equal(t, DerivativeVariant, decision.Kind)
}

func TestCandidateSearchDistanceCoversRepoMockThumbnails(t *testing.T) {
	maxDistance := 0

	for i := 1; i <= 5; i++ {
		parentPath := filepath.Clean(filepath.Join("..", "..", "test_data", "source_mock", fmt.Sprintf("img_2023_05_%02d.jpg", i)))
		childPath := filepath.Clean(filepath.Join("..", "..", "test_data", "source_mock_thumbs", fmt.Sprintf("thumb_2023_05_%02d.jpg", i)))

		parentHash, err := hasher.CalculatePHash(parentPath)
		require.NoError(t, err)
		childHash, err := hasher.CalculatePHash(childPath)
		require.NoError(t, err)

		distance := hasher.HammingDistance(parentHash, childHash)
		if distance > maxDistance {
			maxDistance = distance
		}
		require.LessOrEqual(t, distance, CandidateSearchDistance, "pair %d should be reachable in stage-1 recall", i)
	}

	require.Greater(t, maxDistance, 4, "fixtures should still exercise the tighter recall boundary")
}

func TestClassifyDerivativeRejectsAspectRatioMismatch(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dedupe_thumbnail_ratio_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	parentPath := filepath.Join(tempDir, "master.jpg")
	childPath := filepath.Join(tempDir, "thumb.jpg")
	writePatternJPEG(t, parentPath, 128, 128, 90)
	writePatternJPEG(t, childPath, 128, 64, 90)

	parentMeta := metadata.ExtractImageMetaJson(parentPath)
	childMeta := metadata.ExtractImageMetaJson(childPath)
	childStat, err := os.Stat(childPath)
	require.NoError(t, err)
	parentStat, err := os.Stat(parentPath)
	require.NoError(t, err)

	decision, err := ClassifyDerivative(childPath, childMeta, childStat.Size(), parentPath, parentMeta, parentStat.Size())
	require.NoError(t, err)
	require.False(t, decision.Confirmed)
}

func TestClassifyDerivativeRejectsLargerChild(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dedupe_thumbnail_larger_child")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	parentPath := filepath.Join(tempDir, "master.jpg")
	childPath := filepath.Join(tempDir, "thumb.jpg")
	writePatternJPEG(t, parentPath, 128, 96, 90)
	writePatternJPEG(t, childPath, 192, 144, 90)

	parentMeta := metadata.ExtractImageMetaJson(parentPath)
	childMeta := metadata.ExtractImageMetaJson(childPath)
	childStat, err := os.Stat(childPath)
	require.NoError(t, err)
	parentStat, err := os.Stat(parentPath)
	require.NoError(t, err)

	decision, err := ClassifyDerivative(childPath, childMeta, childStat.Size(), parentPath, parentMeta, parentStat.Size())
	require.NoError(t, err)
	require.False(t, decision.Confirmed)
}

func TestClassifyDerivativeAllowsSameSizedReencode(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dedupe_thumbnail_same_size")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	parentPath := filepath.Join(tempDir, "master.jpg")
	childPath := filepath.Join(tempDir, "derived.jpg")
	writePatternJPEG(t, parentPath, 160, 120, 92)
	writePatternJPEG(t, childPath, 160, 120, 72)

	parentMeta := metadata.ExtractImageMetaJson(parentPath)
	childMeta := metadata.ExtractImageMetaJson(childPath)
	childStat, err := os.Stat(childPath)
	require.NoError(t, err)
	parentStat, err := os.Stat(parentPath)
	require.NoError(t, err)

	decision, err := ClassifyDerivative(childPath, childMeta, childStat.Size(), parentPath, parentMeta, parentStat.Size())
	require.NoError(t, err)
	require.True(t, decision.Confirmed)
}

func TestClassifyDerivativeRejectsDifferentContentSameSize(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dedupe_thumbnail_different_content")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	parentPath := filepath.Join(tempDir, "master.jpg")
	childPath := filepath.Join(tempDir, "different.jpg")
	writePatternJPEGVariant(t, parentPath, 160, 120, 92, 0)
	writePatternJPEGVariant(t, childPath, 160, 120, 72, 2)

	parentMeta := metadata.ExtractImageMetaJson(parentPath)
	childMeta := metadata.ExtractImageMetaJson(childPath)
	childStat, err := os.Stat(childPath)
	require.NoError(t, err)
	parentStat, err := os.Stat(parentPath)
	require.NoError(t, err)

	decision, err := ClassifyDerivative(childPath, childMeta, childStat.Size(), parentPath, parentMeta, parentStat.Size())
	require.NoError(t, err)
	require.False(t, decision.Confirmed)
}

func TestClassifyDerivativeRejectsThumbnailParent(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dedupe_thumbnail_parent")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	parentPath := filepath.Join(tempDir, "thumbnails", "master.jpg")
	childPath := filepath.Join(tempDir, "thumbnails", "thumb.jpg")
	writePatternJPEG(t, parentPath, 128, 96, 90)
	writePatternJPEG(t, childPath, 128, 96, 70)

	parentMeta := metadata.ExtractImageMetaJson(parentPath)
	childMeta := metadata.ExtractImageMetaJson(childPath)
	childStat, err := os.Stat(childPath)
	require.NoError(t, err)
	parentStat, err := os.Stat(parentPath)
	require.NoError(t, err)

	decision, err := ClassifyDerivative(childPath, childMeta, childStat.Size(), parentPath, parentMeta, parentStat.Size())
	require.NoError(t, err)
	require.False(t, decision.Confirmed)
}

func TestRevalidateDerivativeAllowsThumbnailParentForExistingGroups(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dedupe_thumbnail_revalidate_parent")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	parentPath := filepath.Join(tempDir, "thumbnails", "master.jpg")
	childPath := filepath.Join(tempDir, "thumbnails", "thumb.jpg")
	writePatternJPEG(t, parentPath, 160, 120, 90)
	writePatternJPEG(t, childPath, 160, 120, 72)

	parentMeta := metadata.ExtractImageMetaJson(parentPath)
	childMeta := metadata.ExtractImageMetaJson(childPath)
	childStat, err := os.Stat(childPath)
	require.NoError(t, err)
	parentStat, err := os.Stat(parentPath)
	require.NoError(t, err)

	decision, err := RevalidateDerivative(childPath, childMeta, childStat.Size(), parentPath, parentMeta, parentStat.Size())
	require.NoError(t, err)
	require.True(t, decision.Confirmed)
	require.Equal(t, DerivativeVariant, decision.Kind)
}

func TestCompareMasterPreferencePrefersHigherResolutionOverFileSize(t *testing.T) {
	decision := CompareMasterPreference(
		"candidate.jpg",
		metadata.MediaMeta{Width: 128, Height: 128},
		900,
		"existing.jpg",
		metadata.MediaMeta{Width: 64, Height: 64},
		1500,
	)
	require.Equal(t, 1, decision)
}

func TestCompareMasterPreferencePrefersRAWWhenResolutionMatches(t *testing.T) {
	decision := CompareMasterPreference(
		"candidate.CR2",
		metadata.MediaMeta{Width: 6000, Height: 4000},
		2_000_000,
		"existing.jpg",
		metadata.MediaMeta{Width: 6000, Height: 4000},
		8_000_000,
	)
	require.Equal(t, 1, decision)
}

func TestCompareMasterPreferencePrefersNonThumbnailPath(t *testing.T) {
	decision := CompareMasterPreference(
		"album/master.jpg",
		metadata.MediaMeta{Width: 1000, Height: 800},
		100,
		"thumbnails/album/defaultimg_123.jpg",
		metadata.MediaMeta{Width: 1000, Height: 800},
		1000,
	)
	require.Equal(t, 1, decision)
}
