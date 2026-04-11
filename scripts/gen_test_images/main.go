package main

import (
	"bytes"
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	projectexiftool "github.com/linuxhenhao/photo_organize/internal/exiftool"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/nfnt/resize"
)

type thumbSpec struct {
	Name    string
	Width   uint
	Quality int
}

var realThumbSpecs = []thumbSpec{
	{Name: "large", Width: 960, Quality: 88},
	{Name: "medium", Width: 480, Quality: 78},
	{Name: "small", Width: 160, Quality: 68},
}

func generateImage(path string, width, height int, imgColor color.RGBA) error {
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for x := 0; x < width; x++ {
		for y := 0; y < height; y++ {
			noise := color.RGBA{
				R: imgColor.R + uint8(rand.Intn(20)),
				G: imgColor.G + uint8(rand.Intn(20)),
				B: imgColor.B + uint8(rand.Intn(20)),
				A: 255,
			}
			img.Set(x, y, noise)
		}
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return jpeg.Encode(f, img, &jpeg.Options{Quality: 90})
}

func createThumbnail(srcPath, destPath string) error {
	file, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer file.Close()

	img, err := jpeg.Decode(file)
	if err != nil {
		return err
	}

	m := resize.Resize(160, 0, img, resize.Lanczos3)

	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return err
	}
	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	return jpeg.Encode(out, m, &jpeg.Options{Quality: 75})
}

func extractPreviewImage(srcPath string) ([]byte, error) {
	pool, err := projectexiftool.SharedPool()
	if err != nil {
		return nil, err
	}

	results, err := pool.Extract([]string{srcPath}, []string{
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
		return nil, fmt.Errorf("unexpected exiftool result count for %s: %d", srcPath, len(results))
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

	return nil, fmt.Errorf("no extractable preview image found in %s", srcPath)
}

func writeJPEG(path string, img image.Image, quality int) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return jpeg.Encode(f, img, &jpeg.Options{Quality: quality})
}

func generateMockImages(outputRoot string) error {
	mockSource := filepath.Join(outputRoot, "source_mock")
	mockThumbSource := filepath.Join(outputRoot, "source_mock_thumbs")

	fmt.Println("Generating mock original images...")
	for i := 1; i <= 5; i++ {
		origPath := filepath.Join(mockSource, fmt.Sprintf("img_2023_05_%02d.jpg", i))
		col := color.RGBA{uint8(rand.Intn(200)), uint8(rand.Intn(200)), uint8(rand.Intn(200)), 255}

		if err := generateImage(origPath, 800, 600, col); err != nil {
			return fmt.Errorf("failed to generate mock image %s: %w", origPath, err)
		}

		t := time.Date(2023, 5, i, 12, 0, 0, 0, time.Local)
		if err := os.Chtimes(origPath, t, t); err != nil {
			return err
		}

		thumbPath := filepath.Join(mockThumbSource, fmt.Sprintf("thumb_2023_05_%02d.jpg", i))
		if err := createThumbnail(origPath, thumbPath); err != nil {
			return fmt.Errorf("failed to generate mock thumbnail %s: %w", thumbPath, err)
		}
		if err := os.Chtimes(thumbPath, t, t); err != nil {
			return err
		}
	}

	fmt.Printf("Mock originals are in: %s\n", mockSource)
	fmt.Printf("Mock thumbnails are in: %s\n", mockThumbSource)
	return nil
}

func discoverRealSources(repoRoot string) ([]string, error) {
	patterns := []string{
		filepath.Join(repoRoot, "test_data", "source", "*.ARW"),
		filepath.Join(repoRoot, "test_data", "source1", "*.ARW"),
	}

	var files []string
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		files = append(files, matches...)
	}

	sort.Strings(files)
	return files, nil
}

func generateRealThumbs(repoRoot string, outputRoot string) error {
	sourceFiles, err := discoverRealSources(repoRoot)
	if err != nil {
		return err
	}

	realThumbRoot := filepath.Join(outputRoot, "source_real_thumbs")
	fmt.Println("Generating real-image thumbnails from ARW previews...")

	for _, srcPath := range sourceFiles {
		previewBytes, err := extractPreviewImage(srcPath)
		if err != nil {
			log.Printf("Skipping %s: %v", srcPath, err)
			continue
		}

		img, _, err := image.Decode(bytes.NewReader(previewBytes))
		if err != nil {
			log.Printf("Skipping %s: failed to decode preview: %v", srcPath, err)
			continue
		}

		stat, err := os.Stat(srcPath)
		if err != nil {
			log.Printf("Skipping %s: %v", srcPath, err)
			continue
		}
		createTime, _, err := metadata.GetMetadata(srcPath, stat)
		if err != nil {
			createTime = stat.ModTime()
		}

		baseName := strings.TrimSuffix(filepath.Base(srcPath), filepath.Ext(srcPath))
		datePrefix := createTime.Format("2006-01-02")
		for _, spec := range realThumbSpecs {
			destPath := filepath.Join(realThumbRoot, spec.Name, fmt.Sprintf("%s_%s_%s.jpg", datePrefix, baseName, spec.Name))
			thumb := resize.Resize(spec.Width, 0, img, resize.Lanczos3)
			if err := writeJPEG(destPath, thumb, spec.Quality); err != nil {
				return fmt.Errorf("failed to write real thumbnail %s: %w", destPath, err)
			}
			if err := os.Chtimes(destPath, createTime, createTime); err != nil {
				return err
			}
		}
	}

	fmt.Printf("Real-image thumbnails are in: %s\n", realThumbRoot)
	return nil
}

func main() {
	rand.Seed(time.Now().UnixNano())

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to resolve cwd: %v", err)
	}

	defaultOutputRoot := filepath.Join(cwd, "test_data")
	outputRoot := flag.String("output-root", defaultOutputRoot, "root directory for generated fixtures")
	generateMock := flag.Bool("generate-mock", true, "generate synthetic mock images and thumbnails")
	generateReal := flag.Bool("generate-real-thumbs", true, "generate thumbnails derived from real ARW files")
	flag.Parse()

	if *generateMock {
		if err := generateMockImages(*outputRoot); err != nil {
			log.Fatal(err)
		}
	}

	if *generateReal {
		if err := generateRealThumbs(cwd, *outputRoot); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Fixture generation complete.")
}
