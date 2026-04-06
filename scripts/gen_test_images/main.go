package main

import (
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/nfnt/resize" // We can use this to generate fake thumbnails
)

// generateImage creates a random rectangle image and saves it to the path
func generateImage(path string, width, height int, imgColor color.RGBA) error {
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for x := 0; x < width; x++ {
		for y := 0; y < height; y++ {
			// Add slight noise to make hashes more robust
			noise := color.RGBA{
				R: imgColor.R + uint8(rand.Intn(20)),
				G: imgColor.G + uint8(rand.Intn(20)),
				B: imgColor.B + uint8(rand.Intn(20)),
				A: 255,
			}
			img.Set(x, y, noise)
		}
	}

	// Create directory if not exists
	os.MkdirAll(filepath.Dir(path), 0755)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return jpeg.Encode(f, img, &jpeg.Options{Quality: 90})
}

// createThumbnail reads an image, resizes it, and saves as thumbnail
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

	// Resize to width 160, keeping aspect ratio
	m := resize.Resize(160, 0, img, resize.Lanczos3)

	os.MkdirAll(filepath.Dir(destPath), 0755)
	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	return jpeg.Encode(out, m, &jpeg.Options{Quality: 75})
}

func main() {
	rand.Seed(time.Now().UnixNano())

	cwd, _ := os.Getwd()
	mockSource := filepath.Join(cwd, "test_data", "source_mock")
	mockThumbSource := filepath.Join(cwd, "test_data", "source_mock_thumbs")

	fmt.Println("Generating mock original images...")
	for i := 1; i <= 5; i++ {
		origPath := filepath.Join(mockSource, fmt.Sprintf("img_2023_05_%02d.jpg", i))
		col := color.RGBA{uint8(rand.Intn(200)), uint8(rand.Intn(200)), uint8(rand.Intn(200)), 255}
		
		if err := generateImage(origPath, 800, 600, col); err != nil {
			log.Fatalf("Failed to generate image %s: %v", origPath, err)
		}
		
		// Set fake file create time for the mock image metadata (for scanner logic)
		t := time.Date(2023, 5, i, 12, 0, 0, 0, time.Local)
		os.Chtimes(origPath, t, t)

		// Generate thumbnail in a different source dir
		thumbPath := filepath.Join(mockThumbSource, fmt.Sprintf("thumb_2023_05_%02d.jpg", i))
		if err := createThumbnail(origPath, thumbPath); err != nil {
			log.Fatalf("Failed to generate thumbnail %s: %v", thumbPath, err)
		}
		os.Chtimes(thumbPath, t, t)
	}

	fmt.Println("Successfully generated mock images and their thumbnails!")
	fmt.Printf("Originals are in: %s\n", mockSource)
	fmt.Printf("Thumbnails are in: %s\n", mockThumbSource)
}
