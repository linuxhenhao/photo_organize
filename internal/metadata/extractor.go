package metadata

import (
	"encoding/json"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// MediaMeta holds basic dimensions, size and creation time of a media file (image or video)
type MediaMeta struct {
	Width      int    `json:"width"`
	Height     int    `json:"height"`
	Size       int64  `json:"size"`
	CreateTime string `json:"create_time"`
}

// ExtractImageMeta opens a media file and extracts its dimensions and modification time.
// It supports images via native Go decoding and videos via exiftool.
func ExtractImageMeta(fullPath string) MediaMeta {
	var meta MediaMeta

	if stat, err := os.Stat(fullPath); err == nil {
		meta.CreateTime = stat.ModTime().Format("2006-01-02 15:04:05")
		meta.Size = stat.Size()
	}

	// 1. Try native Go image decoding (fast for JPEG/PNG/GIF) for dimensions
	if file, err := os.Open(fullPath); err == nil {
		config, _, err := image.DecodeConfig(file)
		file.Close()
		if err == nil {
			meta.Width = config.Width
			meta.Height = config.Height
		}
	}

	// 2. Fallback or augment with exiftool for videos, complex image formats, or metadata
	// Use -s instead of -s -s -s to keep the "Key : Value" format for parsing.
	cmd := exec.Command("exiftool", "-s",
		"-ImageWidth", "-ImageHeight", "-VideoSize",
		"-CreateDate", "-DateTimeOriginal", "-MediaCreateDate",
		"-fast", fullPath)
	output, err := cmd.Output()
	if err == nil {
		lines := strings.Split(string(output), "\n")
		for _, line := range lines {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) < 2 {
				continue
			}
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])

			switch key {
			case "ImageWidth":
				if w, err := strconv.Atoi(val); err == nil && meta.Width == 0 {
					meta.Width = w
				}
			case "ImageHeight":
				if h, err := strconv.Atoi(val); err == nil && meta.Height == 0 {
					meta.Height = h
				}
			case "VideoSize":
				// Often in format "1920x1080"
				dims := strings.Split(val, "x")
				if len(dims) == 2 {
					if w, err := strconv.Atoi(dims[0]); err == nil && meta.Width == 0 {
						meta.Width = w
					}
					if h, err := strconv.Atoi(dims[1]); err == nil && meta.Height == 0 {
						meta.Height = h
					}
				}
			case "CreateDate", "DateTimeOriginal", "MediaCreateDate":
				// Exiftool often returns "2023:01:01 12:00:00". Normalize to "2023-01-01 12:00:00"
				normalized := strings.Replace(val, ":", "-", 2)
				// Prefer non-zero exif dates over the filesystem modtime
				if !strings.Contains(val, "0000:00:00") && val != "" {
					meta.CreateTime = normalized
				}
			}
		}
	}

	return meta
}

// ExtractImageMetaJson returns the extracted metadata as a JSON string.
func ExtractImageMetaJson(fullPath string) string {
	meta := ExtractImageMeta(fullPath)
	b, err := json.Marshal(meta)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// ParseMediaMetaJSON decodes cached metadata JSON. Invalid input returns zero values.
func ParseMediaMetaJSON(raw string) MediaMeta {
	if raw == "" {
		return MediaMeta{}
	}

	var meta MediaMeta
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return MediaMeta{}
	}
	return meta
}
