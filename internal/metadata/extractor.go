package metadata

import (
	"encoding/json"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"strconv"
	"strings"

	projectexiftool "github.com/linuxhenhao/photo_organize/internal/exiftool"
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

	// 2. Fallback or augment with the shared exiftool pool for videos, complex image formats, or metadata.
	pool, err := projectexiftool.SharedPool()
	if err == nil {
		results, queryErr := pool.Extract([]string{fullPath}, []string{
			"ImageWidth",
			"ImageHeight",
			"VideoSize",
			"CreateDate",
			"DateTimeOriginal",
			"MediaCreateDate",
		}, projectexiftool.QueryOptions{
			Fast:              true,
			IgnoreMinorErrors: true,
			DateFormat:        "%Y-%m-%d %H:%M:%S",
		})
		if queryErr == nil && len(results) == 1 {
			if width, ok := results[0].GetInt("ImageWidth"); ok && meta.Width == 0 {
				meta.Width = width
			}
			if height, ok := results[0].GetInt("ImageHeight"); ok && meta.Height == 0 {
				meta.Height = height
			}

			if value, ok := results[0].GetString("VideoSize"); ok {
				dims := strings.Split(value, "x")
				if len(dims) == 2 {
					if w, err := strconv.Atoi(strings.TrimSpace(dims[0])); err == nil && meta.Width == 0 {
						meta.Width = w
					}
					if h, err := strconv.Atoi(strings.TrimSpace(dims[1])); err == nil && meta.Height == 0 {
						meta.Height = h
					}
				}
			}

			for _, key := range []string{"CreateDate", "DateTimeOriginal", "MediaCreateDate"} {
				value, ok := results[0].GetString(key)
				if !ok || value == "" || strings.Contains(value, "0000:00:00") {
					continue
				}
				meta.CreateTime = value
				break
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
