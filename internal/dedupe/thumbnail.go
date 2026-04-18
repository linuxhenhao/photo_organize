package dedupe

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"

	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
)

const (
	// CandidateSearchDistance is intentionally looser than the old 12-bit cutoff.
	// Repo mock thumbnails currently reach dHash distance 14 after JPEG re-encode
	// and resize, while ARW preview-derived thumbnails stay near 0-1. Using 16
	// keeps a small recall margin above the worst known thumbnail fixture without
	// pushing BK-tree candidate volume into the much noisier 18+ range.
	CandidateSearchDistance = 16
	maxConfirmPHashDistance = 16
	aspectRatioTolerance    = 0.03
	maxColorSignatureDelta  = 12
)

// ThumbnailDecision reports whether two files are the same image and which one should remain master.
type ThumbnailDecision struct {
	Confirmed       bool
	PreferCandidate bool
}

// EvaluateThumbnailMatch confirms whether candidate and existing files are the same visual asset.
// It uses metadata shape checks plus a stronger perceptual hash before allowing auto-moves.
func EvaluateThumbnailMatch(candidatePath string, candidateMetaJSON string, candidateSize int64, existingPath string, existingMetaJSON string, existingSize int64) (ThumbnailDecision, error) {
	candidateMeta := loadComparableMeta(candidatePath, candidateMetaJSON)
	existingMeta := loadComparableMeta(existingPath, existingMetaJSON)

	if !aspectRatioCompatible(candidateMeta, existingMeta) {
		return ThumbnailDecision{}, nil
	}

	candidateHash, candidateHashErr := hasher.CalculatePerceptionHash(candidatePath)
	existingHash, existingHashErr := hasher.CalculatePerceptionHash(existingPath)
	if candidateHashErr == nil && existingHashErr == nil && hasher.HammingDistance(candidateHash, existingHash) <= maxConfirmPHashDistance {
		return ThumbnailDecision{
			Confirmed:       true,
			PreferCandidate: ComparePreference(candidatePath, candidateMeta, candidateSize, existingPath, existingMeta, existingSize) > 0,
		}, nil
	}

	candidateSignature, sigErr := hasher.CalculateColorSignature(candidatePath)
	existingSignature, existingSigErr := hasher.CalculateColorSignature(existingPath)
	if sigErr == nil && existingSigErr == nil {
		if hasher.ColorSignatureDistance(candidateSignature, existingSignature) <= maxColorSignatureDelta {
			return ThumbnailDecision{
				Confirmed:       true,
				PreferCandidate: ComparePreference(candidatePath, candidateMeta, candidateSize, existingPath, existingMeta, existingSize) > 0,
			}, nil
		}
		return ThumbnailDecision{}, nil
	}

	if candidateHashErr != nil {
		return ThumbnailDecision{}, fmt.Errorf("failed to confirm candidate %s: %w", candidatePath, candidateHashErr)
	}
	if existingHashErr != nil {
		return ThumbnailDecision{}, fmt.Errorf("failed to confirm existing %s: %w", existingPath, existingHashErr)
	}
	if sigErr != nil {
		return ThumbnailDecision{}, fmt.Errorf("failed to calculate color signature for %s: %w", candidatePath, sigErr)
	}
	if existingSigErr != nil {
		return ThumbnailDecision{}, fmt.Errorf("failed to calculate color signature for %s: %w", existingPath, existingSigErr)
	}

	return ThumbnailDecision{}, nil
}

func loadComparableMeta(path string, raw string) metadata.MediaMeta {
	meta := metadata.ParseMediaMetaJSON(raw)
	if meta.Width > 0 && meta.Height > 0 {
		return meta
	}
	return metadata.ExtractImageMeta(path)
}

func aspectRatioCompatible(a metadata.MediaMeta, b metadata.MediaMeta) bool {
	if a.Width <= 0 || a.Height <= 0 || b.Width <= 0 || b.Height <= 0 {
		return true
	}

	ratioA := float64(a.Width) / float64(a.Height)
	ratioB := float64(b.Width) / float64(b.Height)
	diff := math.Abs(ratioA-ratioB) / math.Max(ratioA, ratioB)
	return diff <= aspectRatioTolerance
}

// ComparePreference chooses which of two visually equivalent files should remain the master.
func ComparePreference(candidatePath string, candidate metadata.MediaMeta, candidateSize int64, existingPath string, existing metadata.MediaMeta, existingSize int64) int {
	candidateArea := pixelArea(candidate)
	existingArea := pixelArea(existing)
	if candidateArea > 0 && existingArea > 0 && candidateArea != existingArea {
		if candidateArea > existingArea {
			return 1
		}
		return -1
	}

	if candidate.Width > 0 && existing.Width > 0 && candidate.Width != existing.Width {
		if candidate.Width > existing.Width {
			return 1
		}
		return -1
	}

	if candidate.Height > 0 && existing.Height > 0 && candidate.Height != existing.Height {
		if candidate.Height > existing.Height {
			return 1
		}
		return -1
	}

	candidateRaw := isRawPath(candidatePath)
	existingRaw := isRawPath(existingPath)
	if candidateRaw != existingRaw {
		if candidateRaw {
			return 1
		}
		return -1
	}

	if candidateSize > existingSize {
		return 1
	}
	if candidateSize < existingSize {
		return -1
	}
	return 0
}

func isRawPath(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".cr2", ".cr3", ".arw", ".nef", ".nrw", ".dng", ".rw2", ".orf", ".raf", ".srw", ".raw":
		return true
	default:
		return false
	}
}

func pixelArea(meta metadata.MediaMeta) int64 {
	if meta.Width <= 0 || meta.Height <= 0 {
		return 0
	}
	return int64(meta.Width) * int64(meta.Height)
}
