package dedupe

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"

	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/linuxhenhao/photo_organize/internal/vision"
)

const (
	// CandidateSearchDistance is intentionally conservative because automatic
	// matching only targets derived thumbnails, not generic similar-image search.
	CandidateSearchDistance = 16
	maxConfirmPHashDistance = 12
	maxAssistPHashDistance  = 16
	aspectRatioTolerance    = 0.02
	maxColorSignatureDelta  = 12
	maxParentUpscaleRatio   = 1.05
)

type DerivativeKind int

const (
	DerivativeNoMatch DerivativeKind = iota
	DerivativeVariant
)

// DerivativeDecision reports whether child is a derived variant of parent.
type DerivativeDecision struct {
	Kind      DerivativeKind
	Confirmed bool
}

// ClassifyDerivative confirms whether child should be treated as a derived
// thumbnail/export of parent. The relationship is directional.
func ClassifyDerivative(childPath string, childMetaJSON string, childSize int64, parentPath string, parentMetaJSON string, parentSize int64) (DerivativeDecision, error) {
	if !CanAutoGroupUnderParent(childPath, parentPath) {
		return DerivativeDecision{}, nil
	}

	childMeta := loadComparableMeta(childPath, childMetaJSON)
	parentMeta := loadComparableMeta(parentPath, parentMetaJSON)

	if !hasComparableDimensions(childMeta, parentMeta) {
		return DerivativeDecision{}, nil
	}
	if !aspectRatioCompatible(childMeta, parentMeta) {
		return DerivativeDecision{}, nil
	}
	if !childFitsWithinParent(childMeta, parentMeta) {
		return DerivativeDecision{}, nil
	}

	childHash, err := hasher.CalculatePHash(childPath)
	if err != nil {
		return DerivativeDecision{}, fmt.Errorf("failed to calculate child candidate hash %s: %w", childPath, err)
	}
	parentHash, err := hasher.CalculatePHash(parentPath)
	if err != nil {
		return DerivativeDecision{}, fmt.Errorf("failed to calculate parent candidate hash %s: %w", parentPath, err)
	}

	hashDistance := hasher.HammingDistance(childHash, parentHash)
	if hashDistance > maxAssistPHashDistance {
		return DerivativeDecision{}, nil
	}

	childSignature, err := hasher.CalculateColorSignature(childPath)
	if err != nil {
		return DerivativeDecision{}, fmt.Errorf("failed to calculate color signature for %s: %w", childPath, err)
	}
	parentSignature, err := hasher.CalculateColorSignature(parentPath)
	if err != nil {
		return DerivativeDecision{}, fmt.Errorf("failed to calculate color signature for %s: %w", parentPath, err)
	}
	colorDistance := hasher.ColorSignatureDistance(childSignature, parentSignature)
	if hashDistance > maxConfirmPHashDistance && colorDistance > maxColorSignatureDelta {
		return DerivativeDecision{}, nil
	}

	verification, err := vision.VerifyDerivativeWithSIFT(childPath, parentPath)
	if err != nil {
		return DerivativeDecision{}, err
	}
	if verification.Confirmed {
		return DerivativeDecision{Kind: DerivativeVariant, Confirmed: true}, nil
	}

	return DerivativeDecision{}, nil
}

// CompareMasterPreference chooses which of two files is a better canonical
// master once they are already known to represent the same base image.
func CompareMasterPreference(candidatePath string, candidate metadata.MediaMeta, candidateSize int64, existingPath string, existing metadata.MediaMeta, existingSize int64) int {
	candidateMasterLike := IsLikelyMasterPath(candidatePath)
	existingMasterLike := IsLikelyMasterPath(existingPath)
	if candidateMasterLike != existingMasterLike {
		if candidateMasterLike {
			return 1
		}
		return -1
	}

	candidateRaw := isRawPath(candidatePath)
	existingRaw := isRawPath(existingPath)
	if candidateRaw != existingRaw && dimensionsNearlyEqual(candidate, existing, aspectRatioTolerance) {
		if candidateRaw {
			return 1
		}
		return -1
	}

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

	if candidateSize > existingSize {
		return 1
	}
	if candidateSize < existingSize {
		return -1
	}
	return 0
}

func CanAutoGroupUnderParent(childPath, parentPath string) bool {
	if childPath == "" || parentPath == "" {
		return false
	}
	if filepath.Clean(childPath) == filepath.Clean(parentPath) {
		return false
	}
	return IsLikelyMasterPath(parentPath)
}

func IsThumbnailPath(path string) bool {
	clean := filepath.Clean(path)
	parts := strings.Split(clean, string(filepath.Separator))
	for _, part := range parts {
		if strings.EqualFold(part, "thumbnails") {
			return true
		}
	}
	return false
}

func ThumbnailLikeFilename(path string) bool {
	name := strings.ToLower(filepath.Base(path))
	return strings.Contains(name, "thumb") || strings.HasPrefix(name, "defaultimg_")
}

func IsLikelyMasterPath(path string) bool {
	return !IsThumbnailPath(path) && !ThumbnailLikeFilename(path)
}

func loadComparableMeta(path string, raw string) metadata.MediaMeta {
	meta := metadata.ParseMediaMetaJSON(raw)
	if meta.Width > 0 && meta.Height > 0 {
		return meta
	}
	return metadata.ExtractImageMeta(path)
}

func hasComparableDimensions(a metadata.MediaMeta, b metadata.MediaMeta) bool {
	return a.Width > 0 && a.Height > 0 && b.Width > 0 && b.Height > 0
}

func aspectRatioCompatible(a metadata.MediaMeta, b metadata.MediaMeta) bool {
	ratioA := float64(a.Width) / float64(a.Height)
	ratioB := float64(b.Width) / float64(b.Height)
	diff := math.Abs(ratioA-ratioB) / math.Max(ratioA, ratioB)
	return diff <= aspectRatioTolerance
}

func childFitsWithinParent(child metadata.MediaMeta, parent metadata.MediaMeta) bool {
	return float64(child.Width) <= float64(parent.Width)*maxParentUpscaleRatio &&
		float64(child.Height) <= float64(parent.Height)*maxParentUpscaleRatio
}

func dimensionsNearlyEqual(a metadata.MediaMeta, b metadata.MediaMeta, tolerance float64) bool {
	if a.Width <= 0 || a.Height <= 0 || b.Width <= 0 || b.Height <= 0 {
		return false
	}

	widthDiff := math.Abs(float64(a.Width)-float64(b.Width)) / math.Max(float64(a.Width), float64(b.Width))
	heightDiff := math.Abs(float64(a.Height)-float64(b.Height)) / math.Max(float64(a.Height), float64(b.Height))
	return widthDiff <= tolerance && heightDiff <= tolerance
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
