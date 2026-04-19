package target

import (
	"context"

	"github.com/linuxhenhao/photo_organize/internal/dedupe"
	"github.com/linuxhenhao/photo_organize/internal/precompute"
)

func resolveDedupeFeatures(ctx context.Context, resolver *precompute.Resolver, mmh3 string, dhash uint64, hasDHash bool, absPath string) dedupe.ResolvedVisualFeatures {
	features := dedupe.ResolvedVisualFeatures{
		DHash:    dhash,
		HasDHash: hasDHash,
	}
	if resolver == nil || mmh3 == "" {
		return features
	}

	resolved, _, err := resolver.ResolveOrCompute(ctx, mmh3, absPath)
	if err != nil {
		return features
	}
	if resolved == nil {
		return features
	}

	features.ColorSignature = resolved.ColorSignature
	features.HasColorSignature = resolved.HasColorSignature
	features.ORB = resolved.ORB
	return features
}
