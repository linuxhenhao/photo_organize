package precompute

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/vision"
)

type ResolvedFeatures struct {
	ColorSignature    []uint8
	HasColorSignature bool
	ORB               *vision.ORBFeatureSet
	HasORB            bool
}

type Resolver struct {
	db *sql.DB

	mu       sync.Mutex
	entries  map[string]*ResolvedFeatures
	inflight map[string]*inflightCompute
	closed   bool

	Hits    int
	Misses  int
	Invalid int
}

type inflightCompute struct {
	done     chan struct{}
	features *ResolvedFeatures
	ok       bool
	err      error
}

func NewResolver(ctx context.Context, db *sql.DB) (*Resolver, error) {
	if err := EnsureVisualFeatureCacheTable(ctx, db); err != nil {
		return nil, err
	}
	return &Resolver{
		db:       db,
		entries:  make(map[string]*ResolvedFeatures),
		inflight: make(map[string]*inflightCompute),
	}, nil
}

func (r *Resolver) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	for _, entry := range r.entries {
		if entry != nil && entry.ORB != nil {
			entry.ORB.Close()
		}
	}
	r.entries = make(map[string]*ResolvedFeatures)
	r.inflight = make(map[string]*inflightCompute)
}

func (r *Resolver) Resolve(ctx context.Context, mmh3 string) (*ResolvedFeatures, bool) {
	if mmh3 == "" {
		r.mu.Lock()
		r.Misses++
		r.mu.Unlock()
		return nil, false
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, false
	}
	if cached, ok := r.entries[mmh3]; ok {
		r.Hits++
		r.mu.Unlock()
		if cached == nil {
			return nil, false
		}
		return cached, true
	}
	r.mu.Unlock()

	features, ok, invalid := r.loadPersisted(ctx, mmh3)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		if features != nil && features.ORB != nil {
			features.ORB.Close()
		}
		return nil, false
	}
	r.entries[mmh3] = features
	if ok {
		r.Hits++
	} else {
		r.Misses++
	}
	if invalid {
		r.Invalid++
	}
	if features == nil {
		return nil, false
	}
	return features, ok
}

// ResolveOrCompute returns persisted second-stage features for mmh3 when available.
// If the cache entry is missing or invalid, it computes features from absPath,
// persists them into visual_feature_cache, and then returns the computed value.
func (r *Resolver) ResolveOrCompute(ctx context.Context, mmh3 string, absPath string) (*ResolvedFeatures, bool, error) {
	if mmh3 == "" {
		return nil, false, fmt.Errorf("missing mmh3_hash")
	}

	if cached, ok := r.Resolve(ctx, mmh3); ok && cached != nil {
		return cached, true, nil
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, false, fmt.Errorf("resolver closed")
	}
	if inflight, ok := r.inflight[mmh3]; ok && inflight != nil {
		done := inflight.done
		r.mu.Unlock()
		select {
		case <-done:
			return inflight.features, inflight.ok, inflight.err
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}

	inflight := &inflightCompute{done: make(chan struct{})}
	r.inflight[mmh3] = inflight
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.inflight, mmh3)
		r.mu.Unlock()
		close(inflight.done)
	}()

	if absPath == "" {
		inflight.err = fmt.Errorf("missing absPath for compute")
		return nil, false, inflight.err
	}
	if _, err := os.Stat(absPath); err != nil {
		inflight.err = fmt.Errorf("stat %s: %w", absPath, err)
		return nil, false, inflight.err
	}

	features, err := computeSecondStageFeatures(absPath)
	if err != nil {
		inflight.err = err
		return nil, false, inflight.err
	}
	features.MMH3 = mmh3
	features.FeatureVersion = visualFeatureVersion

	if err := upsertVisualFeatures(ctx, r.db, features); err != nil {
		inflight.err = err
		return nil, false, inflight.err
	}

	computed := &ResolvedFeatures{
		ColorSignature:    features.ColorSignature,
		HasColorSignature: len(features.ColorSignature) > 0,
	}
	if len(features.ORBKeypoints) > 0 && len(features.ORBDescriptors) > 0 && features.ORBRows > 0 && features.ORBCols > 0 && features.ORBImgWidth > 0 && features.ORBImgHeight > 0 {
		orbSet, orbErr := vision.DeserializeORBFeatureSet(vision.ORBSerializedFeatures{
			Keypoints:   features.ORBKeypoints,
			Descriptors: features.ORBDescriptors,
			Rows:        features.ORBRows,
			Cols:        features.ORBCols,
			MatType:     features.ORBType,
			ImgWidth:    features.ORBImgWidth,
			ImgHeight:   features.ORBImgHeight,
		})
		if orbErr == nil && !orbSet.Descriptors.Empty() && len(orbSet.Keypoints) > 0 {
			computed.ORB = &orbSet
			computed.HasORB = true
		} else {
			orbSet.Close()
		}
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		if computed.ORB != nil {
			computed.ORB.Close()
		}
		inflight.err = fmt.Errorf("resolver closed")
		return nil, false, inflight.err
	}
	if prior, exists := r.entries[mmh3]; exists && prior != nil && prior.ORB != nil {
		prior.ORB.Close()
	}
	r.entries[mmh3] = computed
	r.mu.Unlock()

	inflight.features = computed
	inflight.ok = false
	return computed, false, nil
}

func (r *Resolver) loadPersisted(ctx context.Context, mmh3 string) (*ResolvedFeatures, bool, bool) {
	var colorSig []byte
	var orbKeypoints []byte
	var orbDescriptors []byte
	var orbRows int
	var orbCols int
	var orbType int
	var orbImgWidth int
	var orbImgHeight int

	err := r.db.QueryRowContext(ctx, `
		SELECT
			color_signature,
			orb_keypoints,
			orb_descriptors,
			orb_rows,
			orb_cols,
			orb_type,
			orb_img_width,
			orb_img_height
		FROM visual_feature_cache
		WHERE mmh3_hash = ? AND feature_version = ?
	`, mmh3, visualFeatureVersion).Scan(
		&colorSig,
		&orbKeypoints,
		&orbDescriptors,
		&orbRows,
		&orbCols,
		&orbType,
		&orbImgWidth,
		&orbImgHeight,
	)
	if err == sql.ErrNoRows {
		return nil, false, false
	}
	if err != nil {
		// Treat DB errors as "no cache" so cleangroups can proceed.
		return nil, false, true
	}

	resolved := &ResolvedFeatures{}
	if len(colorSig) > 0 {
		resolved.ColorSignature = append([]uint8(nil), colorSig...)
		resolved.HasColorSignature = true
	}

	if len(orbKeypoints) > 0 && len(orbDescriptors) > 0 && orbRows > 0 && orbCols > 0 && orbImgWidth > 0 && orbImgHeight > 0 {
		featureSet, featureErr := vision.DeserializeORBFeatureSet(vision.ORBSerializedFeatures{
			Keypoints:   orbKeypoints,
			Descriptors: orbDescriptors,
			Rows:        orbRows,
			Cols:        orbCols,
			MatType:     orbType,
			ImgWidth:    orbImgWidth,
			ImgHeight:   orbImgHeight,
		})
		if featureErr != nil {
			return resolved, true, true
		}
		if !featureSet.Descriptors.Empty() && len(featureSet.Keypoints) > 0 {
			resolved.ORB = &featureSet
			resolved.HasORB = true
		} else {
			featureSet.Close()
		}
	}

	return resolved, true, false
}

func (r *Resolver) Stats() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprintf("hits=%d misses=%d invalid=%d", r.Hits, r.Misses, r.Invalid)
}

func (r *Resolver) Snapshot() (hits int, misses int, invalid int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Hits, r.Misses, r.Invalid
}

func computeSecondStageFeatures(absPath string) (VisualFeatures, error) {
	perceptionHash, err := hasher.CalculateFullPerceptionHash(absPath)
	if err != nil {
		return VisualFeatures{}, fmt.Errorf("perception hash %s: %w", absPath, err)
	}

	colorSig, err := hasher.CalculateColorSignature(absPath)
	if err != nil {
		return VisualFeatures{}, fmt.Errorf("color signature %s: %w", absPath, err)
	}

	orb, err := vision.ComputeORBSerializedFeatures(absPath)
	if err != nil {
		return VisualFeatures{}, fmt.Errorf("ORB %s: %w", absPath, err)
	}

	return VisualFeatures{
		PerceptionHash: perceptionHash,
		ColorSignature: colorSig,
		ORBKeypoints:   orb.Keypoints,
		ORBDescriptors: orb.Descriptors,
		ORBRows:        orb.Rows,
		ORBCols:        orb.Cols,
		ORBType:        orb.MatType,
		ORBImgWidth:    orb.ImgWidth,
		ORBImgHeight:   orb.ImgHeight,
	}, nil
}
