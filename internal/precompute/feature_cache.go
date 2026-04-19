package precompute

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const visualFeatureVersion = "v1"

type VisualFeatures struct {
	MMH3           string
	FeatureVersion string

	PerceptionHash uint64
	ColorSignature []uint8

	ORBKeypoints   []byte
	ORBDescriptors []byte
	ORBRows        int
	ORBCols        int
	ORBType        int
	ORBImgWidth    int
	ORBImgHeight   int

	UpdatedAt string
}

func EnsureVisualFeatureCacheTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS visual_feature_cache (
			mmh3_hash TEXT NOT NULL,
			feature_version TEXT NOT NULL,
			perception_hash TEXT NOT NULL DEFAULT '',
			color_signature BLOB,
			orb_keypoints BLOB,
			orb_descriptors BLOB,
			orb_rows INTEGER NOT NULL DEFAULT 0,
			orb_cols INTEGER NOT NULL DEFAULT 0,
			orb_type INTEGER NOT NULL DEFAULT 0,
			orb_img_width INTEGER NOT NULL DEFAULT 0,
			orb_img_height INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (mmh3_hash, feature_version)
		);
	`)
	if err != nil {
		return fmt.Errorf("create visual_feature_cache: %w", err)
	}

	// Best-effort migrations for existing databases.
	_, _ = db.ExecContext(ctx, `ALTER TABLE visual_feature_cache ADD COLUMN orb_img_width INTEGER NOT NULL DEFAULT 0`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE visual_feature_cache ADD COLUMN orb_img_height INTEGER NOT NULL DEFAULT 0`)
	return nil
}

func hasCachedVisualFeatures(ctx context.Context, db *sql.DB, mmh3 string, featureVersion string) (bool, error) {
	var exists int
	err := db.QueryRowContext(ctx, `
		SELECT 1
		FROM visual_feature_cache
		WHERE mmh3_hash = ? AND feature_version = ?
		LIMIT 1
	`, mmh3, featureVersion).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lookup visual_feature_cache for %s: %w", mmh3, err)
	}
	return true, nil
}

func upsertVisualFeatures(ctx context.Context, db *sql.DB, features VisualFeatures) error {
	if features.FeatureVersion == "" {
		features.FeatureVersion = visualFeatureVersion
	}
	if features.UpdatedAt == "" {
		features.UpdatedAt = time.Now().Format(time.RFC3339)
	}

	_, err := db.ExecContext(ctx, `
		INSERT INTO visual_feature_cache (
			mmh3_hash,
			feature_version,
			perception_hash,
			color_signature,
			orb_keypoints,
			orb_descriptors,
			orb_rows,
			orb_cols,
			orb_type,
			orb_img_width,
			orb_img_height,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(mmh3_hash, feature_version) DO UPDATE SET
			perception_hash = excluded.perception_hash,
			color_signature = excluded.color_signature,
			orb_keypoints = excluded.orb_keypoints,
			orb_descriptors = excluded.orb_descriptors,
			orb_rows = excluded.orb_rows,
			orb_cols = excluded.orb_cols,
			orb_type = excluded.orb_type,
			orb_img_width = excluded.orb_img_width,
			orb_img_height = excluded.orb_img_height,
			updated_at = excluded.updated_at
	`, features.MMH3,
		features.FeatureVersion,
		fmt.Sprintf("%d", features.PerceptionHash),
		features.ColorSignature,
		features.ORBKeypoints,
		features.ORBDescriptors,
		features.ORBRows,
		features.ORBCols,
		features.ORBType,
		features.ORBImgWidth,
		features.ORBImgHeight,
		features.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert visual_feature_cache for %s: %w", features.MMH3, err)
	}
	return nil
}
