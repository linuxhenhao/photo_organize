use crate::db::FEATURE_VERSION;
use crate::features::{
    AkazeStatus, VisualFeatures, compute_visual_features_for_mime_from_bytes,
    deserialize_akaze_descriptors, deserialize_akaze_points, phash_to_u64,
    serialize_akaze_descriptors, serialize_akaze_points, supports_visual_features,
};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FeatureCacheKey {
    exact_hash: String,
    size_bytes: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct FeatureRequest<'a> {
    pub path: &'a Path,
    pub mime_type: &'a str,
    pub exact_hash: &'a str,
    pub size_bytes: i64,
    pub phash_hint: &'a str,
    pub phash_bits: i64,
    pub width: i64,
    pub height: i64,
}

#[derive(Debug, Default)]
pub struct FeatureLoader {
    memo: HashMap<FeatureCacheKey, VisualFeatures>,
}

impl FeatureLoader {
    pub fn load(
        &mut self,
        conn: &Connection,
        request: FeatureRequest<'_>,
    ) -> Result<VisualFeatures> {
        if !supports_visual_features(request.path, request.mime_type) {
            let fallback = fallback_features(request, AkazeStatus::Unavailable);
            let key = FeatureCacheKey {
                exact_hash: request.exact_hash.to_string(),
                size_bytes: request.size_bytes,
            };
            self.memo.insert(key, fallback.clone());
            return Ok(fallback);
        }

        let key = FeatureCacheKey {
            exact_hash: request.exact_hash.to_string(),
            size_bytes: request.size_bytes,
        };
        if let Some(cached) = self.memo.get(&key) {
            return Ok(cached.clone());
        }

        if let Some(cached) = load_cached_feature(conn, &key, request)? {
            self.memo.insert(key, cached.clone());
            return Ok(cached);
        }

        let mut computed = compute_feature(request)?;
        computed.size_bytes_hint = request.size_bytes;
        save_feature_cache(conn, request.size_bytes, &computed)?;
        self.memo.insert(key, computed.clone());
        Ok(computed)
    }

    pub fn compute_only(&self, item: &crate::scan::DiscoveredFile) -> Result<VisualFeatures> {
        let mut feat = compute_feature(FeatureRequest {
            path: &item.path,
            mime_type: &item.mime_type,
            exact_hash: &item.exact_hash,
            size_bytes: item.size_bytes,
            phash_hint: &item.phash,
            phash_bits: item.phash_bits,
            width: item.width,
            height: item.height,
        })?;
        feat.size_bytes_hint = item.size_bytes;
        Ok(feat)
    }
}

fn compute_feature(request: FeatureRequest<'_>) -> Result<VisualFeatures> {
    let bytes = match std::fs::read(request.path) {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::warn!(
                path = %request.path.display(),
                error = %err,
                "feature computation failed while reading file"
            );
            return Ok(fallback_features(request, AkazeStatus::DecodeError));
        }
    };
    if let Some(mut computed) =
        compute_visual_features_for_mime_from_bytes(&bytes, request.path, request.mime_type)?
    {
        computed.exact_hash = request.exact_hash.to_string();
        return Ok(computed);
    }

    Ok(fallback_features(request, AkazeStatus::DecodeError))
}

fn fallback_features(request: FeatureRequest<'_>, akaze_status: AkazeStatus) -> VisualFeatures {
    VisualFeatures {
        exact_hash: request.exact_hash.to_string(),
        phash: request.phash_hint.to_string(),
        phash_bits: request.phash_bits,
        phash_value: phash_to_u64(request.phash_hint).unwrap_or(0),
        width: request.width,
        height: request.height,
        size_bytes_hint: request.size_bytes,
        akaze_status,
        akaze_keypoints: None,
        akaze_points: None,
        akaze_descriptors: None,
    }
}

fn load_cached_feature(
    conn: &Connection,
    key: &FeatureCacheKey,
    request: FeatureRequest<'_>,
) -> Result<Option<VisualFeatures>> {
    let row = conn
        .query_row(
            r#"
        SELECT akaze_status, akaze_keypoints, akaze_descriptors
             , akaze_points
        FROM feature_cache
        WHERE exact_hash = ?1 AND size_bytes = ?2 AND feature_version = ?3
        "#,
            params![key.exact_hash, key.size_bytes, FEATURE_VERSION],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((status_text, keypoints, descriptors_blob, points_blob)) = row else {
        return Ok(None);
    };
    let status = AkazeStatus::from_db_str(&status_text)
        .with_context(|| format!("unknown akaze status {status_text}"))?;
    if status.is_retryable() {
        return Ok(None);
    }
    let descriptors = descriptors_blob
        .as_deref()
        .map(deserialize_akaze_descriptors)
        .transpose()?;
    let points = points_blob
        .as_deref()
        .map(deserialize_akaze_points)
        .transpose()?;
    if status == AkazeStatus::Ready && (descriptors.is_none() || points.is_none()) {
        return Ok(None);
    }
    Ok(Some(VisualFeatures {
        exact_hash: request.exact_hash.to_string(),
        phash: request.phash_hint.to_string(),
        phash_bits: request.phash_bits,
        phash_value: phash_to_u64(request.phash_hint).unwrap_or(0),
        width: request.width,
        height: request.height,
        size_bytes_hint: request.size_bytes,
        akaze_status: status,
        akaze_keypoints: keypoints.and_then(|value| usize::try_from(value).ok()),
        akaze_points: points,
        akaze_descriptors: descriptors,
    }))
}

pub fn has_reusable_feature_cache(
    conn: &Connection,
    exact_hash: &str,
    size_bytes: i64,
) -> Result<bool> {
    let status = conn
        .query_row(
            r#"
            SELECT akaze_status
            FROM feature_cache
            WHERE exact_hash = ?1 AND size_bytes = ?2 AND feature_version = ?3
            "#,
            params![exact_hash, size_bytes, FEATURE_VERSION],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(status_text) = status else {
        return Ok(false);
    };
    let status = AkazeStatus::from_db_str(&status_text)
        .with_context(|| format!("unknown akaze status {status_text}"))?;
    Ok(!status.is_retryable())
}

pub fn save_feature_cache(
    conn: &Connection,
    size_bytes: i64,
    visual: &VisualFeatures,
) -> Result<()> {
    let descriptors = visual
        .akaze_descriptors
        .as_ref()
        .map(|value| serialize_akaze_descriptors(value))
        .transpose()?;
    let points = visual
        .akaze_points
        .as_ref()
        .map(|value| serialize_akaze_points(value))
        .transpose()?;
    let keypoints = visual
        .akaze_keypoints
        .and_then(|value| i64::try_from(value).ok());
    conn.execute(
        r#"
        INSERT INTO feature_cache (
            exact_hash, size_bytes, akaze_status, akaze_keypoints, akaze_descriptors, akaze_points, feature_version, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, datetime('now'))
        ON CONFLICT(exact_hash, size_bytes) DO UPDATE SET
            akaze_status = excluded.akaze_status,
            akaze_keypoints = excluded.akaze_keypoints,
            akaze_descriptors = excluded.akaze_descriptors,
            akaze_points = excluded.akaze_points,
            feature_version = excluded.feature_version,
            updated_at = excluded.updated_at
        "#,
        params![
            &visual.exact_hash,
            size_bytes,
            visual.akaze_status.as_db_str(),
            keypoints,
            descriptors,
            points,
            FEATURE_VERSION,
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_catalog_db;
    use crate::features::exact_hash;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn mock_fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data/source_mock")
            .join(name)
    }

    #[test]
    fn loader_reuses_db_cache_for_same_hash_and_size() {
        let tmp = tempdir().unwrap();
        let image_path = tmp.path().join("a.jpg");
        fs::copy(mock_fixture_path("img_2023_05_01.jpg"), &image_path).unwrap();
        let bytes = fs::read(&image_path).unwrap();
        let exact_hash = exact_hash(&bytes);
        let size_bytes = i64::try_from(bytes.len()).unwrap();

        let catalog = open_catalog_db(tmp.path().join("catalog.db")).unwrap();
        let mut loader = FeatureLoader::default();
        let first = loader
            .load(
                &catalog,
                FeatureRequest {
                    path: &image_path,
                    mime_type: "image/jpeg",
                    exact_hash: &exact_hash,
                    size_bytes,
                    phash_hint: "",
                    phash_bits: 0,
                    width: 0,
                    height: 0,
                },
            )
            .unwrap();
        assert!(!first.phash.is_empty());

        let missing_path = tmp.path().join("missing-but-cacheable.jpg");
        let mut fresh_loader = FeatureLoader::default();
        let second = fresh_loader
            .load(
                &catalog,
                FeatureRequest {
                    path: &missing_path,
                    mime_type: "image/jpeg",
                    exact_hash: &exact_hash,
                    size_bytes,
                    phash_hint: &first.phash,
                    phash_bits: first.phash_bits,
                    width: first.width,
                    height: first.height,
                },
            )
            .unwrap();
        assert_eq!(second.phash, first.phash);
        assert_eq!(second.akaze_status, first.akaze_status);
        assert_eq!(second.akaze_points, first.akaze_points);
        assert_eq!(second.akaze_descriptors, first.akaze_descriptors);

        let count: i64 = catalog
            .query_row("SELECT COUNT(*) FROM feature_cache", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn loader_skips_non_visual_files_without_reading() {
        let tmp = tempdir().unwrap();
        let catalog = open_catalog_db(tmp.path().join("catalog.db")).unwrap();
        let mut loader = FeatureLoader::default();
        let features = loader
            .load(
                &catalog,
                FeatureRequest {
                    path: &tmp.path().join("missing.mp4"),
                    mime_type: "video/mp4",
                    exact_hash: "hash",
                    size_bytes: 123,
                    phash_hint: "",
                    phash_bits: 0,
                    width: 0,
                    height: 0,
                },
            )
            .unwrap();
        assert!(features.phash.is_empty());
        assert_eq!(features.akaze_status, AkazeStatus::Unavailable);
        assert!(features.akaze_descriptors.is_none());

        let count: i64 = catalog
            .query_row("SELECT COUNT(*) FROM feature_cache", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn loader_persists_too_small_akaze_status() {
        let tmp = tempdir().unwrap();
        let image_path = tmp.path().join("tiny.png");
        ::image::RgbaImage::from_pixel(20, 20, ::image::Rgba([255, 255, 255, 255]))
            .save(&image_path)
            .unwrap();
        let bytes = fs::read(&image_path).unwrap();
        let exact_hash = exact_hash(&bytes);
        let size_bytes = i64::try_from(bytes.len()).unwrap();

        let catalog = open_catalog_db(tmp.path().join("catalog.db")).unwrap();
        let mut loader = FeatureLoader::default();
        let features = loader
            .load(
                &catalog,
                FeatureRequest {
                    path: &image_path,
                    mime_type: "image/png",
                    exact_hash: &exact_hash,
                    size_bytes,
                    phash_hint: "",
                    phash_bits: 0,
                    width: 0,
                    height: 0,
                },
            )
            .unwrap();
        assert_eq!(features.akaze_status, AkazeStatus::TooSmall);
        assert!(features.akaze_descriptors.is_none());

        let stored_status: String = catalog
            .query_row(
                "SELECT akaze_status FROM feature_cache WHERE exact_hash = ?1 AND size_bytes = ?2",
                params![exact_hash, size_bytes],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_status, "too_small");
    }

    #[test]
    fn loader_retries_decode_error_cache_rows() {
        let tmp = tempdir().unwrap();
        let image_path = tmp.path().join("retry.jpg");
        fs::copy(mock_fixture_path("img_2023_05_01.jpg"), &image_path).unwrap();
        let bytes = fs::read(&image_path).unwrap();
        let exact_hash = exact_hash(&bytes);
        let size_bytes = i64::try_from(bytes.len()).unwrap();

        let catalog = open_catalog_db(tmp.path().join("catalog.db")).unwrap();
        catalog
            .execute(
                "INSERT INTO feature_cache (exact_hash, size_bytes, akaze_status, akaze_keypoints, akaze_descriptors, feature_version, updated_at)
                 VALUES (?1, ?2, 'decode_error', NULL, NULL, ?3, datetime('now'))",
                params![exact_hash, size_bytes, FEATURE_VERSION],
            )
            .unwrap();

        assert!(!has_reusable_feature_cache(&catalog, &exact_hash, size_bytes).unwrap());

        let mut loader = FeatureLoader::default();
        let features = loader
            .load(
                &catalog,
                FeatureRequest {
                    path: &image_path,
                    mime_type: "image/jpeg",
                    exact_hash: &exact_hash,
                    size_bytes,
                    phash_hint: "",
                    phash_bits: 0,
                    width: 0,
                    height: 0,
                },
            )
            .unwrap();
        assert_ne!(features.akaze_status, AkazeStatus::DecodeError);

        let stored_status: String = catalog
            .query_row(
                "SELECT akaze_status FROM feature_cache WHERE exact_hash = ?1 AND size_bytes = ?2",
                params![exact_hash, size_bytes],
                |row| row.get(0),
            )
            .unwrap();
        assert_ne!(stored_status, "decode_error");
    }

    #[test]
    fn loader_recomputes_stale_feature_version_rows() {
        let tmp = tempdir().unwrap();
        let image_path = tmp.path().join("stale.jpg");
        fs::copy(mock_fixture_path("img_2023_05_01.jpg"), &image_path).unwrap();
        let bytes = fs::read(&image_path).unwrap();
        let exact_hash = exact_hash(&bytes);
        let size_bytes = i64::try_from(bytes.len()).unwrap();

        let catalog = open_catalog_db(tmp.path().join("catalog.db")).unwrap();
        catalog
            .execute(
                "INSERT INTO feature_cache (exact_hash, size_bytes, akaze_status, akaze_keypoints, akaze_descriptors, feature_version, updated_at)
                 VALUES (?1, ?2, 'no_keypoints', NULL, NULL, ?3, datetime('now'))",
                params![exact_hash, size_bytes, FEATURE_VERSION - 1],
            )
            .unwrap();

        assert!(!has_reusable_feature_cache(&catalog, &exact_hash, size_bytes).unwrap());

        let mut loader = FeatureLoader::default();
        let features = loader
            .load(
                &catalog,
                FeatureRequest {
                    path: &image_path,
                    mime_type: "image/jpeg",
                    exact_hash: &exact_hash,
                    size_bytes,
                    phash_hint: "",
                    phash_bits: 0,
                    width: 0,
                    height: 0,
                },
            )
            .unwrap();
        assert_eq!(features.size_bytes_hint, size_bytes);

        let stored_row: (String, i64) = catalog
            .query_row(
                "SELECT akaze_status, feature_version FROM feature_cache WHERE exact_hash = ?1 AND size_bytes = ?2",
                params![exact_hash, size_bytes],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored_row.1, FEATURE_VERSION);
        assert!(has_reusable_feature_cache(&catalog, &exact_hash, size_bytes).unwrap());
    }

    #[test]
    fn loader_persists_read_failures_as_retryable_decode_error() {
        let tmp = tempdir().unwrap();
        let missing_path = tmp.path().join("missing.jpg");
        let catalog = open_catalog_db(tmp.path().join("catalog.db")).unwrap();
        let mut loader = FeatureLoader::default();

        let features = loader
            .load(
                &catalog,
                FeatureRequest {
                    path: &missing_path,
                    mime_type: "image/jpeg",
                    exact_hash: "missing-hash",
                    size_bytes: 321,
                    phash_hint: "",
                    phash_bits: 0,
                    width: 0,
                    height: 0,
                },
            )
            .unwrap();
        assert_eq!(features.akaze_status, AkazeStatus::DecodeError);
        assert!(!has_reusable_feature_cache(&catalog, "missing-hash", 321).unwrap());

        let stored_status: String = catalog
            .query_row(
                "SELECT akaze_status FROM feature_cache WHERE exact_hash = 'missing-hash' AND size_bytes = 321",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_status, "decode_error");
    }
}
