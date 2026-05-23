use crate::db::FEATURE_VERSION;
use crate::features::{
    AkazeStatus, MIN_AKAZE_DESCRIPTORS_FOR_MATCH, VisualFeatures,
    compute_visual_features_for_mime_from_bytes, deserialize_akaze_descriptors,
    deserialize_akaze_points, phash_to_u64, serialize_akaze_descriptors, serialize_akaze_points,
    supports_visual_features,
};
use anyhow::{Context, Result};
use lru::LruCache;
use rusqlite::{Connection, OptionalExtension, params};
#[cfg(test)]
use std::mem::size_of;
use std::num::NonZeroUsize;
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

// Committed fixture measurements put ready-feature payloads at roughly
// 0.7-6.8 KiB as a lower-bound heap estimate. A synthetic RSS probe of the
// current LruCache<FeatureCacheKey, VisualFeatures> shape with a representative
// "large ready feature" landed at about 7.9 KiB per cached entry including
// cache/key overhead, so 4096 entries is about 31.9 MiB of cache usage.
const FEATURE_LOADER_MEMO_CAPACITY: usize = 4096;

#[derive(Debug)]
pub struct FeatureLoader {
    memo: LruCache<FeatureCacheKey, VisualFeatures>,
}

impl Default for FeatureLoader {
    fn default() -> Self {
        Self {
            memo: LruCache::new(
                NonZeroUsize::new(FEATURE_LOADER_MEMO_CAPACITY)
                    .expect("feature loader memo capacity must be non-zero"),
            ),
        }
    }
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
            self.maybe_memoize(key, &fallback);
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
            self.maybe_memoize(key, &cached);
            return Ok(cached);
        }

        let mut computed = compute_feature(request)?;
        computed.size_bytes_hint = request.size_bytes;
        save_feature_cache(conn, request.size_bytes, &computed)?;
        self.maybe_memoize(key, &computed);
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

    fn maybe_memoize(&mut self, key: FeatureCacheKey, features: &VisualFeatures) {
        self.memo.put(key, features.clone());
    }

    #[cfg(test)]
    fn memo_len(&self) -> usize {
        self.memo.len()
    }

    #[cfg(test)]
    fn memo_contains(&self, exact_hash: &str, size_bytes: i64) -> bool {
        self.memo.contains(&FeatureCacheKey {
            exact_hash: exact_hash.to_string(),
            size_bytes,
        })
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

#[cfg(test)]
fn estimated_feature_bytes(features: &VisualFeatures) -> usize {
    let mut total = size_of::<VisualFeatures>();
    total += features.exact_hash.capacity();
    total += features.phash.capacity();

    if let Some(points) = features.akaze_points.as_ref() {
        total += points.capacity() * size_of::<crate::features::AkazePoint>();
    }

    if let Some(descriptors) = features.akaze_descriptors.as_ref() {
        total += descriptors.capacity() * size_of::<Vec<u8>>();
        total += descriptors
            .iter()
            .map(|desc| desc.capacity())
            .sum::<usize>();
    }

    total
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
    let keypoints = keypoints.and_then(|value| usize::try_from(value).ok());
    let descriptors = descriptors_blob
        .as_deref()
        .map(deserialize_akaze_descriptors)
        .transpose()?;
    let points = points_blob
        .as_deref()
        .map(deserialize_akaze_points)
        .transpose()?;
    if status == AkazeStatus::Ready
        && (descriptors.is_none()
            || points.is_none()
            || !ready_feature_is_reusable(keypoints, descriptors.as_deref(), points.as_deref()))
    {
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
        akaze_keypoints: keypoints,
        akaze_points: points,
        akaze_descriptors: descriptors,
    }))
}

fn ready_feature_is_reusable(
    keypoints: Option<usize>,
    descriptors: Option<&[Vec<u8>]>,
    points: Option<&[crate::features::AkazePoint]>,
) -> bool {
    let descriptor_count = descriptors.map_or(0, <[Vec<u8>]>::len);
    let point_count = points.map_or(0, <[crate::features::AkazePoint]>::len);
    if descriptor_count == 0 || point_count == 0 || descriptor_count != point_count {
        return false;
    }

    let effective_count = keypoints.unwrap_or(descriptor_count).min(descriptor_count);
    effective_count >= MIN_AKAZE_DESCRIPTORS_FOR_MATCH
}

pub fn has_reusable_feature_cache(
    conn: &Connection,
    exact_hash: &str,
    size_bytes: i64,
) -> Result<bool> {
    let row = conn
        .query_row(
            r#"
            SELECT akaze_status, akaze_keypoints
            FROM feature_cache
            WHERE exact_hash = ?1 AND size_bytes = ?2 AND feature_version = ?3
            "#,
            params![exact_hash, size_bytes, FEATURE_VERSION],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
        )
        .optional()?;
    let Some((status_text, keypoints)) = row else {
        return Ok(false);
    };
    let status = AkazeStatus::from_db_str(&status_text)
        .with_context(|| format!("unknown akaze status {status_text}"))?;
    if status.is_retryable() {
        return Ok(false);
    }
    if status == AkazeStatus::Ready
        && keypoints
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(0)
            < MIN_AKAZE_DESCRIPTORS_FOR_MATCH
    {
        return Ok(false);
    }
    Ok(true)
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

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
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
    fn loader_memoizes_ready_features() {
        let tmp = tempdir().unwrap();
        let catalog = open_catalog_db(tmp.path().join("catalog.db")).unwrap();
        let exact_hash = "ready-hash";
        let size_bytes = 123_i64;
        let expected = VisualFeatures {
            exact_hash: exact_hash.to_string(),
            phash: "AAAAAAAAAAA=".to_string(),
            phash_bits: 64,
            phash_value: 0,
            width: 640,
            height: 480,
            size_bytes_hint: size_bytes,
            akaze_status: AkazeStatus::Ready,
            akaze_keypoints: Some(31),
            akaze_points: Some(vec![crate::features::AkazePoint { x: 1.0, y: 2.0 }; 31]),
            akaze_descriptors: Some(vec![vec![1, 2, 3]; 31]),
        };
        save_feature_cache(&catalog, size_bytes, &expected).unwrap();

        let mut loader = FeatureLoader::default();
        let features = loader
            .load(
                &catalog,
                FeatureRequest {
                    path: &tmp.path().join("missing-but-cacheable.jpg"),
                    mime_type: "image/jpeg",
                    exact_hash,
                    size_bytes,
                    phash_hint: &expected.phash,
                    phash_bits: expected.phash_bits,
                    width: expected.width,
                    height: expected.height,
                },
            )
            .unwrap();

        assert_eq!(features.akaze_status, AkazeStatus::Ready);
        assert_eq!(features.akaze_points, expected.akaze_points);
        assert_eq!(features.akaze_descriptors, expected.akaze_descriptors);
        assert_eq!(loader.memo_len(), 1);
        assert!(loader.memo_contains(exact_hash, size_bytes));
    }

    #[test]
    fn loader_evicts_oldest_ready_features_when_capacity_is_exceeded() {
        let tmp = tempdir().unwrap();
        let catalog = open_catalog_db(tmp.path().join("catalog.db")).unwrap();
        let mut loader = FeatureLoader::default();

        for index in 0..=FEATURE_LOADER_MEMO_CAPACITY {
            let exact_hash = format!("ready-hash-{index}");
            let size_bytes = 1000 + index as i64;
            let expected = VisualFeatures {
                exact_hash: exact_hash.clone(),
                phash: "AAAAAAAAAAA=".to_string(),
                phash_bits: 64,
                phash_value: 0,
                width: 640,
                height: 480,
                size_bytes_hint: size_bytes,
                akaze_status: AkazeStatus::Ready,
                akaze_keypoints: Some(31),
                akaze_points: Some(vec![crate::features::AkazePoint { x: 1.0, y: 2.0 }; 31]),
                akaze_descriptors: Some(vec![vec![1, 2, 3]; 31]),
            };
            save_feature_cache(&catalog, size_bytes, &expected).unwrap();

            let features = loader
                .load(
                    &catalog,
                    FeatureRequest {
                        path: &tmp.path().join(format!("cached-{index}.jpg")),
                        mime_type: "image/jpeg",
                        exact_hash: &exact_hash,
                        size_bytes,
                        phash_hint: &expected.phash,
                        phash_bits: expected.phash_bits,
                        width: expected.width,
                        height: expected.height,
                    },
                )
                .unwrap();
            assert_eq!(features.akaze_status, AkazeStatus::Ready);
        }

        assert_eq!(loader.memo_len(), FEATURE_LOADER_MEMO_CAPACITY);
        assert!(!loader.memo_contains("ready-hash-0", 1000));
        assert!(loader.memo_contains(
            &format!("ready-hash-{}", FEATURE_LOADER_MEMO_CAPACITY),
            1000 + FEATURE_LOADER_MEMO_CAPACITY as i64
        ));
    }

    #[test]
    #[ignore = "diagnostic measurement for feature cache sizing"]
    fn report_feature_memory_estimates() {
        let samples = [
            ("source_mock/img_2023_05_01.jpg", "image/jpeg"),
            ("source_mock/img_2023_05_02.jpg", "image/jpeg"),
            ("source/DSC00903.ARW", "image/x-sony-arw"),
            ("source/IMG_5798.CR2", "image/x-canon-cr2"),
            ("problematic_images/IMG_5887.JPG", "image/jpeg"),
        ];

        for (rel, mime) in samples {
            let path = fixture_path(rel);
            let bytes = fs::read(&path).unwrap();
            let exact_hash = exact_hash(&bytes);
            let size_bytes = i64::try_from(bytes.len()).unwrap();
            let mut loader = FeatureLoader::default();
            let catalog = open_catalog_db(tempdir().unwrap().path().join("catalog.db")).unwrap();
            let features = loader
                .load(
                    &catalog,
                    FeatureRequest {
                        path: &path,
                        mime_type: mime,
                        exact_hash: &exact_hash,
                        size_bytes,
                        phash_hint: "",
                        phash_bits: 0,
                        width: 0,
                        height: 0,
                    },
                )
                .unwrap();
            eprintln!(
                "{} status={:?} keypoints={:?} descriptors={} estimated_bytes={}",
                rel,
                features.akaze_status,
                features.akaze_keypoints,
                features.akaze_descriptors.as_ref().map_or(0, Vec::len),
                estimated_feature_bytes(&features)
            );
        }
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
        assert_eq!(loader.memo_len(), 1);

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
    fn loader_recomputes_low_keypoint_ready_cache_rows() {
        let tmp = tempdir().unwrap();
        let image_path = tmp.path().join("2013-02-05-14.59.19-anon-default.jpg");
        fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("docs")
                .join("group_bad_cases")
                .join("sparse-default-retry")
                .join("assets")
                .join("2013-02-05-14.59.19-anon-default.jpg"),
            &image_path,
        )
        .unwrap();
        let bytes = fs::read(&image_path).unwrap();
        let exact_hash = exact_hash(&bytes);
        let size_bytes = i64::try_from(bytes.len()).unwrap();

        let catalog = open_catalog_db(tmp.path().join("catalog.db")).unwrap();
        catalog
            .execute(
                "INSERT INTO feature_cache (exact_hash, size_bytes, akaze_status, akaze_keypoints, akaze_descriptors, akaze_points, feature_version, updated_at)
                 VALUES (?1, ?2, 'ready', ?3, ?4, ?5, ?6, datetime('now'))",
                params![
                    exact_hash,
                    size_bytes,
                    2_i64,
                    serialize_akaze_descriptors(&vec![vec![0u8; 64]; 2]).unwrap(),
                    serialize_akaze_points(&vec![crate::features::AkazePoint { x: 1.0, y: 2.0 }; 2]).unwrap(),
                    FEATURE_VERSION
                ],
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
        assert!(
            features.akaze_status == AkazeStatus::NoKeypoints
                || (features.akaze_status == AkazeStatus::Ready
                    && features.akaze_keypoints.unwrap_or(0) >= MIN_AKAZE_DESCRIPTORS_FOR_MATCH)
        );

        let stored_row: (String, Option<i64>) = catalog
            .query_row(
                "SELECT akaze_status, akaze_keypoints FROM feature_cache WHERE exact_hash = ?1 AND size_bytes = ?2",
                params![exact_hash, size_bytes],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(
            stored_row.0 == "no_keypoints"
                || (stored_row.0 == "ready"
                    && stored_row.1.unwrap_or(0) >= MIN_AKAZE_DESCRIPTORS_FOR_MATCH as i64)
        );
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
