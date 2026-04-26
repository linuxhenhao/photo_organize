use crate::db::FEATURE_VERSION;
use crate::features::{
    VisualFeatures, compute_visual_features_for_mime, deserialize_akaze_descriptors,
    serialize_akaze_descriptors,
};
use anyhow::Result;
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

        let computed = compute_feature(request)?;
        save_feature_cache(conn, request.size_bytes, &computed)?;
        self.memo.insert(key, computed.clone());
        Ok(computed)
    }
}

fn compute_feature(request: FeatureRequest<'_>) -> Result<VisualFeatures> {
    if let Some(mut computed) = compute_visual_features_for_mime(request.path, request.mime_type)? {
        computed.exact_hash = request.exact_hash.to_string();
        return Ok(computed);
    }

    Ok(VisualFeatures {
        exact_hash: request.exact_hash.to_string(),
        phash: request.phash_hint.to_string(),
        phash_bits: request.phash_bits,
        width: request.width,
        height: request.height,
        akaze_keypoints: None,
        akaze_descriptors: None,
    })
}

fn load_cached_feature(
    conn: &Connection,
    key: &FeatureCacheKey,
    request: FeatureRequest<'_>,
) -> Result<Option<VisualFeatures>> {
    let row = conn
        .query_row(
            r#"
        SELECT akaze_keypoints, akaze_descriptors
        FROM feature_cache
        WHERE exact_hash = ?1 AND size_bytes = ?2 AND feature_version = ?3
        "#,
            params![key.exact_hash, key.size_bytes, FEATURE_VERSION],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                ))
            },
        )
        .optional()?;
    let Some((keypoints, descriptors_blob)) = row else {
        return Ok(None);
    };
    let descriptors = descriptors_blob
        .as_deref()
        .map(deserialize_akaze_descriptors)
        .transpose()?;
    Ok(Some(VisualFeatures {
        exact_hash: request.exact_hash.to_string(),
        phash: request.phash_hint.to_string(),
        phash_bits: request.phash_bits,
        width: request.width,
        height: request.height,
        akaze_keypoints: keypoints.and_then(|value| usize::try_from(value).ok()),
        akaze_descriptors: descriptors,
    }))
}

fn save_feature_cache(conn: &Connection, size_bytes: i64, visual: &VisualFeatures) -> Result<()> {
    let descriptors = visual
        .akaze_descriptors
        .as_ref()
        .map(|value| serialize_akaze_descriptors(value))
        .transpose()?;
    let keypoints = visual
        .akaze_keypoints
        .and_then(|value| i64::try_from(value).ok());
    conn.execute(
        r#"
        INSERT INTO feature_cache (
            exact_hash, size_bytes, akaze_keypoints, akaze_descriptors, feature_version, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))
        ON CONFLICT(exact_hash, size_bytes) DO UPDATE SET
            akaze_keypoints = excluded.akaze_keypoints,
            akaze_descriptors = excluded.akaze_descriptors,
            feature_version = excluded.feature_version,
            updated_at = excluded.updated_at
        "#,
        params![
            &visual.exact_hash,
            size_bytes,
            keypoints,
            descriptors,
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
            .join("../test_data/source_mock")
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
        assert_eq!(second.akaze_descriptors, first.akaze_descriptors);

        let count: i64 = catalog
            .query_row("SELECT COUNT(*) FROM feature_cache", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
