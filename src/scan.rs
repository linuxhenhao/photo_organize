use crate::db::{now_string, open_scan_db};
use crate::features::{
    VisualFeatures, compute_base_features_from_bytes, exact_hash, supports_visual_features,
};
use crate::interrupt;
use crate::util::{
    ProgressReporter, best_effort_mime, fallback_file_time, filename_date, is_excluded_dir,
    parse_exif_datetime, to_bytes_lossless,
};
use anyhow::{Context, Result};
use rayon::prelude::*;
use rusqlite::{Connection, Transaction, params};
use serde_json::json;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub(crate) struct DiscoveredFile {
    pub(crate) path: PathBuf,
    pub(crate) size_bytes: i64,
    pub(crate) mime_type: String,
    pub(crate) created_at: String,
    pub(crate) exact_hash: String,
    pub(crate) phash: String,
    pub(crate) phash_bits: i64,
    pub(crate) width: i64,
    pub(crate) height: i64,
    pub(crate) scan_status: String,
    pub(crate) last_scanned_at: String,
    pub(crate) meta_json: String,
}

pub fn run(scan_db: &Path, roots: &[PathBuf]) -> Result<()> {
    scan_with_db_path(scan_db, roots)
}

#[allow(dead_code)]
pub fn scan_with_conn(conn: &mut Connection, roots: &[PathBuf]) -> Result<()> {
    let items = collect_discovered_files(roots)?;
    interrupt::check()?;
    let run_id = now_string();
    let tx = conn.transaction()?;
    upsert_items(&tx, &items, &run_id)?;
    mark_missing_unseen(&tx, &run_id)?;
    tx.commit()?;
    tracing::info!(count = items.len(), "scan complete");
    Ok(())
}

fn scan_with_db_path(scan_db: &Path, roots: &[PathBuf]) -> Result<()> {
    let files = collect_file_paths(roots)?;
    let total = files.len();
    let run_id = now_string();
    let progress = ProgressReporter::new("scan discover", total);
    progress.log_start();

    let (tx_chan, rx_chan) = mpsc::sync_channel::<DiscoveredFile>(100);
    let db_path = scan_db.to_path_buf();
    let run_id_for_writer = run_id.clone();
    let consumer = std::thread::spawn(move || -> Result<usize> {
        let mut conn = open_scan_db(&db_path)?;
        let mut buffer = Vec::with_capacity(100);
        let mut count = 0;

        while let Ok(item) = rx_chan.recv() {
            buffer.push(item);
            if buffer.len() >= 100 {
                write_scan_batch(&mut conn, &buffer, &run_id_for_writer)?;
                count += buffer.len();
                buffer.clear();
            }
        }

        if !buffer.is_empty() {
            write_scan_batch(&mut conn, &buffer, &run_id_for_writer)?;
            count += buffer.len();
        }

        Ok(count)
    });

    files.par_iter().for_each(|path| {
        if interrupt::requested() {
            return;
        }

        let result = match discover_file(path) {
            Ok(mut item) => {
                item.last_scanned_at = run_id.clone();
                Some(item)
            }
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "scan failed");
                None
            }
        };

        if let Some(item) = result {
            let _ = tx_chan.send(item);
        }
        progress.item_done();
    });

    drop(tx_chan);
    let written = consumer.join().expect("consumer thread panicked")?;
    interrupt::check()?;

    let mut conn = open_scan_db(scan_db)?;
    let tx = conn.transaction()?;
    mark_missing_unseen(&tx, &run_id)?;
    tx.commit()?;
    tracing::info!(written, "scan complete");
    Ok(())
}

pub(crate) fn collect_file_paths(roots: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for root in roots {
        for entry in WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| e.depth() == 0 || !is_excluded_relative(root, e.path()))
        {
            interrupt::check()?;
            let entry = entry?;
            if !entry.file_type().is_file() || is_excluded_relative(root, entry.path()) {
                continue;
            }
            files.push(entry.path().to_path_buf());
        }
    }
    Ok(files)
}

#[allow(dead_code)]
pub(crate) fn collect_discovered_files(roots: &[PathBuf]) -> Result<Vec<DiscoveredFile>> {
    let files = collect_file_paths(roots)?;
    let progress = ProgressReporter::new("scan discover", files.len());
    progress.log_start();
    let items: Vec<DiscoveredFile> = files
        .par_iter()
        .filter_map(|path| {
            if interrupt::requested() {
                return None;
            }
            let result = match discover_file(path) {
                Ok(item) => Some(item),
                Err(err) => {
                    tracing::warn!(path = %path.display(), error = %err, "scan failed");
                    None
                }
            };
            progress.item_done();
            result
        })
        .collect();
    interrupt::check()?;
    Ok(items)
}

pub(crate) fn discover_file(path: &Path) -> Result<DiscoveredFile> {
    let meta = std::fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    let bytes = to_bytes_lossless(path)?;
    let now = now_string();
    let mime_type = best_effort_mime(path, &bytes);
    let created_at =
        metadata_time(path, &meta, &bytes).unwrap_or_else(|| fallback_file_time(&meta));
    let exact_hash = exact_hash(&bytes);
    let visual = if supports_visual_features(path, &mime_type) {
        compute_base_features_from_bytes(&bytes, path, &mime_type)?.unwrap_or(VisualFeatures {
            exact_hash: String::new(),
            phash: String::new(),
            phash_bits: 0,
            phash_value: 0,
            width: 0,
            height: 0,
            size_bytes_hint: 0,
            akaze_keypoints: None,
            akaze_descriptors: None,
        })
    } else {
        VisualFeatures {
            exact_hash: String::new(),
            phash: String::new(),
            phash_bits: 0,
            phash_value: 0,
            width: 0,
            height: 0,
            size_bytes_hint: 0,
            akaze_keypoints: None,
            akaze_descriptors: None,
        }
    };
    let meta_json = json!({
        "fingerprint": {
            "size_bytes": meta.len(),
            "modified_at": meta.modified().ok().map(crate::util::system_time_to_rfc3339)
        }
    })
    .to_string();

    Ok(DiscoveredFile {
        path: path.to_path_buf(),
        size_bytes: i64::try_from(meta.len()).unwrap_or(i64::MAX),
        mime_type,
        created_at,
        exact_hash,
        phash: visual.phash,
        phash_bits: visual.phash_bits,
        width: visual.width,
        height: visual.height,
        scan_status: "present".to_string(),
        last_scanned_at: now,
        meta_json,
    })
}

fn metadata_time(path: &Path, meta: &std::fs::Metadata, bytes: &[u8]) -> Option<String> {
    let mut cursor = Cursor::new(bytes);
    if let Ok(exif) = exif::Reader::new().read_from_container(&mut cursor) {
        for tag in [
            exif::Tag::DateTimeOriginal,
            exif::Tag::DateTimeDigitized,
            exif::Tag::DateTime,
        ] {
            if let Some(field) = exif.get_field(tag, exif::In::PRIMARY) {
                if let exif::Value::Ascii(values) = &field.value {
                    if let Some(value) = values.first().and_then(|v| std::str::from_utf8(v).ok()) {
                        if let Some(ts) = parse_exif_datetime(value) {
                            return Some(ts);
                        }
                    }
                }
            }
        }
    }
    filename_date(path)
        .or_else(|| meta.created().ok().map(crate::util::system_time_to_rfc3339))
        .or_else(|| {
            meta.modified()
                .ok()
                .map(crate::util::system_time_to_rfc3339)
        })
}

fn upsert_items(tx: &Transaction<'_>, items: &[DiscoveredFile], run_id: &str) -> Result<()> {
    let mut stmt = tx.prepare(
        r#"
        INSERT INTO source_items (
            source_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
            width, height, scan_status, last_scanned_at, meta_json
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12
        )
        ON CONFLICT(source_path) DO UPDATE SET
            size_bytes = excluded.size_bytes,
            mime_type = excluded.mime_type,
            created_at = excluded.created_at,
            exact_hash = excluded.exact_hash,
            phash = excluded.phash,
            phash_bits = excluded.phash_bits,
            width = excluded.width,
            height = excluded.height,
            scan_status = excluded.scan_status,
            last_scanned_at = excluded.last_scanned_at,
            meta_json = excluded.meta_json
        "#,
    )?;

    for item in items {
        stmt.execute(params![
            item.path.to_string_lossy(),
            item.size_bytes,
            item.mime_type,
            item.created_at,
            item.exact_hash,
            item.phash,
            item.phash_bits,
            item.width,
            item.height,
            item.scan_status,
            run_id,
            item.meta_json,
        ])?;
    }
    Ok(())
}

fn mark_missing_unseen(tx: &Transaction<'_>, run_id: &str) -> Result<()> {
    tx.execute(
        "UPDATE source_items SET scan_status = 'missing' WHERE last_scanned_at <> ?1",
        params![run_id],
    )?;
    Ok(())
}

fn write_scan_batch(conn: &mut Connection, batch: &[DiscoveredFile], run_id: &str) -> Result<()> {
    let tx = conn.transaction()?;
    upsert_items(&tx, batch, run_id)?;
    tx.commit()?;
    Ok(())
}

fn is_excluded_relative(root: &Path, path: &Path) -> bool {
    let relative = path.strip_prefix(root).unwrap_or(path);
    is_excluded_dir(relative)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_scan_db;
    use crate::features::exact_hash;
    use crate::interrupt;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data/source_mock")
            .join(name)
    }

    fn copy_fixture(name: &str, path: &Path) {
        fs::copy(fixture_path(name), path).unwrap();
    }

    #[test]
    fn scan_populates_hashes_and_marks_missing() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir_all(&src).unwrap();
        let a = src.join("2024-06-09_a.png");
        let b = src.join("2024-06-10_b.png");
        copy_fixture("img_2023_05_01.jpg", &a);
        copy_fixture("img_2023_05_02.jpg", &b);

        let scan_db = tmp.path().join("scan.db");
        run(&scan_db, &[src.clone()]).unwrap();

        let conn = open_scan_db(&scan_db).unwrap();
        let present: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM source_items WHERE scan_status = 'present'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(present, 2);
        let phash: String = conn
            .query_row(
                "SELECT phash FROM source_items WHERE source_path = ?1",
                [a.to_string_lossy().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!phash.is_empty());

        fs::remove_file(&b).unwrap();
        run(&scan_db, &[src]).unwrap();
        let missing: String = conn
            .query_row(
                "SELECT scan_status FROM source_items WHERE source_path = ?1",
                [b.to_string_lossy().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(missing, "missing");
    }

    #[test]
    fn scan_leaves_non_images_without_visual_hashes() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir_all(&src).unwrap();
        let video = src.join("clip.mp4");
        copy_fixture("clip_2023_05_06.mp4", &video);
        let bytes = fs::read(&video).unwrap();
        let expected_hash = exact_hash(&bytes);
        let expected_size = i64::try_from(bytes.len()).unwrap();

        let scan_db = tmp.path().join("scan.db");
        run(&scan_db, &[src]).unwrap();

        let conn = open_scan_db(&scan_db).unwrap();
        let (size_bytes, exact_hash, phash, phash_bits): (i64, String, String, i64) = conn
            .query_row(
                "SELECT size_bytes, exact_hash, phash, phash_bits FROM source_items WHERE source_path = ?1",
                [video.to_string_lossy().to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(size_bytes, expected_size);
        assert_eq!(exact_hash, expected_hash);
        assert!(phash.is_empty());
        assert_eq!(phash_bits, 0);
    }

    #[test]
    fn interrupted_scan_does_not_mark_unseen_rows_missing() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir_all(&src).unwrap();
        let a = src.join("2024-06-09_a.png");
        let b = src.join("2024-06-10_b.png");
        copy_fixture("img_2023_05_01.jpg", &a);
        copy_fixture("img_2023_05_02.jpg", &b);

        let scan_db = tmp.path().join("scan.db");
        run(&scan_db, &[src.clone()]).unwrap();

        fs::remove_file(&b).unwrap();
        interrupt::request_for_test();
        let result = run(&scan_db, &[src]);
        interrupt::reset();
        assert!(result.is_err());

        let conn = open_scan_db(&scan_db).unwrap();
        let status: String = conn
            .query_row(
                "SELECT scan_status FROM source_items WHERE source_path = ?1",
                [b.to_string_lossy().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "present");
    }
}
