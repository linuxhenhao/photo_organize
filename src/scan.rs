use crate::db::{FEATURE_VERSION, now_string, open_scan_db};
use crate::feature_loader::{has_reusable_feature_cache, save_feature_cache};
use crate::features::{
    AkazeStatus, VisualFeatures, compute_base_features_from_bytes,
    compute_base_features_from_reader, compute_visual_features_for_mime_from_bytes,
    is_raw_like_mime, supports_visual_features,
};
use crate::interrupt;
use crate::util::{
    ProgressReporter, best_effort_mime, fallback_file_time, filename_date, is_excluded_dir,
    parse_exif_datetime, parse_video_container_datetime,
};
use anyhow::{Context, Result};
use blake3::Hasher as Blake3Hasher;
use rayon::prelude::*;
use rusqlite::{Connection, Transaction, params};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Instant;
use walkdir::WalkDir;

const MIME_PREFIX_BYTES: usize = 3072;
const HASH_BUFFER_BYTES: usize = 64 * 1024;

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
    pub(crate) visual_features: Option<VisualFeatures>,
}

#[derive(Debug, Clone)]
struct ExistingScanItem {
    phash: String,
    phash_bits: i64,
    width: i64,
    height: i64,
}

#[derive(Debug, Default)]
struct ScanReuse {
    items_by_hash: HashMap<(String, i64), ExistingScanItem>,
    reusable_features: HashSet<(String, i64)>,
}

pub fn run(scan_db: &Path, roots: &[PathBuf]) -> Result<()> {
    scan_with_db_path(scan_db, roots)
}

fn scan_with_db_path(scan_db: &Path, roots: &[PathBuf]) -> Result<()> {
    let files = collect_file_paths(roots)?;
    let total = files.len();
    let run_id = now_string();
    let reuse = {
        let conn = open_scan_db(scan_db)?;
        load_scan_reuse(&conn)?
    };
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

        let file_started = Instant::now();
        tracing::info!(path = %path.display(), "scan file start");
        let result = match discover_file_for_scan(path, &reuse) {
            Ok(mut item) => {
                item.last_scanned_at = run_id.clone();
                Some(item)
            }
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "scan failed");
                None
            }
        };
        tracing::info!(
            path = %path.display(),
            ok = result.is_some(),
            elapsed_ms = file_started.elapsed().as_millis(),
            "scan file done"
        );

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

pub(crate) fn discover_file(path: &Path) -> Result<DiscoveredFile> {
    discover_file_inner(path, false, None)
}

fn discover_file_for_scan(path: &Path, reuse: &ScanReuse) -> Result<DiscoveredFile> {
    discover_file_inner(path, true, Some(reuse))
}

fn discover_file_inner(
    path: &Path,
    compute_akaze: bool,
    reuse: Option<&ScanReuse>,
) -> Result<DiscoveredFile> {
    let meta = std::fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let now = now_string();
    let sniff_bytes = read_prefix(&mut file, MIME_PREFIX_BYTES)?;
    let mime_type = best_effort_mime(path, &sniff_bytes);
    let created_at = metadata_time(path, &mime_type, &meta, &mut file)
        .unwrap_or_else(|| fallback_file_time(&meta));
    let exact_hash = exact_hash_file(&mut file)?;
    let size_bytes = i64::try_from(meta.len()).unwrap_or(i64::MAX);
    let visual_supported = supports_visual_features(path, &mime_type);
    let cache_key = (exact_hash.clone(), size_bytes);
    let reusable_akaze = reuse
        .map(|state| state.reusable_features.contains(&cache_key))
        .unwrap_or(false);
    let prior_item = reuse.and_then(|state| state.items_by_hash.get(&cache_key));
    let (visual, persist_features) = if reusable_akaze {
        let visual = if let Some(prev) = prior_item.filter(|item| !item.phash.is_empty()) {
            VisualFeatures {
                exact_hash: exact_hash.clone(),
                phash: prev.phash.clone(),
                phash_bits: prev.phash_bits,
                phash_value: 0,
                width: prev.width,
                height: prev.height,
                size_bytes_hint: size_bytes,
                akaze_status: AkazeStatus::Pending,
                akaze_keypoints: None,
                akaze_points: None,
                akaze_descriptors: None,
            }
        } else {
            compute_scan_base_features(&mut file, path, &mime_type)?
        };
        (visual, false)
    } else if compute_akaze && visual_supported {
        (
            compute_scan_akaze_features(&mut file, path, &mime_type, &exact_hash, size_bytes)?,
            true,
        )
    } else if visual_supported {
        (compute_scan_base_features(&mut file, path, &mime_type)?, false)
    } else {
        (empty_visual_features(), false)
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
        size_bytes,
        mime_type,
        created_at,
        exact_hash,
        phash: visual.phash.clone(),
        phash_bits: visual.phash_bits,
        width: visual.width,
        height: visual.height,
        scan_status: "present".to_string(),
        last_scanned_at: now,
        meta_json,
        visual_features: persist_features.then_some(visual),
    })
}

fn compute_scan_base_features(
    file: &mut File,
    path: &Path,
    mime_type: &str,
) -> Result<VisualFeatures> {
    if is_raw_like_mime(mime_type) {
        let bytes = read_all_bytes(file, path)?;
        return Ok(
            compute_base_features_from_bytes(&bytes, path, mime_type)?
                .unwrap_or_else(empty_visual_features),
        );
    }

    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind {}", path.display()))?;
    match compute_base_features_from_reader(BufReader::new(&mut *file), path) {
        Ok(features) => Ok(features),
        Err(err) => {
            tracing::warn!(path = %path.display(), error = %err, "base feature extraction failed");
            Ok(empty_visual_features())
        }
    }
}

fn compute_scan_akaze_features(
    file: &mut File,
    path: &Path,
    mime_type: &str,
    exact_hash: &str,
    size_bytes: i64,
) -> Result<VisualFeatures> {
    let bytes = read_all_bytes(file, path)?;
    let mut features = match compute_visual_features_for_mime_from_bytes(&bytes, path, mime_type)? {
        Some(features) => features,
        None => {
            let mut fallback = empty_visual_features();
            fallback.akaze_status = AkazeStatus::DecodeError;
            fallback
        }
    };
    features.exact_hash = exact_hash.to_string();
    features.size_bytes_hint = size_bytes;
    Ok(features)
}

fn load_scan_reuse(conn: &Connection) -> Result<ScanReuse> {
    let mut items_by_hash = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT exact_hash, size_bytes, phash, phash_bits, width, height FROM source_items",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            ExistingScanItem {
                phash: row.get(2)?,
                phash_bits: row.get(3)?,
                width: row.get(4)?,
                height: row.get(5)?,
            },
        ))
    })?;
    for row in rows {
        let (exact_hash, size_bytes, item) = row?;
        if exact_hash.is_empty() {
            continue;
        }
        items_by_hash
            .entry((exact_hash, size_bytes))
            .and_modify(|prev: &mut ExistingScanItem| {
                if prev.phash.is_empty() && !item.phash.is_empty() {
                    *prev = item.clone();
                }
            })
            .or_insert(item);
    }

    let mut reusable_features = HashSet::new();
    let mut feat_stmt =
        conn.prepare("SELECT exact_hash, size_bytes FROM feature_cache WHERE feature_version = ?1")?;
    let feat_rows = feat_stmt.query_map(params![FEATURE_VERSION], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    for row in feat_rows {
        let (exact_hash, size_bytes) = row?;
        if has_reusable_feature_cache(conn, &exact_hash, size_bytes)? {
            reusable_features.insert((exact_hash, size_bytes));
        }
    }

    Ok(ScanReuse {
        items_by_hash,
        reusable_features,
    })
}

fn metadata_time(
    path: &Path,
    mime_type: &str,
    meta: &std::fs::Metadata,
    file: &mut File,
) -> Option<String> {
    if let Some(ts) = parse_video_container_datetime(file, mime_type) {
        return Some(ts);
    }
    let _ = file.seek(SeekFrom::Start(0));
    let mut reader = BufReader::new(file);
    if let Ok(exif) = exif::Reader::new().read_from_container(&mut reader) {
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

fn read_prefix(file: &mut File, limit: usize) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))
        .context("rewind file for prefix read")?;
    let mut buf = vec![0u8; limit];
    let count = file.read(&mut buf).context("read prefix bytes")?;
    buf.truncate(count);
    Ok(buf)
}

fn exact_hash_file(file: &mut File) -> Result<String> {
    file.seek(SeekFrom::Start(0))
        .context("rewind file for exact hash")?;
    let mut hasher = Blake3Hasher::new();
    let mut buffer = [0u8; HASH_BUFFER_BYTES];
    loop {
        let count = file
            .read(&mut buffer)
            .context("stream file for exact hash")?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn read_all_bytes(file: &mut File, path: &Path) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind {}", path.display()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;
    Ok(bytes)
}

fn empty_visual_features() -> VisualFeatures {
    VisualFeatures {
        exact_hash: String::new(),
        phash: String::new(),
        phash_bits: 0,
        phash_value: 0,
        width: 0,
        height: 0,
        size_bytes_hint: 0,
        akaze_status: AkazeStatus::Pending,
        akaze_keypoints: None,
        akaze_points: None,
        akaze_descriptors: None,
    }
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
    for item in batch {
        if let Some(visual) = &item.visual_features {
            save_feature_cache(&tx, item.size_bytes, visual)?;
        }
    }
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
    use std::fs::File;
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
        let cached: i64 = conn
            .query_row("SELECT COUNT(*) FROM feature_cache", [], |row| row.get(0))
            .unwrap();
        assert_eq!(cached, 2);
        let ready: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM feature_cache WHERE akaze_status != 'pending'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ready, 2);

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
        let cached: i64 = conn
            .query_row("SELECT COUNT(*) FROM feature_cache", [], |row| row.get(0))
            .unwrap();
        assert_eq!(cached, 0);
    }

    #[test]
    fn discover_file_computes_phash_for_raw() {
        let arw = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data/source/DSC00903.ARW");
        let cr2 = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data/source/IMG_5798.CR2");
        for path in [&arw, &cr2] {
            let discovered = discover_file(path).unwrap();
            assert!(
                !discovered.phash.is_empty(),
                "expected pHash for {}",
                path.display()
            );
            assert!(discovered.width > 0);
            assert!(discovered.height > 0);
        }
    }

    #[test]
    fn scan_reuses_akaze_for_renamed_exact_copy() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir_all(&src).unwrap();
        let first = src.join("a.jpg");
        copy_fixture("img_2023_05_01.jpg", &first);

        let scan_db = tmp.path().join("scan.db");
        run(&scan_db, &[src.clone()]).unwrap();
        let conn = open_scan_db(&scan_db).unwrap();
        let cached_before: i64 = conn
            .query_row("SELECT COUNT(*) FROM feature_cache", [], |row| row.get(0))
            .unwrap();
        assert_eq!(cached_before, 1);
        let updated_before: String = conn
            .query_row("SELECT updated_at FROM feature_cache", [], |row| row.get(0))
            .unwrap();
        drop(conn);

        let second = src.join("renamed.jpg");
        fs::copy(&first, &second).unwrap();
        run(&scan_db, &[src]).unwrap();

        let conn = open_scan_db(&scan_db).unwrap();
        let cached_after: i64 = conn
            .query_row("SELECT COUNT(*) FROM feature_cache", [], |row| row.get(0))
            .unwrap();
        assert_eq!(cached_after, 1);
        let updated_after: String = conn
            .query_row("SELECT updated_at FROM feature_cache", [], |row| row.get(0))
            .unwrap();
        assert_eq!(updated_before, updated_after);
        let renamed_phash: String = conn
            .query_row(
                "SELECT phash FROM source_items WHERE source_path = ?1",
                [second.to_string_lossy().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!renamed_phash.is_empty());
    }

    #[test]
    fn exact_hash_file_matches_buffer_hash() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("clip.mp4");
        copy_fixture("clip_2023_05_06.mp4", &path);
        let bytes = fs::read(&path).unwrap();
        let expected = exact_hash(&bytes);

        let mut file = File::open(&path).unwrap();
        let actual = exact_hash_file(&mut file).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn metadata_time_prefers_video_container_timestamp() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("clip.mp4");
        fs::write(
            &path,
            minimal_mp4_with_mvhd_v0(quicktime_seconds_for("2025-01-02T03:04:56+00:00")),
        )
        .unwrap();

        let meta = fs::metadata(&path).unwrap();
        let mut file = File::open(&path).unwrap();

        let actual = metadata_time(&path, "video/mp4", &meta, &mut file);

        assert_eq!(actual.as_deref(), Some("2025-01-02T03:04:56+00:00"));
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
        let result = interrupt::run_with_requested_interrupt(|| run(&scan_db, &[src]));
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

    fn minimal_mp4_with_mvhd_v0(seconds: u64) -> Vec<u8> {
        let creation = u32::try_from(seconds).unwrap().to_be_bytes();
        let mut mvhd = Vec::new();
        mvhd.extend_from_slice(&16_u32.to_be_bytes());
        mvhd.extend_from_slice(b"mvhd");
        mvhd.extend_from_slice(&[0, 0, 0, 0]);
        mvhd.extend_from_slice(&creation);

        let moov_len = 8 + mvhd.len() as u32;
        let mut moov = Vec::new();
        moov.extend_from_slice(&moov_len.to_be_bytes());
        moov.extend_from_slice(b"moov");
        moov.extend_from_slice(&mvhd);

        let mut ftyp = Vec::new();
        ftyp.extend_from_slice(&24_u32.to_be_bytes());
        ftyp.extend_from_slice(b"ftyp");
        ftyp.extend_from_slice(b"isom");
        ftyp.extend_from_slice(&0_u32.to_be_bytes());
        ftyp.extend_from_slice(b"isom");
        ftyp.extend_from_slice(b"mp41");

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ftyp);
        bytes.extend_from_slice(&moov);
        bytes
    }

    fn quicktime_seconds_for(rfc3339: &str) -> u64 {
        use chrono::{DateTime, NaiveDate, Utc};

        let target = DateTime::parse_from_rfc3339(rfc3339).unwrap();
        let epoch = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(1904, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            Utc,
        );
        target
            .with_timezone(&Utc)
            .signed_duration_since(epoch)
            .num_seconds()
            .try_into()
            .unwrap()
    }
}
