use crate::db::{max_group_id, open_catalog_db, open_scan_db};
use crate::feature_loader::{FeatureLoader, FeatureRequest};
use crate::features::{VisualFeatures, akaze_confirm};
use crate::phash_index::PhashIndex;
use crate::scan::{DiscoveredFile, collect_file_paths, discover_file, run as scan_run};
use crate::util::{ProgressReporter, date_for_target, safe_file_name, system_time_to_rfc3339};
use anyhow::{Context, Result};
use rayon::prelude::*;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

#[derive(Debug, Clone)]
struct ScanRow {
    id: i64,
    source_path: String,
    size_bytes: i64,
    mime_type: String,
    created_at: String,
    exact_hash: String,
    phash: String,
    phash_bits: i64,
    width: i64,
    height: i64,
    scan_status: String,
    meta_json: String,
}

#[derive(Debug, Clone)]
struct TargetRow {
    id: i64,
    target_path: String,
    size_bytes: i64,
    mime_type: String,
    exact_hash: String,
    phash: String,
    phash_bits: i64,
    width: i64,
    height: i64,
    group_id: Option<i64>,
    keep_state: String,
    is_group_primary: bool,
}

#[derive(Debug, Clone)]
struct CatalogInput {
    target_path: PathBuf,
    size_bytes: i64,
    mime_type: String,
    created_at: String,
    exact_hash: String,
    phash: String,
    phash_bits: i64,
    width: i64,
    height: i64,
    meta_json: String,
    origin_source_id: Option<i64>,
}

#[derive(Debug, Clone)]
struct ExistingTargetFact {
    size_bytes: i64,
    exact_hash: String,
    modified_at: Option<String>,
}

const TARGET_ROW_SELECT_COLUMNS: &str = r#"
    id, target_path, size_bytes, mime_type, exact_hash, phash, phash_bits, width, height,
    group_id, keep_state, is_group_primary
"#;

const INITCACHE_PROFILE_ENV: &str = "PHOTO_ORG_PROFILE_INITCACHE";

#[derive(Debug, Clone, Default)]
struct InitcacheProfileStats {
    input_feature_calls: usize,
    input_feature_elapsed: Duration,
    candidate_load_calls: usize,
    candidate_load_elapsed: Duration,
    candidate_rows_loaded: usize,
    candidate_distance_checks: usize,
    candidate_feature_calls: usize,
    candidate_feature_elapsed: Duration,
    candidate_confirm_calls: usize,
    candidate_confirm_elapsed: Duration,
    candidate_matches: usize,
    db_tx_calls: usize,
    db_tx_elapsed: Duration,
}

fn initcache_profiling_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os(INITCACHE_PROFILE_ENV).is_some())
}

fn initcache_profile_stats() -> Option<&'static Mutex<InitcacheProfileStats>> {
    if !initcache_profiling_enabled() {
        return None;
    }

    static STATS: OnceLock<Mutex<InitcacheProfileStats>> = OnceLock::new();
    Some(STATS.get_or_init(|| Mutex::new(InitcacheProfileStats::default())))
}

fn reset_initcache_profile() {
    if let Some(stats) = initcache_profile_stats() {
        *stats.lock().expect("initcache profile lock poisoned") = InitcacheProfileStats::default();
    }
}

fn record_initcache_profile(update: impl FnOnce(&mut InitcacheProfileStats)) {
    if let Some(stats) = initcache_profile_stats() {
        let mut guard = stats.lock().expect("initcache profile lock poisoned");
        update(&mut guard);
    }
}

fn snapshot_initcache_profile() -> Option<InitcacheProfileStats> {
    initcache_profile_stats().map(|stats| {
        stats
            .lock()
            .expect("initcache profile lock poisoned")
            .clone()
    })
}

fn log_initcache_profile(scan_elapsed: Duration, adopt_elapsed: Duration, total_elapsed: Duration) {
    let Some(stats) = snapshot_initcache_profile() else {
        return;
    };

    tracing::info!(
        total_elapsed_ms = total_elapsed.as_millis(),
        scan_elapsed_ms = scan_elapsed.as_millis(),
        adopt_elapsed_ms = adopt_elapsed.as_millis(),
        input_feature_calls = stats.input_feature_calls,
        input_feature_elapsed_ms = stats.input_feature_elapsed.as_millis(),
        candidate_load_calls = stats.candidate_load_calls,
        candidate_load_elapsed_ms = stats.candidate_load_elapsed.as_millis(),
        candidate_rows_loaded = stats.candidate_rows_loaded,
        candidate_distance_checks = stats.candidate_distance_checks,
        candidate_feature_calls = stats.candidate_feature_calls,
        candidate_feature_elapsed_ms = stats.candidate_feature_elapsed.as_millis(),
        candidate_confirm_calls = stats.candidate_confirm_calls,
        candidate_confirm_elapsed_ms = stats.candidate_confirm_elapsed.as_millis(),
        candidate_matches = stats.candidate_matches,
        db_tx_calls = stats.db_tx_calls,
        db_tx_elapsed_ms = stats.db_tx_elapsed.as_millis(),
        profile_env = INITCACHE_PROFILE_ENV,
        "initcache profile summary"
    );
}

pub fn run(
    catalog_db: &Path,
    scan_db: Option<&PathBuf>,
    src_roots: &[PathBuf],
    dest: &Path,
    phash_threshold: u32,
    akaze_min_matches: usize,
) -> Result<()> {
    if let Some(src_roots) = (!src_roots.is_empty()).then_some(src_roots) {
        let scan_db_path = scan_db
            .cloned()
            .unwrap_or_else(|| dest.join(".photo-org").join("import-scan.db"));
        if let Some(parent) = scan_db_path.parent() {
            fs::create_dir_all(parent)?;
        }
        scan_run(&scan_db_path, src_roots)?;
        import_from_scan_db(
            catalog_db,
            &scan_db_path,
            dest,
            phash_threshold,
            akaze_min_matches,
        )
    } else {
        let scan_db_path = scan_db.context("import requires either --scan-db or --src")?;
        import_from_scan_db(
            catalog_db,
            scan_db_path,
            dest,
            phash_threshold,
            akaze_min_matches,
        )
    }
}

pub fn initcache(
    catalog_db: &Path,
    dest: &Path,
    phash_threshold: u32,
    akaze_min_matches: usize,
) -> Result<()> {
    reset_initcache_profile();
    let total_started = Instant::now();
    let existing = {
        let catalog_conn = open_catalog_db(catalog_db)?;
        load_existing_target_facts(&catalog_conn)?
    };
    let scan_started = Instant::now();
    let discovered = collect_initcache_discovered_files(&[dest.to_path_buf()], &existing)?;
    let scan_elapsed = scan_started.elapsed();
    let adopt_started = Instant::now();
    let result = adopt_discovered_files(catalog_db, discovered, phash_threshold, akaze_min_matches);
    let adopt_elapsed = adopt_started.elapsed();
    if initcache_profiling_enabled() {
        log_initcache_profile(scan_elapsed, adopt_elapsed, total_started.elapsed());
    }
    result
}

fn collect_initcache_discovered_files(
    roots: &[PathBuf],
    existing: &HashMap<String, ExistingTargetFact>,
) -> Result<Vec<DiscoveredFile>> {
    let files = collect_file_paths(roots)?;
    let skipped = Mutex::new(0usize);
    let progress = ProgressReporter::new("initcache discover", files.len());
    progress.log_start();
    let items: Vec<DiscoveredFile> = files
        .par_iter()
        .filter_map(|path| {
            let result = match discover_or_reuse_target_file(path, existing) {
                Ok(Some(item)) => Some(item),
                Ok(None) => {
                    let mut guard = skipped.lock().expect("skip count lock poisoned");
                    *guard += 1;
                    None
                }
                Err(err) => {
                    tracing::warn!(path = %path.display(), error = %err, "initcache scan failed");
                    None
                }
            };
            progress.item_done();
            result
        })
        .collect();
    let skipped = *skipped.lock().expect("skip count lock poisoned");
    tracing::info!(count = items.len(), skipped, "initcache scan complete");
    Ok(items)
}

fn discover_or_reuse_target_file(
    path: &Path,
    existing: &HashMap<String, ExistingTargetFact>,
) -> Result<Option<DiscoveredFile>> {
    let meta = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    let path_key = path.to_string_lossy().to_string();
    let size_bytes = i64::try_from(meta.len()).unwrap_or(i64::MAX);
    let modified_at = meta.modified().ok().map(system_time_to_rfc3339);

    if let Some(previous) = existing.get(&path_key) {
        if previous.exact_hash.is_empty() {
            return Ok(Some(discover_file(path)?));
        }
        if previous.size_bytes == size_bytes && previous.modified_at == modified_at {
            return Ok(None);
        }
    }

    Ok(Some(discover_file(path)?))
}

fn import_from_scan_db(
    catalog_db: &Path,
    scan_db: &Path,
    dest: &Path,
    phash_threshold: u32,
    akaze_min_matches: usize,
) -> Result<()> {
    fs::create_dir_all(dest)?;
    let scan_conn = open_scan_db(scan_db)?;
    let mut catalog_conn = open_catalog_db(catalog_db)?;
    let mut feature_loader = FeatureLoader::default();
    let mut phash_index = PhashIndex::from_catalog(&catalog_conn)?;
    let rows = load_scan_rows(&scan_conn)?;
    let mut grouped: BTreeMap<String, Vec<ScanRow>> = BTreeMap::new();
    for row in rows.into_iter().filter(|row| row.scan_status == "present") {
        if row.exact_hash.is_empty() {
            continue;
        }
        grouped.entry(row.exact_hash.clone()).or_default().push(row);
    }

    let progress = ProgressReporter::new("import canonicals", grouped.len());
    progress.log_start();
    let mut imported = 0usize;
    for (_exact_hash, candidates) in grouped {
        let canonical = choose_canonical(&candidates);
        if target_exists_with_hash(&catalog_conn, &canonical.exact_hash)? {
            tracing::info!(source = %canonical.source_path, hash = %canonical.exact_hash, "skipping already imported exact duplicate");
            progress.item_done();
            continue;
        }
        let visual = import_single(
            &mut catalog_conn,
            &mut feature_loader,
            &mut phash_index,
            dest,
            &canonical,
            phash_threshold,
            akaze_min_matches,
        )?;
        imported += 1;
        tracing::info!(target = %visual.target_path, group_id = ?visual.group_id, "imported canonical file");
        progress.item_done();
    }

    tracing::info!(db = %catalog_db.display(), imported, "import complete");
    Ok(())
}

fn adopt_discovered_files(
    catalog_db: &Path,
    discovered: Vec<DiscoveredFile>,
    phash_threshold: u32,
    akaze_min_matches: usize,
) -> Result<()> {
    let mut catalog_conn = open_catalog_db(catalog_db)?;
    let mut feature_loader = FeatureLoader::default();
    let mut phash_index = PhashIndex::from_catalog(&catalog_conn)?;

    let total = discovered
        .iter()
        .filter(|file| file.scan_status == "present")
        .count();
    let progress = ProgressReporter::new("initcache adopt", total);
    progress.log_start();
    let mut adopted = 0usize;
    for file in discovered
        .into_iter()
        .filter(|file| file.scan_status == "present")
    {
        let visual = adopt_single(
            &mut catalog_conn,
            &mut feature_loader,
            &mut phash_index,
            catalog_input_from_discovered_file(&file),
            phash_threshold,
            akaze_min_matches,
        )?;
        adopted += 1;
        tracing::info!(target = %visual.target_path, group_id = ?visual.group_id, "adopted target file");
        progress.item_done();
    }

    tracing::info!(db = %catalog_db.display(), adopted, "initcache complete");
    Ok(())
}

fn load_scan_rows(conn: &Connection) -> Result<Vec<ScanRow>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, source_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits, width, height, scan_status, meta_json
        FROM source_items
        "#,
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(ScanRow {
                id: row.get(0)?,
                source_path: row.get(1)?,
                size_bytes: row.get(2)?,
                mime_type: row.get(3)?,
                created_at: row.get(4)?,
                exact_hash: row.get(5)?,
                phash: row.get(6)?,
                phash_bits: row.get(7)?,
                width: row.get(8)?,
                height: row.get(9)?,
                scan_status: row.get(10)?,
                meta_json: row.get(11)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn choose_canonical(rows: &[ScanRow]) -> ScanRow {
    let mut rows = rows.to_vec();
    rows.sort_by(|a, b| {
        canonical_rank(b)
            .cmp(&canonical_rank(a))
            .then_with(|| a.source_path.cmp(&b.source_path))
    });
    rows.into_iter()
        .next()
        .expect("exact-hash group has at least one row")
}

fn canonical_rank(row: &ScanRow) -> (bool, i64, i64, i64) {
    (
        row.mime_type.starts_with("image/"),
        row.width * row.height,
        row.size_bytes,
        -(row.source_path.len() as i64),
    )
}

fn target_exists_with_hash(conn: &Connection, exact_hash: &str) -> Result<bool> {
    let exists = conn
        .query_row(
            "SELECT 1 FROM target_items WHERE exact_hash = ?1 LIMIT 1",
            params![exact_hash],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    Ok(exists)
}

fn import_single(
    conn: &mut Connection,
    feature_loader: &mut FeatureLoader,
    phash_index: &mut PhashIndex,
    dest: &Path,
    row: &ScanRow,
    phash_threshold: u32,
    akaze_min_matches: usize,
) -> Result<TargetRow> {
    let source_path = Path::new(&row.source_path);
    let target_path = reserve_target_path(dest, &row.created_at, source_path)?;
    copy_to_target(source_path, &target_path)?;
    let input = catalog_input_from_scan_row(row, target_path);
    process_catalog_input(
        conn,
        feature_loader,
        phash_index,
        input,
        phash_threshold,
        akaze_min_matches,
    )
}

fn adopt_single(
    conn: &mut Connection,
    feature_loader: &mut FeatureLoader,
    phash_index: &mut PhashIndex,
    input: CatalogInput,
    phash_threshold: u32,
    akaze_min_matches: usize,
) -> Result<TargetRow> {
    process_catalog_input(
        conn,
        feature_loader,
        phash_index,
        input,
        phash_threshold,
        akaze_min_matches,
    )
}

fn process_catalog_input(
    conn: &mut Connection,
    feature_loader: &mut FeatureLoader,
    phash_index: &mut PhashIndex,
    input: CatalogInput,
    phash_threshold: u32,
    akaze_min_matches: usize,
) -> Result<TargetRow> {
    let input_feature_started = Instant::now();
    let visual = feature_loader.load(
        conn,
        FeatureRequest {
            path: &input.target_path,
            mime_type: &input.mime_type,
            exact_hash: &input.exact_hash,
            size_bytes: input.size_bytes,
            phash_hint: &input.phash,
            phash_bits: input.phash_bits,
            width: input.width,
            height: input.height,
        },
    )?;
    let input_feature_elapsed = input_feature_started.elapsed();
    let candidate_load_started = Instant::now();
    let candidate_ids = if visual.phash.is_empty() {
        Vec::new()
    } else {
        phash_index.search(&visual.phash, visual.phash_bits, phash_threshold)
    };
    let candidate_load_elapsed = candidate_load_started.elapsed();
    let candidate_rows_loaded = candidate_ids.len();
    let mut matches = Vec::new();
    let mut candidate_distance_checks = 0usize;
    let mut candidate_feature_calls = 0usize;
    let mut candidate_feature_elapsed = Duration::default();
    let mut candidate_confirm_calls = 0usize;
    let mut candidate_confirm_elapsed = Duration::default();
    let mut candidate_matches = 0usize;
    let input_target_path = input.target_path.to_string_lossy().to_string();
    for candidate_id in candidate_ids {
        let candidate = load_target_by_id(conn, candidate_id)?;
        if candidate.target_path == input_target_path
            || candidate.exact_hash == input.exact_hash
            || candidate.phash.is_empty()
        {
            continue;
        }
        candidate_distance_checks += 1;
        let candidate_feature_started = Instant::now();
        let candidate_features = feature_loader.load(
            conn,
            FeatureRequest {
                path: Path::new(&candidate.target_path),
                mime_type: &candidate.mime_type,
                exact_hash: &candidate.exact_hash,
                size_bytes: candidate.size_bytes,
                phash_hint: &candidate.phash,
                phash_bits: candidate.phash_bits,
                width: candidate.width,
                height: candidate.height,
            },
        )?;
        candidate_feature_calls += 1;
        candidate_feature_elapsed += candidate_feature_started.elapsed();
        let candidate_confirm_started = Instant::now();
        let matched = akaze_confirm(&visual, &candidate_features, akaze_min_matches);
        candidate_confirm_calls += 1;
        candidate_confirm_elapsed += candidate_confirm_started.elapsed();
        if matched {
            candidate_matches += 1;
            matches.push(candidate);
        }
    }

    record_initcache_profile(|stats| {
        stats.input_feature_calls += 1;
        stats.input_feature_elapsed += input_feature_elapsed;
        stats.candidate_load_calls += 1;
        stats.candidate_load_elapsed += candidate_load_elapsed;
        stats.candidate_rows_loaded += candidate_rows_loaded;
        stats.candidate_distance_checks += candidate_distance_checks;
        stats.candidate_feature_calls += candidate_feature_calls;
        stats.candidate_feature_elapsed += candidate_feature_elapsed;
        stats.candidate_confirm_calls += candidate_confirm_calls;
        stats.candidate_confirm_elapsed += candidate_confirm_elapsed;
        stats.candidate_matches += candidate_matches;
    });

    let db_tx_started = Instant::now();
    let tx = conn.transaction()?;
    upsert_catalog_item(&tx, &input, &visual)?;
    let inserted = load_target_by_path(&tx, input.target_path.as_path())?;
    let target_row = if matches.is_empty() {
        tx.execute(
            "UPDATE target_items SET is_group_primary = 1 WHERE id = ?1",
            params![inserted.id],
        )?;
        let mut row = inserted.clone();
        row.is_group_primary = true;
        row
    } else {
        let mut group_ids: HashSet<i64> = matches.iter().filter_map(|m| m.group_id).collect();
        let chosen_group = if group_ids.is_empty() {
            max_group_id(&tx)? + 1
        } else {
            *group_ids.iter().min().unwrap()
        };
        for candidate in &matches {
            if candidate.group_id.is_none() {
                tx.execute(
                    "UPDATE target_items SET group_id = ?1 WHERE id = ?2",
                    params![chosen_group, candidate.id],
                )?;
            }
        }
        for group_id in group_ids.drain() {
            tx.execute(
                "UPDATE target_items SET group_id = ?1 WHERE group_id = ?2",
                params![chosen_group, group_id],
            )?;
        }
        tx.execute(
            "UPDATE target_items SET group_id = ?1, keep_state = 'undecided', is_group_primary = 0 WHERE id = ?2",
            params![chosen_group, inserted.id],
        )?;
        let all_members = load_group_members(&tx, chosen_group)?;
        let primary_id = choose_primary_member(&all_members)?;
        tx.execute(
            "UPDATE target_items SET is_group_primary = CASE WHEN id = ?1 THEN 1 ELSE 0 END WHERE group_id = ?2",
            params![primary_id, chosen_group],
        )?;
        tx.execute(
            "UPDATE target_items SET keep_state = 'undecided' WHERE group_id = ?1",
            params![chosen_group],
        )?;
        let mut row = inserted.clone();
        row.group_id = Some(chosen_group);
        row.keep_state = "undecided".to_string();
        row.is_group_primary = primary_id == inserted.id;
        row
    };
    tx.commit()?;
    phash_index.upsert(target_row.id, &visual.phash, visual.phash_bits);
    let db_tx_elapsed = db_tx_started.elapsed();
    record_initcache_profile(|stats| {
        stats.db_tx_calls += 1;
        stats.db_tx_elapsed += db_tx_elapsed;
    });
    Ok(target_row)
}

fn catalog_input_from_scan_row(row: &ScanRow, target_path: PathBuf) -> CatalogInput {
    CatalogInput {
        target_path,
        size_bytes: row.size_bytes,
        mime_type: row.mime_type.clone(),
        created_at: row.created_at.clone(),
        exact_hash: row.exact_hash.clone(),
        phash: row.phash.clone(),
        phash_bits: row.phash_bits,
        width: row.width,
        height: row.height,
        meta_json: row.meta_json.clone(),
        origin_source_id: Some(row.id),
    }
}

fn catalog_input_from_discovered_file(file: &DiscoveredFile) -> CatalogInput {
    CatalogInput {
        target_path: file.path.clone(),
        size_bytes: file.size_bytes,
        mime_type: file.mime_type.clone(),
        created_at: file.created_at.clone(),
        exact_hash: file.exact_hash.clone(),
        phash: file.phash.clone(),
        phash_bits: file.phash_bits,
        width: file.width,
        height: file.height,
        meta_json: file.meta_json.clone(),
        origin_source_id: None,
    }
}

fn reserve_target_path(dest: &Path, created_at: &str, source_path: &Path) -> Result<PathBuf> {
    let (year, month, day) = date_for_target(created_at);
    let folder = dest.join(year).join(month).join(day);
    fs::create_dir_all(&folder)?;
    let mut candidate = folder.join(safe_file_name(source_path));
    let stem = candidate
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("file")
        .to_string();
    let ext = candidate
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();
    let mut idx = 0usize;
    while candidate.exists() {
        idx += 1;
        let name = if ext.is_empty() {
            format!("{}-{}", stem, idx)
        } else {
            format!("{}-{}.{}", stem, idx, ext)
        };
        candidate = folder.join(name);
    }
    Ok(candidate)
}

fn copy_to_target(source: &Path, target: &Path) -> Result<()> {
    let parent = target.parent().context("target path has no parent")?;
    fs::create_dir_all(parent)?;
    let temp = NamedTempFile::new_in(parent)?;
    fs::copy(source, temp.path())?;
    temp.persist(target)
        .with_context(|| format!("persist {} -> {}", source.display(), target.display()))?;
    Ok(())
}

fn upsert_catalog_item(
    tx: &Connection,
    input: &CatalogInput,
    visual: &VisualFeatures,
) -> Result<()> {
    tx.execute(
        r#"
        INSERT INTO target_items (
            target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits, width, height,
            group_id, keep_state, is_group_primary, origin_source_id, meta_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, 'undecided', 0, ?10, ?11)
        ON CONFLICT(target_path) DO UPDATE SET
            size_bytes = excluded.size_bytes,
            mime_type = excluded.mime_type,
            created_at = excluded.created_at,
            exact_hash = excluded.exact_hash,
            phash = excluded.phash,
            phash_bits = excluded.phash_bits,
            width = excluded.width,
            height = excluded.height,
            origin_source_id = excluded.origin_source_id,
            meta_json = excluded.meta_json
        "#,
        params![
            input.target_path.to_string_lossy(),
            input.size_bytes,
            input.mime_type,
            input.created_at,
            input.exact_hash,
            visual.phash.clone(),
            visual.phash_bits,
            visual.width,
            visual.height,
            input.origin_source_id,
            input.meta_json,
        ],
    )?;
    Ok(())
}

fn load_existing_target_facts(conn: &Connection) -> Result<HashMap<String, ExistingTargetFact>> {
    let mut stmt =
        conn.prepare("SELECT target_path, size_bytes, exact_hash, meta_json FROM target_items")?;
    let rows = stmt
        .query_map([], |row| {
            let target_path = row.get::<_, String>(0)?;
            let size_bytes = row.get::<_, i64>(1)?;
            let exact_hash = row.get::<_, String>(2)?;
            let meta_json = row.get::<_, String>(3)?;
            Ok((
                target_path,
                ExistingTargetFact {
                    size_bytes,
                    exact_hash,
                    modified_at: extract_modified_at(&meta_json),
                },
            ))
        })?
        .collect::<rusqlite::Result<HashMap<_, _>>>()?;
    Ok(rows)
}

fn extract_modified_at(meta_json: &str) -> Option<String> {
    let value: Value = serde_json::from_str(meta_json).ok()?;
    value
        .get("fingerprint")?
        .get("modified_at")?
        .as_str()
        .map(str::to_owned)
}

fn map_target_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TargetRow> {
    Ok(TargetRow {
        id: row.get(0)?,
        target_path: row.get(1)?,
        size_bytes: row.get(2)?,
        mime_type: row.get(3)?,
        exact_hash: row.get(4)?,
        phash: row.get(5)?,
        phash_bits: row.get(6)?,
        width: row.get(7)?,
        height: row.get(8)?,
        group_id: row.get(9)?,
        keep_state: row.get(10)?,
        is_group_primary: row.get::<_, i64>(11)? != 0,
    })
}

fn load_target_by_path(conn: &Connection, target_path: &Path) -> Result<TargetRow> {
    conn.query_row(
        &format!("SELECT {TARGET_ROW_SELECT_COLUMNS} FROM target_items WHERE target_path = ?1"),
        params![target_path.to_string_lossy()],
        map_target_row,
    )
    .context("load target row by path")
}

fn load_target_by_id(conn: &Connection, id: i64) -> Result<TargetRow> {
    conn.query_row(
        &format!("SELECT {TARGET_ROW_SELECT_COLUMNS} FROM target_items WHERE id = ?1"),
        params![id],
        map_target_row,
    )
    .context("load target row by id")
}

fn load_group_members(conn: &Connection, group_id: i64) -> Result<Vec<TargetRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {TARGET_ROW_SELECT_COLUMNS} FROM target_items WHERE group_id = ?1"
    ))?;
    let rows = stmt
        .query_map(params![group_id], map_target_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn choose_primary_member(rows: &[TargetRow]) -> Result<i64> {
    rows.iter()
        .max_by(|a, b| {
            canonical_target_rank(a)
                .cmp(&canonical_target_rank(b))
                .then_with(|| a.target_path.cmp(&b.target_path))
        })
        .map(|row| row.id)
        .context("group should contain at least one row")
}

fn canonical_target_rank(row: &TargetRow) -> (bool, i64, i64, i64) {
    (
        row.mime_type.starts_with("image/"),
        row.width * row.height,
        row.size_bytes,
        -(row.target_path.len() as i64),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_catalog_db;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn mock_fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../test_data/source_mock")
            .join(name)
    }

    fn source_fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../test_data/source")
            .join(name)
    }

    fn source1_fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../test_data/source1")
            .join(name)
    }

    fn copy_mock_fixture(name: &str, path: &Path) {
        fs::copy(mock_fixture_path(name), path).unwrap();
    }

    fn copy_source_fixture(name: &str, path: &Path) {
        fs::copy(source_fixture_path(name), path).unwrap();
    }

    fn copy_source1_fixture(name: &str, path: &Path) {
        fs::copy(source1_fixture_path(name), path).unwrap();
    }

    fn append_trailing_byte(path: &Path) {
        use std::io::Write;

        let mut file = fs::OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(b"x").unwrap();
    }

    #[test]
    fn import_skips_exact_duplicate_groups() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src");
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(&dest).unwrap();

        let a = src.join("2024-06-09_a.ARW");
        let b = src.join("2024-06-09_b.ARW");
        let c = src.join("2024-06-10_c.ARW");
        copy_source_fixture("DSC00903.ARW", &a);
        copy_source_fixture("DSC0aa.ARW", &b);
        copy_source1_fixture("DSC01075.ARW", &c);

        let scan_db = tmp.path().join("scan.db");
        run(
            &tmp.path().join("catalog.db"),
            Some(&scan_db),
            &[src],
            &dest,
            64,
            1,
        )
        .unwrap();

        let catalog = open_catalog_db(tmp.path().join("catalog.db")).unwrap();
        let count: i64 = catalog
            .query_row("SELECT COUNT(*) FROM target_items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
        let imported_paths: i64 = catalog
            .query_row(
                "SELECT COUNT(*) FROM target_items WHERE target_path LIKE ?1",
                [format!("{}%", dest.display())],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(imported_paths, 2);
    }

    #[test]
    fn import_groups_similar_images() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src");
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(&dest).unwrap();

        let a = src.join("2024-06-09_a.jpg");
        let b = src.join("2024-06-09_b.jpg");
        copy_mock_fixture("img_2023_05_01.jpg", &a);
        fs::copy(&a, &b).unwrap();
        append_trailing_byte(&b);

        let scan_db = tmp.path().join("scan.db");
        run(
            &tmp.path().join("catalog.db"),
            Some(&scan_db),
            &[src],
            &dest,
            14,
            6,
        )
        .unwrap();

        let catalog = open_catalog_db(tmp.path().join("catalog.db")).unwrap();
        let count: i64 = catalog
            .query_row("SELECT COUNT(*) FROM target_items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
        let grouped: i64 = catalog
            .query_row(
                "SELECT COUNT(*) FROM target_items WHERE group_id IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(grouped, 2);
        let distinct_groups: i64 = catalog
            .query_row(
                "SELECT COUNT(DISTINCT group_id) FROM target_items WHERE group_id IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(distinct_groups, 1);
    }

    #[test]
    fn initcache_adopts_existing_paths_in_place() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let a = dest.join("DSC00903.ARW");
        let b = dest.join("DSC00903.thumb.jpg");
        let c = dest.join("DSC0aa.ARW");
        let d = dest.join("DSC0aa.thumb.jpg");
        let e = dest.join("DSC01094.ARW");
        let f = dest.join("DSC01094.thumb.jpg");
        let g = dest.join("DSC01075.ARW");
        let h = dest.join("DSC01075.thumb.jpg");
        let i = dest.join("IMG_5798.CR2");
        copy_source_fixture("DSC00903.ARW", &a);
        copy_source_fixture("DSC00903.thumb.jpg", &b);
        copy_source_fixture("DSC0aa.ARW", &c);
        copy_source_fixture("DSC0aa.thumb.jpg", &d);
        copy_source_fixture("DSC01094.ARW", &e);
        copy_source_fixture("DSC01094.thumb.jpg", &f);
        copy_source1_fixture("DSC01075.ARW", &g);
        copy_source1_fixture("DSC01075.thumb.jpg", &h);
        copy_source_fixture("IMG_5798.CR2", &i);
        let hidden = dest.join(".photo-org/ignore.jpg");
        fs::create_dir_all(hidden.parent().unwrap()).unwrap();
        copy_mock_fixture("img_2023_05_02.jpg", &hidden);

        let catalog_db = tmp.path().join("catalog.db");
        initcache(&catalog_db, &dest, 14, 6).unwrap();
        assert!(
            !dest.join(".photo-org").join("initcache-scan.db").exists(),
            "initcache should not persist a target-side scan db"
        );

        let catalog = open_catalog_db(&catalog_db).unwrap();
        let count: i64 = catalog
            .query_row("SELECT COUNT(*) FROM target_items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 9);
        let mut paths = Vec::new();
        let mut stmt = catalog
            .prepare("SELECT target_path FROM target_items ORDER BY target_path")
            .unwrap();
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        paths.extend(rows);
        assert_eq!(
            paths,
            vec![
                a.to_string_lossy().to_string(),
                b.to_string_lossy().to_string(),
                g.to_string_lossy().to_string(),
                h.to_string_lossy().to_string(),
                e.to_string_lossy().to_string(),
                f.to_string_lossy().to_string(),
                c.to_string_lossy().to_string(),
                d.to_string_lossy().to_string(),
                i.to_string_lossy().to_string()
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn initcache_skips_rehash_for_unchanged_files() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let image = dest.join("img.jpg");
        copy_mock_fixture("img_2023_05_01.jpg", &image);

        let catalog_db = tmp.path().join("catalog.db");
        initcache(&catalog_db, &dest, 14, 6).unwrap();

        let mut perms = fs::metadata(&image).unwrap().permissions();
        perms.set_mode(0o000);
        fs::set_permissions(&image, perms).unwrap();

        initcache(&catalog_db, &dest, 14, 6).unwrap();
    }
}
