use rusqlite::Connection;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::tempdir;
use walkdir::WalkDir;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_data/source_mock")
        .join(name)
}

fn dest_file_count(dest: &Path) -> usize {
    WalkDir::new(dest)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .count()
}

fn run_photo_org(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_photo-org"))
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("spawn photo-org {args:?} failed: {err}"))
}

fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn assert_ok(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context} failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        stderr_text(output)
    );
}

fn assert_stage_logs_before_done(stderr: &str) {
    let stages = [
        "open_scan_db",
        "open_catalog_db",
        "normalize_catalog_target_paths",
        "load_scan_rows",
        "copy_feature_cache",
        "phash_index",
        "prewarm_select",
        "copy canonicals",
    ];
    for stage in stages {
        let start = format!("import stage start: {stage}");
        let done = format!("import stage done: {stage}");
        let start_pos = stderr
            .find(&start)
            .unwrap_or_else(|| panic!("missing start log for {stage} in stderr:\n{stderr}"));
        let done_pos = stderr
            .find(&done)
            .unwrap_or_else(|| panic!("missing done log for {stage} in stderr:\n{stderr}"));
        assert!(
            start_pos < done_pos,
            "start log for {stage} must appear before done log\n{stderr}"
        );
        let done_line = stderr[done_pos..]
            .lines()
            .next()
            .unwrap_or_default();
        assert!(
            done_line.contains("elapsed_ms"),
            "done log for {stage} must include elapsed_ms: {done_line}"
        );
    }
}

fn run_import_pair() {
    let tmp = tempdir().unwrap();
    let src = tmp.path().join("src");
    let dest = tmp.path().join("dest");
    fs::create_dir_all(&src).unwrap();
    fs::create_dir_all(&dest).unwrap();
    fs::copy(fixture("img_2023_05_01.jpg"), src.join("a.jpg")).unwrap();
    fs::copy(fixture("img_2023_05_02.jpg"), src.join("b.jpg")).unwrap();

    let catalog_db = tmp.path().join("catalog.db");
    let scan_db = tmp.path().join("scan.db");

    let scan = run_photo_org(&[
        "scan",
        "--scan-db",
        scan_db.to_str().unwrap(),
        "--src",
        src.to_str().unwrap(),
    ]);
    assert_ok(&scan, "scan");

    let first = run_photo_org(&[
        "import",
        "--db",
        catalog_db.to_str().unwrap(),
        "--dest",
        dest.to_str().unwrap(),
        "--scan-db",
        scan_db.to_str().unwrap(),
    ]);
    assert_ok(&first, "first import");
    let first_err = stderr_text(&first);
    assert_stage_logs_before_done(&first_err);
    assert!(
        first_err.contains("opening catalog db"),
        "catalog open must log before work\n{first_err}"
    );
    let dest_after_first = dest_file_count(&dest);
    assert_eq!(dest_after_first, 2);

    let catalog = Connection::open(&catalog_db).unwrap();
    let targets_after_first: i64 = catalog
        .query_row("SELECT COUNT(*) FROM target_items", [], |row| row.get(0))
        .unwrap();
    assert_eq!(targets_after_first, 2);
    drop(catalog);

    let second = run_photo_org(&[
        "import",
        "--db",
        catalog_db.to_str().unwrap(),
        "--dest",
        dest.to_str().unwrap(),
        "--scan-db",
        scan_db.to_str().unwrap(),
    ]);
    assert_ok(&second, "second import");
    let second_err = stderr_text(&second);
    assert_stage_logs_before_done(&second_err);
    assert!(
        second_err.contains("skipping already imported exact duplicate"),
        "second import must skip already-imported hashes\n{second_err}"
    );
    assert!(
        !second_err.contains("copying canonical file"),
        "second import must not recopy dest bytes\n{second_err}"
    );
    assert_eq!(dest_file_count(&dest), dest_after_first);

    let catalog = Connection::open(&catalog_db).unwrap();
    let targets_after_second: i64 = catalog
        .query_row("SELECT COUNT(*) FROM target_items", [], |row| row.get(0))
        .unwrap();
    assert_eq!(targets_after_second, targets_after_first);
}

#[test]
fn import_cli_twice_skips_already_imported_hashes() {
    run_import_pair();
    run_import_pair();
}
