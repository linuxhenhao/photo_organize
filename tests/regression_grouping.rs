use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;

fn test_data_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data")
}

#[test]
fn regression_grouping_issue_efficient() {
    let tmp = tempdir().expect("tempdir");
    let db = tmp.path().join("catalog.db");
    let src_dir = test_data_root().join("problematic_images");
    let test_src = tmp.path().join("test_src");
    fs::create_dir_all(&test_src).expect("create test_src");

    // Only copy the specific files reported as problematic that are still available
    let files_to_test = [
        "2197-2a180547_2197.jpg",
        "2197-2a180547_default2197.jpg",
        "2197-2a180547_1707.jpg",
        "2197-2a180547_default1707.jpg",
        "IMG_3001-a83f5973_IMG_5887.JPG",
        "1260-671ad12c_1260.jpg",
    ];

    let mut copied = 0;
    for file in &files_to_test {
        let src = src_dir.join(file);
        if src.exists() {
            fs::copy(src, test_src.join(file)).expect("copy file");
            copied += 1;
        }
    }

    if copied == 0 {
        return; // Skip if no files available to avoid failing in environments without test data
    }

    // Run initcache with threshold 14 (needed for these low-detail images)
    let output = Command::new(env!("CARGO_BIN_EXE_photo-org"))
        .arg("initcache")
        .arg("--db")
        .arg(&db)
        .arg("--dest")
        .arg(&test_src)
        .arg("--phash-threshold")
        .arg("14")
        .output()
        .expect("run photo-org initcache");

    assert!(output.status.success(), "initcache failed: {}", String::from_utf8_lossy(&output.stderr));

    let conn = Connection::open(&db).expect("open catalog db");
    
    // Load all groupings
    let mut stmt = conn.prepare("SELECT group_id, target_path FROM target_items WHERE group_id IS NOT NULL")
        .expect("prepare stmt");
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    }).expect("query map");

    let mut groups: HashMap<i64, HashSet<String>> = HashMap::new();
    for row in rows {
        let (group_id, path) = row.unwrap();
        let filename = Path::new(&path).file_name().unwrap().to_string_lossy().to_string();
        groups.entry(group_id).or_default().insert(filename);
    }

    // 1. Group 2197
    if let Some(g1) = find_group_containing(&groups, "2197-2a180547_2197.jpg") {
        assert!(g1.contains("2197-2a180547_default2197.jpg"));
        assert!(!g1.contains("2197-2a180547_1707.jpg"), "2197 and 1707 should NOT be in the same group");
    }

    // 2. Group 1707
    if let Some(g2) = find_group_containing(&groups, "2197-2a180547_1707.jpg") {
        assert!(g2.contains("2197-2a180547_default1707.jpg"));
    }
}

fn find_group_containing<'a>(groups: &'a HashMap<i64, HashSet<String>>, filename: &str) -> Option<&'a HashSet<String>> {
    for group in groups.values() {
        if group.contains(filename) {
            return Some(group);
        }
    }
    None
}
