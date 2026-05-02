use rusqlite::Connection;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;
use walkdir::WalkDir;

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data")
}

fn copy_tree(src: &Path, dst: &Path) {
    for entry in WalkDir::new(src) {
        let entry = entry.expect("walk fixture tree");
        let relative = entry
            .path()
            .strip_prefix(src)
            .expect("relative fixture path");
        let target = dst.join(relative);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target).expect("create fixture directory");
            continue;
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).expect("create fixture file parent");
        }
        fs::copy(entry.path(), &target).expect("copy fixture file");
    }
}

fn is_hidden_metadata_path(path: &Path) -> bool {
    path.components().any(|component| match component {
        Component::Normal(part) => part.to_string_lossy().starts_with('.'),
        _ => false,
    })
}

fn managed_files(root: &Path) -> BTreeSet<String> {
    let root_name = PathBuf::from(
        root.file_name()
            .expect("managed root should have a final path component"),
    );
    WalkDir::new(root)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.into_path())
        .filter(|path| {
            let relative = path.strip_prefix(root).expect("relative managed file path");
            !is_hidden_metadata_path(relative)
        })
        .map(|path| {
            root_name
                .join(path.strip_prefix(root).expect("relative managed file path"))
                .to_string_lossy()
                .to_string()
        })
        .collect()
}

#[test]
#[ignore = "expensive full-tree initcache integration coverage"]
fn initcache_adopts_full_test_data_tree_in_place() {
    let tmp = tempdir().expect("tempdir");
    let dest = tmp.path().join("test_data");
    copy_tree(&fixture_root(), &dest);

    let before = managed_files(&dest);
    let db = tmp.path().join("catalog.db");
    let output = Command::new(env!("CARGO_BIN_EXE_photo-org"))
        .arg("initcache")
        .arg("--db")
        .arg(&db)
        .arg("--dest")
        .arg(&dest)
        .output()
        .expect("run photo-org initcache");

    assert!(
        output.status.success(),
        "initcache failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let after = managed_files(&dest);
    assert_eq!(
        after, before,
        "initcache should not create copied media files"
    );
    assert!(
        !dest.join(".photo-org").join("initcache-scan.db").exists(),
        "initcache should not persist a target-side scan db"
    );

    let conn = Connection::open(&db).expect("open catalog db");
    let mut stmt = conn
        .prepare("SELECT target_path FROM target_items ORDER BY target_path")
        .expect("prepare target query");
    let catalog_paths: BTreeSet<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query target paths")
        .collect::<rusqlite::Result<_>>()
        .expect("collect target paths");

    assert_eq!(
        catalog_paths, before,
        "catalog target paths should match the original on-disk file set"
    );

    let grouped: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM target_items WHERE group_id IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .expect("count grouped rows");
    assert!(grouped > 0, "fixture tree should exercise visual grouping");

    let hidden_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM target_items WHERE target_path LIKE '%.photo-org/%'",
            [],
            |row| row.get(0),
        )
        .expect("count hidden rows");
    assert_eq!(hidden_rows, 0, "hidden metadata files must stay excluded");
}
