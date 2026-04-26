use rusqlite::Connection;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;

fn test_data_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data")
}

struct GroupCase {
    key: &'static str,
    files: &'static [&'static str],
    expect_grouped: bool,
}

#[test]
fn regression_grouping_issue_efficient() {
    let tmp = tempdir().expect("tempdir");
    let db = tmp.path().join("catalog.db");
    let src_dir = test_data_root().join("problematic_images");
    let test_src = tmp.path().join("test_src");
    fs::create_dir_all(&test_src).expect("create test_src");

    let cases = [
        GroupCase {
            key: "1707",
            files: &["1707.jpg", "default1707.jpg"],
            expect_grouped: true,
        },
        GroupCase {
            key: "2197",
            files: &["2197.jpg", "default2197.jpg"],
            expect_grouped: true,
        },
        GroupCase {
            key: "5887",
            files: &["IMG_5887.JPG", "defaultimg_5887.jpg"],
            expect_grouped: false,
        },
    ];

    let mut copied = 0usize;
    for case in &cases {
        for file in case.files {
            let src = src_dir.join(file);
            if src.exists() {
                fs::copy(src, test_src.join(file)).expect("copy file");
                copied += 1;
            }
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

    let mut stmt = conn
        .prepare("SELECT group_id, target_path FROM target_items")
        .expect("prepare stmt");
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, String>(1)?))
    })
    .expect("query map");

    let mut assignments: HashMap<String, Option<i64>> = HashMap::new();
    for row in rows {
        let (group_id, path) = row.unwrap();
        let filename = Path::new(&path).file_name().unwrap().to_string_lossy().to_string();
        assignments.insert(filename, group_id);
    }

    let mut positive_group_ids = HashMap::new();
    for case in &cases {
        for file in case.files {
            assert!(
                assignments.contains_key(*file),
                "missing test fixture in DB for key {}: {}",
                case.key,
                file
            );
        }

        if case.expect_grouped {
            let first_group = assignments[case.files[0]]
                .expect("expected grouped file to have group_id");
            for file in &case.files[1..] {
                assert_eq!(
                    assignments[*file],
                    Some(first_group),
                    "expected {} files to share a group",
                    case.key
                );
            }
            positive_group_ids.insert(case.key, first_group);
        } else {
            for file in case.files {
                for (other_key, other_group) in &positive_group_ids {
                    assert_ne!(
                        assignments[*file],
                        Some(*other_group),
                        "expected {} file {} not to join {} group",
                        case.key,
                        file,
                        other_key
                    );
                }
            }
        }
    }

    assert_ne!(
        positive_group_ids["1707"],
        positive_group_ids["2197"],
        "1707 and 2197 should not collapse into the same group"
    );
}
