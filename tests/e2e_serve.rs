use image::{ImageBuffer, Rgb};
use reqwest::{Client, StatusCode};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::fs;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::{TempDir, tempdir};

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn test_data_root() -> PathBuf {
    manifest_dir().join("test_data")
}

fn find_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read local addr")
        .port()
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).expect("create destination directory");
    for entry in fs::read_dir(src).expect("read source directory") {
        let entry = entry.expect("dir entry");
        let entry_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if entry.file_type().expect("entry file type").is_dir() {
            copy_dir_recursive(&entry_path, &dst_path);
        } else {
            fs::copy(&entry_path, &dst_path).unwrap_or_else(|err| {
                panic!(
                    "copy {} -> {} failed: {err}",
                    entry_path.display(),
                    dst_path.display()
                )
            });
        }
    }
}

fn make_png(path: &Path, color: [u8; 3]) {
    let image = ImageBuffer::from_fn(
        32,
        32,
        |x, y| {
            if x == y { Rgb([0, 0, 0]) } else { Rgb(color) }
        },
    );
    image.save(path).unwrap();
}

fn run_photo_org(args: &[&str], cwd: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_photo-org"))
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap_or_else(|err| panic!("spawn photo-org {:?} failed: {err}", args))
}

fn assert_command_ok(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context} failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn open_catalog_db(path: &Path) -> Connection {
    let conn = Connection::open(path)
        .unwrap_or_else(|err| panic!("open catalog db {} failed: {err}", path.display()));
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();
    conn
}

struct ServeProcess {
    child: Child,
    port: u16,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl ServeProcess {
    fn spawn(workdir: &Path, db: &Path, dest_arg: &str) -> Self {
        let port = find_free_port();
        let stdout_path = workdir.join(format!("serve-{port}.stdout.log"));
        let stderr_path = workdir.join(format!("serve-{port}.stderr.log"));
        let stdout = File::create(&stdout_path).expect("create stdout log");
        let stderr = File::create(&stderr_path).expect("create stderr log");
        let child = Command::new(env!("CARGO_BIN_EXE_photo-org"))
            .current_dir(workdir)
            .arg("serve")
            .arg("--db")
            .arg(db)
            .arg("--dest")
            .arg(dest_arg)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap_or_else(|err| panic!("spawn serve failed: {err}"));

        Self {
            child,
            port,
            stdout_path,
            stderr_path,
        }
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn wait_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if http_get_ok(self.port, "/api/groups?page_index=0&page_size=1") {
                return;
            }
            if let Ok(Some(status)) = self.child.try_wait() {
                panic!(
                    "serve exited before ready: {status}\nstdout:\n{}\nstderr:\n{}",
                    self.stdout_log(),
                    self.stderr_log()
                );
            }
            if Instant::now() >= deadline {
                panic!(
                    "timed out waiting for serve readiness\nstdout:\n{}\nstderr:\n{}",
                    self.stdout_log(),
                    self.stderr_log()
                );
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn stdout_log(&self) -> String {
        read_file(&self.stdout_path)
    }

    fn stderr_log(&self) -> String {
        read_file(&self.stderr_path)
    }
}

fn http_get_ok(port: u16, path: &str) -> bool {
    let mut stream = match TcpStream::connect(("127.0.0.1", port)) {
        Ok(stream) => stream,
        Err(_) => return false,
    };
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = String::new();
    if stream.read_to_string(&mut response).is_err() {
        return false;
    }
    response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200")
}

impl Drop for ServeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn read_file(path: &Path) -> String {
    let mut content = String::new();
    if let Ok(mut file) = File::open(path) {
        let _ = file.read_to_string(&mut content);
    }
    content
}

fn assert_status(resp_status: StatusCode, expected: StatusCode, body: &str, context: &str) {
    assert_eq!(
        resp_status, expected,
        "{context} returned unexpected status\nexpected: {expected}\nactual: {resp_status}\nbody:\n{body}"
    );
}

fn make_seeded_workspace(dest_arg: &str) -> (TempDir, PathBuf, PathBuf) {
    let tmp = tempdir().expect("tempdir");
    let repo = tmp.path().join("repo");
    fs::create_dir_all(repo.join("2024/06/09")).unwrap();
    fs::create_dir_all(repo.join(".photo-org/trash/group-42")).unwrap();
    fs::create_dir_all(repo.join(".photo-org/trash/group-43")).unwrap();

    let keep_a = repo.join("2024/06/09/keep-a.png");
    let keep_b = repo.join("2024/06/09/keep-b.png");
    let keep_c = repo.join("2024/06/09/keep-c.png");
    let reject_a = repo.join("2024/06/09/reject-a.png");
    let reject_b = repo.join("2024/06/09/reject-b.png");
    let trash_c = repo.join(".photo-org/trash/group-43/reject-c.png");

    make_png(&keep_a, [255, 0, 0]);
    make_png(&keep_b, [255, 255, 0]);
    make_png(&keep_c, [0, 0, 255]);
    make_png(&reject_a, [0, 255, 0]);
    make_png(&reject_b, [255, 0, 255]);
    make_png(&trash_c, [0, 255, 255]);

    let db = tmp.path().join("catalog.db");
    let conn = open_catalog_db(&db);
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS target_items (
            id INTEGER PRIMARY KEY,
            target_path TEXT UNIQUE NOT NULL,
            size_bytes INTEGER NOT NULL,
            mime_type TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            exact_hash TEXT NOT NULL DEFAULT '',
            phash TEXT NOT NULL DEFAULT '',
            phash_bits INTEGER NOT NULL DEFAULT 0,
            width INTEGER NOT NULL DEFAULT 0,
            height INTEGER NOT NULL DEFAULT 0,
            group_id INTEGER,
            keep_state TEXT NOT NULL DEFAULT 'undecided',
            is_group_primary INTEGER NOT NULL DEFAULT 0,
            group_status TEXT NOT NULL DEFAULT 'pending',
            origin_source_id INTEGER,
            meta_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS operations_log (
            id INTEGER PRIMARY KEY,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        "#,
    )
    .unwrap();

    let rows = [
        (
            1_i64,
            "repo/2024/06/09/keep-a.png",
            "2024-06-09T00:00:00Z",
            42_i64,
            "undecided",
            1_i64,
        ),
        (
            2_i64,
            "repo/2024/06/09/reject-a.png",
            "2024-06-09T00:00:00Z",
            42_i64,
            "undecided",
            0_i64,
        ),
        (
            3_i64,
            "repo/2024/06/09/keep-b.png",
            "2024-06-10T00:00:00Z",
            43_i64,
            "kept",
            1_i64,
        ),
        (
            4_i64,
            "repo/2024/06/09/keep-c.png",
            "2024-06-10T00:00:00Z",
            43_i64,
            "kept",
            0_i64,
        ),
        (
            5_i64,
            "repo/.photo-org/trash/group-43/reject-c.png",
            "2024-06-10T00:00:00Z",
            43_i64,
            "rejected",
            0_i64,
        ),
    ];

    for (id, path, created_at, group_id, keep_state, is_group_primary) in rows {
        conn.execute(
            r#"
            INSERT INTO target_items (
                id, target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, group_status,
                origin_source_id, meta_json
            ) VALUES (
                ?1, ?2, 7, 'image/png', ?3, ?4, 'phash', 64,
                32, 32, ?5, ?6, ?7, 'completed', NULL, '{}'
            )
            "#,
            params![
                id,
                path,
                created_at,
                format!("hash-{id}"),
                group_id,
                keep_state,
                is_group_primary
            ],
        )
        .unwrap();
    }
    drop(conn);

    let normalized = if dest_arg == "./repo" {
        tmp.path().join("./repo")
    } else {
        repo.clone()
    };
    (tmp, normalized, db)
}

#[tokio::test]
async fn e2e_cli_scan_import_initcache_and_serve() {
    let tmp = tempdir().unwrap();
    let workdir = tmp.path();
    let scan_db = workdir.join("scan.db");
    let catalog_db = workdir.join("catalog.db");
    let repo = workdir.join("repo");
    fs::create_dir_all(&repo).unwrap();

    let src_a = test_data_root().join("source");
    let src_b = test_data_root().join("source1");
    let scan_args = vec![
        "scan",
        "--scan-db",
        scan_db.to_str().unwrap(),
        "--src",
        src_a.to_str().unwrap(),
        "--src",
        src_b.to_str().unwrap(),
    ];
    let scan_output = run_photo_org(&scan_args, workdir);
    assert_command_ok(&scan_output, "scan");

    let scan_conn = Connection::open(&scan_db).unwrap();
    let scanned: i64 = scan_conn
        .query_row("SELECT COUNT(*) FROM source_items", [], |row| row.get(0))
        .unwrap();
    assert!(scanned > 0, "scan produced no rows");
    let scanned_features: i64 = scan_conn
        .query_row("SELECT COUNT(*) FROM feature_cache", [], |row| row.get(0))
        .unwrap();
    assert!(
        scanned_features > 0,
        "scan did not persist AKAZE feature_cache rows"
    );
    drop(scan_conn);

    let import_args = vec![
        "import",
        "--db",
        catalog_db.to_str().unwrap(),
        "--scan-db",
        scan_db.to_str().unwrap(),
        "--dest",
        repo.to_str().unwrap(),
    ];
    let import_output = run_photo_org(&import_args, workdir);
    assert_command_ok(&import_output, "import");

    let import_conn = open_catalog_db(&catalog_db);
    let imported: i64 = import_conn
        .query_row("SELECT COUNT(*) FROM target_items", [], |row| row.get(0))
        .unwrap();
    assert!(imported > 0, "import produced no target_items");
    let imported_features: i64 = import_conn
        .query_row("SELECT COUNT(*) FROM feature_cache", [], |row| row.get(0))
        .unwrap();
    assert!(
        imported_features > 0,
        "import did not copy AKAZE feature_cache into catalog"
    );
    drop(import_conn);

    fs::remove_file(&catalog_db).unwrap();
    let initcache_args = vec![
        "initcache",
        "--db",
        catalog_db.to_str().unwrap(),
        "--dest",
        repo.to_str().unwrap(),
    ];
    let initcache_output = run_photo_org(&initcache_args, workdir);
    assert_command_ok(&initcache_output, "initcache");

    let adopted_conn = open_catalog_db(&catalog_db);
    let adopted: i64 = adopted_conn
        .query_row("SELECT COUNT(*) FROM target_items", [], |row| row.get(0))
        .unwrap();
    let completed: i64 = adopted_conn
        .query_row(
            "SELECT COUNT(*) FROM target_items WHERE group_status = 'completed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let first_target_path: String = adopted_conn
        .query_row(
            r#"
            SELECT target_path
            FROM target_items
            WHERE lower(target_path) LIKE '%.jpg'
               OR lower(target_path) LIKE '%.jpeg'
               OR lower(target_path) LIKE '%.png'
            LIMIT 1
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(adopted, imported, "initcache row count changed");
    assert_eq!(completed, adopted, "not all initcache rows completed");
    drop(adopted_conn);

    let client = Client::builder().build().unwrap();
    let mut serve = ServeProcess::spawn(workdir, &catalog_db, repo.to_str().unwrap());
    serve.wait_ready();

    let groups_resp = client
        .get(format!(
            "{}/api/groups?page_index=0&page_size=5",
            serve.base_url()
        ))
        .send()
        .await
        .unwrap();
    let groups_status = groups_resp.status();
    let groups_body = groups_resp.text().await.unwrap();
    assert_status(
        groups_status,
        StatusCode::OK,
        &groups_body,
        "GET /api/groups",
    );
    let groups_json: Value = serde_json::from_str(&groups_body).unwrap();
    assert!(groups_json["groups"].is_array());
    assert!(groups_json["total_groups"].is_number());

    let image_resp = client
        .get(format!("{}/image", serve.base_url()))
        .query(&[("path", first_target_path.as_str()), ("size", "400")])
        .send()
        .await
        .unwrap();
    let image_status = image_resp.status();
    let image_body = image_resp.bytes().await.unwrap();
    assert_eq!(
        image_status,
        StatusCode::OK,
        "GET /image failed\nstdout:\n{}\nstderr:\n{}",
        serve.stdout_log(),
        serve.stderr_log()
    );
    assert!(!image_body.is_empty(), "GET /image returned empty body");
}

#[tokio::test]
async fn e2e_serve_http_api_and_dest_path_equivalence() {
    for dest_arg in ["repo", "./repo"] {
        let (tmp, repo, db) = make_seeded_workspace(dest_arg);
        let workdir = tmp.path();
        let client = Client::builder().build().unwrap();
        let mut serve = ServeProcess::spawn(workdir, &db, dest_arg);
        serve.wait_ready();

        let pending_resp = client
            .get(format!(
                "{}/api/groups?page_index=0&page_size=10",
                serve.base_url()
            ))
            .send()
            .await
            .unwrap();
        let pending_status = pending_resp.status();
        let pending_body = pending_resp.text().await.unwrap();
        assert_status(
            pending_status,
            StatusCode::OK,
            &pending_body,
            &format!("GET /api/groups with dest={dest_arg}"),
        );
        let pending_json: Value = serde_json::from_str(&pending_body).unwrap();
        assert_eq!(pending_json["review_mode"], "pending");
        assert_eq!(pending_json["groups"][0]["group_id"], 42);

        let archive_resp = client
            .get(format!("{}/api/groups/42/archive", serve.base_url()))
            .send()
            .await
            .unwrap();
        let archive_status = archive_resp.status();
        let archive_body = archive_resp.text().await.unwrap();
        assert_status(
            archive_status,
            StatusCode::OK,
            &archive_body,
            &format!("GET /api/groups/42/archive with dest={dest_arg}"),
        );

        let image_resp = client
            .get(format!("{}/image", serve.base_url()))
            .query(&[("path", "repo/2024/06/09/keep-a.png"), ("size", "400")])
            .send()
            .await
            .unwrap();
        let image_status = image_resp.status();
        let image_bytes = image_resp.bytes().await.unwrap();
        assert_eq!(image_status, StatusCode::OK);
        assert!(!image_bytes.is_empty());

        let resolve_resp = client
            .post(format!("{}/api/groups/42/resolve", serve.base_url()))
            .json(&json!({
                "kept": ["repo/2024/06/09/keep-a.png"],
                "rejected": ["repo/2024/06/09/reject-a.png"],
                "primary": "repo/2024/06/09/keep-a.png"
            }))
            .send()
            .await
            .unwrap();
        let resolve_status = resolve_resp.status();
        let resolve_body = resolve_resp.text().await.unwrap();
        assert_status(
            resolve_status,
            StatusCode::OK,
            &resolve_body,
            &format!("POST /api/groups/42/resolve with dest={dest_arg}"),
        );
        assert!(repo.join(".photo-org/trash/group-42/reject-a.png").exists());
        assert!(!repo.join("2024/06/09/reject-a.png").exists());

        let conn = open_catalog_db(&db);
        let moved_row: (String, String) = conn
            .query_row(
                "SELECT target_path, keep_state FROM target_items WHERE id = 2",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(moved_row.0, "repo/.photo-org/trash/group-42/reject-a.png");
        assert_eq!(moved_row.1, "rejected");
        drop(conn);

        let trash_resp = client
            .get(format!(
                "{}/api/groups?view=trash&page_index=0&page_size=10",
                serve.base_url()
            ))
            .send()
            .await
            .unwrap();
        let trash_status = trash_resp.status();
        let trash_body = trash_resp.text().await.unwrap();
        assert_status(
            trash_status,
            StatusCode::OK,
            &trash_body,
            &format!("GET /api/groups?view=trash with dest={dest_arg}"),
        );
        let trash_json: Value = serde_json::from_str(&trash_body).unwrap();
        assert_eq!(trash_json["review_mode"], "trash");
        assert_eq!(trash_json["total_groups"], 2);

        let restore_resp = client
            .post(format!(
                "{}/api/groups/42/members/2/restore_trash",
                serve.base_url()
            ))
            .send()
            .await
            .unwrap();
        let restore_status = restore_resp.status();
        let restore_body = restore_resp.text().await.unwrap();
        assert_status(
            restore_status,
            StatusCode::OK,
            &restore_body,
            &format!("POST restore_trash with dest={dest_arg}"),
        );
        assert!(repo.join("2024/06/09/reject-a.png").exists());
        assert!(!repo.join(".photo-org/trash/group-42").exists());

        let bulk_resolve_resp = client
            .post(format!("{}/api/groups/resolve_bulk", serve.base_url()))
            .json(&json!({
                "resolutions": [{
                    "group_id": 43,
                    "kept": ["repo/2024/06/09/keep-b.png"],
                    "rejected": ["repo/2024/06/09/keep-c.png"],
                    "primary": "repo/2024/06/09/keep-b.png"
                }]
            }))
            .send()
            .await
            .unwrap();
        let bulk_resolve_status = bulk_resolve_resp.status();
        let bulk_resolve_body = bulk_resolve_resp.text().await.unwrap();
        assert_status(
            bulk_resolve_status,
            StatusCode::OK,
            &bulk_resolve_body,
            &format!("POST /api/groups/resolve_bulk with dest={dest_arg}"),
        );
        assert!(repo.join(".photo-org/trash/group-43/keep-c.png").exists());

        let bulk_delete_resp = client
            .post(format!("{}/api/groups/delete_trash_bulk", serve.base_url()))
            .json(&json!({ "member_ids": [4] }))
            .send()
            .await
            .unwrap();
        let bulk_delete_status = bulk_delete_resp.status();
        let bulk_delete_body = bulk_delete_resp.text().await.unwrap();
        assert_status(
            bulk_delete_status,
            StatusCode::OK,
            &bulk_delete_body,
            &format!("POST /api/groups/delete_trash_bulk with dest={dest_arg}"),
        );
        assert!(!repo.join(".photo-org/trash/group-43/keep-c.png").exists());
        assert!(repo.join(".photo-org/trash/group-43/reject-c.png").exists());

        let delete_member_resp = client
            .post(format!(
                "{}/api/groups/43/members/5/delete_trash",
                serve.base_url()
            ))
            .send()
            .await
            .unwrap();
        let delete_member_status = delete_member_resp.status();
        let delete_member_body = delete_member_resp.text().await.unwrap();
        assert_status(
            delete_member_status,
            StatusCode::OK,
            &delete_member_body,
            &format!("POST delete_trash member with dest={dest_arg}"),
        );
        assert!(!repo.join(".photo-org/trash/group-43/reject-c.png").exists());

        fs::create_dir_all(repo.join(".photo-org/trash/group-43")).unwrap();
        make_png(
            &repo.join(".photo-org/trash/group-43/reject-d.png"),
            [120, 120, 120],
        );
        let conn = open_catalog_db(&db);
        conn.execute(
            r#"
            INSERT INTO target_items (
                id, target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, group_status,
                origin_source_id, meta_json
            ) VALUES (
                6, 'repo/.photo-org/trash/group-43/reject-d.png', 8, 'image/png',
                '2024-06-10T00:00:00Z', 'hash-6', 'phash', 64, 32, 32, 43, 'rejected', 0,
                'completed', NULL, '{}'
            )
            "#,
            [],
        )
        .unwrap();
        drop(conn);

        let delete_group_resp = client
            .post(format!("{}/api/groups/43/delete_trash", serve.base_url()))
            .send()
            .await
            .unwrap();
        let delete_group_status = delete_group_resp.status();
        let delete_group_body = delete_group_resp.text().await.unwrap();
        assert_status(
            delete_group_status,
            StatusCode::OK,
            &delete_group_body,
            &format!("POST /api/groups/43/delete_trash with dest={dest_arg}"),
        );
        assert!(!repo.join(".photo-org/trash/group-43").exists());

        let conn = open_catalog_db(&db);
        let logical_paths: Vec<String> = conn
            .prepare("SELECT target_path FROM target_items ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(logical_paths.iter().all(|path| path.starts_with("repo/")));
        let dot_prefixed_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM target_items WHERE target_path LIKE './repo/%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(dot_prefixed_count, 0);
    }
}

#[test]
fn e2e_initcache_on_existing_fixture_dest() {
    let tmp = tempdir().unwrap();
    let workdir = tmp.path();
    let repo = workdir.join("repo");
    copy_dir_recursive(&test_data_root().join("integration_test_dest"), &repo);
    let catalog_db = workdir.join("catalog.db");

    let output = run_photo_org(
        &[
            "initcache",
            "--db",
            catalog_db.to_str().unwrap(),
            "--dest",
            repo.to_str().unwrap(),
        ],
        workdir,
    );
    assert_command_ok(&output, "initcache on existing fixture dest");

    let conn = open_catalog_db(&catalog_db);
    let item_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM target_items", [], |row| row.get(0))
        .unwrap();
    let completed_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM target_items WHERE group_status = 'completed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let copied_paths: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM target_items WHERE target_path LIKE 'repo/%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(item_count > 0, "fixture initcache adopted no files");
    assert_eq!(
        completed_count, item_count,
        "fixture initcache left pending rows"
    );
    assert_eq!(
        copied_paths, item_count,
        "fixture target_path escaped repo/ root"
    );
}
