use crate::db::{insert_operation, open_catalog_db};
use crate::util::{ensure_under_root, safe_file_name};
use anyhow::Result;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{Html, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use image::ImageFormat;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use tokio::net::TcpListener;

#[derive(Clone)]
struct AppState {
    db_path: PathBuf,
    dest: PathBuf,
}

#[derive(Debug, Serialize)]
struct GroupSummary {
    group_id: i64,
    status: String,
    members: Vec<GroupMember>,
}

#[derive(Debug, Serialize)]
struct GroupMember {
    id: i64,
    target_path: String,
    mime_type: String,
    keep_state: String,
    is_group_primary: bool,
    exact_hash: String,
    phash: String,
}

#[derive(Debug, Deserialize)]
struct ResolveRequest {
    kept: Option<Vec<String>>,
    rejected: Option<Vec<String>>,
    primary: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ImageQuery {
    path: String,
}

pub async fn run(db: PathBuf, dest: PathBuf, host: String, port: u16) -> Result<()> {
    let state = AppState { db_path: db, dest };
    let app = router(state);
    let listener = TcpListener::bind((host.as_str(), port)).await?;
    tracing::info!(addr = %listener.local_addr()?, "serve listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/groups", get(list_groups))
        .route("/api/groups/{id}/resolve", post(resolve_group))
        .route("/api/groups/{id}/archive", get(archive_group))
        .route("/image", get(image))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>photo-org</title>
  <style>
    body { font-family: sans-serif; margin: 0; background: #111827; color: #e5e7eb; }
    header { padding: 16px 20px; background: #0f172a; position: sticky; top: 0; }
    main { padding: 16px 20px; display: grid; gap: 16px; }
    .group { border: 1px solid #334155; border-radius: 12px; padding: 12px; background: #1f2937; }
    .member { display: flex; gap: 12px; align-items: center; padding: 8px 0; border-top: 1px solid #334155; }
    .member:first-child { border-top: 0; }
    img { width: 160px; height: 160px; object-fit: cover; background: #0b1220; }
    button { background: #2563eb; color: white; border: 0; border-radius: 8px; padding: 8px 12px; }
  </style>
</head>
<body>
  <header><strong>photo-org</strong> local review</header>
  <main id="groups"></main>
  <script>
    async function loadGroups() {
      const resp = await fetch('/api/groups');
      const groups = await resp.json();
      const root = document.getElementById('groups');
      root.innerHTML = '';
      for (const group of groups) {
        const section = document.createElement('section');
        section.className = 'group';
        section.innerHTML = `<h2>Group ${group.group_id} (${group.status})</h2>`;
        for (const member of group.members) {
          const row = document.createElement('div');
          row.className = 'member';
          row.innerHTML = `
            <div>
              <div>${member.target_path}</div>
              <div>${member.keep_state}${member.is_group_primary ? ' primary' : ''}</div>
            </div>`;
          section.appendChild(row);
        }
        root.appendChild(section);
      }
    }
    loadGroups();
  </script>
</body>
</html>"#,
    )
}

async fn list_groups(
    State(state): State<AppState>,
) -> Result<Json<Vec<GroupSummary>>, (StatusCode, String)> {
    let conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let groups = load_groups(&conn).map_err(internal_error)?;
    Ok(Json(groups))
}

async fn resolve_group(
    Path(id): Path<i64>,
    State(state): State<AppState>,
    Json(request): Json<ResolveRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let group = load_group_members(&conn, id).map_err(internal_error)?;
    if group.is_empty() {
        return Err((StatusCode::NOT_FOUND, "group not found".into()));
    }
    let kept = request.kept.unwrap_or_default();
    let rejected = request.rejected.unwrap_or_default();
    let primary = request.primary;
    let kept_set: HashSet<_> = kept.iter().collect();
    let rejected_set: HashSet<_> = rejected.iter().collect();
    if !kept_set.is_disjoint(&rejected_set) {
        return Err((
            StatusCode::BAD_REQUEST,
            "kept and rejected sets overlap".into(),
        ));
    }
    for path in kept_set.iter().chain(rejected_set.iter()) {
        if !group.iter().any(|member| &member.target_path == *path) {
            return Err((
                StatusCode::BAD_REQUEST,
                "requested path is not in the group".into(),
            ));
        }
    }
    if let Some(primary_path) = primary.as_ref() {
        if !group
            .iter()
            .any(|member| &member.target_path == primary_path)
        {
            return Err((
                StatusCode::BAD_REQUEST,
                "primary path is not in the group".into(),
            ));
        }
        if !kept_set.contains(primary_path) {
            return Err((StatusCode::BAD_REQUEST, "primary path must be kept".into()));
        }
    }

    let mut moved_paths = Vec::new();

    for member in &group {
        let keep_path = member.target_path.clone();
        if rejected_set.contains(&keep_path) {
            let moved_to = move_to_trash(&state.dest, &keep_path, id).map_err(internal_error)?;
            moved_paths.push((member.id, keep_path, moved_to));
        }
    }

    let tx = conn.transaction().map_err(internal_error)?;
    for member in &group {
        let keep_path = member.target_path.clone();
        let mut keep_state = "undecided";
        if kept_set.contains(&keep_path) {
            keep_state = "kept";
        }
        if rejected_set.contains(&keep_path) {
            keep_state = "rejected";
        }
        let is_primary = primary.as_ref().map(|p| p == &keep_path).unwrap_or(false);
        tx.execute(
            "UPDATE target_items SET keep_state = ?1, is_group_primary = ?2 WHERE id = ?3",
            rusqlite::params![keep_state, if is_primary { 1 } else { 0 }, member.id],
        )
        .map_err(internal_error)?;
    }
    for (id, _, moved_to) in &moved_paths {
        tx.execute(
            "UPDATE target_items SET target_path = ?1 WHERE id = ?2",
            rusqlite::params![moved_to.to_string_lossy(), id],
        )
        .map_err(internal_error)?;
    }
    insert_operation(
        &tx,
        "resolve_group",
        &json!({"group_id": id, "kept": kept, "rejected": rejected, "primary": primary, "moved": moved_paths.iter().map(|(row_id, _, path)| json!({"id": row_id, "path": path})).collect::<Vec<_>>()})
            .to_string(),
    )
    .map_err(internal_error)?;
    if let Err(err) = tx.commit() {
        for (_, original_path, moved_to) in moved_paths.iter().rev() {
            let _ = fs::rename(moved_to, original_path);
        }
        return Err(internal_error(err));
    }
    Ok(Json(json!({"group_id": id, "status": "ok"})))
}

async fn archive_group(
    Path(id): Path<i64>,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let group = load_group_members(&conn, id).map_err(internal_error)?;
    Ok(Json(json!({ "group_id": id, "members": group })))
}

async fn image(
    State(state): State<AppState>,
    Query(query): Query<ImageQuery>,
) -> Result<Response, (StatusCode, String)> {
    let path = PathBuf::from(&query.path);
    ensure_under_root(&state.dest, &path).map_err(internal_error)?;
    let bytes = tokio::task::spawn_blocking(move || fs::read(&path))
        .await
        .map_err(internal_error)?
        .map_err(internal_error)?;
    let mime = infer::get(&bytes)
        .map(|k| k.mime_type())
        .unwrap_or("application/octet-stream");
    if mime.starts_with("image/") && bytes.len() < 2 * 1024 * 1024 {
        let mut resp = Response::new(axum::body::Body::from(bytes));
        resp.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_str(mime).map_err(internal_error)?,
        );
        return Ok(resp);
    }

    let image = image::load_from_memory(&bytes).map_err(internal_error)?;
    let preview = tokio::task::spawn_blocking(move || {
        let preview = image.thumbnail(1600, 1600);
        let mut out = Cursor::new(Vec::new());
        preview
            .write_to(&mut out, ImageFormat::Jpeg)
            .map_err(anyhow::Error::from)?;
        Ok::<_, anyhow::Error>(out.into_inner())
    })
    .await
    .map_err(internal_error)?
    .map_err(internal_error)?;
    let mut resp = Response::new(axum::body::Body::from(preview));
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("image/jpeg"),
    );
    Ok(resp)
}

fn move_to_trash(dest: &PathBuf, target_path: &str, group_id: i64) -> Result<PathBuf> {
    let path = PathBuf::from(target_path);
    ensure_under_root(dest, &path)?;
    let trash_dir = dest
        .join(".photo-org")
        .join("trash")
        .join(format!("group-{}", group_id));
    fs::create_dir_all(&trash_dir)?;
    let file_name = safe_file_name(&path);
    let mut candidate = trash_dir.join(file_name);
    let mut idx = 0usize;
    while candidate.exists() {
        idx += 1;
        candidate = trash_dir.join(format!("{}-{}", idx, safe_file_name(&path)));
    }
    fs::rename(&path, &candidate)?;
    Ok(candidate)
}

fn load_groups(conn: &rusqlite::Connection) -> Result<Vec<GroupSummary>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT group_id
        FROM target_items
        WHERE group_id IS NOT NULL
        GROUP BY group_id
        HAVING SUM(CASE WHEN keep_state = 'undecided' THEN 1 ELSE 0 END) > 0
        ORDER BY group_id
        "#,
    )?;
    let ids = stmt
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut groups = Vec::new();
    for id in ids {
        let members = load_group_members(conn, id)?;
        let status = if members.iter().any(|m| m.keep_state == "undecided") {
            "pending"
        } else {
            "reviewed"
        };
        groups.push(GroupSummary {
            group_id: id,
            status: status.to_string(),
            members: members
                .into_iter()
                .map(|m| GroupMember {
                    id: m.id,
                    target_path: m.target_path,
                    mime_type: m.mime_type,
                    keep_state: m.keep_state,
                    is_group_primary: m.is_group_primary,
                    exact_hash: m.exact_hash,
                    phash: m.phash,
                })
                .collect(),
        });
    }
    Ok(groups)
}

fn load_group_members(conn: &rusqlite::Connection, group_id: i64) -> Result<Vec<MemberRow>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, target_path, mime_type, keep_state, is_group_primary, exact_hash, phash
        FROM target_items
        WHERE group_id = ?1
        ORDER BY id
        "#,
    )?;
    let rows = stmt
        .query_map(rusqlite::params![group_id], |row| {
            Ok(MemberRow {
                id: row.get(0)?,
                target_path: row.get(1)?,
                mime_type: row.get(2)?,
                keep_state: row.get(3)?,
                is_group_primary: row.get::<_, i64>(4)? != 0,
                exact_hash: row.get(5)?,
                phash: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

#[derive(Debug, Serialize)]
struct MemberRow {
    id: i64,
    target_path: String,
    mime_type: String,
    keep_state: String,
    is_group_primary: bool,
    exact_hash: String,
    phash: String,
}

fn internal_error(err: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_catalog_db;
    use image::{ImageBuffer, Rgb};
    use serde_json::json;
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;
    use tower::ServiceExt;

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

    #[tokio::test]
    async fn resolve_moves_rejected_to_trash_and_updates_state() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let keep_path = dest.join("2024/06/09/keep.png");
        let reject_path = dest.join("2024/06/09/reject.png");
        fs::create_dir_all(keep_path.parent().unwrap()).unwrap();
        make_png(&keep_path, [255, 0, 0]);
        make_png(&reject_path, [0, 255, 0]);

        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        let keep_path_s = keep_path.to_string_lossy().to_string();
        let reject_path_s = reject_path.to_string_lossy().to_string();
        conn.execute(
            r#"
            INSERT INTO target_items (
                target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, origin_source_id, meta_json
            ) VALUES
                (?1, 1, 'image/png', '2024-06-09T00:00:00Z', 'a', 'p', 64, 32, 32, 1, 'undecided', 1, NULL, '{}'),
                (?2, 1, 'image/png', '2024-06-09T00:00:00Z', 'b', 'p', 64, 32, 32, 1, 'undecided', 0, NULL, '{}')
            "#,
            rusqlite::params![keep_path_s, reject_path_s],
        )
        .unwrap();

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/api/groups/1/resolve")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({"kept":[keep_path_s.clone()], "rejected":[reject_path_s.clone()], "primary": keep_path_s.clone()}).to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(keep_path.exists());
        assert!(!reject_path.exists());
        let moved_path = dest.join(".photo-org/trash/group-1/reject.png");
        assert!(moved_path.exists());

        let conn = open_catalog_db(&db_path).unwrap();
        let keep_state: String = conn
            .query_row(
                "SELECT keep_state FROM target_items WHERE target_path = ?1",
                [keep_path.to_string_lossy().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        let moved_target_path: String = conn
            .query_row(
                "SELECT target_path FROM target_items WHERE id = 2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let reject_state: String = conn
            .query_row(
                "SELECT keep_state FROM target_items WHERE id = 2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(keep_state, "kept");
        assert_eq!(moved_target_path, moved_path.to_string_lossy());
        let resolved_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM operations_log", [], |row| row.get(0))
            .unwrap();
        assert_eq!(resolved_count, 1);
        assert_eq!(reject_state, "rejected");
    }

    #[tokio::test]
    async fn resolve_rejects_primary_outside_group() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let keep_path = dest.join("2024/06/09/keep.png");
        fs::create_dir_all(keep_path.parent().unwrap()).unwrap();
        make_png(&keep_path, [255, 0, 0]);

        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        let keep_path_s = keep_path.to_string_lossy().to_string();
        conn.execute(
            r#"
            INSERT INTO target_items (
                target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, origin_source_id, meta_json
            ) VALUES
                (?1, 1, 'image/png', '2024-06-09T00:00:00Z', 'a', 'p', 64, 32, 32, 1, 'undecided', 1, NULL, '{}')
            "#,
            rusqlite::params![keep_path_s],
        )
        .unwrap();

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/api/groups/1/resolve")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({"kept":[keep_path_s.clone()], "rejected":[], "primary": "/tmp/not-in-group.png"}).to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
