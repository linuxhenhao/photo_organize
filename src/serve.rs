use crate::db::{insert_operation, open_catalog_db};
use crate::interrupt;
use crate::util::{ensure_under_root, safe_file_name};
use anyhow::Result;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{Html, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use image::ImageFormat;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::fs;
use std::future::Future;
use std::io::Cursor;
use std::path::{Path as StdPath, PathBuf};
use tokio::net::TcpListener;

static UGOS_MODE: Lazy<bool> = Lazy::new(detect_ugos_system);

fn detect_ugos_system() -> bool {
    let sentinels = ["/usr/ugreen", "/ugreen", "/etc/sysconfig/thumb_core.sh"];
    sentinels.iter().any(|p| StdPath::new(p).exists())
}

#[derive(Clone)]
struct AppState {
    db_path: PathBuf,
    dest: PathBuf,
}

#[derive(Debug, Serialize)]
struct PagedGroups {
    groups: Vec<GroupSummary>,
    total_groups: usize,
    total_pages: usize,
    current_page: usize,
    limit: usize,
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
    width: i64,
    height: i64,
    size_bytes: i64,
}

#[derive(Debug, Deserialize)]
struct GroupParams {
    page: Option<usize>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ResolveRequest {
    kept: Option<Vec<String>>,
    rejected: Option<Vec<String>>,
    primary: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BulkResolveRequest {
    resolutions: Vec<GroupResolution>,
}

#[derive(Debug, Deserialize)]
struct GroupResolution {
    group_id: i64,
    kept: Vec<String>,
    rejected: Vec<String>,
    primary: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ImageQuery {
    path: String,
    size: Option<u32>,
}

pub async fn run(db: PathBuf, dest: PathBuf, host: String, port: u16) -> Result<()> {
    let state = AppState { db_path: db, dest };
    let app = router(state);
    let listener = TcpListener::bind((host.as_str(), port)).await?;
    tracing::info!(addr = %listener.local_addr()?, "serve listening (UGOS mode: {})", *UGOS_MODE);
    serve_with_shutdown(listener, app, interrupt::wait()).await?;
    Ok(())
}

async fn serve_with_shutdown<F>(listener: TcpListener, app: Router, shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/groups", get(list_groups))
        .route("/api/groups/{id}/resolve", post(resolve_group))
        .route("/api/groups/resolve_bulk", post(resolve_bulk))
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
  <title>photo-org local review</title>
  <style>
    :root {
      --bg: #020617;
      --card-bg: #0f172a;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --primary: #3b82f6;
      --primary-hover: #60a5fa;
      --danger: #ef4444;
      --success: #22c55e;
      --border: #1e293b;
      --star: #f59e0b;
    }
    body { font-family: sans-serif; margin: 0; background: var(--bg); color: var(--text); line-height: 1.5; }
    header { padding: 1rem 2rem; background: #020617; border-bottom: 1px solid var(--border); position: sticky; top: 0; z-index: 10; display: flex; justify-content: space-between; align-items: center; }
    h1 { margin: 0; font-size: 1.25rem; }
    main { padding: 2rem; max-width: 1600px; margin: 0 auto; }
    
    .toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 2rem; background: #1e293b; padding: 1rem 1.5rem; border-radius: 0.75rem; border: 1px solid var(--border); }
    .pagination { display: flex; gap: 0.5rem; align-items: center; }
    .page-btn { padding: 0.5rem 1rem; background: #334155; border: 1px solid var(--border); border-radius: 0.5rem; color: #fff; cursor: pointer; }
    .page-btn:disabled { opacity: 0.3; cursor: not-allowed; }
    .page-btn.active { background: var(--primary); border-color: var(--primary); }
    
    .group { border: 1px solid var(--border); border-radius: 1rem; margin-bottom: 4rem; background: var(--card-bg); overflow: hidden; box-shadow: 0 10px 15px -3px rgba(0,0,0,0.1); }
    .group-header { padding: 1rem 1.5rem; background: #1e293b; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; }
    .group-id { font-weight: bold; font-size: 1.1rem; }
    
    .members { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 1.5rem; padding: 1.5rem; }
    .member { border: 2px solid var(--border); border-radius: 0.75rem; background: #020617; overflow: hidden; display: flex; flex-direction: column; transition: all 0.2s; position: relative; }
    
    .member.rejected .img-container img { filter: brightness(0.5); opacity: 0.8; }
    .member.rejected:hover .img-container img { filter: none; opacity: 1; }
    .member.rejected { border-color: #334155; border-style: dashed; }
    .member.kept { border-color: var(--success); }
    .member.primary { border-color: var(--star); border-style: solid; box-shadow: 0 0 15px rgba(245, 158, 11, 0.2); }
    
    .img-container { aspect-ratio: 1; background: #000; overflow: hidden; cursor: pointer; position: relative; }
    img { width: 100%; height: 100%; object-fit: contain; pointer-events: none; transition: all 0.2s; }
    
    .star-btn { position: absolute; top: 0.75rem; right: 0.75rem; width: 2.5rem; height: 2.5rem; background: rgba(0,0,0,0.6); border-radius: 50%; display: flex; align-items: center; justify-content: center; color: #fff; font-size: 1.5rem; cursor: pointer; z-index: 5; transition: all 0.2s; border: 1px solid rgba(255,255,255,0.2); }
    .star-btn:hover { background: rgba(245, 158, 11, 0.8); transform: scale(1.1); }
    .member.primary .star-btn { color: var(--star); background: rgba(0,0,0,0.8); border-color: var(--star); }
    
    .rejected-icon { position: absolute; top: 0.75rem; left: 0.75rem; width: 2.5rem; height: 2.5rem; background: var(--danger); border-radius: 50%; display: none; align-items: center; justify-content: center; font-size: 1.25rem; color: #fff; font-weight: bold; z-index: 4; pointer-events: none; box-shadow: 0 0 10px rgba(0,0,0,0.5); border: 1px solid rgba(255,255,255,0.2); }
    .member.rejected .rejected-icon { display: flex; }
    .member.rejected:hover .rejected-icon { opacity: 0.2; }

    .member-info { padding: 1rem; flex-grow: 1; font-size: 0.875rem; pointer-events: none; }
    .path { word-break: break-all; margin-bottom: 0.25rem; font-family: monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: bold; }
    .meta { color: var(--text-muted); font-size: 0.75rem; }
    
    .group-footer { padding: 1.25rem 1.5rem; background: #1e293b; border-top: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; }
    .group-stats { font-size: 0.875rem; color: var(--text-muted); }
    .btn-resolve { background: var(--primary); color: white; border: 0; padding: 0.75rem 2.5rem; border-radius: 0.5rem; font-weight: bold; cursor: pointer; transition: background 0.2s; }
    .btn-resolve:hover { background: var(--primary-hover); }
    .btn-resolve:disabled { opacity: 0.5; cursor: not-allowed; }
    
    .btn-bulk { background: var(--success); color: #000; font-weight: bold; padding: 0.75rem 1.5rem; border-radius: 0.5rem; border: 0; cursor: pointer; }
    .btn-bulk:hover { background: #4ade80; }

    .status-badge { padding: 0.25rem 0.75rem; border-radius: 9999px; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; margin-left: 0.5rem; }
    .status-pending { background: #f59e0b; color: #000; }

    #overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.95); display: none; justify-content: center; align-items: center; z-index: 100; cursor: pointer; }
    #overlay img { max-width: 98vw; max-height: 98vh; object-fit: contain; }
  </style>
</head>
<body>
  <header>
    <h1>photo-org <span style="font-weight: 300; opacity: 0.6">local review</span></h1>
    <div id="stats-header"></div>
  </header>
  <main>
    <div id="top-toolbar"></div>
    <div id="groups-container">
        <div style="text-align: center; padding: 10rem;">
            <div style="font-size: 1.5rem; margin-bottom: 1rem;">Loading Groups...</div>
        </div>
    </div>
    <div id="bottom-toolbar"></div>
  </main>
  <div id="overlay"><img src="" alt=""></div>
  <script>
    const groupsContainer = document.getElementById('groups-container');
    const topToolbar = document.getElementById('top-toolbar');
    const bottomToolbar = document.getElementById('bottom-toolbar');
    const overlay = document.getElementById('overlay');
    const overlayImg = overlay.querySelector('img');

    overlay.onclick = () => overlay.style.display = 'none';

    function showOverlay(src) {
      overlayImg.src = src;
      overlay.style.display = 'flex';
    }

    function formatSize(bytes) {
      if (bytes === 0) return '0 B';
      const k = 1024;
      const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    let pagedData = {
        groups: [],
        total_groups: 0,
        total_pages: 0,
        current_page: 1,
        limit: 20
    };

    async function fetchGroups(page = 1) {
      try {
        groupsContainer.innerHTML = '<div style="text-align: center; padding: 10rem;"><div style="font-size: 1.5rem;">Loading Page ' + page + '...</div></div>';
        window.scrollTo(0, 0);

        const resp = await fetch(`/api/groups?page=${page}&limit=${pagedData.limit}`);
        pagedData = await resp.json();
        
        // Initialize UI state
        pagedData.groups.forEach(group => {
            group.members.forEach(m => {
                m.ui_primary = m.is_group_primary;
                // Default logic: only keep if it's already marked 'kept' 
                // OR if it's 'undecided' AND it is the primary version.
                if (m.keep_state === 'undecided') {
                    m.ui_keep = m.ui_primary;
                } else {
                    m.ui_keep = (m.keep_state === 'kept');
                }
            });
        });
        
        renderUI();
      } catch (err) {
        groupsContainer.innerHTML = `<div style="color: var(--danger); padding: 4rem; text-align: center">Error loading groups: ${err.message}</div>`;
      }
    }

    function renderUI() {
      renderToolbar(topToolbar);
      renderGroups();
      renderToolbar(bottomToolbar);
    }

    function renderToolbar(container) {
      if (pagedData.total_groups === 0) {
        container.innerHTML = '';
        return;
      }

      container.innerHTML = `
        <div class="toolbar">
          <div class="pagination">
            <button class="page-btn" ${pagedData.current_page <= 1 ? 'disabled' : ''} onclick="fetchGroups(${pagedData.current_page - 1})">Prev</button>
            <span style="margin: 0 1rem">Page <strong>${pagedData.current_page}</strong> of ${pagedData.total_pages} (${pagedData.total_groups} groups)</span>
            <button class="page-btn" ${pagedData.current_page >= pagedData.total_pages ? 'disabled' : ''} onclick="fetchGroups(${pagedData.current_page + 1})">Next</button>
          </div>
          <button class="btn-bulk" onclick="confirmAllOnPage()">Confirm All on This Page</button>
        </div>
      `;
    }

    function renderGroups() {
      if (pagedData.groups.length === 0) {
        groupsContainer.innerHTML = '<div style="text-align: center; padding: 10rem;"><div style="font-size: 1.5rem; color: var(--success)">All clear!</div><div style="color: var(--text-muted)">No pending duplicate groups found.</div></div>';
        return;
      }

      groupsContainer.innerHTML = '';
      pagedData.groups.forEach(group => {
        const groupEl = document.createElement('div');
        groupEl.className = 'group';
        
        const header = document.createElement('div');
        header.className = 'group-header';
        header.innerHTML = `
          <div class="group-id">Group ID: ${group.group_id} <span class="status-badge status-pending">Pending</span></div>
          <div style="font-size: 0.875rem; opacity: 0.7">${group.members.length} versions</div>
        `;
        groupEl.appendChild(header);

        const membersList = document.createElement('div');
        membersList.className = 'members';
        
        group.members.forEach(member => {
          const memberEl = document.createElement('div');
          memberEl.className = 'member' + (member.ui_primary ? ' primary' : '') + (member.ui_keep ? ' kept' : ' rejected');
          memberEl.id = `member-${member.id}`;
          
          const imgPath = encodeURIComponent(member.target_path);
          const thumbSrc = `/image?path=${imgPath}&size=400`;
          const fullSrc = `/image?path=${imgPath}&size=1600`;

          memberEl.innerHTML = `
            <div class="img-container" onclick="handleImageClick(event, ${group.group_id}, ${member.id})">
              <div class="star-btn" onclick="handleStarClick(event, ${group.group_id}, ${member.id})" title="Set as Primary">★</div>
              <div class="rejected-icon">✕</div>
              <img src="${thumbSrc}" loading="lazy">
            </div>
            <div class="member-info">
              <div class="path" title="${member.target_path}">${member.target_path.split('/').pop()}</div>
              <div class="meta">
                ${member.width} × ${member.height} • ${formatSize(member.size_bytes)}<br>
                ${member.mime_type}
              </div>
            </div>
          `;
          membersList.appendChild(memberEl);
        });
        groupEl.appendChild(membersList);

        const keptCount = group.members.filter(m => m.ui_keep).length;
        const rejectedCount = group.members.filter(m => !m.ui_keep).length;

        const footer = document.createElement('div');
        footer.className = 'group-footer';
        footer.innerHTML = `
            <div class="group-stats">
                Keeping <strong>${keptCount}</strong>, discarding <strong>${rejectedCount}</strong>
            </div>
        `;
        const resolveBtn = document.createElement('button');
        resolveBtn.className = 'btn-resolve';
        resolveBtn.innerText = 'Confirm Decisions';
        resolveBtn.onclick = () => resolveGroup(group.group_id);
        footer.appendChild(resolveBtn);
        groupEl.appendChild(footer);

        groupsContainer.appendChild(groupEl);
      });
    }

    function handleImageClick(event, groupId, memberId) {
      const group = pagedData.groups.find(g => g.group_id === groupId);
      const member = group.members.find(m => m.id === memberId);
      if (member.ui_primary && member.ui_keep) return;
      member.ui_keep = !member.ui_keep;
      renderUI();
    }

    function handleStarClick(event, groupId, memberId) {
      event.stopPropagation();
      const group = pagedData.groups.find(g => g.group_id === groupId);
      group.members.forEach(m => {
          m.ui_primary = (m.id === memberId);
          if (m.ui_primary) m.ui_keep = true;
      });
      renderUI();
    }

    async function resolveGroup(groupId) {
      const group = pagedData.groups.find(g => g.group_id === groupId);
      const kept = group.members.filter(m => m.ui_keep).map(m => m.target_path);
      const rejected = group.members.filter(m => !m.ui_keep).map(m => m.target_path);
      const primary = group.members.find(m => m.ui_primary)?.target_path;

      try {
        const resp = await fetch(`/api/groups/${groupId}/resolve`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ kept, rejected, primary })
        });
        
        if (resp.ok) {
          pagedData.groups = pagedData.groups.filter(g => g.group_id !== groupId);
          if (pagedData.groups.length === 0) {
            fetchGroups(pagedData.current_page);
          } else {
            renderUI();
          }
        } else {
          alert('Failed to resolve: ' + await resp.text());
        }
      } catch (err) {
        alert('Error: ' + err.message);
      }
    }

    async function confirmAllOnPage() {
      const resolutions = pagedData.groups.map(group => ({
          group_id: group.group_id,
          kept: group.members.filter(m => m.ui_keep).map(m => m.target_path),
          rejected: group.members.filter(m => !m.ui_keep).map(m => m.target_path),
          primary: group.members.find(m => m.ui_primary)?.target_path
      }));

      if (!confirm(`Confirm all ${resolutions.length} groups on this page?`)) return;

      try {
        const resp = await fetch('/api/groups/resolve_bulk', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ resolutions })
        });
        
        if (resp.ok) {
          fetchGroups(pagedData.current_page);
        } else {
          alert('Bulk resolve failed: ' + await resp.text());
        }
      } catch (err) {
        alert('Error: ' + err.message);
      }
    }

    fetchGroups();
  </script>
</body>
</html>"#,
    )
}

async fn list_groups(
    State(state): State<AppState>,
    Query(params): Query<GroupParams>,
) -> Result<Json<PagedGroups>, (StatusCode, String)> {
    let conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let limit = params.limit.unwrap_or(20);
    let page = params.page.unwrap_or(1).max(1);

    let total_groups: i64 = conn
        .query_row(
            r#"
        SELECT COUNT(DISTINCT group_id)
        FROM target_items
        WHERE group_id IS NOT NULL
        AND group_id IN (
            SELECT group_id FROM target_items WHERE keep_state = 'undecided'
        )
        "#,
            [],
            |row| row.get(0),
        )
        .map_err(internal_error)?;
    let total_groups = total_groups as usize;

    let total_pages = (total_groups + limit - 1) / limit;
    let offset = (page - 1) * limit;

    let mut stmt = conn
        .prepare(
            r#"
        SELECT group_id
        FROM target_items
        WHERE group_id IS NOT NULL
        GROUP BY group_id
        HAVING SUM(CASE WHEN keep_state = 'undecided' THEN 1 ELSE 0 END) > 0
        ORDER BY group_id
        LIMIT ?1 OFFSET ?2
        "#,
        )
        .map_err(internal_error)?;

    let ids = stmt
        .query_map(rusqlite::params![limit as i64, offset as i64], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(internal_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(internal_error)?;

    let mut groups = Vec::new();
    for id in ids {
        let members = load_group_members(&conn, id).map_err(internal_error)?;
        groups.push(GroupSummary {
            group_id: id,
            status: "pending".to_string(),
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
                    width: m.width,
                    height: m.height,
                    size_bytes: m.size_bytes,
                })
                .collect(),
        });
    }

    Ok(Json(PagedGroups {
        groups,
        total_groups,
        total_pages,
        current_page: page,
        limit,
    }))
}

async fn resolve_bulk(
    State(state): State<AppState>,
    Json(request): Json<BulkResolveRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let tx = conn.transaction().map_err(internal_error)?;

    for res in request.resolutions {
        let group = load_group_members(&tx, res.group_id).map_err(internal_error)?;
        let kept_set: HashSet<_> = res.kept.iter().collect();
        let rejected_set: HashSet<_> = res.rejected.iter().collect();

        let mut moved_paths = Vec::new();
        for member in &group {
            if rejected_set.contains(&member.target_path) {
                let moved_to = move_to_trash(&state.dest, &member.target_path, res.group_id)
                    .map_err(internal_error)?;
                moved_paths.push((member.id, member.target_path.clone(), moved_to));
            }
        }

        for member in &group {
            let keep_state = if kept_set.contains(&member.target_path) {
                "kept"
            } else if rejected_set.contains(&member.target_path) {
                "rejected"
            } else {
                "undecided"
            };
            let is_primary = res
                .primary
                .as_ref()
                .map(|p| p == &member.target_path)
                .unwrap_or(false);
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
            &json!({"group_id": res.group_id, "kept": res.kept, "rejected": res.rejected, "primary": res.primary}).to_string(),
        ).map_err(internal_error)?;
    }

    tx.commit().map_err(internal_error)?;
    Ok(Json(json!({"status": "ok"})))
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

    // 1. Try UGOS thumbnail if enabled
    if *UGOS_MODE {
        if let Some(thumb_path) = resolve_ugos_thumb(&path) {
            if let Ok(bytes) = fs::read(&thumb_path) {
                let mime = infer::get(&bytes)
                    .map(|k| k.mime_type())
                    .unwrap_or("image/jpeg");
                let mut resp = Response::new(axum::body::Body::from(bytes));
                resp.headers_mut().insert(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_str(mime).map_err(internal_error)?,
                );
                return Ok(resp);
            }
        }
    }

    let requested_size = query.size.unwrap_or(1600);

    // 2. Fast path for small browser-safe images
    if requested_size >= 1600 {
        if let Ok(bytes) = fs::read(&path) {
            let mime = infer::get(&bytes)
                .map(|k| k.mime_type())
                .unwrap_or("application/octet-stream");
            if bytes.len() < 5 * 1024 * 1024
                && matches!(
                    mime,
                    "image/jpeg" | "image/png" | "image/webp" | "image/gif"
                )
            {
                let mut resp = Response::new(axum::body::Body::from(bytes));
                resp.headers_mut().insert(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_str(mime).map_err(internal_error)?,
                );
                return Ok(resp);
            }
        }
    }

    // 3. Fallback to on-demand resize (blocking)
    let preview = tokio::task::spawn_blocking(move || {
        let bytes = fs::read(&path).map_err(anyhow::Error::from)?;

        let img = if let Ok(img) = image::load_from_memory(&bytes) {
            img
        } else {
            // Try RAW preview
            use rsraw::{RawImage, ThumbFormat};
            let mut raw =
                RawImage::open(&bytes).map_err(|e| anyhow::anyhow!("rsraw open error: {}", e))?;
            let thumbs = raw
                .extract_thumbs()
                .map_err(|e| anyhow::anyhow!("rsraw extract error: {}", e))?;
            let thumb = thumbs
                .iter()
                .find(|t| matches!(t.format, ThumbFormat::Jpeg))
                .ok_or_else(|| anyhow::anyhow!("no jpeg preview found in raw"))?;
            image::load_from_memory(&thumb.data).map_err(anyhow::Error::from)?
        };

        let preview = img.thumbnail(requested_size, requested_size);
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

fn resolve_ugos_thumb(path: &StdPath) -> Option<PathBuf> {
    let thumb_dir = xattr::get(path, "user.thumb.dir").ok()??;
    let thumb_dir_str = String::from_utf8(thumb_dir).ok()?;
    let thumb_id = xattr::get(path, "user.thumb.id").ok()??;
    let thumb_id_str = String::from_utf8(thumb_id).ok()?;
    let stem = thumb_id_str.split('-').next()?.trim();
    if stem.is_empty() {
        return None;
    }

    let candidates = [
        "_640_40.webp",
        "_640_40.jpg",
        "_320_40.webp",
        "_320_40.jpg",
        "_mini.webp",
        "_mini.jpg",
        "_1600_40.webp",
        "_1600_40.jpg",
    ];

    for suffix in candidates {
        let p = StdPath::new(&thumb_dir_str).join(format!("{}{}", stem, suffix));
        if p.exists() {
            return Some(p);
        }
    }
    None
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

fn load_group_members(conn: &rusqlite::Connection, group_id: i64) -> Result<Vec<MemberRow>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, target_path, mime_type, keep_state, is_group_primary, exact_hash, phash, width, height, size_bytes
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
                width: row.get(7)?,
                height: row.get(8)?,
                size_bytes: row.get(9)?,
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
    width: i64,
    height: i64,
    size_bytes: i64,
}

fn internal_error(err: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_catalog_db;
    use crate::interrupt;
    use image::{ImageBuffer, Rgb};
    use serde_json::json;
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};
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

    #[tokio::test]
    async fn serve_stops_when_shutdown_future_resolves() {
        interrupt::reset();

        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        open_catalog_db(&db_path).unwrap();

        let state = AppState { db_path, dest };
        let app = router(state);
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let server = tokio::spawn(async move {
            serve_with_shutdown(listener, app, async move {
                let _ = shutdown_rx.await;
            })
            .await
        });

        let response = reqwest::get(format!("http://{addr}/")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        shutdown_tx.send(()).unwrap();

        let result = timeout(Duration::from_secs(2), server).await;
        assert!(result.is_ok(), "server did not stop after shutdown signal");
        let join = result.unwrap().unwrap();
        assert!(join.is_ok(), "server exited with error: {join:?}");
    }

    #[tokio::test]
    async fn serve_stops_when_interrupt_is_requested() {
        interrupt::reset();

        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        open_catalog_db(&db_path).unwrap();

        let server = tokio::spawn(run(db_path, dest, "127.0.0.1".to_string(), 0));

        tokio::time::sleep(Duration::from_millis(100)).await;
        interrupt::request_for_test();

        let result = timeout(Duration::from_secs(2), server).await;
        interrupt::reset();
        assert!(result.is_ok(), "serve did not stop after interrupt");
        let join = result.unwrap().unwrap();
        assert!(join.is_ok(), "serve exited with error: {join:?}");
    }
}
