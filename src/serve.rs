use crate::db::{insert_operation, open_catalog_db};
use crate::interrupt;
use crate::util::{best_effort_mime, ensure_under_root, safe_file_name};
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
use std::path::{Component, Path as StdPath, PathBuf};
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
    page_index: usize,
    page_size: usize,
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
    page_index: Option<usize>,
    page_size: Option<usize>,
    page: Option<usize>,
    limit: Option<usize>,
}

const DEFAULT_PAGE_SIZE: usize = 20;
const MAX_PAGE_SIZE: usize = 1000;

#[derive(Debug, Serialize)]
struct PagingRequest {
    page_index: usize,
    page_size: usize,
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

fn normalize_group_params(params: &GroupParams) -> PagingRequest {
    let page_size = params
        .page_size
        .or(params.limit)
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let page_index = params
        .page_index
        .unwrap_or_else(|| params.page.unwrap_or(1).saturating_sub(1));
    PagingRequest {
        page_index,
        page_size,
    }
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/groups", get(list_groups))
        .route("/api/groups/{id}/resolve", post(resolve_group))
        .route("/api/groups/resolve_bulk", post(resolve_bulk))
        .route("/api/groups/{id}/archive", get(archive_group))
        .route(
            "/api/groups/{group_id}/members/{member_id}/delete_trash",
            post(delete_trash_member),
        )
        .route("/image", get(image))
        .with_state(state)
}

async fn index(Query(params): Query<GroupParams>) -> Html<String> {
    let initial_paging = serde_json::to_string(&normalize_group_params(&params))
        .expect("paging request should serialize");
    let html = r#"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>photo-org local review</title>
  <style>
    :root {
      --bg: #020617;
      --bg-elevated: #081121;
      --card-bg: #0f172a;
      --surface: #1e293b;
      --surface-strong: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --primary: #3b82f6;
      --primary-hover: #60a5fa;
      --danger: #ef4444;
      --danger-bg: rgba(239, 68, 68, 0.18);
      --success: #22c55e;
      --success-bg: rgba(34, 197, 94, 0.18);
      --border: #1e293b;
      --star: #f59e0b;
      --star-bg: rgba(245, 158, 11, 0.18);
      --shadow: 0 18px 50px rgba(0, 0, 0, 0.28);
    }
    * { box-sizing: border-box; }
    body { font-family: sans-serif; margin: 0; background: radial-gradient(circle at top, #0b1730 0, var(--bg) 22rem); color: var(--text); line-height: 1.5; }
    button, input { font: inherit; }
    button { cursor: pointer; }
    header { padding: 1rem 1.5rem; background: rgba(2, 6, 23, 0.92); backdrop-filter: blur(16px); border-bottom: 1px solid rgba(148, 163, 184, 0.12); position: sticky; top: 0; z-index: 10; display: flex; justify-content: space-between; align-items: center; gap: 1rem; }
    h1 { margin: 0; font-size: 1.15rem; }
    .brand-subtitle { font-weight: 300; opacity: 0.6; }
    .header-stats { display: flex; gap: 0.75rem; flex-wrap: wrap; justify-content: flex-end; align-items: center; }
    .header-stats-desktop { display: flex; gap: 0.75rem; flex-wrap: wrap; justify-content: flex-end; }
    .header-stats-mobile { display: none; }
    .header-pill { padding: 0.45rem 0.8rem; border-radius: 999px; background: rgba(15, 23, 42, 0.88); border: 1px solid rgba(148, 163, 184, 0.15); color: var(--text-muted); font-size: 0.8125rem; white-space: nowrap; }
    main { padding: 1.5rem; max-width: 1600px; margin: 0 auto; }

    .toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem; background: rgba(15, 23, 42, 0.92); padding: 1rem 1.25rem; border-radius: 1rem; border: 1px solid rgba(148, 163, 184, 0.12); gap: 1rem; flex-wrap: wrap; box-shadow: var(--shadow); }
    .toolbar-block { display: flex; gap: 0.75rem; align-items: center; flex-wrap: wrap; }
    .pagination { display: flex; gap: 0.75rem; align-items: center; flex-wrap: wrap; }
    .pagination-status { color: var(--text-muted); font-size: 0.9375rem; }
    .pagination-meta { display: flex; gap: 1rem; align-items: center; color: var(--text-muted); font-size: 0.875rem; flex-wrap: wrap; }
    .page-btn, .btn-resolve, .btn-bulk, .member-action { min-height: 2.75rem; border-radius: 0.75rem; border: 1px solid transparent; transition: background 0.2s, border-color 0.2s, color 0.2s, transform 0.2s; }
    .page-btn { padding: 0.6rem 1rem; background: var(--surface-strong); border-color: rgba(148, 163, 184, 0.12); color: #fff; }
    .page-btn:hover:not(:disabled) { background: #475569; }
    .page-btn:disabled { opacity: 0.35; cursor: not-allowed; }
    .page-size-label { display: flex; align-items: center; gap: 0.5rem; }
    .page-input { width: 5.5rem; padding: 0.55rem 0.7rem; background: var(--bg-elevated); border: 1px solid rgba(148, 163, 184, 0.16); border-radius: 0.75rem; color: #fff; }

    .group { border: 1px solid rgba(148, 163, 184, 0.12); border-radius: 1.25rem; margin-bottom: 2rem; background: rgba(15, 23, 42, 0.94); overflow: hidden; box-shadow: var(--shadow); }
    .group-header { padding: 1rem 1.25rem; background: rgba(30, 41, 59, 0.9); border-bottom: 1px solid rgba(148, 163, 184, 0.12); display: flex; justify-content: space-between; align-items: center; gap: 1rem; }
    .group-heading { display: flex; align-items: center; gap: 0.75rem; flex-wrap: wrap; }
    .group-id { font-weight: bold; font-size: 1.05rem; }
    .group-summary { font-size: 0.875rem; color: var(--text-muted); }

    .members { display: grid; grid-template-columns: repeat(auto-fit, minmax(290px, 1fr)); gap: 1.25rem; padding: 1.25rem; }
    .member { border: 2px solid rgba(148, 163, 184, 0.12); border-radius: 1rem; background: var(--bg-elevated); overflow: hidden; display: flex; flex-direction: column; transition: border-color 0.2s, box-shadow 0.2s; position: relative; min-width: 0; }
    .member.rejected { border-color: rgba(148, 163, 184, 0.18); border-style: dashed; }
    .member.kept { border-color: rgba(34, 197, 94, 0.55); }
    .member.primary { border-color: rgba(245, 158, 11, 0.72); box-shadow: 0 0 0 1px rgba(245, 158, 11, 0.18), 0 14px 35px rgba(245, 158, 11, 0.12); }
    .member.rejected .img-container img { filter: brightness(0.52); opacity: 0.82; }
    .member.rejected:hover .img-container img { filter: none; opacity: 1; }

    .img-container { aspect-ratio: 1; background: #000; overflow: hidden; cursor: pointer; position: relative; }
    img { width: 100%; height: 100%; object-fit: contain; pointer-events: none; transition: all 0.2s; }
    .img-container::after { content: "Click image to keep/reject"; position: absolute; right: 0.75rem; bottom: 0.75rem; padding: 0.35rem 0.6rem; border-radius: 999px; background: rgba(2, 6, 23, 0.72); border: 1px solid rgba(148, 163, 184, 0.18); color: #e2e8f0; font-size: 0.75rem; letter-spacing: 0.01em; }
    .preview-badge { position: absolute; top: 0.75rem; right: 0.75rem; z-index: 2; min-height: auto; padding: 0.4rem 0.7rem; border-radius: 999px; border: 1px solid rgba(148, 163, 184, 0.18); background: rgba(2, 6, 23, 0.78); color: #e2e8f0; font-size: 0.75rem; font-weight: 700; letter-spacing: 0.01em; }
    .preview-badge:hover { background: rgba(15, 23, 42, 0.92); }

    .member-status { position: absolute; top: 0.75rem; left: 0.75rem; display: inline-flex; align-items: center; gap: 0.35rem; padding: 0.35rem 0.65rem; border-radius: 999px; font-size: 0.75rem; font-weight: 700; letter-spacing: 0.03em; text-transform: uppercase; z-index: 2; }
    .member-status.primary { background: var(--star-bg); color: #fcd34d; border: 1px solid rgba(245, 158, 11, 0.35); }
    .member-status.kept { background: var(--success-bg); color: #86efac; border: 1px solid rgba(34, 197, 94, 0.3); }
    .member-status.rejected { background: var(--danger-bg); color: #fca5a5; border: 1px solid rgba(239, 68, 68, 0.3); }

    .member-info { padding: 1rem; display: flex; flex-direction: column; gap: 0.8rem; flex-grow: 1; }
    .path { word-break: break-word; margin: 0; font-family: monospace; font-weight: bold; color: #e2e8f0; }
    .meta-grid { display: flex; flex-wrap: wrap; gap: 0.5rem 1rem; color: var(--text-muted); font-size: 0.8rem; }
    .member-actions { display: grid; grid-template-columns: repeat(auto-fit, minmax(100px, 1fr)); gap: 0.6rem; margin-top: auto; }
    .member-action { padding: 0.65rem 0.75rem; background: rgba(30, 41, 59, 0.9); color: var(--text); border-color: rgba(148, 163, 184, 0.14); font-weight: 600; font-size: 0.8125rem; }
    .member-action:hover:not(:disabled) { background: #334155; transform: translateY(-1px); }
    .member-action:disabled { opacity: 0.45; cursor: not-allowed; }
    .member-action.keep.active { background: var(--success-bg); border-color: rgba(34, 197, 94, 0.4); color: #bbf7d0; }
    .member-action.reject.active { background: var(--danger-bg); border-color: rgba(239, 68, 68, 0.4); color: #fecaca; }
    .member-action.primary.active { background: var(--star-bg); border-color: rgba(245, 158, 11, 0.4); color: #fde68a; }

    .group-footer { padding: 1rem 1.25rem 1.25rem; background: rgba(30, 41, 59, 0.72); border-top: 1px solid rgba(148, 163, 184, 0.12); display: flex; justify-content: space-between; align-items: center; gap: 1rem; }
    .group-stats { font-size: 0.9rem; color: var(--text-muted); }
    .btn-resolve { background: var(--primary); color: white; padding: 0.75rem 1.5rem; font-weight: bold; }
    .btn-resolve:hover { background: var(--primary-hover); }
    .btn-resolve:disabled { opacity: 0.5; cursor: not-allowed; }

    .btn-bulk { background: var(--success); color: #04110a; font-weight: bold; padding: 0.75rem 1.1rem; }
    .btn-bulk:hover { background: #4ade80; }

    .status-badge { padding: 0.28rem 0.7rem; border-radius: 9999px; font-size: 0.72rem; font-weight: 700; text-transform: uppercase; background: rgba(245, 158, 11, 0.16); color: #fcd34d; border: 1px solid rgba(245, 158, 11, 0.3); }
    .empty-state { text-align: center; padding: clamp(3rem, 12vw, 8rem) 1rem; }
    .empty-title { font-size: clamp(1.2rem, 4vw, 1.5rem); margin-bottom: 0.5rem; }
    .empty-copy { color: var(--text-muted); max-width: 30rem; margin: 0 auto; }
    .error-copy { color: #fecaca; }

    #overlay { position: fixed; inset: 0; background: rgba(0, 0, 0, 0.95); display: none; justify-content: center; align-items: center; z-index: 100; cursor: pointer; padding: 1rem; }
    #overlay img { max-width: 98vw; max-height: 94vh; object-fit: contain; }

    @media (hover: none), (pointer: coarse) {
      .img-container { cursor: zoom-in; }
      .img-container::after { content: "Tap image to preview"; }
    }

    @media (max-width: 720px) {
      header { padding: 0.8rem 1rem; align-items: center; gap: 0.75rem; }
      h1 { font-size: 1rem; }
      .header-stats { justify-content: flex-end; min-width: 0; flex: 1; }
      .header-stats-desktop { display: none; }
      .header-stats-mobile { display: block; min-width: 0; }
      .header-pill { padding: 0.35rem 0.65rem; font-size: 0.74rem; max-width: 100%; overflow: hidden; text-overflow: ellipsis; }
      main { padding: 1rem; }
      .toolbar, .group-header, .group-footer { padding: 1rem; }
      .toolbar { margin-bottom: 1rem; }
      .toolbar-block, .pagination, .pagination-meta { width: 100%; }
      .pagination { justify-content: space-between; }
      .pagination-status { width: 100%; }
      .page-btn, .btn-bulk, .btn-resolve { width: 100%; }
      .page-size-label { width: 100%; justify-content: space-between; }
      .page-input { width: 6rem; }
      .group-header, .group-footer { flex-direction: column; align-items: stretch; }
      .members { grid-template-columns: 1fr; gap: 1rem; padding: 1rem; }
      .member-info { padding: 0.9rem; }
      .member-actions { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .img-container::after { content: "Tap image to preview"; }
    }
  </style>
</head>
<body>
  <header>
    <h1>photo-org <span class="brand-subtitle">local review</span></h1>
    <div id="stats-header"></div>
  </header>
  <main>
    <div id="top-toolbar"></div>
    <div id="groups-container">
      <div class="empty-state">
        <div class="empty-title">Loading groups...</div>
      </div>
    </div>
    <div id="bottom-toolbar"></div>
  </main>
  <div id="overlay"><img src="" alt="Full-size preview"></div>
  <script>
    const groupsContainer = document.getElementById('groups-container');
    const topToolbar = document.getElementById('top-toolbar');
    const bottomToolbar = document.getElementById('bottom-toolbar');
    const statsHeader = document.getElementById('stats-header');
    const overlay = document.getElementById('overlay');
    const overlayImg = overlay.querySelector('img');

    overlay.onclick = () => overlay.style.display = 'none';

    function showOverlay(src) {
      overlayImg.src = src;
      overlay.style.display = 'flex';
    }

    function usePreviewFirstImageClick() {
      return window.matchMedia('(max-width: 720px), (hover: none), (pointer: coarse)').matches;
    }

    function formatSize(bytes) {
      if (bytes === 0) return '0 B';
      const k = 1024;
      const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    function loadingMarkup(message) {
      return `<div class="empty-state"><div class="empty-title">${message}</div></div>`;
    }

    function emptyMarkup(title, subtitle, extraClass = '') {
      return `
        <div class="empty-state">
          <div class="empty-title ${extraClass}">${title}</div>
          ${subtitle ? `<div class="empty-copy ${extraClass}">${subtitle}</div>` : ''}
        </div>
      `;
    }

    let pagedData = {
      groups: [],
      total_groups: 0,
      total_pages: 0,
      page_index: 0,
      page_size: 20,
      current_page: 1,
      limit: 20
    };

    function syncBrowserUrl() {
      const url = new URL(window.location.href);
      url.searchParams.set('page_index', String(pagedData.page_index));
      url.searchParams.set('page_size', String(pagedData.page_size));
      url.searchParams.delete('page');
      url.searchParams.delete('limit');
      window.history.replaceState(null, '', url);
    }

    function renderHeaderStats() {
      if (pagedData.total_groups === 0) {
        statsHeader.innerHTML = '<div class="header-stats"><div class="header-pill">No pending groups</div></div>';
        return;
      }
      const visibleMembers = pagedData.groups.reduce((sum, group) => sum + group.members.length, 0);
      statsHeader.innerHTML = `
        <div class="header-stats">
          <div class="header-stats-desktop">
            <div class="header-pill">${pagedData.total_groups} pending groups</div>
            <div class="header-pill">Page ${pagedData.current_page} / ${Math.max(pagedData.total_pages, 1)}</div>
            <div class="header-pill">${visibleMembers} items on screen</div>
          </div>
          <div class="header-stats-mobile">
            <div class="header-pill">${pagedData.total_groups} groups • ${visibleMembers} items • p${pagedData.current_page}/${Math.max(pagedData.total_pages, 1)}</div>
          </div>
        </div>
      `;
    }

    async function fetchGroups(pageIndex = pagedData.page_index, pageSize = pagedData.page_size) {
      try {
        groupsContainer.innerHTML = loadingMarkup(`Loading page ${pageIndex + 1}...`);
        window.scrollTo(0, 0);

        const resp = await fetch(`/api/groups?page_index=${pageIndex}&page_size=${pageSize}`);
        pagedData = await resp.json();
        syncBrowserUrl();

        pagedData.groups.forEach(group => {
          group.members.forEach(member => {
            member.ui_primary = member.is_group_primary;
            if (member.keep_state === 'undecided') {
              member.ui_keep = member.ui_primary;
            } else {
              member.ui_keep = member.keep_state === 'kept';
            }
          });
        });

        renderUI();
      } catch (err) {
        groupsContainer.innerHTML = emptyMarkup(`Error loading groups`, err.message, 'error-copy');
      }
    }

    function renderUI() {
      renderHeaderStats();
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
          <div class="toolbar-block pagination">
            <button class="page-btn" ${pagedData.page_index <= 0 ? 'disabled' : ''} onclick="fetchGroups(${pagedData.page_index - 1}, ${pagedData.page_size})">Prev page</button>
            <div class="pagination-status">Page <strong>${pagedData.current_page}</strong> of ${pagedData.total_pages} with ${pagedData.total_groups} pending groups</div>
            <button class="page-btn" ${pagedData.page_index + 1 >= pagedData.total_pages ? 'disabled' : ''} onclick="fetchGroups(${pagedData.page_index + 1}, ${pagedData.page_size})">Next page</button>
          </div>
          <div class="toolbar-block pagination-meta">
            <label class="page-size-label">page_index <input class="page-input" id="${container.id}-page-index" type="number" min="0" max="${Math.max(pagedData.total_pages - 1, 0)}" value="${pagedData.page_index}" onchange="changePageIndex(this.value)"></label>
            <label class="page-size-label">page_size <input class="page-input" id="${container.id}-page-size" type="number" min="1" max="1000" value="${pagedData.page_size}" onchange="changePageSize(this.value)"></label>
          </div>
          <button class="btn-bulk" onclick="confirmAllOnPage()">Confirm all on this page</button>
        </div>
      `;
    }

    function changePageIndex(value) {
      const nextIndex = Number.parseInt(value, 10);
      if (!Number.isFinite(nextIndex) || nextIndex < 0) {
        renderUI();
        return;
      }
      const maxIndex = Math.max(pagedData.total_pages - 1, 0);
      fetchGroups(Math.min(nextIndex, maxIndex), pagedData.page_size);
    }

    function changePageSize(value) {
      const nextSize = Number.parseInt(value, 10);
      if (!Number.isFinite(nextSize) || nextSize <= 0) {
        renderUI();
        return;
      }
      fetchGroups(0, nextSize);
    }

    function setKeepState(groupId, memberId, keep) {
      const group = pagedData.groups.find(g => g.group_id === groupId);
      const member = group.members.find(m => m.id === memberId);
      if (!keep && member.ui_primary) return;
      member.ui_keep = keep;
      renderUI();
    }

    function toggleKeepState(groupId, memberId) {
      const group = pagedData.groups.find(g => g.group_id === groupId);
      const member = group.members.find(m => m.id === memberId);
      if (member.ui_primary && member.ui_keep) return;
      member.ui_keep = !member.ui_keep;
      renderUI();
    }

    function setPrimary(groupId, memberId) {
      const group = pagedData.groups.find(g => g.group_id === groupId);
      group.members.forEach(member => {
        member.ui_primary = member.id === memberId;
        if (member.ui_primary) member.ui_keep = true;
      });
      renderUI();
    }

    function handleImageClick(groupId, memberId, previewSrc) {
      if (usePreviewFirstImageClick()) {
        showOverlay(previewSrc);
        return;
      }
      toggleKeepState(groupId, memberId);
    }

    function handlePreviewClick(event, previewSrc) {
      event.stopPropagation();
      showOverlay(previewSrc);
    }

    function isTrashMember(member) {
      return member.target_path.includes('/.photo-org/trash/');
    }

    async function deleteTrashMember(groupId, memberId, fileName) {
      if (!confirm(`Permanently delete trash file ${fileName}?`)) return;

      try {
        const resp = await fetch(`/api/groups/${groupId}/members/${memberId}/delete_trash`, {
          method: 'POST'
        });

        if (resp.ok) {
          fetchGroups(pagedData.page_index, pagedData.page_size);
        } else {
          alert('Failed to delete trash file: ' + await resp.text());
        }
      } catch (err) {
        alert('Error: ' + err.message);
      }
    }

    function renderGroups() {
      if (pagedData.groups.length === 0) {
        groupsContainer.innerHTML = emptyMarkup('All clear!', 'No pending duplicate groups found.');
        return;
      }

      groupsContainer.innerHTML = '';
      pagedData.groups.forEach(group => {
        const groupEl = document.createElement('div');
        groupEl.className = 'group';

        const header = document.createElement('div');
        header.className = 'group-header';
        header.innerHTML = `
          <div class="group-heading">
            <div class="group-id">Group ${group.group_id}</div>
            <span class="status-badge">Pending</span>
          </div>
          <div class="group-summary">${group.members.length} versions to compare</div>
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
          const fileName = member.target_path.split('/').pop();
          const deleteButton = isTrashMember(member)
            ? `<button class="member-action reject" onclick="deleteTrashMember(${group.group_id}, ${member.id}, ${JSON.stringify(fileName)})">Delete trash file</button>`
            : '';
          const statusClass = member.ui_primary ? 'primary' : (member.ui_keep ? 'kept' : 'rejected');
          const statusLabel = member.ui_primary ? 'Primary' : (member.ui_keep ? 'Keep' : 'Reject');

          memberEl.innerHTML = `
            <div class="img-container" onclick="handleImageClick(${group.group_id}, ${member.id}, '${fullSrc}')">
              <div class="member-status ${statusClass}">${statusLabel}</div>
              <button class="preview-badge" type="button" onclick="handlePreviewClick(event, '${fullSrc}')" aria-label="Preview ${fileName}">Preview</button>
              <img src="${thumbSrc}" loading="lazy" alt="${fileName}">
            </div>
            <div class="member-info">
              <p class="path" title="${member.target_path}">${fileName}</p>
              <div class="meta-grid">
                <span>${member.width} × ${member.height}</span>
                <span>${formatSize(member.size_bytes)}</span>
                <span>${member.mime_type}</span>
              </div>
              <div class="member-actions">
                <button class="member-action keep ${member.ui_keep ? 'active' : ''}" onclick="setKeepState(${group.group_id}, ${member.id}, true)">Keep</button>
                <button class="member-action reject ${!member.ui_keep ? 'active' : ''}" onclick="setKeepState(${group.group_id}, ${member.id}, false)" ${member.ui_primary ? 'disabled' : ''}>Reject</button>
                <button class="member-action primary ${member.ui_primary ? 'active' : ''}" onclick="setPrimary(${group.group_id}, ${member.id})">Primary</button>
                <button class="member-action" onclick="showOverlay('${fullSrc}')">Preview</button>
                ${deleteButton}
              </div>
            </div>
          `;
          membersList.appendChild(memberEl);
        });
        groupEl.appendChild(membersList);

        const keptCount = group.members.filter(member => member.ui_keep).length;
        const rejectedCount = group.members.filter(member => !member.ui_keep).length;

        const footer = document.createElement('div');
        footer.className = 'group-footer';
        footer.innerHTML = `
          <div class="group-stats">
            Keeping <strong>${keptCount}</strong>, discarding <strong>${rejectedCount}</strong>
          </div>
        `;
        const resolveBtn = document.createElement('button');
        resolveBtn.className = 'btn-resolve';
        resolveBtn.innerText = 'Confirm decisions';
        resolveBtn.onclick = () => resolveGroup(group.group_id);
        footer.appendChild(resolveBtn);
        groupEl.appendChild(footer);

        groupsContainer.appendChild(groupEl);
      });
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
            fetchGroups(pagedData.page_index, pagedData.page_size);
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
          fetchGroups(pagedData.page_index, pagedData.page_size);
        } else {
          alert('Bulk resolve failed: ' + await resp.text());
        }
      } catch (err) {
        alert('Error: ' + err.message);
      }
    }

    const initialPaging = __INITIAL_PAGING__;
    pagedData.page_index = initialPaging.page_index;
    pagedData.page_size = initialPaging.page_size;
    pagedData.current_page = initialPaging.page_index + 1;
    pagedData.limit = initialPaging.page_size;
    fetchGroups(initialPaging.page_index, initialPaging.page_size);
  </script>
</body>
</html>"#;
    Html(html.replace("__INITIAL_PAGING__", &initial_paging))
}

async fn list_groups(
    State(state): State<AppState>,
    Query(params): Query<GroupParams>,
) -> Result<Json<PagedGroups>, (StatusCode, String)> {
    let conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let paging = normalize_group_params(&params);
    let page_size = paging.page_size;
    let requested_page_index = paging.page_index;

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

    let total_pages = total_groups.div_ceil(page_size);
    let page_index = if total_pages == 0 {
        0
    } else {
        requested_page_index.min(total_pages - 1)
    };
    let offset = page_index * page_size;

    let mut stmt = conn
        .prepare(
            r#"
        SELECT group_id
        FROM target_items
        WHERE group_id IS NOT NULL
        GROUP BY group_id
        HAVING SUM(CASE WHEN keep_state = 'undecided' THEN 1 ELSE 0 END) > 0
        ORDER BY CASE WHEN MIN(created_at) != '' THEN MIN(created_at) ELSE '9999' END ASC
        LIMIT ?1 OFFSET ?2
        "#,
        )
        .map_err(internal_error)?;

    let ids = stmt
        .query_map(rusqlite::params![page_size as i64, offset as i64], |row| {
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
        page_index,
        page_size,
        current_page: page_index + 1,
        limit: page_size,
    }))
}

fn validate_path_idempotent(
    dest: &PathBuf,
    target_path: &str,
    group_id: i64,
) -> Result<PathBuf, (StatusCode, String)> {
    // According to src/import.rs, target_path is stored relative to dest.parent().
    // For example, if --dest is "repo", the parent is the current directory ".",
    // and target_path "repo/xxx.jpg" is correctly located at "./repo/xxx.jpg".
    let base = dest.parent().unwrap_or(dest);
    let original_abs = base.join(target_path);

    // 1. Try strict check at the logically correct location
    if let Ok(()) = ensure_under_root(base, &original_abs) {
        return Ok(original_abs);
    }

    // 2. If original is missing, check if it's already in the trash.
    // Trash is always under dest/.photo-org/trash/
    let expected_trash = find_in_trash(dest, target_path, group_id);
    if let Some(trash_path) = expected_trash {
        // Double check the found trash path is also safe (under dest root)
        ensure_under_root(dest, &trash_path).map_err(internal_error)?;
        return Ok(trash_path);
    }

    // 3. If neither, it's a real error
    Err((
        StatusCode::BAD_REQUEST,
        format!("file not found at source or in trash: {}", target_path),
    ))
}

fn find_in_trash(dest: &PathBuf, target_path: &str, group_id: i64) -> Option<PathBuf> {
    let trash_dir = dest
        .join(".photo-org")
        .join("trash")
        .join(format!("group-{}", group_id));
    if !trash_dir.exists() {
        return None;
    }

    let expected_name = safe_file_name(StdPath::new(target_path));
    let mut idx = 0usize;
    loop {
        let candidate = if idx == 0 {
            trash_dir.join(&expected_name)
        } else {
            trash_dir.join(format!("{}-{}", idx, expected_name))
        };

        if candidate.exists() {
            return Some(candidate);
        }

        if idx > 100 {
            break;
        }
        idx += 1;
    }
    None
}

async fn resolve_bulk(
    State(state): State<AppState>,
    Json(request): Json<BulkResolveRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut conn = open_catalog_db(&state.db_path).map_err(internal_error)?;

    for res in &request.resolutions {
        let kept_set: HashSet<_> = res.kept.iter().collect();
        let rejected_set: HashSet<_> = res.rejected.iter().collect();
        if !kept_set.is_disjoint(&rejected_set) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "kept and rejected sets overlap in group {}",
                    res.group_id
                ),
            ));
        }
        for path in kept_set.iter().chain(rejected_set.iter()) {
            validate_path_idempotent(&state.dest, path, res.group_id)?;
        }
    }

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
        validate_path_idempotent(&state.dest, path, id)?;
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

async fn delete_trash_member(
    Path((group_id, member_id)): Path<(i64, i64)>,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let group = load_group_members(&conn, group_id).map_err(internal_error)?;
    let member = group
        .iter()
        .find(|member| member.id == member_id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "group member not found".to_string()))?;
    let member_path = PathBuf::from(&member.target_path);
    if !is_logical_trash_path(&member_path) {
        return Err((
            StatusCode::BAD_REQUEST,
            "member is not under .photo-org/trash".into(),
        ));
    }

    let file_deleted = if member_path.exists() {
        ensure_under_root(&state.dest, &member_path).map_err(internal_error)?;
        if member_path.is_dir() {
            return Err((
                StatusCode::BAD_REQUEST,
                "trash member path must be a file".into(),
            ));
        }
        fs::remove_file(&member_path).map_err(internal_error)?;
        true
    } else {
        false
    };

    let tx = conn.transaction().map_err(internal_error)?;
    tx.execute("DELETE FROM target_items WHERE id = ?1", rusqlite::params![member_id])
        .map_err(internal_error)?;

    let remaining = load_group_members(&tx, group_id).map_err(internal_error)?;
    let group_cleared = remaining.len() <= 1;
    if group_cleared {
        for survivor in &remaining {
            tx.execute(
                "UPDATE target_items SET group_id = NULL, keep_state = 'undecided', is_group_primary = 0 WHERE id = ?1",
                rusqlite::params![survivor.id],
            )
            .map_err(internal_error)?;
        }
    } else if remaining.iter().all(|member| !member.is_group_primary) {
        if let Some(primary_id) = choose_best_primary_member(&remaining) {
            tx.execute(
                "UPDATE target_items SET is_group_primary = CASE WHEN id = ?1 THEN 1 ELSE 0 END WHERE group_id = ?2",
                rusqlite::params![primary_id, group_id],
            )
            .map_err(internal_error)?;
        }
    }

    insert_operation(
        &tx,
        "delete_trash_member",
        &json!({
            "group_id": group_id,
            "member_id": member_id,
            "target_path": member.target_path,
            "file_deleted": file_deleted,
            "group_cleared": group_cleared
        })
        .to_string(),
    )
    .map_err(internal_error)?;
    tx.commit().map_err(internal_error)?;

    Ok(Json(json!({
        "group_id": group_id,
        "member_id": member_id,
        "status": "ok",
        "file_deleted": file_deleted,
        "group_cleared": group_cleared
    })))
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
                let mime = best_effort_mime(&thumb_path, &bytes);
                let mut resp = Response::new(axum::body::Body::from(bytes));
                resp.headers_mut().insert(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_str(&mime).map_err(internal_error)?,
                );
                return Ok(resp);
            }
        }
    }

    let requested_size = query.size.unwrap_or(1600);

    // 2. Fast path for small browser-safe images
    if requested_size >= 1600 {
        if let Ok(bytes) = fs::read(&path) {
            let mime = best_effort_mime(&path, &bytes);
            if bytes.len() < 5 * 1024 * 1024
                && matches!(
                    mime.as_str(),
                    "image/jpeg" | "image/png" | "image/webp" | "image/gif"
                )
            {
                let mut resp = Response::new(axum::body::Body::from(bytes));
                resp.headers_mut().insert(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_str(&mime).map_err(internal_error)?,
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
            use crate::features::select_best_thumbnail;
            use rsraw::RawImage;
            let mut raw =
                RawImage::open(&bytes).map_err(|e| anyhow::anyhow!("rsraw open error: {}", e))?;
            let thumbs = raw
                .extract_thumbs()
                .map_err(|e| anyhow::anyhow!("rsraw extract error: {}", e))?;
            let thumb = select_best_thumbnail(&thumbs, requested_size)
                .ok_or_else(|| anyhow::anyhow!("no decodable raw preview found"))?;
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
    let base = dest.parent().unwrap_or(dest);
    let path = base.join(target_path);
    let trash_dir = dest
        .join(".photo-org")
        .join("trash")
        .join(format!("group-{}", group_id));

    // If the file already exists at the target path, move it to trash.
    if path.exists() {
        ensure_under_root(base, &path)?;
        fs::create_dir_all(&trash_dir)?;
        let file_name = safe_file_name(&path);
        let mut candidate = trash_dir.join(&file_name);
        let mut idx = 0usize;
        while candidate.exists() {
            // If the file at candidate has the same size, we might consider it already moved,
            // but to be safe and simple, we follow the original indexing logic.
            idx += 1;
            candidate = trash_dir.join(format!("{}-{}", idx, file_name));
        }
        fs::rename(&path, &candidate)?;
        return Ok(candidate);
    }

    // If the file is NOT at the target path, check if it's already in the trash.
    if trash_dir.exists() {
        let expected_name = safe_file_name(&PathBuf::from(target_path));
        // Check for the base name or indexed versions.
        // We prioritize the most recent one if multiple exist, or just the first one found.
        let mut idx = 0usize;
        loop {
            let candidate = if idx == 0 {
                trash_dir.join(&expected_name)
            } else {
                trash_dir.join(format!("{}-{}", idx, expected_name))
            };
            
            if candidate.exists() {
                // Found it already in trash!
                return Ok(candidate);
            }
            
            if idx > 100 { break; } // Safety break
            idx += 1;
        }
    }

    // If we reach here, the file is missing from both source and trash.
    Err(anyhow::anyhow!("File not found at source or in trash: {}", target_path))
}

fn is_logical_trash_path(path: &StdPath) -> bool {
    let mut saw_photo_org = false;
    for component in path.components() {
        let Component::Normal(part) = component else {
            saw_photo_org = false;
            continue;
        };
        let part = part.to_string_lossy();
        if saw_photo_org && part == "trash" {
            return true;
        }
        saw_photo_org = part == ".photo-org";
    }
    false
}

fn choose_best_primary_member(members: &[MemberRow]) -> Option<i64> {
    members
        .iter()
        .max_by_key(|member| {
            (
                i128::from(member.width) * i128::from(member.height),
                member.size_bytes,
                -member.id,
            )
        })
        .map(|member| member.id)
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

#[derive(Clone, Debug, Serialize)]
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
    use axum::body::to_bytes;
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

    fn insert_pending_group(conn: &rusqlite::Connection, group_id: i64, path_prefix: &str) {
        conn.execute(
            r#"
            INSERT INTO target_items (
                target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, origin_source_id, meta_json
            ) VALUES
                (?1, 1, 'image/png', '2024-06-09T00:00:00Z', ?2, 'p', 64, 32, 32, ?3, 'undecided', 1, NULL, '{}'),
                (?4, 1, 'image/png', '2024-06-09T00:00:00Z', ?5, 'p', 64, 32, 32, ?3, 'undecided', 0, NULL, '{}')
            "#,
            rusqlite::params![
                format!("{path_prefix}/g{group_id}-a.png"),
                format!("hash-{group_id}-a"),
                group_id,
                format!("{path_prefix}/g{group_id}-b.png"),
                format!("hash-{group_id}-b"),
            ],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn list_groups_supports_page_index_and_page_size() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        insert_pending_group(&conn, 1, "/tmp");
        insert_pending_group(&conn, 2, "/tmp");
        insert_pending_group(&conn, 3, "/tmp");

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/api/groups?page_index=1&page_size=1")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(paged["page_index"], 1);
        assert_eq!(paged["page_size"], 1);
        assert_eq!(paged["current_page"], 2);
        assert_eq!(paged["total_pages"], 3);
        assert_eq!(paged["groups"].as_array().unwrap().len(), 1);
        assert_eq!(paged["groups"][0]["group_id"], 2);
    }

    #[tokio::test]
    async fn index_html_mentions_page_index_and_page_size() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        open_catalog_db(&db_path).unwrap();

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("page_index"));
        assert!(html.contains("page_size"));
        assert!(html.contains("changePageIndex"));
        assert!(html.contains("changePageSize"));
    }

    #[tokio::test]
    async fn index_html_includes_mobile_responsive_controls() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        open_catalog_db(&db_path).unwrap();

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            html.contains(
                r#"<meta name="viewport" content="width=device-width, initial-scale=1">"#
            )
        );
        assert!(html.contains("member-actions"));
        assert!(html.contains("Tap image to preview"));
        assert!(html.contains("Click image to keep/reject"));
    }

    #[tokio::test]
    async fn index_html_embeds_initial_paging_from_query() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        open_catalog_db(&db_path).unwrap();

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/?page_index=2&page_size=1")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains(r#"const initialPaging = {"page_index":2,"page_size":1};"#));
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
    async fn delete_trash_member_removes_file_and_dissolves_singleton_group() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let keep_path = dest.join("2024/06/09/keep.png");
        let trash_path = dest.join(".photo-org/trash/group-1/reject.png");
        fs::create_dir_all(keep_path.parent().unwrap()).unwrap();
        fs::create_dir_all(trash_path.parent().unwrap()).unwrap();
        make_png(&keep_path, [255, 0, 0]);
        make_png(&trash_path, [0, 255, 0]);

        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        conn.execute(
            r#"
            INSERT INTO target_items (
                target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, origin_source_id, meta_json
            ) VALUES
                (?1, 1, 'image/png', '2024-06-09T00:00:00Z', 'a', 'p', 64, 32, 32, 1, 'undecided', 1, NULL, '{}'),
                (?2, 1, 'image/png', '2024-06-09T00:00:00Z', 'b', 'p', 64, 32, 32, 1, 'rejected', 0, NULL, '{}')
            "#,
            rusqlite::params![
                keep_path.to_string_lossy().to_string(),
                trash_path.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/api/groups/1/members/2/delete_trash")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!trash_path.exists());

        let conn = open_catalog_db(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM target_items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
        let survivor: (Option<i64>, String, i64) = conn
            .query_row(
                "SELECT group_id, keep_state, is_group_primary FROM target_items WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(survivor.0, None);
        assert_eq!(survivor.1, "undecided");
        assert_eq!(survivor.2, 0);
    }

    #[tokio::test]
    async fn delete_trash_member_rejects_non_trash_path() {
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
        conn.execute(
            r#"
            INSERT INTO target_items (
                target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, origin_source_id, meta_json
            ) VALUES
                (?1, 1, 'image/png', '2024-06-09T00:00:00Z', 'a', 'p', 64, 32, 32, 1, 'undecided', 1, NULL, '{}'),
                (?2, 1, 'image/png', '2024-06-09T00:00:00Z', 'b', 'p', 64, 32, 32, 1, 'rejected', 0, NULL, '{}')
            "#,
            rusqlite::params![
                keep_path.to_string_lossy().to_string(),
                reject_path.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/api/groups/1/members/2/delete_trash")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(reject_path.exists());
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
        interrupt::enter_interrupt_test();
        interrupt::request_for_test();

        let result = timeout(Duration::from_secs(2), server).await;
        interrupt::release_interrupt_test();
        assert!(result.is_ok(), "serve did not stop after interrupt");
        let join = result.unwrap().unwrap();
        assert!(join.is_ok(), "serve exited with error: {join:?}");
    }
}
