use crate::db::{insert_operation, open_catalog_db, open_catalog_db_readonly};
use crate::interrupt;
use crate::util::{
    PROFILE_ENV, best_effort_mime, date_for_target, ensure_under_root, ensure_under_target_root,
    logical_target_path, remove_empty_parent_dirs, resolve_physical_path, safe_file_name,
};
use anyhow::Result;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{Html, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use image::ImageFormat;
use once_cell::sync::Lazy;
use regex::Regex;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::fs;
use std::future::Future;
use std::io::{BufReader, Cursor};
use std::path::{Component, Path as StdPath, PathBuf};
use std::time::Instant;
use std::sync::OnceLock;
use tokio::net::TcpListener;

static UGOS_MODE: Lazy<bool> = Lazy::new(detect_ugos_system);

fn detect_ugos_system() -> bool {
    let sentinels = ["/usr/ugreen", "/ugreen", "/etc/sysconfig/thumb_core.sh"];
    sentinels.iter().any(|p| StdPath::new(p).exists())
}

fn serve_profiling_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os(PROFILE_ENV).is_some())
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
    review_mode: String,
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

#[derive(Clone, Debug)]
struct FilenameReviewGroup {
    review_group_id: i64,
    default_member_id: i64,
    members: Vec<MemberRow>,
}

#[derive(Clone, Debug)]
struct FilenameReviewCandidate {
    member: MemberRow,
    keys: Vec<String>,
    derived: bool,
    optics_signature: Option<String>,
    effective_orientation: EffectiveOrientation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EffectiveOrientation {
    Landscape,
    Portrait,
    Square,
}

#[derive(Clone, Debug)]
struct FilenameReviewHints {
    optics_signature: Option<String>,
    effective_orientation: EffectiveOrientation,
}

static TIMESTAMP_TOKEN_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)(\d{8})[-_](\d{6})").expect("valid timestamp token regex"));
static TIMESTAMP_DERIVED_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^(\d{8})-(\d{6})(?:-\d+)?$").expect("valid timestamp derived regex")
});
static TIMESTAMP_MILLIS_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^(\d{8})-(\d{6})\d{3,}$").expect("valid timestamp millis regex")
});
static DEFAULT_NUMERIC_SUFFIX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)-\d+$").expect("valid default numeric suffix regex"));
static DEFAULT_LEADING_INDEX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\d+-").expect("valid default leading index regex"));
static DEFAULT_TRAILING_FAMILY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)_(shotwell|embedded(?:_\d+)*)$").expect("valid default trailing family regex")
});
static DEFAULT_RAW_HINT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)_(arw|cr2|jpg|jpeg|png)$").expect("valid default raw hint regex")
});
static IMG_SAME_ID_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^img_\d{8}_\d{6}_(\d{3,5})$").expect("valid img same id regex")
});

#[derive(Debug, Deserialize)]
struct GroupParams {
    page_index: Option<usize>,
    page_size: Option<usize>,
    page: Option<usize>,
    limit: Option<usize>,
    group_id: Option<i64>,
    view: Option<String>,
}

const DEFAULT_PAGE_SIZE: usize = 20;
const MAX_PAGE_SIZE: usize = 1000;

#[derive(Debug, Serialize)]
struct PagingRequest {
    page_index: usize,
    page_size: usize,
    group_id: Option<i64>,
    review_mode: String,
}

#[derive(Debug, Deserialize)]
struct DeleteTrashMembersRequest {
    member_ids: Vec<i64>,
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

#[derive(Debug)]
struct ResolvedGroupSelection {
    members: Vec<MemberRow>,
    kept_member_ids: HashSet<i64>,
    rejected_member_ids: HashSet<i64>,
    primary_member_id: Option<i64>,
}

#[derive(Debug, Serialize)]
struct BulkResolveGroupError {
    group_id: i64,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ImageQuery {
    path: String,
    size: Option<u32>,
}

pub async fn run(db: PathBuf, dest: PathBuf, host: String, port: u16) -> Result<()> {
    open_catalog_db(&db)?;
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
        group_id: params.group_id,
        review_mode: params.view.clone().unwrap_or_else(|| "pending".to_string()),
    }
}

fn is_trash_review_mode(review_mode: &str) -> bool {
    review_mode == "trash"
}

fn is_filename_default_review_mode(review_mode: &str) -> bool {
    review_mode == "filename"
}

fn is_filename_trash_review_mode(review_mode: &str) -> bool {
    review_mode == "filename_trash"
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/groups", get(list_groups))
        .route("/api/groups/{id}/resolve", post(resolve_group))
        .route("/api/groups/resolve_bulk", post(resolve_bulk))
        .route("/api/groups/{id}/delete_trash", post(delete_trash_group))
        .route("/api/groups/delete_trash_bulk", post(delete_trash_bulk))
        .route(
            "/api/groups/{group_id}/members/{member_id}/restore_trash",
            post(restore_trash_member),
        )
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
    .header-mode-actions { display: flex; gap: 0.5rem; flex-wrap: wrap; justify-content: flex-end; }
    .header-mode-btn { min-height: 2.4rem; padding: 0.55rem 0.9rem; border-radius: 999px; border: 1px solid rgba(148, 163, 184, 0.16); background: rgba(15, 23, 42, 0.92); color: var(--text); font-weight: 700; }
    .header-mode-btn:hover:not(:disabled) { background: #334155; }
    .header-mode-btn:disabled { opacity: 0.55; cursor: not-allowed; }
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
      .header-mode-actions { width: 100%; justify-content: stretch; }
      .header-mode-btn { width: 100%; }
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
      group_id: null,
      review_mode: 'pending',
      current_page: 1,
      limit: 20
    };

    function isTrashReviewMode() {
      return pagedData.review_mode === 'trash';
    }

    function isFilenameReviewMode() {
      return pagedData.review_mode === 'filename';
    }

    function isFilenameTrashReviewMode() {
      return pagedData.review_mode === 'filename_trash';
    }

    function isTrashLikeReviewMode() {
      return isTrashReviewMode() || isFilenameTrashReviewMode();
    }

    function isPendingLikeReviewMode() {
      return !isTrashLikeReviewMode();
    }

    function reviewModeLabel() {
      if (isTrashReviewMode()) return 'trash-review';
      if (isFilenameTrashReviewMode()) return 'filename-trash-review';
      if (isFilenameReviewMode()) return 'filename-review';
      return 'pending';
    }

    function syncBrowserUrl() {
      const url = new URL(window.location.href);
      url.searchParams.set('page_index', String(pagedData.page_index));
      url.searchParams.set('page_size', String(pagedData.page_size));
      if (pagedData.review_mode !== 'pending') {
        url.searchParams.set('view', pagedData.review_mode);
      } else {
        url.searchParams.delete('view');
      }
      if (pagedData.group_id !== null && pagedData.group_id !== undefined && pagedData.group_id !== '') {
        url.searchParams.set('group_id', String(pagedData.group_id));
      } else {
        url.searchParams.delete('group_id');
      }
      url.searchParams.delete('page');
      url.searchParams.delete('limit');
      window.history.replaceState(null, '', url);
    }

    function renderHeaderStats() {
      const modeActions = `
        <div class="header-mode-actions">
          <button class="header-mode-btn" onclick="setReviewMode('pending')" ${pagedData.review_mode === 'pending' ? 'disabled' : ''}>Pending review</button>
          <button class="header-mode-btn" onclick="setReviewMode('filename')" ${isFilenameReviewMode() ? 'disabled' : ''}>Filename review</button>
          <button class="header-mode-btn" onclick="setReviewMode('filename_trash')" ${isFilenameTrashReviewMode() ? 'disabled' : ''}>Filename trash</button>
          <button class="header-mode-btn" onclick="setReviewMode('trash')" ${isTrashReviewMode() ? 'disabled' : ''}>Trash review</button>
        </div>
      `;
      if (pagedData.total_groups === 0) {
        const emptyLabel = pagedData.group_id
          ? `Group ${pagedData.group_id} not found`
          : (isTrashReviewMode()
            ? 'No trash-review groups'
            : (isFilenameTrashReviewMode()
              ? 'No filename-trash groups'
              : (isFilenameReviewMode() ? 'No filename-review groups' : 'No pending groups')));
        statsHeader.innerHTML = `<div class="header-stats">${modeActions}<div class="header-pill">${emptyLabel}</div></div>`;
        return;
      }
      const visibleMembers = pagedData.groups.reduce((sum, group) => sum + group.members.length, 0);
      const trashMembers = pagedData.groups.reduce((sum, group) => sum + group.members.filter(isTrashMember).length, 0);
      const scopePill = pagedData.group_id
        ? `<div class="header-pill">Group filter ${pagedData.group_id}</div>`
        : `<div class="header-pill">${pagedData.total_groups} ${reviewModeLabel()} groups</div>`;
      statsHeader.innerHTML = `
        <div class="header-stats">
          ${modeActions}
          <div class="header-stats-desktop">
            ${scopePill}
            <div class="header-pill">Page ${pagedData.current_page} / ${Math.max(pagedData.total_pages, 1)}</div>
            <div class="header-pill">${visibleMembers} items on screen</div>
            ${isTrashLikeReviewMode() ? `<div class="header-pill">${trashMembers} trash files on screen</div>` : ''}
          </div>
          <div class="header-stats-mobile">
            <div class="header-pill">${pagedData.group_id ? `group ${pagedData.group_id}` : `${pagedData.total_groups} groups`} • ${visibleMembers} items${isTrashLikeReviewMode() ? ` • ${trashMembers} trash` : ''} • p${pagedData.current_page}/${Math.max(pagedData.total_pages, 1)}</div>
          </div>
        </div>
      `;
    }

    async function fetchGroups(pageIndex = pagedData.page_index, pageSize = pagedData.page_size, groupId = pagedData.group_id, reviewMode = pagedData.review_mode) {
      try {
        groupsContainer.innerHTML = loadingMarkup(`Loading page ${pageIndex + 1}...`);
        window.scrollTo(0, 0);

        const params = new URLSearchParams({
          page_index: String(pageIndex),
          page_size: String(pageSize)
        });
        if (groupId !== null && groupId !== undefined && groupId !== '') {
          params.set('group_id', String(groupId));
        }
        if (reviewMode !== 'pending') {
          params.set('view', reviewMode);
        }
        const resp = await fetch(`/api/groups?${params.toString()}`);
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

      const canBulkDeleteTrash = pagedData.groups.some(group => group.members.some(isTrashMember));

      container.innerHTML = `
        <div class="toolbar">
          <div class="toolbar-block pagination">
            <button class="page-btn" ${pagedData.page_index <= 0 ? 'disabled' : ''} onclick="fetchGroups(${pagedData.page_index - 1}, ${pagedData.page_size}, pagedData.group_id, pagedData.review_mode)">Prev page</button>
            <div class="pagination-status">${pagedData.group_id ? `Viewing group ${pagedData.group_id}` : `Page <strong>${pagedData.current_page}</strong> of ${pagedData.total_pages} with ${pagedData.total_groups} ${reviewModeLabel()} groups`}</div>
            <button class="page-btn" ${pagedData.page_index + 1 >= pagedData.total_pages ? 'disabled' : ''} onclick="fetchGroups(${pagedData.page_index + 1}, ${pagedData.page_size}, pagedData.group_id, pagedData.review_mode)">Next page</button>
          </div>
          <div class="toolbar-block pagination-meta">
            <label class="page-size-label">page_index <input class="page-input" id="${container.id}-page-index" type="number" min="0" max="${Math.max(pagedData.total_pages - 1, 0)}" value="${pagedData.page_index}" onchange="changePageIndex(this.value)"></label>
            <label class="page-size-label">page_size <input class="page-input" id="${container.id}-page-size" type="number" min="1" max="1000" value="${pagedData.page_size}" onchange="changePageSize(this.value)"></label>
            <label class="page-size-label">group_id <input class="page-input" id="${container.id}-group-id" type="number" value="${pagedData.group_id ?? ''}" onchange="changeGroupId(this.value)"></label>
          </div>
          <div class="toolbar-block">
            <button class="page-btn" onclick="setReviewMode('pending')" ${pagedData.review_mode === 'pending' ? 'disabled' : ''}>Pending review</button>
            <button class="page-btn" onclick="setReviewMode('filename')" ${isFilenameReviewMode() ? 'disabled' : ''}>Filename review</button>
            <button class="page-btn" onclick="setReviewMode('filename_trash')" ${isFilenameTrashReviewMode() ? 'disabled' : ''}>Filename trash</button>
            <button class="page-btn" onclick="setReviewMode('trash')" ${isTrashReviewMode() ? 'disabled' : ''}>Trash review</button>
            ${isTrashLikeReviewMode()
              ? `<button class="btn-bulk" onclick="deleteTrashOnPage()" ${canBulkDeleteTrash ? '' : 'disabled'}>Delete trash on this page</button>`
              : `<button class="btn-bulk" onclick="confirmAllOnPage()" ${pagedData.group_id !== null && pagedData.groups.some(group => group.status !== 'pending') ? 'disabled' : ''}>Confirm all on this page</button>`}
          </div>
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
      fetchGroups(Math.min(nextIndex, maxIndex), pagedData.page_size, pagedData.group_id, pagedData.review_mode);
    }

    function changePageSize(value) {
      const nextSize = Number.parseInt(value, 10);
      if (!Number.isFinite(nextSize) || nextSize <= 0) {
        renderUI();
        return;
      }
      fetchGroups(0, nextSize, pagedData.group_id, pagedData.review_mode);
    }

    function changeGroupId(value) {
      const trimmed = String(value).trim();
      if (trimmed === '') {
        fetchGroups(0, pagedData.page_size, null, pagedData.review_mode);
        return;
      }
      const nextGroupId = Number.parseInt(trimmed, 10);
      if (!Number.isFinite(nextGroupId) || nextGroupId === 0) {
        renderUI();
        return;
      }
      fetchGroups(0, pagedData.page_size, nextGroupId, pagedData.review_mode);
    }

    function setReviewMode(mode) {
      if (mode !== 'pending' && mode !== 'trash' && mode !== 'filename' && mode !== 'filename_trash') return;
      fetchGroups(0, pagedData.page_size, null, mode);
    }

    function setKeepState(groupId, memberId, keep) {
      const group = pagedData.groups.find(g => g.group_id === groupId);
      if (group.status !== 'pending') return;
      const member = group.members.find(m => m.id === memberId);
      if (!keep && member.ui_primary) return;
      member.ui_keep = keep;
      renderUI();
    }

    function toggleKeepState(groupId, memberId) {
      const group = pagedData.groups.find(g => g.group_id === groupId);
      if (group.status !== 'pending') return;
      const member = group.members.find(m => m.id === memberId);
      if (member.ui_primary && member.ui_keep) return;
      member.ui_keep = !member.ui_keep;
      renderUI();
    }

    function setPrimary(groupId, memberId) {
      const group = pagedData.groups.find(g => g.group_id === groupId);
      if (group.status !== 'pending') return;
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

    function groupHeading(group) {
      if (isFilenameReviewMode()) {
        const defaultMember = group.members.find(member => member.target_path.split('/').pop().toLowerCase().startsWith('default'));
        if (!defaultMember) return `Filename group ${Math.abs(group.group_id)}`;
        return `Filename group: ${defaultMember.target_path.split('/').pop()}`;
      }
      if (isFilenameTrashReviewMode()) {
        const trashMember = group.members.find(isTrashMember) ?? group.members[0];
        return `Filename trash: ${trashMember.target_path.split('/').pop()}`;
      }
      return `Group ${group.group_id}`;
    }

    groupsContainer.addEventListener('click', (event) => {
      const actionButton = event.target.closest('button[data-action]');
      if (!actionButton) return;
      const groupId = Number.parseInt(actionButton.dataset.groupId, 10);
      const memberId = Number.parseInt(actionButton.dataset.memberId, 10);
      const fileName = actionButton.dataset.fileName || 'file';

      if (actionButton.dataset.action === 'restore-trash-member') {
        restoreTrashMember(groupId, memberId, fileName);
        return;
      }
      if (actionButton.dataset.action === 'delete-trash-member') {
        deleteTrashMember(groupId, memberId, fileName);
      }
    });

    async function deleteTrashMember(groupId, memberId, fileName) {
      if (!confirm(`Permanently delete trash file ${fileName}?`)) return;

      try {
        const resp = await fetch(`/api/groups/${groupId}/members/${memberId}/delete_trash`, {
          method: 'POST'
        });

        if (resp.ok) {
          fetchGroups(pagedData.page_index, pagedData.page_size, pagedData.group_id, pagedData.review_mode);
        } else {
          alert('Failed to delete trash file: ' + await resp.text());
        }
      } catch (err) {
        alert('Error: ' + err.message);
      }
    }

    async function restoreTrashMember(groupId, memberId, fileName) {
      if (!confirm(`Restore ${fileName} from trash back into the library?`)) return;

      try {
        const resp = await fetch(`/api/groups/${groupId}/members/${memberId}/restore_trash`, {
          method: 'POST'
        });

        if (resp.ok) {
          const payload = await resp.json();
          const group = pagedData.groups.find(g => g.group_id === groupId);
          if (!group) return;

          const member = group.members.find(m => m.id === memberId);
          if (!member) return;

          member.target_path = payload.target_path;
          member.keep_state = 'kept';
          member.ui_keep = true;

          const refreshedPrimaryId = group.members.find(m => m.is_group_primary)?.id ?? null;
          group.members.forEach(m => {
            m.ui_primary = refreshedPrimaryId !== null && m.id === refreshedPrimaryId;
          });

          if (isTrashLikeReviewMode() && !group.members.some(isTrashMember)) {
            pagedData.groups = pagedData.groups.filter(g => g.group_id !== groupId);
            pagedData.total_groups = Math.max(0, pagedData.total_groups - 1);
            if (pagedData.groups.length === 0) {
              renderUI();
              return;
            }
          }
          renderUI();
        } else {
          alert('Failed to restore trash file: ' + await resp.text());
        }
      } catch (err) {
        alert('Error: ' + err.message);
      }
    }

    async function deleteTrashGroup(groupId) {
      if (!confirm(`Permanently delete all trash files in group ${groupId}?`)) return;

      try {
        const resp = await fetch(`/api/groups/${groupId}/delete_trash`, {
          method: 'POST'
        });

        if (resp.ok) {
          fetchGroups(pagedData.page_index, pagedData.page_size, pagedData.group_id, pagedData.review_mode);
        } else {
          alert('Failed to delete group trash files: ' + await resp.text());
        }
      } catch (err) {
        alert('Error: ' + err.message);
      }
    }

    async function deleteTrashOnPage() {
      const memberIds = pagedData.groups
        .flatMap(group => group.members.filter(isTrashMember).map(member => member.id));

      if (memberIds.length === 0) {
        alert('No trash files on this page.');
        return;
      }
      if (!confirm(`Permanently delete ${memberIds.length} trash files on this page?`)) return;

      try {
        const resp = await fetch('/api/groups/delete_trash_bulk', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ member_ids: memberIds })
        });

        if (resp.ok) {
          fetchGroups(pagedData.page_index, pagedData.page_size, pagedData.group_id, pagedData.review_mode);
        } else {
          alert('Failed to delete page trash files: ' + await resp.text());
        }
      } catch (err) {
        alert('Error: ' + err.message);
      }
    }

    function renderGroups() {
      if (pagedData.groups.length === 0) {
        groupsContainer.innerHTML = emptyMarkup(
          pagedData.group_id ? `Group ${pagedData.group_id} not found` : 'All clear!',
          pagedData.group_id
            ? 'No review group matched that id.'
            : (isTrashReviewMode()
              ? 'No groups with trash files found. Use the header button to switch back to pending review.'
              : (isFilenameTrashReviewMode()
                ? 'No filename-trash groups found. Use the header button to switch back to filename or pending review.'
              : (isFilenameReviewMode()
                ? 'No default-prefix filename review groups found. Use the header button to switch back to pending review.'
                : 'No pending duplicate groups found. Use the header button to open filename or trash review.')))
        );
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
            <div class="group-id">${groupHeading(group)}</div>
            <span class="status-badge">${group.status}</span>
          </div>
          <div class="group-summary">${group.members.length} candidate file(s) to compare</div>
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
            ? `<button type="button" class="member-action reject" data-action="delete-trash-member" data-group-id="${group.group_id}" data-member-id="${member.id}" data-file-name="${fileName.replace(/"/g, '&quot;')}">Delete trash file</button>`
            : '';
          const restoreButton = isTrashLikeReviewMode() && isTrashMember(member)
            ? `<button type="button" class="member-action keep active" data-action="restore-trash-member" data-group-id="${group.group_id}" data-member-id="${member.id}" data-file-name="${fileName.replace(/"/g, '&quot;')}">Restore</button>`
            : '';
          const decisionButtonsDisabled = group.status !== 'pending' || isTrashLikeReviewMode() ? 'disabled' : '';
          const statusClass = member.ui_primary ? 'primary' : (member.ui_keep ? 'kept' : 'rejected');
          const statusLabel = member.ui_primary ? 'Primary' : (member.ui_keep ? 'Keep' : 'Reject');
          const actionButtons = isTrashLikeReviewMode()
            ? `
                <button type="button" class="member-action" onclick="showOverlay('${fullSrc}')">Preview</button>
                ${restoreButton}
                ${deleteButton}
              `
            : `
                <button type="button" class="member-action keep ${member.ui_keep ? 'active' : ''}" onclick="setKeepState(${group.group_id}, ${member.id}, true)" ${decisionButtonsDisabled}>Keep</button>
                <button type="button" class="member-action reject ${!member.ui_keep ? 'active' : ''}" onclick="setKeepState(${group.group_id}, ${member.id}, false)" ${member.ui_primary || group.status !== 'pending' ? 'disabled' : ''}>Reject</button>
                <button type="button" class="member-action primary ${member.ui_primary ? 'active' : ''}" onclick="setPrimary(${group.group_id}, ${member.id})" ${decisionButtonsDisabled}>Primary</button>
                <button type="button" class="member-action" onclick="showOverlay('${fullSrc}')">Preview</button>
                ${deleteButton}
              `;

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
              <div class="member-actions">${actionButtons}</div>
            </div>
          `;
          membersList.appendChild(memberEl);
        });
        groupEl.appendChild(membersList);

        const keptCount = group.members.filter(member => member.ui_keep).length;
        const rejectedCount = group.members.filter(member => !member.ui_keep).length;

        const footer = document.createElement('div');
        footer.className = 'group-footer';
        const trashCount = group.members.filter(isTrashMember).length;
        footer.innerHTML = `
            <div class="group-stats">
            ${isTrashLikeReviewMode()
              ? `Trash review: <strong>${trashCount}</strong> file(s) ready for permanent deletion`
              : group.status === 'pending'
              ? `Keeping <strong>${keptCount}</strong>, discarding <strong>${rejectedCount}</strong>`
              : `Archived group with <strong>${keptCount}</strong> kept and <strong>${rejectedCount}</strong> rejected members`}
          </div>
        `;
        if (isTrashReviewMode()) {
          const deleteGroupBtn = document.createElement('button');
          deleteGroupBtn.className = 'btn-resolve';
          deleteGroupBtn.innerText = `Delete ${trashCount} trash file(s)`;
          deleteGroupBtn.disabled = trashCount === 0;
          deleteGroupBtn.onclick = () => deleteTrashGroup(group.group_id);
          footer.appendChild(deleteGroupBtn);
        } else if (group.status === 'pending') {
          const resolveBtn = document.createElement('button');
          resolveBtn.className = 'btn-resolve';
          resolveBtn.innerText = 'Confirm decisions';
          resolveBtn.onclick = () => resolveGroup(group.group_id);
          footer.appendChild(resolveBtn);
        }
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
            fetchGroups(pagedData.page_index, pagedData.page_size, pagedData.group_id, pagedData.review_mode);
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
          fetchGroups(pagedData.page_index, pagedData.page_size, pagedData.group_id, pagedData.review_mode);
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
    pagedData.group_id = initialPaging.group_id ?? null;
    pagedData.review_mode = initialPaging.review_mode ?? 'pending';
    pagedData.current_page = initialPaging.page_index + 1;
    pagedData.limit = initialPaging.page_size;
    fetchGroups(initialPaging.page_index, initialPaging.page_size, pagedData.group_id, pagedData.review_mode);
  </script>
</body>
</html>"#;
    Html(html.replace("__INITIAL_PAGING__", &initial_paging))
}

async fn list_groups(
    State(state): State<AppState>,
    Query(params): Query<GroupParams>,
) -> Result<Json<PagedGroups>, (StatusCode, String)> {
    let conn = open_catalog_db_readonly(&state.db_path).map_err(internal_error)?;
    let paging = normalize_group_params(&params);
    let page_size = paging.page_size;
    let requested_page_index = paging.page_index;
    let review_mode = paging.review_mode.as_str();
    let review_groups_with_trash = is_trash_review_mode(review_mode);

    if is_filename_default_review_mode(review_mode) {
        let all_groups =
            load_filename_default_review_groups(&conn, &state.dest).map_err(internal_error)?;
        return Ok(Json(page_filename_review_groups(
            all_groups,
            page_size,
            requested_page_index,
            paging.group_id,
            review_mode,
        )));
    }

    if is_filename_trash_review_mode(review_mode) {
        let all_groups =
            load_filename_trash_review_groups(&conn, &state.dest).map_err(internal_error)?;
        return Ok(Json(page_filename_review_groups(
            all_groups,
            page_size,
            requested_page_index,
            paging.group_id,
            review_mode,
        )));
    }

    if let Some(group_id) = paging.group_id {
        let members = load_group_members(&conn, group_id).map_err(internal_error)?;
        let matches_mode = if review_groups_with_trash {
            !trash_member_ids(&members).is_empty()
        } else {
            members
                .iter()
                .any(|member| member.keep_state == "undecided")
        };
        if members.is_empty() || !matches_mode {
            return Ok(Json(PagedGroups {
                groups: Vec::new(),
                total_groups: 0,
                total_pages: 0,
                page_index: 0,
                page_size,
                review_mode: review_mode.to_string(),
                current_page: 0,
                limit: page_size,
            }));
        }

        return Ok(Json(PagedGroups {
            groups: vec![GroupSummary {
                group_id,
                status: group_status_for_mode(&members, review_mode),
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
            }],
            total_groups: 1,
            total_pages: 1,
            page_index: 0,
            page_size,
            review_mode: review_mode.to_string(),
            current_page: 1,
            limit: page_size,
        }));
    }

    let total_groups: i64 = conn
        .query_row(
            if review_groups_with_trash {
                r#"
        SELECT COUNT(DISTINCT group_id)
        FROM target_items
        WHERE group_id IS NOT NULL
        AND instr(target_path, '/.photo-org/trash/') > 0
        "#
            } else {
                r#"
        SELECT COUNT(DISTINCT group_id)
        FROM target_items
        WHERE group_id IS NOT NULL
        AND group_id IN (
            SELECT group_id FROM target_items WHERE keep_state = 'undecided'
        )
        "#
            },
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
        .prepare(if review_groups_with_trash {
            r#"
        SELECT group_id
        FROM target_items
        WHERE group_id IS NOT NULL
        AND instr(target_path, '/.photo-org/trash/') > 0
        GROUP BY group_id
        ORDER BY CASE WHEN MIN(created_at) != '' THEN MIN(created_at) ELSE '9999' END ASC
        LIMIT ?1 OFFSET ?2
        "#
        } else {
            r#"
        SELECT group_id
        FROM target_items
        WHERE group_id IS NOT NULL
        GROUP BY group_id
        HAVING SUM(CASE WHEN keep_state = 'undecided' THEN 1 ELSE 0 END) > 0
        ORDER BY CASE WHEN MIN(created_at) != '' THEN MIN(created_at) ELSE '9999' END ASC
        LIMIT ?1 OFFSET ?2
        "#
        })
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
            status: group_status_for_mode(&members, review_mode),
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
        review_mode: review_mode.to_string(),
        current_page: page_index + 1,
        limit: page_size,
    }))
}

fn page_filename_review_groups(
    all_groups: Vec<FilenameReviewGroup>,
    page_size: usize,
    requested_page_index: usize,
    requested_group_id: Option<i64>,
    review_mode: &str,
) -> PagedGroups {
    if let Some(group_id) = requested_group_id {
        let groups = all_groups
            .into_iter()
            .filter(|group| group.review_group_id == group_id)
            .collect::<Vec<_>>();
        if groups.is_empty() {
            return PagedGroups {
                groups: Vec::new(),
                total_groups: 0,
                total_pages: 0,
                page_index: 0,
                page_size,
                review_mode: review_mode.to_string(),
                current_page: 0,
                limit: page_size,
            };
        }
        return PagedGroups {
            groups: groups
                .into_iter()
                .map(filename_review_group_summary)
                .collect(),
            total_groups: 1,
            total_pages: 1,
            page_index: 0,
            page_size,
            review_mode: review_mode.to_string(),
            current_page: 1,
            limit: page_size,
        };
    }

    let total_groups = all_groups.len();
    let total_pages = total_groups.div_ceil(page_size);
    let page_index = if total_pages == 0 {
        0
    } else {
        requested_page_index.min(total_pages - 1)
    };
    let offset = page_index * page_size;
    let groups = all_groups
        .into_iter()
        .skip(offset)
        .take(page_size)
        .map(filename_review_group_summary)
        .collect::<Vec<_>>();
    PagedGroups {
        groups,
        total_groups,
        total_pages,
        page_index,
        page_size,
        review_mode: review_mode.to_string(),
        current_page: if total_pages == 0 { 0 } else { page_index + 1 },
        limit: page_size,
    }
}

fn filename_review_group_summary(group: FilenameReviewGroup) -> GroupSummary {
    GroupSummary {
        group_id: group.review_group_id,
        status: filename_review_group_status(&group.members),
        members: group
            .members
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
    }
}

fn filename_review_group_status(members: &[MemberRow]) -> String {
    if members.iter().any(|member| is_logical_trash_path(StdPath::new(&member.target_path))) {
        "filename-trash-review".to_string()
    } else {
        "pending".to_string()
    }
}

fn resolve_operation_name(group_id: i64) -> &'static str {
    if group_id < 0 {
        "resolve_filename_group"
    } else {
        "resolve_group"
    }
}

fn trash_group_dir_name(group_id: i64) -> String {
    if group_id < 0 {
        format!(
            "filename-group-{}",
            group_id.checked_neg().unwrap_or_default()
        )
    } else {
        format!("group-{}", group_id)
    }
}

fn validate_path_idempotent(
    dest: &PathBuf,
    target_path: &str,
    group_id: i64,
) -> Result<PathBuf, (StatusCode, String)> {
    let original_abs = resolve_physical_path(dest, target_path);

    // 1. Try strict check at the logically correct location
    if let Ok(()) = ensure_under_target_root(dest, &original_abs) {
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
        .join(trash_group_dir_name(group_id));
    if !trash_dir.exists() {
        return None;
    }

    // Since target_path in DB includes the "repo/" prefix (or whatever dest is),
    // but the file in trash might only use the basename, we use the original path's basename.
    let expected_name = safe_file_name(StdPath::new(target_path));
    let mut idx = 0usize;
    loop {
        let candidate = if idx == 0 {
            trash_dir.join(&expected_name)
        } else {
            trash_dir.join(format!("{}-{}", idx, &expected_name))
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

fn group_status_for_mode(members: &[MemberRow], review_mode: &str) -> String {
    if review_mode == "trash" {
        "trash-review".to_string()
    } else if members
        .iter()
        .any(|member| member.keep_state == "undecided")
    {
        "pending".to_string()
    } else {
        "archived".to_string()
    }
}

fn trash_member_ids(group: &[MemberRow]) -> Vec<i64> {
    group
        .iter()
        .filter(|member| is_logical_trash_path(StdPath::new(&member.target_path)))
        .map(|member| member.id)
        .collect()
}

fn load_filename_default_review_groups(
    conn: &rusqlite::Connection,
    dest: &StdPath,
) -> Result<Vec<FilenameReviewGroup>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, target_path, mime_type, keep_state, is_group_primary, exact_hash, phash, width, height, size_bytes, created_at
        FROM target_items
        WHERE keep_state = 'undecided'
          AND group_id IS NULL
          AND instr(target_path, '/.photo-org/trash/') = 0
        ORDER BY created_at ASC, id ASC
        "#,
    )?;
    let rows = stmt
        .query_map([], |row| {
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
                created_at: row.get(10)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let candidates = rows
        .into_iter()
        .filter_map(|member| filename_review_candidate(dest, member))
        .collect::<Vec<_>>();
    let mut by_id = HashMap::new();
    let mut key_to_ids: HashMap<String, Vec<i64>> = HashMap::new();
    for candidate in candidates {
        for key in &candidate.keys {
            key_to_ids
                .entry(key.clone())
                .or_default()
                .push(candidate.member.id);
        }
        by_id.insert(candidate.member.id, candidate);
    }

    let mut groups = Vec::new();
    let mut seen = HashSet::new();
    for start_id in by_id.keys().copied().collect::<Vec<_>>() {
        if seen.contains(&start_id) {
            continue;
        }
        let mut stack = vec![start_id];
        let mut component_ids = Vec::new();
        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            component_ids.push(id);
            let candidate = &by_id[&id];
            for key in &candidate.keys {
                if let Some(neighbors) = key_to_ids.get(key) {
                    for neighbor_id in neighbors {
                        if !seen.contains(neighbor_id)
                            && filename_review_candidates_compatible(
                                candidate,
                                &by_id[neighbor_id],
                            )
                        {
                            stack.push(*neighbor_id);
                        }
                    }
                }
            }
        }

        if component_ids.len() < 2 {
            continue;
        }

        let component = component_ids
            .into_iter()
            .filter_map(|id| by_id.get(&id).cloned())
            .collect::<Vec<_>>();
        if !component.iter().any(|candidate| candidate.derived) {
            continue;
        }

        let default_member_id = component
            .iter()
            .filter(|candidate| is_default_prefixed_target_path(&candidate.member.target_path))
            .map(|candidate| candidate.member.id)
            .min()
            .or_else(|| component.iter().filter(|candidate| candidate.derived).map(|candidate| candidate.member.id).min())
            .unwrap_or(component[0].member.id);

        let mut members = component
            .iter()
            .map(|candidate| {
                let mut member = candidate.member.clone();
                member.is_group_primary = false;
                member
            })
            .collect::<Vec<_>>();

        if let Some(primary_id) = choose_best_primary_member(&members) {
            for member in &mut members {
                member.is_group_primary = member.id == primary_id;
            }
        }

        members.sort_by_key(|member| {
            (
                !member.is_group_primary,
                !is_default_prefixed_target_path(&member.target_path),
                member.created_at.clone(),
                member.id,
            )
        });

        groups.push(FilenameReviewGroup {
            review_group_id: filename_review_group_id(default_member_id),
            default_member_id,
            members,
        });
    }

    groups.sort_by_key(|group| {
        let first_created = group
            .members
            .iter()
            .map(|member| member.created_at.as_str())
            .min()
            .unwrap_or("");
        (first_created.to_string(), group.default_member_id)
    });
    Ok(groups)
}

fn load_filename_trash_review_groups(
    conn: &rusqlite::Connection,
    dest: &StdPath,
) -> Result<Vec<FilenameReviewGroup>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, target_path, mime_type, keep_state, is_group_primary, exact_hash, phash, width, height, size_bytes, created_at
        FROM target_items
        WHERE instr(target_path, '/.photo-org/trash/') = 0
           OR instr(target_path, '/.photo-org/trash/filename-group-') > 0
        ORDER BY created_at ASC, id ASC
        "#,
    )?;
    let rows = stmt
        .query_map([], |row| {
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
                created_at: row.get(10)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let candidates = rows
        .into_iter()
        .filter_map(|member| filename_review_candidate(dest, member))
        .collect::<Vec<_>>();
    let mut by_id = HashMap::new();
    let mut key_to_ids: HashMap<String, Vec<i64>> = HashMap::new();
    for candidate in candidates {
        for key in &candidate.keys {
            key_to_ids
                .entry(key.clone())
                .or_default()
                .push(candidate.member.id);
        }
        by_id.insert(candidate.member.id, candidate);
    }

    let trash_seed_ids = by_id
        .values()
        .filter(|candidate| is_filename_group_trash_target_path(&candidate.member.target_path))
        .map(|candidate| candidate.member.id)
        .collect::<Vec<_>>();

    let mut groups = Vec::new();
    let mut seen = HashSet::new();
    for start_id in trash_seed_ids {
        if seen.contains(&start_id) {
            continue;
        }
        let mut stack = vec![start_id];
        let mut component_ids = Vec::new();
        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            component_ids.push(id);
            let candidate = &by_id[&id];
            for key in &candidate.keys {
                if let Some(neighbors) = key_to_ids.get(key) {
                    for neighbor_id in neighbors {
                        if !seen.contains(neighbor_id)
                            && filename_review_candidates_compatible(candidate, &by_id[neighbor_id])
                        {
                            stack.push(*neighbor_id);
                        }
                    }
                }
            }
        }

        if component_ids.len() < 2 {
            continue;
        }

        let component = component_ids
            .into_iter()
            .filter_map(|id| by_id.get(&id).cloned())
            .collect::<Vec<_>>();
        if !component
            .iter()
            .any(|candidate| is_filename_group_trash_target_path(&candidate.member.target_path))
        {
            continue;
        }
        if !component
            .iter()
            .any(|candidate| !is_logical_trash_path(StdPath::new(&candidate.member.target_path)))
        {
            continue;
        }

        let trash_member_id = component
            .iter()
            .filter(|candidate| is_filename_group_trash_target_path(&candidate.member.target_path))
            .map(|candidate| candidate.member.id)
            .min()
            .unwrap_or(component[0].member.id);

        let mut members = component
            .iter()
            .map(|candidate| {
                let mut member = candidate.member.clone();
                member.is_group_primary = false;
                member
            })
            .collect::<Vec<_>>();

        if let Some(primary_id) = choose_best_primary_member(&members) {
            for member in &mut members {
                member.is_group_primary = member.id == primary_id;
            }
        }

        members.sort_by_key(|member| {
            (
                !member.is_group_primary,
                is_logical_trash_path(StdPath::new(&member.target_path)),
                member.created_at.clone(),
                member.id,
            )
        });

        groups.push(FilenameReviewGroup {
            review_group_id: filename_trash_review_group_id(trash_member_id),
            default_member_id: trash_member_id,
            members,
        });
    }

    groups.sort_by_key(|group| {
        let first_created = group
            .members
            .iter()
            .map(|member| member.created_at.as_str())
            .min()
            .unwrap_or("");
        (first_created.to_string(), group.default_member_id)
    });
    Ok(groups)
}

fn split_filename_stem_and_ext(target_path: &str) -> Option<(String, String)> {
    let file_name = StdPath::new(target_path).file_name()?.to_str()?.to_lowercase();
    let dot = file_name.rfind('.')?;
    if dot == 0 || dot + 1 >= file_name.len() {
        return None;
    }
    let stem = file_name[..dot].to_string();
    let ext = file_name[dot + 1..].to_string();
    Some((stem, ext))
}

fn default_prefix_base_stem(stem: &str) -> Option<String> {
    let stem = DEFAULT_LEADING_INDEX_RE.replace(stem, "");
    let stem = stem.as_ref();
    let base = stem.strip_prefix("default")?;
    let mut normalized = DEFAULT_TRAILING_FAMILY_RE.replace(base, "").to_string();
    normalized = DEFAULT_RAW_HINT_RE.replace(&normalized, "").to_string();
    normalized = DEFAULT_NUMERIC_SUFFIX_RE.replace(&normalized, "").to_string();
    (!normalized.is_empty()).then_some(normalized)
}

fn is_default_prefixed_target_path(target_path: &str) -> bool {
    split_filename_stem_and_ext(target_path)
        .and_then(|(stem, _)| default_prefix_base_stem(&stem))
        .is_some()
}

fn filename_review_candidate(dest: &StdPath, member: MemberRow) -> Option<FilenameReviewCandidate> {
    let fallback_orientation = effective_orientation_from_dimensions(member.width, member.height);
    let (stem, _ext) = split_filename_stem_and_ext(&member.target_path)?;
    let mut keys = Vec::new();
    let mut derived = false;

    if let Some(base_stem) = default_prefix_base_stem(&stem) {
        keys.push(format!("subject:{base_stem}"));
        if let Some(timestamp_key) = canonical_timestamp_key(&base_stem) {
            keys.push(format!("timestamp:{timestamp_key}"));
        }
        derived = true;
    } else {
        keys.push(format!("subject:{stem}"));
        if let Some(same_id_key) = img_same_id_subject_key(&stem) {
            keys.push(format!("subject:{same_id_key}"));
        }
        if let Some(timestamp_key) = canonical_timestamp_key(&stem) {
            keys.push(format!("timestamp:{timestamp_key}"));
            if is_timestamp_rendition_derived(&stem) {
                derived = true;
            }
        }
    }

    keys.sort();
    keys.dedup();
    let hints = read_filename_review_hints(dest, &member);
    (!keys.is_empty()).then_some(FilenameReviewCandidate {
        member,
        keys,
        derived,
        optics_signature: hints.as_ref().and_then(|hint| hint.optics_signature.clone()),
        effective_orientation: hints
            .map(|hint| hint.effective_orientation)
            .unwrap_or(fallback_orientation),
    })
}

fn canonical_timestamp_key(stem: &str) -> Option<String> {
    if let Some(caps) = TIMESTAMP_DERIVED_RE.captures(stem) {
        return Some(format!("{}-{}", &caps[1], &caps[2]));
    }
    if let Some(caps) = TIMESTAMP_MILLIS_RE.captures(stem) {
        return Some(format!("{}-{}", &caps[1], &caps[2]));
    }
    let caps = TIMESTAMP_TOKEN_RE.captures(stem)?;
    Some(format!("{}-{}", &caps[1], &caps[2]))
}

fn is_timestamp_rendition_derived(stem: &str) -> bool {
    TIMESTAMP_DERIVED_RE.is_match(stem)
}

fn img_same_id_subject_key(stem: &str) -> Option<String> {
    let caps = IMG_SAME_ID_RE.captures(stem)?;
    Some(format!("img_{}", &caps[1]))
}

// Filename-only grouping is too permissive for cases like IMG_2454.JPG vs img_2454.jpg:
// both share the same basename family, but EXIF shows different optics/orientation.
// We keep the filename families as the coarse candidate generator, then require
// orientation agreement and reject links when both sides expose conflicting lens signatures.
fn filename_review_candidates_compatible(
    left: &FilenameReviewCandidate,
    right: &FilenameReviewCandidate,
) -> bool {
    if left.effective_orientation != right.effective_orientation {
        return false;
    }
    match (&left.optics_signature, &right.optics_signature) {
        (Some(left_sig), Some(right_sig)) => left_sig == right_sig,
        _ => true,
    }
}

fn read_filename_review_hints(
    dest: &StdPath,
    member: &MemberRow,
) -> Option<FilenameReviewHints> {
    let path = resolve_physical_path(dest, &member.target_path);
    let file = File::open(path).ok()?;
    let mut reader = BufReader::new(file);
    let exif = exif::Reader::new().read_from_container(&mut reader).ok()?;
    let orientation = exif
        .get_field(exif::Tag::Orientation, exif::In::PRIMARY)
        .and_then(|field| field.value.get_uint(0))
        .map(effective_orientation_from_exif)
        .unwrap_or_else(|| effective_orientation_from_dimensions(member.width, member.height));
    Some(FilenameReviewHints {
        optics_signature: exif_optics_signature(&exif),
        effective_orientation: orientation,
    })
}

fn exif_optics_signature(exif: &exif::Exif) -> Option<String> {
    for tag in [
        exif::Tag::LensModel,
        exif::Tag::LensSpecification,
        exif::Tag::FocalLengthIn35mmFilm,
        exif::Tag::FocalLength,
    ] {
        let Some(field) = exif.get_field(tag, exif::In::PRIMARY) else {
            continue;
        };
        let rendered = field.display_value().with_unit(exif).to_string();
        let normalized = rendered.trim().trim_matches('"').to_ascii_lowercase();
        if !normalized.is_empty() {
            return Some(normalized);
        }
    }
    None
}

fn effective_orientation_from_exif(value: u32) -> EffectiveOrientation {
    match value {
        5..=8 => EffectiveOrientation::Portrait,
        _ => EffectiveOrientation::Landscape,
    }
}

fn effective_orientation_from_dimensions(width: i64, height: i64) -> EffectiveOrientation {
    if width == height {
        EffectiveOrientation::Square
    } else if height > width {
        EffectiveOrientation::Portrait
    } else {
        EffectiveOrientation::Landscape
    }
}

fn filename_review_group_id(default_member_id: i64) -> i64 {
    -default_member_id
}

fn filename_trash_review_group_id(trash_member_id: i64) -> i64 {
    -(1_000_000_000_i64 + trash_member_id)
}

fn is_filename_trash_review_group_id(review_group_id: i64) -> bool {
    review_group_id <= -1_000_000_000_i64
}

fn filename_default_group_members(
    conn: &rusqlite::Connection,
    dest: &StdPath,
    review_group_id: i64,
) -> Result<Vec<MemberRow>> {
    if is_filename_trash_review_group_id(review_group_id) {
        let groups = load_filename_trash_review_groups(conn, dest)?;
        return groups
            .into_iter()
            .find(|group| group.review_group_id == review_group_id)
            .map(|group| group.members)
            .ok_or_else(|| anyhow::anyhow!("filename trash review group not found: {review_group_id}"));
    }
    let default_member_id = review_group_id
        .checked_neg()
        .ok_or_else(|| anyhow::anyhow!("invalid filename review group id {review_group_id}"))?;
    let groups = load_filename_default_review_groups(conn, dest)?;
    groups
        .into_iter()
        .find(|group| group.default_member_id == default_member_id)
        .map(|group| group.members)
        .ok_or_else(|| anyhow::anyhow!("filename review group not found: {review_group_id}"))
}

fn reserve_restore_path(
    dest: &StdPath,
    created_at: &str,
    current_path: &StdPath,
) -> Result<PathBuf> {
    let (year, month, day) = date_for_target(created_at);
    let folder = dest.join(year).join(month).join(day);
    fs::create_dir_all(&folder)?;
    let mut candidate = folder.join(safe_file_name(current_path));
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

async fn resolve_bulk(
    State(state): State<AppState>,
    Json(request): Json<BulkResolveRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let total_groups = request.resolutions.len();
    let started = Instant::now();

    let mut applied_groups = 0usize;
    let mut skipped_groups = 0usize;
    let mut failed_groups = Vec::new();

    for (index, res) in request.resolutions.into_iter().enumerate() {
        tracing::info!(
            group_id = res.group_id,
            group_index = index + 1,
            total_groups,
            kept = res.kept.len(),
            rejected = res.rejected.len(),
            "serve resolve_bulk group start"
        );

        match apply_bulk_resolution_group(&mut conn, &state.dest, &res) {
            Ok(BulkGroupOutcome::Skipped { elapsed_ms }) => {
                skipped_groups += 1;
                tracing::info!(
                    group_id = res.group_id,
                    group_index = index + 1,
                    total_groups,
                    elapsed_ms,
                    "serve resolve_bulk group already satisfied"
                );
            }
            Ok(BulkGroupOutcome::Applied {
                moved_files,
                elapsed_ms,
            }) => {
                applied_groups += 1;
                tracing::info!(
                    group_id = res.group_id,
                    group_index = index + 1,
                    total_groups,
                    moved_files,
                    elapsed_ms,
                    completed_groups = applied_groups + skipped_groups + failed_groups.len(),
                    remaining_groups = total_groups
                        .saturating_sub(applied_groups + skipped_groups + failed_groups.len()),
                    "serve resolve_bulk group committed"
                );
            }
            Err(error) => {
                tracing::error!(
                    group_id = res.group_id,
                    group_index = index + 1,
                    total_groups,
                    error = %error,
                    completed_groups = applied_groups + skipped_groups + failed_groups.len() + 1,
                    remaining_groups = total_groups
                        .saturating_sub(applied_groups + skipped_groups + failed_groups.len() + 1),
                    "serve resolve_bulk group failed"
                );
                failed_groups.push(BulkResolveGroupError {
                    group_id: res.group_id,
                    error,
                });
            }
        }
    }

    tracing::info!(
        total_groups,
        applied_groups,
        skipped_groups,
        failed_groups = failed_groups.len(),
        elapsed_ms = started.elapsed().as_millis(),
        "serve resolve_bulk finished"
    );
    Ok(Json(json!({
        "status": if failed_groups.is_empty() { "ok" } else { "partial" },
        "total_groups": total_groups,
        "applied_groups": applied_groups,
        "skipped_groups": skipped_groups,
        "failed_groups": failed_groups.len(),
        "errors": failed_groups
    })))
}

enum BulkGroupOutcome {
    Skipped { elapsed_ms: u128 },
    Applied { moved_files: usize, elapsed_ms: u128 },
}

fn apply_bulk_resolution_group(
    conn: &mut rusqlite::Connection,
    dest: &PathBuf,
    res: &GroupResolution,
) -> Result<BulkGroupOutcome, String> {
    let kept_set: HashSet<_> = res.kept.iter().collect();
    let rejected_set: HashSet<_> = res.rejected.iter().collect();
    if !kept_set.is_disjoint(&rejected_set) {
        return Err(format!(
            "kept and rejected sets overlap in group {}",
            res.group_id
        ));
    }
    for path in &res.kept {
        validate_kept_path_present(dest, path).map_err(|(_, message)| message)?;
    }
    for path in &res.rejected {
        validate_rejected_path_idempotent(dest, path, res.group_id)
            .map_err(|(_, message)| message)?;
    }
    if let Some(primary) = res.primary.as_ref() {
        validate_kept_path_present(dest, primary).map_err(|(_, message)| message)?;
    }

    let selection = load_bulk_group_selection(conn, dest, res).map_err(|err| err.to_string())?;
    let group_started = Instant::now();
    let tx = conn.transaction().map_err(|err| err.to_string())?;

    let mut moved_paths = Vec::new();
    for member in &selection.members {
        if selection.rejected_member_ids.contains(&member.id)
            && !is_expected_trash_member_path(&member.target_path, res.group_id)
        {
            match move_to_trash(dest, &member.target_path, res.group_id) {
                Ok(moved_to) => moved_paths.push((member.id, member.target_path.clone(), moved_to)),
                Err(err) => {
                    for (_, original_path, moved_to) in moved_paths.iter().rev() {
                        let _ = fs::rename(moved_to, original_path);
                    }
                    return Err(err.to_string());
                }
            }
        }
    }

    let tx_result = (|| -> Result<(), String> {
        for member in &selection.members {
            let keep_state = if selection.kept_member_ids.contains(&member.id) {
                "kept"
            } else if selection.rejected_member_ids.contains(&member.id) {
                "rejected"
            } else {
                "undecided"
            };
            let is_primary = selection
                .primary_member_id
                .map(|primary_id| primary_id == member.id)
                .unwrap_or(false);
            tx.execute(
                "UPDATE target_items SET keep_state = ?1, is_group_primary = ?2 WHERE id = ?3",
                rusqlite::params![keep_state, if is_primary { 1 } else { 0 }, member.id],
            )
            .map_err(|err| err.to_string())?;
        }

        for (id, _, moved_to) in &moved_paths {
            tx.execute(
                "UPDATE target_items SET target_path = ?1 WHERE id = ?2",
                rusqlite::params![logical_target_path(dest, moved_to).map_err(|err| err.to_string())?, id],
            )
            .map_err(|err| err.to_string())?;
        }

        insert_operation(
            &tx,
            resolve_operation_name(res.group_id),
            &json!({"group_id": res.group_id, "kept": res.kept, "rejected": res.rejected, "primary": res.primary}).to_string(),
        )
        .map_err(|err| err.to_string())?;
        Ok(())
    })();

    if let Err(err) = tx_result {
        drop(tx);
        for (_, original_path, moved_to) in moved_paths.iter().rev() {
            let _ = fs::rename(moved_to, original_path);
        }
        return Err(err);
    }

    if let Err(err) = tx.commit() {
        for (_, original_path, moved_to) in moved_paths.iter().rev() {
            let _ = fs::rename(moved_to, original_path);
        }
        return Err(err.to_string());
    }

    let moved_count = moved_paths.len();
    let elapsed_ms = group_started.elapsed().as_millis();
    if moved_count == 0
        && selection.members.iter().all(|member| {
            let keep_state = if selection.kept_member_ids.contains(&member.id) {
                "kept"
            } else if selection.rejected_member_ids.contains(&member.id) {
                "rejected"
            } else {
                "undecided"
            };
            let is_primary = selection
                .primary_member_id
                .map(|primary_id| primary_id == member.id)
                .unwrap_or(false);
            member.keep_state == keep_state && member.is_group_primary == is_primary
        })
    {
        Ok(BulkGroupOutcome::Skipped { elapsed_ms })
    } else {
        Ok(BulkGroupOutcome::Applied {
            moved_files: moved_count,
            elapsed_ms,
        })
    }
}

fn load_bulk_group_selection(
    conn: &rusqlite::Connection,
    dest: &StdPath,
    resolution: &GroupResolution,
) -> Result<ResolvedGroupSelection> {
    let members = load_resolution_members(conn, dest, resolution.group_id, &resolution.kept, &resolution.rejected, resolution.primary.as_ref())?;

    resolve_group_selection(dest, resolution.group_id, members, resolution)
}

fn load_resolution_members(
    conn: &rusqlite::Connection,
    dest: &StdPath,
    group_id: i64,
    kept: &[String],
    rejected: &[String],
    primary: Option<&String>,
) -> Result<Vec<MemberRow>> {
    if group_id >= 0 {
        return load_review_group_members(conn, dest, group_id);
    }

    let resolution = GroupResolution {
        group_id,
        kept: kept.to_vec(),
        rejected: rejected.to_vec(),
        primary: primary.cloned(),
    };
    load_filename_members_from_resolution_paths(conn, dest, &resolution)
}

fn resolve_group_selection(
    dest: &StdPath,
    group_id: i64,
    members: Vec<MemberRow>,
    resolution: &GroupResolution,
) -> Result<ResolvedGroupSelection> {
    let dest_buf = dest.to_path_buf();
    let mut by_physical_path = HashMap::new();
    for member in &members {
        by_physical_path.insert(resolve_physical_path(dest, &member.target_path), member.id);
    }

    let mut kept_member_ids = HashSet::new();
    for path in &resolution.kept {
        let physical_path = validate_path_idempotent_anyhow(&dest_buf, path, group_id)?;
        let member_id = *by_physical_path
            .get(&physical_path)
            .ok_or_else(|| anyhow::anyhow!("kept path not found in group {}: {}", group_id, path))?;
        kept_member_ids.insert(member_id);
    }

    let mut rejected_member_ids = HashSet::new();
    for path in &resolution.rejected {
        let physical_path = validate_path_idempotent_anyhow(&dest_buf, path, group_id)?;
        let member_id = *by_physical_path
            .get(&physical_path)
            .ok_or_else(|| anyhow::anyhow!("rejected path not found in group {}: {}", group_id, path))?;
        rejected_member_ids.insert(member_id);
    }

    if !kept_member_ids.is_disjoint(&rejected_member_ids) {
        return Err(anyhow::anyhow!(
            "kept and rejected sets overlap in group {}",
            group_id
        ));
    }

    let primary_member_id = if let Some(primary_path) = resolution.primary.as_ref() {
        let physical_path = validate_path_idempotent_anyhow(&dest_buf, primary_path, group_id)?;
        let member_id = *by_physical_path
            .get(&physical_path)
            .ok_or_else(|| {
                anyhow::anyhow!("primary path not found in group {}: {}", group_id, primary_path)
            })?;
        if !kept_member_ids.contains(&member_id) {
            return Err(anyhow::anyhow!(
                "primary path must be kept in group {}",
                group_id
            ));
        }
        Some(member_id)
    } else {
        None
    };

    Ok(ResolvedGroupSelection {
        members,
        kept_member_ids,
        rejected_member_ids,
        primary_member_id,
    })
}

fn validate_path_idempotent_anyhow(dest: &PathBuf, target_path: &str, group_id: i64) -> Result<PathBuf> {
    validate_path_idempotent(dest, target_path, group_id)
        .map_err(|(_, message)| anyhow::anyhow!(message))
}

fn validate_kept_path_present(
    dest: &PathBuf,
    target_path: &str,
) -> Result<PathBuf, (StatusCode, String)> {
    let original_abs = resolve_physical_path(dest, target_path);
    if !original_abs.exists() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("kept file not found at source: {}", target_path),
        ));
    }
    ensure_under_target_root(dest, &original_abs).map_err(internal_error)?;
    Ok(original_abs)
}

fn validate_rejected_path_idempotent(
    dest: &PathBuf,
    target_path: &str,
    group_id: i64,
) -> Result<PathBuf, (StatusCode, String)> {
    validate_path_idempotent(dest, target_path, group_id)
}

fn load_filename_members_from_resolution_paths(
    conn: &rusqlite::Connection,
    dest: &StdPath,
    resolution: &GroupResolution,
) -> Result<Vec<MemberRow>> {
    let trash_dir = dest
        .join(".photo-org")
        .join("trash")
        .join(trash_group_dir_name(resolution.group_id));
    let trash_prefix = format!(
        "{}/%",
        logical_target_path(dest, &trash_dir)?
    );
    let mut exact_paths = resolution
        .kept
        .iter()
        .chain(resolution.rejected.iter())
        .cloned()
        .collect::<Vec<_>>();
    if let Some(primary) = resolution.primary.as_ref() {
        exact_paths.push(primary.clone());
    }
    exact_paths.sort();
    exact_paths.dedup();

    let mut sql = String::from(
        "SELECT id, target_path, mime_type, keep_state, is_group_primary, exact_hash, phash, width, height, size_bytes, created_at FROM target_items WHERE ",
    );
    for (index, _) in exact_paths.iter().enumerate() {
        if index > 0 {
            sql.push_str(" OR ");
        }
        sql.push_str("target_path = ?");
    }
    if !exact_paths.is_empty() {
        sql.push_str(" OR ");
    }
    sql.push_str("target_path LIKE ?");
    sql.push_str(" ORDER BY id");

    let mut params = exact_paths
        .iter()
        .map(|path| path.as_str())
        .collect::<Vec<_>>();
    params.push(trash_prefix.as_str());

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params), |row| {
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
                created_at: row.get(10)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(rows)
}

fn is_expected_trash_member_path(target_path: &str, group_id: i64) -> bool {
    target_path.contains(&format!(
        "/.photo-org/trash/{}/",
        trash_group_dir_name(group_id)
    ))
}

async fn resolve_group(
    Path(id): Path<i64>,
    State(state): State<AppState>,
    Json(request): Json<ResolveRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let kept = request.kept.unwrap_or_default();
    let rejected = request.rejected.unwrap_or_default();
    let primary = request.primary;
    let group = load_resolution_members(&conn, &state.dest, id, &kept, &rejected, primary.as_ref())
        .map_err(internal_error)?;
    if group.is_empty() {
        return Err((StatusCode::NOT_FOUND, "group not found".into()));
    }
    let kept_set: HashSet<_> = kept.iter().collect();
    let rejected_set: HashSet<_> = rejected.iter().collect();
    if !kept_set.is_disjoint(&rejected_set) {
        return Err((
            StatusCode::BAD_REQUEST,
            "kept and rejected sets overlap".into(),
        ));
    }
    for path in &kept {
        validate_kept_path_present(&state.dest, path)?;
    }
    for path in &rejected {
        validate_rejected_path_idempotent(&state.dest, path, id)?;
    }
    if let Some(primary_path) = primary.as_ref() {
        validate_kept_path_present(&state.dest, primary_path)?;
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
            rusqlite::params![
                logical_target_path(&state.dest, moved_to).map_err(internal_error)?,
                id
            ],
        )
        .map_err(internal_error)?;
    }
    insert_operation(
        &tx,
        resolve_operation_name(id),
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
    let conn = open_catalog_db_readonly(&state.db_path).map_err(internal_error)?;
    let group = load_review_group_members(&conn, &state.dest, id).map_err(internal_error)?;
    Ok(Json(json!({ "group_id": id, "members": group })))
}

fn delete_trash_members_by_ids(
    conn: &mut rusqlite::Connection,
    dest: &PathBuf,
    member_ids: &[i64],
) -> Result<serde_json::Value, (StatusCode, String)> {
    let started = Instant::now();
    if member_ids.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "no member ids provided".into()));
    }

    let mut seen = HashSet::new();
    let unique_member_ids = member_ids
        .iter()
        .copied()
        .filter(|member_id| seen.insert(*member_id))
        .collect::<Vec<_>>();
    tracing::info!(
        requested_members = member_ids.len(),
        unique_members = unique_member_ids.len(),
        "serve trash delete batch start"
    );
    let dedupe_elapsed = started.elapsed();

    let mut members = Vec::new();
    let member_load_started = Instant::now();
    for member_id in &unique_member_ids {
        let member = conn
            .query_row(
                r#"
                SELECT id, target_path, mime_type, keep_state, is_group_primary, exact_hash, phash, width, height, size_bytes, group_id
                FROM target_items
                WHERE id = ?1
                "#,
                rusqlite::params![member_id],
                |row| {
                    Ok((
                        GroupMember {
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
                        },
                        row.get::<_, Option<i64>>(10)?,
                    ))
                },
            )
            .optional()
            .map_err(internal_error)?
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("group member {} not found", member_id)))?;
        let member_path = resolve_physical_path(dest, &member.0.target_path);
        if !is_logical_trash_path(&member_path) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("member {} is not under .photo-org/trash", member_id),
            ));
        }
        members.push(member);
    }
    let member_load_elapsed = member_load_started.elapsed();
    tracing::info!(
        unique_members = unique_member_ids.len(),
        member_load_ms = member_load_elapsed.as_millis(),
        "serve trash delete batch loaded members"
    );

    let mut deleted = Vec::new();
    let photo_org_root = dest.join(".photo-org");
    let fs_delete_started = Instant::now();
    for (member, group_id) in &members {
        let member_path = resolve_physical_path(dest, &member.target_path);
        let file_deleted = if member_path.exists() {
            ensure_under_target_root(dest, &member_path).map_err(internal_error)?;
            if member_path.is_dir() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "trash member path must be a file".into(),
                ));
            }
            fs::remove_file(&member_path).map_err(internal_error)?;
            if let Some(parent) = member_path.parent() {
                remove_empty_parent_dirs(parent, &photo_org_root).map_err(internal_error)?;
            }
            true
        } else {
            false
        };
        deleted.push(json!({
            "member_id": member.id,
            "group_id": group_id,
            "target_path": member.target_path,
            "file_deleted": file_deleted
        }));
    }
    let fs_delete_elapsed = fs_delete_started.elapsed();
    tracing::info!(
        unique_members = unique_member_ids.len(),
        deleted_entries = deleted.len(),
        fs_delete_ms = fs_delete_elapsed.as_millis(),
        "serve trash delete batch removed files"
    );

    let tx_started = Instant::now();
    let tx = conn.transaction().map_err(internal_error)?;
    for (member, _) in &members {
        tx.execute(
            "DELETE FROM target_items WHERE id = ?1",
            rusqlite::params![member.id],
        )
        .map_err(internal_error)?;
    }
    let row_delete_elapsed = tx_started.elapsed();

    let mut touched_groups = HashSet::new();
    for (_, group_id) in &members {
        if let Some(group_id) = group_id {
            touched_groups.insert(*group_id);
        }
    }

    let mut group_results = Vec::new();
    let group_repair_started = Instant::now();
    for group_id in touched_groups {
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
        group_results.push(json!({
            "group_id": group_id,
            "group_cleared": group_cleared,
            "remaining_members": remaining.len()
        }));
    }
    let group_repair_elapsed = group_repair_started.elapsed();

    let op_log_started = Instant::now();
    insert_operation(
        &tx,
        "delete_trash_member",
        &json!({
            "deleted": deleted,
            "groups": group_results
        })
        .to_string(),
    )
    .map_err(internal_error)?;
    let op_log_elapsed = op_log_started.elapsed();
    let commit_started = Instant::now();
    tx.commit().map_err(internal_error)?;
    let commit_elapsed = commit_started.elapsed();
    tracing::info!(
        unique_members = unique_member_ids.len(),
        deleted_entries = deleted.len(),
        touched_groups = group_results.len(),
        dedupe_ms = dedupe_elapsed.as_millis(),
        member_load_ms = member_load_elapsed.as_millis(),
        fs_delete_ms = fs_delete_elapsed.as_millis(),
        row_delete_ms = row_delete_elapsed.as_millis(),
        group_repair_ms = group_repair_elapsed.as_millis(),
        op_log_ms = op_log_elapsed.as_millis(),
        commit_ms = commit_elapsed.as_millis(),
        elapsed_ms = started.elapsed().as_millis(),
        "serve trash delete batch finished"
    );

    if serve_profiling_enabled() {
        tracing::info!(
            member_count = unique_member_ids.len(),
            deleted_count = deleted.len(),
            touched_groups = group_results.len(),
            dedupe_ms = dedupe_elapsed.as_millis(),
            member_load_ms = member_load_elapsed.as_millis(),
            fs_delete_ms = fs_delete_elapsed.as_millis(),
            row_delete_ms = row_delete_elapsed.as_millis(),
            group_repair_ms = group_repair_elapsed.as_millis(),
            op_log_ms = op_log_elapsed.as_millis(),
            commit_ms = commit_elapsed.as_millis(),
            total_ms = started.elapsed().as_millis(),
            profile_env = PROFILE_ENV,
            "serve delete_trash profile"
        );
    }

    Ok(json!({
        "status": "ok",
        "deleted": deleted,
        "groups": group_results
    }))
}

async fn delete_trash_group(
    Path(group_id): Path<i64>,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    tracing::info!(group_id, "serve trash delete group start");
    let mut conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let group = load_review_group_members(&conn, &state.dest, group_id).map_err(internal_error)?;
    if group.is_empty() {
        return Err((StatusCode::NOT_FOUND, "group not found".into()));
    }
    let member_ids = trash_member_ids(&group);
    if member_ids.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "group has no members under .photo-org/trash".into(),
        ));
    }
    let payload = delete_trash_members_by_ids(&mut conn, &state.dest, &member_ids)?;
    tracing::info!(
        group_id,
        deleted_members = member_ids.len(),
        "serve trash delete group finished"
    );
    Ok(Json(json!({
        "group_id": group_id,
        "deleted_members": member_ids.len(),
        "result": payload
    })))
}

async fn delete_trash_bulk(
    State(state): State<AppState>,
    Json(request): Json<DeleteTrashMembersRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    tracing::info!(
        requested_members = request.member_ids.len(),
        "serve trash delete bulk start"
    );
    let mut conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let payload = delete_trash_members_by_ids(&mut conn, &state.dest, &request.member_ids)?;
    tracing::info!(
        requested_members = request.member_ids.len(),
        "serve trash delete bulk finished"
    );
    Ok(Json(payload))
}

async fn delete_trash_member(
    Path((group_id, member_id)): Path<(i64, i64)>,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    tracing::info!(group_id, member_id, "serve trash delete member start");
    let mut conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let group = load_review_group_members(&conn, &state.dest, group_id).map_err(internal_error)?;
    let member = group
        .iter()
        .find(|member| member.id == member_id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "group member not found".to_string()))?;
    let payload = delete_trash_members_by_ids(&mut conn, &state.dest, &[member.id])?;
    tracing::info!(group_id, member_id, "serve trash delete member finished");
    Ok(Json(json!({
        "group_id": group_id,
        "member_id": member_id,
        "result": payload
    })))
}

async fn restore_trash_member(
    Path((group_id, member_id)): Path<(i64, i64)>,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let started = Instant::now();
    tracing::info!(group_id, member_id, "serve trash restore member start");
    let mut conn = open_catalog_db(&state.db_path).map_err(internal_error)?;
    let group = load_review_group_members(&conn, &state.dest, group_id).map_err(internal_error)?;
    let member = group
        .iter()
        .find(|member| member.id == member_id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "group member not found".to_string()))?;
    let current_path = resolve_physical_path(&state.dest, &member.target_path);
    if !is_logical_trash_path(&current_path) {
        return Err((
            StatusCode::BAD_REQUEST,
            "member is not under .photo-org/trash".into(),
        ));
    }
    ensure_under_target_root(&state.dest, &current_path).map_err(internal_error)?;
    if !current_path.exists() {
        return Err((StatusCode::BAD_REQUEST, "trash file is missing".into()));
    }
    if current_path.is_dir() {
        return Err((
            StatusCode::BAD_REQUEST,
            "trash member path must be a file".into(),
        ));
    }

    let restored_path = reserve_restore_path(&state.dest, &member.created_at, &current_path)
        .map_err(internal_error)?;
    fs::rename(&current_path, &restored_path).map_err(internal_error)?;
    if let Some(parent) = current_path.parent() {
        remove_empty_parent_dirs(parent, &state.dest.join(".photo-org")).map_err(internal_error)?;
    }

    let tx = conn.transaction().map_err(internal_error)?;
    tx.execute(
        "UPDATE target_items SET target_path = ?1, keep_state = 'kept' WHERE id = ?2",
        rusqlite::params![
            logical_target_path(&state.dest, &restored_path).map_err(internal_error)?,
            member_id
        ],
    )
    .map_err(internal_error)?;

    if group_id >= 0 {
        let remaining = load_review_group_members(&tx, &state.dest, group_id).map_err(internal_error)?;
        if remaining.iter().all(|member| !member.is_group_primary) {
            if let Some(primary_id) = choose_best_primary_member(&remaining) {
                tx.execute(
                    "UPDATE target_items SET is_group_primary = CASE WHEN id = ?1 THEN 1 ELSE 0 END WHERE group_id = ?2",
                    rusqlite::params![primary_id, group_id],
                )
                .map_err(internal_error)?;
            }
        }
    }

    insert_operation(
        &tx,
        "restore_trash_member",
        &json!({
            "group_id": group_id,
            "member_id": member_id,
            "from": member.target_path,
            "to": restored_path
        })
        .to_string(),
    )
    .map_err(internal_error)?;
    if let Err(err) = tx.commit() {
        let _ = fs::rename(&restored_path, &current_path);
        return Err(internal_error(err));
    }
    tracing::info!(
        group_id,
        member_id,
        elapsed_ms = started.elapsed().as_millis(),
        "serve trash restore member finished"
    );

    Ok(Json(json!({
        "group_id": group_id,
        "member_id": member_id,
        "status": "ok",
        "target_path": logical_target_path(&state.dest, &restored_path).map_err(internal_error)?
    })))
}

async fn image(
    State(state): State<AppState>,
    Query(query): Query<ImageQuery>,
) -> Result<Response, (StatusCode, String)> {
    let path = resolve_physical_path(&state.dest, &query.path);
    ensure_under_target_root(&state.dest, &path).map_err(internal_error)?;

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
    let path = resolve_physical_path(dest, target_path);
    let trash_dir = dest
        .join(".photo-org")
        .join("trash")
        .join(trash_group_dir_name(group_id));

    // If the file already exists at the target path, move it to trash.
    if path.exists() {
        ensure_under_target_root(dest, &path)?;
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

            if idx > 100 {
                break;
            } // Safety break
            idx += 1;
        }
    }

    // If we reach here, the file is missing from both source and trash.
    Err(anyhow::anyhow!(
        "File not found at source or in trash: {}",
        target_path
    ))
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

fn is_filename_group_trash_target_path(target_path: &str) -> bool {
    target_path.contains("/.photo-org/trash/filename-group-")
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

fn load_review_group_members(
    conn: &rusqlite::Connection,
    dest: &StdPath,
    group_id: i64,
) -> Result<Vec<MemberRow>> {
    if group_id < 0 {
        filename_default_group_members(conn, dest, group_id)
    } else {
        load_group_members(conn, group_id)
    }
}

fn load_group_members(conn: &rusqlite::Connection, group_id: i64) -> Result<Vec<MemberRow>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, target_path, mime_type, keep_state, is_group_primary, exact_hash, phash, width, height, size_bytes, created_at
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
                created_at: row.get(10)?,
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
    created_at: String,
}

fn internal_error(err: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_catalog_db;
    use crate::interrupt;
    use crate::util::logical_target_path;
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

    fn insert_trash_review_group(
        conn: &rusqlite::Connection,
        group_id: i64,
        keep_path: &str,
        trash_path: &str,
    ) {
        conn.execute(
            r#"
            INSERT INTO target_items (
                target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, origin_source_id, meta_json
            ) VALUES
                (?1, 1, 'image/png', '2024-06-09T00:00:00Z', 'a', 'p', 64, 32, 32, ?3, 'kept', 1, NULL, '{}'),
                (?2, 1, 'image/png', '2024-06-09T00:00:00Z', 'b', 'p', 64, 32, 32, ?3, 'rejected', 0, NULL, '{}')
            "#,
            rusqlite::params![keep_path, trash_path, group_id],
        )
        .unwrap();
    }

    fn insert_filename_review_rows(
        conn: &rusqlite::Connection,
        dest: &Path,
        plain_rel: &str,
        default_rel: &str,
    ) {
        let plain_abs = dest.join(plain_rel);
        let default_abs = dest.join(default_rel);
        fs::create_dir_all(plain_abs.parent().unwrap()).unwrap();
        fs::create_dir_all(default_abs.parent().unwrap()).unwrap();
        make_png(&plain_abs, [10, 140, 30]);
        make_png(&default_abs, [120, 40, 10]);

        conn.execute(
            r#"
            INSERT INTO target_items (
                target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, origin_source_id, meta_json
            ) VALUES
                (?1, 10, 'image/png', '2024-06-09T00:00:00Z', 'plain-hash', 'plain-phash', 64, 32, 32, NULL, 'undecided', 0, NULL, '{}'),
                (?2, 8, 'image/png', '2024-06-10T00:00:00Z', 'default-hash', 'default-phash', 64, 32, 32, NULL, 'undecided', 0, NULL, '{}')
            "#,
            rusqlite::params![
                logical_target_path(dest, &plain_abs).unwrap(),
                logical_target_path(dest, &default_abs).unwrap(),
            ],
        )
        .unwrap();
    }

    fn insert_ungrouped_review_row(
        conn: &rusqlite::Connection,
        dest: &Path,
        logical_rel: &str,
        exact_hash: &str,
        created_at: &str,
    ) {
        let abs = dest.join(logical_rel);
        fs::create_dir_all(abs.parent().unwrap()).unwrap();
        fs::write(&abs, b"review-test-bytes").unwrap();
        conn.execute(
            r#"
            INSERT INTO target_items (
                target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, origin_source_id, meta_json
            ) VALUES
                (?1, 10, 'image/png', ?2, ?3, 'phash', 64, 32, 32, NULL, 'undecided', 0, NULL, '{}')
            "#,
            rusqlite::params![logical_target_path(dest, &abs).unwrap(), created_at, exact_hash],
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
        assert_eq!(paged["review_mode"], "pending");
    }

    #[tokio::test]
    async fn list_groups_supports_trash_review_mode() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let trash_dir_1 = dest.join(".photo-org/trash/group-1");
        let trash_dir_2 = dest.join(".photo-org/trash/group-2");
        let keep_dir = dest.join("2024/06/09");
        fs::create_dir_all(&trash_dir_1).unwrap();
        fs::create_dir_all(&trash_dir_2).unwrap();
        fs::create_dir_all(&keep_dir).unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        insert_trash_review_group(
            &conn,
            1,
            &keep_dir.join("keep-1.png").to_string_lossy(),
            &trash_dir_1.join("reject-1.png").to_string_lossy(),
        );
        insert_trash_review_group(
            &conn,
            2,
            &keep_dir.join("keep-2.png").to_string_lossy(),
            &trash_dir_2.join("reject-2.png").to_string_lossy(),
        );

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/api/groups?view=trash&page_index=0&page_size=1")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(paged["review_mode"], "trash");
        assert_eq!(paged["total_groups"], 2);
        assert_eq!(paged["groups"][0]["status"], "trash-review");
    }

    #[tokio::test]
    async fn list_groups_supports_filename_default_review_mode() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        insert_filename_review_rows(
            &conn,
            &dest,
            "2024/06/09/IMG_1234.JPG",
            "2025/03/17/defaultimg_1234.jpg",
        );

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/api/groups?view=filename&page_index=0&page_size=10")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(paged["review_mode"], "filename");
        assert_eq!(paged["total_groups"], 1);
        let members = paged["groups"][0]["members"].as_array().unwrap();
        assert_eq!(members.len(), 2);
        assert!(paged["groups"][0]["group_id"].as_i64().unwrap() < 0);
    }

    #[tokio::test]
    async fn filename_review_mode_includes_shotwell_and_timestamp_patterns() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        insert_ungrouped_review_row(
            &conn,
            &dest,
            "2013/10/17/IMG_5786.CR2",
            "hash-shotwell-plain",
            "2013-10-17T00:00:00Z",
        );
        insert_ungrouped_review_row(
            &conn,
            &dest,
            "2025/03/17/defaultimg_5786_cr2_shotwell.jpg",
            "hash-shotwell-derived",
            "2025-03-17T00:00:00Z",
        );
        insert_ungrouped_review_row(
            &conn,
            &dest,
            "2019/12/19/IMG_20191219_215605.jpg",
            "hash-ts-plain",
            "2019-12-19T00:00:00Z",
        );
        insert_ungrouped_review_row(
            &conn,
            &dest,
            "2019/12/19/20191219-215605-3.jpg",
            "hash-ts-derived",
            "2019-12-19T00:00:01Z",
        );

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/api/groups?view=filename&page_index=0&page_size=10")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(paged["review_mode"], "filename");
        assert_eq!(paged["total_groups"], 2);

        let names = paged["groups"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["members"].as_array().unwrap().iter())
            .filter_map(|member| member["target_path"].as_str())
            .collect::<Vec<_>>();
        assert!(names.iter().any(|path| path.contains("defaultimg_5786_cr2_shotwell")));
        assert!(names.iter().any(|path| path.contains("20191219-215605-3")));
    }

    #[tokio::test]
    async fn filename_review_primary_uses_resolution_and_size_for_timestamp_renditions() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        insert_ungrouped_review_row(
            &conn,
            &dest,
            "2017/10/24/20171024-180102.jpg",
            "hash-ts-base",
            "2017-10-24T00:00:00Z",
        );
        insert_ungrouped_review_row(
            &conn,
            &dest,
            "2017/10/24/20171024-180102-3.jpg",
            "hash-ts-derived",
            "2017-10-24T00:00:00Z",
        );
        conn.execute(
            "UPDATE target_items SET width = 400, height = 533, size_bytes = 48744 WHERE target_path LIKE ?1",
            rusqlite::params!["%20171024-180102.jpg"],
        )
        .unwrap();
        conn.execute(
            "UPDATE target_items SET width = 800, height = 1066, size_bytes = 560895 WHERE target_path LIKE ?1",
            rusqlite::params!["%20171024-180102-3.jpg"],
        )
        .unwrap();

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/api/groups?view=filename&page_index=0&page_size=10")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let target_group = paged["groups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|group| {
                group["members"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|member| {
                        member["target_path"]
                            .as_str()
                            .unwrap()
                            .ends_with("20171024-180102-3.jpg")
                    })
            })
            .unwrap();
        let primary = target_group["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|member| member["is_group_primary"].as_bool().unwrap())
            .unwrap();
        assert!(primary["target_path"]
            .as_str()
            .unwrap()
            .ends_with("20171024-180102-3.jpg"));
    }

    #[tokio::test]
    async fn list_groups_supports_filename_trash_review_mode() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("repo");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        insert_filename_review_rows(
            &conn,
            &dest,
            "2024/06/09/IMG_1234.JPG",
            "2025/03/17/defaultimg_1234.jpg",
        );

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let request = axum::http::Request::builder()
            .uri("/api/groups?view=filename&page_index=0&page_size=10")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let group_id = paged["groups"][0]["group_id"].as_i64().unwrap();
        let members = paged["groups"][0]["members"].as_array().unwrap();
        let default_path = members
            .iter()
            .find_map(|member| {
                let path = member["target_path"].as_str()?;
                path.contains("defaultimg_1234").then_some(path.to_string())
            })
            .unwrap();
        let plain_path = members
            .iter()
            .find_map(|member| {
                let path = member["target_path"].as_str()?;
                path.contains("IMG_1234").then_some(path.to_string())
            })
            .unwrap();

        let resolve_request = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/api/groups/{group_id}/resolve"))
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({
                    "kept": [plain_path.clone()],
                    "rejected": [default_path.clone()],
                    "primary": plain_path.clone(),
                })
                .to_string(),
            ))
            .unwrap();
        let response = app.clone().oneshot(resolve_request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let request = axum::http::Request::builder()
            .uri("/api/groups?view=filename_trash&page_index=0&page_size=10")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(paged["review_mode"], "filename_trash");
        assert_eq!(paged["total_groups"], 1);
        assert_eq!(paged["groups"][0]["status"], "filename-trash-review");
        let members = paged["groups"][0]["members"].as_array().unwrap();
        assert_eq!(members.len(), 2);
        assert!(members.iter().any(|member| {
            member["target_path"]
                .as_str()
                .unwrap()
                .contains("/.photo-org/trash/filename-group-")
        }));
        assert!(members.iter().any(|member| {
            member["target_path"]
                .as_str()
                .unwrap()
                .ends_with("IMG_1234.JPG")
        }));
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
        assert!(html.contains(
            r#"const initialPaging = {"page_index":2,"page_size":1,"group_id":null,"review_mode":"pending"};"#
        ));
    }

    #[tokio::test]
    async fn index_html_embeds_group_id_filter_from_query() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        open_catalog_db(&db_path).unwrap();

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/?group_id=42&page_size=5")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains(r#""group_id":42"#));
        assert!(html.contains(r#""review_mode":"pending""#));
    }

    #[tokio::test]
    async fn index_html_embeds_trash_review_mode_from_query() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        open_catalog_db(&db_path).unwrap();

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/?view=trash&page_size=5")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains(r#""review_mode":"trash""#));
    }

    #[tokio::test]
    async fn resolve_filename_default_review_group_moves_rejected_to_trash() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("repo");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        insert_filename_review_rows(
            &conn,
            &dest,
            "2024/06/09/IMG_1234.JPG",
            "2025/03/17/defaultimg_1234.jpg",
        );

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let request = axum::http::Request::builder()
            .uri("/api/groups?view=filename&page_index=0&page_size=10")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let group_id = paged["groups"][0]["group_id"].as_i64().unwrap();
        let members = paged["groups"][0]["members"].as_array().unwrap();
        let default_path = members
            .iter()
            .find_map(|member| {
                let path = member["target_path"].as_str()?;
                path.contains("defaultimg_1234").then_some(path.to_string())
            })
            .unwrap();
        let plain_path = members
            .iter()
            .find_map(|member| {
                let path = member["target_path"].as_str()?;
                path.contains("IMG_1234").then_some(path.to_string())
            })
            .unwrap();

        let resolve_request = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/api/groups/{group_id}/resolve"))
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({
                    "kept": [plain_path],
                    "rejected": [default_path],
                    "primary": plain_path,
                })
                .to_string(),
            ))
            .unwrap();
        let response = app.oneshot(resolve_request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let verify = open_catalog_db(&db_path).unwrap();
        let rows = verify
            .prepare("SELECT target_path, keep_state, is_group_primary, group_id FROM target_items ORDER BY target_path")
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();

        assert!(rows.iter().any(|(path, keep_state, _, group_id)| {
            path.contains("/.photo-org/trash/filename-group-")
                && path.contains("defaultimg_1234")
                && keep_state == "rejected"
                && group_id.is_none()
        }));
        assert!(rows.iter().any(|(path, keep_state, is_primary, group_id)| {
            path.ends_with("IMG_1234.JPG")
                && keep_state == "kept"
                && *is_primary == 1
                && group_id.is_none()
        }));
    }

    #[tokio::test]
    async fn resolve_bulk_filename_review_is_idempotent_after_partial_completion() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("repo");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        insert_filename_review_rows(
            &conn,
            &dest,
            "2024/06/09/IMG_1234.JPG",
            "2025/03/17/defaultimg_1234.jpg",
        );

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let request = axum::http::Request::builder()
            .uri("/api/groups?view=filename&page_index=0&page_size=10")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let group_id = paged["groups"][0]["group_id"].as_i64().unwrap();
        let members = paged["groups"][0]["members"].as_array().unwrap();
        let default_path = members
            .iter()
            .find_map(|member| {
                let path = member["target_path"].as_str()?;
                path.contains("defaultimg_1234").then_some(path.to_string())
            })
            .unwrap();
        let plain_path = members
            .iter()
            .find_map(|member| {
                let path = member["target_path"].as_str()?;
                path.contains("IMG_1234").then_some(path.to_string())
            })
            .unwrap();
        let payload = json!({
            "resolutions": [{
                "group_id": group_id,
                "kept": [plain_path],
                "rejected": [default_path],
                "primary": plain_path,
            }]
        });

        for _ in 0..2 {
            let resolve_request = axum::http::Request::builder()
                .method("POST")
                .uri("/api/groups/resolve_bulk")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(payload.to_string()))
                .unwrap();
            let response = app.clone().oneshot(resolve_request).await.unwrap();
            let status = response.status();
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        }

        assert!(dest.join(".photo-org/trash/filename-group-2/defaultimg_1234.jpg").exists());
        assert!(!dest
            .join(".photo-org/trash/filename-group-2/1-defaultimg_1234.jpg")
            .exists());

        let verify = open_catalog_db(&db_path).unwrap();
        let rows = verify
            .prepare("SELECT target_path, keep_state, is_group_primary FROM target_items ORDER BY target_path")
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            rows.iter()
                .filter(|(path, keep_state, _)| {
                    path.contains("defaultimg_1234") && keep_state == "rejected"
                })
                .count(),
            1
        );
        assert!(rows.iter().any(|(path, keep_state, is_primary)| {
            path.ends_with("IMG_1234.JPG") && keep_state == "kept" && *is_primary == 1
        }));
    }

    #[tokio::test]
    async fn resolve_filename_review_rejects_kept_path_that_only_exists_in_trash() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("repo");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        insert_filename_review_rows(
            &conn,
            &dest,
            "2024/06/09/IMG_1234.JPG",
            "2025/03/17/defaultimg_1234.jpg",
        );

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let request = axum::http::Request::builder()
            .uri("/api/groups?view=filename&page_index=0&page_size=10")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let group_id = paged["groups"][0]["group_id"].as_i64().unwrap();
        let members = paged["groups"][0]["members"].as_array().unwrap();
        let default_path = members
            .iter()
            .find_map(|member| {
                let path = member["target_path"].as_str()?;
                path.contains("defaultimg_1234").then_some(path.to_string())
            })
            .unwrap();
        let plain_path = members
            .iter()
            .find_map(|member| {
                let path = member["target_path"].as_str()?;
                path.contains("IMG_1234").then_some(path.to_string())
            })
            .unwrap();

        let first_resolve = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/api/groups/{group_id}/resolve"))
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({
                    "kept": [plain_path.clone()],
                    "rejected": [default_path.clone()],
                    "primary": plain_path.clone(),
                })
                .to_string(),
            ))
            .unwrap();
        let response = app.clone().oneshot(first_resolve).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let second_resolve = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/api/groups/{group_id}/resolve"))
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({
                    "kept": [default_path.clone()],
                    "rejected": [],
                    "primary": default_path,
                })
                .to_string(),
            ))
            .unwrap();
        let response = app.oneshot(second_resolve).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("kept file not found at source"));
    }

    #[tokio::test]
    async fn restore_trash_member_supports_filename_trash_review_group() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("repo");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        insert_filename_review_rows(
            &conn,
            &dest,
            "2024/06/09/IMG_1234.JPG",
            "2025/03/17/defaultimg_1234.jpg",
        );

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let request = axum::http::Request::builder()
            .uri("/api/groups?view=filename&page_index=0&page_size=10")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let group_id = paged["groups"][0]["group_id"].as_i64().unwrap();
        let members = paged["groups"][0]["members"].as_array().unwrap();
        let default_path = members
            .iter()
            .find_map(|member| {
                let path = member["target_path"].as_str()?;
                path.contains("defaultimg_1234").then_some(path.to_string())
            })
            .unwrap();
        let plain_path = members
            .iter()
            .find_map(|member| {
                let path = member["target_path"].as_str()?;
                path.contains("IMG_1234").then_some(path.to_string())
            })
            .unwrap();

        let resolve_request = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/api/groups/{group_id}/resolve"))
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({
                    "kept": [plain_path.clone()],
                    "rejected": [default_path.clone()],
                    "primary": plain_path.clone(),
                })
                .to_string(),
            ))
            .unwrap();
        let response = app.clone().oneshot(resolve_request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let request = axum::http::Request::builder()
            .uri("/api/groups?view=filename_trash&page_index=0&page_size=10")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let filename_trash_group_id = paged["groups"][0]["group_id"].as_i64().unwrap();
        let trash_member_id = paged["groups"][0]["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|member| {
                member["target_path"]
                    .as_str()
                    .unwrap()
                    .contains("/.photo-org/trash/filename-group-")
            })
            .unwrap()["id"]
            .as_i64()
            .unwrap();

        let restore_request = axum::http::Request::builder()
            .method("POST")
            .uri(format!(
                "/api/groups/{filename_trash_group_id}/members/{trash_member_id}/restore_trash"
            ))
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(restore_request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(payload["target_path"]
            .as_str()
            .unwrap()
            .contains("defaultimg_1234"));
    }

    #[tokio::test]
    async fn resolve_bulk_continues_after_group_failure() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();

        let keep_1 = dest.join("2024/06/09/keep-1.png");
        let reject_1 = dest.join("2024/06/09/reject-1.png");
        let keep_2 = dest.join("2024/06/09/keep-2.png");
        let reject_2 = dest.join("2024/06/09/reject-2.png");
        fs::create_dir_all(keep_1.parent().unwrap()).unwrap();
        make_png(&keep_1, [255, 0, 0]);
        make_png(&reject_1, [0, 255, 0]);
        make_png(&keep_2, [0, 0, 255]);
        make_png(&reject_2, [255, 255, 0]);

        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        conn.execute(
            r#"
            INSERT INTO target_items (
                target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, origin_source_id, meta_json
            ) VALUES
                (?1, 1, 'image/png', '2024-06-09T00:00:00Z', 'a1', 'p', 64, 32, 32, 1, 'undecided', 1, NULL, '{}'),
                (?2, 1, 'image/png', '2024-06-09T00:00:00Z', 'b1', 'p', 64, 32, 32, 1, 'undecided', 0, NULL, '{}'),
                (?3, 1, 'image/png', '2024-06-09T00:00:00Z', 'a2', 'p', 64, 32, 32, 2, 'undecided', 1, NULL, '{}'),
                (?4, 1, 'image/png', '2024-06-09T00:00:00Z', 'b2', 'p', 64, 32, 32, 2, 'undecided', 0, NULL, '{}')
            "#,
            rusqlite::params![
                keep_1.to_string_lossy().to_string(),
                reject_1.to_string_lossy().to_string(),
                keep_2.to_string_lossy().to_string(),
                reject_2.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        fs::rename(&keep_1, dest.join(".photo-org-trash-temp-keep-1.png")).unwrap();

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let payload = json!({
            "resolutions": [
                {
                    "group_id": 1,
                    "kept": [keep_1.to_string_lossy().to_string()],
                    "rejected": [reject_1.to_string_lossy().to_string()],
                    "primary": keep_1.to_string_lossy().to_string(),
                },
                {
                    "group_id": 2,
                    "kept": [keep_2.to_string_lossy().to_string()],
                    "rejected": [reject_2.to_string_lossy().to_string()],
                    "primary": keep_2.to_string_lossy().to_string(),
                }
            ]
        });

        let resolve_request = axum::http::Request::builder()
            .method("POST")
            .uri("/api/groups/resolve_bulk")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(payload.to_string()))
            .unwrap();
        let response = app.oneshot(resolve_request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["status"], "partial");
        assert_eq!(result["applied_groups"], 1);
        assert_eq!(result["skipped_groups"], 0);
        assert_eq!(result["failed_groups"], 1);
        assert_eq!(result["errors"][0]["group_id"], 1);
        assert!(result["errors"][0]["error"]
            .as_str()
            .unwrap()
            .contains("kept file not found at source"));

        let verify = open_catalog_db(&db_path).unwrap();
        let rows = verify
            .prepare("SELECT group_id, target_path, keep_state, is_group_primary FROM target_items ORDER BY group_id, target_path")
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();

        assert!(rows.iter().any(|(group_id, path, keep_state, is_primary)| {
            *group_id == Some(1)
                && path == &reject_1.to_string_lossy()
                && keep_state == "undecided"
                && *is_primary == 0
        }));
        assert!(rows.iter().any(|(group_id, path, keep_state, is_primary)| {
            *group_id == Some(2)
                && path.ends_with("/.photo-org/trash/group-2/reject-2.png")
                && keep_state == "rejected"
                && *is_primary == 0
        }));
        assert!(rows.iter().any(|(group_id, path, keep_state, is_primary)| {
            *group_id == Some(2)
                && path == &keep_2.to_string_lossy()
                && keep_state == "kept"
                && *is_primary == 1
        }));
        assert!(dest.join(".photo-org/trash/group-2/reject-2.png").exists());
    }

    #[tokio::test]
    async fn trash_review_html_uses_restore_instead_of_decision_buttons() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let db_path = tmp.path().join("catalog.db");
        open_catalog_db(&db_path).unwrap();

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/?view=trash")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("Restore"));
        assert!(html.contains("Delete trash file"));
    }

    #[tokio::test]
    async fn list_groups_can_load_trash_review_group_by_group_id() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let trash_dir = dest.join(".photo-org/trash/group-8");
        fs::create_dir_all(&trash_dir).unwrap();
        let keep_path = dest.join("2024/06/09/keep.png");
        let reject_path = trash_dir.join("reject.png");
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
                (?1, 1, 'image/png', '2024-06-09T00:00:00Z', 'a', 'p', 64, 32, 32, 8, 'kept', 1, NULL, '{}'),
                (?2, 1, 'image/png', '2024-06-09T00:00:00Z', 'b', 'p', 64, 32, 32, 8, 'rejected', 0, NULL, '{}')
            "#,
            rusqlite::params![
                keep_path.to_string_lossy().to_string(),
                reject_path.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        let app = router(AppState { db_path, dest });
        let request = axum::http::Request::builder()
            .uri("/api/groups?group_id=8&view=trash")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let paged: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(paged["total_groups"], 1);
        assert_eq!(paged["groups"][0]["group_id"], 8);
        assert_eq!(paged["groups"][0]["status"], "trash-review");
        assert_eq!(paged["review_mode"], "trash");
        assert_eq!(paged["groups"][0]["members"].as_array().unwrap().len(), 2);
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
        assert_eq!(
            moved_target_path,
            logical_target_path(&dest, &moved_path).unwrap()
        );
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
        let trash_group_dir = trash_path.parent().unwrap().to_path_buf();
        let trash_root = dest.join(".photo-org/trash");
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
        assert!(!trash_group_dir.exists());
        assert!(!trash_root.exists());

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
    async fn restore_trash_member_moves_file_back_and_marks_kept() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let keep_path = dest.join("2024/06/09/keep.png");
        let trash_path = dest.join(".photo-org/trash/group-5/reject.png");
        let trash_group_dir = trash_path.parent().unwrap().to_path_buf();
        let trash_root = dest.join(".photo-org/trash");
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
                (?1, 1, 'image/png', '2024-06-09T00:00:00Z', 'a', 'p', 64, 32, 32, 5, 'kept', 1, NULL, '{}'),
                (?2, 1, 'image/png', '2024-06-09T00:00:00Z', 'b', 'p', 64, 32, 32, 5, 'rejected', 0, NULL, '{}')
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
            .uri("/api/groups/5/members/2/restore_trash")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!trash_path.exists());
        assert!(!trash_group_dir.exists());
        assert!(!trash_root.exists());
        let restored_path = dest.join("2024/06/09/reject.png");
        assert!(restored_path.exists());

        let conn = open_catalog_db(&db_path).unwrap();
        let restored_row: (String, String) = conn
            .query_row(
                "SELECT target_path, keep_state FROM target_items WHERE id = 2",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            restored_row.0,
            logical_target_path(&dest, &restored_path).unwrap()
        );
        assert_eq!(restored_row.1, "kept");
    }

    #[tokio::test]
    async fn delete_trash_group_removes_all_trash_members_in_group() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let keep_path = dest.join("2024/06/09/keep.png");
        let trash_path_a = dest.join(".photo-org/trash/group-7/reject-a.png");
        let trash_path_b = dest.join(".photo-org/trash/group-7/reject-b.png");
        let trash_group_dir = trash_path_a.parent().unwrap().to_path_buf();
        let trash_root = dest.join(".photo-org/trash");
        fs::create_dir_all(keep_path.parent().unwrap()).unwrap();
        fs::create_dir_all(trash_path_a.parent().unwrap()).unwrap();
        make_png(&keep_path, [255, 0, 0]);
        make_png(&trash_path_a, [0, 255, 0]);
        make_png(&trash_path_b, [0, 0, 255]);

        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        conn.execute(
            r#"
            INSERT INTO target_items (
                target_path, size_bytes, mime_type, created_at, exact_hash, phash, phash_bits,
                width, height, group_id, keep_state, is_group_primary, origin_source_id, meta_json
            ) VALUES
                (?1, 1, 'image/png', '2024-06-09T00:00:00Z', 'a', 'p', 64, 32, 32, 7, 'kept', 1, NULL, '{}'),
                (?2, 1, 'image/png', '2024-06-09T00:00:00Z', 'b', 'p', 64, 32, 32, 7, 'rejected', 0, NULL, '{}'),
                (?3, 1, 'image/png', '2024-06-09T00:00:00Z', 'c', 'p', 64, 32, 32, 7, 'rejected', 0, NULL, '{}')
            "#,
            rusqlite::params![
                keep_path.to_string_lossy().to_string(),
                trash_path_a.to_string_lossy().to_string(),
                trash_path_b.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/api/groups/7/delete_trash")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!trash_path_a.exists());
        assert!(!trash_path_b.exists());
        assert!(!trash_group_dir.exists());
        assert!(!trash_root.exists());

        let conn = open_catalog_db(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM target_items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn delete_trash_bulk_removes_members_across_multiple_groups() {
        let tmp = tempdir().unwrap();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(&dest).unwrap();
        let keep_dir = dest.join("2024/06/09");
        let trash_dir_1 = dest.join(".photo-org/trash/group-11");
        let trash_dir_2 = dest.join(".photo-org/trash/group-12");
        let trash_root = dest.join(".photo-org/trash");
        fs::create_dir_all(&keep_dir).unwrap();
        fs::create_dir_all(&trash_dir_1).unwrap();
        fs::create_dir_all(&trash_dir_2).unwrap();
        make_png(&keep_dir.join("keep-1.png"), [255, 0, 0]);
        make_png(&keep_dir.join("keep-2.png"), [255, 255, 0]);
        let trash_path_1 = trash_dir_1.join("reject-1.png");
        let trash_path_2 = trash_dir_2.join("reject-2.png");
        make_png(&trash_path_1, [0, 255, 0]);
        make_png(&trash_path_2, [0, 0, 255]);

        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        insert_trash_review_group(
            &conn,
            11,
            &keep_dir.join("keep-1.png").to_string_lossy(),
            &trash_path_1.to_string_lossy(),
        );
        insert_trash_review_group(
            &conn,
            12,
            &keep_dir.join("keep-2.png").to_string_lossy(),
            &trash_path_2.to_string_lossy(),
        );

        let app = router(AppState {
            db_path: db_path.clone(),
            dest: dest.clone(),
        });
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/api/groups/delete_trash_bulk")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({"member_ids":[2, 4]}).to_string(),
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!trash_path_1.exists());
        assert!(!trash_path_2.exists());
        assert!(!trash_dir_1.exists());
        assert!(!trash_dir_2.exists());
        assert!(!trash_root.exists());

        let conn = open_catalog_db(&db_path).unwrap();
        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM target_items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(remaining, 2);
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
