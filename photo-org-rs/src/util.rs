use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use regex::Regex;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

pub fn canonicalize_for_check(path: impl AsRef<Path>) -> Result<PathBuf> {
    fs::canonicalize(path.as_ref())
        .with_context(|| format!("canonicalize {}", path.as_ref().display()))
}

pub fn ensure_under_root(root: &Path, candidate: &Path) -> Result<()> {
    let root = canonicalize_for_check(root)?;
    let candidate = canonicalize_for_check(candidate)?;
    if candidate == root || candidate.starts_with(&root) {
        Ok(())
    } else {
        anyhow::bail!(
            "path {} escapes destination root {}",
            candidate.display(),
            root.display()
        );
    }
}

pub fn is_excluded_dir(path: &Path) -> bool {
    path.components().any(|component| match component {
        Component::Normal(part) => {
            let s = part.to_string_lossy();
            s.starts_with('.') || s == "trash" || s == ".photo-org"
        }
        _ => false,
    })
}

pub fn system_time_to_rfc3339(time: SystemTime) -> String {
    let dt: DateTime<Utc> = time.into();
    dt.to_rfc3339()
}

pub fn fallback_file_time(meta: &fs::Metadata) -> String {
    meta.created()
        .or_else(|_| meta.modified())
        .map(system_time_to_rfc3339)
        .unwrap_or_else(|_| Utc::now().to_rfc3339())
}

pub fn filename_date(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_string_lossy();
    let re = Regex::new(r"(?P<y>\d{4})[-_]?((?P<m>\d{2})[-_]?)(?P<d>\d{2})").ok()?;
    let caps = re.captures(&name)?;
    let y = caps.name("y")?.as_str().parse::<i32>().ok()?;
    let m = caps.name("m")?.as_str().parse::<u32>().ok()?;
    let d = caps.name("d")?.as_str().parse::<u32>().ok()?;
    let date = NaiveDate::from_ymd_opt(y, m, d)?;
    Some(DateTime::<Utc>::from_naive_utc_and_offset(date.and_hms_opt(0, 0, 0)?, Utc).to_rfc3339())
}

pub fn parse_exif_datetime(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let naive = NaiveDateTime::parse_from_str(raw, "%Y:%m:%d %H:%M:%S").ok()?;
    Some(Utc.from_utc_datetime(&naive).to_rfc3339())
}

pub fn date_for_target(created_at: &str) -> (String, String, String) {
    let parsed = DateTime::parse_from_rfc3339(created_at)
        .ok()
        .map(|dt| dt.date_naive());
    let date = parsed.unwrap_or_else(|| Utc::now().date_naive());
    (
        format!("{:04}", date.year()),
        format!("{:02}", date.month()),
        format!("{:02}", date.day()),
    )
}

pub fn to_bytes_lossless(path: &Path) -> Result<Vec<u8>> {
    Ok(fs::read(path).with_context(|| format!("read {}", path.display()))?)
}

pub fn best_effort_mime(path: &Path, bytes: &[u8]) -> String {
    if let Some(kind) = infer::get(bytes) {
        return kind.mime_type().to_string();
    }
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "tif" | "tiff" => "image/tiff",
        "heic" | "heif" => "image/heic",
        "mp4" => "video/mp4",
        "mov" => "video/quicktime",
        "mkv" => "video/x-matroska",
        _ => "application/octet-stream",
    }
    .to_string()
}

pub fn safe_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("file")
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[derive(Debug)]
pub struct ProgressReporter {
    label: String,
    total: usize,
    step: usize,
    processed: AtomicUsize,
    next_log_at: AtomicUsize,
}

impl ProgressReporter {
    pub fn new(label: impl Into<String>, total: usize) -> Self {
        let step = progress_step(total);
        Self {
            label: label.into(),
            total,
            step,
            processed: AtomicUsize::new(0),
            next_log_at: AtomicUsize::new(step.min(total.max(1))),
        }
    }

    pub fn log_start(&self) {
        tracing::info!(stage = %self.label, total = self.total, "progress start");
    }

    pub fn item_done(&self) {
        let processed = self.processed.fetch_add(1, Ordering::Relaxed) + 1;
        self.maybe_log(processed);
    }

    fn maybe_log(&self, processed: usize) {
        if self.total == 0 {
            return;
        }

        loop {
            let next = self.next_log_at.load(Ordering::Relaxed);
            if processed < next {
                return;
            }

            let new_next = if next >= self.total {
                self.total.saturating_add(1)
            } else {
                (next + self.step).min(self.total)
            };
            if self
                .next_log_at
                .compare_exchange(next, new_next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                tracing::info!(
                    stage = %self.label,
                    processed,
                    total = self.total,
                    remaining = self.total.saturating_sub(processed),
                    "progress update"
                );
                return;
            }
        }
    }
}

fn progress_step(total: usize) -> usize {
    match total {
        0..=20 => 1,
        21..=200 => 10,
        _ => (total / 20).max(25),
    }
}
