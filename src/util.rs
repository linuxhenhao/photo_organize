use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Utc};
use regex::Regex;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

pub const PROFILE_ENV: &str = "PHOTO_ORG_PROFILE";

pub fn canonicalize_for_check(path: impl AsRef<Path>) -> Result<PathBuf> {
    fs::canonicalize(path.as_ref())
        .with_context(|| format!("canonicalize {}", path.as_ref().display()))
}

pub fn target_root_name(dest: &Path) -> Result<&std::ffi::OsStr> {
    dest.file_name()
        .filter(|name| !name.is_empty())
        .with_context(|| {
            format!(
                "destination root {} has no final path component",
                dest.display()
            )
        })
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

/// The "logical root" for target_path storage is the final path component of `--dest`.
/// For example, both `--dest repo` and `--dest /root/a/b/repo` store `target_path`
/// as `repo/2023/xxx.jpg`.
pub fn target_base_path(dest: &Path) -> PathBuf {
    match dest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Converts a physical path under `--dest` into the logical `target_items.target_path`
/// representation stored in SQLite.
pub fn logical_target_path(dest: &Path, physical_path: &Path) -> Result<String> {
    let root_name = target_root_name(dest)?;
    let base = target_base_path(dest);
    if let Ok(rooted) = physical_path.strip_prefix(&base) {
        let mut components = rooted.components();
        if matches!(components.next(), Some(Component::Normal(part)) if part == root_name) {
            return Ok(rooted.to_string_lossy().to_string());
        }
    }

    let relative = physical_path.strip_prefix(dest).with_context(|| {
        format!(
            "path {} is not under destination root {}",
            physical_path.display(),
            dest.display()
        )
    })?;
    Ok(PathBuf::from(root_name)
        .join(relative)
        .to_string_lossy()
        .to_string())
}

/// Resolves a logical target_path from the database to a physical path on disk.
pub fn resolve_physical_path(dest: &Path, target_path: &str) -> PathBuf {
    let target = Path::new(target_path);
    if target.is_absolute() {
        return target.to_path_buf();
    }

    let root_name = dest.file_name().filter(|name| !name.is_empty());
    let starts_with_root = root_name
        .and_then(|root| {
            target
                .components()
                .next()
                .map(|component| (root, component))
        })
        .map(|(root, component)| matches!(component, Component::Normal(part) if part == root))
        .unwrap_or(false);

    if starts_with_root {
        target_base_path(dest).join(target)
    } else {
        dest.join(target)
    }
}

/// A centralized check for ensuring a path is safely under the target root.
pub fn ensure_under_target_root(dest: &Path, candidate: &Path) -> Result<()> {
    ensure_under_root(&target_base_path(dest), candidate)
}

fn normalize_lexical_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

pub fn remove_empty_parent_dirs(start_dir: &Path, stop_at: &Path) -> Result<()> {
    let stop_at_normalized = normalize_lexical_path(stop_at);
    let mut current = start_dir.to_path_buf();
    while normalize_lexical_path(&current) != stop_at_normalized {
        let current_normalized = normalize_lexical_path(&current);
        if !current_normalized.starts_with(&stop_at_normalized) {
            anyhow::bail!(
                "directory {} escapes cleanup root {}",
                current.display(),
                stop_at.display()
            );
        }
        match fs::remove_dir(&current) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
            Err(err) => return Err(err.into()),
        }
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent.to_path_buf();
    }
    Ok(())
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

pub fn parse_video_container_datetime<R: Read + Seek>(
    reader: &mut R,
    mime_type: &str,
) -> Option<String> {
    let mime = mime_type.trim().to_ascii_lowercase();
    match mime.as_str() {
        "video/mp4" | "video/quicktime" | "application/mp4" | "video/x-m4v" | "video/3gpp"
        | "video/3gpp2" => {
            let end = reader.seek(SeekFrom::End(0)).ok()?;
            reader.seek(SeekFrom::Start(0)).ok()?;
            parse_isobmff_boxes_for_datetime(reader, 0, end)
        }
        "video/x-matroska" | "video/webm" => {
            let end = reader.seek(SeekFrom::End(0)).ok()?;
            reader.seek(SeekFrom::Start(0)).ok()?;
            parse_matroska_date_utc(reader, 0, end)
        }
        _ => None,
    }
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

pub fn best_effort_mime(path: &Path, bytes: &[u8]) -> String {
    let detected = mimetype_detector::detect(bytes).mime().to_string();
    if is_generic_mime(&detected) {
        if let Some(fallback) = mime_from_extension(path) {
            return fallback.to_string();
        }
    }
    detected
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

#[derive(Clone, Copy)]
struct BoxDescriptor {
    kind: [u8; 4],
    payload_start: u64,
    box_end: u64,
}

fn mime_from_extension(path: &Path) -> Option<&'static str> {
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    match ext.as_str() {
        "mov" | "qt" => Some("video/quicktime"),
        "mp4" | "m4v" => Some("video/mp4"),
        "3gp" => Some("video/3gpp"),
        "3g2" => Some("video/3gpp2"),
        "mkv" => Some("video/x-matroska"),
        "webm" => Some("video/webm"),
        "mts" | "m2ts" => Some("video/mp2t"),
        "avi" => Some("video/x-msvideo"),
        "mxf" => Some("application/mxf"),
        _ => None,
    }
}

fn is_generic_mime(mime: &str) -> bool {
    matches!(
        mime.trim().to_ascii_lowercase().as_str(),
        "" | "application/octet-stream" | "application/x-empty"
    )
}

fn parse_isobmff_boxes_for_datetime<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
) -> Option<String> {
    let mut header_candidate = None;
    let mut offset = start;
    while offset.checked_add(8)? <= end {
        let desc = read_isobmff_box_descriptor(reader, offset, end)?;
        match desc.kind.as_slice() {
            b"moov" | b"trak" | b"mdia" | b"udta" => {
                if let Some(ts) =
                    parse_isobmff_boxes_for_datetime(reader, desc.payload_start, desc.box_end)
                {
                    return Some(ts);
                }
            }
            b"meta" => {
                if let Some(ts) =
                    parse_isobmff_meta_datetime(reader, desc.payload_start, desc.box_end)
                {
                    return Some(ts);
                }
            }
            b"\xa9day" => {
                if let Some(ts) =
                    parse_isobmff_metadata_item_datetime(reader, desc.payload_start, desc.box_end)
                {
                    return Some(ts);
                }
            }
            b"mvhd" => {
                if header_candidate.is_none() {
                    header_candidate =
                        parse_quicktime_creation_time(reader, desc.payload_start, desc.box_end);
                }
            }
            b"mdhd" | b"tkhd" => {
                if header_candidate.is_none() {
                    header_candidate =
                        parse_quicktime_creation_time(reader, desc.payload_start, desc.box_end);
                }
            }
            _ => {}
        }

        if desc.box_end <= offset {
            return None;
        }
        offset = desc.box_end;
    }
    header_candidate
}

fn parse_isobmff_meta_datetime<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
) -> Option<String> {
    if start.checked_add(4)? > end {
        return None;
    }

    let child_start = start.checked_add(4)?;
    let children = collect_isobmff_boxes(reader, child_start, end)?;

    let mut keys = None;
    for child in &children {
        if child.kind == *b"keys" {
            keys = parse_isobmff_keys_box(reader, child.payload_start, child.box_end);
            break;
        }
    }

    for child in &children {
        match child.kind.as_slice() {
            b"ilst" => {
                if let Some(ts) = parse_isobmff_ilst_datetime(
                    reader,
                    child.payload_start,
                    child.box_end,
                    keys.as_deref(),
                ) {
                    return Some(ts);
                }
            }
            b"\xa9day" => {
                if let Some(ts) =
                    parse_isobmff_metadata_item_datetime(reader, child.payload_start, child.box_end)
                {
                    return Some(ts);
                }
            }
            _ => {}
        }
    }

    None
}

fn collect_isobmff_boxes<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
) -> Option<Vec<BoxDescriptor>> {
    let mut boxes = Vec::new();
    let mut offset = start;
    while offset.checked_add(8)? <= end {
        let desc = read_isobmff_box_descriptor(reader, offset, end)?;
        if desc.box_end <= offset {
            return None;
        }
        boxes.push(desc);
        offset = desc.box_end;
    }
    Some(boxes)
}

fn read_isobmff_box_descriptor<R: Read + Seek>(
    reader: &mut R,
    offset: u64,
    end: u64,
) -> Option<BoxDescriptor> {
    reader.seek(SeekFrom::Start(offset)).ok()?;
    let size32 = read_u32(reader)? as u64;
    let kind = read_box_type(reader)?;
    let (header_len, box_len) = if size32 == 1 {
        let size64 = read_u64(reader)?;
        (16_u64, size64)
    } else if size32 == 0 {
        (8_u64, end.checked_sub(offset)?)
    } else {
        (8_u64, size32)
    };

    if box_len < header_len {
        return None;
    }

    let box_end = offset.checked_add(box_len)?;
    if box_end > end {
        return None;
    }

    Some(BoxDescriptor {
        kind,
        payload_start: offset.checked_add(header_len)?,
        box_end,
    })
}

fn parse_isobmff_keys_box<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
) -> Option<Vec<String>> {
    if start.checked_add(8)? > end {
        return None;
    }

    reader.seek(SeekFrom::Start(start)).ok()?;
    let _version_and_flags = read_u32(reader)?;
    let entry_count = read_u32(reader)? as usize;
    let mut keys = Vec::with_capacity(entry_count);

    for _ in 0..entry_count {
        let entry_size = read_u32(reader)? as u64;
        let _namespace = read_box_type(reader)?;
        if entry_size < 8 {
            return None;
        }
        let name_len = usize::try_from(entry_size.checked_sub(8)?).ok()?;
        let mut buf = vec![0u8; name_len];
        reader.read_exact(&mut buf).ok()?;
        keys.push(String::from_utf8_lossy(&buf).trim().to_string());
    }

    Some(keys)
}

fn parse_isobmff_ilst_datetime<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
    keys: Option<&[String]>,
) -> Option<String> {
    let children = collect_isobmff_boxes(reader, start, end)?;
    for child in children {
        let direct_day = child.kind == *b"\xa9day";
        let mapped_key = keys.and_then(|keys| {
            let index = u32::from_be_bytes(child.kind);
            usize::try_from(index)
                .ok()
                .and_then(|i| i.checked_sub(1))
                .and_then(|i| keys.get(i))
        });
        if direct_day || mapped_key.is_some_and(|key| metadata_key_looks_like_datetime(key)) {
            if let Some(ts) =
                parse_isobmff_metadata_item_datetime(reader, child.payload_start, child.box_end)
            {
                return Some(ts);
            }
        }
    }
    None
}

fn metadata_key_looks_like_datetime(key: &str) -> bool {
    let key = key.trim().to_ascii_lowercase();
    matches!(
        key.as_str(),
        "com.apple.quicktime.creationdate" | "creation_time" | "date" | "creationdate"
    ) || (key.contains("creation") && key.contains("date"))
}

fn parse_isobmff_metadata_item_datetime<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
) -> Option<String> {
    if let Some(ts) = parse_isobmff_item_data_children_datetime(reader, start, end) {
        return Some(ts);
    }

    let bytes = read_exact_range(reader, start, end)?;
    parse_loose_datetime_text(&String::from_utf8_lossy(&bytes))
}

fn parse_isobmff_item_data_children_datetime<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
) -> Option<String> {
    let children = collect_isobmff_boxes(reader, start, end)?;
    for child in children {
        if child.kind != *b"data" {
            continue;
        }
        let payload_start = child.payload_start.checked_add(8)?;
        if payload_start > child.box_end {
            continue;
        }
        let bytes = read_exact_range(reader, payload_start, child.box_end)?;
        if let Some(ts) = parse_loose_datetime_text(&String::from_utf8_lossy(&bytes)) {
            return Some(ts);
        }
    }
    None
}

fn parse_quicktime_creation_time<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
) -> Option<String> {
    if start.checked_add(8)? > end {
        return None;
    }

    reader.seek(SeekFrom::Start(start)).ok()?;
    let mut version_and_flags = [0u8; 4];
    reader.read_exact(&mut version_and_flags).ok()?;
    let version = version_and_flags[0];
    let seconds = match version {
        0 => read_u32(reader)? as u64,
        1 => read_u64(reader)?,
        _ => return None,
    };
    quicktime_epoch_seconds_to_rfc3339(seconds)
}

fn parse_loose_datetime_text(raw: &str) -> Option<String> {
    let raw = raw.trim_matches('\0').trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return Some(parsed.with_timezone(&Utc).to_rfc3339());
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y:%m:%d %H:%M:%S",
        "%Y-%m-%d",
    ] {
        if fmt == "%Y-%m-%d" {
            if let Ok(date) = NaiveDate::parse_from_str(raw, fmt) {
                return Some(
                    DateTime::<Utc>::from_naive_utc_and_offset(date.and_hms_opt(0, 0, 0)?, Utc)
                        .to_rfc3339(),
                );
            }
            continue;
        }
        if let Ok(parsed) = NaiveDateTime::parse_from_str(raw, fmt) {
            return Some(Utc.from_utc_datetime(&parsed).to_rfc3339());
        }
    }
    None
}

fn quicktime_epoch_seconds_to_rfc3339(seconds: u64) -> Option<String> {
    if seconds == 0 {
        return None;
    }

    let epoch = DateTime::<Utc>::from_naive_utc_and_offset(
        NaiveDate::from_ymd_opt(1904, 1, 1)?.and_hms_opt(0, 0, 0)?,
        Utc,
    );
    let seconds = i64::try_from(seconds).ok()?;
    epoch
        .checked_add_signed(Duration::seconds(seconds))
        .map(|dt| dt.to_rfc3339())
}

fn read_u32<R: Read>(reader: &mut R) -> Option<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).ok()?;
    Some(u32::from_be_bytes(buf))
}

fn read_u64<R: Read>(reader: &mut R) -> Option<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).ok()?;
    Some(u64::from_be_bytes(buf))
}

fn read_box_type<R: Read>(reader: &mut R) -> Option<[u8; 4]> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).ok()?;
    Some(buf)
}

fn read_exact_range<R: Read + Seek>(reader: &mut R, start: u64, end: u64) -> Option<Vec<u8>> {
    let len = usize::try_from(end.checked_sub(start)?).ok()?;
    reader.seek(SeekFrom::Start(start)).ok()?;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).ok()?;
    Some(buf)
}

fn parse_matroska_date_utc<R: Read + Seek>(reader: &mut R, start: u64, end: u64) -> Option<String> {
    let mut offset = start;
    while offset < end {
        let (id, id_len, _) = read_ebml_vint(reader, offset, true)?;
        let size_offset = offset.checked_add(id_len)?;
        let (size, size_len, unknown_size) = read_ebml_vint(reader, size_offset, false)?;
        let payload_start = size_offset.checked_add(size_len)?;
        let payload_end = if unknown_size {
            end
        } else {
            payload_start.checked_add(size)?
        };
        if payload_end > end || payload_end <= offset {
            return None;
        }

        match id {
            0x1853_8067 | 0x1549_A966 => {
                if let Some(ts) = parse_matroska_date_utc(reader, payload_start, payload_end) {
                    return Some(ts);
                }
            }
            0x4461 => {
                return parse_matroska_date_utc_value(reader, payload_start, payload_end);
            }
            _ => {}
        }

        offset = payload_end;
    }
    None
}

fn parse_matroska_date_utc_value<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
) -> Option<String> {
    let len = end.checked_sub(start)?;
    if len == 0 || len > 8 {
        return None;
    }
    reader.seek(SeekFrom::Start(start)).ok()?;
    let mut buf = [0u8; 8];
    let skip = usize::try_from(8_u64.checked_sub(len)?).ok()?;
    reader.read_exact(&mut buf[skip..]).ok()?;
    let nanos = i64::from_be_bytes(buf);
    let epoch = DateTime::<Utc>::from_naive_utc_and_offset(
        NaiveDate::from_ymd_opt(2001, 1, 1)?.and_hms_opt(0, 0, 0)?,
        Utc,
    );
    let secs = nanos.div_euclid(1_000_000_000);
    let rem_nanos = nanos.rem_euclid(1_000_000_000) as u32;
    epoch
        .checked_add_signed(Duration::seconds(secs))?
        .checked_add_signed(Duration::nanoseconds(i64::from(rem_nanos)))
        .map(|dt| dt.to_rfc3339())
}

fn read_ebml_vint<R: Read + Seek>(
    reader: &mut R,
    offset: u64,
    keep_marker: bool,
) -> Option<(u64, u64, bool)> {
    reader.seek(SeekFrom::Start(offset)).ok()?;
    let mut first = [0u8; 1];
    reader.read_exact(&mut first).ok()?;
    let first = first[0];
    if first == 0 {
        return None;
    }

    let width = u64::from(first.leading_zeros() + 1);
    if width == 0 || width > 8 {
        return None;
    }

    let mut value = if keep_marker {
        u64::from(first)
    } else {
        u64::from(first & ((1_u8 << (8 - width)) - 1))
    };
    for _ in 1..width {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).ok()?;
        value = (value << 8) | u64::from(byte[0]);
    }

    let unknown_size = !keep_marker && width < 8 && value == ((1_u64 << (7 * width)) - 1);
    Some((value, width, unknown_size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::tempdir;

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join(name)
    }

    #[test]
    fn best_effort_mime_uses_content_for_jpeg_with_raw_extension() {
        let bytes = fs::read(fixture_path("source_mock/img_2023_05_01.jpg")).unwrap();

        let mime = best_effort_mime(Path::new("misleading.arw"), &bytes);

        assert_eq!(mime, "image/jpeg");
    }

    #[test]
    fn best_effort_mime_detects_raw_bytes_with_wrong_extension() {
        let bytes = fs::read(fixture_path("source/DSC00903.ARW")).unwrap();

        let mime = best_effort_mime(Path::new("misleading.jpg"), &bytes);

        assert_eq!(mime, "image/x-sony-sr2");
    }

    #[test]
    fn parse_video_container_datetime_reads_mp4_mvhd_version_0() {
        let seconds = quicktime_seconds_for("2025-01-02T03:04:56+00:00");
        let mut cursor = Cursor::new(build_minimal_mp4_with_mvhd_v0(seconds));

        let actual = parse_video_container_datetime(&mut cursor, "video/mp4");

        assert_eq!(actual.as_deref(), Some("2025-01-02T03:04:56+00:00"));
    }

    #[test]
    fn parse_video_container_datetime_ignores_non_video_mime() {
        let seconds = quicktime_seconds_for("2025-01-02T03:04:56+00:00");
        let mut cursor = Cursor::new(build_minimal_mp4_with_mvhd_v0(seconds));

        let actual = parse_video_container_datetime(&mut cursor, "image/jpeg");

        assert!(actual.is_none());
    }

    #[test]
    fn parse_video_container_datetime_prefers_quicktime_day_metadata() {
        let mut cursor = Cursor::new(build_quicktime_day_metadata_mp4(
            "2024-09-10T11:12:13+08:00",
        ));

        let actual = parse_video_container_datetime(&mut cursor, "video/quicktime");

        assert_eq!(actual.as_deref(), Some("2024-09-10T03:12:13+00:00"));
    }

    #[test]
    fn parse_video_container_datetime_reads_matroska_date_utc() {
        let mut cursor = Cursor::new(build_minimal_matroska_with_date_utc(
            "2024-04-05T06:07:08+00:00",
        ));

        let actual = parse_video_container_datetime(&mut cursor, "video/x-matroska");

        assert_eq!(actual.as_deref(), Some("2024-04-05T06:07:08+00:00"));
    }

    #[test]
    fn best_effort_mime_falls_back_to_common_video_extension() {
        let mime = best_effort_mime(Path::new("clip.mts"), &[]);

        assert_eq!(mime, "video/mp2t");
    }

    #[test]
    fn remove_empty_parent_dirs_accepts_curdir_prefixed_paths() {
        let tmp = tempdir().unwrap();
        let repo = tmp.path().join("repo");
        let group_dir = repo.join(".photo-org/trash/group-10361");
        fs::create_dir_all(&group_dir).unwrap();

        let cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(tmp.path()).unwrap();

        let result = remove_empty_parent_dirs(
            Path::new("./repo/.photo-org/trash/group-10361"),
            Path::new("repo/.photo-org"),
        );

        std::env::set_current_dir(cwd).unwrap();

        assert!(result.is_ok());
        assert!(!group_dir.exists());
        assert!(repo.join(".photo-org").exists());
    }

    fn build_minimal_mp4_with_mvhd_v0(seconds: u64) -> Vec<u8> {
        let creation = u32::try_from(seconds).unwrap().to_be_bytes();
        let mut mvhd = Vec::new();
        mvhd.extend_from_slice(&16_u32.to_be_bytes());
        mvhd.extend_from_slice(b"mvhd");
        mvhd.extend_from_slice(&[0, 0, 0, 0]);
        mvhd.extend_from_slice(&creation);

        let moov_len = 8 + mvhd.len() as u32;
        let mut moov = Vec::new();
        moov.extend_from_slice(&moov_len.to_be_bytes());
        moov.extend_from_slice(b"moov");
        moov.extend_from_slice(&mvhd);

        let mut ftyp = Vec::new();
        ftyp.extend_from_slice(&24_u32.to_be_bytes());
        ftyp.extend_from_slice(b"ftyp");
        ftyp.extend_from_slice(b"isom");
        ftyp.extend_from_slice(&0_u32.to_be_bytes());
        ftyp.extend_from_slice(b"isom");
        ftyp.extend_from_slice(b"mp41");

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ftyp);
        bytes.extend_from_slice(&moov);
        bytes
    }

    fn build_quicktime_day_metadata_mp4(day: &str) -> Vec<u8> {
        let day_bytes = day.as_bytes();
        let mut data = Vec::new();
        data.extend_from_slice(&u32::try_from(16 + day_bytes.len()).unwrap().to_be_bytes());
        data.extend_from_slice(b"data");
        data.extend_from_slice(&1_u32.to_be_bytes());
        data.extend_from_slice(&0_u32.to_be_bytes());
        data.extend_from_slice(day_bytes);

        let mut day_box = Vec::new();
        day_box.extend_from_slice(&u32::try_from(8 + data.len()).unwrap().to_be_bytes());
        day_box.extend_from_slice(b"\xa9day");
        day_box.extend_from_slice(&data);

        let mut ilst = Vec::new();
        ilst.extend_from_slice(&u32::try_from(8 + day_box.len()).unwrap().to_be_bytes());
        ilst.extend_from_slice(b"ilst");
        ilst.extend_from_slice(&day_box);

        let mut meta_payload = Vec::new();
        meta_payload.extend_from_slice(&0_u32.to_be_bytes());
        meta_payload.extend_from_slice(&ilst);

        let mut meta = Vec::new();
        meta.extend_from_slice(&u32::try_from(8 + meta_payload.len()).unwrap().to_be_bytes());
        meta.extend_from_slice(b"meta");
        meta.extend_from_slice(&meta_payload);

        let mut udta = Vec::new();
        udta.extend_from_slice(&u32::try_from(8 + meta.len()).unwrap().to_be_bytes());
        udta.extend_from_slice(b"udta");
        udta.extend_from_slice(&meta);

        let mut moov = Vec::new();
        moov.extend_from_slice(&u32::try_from(8 + udta.len()).unwrap().to_be_bytes());
        moov.extend_from_slice(b"moov");
        moov.extend_from_slice(&udta);

        let mut ftyp = Vec::new();
        ftyp.extend_from_slice(&24_u32.to_be_bytes());
        ftyp.extend_from_slice(b"ftyp");
        ftyp.extend_from_slice(b"qt  ");
        ftyp.extend_from_slice(&0_u32.to_be_bytes());
        ftyp.extend_from_slice(b"qt  ");
        ftyp.extend_from_slice(b"mp42");

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ftyp);
        bytes.extend_from_slice(&moov);
        bytes
    }

    fn build_minimal_matroska_with_date_utc(rfc3339: &str) -> Vec<u8> {
        let target = DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc);
        let epoch = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2001, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            Utc,
        );
        let nanos = target
            .signed_duration_since(epoch)
            .num_nanoseconds()
            .unwrap()
            .to_be_bytes();

        let mut date_utc = vec![0x44, 0x61, 0x88];
        date_utc.extend_from_slice(&nanos);

        let mut info = vec![0x15, 0x49, 0xA9, 0x66, 0x8B];
        info.extend_from_slice(&date_utc);

        let mut segment = vec![0x18, 0x53, 0x80, 0x67, 0x90];
        segment.extend_from_slice(&info);

        let mut ebml = vec![0x1A, 0x45, 0xDF, 0xA3, 0x80];
        ebml.extend_from_slice(&segment);
        ebml
    }

    fn quicktime_seconds_for(rfc3339: &str) -> u64 {
        let target = DateTime::parse_from_rfc3339(rfc3339).unwrap();
        let epoch = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(1904, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            Utc,
        );
        target
            .with_timezone(&Utc)
            .signed_duration_since(epoch)
            .num_seconds()
            .try_into()
            .unwrap()
    }

    #[test]
    fn path_resolution_consistent_with_db_storage() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("repo");
        fs::create_dir_all(&dest).unwrap();

        // 1. target_base_path should be the parent of dest
        let base = target_base_path(&dest);
        assert_eq!(base, tmp.path());

        // 2. resolve_physical_path should correctly join
        let target_path = "repo/2023/05/01/img.jpg";
        let physical = resolve_physical_path(&dest, target_path);
        assert_eq!(physical, tmp.path().join(target_path));
        assert_eq!(logical_target_path(&dest, &physical).unwrap(), target_path);

        // 3. ensure_under_target_root safety check
        let safe_file = tmp.path().join("repo/safe.jpg");
        fs::write(&safe_file, "data").unwrap();
        assert!(ensure_under_target_root(&dest, &safe_file).is_ok());

        // For unsafe file, we need something TRULY outside the logical base (tmp.path())
        let unsafe_tmp = tempfile::tempdir().unwrap();
        let unsafe_file = unsafe_tmp.path().join("outside.jpg");
        fs::write(&unsafe_file, "data").unwrap();
        assert!(ensure_under_target_root(&dest, &unsafe_file).is_err());
    }

    #[test]
    fn path_resolution_handles_relative_dest() {
        let dest = Path::new("repo");
        // target_base_path("repo") -> "."
        assert_eq!(target_base_path(dest), Path::new("."));
        assert_eq!(
            resolve_physical_path(dest, "repo/a.jpg"),
            Path::new("./repo/a.jpg")
        );
        assert_eq!(
            logical_target_path(dest, Path::new("repo/a.jpg")).unwrap(),
            "repo/a.jpg"
        );
        assert_eq!(
            logical_target_path(dest, Path::new("./repo/a.jpg")).unwrap(),
            "repo/a.jpg"
        );
    }
}
