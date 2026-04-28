use akaze::Akaze;
use anyhow::{Context, Result};
use blake3::Hasher as Blake3Hasher;
use image::ImageReader;
use img_hash::image::{DynamicImage, GenericImageView};
use img_hash::{HashAlg, HasherConfig};
use rsraw::{RawImage, ThumbFormat, ThumbnailImage};
use std::io::{BufRead, Seek};
use std::path::Path;

const PHASH_MAX_DIMENSION: u32 = 256;
const AKAZE_MAX_DIMENSION: u32 = 640;
const AKAZE_MIN_DIMENSION: u32 = 40;
const MAX_KEYPOINTS_FOR_MATCH: usize = 500;
const LOWE_RATIO_THRESHOLD: f32 = 0.75;
const MATCH_DISTANCE_THRESHOLD: u32 = 96;
const RANSAC_REPROJECTION_THRESHOLD: f32 = 8.0;
const RANSAC_MIN_INLIER_RATIO: f32 = 0.4;
const RANSAC_EXHAUSTIVE_LIMIT: usize = 12;
const RANSAC_MAX_RANDOM_SAMPLES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AkazeStatus {
    Pending,
    Ready,
    NoKeypoints,
    TooSmall,
    DecodeError,
    Unavailable,
}

impl AkazeStatus {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::NoKeypoints => "no_keypoints",
            Self::TooSmall => "too_small",
            Self::DecodeError => "decode_error",
            Self::Unavailable => "unavailable",
        }
    }

    pub fn from_db_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "ready" => Some(Self::Ready),
            "no_keypoints" => Some(Self::NoKeypoints),
            "too_small" => Some(Self::TooSmall),
            "decode_error" => Some(Self::DecodeError),
            "unavailable" => Some(Self::Unavailable),
            _ => None,
        }
    }

    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Pending | Self::DecodeError)
    }
}

#[derive(Debug, Clone)]
pub struct VisualFeatures {
    pub exact_hash: String,
    pub phash: String,
    pub phash_bits: i64,
    pub phash_value: u64,
    pub width: i64,
    pub height: i64,
    pub size_bytes_hint: i64,
    pub akaze_status: AkazeStatus,
    pub akaze_keypoints: Option<usize>,
    pub akaze_points: Option<Vec<AkazePoint>>,
    pub akaze_descriptors: Option<Vec<Vec<u8>>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AkazePoint {
    pub x: f32,
    pub y: f32,
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn exact_hash(bytes: &[u8]) -> String {
    let mut hasher = Blake3Hasher::new();
    hasher.update(bytes);
    hasher.finalize().to_hex().to_string()
}

pub fn compute_visual_features_from_bytes(bytes: &[u8], path: &Path) -> Result<VisualFeatures> {
    let image = decode_image(bytes, path)?;
    Ok(compute_visual_features_from_image(&image))
}

fn decode_image(bytes: &[u8], path: &Path) -> Result<img_hash::image::DynamicImage> {
    if let Ok(image) = img_hash::image::load_from_memory(bytes) {
        return Ok(image);
    }

    let image = ::image::load_from_memory(bytes)
        .with_context(|| format!("decode image {}", path.display()))?;
    Ok(convert_direct_image(image))
}

fn convert_direct_image(image: ::image::DynamicImage) -> img_hash::image::DynamicImage {
    let rgba = image.to_rgba8();
    let (width, height) = rgba.dimensions();
    let buffer = img_hash::image::RgbaImage::from_raw(width, height, rgba.into_raw())
        .expect("rgba buffer dimensions should match");
    img_hash::image::DynamicImage::ImageRgba8(buffer)
}

pub fn compute_visual_features_for_mime_from_bytes(
    bytes: &[u8],
    path: &Path,
    mime_type: &str,
) -> Result<Option<VisualFeatures>> {
    let raw_by_content = bytes_are_raw(bytes);
    if !raw_by_content && !supports_visual_features(path, mime_type) {
        return Ok(None);
    }

    if raw_by_content || is_raw_like_mime(mime_type) {
        match compute_raw_preview_features_from_bytes(bytes, path) {
            Ok(features) => return Ok(Some(features)),
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    mime = %mime_type,
                    error = %err,
                    "raw preview extraction failed"
                );
            }
        }
    }

    match compute_visual_features_from_bytes(bytes, path) {
        Ok(features) => Ok(Some(features)),
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                mime = %mime_type,
                error = %err,
                "visual feature extraction failed"
            );
            Ok(None)
        }
    }
}

pub fn compute_visual_features_from_image(image: &DynamicImage) -> VisualFeatures {
    let width = i64::from(image.width());
    let height = i64::from(image.height());
    let phash_image = resize_for_feature(image, PHASH_MAX_DIMENSION);
    let akaze_image = resize_for_feature(image, AKAZE_MAX_DIMENSION);
    let hasher = HasherConfig::new()
        .hash_size(8, 8)
        .hash_alg(HashAlg::Gradient)
        .preproc_dct()
        .to_hasher();
    let hash = hasher.hash_image(&phash_image);
    let phash = hash.to_base64();
    let phash_bits = i64::try_from(hash.as_bytes().len() * 8).unwrap_or(64);
    let phash_value = phash_to_u64(&phash).unwrap_or(0);
    let (akaze_status, akaze_keypoints, akaze_points, akaze_descriptors) =
        extract_akaze_features(&akaze_image);
    VisualFeatures {
        exact_hash: String::new(),
        phash,
        phash_bits,
        phash_value,
        width,
        height,
        size_bytes_hint: 0,
        akaze_status,
        akaze_keypoints,
        akaze_points,
        akaze_descriptors,
    }
}

fn extract_akaze_features(
    image: &DynamicImage,
) -> (
    AkazeStatus,
    Option<usize>,
    Option<Vec<AkazePoint>>,
    Option<Vec<Vec<u8>>>,
) {
    if image.width().min(image.height()) < AKAZE_MIN_DIMENSION {
        return (AkazeStatus::TooSmall, None, None, None);
    }

    let sparse = extract_akaze_features_with(image, Akaze::sparse());
    if sparse.0 == AkazeStatus::NoKeypoints {
        return extract_akaze_features_with(image, Akaze::default());
    }
    sparse
}

fn extract_akaze_features_with(
    image: &DynamicImage,
    akaze: Akaze,
) -> (
    AkazeStatus,
    Option<usize>,
    Option<Vec<AkazePoint>>,
    Option<Vec<Vec<u8>>>,
) {
    let extracted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| akaze.extract(image)));
    let Ok((keypoints, descriptors)) = extracted else {
        tracing::warn!(
            width = image.width(),
            height = image.height(),
            "akaze extractor panicked; continuing without descriptors"
        );
        return (AkazeStatus::DecodeError, None, None, None);
    };

    let akaze_keypoints = (!keypoints.is_empty()).then_some(keypoints.len());
    let (akaze_status, akaze_points, akaze_descriptors) =
        if keypoints.is_empty() && descriptors.is_empty() {
            (AkazeStatus::NoKeypoints, None, None)
        } else if descriptors.is_empty() {
            (AkazeStatus::DecodeError, None, None)
        } else if keypoints.len() != descriptors.len() {
            tracing::warn!(
                width = image.width(),
                height = image.height(),
                keypoints = keypoints.len(),
                descriptors = descriptors.len(),
                "akaze extractor returned mismatched keypoints and descriptors"
            );
            (AkazeStatus::DecodeError, None, None)
        } else {
            (
                AkazeStatus::Ready,
                Some(
                    keypoints
                        .iter()
                        .map(|point| AkazePoint {
                            x: point.point.0,
                            y: point.point.1,
                        })
                        .collect(),
                ),
                Some(descriptors.iter().map(|d| d.bytes().to_vec()).collect()),
            )
        };
    (
        akaze_status,
        akaze_keypoints,
        akaze_points,
        akaze_descriptors,
    )
}

#[derive(Debug, Clone, Copy)]
struct TentativeMatch {
    a_index: usize,
    b_index: usize,
}

#[derive(Debug, Clone, Copy)]
struct NeighborMatch {
    best_index: usize,
    best_distance: u32,
    second_distance: u32,
}

#[derive(Debug, Clone, Copy)]
struct AffineModel {
    a11: f32,
    a12: f32,
    a13: f32,
    a21: f32,
    a22: f32,
    a23: f32,
}

impl AffineModel {
    fn project(self, point: AkazePoint) -> AkazePoint {
        AkazePoint {
            x: self.a11 * point.x + self.a12 * point.y + self.a13,
            y: self.a21 * point.x + self.a22 * point.y + self.a23,
        }
    }
}

fn collect_mutual_matches(a_descs: &[Vec<u8>], b_descs: &[Vec<u8>]) -> Vec<TentativeMatch> {
    if a_descs.len() < 2 || b_descs.len() < 2 {
        return Vec::new();
    }

    let forward: Vec<_> = a_descs
        .iter()
        .map(|desc| best_two_neighbors(desc, b_descs))
        .collect();
    let reverse: Vec<_> = b_descs
        .iter()
        .map(|desc| best_two_neighbors(desc, a_descs))
        .collect();

    let mut matches = Vec::new();
    for (a_index, neighbor) in forward.iter().enumerate() {
        let Some(neighbor) = neighbor.as_ref().copied().filter(satisfies_lowe_ratio) else {
            continue;
        };
        let Some(reverse_neighbor) = reverse[neighbor.best_index]
            .as_ref()
            .copied()
            .filter(satisfies_lowe_ratio)
        else {
            continue;
        };
        if reverse_neighbor.best_index == a_index {
            matches.push(TentativeMatch {
                a_index,
                b_index: neighbor.best_index,
            });
        }
    }
    matches
}

fn best_two_neighbors(desc: &[u8], candidates: &[Vec<u8>]) -> Option<NeighborMatch> {
    if candidates.len() < 2 {
        return None;
    }

    let mut best_index = 0usize;
    let mut best_distance = u32::MAX;
    let mut second_distance = u32::MAX;
    for (index, candidate) in candidates.iter().enumerate() {
        let distance = hamming_distance(desc, candidate);
        if distance < best_distance {
            second_distance = best_distance;
            best_distance = distance;
            best_index = index;
        } else if distance < second_distance {
            second_distance = distance;
        }
    }

    Some(NeighborMatch {
        best_index,
        best_distance,
        second_distance,
    })
}

fn satisfies_lowe_ratio(m: &NeighborMatch) -> bool {
    (m.best_distance as f32) < (m.second_distance as f32) * LOWE_RATIO_THRESHOLD
        && m.best_distance < MATCH_DISTANCE_THRESHOLD
}

fn affine_ransac_inliers(
    a_points: &[AkazePoint],
    b_points: &[AkazePoint],
    matches: &[TentativeMatch],
) -> usize {
    if matches.len() < 3 {
        return 0;
    }

    let mut best_inliers = 0usize;
    if matches.len() <= RANSAC_EXHAUSTIVE_LIMIT {
        for i in 0..matches.len() - 2 {
            for j in i + 1..matches.len() - 1 {
                for k in j + 1..matches.len() {
                    if let Some(model) =
                        estimate_affine(a_points, b_points, [matches[i], matches[j], matches[k]])
                    {
                        best_inliers = best_inliers
                            .max(count_affine_inliers(model, a_points, b_points, matches));
                    }
                }
            }
        }
        return best_inliers;
    }

    let mut state = ransac_seed(matches.len());
    for _ in 0..RANSAC_MAX_RANDOM_SAMPLES {
        let sample = random_triplet(matches.len(), &mut state);
        if let Some(model) = estimate_affine(
            a_points,
            b_points,
            [matches[sample[0]], matches[sample[1]], matches[sample[2]]],
        ) {
            best_inliers =
                best_inliers.max(count_affine_inliers(model, a_points, b_points, matches));
            if best_inliers == matches.len() {
                break;
            }
        }
    }
    best_inliers
}

fn estimate_affine(
    a_points: &[AkazePoint],
    b_points: &[AkazePoint],
    sample: [TentativeMatch; 3],
) -> Option<AffineModel> {
    let mut matrix = [[0.0f32; 7]; 6];
    for (row, matched) in sample.iter().enumerate() {
        let a = a_points[matched.a_index];
        let b = b_points[matched.b_index];
        matrix[row * 2] = [a.x, a.y, 1.0, 0.0, 0.0, 0.0, b.x];
        matrix[row * 2 + 1] = [0.0, 0.0, 0.0, a.x, a.y, 1.0, b.y];
    }

    let solution = solve_linear_system(&mut matrix)?;
    Some(AffineModel {
        a11: solution[0],
        a12: solution[1],
        a13: solution[2],
        a21: solution[3],
        a22: solution[4],
        a23: solution[5],
    })
}

fn solve_linear_system(matrix: &mut [[f32; 7]; 6]) -> Option<[f32; 6]> {
    for pivot in 0..6 {
        let mut pivot_row = pivot;
        let mut pivot_value = matrix[pivot][pivot].abs();
        for row in pivot + 1..6 {
            let value = matrix[row][pivot].abs();
            if value > pivot_value {
                pivot_row = row;
                pivot_value = value;
            }
        }
        if pivot_value <= 1e-6 {
            return None;
        }
        if pivot_row != pivot {
            matrix.swap(pivot, pivot_row);
        }

        let scale = matrix[pivot][pivot];
        for col in pivot..7 {
            matrix[pivot][col] /= scale;
        }
        for row in 0..6 {
            if row == pivot {
                continue;
            }
            let factor = matrix[row][pivot];
            if factor.abs() <= 1e-6 {
                continue;
            }
            for col in pivot..7 {
                matrix[row][col] -= factor * matrix[pivot][col];
            }
        }
    }

    let mut solution = [0.0f32; 6];
    for row in 0..6 {
        solution[row] = matrix[row][6];
    }
    Some(solution)
}

fn count_affine_inliers(
    model: AffineModel,
    a_points: &[AkazePoint],
    b_points: &[AkazePoint],
    matches: &[TentativeMatch],
) -> usize {
    let threshold_sq = RANSAC_REPROJECTION_THRESHOLD * RANSAC_REPROJECTION_THRESHOLD;
    matches
        .iter()
        .filter(|matched| {
            let projected = model.project(a_points[matched.a_index]);
            let target = b_points[matched.b_index];
            let dx = projected.x - target.x;
            let dy = projected.y - target.y;
            dx * dx + dy * dy <= threshold_sq
        })
        .count()
}

fn ransac_seed(match_count: usize) -> u64 {
    0x9E37_79B9_7F4A_7C15u64 ^ (match_count as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9)
}

fn random_triplet(len: usize, state: &mut u64) -> [usize; 3] {
    let first = next_index(len, state);
    let mut second = next_index(len, state);
    while second == first {
        second = next_index(len, state);
    }
    let mut third = next_index(len, state);
    while third == first || third == second {
        third = next_index(len, state);
    }
    [first, second, third]
}

fn next_index(len: usize, state: &mut u64) -> usize {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    ((*state >> 32) as usize) % len
}

pub fn compute_base_features_from_image(image: &DynamicImage) -> VisualFeatures {
    let width = i64::from(image.width());
    let height = i64::from(image.height());
    let phash_image = resize_for_feature(image, PHASH_MAX_DIMENSION);
    let hasher = HasherConfig::new()
        .hash_size(8, 8)
        .hash_alg(HashAlg::Gradient)
        .preproc_dct()
        .to_hasher();
    let hash = hasher.hash_image(&phash_image);
    let phash = hash.to_base64();
    let phash_bits = i64::try_from(hash.as_bytes().len() * 8).unwrap_or(64);
    let phash_value = phash_to_u64(&phash).unwrap_or(0);

    VisualFeatures {
        exact_hash: String::new(),
        phash,
        phash_bits,
        phash_value,
        width,
        height,
        size_bytes_hint: 0,
        akaze_status: AkazeStatus::Pending,
        akaze_keypoints: None,
        akaze_points: None,
        akaze_descriptors: None,
    }
}

pub fn compute_base_features_from_bytes(
    bytes: &[u8],
    path: &Path,
    mime_type: &str,
) -> Result<Option<VisualFeatures>> {
    let raw_by_content = bytes_are_raw(bytes);
    if !raw_by_content && !supports_visual_features(path, mime_type) {
        return Ok(None);
    }

    if raw_by_content || is_raw_like_mime(mime_type) {
        if let Ok(mut raw) = RawImage::open(bytes) {
            if let Ok(thumbs) = raw.extract_thumbs() {
                if let Some(preview) = select_best_thumbnail(&thumbs, PHASH_MAX_DIMENSION) {
                    if let Ok(image) = decode_thumbnail_image(preview) {
                        return Ok(Some(compute_base_features_from_image(&image)));
                    }
                }
            }
        }
    }

    match decode_image(bytes, path) {
        Ok(image) => Ok(Some(compute_base_features_from_image(&image))),
        Err(err) => {
            tracing::warn!(path = %path.display(), error = %err, "base feature extraction failed");
            Ok(None)
        }
    }
}

pub fn compute_base_features_from_reader<R>(reader: R, path: &Path) -> Result<VisualFeatures>
where
    R: BufRead + Seek,
{
    let image = decode_image_from_reader(reader, path)?;
    Ok(compute_base_features_from_image(&image))
}

pub fn akaze_confirm(
    a: &VisualFeatures,
    b: &VisualFeatures,
    min_matches: usize,
    phash_threshold: u32,
) -> bool {
    // 1. Special case: if BOTH were computed and produced no keypoints (empty/solid color),
    // trust pHash match within threshold.
    if a.akaze_status == AkazeStatus::NoKeypoints
        && b.akaze_status == AkazeStatus::NoKeypoints
        && !a.phash.is_empty()
        && !b.phash.is_empty()
    {
        let distance = (a.phash_value ^ b.phash_value).count_ones();
        if distance <= phash_threshold {
            return true;
        }
    }

    if a.akaze_status != AkazeStatus::Ready || b.akaze_status != AkazeStatus::Ready {
        return false;
    }

    let a_descs = a.akaze_descriptors.as_ref();
    let b_descs = b.akaze_descriptors.as_ref();
    let Some(a_descs) = a_descs else {
        return false;
    };
    let Some(b_descs) = b_descs else {
        return false;
    };
    let Some(a_points) = a.akaze_points.as_ref() else {
        return false;
    };
    let Some(b_points) = b.akaze_points.as_ref() else {
        return false;
    };
    if a_descs.len() != a_points.len() || b_descs.len() != b_points.len() {
        return false;
    }

    // 2. Minimum keypoint count protection
    if a_descs.len() < 25 || b_descs.len() < 25 {
        return false;
    }

    // Performance protection: limit number of descriptors to compare
    let a_len = a_descs.len().min(MAX_KEYPOINTS_FOR_MATCH);
    let b_len = b_descs.len().min(MAX_KEYPOINTS_FOR_MATCH);
    let a_descs = &a_descs[..a_len];
    let b_descs = &b_descs[..b_len];
    let a_points = &a_points[..a_len];
    let b_points = &b_points[..b_len];

    // Use symmetric tentative matches, then require a consistent affine geometry.
    let matches = collect_mutual_matches(a_descs, b_descs);
    if matches.len() < min_matches.max(3) {
        return false;
    }

    let inliers = affine_ransac_inliers(a_points, b_points, &matches);
    let inlier_ratio = inliers as f32 / matches.len() as f32;
    inliers >= min_matches.max(3) && inlier_ratio >= RANSAC_MIN_INLIER_RATIO
}

fn hamming_distance(left: &[u8], right: &[u8]) -> u32 {
    left.iter()
        .zip(right.iter())
        .map(|(l, r)| (l ^ r).count_ones())
        .sum()
}

pub fn phash_to_u64(phash: &str) -> Option<u64> {
    let hash = img_hash::ImageHash::<Box<[u8]>>::from_base64(phash).ok()?;
    let bytes = hash.as_bytes();
    if bytes.len() != 8 {
        return None;
    }

    let mut value = 0u64;
    for byte in bytes {
        value = (value << 8) | u64::from(*byte);
    }
    Some(value)
}

pub fn serialize_akaze_points(points: &[AkazePoint]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(
        &u32::try_from(points.len())
            .context("too many akaze points")?
            .to_le_bytes(),
    );
    for point in points {
        out.extend_from_slice(&point.x.to_le_bytes());
        out.extend_from_slice(&point.y.to_le_bytes());
    }
    Ok(out)
}

pub fn deserialize_akaze_points(blob: &[u8]) -> Result<Vec<AkazePoint>> {
    if blob.len() < 4 {
        anyhow::bail!("point blob too small");
    }

    let mut offset = 0usize;
    let count = read_u32(blob, &mut offset)? as usize;
    let mut points = Vec::with_capacity(count);
    for _ in 0..count {
        let x = read_f32(blob, &mut offset)?;
        let y = read_f32(blob, &mut offset)?;
        points.push(AkazePoint { x, y });
    }

    if offset != blob.len() {
        anyhow::bail!("point blob has trailing bytes");
    }

    Ok(points)
}

pub fn serialize_akaze_descriptors(descriptors: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(
        &u32::try_from(descriptors.len())
            .context("too many akaze descriptors")?
            .to_le_bytes(),
    );
    for descriptor in descriptors {
        out.extend_from_slice(
            &u32::try_from(descriptor.len())
                .context("akaze descriptor too large")?
                .to_le_bytes(),
        );
        out.extend_from_slice(descriptor);
    }
    Ok(out)
}

pub fn deserialize_akaze_descriptors(blob: &[u8]) -> Result<Vec<Vec<u8>>> {
    if blob.len() < 4 {
        anyhow::bail!("descriptor blob too small");
    }

    let mut offset = 0usize;
    let count = read_u32(blob, &mut offset)? as usize;
    let mut descriptors = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u32(blob, &mut offset)? as usize;
        if blob.len() < offset + len {
            anyhow::bail!("descriptor blob truncated");
        }
        descriptors.push(blob[offset..offset + len].to_vec());
        offset += len;
    }

    if offset != blob.len() {
        anyhow::bail!("descriptor blob has trailing bytes");
    }

    Ok(descriptors)
}

fn read_u32(blob: &[u8], offset: &mut usize) -> Result<u32> {
    if blob.len() < *offset + 4 {
        anyhow::bail!("descriptor blob truncated");
    }
    let value = u32::from_le_bytes([
        blob[*offset],
        blob[*offset + 1],
        blob[*offset + 2],
        blob[*offset + 3],
    ]);
    *offset += 4;
    Ok(value)
}

fn read_f32(blob: &[u8], offset: &mut usize) -> Result<f32> {
    if blob.len() < *offset + 4 {
        anyhow::bail!("point blob truncated");
    }
    let value = f32::from_le_bytes([
        blob[*offset],
        blob[*offset + 1],
        blob[*offset + 2],
        blob[*offset + 3],
    ]);
    *offset += 4;
    Ok(value)
}

fn compute_raw_preview_features_from_bytes(bytes: &[u8], path: &Path) -> Result<VisualFeatures> {
    let mut raw = RawImage::open(bytes).with_context(|| format!("open raw {}", path.display()))?;
    let thumbs = raw
        .extract_thumbs()
        .with_context(|| format!("extract raw previews {}", path.display()))?;
    let preview = select_best_thumbnail(&thumbs, AKAZE_MAX_DIMENSION)
        .with_context(|| format!("no decodable raw preview in {}", path.display()))?;
    let image = decode_thumbnail_image(preview)
        .with_context(|| format!("decode raw preview {}", path.display()))?;
    Ok(compute_visual_features_from_image(&image))
}

fn decode_image_from_reader<R>(reader: R, path: &Path) -> Result<DynamicImage>
where
    R: BufRead + Seek,
{
    let image = ImageReader::new(reader)
        .with_guessed_format()
        .with_context(|| format!("guess image format {}", path.display()))?
        .decode()
        .with_context(|| format!("decode image {}", path.display()))?;
    Ok(convert_direct_image(image))
}

fn select_best_thumbnail(thumbs: &[ThumbnailImage], min_dimension: u32) -> Option<&ThumbnailImage> {
    let supported: Vec<_> = thumbs
        .iter()
        .filter(|thumb| {
            matches!(
                thumb.format,
                ThumbFormat::Jpeg | ThumbFormat::Bitmap | ThumbFormat::Bitmap16
            )
        })
        .collect();
    if supported.is_empty() {
        return None;
    }

    supported
        .iter()
        .copied()
        .filter(|thumb| preview_max_dimension(thumb) >= min_dimension)
        .min_by_key(|thumb| {
            (
                preview_max_dimension(thumb),
                u64::from(thumb.width) * u64::from(thumb.height),
                preview_format_rank(thumb),
            )
        })
        .or_else(|| {
            supported.into_iter().max_by_key(|thumb| {
                (
                    preview_max_dimension(thumb),
                    u64::from(thumb.width) * u64::from(thumb.height),
                    preview_format_rank(thumb),
                )
            })
        })
}

fn preview_max_dimension(thumb: &ThumbnailImage) -> u32 {
    thumb.width.max(thumb.height)
}

fn preview_format_rank(thumb: &ThumbnailImage) -> u8 {
    match thumb.format {
        ThumbFormat::Bitmap => 3,
        ThumbFormat::Bitmap16 => 2,
        ThumbFormat::Jpeg => 1,
        _ => 0,
    }
}

fn decode_thumbnail_image(thumb: &ThumbnailImage) -> Result<DynamicImage> {
    match thumb.format {
        ThumbFormat::Jpeg => decode_image(&thumb.data, Path::new("<raw-preview>")),
        ThumbFormat::Bitmap => decode_rgb8_thumbnail(thumb),
        ThumbFormat::Bitmap16 => decode_rgb16_thumbnail(thumb),
        _ => anyhow::bail!("unsupported raw preview format {:?}", thumb.format),
    }
}

fn decode_rgb8_thumbnail(thumb: &ThumbnailImage) -> Result<DynamicImage> {
    let expected_len = usize::try_from(thumb.width)
        .ok()
        .and_then(|width| {
            usize::try_from(thumb.height)
                .ok()
                .map(|height| width * height * 3)
        })
        .context("invalid thumbnail dimensions")?;
    anyhow::ensure!(
        thumb.data.len() == expected_len,
        "unexpected 8-bit thumbnail size: got {}, want {}",
        thumb.data.len(),
        expected_len
    );

    let image = ::image::RgbImage::from_raw(thumb.width, thumb.height, thumb.data.clone())
        .context("bitmap thumbnail dimensions should match")?;
    Ok(convert_direct_image(::image::DynamicImage::ImageRgb8(
        image,
    )))
}

fn decode_rgb16_thumbnail(thumb: &ThumbnailImage) -> Result<DynamicImage> {
    let expected_values = usize::try_from(thumb.width)
        .ok()
        .and_then(|width| {
            usize::try_from(thumb.height)
                .ok()
                .map(|height| width * height * 3)
        })
        .context("invalid thumbnail dimensions")?;
    anyhow::ensure!(
        thumb.data.len() == expected_values * 2,
        "unexpected 16-bit thumbnail size: got {}, want {}",
        thumb.data.len(),
        expected_values * 2
    );

    let pixels = thumb
        .data
        .chunks_exact(2)
        .map(|chunk| u16::from_ne_bytes([chunk[0], chunk[1]]))
        .collect();
    let image = ::image::ImageBuffer::<::image::Rgb<u16>, Vec<u16>>::from_raw(
        thumb.width,
        thumb.height,
        pixels,
    )
    .context("bitmap16 thumbnail dimensions should match")?;
    Ok(convert_direct_image(::image::DynamicImage::ImageRgb16(
        image,
    )))
}

fn resize_for_feature(image: &DynamicImage, max_dimension: u32) -> DynamicImage {
    let (width, height) = image.dimensions();
    if width <= max_dimension && height <= max_dimension {
        return image.clone();
    }

    image.thumbnail(max_dimension, max_dimension)
}

pub(crate) fn supports_visual_features(path: &Path, mime_type: &str) -> bool {
    if is_raw_like_mime(mime_type) {
        return true;
    }

    let _ = path;
    mime_type.trim().to_ascii_lowercase().starts_with("image/")
}

pub(crate) fn is_raw_like_mime(mime_type: &str) -> bool {
    let mime = mime_type.trim().to_ascii_lowercase();
    mime == "image/x-raw"
        || matches!(
            mime.as_str(),
            "image/x-sony-arw"
                | "image/x-sony-sr2"
                | "image/x-canon-cr2"
                | "image/x-canon-cr3"
                | "image/x-nikon-nef"
                | "image/x-nikon-nrw"
                | "image/x-adobe-dng"
                | "image/x-fuji-raf"
                | "image/x-olympus-orf"
                | "image/x-panasonic-rw2"
                | "image/x-pentax-pef"
                | "image/x-sigma-x3f"
                | "image/x-hasselblad-3fr"
                | "image/x-raw"
        )
}

fn bytes_are_raw(bytes: &[u8]) -> bool {
    RawImage::open(bytes).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn fixture_path(name: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join(name)
    }

    fn bad_case_fixture_path(name: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("docs")
            .join("group_bad_cases")
            .join("group-71-investigation")
            .join("assets")
            .join(name)
    }

    fn group_189_snapshot_path(name: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("docs")
            .join("group_bad_cases")
            .join("group-189-investigation")
            .join("snapshot")
            .join("group-189")
            .join("files")
            .join("repo")
            .join(name)
    }

    fn visual_features_for_path(path: &Path, mime_type: &str) -> VisualFeatures {
        let bytes = fs::read(path).unwrap();
        compute_visual_features_for_mime_from_bytes(&bytes, path, mime_type)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn compute_visual_features_for_arw_raw_preview() {
        let arw = fixture_path("source/DSC00903.ARW");
        let arw_bytes = fs::read(&arw).unwrap();
        let arw_features =
            compute_visual_features_for_mime_from_bytes(&arw_bytes, &arw, "image/x-sony-arw")
                .unwrap()
                .expect("ARW preview should decode");
        assert!(!arw_features.phash.is_empty());
        assert!(arw_features.width > 0);
        assert!(arw_features.height > 0);
    }

    #[test]
    fn compute_visual_features_for_cr2_raw_preview() {
        let cr2 = fixture_path("source/IMG_5798.CR2");
        let cr2_bytes = fs::read(&cr2).unwrap();
        let cr2_features =
            compute_visual_features_for_mime_from_bytes(&cr2_bytes, &cr2, "image/x-canon-cr2")
                .unwrap()
                .expect("CR2 preview should decode");
        assert!(!cr2_features.phash.is_empty());
        assert!(cr2_features.width > 0);
        assert!(cr2_features.height > 0);
    }

    #[test]
    fn compute_visual_features_skips_video_mime() {
        let video = std::path::Path::new("clip.mp4");
        let features =
            compute_visual_features_for_mime_from_bytes(b"not a real video", video, "video/mp4")
                .unwrap();
        assert!(features.is_none());
    }

    #[test]
    fn compute_base_features_skips_video_mime() {
        let video = std::path::Path::new("clip.mov");
        let features =
            compute_base_features_from_bytes(b"not a real video", video, "video/quicktime")
                .unwrap();
        assert!(features.is_none());
    }

    #[test]
    fn compute_visual_features_does_not_treat_jpeg_bytes_as_raw_from_extension() {
        let jpeg = fixture_path("source_mock/img_2023_05_01.jpg");
        let jpeg_bytes = fs::read(&jpeg).unwrap();
        let fake_raw_name = std::path::Path::new("pretend.arw");

        let features =
            compute_visual_features_for_mime_from_bytes(&jpeg_bytes, fake_raw_name, "image/jpeg")
                .unwrap()
                .expect("jpeg should decode normally");

        assert!(!features.phash.is_empty());
        assert!(features.width > 0);
        assert!(features.height > 0);
    }

    #[test]
    fn compute_visual_features_detects_raw_bytes_even_with_non_raw_mime() {
        let arw = fixture_path("source/DSC00903.ARW");
        let arw_bytes = fs::read(&arw).unwrap();

        let features = compute_visual_features_for_mime_from_bytes(
            &arw_bytes,
            Path::new("pretend.jpg"),
            "application/octet-stream",
        )
        .unwrap()
        .expect("raw bytes should still be treated as raw");

        assert!(!features.phash.is_empty());
        assert!(features.width > 0);
        assert!(features.height > 0);
    }

    #[test]
    fn supports_visual_features_does_not_trust_raw_extension() {
        let fake_raw_name = std::path::Path::new("pretend.cr2");

        assert!(!is_raw_like_mime("image/jpeg"));
        assert!(supports_visual_features(fake_raw_name, "image/jpeg"));
    }

    #[test]
    fn select_best_thumbnail_prefers_smallest_adequate_preview() {
        let thumbs = vec![
            ThumbnailImage {
                format: ThumbFormat::Jpeg,
                width: 160,
                height: 120,
                colors: 3,
                data: vec![],
            },
            ThumbnailImage {
                format: ThumbFormat::Jpeg,
                width: 1616,
                height: 1080,
                colors: 3,
                data: vec![],
            },
            ThumbnailImage {
                format: ThumbFormat::Jpeg,
                width: 7008,
                height: 4672,
                colors: 3,
                data: vec![],
            },
        ];

        let selected = select_best_thumbnail(&thumbs, AKAZE_MAX_DIMENSION).unwrap();
        assert_eq!(selected.width, 1616);
        assert_eq!(selected.height, 1080);
    }

    #[test]
    fn select_best_thumbnail_falls_back_to_largest_when_needed() {
        let thumbs = vec![
            ThumbnailImage {
                format: ThumbFormat::Jpeg,
                width: 160,
                height: 120,
                colors: 3,
                data: vec![],
            },
            ThumbnailImage {
                format: ThumbFormat::Bitmap,
                width: 660,
                height: 441,
                colors: 3,
                data: vec![],
            },
        ];

        let selected = select_best_thumbnail(&thumbs, 960).unwrap();
        assert_eq!(selected.width, 660);
        assert_eq!(selected.height, 441);
    }

    #[test]
    fn compute_visual_features_skips_akaze_for_tiny_images() {
        let image = DynamicImage::ImageRgba8(img_hash::image::RgbaImage::from_pixel(
            20,
            20,
            img_hash::image::Rgba([255, 255, 255, 255]),
        ));

        let features = compute_visual_features_from_image(&image);

        assert!(!features.phash.is_empty());
        assert_eq!(features.akaze_status, AkazeStatus::TooSmall);
        assert!(features.akaze_keypoints.is_none());
        assert!(features.akaze_points.is_none());
        assert!(features.akaze_descriptors.is_none());
    }

    #[test]
    fn compute_visual_features_skips_akaze_for_thin_images_after_resize() {
        let image = DynamicImage::ImageRgba8(img_hash::image::RgbaImage::from_pixel(
            2_000,
            20,
            img_hash::image::Rgba([255, 255, 255, 255]),
        ));

        let features = compute_visual_features_from_image(&image);

        assert!(!features.phash.is_empty());
        assert_eq!(features.akaze_status, AkazeStatus::TooSmall);
        assert!(features.akaze_keypoints.is_none());
        assert!(features.akaze_points.is_none());
        assert!(features.akaze_descriptors.is_none());
    }

    #[test]
    fn compute_visual_features_retries_no_keypoints_with_default_akaze() {
        let path = bad_case_fixture_path("defaultimg_0183-3.jpg");
        let bytes = fs::read(&path).unwrap();
        let image = decode_image(&bytes, &path).unwrap();
        let scaled = resize_for_feature(&image, AKAZE_MAX_DIMENSION);

        let sparse = extract_akaze_features_with(&scaled, Akaze::sparse());
        assert_eq!(sparse.0, AkazeStatus::NoKeypoints);

        let features = compute_visual_features_from_image(&image);
        assert_eq!(features.akaze_status, AkazeStatus::Ready);
        assert!(features.akaze_keypoints.unwrap_or(0) > 0);
        assert_eq!(
            features.akaze_keypoints,
            features.akaze_points.as_ref().map(Vec::len)
        );
        assert!(features.akaze_descriptors.is_some());
    }

    #[test]
    fn akaze_point_blob_roundtrips() {
        let points = vec![AkazePoint { x: 1.5, y: 2.5 }, AkazePoint { x: 3.0, y: 4.0 }];
        let blob = serialize_akaze_points(&points).unwrap();
        let decoded = deserialize_akaze_points(&blob).unwrap();
        assert_eq!(decoded, points);
    }

    #[test]
    fn akaze_confirm_accepts_group_189_photo_derivative_pair() {
        let original = visual_features_for_path(
            &group_189_snapshot_path("2015/10/01/IMG_2999.JPG"),
            "image/jpeg",
        );
        let derivative = visual_features_for_path(
            &group_189_snapshot_path("2025/03/17/defaultimg_2999.jpg"),
            "image/jpeg",
        );

        assert!(akaze_confirm(&original, &derivative, 10, 14));
        assert!(akaze_confirm(&derivative, &original, 10, 14));
    }

    #[test]
    fn akaze_confirm_rejects_group_189_photo_to_text_bridge() {
        let photo = visual_features_for_path(
            &group_189_snapshot_path("2025/03/17/defaultimg_2999.jpg"),
            "image/jpeg",
        );
        let text = visual_features_for_path(
            &group_189_snapshot_path("2017/06/08/type2_Cochin_24pt_Cell_26x28.png"),
            "image/png",
        );

        assert!(!akaze_confirm(&photo, &text, 10, 14));
        assert!(!akaze_confirm(&text, &photo, 10, 14));
    }
}
