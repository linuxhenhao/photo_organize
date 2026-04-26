use akaze::Akaze;
use anyhow::{Context, Result};
use blake3::Hasher as Blake3Hasher;
use img_hash::image::{DynamicImage, GenericImageView};
use img_hash::{HashAlg, HasherConfig};
use rsraw::{RawImage, ThumbFormat, ThumbnailImage};
use std::path::Path;

const PHASH_MAX_DIMENSION: u32 = 256;
const AKAZE_MAX_DIMENSION: u32 = 640;
const MAX_KEYPOINTS_FOR_MATCH: usize = 500;

#[derive(Debug, Clone)]
pub struct VisualFeatures {
    pub exact_hash: String,
    pub phash: String,
    pub phash_bits: i64,
    pub phash_value: u64,
    pub width: i64,
    pub height: i64,
    pub size_bytes_hint: i64,
    pub akaze_keypoints: Option<usize>,
    pub akaze_descriptors: Option<Vec<Vec<u8>>>,
}

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
    if !supports_visual_features(path, mime_type) {
        return Ok(None);
    }

    if is_raw_like_mime(path, mime_type) {
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
    let akaze = Akaze::sparse();
    let (keypoints, descriptors) = akaze.extract(&akaze_image);
    let akaze_keypoints = (!keypoints.is_empty()).then_some(keypoints.len());
    let akaze_descriptors = if descriptors.is_empty() {
        None
    } else {
        Some(descriptors.iter().map(|d| d.bytes().to_vec()).collect())
    };
    VisualFeatures {
        exact_hash: String::new(),
        phash,
        phash_bits,
        phash_value,
        width,
        height,
        size_bytes_hint: 0,
        akaze_keypoints,
        akaze_descriptors,
    }
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
        akaze_keypoints: None,
        akaze_descriptors: None,
    }
}

pub fn compute_base_features_from_bytes(
    bytes: &[u8],
    path: &Path,
    mime_type: &str,
) -> Result<Option<VisualFeatures>> {
    if !supports_visual_features(path, mime_type) {
        return Ok(None);
    }

    if is_raw_like_mime(path, mime_type) {
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

pub fn akaze_confirm(
    a: &VisualFeatures,
    b: &VisualFeatures,
    min_matches: usize,
    phash_threshold: u32,
) -> bool {
    let a_descs = a.akaze_descriptors.as_ref();
    let b_descs = b.akaze_descriptors.as_ref();

    // 1. Special case: if BOTH have zero keypoints (empty/solid color), trust phash match within threshold
    if a_descs.is_none() && b_descs.is_none() && !a.phash.is_empty() && !b.phash.is_empty() {
        let distance = (a.phash_value ^ b.phash_value).count_ones();
        if distance <= phash_threshold {
            return true;
        }
    }

    let Some(a_descs) = a_descs else {
        return false;
    };
    let Some(b_descs) = b_descs else {
        return false;
    };

    // 2. Minimum keypoint count protection
    if a_descs.len() < 25 || b_descs.len() < 25 {
        return false;
    }

    // Performance protection: limit number of descriptors to compare
    let a_subset = &a_descs[..a_descs.len().min(MAX_KEYPOINTS_FOR_MATCH)];
    let b_subset = &b_descs[..b_descs.len().min(MAX_KEYPOINTS_FOR_MATCH)];

    let mut good_matches = 0usize;
    let ratio_threshold = 0.75f32;

    // 3. Lowe's Ratio Test
    for desc_a in a_subset {
        let mut min_dist1 = u32::MAX;
        let mut min_dist2 = u32::MAX;

        for desc_b in b_subset {
            let dist = hamming_distance(desc_a, desc_b);
            if dist < min_dist1 {
                min_dist2 = min_dist1;
                min_dist1 = dist;
            } else if dist < min_dist2 {
                min_dist2 = dist;
            }
        }

        // Ratio Test: Best match must be significantly better than the second best
        if (min_dist1 as f32) < (min_dist2 as f32) * ratio_threshold && min_dist1 < 96 {
            good_matches += 1;
        }
    }

    // 4. Final judgment: absolute count + relative ratio
    let min_points = a_subset.len().min(b_subset.len());
    let match_ratio = good_matches as f32 / min_points as f32;

    // We require at least min_matches AND at least 15% of the points to match
    good_matches >= min_matches && match_ratio >= 0.15
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
    if is_raw_like_mime(path, mime_type) {
        return true;
    }

    mime_type.trim().to_ascii_lowercase().starts_with("image/")
}

fn is_raw_like_mime(path: &Path, mime_type: &str) -> bool {
    let mime = mime_type.trim().to_ascii_lowercase();
    if mime.starts_with("image/x-raw") || mime.starts_with("image/x-") {
        return true;
    }

    if mime == "image/tiff" {
        return is_raw_extension(path);
    }

    is_raw_extension(path)
}

fn is_raw_extension(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|s| s.to_str()) else {
        return false;
    };
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "3fr"
            | "ari"
            | "arw"
            | "cr2"
            | "cr3"
            | "crw"
            | "dcr"
            | "dng"
            | "erf"
            | "iiq"
            | "kdc"
            | "mef"
            | "mos"
            | "mrw"
            | "nef"
            | "nrw"
            | "orf"
            | "pef"
            | "raf"
            | "raw"
            | "rw2"
            | "sr2"
            | "srw"
            | "x3f"
    )
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
}
