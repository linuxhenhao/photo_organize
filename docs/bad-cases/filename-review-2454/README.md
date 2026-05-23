# Filename Review Bad Case: `2454`

This note records a concrete false-merge risk in the filename review flow.

The filenames look compatible at the basename level:

- `img_2454.jpg`
- `defaultimg_2454-2.jpg`
- `IMG_2454.JPG`

But the files do not represent the same source image.

## Local Evidence

The files were downloaded locally for inspection and are intentionally **not**
checked into git because they contain private photos.

Local paths:

- `downloads/exif-check-2454/img_2454.jpg`
- `downloads/exif-check-2454/defaultimg_2454-2.jpg`
- `downloads/exif-check-2454/IMG_2454.JPG`

## Current Repo DB Rows

Observed in the live NAS catalog:

- `repo/2013/05/26/img_2454.jpg`
- `repo/2015/07/27/IMG_2454.JPG`
- `repo/2025/03/17/defaultimg_2454-2.jpg`

All three were ungrouped at the time of inspection.

## EXIF Distinguishers

`img_2454.jpg`

- `DateTimeOriginal`: `2013:05:26 13:25:50`
- `Make/Model`: `Canon EOS 650D`
- `Lens`: `18.0 - 135.0 mm`
- `Orientation`: `Horizontal (normal)`
- `Software`: `Digital Photo Professional`
- `Image Size`: `5184x3456`

`IMG_2454.JPG`

- `DateTimeOriginal`: `2015:07:27 14:54:15.41`
- `Make/Model`: `Canon EOS 650D`
- `Lens`: `50.0 mm`
- `Orientation`: `Rotate 270 CW`
- `File Number`: `100-2454`
- `Image Size`: `5184x3456`

`defaultimg_2454-2.jpg`

- no useful EXIF lens metadata
- `Image Size`: `400x600`

## Why Filename-Only Matching Is Wrong Here

The default rendition is portrait-shaped (`400x600`), which is consistent with
`IMG_2454.JPG` after applying EXIF orientation, but inconsistent with
`img_2454.jpg`, which is a normal landscape image.

The two full-size JPEGs also carry different lens information:

- `img_2454.jpg`: `18.0 - 135.0 mm`
- `IMG_2454.JPG`: `50.0 mm`

So this is not just a weak visual-match issue. The basename family itself is
ambiguous and needs metadata filtering.

## Practical Rule For Filename Review

When the filename review logic builds connected components from trusted naming
families, it should reject edges when:

- effective orientation does not match
- both sides expose lens/optics metadata and the values conflict

That keeps `defaultimg_2454-2.jpg` aligned with the portrait `IMG_2454.JPG`
instead of incorrectly pulling in `img_2454.jpg`.
