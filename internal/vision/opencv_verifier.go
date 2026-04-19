package vision

import (
	"fmt"
	"image"
	"math"

	projectexiftool "github.com/linuxhenhao/photo_organize/internal/exiftool"
	"gocv.io/x/gocv"
)

const (
	maxImageDimension      = 1600
	minImageDimension      = 320
	orbLoweRatioThreshold  = 0.80
	orbFeatureCount        = 2000
	orbScaleFactor         = 1.2
	orbLevels              = 8
	orbEdgeThreshold       = 31
	orbFirstLevel          = 0
	orbWTAK                = 2
	orbPatchSize           = 31
	orbFastThreshold       = 12
	minGoodMatches         = 10
	minInliers             = 6
	minInlierRatio         = 0.30
	maxCornerOverflowRatio = 0.08
	maxScaleDeviationRatio = 0.35
	ransacReprojThreshold  = 3.0
	ransacConfidence       = 0.99
	ransacMaxIterations    = 2000
	ransacRefinementIters  = 10
)

type DerivativeVerification struct {
	Confirmed   bool
	GoodMatches int
	Inliers     int
	InlierRatio float64
}

type ORBSerializedFeatures struct {
	Keypoints   []byte
	Descriptors []byte
	Rows        int
	Cols        int
	MatType     int
	ImgWidth    int
	ImgHeight   int
}

type ORBFeatureSet struct {
	Keypoints   []gocv.KeyPoint
	Descriptors gocv.Mat
	ImageWidth  int
	ImageHeight int
}

func (f *ORBFeatureSet) Close() {
	if f == nil {
		return
	}
	if !f.Descriptors.Empty() {
		f.Descriptors.Close()
	}
}

// VerifyDerivativeWithORB checks whether child is a derived version of parent
// using ORB feature matching plus RANSAC geometric verification.
func VerifyDerivativeWithORB(childPath, parentPath string) (DerivativeVerification, error) {
	childFeatures, parentFeatures, err := computeORBFeatureSets(childPath, parentPath)
	if err != nil {
		return DerivativeVerification{}, err
	}
	defer childFeatures.Close()
	defer parentFeatures.Close()

	return VerifyDerivativeWithORBFeatures(childFeatures, parentFeatures), nil
}

func VerifyDerivativeWithORBFeatures(child ORBFeatureSet, parent ORBFeatureSet) DerivativeVerification {
	if len(child.Keypoints) < minGoodMatches || len(parent.Keypoints) < minGoodMatches {
		return DerivativeVerification{}
	}
	if child.Descriptors.Empty() || parent.Descriptors.Empty() {
		return DerivativeVerification{}
	}

	matcher := gocv.NewBFMatcherWithParams(gocv.NormHamming, false)
	defer matcher.Close()

	knnMatches := matcher.KnnMatch(child.Descriptors, parent.Descriptors, 2)
	goodMatches := make([]gocv.DMatch, 0, len(knnMatches))
	for _, pair := range knnMatches {
		if len(pair) < 2 {
			continue
		}
		if pair[0].Distance < orbLoweRatioThreshold*pair[1].Distance {
			goodMatches = append(goodMatches, pair[0])
		}
	}
	if len(goodMatches) < minGoodMatches {
		return DerivativeVerification{GoodMatches: len(goodMatches)}
	}

	fromPoints := make([]gocv.Point2f, 0, len(goodMatches))
	toPoints := make([]gocv.Point2f, 0, len(goodMatches))
	for _, match := range goodMatches {
		fromPoints = append(fromPoints, gocv.NewPoint2f(float32(child.Keypoints[match.QueryIdx].X), float32(child.Keypoints[match.QueryIdx].Y)))
		toPoints = append(toPoints, gocv.NewPoint2f(float32(parent.Keypoints[match.TrainIdx].X), float32(parent.Keypoints[match.TrainIdx].Y)))
	}

	fromVec := gocv.NewPoint2fVectorFromPoints(fromPoints)
	defer fromVec.Close()
	toVec := gocv.NewPoint2fVectorFromPoints(toPoints)
	defer toVec.Close()

	inliers := gocv.NewMat()
	defer inliers.Close()

	transform := gocv.EstimateAffinePartial2DWithParams(
		fromVec,
		toVec,
		inliers,
		int(gocv.HomographyMethodRANSAC),
		ransacReprojThreshold,
		ransacMaxIterations,
		ransacConfidence,
		ransacRefinementIters,
	)
	defer transform.Close()

	if transform.Empty() || transform.Rows() != 2 || transform.Cols() != 3 {
		return DerivativeVerification{GoodMatches: len(goodMatches)}
	}

	inlierCount := countInliers(inliers)
	inlierRatio := float64(inlierCount) / float64(len(goodMatches))
	if inlierCount < minInliers || inlierRatio < minInlierRatio {
		return DerivativeVerification{GoodMatches: len(goodMatches), Inliers: inlierCount, InlierRatio: inlierRatio}
	}

	if !scaleLooksReasonable(transform, child.ImageWidth, child.ImageHeight, parent.ImageWidth, parent.ImageHeight) {
		return DerivativeVerification{GoodMatches: len(goodMatches), Inliers: inlierCount, InlierRatio: inlierRatio}
	}

	if !cornersFitInsideParent(transform, child.ImageWidth, child.ImageHeight, parent.ImageWidth, parent.ImageHeight) {
		return DerivativeVerification{GoodMatches: len(goodMatches), Inliers: inlierCount, InlierRatio: inlierRatio}
	}

	return DerivativeVerification{
		Confirmed:   true,
		GoodMatches: len(goodMatches),
		Inliers:     inlierCount,
		InlierRatio: inlierRatio,
	}
}

func computeORBFeatureSets(childPath string, parentPath string) (ORBFeatureSet, ORBFeatureSet, error) {
	child, err := loadImageForFeatureMatch(childPath)
	if err != nil {
		return ORBFeatureSet{}, ORBFeatureSet{}, err
	}
	defer child.Close()

	parent, err := loadImageForFeatureMatch(parentPath)
	if err != nil {
		return ORBFeatureSet{}, ORBFeatureSet{}, err
	}
	defer parent.Close()

	if child.Empty() || parent.Empty() {
		return ORBFeatureSet{}, ORBFeatureSet{}, nil
	}

	childGray, err := normalizeForFeatureMatch(child)
	if err != nil {
		return ORBFeatureSet{}, ORBFeatureSet{}, err
	}
	defer childGray.Close()

	parentGray, err := normalizeForFeatureMatch(parent)
	if err != nil {
		return ORBFeatureSet{}, ORBFeatureSet{}, err
	}
	defer parentGray.Close()

	orb := gocv.NewORBWithParams(
		orbFeatureCount,
		float32(orbScaleFactor),
		orbLevels,
		orbEdgeThreshold,
		orbFirstLevel,
		orbWTAK,
		gocv.ORBScoreTypeHarris,
		orbPatchSize,
		orbFastThreshold,
	)
	defer orb.Close()

	childKeypoints, childDescriptors := orb.DetectAndCompute(childGray, gocv.NewMat())
	parentKeypoints, parentDescriptors := orb.DetectAndCompute(parentGray, gocv.NewMat())
	return ORBFeatureSet{
			Keypoints:   childKeypoints,
			Descriptors: childDescriptors,
			ImageWidth:  childGray.Cols(),
			ImageHeight: childGray.Rows(),
		}, ORBFeatureSet{
			Keypoints:   parentKeypoints,
			Descriptors: parentDescriptors,
			ImageWidth:  parentGray.Cols(),
			ImageHeight: parentGray.Rows(),
		}, nil
}

// ComputeORBSerializedFeatures extracts ORB keypoints/descriptors for a single image and
// returns a serialized representation suitable for persistence.
func ComputeORBSerializedFeatures(path string) (ORBSerializedFeatures, error) {
	src, err := loadImageForFeatureMatch(path)
	if err != nil {
		return ORBSerializedFeatures{}, err
	}
	defer src.Close()

	if src.Empty() {
		return ORBSerializedFeatures{}, nil
	}

	gray, err := normalizeForFeatureMatch(src)
	if err != nil {
		return ORBSerializedFeatures{}, err
	}
	defer gray.Close()

	if gray.Empty() {
		return ORBSerializedFeatures{}, nil
	}

	orb := gocv.NewORBWithParams(
		orbFeatureCount,
		float32(orbScaleFactor),
		orbLevels,
		orbEdgeThreshold,
		orbFirstLevel,
		orbWTAK,
		gocv.ORBScoreTypeHarris,
		orbPatchSize,
		orbFastThreshold,
	)
	defer orb.Close()

	keypoints, descriptors := orb.DetectAndCompute(gray, gocv.NewMat())
	defer descriptors.Close()

	if len(keypoints) == 0 || descriptors.Empty() {
		return ORBSerializedFeatures{}, nil
	}

	encodedKeypoints, err := SerializeORBKeypoints(keypoints)
	if err != nil {
		return ORBSerializedFeatures{}, fmt.Errorf("serialize ORB keypoints: %w", err)
	}

	return ORBSerializedFeatures{
		Keypoints:   encodedKeypoints,
		Descriptors: descriptors.ToBytes(),
		Rows:        descriptors.Rows(),
		Cols:        descriptors.Cols(),
		MatType:     int(descriptors.Type()),
		ImgWidth:    gray.Cols(),
		ImgHeight:   gray.Rows(),
	}, nil
}

func loadImageForFeatureMatch(path string) (gocv.Mat, error) {
	if preview, err := extractPreviewBytes(path); err == nil && len(preview) > 0 {
		mat, decodeErr := gocv.IMDecode(preview, gocv.IMReadColor)
		if decodeErr == nil && !mat.Empty() {
			return mat, nil
		}
		if !mat.Empty() {
			mat.Close()
		}
	}

	mat := gocv.IMRead(path, gocv.IMReadColor)
	if mat.Empty() {
		return gocv.NewMat(), fmt.Errorf("failed to decode image for ORB: %s", path)
	}
	return mat, nil
}

func extractPreviewBytes(path string) ([]byte, error) {
	pool, err := projectexiftool.SharedPool()
	if err != nil {
		return nil, err
	}

	results, err := pool.Extract([]string{path}, []string{
		"PreviewImage",
		"JpgFromRaw",
		"ThumbnailImage",
	}, projectexiftool.QueryOptions{
		Binary:            true,
		IgnoreMinorErrors: true,
	})
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("unexpected exiftool result count for %s: %d", path, len(results))
	}

	for _, key := range []string{"PreviewImage", "JpgFromRaw", "ThumbnailImage"} {
		data, ok, err := results[0].GetBytes(key)
		if err != nil {
			return nil, err
		}
		if ok && len(data) > 0 {
			return data, nil
		}
	}
	return nil, fmt.Errorf("no preview image found for %s", path)
}

func normalizeForFeatureMatch(src gocv.Mat) (gocv.Mat, error) {
	if src.Empty() {
		return gocv.NewMat(), nil
	}

	gray := gocv.NewMat()
	if src.Channels() == 1 {
		src.CopyTo(&gray)
	} else {
		if err := gocv.CvtColor(src, &gray, gocv.ColorBGRToGray); err != nil {
			gray.Close()
			return gocv.NewMat(), err
		}
	}

	if gray.Empty() {
		return gray, nil
	}

	maxSide := gray.Cols()
	if gray.Rows() > maxSide {
		maxSide = gray.Rows()
	}
	if maxSide >= minImageDimension && maxSide <= maxImageDimension {
		return gray, nil
	}

	scale := 1.0
	if maxSide < minImageDimension {
		scale = float64(minImageDimension) / float64(maxSide)
	} else {
		scale = float64(maxImageDimension) / float64(maxSide)
	}
	resized := gocv.NewMat()
	interpolation := gocv.InterpolationArea
	if scale > 1 {
		interpolation = gocv.InterpolationCubic
	}
	if err := gocv.Resize(gray, &resized, image.Point{}, scale, scale, interpolation); err != nil {
		gray.Close()
		resized.Close()
		return gocv.NewMat(), err
	}
	gray.Close()
	return resized, nil
}

func countInliers(mask gocv.Mat) int {
	if mask.Empty() {
		return 0
	}
	count := 0
	for row := 0; row < mask.Rows(); row++ {
		if mask.GetUCharAt(row, 0) != 0 {
			count++
		}
	}
	return count
}

func scaleLooksReasonable(transform gocv.Mat, childWidth, childHeight, parentWidth, parentHeight int) bool {
	a := transform.GetDoubleAt(0, 0)
	b := transform.GetDoubleAt(0, 1)
	c := transform.GetDoubleAt(1, 0)
	d := transform.GetDoubleAt(1, 1)

	scaleX := math.Hypot(a, c)
	scaleY := math.Hypot(b, d)
	expectedX := float64(parentWidth) / float64(childWidth)
	expectedY := float64(parentHeight) / float64(childHeight)
	return relativeDiff(scaleX, expectedX) <= maxScaleDeviationRatio &&
		relativeDiff(scaleY, expectedY) <= maxScaleDeviationRatio
}

func cornersFitInsideParent(transform gocv.Mat, childWidth, childHeight, parentWidth, parentHeight int) bool {
	corners := [][2]float64{
		{0, 0},
		{float64(childWidth), 0},
		{0, float64(childHeight)},
		{float64(childWidth), float64(childHeight)},
	}

	minX := -maxCornerOverflowRatio * float64(parentWidth)
	maxX := float64(parentWidth) * (1 + maxCornerOverflowRatio)
	minY := -maxCornerOverflowRatio * float64(parentHeight)
	maxY := float64(parentHeight) * (1 + maxCornerOverflowRatio)

	for _, corner := range corners {
		x, y := applyAffine(transform, corner[0], corner[1])
		if x < minX || x > maxX || y < minY || y > maxY {
			return false
		}
	}
	return true
}

func applyAffine(transform gocv.Mat, x, y float64) (float64, float64) {
	tx := transform.GetDoubleAt(0, 0)*x + transform.GetDoubleAt(0, 1)*y + transform.GetDoubleAt(0, 2)
	ty := transform.GetDoubleAt(1, 0)*x + transform.GetDoubleAt(1, 1)*y + transform.GetDoubleAt(1, 2)
	return tx, ty
}

func relativeDiff(actual, expected float64) float64 {
	if actual == 0 && expected == 0 {
		return 0
	}
	denom := math.Max(math.Abs(actual), math.Abs(expected))
	if denom == 0 {
		return math.Inf(1)
	}
	return math.Abs(actual-expected) / denom
}
