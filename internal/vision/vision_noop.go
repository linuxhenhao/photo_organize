//go:build !gocv

package vision

import (
	"errors"
)

var ErrNoOpenCV = errors.New("OpenCV/gocv is not available in this build")

type KeyPoint struct {
	X        float32
	Y        float32
	Size     float32
	Angle    float32
	Response float32
	Octave   int
	ClassID  int
}

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

type Mat struct{}

func (m Mat) Empty() bool { return true }
func (m Mat) Close()      {}

type ORBFeatureSet struct {
	Keypoints   []KeyPoint
	Descriptors Mat
	ImageWidth  int
	ImageHeight int
}

func (f *ORBFeatureSet) Close() {
	if f == nil {
		return
	}
	f.Descriptors.Close()
}

func VerifyDerivativeWithORB(childPath, parentPath string) (DerivativeVerification, error) {
	return DerivativeVerification{}, ErrNoOpenCV
}

func VerifyDerivativeWithORBFeatures(child ORBFeatureSet, parent ORBFeatureSet) DerivativeVerification {
	return DerivativeVerification{}
}

func DeserializeORBFeatureSet(serial ORBSerializedFeatures) (ORBFeatureSet, error) {
	return ORBFeatureSet{}, ErrNoOpenCV
}

func ComputeORBSerializedFeatures(path string) (ORBSerializedFeatures, error) {
	return ORBSerializedFeatures{}, ErrNoOpenCV
}

func SerializeORBKeypoints(keypoints []KeyPoint) ([]byte, error) {
	return nil, ErrNoOpenCV
}

func DeserializeORBKeypoints(encoded []byte) ([]KeyPoint, error) {
	return nil, ErrNoOpenCV
}

func DeserializeORBDescriptors(rows int, cols int, matType int, data []byte) (Mat, error) {
	return Mat{}, ErrNoOpenCV
}
