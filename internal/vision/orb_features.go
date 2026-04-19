package vision

import (
	"fmt"
)

// DeserializeORBFeatureSet converts serialized ORB data into an ORBFeatureSet.
// The returned feature set owns a native OpenCV Mat and must be closed by calling Close().
func DeserializeORBFeatureSet(serial ORBSerializedFeatures) (ORBFeatureSet, error) {
	if len(serial.Keypoints) == 0 || len(serial.Descriptors) == 0 || serial.Rows <= 0 || serial.Cols <= 0 {
		return ORBFeatureSet{}, nil
	}

	keypoints, err := DeserializeORBKeypoints(serial.Keypoints)
	if err != nil {
		return ORBFeatureSet{}, fmt.Errorf("decode keypoints: %w", err)
	}

	descriptors, err := DeserializeORBDescriptors(serial.Rows, serial.Cols, serial.MatType, serial.Descriptors)
	if err != nil {
		return ORBFeatureSet{}, fmt.Errorf("decode descriptors: %w", err)
	}

	return ORBFeatureSet{
		Keypoints:   keypoints,
		Descriptors: descriptors,
		ImageWidth:  serial.ImgWidth,
		ImageHeight: serial.ImgHeight,
	}, nil
}
