//go:build gocv

package vision

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gocv.io/x/gocv"
)

func TestSerializeORBKeypointsRoundTrip(t *testing.T) {
	keypoints := []gocv.KeyPoint{
		{X: 1.25, Y: 2.5, Size: 3.0, Angle: 45.0, Response: 0.7, Octave: 1, ClassID: 9},
		{X: -10.0, Y: 0.0, Size: 8.0, Angle: 90.0, Response: 0.1, Octave: 2, ClassID: -1},
	}

	encoded, err := SerializeORBKeypoints(keypoints)
	require.NoError(t, err)

	decoded, err := DeserializeORBKeypoints(encoded)
	require.NoError(t, err)
	require.Equal(t, keypoints, decoded)
}

func TestDeserializeORBDescriptorsReconstructsMat(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5, 6}
	mat, err := DeserializeORBDescriptors(2, 3, int(gocv.MatTypeCV8U), data)
	require.NoError(t, err)
	defer mat.Close()

	require.Equal(t, 2, mat.Rows())
	require.Equal(t, 3, mat.Cols())
	require.Equal(t, gocv.MatTypeCV8U, mat.Type())
	require.Equal(t, data, mat.ToBytes())
}
