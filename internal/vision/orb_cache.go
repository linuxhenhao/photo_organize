package vision

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"gocv.io/x/gocv"
)

// SerializeORBKeypoints encodes keypoints into a stable little-endian binary format.
//
// Format:
// - uint32 count
// - repeated count times:
//   - float64 x, y, size, angle, response
//   - int32 octave, class_id
func SerializeORBKeypoints(keypoints []gocv.KeyPoint) ([]byte, error) {
	if len(keypoints) > int(^uint32(0)) {
		return nil, fmt.Errorf("too many keypoints: %d", len(keypoints))
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(keypoints))); err != nil {
		return nil, err
	}
	for _, kp := range keypoints {
		if err := binary.Write(&buf, binary.LittleEndian, kp.X); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, kp.Y); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, kp.Size); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, kp.Angle); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, kp.Response); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, int32(kp.Octave)); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, int32(kp.ClassID)); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func DeserializeORBKeypoints(encoded []byte) ([]gocv.KeyPoint, error) {
	reader := bytes.NewReader(encoded)
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	keypoints := make([]gocv.KeyPoint, 0, int(count))
	for i := uint32(0); i < count; i++ {
		var kp gocv.KeyPoint
		if err := binary.Read(reader, binary.LittleEndian, &kp.X); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &kp.Y); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &kp.Size); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &kp.Angle); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &kp.Response); err != nil {
			return nil, err
		}
		var octave int32
		if err := binary.Read(reader, binary.LittleEndian, &octave); err != nil {
			return nil, err
		}
		kp.Octave = int(octave)
		var classID int32
		if err := binary.Read(reader, binary.LittleEndian, &classID); err != nil {
			return nil, err
		}
		kp.ClassID = int(classID)

		keypoints = append(keypoints, kp)
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("unexpected trailing bytes: %d", reader.Len())
	}
	return keypoints, nil
}

// DeserializeORBDescriptors reconstructs the ORB descriptor Mat from raw bytes.
// The returned Mat must be closed by the caller.
func DeserializeORBDescriptors(rows int, cols int, matType int, data []byte) (gocv.Mat, error) {
	return gocv.NewMatFromBytes(rows, cols, gocv.MatType(matType), data)
}
