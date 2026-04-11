package exiftool

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultGetBytesDecodesBase64(t *testing.T) {
	expected := []byte("preview-bytes")
	result := Result{
		File: "test.arw",
		Fields: map[string]any{
			"PreviewImage": "base64:" + base64.StdEncoding.EncodeToString(expected),
		},
	}

	actual, ok, err := result.GetBytes("PreviewImage")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, expected, actual)
}

func TestDecodeResultsPreservesRequestOrder(t *testing.T) {
	raw := []byte(`[
		{"SourceFile":"b.jpg","MIMEType":"image/jpeg"},
		{"SourceFile":"a.jpg","MIMEType":"image/jpeg"}
	]`)

	results, err := decodeResults([]string{"b.jpg", "a.jpg"}, raw)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, "b.jpg", results[0].File)
	require.Equal(t, "a.jpg", results[1].File)
}
