package exiftool

import (
	"bytes"
	"encoding/base64"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type nopWriteCloser struct {
	*bytes.Buffer
}

func (n nopWriteCloser) Close() error {
	return nil
}

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

func TestWriteRequestSeparatesOptionsFromPaths(t *testing.T) {
	var stdin bytes.Buffer
	client := &client{stdin: nopWriteCloser{&stdin}}

	dashPath, err := filepath.Abs("-dash.jpg")
	require.NoError(t, err)
	commentPath, err := filepath.Abs("#comment.jpg")
	require.NoError(t, err)
	leadingSpacePath, err := filepath.Abs(" leading.jpg")
	require.NoError(t, err)
	lineBreakPath, err := filepath.Abs("line\nbreak.jpg")
	require.NoError(t, err)

	err = client.writeRequest(
		[]string{"-dash.jpg", "#comment.jpg", " leading.jpg", "line\nbreak.jpg"},
		[]string{"PreviewImage"},
		QueryOptions{Binary: true},
		7,
	)
	require.NoError(t, err)

	encodedDashPath := encodeArgLineForTest(t, dashPath)
	encodedCommentPath := encodeArgLineForTest(t, commentPath)
	encodedLeadingSpacePath := encodeArgLineForTest(t, leadingSpacePath)
	encodedLineBreakPath := encodeArgLineForTest(t, lineBreakPath)

	require.Equal(t, strings.Join([]string{
		"-j",
		"-b",
		"-PreviewImage",
		encodedDashPath,
		encodedCommentPath,
		encodedLeadingSpacePath,
		encodedLineBreakPath,
		"-execute7",
		"",
	}, "\n"), stdin.String())
}

func TestWriteArgfileCStringRejectsNUL(t *testing.T) {
	var out bytes.Buffer
	err := writeArgfileCString(&out, "bad\x00name.jpg")
	require.Error(t, err)
	require.Empty(t, out.String())
}

func encodeArgLineForTest(t *testing.T, arg string) string {
	t.Helper()

	var out bytes.Buffer
	require.NoError(t, writeArgfileCString(&out, arg))
	return strings.TrimSuffix(out.String(), "\n")
}
