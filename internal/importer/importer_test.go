package importer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTargetDirRoot(t *testing.T) {
	tests := []struct {
		target   string
		expected string
	}{
		{
			target:   filepath.FromSlash("/photos/2023/12/20"),
			expected: "/photos",
		},
		{
			target:   filepath.Join("data", "2024", "01", "01"),
			expected: "data",
		},
	}

	for _, tt := range tests {
		got := targetDirRoot(tt.target)
		// On Windows, filepath.Dir will use backslashes.
		// Normalize to forward slashes for the test logic or use filepath.FromSlash
		assert.Equal(t, filepath.Clean(tt.expected), filepath.Clean(got))
	}
}

func TestCopyFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_copy_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	src := filepath.Join(tempDir, "src.txt")
	dst := filepath.Join(tempDir, "dst.txt")
	content := []byte("hello world")

	err = os.WriteFile(src, content, 0644)
	require.NoError(t, err)

	err = copyFile(src, dst)
	require.NoError(t, err)

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}
