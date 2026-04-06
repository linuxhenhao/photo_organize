package hasher

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCalculateHash(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_hash_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "test.txt")
	content := []byte("test content")
	err = os.WriteFile(filePath, content, 0644)
	require.NoError(t, err)

	// Since we are using murmur3, "test content" will have a specific deterministic hash.
	// We just want to check it runs without error and returns non-empty.
	hash1, err := CalculateHash(filePath)
	require.NoError(t, err)
	require.NotEmpty(t, hash1)

	// ensure consistency
	hash2, err := CalculateHash(filePath)
	require.NoError(t, err)
	require.Equal(t, hash1, hash2)
}
