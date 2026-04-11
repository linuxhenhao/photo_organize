package metadata

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExtractTimeFromFilename(t *testing.T) {
	testcases := []struct {
		name string
		path string
		err  error
		time time.Time
	}{
		{
			name: "yyyy-mm-dd",
			path: "/a/b/2023-01-12-fdafa.ja",
			time: time.Date(2023, 01, 12, 0, 0, 0, 0, time.Local),
		},
		{
			name: "yyyy_mm_dd",
			path: "/a/b/c/333-2023_01_12.jpg",
			time: time.Date(2023, 01, 12, 0, 0, 0, 0, time.Local),
		},
		{
			name: "yyyymmdd",
			path: "/c/d/e/f/20230112.raw",
			time: time.Date(2023, 01, 12, 0, 0, 0, 0, time.Local),
		},
		{
			name: "yyyy/mm/dd/filename",
			path: "/a/b/c/2023/01/12/3.jpg",
			time: time.Date(2023, 01, 12, 0, 0, 0, 0, time.Local),
		},
		{
			name: "invalid depth for yyyy/mm/dd",
			path: "/a/b/c/2023/01/12/a/b/3.jpg",
			time: time.Time{},
			err:  errors.New("no valid date format found"),
		},
		{
			name: "future date",
			path: "9999-01-01.jpg",
			time: time.Time{},
			err:  errors.New("unlikely year"),
		},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractTimeFromFilename(tt.path)
			if tt.err != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.time, got)
			}
		})
	}
}

func TestValidateDate(t *testing.T) {
	tests := []struct {
		y, m, d  int
		expected bool
	}{
		{2023, 5, 1, true},
		{1899, 5, 1, false},
		{2023, 13, 1, false},
		{2023, 5, 32, false},
		{2023, 0, 1, false},
	}

	for _, tt := range tests {
		got, _ := validateDate(tt.y, tt.m, tt.d, "test")
		require.Equal(t, tt.expected, got, "validateDate(%d, %d, %d)", tt.y, tt.m, tt.d)
	}
}
