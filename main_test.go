package main

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_extractTime(t *testing.T) {
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
			path: "/a/b/c/333-2023-01-12.jpg",
			time: time.Date(2023, 01, 12, 0, 0, 0, 0, time.Local),
		},
		{
			name: "yyyymmdd",
			path: "/c/d/e/f/20230112.raw",
			time: time.Date(2023, 01, 12, 0, 0, 0, 0, time.Local),
		},
		{
			name: "yyyy/mm/dd/12331212.jpg",
			path: "/a/b/c/2023/01/12/3.jpg",
			time: time.Date(2023, 01, 12, 0, 0, 0, 0, time.Local),
		},
		{
			name: "yyyy/mm/dd/a/b/12331212.jpg",
			path: "/a/b/c/2023/01/12/a/b/3.jpg",
			time: time.Time{},
			err:  errors.New("e"),
		},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractTimeFromFilename(tt.path)
			if tt.err != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.time, got)
		})
	}
}
