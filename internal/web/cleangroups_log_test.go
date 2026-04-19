package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCleanGroupsLogFileParsesStructuredEvents(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "cleangroups.log")
	logBody := "" +
		"2026/04/19 09:00:00 clean_groups.go:120: cleangroups: event=\"standalone\" mode=\"dry-run\" action=\"restore_standalone\" path=\"/tmp/a.jpg\" source_master=\"/tmp/master.jpg\" rehome_reason=\"no_match_found\" size=123 dimensions=\"120x90\" create_time=\"2026-04-19 09:00:00\" has_phash=true\n" +
		"2026/04/19 09:00:01 clean_groups.go:180: cleangroups: event=\"summary\" mode=\"dry-run\" groups_scanned=1 groups_changed=1 thumbnails_scanned=1 removed=1 rehomed=0 standalone_created=1 missing_removed=0 standalone_deleted=0 validation_failures=0 skipped_groups=0\n"
	require.NoError(t, os.WriteFile(logPath, []byte(logBody), 0644))

	response, err := parseCleanGroupsLogFile(logPath)
	require.NoError(t, err)

	require.Equal(t, logPath, response.Path)
	require.Equal(t, "dry-run", response.Summary["mode"])
	require.Len(t, response.Events, 1)
	require.Equal(t, "standalone", response.Events[0].Event)
	require.Equal(t, "restore_standalone", response.Events[0].Action)
	require.Equal(t, "/tmp/a.jpg", response.Events[0].Path)
	require.Equal(t, "no_match_found", response.Events[0].Reason)
	require.True(t, response.Events[0].Changed)
	require.Equal(t, 1, response.EventCounts["standalone"])
}

func TestHandleGetCleanGroupsLogUsesConfiguredAbsolutePath(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "external-cleangroups.log")
	logBody := "2026/04/19 09:00:00 clean_groups.go:130: cleangroups: event=\"rehome\" mode=\"apply\" thumbnail_path=\"/tmp/thumb.jpg\" source_master=\"/tmp/master-a.jpg\" target_master=\"/tmp/master-b.jpg\" rehome_reason=\"exact_hash_match\" standalone_deleted=false\n"
	require.NoError(t, os.WriteFile(logPath, []byte(logBody), 0644))

	ws := NewWebServer(nil, tempDir)
	ws.SetCleanGroupsLogPath(logPath)

	req := httptest.NewRequest(http.MethodGet, "/api/cleangroups-log", nil)
	rr := httptest.NewRecorder()

	ws.handleGetCleanGroupsLog(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var response cleanGroupsLogResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
	require.Equal(t, logPath, response.Path)
	require.Len(t, response.Events, 1)
	require.Equal(t, "rehome", response.Events[0].Event)
	require.Equal(t, "/tmp/master-b.jpg", response.Events[0].TargetMaster)
}

func TestHandleGetCleanGroupsLogRejectsUnexpectedAbsolutePath(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "other.log")
	require.NoError(t, os.WriteFile(logPath, []byte(""), 0644))

	ws := NewWebServer(nil, tempDir)

	req := httptest.NewRequest(http.MethodGet, "/api/cleangroups-log?path="+logPath, nil)
	rr := httptest.NewRecorder()

	ws.handleGetCleanGroupsLog(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
	require.Contains(t, rr.Body.String(), "absolute log path")
}
