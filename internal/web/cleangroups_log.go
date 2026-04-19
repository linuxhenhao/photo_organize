package web

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type cleanGroupsLogEvent struct {
	Line         int               `json:"line"`
	Timestamp    string            `json:"timestamp,omitempty"`
	Source       string            `json:"source,omitempty"`
	Event        string            `json:"event"`
	Action       string            `json:"action,omitempty"`
	Mode         string            `json:"mode,omitempty"`
	Path         string            `json:"path,omitempty"`
	Thumbnail    string            `json:"thumbnailPath,omitempty"`
	Master       string            `json:"masterPath,omitempty"`
	SourceMaster string            `json:"sourceMaster,omitempty"`
	TargetMaster string            `json:"targetMaster,omitempty"`
	Reason       string            `json:"reason,omitempty"`
	Error        string            `json:"error,omitempty"`
	Changed      bool              `json:"changed"`
	Fields       map[string]string `json:"fields"`
	Raw          string            `json:"raw"`
}

type cleanGroupsLogResponse struct {
	Path        string                `json:"path"`
	UpdatedAt   string                `json:"updatedAt"`
	EventCounts map[string]int        `json:"eventCounts"`
	Summary     map[string]string     `json:"summary,omitempty"`
	Events      []cleanGroupsLogEvent `json:"events"`
}

func (ws *WebServer) handleGetCleanGroupsLog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	logPath, err := ws.resolveCleanGroupsLogPath(r.URL.Query().Get("path"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	response, err := parseCleanGroupsLogFile(logPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to parse cleangroups log: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (ws *WebServer) resolveCleanGroupsLogPath(requested string) (string, error) {
	trimmed := strings.TrimSpace(requested)
	if trimmed == "" {
		if ws.cleanGroupsLogPath != "" {
			return filepath.Clean(ws.cleanGroupsLogPath), nil
		}

		defaultPath := filepath.Join(ws.destDir, "cleangroups.log")
		if _, err := os.Stat(defaultPath); err == nil {
			return defaultPath, nil
		}
		return "", fmt.Errorf("cleangroups log is not configured; pass -cleangroups-log or place cleangroups.log under destination")
	}

	if filepath.IsAbs(trimmed) {
		if ws.cleanGroupsLogPath == "" || filepath.Clean(ws.cleanGroupsLogPath) != filepath.Clean(trimmed) {
			return "", fmt.Errorf("absolute log path is only allowed when it matches -cleangroups-log")
		}
		return filepath.Clean(trimmed), nil
	}

	return ws.resolveWithinDest(trimmed)
}

func parseCleanGroupsLogFile(logPath string) (cleanGroupsLogResponse, error) {
	file, err := os.Open(logPath)
	if err != nil {
		return cleanGroupsLogResponse{}, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return cleanGroupsLogResponse{}, err
	}

	response := cleanGroupsLogResponse{
		Path:        logPath,
		UpdatedAt:   info.ModTime().Format(time.RFC3339),
		EventCounts: make(map[string]int),
		Events:      make([]cleanGroupsLogEvent, 0),
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		event, ok := parseCleanGroupsLogLine(scanner.Text(), lineNo)
		if !ok {
			continue
		}
		if event.Event == "summary" {
			response.Summary = cloneStringMap(event.Fields)
			continue
		}
		response.EventCounts[event.Event]++
		response.Events = append(response.Events, event)
	}
	if err := scanner.Err(); err != nil {
		return cleanGroupsLogResponse{}, err
	}

	return response, nil
}

func parseCleanGroupsLogLine(line string, lineNo int) (cleanGroupsLogEvent, bool) {
	idx := strings.Index(line, "cleangroups:")
	if idx < 0 {
		return cleanGroupsLogEvent{}, false
	}

	prefix := strings.TrimSpace(line[:idx])
	body := strings.TrimSpace(line[idx+len("cleangroups:"):])
	fields, ok := parseCleanGroupsLogFields(body)
	if !ok {
		return cleanGroupsLogEvent{}, false
	}

	eventName := fields["event"]
	if eventName == "" {
		return cleanGroupsLogEvent{}, false
	}

	timestamp, source := parseCleanGroupsLogPrefix(prefix)
	event := cleanGroupsLogEvent{
		Line:         lineNo,
		Timestamp:    timestamp,
		Source:       source,
		Event:        eventName,
		Action:       fields["action"],
		Mode:         fields["mode"],
		Path:         firstNonEmpty(fields["path"], fields["thumbnail_path"], fields["candidate_path"]),
		Thumbnail:    fields["thumbnail_path"],
		Master:       fields["master_path"],
		SourceMaster: fields["source_master"],
		TargetMaster: fields["target_master"],
		Reason:       firstNonEmpty(fields["rehome_reason"], fields["reason"]),
		Error:        fields["error"],
		Changed:      isChangedCleanGroupsEvent(eventName),
		Fields:       fields,
		Raw:          line,
	}
	return event, true
}

func parseCleanGroupsLogPrefix(prefix string) (string, string) {
	if len(prefix) < len("2006/01/02 15:04:05") {
		return "", strings.TrimSuffix(prefix, ":")
	}

	timestamp := prefix[:19]
	if _, err := time.Parse("2006/01/02 15:04:05", timestamp); err != nil {
		return "", strings.TrimSuffix(prefix, ":")
	}

	return timestamp, strings.TrimSuffix(strings.TrimSpace(prefix[19:]), ":")
}

func parseCleanGroupsLogFields(body string) (map[string]string, bool) {
	fields := make(map[string]string)
	index := 0
	for index < len(body) {
		for index < len(body) && body[index] == ' ' {
			index++
		}
		if index >= len(body) {
			break
		}

		keyStart := index
		for index < len(body) && body[index] != '=' && body[index] != ' ' {
			index++
		}
		if index >= len(body) || body[index] != '=' {
			return nil, false
		}

		key := strings.TrimSpace(body[keyStart:index])
		index++
		if key == "" || index >= len(body) {
			return nil, false
		}

		value, next, ok := parseCleanGroupsLogValue(body, index)
		if !ok {
			return nil, false
		}
		fields[key] = value
		index = next
	}
	return fields, true
}

func parseCleanGroupsLogValue(body string, start int) (string, int, bool) {
	if start >= len(body) {
		return "", start, false
	}

	if body[start] == '"' {
		end := start + 1
		escaped := false
		for end < len(body) {
			ch := body[end]
			if ch == '\\' && !escaped {
				escaped = true
				end++
				continue
			}
			if ch == '"' && !escaped {
				raw := body[start : end+1]
				value, err := strconv.Unquote(raw)
				if err != nil {
					return "", start, false
				}
				return value, end + 1, true
			}
			escaped = false
			end++
		}
		return "", start, false
	}

	end := start
	for end < len(body) && body[end] != ' ' {
		end++
	}
	return body[start:end], end, true
}

func isChangedCleanGroupsEvent(event string) bool {
	switch event {
	case "rehome", "standalone", "missing_thumbnail":
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}
