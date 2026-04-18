package exiftool

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	defaultPoolSize   = 4
	defaultBufferSize = 64 * 1024 * 1024
)

var (
	errNoReadyToken = errors.New("exiftool response missing ready token")

	sharedPoolOnce sync.Once
	sharedPool     *Pool
	sharedPoolErr  error
)

// QueryOptions controls the arguments used for a single exiftool request.
type QueryOptions struct {
	Fast              bool
	IgnoreMinorErrors bool
	Binary            bool
	DateFormat        string
}

// Result represents the metadata returned for one file.
type Result struct {
	File   string
	Fields map[string]any
}

func (r Result) GetString(key string) (string, bool) {
	if r.Fields == nil {
		return "", false
	}

	value, ok := r.Fields[key]
	if !ok || value == nil {
		return "", false
	}

	switch typed := value.(type) {
	case string:
		return typed, true
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), true
	case int64:
		return strconv.FormatInt(typed, 10), true
	default:
		return fmt.Sprintf("%v", typed), true
	}
}

func (r Result) GetInt(key string) (int, bool) {
	if r.Fields == nil {
		return 0, false
	}

	value, ok := r.Fields[key]
	if !ok || value == nil {
		return 0, false
	}

	switch typed := value.(type) {
	case float64:
		return int(typed), true
	case int64:
		return int(typed), true
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func (r Result) GetBytes(key string) ([]byte, bool, error) {
	value, ok := r.GetString(key)
	if !ok || value == "" {
		return nil, false, nil
	}

	if !strings.HasPrefix(value, "base64:") {
		return []byte(value), true, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, "base64:"))
	if err != nil {
		return nil, true, err
	}
	return decoded, true, nil
}

type client struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	nextID uint64
}

type Pool struct {
	clients chan *client
	all     []*client
}

// SharedPool returns the process-wide exiftool pool used by metadata and hashing code.
func SharedPool() (*Pool, error) {
	sharedPoolOnce.Do(func() {
		sharedPool, sharedPoolErr = NewPool(defaultClientCount())
	})
	return sharedPool, sharedPoolErr
}

func defaultClientCount() int {
	count := runtime.GOMAXPROCS(0) / 2
	if count < 1 {
		count = 1
	}
	if count > defaultPoolSize {
		count = defaultPoolSize
	}
	return count
}

func NewPool(size int) (*Pool, error) {
	if size < 1 {
		size = 1
	}

	pool := &Pool{
		clients: make(chan *client, size),
		all:     make([]*client, 0, size),
	}

	for i := 0; i < size; i++ {
		client, err := newClient()
		if err != nil {
			_ = pool.Close()
			return nil, err
		}
		pool.all = append(pool.all, client)
		pool.clients <- client
	}

	return pool, nil
}

func (p *Pool) Close() error {
	if p == nil {
		return nil
	}

	var closeErrs []string
	for _, client := range p.all {
		if err := client.close(); err != nil {
			closeErrs = append(closeErrs, err.Error())
		}
	}

	if len(closeErrs) > 0 {
		return fmt.Errorf("failed to close exiftool pool: %s", strings.Join(closeErrs, "; "))
	}
	return nil
}

func (p *Pool) Extract(paths []string, tags []string, opts QueryOptions) ([]Result, error) {
	if len(paths) == 0 {
		return nil, nil
	}

	client := <-p.clients
	defer func() {
		p.clients <- client
	}()

	return client.extract(paths, tags, opts)
}

func newClient() (*client, error) {
	cmd := exec.Command("exiftool", "-stay_open", "True", "-@", "-")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create exiftool stdin pipe: %w", err)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create exiftool stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create exiftool stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start exiftool: %w", err)
	}

	go func() {
		_, _ = io.Copy(io.Discard, stderrPipe)
	}()

	return &client{
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewReaderSize(stdoutPipe, defaultBufferSize),
	}, nil
}

func (c *client) close() error {
	if c == nil || c.cmd == nil {
		return nil
	}

	if _, err := fmt.Fprintln(c.stdin, "-stay_open"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(c.stdin, "False"); err != nil {
		return err
	}
	if err := c.stdin.Close(); err != nil {
		return err
	}
	if err := c.cmd.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *client) extract(paths []string, tags []string, opts QueryOptions) ([]Result, error) {
	requestID := atomic.AddUint64(&c.nextID, 1)
	if err := c.writeRequest(paths, tags, opts, requestID); err != nil {
		return nil, err
	}

	response, err := c.readResponse(requestID)
	if err != nil {
		return nil, err
	}

	return decodeResults(paths, response)
}

func (c *client) writeRequest(paths []string, tags []string, opts QueryOptions, requestID uint64) error {
	if _, err := fmt.Fprintln(c.stdin, "-j"); err != nil {
		return err
	}
	if opts.Fast {
		if _, err := fmt.Fprintln(c.stdin, "-fast"); err != nil {
			return err
		}
	}
	if opts.IgnoreMinorErrors {
		if _, err := fmt.Fprintln(c.stdin, "-m"); err != nil {
			return err
		}
	}
	if opts.Binary {
		if _, err := fmt.Fprintln(c.stdin, "-b"); err != nil {
			return err
		}
	}
	if opts.DateFormat != "" {
		if _, err := fmt.Fprintln(c.stdin, "-d"); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(c.stdin, opts.DateFormat); err != nil {
			return err
		}
	}

	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		if !strings.HasPrefix(tag, "-") {
			tag = "-" + tag
		}
		if _, err := fmt.Fprintln(c.stdin, tag); err != nil {
			return err
		}
	}

	for _, path := range paths {
		if err := writeArgfileCString(c.stdin, normalizeArgfilePath(path)); err != nil {
			return err
		}
	}

	_, err := fmt.Fprintf(c.stdin, "-execute%d\n", requestID)
	return err
}

func writeArgfileCString(w io.Writer, arg string) error {
	if strings.ContainsRune(arg, '\x00') {
		return fmt.Errorf("argument contains NUL byte")
	}

	var encoded strings.Builder
	encoded.Grow(len(arg) + len("#[CSTR]") + 1)
	encoded.WriteString("#[CSTR]")

	for _, r := range arg {
		switch r {
		case '\\':
			encoded.WriteString(`\\`)
		case '\a':
			encoded.WriteString(`\a`)
		case '\b':
			encoded.WriteString(`\b`)
		case '\f':
			encoded.WriteString(`\f`)
		case '\n':
			encoded.WriteString(`\n`)
		case '\r':
			encoded.WriteString(`\r`)
		case '\t':
			encoded.WriteString(`\t`)
		case '\v':
			encoded.WriteString(`\v`)
		default:
			if r < 0x20 || r == 0x7f {
				_, _ = fmt.Fprintf(&encoded, `\x%02X`, r)
				continue
			}
			encoded.WriteRune(r)
		}
	}
	encoded.WriteByte('\n')

	_, err := io.WriteString(w, encoded.String())
	return err
}

func normalizeArgfilePath(path string) string {
	if path == "" || path == "-" {
		return path
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return absPath
}

func (c *client) readResponse(requestID uint64) ([]byte, error) {
	readyToken := fmt.Sprintf("{ready%d}", requestID)
	var response bytes.Buffer

	for {
		line, err := c.stdout.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && response.Len() > 0 {
				return nil, errNoReadyToken
			}
			return nil, err
		}

		trimmed := strings.TrimSpace(line)
		if trimmed == readyToken {
			return response.Bytes(), nil
		}
		response.WriteString(line)
	}
}

func decodeResults(paths []string, raw []byte) ([]Result, error) {
	results := make([]Result, len(paths))
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		for i, path := range paths {
			results[i] = Result{
				File:   path,
				Fields: map[string]any{},
			}
		}
		return results, nil
	}

	var rows []map[string]any
	if err := json.Unmarshal(trimmed, &rows); err != nil {
		return nil, fmt.Errorf("failed to decode exiftool response: %w", err)
	}

	if len(rows) == len(paths) {
		for i, row := range rows {
			results[i] = Result{
				File:   sourceFile(row, paths[i]),
				Fields: row,
			}
		}
		return results, nil
	}

	indexByPath := make(map[string]int, len(paths))
	for i, path := range paths {
		indexByPath[path] = i
		results[i] = Result{
			File:   path,
			Fields: map[string]any{},
		}
	}

	for _, row := range rows {
		path := sourceFile(row, "")
		index, ok := indexByPath[path]
		if !ok {
			continue
		}
		results[index] = Result{
			File:   path,
			Fields: row,
		}
	}

	return results, nil
}

func sourceFile(fields map[string]any, fallback string) string {
	value, ok := fields["SourceFile"]
	if !ok || value == nil {
		return fallback
	}
	if typed, ok := value.(string); ok && typed != "" {
		return typed
	}
	return fallback
}
