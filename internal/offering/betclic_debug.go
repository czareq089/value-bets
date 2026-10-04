package offering

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

type betclicTraceKey struct{}

var betclicDebug struct {
	once   sync.Once
	logger *log.Logger
	path   string
	seq    atomic.Uint64
	dumpMu sync.Mutex
}

func initBetclicDebug() {
	betclicDebug.once.Do(func() {
		// Use the working directory so desktop and CLI diagnostics are easy to find.
		dir, err := os.Getwd()
		if err != nil {
			dir = "."
		}
		betclicDebug.path = filepath.Join(dir, "logs", "betclic-debug.log")
		betclicDebug.logger = log.New(os.Stderr, "[Betclic] ", log.Ldate|log.Ltime|log.Lmicroseconds)
		if err := os.MkdirAll(filepath.Dir(betclicDebug.path), 0755); err != nil {
			betclicDebug.logger.Printf("Nie można utworzyć katalogu logów: %v", err)
			return
		}
		file, err := os.OpenFile(betclicDebug.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			betclicDebug.logger.Printf("Nie można otworzyć logu %q: %v", betclicDebug.path, err)
			return
		}
		// Kept open for the process lifetime; log.Logger serializes concurrent writes.
		betclicDebug.logger.SetOutput(io.MultiWriter(os.Stderr, file))
		betclicDebug.logger.Printf("SESSION pid=%d log=%q", os.Getpid(), betclicDebug.path)
	})
}

// BetclicDebugLogPath returns the diagnostic file location, including for GUI errors.
func BetclicDebugLogPath() string {
	initBetclicDebug()
	return betclicDebug.path
}

func betclicTrace(ctx context.Context, stage string) context.Context {
	initBetclicDebug()
	parent, _ := ctx.Value(betclicTraceKey{}).(string)
	id := fmt.Sprintf("%s-%d", stage, betclicDebug.seq.Add(1))
	if parent != "" {
		id = parent + "/" + id
	}
	return context.WithValue(ctx, betclicTraceKey{}, id)
}

func betclicDebugf(ctx context.Context, format string, args ...any) {
	initBetclicDebug()
	trace, _ := ctx.Value(betclicTraceKey{}).(string)
	betclicDebug.logger.Printf("trace=%q %s", trace, fmt.Sprintf(format, args...))
}

func betclicPreview(raw []byte) string {
	const limit = 256
	if len(raw) > limit {
		return fmt.Sprintf("%x... (%d bytes total)", raw[:limit], len(raw))
	}
	return fmt.Sprintf("%x", raw)
}

func betclicProtoSummary(fields []protoField) string {
	var summary []string
	for i, field := range fields {
		if i == 30 {
			summary = append(summary, "...")
			break
		}
		if field.Wire == 0 {
			summary = append(summary, fmt.Sprintf("%d:varint=%d", field.Number, field.Uint))
		} else {
			summary = append(summary, fmt.Sprintf("%d:wire=%d,len=%d", field.Number, field.Wire, len(field.Bytes)))
		}
	}
	return strings.Join(summary, " ")
}

func betclicSaveParserFailure(ctx context.Context, raw []byte) {
	betclicSaveSnapshot(ctx, "betclic-last-parser-error.bin", raw)
}

func betclicSaveSnapshot(ctx context.Context, filename string, raw []byte) {
	initBetclicDebug()
	// Fixed diagnostic filenames keep only the latest response of each kind.
	path := filepath.Join(filepath.Dir(betclicDebug.path), filename)
	betclicDebug.dumpMu.Lock()
	err := os.WriteFile(path, raw, 0600)
	betclicDebug.dumpMu.Unlock()
	betclicDebugf(ctx, "SNAPSHOT_DUMP path=%q bytes=%d error=%v", path, len(raw), err)
}
