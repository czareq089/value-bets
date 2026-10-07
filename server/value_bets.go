// Value Bets server: calendars, match analysis, SofaScore statistics and archives.
// All server source lives in this file. No Wails or desktop imports.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"

	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"

	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	_ "time/tzdata"
	"unicode"
	"unicode/utf8"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"github.com/gorilla/websocket"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// === Existing collectors, protocol parsers and cache ===

// === betclic.go ===
const BetclicMatchService = "https://offering.begmedia.com/web/offering.access.api/offering.access.api.MatchService/"

const maxGRPCFrame = 32 << 20

// ProtoUint and ProtoString build the same protobuf fields used by the desktop client.
func ProtoUint(field, value uint64) []byte {
	return appendVarint(appendVarint(nil, field<<3), value)
}

func ProtoString(field uint64, value string) []byte {
	b := appendVarint(nil, field<<3|2)
	b = appendVarint(b, uint64(len(value)))
	return append(b, value...)
}

func appendVarint(b []byte, value uint64) []byte {
	for value >= 128 {
		b = append(b, byte(value)|0x80)
		value >>= 7
	}
	return append(b, byte(value))
}

// FetchBetclicRPC reuses the desktop gRPC-Web request headers for any MatchService
// method. body is the protobuf message without the five-byte gRPC-Web envelope.
// Notification streams are read one complete frame at a time. A short idle period
// after complete data frames ends the snapshot, but never accepts a partial frame.
func FetchBetclicRPC(parent context.Context, client *http.Client, endpoint string, body []byte) (snapshot []byte, err error) {
	parent = betclicTrace(parent, "rpc")
	info := archiveInfoFrom(parent)
	info.Endpoint, info.Method, info.RequestHex = endpoint, http.MethodPost, fmt.Sprintf("%x", body)
	parent = context.WithValue(parent, archiveContextKey{}, info)
	started := time.Now()
	betclicDebugf(parent, "RPC_START endpoint=%q client_timeout=%s rpc_timeout=25s request_bytes=%d protobuf_hex=%s", endpoint, client.Timeout, len(body), betclicPreview(body))
	defer func() {
		betclicDebugf(parent, "RPC_END duration=%s snapshot_bytes=%d error=%v", time.Since(started), len(snapshot), err)
		if saveErr := archiveResponse(parent, "podsumowanie-rpc", nil, "", map[string]any{"duration": time.Since(started).String(), "snapshotBytes": len(snapshot)}, err); saveErr != nil {
			err = errors.Join(err, saveErr)
		}
	}()
	ctx, cancel := context.WithTimeout(parent, 25*time.Second)
	defer cancel()
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(append(header, body...)))
	if err != nil {
		return nil, err
	}
	for key, value := range map[string]string{
		"Accept": "*/*", "Accept-Language": "pl-PL", "Appversion": "10.5.0-1",
		"Content-Type": "application/grpc-web+proto", "Origin": "https://www.betclic.pl",
		"Referer": "https://www.betclic.pl/", "Sec-Fetch-Dest": "empty",
		"Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "cross-site",
		"User-Agent":     "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
		"X-Bg-Ref-Brand": "BETCLIC", "X-Bg-Ref-Platform": "DESKTOP",
		"X-Bg-Ref-Regulator-Zone": "PL", "X-Bg-Regulation": "PL", "X-Grpc-Web": "1",
	} {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Betclic połączenie: %w", err)
	}
	defer resp.Body.Close()
	info.Status, info.ContentType = resp.StatusCode, resp.Header.Get("Content-Type")
	parent = context.WithValue(parent, archiveContextKey{}, info)
	ctx = context.WithValue(ctx, archiveContextKey{}, info)
	betclicDebugf(ctx, "HTTP_RESPONSE elapsed=%s status=%d protocol=%q content_type=%q content_length=%d grpc_status=%q grpc_message=%q", time.Since(started), resp.StatusCode, resp.Proto, resp.Header.Get("Content-Type"), resp.ContentLength, resp.Header.Get("Grpc-Status"), resp.Header.Get("Grpc-Message"))
	responseErr := grpcStatusError(resp.Header)
	if resp.StatusCode != http.StatusOK {
		responseErr = fmt.Errorf("Betclic HTTP %d", resp.StatusCode)
	} else if contentType := resp.Header.Get("Content-Type"); !strings.HasPrefix(contentType, "application/grpc-web") || strings.Contains(contentType, "grpc-web-text") {
		responseErr = fmt.Errorf("Betclic nieobsługiwany Content-Type: %q", contentType)
	}
	if responseErr != nil {
		raw, readErr := readBounded(resp.Body, maxGRPCFrame)
		if saveErr := archiveResponse(ctx, "bledna-odpowiedz-http", raw, "bin", string(raw), errors.Join(responseErr, readErr)); saveErr != nil {
			return nil, errors.Join(responseErr, saveErr)
		}
		return nil, errors.Join(responseErr, readErr)
	}
	return readBetclicSnapshot(ctx, resp.Body, 2500*time.Millisecond)
}

func grpcStatusError(headers http.Header) error {
	status := headers.Get("Grpc-Status")
	if status == "" || status == "0" {
		return nil
	}
	message := headers.Get("Grpc-Message")
	if decoded, err := url.PathUnescape(message); err == nil {
		message = decoded
	}
	return fmt.Errorf("Betclic gRPC status %s: %s", status, message)
}

func readBetclicSnapshot(ctx context.Context, reader io.ReadCloser, idle time.Duration) ([]byte, error) {
	type result struct {
		frame []byte
		err   error
	}
	frames := make(chan result, 1)
	var inFrame atomic.Bool
	var archiving atomic.Bool
	readerCtx, stop := context.WithCancel(ctx)
	readerDone := make(chan struct{})
	defer func() {
		stop()
		_ = reader.Close() // interrupts a pending read on timeout or cancellation
		<-readerDone       // complete archival of already received bytes before returning
	}()
	go func() {
		defer close(readerDone)
		for {
			header := make([]byte, 5)
			headerBytes, err := io.ReadFull(reader, header[:1])
			if err == nil {
				inFrame.Store(true)
				var count int
				count, err = io.ReadFull(reader, header[1:])
				headerBytes += count
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
			}
			var frame []byte
			if err == nil {
				size := binary.BigEndian.Uint32(header[1:])
				betclicDebugf(ctx, "FRAME_HEADER flags=0x%02x payload_bytes=%d", header[0], size)
				if size > maxGRPCFrame {
					err = fmt.Errorf("zbyt duża ramka gRPC: %d", size)
				} else {
					frame = make([]byte, 5+int(size))
					copy(frame, header)
					var count int
					count, err = io.ReadFull(reader, frame[5:])
					frame = frame[:5+count]
					if err == io.EOF {
						err = io.ErrUnexpectedEOF
					}
				}
			}
			if frame == nil && headerBytes > 0 {
				frame = header[:headerBytes]
			}
			archiving.Store(true)
			inFrame.Store(false)
			if len(frame) > 0 {
				if saveErr := archiveResponse(ctx, "ramka-grpc", frame, "grpc.bin", readableGRPC(frame), err); saveErr != nil {
					err = errors.Join(err, saveErr)
				}
			}
			select {
			case frames <- result{frame, err}:
				archiving.Store(false)
			case <-readerCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var snapshot []byte
	for {
		// No short idle deadline before the first frame.
		var timer *time.Timer
		var timeout <-chan time.Time
		if len(snapshot) > 0 {
			timer = time.NewTimer(idle)
			timeout = timer.C
		}
		select {
		case <-ctx.Done():
			betclicDebugf(ctx, "STREAM_CONTEXT_END received_bytes=%d in_frame=%t error=%v", len(snapshot), inFrame.Load(), ctx.Err())
			if timer != nil {
				timer.Stop()
			}
			return nil, ctx.Err()
		case <-timeout:
			if archiving.Load() || len(frames) > 0 {
				continue // disk compression is not network idleness
			}
			betclicDebugf(ctx, "STREAM_IDLE idle=%s received_bytes=%d in_frame=%t", idle, len(snapshot), inFrame.Load())
			if inFrame.Load() {
				return nil, fmt.Errorf("Betclic przerwana ramka gRPC: %w", io.ErrUnexpectedEOF)
			}
			return snapshot, nil
		case next := <-frames:
			if timer != nil {
				timer.Stop()
			}
			if next.err != nil {
				betclicDebugf(ctx, "STREAM_READ_END received_bytes=%d error=%v", len(snapshot), next.err)
				if next.err == io.EOF && len(snapshot) > 0 {
					return snapshot, nil
				}
				return nil, fmt.Errorf("Betclic odczyt strumienia: %w", next.err)
			}
			flags := next.frame[0]
			betclicDebugf(ctx, "FRAME_COMPLETE flags=0x%02x frame_bytes=%d received_bytes=%d", flags, len(next.frame), len(snapshot))
			if flags == 0x80 {
				trailers := http.Header{}
				for _, line := range strings.Split(string(next.frame[5:]), "\r\n") {
					if key, value, ok := strings.Cut(line, ":"); ok {
						trailers.Set(strings.TrimSpace(key), strings.TrimSpace(value))
					}
				}
				if err := grpcStatusError(trailers); err != nil {
					betclicDebugf(ctx, "GRPC_TRAILERS status=%q message=%q", trailers.Get("Grpc-Status"), trailers.Get("Grpc-Message"))
					return nil, err
				}
				betclicDebugf(ctx, "GRPC_TRAILERS status=%q message=%q", trailers.Get("Grpc-Status"), trailers.Get("Grpc-Message"))
				if len(snapshot) == 0 {
					return nil, fmt.Errorf("Betclic nie zwrócił danych")
				}
				return snapshot, nil
			}
			if flags != 0 {
				return nil, fmt.Errorf("Betclic nieobsługiwane flagi ramki: %d", flags)
			}
			if len(snapshot)+len(next.frame) > maxGRPCFrame {
				return nil, fmt.Errorf("Betclic snapshot przekracza limit rozmiaru")
			}
			snapshot = append(snapshot, next.frame...)
		}
	}
}

// === betclic_debug.go ===
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

// === betclic_calendar.go ===
type Calendar struct {
	Date      string          `json:"date"`
	Events    []CalendarEvent `json:"events"`
	Warnings  []string        `json:"warnings"`
	FetchedAt string          `json:"fetchedAt,omitempty"`
}

type betclicPage struct {
	Offset int
	Events []CalendarEvent
	Total  int
	IDs    []string
}

// The request and field numbers below were checked against live responses from
// GetMatchesBySportWithNotifications: sport_code=football, language=pl, offset=4.
// The service returns 40 matches per page by default. Never treat the first page
// as the complete calendar; field 5 of the response holds the total count.
func BetclicCalendarRequest(offset int) []byte {
	body := append(ProtoString(1, "football"), ProtoString(2, "pl")...)
	if offset > 0 {
		body = append(body, ProtoUint(4, uint64(offset))...)
	}
	return body
}

func FetchBetclicCalendar(parent context.Context, client *http.Client, endpoint, date string) (*Calendar, error) {
	if _, _, err := calendarDay(date); err != nil {
		return nil, err
	}
	full, err := FetchBetclicFullCalendar(parent, client, endpoint)
	if err != nil {
		return nil, err
	}
	return FilterCalendar(full, date)
}

// FetchBetclicFullCalendar keeps every date from the paginated source snapshot.
func FetchBetclicFullCalendar(parent context.Context, client *http.Client, endpoint string) (calendar *Calendar, err error) {
	parent = betclicTrace(parent, "calendar")
	started := time.Now()
	betclicDebugf(parent, "CALENDAR_START scope=full_catalog timeout=2m")
	defer func() {
		if calendar != nil {
			betclicDebugf(parent, "CALENDAR_END duration=%s events=%d warnings=%q error=%v", time.Since(started), len(calendar.Events), calendar.Warnings, err)
		} else {
			betclicDebugf(parent, "CALENDAR_END duration=%s error=%v", time.Since(started), err)
		}
	}()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	loadPage := func(offset int) (betclicPage, error) {
		pageCtx := betclicTrace(ctx, fmt.Sprintf("page-offset-%d", offset))
		info := archiveInfoFrom(pageCtx)
		info.Scope += fmt.Sprintf("_strona-offset-%d", offset)
		pageCtx = context.WithValue(pageCtx, archiveContextKey{}, info)
		betclicDebugf(pageCtx, "PAGE_START offset=%d", offset)
		raw, err := FetchBetclicRPC(pageCtx, client, endpoint, BetclicCalendarRequest(offset))
		if err != nil {
			betclicDebugf(pageCtx, "PAGE_FETCH_ERROR offset=%d error=%v", offset, err)
			return betclicPage{}, err
		}
		betclicDebugf(pageCtx, "PAGE_SNAPSHOT bytes=%d preview_hex=%s", len(raw), betclicPreview(raw))
		page, err := parseBetclicCalendarPageWithContext(pageCtx, raw)
		if err != nil {
			betclicDebugf(pageCtx, "PAGE_PARSE_ERROR offset=%d error=%v", offset, err)
			betclicSaveParserFailure(pageCtx, raw)
			return betclicPage{}, fmt.Errorf("Betclic parser strony od %d: %w", offset, err)
		}
		page.Offset = offset
		if err := archiveResponse(pageCtx, "odczytane-mecze-strony", nil, "", page, nil); err != nil {
			return betclicPage{}, err
		}
		betclicDebugf(pageCtx, "PAGE_END offset=%d ids=%d football_matches=%d total=%d", offset, len(page.IDs), len(page.Events), page.Total)
		if len(page.IDs) == 0 && offset < page.Total {
			betclicDebugf(pageCtx, "PAGE_EMPTY_BEFORE_TOTAL offset=%d declared_total=%d", offset, page.Total)
			betclicSaveSnapshot(pageCtx, "betclic-last-empty-page.bin", raw)
		}
		if len(page.Events) > 0 {
			betclicDebugf(pageCtx, "PAGE_DATES first=%s last=%s", page.Events[0].StartTime, page.Events[len(page.Events)-1].StartTime)
		}
		return page, nil
	}
	first, err := loadPage(0)
	if err != nil {
		return nil, err
	}
	if first.Total > 5120 {
		return nil, fmt.Errorf("Betclic oferta przekracza limit 5120 meczów: %d", first.Total)
	}
	if first.Total > len(first.IDs) && len(first.IDs) == 0 {
		return nil, fmt.Errorf("Betclic pusta pierwsza strona niepełnej oferty")
	}
	pages := []betclicPage{first}
	pageSize := len(first.IDs)
	betclicDebugf(ctx, "PAGINATION page_size=%d total=%d parallel_readers=2", pageSize, first.Total)
	// Two readers bound source load and keep a calendar scan below the timeout.
	for offset := pageSize; pageSize > 0 && offset < first.Total; offset += 2 * pageSize {
		var results [2]betclicPage
		var errs [2]error
		count := 1
		if offset+pageSize < first.Total {
			count = 2
		}
		var wg sync.WaitGroup
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(i int) { defer wg.Done(); results[i], errs[i] = loadPage(offset + i*pageSize) }(i)
		}
		wg.Wait()
		for i := 0; i < count; i++ {
			if errs[i] != nil {
				return nil, fmt.Errorf("Betclic strona od %d: %w", offset+i*pageSize, errs[i])
			}
			pages = append(pages, results[i])
		}
	}
	byID := map[string]CalendarEvent{}
	allIDs := map[string]int{}
	records, duplicates := 0, 0
	var emptyOffsets []int
	changed := false
	for _, page := range pages {
		if page.Total != first.Total {
			changed = true
		}
		if len(page.IDs) == 0 {
			emptyOffsets = append(emptyOffsets, page.Offset)
		}
		records += len(page.IDs)
		for _, id := range page.IDs {
			if firstOffset, exists := allIDs[id]; exists {
				duplicates++
				betclicDebugf(ctx, "DUPLICATE_ID id=%s first_offset=%d repeated_offset=%d", id, firstOffset, page.Offset)
			} else {
				allIDs[id] = page.Offset
			}
		}
		for _, event := range page.Events {
			byID[event.ID] = event
		}
	}
	result := &Calendar{Events: []CalendarEvent{}, Warnings: []string{}}
	betclicDebugf(ctx, "CATALOG pages=%d records=%d duplicates=%d unique_ids=%d football_matches=%d without_two_teams=%d declared_total=%d total_changed=%t empty_offsets=%v", len(pages), records, duplicates, len(allIDs), len(byID), len(allIDs)-len(byID), first.Total, changed, emptyOffsets)
	if changed || len(allIDs) != first.Total {
		warning := fmt.Sprintf("Betclic zwrócił %d rekordów (%d unikalnych ID, %d powtórzeń), a początkowo deklarował %d wydarzeń.", records, len(allIDs), duplicates, first.Total)
		if len(emptyOffsets) > 0 {
			warning += fmt.Sprintf(" Puste strony od pozycji: %v.", emptyOffsets)
		}
		if changed {
			warning += " Licznik wydarzeń zmieniał się podczas pobierania."
		}
		warning += " Nie można potwierdzić kompletności kalendarza."
		result.Warnings = append(result.Warnings, warning)
	}
	for _, event := range byID {
		result.Events = append(result.Events, event)
	}
	sort.Slice(result.Events, func(i, j int) bool {
		left, _ := time.Parse(time.RFC3339, result.Events[i].StartTime)
		right, _ := time.Parse(time.RFC3339, result.Events[j].StartTime)
		if !left.Equal(right) {
			return left.Before(right)
		}
		return result.Events[i].ID < result.Events[j].ID
	})
	return result, nil
}

// Only known message levels are decoded: frame -> response[1] -> matches[3].
// No recursive string guessing, and no IDs from recommendations or markets.
func parseBetclicCalendarPage(raw []byte) (betclicPage, error) {
	return parseBetclicCalendarPageWithContext(context.Background(), raw)
}

func parseBetclicCalendarPageWithContext(ctx context.Context, raw []byte) (betclicPage, error) {
	frameIndex := 0
	for len(raw) > 0 {
		frameIndex++
		if len(raw) < 5 {
			return betclicPage{}, fmt.Errorf("Betclic niepełny nagłówek ramki")
		}
		size := int(binary.BigEndian.Uint32(raw[1:5]))
		if size > len(raw)-5 || raw[0] != 0 {
			return betclicPage{}, fmt.Errorf("Betclic niepoprawna ramka kalendarza")
		}
		fields, err := decodeProtoFields(raw[5 : 5+size])
		if err != nil {
			return betclicPage{}, fmt.Errorf("ramka %d, pola główne: %w", frameIndex, err)
		}
		betclicDebugf(ctx, "PROTO_ROOT frame=%d fields=%s", frameIndex, betclicProtoSummary(fields))
		raw = raw[5+size:]
		for _, root := range fields {
			if root.Number != 1 || root.Wire != 2 {
				continue
			}
			response, err := decodeProtoFields(root.Bytes)
			if err != nil {
				return betclicPage{}, fmt.Errorf("ramka %d, odpowiedź: %w", frameIndex, err)
			}
			betclicDebugf(ctx, "PROTO_RESPONSE frame=%d sport=%q field4=%q fields=%s", frameIndex, protoText(response, 1), protoText(response, 4), betclicProtoSummary(response))
			// Empty tail pages omit the sport label but retain the market code.
			if protoText(response, 1) == "" {
				betclicDebugf(ctx, "PROTO_SKIP_RESPONSE reason=empty_field_1 frame=%d", frameIndex)
				continue
			}
			page := betclicPage{Events: []CalendarEvent{}, Total: int(protoNumber(response, 5))}
			for _, record := range response {
				if record.Number != 3 || record.Wire != 2 {
					continue
				}
				match, err := decodeProtoFields(record.Bytes)
				if err != nil {
					return betclicPage{}, fmt.Errorf("rekord meczu %d: %w", len(page.IDs)+1, err)
				}
				id := protoNumber(match, 1)
				when, err := time.Parse(time.RFC3339, protoText(match, 3))
				if err != nil || id == 0 {
					betclicDebugf(ctx, "PROTO_INVALID_MATCH id=%d start=%q fields=%s date_error=%v", id, protoText(match, 3), betclicProtoSummary(match), err)
					return betclicPage{}, fmt.Errorf("Betclic niepoprawne ID lub termin meczu: id=%d start=%q", id, protoText(match, 3))
				}
				page.IDs = append(page.IDs, strconv.FormatUint(id, 10))
				teams := []string{}
				competition, country := "", ""
				for _, field := range match {
					if field.Wire != 2 {
						continue
					}
					if field.Number == 12 {
						team, err := decodeProtoFields(field.Bytes)
						if err != nil {
							return betclicPage{}, fmt.Errorf("mecz %d, drużyna: %w", id, err)
						}
						teams = append(teams, strings.TrimSpace(protoText(team, 3)))
					}
					if field.Number == 8 {
						league, err := decodeProtoFields(field.Bytes)
						if err != nil {
							return betclicPage{}, fmt.Errorf("mecz %d, liga: %w", id, err)
						}
						competition, country = protoText(league, 2), protoText(league, 5)
					}
				}
				// Outrights do not have two contestants; they are not football matches.
				if len(teams) != 2 {
					betclicDebugf(ctx, "PROTO_SKIP_MATCH id=%d reason=contestant_count contestants=%d", id, len(teams))
					continue
				}
				if teams[0] == "" || teams[1] == "" {
					return betclicPage{}, fmt.Errorf("Betclic brak nazwy drużyny: id=%d teams=%q", id, teams)
				}
				matchID := strconv.FormatUint(id, 10)
				page.Events = append(page.Events, CalendarEvent{ID: matchID, Home: teams[0], Away: teams[1],
					Competition: competition, Country: country, StartTime: when.UTC().Format(time.RFC3339),
					Live: protoNumber(match, 4) == 1,
					URL:  "https://www.betclic.pl/pilka-nozna-s1/" + url.PathEscape(teams[0]+"-"+teams[1]) + "-m" + matchID,
				})
			}
			return page, nil
		}
	}
	return betclicPage{}, fmt.Errorf("Betclic brak początkowego snapshotu kalendarza")
}

type protoField struct {
	Number int
	Wire   byte
	Uint   uint64
	Bytes  []byte
}

func decodeProtoFields(raw []byte) ([]protoField, error) {
	fields := []protoField{}
	for len(raw) > 0 {
		tag, n := binary.Uvarint(raw)
		if n <= 0 || tag>>3 == 0 || tag>>3 > 0x1fffffff {
			return nil, fmt.Errorf("Betclic niepoprawny tag Protobuf")
		}
		raw = raw[n:]
		field := protoField{Number: int(tag >> 3), Wire: byte(tag & 7)}
		switch field.Wire {
		case 0:
			value, n := binary.Uvarint(raw)
			if n <= 0 {
				return nil, fmt.Errorf("Betclic niepełny varint")
			}
			field.Uint = value
			raw = raw[n:]
		case 1, 5:
			size := 8
			if field.Wire == 5 {
				size = 4
			}
			if len(raw) < size {
				return nil, fmt.Errorf("Betclic niepełne pole fixed")
			}
			field.Bytes = raw[:size]
			raw = raw[size:]
		case 2:
			size, n := binary.Uvarint(raw)
			if n <= 0 || size > uint64(len(raw)-n) {
				return nil, fmt.Errorf("Betclic niepełne pole bytes")
			}
			raw = raw[n:]
			field.Bytes = raw[:int(size)]
			raw = raw[int(size):]
		default:
			return nil, fmt.Errorf("Betclic nieobsługiwany wire type %d", field.Wire)
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func protoText(fields []protoField, number int) string {
	for _, field := range fields {
		if field.Number == number && field.Wire == 2 {
			return string(field.Bytes)
		}
	}
	return ""
}

func protoNumber(fields []protoField, number int) uint64 {
	for _, field := range fields {
		if field.Number == number && field.Wire == 0 {
			return field.Uint
		}
	}
	return 0
}

// === calendar_cache.go ===
const CalendarCacheTTL = 2 * time.Hour
const calendarCacheVersion = 1

type calendarCacheEntry struct {
	FetchedAt time.Time `json:"fetchedAt"`
	Calendar  *Calendar `json:"calendar"`
}

type calendarFlight struct {
	done     chan struct{}
	calendar *Calendar
	err      error
}

// CalendarCache persists complete source responses and shares concurrent fetches.
// STS/Betclic keys cover every offered date; Superbet keys cover complete days.
type CalendarCache struct {
	path     string
	once     sync.Once
	mu       sync.Mutex
	entries  map[string]calendarCacheEntry
	inflight map[string]*calendarFlight
	active   sync.WaitGroup
	closing  bool
}

func NewCalendarCache(path string) *CalendarCache {
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	return &CalendarCache{path: path, entries: map[string]calendarCacheEntry{}, inflight: map[string]*calendarFlight{}}
}

var defaultCalendarCache = NewCalendarCache("bookmaker_calendar_cache.json")

func DefaultCalendarCache() *CalendarCache { return defaultCalendarCache }

func cloneCalendar(source *Calendar) *Calendar {
	result := *source
	result.Events = append([]CalendarEvent{}, source.Events...)
	result.Warnings = append([]string{}, source.Warnings...)
	return &result
}

func (cache *CalendarCache) readDisk() {
	file, err := os.Open(cache.path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[CALENDAR-CACHE] READ_ERROR path=%q error=%v", cache.path, err)
		}
		return
	}
	defer file.Close()
	var disk struct {
		Version int                           `json:"version"`
		Entries map[string]calendarCacheEntry `json:"entries"`
	}
	if err := json.NewDecoder(io.LimitReader(file, 64<<20)).Decode(&disk); err != nil {
		log.Printf("[CALENDAR-CACHE] READ_ERROR path=%q error=%v", cache.path, err)
		return
	}
	if disk.Version != calendarCacheVersion {
		log.Printf("[CALENDAR-CACHE] VERSION_MISS path=%q version=%d", cache.path, disk.Version)
		return
	}
	now := time.Now()
	for key, entry := range disk.Entries {
		if entry.Calendar != nil && !entry.FetchedAt.IsZero() && !entry.FetchedAt.After(now) {
			entry.Calendar.FetchedAt = entry.FetchedAt.Format(time.RFC3339Nano)
			cache.entries[key] = entry
		}
	}
	log.Printf("[CALENDAR-CACHE] DISK_LOADED path=%q entries=%d", cache.path, len(cache.entries))
}

// Caller cancellation stops waiting without aborting a fetch shared by others.
// Errors never replace the last successfully downloaded calendar.
func (cache *CalendarCache) Get(ctx context.Context, key string, force bool, load func(context.Context) (*Calendar, error)) (*Calendar, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cache.once.Do(cache.readDisk)
	cache.mu.Lock()
	if cache.closing {
		cache.mu.Unlock()
		return nil, context.Canceled
	}
	flight := cache.inflight[key]
	if flight == nil {
		if entry, exists := cache.entries[key]; exists && !force && time.Since(entry.FetchedAt) >= 0 && time.Since(entry.FetchedAt) < CalendarCacheTTL {
			result := cloneCalendar(entry.Calendar)
			cache.mu.Unlock()
			log.Printf("[CALENDAR-CACHE] HIT key=%q events=%d fetched_at=%s", key, len(result.Events), result.FetchedAt)
			return result, nil
		}
		flight = &calendarFlight{done: make(chan struct{})}
		cache.inflight[key] = flight
		cache.active.Add(1)
		log.Printf("[CALENDAR-CACHE] FETCH key=%q force=%t", key, force)
		go cache.fetch(context.WithoutCancel(ctx), key, flight, load)
	} else {
		log.Printf("[CALENDAR-CACHE] JOIN key=%q", key)
	}
	cache.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-flight.done:
		if flight.err != nil {
			return nil, flight.err
		}
		return cloneCalendar(flight.calendar), nil
	}
}

func (cache *CalendarCache) fetch(parent context.Context, key string, flight *calendarFlight, load func(context.Context) (*Calendar, error)) {
	defer cache.active.Done()
	ctx, cancel := context.WithTimeout(parent, 125*time.Second)
	defer cancel()
	calendar, err := load(ctx)
	if err == nil && calendar == nil {
		err = fmt.Errorf("pobieranie kalendarza nie zwróciło wyniku")
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if err == nil {
		now := time.Now()
		calendar = cloneCalendar(calendar)
		calendar.FetchedAt = now.Format(time.RFC3339Nano)
		for oldKey, entry := range cache.entries {
			if strings.HasPrefix(oldKey, "superbet:") && now.Sub(entry.FetchedAt) > 7*24*time.Hour {
				delete(cache.entries, oldKey)
			}
		}
		cache.entries[key] = calendarCacheEntry{FetchedAt: now, Calendar: calendar}
		if diskErr := cache.writeDisk(); diskErr != nil {
			log.Printf("[CALENDAR-CACHE] WRITE_ERROR path=%q error=%v", cache.path, diskErr)
			calendar.Warnings = append(calendar.Warnings, "Nie zapisano kalendarza na dysku serwera; cache działa do restartu serwera: "+diskErr.Error())
		}
		log.Printf("[CALENDAR-CACHE] STORED key=%q events=%d path=%q", key, len(calendar.Events), cache.path)
	} else {
		log.Printf("[CALENDAR-CACHE] FETCH_ERROR key=%q error=%v", key, err)
	}
	flight.calendar, flight.err = calendar, err
	delete(cache.inflight, key)
	close(flight.done)
}

// Called with cache.mu held; an atomic replacement preserves the old file if
// encoding or writing fails, and simultaneous bookmaker fetches cannot lose entries.
func (cache *CalendarCache) writeDisk() error {
	data, err := json.Marshal(struct {
		Version int                           `json:"version"`
		Entries map[string]calendarCacheEntry `json:"entries"`
	}{calendarCacheVersion, cache.entries})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cache.path), 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(cache.path), ".calendar-cache-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), cache.path)
}

func calendarDay(date string) (time.Time, time.Time, error) {
	zone, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	start, err := time.ParseInLocation("2006-01-02", date, zone)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("niepoprawna data kalendarza: %w", err)
	}
	return start, start.AddDate(0, 0, 1), nil
}

func FilterCalendar(source *Calendar, date string) (*Calendar, error) {
	start, end, err := calendarDay(date)
	if err != nil {
		return nil, err
	}
	result := cloneCalendar(source)
	result.Date = date
	result.Events = []CalendarEvent{}
	for _, event := range source.Events {
		when, err := time.Parse(time.RFC3339, event.StartTime)
		if err != nil || when.Before(start) || !when.Before(end) {
			continue
		}
		event.StartTime = when.In(start.Location()).Format(time.RFC3339)
		result.Events = append(result.Events, event)
	}
	return result, nil
}

func FetchCachedSTSCalendar(ctx context.Context, cache *CalendarCache, endpoint, date string, force bool) (*Calendar, error) {
	if _, _, err := calendarDay(date); err != nil {
		return nil, err
	}
	full, err := cache.Get(ctx, "sts:"+endpoint, force, func(ctx context.Context) (*Calendar, error) {
		raw, err := FetchSTSSnapshot(ctx, endpoint, "")
		if err != nil {
			return nil, err
		}
		events, err := ParseSTSFullCalendar(raw)
		if err != nil {
			return nil, err
		}
		return &Calendar{Events: events, Warnings: []string{}}, nil
	})
	if err != nil {
		return nil, err
	}
	return FilterCalendar(full, date)
}

func FetchCachedBetclicCalendar(ctx context.Context, cache *CalendarCache, client *http.Client, endpoint, date string, force bool) (*Calendar, error) {
	betclicDebugf(ctx, "CACHED_CALENDAR_REQUEST date=%q force=%t", date, force)
	if _, _, err := calendarDay(date); err != nil {
		return nil, err
	}
	full, err := cache.Get(ctx, "betclic:"+endpoint, force, func(ctx context.Context) (*Calendar, error) {
		return FetchBetclicFullCalendar(ctx, client, endpoint)
	})
	if err != nil {
		return nil, err
	}
	result, err := FilterCalendar(full, date)
	if err == nil {
		betclicDebugf(ctx, "DATE_FILTER_RESULT date=%q catalog_events=%d included=%d fetched_at=%q", date, len(full.Events), len(result.Events), full.FetchedAt)
	}
	return result, err
}

// === sts.go ===
const STSWebSocket = "wss://www.sts.pl/sbk/api/sbk"

type CalendarEvent struct {
	ID          string `json:"id"`
	Home        string `json:"home"`
	Away        string `json:"away"`
	Competition string `json:"competition"`
	Country     string `json:"country"`
	StartTime   string `json:"startTime"`
	URL         string `json:"url"`
	Live        bool   `json:"live"`
	BetBuilder  bool   `json:"betBuilder"`
}

// FetchSTSSnapshot subscribes to the existing STS endpoint. Empty fixtureID asks
// for i_pl; otherwise only the selected fixture's market snapshot is accepted.
func FetchSTSSnapshot(parent context.Context, endpoint, fixtureID string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	channel := "i_pl"
	subscriptions := []map[string]any{{"s": channel, "n": 0}}
	if fixtureID != "" {
		for _, c := range fixtureID {
			if c < '0' || c > '9' {
				return nil, fmt.Errorf("niepoprawne ID STS")
			}
		}
		channel = "f_f" + fixtureID + "_pl"
		subscriptions = append(subscriptions, map[string]any{"s": channel, "n": 0})
	}
	dialer := websocket.Dialer{HandshakeTimeout: 7 * time.Second}
	header := http.Header{}
	header.Set("Origin", "https://www.sts.pl")
	header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	conn, _, err := dialer.DialContext(ctx, endpoint, header)
	if err != nil {
		return nil, fmt.Errorf("STS WebSocket: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(32 << 20)
	deadline, _ := ctx.Deadline()
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return nil, err
	}
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	if err := conn.WriteJSON(map[string]any{"t": 1, "u": subscriptions}); err != nil {
		return nil, err
	}
	// The only writer after subscribing sends the same heartbeat as the desktop.
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if conn.WriteJSON(map[string]any{"t": 0}) != nil {
					return
				}
			}
		}
	}()
	pendingChannel := ""
	for {
		messageType, msg, err := conn.ReadMessage()
		if len(msg) > 0 || err != nil {
			info := archiveInfoFrom(ctx)
			info.Endpoint, info.Method = endpoint, "WEBSOCKET"
			archiveCtx := context.WithValue(ctx, archiveContextKey{}, info)
			if saveErr := archiveResponse(archiveCtx, "ramka-websocket-kanal-"+channel, msg, "ws.bin", map[string]any{"messageType": messageType, "values": readableJSONValues(msg)}, err); saveErr != nil {
				return nil, saveErr
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("STS odczyt snapshotu: %w", err)
		}
		// STS can send channel metadata and payload as two JSON values. Support
		// both a combined message and a separate metadata message.
		decoder := json.NewDecoder(bytes.NewReader(msg))
		for {
			var object map[string]json.RawMessage
			if err := decoder.Decode(&object); err != nil {
				break
			}
			if rawChannel, ok := object["s"]; ok {
				_ = json.Unmarshal(rawChannel, &pendingChannel)
				continue
			}
			if len(object) == 0 {
				continue
			}
			matches := false
			if fixtureID == "" {
				matches = (pendingChannel == "" || pendingChannel == channel) && hasSTSCalendar(object)
			} else {
				matches = (pendingChannel == channel && object["P"] != nil) || (pendingChannel == "" && hasSTSFixtureMarkets(object["P"], fixtureID))
			}
			pendingChannel = ""
			if matches {
				return json.Marshal(object)
			}
		}
	}
}

func hasSTSCalendar(object map[string]json.RawMessage) bool {
	var b struct {
		S map[string]json.RawMessage `json:"S"`
	}
	return json.Unmarshal(object["B"], &b) == nil && b.S["1"] != nil
}

func hasSTSFixtureMarkets(raw json.RawMessage, fixtureID string) bool {
	var p map[string]struct {
		Fixture string                     `json:"f"`
		Markets map[string]json.RawMessage `json:"m"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return false
	}
	for key, bundle := range p {
		if (key == "f"+fixtureID || bundle.Fixture == "f"+fixtureID) && len(bundle.Markets) > 0 {
			return true
		}
	}
	return false
}

func ParseSTSCalendar(raw []byte, date string) ([]CalendarEvent, error) {
	if date == "" {
		return nil, fmt.Errorf("niepoprawna data kalendarza: pusta data")
	}
	return parseSTSCalendar(raw, date)
}

func ParseSTSFullCalendar(raw []byte) ([]CalendarEvent, error) {
	return parseSTSCalendar(raw, "")
}

func parseSTSCalendar(raw []byte, date string) ([]CalendarEvent, error) {
	zone, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		return nil, err
	}
	var start, end time.Time
	if date != "" {
		start, err = time.ParseInLocation("2006-01-02", date, zone)
		if err != nil {
			return nil, fmt.Errorf("niepoprawna data kalendarza: %w", err)
		}
		end = start.AddDate(0, 0, 1)
	}
	type fixture struct {
		Home struct {
			Name string `json:"n"`
		} `json:"H"`
		Away struct {
			Name string `json:"n"`
		} `json:"A"`
		Type    string `json:"ft"`
		Start   string `json:"t"`
		Builder bool   `json:"Bb"`
	}
	var snapshot struct {
		B struct {
			Sports map[string]struct {
				Categories map[string]struct {
					Name        string `json:"n"`
					Tournaments map[string]struct {
						Name     string             `json:"n"`
						Fixtures map[string]fixture `json:"FX"`
					} `json:"T"`
				} `json:"C"`
			} `json:"S"`
		} `json:"B"`
		Live map[string]json.RawMessage `json:"Y"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	found := false
	for {
		var object json.RawMessage
		err := decoder.Decode(&object)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("STS niepoprawny snapshot: %w", err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(object, &fields); err != nil {
			return nil, err
		}
		if hasSTSCalendar(fields) {
			if err := json.Unmarshal(object, &snapshot); err != nil {
				return nil, err
			}
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("STS brak kalendarza piłki nożnej w snapshocie i_pl")
	}
	byID := make(map[string]CalendarEvent)
	for _, category := range snapshot.B.Sports["1"].Categories {
		for _, tournament := range category.Tournaments {
			for key, fixture := range tournament.Fixtures {
				id := strings.TrimPrefix(key, "f")
				if id == key || id == "" {
					continue
				}
				validID := true
				for _, digit := range id {
					if digit < '0' || digit > '9' {
						validID = false
					}
				}
				when, err := time.Parse(time.RFC3339, fixture.Start)
				home, away := strings.TrimSpace(fixture.Home.Name), strings.TrimSpace(fixture.Away.Name)
				if !validID || fixture.Type != "m" || err != nil || home == "" || away == "" {
					continue
				}
				if date != "" && (when.Before(start) || !when.Before(end)) {
					continue
				}
				byID[id] = CalendarEvent{ID: id, Home: home, Away: away,
					Competition: strings.TrimSpace(tournament.Name), Country: strings.TrimSpace(category.Name),
					StartTime: when.In(zone).Format(time.RFC3339), BetBuilder: fixture.Builder,
					Live: snapshot.Live[key] != nil,
					URL:  "https://www.sts.pl/kursy/pilka-nozna/" + key,
				}
			}
		}
	}
	events := make([]CalendarEvent, 0, len(byID))
	for _, event := range byID {
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool {
		left, _ := time.Parse(time.RFC3339, events[i].StartTime)
		right, _ := time.Parse(time.RFC3339, events[j].StartTime)
		if !left.Equal(right) {
			return left.Before(right)
		}
		return events[i].ID < events[j].ID
	})
	return events, nil
}

// === Superbet calendar collector ===
type SuperbetCalendarEvent = CalendarEvent
type SuperbetCalendar = Calendar

// Server runtime. Collectors below and above this section are intentionally kept
// together so the server needs just value_bets.go, go.mod and go.sum.
var strokeLetterReplacer = strings.NewReplacer(
	"ł", "l", "Ł", "L", "đ", "d", "Đ", "D", "ø", "o", "Ø", "O",
	"æ", "ae", "Æ", "AE", "œ", "oe", "Œ", "OE", "ß", "ss",
)

func removeDiacritics(s string) string {
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	result, _, _ := transform.String(t, s)
	return strokeLetterReplacer.Replace(result)
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if int64(len(raw)) > limit {
		return raw, fmt.Errorf("odpowiedź przekracza limit %d bajtów", limit)
	}
	return raw, err
}

type archiveContextKey struct{}
type archiveInfo struct {
	Directory         string    `json:"-"`
	AnalysisCreatedAt time.Time `json:"analysisCreatedAt,omitempty"`
	AnalysisID        string    `json:"analysisId,omitempty"`
	Match             string    `json:"match,omitempty"`
	Bookmaker         string    `json:"bookmaker"`
	Scope             string    `json:"scope"`
	Endpoint          string    `json:"endpoint,omitempty"`
	Method            string    `json:"method,omitempty"`
	RequestHex        string    `json:"requestProtobufHex,omitempty"`
	Status            int       `json:"httpStatus,omitempty"`
	ContentType       string    `json:"contentType,omitempty"`
}

var archiveRoot string
var archiveSequence atomic.Uint64

type oddsArchiveBufferKey struct{}
type oddsArchiveRecord struct {
	info            archiveInfo
	kind, extension string
	raw             []byte
	decoded         any
	err             error
}
type oddsArchiveBuffer struct {
	mu      sync.Mutex
	records []oddsArchiveRecord
}

func (buffer *oddsArchiveBuffer) flush(ctx context.Context, directory string) error {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	ctx = context.WithValue(ctx, oddsArchiveBufferKey{}, (*oddsArchiveBuffer)(nil))
	for _, record := range buffer.records {
		info := record.info
		info.Directory = directory
		if err := archiveResponse(context.WithValue(ctx, archiveContextKey{}, info), record.kind, record.raw, record.extension, record.decoded, record.err); err != nil {
			return err
		}
	}
	return nil
}

func archiveInfoFrom(ctx context.Context) archiveInfo {
	info, _ := ctx.Value(archiveContextKey{}).(archiveInfo)
	return info
}

// Raw bytes are preserved before parsing. The JSON companion describes the
// request, errors and decoded content; field-number trees do not invent a schema.
var errArchive = errors.New("błąd zapisu archiwum")

func isArchiveFailure(err error) bool { return errors.Is(err, errArchive) }

func archiveResponse(ctx context.Context, kind string, raw []byte, extension string, decoded any, fetchErr error) (err error) {
	// Match offers defer transport archival until their parsed content is known.
	// An unchanged offer needs only an observation, not another raw payload.
	if buffer, _ := ctx.Value(oddsArchiveBufferKey{}).(*oddsArchiveBuffer); buffer != nil {
		buffer.mu.Lock()
		buffer.records = append(buffer.records, oddsArchiveRecord{info: archiveInfoFrom(ctx), kind: kind, extension: extension, raw: append([]byte(nil), raw...), decoded: decoded, err: fetchErr})
		buffer.mu.Unlock()
		return nil
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %w", errArchive, err)
			log.Printf("ARCHIVE_WRITE_ERROR: %v", err)
		}
	}()
	info := archiveInfoFrom(ctx)
	if archiveRoot == "" || info.Bookmaker == "" {
		return fmt.Errorf("archiwum nie zostało skonfigurowane")
	}
	now := time.Now().UTC()
	dir := filepath.Join(archiveRoot, now.Format("2006-01-02"), strings.ToLower(info.Bookmaker))
	if info.AnalysisID != "" {
		day := now.Format("2006-01-02")
		if !info.AnalysisCreatedAt.IsZero() {
			day = info.AnalysisCreatedAt.UTC().Format("2006-01-02")
		}
		dir = filepath.Join(archiveRoot, day, "mecze", archiveName(info.Match)+"__"+info.AnalysisID, strings.ToLower(info.Bookmaker))
	}
	if info.Directory != "" {
		dir = info.Directory
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("archiwum: %w", err)
	}
	name := strings.ToLower(info.Bookmaker) + "_" + archiveName(info.Scope) + "_" + archiveName(kind) + "_" + now.Format("20060102T150405.000000000Z") + fmt.Sprintf("_%06d", archiveSequence.Add(1))
	rawFile := ""
	if raw != nil {
		rawFile = name + "." + extension + ".gz"
		if err := writeGzip(filepath.Join(dir, rawFile), raw); err != nil {
			return fmt.Errorf("archiwum surowej odpowiedzi: %w", err)
		}
	}
	envelope := struct {
		SchemaVersion int         `json:"schemaVersion"`
		RecordedAt    string      `json:"recordedAt"`
		Request       archiveInfo `json:"request"`
		Kind          string      `json:"kind"`
		RawFile       string      `json:"rawFile,omitempty"`
		RawBytes      int         `json:"rawBytes"`
		Error         string      `json:"error,omitempty"`
		Content       any         `json:"content"`
	}{SchemaVersion: 1, RecordedAt: now.Format(time.RFC3339Nano), Request: info, Kind: kind, RawFile: rawFile, RawBytes: len(raw), Content: decoded}
	if fetchErr != nil {
		envelope.Error = fetchErr.Error()
	}
	data, err := json.MarshalIndent(envelope, "", "  ")
	if err == nil {
		err = writeGzip(filepath.Join(dir, name+".json.gz"), append(data, '\n'))
	}
	if err != nil {
		return fmt.Errorf("archiwum JSON: %w", err)
	}
	return nil
}

func archiveName(s string) string {
	s = calendarTeamSlug(s)
	if len(s) > 80 {
		s = strings.TrimRight(s[:80], "-")
	}
	if s == "" {
		return "bez-opisu"
	}
	return s
}

func writeGzip(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".archive-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	writer := gzip.NewWriter(file)
	_, writeErr := writer.Write(data)
	err = errors.Join(writeErr, writer.Close(), file.Sync(), file.Close())
	if err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

func readableJSONValues(raw []byte) any {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	values := []any{}
	for {
		var value any
		err := decoder.Decode(&value)
		if err == io.EOF {
			return values
		}
		if err != nil {
			return map[string]any{"values": values, "decodeError": err.Error(), "rawStoredSeparately": true}
		}
		values = append(values, value)
	}
}

func readableGRPC(raw []byte) any {
	if len(raw) < 5 {
		return map[string]any{"partialHeaderHex": fmt.Sprintf("%x", raw)}
	}
	result := map[string]any{"flags": raw[0], "declaredPayloadBytes": binary.BigEndian.Uint32(raw[1:5]), "receivedPayloadBytes": len(raw) - 5}
	if raw[0] == 0x80 {
		result["trailers"] = string(raw[5:])
	} else if raw[0] == 0 {
		result["protobufFields"] = readableProto(raw[5:], 0)
	} else {
		result["unsupportedFlags"] = true
	}
	return result
}

func readableProto(raw []byte, depth int) any {
	fields, err := decodeProtoFields(raw)
	if err != nil {
		return map[string]any{"decodeError": err.Error(), "bytesHex": fmt.Sprintf("%x", raw)}
	}
	result := make([]map[string]any, 0, len(fields))
	for _, field := range fields {
		item := map[string]any{"field": field.Number, "wire": field.Wire}
		if field.Wire == 2 {
			item["bytes"] = len(field.Bytes)
			text := utf8.Valid(field.Bytes)
			for _, c := range string(field.Bytes) {
				if !unicode.IsPrint(c) && c != '\n' && c != '\r' && c != '\t' {
					text = false
					break
				}
			}
			if text {
				item["text"] = string(field.Bytes)
			} else if depth < 12 {
				item["possibleMessage"] = readableProto(field.Bytes, depth+1)
			} else {
				item["bytesHex"] = fmt.Sprintf("%x", field.Bytes)
			}
		} else if field.Wire == 0 {
			item["unsignedDecimal"] = strconv.FormatUint(field.Uint, 10)
		} else {
			item["bytesHex"] = fmt.Sprintf("%x", field.Bytes)
		}
		result = append(result, item)
	}
	return result
}

func (cache *CalendarCache) Peek(key string) *Calendar {
	cache.once.Do(cache.readDisk)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry, ok := cache.entries[key]; ok {
		return cloneCalendar(entry.Calendar)
	}
	return nil
}

func (cache *CalendarCache) stopAndWait(ctx context.Context) {
	cache.mu.Lock()
	cache.closing = true
	cache.mu.Unlock()
	done := make(chan struct{})
	go func() { cache.active.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		log.Printf("SHUTDOWN: przekroczono czas kończenia pobrań")
	}
}

// Once a future Superbet day has been requested, keep it refreshed on the same
// source schedule. Past days remain available as historical cache only.
func (cache *CalendarCache) superbetRefreshDates() []string {
	today, tomorrow := polishDates()
	days := map[string]bool{today: true, tomorrow: true}
	cache.once.Do(cache.readDisk)
	cache.mu.Lock()
	for key := range cache.entries {
		if strings.HasPrefix(key, "superbet:") {
			date := strings.TrimPrefix(key, "superbet:")
			if date >= today && validateCalendarQuery("SUPERBET", date) == nil {
				days[date] = true
			}
		}
	}
	cache.mu.Unlock()
	result := make([]string, 0, len(days))
	for date := range days {
		result = append(result, date)
	}
	sort.Strings(result)
	return result
}

type collectorSchedule struct {
	NextRun     time.Time `json:"nextRun"`
	LastSuccess time.Time `json:"lastSuccess,omitempty"`
	LastError   string    `json:"lastError,omitempty"`
}

type calendarServer struct {
	workMu           sync.Mutex
	foregroundWork   int
	backgroundCancel context.CancelFunc
	backgroundFlight chan struct{}
	backgroundDone   chan struct{}
	prefetchMu       sync.Mutex
	prefetch         statsBackgroundState
	oddsMu           sync.Mutex
	oddsCache        map[string]matchOddsSnapshot
	oddsFlights      map[string]*matchOddsFlight
	marketMu         sync.Mutex
	marketCache      map[string]MatchMarketAvailability
	marketFlights    map[string]*marketAvailabilityFlight
	marketSlots      chan struct{}
	analysisMu       sync.Mutex
	jobs             map[string]*analysisTask
	analysisQueue    chan *analysisTask
	analysisDone     chan struct{}
	cache            *CalendarCache
	client           *http.Client
	dataDir          string
	ctx              context.Context
	mu               sync.Mutex
	schedule         map[string]collectorSchedule
	running          map[string]bool
	slots            chan struct{}
}

var serverBookmakers = []string{"STS", "BETCLIC", "SUPERBET"}

// Idle downloads never share the network with foreground work. Foreground
// callers cancel a warmup and wait for its in-flight requests to return.
func (s *calendarServer) beginForeground(ctx context.Context) (func(), error) {
	s.workMu.Lock()
	s.foregroundWork++
	done := s.backgroundFlight
	if s.backgroundCancel != nil {
		s.backgroundCancel()
		if s.foregroundWork == 1 {
			log.Printf("[STATS-BACKGROUND] YIELD reason=foreground-download")
		}
	}
	s.workMu.Unlock()
	release := func() { s.workMu.Lock(); s.foregroundWork--; s.workMu.Unlock() }
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			release()
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func (s *calendarServer) beginBackground() (context.Context, func(), bool) {
	if !sofaCooldownUntil().IsZero() {
		return nil, nil, false
	}
	s.workMu.Lock()
	defer s.workMu.Unlock()
	if s.ctx.Err() != nil || s.foregroundWork != 0 || s.backgroundFlight != nil || len(s.analysisQueue) != 0 {
		return nil, nil, false
	}
	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Minute)
	ctx = context.WithValue(ctx, sofaBackgroundContextKey{}, true)
	done := make(chan struct{})
	s.backgroundCancel, s.backgroundFlight = cancel, done
	return ctx, func() {
		cancel()
		s.workMu.Lock()
		s.backgroundCancel, s.backgroundFlight = nil, nil
		close(done)
		s.workMu.Unlock()
	}, true
}

type statsFixture struct {
	ID          string    `json:"id"`
	HomeID      string    `json:"homeId"`
	AwayID      string    `json:"awayId"`
	StartTime   time.Time `json:"startTime"`
	ValidUntil  time.Time `json:"validUntil"`
	NextCheckAt time.Time `json:"nextCheckAt"`
	Finished    bool      `json:"finished"`
}

type statsWarmupState struct {
	Match             string    `json:"match"`
	StartTime         time.Time `json:"startTime"`
	CalendarFetchedAt time.Time `json:"calendarFetchedAt"`
	NextAttempt       time.Time `json:"nextAttempt"`
	Status            string    `json:"status"`
	Players           int       `json:"players"`
	Error             string    `json:"error,omitempty"`
}

type statsBackgroundState struct {
	Events   map[string]statsWarmupState `json:"events"`
	Fixtures map[string]statsFixture     `json:"fixtures"`
}

func (s *calendarServer) saveStatsBackgroundLocked() {
	for key, entry := range s.prefetch.Events {
		if entry.StartTime.Before(time.Now().Add(-7 * 24 * time.Hour)) {
			delete(s.prefetch.Events, key)
		}
	}
	for key, entry := range s.prefetch.Fixtures {
		if entry.StartTime.Before(time.Now().Add(-7 * 24 * time.Hour)) {
			delete(s.prefetch.Fixtures, key)
		}
	}
	if err := atomicJSON(filepath.Join(s.dataDir, "player_stats_background.json"), s.prefetch); err != nil {
		log.Printf("STATS_BACKGROUND_WRITE_ERROR: %v", err)
	}
}

func (s *calendarServer) rememberStatsFixture(fixture statsFixture) {
	s.prefetchMu.Lock()
	s.prefetch.Fixtures[fixture.ID] = fixture
	s.saveStatsBackgroundLocked()
	s.prefetchMu.Unlock()
	cacheLoadedOnce.Do(initPlayerCache)
	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()
	changed := false
	for key, entry := range playerStatsCache {
		if entry.FixtureID == fixture.ID && !entry.ValidUntil.Equal(fixture.ValidUntil) {
			entry.ValidUntil = fixture.ValidUntil
			playerStatsCache[key], changed = entry, true
		}
	}
	if changed {
		if err := atomicJSON(getCacheFilePath("sofa_player_stats_cache.json"), playerStatsCache); err != nil {
			log.Printf("STATS_CACHE_WRITE_ERROR: %v", err)
		}
	}
}

func (s *calendarServer) refreshStatsFixture(client *ResilientClient, fixture statsFixture) (statsFixture, error) {
	body, err := client.withScope("status-meczu-waznosc-statystyk-" + fixture.ID).GetSofa("/event/" + fixture.ID)
	if err != nil {
		return fixture, err
	}
	var response struct {
		Event struct {
			ID       int          `json:"id"`
			StartTS  int64        `json:"startTimestamp"`
			HomeTeam SofaTeamItem `json:"homeTeam"`
			AwayTeam SofaTeamItem `json:"awayTeam"`
			Status   struct {
				Code int    `json:"code"`
				Type string `json:"type"`
			} `json:"status"`
		} `json:"event"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return fixture, err
	}
	event := response.Event
	if strconv.Itoa(event.ID) != fixture.ID {
		return fixture, fmt.Errorf("brak właściwego wydarzenia w odpowiedzi statusu")
	}
	if event.StartTS > 0 {
		fixture.StartTime = time.Unix(event.StartTS, 0).UTC()
	}
	fixture.Finished = event.Status.Type == "finished" || event.Status.Type == "canceled" || event.Status.Code == 100
	fixture.ValidUntil = fixture.StartTime.Add(4 * time.Hour)
	if fixture.Finished {
		fixture.ValidUntil = time.Now().UTC()
		log.Printf("[STATS-CACHE] FIXTURE_ENDED event_id=%s; invalidating pre-match histories", fixture.ID)
	}
	fixture.NextCheckAt = time.Now().Add(5 * time.Minute)
	s.rememberStatsFixture(fixture)
	return fixture, nil
}

func (s *calendarServer) statsFixtureForMatch(client *ResilientClient, homeID, awayID string, expected time.Time) (statsFixture, bool, error) {
	s.prefetchMu.Lock()
	var cached statsFixture
	for _, fixture := range s.prefetch.Fixtures {
		if fixture.Finished || !time.Now().Before(fixture.ValidUntil) {
			continue
		}
		if !((fixture.HomeID == homeID && fixture.AwayID == awayID) || (fixture.HomeID == awayID && fixture.AwayID == homeID)) {
			continue
		}
		if !expected.IsZero() {
			if delta := fixture.StartTime.Sub(expected); delta < -time.Hour || delta > time.Hour {
				continue
			}
		}
		if cached.ID == "" || fixture.StartTime.Before(cached.StartTime) {
			cached = fixture
		}
	}
	s.prefetchMu.Unlock()
	if cached.ID != "" {
		if !time.Now().Before(cached.StartTime) && !time.Now().Before(cached.NextCheckAt) {
			var err error
			cached, err = s.refreshStatsFixture(client, cached)
			if err != nil {
				return statsFixture{}, false, err
			}
			if cached.Finished {
				return statsFixture{}, false, fmt.Errorf("mecz %s jest już zakończony", cached.ID)
			}
		}
		return cached, cached.HomeID != homeID, nil
	}
	var lastErr error
	for _, teamID := range []string{homeID, awayID} {
		body, err := client.withScope("terminarz-druzyny-" + teamID).GetSofa("/team/" + teamID + "/events/next/0")
		if err != nil {
			lastErr = err
			continue
		}
		var response SofaTeamEventsResponse
		if err := json.Unmarshal(body, &response); err != nil {
			lastErr = err
			continue
		}
		for _, event := range response.Events {
			h, a := strconv.Itoa(event.HomeTeam.ID), strconv.Itoa(event.AwayTeam.ID)
			if !((h == homeID && a == awayID) || (h == awayID && a == homeID)) || event.ID <= 0 || event.StartTS <= 0 || event.Status.Code >= 100 {
				continue
			}
			when := time.Unix(event.StartTS, 0).UTC()
			if when.Before(time.Now().Add(-2 * time.Hour)) {
				continue
			}
			if !expected.IsZero() {
				if delta := when.Sub(expected); delta < -time.Hour || delta > time.Hour {
					continue
				}
			}
			if cached.ID == "" || when.Before(cached.StartTime) {
				cached = statsFixture{ID: strconv.Itoa(event.ID), HomeID: h, AwayID: a, StartTime: when, ValidUntil: when.Add(4 * time.Hour), NextCheckAt: when}
			}
		}
		if cached.ID != "" {
			s.rememberStatsFixture(cached)
			log.Printf("[SOFA-EVENT] MATCH eventID=%s home_id=%s away_id=%s start=%s", cached.ID, cached.HomeID, cached.AwayID, cached.StartTime.Format(time.RFC3339))
			return cached, cached.HomeID != homeID, nil
		}
	}
	if lastErr != nil {
		return statsFixture{}, false, lastErr
	}
	return statsFixture{}, false, fmt.Errorf("nie znaleziono nadchodzącego meczu %s vs %s", homeID, awayID)
}

type statsWarmupCandidate struct {
	Bookmaker    string
	Event        CalendarEvent
	StartTime    time.Time
	FetchedAt    time.Time
	KnownMarkets bool
}

func (s *calendarServer) nextStatsWarmup() (statsWarmupCandidate, bool) {
	s.marketMu.Lock()
	available := map[string]bool{}
	for key, result := range s.marketCache {
		available[key] = result.Status == "available"
	}
	s.marketMu.Unlock()
	s.cache.once.Do(s.cache.readDisk)
	s.cache.mu.Lock()
	unique := map[string]statsWarmupCandidate{}
	for key, entry := range s.cache.entries {
		book := ""
		for _, name := range serverBookmakers {
			if strings.HasPrefix(key, strings.ToLower(name)+":") {
				book = name
				break
			}
		}
		if book == "" || entry.Calendar == nil {
			continue
		}
		for _, event := range entry.Calendar.Events {
			start, err := time.Parse(time.RFC3339, event.StartTime)
			if err != nil || event.Live || !start.After(time.Now()) || start.After(time.Now().Add(24*time.Hour)) || event.URL == "" {
				continue
			}
			id := book + ":" + event.ID
			if previous, ok := unique[id]; ok && !entry.FetchedAt.After(previous.FetchedAt) {
				continue
			}
			unique[id] = statsWarmupCandidate{Bookmaker: book, Event: event, StartTime: start, FetchedAt: entry.FetchedAt, KnownMarkets: available[id]}
		}
	}
	s.cache.mu.Unlock()
	s.prefetchMu.Lock()
	var candidates []statsWarmupCandidate
	for key, candidate := range unique {
		state, exists := s.prefetch.Events[key]
		if exists && time.Now().Before(state.NextAttempt) {
			continue
		}
		if exists && !candidate.FetchedAt.After(state.CalendarFetchedAt) && (state.Status == "completed" || state.Status == "unavailable") {
			continue
		}
		candidates = append(candidates, candidate)
	}
	s.prefetchMu.Unlock()
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].KnownMarkets != candidates[j].KnownMarkets {
			return candidates[i].KnownMarkets
		}
		if !candidates[i].StartTime.Equal(candidates[j].StartTime) {
			return candidates[i].StartTime.Before(candidates[j].StartTime)
		}
		return candidates[i].Bookmaker+":"+candidates[i].Event.ID < candidates[j].Bookmaker+":"+candidates[j].Event.ID
	})
	if len(candidates) == 0 {
		return statsWarmupCandidate{}, false
	}
	return candidates[0], true
}

func (s *calendarServer) warmupPlayerStats(ctx context.Context, candidate statsWarmupCandidate) (int, string, error) {
	event, book := candidate.Event, candidate.Bookmaker
	ctx = context.WithValue(ctx, archiveContextKey{}, archiveInfo{Bookmaker: book, Match: event.Home + " vs " + event.Away, Scope: "tlo-statystyki-" + event.Home + "-vs-" + event.Away + "-mecz-" + event.ID})
	odds, err := s.matchOdds(ctx, book, event.ID)
	props := odds.Props
	if ctx.Err() != nil {
		return 0, "paused", ctx.Err()
	}
	availability := marketAvailabilityFromOdds(book, event.ID, odds, err)
	s.rememberMarketAvailability(availability)
	if saveErr := archiveResponse(ctx, "oferta-do-pobrania-statystyk-w-tle", nil, "", map[string]any{"event": event, "availability": availability, "archiveVersion": odds.ArchiveVersion, "propsCount": len(props), "oddsObservedAt": odds.ObservedAt, "oddsExpiresAt": odds.ExpiresAt, "reusedFromCache": odds.FromCache}, err); saveErr != nil {
		return 0, "error", errors.Join(err, saveErr)
	}
	if err != nil {
		return 0, "error", err
	}
	if len(props) == 0 {
		return 0, "unavailable", nil
	}
	client, err := newSofaClient(ctx, func(message string) { log.Printf("[STATS-BACKGROUND] WARNING %s", message) }, nil)
	if err != nil {
		return 0, "error", err
	}
	client.statsPrefetch = true
	defer client.tlsClient.CloseIdleConnections()
	match := s.sofaContextForURL(event.URL)
	match.StartTime, _ = time.Parse(time.RFC3339, event.StartTime)
	home, away := calendarTeamSlug(event.Home), calendarTeamSlug(event.Away)
	teams := detectTeamsBySlugs(client, home, away, match)
	if teams.HomeID == "" || teams.AwayID == "" {
		return 0, "error", fmt.Errorf("rozpoznanie drużyn: %s", strings.Join(teams.Warnings, "; "))
	}
	fixture, _, err := s.statsFixtureForMatch(client, teams.HomeID, teams.AwayID, match.StartTime)
	if err != nil {
		return 0, "error", err
	}
	client.statsFixture = fixture
	targets, seen := []string{}, map[string]bool{}
	for _, prop := range props {
		if !seen[prop.PlayerName] {
			seen[prop.PlayerName] = true
			targets = append(targets, prop.PlayerName)
		}
	}
	players := []*PlayerStatHistory{}
	for _, id := range []string{teams.HomeID, teams.AwayID} {
		histories, _, err := fetchSofaTeamDataComprehensive(client, id, 10, "ALL", targets)
		players = append(players, histories...)
		if err != nil {
			return len(players), "partial", err
		}
	}
	if err := errors.Join(ctx.Err(), client.archiveError()); err != nil {
		return len(players), "error", err
	}
	if len(players) == 0 {
		return 0, "error", fmt.Errorf("brak historii zawodników dla obsługiwanych rynków")
	}
	for _, player := range players {
		if player.Incomplete {
			return len(players), "partial", fmt.Errorf("część historii jest niepełna; ponowienie w czasie bezczynności")
		}
	}
	if err := archiveResponse(ctx, "statystyki-w-tle-gotowe-do-analizy", nil, "", map[string]any{"fixture": fixture, "bookmakerEvent": event, "matchLimit": 10, "tournamentIds": "ALL", "players": players}, nil); err != nil {
		return len(players), "error", err
	}
	return len(players), "completed", nil
}

func (s *calendarServer) statsBackgroundStep(ctx context.Context) bool {
	// Status checks expire pre-match statistics after the game actually ends.
	s.prefetchMu.Lock()
	var due statsFixture
	for _, fixture := range s.prefetch.Fixtures {
		if !fixture.Finished && !time.Now().Before(fixture.StartTime) && !time.Now().Before(fixture.NextCheckAt) && time.Now().Before(fixture.StartTime.Add(24*time.Hour)) {
			if due.ID == "" || fixture.NextCheckAt.Before(due.NextCheckAt) {
				due = fixture
			}
		}
	}
	s.prefetchMu.Unlock()
	if due.ID != "" {
		client, err := newSofaClient(ctx, nil, nil)
		if err == nil {
			_, err = s.refreshStatsFixture(client, due)
			client.tlsClient.CloseIdleConnections()
		}
		if err != nil && ctx.Err() == nil {
			log.Printf("[STATS-BACKGROUND] STATUS_ERROR event_id=%s error=%v", due.ID, err)
			due.NextCheckAt = time.Now().Add(5 * time.Minute)
			if until := sofaCooldownUntil(); until.After(due.NextCheckAt) {
				due.NextCheckAt = until
			}
			s.rememberStatsFixture(due)
		}
		return ctx.Err() == nil
	}
	candidate, ok := s.nextStatsWarmup()
	if !ok {
		return false
	}
	key := candidate.Bookmaker + ":" + candidate.Event.ID
	log.Printf("[STATS-BACKGROUND] START bookmaker=%s event_id=%s match=%q", candidate.Bookmaker, candidate.Event.ID, candidate.Event.Home+" vs "+candidate.Event.Away)
	players, status, err := s.warmupPlayerStats(ctx, candidate)
	if ctx.Err() != nil {
		status = "paused"
	}
	start, _ := time.Parse(time.RFC3339, candidate.Event.StartTime)
	state := statsWarmupState{Match: candidate.Event.Home + " vs " + candidate.Event.Away, StartTime: start, CalendarFetchedAt: candidate.FetchedAt, Status: status, Players: players}
	if err != nil {
		state.Error = err.Error()
	}
	switch status {
	case "paused":
		state.NextAttempt = time.Now().Add(15 * time.Second)
	case "partial", "error":
		state.NextAttempt = time.Now().Add(15 * time.Minute)
	}
	if until := sofaCooldownUntil(); until.After(state.NextAttempt) {
		state.NextAttempt = until
	}
	s.prefetchMu.Lock()
	s.prefetch.Events[key] = state
	s.saveStatsBackgroundLocked()
	s.prefetchMu.Unlock()
	log.Printf("[STATS-BACKGROUND] END bookmaker=%s event_id=%s status=%s players=%d error=%v", candidate.Bookmaker, candidate.Event.ID, status, players, err)
	return ctx.Err() == nil
}

func (s *calendarServer) statsBackgroundWorker() {
	defer close(s.backgroundDone)
	log.Printf("[STATS-BACKGROUND] PACING requests=1 delay=2s-4s between_matches=30s-60s")
	for {
		delay := 15 * time.Second
		ctx, finish, ok := s.beginBackground()
		if ok {
			didWork := false
			func() {
				defer finish()
				defer func() {
					if recovered := recover(); recovered != nil {
						log.Printf("[STATS-BACKGROUND] INTERNAL_ERROR error=%v", recovered)
					}
				}()
				didWork = s.statsBackgroundStep(ctx)
			}()
			if didWork {
				delay = 30*time.Second + time.Duration(rand.Int64N(int64(30*time.Second)+1))
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func sourceKey(bookmaker, date string) string {
	switch bookmaker {
	case "STS":
		return "sts:" + STSWebSocket
	case "BETCLIC":
		return "betclic:" + BetclicMatchService + "GetMatchesBySportWithNotifications"
	default:
		return "superbet:" + date
	}
}

func polishDates() (string, string) {
	zone, _ := time.LoadLocation("Europe/Warsaw")
	today := time.Now().In(zone)
	return today.Format("2006-01-02"), today.AddDate(0, 0, 1).Format("2006-01-02")
}

func calendarFetchedAt(calendar *Calendar) time.Time {
	if calendar == nil {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339Nano, calendar.FetchedAt)
	return t
}

func refreshInterval() time.Duration {
	return 2*time.Hour + time.Duration(rand.Int64N(int64(20*time.Minute)+1)) - 10*time.Minute
}

func newCalendarServer(ctx context.Context, dir string) (*calendarServer, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	s := &calendarServer{cache: NewCalendarCache(filepath.Join(dir, "bookmaker_calendar_cache.json")), client: &http.Client{Timeout: 30 * time.Second}, dataDir: dir, ctx: ctx, schedule: map[string]collectorSchedule{}, running: map[string]bool{}, slots: make(chan struct{}, 3)}
	defaultCalendarCache = s.cache
	serverDataDir = dir
	loadSofaBackoff()
	s.oddsCache = map[string]matchOddsSnapshot{}
	s.oddsFlights = map[string]*matchOddsFlight{}
	if raw, err := os.ReadFile(filepath.Join(dir, "bookmaker_match_odds_cache.json")); err == nil {
		if err := json.Unmarshal(raw, &s.oddsCache); err != nil {
			log.Printf("ODDS_CACHE_READ_ERROR: %v", err)
			s.oddsCache = map[string]matchOddsSnapshot{}
		}
	} else if !os.IsNotExist(err) {
		log.Printf("ODDS_CACHE_READ_ERROR: %v", err)
	}
	if s.oddsCache == nil {
		s.oddsCache = map[string]matchOddsSnapshot{}
	}
	for key, snapshot := range s.oddsCache {
		if key != snapshot.Bookmaker+":"+snapshot.EventID || !snapshot.retain(time.Now()) {
			delete(s.oddsCache, key)
			continue
		}
		props := usableMatchProps(snapshot.Props)
		if len(props) != len(snapshot.Props) {
			delete(s.oddsCache, key)
			continue
		}
		snapshot.Props = props
		s.oddsCache[key] = snapshot
	}
	s.marketCache = map[string]MatchMarketAvailability{}
	s.marketFlights = map[string]*marketAvailabilityFlight{}
	s.marketSlots = make(chan struct{}, 3)
	if raw, err := os.ReadFile(filepath.Join(dir, "market_availability_cache.json")); err == nil {
		if err := json.Unmarshal(raw, &s.marketCache); err != nil {
			log.Printf("MARKET_CACHE_READ_ERROR: %v", err)
			s.marketCache = map[string]MatchMarketAvailability{}
		}
	} else if !os.IsNotExist(err) {
		log.Printf("MARKET_CACHE_READ_ERROR: %v", err)
	}
	if s.marketCache == nil {
		s.marketCache = map[string]MatchMarketAvailability{}
	}
	s.jobs = map[string]*analysisTask{}
	s.analysisQueue = make(chan *analysisTask, 4)
	s.analysisDone = make(chan struct{})
	s.backgroundDone = make(chan struct{})
	s.prefetch = statsBackgroundState{Events: map[string]statsWarmupState{}, Fixtures: map[string]statsFixture{}}
	if raw, err := os.ReadFile(filepath.Join(dir, "player_stats_background.json")); err == nil {
		if err := json.Unmarshal(raw, &s.prefetch); err != nil {
			log.Printf("STATS_BACKGROUND_READ_ERROR: %v", err)
		}
	} else if !os.IsNotExist(err) {
		log.Printf("STATS_BACKGROUND_READ_ERROR: %v", err)
	}
	if s.prefetch.Events == nil {
		s.prefetch.Events = map[string]statsWarmupState{}
	}
	if s.prefetch.Fixtures == nil {
		s.prefetch.Fixtures = map[string]statsFixture{}
	}
	if err := os.MkdirAll(filepath.Join(dir, "analysis-jobs"), 0750); err != nil {
		return nil, err
	}
	archiveRoot = filepath.Join(dir, "archive")
	if err := os.MkdirAll(archiveRoot, 0750); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(dir, "calendar_schedule.json"))
	if err == nil {
		if err := json.Unmarshal(raw, &s.schedule); err != nil {
			log.Printf("SCHEDULE_READ_ERROR: %v", err)
		}
	} else if !os.IsNotExist(err) {
		log.Printf("SCHEDULE_READ_ERROR: %v", err)
	}
	if s.schedule == nil {
		s.schedule = map[string]collectorSchedule{}
	}
	today, tomorrow := polishDates()
	for _, book := range serverBookmakers {
		entry := s.schedule[book]
		calendar := s.cache.Peek(sourceKey(book, today))
		if book == "SUPERBET" {
			other := s.cache.Peek(sourceKey(book, tomorrow))
			if other == nil || calendarFetchedAt(other).Before(calendarFetchedAt(calendar)) {
				calendar = other
			}
		}
		if calendar == nil {
			entry.NextRun = time.Now()
		} else if entry.NextRun.IsZero() {
			entry.LastSuccess = calendarFetchedAt(calendar)
			entry.NextRun = entry.LastSuccess.Add(refreshInterval())
		}
		s.schedule[book] = entry
	}
	if err := s.saveScheduleLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

func atomicJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".state-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(append(data, '\n'))
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

// The caller holds s.mu, or initializes the service before starting goroutines.
func (s *calendarServer) saveScheduleLocked() error {
	return atomicJSON(filepath.Join(s.dataDir, "calendar_schedule.json"), s.schedule)
}

func (s *calendarServer) recordRun(book string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.schedule[book]
	if err == nil {
		entry.LastSuccess, entry.LastError = time.Now(), ""
		entry.NextRun = time.Now().Add(refreshInterval())
	} else {
		entry.LastError = err.Error()
		entry.NextRun = time.Now().Add(5*time.Minute + time.Duration(rand.Int64N(int64(time.Minute))) - 30*time.Second)
	}
	s.schedule[book] = entry
	if saveErr := s.saveScheduleLocked(); saveErr != nil {
		log.Printf("SCHEDULE_WRITE_ERROR: %v", saveErr)
	}
	log.Printf("CALENDAR_RUN bookmaker=%s next=%s error=%v", book, entry.NextRun.Format(time.RFC3339), err)
}

func normalizeCalendarLinks(calendar *Calendar) {
	for i := range calendar.Events {
		event := &calendar.Events[i]
		slug := calendarTeamSlug(event.Home) + "-vs-" + calendarTeamSlug(event.Away)
		if strings.Contains(event.URL, "www.sts.pl") {
			event.URL = "https://www.sts.pl/kursy/pilka-nozna/" + slug + "/f" + event.ID
		}
		if strings.Contains(event.URL, "www.betclic.pl") {
			event.URL = "https://www.betclic.pl/pilka-nozna-s1/" + slug + "-m" + event.ID
		}
	}
}

func (s *calendarServer) collect(ctx context.Context, book, date string, force bool) (*Calendar, error) {
	key := sourceKey(book, date)
	// Coalesce concurrent requests in cache.Get; repeated refresh clicks within
	// 30 seconds reuse the most recent snapshot rather than re-download it.
	if previous := s.cache.Peek(key); force && previous != nil && time.Since(calendarFetchedAt(previous)) < 30*time.Second {
		return FilterCalendar(previous, date)
	}
	result, err := s.cache.Get(ctx, key, force, func(fetchCtx context.Context) (calendar *Calendar, fetchErr error) {
		release, err := s.beginForeground(fetchCtx)
		if err != nil {
			return nil, err
		}
		defer release()
		select {
		case s.slots <- struct{}{}:
		case <-fetchCtx.Done():
			return nil, fetchCtx.Err()
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		}
		defer func() { <-s.slots }()
		fetchCtx, cancel := context.WithCancel(fetchCtx)
		stop := context.AfterFunc(s.ctx, cancel)
		defer cancel()
		defer stop()
		scope := "kalendarz-pilka-nozna-wszystkie-daty"
		if book == "SUPERBET" {
			scope = "kalendarz-pilka-nozna-dzien-" + date
		}
		fetchCtx = context.WithValue(fetchCtx, archiveContextKey{}, archiveInfo{Bookmaker: book, Scope: scope})
		defer func() {
			if fetchErr != nil {
				if err := archiveResponse(fetchCtx, "blad-pobierania-kalendarza", nil, "", nil, fetchErr); err != nil {
					fetchErr = errors.Join(fetchErr, err)
				}
			}
		}()
		switch book {
		case "STS":
			var raw []byte
			raw, fetchErr = FetchSTSSnapshot(fetchCtx, STSWebSocket, "")
			if fetchErr == nil {
				var events []CalendarEvent
				events, fetchErr = ParseSTSFullCalendar(raw)
				calendar = &Calendar{Events: events, Warnings: []string{}}
			}
		case "BETCLIC":
			calendar, fetchErr = FetchBetclicFullCalendar(fetchCtx, s.client, BetclicMatchService+"GetMatchesBySportWithNotifications")
		case "SUPERBET":
			calendar, fetchErr = loadSuperbetCalendarContext(fetchCtx, s.client, date, "https://production-superbet-offer-pl.freetls.fastly.net", "https://production-superbet-bmb.freetls.fastly.net")
		default:
			return nil, fmt.Errorf("nieobsługiwany bukmacher: %s", book)
		}
		if fetchErr != nil {
			return nil, fetchErr
		}
		normalizeCalendarLinks(calendar)
		calendar.FetchedAt = time.Now().UTC().Format(time.RFC3339Nano)
		fetchErr = archiveResponse(fetchCtx, fmt.Sprintf("odczytane-mecze-liczba-%d", len(calendar.Events)), nil, "", calendar, nil)
		return calendar, fetchErr
	})
	if err != nil {
		return nil, err
	}
	return FilterCalendar(result, date)
}

func (s *calendarServer) scheduler() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		for _, book := range serverBookmakers {
			if s.running[book] || time.Now().Before(s.schedule[book].NextRun) {
				continue
			}
			s.running[book] = true
			go func(book string) {
				today, _ := polishDates()
				dates := []string{today}
				if book == "SUPERBET" {
					dates = s.cache.superbetRefreshDates()
				}
				var err error
				for _, date := range dates {
					if s.ctx.Err() != nil {
						return
					}
					_, dayErr := s.collect(s.ctx, book, date, true)
					err = errors.Join(err, dayErr)
				}
				s.recordRun(book, err)
				s.mu.Lock()
				s.running[book] = false
				s.mu.Unlock()
			}(book)
		}
		s.mu.Unlock()
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func jsonResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("API_WRITE_ERROR: %v", err)
	}
}

func apiError(w http.ResponseWriter, status int, err error) {
	jsonResponse(w, status, map[string]string{"error": err.Error()})
}

func (s *calendarServer) handler() http.Handler {
	mux := http.NewServeMux()
	s.registerAnalysisAPI(mux)
	s.registerMarketAvailabilityAPI(mux)
	s.registerMatchFlowAPI(mux)
	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			apiError(w, 405, fmt.Errorf("wymagana metoda GET"))
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		jsonResponse(w, 200, map[string]any{"status": "ok", "apiVersion": 1, "phase": "match-analysis", "collectors": s.schedule, "running": s.running})
	})
	mux.HandleFunc("/api/v1/calendars", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			apiError(w, 405, fmt.Errorf("wymagana metoda GET"))
			return
		}
		book, date := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("bookmaker"))), r.URL.Query().Get("date")
		if err := validateCalendarQuery(book, date); err != nil {
			apiError(w, 400, err)
			return
		}
		calendar := s.cache.Peek(sourceKey(book, date))
		if calendar == nil {
			var err error
			calendar, err = s.collect(r.Context(), book, date, false)
			if err != nil {
				apiError(w, 502, err)
				return
			}
		}
		filtered, err := FilterCalendar(calendar, date)
		if err != nil {
			apiError(w, 400, err)
			return
		}
		s.mu.Lock()
		state := s.schedule[book]
		s.mu.Unlock()
		if time.Since(calendarFetchedAt(calendar)) > 130*time.Minute || state.LastError != "" {
			filtered.Warnings = append(filtered.Warnings, "Dane z cache serwera, pobrane: "+calendar.FetchedAt+". Oczekiwanie na odświeżenie.")
		}
		if state.LastError != "" {
			filtered.Warnings = append(filtered.Warnings, "Ostatnie odświeżenie: "+state.LastError)
		}
		jsonResponse(w, 200, filtered)
	})
	mux.HandleFunc("/api/v1/calendars/refresh", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			apiError(w, 405, fmt.Errorf("wymagana metoda POST"))
			return
		}
		var query struct {
			Bookmaker string `json:"bookmaker"`
			Date      string `json:"date"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&query); err != nil {
			apiError(w, 400, err)
			return
		}
		query.Bookmaker = strings.ToUpper(strings.TrimSpace(query.Bookmaker))
		if err := validateCalendarQuery(query.Bookmaker, query.Date); err != nil {
			apiError(w, 400, err)
			return
		}
		calendar, err := s.collect(r.Context(), query.Bookmaker, query.Date, true)
		if query.Bookmaker != "SUPERBET" {
			s.recordRun(query.Bookmaker, err)
		}
		if err != nil {
			apiError(w, 502, err)
			return
		}
		jsonResponse(w, 200, calendar)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Polling, health checks and cached calendars do not stop idle work.
		switch r.URL.Path {
		case "/api/v1/teams/detect", "/api/v1/tournaments", "/api/v1/markets/check", "/api/v1/analyses":
			if r.URL.Path != "/api/v1/analyses" || r.Method == http.MethodPost {
				release, err := s.beginForeground(r.Context())
				if err != nil {
					apiError(w, 503, err)
					return
				}
				defer release()
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func validateCalendarQuery(book, date string) error {
	if book != "STS" && book != "BETCLIC" && book != "SUPERBET" {
		return fmt.Errorf("nieobsługiwany bukmacher: %s", book)
	}
	start, _, err := calendarDay(date)
	if err != nil {
		return err
	}
	// Avoid an unbounded day cache caused by accidental or malicious dates.
	today, _ := polishDates()
	current, _, _ := calendarDay(today)
	if start.Before(current.AddDate(0, 0, -7)) || start.After(current.AddDate(0, 0, 60)) {
		return fmt.Errorf("data musi mieścić się między 7 dni temu a 60 dni naprzód")
	}
	return nil
}

func main() {
	listen := flag.String("listen", "127.0.0.1:8090", "adres nasłuchu HTTP API (lokalnie dla Cloudflare Tunnel)")
	dataDir := flag.String("data", "data", "katalog cache, harmonogramu i archiwum")
	sofaDiagnose := flag.Bool("sofa-diagnose", false, "porównaj połączenia SofaScore i zakończ bez uruchamiania API")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *sofaDiagnose {
		if err := runSofaDiagnostics(ctx, *dataDir); err != nil {
			log.Fatal(err)
		}
		return
	}
	s, err := newCalendarServer(ctx, *dataDir)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Addr: *listen, Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 180 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	go s.scheduler()
	go s.analysisWorker()
	go s.statsBackgroundWorker()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("VALUE_BETS_START listen=%s data=%s calendars=STS,BETCLIC,SUPERBET", *listen, *dataDir)
	err = server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	stop()
	<-shutdownDone
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.cache.stopAndWait(shutdownCtx)
	select {
	case <-s.analysisDone:
	case <-shutdownCtx.Done():
		log.Printf("ANALYSIS_SHUTDOWN_TIMEOUT")
	}
	select {
	case <-s.backgroundDone:
	case <-shutdownCtx.Done():
		log.Printf("STATS_BACKGROUND_SHUTDOWN_TIMEOUT")
	}
}

type superbetEventsResponse struct {
	Events []struct {
		ID      int `json:"event_id"`
		Fixture struct {
			Name         string `json:"event_name"`
			Date         string `json:"utc_date"`
			SportID      int    `json:"sport_id"`
			TournamentID int    `json:"tournament_id"`
			CategoryID   int    `json:"category_id"`
		} `json:"fixture"`
	} `json:"events"`
}

type superbetNamedItem struct {
	ID    string            `json:"id"`
	Names map[string]string `json:"localNames"`
}

func superbetCalendarDay(date string) (time.Time, time.Time, error) {
	zone, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	start, err := time.ParseInLocation("2006-01-02", date, zone)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("niepoprawna data kalendarza: %s", date)
	}
	return start, start.AddDate(0, 0, 1), nil
}

func getSuperbetCalendarJSON(ctx context.Context, client *http.Client, endpoint string, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0")
	resp, err := client.Do(req)
	if err != nil {
		info := archiveInfoFrom(ctx)
		info.Endpoint, info.Method = endpoint, http.MethodGet
		saveErr := archiveResponse(context.WithValue(ctx, archiveContextKey{}, info), "blad-polaczenia", nil, "", nil, err)
		return errors.Join(err, saveErr)
	}
	defer resp.Body.Close()
	raw, readErr := readBounded(resp.Body, 32<<20)
	info := archiveInfoFrom(ctx)
	info.Endpoint, info.Method, info.Status, info.ContentType = endpoint, http.MethodGet, resp.StatusCode, resp.Header.Get("Content-Type")
	u, _ := url.Parse(endpoint)
	if saveErr := archiveResponse(context.WithValue(ctx, archiveContextKey{}, info), "odpowiedz-"+calendarTeamSlug(u.Path)+"-"+calendarTeamSlug(u.RawQuery), raw, "http.bin", readableJSONValues(raw), readErr); saveErr != nil {
		return saveErr
	}
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Superbet HTTP %d", resp.StatusCode)
	}
	return json.Unmarshal(raw, result)
}

var calendarSlugPattern = regexp.MustCompile(`[^a-z0-9]+`)

func calendarTeamSlug(name string) string {
	return strings.Trim(calendarSlugPattern.ReplaceAllString(removeDiacritics(strings.ToLower(name)), "-"), "-")
}

func loadSuperbetCalendar(client *http.Client, date, offerBase, builderBase string) (*SuperbetCalendar, error) {
	return loadSuperbetCalendarContext(context.Background(), client, date, offerBase, builderBase)
}

func loadSuperbetCalendarContext(parent context.Context, client *http.Client, date, offerBase, builderBase string) (*SuperbetCalendar, error) {
	start, end, err := superbetCalendarDay(date)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 25*time.Second)
	defer cancel()
	var prematch, live superbetEventsResponse
	var metadata struct {
		Error bool `json:"error"`
		Data  struct {
			Tournaments []superbetNamedItem `json:"tournaments"`
			Categories  []superbetNamedItem `json:"categories"`
		} `json:"data"`
	}
	var builder struct {
		Events []string `json:"events"`
	}
	errors := make([]error, 4)
	var wg sync.WaitGroup
	for i, index := range []string{"active-prematch", "active-live"} {
		wg.Add(1)
		go func(i int, index string) {
			defer wg.Done()
			query := url.Values{
				"startDate": {start.UTC().Format(time.RFC3339)},
				"endDate":   {end.UTC().Format(time.RFC3339)},
				"index":     {index}, "sports": {"5"},
			}
			result := &prematch
			if i == 1 {
				result = &live
			}
			errors[i] = getSuperbetCalendarJSON(ctx, client, offerBase+"/v3/pl-PL/events?"+query.Encode(), result)
		}(i, index)
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		errors[2] = getSuperbetCalendarJSON(ctx, client, offerBase+"/v2/pl-PL/struct", &metadata)
		if errors[2] == nil && metadata.Error {
			errors[2] = fmt.Errorf("błąd katalogu rozgrywek")
		}
	}()
	go func() {
		defer wg.Done()
		errors[3] = getSuperbetCalendarJSON(ctx, client, builderBase+"/betbuilder/v2/getBetbuilderEvents?target=SB_PL", &builder)
	}()
	wg.Wait()
	for _, err := range errors {
		if isArchiveFailure(err) {
			return nil, err
		}
	}
	if errors[0] != nil && errors[1] != nil {
		return nil, fmt.Errorf("nie udało się pobrać kalendarza Superbeta: %w", errors[0])
	}
	result := &SuperbetCalendar{Date: date, Events: []SuperbetCalendarEvent{}, Warnings: []string{}}
	labels := []string{"Nie pobrano meczów przed rozpoczęciem", "Nie pobrano meczów na żywo", "Nie pobrano nazw rozgrywek", "Nie sprawdzono dostępności BetBuildera"}
	for i, err := range errors {
		if err != nil {
			result.Warnings = append(result.Warnings, labels[i]+": "+err.Error())
		}
	}
	names := make(map[string]string)
	countries := make(map[string]string)
	for _, item := range metadata.Data.Tournaments {
		names[item.ID] = item.Names["pl-PL"]
	}
	for _, item := range metadata.Data.Categories {
		countries[item.ID] = item.Names["pl-PL"]
	}
	builders := make(map[string]bool)
	for _, id := range builder.Events {
		builders[id] = true
	}
	byID := make(map[string]SuperbetCalendarEvent)
	for i, response := range []superbetEventsResponse{prematch, live} {
		if errors[i] != nil {
			continue
		}
		for _, event := range response.Events {
			fixture := event.Fixture
			when, err := time.Parse(time.RFC3339, fixture.Date)
			if err != nil || when.Before(start) || !when.Before(end) || fixture.SportID != 5 || event.ID <= 0 {
				continue
			}
			teams := strings.SplitN(fixture.Name, "·", 2)
			if len(teams) != 2 || strings.TrimSpace(teams[0]) == "" || strings.TrimSpace(teams[1]) == "" {
				continue
			}
			home, away := strings.TrimSpace(teams[0]), strings.TrimSpace(teams[1])
			id := strconv.Itoa(event.ID)
			competition := names[strconv.Itoa(fixture.TournamentID)]
			if competition == "" {
				competition = "Pozostałe rozgrywki"
			}
			byID[id] = SuperbetCalendarEvent{
				ID: id, Home: home, Away: away, Competition: competition,
				Country:   countries[strconv.Itoa(fixture.CategoryID)],
				StartTime: when.In(start.Location()).Format(time.RFC3339),
				URL:       "https://superbet.pl/kursy/pilka-nozna/" + calendarTeamSlug(home) + "-vs-" + calendarTeamSlug(away) + "-" + id + "?mdt=o",
				Live:      i == 1, BetBuilder: builders[id],
			}
		}
	}
	for _, event := range byID {
		result.Events = append(result.Events, event)
	}
	sort.Slice(result.Events, func(i, j int) bool {
		left, _ := time.Parse(time.RFC3339, result.Events[i].StartTime)
		right, _ := time.Parse(time.RFC3339, result.Events[j].StartTime)
		if !left.Equal(right) {
			return left.Before(right)
		}
		return result.Events[i].ID < result.Events[j].ID
	})
	return result, nil
}

// === Match analysis and statistics (same algorithm as the local application) ===
type Bookmaker string

const (
	BookmakerSuperbet Bookmaker = "SUPERBET"
	BookmakerSTS      Bookmaker = "STS"
	BookmakerBetclic  Bookmaker = "BETCLIC"
)

type CanonicalMarket string

const (
	MarketShotsTotal              CanonicalMarket = "SHOTS_TOTAL"
	MarketShotsOnTarget           CanonicalMarket = "SHOTS_ON_TARGET"
	MarketShotsBlocked            CanonicalMarket = "SHOTS_BLOCKED"
	MarketShotsOffTarget          CanonicalMarket = "SHOTS_OFF_TARGET"
	MarketShotsOutsideBox         CanonicalMarket = "SHOTS_OUTSIDE_BOX"
	MarketShotsInsideBox          CanonicalMarket = "SHOTS_INSIDE_BOX"
	MarketShotsOnTargetOutsideBox CanonicalMarket = "SHOTS_ON_TARGET_OUTSIDE_BOX"
	MarketShotsOnTargetInsideBox  CanonicalMarket = "SHOTS_ON_TARGET_INSIDE_BOX"
	MarketTackles                 CanonicalMarket = "TACKLES"
	MarketDispossessed            CanonicalMarket = "DISPOSSESSED"
	MarketFoulsCommitted          CanonicalMarket = "FOULS_COMMITTED"
	MarketFoulsDrawn              CanonicalMarket = "FOULS_DRAWN"
	MarketInterceptions           CanonicalMarket = "INTERCEPTIONS"
	MarketOffsides                CanonicalMarket = "OFFSIDES"
)

type ShotCoordinate struct {
	X        float64 `json:"x"`
	Y        float64 `json:"y"`
	ShotType string  `json:"shotType"`
	InBox    bool    `json:"inBox"`
	OnTarget bool    `json:"onTarget"`
	IsSub    bool    `json:"isSub"`
}

type NormalizedProp struct {
	Bookmaker   Bookmaker       `json:"bookmaker"`
	PlayerName  string          `json:"playerName"`
	MarketType  CanonicalMarket `json:"marketType"`
	MarketLabel string          `json:"marketLabel"`
	Line        float64         `json:"line"`
	IsUnder     bool            `json:"isUnder"`
	Odds        float64         `json:"odds"`
	MarketID    string          `json:"marketId"`
	SelectionID string          `json:"selectionId"`
}

type PlayerStatHistory struct {
	Ratings                  []*float64                `json:"ratings"`
	MatchContexts            []PlayerMatchContext      `json:"matchContexts"`
	EventIDs                 []int                     `json:"eventIds"`
	MatchStartedAt           []time.Time               `json:"matchStartedAt"`
	Incomplete               bool                      `json:"incomplete"`
	FromCache                bool                      `json:"fromCache"`
	DataFetchedAt            time.Time                 `json:"dataFetchedAt"`
	ID                       int                       `json:"id"`
	FullName                 string                    `json:"fullName"`
	TeamSide                 string                    `json:"teamSide"`
	TeamID                   string                    `json:"teamId"`
	TeamName                 string                    `json:"teamName"`
	Matches                  int                       `json:"matches"`
	MetricValues             map[CanonicalMarket][]int `json:"metricValues"`
	MetricValuesWithSuperSub map[CanonicalMarket][]int `json:"metricValuesWithSuperSub"`
	Minutes                  []int                     `json:"minutes"`
	Positions                []string                  `json:"positions"`
	Opponents                []string                  `json:"opponents"`
	Dates                    []string                  `json:"dates"`
	MatchShots               [][]ShotCoordinate        `json:"matchShots"`
}

type CachedPlayerHistory struct {
	History    PlayerStatHistory `json:"history"`
	CreatedAt  time.Time         `json:"createdAt"`
	FixtureID  string            `json:"fixtureId,omitempty"`
	ValidUntil time.Time         `json:"validUntil,omitempty"`
}

type AnalyzedBet struct {
	SampleRatings        []*float64         `json:"sampleRatings"`
	FairModel            *FairModelDetails  `json:"fairModel,omitempty"`
	MatchOverview        *MatchOverview     `json:"matchOverview,omitempty"`
	Bookmaker            Bookmaker          `json:"bookmaker"`
	PlayerName           string             `json:"playerName"`
	MarketType           CanonicalMarket    `json:"marketType"`
	MarketLabel          string             `json:"marketLabel"`
	Line                 float64            `json:"line"`
	IsUnder              bool               `json:"isUnder"`
	BookmakerOdds        float64            `json:"bookmakerOdds"`
	FairOdds             float64            `json:"fairOdds"`
	HitRate              float64            `json:"hitRate"`
	EdgePercent          float64            `json:"edgePercent"`
	EVWithTax            float64            `json:"evWithTax"`
	MatchesSample        int                `json:"matchesSample"`
	SampleValues         []int              `json:"sampleValues"`
	SampleValuesSuperSub []int              `json:"sampleValuesSuperSub"`
	SampleMinutes        []int              `json:"sampleMinutes"`
	SamplePositions      []string           `json:"samplePositions"`
	SampleOpponents      []string           `json:"sampleOpponents"`
	SampleDates          []string           `json:"sampleDates"`
	SampleShots          [][]ShotCoordinate `json:"sampleShots"`
	StatsSource          string             `json:"statsSource"`
	TeamSide             string             `json:"teamSide"`
	TeamID               string             `json:"teamId"`
	TeamName             string             `json:"teamName"`
	Verdict              string             `json:"verdict"`
	LineupStatus         string             `json:"lineupStatus"`
	IsStartingXI         bool               `json:"isStartingXI"`
}

type TeamDetectionResult struct {
	HomeSlug string   `json:"homeSlug"`
	AwaySlug string   `json:"awaySlug"`
	HomeID   string   `json:"homeId"`
	AwayID   string   `json:"awayId"`
	Warnings []string `json:"warnings"`
}

type LeagueOption struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

const sofaBase = "https://www.sofascore.com/api/v1"

type SofaTeamItem struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Slug   string `json:"slug"`
	Gender string `json:"gender"`
	Sport  struct {
		ID   int    `json:"id"`
		Slug string `json:"slug"`
	} `json:"sport"`
	FieldTranslations struct {
		NameTranslation map[string]string `json:"nameTranslation"`
	} `json:"fieldTranslations"`
	Country struct {
		Name   string `json:"name"`
		Alpha2 string `json:"alpha2"`
	} `json:"country"`
}

type SofaSearchResponse struct {
	Teams   []SofaTeamItem `json:"teams"`
	Results []struct {
		Type   string       `json:"type"`
		Entity SofaTeamItem `json:"entity"`
	} `json:"results"`
}

func (s *SofaSearchResponse) ExtractTeams() []SofaTeamItem {
	var list []SofaTeamItem
	seen := make(map[int]bool)
	for _, t := range s.Teams {
		if t.ID > 0 && !seen[t.ID] {
			seen[t.ID] = true
			list = append(list, t)
		}
	}
	for _, r := range s.Results {
		if (r.Type == "team" || r.Type == "") && r.Entity.ID > 0 && !seen[r.Entity.ID] {
			seen[r.Entity.ID] = true
			list = append(list, r.Entity)
		}
	}
	return list
}

type SofaPlayerStats struct {
	Rating                 *float64 `json:"rating"`
	MinutesPlayed          int      `json:"minutesPlayed"`
	TotalShots             int      `json:"totalShots"`
	OnTargetScoringAttempt int      `json:"onTargetScoringAttempt"`
	ShotOffTarget          int      `json:"shotOffTarget"`
	BlockedScoringAttempt  int      `json:"blockedScoringAttempt"`
	TotalTackle            int      `json:"totalTackle"`
	WonTackle              int      `json:"wonTackle"`
	InterceptionWon        int      `json:"interceptionWon"`
	Fouls                  int      `json:"fouls"`
	WasFouled              int      `json:"wasFouled"`
	TotalOffside           int      `json:"totalOffside"`
	Dispossessed           int      `json:"dispossessed"`
}

func excludeBlockedShotsFromOnTarget(stats *SofaPlayerStats, shots []ShotCoordinate, isSub bool) {
	for _, shot := range shots {
		// SofaScore includes goal-line blocks in onTargetScoringAttempt.
		// Our shot markets classify every blocked shot separately from saves and goals.
		if shot.IsSub == isSub && shot.ShotType == "blocked-off-line" && stats.OnTargetScoringAttempt > 0 {
			stats.OnTargetScoringAttempt--
			stats.BlockedScoringAttempt++
		}
	}
}

type SofaPlayerDetails struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Slug      string `json:"slug"`
	ShortName string `json:"shortName"`
	Position  string `json:"position"`
}

type SofaLineupPlayer struct {
	Player     SofaPlayerDetails `json:"player"`
	Substitute *bool             `json:"substitute"`
	Position   string            `json:"position"`
	Statistics SofaPlayerStats   `json:"statistics"`
}

type SofaTeamPlayersResponse struct {
	Players []struct {
		Player SofaPlayerDetails `json:"player"`
	} `json:"players"`
}

type SofaLineupResponse struct {
	Confirmed *bool `json:"confirmed"`
	Home      struct {
		Players []SofaLineupPlayer `json:"players"`
	} `json:"home"`
	Away struct {
		Players []SofaLineupPlayer `json:"players"`
	} `json:"away"`
}

type SofaIncidentResponse struct {
	Incidents []struct {
		IncidentType string `json:"incidentType"`
		PlayerIn     struct {
			ID int `json:"id"`
		} `json:"playerIn"`
		PlayerOut struct {
			ID int `json:"id"`
		} `json:"playerOut"`
	} `json:"incidents"`
}

type SofaShotmapResponse struct {
	Shotmap []struct {
		Player struct {
			ID int `json:"id"`
		} `json:"player"`
		IsHome            bool   `json:"isHome"`
		ShotType          string `json:"shotType"`
		GoalType          string `json:"goalType"`
		PlayerCoordinates struct {
			X float64 `json:"x"`
			Y float64 `json:"y"`
		} `json:"playerCoordinates"`
	} `json:"shotmap"`
}

func (s SofaShotmapResponse) PlayerShots(playerID, subPlayerID int) []ShotCoordinate {
	var shots []ShotCoordinate
	for _, shot := range s.Shotmap {
		// Own goals are defensive incidents, not the player's scoring attempts.
		if shot.GoalType == "own" {
			continue
		}
		isSub := subPlayerID > 0 && shot.Player.ID == subPlayerID
		if shot.Player.ID != playerID && !isSub {
			continue
		}
		shots = append(shots, ShotCoordinate{
			X:        shot.PlayerCoordinates.X,
			Y:        shot.PlayerCoordinates.Y,
			ShotType: shot.ShotType,
			InBox:    isShotInsideBox(shot.PlayerCoordinates.X, shot.PlayerCoordinates.Y),
			OnTarget: shot.ShotType == "save" || shot.ShotType == "goal",
			IsSub:    shot.Player.ID != playerID,
		})
	}
	return shots
}

type SofaPlayerEventsResponse struct {
	Events []struct {
		ID         int    `json:"id"`
		Slug       string `json:"slug"`
		StartTS    int64  `json:"startTimestamp"`
		Tournament struct {
			ID               int    `json:"id"`
			Name             string `json:"name"`
			UniqueTournament struct {
				ID   int    `json:"id"`
				Name string `json:"name"`
			} `json:"uniqueTournament"`
		} `json:"tournament"`
		HomeTeam struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		} `json:"homeTeam"`
		AwayTeam struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		} `json:"awayTeam"`
		Status struct {
			Code int `json:"code"`
		} `json:"status"`
	} `json:"events"`
	PlayedForTeamMap map[string]int `json:"playedForTeamMap"`
}

type SofaPlayerMatchStatResponse struct {
	Statistics SofaPlayerStats `json:"statistics"`
	Position   string          `json:"position"`
}

type SofaTeamEventsResponse struct {
	Events []struct {
		ID         int    `json:"id"`
		Slug       string `json:"slug"`
		StartTS    int64  `json:"startTimestamp"`
		Tournament struct {
			ID               int    `json:"id"`
			Name             string `json:"name"`
			UniqueTournament struct {
				ID   int    `json:"id"`
				Name string `json:"name"`
			} `json:"uniqueTournament"`
		} `json:"tournament"`
		HomeTeam  SofaTeamItem  `json:"homeTeam"`
		AwayTeam  SofaTeamItem  `json:"awayTeam"`
		HomeScore SofaFlowScore `json:"homeScore"`
		AwayScore SofaFlowScore `json:"awayScore"`
		Status    struct {
			Code int `json:"code"`
		} `json:"status"`
	} `json:"events"`
}

type SofaSeasonsResponse struct {
	TournamentSeasons []struct {
		Tournament struct {
			Name             string `json:"name"`
			UniqueTournament struct {
				Name string `json:"name"`
				ID   int    `json:"id"`
			} `json:"uniqueTournament"`
		} `json:"tournament"`
	} `json:"tournamentSeasons"`
}

// ==========================================
// 2. CACHE I DETEKCJA SLUGÓW
// ==========================================

var (
	cacheMutex         sync.Mutex
	playerStatsCacheMu sync.Mutex
	playerStatsCache   map[string]CachedPlayerHistory
	cacheLoadedOnce    sync.Once
)

// Used only when a specific upcoming fixture cannot be established.
const playerCacheTTL = 24 * time.Hour
const playerCacheVersion = 6

func initPlayerCache() {
	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()

	playerStatsCache = make(map[string]CachedPlayerHistory)
	path := getCacheFilePath("sofa_player_stats_cache.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &playerStatsCache)
	if playerStatsCache == nil {
		playerStatsCache = make(map[string]CachedPlayerHistory)
	}
	log.Printf("[STATS-CACHE] Załadowano z dysku %d wpisów historii graczy", len(playerStatsCache))
}

func getPlayerCache(key string) (*PlayerStatHistory, bool) {
	cacheLoadedOnce.Do(initPlayerCache)

	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()

	item, exists := playerStatsCache[key]
	if !exists {
		return nil, false
	}

	expires := item.ValidUntil
	if expires.IsZero() {
		expires = item.CreatedAt.Add(playerCacheTTL)
	}
	if !time.Now().Before(expires) {
		delete(playerStatsCache, key)
		return nil, false
	}

	cp := item.History
	cp.DataFetchedAt = item.CreatedAt
	cp.FromCache = true
	return &cp, true
}

func savePlayerCache(key string, hist *PlayerStatHistory, fixture statsFixture) {
	cacheLoadedOnce.Do(initPlayerCache)

	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()

	playerStatsCache[key] = CachedPlayerHistory{
		History:    *hist,
		CreatedAt:  hist.DataFetchedAt,
		FixtureID:  fixture.ID,
		ValidUntil: fixture.ValidUntil,
	}

	if err := atomicJSON(getCacheFilePath("sofa_player_stats_cache.json"), playerStatsCache); err != nil {
		log.Printf("STATS_CACHE_WRITE_ERROR: %v", err)
	}
}

func getCacheFilePath(filename string) string {
	return filepath.Join(serverDataDir, filename)
}

func loadTeamCache() map[string]string {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("sofa_team_cache.json")
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[CACHE] Brak pliku cache drużyn (%s) lub błąd odczytu: %v", path, err)
		return make(map[string]string)
	}

	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("[CACHE] Błąd deserializacji cache: %v", err)
		return make(map[string]string)
	}
	log.Printf("[CACHE] Załadowano %d drużyn z pamięci podręcznej (%s)", len(m), path)
	return m
}

type sofaTeamIdentity struct {
	ID      string `json:"id"`
	Name    string `json:"name,omitempty"`
	Slug    string `json:"slug,omitempty"`
	Manual  bool   `json:"manual,omitempty"`
	Gender  string `json:"gender,omitempty"`
	Version int    `json:"version"`
}

func verifiedTeamIDInCache(teamSlug string, cache map[string]string, gender string) string {
	id := findTeamIDInCache(teamSlug, cache)
	if id == "" {
		return ""
	}
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	var identities map[string]sofaTeamIdentity
	raw, err := os.ReadFile(getCacheFilePath("sofa_team_identity_cache.json"))
	if err == nil && json.Unmarshal(raw, &identities) == nil {
		for slug, identity := range identities {
			if (identity.Manual || (identity.Version == 3 && identity.Gender == gender)) && identity.ID == id &&
				(strings.EqualFold(slug, teamSlug) || canonicalTeamCore(slug) == canonicalTeamCore(teamSlug)) {
				return id
			}
		}
	}
	log.Printf("[CACHE] UNVERIFIED slug=%q id=%s; ponowne rozpoznanie drużyny zamiast zaufania staremu dopasowaniu", teamSlug, id)
	return ""
}

func saveTeamCache(slug string, team SofaTeamItem, gender string) {
	slug = strings.TrimSpace(strings.ToLower(slug))
	if slug == "" || team.ID <= 0 {
		return
	}

	if err := persistTeamIdentity(slug, sofaTeamIdentity{ID: strconv.Itoa(team.ID), Name: team.Name, Slug: team.Slug, Gender: gender, Version: 3}); err != nil {
		log.Printf("TEAM_CACHE_WRITE_ERROR: %v", err)
	}
}

func extractIDAndSlug(rawURL string) (string, string, string) {
	log.Printf("[EXTRACT] Rozpoczynam ekstrakcję URL: %s", rawURL)
	clean := strings.TrimSpace(rawURL)
	if decoded, err := url.PathUnescape(clean); err == nil {
		clean = decoded
	}
	if idx := strings.Index(clean, "?"); idx != -1 {
		clean = clean[:idx]
	}
	clean = strings.TrimRight(clean, "/")

	lastSlash := strings.LastIndex(clean, "/")
	lastSegment := clean
	if lastSlash != -1 {
		lastSegment = clean[lastSlash+1:]
	}

	reBetclic := regexp.MustCompile(`^(.*)-m(\d+)$`)
	if m := reBetclic.FindStringSubmatch(lastSegment); len(m) == 3 {
		log.Printf("[EXTRACT] Wykryto BETCLIC: ID=%s, Slug=%s", m[2], m[1])
		return m[2], m[1], "BETCLIC"
	}

	var reSTS = regexp.MustCompile(`^f(\d+)$`)
	if m := reSTS.FindStringSubmatch(lastSegment); len(m) == 2 {
		id := m[1]
		slug := ""
		parts := strings.Split(clean, "/")
		if len(parts) >= 2 {
			slug = parts[len(parts)-2]
		}
		log.Printf("[EXTRACT] Wykryto STS: ID=%s, Slug=%s", id, slug)
		return id, slug, "STS"
	}

	reSb := regexp.MustCompile(`^(.*?)-(\d+)$`)
	if m := reSb.FindStringSubmatch(lastSegment); len(m) == 3 {
		log.Printf("[EXTRACT] Wykryto SUPERBET: ID=%s, Slug=%s", m[2], m[1])
		return m[2], m[1], "SUPERBET"
	}

	numRe := regexp.MustCompile(`\d+`)
	id := numRe.FindString(lastSegment)
	log.Printf("[EXTRACT] Nieznany bukmacher (UNKNOWN): ID=%s, Segment=%s", id, lastSegment)
	return id, lastSegment, "UNKNOWN"
}

func parseTeamsFromSlug(slug, _ string) (string, string) {
	log.Printf("[PARSER] Parsowanie slug drużyn: %s", slug)
	slug = strings.ToLower(slug)
	slug = strings.ReplaceAll(slug, "–", "-")

	if strings.Contains(slug, "-vs-") {
		pts := strings.Split(slug, "-vs-")
		h, a := strings.TrimSpace(pts[0]), strings.TrimSpace(pts[1])
		log.Printf("[PARSER] Rozpoznano wg '-vs-': %s vs %s", h, a)
		return h, a
	}
	if strings.Contains(slug, "-v-") {
		pts := strings.Split(slug, "-v-")
		h, a := strings.TrimSpace(pts[0]), strings.TrimSpace(pts[1])
		log.Printf("[PARSER] Rozpoznano wg '-v-': %s vs %s", h, a)
		return h, a
	}

	pts := strings.Split(slug, "-")
	if len(pts) == 2 {
		log.Printf("[PARSER] Rozpoznano wg 2 tokenów: %s vs %s", pts[0], pts[1])
		return pts[0], pts[1]
	}

	if len(pts) == 3 {
		p0 := pts[0]
		if p0 == "fc" || p0 == "sc" || p0 == "sv" || p0 == "vfb" || p0 == "vfl" || p0 == "tsg" || p0 == "rb" || p0 == "real" {
			log.Printf("[PARSER] Rozpoznano prefiks w 3 tokenach: %s-%s vs %s", pts[0], pts[1], pts[2])
			return pts[0] + "-" + pts[1], pts[2]
		}
		p1 := pts[1]
		if p1 == "fc" || p1 == "sc" || p1 == "sv" || p1 == "vfb" || p1 == "vfl" || p1 == "tsg" || p1 == "rb" || p1 == "real" {
			log.Printf("[PARSER] Rozpoznano prefiks gości w 3 tokenach: %s vs %s-%s", pts[0], pts[1], pts[2])
			return pts[0], pts[1] + "-" + pts[2]
		}
	}

	if len(pts) >= 4 {
		if pts[0] == "real" || pts[0] == "atletico" || pts[0] == "rayo" || (pts[0] == "1" && pts[1] == "fc") {
			splitIdx := 2
			if pts[0] == "1" && pts[1] == "fc" {
				splitIdx = 3
			}
			h, a := strings.Join(pts[:splitIdx], "-"), strings.Join(pts[splitIdx:], "-")
			log.Printf("[PARSER] Rozpoznano złożony klub na początku: %s vs %s", h, a)
			return h, a
		}
		mid := len(pts) / 2
		h, a := strings.Join(pts[:mid], "-"), strings.Join(pts[mid:], "-")
		log.Printf("[PARSER] Podział przez środek (długość %d): %s vs %s", len(pts), h, a)
		return h, a
	}

	log.Printf("[PARSER] Nie udało się jednoznacznie podzielić sluga: %s", slug)
	return slug, ""
}

func canonicalTeamCore(s string) string {
	s = removeDiacritics(strings.ToLower(strings.TrimSpace(s)))
	if alias, ok := teamSlugAliases[s]; ok {
		s = alias
	}
	s = strings.ReplaceAll(s, "–", "-")
	s = strings.ReplaceAll(s, "—", "-")
	s = strings.ReplaceAll(s, "_", "-")
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "madryt", "madrid")
	s = strings.ReplaceAll(s, "koeln", "koln")
	s = strings.ReplaceAll(s, "muenchen", "munchen")
	s = strings.ReplaceAll(s, "monachium", "munchen")
	s = strings.ReplaceAll(s, "mgladbach", "monchengladbach")
	s = strings.Trim(s, "-")
	prefixes := []string{"1-fc-", "1-fsv-", "fc-", "sc-", "sv-", "vfb-", "vfl-", "tsg-", "1-"}
	for _, p := range prefixes {
		s = strings.TrimPrefix(s, p)
	}
	// RB distinguishes clubs such as Red Bull Bragantino from other Bragantinos.
	// Expand this qualifier instead of deleting it during normalization.
	if strings.HasPrefix(s, "rb-") {
		s = "red-bull-" + strings.TrimPrefix(s, "rb-")
	}
	for _, suffix := range []string{"-sv", "-fc", "-cf", "-afc"} {
		s = strings.TrimSuffix(s, suffix)
	}
	return strings.Trim(s, "-")
}

var teamSlugAliases = map[string]string{
	"rb-lipsk": "rb-leipzig",
	"lipsk":    "leipzig",
}

func findTeamIDInCache(teamSlug string, cache map[string]string) string {
	if teamSlug == "" {
		return ""
	}
	clean := strings.ToLower(strings.TrimSpace(teamSlug))
	if alias, ok := teamSlugAliases[clean]; ok {
		clean = alias
	}
	cleanCore := canonicalTeamCore(clean)

	if id, ok := cache[clean]; ok {
		log.Printf("[CACHE] Trafienie dokładne w cache dla: %s -> %s", clean, id)
		return id
	}

	var matchingID string
	for cacheSlug, id := range cache {
		if canonicalTeamCore(cacheSlug) == cleanCore {
			if matchingID != "" && matchingID != id {
				log.Printf("[CACHE] Niejednoznaczne ID dla sluga %q; wymagane ponowne dopasowanie", teamSlug)
				return ""
			}
			matchingID = id
		}
	}
	if matchingID != "" {
		log.Printf("[CACHE] Trafienie kanoniczne w cache dla: %s -> %s", teamSlug, matchingID)
		return matchingID
	}

	log.Printf("[CACHE] Brak drużyny w cache dla sluga: %s (rdzeń: %s)", teamSlug, cleanCore)
	return ""
}

func displayOpponentName(name string) string {
	original := strings.Fields(strings.TrimSpace(name))
	parts := append([]string(nil), original...)
	for len(parts) > 0 {
		token := strings.Trim(strings.ToLower(parts[0]), ".")
		switch token {
		case "1", "fc", "sc", "sv", "vfb", "vfl", "tsg", "rb", "ac", "afc", "cf":
			parts = parts[1:]
		default:
			if len(parts) > 2 {
				parts = parts[:2]
			}
			return strings.Join(parts, " ")
		}
	}
	return strings.Join(original, " ")
}

// ==========================================
// 3. POBIERANIE SIECIOWE (TLS BROWSER IMPERSONATION + REUSE)
// ==========================================

// Context-aware SofaScore transport is defined below.

var sofaWomenPattern = regexp.MustCompile(`(?i)(^|[^a-z])(women|womens|female|feminino|feminina|femenino|femenina|feminin|feminine|kobiet|kobiety)([^a-z]|$)`)
var sofaAgePattern = regexp.MustCompile(`(^|-)(u[0-9]{2})(-|$)`)

func sofaTeamNameAndGender(name string) (string, string) {
	core := canonicalTeamCore(name)
	gender := ""
	parts := strings.Split(core, "-")
	kept := make([]string, 0, len(parts))
	for i, part := range parts {
		if sofaWomenPattern.MatchString(part) || (i == len(parts)-1 && (part == "w" || part == "k" || part == "(k)")) {
			gender = "F"
			continue
		}
		kept = append(kept, part)
	}
	return strings.Join(kept, "-"), gender
}

// Brazilian bookmakers append state abbreviations to club names. Keep them
// in cache keys, but allow a Brazilian search result without that suffix.
func sofaBrazilianCore(core string) (string, bool) {
	index := strings.LastIndexByte(core, '-')
	if index < 1 {
		return core, false
	}
	switch core[index+1:] {
	case "ac", "al", "ap", "am", "ba", "ce", "df", "es", "go", "ma", "mt", "ms", "mg", "pa", "pb", "pr", "pe", "pi", "rj", "rn", "rs", "ro", "rr", "sc", "sp", "se", "to":
		return core[:index], true
	}
	return core, false
}

func sofaTeamQualifier(core string) string {
	if match := sofaAgePattern.FindStringSubmatch(core); len(match) > 2 {
		return match[2]
	}
	for _, suffix := range []string{"-b", "-ii", "-reserves", "-reserve"} {
		if strings.HasSuffix(core, suffix) {
			return "reserve"
		}
	}
	return ""
}

func sofaTeamMatchesSlug(teamSlug string, team SofaTeamItem, gender string) bool {
	if team.ID <= 0 || (team.Sport.ID != 0 && team.Sport.ID != 1) ||
		(team.Sport.Slug != "" && team.Sport.Slug != "football") {
		return false
	}
	target, namedGender := sofaTeamNameAndGender(teamSlug)
	if namedGender != "" {
		gender = namedGender
	}
	actualGender := strings.ToUpper(strings.TrimSpace(team.Gender))
	if actualGender == "" {
		_, actualGender = sofaTeamNameAndGender(team.Name + "-" + team.Slug)
	}
	if actualGender != "" && actualGender != gender {
		return false
	}
	if target == "" {
		return false
	}
	qualifier := sofaTeamQualifier(target)
	for _, name := range []string{team.Name, team.Slug} {
		core, _ := sofaTeamNameAndGender(name)
		if candidateQualifier := sofaTeamQualifier(core); candidateQualifier != "" && candidateQualifier != qualifier {
			return false
		}
	}
	base, hasState := sofaBrazilianCore(target)
	brazilian := strings.EqualFold(team.Country.Alpha2, "BR") || strings.EqualFold(team.Country.Name, "Brazil") || strings.EqualFold(team.Country.Name, "Brasil")
	if hasState && brazilian {
		target = base
	}
	matchesName := func(name string) bool {
		core, _ := sofaTeamNameAndGender(name)
		if hasState && brazilian {
			core, _ = sofaBrazilianCore(core)
		}
		return core == target
	}
	for _, name := range []string{team.Name, team.Slug} {
		if matchesName(name) {
			return true
		}
	}
	for _, name := range team.FieldTranslations.NameTranslation {
		if matchesName(name) {
			return true
		}
	}
	return false
}

func searchSofaTeamCandidates(client *ResilientClient, teamSlug, gender string) ([]SofaTeamItem, error) {
	log.Printf("[SOFA-SEARCH] Rozpoczynam wyszukiwanie ID drużyny dla sluga: %s", teamSlug)
	core, _ := sofaTeamNameAndGender(teamSlug)
	searchCore, _ := sofaBrazilianCore(core)
	queryName := strings.ReplaceAll(searchCore, "-", " ")
	if queryName == "" {
		return nil, fmt.Errorf("pusty slug")
	}
	queries := []string{queryName}
	rawName := strings.ReplaceAll(calendarTeamSlug(teamSlug), "-", " ")
	if rawName != queryName {
		queries = append(queries, rawName)
	}
	parts := strings.Fields(queryName)
	if len(parts) > 1 && len(parts[len(parts)-1]) >= 4 && sofaTeamQualifier(parts[len(parts)-1]) == "" {
		queries = append(queries, parts[len(parts)-1])
	}
	seenQueries := map[string]bool{}
	for _, query := range queries {
		if seenQueries[query] {
			continue
		}
		seenQueries[query] = true
		body, err := client.GetSofa("/search/all?q=" + url.QueryEscape(query) + "&page=0")
		if err != nil {
			return nil, err
		}
		var res SofaSearchResponse
		if err := json.Unmarshal(body, &res); err != nil {
			return nil, fmt.Errorf("niepoprawna odpowiedź wyszukiwania SofaScore dla %q: %w", query, err)
		}
		teams := res.ExtractTeams()
		matches := []SofaTeamItem{}
		for _, team := range teams {
			matched := sofaTeamMatchesSlug(teamSlug, team, gender)
			log.Printf("[SOFA-SEARCH] CANDIDATE query=%q id=%d name=%q slug=%q gender=%q sport=%q sport_id=%d country=%q wanted_gender=%q matches=%v", query, team.ID, team.Name, team.Slug, team.Gender, team.Sport.Slug, team.Sport.ID, team.Country.Name, gender, matched)
			if matched {
				matches = append(matches, team)
			}
		}
		if len(matches) > 0 {
			return matches, nil
		}
	}
	return nil, fmt.Errorf("brak dopasowania nazwy %q i kategorii %s w SofaScore", teamSlug, gender)
}

func fetchSofaTeamSquad(client *ResilientClient, teamID string) ([]SofaPlayerDetails, string, error) {
	log.Printf("[SOFA-SQUAD] Pobieranie kadry dla drużyny ID: %s", teamID)

	playerMap := make(map[int]SofaPlayerDetails)
	teamDisplayName := teamID

	body, err := client.GetSofa(fmt.Sprintf("/team/%s/players", teamID))
	if err == nil {
		var sqRes SofaTeamPlayersResponse
		if json.Unmarshal(body, &sqRes) == nil && len(sqRes.Players) > 0 {
			for _, p := range sqRes.Players {
				if p.Player.ID > 0 {
					playerMap[p.Player.ID] = p.Player
				}
			}
			log.Printf("[SOFA-SQUAD] Załadowano %d zawodników z endpointu /team/%s/players", len(playerMap), teamID)
		}
	} else {
		log.Printf("[SOFA-SQUAD] Ostrzeżenie: nie udało się pobrać /team/%s/players: %v", teamID, err)
	}

	evBody, err := client.GetSofa(fmt.Sprintf("/team/%s/events/last/0", teamID))
	if err == nil {
		var evRes SofaTeamEventsResponse
		if json.Unmarshal(evBody, &evRes) == nil && len(evRes.Events) > 0 {
			teamNumericID, _ := strconv.Atoi(teamID)
			for _, e := range evRes.Events {
				if e.HomeTeam.ID == teamNumericID && e.HomeTeam.Name != "" {
					teamDisplayName = e.HomeTeam.Name
					break
				}
				if e.AwayTeam.ID == teamNumericID && e.AwayTeam.Name != "" {
					teamDisplayName = e.AwayTeam.Name
					break
				}
			}
			log.Printf("[SOFA-SQUAD] Wykryta nazwa drużyny dla ID %s: '%s'", teamID, teamDisplayName)

			limit := 3
			if len(evRes.Events) < limit {
				limit = len(evRes.Events)
			}
			for i := 0; i < limit; i++ {
				lBody, err := client.GetSofa(fmt.Sprintf("/event/%d/lineups", evRes.Events[i].ID))
				if err != nil {
					continue
				}
				var lRes SofaLineupResponse
				if json.Unmarshal(lBody, &lRes) != nil {
					continue
				}

				collect := func(players []SofaLineupPlayer) {
					for _, p := range players {
						if p.Player.ID > 0 {
							playerMap[p.Player.ID] = p.Player
						}
					}
				}

				if evRes.Events[i].HomeTeam.ID == teamNumericID {
					collect(lRes.Home.Players)
				} else {
					collect(lRes.Away.Players)
				}
			}
		}
	}

	var squad []SofaPlayerDetails
	for _, p := range playerMap {
		squad = append(squad, p)
	}

	if len(squad) == 0 {
		log.Printf("[SOFA-SQUAD] BŁĄD: Pusta kadra dla drużyny ID: %s", teamID)
		return nil, teamDisplayName, fmt.Errorf("nie udało się pobrać kadry dla drużyny %s", teamID)
	}

	log.Printf("[SOFA-SQUAD] Łącznie unikalnych zawodników w kadrze (%s): %d", teamDisplayName, len(squad))
	return squad, teamDisplayName, nil
}

func isShotInsideBox(x, y float64) bool {
	if x <= 16.5 || x >= 83.5 {
		if y >= 21.0 && y <= 79.0 {
			return true
		}
	}
	return false
}

func fetchSofaPlayerCompleteHistory(client *ResilientClient, playerID int, matchLimit int, allowedTournamentIDs string) (*PlayerStatHistory, error) {
	var incomplete atomic.Bool
	getHistoryData := func(c *ResilientClient, endpoint string) ([]byte, error) {
		body, err := c.GetSofa(endpoint)
		if err != nil {
			incomplete.Store(true)
		}
		return body, err
	}
	decodeHistoryJSON := func(body []byte, target any, kind string) error {
		err := json.Unmarshal(body, target)
		if err != nil {
			incomplete.Store(true)
			client.warn(fmt.Sprintf("Błąd odczytu %s zawodnika %d: %v", kind, playerID, err))
		}
		return err
	}
	// A pre-match history belongs to this fixture, not the next match of a team.
	cacheKey := fmt.Sprintf("v%d_%s_%d_%d_%s", playerCacheVersion, client.statsFixture.ID, playerID, matchLimit, allowedTournamentIDs)
	if cached, ok := getPlayerCache(cacheKey); ok {
		log.Printf("[SOFA-PLAYER-CACHE] Trafienie w cache dla gracza %d (limit: %d, turnieje: %s)", playerID, matchLimit, allowedTournamentIDs)
		return cached, nil
	}

	log.Printf("[SOFA-PLAYER] Brak w cache. Pobieranie historii z sieci dla playerID=%d (limit: %d)", playerID, matchLimit)

	endpoint := fmt.Sprintf("/player/%d/events/last/0", playerID)
	body, err := client.GetSofa(endpoint)
	if err != nil {
		log.Printf("[SOFA-PLAYER] Błąd pobierania zdarzeń dla gracza %d: %v", playerID, err)
		return nil, err
	}

	var pEvents struct {
		Events []struct {
			ID      int    `json:"id"`
			Slug    string `json:"slug"`
			StartTS int64  `json:"startTimestamp"`
			Season  struct {
				ID int `json:"id"`
			} `json:"season"`
			Tournament struct {
				ID               int    `json:"id"`
				Name             string `json:"name"`
				UniqueTournament struct {
					ID   int    `json:"id"`
					Name string `json:"name"`
				} `json:"uniqueTournament"`
			} `json:"tournament"`
			HomeTeam struct {
				ID   int    `json:"id"`
				Name string `json:"name"`
			} `json:"homeTeam"`
			AwayTeam struct {
				ID   int    `json:"id"`
				Name string `json:"name"`
			} `json:"awayTeam"`
			Status struct {
				Code int `json:"code"`
			} `json:"status"`
		} `json:"events"`
		PlayedForTeamMap map[string]int `json:"playedForTeamMap"`
		StatisticsMap    map[string]struct {
			MinutesPlayed int      `json:"minutesPlayed"`
			Rating        *float64 `json:"rating"`
		} `json:"statisticsMap"`
	}

	if err := json.Unmarshal(body, &pEvents); err != nil {
		log.Printf("[SOFA-PLAYER] Błąd unmarshalu zdarzeń dla gracza %d: %v", playerID, err)
		return nil, err
	}

	allowedTIDs := make(map[int]bool)
	if allowedTournamentIDs != "" && allowedTournamentIDs != "ALL" {
		for _, part := range strings.Split(allowedTournamentIDs, ",") {
			if id, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
				allowedTIDs[id] = true
			}
		}
	}

	type matchCandidate struct {
		Context  PlayerMatchContext
		Rating   *float64
		EventID  int
		Opponent string
		Date     string
		StartTS  int64
	}

	var allPlayedMatches []matchCandidate

	for _, e := range pEvents.Events {
		if e.Status.Code != 100 {
			continue
		}
		if !client.statsFixture.StartTime.IsZero() && e.StartTS >= client.statsFixture.StartTime.Unix() {
			continue
		}

		evIDStr := strconv.Itoa(e.ID)
		if stat, ok := pEvents.StatisticsMap[evIDStr]; ok {
			if stat.MinutesPlayed <= 0 {
				continue
			}
		}

		uTID := e.Tournament.UniqueTournament.ID
		if uTID == 0 {
			uTID = e.Tournament.ID
		}
		if len(allowedTIDs) > 0 && !allowedTIDs[uTID] {
			continue
		}

		playedTeamID := pEvents.PlayedForTeamMap[evIDStr]
		matchContext := PlayerMatchContext{TeamID: playedTeamID, LeagueID: e.Tournament.UniqueTournament.ID, SeasonID: e.Season.ID}
		if playedTeamID > 0 && playedTeamID == e.HomeTeam.ID {
			matchContext.OpponentID, matchContext.Venue = e.AwayTeam.ID, "HOME"
		} else if playedTeamID > 0 && playedTeamID == e.AwayTeam.ID {
			matchContext.OpponentID, matchContext.Venue = e.HomeTeam.ID, "AWAY"
		}
		opp := "Mecz"
		if e.HomeTeam.Name != "" && e.AwayTeam.Name != "" {
			if playedTeamID == e.HomeTeam.ID {
				opp = displayOpponentName(e.AwayTeam.Name)
			} else {
				opp = displayOpponentName(e.HomeTeam.Name)
			}
		}

		dStr := ""
		if e.StartTS > 0 {
			zone, _ := time.LoadLocation("Europe/Warsaw")
			dStr = time.Unix(e.StartTS, 0).In(zone).Format("02.01")
		}

		allPlayedMatches = append(allPlayedMatches, matchCandidate{
			Context:  matchContext,
			Rating:   pEvents.StatisticsMap[evIDStr].Rating,
			EventID:  e.ID,
			Opponent: opp,
			Date:     dStr,
			StartTS:  e.StartTS,
		})
	}

	sort.Slice(allPlayedMatches, func(i, j int) bool {
		return allPlayedMatches[i].StartTS > allPlayedMatches[j].StartTS
	})

	if matchLimit > 0 && len(allPlayedMatches) > matchLimit {
		allPlayedMatches = allPlayedMatches[:matchLimit]
	}

	log.Printf("[SOFA-PLAYER] Gracz %d: zakwalifikowano %d meczów do pobrania szczegółów", playerID, len(allPlayedMatches))

	hist := &PlayerStatHistory{
		DataFetchedAt:            time.Now().UTC(),
		ID:                       playerID,
		MetricValues:             make(map[CanonicalMarket][]int),
		MetricValuesWithSuperSub: make(map[CanonicalMarket][]int),
	}

	type zoneShots struct {
		inside        int
		outside       int
		insideTarget  int
		outsideTarget int
	}

	type matchStatResult struct {
		rating   *float64
		order    int
		opp      string
		date     string
		stats    SofaPlayerStats
		subStats SofaPlayerStats
		zone     zoneShots
		subZone  zoneShots
		shots    []ShotCoordinate
		pos      string
	}

	resultsChan := make(chan matchStatResult, len(allPlayedMatches))
	var wg sync.WaitGroup

	matchConcurrency := 2
	if client.statsPrefetch {
		matchConcurrency = 1
	}
	matchSem := make(chan struct{}, matchConcurrency)

	for i, m := range allPlayedMatches {
		wg.Add(1)
		go func(idx int, match matchCandidate) {
			defer wg.Done()
			select {
			case matchSem <- struct{}{}:
			case <-client.ctx.Done():
				return
			}
			defer func() { <-matchSem }()
			if client.background && !sofaCooldownUntil().IsZero() {
				incomplete.Store(true)
				return
			}
			client := client.withScope(archiveInfoFrom(client.ctx).Scope + "-mecz-" + match.Opponent + "-" + time.Unix(match.StartTS, 0).UTC().Format("2006-01-02") + "-event-" + strconv.Itoa(match.EventID))

			// 1. Statystyki bazowe zawodnika
			statURL := fmt.Sprintf("/event/%d/player/%d/statistics", match.EventID, playerID)
			sBody, err := getHistoryData(client, statURL)
			if err != nil {
				return
			}
			var pStat SofaPlayerMatchStatResponse
			if err := json.Unmarshal(sBody, &pStat); err != nil || pStat.Statistics.MinutesPlayed <= 0 {
				incomplete.Store(true)
				client.warn(fmt.Sprintf("Niekompletne statystyki gracza %d w meczu %d", playerID, match.EventID))
				return
			}

			// 2. Wyszukiwanie zmiennika (SuperSub) tylko gdy zagrał poniżej pełnych 90 minut
			var subStats SofaPlayerStats
			subPlayerID := 0
			if pStat.Statistics.MinutesPlayed < 85 {
				incURL := fmt.Sprintf("/event/%d/incidents", match.EventID)
				if incBody, err := getHistoryData(client, incURL); err == nil {
					var incRes SofaIncidentResponse
					if decodeHistoryJSON(incBody, &incRes, "incydentów") == nil {
						for _, inc := range incRes.Incidents {
							if inc.IncidentType == "substitution" && inc.PlayerOut.ID == playerID {
								subPlayerID = inc.PlayerIn.ID
								break
							}
						}
						if subPlayerID > 0 {
							subURL := fmt.Sprintf("/event/%d/player/%d/statistics", match.EventID, subPlayerID)
							if subBody, err := getHistoryData(client, subURL); err == nil {
								var subStatRes SofaPlayerMatchStatResponse
								if decodeHistoryJSON(subBody, &subStatRes, "statystyk zmiennika") == nil {
									subStats = subStatRes.Statistics
								}
							}
						}
					}
				}
			}

			// 3. Pobieranie mapy strzałów (Shotmap) TYLKO gdy gracz lub zmiennik oddali strzał
			var playerZone, subZone zoneShots
			var matchShots []ShotCoordinate

			if pStat.Statistics.TotalShots > 0 || subStats.TotalShots > 0 {
				smURL := fmt.Sprintf("/event/%d/shotmap", match.EventID)
				if smBody, err := getHistoryData(client, smURL); err == nil {
					var smRes SofaShotmapResponse
					if decodeHistoryJSON(smBody, &smRes, "mapy strzałów") == nil {
						matchShots = smRes.PlayerShots(playerID, subPlayerID)
						if len(matchShots) == 0 {
							incomplete.Store(true)
							client.warn(fmt.Sprintf("Brak mapy strzałów gracza %d w meczu %d pomimo dodatniej liczby strzałów", playerID, match.EventID))
						}
						for _, shot := range matchShots {
							zone := &playerZone
							if shot.IsSub {
								zone = &subZone
							}
							if shot.InBox {
								zone.inside++
								if shot.OnTarget {
									zone.insideTarget++
								}
							} else {
								zone.outside++
								if shot.OnTarget {
									zone.outsideTarget++
								}
							}
						}
					}
				}
			}

			excludeBlockedShotsFromOnTarget(&pStat.Statistics, matchShots, false)
			excludeBlockedShotsFromOnTarget(&subStats, matchShots, true)

			rating := pStat.Statistics.Rating
			if rating == nil {
				rating = match.Rating
			}
			resultsChan <- matchStatResult{
				rating:   rating,
				order:    idx,
				opp:      match.Opponent,
				date:     match.Date,
				stats:    pStat.Statistics,
				subStats: subStats,
				zone:     playerZone,
				subZone:  subZone,
				shots:    matchShots,
				pos:      pStat.Position,
			}
		}(i, m)
	}

	wg.Wait()
	close(resultsChan)
	if client.ctx != nil && client.ctx.Err() != nil {
		return nil, client.ctx.Err()
	}

	var collected []matchStatResult
	for r := range resultsChan {
		collected = append(collected, r)
	}

	sort.Slice(collected, func(i, j int) bool {
		return collected[i].order < collected[j].order
	})

	for _, c := range collected {
		hist.Matches++
		hist.Ratings = append(hist.Ratings, c.rating)
		hist.MatchContexts = append(hist.MatchContexts, allPlayedMatches[c.order].Context)
		hist.EventIDs = append(hist.EventIDs, allPlayedMatches[c.order].EventID)
		hist.MatchStartedAt = append(hist.MatchStartedAt, time.Unix(allPlayedMatches[c.order].StartTS, 0).UTC())
		hist.Minutes = append(hist.Minutes, c.stats.MinutesPlayed)
		hist.Positions = append(hist.Positions, c.pos)
		hist.Opponents = append(hist.Opponents, c.opp)
		hist.Dates = append(hist.Dates, c.date)
		hist.MatchShots = append(hist.MatchShots, c.shots)

		tackles := c.stats.WonTackle
		if tackles == 0 {
			tackles = c.stats.TotalTackle
		}
		subTackles := c.subStats.WonTackle
		if subTackles == 0 {
			subTackles = c.subStats.TotalTackle
		}

		hist.MetricValues[MarketShotsTotal] = append(hist.MetricValues[MarketShotsTotal], c.stats.TotalShots)
		hist.MetricValues[MarketShotsOnTarget] = append(hist.MetricValues[MarketShotsOnTarget], c.stats.OnTargetScoringAttempt)
		hist.MetricValues[MarketShotsBlocked] = append(hist.MetricValues[MarketShotsBlocked], c.stats.BlockedScoringAttempt)
		hist.MetricValues[MarketShotsOffTarget] = append(hist.MetricValues[MarketShotsOffTarget], c.stats.ShotOffTarget)
		hist.MetricValues[MarketTackles] = append(hist.MetricValues[MarketTackles], tackles)
		hist.MetricValues[MarketInterceptions] = append(hist.MetricValues[MarketInterceptions], c.stats.InterceptionWon)
		hist.MetricValues[MarketFoulsCommitted] = append(hist.MetricValues[MarketFoulsCommitted], c.stats.Fouls)
		hist.MetricValues[MarketFoulsDrawn] = append(hist.MetricValues[MarketFoulsDrawn], c.stats.WasFouled)
		hist.MetricValues[MarketOffsides] = append(hist.MetricValues[MarketOffsides], c.stats.TotalOffside)
		hist.MetricValues[MarketDispossessed] = append(hist.MetricValues[MarketDispossessed], c.stats.Dispossessed)

		hist.MetricValues[MarketShotsOutsideBox] = append(hist.MetricValues[MarketShotsOutsideBox], c.zone.outside)
		hist.MetricValues[MarketShotsInsideBox] = append(hist.MetricValues[MarketShotsInsideBox], c.zone.inside)
		hist.MetricValues[MarketShotsOnTargetOutsideBox] = append(hist.MetricValues[MarketShotsOnTargetOutsideBox], c.zone.outsideTarget)
		hist.MetricValues[MarketShotsOnTargetInsideBox] = append(hist.MetricValues[MarketShotsOnTargetInsideBox], c.zone.insideTarget)

		hist.MetricValuesWithSuperSub[MarketShotsTotal] = append(hist.MetricValuesWithSuperSub[MarketShotsTotal], c.stats.TotalShots+c.subStats.TotalShots)
		hist.MetricValuesWithSuperSub[MarketShotsOnTarget] = append(hist.MetricValuesWithSuperSub[MarketShotsOnTarget], c.stats.OnTargetScoringAttempt+c.subStats.OnTargetScoringAttempt)
		hist.MetricValuesWithSuperSub[MarketShotsBlocked] = append(hist.MetricValuesWithSuperSub[MarketShotsBlocked], c.stats.BlockedScoringAttempt+c.subStats.BlockedScoringAttempt)
		hist.MetricValuesWithSuperSub[MarketShotsOffTarget] = append(hist.MetricValuesWithSuperSub[MarketShotsOffTarget], c.stats.ShotOffTarget+c.subStats.ShotOffTarget)
		hist.MetricValuesWithSuperSub[MarketTackles] = append(hist.MetricValuesWithSuperSub[MarketTackles], tackles+subTackles)
		hist.MetricValuesWithSuperSub[MarketInterceptions] = append(hist.MetricValuesWithSuperSub[MarketInterceptions], c.stats.InterceptionWon+c.subStats.InterceptionWon)
		hist.MetricValuesWithSuperSub[MarketFoulsCommitted] = append(hist.MetricValuesWithSuperSub[MarketFoulsCommitted], c.stats.Fouls+c.subStats.Fouls)
		hist.MetricValuesWithSuperSub[MarketFoulsDrawn] = append(hist.MetricValuesWithSuperSub[MarketFoulsDrawn], c.stats.WasFouled+c.subStats.WasFouled)
		hist.MetricValuesWithSuperSub[MarketOffsides] = append(hist.MetricValuesWithSuperSub[MarketOffsides], c.stats.TotalOffside+c.subStats.TotalOffside)
		hist.MetricValuesWithSuperSub[MarketDispossessed] = append(hist.MetricValuesWithSuperSub[MarketDispossessed], c.stats.Dispossessed+c.subStats.Dispossessed)

		hist.MetricValuesWithSuperSub[MarketShotsOutsideBox] = append(hist.MetricValuesWithSuperSub[MarketShotsOutsideBox], c.zone.outside+c.subZone.outside)
		hist.MetricValuesWithSuperSub[MarketShotsInsideBox] = append(hist.MetricValuesWithSuperSub[MarketShotsInsideBox], c.zone.inside+c.subZone.inside)
		hist.MetricValuesWithSuperSub[MarketShotsOnTargetOutsideBox] = append(hist.MetricValuesWithSuperSub[MarketShotsOnTargetOutsideBox], c.zone.outsideTarget+c.subZone.outsideTarget)
		hist.MetricValuesWithSuperSub[MarketShotsOnTargetInsideBox] = append(hist.MetricValuesWithSuperSub[MarketShotsOnTargetInsideBox], c.zone.insideTarget+c.subZone.insideTarget)
	}

	hist.Incomplete = incomplete.Load()
	log.Printf("[SOFA-PLAYER] Zebrano historię gracza %d: mecze=%d niepelna=%t", playerID, hist.Matches, hist.Incomplete)
	if !hist.Incomplete && hist.Matches > 0 {
		savePlayerCache(cacheKey, hist, client.statsFixture)
	}
	return hist, nil
}

func fetchSofaTeamDataComprehensive(client *ResilientClient, teamID string, matchLimit int, tournamentIDs string, targetPlayerNames []string) ([]*PlayerStatHistory, string, error) {
	log.Printf("[COMPREHENSIVE] Analiza kompleksowa drużyny %s (wymagani gracze: %d)", teamID, len(targetPlayerNames))
	squad, teamDisplayName, err := fetchSofaTeamSquad(client, teamID)
	if err != nil {
		log.Printf("[COMPREHENSIVE] Błąd pobierania składu %s: %v", teamID, err)
		return nil, "", err
	}

	lookupMap := make(map[string]int)
	for _, p := range squad {
		regSofaAliases(lookupMap, p.Name, p.ShortName, p.Slug, p.ID)
	}

	matchedPlayerIDs := make(map[int]SofaPlayerDetails)
	lookupStringMap := lookupMapInt(lookupMap)

	for _, targetName := range targetPlayerNames {
		matchedID := matchPlayerStrict(targetName, lookupStringMap)
		if matchedID != "" {
			idVal, _ := strconv.Atoi(matchedID)
			for _, p := range squad {
				if p.ID == idVal {
					matchedPlayerIDs[idVal] = p
					break
				}
			}
		} else {
			targetNorm := NormalizePlayerName(targetName)
			parts := strings.Fields(targetNorm)
			if len(parts) > 0 {
				last := parts[len(parts)-1]
				for _, p := range squad {
					pNorm := NormalizePlayerName(p.Name)
					pParts := strings.Fields(pNorm)
					if len(pParts) > 0 && pParts[len(pParts)-1] == last {
						matchedPlayerIDs[p.ID] = p
						break
					}
				}
			}
		}
	}

	var playersToFetch []SofaPlayerDetails
	if len(matchedPlayerIDs) > 0 {
		for _, p := range matchedPlayerIDs {
			playersToFetch = append(playersToFetch, p)
		}
		log.Printf("[COMPREHENSIVE] Dopasowano %d graczy do pobrania historii z oferty bukmacherskiej dla %s", len(playersToFetch), teamDisplayName)
	} else {
		if client.statsPrefetch {
			log.Printf("[STATS-BACKGROUND] SKIP_TEAM team_id=%s reason=no-matched-market-players", teamID)
			return nil, teamDisplayName, nil
		}
		playersToFetch = squad
		log.Printf("[COMPREHENSIVE] Brak pasujących filtrów graczy. Pobieranie historii dla CAŁEJ kadry (%d) dla %s", len(playersToFetch), teamDisplayName)
	}
	sort.Slice(playersToFetch, func(i, j int) bool { return playersToFetch[i].ID < playersToFetch[j].ID })
	client.report(0, len(playersToFetch))

	var finalHistories []*PlayerStatHistory
	var mu sync.Mutex
	var wg sync.WaitGroup
	playerConcurrency := 2
	if client.statsPrefetch {
		playerConcurrency = 1
	}
	sem := make(chan struct{}, playerConcurrency)

	for _, p := range playersToFetch {
		wg.Add(1)
		go func(player SofaPlayerDetails) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-client.ctx.Done():
				return
			}
			defer func() { <-sem }()
			if client.background && !sofaCooldownUntil().IsZero() {
				return
			}
			defer client.report(1, 0)
			client := client.withScope("zawodnik-" + player.Name + "-druzyna-" + teamDisplayName + "-proba-" + strconv.Itoa(matchLimit))

			h, err := fetchSofaPlayerCompleteHistory(client, player.ID, matchLimit, tournamentIDs)
			if err != nil {
				client.warn("Historia " + player.Name + ": " + err.Error())
				return
			}
			if h == nil || h.Matches == 0 {
				client.warn("Brak zakończonych meczów ze statystykami: " + player.Name)
				return
			}
			h.FullName = player.Name
			h.TeamID = teamID
			h.TeamName = teamDisplayName
			if err := archiveResponse(client.ctx, "historia-zawodnika-statystyki-proba-"+strconv.Itoa(h.Matches)+"-meczow", nil, "", map[string]any{"history": h, "fetchedAt": h.DataFetchedAt, "reusedFromCache": h.FromCache, "requestedMatchLimit": matchLimit, "tournamentIds": tournamentIDs}, nil); err != nil {
				client.recordArchiveError(err)
				return
			}

			mu.Lock()
			finalHistories = append(finalHistories, h)
			mu.Unlock()
		}(p)
	}

	wg.Wait()
	log.Printf("[COMPREHENSIVE] Zakończono analizę zespołu %s. Zwrócono %d profili statystycznych", teamDisplayName, len(finalHistories))
	if err := errors.Join(client.ctx.Err(), client.archiveError()); err != nil {
		return nil, teamDisplayName, err
	}
	if client.background {
		if err := sofaCooldownError(); err != nil {
			return finalHistories, teamDisplayName, err
		}
	}
	if client.statsPrefetch && len(finalHistories) != len(playersToFetch) {
		return finalHistories, teamDisplayName, fmt.Errorf("pobrano historie %d/%d dopasowanych zawodników", len(finalHistories), len(playersToFetch))
	}
	sort.Slice(finalHistories, func(i, j int) bool { return finalHistories[i].ID < finalHistories[j].ID })
	return finalHistories, teamDisplayName, nil
}

func regSofaAliases(m map[string]int, name, shortName, slug string, id int) {
	for _, raw := range []string{name, shortName, strings.ReplaceAll(slug, "-", " ")} {
		norm := NormalizePlayerName(raw)
		if norm == "" {
			continue
		}
		m[norm] = id
		parts := strings.Fields(norm)
		if len(parts) >= 2 {
			first := parts[0]
			last := parts[len(parts)-1]
			m[last] = id
			m[last+" "+first] = id
			if r := []rune(first); len(r) > 0 {
				m[string(r[0])+" "+last] = id
				m[last+" "+string(r[0])] = id
			}
		}
	}
}

func lookupMapInt(src map[string]int) map[string]string {
	res := make(map[string]string, len(src))
	for k, v := range src {
		res[k] = strconv.Itoa(v)
	}
	return res
}

func fetchSofaLineups(client *ResilientClient, eventID string) (map[string]bool, map[string]bool, bool, error) {
	if eventID == "" {
		return nil, nil, false, fmt.Errorf("brak ID meczu do pobrania składów")
	}
	log.Printf("[SOFA-LINEUP] Pobieranie składów dla meczu eventID=%s", eventID)
	body, err := client.GetSofa(fmt.Sprintf("/event/%s/lineups", eventID))
	if err != nil {
		log.Printf("[SOFA-LINEUP] Błąd pobierania składów: %v", err)
		return nil, nil, false, err
	}

	var res SofaLineupResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, nil, false, fmt.Errorf("odczyt składów: %w", err)
	}
	log.Printf("[SOFA-LINEUP] RESPONSE eventID=%s confirmed_present=%v home_players=%d away_players=%d", eventID, res.Confirmed != nil, len(res.Home.Players), len(res.Away.Players))
	if res.Confirmed == nil {
		return nil, nil, false, fmt.Errorf("odpowiedź składów nie zawiera statusu confirmed")
	}

	homeMap := make(map[string]bool)
	awayMap := make(map[string]bool)

	regLineup := func(m map[string]bool, p SofaLineupPlayer) {
		if p.Substitute == nil {
			return // Missing classification must not default to a starting player.
		}
		isStarter := !*p.Substitute
		if p.Player.ID > 0 {
			m["player:"+strconv.Itoa(p.Player.ID)] = isStarter
			position := p.Position
			if position == "" {
				position = p.Player.Position
			}
			client.state.mu.Lock()
			if client.state.lineupRoles == nil {
				client.state.lineupRoles = make(map[int]string)
			}
			client.state.lineupRoles[p.Player.ID] = position
			client.state.mu.Unlock()
		}
		for _, raw := range []string{p.Player.Name, p.Player.ShortName, strings.ReplaceAll(p.Player.Slug, "-", " ")} {
			norm := NormalizePlayerName(raw)
			if norm != "" {
				m[norm] = isStarter
			}
		}
	}

	for _, p := range res.Home.Players {
		regLineup(homeMap, p)
	}
	for _, p := range res.Away.Players {
		regLineup(awayMap, p)
	}

	if len(homeMap) == 0 && len(awayMap) == 0 {
		return nil, nil, false, fmt.Errorf("brak zawodników z oznaczeniem substitute w składach (confirmed=%v)", *res.Confirmed)
	}
	log.Printf("[SOFA-LINEUP] Składy załadowane (potwierdzone: %v) - Gospodarze: %d graczy, Goście: %d graczy", *res.Confirmed, len(res.Home.Players), len(res.Away.Players))
	return homeMap, awayMap, *res.Confirmed, nil
}

// ==========================================
// 5. PARSERY BUKMACHERSKIE I MAPOWANIE RYNKÓW
// ==========================================

var numRe = regexp.MustCompile(`(\d+(?:\.\d+)?)`)

func isHalfOrPeriodMarket(s string) bool {
	low := strings.ToLower(s)
	keywords := []string{
		"połow", "polow", "half", "1st", "2nd",
		"1. p", "2. p", "1.p", "2.p", "1 p", "2 p",
		"extra time", "kwart", "tercj",
	}
	for _, kw := range keywords {
		if strings.Contains(low, kw) {
			return true
		}
	}
	return false
}

func isUnsupportedMethodOrZone(s string) bool {
	low := removeDiacritics(strings.ToLower(s))
	markers := []string{
		"lewa noga", "lewej nogi", "prawa noga", "prawej nogi",
		"glowa", "glowka", "head",
		"podania", "podan", "pass",
		"kartki", "kartek", "card",
		"asyst",
	}
	for _, m := range markers {
		if strings.Contains(low, m) {
			return true
		}
	}
	return false
}

func mapZoneShotMarket(raw string) (CanonicalMarket, bool) {
	s := strings.ToLower(raw)
	sd := removeDiacritics(s)

	if !strings.Contains(s, "strzał") && !strings.Contains(sd, "strzal") && !strings.Contains(s, "shot") {
		return "", false
	}
	if strings.Contains(s, "zablokowan") || strings.Contains(s, "nieceln") || strings.Contains(s, "off target") || strings.Contains(s, "blocked") {
		return "", false
	}

	isOutside := strings.Contains(sd, "spoza") || strings.Contains(sd, "z dystansu") ||
		strings.Contains(s, "outside the box") || strings.Contains(s, "outside box")
	isInside := strings.Contains(sd, "z pola karnego") || strings.Contains(sd, "w polu karnym") ||
		strings.Contains(sd, "w szesnastce") || strings.Contains(sd, "z szesnastki") ||
		strings.Contains(s, "inside the box") || strings.Contains(s, "inside box")

	isOnTarget := strings.Contains(s, "celn") || strings.Contains(s, "on target")

	if isOutside {
		if isOnTarget {
			return MarketShotsOnTargetOutsideBox, true
		}
		return MarketShotsOutsideBox, true
	}
	if isInside {
		if isOnTarget {
			return MarketShotsOnTargetInsideBox, true
		}
		return MarketShotsInsideBox, true
	}
	return "", false
}

func mapMarketCanonical(raw string) (CanonicalMarket, bool) {
	s := strings.ToLower(raw)
	sd := removeDiacritics(s)

	if strings.Contains(s, "drużyn") || strings.Contains(sd, "druzyn") ||
		strings.Contains(s, "zespół") || strings.Contains(sd, "zespol") ||
		strings.Contains(s, "mecz") {
		return "", false
	}
	if isHalfOrPeriodMarket(raw) {
		return "", false
	}

	if zone, ok := mapZoneShotMarket(raw); ok {
		return zone, true
	}

	if isUnsupportedMethodOrZone(raw) {
		return "", false
	}

	if strings.Contains(sd, "przechwyt") || strings.Contains(s, "interception") {
		return MarketInterceptions, true
	}
	if strings.Contains(sd, "odbiorow na zawodniku") || strings.Contains(sd, "odbior na zawodnik") ||
		strings.Contains(sd, "odbiory na zawodniku") {
		return MarketDispossessed, true
	}
	if strings.Contains(sd, "odbior") || strings.Contains(s, "tackle") {
		return MarketTackles, true
	}

	switch {
	case strings.Contains(s, "zablokowan"):
		return MarketShotsBlocked, true
	case strings.Contains(s, "nieceln") || strings.Contains(s, "poza światło") || strings.Contains(s, "poza bramk"):
		return MarketShotsOffTarget, true
	case strings.Contains(s, "celn"):
		return MarketShotsOnTarget, true
	case strings.Contains(s, "strzał") || strings.Contains(sd, "strzal"):
		return MarketShotsTotal, true
	case strings.Contains(s, "disposs") || strings.Contains(s, "dispos"):
		return MarketDispossessed, true
	}

	switch {
	case strings.Contains(s, "wywalczon") || strings.Contains(s, "faulowany") || strings.Contains(sd, "faulowany") ||
		strings.Contains(sd, "fauli na") || strings.Contains(sd, "faule na") || strings.Contains(sd, "faul na"):
		return MarketFoulsDrawn, true
	case strings.Contains(s, "faul"):
		return MarketFoulsCommitted, true
	case strings.Contains(sd, "spalon") || strings.Contains(s, "offside"):
		return MarketOffsides, true
	}

	return "", false
}

func ParseSuperbetProps(rawJSON string) ([]NormalizedProp, error) {
	log.Printf("[PARSER-SB] Rozpoczynam parsowanie oferty Superbet...")
	var resp struct {
		Markets []struct {
			Name string `json:"name"`
			Odds []struct {
				Name       string            `json:"name"`
				Price      float64           `json:"price"`
				Status     string            `json:"status"`
				Specifiers map[string]string `json:"specifiers"`
			} `json:"odds"`
		} `json:"markets"`
	}
	if err := json.Unmarshal([]byte(rawJSON), &resp); err != nil {
		log.Printf("[PARSER-SB] Błąd unmarshalu JSON: %v", err)
		return nil, err
	}

	var props []NormalizedProp
	for _, m := range resp.Markets {
		if isHalfOrPeriodMarket(m.Name) || isUnsupportedMethodOrZone(m.Name) {
			continue
		}

		canon, ok := mapMarketCanonical(m.Name)
		if !ok {
			continue
		}

		for _, odd := range m.Odds {
			if odd.Status != "ACTIVE" && odd.Status != "" {
				continue
			}
			if isHalfOrPeriodMarket(odd.Name) || isUnsupportedMethodOrZone(odd.Name) {
				continue
			}

			pName := ""
			if odd.Specifiers != nil {
				pName = odd.Specifiers["player_name"]
				if pName == "" {
					pName = odd.Specifiers["player"]
				}
			}
			if pName == "" && strings.Contains(odd.Name, " - ") {
				pName = strings.TrimSpace(strings.SplitN(odd.Name, " - ", 2)[0])
			}
			if pName == "" {
				continue
			}

			line := 0.5
			if odd.Specifiers != nil {
				if l, exists := odd.Specifiers["total"]; exists {
					line, _ = strconv.ParseFloat(l, 64)
				}
			}
			if line == 0.5 {
				if match := numRe.FindStringSubmatch(odd.Name); len(match) > 1 {
					line, _ = strconv.ParseFloat(match[1], 64)
				}
			}

			lower := strings.ToLower(odd.Name)
			isUnder := strings.Contains(lower, "poniżej") || strings.Contains(lower, "under")

			props = append(props, NormalizedProp{
				Bookmaker:   BookmakerSuperbet,
				PlayerName:  pName,
				MarketType:  canon,
				MarketLabel: m.Name,
				Line:        line,
				IsUnder:     isUnder,
				Odds:        odd.Price,
			})
		}
	}
	log.Printf("[PARSER-SB] Sukces: sparsowano %d propozycji zakładów z Superbet", len(props))
	return props, nil
}

func ParseSTSProps(rawJSON string) ([]NormalizedProp, error) {
	log.Printf("[PARSER-STS] Rozpoczynam parsowanie oferty STS...")
	var root map[string]any
	if err := json.Unmarshal([]byte(rawJSON), &root); err != nil {
		lines := strings.Split(rawJSON, "\n")
		for _, l := range lines {
			if strings.Contains(l, `"P":`) {
				_ = json.Unmarshal([]byte(l), &root)
				break
			}
		}
	}

	pData, _ := root["P"].(map[string]any)
	if pData == nil {
		log.Printf("[PARSER-STS] Błąd: Brak obiektu P w odpowiedzi STS")
		return nil, fmt.Errorf("brak obiektu P w odpowiedzi STS")
	}

	isIDToken := func(s string) bool {
		matched, _ := regexp.MatchString(`^(\d+m\w+|f\d+|m\d+|\d+)$`, strings.TrimSpace(s))
		return matched
	}

	var props []NormalizedProp

	for _, v := range pData {
		bundle, ok := v.(map[string]any)
		if !ok {
			continue
		}
		markets, ok := bundle["m"].(map[string]any)
		if !ok {
			continue
		}

		for mID, mVal := range markets {
			mMap, _ := mVal.(map[string]any)
			linesMap, _ := mMap["l"].(map[string]any)

			for _, lVal := range linesMap {
				lMap, _ := lVal.(map[string]any)
				rawName, _ := lMap["n"].(string)
				if rawName == "" {
					continue
				}

				cleanName := strings.ReplaceAll(rawName, "(z dogrywką)", "")
				cleanName = strings.ReplaceAll(cleanName, "(z dogrywka)", "")
				cleanName = strings.TrimSpace(cleanName)

				if isHalfOrPeriodMarket(cleanName) {
					continue
				}

				pName := ""
				mType := ""
				var canon CanonicalMarket
				var okMarket bool

				if strings.Contains(cleanName, " - ") {
					pts := strings.SplitN(cleanName, " - ", 2)
					partA := strings.TrimSpace(pts[0])
					partB := strings.TrimSpace(pts[1])

					if c, matched := mapMarketCanonical(partB); matched {
						pName = partA
						mType = partB
						canon = c
						okMarket = true
					} else if c, matched := mapMarketCanonical(partA); matched {
						pName = partB
						mType = partA
						canon = c
						okMarket = true
					}
				}

				if !okMarket || pName == "" || isIDToken(pName) || isUnsupportedMethodOrZone(mType) {
					continue
				}

				pLow := strings.ToLower(pName)
				if strings.Contains(pLow, "drużyn") || strings.Contains(pLow, "druzyn") ||
					strings.Contains(pLow, "zespół") || strings.Contains(pLow, "zespol") ||
					strings.HasPrefix(pLow, "1.") || strings.HasPrefix(pLow, "2.") {
					continue
				}

				outcomes, _ := lMap["o"].(map[string]any)
				for outID, outVal := range outcomes {
					outMap, _ := outVal.(map[string]any)
					lineStr, _ := outMap["n"].(string)
					odds, _ := outMap["O"].(float64)

					if odds < 1.01 {
						continue
					}

					lowerLine := strings.ToLower(strings.TrimSpace(lineStr))
					isUnder := strings.Contains(lowerLine, "poniżej") ||
						strings.Contains(lowerLine, "under") ||
						strings.HasPrefix(lowerLine, "-")

					lineVal := 0.5
					cleanLineStr := strings.TrimSpace(lowerLine)
					if strings.HasSuffix(cleanLineStr, "+") {
						valStr := strings.TrimSuffix(cleanLineStr, "+")
						if parsed, err := strconv.ParseFloat(valStr, 64); err == nil {
							lineVal = parsed - 0.5
							isUnder = false
						}
					} else if m := numRe.FindStringSubmatch(cleanLineStr); len(m) > 1 {
						if parsed, err := strconv.ParseFloat(m[1], 64); err == nil {
							lineVal = parsed
						}
					}

					props = append(props, NormalizedProp{
						Bookmaker:   BookmakerSTS,
						PlayerName:  pName,
						MarketType:  canon,
						MarketLabel: mType,
						Line:        lineVal,
						IsUnder:     isUnder,
						Odds:        odds,
						MarketID:    mID,
						SelectionID: outID,
					})
				}
			}
		}
	}

	log.Printf("[PARSER-STS] Sukces: sparsowano %d propozycji zakładów z STS", len(props))
	return props, nil
}

type protoNode struct {
	fieldNo  int
	wireType int
	valInt   uint64
	valF64   float64
	text     string
	children []protoNode
}

func decodeProtoVarint(buf []byte, pos int) (uint64, int) {
	var res uint64
	var shift uint
	var bytesRead int
	for pos+bytesRead < len(buf) {
		b := buf[pos+bytesRead]
		bytesRead++
		res |= uint64(b&0x7F) << shift
		shift += 7
		if (b & 0x80) == 0 {
			break
		}
	}
	return res, bytesRead
}

func parseProtoNodes(buf []byte) []protoNode {
	return parseProtoNodesDepth(buf, 0)
}

func parseProtoNodesDepth(buf []byte, depth int) []protoNode {
	if depth > 32 {
		return nil
	}
	var nodes []protoNode
	pos := 0

	for pos < len(buf) {
		tag, n := decodeProtoVarint(buf, pos)
		if n == 0 {
			break
		}
		pos += n
		wireType := int(tag & 7)
		fieldNo := int(tag >> 3)
		if fieldNo == 0 {
			break
		}

		switch wireType {
		case 0:
			val, r := decodeProtoVarint(buf, pos)
			pos += r
			nodes = append(nodes, protoNode{fieldNo: fieldNo, wireType: wireType, valInt: val})
		case 1:
			if pos+8 > len(buf) {
				return nodes
			}
			bits := binary.LittleEndian.Uint64(buf[pos : pos+8])
			pos += 8
			nodes = append(nodes, protoNode{fieldNo: fieldNo, wireType: wireType, valF64: math.Float64frombits(bits)})
		case 2:
			l, r := decodeProtoVarint(buf, pos)
			pos += r
			if l > uint64(len(buf)-pos) {
				return nodes
			}
			lenVal := int(l)
			slice := buf[pos : pos+lenVal]
			pos += lenVal

			if utf8.Valid(slice) && !bytes.ContainsAny(slice, "\x00\x01\x02\x03\x04\x05") && len(bytes.TrimSpace(slice)) > 0 {
				nodes = append(nodes, protoNode{fieldNo: fieldNo, wireType: wireType, text: strings.TrimSpace(string(slice))})
			} else {
				nodes = append(nodes, protoNode{fieldNo: fieldNo, wireType: wireType, children: parseProtoNodesDepth(slice, depth+1)})
			}
		case 5:
			if pos+4 > len(buf) {
				return nodes
			}
			bits := binary.LittleEndian.Uint32(buf[pos : pos+4])
			pos += 4
			nodes = append(nodes, protoNode{fieldNo: fieldNo, wireType: wireType, valF64: float64(math.Float32frombits(bits))})
		default:
			return nodes
		}
	}
	return nodes
}

var betclicLinePattern = regexp.MustCompile(`(?i)(.+?)\s+(Powyżej|Poniżej)\s+([\d,.]+)`)

func ParseBetclicProps(rawBuf []byte) []NormalizedProp {
	log.Printf("[PARSER-BETCLIC] Rozpoczynam dekodowanie Protobuf Betclic (rozmiar: %d bajtów)...", len(rawBuf))
	var frames [][]byte
	offset := 0
	for offset+5 <= len(rawBuf) {
		frameLen := int(binary.BigEndian.Uint32(rawBuf[offset+1 : offset+5]))
		offset += 5
		if frameLen > 0 && offset+frameLen <= len(rawBuf) {
			frames = append(frames, rawBuf[offset:offset+frameLen])
			offset += frameLen
		} else {
			frames = append(frames, rawBuf[offset-5:])
			break
		}
	}

	type parserCtx struct {
		market    string
		playerRaw string
		marketID  string
		selID     string
	}

	var props []NormalizedProp

	var walk func(nodes []protoNode, ctx parserCtx)
	walk = func(nodes []protoNode, ctx parserCtx) {
		current := ctx

		for _, n := range nodes {
			if n.wireType == 0 && n.valInt > 1000000000000000 {
				idStr := strconv.FormatUint(n.valInt, 10)
				if current.marketID == "" {
					current.marketID = idStr
				} else {
					current.selID = idStr
				}
			}
			if n.text != "" {
				t := n.text
				if len(t) > 3 && !strings.HasPrefix(t, "contestant:") && !strings.HasPrefix(t, "ca_") {
					if strings.Contains(t, "-") || strings.Contains(t, "Liczba") || strings.Contains(t, "Opta") {
						current.market = t
					} else if strings.Contains(t, " ") {
						current.playerRaw = t
					}
				}
			}
		}

		for _, n := range nodes {
			if n.wireType == 1 && n.valF64 >= 1.01 && n.valF64 <= 500.0 {
				if isHalfOrPeriodMarket(current.market) || isHalfOrPeriodMarket(current.playerRaw) {
					continue
				}
				if isUnsupportedMethodOrZone(current.market) || isUnsupportedMethodOrZone(current.playerRaw) {
					continue
				}

				m := betclicLinePattern.FindStringSubmatch(current.playerRaw)
				if len(m) == 4 {
					player := strings.TrimSpace(m[1])
					dir := strings.ToLower(m[2])
					line, _ := strconv.ParseFloat(strings.ReplaceAll(m[3], ",", "."), 64)

					canon, ok := mapMarketCanonical(current.market)
					if ok {
						props = append(props, NormalizedProp{
							Bookmaker:   BookmakerBetclic,
							PlayerName:  player,
							MarketType:  canon,
							MarketLabel: current.market,
							Line:        line,
							IsUnder:     strings.Contains(dir, "poniżej"),
							Odds:        math.Round(n.valF64*100) / 100,
							MarketID:    current.marketID,
							SelectionID: current.selID,
						})
					}
				}
			}
		}

		for _, n := range nodes {
			if len(n.children) > 0 {
				walk(n.children, current)
			}
		}
	}

	for _, f := range frames {
		walk(parseProtoNodes(f), parserCtx{market: "Statystyki"})
	}

	var unique []NormalizedProp
	seen := make(map[string]bool)
	for _, p := range props {
		key := fmt.Sprintf("%s_%s_%s_%.2f_%.2f", p.PlayerName, p.MarketType, p.MarketLabel, p.Line, p.Odds)
		if !seen[key] {
			seen[key] = true
			unique = append(unique, p)
		}
	}

	log.Printf("[PARSER-BETCLIC] Sukces: wyodrębniono %d unikalnych kursów z Betclic", len(unique))
	return unique
}

// ==========================================
// 6. ADAPTACYJNY FUZZY MATCHING I STATYSTYKA
// ==========================================

func NormalizePlayerName(s string) string {
	s = removeDiacritics(s)
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, ",", " ")
	s = strings.ReplaceAll(s, ".", " ")
	s = strings.ReplaceAll(s, "-", " ")

	stopWords := map[string]bool{
		"de": true, "del": true, "la": true, "las": true,
		"los": true, "da": true, "das": true, "dos": true,
		"jr": true, "junior": true,
	}

	parts := strings.Fields(s)
	var filtered []string
	for _, p := range parts {
		if _, err := strconv.Atoi(p); err == nil {
			continue
		}
		if stopWords[p] {
			continue
		}
		filtered = append(filtered, p)
	}
	return strings.TrimSpace(strings.Join(filtered, " "))
}

func matchPlayerStrict(targetName string, playerMap map[string]string) string {
	targetNorm := NormalizePlayerName(targetName)
	if targetNorm == "" {
		return ""
	}
	if id, ok := playerMap[targetNorm]; ok {
		return id
	}

	parts := strings.Fields(targetNorm)
	if len(parts) == 0 {
		return ""
	}

	if len(parts) >= 2 {
		first := parts[0]
		last := parts[len(parts)-1]

		if len(first) == 1 {
			targetInitial := rune(first[0])
			for candName, id := range playerMap {
				candParts := strings.Fields(candName)
				if len(candParts) >= 2 && candParts[len(candParts)-1] == last {
					if rune(candParts[0][0]) == targetInitial {
						return id
					}
				}
			}
		}

		if len(last) == 1 {
			targetInitial := rune(last[0])
			for candName, id := range playerMap {
				candParts := strings.Fields(candName)
				if len(candParts) >= 2 && candParts[len(candParts)-1] == first {
					if rune(candParts[0][0]) == targetInitial {
						return id
					}
				}
			}
		}

		reversed := last + " " + strings.Join(parts[:len(parts)-1], " ")
		if id, ok := playerMap[reversed]; ok {
			return id
		}
	}

	surname := parts[len(parts)-1]
	var candidateIDs []string
	for fullName, id := range playerMap {
		fParts := strings.Fields(fullName)
		if len(fParts) > 0 && fParts[len(fParts)-1] == surname {
			candidateIDs = append(candidateIDs, id)
		}
	}
	if len(candidateIDs) == 1 {
		return candidateIDs[0]
	}

	var scoredCandidates []struct {
		id    string
		score int
	}
	bestScore := 0

	for fullName, id := range playerMap {
		candTokens := strings.Fields(fullName)
		score := 0
		for _, p := range parts {
			for _, ct := range candTokens {
				if p == ct {
					score += 2
				} else if strings.HasPrefix(ct, p) || strings.HasPrefix(p, ct) {
					score += 1
				}
			}
		}
		if score > bestScore {
			bestScore = score
		}
		if score > 0 {
			scoredCandidates = append(scoredCandidates, struct {
				id    string
				score int
			}{id: id, score: score})
		}
	}

	if bestScore >= 2 {
		var matched string
		for _, sc := range scoredCandidates {
			if sc.score == bestScore {
				if matched == "" {
					matched = sc.id
				} else if matched != sc.id {
					return ""
				}
			}
		}
		return matched
	}

	return ""
}

func fact(n int) float64 {
	res := 1.0
	for i := 2; i <= n; i++ {
		res *= float64(i)
	}
	return res
}

func poissonProb(k int, lambda float64) float64 {
	if lambda <= 0.0001 {
		if k == 0 {
			return 1.0
		}
		return 0.0
	}
	return (math.Pow(lambda, float64(k)) * math.Exp(-lambda)) / fact(k)
}

func calcPoissonOver(line float64, lambda float64) float64 {
	threshold := int(math.Floor(line))
	underProb := 0.0
	for i := 0; i <= threshold; i++ {
		underProb += poissonProb(i, lambda)
	}
	res := 1.0 - underProb
	if res < 0.0 {
		return 0.0
	}
	return res
}

func calcPoissonUnder(line float64, lambda float64) float64 {
	threshold := int(math.Floor(line))
	underProb := 0.0
	for i := 0; i <= threshold; i++ {
		underProb += poissonProb(i, lambda)
	}
	if underProb > 1.0 {
		return 1.0
	}
	return underProb
}

// Context v2 uses match counts, never per90. Ratings and table rows belong to
// MatchOverview and are deliberately absent from fairModelInput.
const fairModelVersion = "context-count-v2"

type PlayerMatchContext struct {
	TeamID     int    `json:"teamId"`
	OpponentID int    `json:"opponentId"`
	LeagueID   int    `json:"leagueId"`
	SeasonID   int    `json:"seasonId"`
	Venue      string `json:"venue"`
}

type FairModelDetails struct {
	Version         string   `json:"version"`
	Distribution    string   `json:"distribution"`
	Probability     float64  `json:"probability"`
	BaseMean        float64  `json:"baseMean"`
	ExpectedCount   float64  `json:"expectedCount"`
	MatchupFactor   float64  `json:"matchupFactor"`
	EffectiveSample float64  `json:"effectiveSample"`
	ContextMatches  int      `json:"contextMatches"`
	Role            string   `json:"role"`
	Venue           string   `json:"venue"`
	TeamID          string   `json:"teamId"`
	OpponentID      string   `json:"opponentId"`
	Features        []string `json:"features"`
	Notes           []string `json:"notes"`
}

type TableSummary struct {
	Position int `json:"position"`
	Matches  int `json:"matches"`
	Points   int `json:"points"`
}

type TeamOverview struct {
	Flow         *TeamFlowOverview  `json:"flow,omitempty"`
	Name         string             `json:"name"`
	Venue        string             `json:"venue"`
	Table        *TableSummary      `json:"table,omitempty"`
	VenueTable   *TableSummary      `json:"venueTable,omitempty"`
	Form         []string           `json:"form"`
	VenueForm    []string           `json:"venueForm"`
	StatsMatches int                `json:"statsMatches"`
	Metrics      map[string]float64 `json:"metrics"`
}

type MatchOverview struct {
	AsOf     time.Time                `json:"asOf"`
	LeagueID int                      `json:"leagueId"`
	SeasonID int                      `json:"seasonId"`
	Teams    map[string]*TeamOverview `json:"teams"`
	Notes    []string                 `json:"notes"`
}

type SofaFlowScore struct {
	Current    *int `json:"current"`
	Normaltime *int `json:"normaltime"`
}

type FlowWindow struct {
	Points   int     `json:"points"`
	Mean     float64 `json:"mean"`
	Positive int     `json:"positive"`
	Negative int     `json:"negative"`
}

type FlowStateSummary struct {
	State   string     `json:"state"`
	Matches int        `json:"matches"`
	Window  FlowWindow `json:"window"`
}

type FlowReactionSummary struct {
	Kind    string  `json:"kind"`
	Samples int     `json:"samples"`
	Matches int     `json:"matches"`
	Before  float64 `json:"before"`
	After   float64 `json:"after"`
	Delta   float64 `json:"delta"`
}

type FlowMatchReference struct {
	EventID   string    `json:"eventId"`
	Revision  string    `json:"revision"`
	StartedAt time.Time `json:"startedAt"`
	Opponent  string    `json:"opponent"`
	Venue     string    `json:"venue"`
	HomeName  string    `json:"homeName"`
	AwayName  string    `json:"awayName"`
	HomeScore *int      `json:"homeScore"`
	AwayScore *int      `json:"awayScore"`
	Available bool      `json:"available"`
	Reason    string    `json:"reason,omitempty"`
}

type TeamFlowOverview struct {
	Requested int                   `json:"requested"`
	Available int                   `json:"available"`
	Matches   []FlowMatchReference  `json:"matches"`
	States    []FlowStateSummary    `json:"states"`
	Reactions []FlowReactionSummary `json:"reactions"`
	Notes     []string              `json:"notes"`
}

type FlowGraphPoint struct {
	Minute float64 `json:"minute"`
	Value  float64 `json:"value"`
	State  string  `json:"state,omitempty"`
}

type FlowIncident struct {
	ID        int64             `json:"id"`
	Type      string            `json:"incidentType"`
	Class     string            `json:"incidentClass"`
	Time      int               `json:"time"`
	AddedTime int               `json:"addedTime"`
	IsHome    *bool             `json:"isHome"`
	HomeScore *int              `json:"homeScore"`
	AwayScore *int              `json:"awayScore"`
	Rescinded bool              `json:"rescinded"`
	Player    SofaPlayerDetails `json:"player"`
	PlayerIn  SofaPlayerDetails `json:"playerIn"`
	PlayerOut SofaPlayerDetails `json:"playerOut"`
}

type FlowReaction struct {
	Kind        string     `json:"kind"`
	Minute      float64    `json:"minute"`
	Clock       string     `json:"clock"`
	IsHome      *bool      `json:"isHome"`
	Description string     `json:"description"`
	HomeScore   *int       `json:"homeScore"`
	AwayScore   *int       `json:"awayScore"`
	StateBefore string     `json:"stateBefore"`
	StateAfter  string     `json:"stateAfter"`
	Before      FlowWindow `json:"before"`
	After       FlowWindow `json:"after"`
	Eligible    bool       `json:"eligible"`
}

type HistoricalMatchFlow struct {
	Version       int              `json:"version"`
	EventID       string           `json:"eventId"`
	Revision      string           `json:"revision"`
	FetchedAt     time.Time        `json:"fetchedAt"`
	HomeID        int              `json:"homeId"`
	AwayID        int              `json:"awayId"`
	HomeName      string           `json:"homeName"`
	AwayName      string           `json:"awayName"`
	HomeScore     *int             `json:"homeScore"`
	AwayScore     *int             `json:"awayScore"`
	TimelineValid bool             `json:"timelineValid"`
	GraphPoints   []FlowGraphPoint `json:"graphPoints"`
	Reactions     []FlowReaction   `json:"reactions"`
	Notes         []string         `json:"notes"`
}

const matchFlowVersion = 1

var matchFlowRevisionPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func flowClock(incident FlowIncident) float64 {
	if incident.AddedTime > 0 && (incident.Time == 45 || incident.Time == 90) {
		return float64(incident.Time) + .5
	}
	return float64(incident.Time)
}

func flowState(home, away int) string {
	if home > away {
		return "HOME_LEAD"
	}
	if home < away {
		return "AWAY_LEAD"
	}
	return "DRAW"
}

func addFlowPoint(window *FlowWindow, value float64) {
	window.Mean = (window.Mean*float64(window.Points) + value) / float64(window.Points+1)
	window.Points++
	if value > 0 {
		window.Positive++
	} else if value < 0 {
		window.Negative++
	}
}

func flowPerspective(window FlowWindow, home bool) FlowWindow {
	if !home {
		window.Mean = -window.Mean
		window.Positive, window.Negative = window.Negative, window.Positive
	}
	return window
}

func flowCachePath(eventID, revision string) string {
	return filepath.Join(serverDataDir, "match_flow_cache", "mecz-"+eventID, "wersja-"+revision, "przebieg-meczu.json")
}

func readMatchFlow(eventID, revision string) (*HistoricalMatchFlow, error) {
	if !marketEventIDPattern.MatchString(eventID) || !matchFlowRevisionPattern.MatchString(revision) {
		return nil, fmt.Errorf("niepoprawny identyfikator przebiegu meczu")
	}
	raw, err := os.ReadFile(flowCachePath(eventID, revision))
	if err != nil {
		return nil, err
	}
	var flow HistoricalMatchFlow
	if err := json.Unmarshal(raw, &flow); err != nil {
		return nil, err
	}
	if flow.Version != matchFlowVersion || flow.EventID != eventID || flow.Revision != revision {
		return nil, fmt.Errorf("niezgodna wersja przebiegu meczu")
	}
	return &flow, nil
}

func (s *calendarServer) registerMatchFlowAPI(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/match-flow", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			apiError(w, 405, fmt.Errorf("wymagana metoda GET"))
			return
		}
		id, revision := r.URL.Query().Get("eventId"), r.URL.Query().Get("revision")
		if !marketEventIDPattern.MatchString(id) || !matchFlowRevisionPattern.MatchString(revision) {
			apiError(w, 400, fmt.Errorf("niepoprawne ID lub wersja przebiegu"))
			return
		}
		flow, err := readMatchFlow(id, revision)
		if err != nil {
			apiError(w, 404, fmt.Errorf("brak zapisanej wersji przebiegu meczu"))
			return
		}
		// Read-only: opening a chart cannot start new SofaScore requests.
		jsonResponse(w, 200, flow)
	})
}

func fetchHistoricalMatchFlow(client *ResilientClient, ref FlowMatchReference, homeID, awayID int, regulationScoreKnown bool) (*HistoricalMatchFlow, error) {
	ttl := 7 * 24 * time.Hour
	if time.Since(ref.StartedAt) < 24*time.Hour {
		ttl = 30 * time.Minute
	}
	indexPath := filepath.Join(serverDataDir, "match_flow_cache", "mecz-"+ref.EventID, "ostatnia-wersja.json")
	var index struct {
		Revision  string    `json:"revision"`
		FetchedAt time.Time `json:"fetchedAt"`
	}
	if raw, err := os.ReadFile(indexPath); err == nil && json.Unmarshal(raw, &index) == nil && time.Since(index.FetchedAt) >= 0 && time.Since(index.FetchedAt) < ttl {
		if flow, err := readMatchFlow(ref.EventID, index.Revision); err == nil {
			return flow, nil
		}
	}
	var graph struct {
		GraphPoints   []FlowGraphPoint `json:"graphPoints"`
		PeriodTime    int              `json:"periodTime"`
		PeriodCount   int              `json:"periodCount"`
		OvertimeCount *int             `json:"overtimeCount"`
	}
	_, err := getSofaContextJSON(client, "/event/"+ref.EventID+"/graph", ttl, &graph)
	if err != nil {
		return nil, err
	}
	if len(graph.GraphPoints) == 0 || graph.PeriodTime != 45 || graph.PeriodCount != 2 {
		return nil, fmt.Errorf("brak wykresu momentum dla regulaminowych dwóch połów")
	}
	var response struct {
		Incidents []FlowIncident `json:"incidents"`
	}
	_, err = getSofaContextJSON(client, "/event/"+ref.EventID+"/incidents", ttl, &response)
	if err != nil {
		return nil, err
	}
	flow := &HistoricalMatchFlow{Version: matchFlowVersion, EventID: ref.EventID, FetchedAt: time.Now().UTC(), HomeID: homeID, AwayID: awayID, HomeName: ref.HomeName, AwayName: ref.AwayName, HomeScore: ref.HomeScore, AwayScore: ref.AwayScore, TimelineValid: true, GraphPoints: []FlowGraphPoint{}, Reactions: []FlowReaction{}, Notes: []string{}}
	if graph.OvertimeCount != nil && *graph.OvertimeCount > 0 {
		flow.Notes = append(flow.Notes, "Podgląd obejmuje regulaminowe 90 minut; dogrywka i seria karnych są pominięte.")
		if !regulationScoreKnown {
			flow.TimelineValid = false
		}
	}
	sort.SliceStable(response.Incidents, func(i, j int) bool {
		a, b := response.Incidents[i], response.Incidents[j]
		if a.Time != b.Time {
			return a.Time < b.Time
		}
		if a.AddedTime != b.AddedTime {
			return a.AddedTime < b.AddedTime
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		if a.Type == "goal" {
			aKnown, bKnown := a.HomeScore != nil && a.AwayScore != nil, b.HomeScore != nil && b.AwayScore != nil
			if aKnown != bKnown {
				return aKnown
			}
			if aKnown {
				return *a.HomeScore+*a.AwayScore < *b.HomeScore+*b.AwayScore
			}
		}
		return false
	})
	home, away := 0, 0
	seen := map[int64]bool{}
	for _, incident := range response.Incidents {
		if incident.Rescinded || incident.Time < 0 || incident.Time > 90 || incident.AddedTime >= 999 {
			continue
		}
		if incident.ID > 0 && seen[incident.ID] {
			continue
		}
		if incident.ID > 0 {
			seen[incident.ID] = true
		}
		kind := ""
		switch incident.Type {
		case "goal":
			kind = "GOAL"
		case "substitution":
			kind = "SUBSTITUTION"
		case "card":
			if incident.Class == "red" || incident.Class == "yellowRed" {
				kind = "RED_CARD"
			} else if incident.Class == "yellow" {
				kind = "YELLOW_CARD"
			}
		}
		if kind == "" {
			continue
		}
		r := FlowReaction{Kind: kind, Minute: flowClock(incident), Clock: strconv.Itoa(incident.Time), IsHome: incident.IsHome, HomeScore: incident.HomeScore, AwayScore: incident.AwayScore, StateBefore: flowState(home, away)}
		if incident.AddedTime > 0 {
			r.Clock += "+" + strconv.Itoa(incident.AddedTime)
		}
		r.Clock += "′"
		r.Description = incident.Player.Name
		if kind == "SUBSTITUTION" {
			r.Description = incident.PlayerOut.Name + " → " + incident.PlayerIn.Name
		}
		if kind == "GOAL" {
			if incident.HomeScore == nil || incident.AwayScore == nil || *incident.HomeScore < home || *incident.AwayScore < away || (*incident.HomeScore-home)+(*incident.AwayScore-away) != 1 {
				flow.TimelineValid = false
			} else {
				// The score, not the scorer's team, resolves own goals correctly.
				isHome := *incident.HomeScore > home
				r.IsHome = &isHome
			}
			if incident.HomeScore != nil && incident.AwayScore != nil {
				home, away = *incident.HomeScore, *incident.AwayScore
			}
		}
		r.StateAfter = flowState(home, away)
		flow.Reactions = append(flow.Reactions, r)
	}
	if ref.HomeScore == nil || ref.AwayScore == nil || home != *ref.HomeScore || away != *ref.AwayScore {
		flow.TimelineValid = false
	}
	if !flow.TimelineValid {
		flow.Notes = append(flow.Notes, "Incydenty nie odtwarzają wyniku końcowego. Stany wyniku i agregaty reakcji są pominięte.")
	}
	sort.SliceStable(graph.GraphPoints, func(i, j int) bool { return graph.GraphPoints[i].Minute < graph.GraphPoints[j].Minute })
	lastMinute := -1.0
	for _, point := range graph.GraphPoints {
		if math.IsNaN(point.Minute) || math.IsInf(point.Minute, 0) || point.Minute <= 0 || point.Minute > 90.5 || point.Minute == lastMinute || math.IsNaN(point.Value) || math.IsInf(point.Value, 0) {
			continue
		}
		lastMinute = point.Minute
		point.State = "UNKNOWN"
		if flow.TimelineValid && point.Minute == math.Floor(point.Minute) {
			h, a := 0, 0
			ambiguous := false
			for _, r := range flow.Reactions {
				if r.Kind == "GOAL" {
					if r.Minute == point.Minute {
						ambiguous = true
					}
					if r.Minute < point.Minute && r.HomeScore != nil && r.AwayScore != nil {
						h, a = *r.HomeScore, *r.AwayScore
					}
				}
			}
			if !ambiguous {
				point.State = flowState(h, a)
			}
		}
		flow.GraphPoints = append(flow.GraphPoints, point)
	}
	if len(flow.GraphPoints) == 0 {
		return nil, fmt.Errorf("brak poprawnych punktów wykresu momentum")
	}
	for i := range flow.Reactions {
		r := &flow.Reactions[i]
		if r.Minute != math.Floor(r.Minute) {
			continue
		} // Added time is compressed into one ambiguous graph slot.
		start, end := 0.0, 45.0
		if r.Minute > 45 {
			start, end = 45, 90
		}
		beforeStart, afterEnd := math.Max(start, r.Minute-5), math.Min(end, r.Minute+5)
		confounded := false
		for j, other := range flow.Reactions {
			if i == j {
				continue
			}
			if other.Minute == r.Minute {
				// Only a batch of substitutions for the same side is one incident.
				if r.Kind != "SUBSTITUTION" || other.Kind != r.Kind || r.IsHome == nil || other.IsHome == nil || *r.IsHome != *other.IsHome {
					confounded = true
				}
				if r.Kind != "GOAL" && other.Kind == "GOAL" {
					r.StateBefore, r.StateAfter = "UNKNOWN", "UNKNOWN"
				}
				continue
			}
			if other.Minute < r.Minute && other.Minute > beforeStart {
				beforeStart = other.Minute
			}
			if other.Minute > r.Minute && other.Minute < afterEnd {
				afterEnd = other.Minute
			}
		}
		for _, point := range flow.GraphPoints {
			if point.Minute != math.Floor(point.Minute) || point.Minute <= start || point.Minute > end {
				continue
			}
			// Skip every incident minute; ordering within a minute is unknown.
			atIncident := false
			for _, other := range flow.Reactions {
				if other.Minute == point.Minute {
					atIncident = true
					break
				}
			}
			if atIncident {
				continue
			}
			if point.Minute >= beforeStart && point.Minute < r.Minute {
				addFlowPoint(&r.Before, point.Value)
			}
			if point.Minute > r.Minute && point.Minute <= afterEnd {
				addFlowPoint(&r.After, point.Value)
			}
		}
		r.Eligible = flow.TimelineValid && !confounded && r.IsHome != nil && r.Before.Points >= 3 && r.After.Points >= 3
	}
	flow.Notes = append(flow.Notes, "Momentum jest indeksem przebiegu gry, nie liczbą strzałów ani xG. Reakcje porównują do 5 minut przed i po incydencie; inne incydenty i przerwa skracają okno. To obserwacja, nie dowód przyczynowości.", "Minuta incydentu i ułamkowe punkty doliczonego czasu nie wchodzą do agregatów. Co najmniej 3 punkty po każdej stronie są wymagane do agregacji reakcji.", "Nakładające się incydenty w tej samej minucie są pomijane w agregatach reakcji; wyjątkiem jest pakiet zmian jednej drużyny, liczony jako jedno okno.")
	// Stable content identity excludes the observation time. A -> B -> A reuses A.
	identity := *flow
	identity.FetchedAt = time.Time{}
	raw, err := json.Marshal(identity)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	flow.Revision = fmt.Sprintf("%x", digest)
	path := flowCachePath(ref.EventID, flow.Revision)
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, err
	}
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := atomicJSON(path, flow); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	archiveDir := filepath.Join(archiveRoot, "przebieg-meczow", "mecz-"+ref.EventID, "wersja-"+flow.Revision)
	archivePath := filepath.Join(archiveDir, archiveName(ref.HomeName+"-vs-"+ref.AwayName)+"_przebieg-meczu.json.gz")
	if _, err := os.Stat(archivePath); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(archiveDir, 0750); err != nil {
			client.recordArchiveError(err)
			return nil, err
		}
		data, err := json.MarshalIndent(flow, "", "  ")
		if err != nil {
			return nil, err
		}
		if err := writeGzip(archivePath, data); err != nil {
			client.recordArchiveError(err)
			return nil, err
		}
	} else if err != nil {
		client.recordArchiveError(err)
		return nil, err
	}
	index.Revision, index.FetchedAt = flow.Revision, flow.FetchedAt
	if err := atomicJSON(indexPath, index); err != nil {
		return nil, err
	}
	return flow, nil
}

func fetchTeamFlowOverview(client *ResilientClient, teamID string, leagueID int, shared map[string]*HistoricalMatchFlow) *TeamFlowOverview {
	result := &TeamFlowOverview{Requested: 5, Matches: []FlowMatchReference{}, States: []FlowStateSummary{}, Reactions: []FlowReactionSummary{}, Notes: []string{}}
	var events SofaTeamEventsResponse
	_, err := getSofaContextJSON(client, "/team/"+teamID+"/events/last/0", 6*time.Hour, &events)
	if err != nil {
		result.Notes = append(result.Notes, "Nie udało się pobrać historii meczów: "+err.Error())
		return result
	}
	sort.Slice(events.Events, func(i, j int) bool { return events.Events[i].StartTS > events.Events[j].StartTS })
	stateTotals := map[string]*FlowStateSummary{}
	type reactionTotal struct {
		row     FlowReactionSummary
		matches map[string]bool
	}
	reactionTotals := map[string]*reactionTotal{}
	for _, event := range events.Events {
		if client.ctx.Err() != nil {
			break
		}
		if len(result.Matches) >= result.Requested {
			break
		}
		if event.Status.Code != 100 || event.StartTS <= 0 || (!client.statsFixture.StartTime.IsZero() && event.StartTS >= client.statsFixture.StartTime.Unix()) || (leagueID > 0 && event.Tournament.UniqueTournament.ID != leagueID) {
			continue
		}
		home := strconv.Itoa(event.HomeTeam.ID) == teamID
		if !home && strconv.Itoa(event.AwayTeam.ID) != teamID {
			continue
		}
		ref := FlowMatchReference{EventID: strconv.Itoa(event.ID), StartedAt: time.Unix(event.StartTS, 0).UTC(), HomeName: event.HomeTeam.Name, AwayName: event.AwayTeam.Name, HomeScore: event.HomeScore.Normaltime, AwayScore: event.AwayScore.Normaltime, Venue: "HOME", Opponent: event.AwayTeam.Name}
		if ref.HomeScore == nil {
			ref.HomeScore = event.HomeScore.Current
		}
		if ref.AwayScore == nil {
			ref.AwayScore = event.AwayScore.Current
		}
		if !home {
			ref.Venue, ref.Opponent = "AWAY", event.HomeTeam.Name
		}
		flow := shared[ref.EventID]
		var err error
		if flow == nil {
			flow, err = fetchHistoricalMatchFlow(client.withScope("przebieg-"+ref.HomeName+"-vs-"+ref.AwayName+"-mecz-"+ref.EventID), ref, event.HomeTeam.ID, event.AwayTeam.ID, event.HomeScore.Normaltime != nil && event.AwayScore.Normaltime != nil)
		}
		if err != nil {
			ref.Reason = err.Error()
			result.Matches = append(result.Matches, ref)
			continue
		}
		shared[ref.EventID] = flow
		ref.Available, ref.Revision = true, flow.Revision
		result.Available++
		result.Matches = append(result.Matches, ref)
		if !flow.TimelineValid {
			continue
		}
		matchStates := map[string]bool{}
		for _, point := range flow.GraphPoints {
			if point.State == "UNKNOWN" {
				continue
			}
			state := "DRAW"
			value := point.Value
			if (home && point.State == "HOME_LEAD") || (!home && point.State == "AWAY_LEAD") {
				state = "LEADING"
			} else if point.State != "DRAW" {
				state = "TRAILING"
			}
			if !home {
				value = -value
			}
			if stateTotals[state] == nil {
				stateTotals[state] = &FlowStateSummary{State: state}
			}
			addFlowPoint(&stateTotals[state].Window, value)
			matchStates[state] = true
		}
		for state := range matchStates {
			stateTotals[state].Matches++
		}
		seenReactions := map[string]bool{}
		for _, r := range flow.Reactions {
			if !r.Eligible || r.IsHome == nil {
				continue
			}
			own := *r.IsHome == home
			keys := []string{}
			if r.Kind == "GOAL" {
				if own {
					keys = append(keys, "AFTER_SCORED")
				} else {
					keys = append(keys, "AFTER_CONCEDED")
				}
				leading := "HOME_LEAD"
				trailing := "AWAY_LEAD"
				if !home {
					leading, trailing = trailing, leading
				}
				if r.StateBefore != leading && r.StateAfter == leading {
					keys = append(keys, "TOOK_LEAD")
				}
				if r.StateBefore != trailing && r.StateAfter == trailing {
					keys = append(keys, "FELL_BEHIND")
				}
				if r.StateBefore != "DRAW" && r.StateAfter == "DRAW" {
					if own {
						keys = append(keys, "EQUALIZED")
					} else {
						keys = append(keys, "LOST_LEAD")
					}
				}
			} else {
				prefix := "OPP_"
				if own {
					prefix = "OWN_"
				}
				keys = append(keys, prefix+r.Kind)
			}
			before, after := flowPerspective(r.Before, home), flowPerspective(r.After, home)
			for _, key := range keys {
				groupKey := key + "/" + r.Clock
				if seenReactions[groupKey] {
					continue
				}
				seenReactions[groupKey] = true
				if reactionTotals[key] == nil {
					reactionTotals[key] = &reactionTotal{row: FlowReactionSummary{Kind: key}, matches: map[string]bool{}}
				}
				total := reactionTotals[key]
				total.row.Samples++
				total.row.Before += before.Mean
				total.row.After += after.Mean
				total.matches[ref.EventID] = true
			}
		}
	}
	for _, state := range []string{"LEADING", "DRAW", "TRAILING"} {
		if row := stateTotals[state]; row != nil {
			result.States = append(result.States, *row)
		}
	}
	var keys []string
	for key := range reactionTotals {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		total := reactionTotals[key]
		row := total.row
		row.Before /= float64(row.Samples)
		row.After /= float64(row.Samples)
		row.Delta = row.After - row.Before
		row.Matches = len(total.matches)
		result.Reactions = append(result.Reactions, row)
	}
	result.Notes = append(result.Notes, "Ostatnie maksymalnie 5 zakończonych meczów w tych rozgrywkach z pierwszej strony historii. Dodatnie saldo oznacza przewagę analizowanej drużyny.", "Agregaty są poglądowe i nie zmieniają fair kursu. Brak stanu lub reakcji oznacza brak wystarczającej próbki.")
	log.Printf("[MATCH-FLOW] team=%s selected=%d available=%d reactions=%d", teamID, len(result.Matches), result.Available, len(result.Reactions))
	return result
}

type teamMatchProfile struct {
	FetchedAt  time.Time          `json:"fetchedAt"`
	Statistics map[string]float64 `json:"statistics"`
}

func (profile *teamMatchProfile) UnmarshalJSON(raw []byte) error {
	var envelope struct {
		Statistics map[string]json.RawMessage `json:"statistics"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return err
	}
	profile.Statistics = make(map[string]float64)
	for key, rawValue := range envelope.Statistics {
		var value *float64
		// Metadata such as statisticsType is an object, not a count.
		if json.Unmarshal(rawValue, &value) == nil && value != nil && !math.IsNaN(*value) && !math.IsInf(*value, 0) {
			profile.Statistics[key] = *value
		}
	}
	return nil
}

type fairModelInput struct {
	HomeID   int                      `json:"homeId"`
	AwayID   int                      `json:"awayId"`
	LeagueID int                      `json:"leagueId"`
	SeasonID int                      `json:"seasonId"`
	Roles    map[int]string           `json:"roles"`
	Profiles map[int]teamMatchProfile `json:"profiles"`
}

type analysisMatchContext struct {
	Input    fairModelInput `json:"fairInput"`
	Overview *MatchOverview `json:"overview"`
}

type sofaContextSnapshot struct {
	FetchedAt time.Time       `json:"fetchedAt"`
	Body      json.RawMessage `json:"body"`
}

// Small shared snapshots survive restart. Actual HTTP calls retain the existing
// raw archive; cache hits are captured by the analysis input archive instead.
var sofaContextFailures = struct {
	sync.Mutex
	until map[string]time.Time
}{until: make(map[string]time.Time)}

func getSofaContextJSON(client *ResilientClient, endpoint string, ttl time.Duration, target any) (time.Time, error) {
	path := filepath.Join(serverDataDir, "matchup_cache", archiveName(endpoint)+".json")
	var snapshot sofaContextSnapshot
	if raw, err := os.ReadFile(path); err == nil && json.Unmarshal(raw, &snapshot) == nil && time.Since(snapshot.FetchedAt) >= 0 && time.Since(snapshot.FetchedAt) < ttl {
		if err := json.Unmarshal(snapshot.Body, target); err == nil {
			return snapshot.FetchedAt, nil
		}
	}
	sofaContextFailures.Lock()
	retryAt := sofaContextFailures.until[path]
	sofaContextFailures.Unlock()
	if time.Now().Before(retryAt) {
		return time.Time{}, fmt.Errorf("źródło chwilowo niedostępne (ponowienie po %s)", retryAt.Format("15:04"))
	}
	body, err := client.GetSofa(endpoint)
	if err == nil {
		err = json.Unmarshal(body, target)
	}
	if err != nil {
		// A 403 must not impose an endpoint-specific pause on manual attempts.
		if client.ctx.Err() == nil && !strings.Contains(err.Error(), "SofaScore HTTP 403") && !strings.HasPrefix(err.Error(), "sesja SofaScore:") {
			sofaContextFailures.Lock()
			sofaContextFailures.until[path] = time.Now().Add(15 * time.Minute)
			sofaContextFailures.Unlock()
		}
		return time.Time{}, err
	}
	snapshot = sofaContextSnapshot{FetchedAt: time.Now().UTC(), Body: body}
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return snapshot.FetchedAt, err
	}
	if err := atomicJSON(path, snapshot); err != nil {
		return snapshot.FetchedAt, fmt.Errorf("zapis cache matchupu: %w", err)
	}
	return snapshot.FetchedAt, nil
}

func fetchAnalysisMatchContext(client *ResilientClient, homeID, awayID string, players []*PlayerStatHistory) analysisMatchContext {
	result := analysisMatchContext{Input: fairModelInput{Roles: map[int]string{}, Profiles: map[int]teamMatchProfile{}}, Overview: &MatchOverview{AsOf: time.Now().UTC(), Teams: map[string]*TeamOverview{}, Notes: []string{}}}
	client.state.mu.Lock()
	for id, role := range client.state.lineupRoles {
		result.Input.Roles[id] = role
	}
	client.state.mu.Unlock()
	warn := func(label string, err error) {
		if err != nil {
			message := label + ": " + err.Error()
			result.Overview.Notes = append(result.Overview.Notes, message)
			client.warn(message)
		}
	}
	if client.statsFixture.ID == "" {
		result.Overview.Notes = append(result.Overview.Notes, "Brak powiązanego wydarzenia: matchup niedostępny.")
		return result
	}
	var event struct {
		Event struct {
			HomeTeam SofaTeamItem `json:"homeTeam"`
			AwayTeam SofaTeamItem `json:"awayTeam"`
			Season   struct {
				ID int `json:"id"`
			} `json:"season"`
			Tournament struct {
				UniqueTournament struct {
					ID int `json:"id"`
				} `json:"uniqueTournament"`
			} `json:"tournament"`
		} `json:"event"`
	}
	_, err := getSofaContextJSON(client, "/event/"+client.statsFixture.ID, time.Hour, &event)
	if err != nil {
		warn("Kontekst meczu", err)
		return result
	}
	in := &result.Input
	in.HomeID, in.AwayID = event.Event.HomeTeam.ID, event.Event.AwayTeam.ID
	in.LeagueID, in.SeasonID = event.Event.Tournament.UniqueTournament.ID, event.Event.Season.ID
	if !((strconv.Itoa(in.HomeID) == homeID && strconv.Itoa(in.AwayID) == awayID) || (strconv.Itoa(in.HomeID) == awayID && strconv.Itoa(in.AwayID) == homeID)) {
		result.Input.HomeID, result.Input.AwayID = 0, 0
		warn("Kontekst meczu", fmt.Errorf("drużyny wydarzenia nie zgadzają się z analizą"))
		return result
	}
	result.Overview.LeagueID, result.Overview.SeasonID = in.LeagueID, in.SeasonID
	for _, item := range []struct {
		team  SofaTeamItem
		venue string
	}{{event.Event.HomeTeam, "HOME"}, {event.Event.AwayTeam, "AWAY"}} {
		result.Overview.Teams[strconv.Itoa(item.team.ID)] = &TeamOverview{Name: item.team.Name, Venue: item.venue, Form: []string{}, VenueForm: []string{}, Metrics: map[string]float64{}}
	}
	if in.LeagueID == 0 || in.SeasonID == 0 {
		warn("Kontekst ligi", fmt.Errorf("brak identyfikatora ligi lub sezonu"))
		return result
	}
	base := fmt.Sprintf("/unique-tournament/%d/season/%d", in.LeagueID, in.SeasonID)
	// Historical comparisons use profiles of the same league and season, observed
	// now. They are NOT reconstructed statistics as of each historical fixture.
	ids := map[int]bool{in.HomeID: true, in.AwayID: true}
	for _, hist := range players {
		for _, m := range hist.MatchContexts {
			if m.LeagueID == in.LeagueID && m.SeasonID == in.SeasonID {
				if m.TeamID > 0 {
					ids[m.TeamID] = true
				}
				if m.OpponentID > 0 {
					ids[m.OpponentID] = true
				}
			}
		}
	}
	orderedIDs := []int{in.HomeID, in.AwayID}
	var historicalIDs []int
	for id := range ids {
		if id != in.HomeID && id != in.AwayID {
			historicalIDs = append(historicalIDs, id)
		}
	}
	sort.Ints(historicalIDs)
	orderedIDs = append(orderedIDs, historicalIDs...)
	// Transfers and very large samples cannot trigger an unbounded request burst.
	if len(orderedIDs) > 32 {
		orderedIDs = orderedIDs[:32]
	}
	missing := 0
	for _, id := range orderedIDs {
		if client.ctx.Err() != nil {
			break
		}
		var profile teamMatchProfile
		profile.FetchedAt, err = getSofaContextJSON(client, fmt.Sprintf("/team/%d%s/statistics/overall", id, base), 6*time.Hour, &profile)
		if err != nil || profile.Statistics["matches"] <= 0 {
			missing++
			continue
		}
		if overview := result.Overview.Teams[strconv.Itoa(id)]; overview != nil {
			overview.StatsMatches = int(profile.Statistics["matches"])
			for _, key := range []string{"shots", "shotsAgainst", "shotsOnTarget", "shotsOnTargetAgainst", "expectedGoals", "tackles", "interceptions", "fouls"} {
				if value, ok := profile.Statistics[key]; ok {
					overview.Metrics[key] = value / profile.Statistics["matches"]
				}
			}
			for _, key := range []string{"averageBallPossession", "avgRating"} {
				if value, ok := profile.Statistics[key]; ok {
					overview.Metrics[key] = value
				}
			}
		}
		modelStats := map[string]float64{"matches": profile.Statistics["matches"]}
		for _, market := range []CanonicalMarket{MarketShotsTotal, MarketShotsOnTarget, MarketShotsOffTarget, MarketShotsBlocked, MarketShotsInsideBox, MarketShotsOutsideBox, MarketTackles, MarketInterceptions, MarketOffsides, MarketFoulsDrawn} {
			own, opponent := matchupFeatures(market)
			for _, key := range []string{own, opponent} {
				if value, ok := profile.Statistics[key]; ok && key != "" {
					modelStats[key] = value
				}
			}
		}
		in.Profiles[id] = teamMatchProfile{FetchedAt: profile.FetchedAt, Statistics: modelStats}
	}
	if missing > 0 {
		warn("Kontekst drużyn", fmt.Errorf("brak %d/%d profili; dla brakujących danych model pomija korektę", missing, len(orderedIDs)))
	}
	for _, venue := range []string{"total", "home", "away"} {
		if client.ctx.Err() != nil {
			break
		}
		var table struct {
			Standings []struct {
				Rows []struct {
					Team     SofaTeamItem `json:"team"`
					Position int          `json:"position"`
					Matches  int          `json:"matches"`
					Points   int          `json:"points"`
				} `json:"rows"`
			} `json:"standings"`
		}
		_, err := getSofaContextJSON(client, base+"/standings/"+venue, time.Hour, &table)
		if err != nil {
			warn("Tabela "+venue, err)
			continue
		}
		for _, standing := range table.Standings {
			for _, row := range standing.Rows {
				if team := result.Overview.Teams[strconv.Itoa(row.Team.ID)]; team != nil {
					summary := &TableSummary{Position: row.Position, Matches: row.Matches, Points: row.Points}
					if venue == "total" {
						team.Table = summary
					} else if strings.EqualFold(venue, team.Venue) {
						team.VenueTable = summary
					}
				}
			}
		}
	}
	// The outer team-events key is tournament.id, NOT uniqueTournament.id.
	for _, venue := range []string{"home", "away"} {
		if client.ctx.Err() != nil {
			break
		}
		var events struct {
			TournamentTeamEvents map[string]map[string][]struct {
				ID        int          `json:"id"`
				StartTS   int64        `json:"startTimestamp"`
				HomeTeam  SofaTeamItem `json:"homeTeam"`
				AwayTeam  SofaTeamItem `json:"awayTeam"`
				HomeScore struct {
					Current *int `json:"current"`
				} `json:"homeScore"`
				AwayScore struct {
					Current *int `json:"current"`
				} `json:"awayScore"`
				Status struct {
					Code int `json:"code"`
				} `json:"status"`
			} `json:"tournamentTeamEvents"`
		}
		_, err := getSofaContextJSON(client, base+"/team-events/"+venue, time.Hour, &events)
		if err != nil {
			warn("Forma "+venue, err)
			continue
		}
		for id, team := range result.Overview.Teams {
			if !strings.EqualFold(team.Venue, venue) {
				continue
			}
			type formItem struct {
				id      int
				started int64
				value   string
			}
			var form []formItem
			seen := map[int]bool{}
			for _, teams := range events.TournamentTeamEvents {
				for _, e := range teams[id] {
					if seen[e.ID] || e.Status.Code != 100 || e.HomeScore.Current == nil || e.AwayScore.Current == nil || e.StartTS >= client.statsFixture.StartTime.Unix() {
						continue
					}
					own, opp := *e.HomeScore.Current, *e.AwayScore.Current
					if venue == "home" && strconv.Itoa(e.HomeTeam.ID) != id {
						continue
					}
					if venue == "away" {
						if strconv.Itoa(e.AwayTeam.ID) != id {
							continue
						}
						own, opp = opp, own
					}
					value := "D"
					if own > opp {
						value = "W"
					} else if own < opp {
						value = "L"
					}
					form = append(form, formItem{e.ID, e.StartTS, value})
					seen[e.ID] = true
				}
			}
			sort.Slice(form, func(i, j int) bool { return form[i].started > form[j].started })
			for i, e := range form {
				if i == 5 {
					break
				}
				team.VenueForm = append(team.VenueForm, e.value)
			}
		}
	}
	var pregame struct {
		HomeTeam struct {
			Form []string `json:"form"`
		} `json:"homeTeam"`
		AwayTeam struct {
			Form []string `json:"form"`
		} `json:"awayTeam"`
	}
	if _, err := getSofaContextJSON(client, "/event/"+client.statsFixture.ID+"/pregame-form", time.Hour, &pregame); err == nil {
		result.Overview.Teams[strconv.Itoa(in.HomeID)].Form = pregame.HomeTeam.Form
		result.Overview.Teams[strconv.Itoa(in.AwayID)].Form = pregame.AwayTeam.Form
	} else {
		warn("Forma przedmeczowa", err)
	}
	result.Overview.Notes = append(result.Overview.Notes, "Tabela, forma, posiadanie, xG i rating są podglądem; nie mnożą fair kursu. Na początku sezonu tabela ma małą próbkę.")
	sharedFlows := map[string]*HistoricalMatchFlow{}
	for _, id := range []int{in.HomeID, in.AwayID} {
		if client.ctx.Err() != nil {
			break
		}
		result.Overview.Teams[strconv.Itoa(id)].Flow = fetchTeamFlowOverview(client, strconv.Itoa(id), in.LeagueID, sharedFlows)
	}
	log.Printf("[MATCHUP] event=%s league=%d season=%d profiles=%d roles=%d", client.statsFixture.ID, in.LeagueID, in.SeasonID, len(in.Profiles), len(in.Roles))
	return result
}

// Only verified count fields enter this allowlist. No synthetic xGA, no rating,
// no table position and no assumption that possession raises every market.
func matchupFeatures(market CanonicalMarket) (own, opponent string) {
	switch market {
	case MarketShotsTotal:
		return "shots", "shotsAgainst"
	case MarketShotsOnTarget:
		return "shotsOnTarget", "shotsOnTargetAgainst"
	case MarketShotsOffTarget:
		return "shotsOffTarget", "shotsOffTargetAgainst"
	case MarketShotsBlocked:
		return "blockedScoringAttempt", "blockedScoringAttemptAgainst"
	case MarketShotsInsideBox:
		return "shotsFromInsideTheBox", "shotsFromInsideTheBoxAgainst"
	case MarketShotsOutsideBox:
		return "shotsFromOutsideTheBox", "shotsFromOutsideTheBoxAgainst"
	case MarketTackles:
		return "tackles", "tacklesAgainst"
	case MarketInterceptions:
		return "interceptions", "interceptionsAgainst"
	case MarketOffsides:
		return "offsides", "offsidesAgainst"
	case MarketFoulsDrawn:
		return "", "fouls"
	default:
		return "", ""
	}
}

func matchupRate(input fairModelInput, teamID int, key string) (float64, float64, bool) {
	p := input.Profiles[teamID].Statistics
	n, ok := p["matches"]
	v, exists := p[key]
	if !ok || !exists || n <= 0 || v < 0 || key == "" {
		return 0, 0, false
	}
	return v / n, n, true
}

func contextualFairModel(hist *PlayerStatHistory, market CanonicalMarket, line float64, under bool, input fairModelInput) *FairModelDetails {
	model := &FairModelDetails{Version: fairModelVersion, Distribution: "Poisson", MatchupFactor: 1, Features: []string{}, Notes: []string{"Model eksperymentalny; parametry wymagają kalibracji na zapisanych wynikach."}}
	model.TeamID = hist.TeamID
	teamID, _ := strconv.Atoi(hist.TeamID)
	opponentID := 0
	if teamID > 0 && teamID == input.HomeID {
		model.Venue, opponentID = "HOME", input.AwayID
	} else if teamID > 0 && teamID == input.AwayID {
		model.Venue, opponentID = "AWAY", input.HomeID
	}
	if opponentID > 0 {
		model.OpponentID = strconv.Itoa(opponentID)
	}
	model.Role = strings.ToUpper(strings.TrimSpace(input.Roles[hist.ID]))
	values := hist.MetricValues[market]
	weights := make([]float64, len(values))
	var sumW, sumW2, sumV float64
	for i, v := range values {
		w := 1.0
		if model.Role != "" && i < len(hist.Positions) && strings.TrimSpace(hist.Positions[i]) != "" && !strings.EqualFold(model.Role, strings.TrimSpace(hist.Positions[i])) {
			w *= .5
		}
		if model.Venue != "" && i < len(hist.MatchContexts) && hist.MatchContexts[i].Venue != "" && hist.MatchContexts[i].Venue != model.Venue {
			w *= .7
		}
		weights[i] = w
		sumW += w
		sumW2 += w * w
		sumV += w * float64(v)
	}
	if sumW == 0 {
		return model
	}
	model.BaseMean = sumV / sumW
	model.EffectiveSample = sumW * sumW / sumW2
	model.Features = append(model.Features, "liczności na mecz")
	if model.Role != "" {
		model.Features = append(model.Features, "pozycja "+model.Role)
	} else {
		model.Notes = append(model.Notes, "Brak aktualnej pozycji: bez ważenia według roli.")
	}
	if model.Venue != "" {
		model.Features = append(model.Features, "home/away")
	}
	own, opp := matchupFeatures(market)
	logAdjustment := 0.0
	contextUsed := make([]bool, len(values))
	appliedFeatures := 0
	for _, feature := range []struct {
		key    string
		target int
		own    bool
	}{{own, teamID, true}, {opp, opponentID, false}} {
		target, n, ok := matchupRate(input, feature.target, feature.key)
		if !ok {
			continue
		}
		var baseline, covered, support float64
		coveredIndexes := []int{}
		for i, m := range hist.MatchContexts {
			if i >= len(weights) || m.LeagueID != input.LeagueID || m.SeasonID != input.SeasonID {
				continue
			}
			id := m.OpponentID
			if feature.own {
				id = m.TeamID
			}
			rate, matches, ok := matchupRate(input, id, feature.key)
			if !ok {
				continue
			}
			baseline += weights[i] * rate
			covered += weights[i]
			support += weights[i] * matches
			coveredIndexes = append(coveredIndexes, i)
		}
		if covered == 0 || baseline <= 0 {
			continue
		}
		baseline /= covered
		// Smooth sparse counts, shrink early-season and incomplete context. The
		// final cap is a guardrail, not a learned coefficient or confidence level.
		reliability := math.Min(n, support/covered) / (math.Min(n, support/covered) + 8)
		logAdjustment += .5 * math.Log((target+.25)/(baseline+.25)) * reliability * (covered / sumW)
		model.Features = append(model.Features, feature.key+" / mecz")
		appliedFeatures++
		for _, i := range coveredIndexes {
			contextUsed[i] = true
		}
	}
	for _, used := range contextUsed {
		if used {
			model.ContextMatches++
		}
	}
	model.MatchupFactor = math.Max(.8, math.Min(1.25, math.Exp(logAdjustment)))
	model.ExpectedCount = model.BaseMean * model.MatchupFactor
	if appliedFeatures == 0 {
		model.Notes = append(model.Notes, "Brak zgodnych danych dla tego rynku: korekta matchupu = 1.")
	} else if own != "" && opp != "" && appliedFeatures < 2 {
		model.Notes = append(model.Notes, "Częściowy profil matchupu: brak jednego z czynników drużyna/rywal.")
	}
	if model.EffectiveSample < 5 {
		model.Notes = append(model.Notes, "Mała efektywna próbka; fair kurs ma wysoką niepewność.")
	}
	var variance float64
	for i, v := range values {
		variance += weights[i] * math.Pow(float64(v)-model.BaseMean, 2)
	}
	if denom := sumW - sumW2/sumW; denom > 0 {
		variance /= denom
	} else {
		variance = model.BaseMean
	}
	// Overdispersion is estimated from counts, then shrunk with sample size.
	excess := math.Max(0, variance-model.BaseMean) * model.MatchupFactor * model.MatchupFactor * model.EffectiveSample / (model.EffectiveSample + 10)
	pUnder := calcPoissonUnder(line, model.ExpectedCount)
	if model.ExpectedCount > 0 && excess > .01 {
		model.Distribution = "Negative binomial"
		r := model.ExpectedCount * model.ExpectedCount / excess
		p := r / (r + model.ExpectedCount)
		term := math.Exp(r * math.Log(p))
		pUnder = term
		for k := 1; k <= int(math.Floor(line)); k++ {
			term *= (float64(k-1) + r) / float64(k) * (1 - p)
			pUnder += term
		}
	}
	probability := pUnder
	if !under {
		probability = 1 - pUnder
	}
	model.Probability = math.Max(.005, math.Min(.95, probability))
	return model
}

func CalculateValueBets(
	props []NormalizedProp,
	homePlayers, awayPlayers []*PlayerStatHistory,
	homeLineup, awayLineup map[string]bool,
	lineupsConfirmed bool,
	contexts ...analysisMatchContext,
) []AnalyzedBet {
	var contextData analysisMatchContext
	if len(contexts) > 0 {
		contextData = contexts[0]
	}
	log.Printf("[CALCULATE] Rozpoczynam ewaluację Value Betów dla %d zakładów bukmacherskich...", len(props))
	playerHistory := make(map[string]*PlayerStatHistory)
	strictMap := make(map[string]string)

	registerPlayers := func(list []*PlayerStatHistory, side string) {
		for _, p := range list {
			p.TeamSide = side
			idStr := strconv.Itoa(p.ID)
			playerHistory[idStr] = p

			norm := NormalizePlayerName(p.FullName)
			if norm != "" {
				strictMap[norm] = idStr
				parts := strings.Fields(norm)
				if len(parts) >= 2 {
					strictMap[parts[len(parts)-1]+" "+strings.Join(parts[:len(parts)-1], " ")] = idStr
					first := parts[0]
					last := parts[len(parts)-1]
					if len(first) > 0 {
						strictMap[string(first[0])+" "+last] = idStr
						strictMap[last+" "+string(first[0])] = idStr
					}
				}
			}
		}
	}

	registerPlayers(homePlayers, "HOME")
	registerPlayers(awayPlayers, "AWAY")
	log.Printf("[CALCULATE] Zarejestrowano w mapie dopasowań %d zawodników (Gospodarze: %d, Goście: %d)", len(playerHistory), len(homePlayers), len(awayPlayers))

	getLineupStatus := func(playerID int, pName, side string) (string, bool) {
		norm := NormalizePlayerName(pName)
		var lineupMap map[string]bool
		if side == "HOME" {
			lineupMap = homeLineup
		} else if side == "AWAY" {
			lineupMap = awayLineup
		}

		if lineupMap == nil || len(lineupMap) == 0 {
			return "UNKNOWN", false
		}

		statusFor := func(isStarter bool) (string, bool) {
			prefix := ""
			if !lineupsConfirmed {
				prefix = "PREDICTED_"
			}
			if isStarter {
				return prefix + "STARTER", true
			}
			return prefix + "BENCH", false
		}
		if isStarter, exists := lineupMap["player:"+strconv.Itoa(playerID)]; exists {
			return statusFor(isStarter)
		}
		if isStarter, exists := lineupMap[norm]; exists {
			return statusFor(isStarter)
		}
		// A prediction can list just eleven players. Missing names do not prove
		// that the other players will be substitutes or outside the squad.
		return "UNKNOWN", false
	}

	results := make([]AnalyzedBet, 0)
	matchedPropsCount := 0

	for _, prop := range props {
		matchedID := matchPlayerStrict(prop.PlayerName, strictMap)
		if matchedID == "" {
			continue
		}

		hist, exists := playerHistory[matchedID]
		if !exists || hist.Matches == 0 {
			continue
		}

		values, ok := hist.MetricValues[prop.MarketType]
		if !ok || len(values) == 0 {
			continue
		}
		valuesSuperSub := hist.MetricValuesWithSuperSub[prop.MarketType]

		matchedPropsCount++
		sample := len(values)
		hitsOver, hitsUnder := 0, 0
		for _, v := range values {
			if float64(v) > prop.Line {
				hitsOver++
			} else {
				hitsUnder++
			}
		}

		var pHit float64
		if prop.IsUnder {
			pHit = float64(hitsUnder) / float64(sample)
		} else {
			pHit = float64(hitsOver) / float64(sample)
		}
		model := contextualFairModel(hist, prop.MarketType, prop.Line, prop.IsUnder, contextData.Input)
		pFinal := model.Probability

		fairOdds := 1.0 / pFinal
		edge := (prop.Odds - fairOdds) / fairOdds * 100.0
		evTax := ((1.0/fairOdds)*(prop.Odds*0.88-1.0) - (1.0 - (1.0 / fairOdds))) * 100.0

		verdict := "NEUTRAL"
		if evTax > 0.0 {
			verdict = "VALUE"
		} else if evTax < -15.0 {
			verdict = "TRAP"
		}

		label := prop.MarketLabel
		if prop.IsUnder && !strings.Contains(label, "Poniżej") {
			label += " (Poniżej)"
		}

		status, isStarting := getLineupStatus(hist.ID, hist.FullName, hist.TeamSide)

		results = append(results, AnalyzedBet{
			SampleRatings:        append([]*float64(nil), hist.Ratings...),
			FairModel:            model,
			MatchOverview:        contextData.Overview,
			Bookmaker:            prop.Bookmaker,
			PlayerName:           hist.FullName,
			MarketType:           prop.MarketType,
			MarketLabel:          label,
			Line:                 prop.Line,
			IsUnder:              prop.IsUnder,
			BookmakerOdds:        prop.Odds,
			FairOdds:             math.Round(fairOdds*100) / 100,
			HitRate:              math.Round(pHit*1000) / 10,
			EdgePercent:          math.Round(edge*10) / 10,
			EVWithTax:            math.Round(evTax*10) / 10,
			MatchesSample:        sample,
			SampleValues:         append([]int(nil), values...),
			SampleValuesSuperSub: append([]int(nil), valuesSuperSub...),
			SampleMinutes:        append([]int(nil), hist.Minutes...),
			SamplePositions:      append([]string(nil), hist.Positions...),
			SampleOpponents:      append([]string(nil), hist.Opponents...),
			SampleDates:          append([]string(nil), hist.Dates...),
			SampleShots:          hist.MatchShots,
			StatsSource:          "SOFASCORE",
			TeamSide:             hist.TeamSide,
			TeamID:               hist.TeamID,
			TeamName:             hist.TeamName,
			Verdict:              verdict,
			LineupStatus:         status,
			IsStartingXI:         isStarting,
		})
	}

	log.Printf("[CALCULATE] Zakończono analizę. Dopasowano %d zakładów, utworzono %d przeanalizowanych rekordów", matchedPropsCount, len(results))
	return results
}

// === Asynchronous analysis API and context-aware source transport ===

var serverDataDir = "data"
var globalSofaThrottle = make(chan struct{}, 6)

type sofaBackgroundContextKey struct{}

type sofaBackoffState struct {
	Status      int       `json:"status"`
	Until       time.Time `json:"until"`
	LastBlocked time.Time `json:"lastBlocked"`
	Blocks      int       `json:"consecutiveBlocks"`
}

var sofaBackoff = struct {
	sync.Mutex
	state sofaBackoffState
}{}
var sofaBackgroundSlot = make(chan struct{}, 1)
var sofaBackgroundLastFinished time.Time // protected by sofaBackgroundSlot

func loadSofaBackoff() {
	sofaBackoff.Lock()
	defer sofaBackoff.Unlock()
	var state sofaBackoffState
	raw, err := os.ReadFile(filepath.Join(serverDataDir, "sofa_request_backoff.json"))
	if err == nil {
		err = json.Unmarshal(raw, &state)
	}
	if err != nil && !os.IsNotExist(err) {
		log.Printf("[SOFA-BACKOFF] READ_ERROR error=%v", err)
	}
	// Legacy snapshots mixed 403 and 429 without identifying their source.
	// Ignore them so an earlier 403 cannot keep manual attempts blocked.
	if state.Status != 429 {
		if !state.Until.IsZero() {
			log.Printf("[SOFA-BACKOFF] IGNORED_LEGACY reason=no-explicit-429-status")
		}
		state = sofaBackoffState{}
	}
	if state.Blocks < 0 {
		state.Blocks = 0
	}
	if state.Blocks > 3 {
		state.Blocks = 3
	}
	sofaBackoff.state = state
	if time.Now().Before(state.Until) {
		log.Printf("[SOFA-BACKOFF] LOADED until=%s blocks=%d", state.Until.Format(time.RFC3339), state.Blocks)
	}
}

func sofaCooldownUntil() time.Time {
	sofaBackoff.Lock()
	defer sofaBackoff.Unlock()
	if sofaBackoff.state.Status == 429 && time.Now().Before(sofaBackoff.state.Until) {
		return sofaBackoff.state.Until
	}
	return time.Time{}
}

func sofaCooldownError() error {
	if until := sofaCooldownUntil(); !until.IsZero() {
		return fmt.Errorf("SofaScore: przerwa po HTTP 429 do %s; nie wysłano kolejnego żądania", until.Format(time.RFC3339))
	}
	return nil
}

func noteSofaBlocked(status int, retryAfter string) {
	if status != 429 {
		return
	}
	sofaBackoff.Lock()
	defer sofaBackoff.Unlock()
	now := time.Now().UTC()
	state := &sofaBackoff.state
	// Concurrent responses from a foreground analysis are one blocking episode.
	if now.Before(state.Until) {
		return
	}
	if now.Sub(state.LastBlocked) >= 24*time.Hour {
		state.Blocks = 0
	}
	if state.Blocks < 3 {
		state.Blocks++
	}
	delay := 15 * time.Minute * time.Duration(1<<(state.Blocks-1))
	if seconds, err := strconv.ParseInt(strings.TrimSpace(retryAfter), 10, 64); err == nil && seconds > 0 {
		// Bound untrusted input before multiplying to avoid duration overflow.
		if seconds > 24*60*60 {
			seconds = 24 * 60 * 60
		}
		if requested := time.Duration(seconds) * time.Second; requested > delay {
			delay = requested
		}
	} else if date, err := http.ParseTime(retryAfter); err == nil {
		requested := date.Sub(now)
		if requested > 24*time.Hour {
			requested = 24 * time.Hour
		}
		if requested > delay {
			delay = requested
		}
	}
	state.Status, state.LastBlocked, state.Until = status, now, now.Add(delay)
	if err := atomicJSON(filepath.Join(serverDataDir, "sofa_request_backoff.json"), state); err != nil {
		log.Printf("[SOFA-BACKOFF] WRITE_ERROR error=%v", err)
	}
	log.Printf("[SOFA-BACKOFF] PAUSE status=%d blocks=%d duration=%s until=%s", status, state.Blocks, delay, state.Until.Format(time.RFC3339))
}

func waitBackgroundSofa(ctx context.Context) (func(), error) {
	if err := sofaCooldownError(); err != nil {
		return nil, err
	}
	select {
	case sofaBackgroundSlot <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := func() {
		sofaBackgroundLastFinished = time.Now()
		<-sofaBackgroundSlot
	}
	if err := sofaCooldownError(); err != nil {
		release()
		return nil, err
	}
	gap := 2*time.Second + time.Duration(rand.Int64N(int64(2*time.Second)+1))
	if delay := time.Until(sofaBackgroundLastFinished.Add(gap)); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			release()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

type AnalysisRequest struct {
	SuperbetURL   string `json:"superbetUrl"`
	BetclicURL    string `json:"betclicUrl"`
	STSURL        string `json:"stsUrl"`
	HomeID        string `json:"homeId"`
	AwayID        string `json:"awayId"`
	MatchLimit    int    `json:"matchLimit"`
	TournamentIDs string `json:"tournamentIds"`
}

type AnalysisJob struct {
	ID           string          `json:"id"`
	Match        string          `json:"match"`
	Status       string          `json:"status"`
	Stage        string          `json:"stage"`
	Message      string          `json:"message"`
	PlayersDone  int             `json:"playersDone"`
	PlayersTotal int             `json:"playersTotal"`
	CreatedAt    time.Time       `json:"createdAt"`
	StartedAt    time.Time       `json:"startedAt,omitempty"`
	FinishedAt   time.Time       `json:"finishedAt,omitempty"`
	Request      AnalysisRequest `json:"request"`
	Warnings     []string        `json:"warnings"`
	Result       []AnalyzedBet   `json:"result,omitempty"`
	Error        string          `json:"error,omitempty"`
}

type analysisTask struct {
	ctx    context.Context
	mu     sync.Mutex
	job    AnalysisJob
	cancel context.CancelFunc
}

func (task *analysisTask) snapshot() AnalysisJob {
	task.mu.Lock()
	defer task.mu.Unlock()
	job := task.job
	job.Warnings = append([]string{}, job.Warnings...)
	// Results are immutable after publication.
	return job
}

func (task *analysisTask) report(stage, message string, done, total int) {
	task.mu.Lock()
	defer task.mu.Unlock()
	if stage != "" {
		task.job.Stage = stage
	}
	if message != "" {
		task.job.Message = message
	}
	task.job.PlayersDone += done
	task.job.PlayersTotal += total
}

func (task *analysisTask) warn(message string) {
	task.mu.Lock()
	defer task.mu.Unlock()
	for _, old := range task.job.Warnings {
		if old == message {
			return
		}
	}
	task.job.Warnings = append(task.job.Warnings, message)
	log.Printf("ANALYSIS_WARNING id=%s %s", task.job.ID, message)
}

func analysisContext(ctx context.Context, job AnalysisJob, source, scope string) context.Context {
	return context.WithValue(ctx, archiveContextKey{}, archiveInfo{AnalysisID: job.ID, AnalysisCreatedAt: job.CreatedAt, Match: job.Match, Bookmaker: source, Scope: scope})
}

func validateAnalysisRequest(query *AnalysisRequest) error {
	query.HomeID, query.AwayID = strings.TrimSpace(query.HomeID), strings.TrimSpace(query.AwayID)
	if query.MatchLimit == 0 {
		query.MatchLimit = 10
	}
	if query.MatchLimit < 1 || query.MatchLimit > 30 {
		return fmt.Errorf("próba musi zawierać od 1 do 30 meczów")
	}
	for _, id := range []string{query.HomeID, query.AwayID} {
		if id != "" && !regexp.MustCompile(`^[0-9]{1,12}$`).MatchString(id) {
			return fmt.Errorf("niepoprawne ID SofaScore")
		}
	}
	if query.HomeID != "" && query.HomeID == query.AwayID {
		return fmt.Errorf("wybierz dwie różne drużyny")
	}
	query.TournamentIDs = strings.TrimSpace(query.TournamentIDs)
	if query.TournamentIDs == "" {
		query.TournamentIDs = "ALL"
	}
	if query.TournamentIDs != "ALL" {
		parts := strings.Split(query.TournamentIDs, ",")
		if len(parts) > 50 {
			return fmt.Errorf("zbyt wiele rozgrywek")
		}
		for _, part := range parts {
			if id, err := strconv.Atoi(strings.TrimSpace(part)); err != nil || id <= 0 {
				return fmt.Errorf("niepoprawny identyfikator rozgrywek")
			}
		}
	}
	count := 0
	for _, source := range []struct {
		input    *string
		provider string
	}{{&query.SuperbetURL, "SUPERBET"}, {&query.BetclicURL, "BETCLIC"}, {&query.STSURL, "STS"}} {
		*source.input = strings.TrimSpace(*source.input)
		if *source.input == "" {
			continue
		}
		if err := validateMatchURL(*source.input, source.provider); err != nil {
			return err
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("podaj co najmniej jeden link do meczu")
	}
	return nil
}

// Bookmaker URLs are identifiers only. Network destinations remain fixed in collectors.
func validateMatchURL(input, provider string) error {
	u, err := url.Parse(input)
	if err != nil || len(input) > 2048 {
		return fmt.Errorf("niepoprawny link %s", provider)
	}
	hosts := map[string]string{"SUPERBET": "superbet.pl", "BETCLIC": "betclic.pl", "STS": "sts.pl"}
	host := strings.ToLower(u.Hostname())
	if (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || (host != hosts[provider] && host != "www."+hosts[provider]) {
		return fmt.Errorf("link musi prowadzić do %s", hosts[provider])
	}
	id, _, detected := extractIDAndSlug(input)
	if id == "" || detected != provider || !regexp.MustCompile(`^[0-9]{1,20}$`).MatchString(id) {
		return fmt.Errorf("brak poprawnego ID meczu %s w linku", provider)
	}
	return nil
}

func analysisTitle(query AnalysisRequest) string {
	for _, input := range []string{query.SuperbetURL, query.BetclicURL, query.STSURL} {
		_, slug, provider := extractIDAndSlug(input)
		home, away := parseTeamsFromSlug(slug, provider)
		if home != "" && away != "" {
			return strings.ReplaceAll(home, "-", " ") + " vs " + strings.ReplaceAll(away, "-", " ")
		}
	}
	return "druzyny-" + query.HomeID + "-vs-" + query.AwayID
}

func decodeAPIRequest(w http.ResponseWriter, r *http.Request, target any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("wymagany jeden obiekt JSON")
	}
	return nil
}

func (s *calendarServer) registerAnalysisAPI(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/analyses", func(w http.ResponseWriter, r *http.Request) {
		var query AnalysisRequest
		if err := decodeAPIRequest(w, r, &query); err != nil {
			apiError(w, 400, err)
			return
		}
		if err := validateAnalysisRequest(&query); err != nil {
			apiError(w, 400, err)
			return
		}
		var random [12]byte
		if _, err := cryptorand.Read(random[:]); err != nil {
			apiError(w, 500, err)
			return
		}
		ctx, cancel := context.WithTimeout(s.ctx, 20*time.Minute)
		task := &analysisTask{ctx: ctx, job: AnalysisJob{ID: fmt.Sprintf("%x", random[:]), Match: analysisTitle(query), Status: "queued", Stage: "queued", Message: "Oczekiwanie na analizę na serwerze", CreatedAt: time.Now().UTC(), Request: query, Warnings: []string{}}, cancel: cancel}
		s.analysisMu.Lock()
		defer s.analysisMu.Unlock()
		if s.ctx.Err() != nil || len(s.analysisQueue) >= cap(s.analysisQueue) {
			cancel()
			apiError(w, 503, fmt.Errorf("kolejka analiz jest pełna lub serwer jest zatrzymywany"))
			return
		}
		job := task.snapshot()
		archiveCtx := analysisContext(ctx, job, "ANALIZA", "zlecenie-proba-"+strconv.Itoa(query.MatchLimit)+"-meczow")
		if err := archiveResponse(archiveCtx, "parametry-analizy", nil, "", job, nil); err != nil {
			cancel()
			apiError(w, 500, err)
			return
		}
		if err := atomicJSON(filepath.Join(s.dataDir, "analysis-jobs", job.ID+".json"), job); err != nil {
			cancel()
			apiError(w, 500, err)
			return
		}
		// Keep at most 64 completed jobs in memory; all final jobs remain on disk.
		if len(s.jobs) >= 64 {
			for id, old := range s.jobs {
				state := old.snapshot()
				if state.Status != "queued" && state.Status != "running" {
					delete(s.jobs, id)
					if len(s.jobs) < 64 {
						break
					}
				}
			}
		}
		s.jobs[job.ID] = task
		s.analysisQueue <- task
		jsonResponse(w, http.StatusAccepted, job)
	})
	mux.HandleFunc("GET /api/v1/analyses/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(id) {
			apiError(w, 400, fmt.Errorf("niepoprawne ID analizy"))
			return
		}
		s.analysisMu.Lock()
		task := s.jobs[id]
		s.analysisMu.Unlock()
		if task != nil {
			jsonResponse(w, 200, task.snapshot())
			return
		}
		var job AnalysisJob
		raw, err := os.ReadFile(filepath.Join(s.dataDir, "analysis-jobs", id+".json"))
		if os.IsNotExist(err) {
			apiError(w, 404, fmt.Errorf("nie znaleziono analizy"))
			return
		}
		if err == nil {
			err = json.Unmarshal(raw, &job)
		}
		if err != nil {
			apiError(w, 500, err)
			return
		}
		if job.Status == "queued" || job.Status == "running" {
			job.Status = "failed"
			job.Stage = "failed"
			job.Error = "Serwer został uruchomiony ponownie. Zleć analizę jeszcze raz."
			job.Message = job.Error
		}
		jsonResponse(w, 200, job)
	})
	mux.HandleFunc("POST /api/v1/teams/detect", func(w http.ResponseWriter, r *http.Request) {
		var query struct {
			URL  string `json:"url"`
			Home string `json:"home"`
			Away string `json:"away"`
		}
		if err := decodeAPIRequest(w, r, &query); err != nil {
			apiError(w, 400, err)
			return
		}
		if len(query.Home) > 200 || len(query.Away) > 200 || len(query.URL) > 2048 {
			apiError(w, 400, fmt.Errorf("zbyt długa nazwa lub link"))
			return
		}
		home, away := calendarTeamSlug(query.Home), calendarTeamSlug(query.Away)
		if query.URL != "" {
			_, slug, provider := extractIDAndSlug(query.URL)
			if err := validateMatchURL(query.URL, provider); err != nil {
				apiError(w, 400, err)
				return
			}
			home, away = parseTeamsFromSlug(slug, provider)
		}
		ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
		defer cancel()
		client, err := newSofaClient(context.WithValue(ctx, archiveContextKey{}, archiveInfo{Bookmaker: "SOFASCORE", Scope: "wyszukiwanie-druzyn-" + home + "-vs-" + away}), nil, nil)
		if err != nil {
			apiError(w, 502, err)
			return
		}
		defer client.tlsClient.CloseIdleConnections()
		result := detectTeamsBySlugs(client, home, away, s.sofaContextForURL(query.URL))
		if err := client.archiveError(); err != nil {
			apiError(w, 500, err)
			return
		}
		jsonResponse(w, 200, result)
	})
	mux.HandleFunc("POST /api/v1/teams/mapping", func(w http.ResponseWriter, r *http.Request) {
		var query struct {
			Slug   string `json:"slug"`
			SofaID string `json:"sofaId"`
		}
		if err := decodeAPIRequest(w, r, &query); err != nil {
			apiError(w, 400, err)
			return
		}
		if query.Slug == "" || len(query.Slug) > 200 || !regexp.MustCompile(`^[0-9]{1,12}$`).MatchString(query.SofaID) {
			apiError(w, 400, fmt.Errorf("niepoprawna nazwa lub ID drużyny"))
			return
		}
		if err := persistTeamCache(query.Slug, query.SofaID); err != nil {
			apiError(w, 500, err)
			return
		}
		jsonResponse(w, 200, true)
	})
	mux.HandleFunc("GET /api/v1/tournaments", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("homeId")
		if !regexp.MustCompile(`^[0-9]{1,12}$`).MatchString(id) {
			apiError(w, 400, fmt.Errorf("niepoprawne ID drużyny"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		client, err := newSofaClient(context.WithValue(ctx, archiveContextKey{}, archiveInfo{Bookmaker: "SOFASCORE", Scope: "rozgrywki-druzyny-" + id}), nil, nil)
		if err != nil {
			apiError(w, 502, err)
			return
		}
		defer client.tlsClient.CloseIdleConnections()
		body, err := client.GetSofa("/team/" + id + "/standings/seasons")
		if err != nil {
			apiError(w, 502, err)
			return
		}
		var res SofaSeasonsResponse
		if err := json.Unmarshal(body, &res); err != nil {
			apiError(w, 502, err)
			return
		}
		seen := map[int]bool{}
		list := []LeagueOption{}
		for _, ts := range res.TournamentSeasons {
			tournament := ts.Tournament.UniqueTournament
			if tournament.ID > 0 && !seen[tournament.ID] {
				seen[tournament.ID] = true
				list = append(list, LeagueOption{ID: tournament.ID, Name: tournament.Name})
			}
		}
		jsonResponse(w, 200, list)
	})
}

func (s *calendarServer) analysisWorker() {
	defer close(s.analysisDone)
	for {
		select {
		case task := <-s.analysisQueue:
			s.runAnalysisTask(task)
		case <-s.ctx.Done():
			for {
				select {
				case task := <-s.analysisQueue:
					s.finishAnalysisTask(task, nil, s.ctx.Err())
				default:
					return
				}
			}
		}
	}
}

func (s *calendarServer) runAnalysisTask(task *analysisTask) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.finishAnalysisTask(task, nil, fmt.Errorf("błąd wewnętrzny analizy: %v", recovered))
		}
	}()
	if err := task.ctx.Err(); err != nil {
		s.finishAnalysisTask(task, nil, err)
		return
	}
	release, err := s.beginForeground(task.ctx)
	if err != nil {
		s.finishAnalysisTask(task, nil, err)
		return
	}
	defer release()
	task.mu.Lock()
	task.job.Status = "running"
	task.job.StartedAt = time.Now().UTC()
	task.mu.Unlock()
	job := task.snapshot()
	log.Printf("ANALYSIS_START id=%s match=%q", job.ID, job.Match)
	client, err := newSofaClient(analysisContext(task.ctx, job, "SOFASCORE", "statystyki"), task.warn, task.report)
	if err != nil {
		s.finishAnalysisTask(task, nil, err)
		return
	}
	defer client.tlsClient.CloseIdleConnections()
	engine := analysisEngine{client: client, task: task, server: s}
	result, err := engine.analyze(job.Request)
	err = errors.Join(err, client.archiveError(), task.ctx.Err())
	s.finishAnalysisTask(task, result, err)
}

func (s *calendarServer) finishAnalysisTask(task *analysisTask, result []AnalyzedBet, err error) {
	defer task.cancel()
	job := task.snapshot()
	job.FinishedAt = time.Now().UTC()
	job.Result = result
	job.Status, job.Stage, job.Message = "completed", "completed", fmt.Sprintf("Analiza zakończona: %d zakładów", len(result))
	if err != nil {
		job.Status, job.Stage, job.Error = "failed", "failed", err.Error()
		job.Message = job.Error
		job.Result = nil
	}
	ctx := analysisContext(context.Background(), job, "ANALIZA", "wynik-proba-"+strconv.Itoa(job.Request.MatchLimit)+"-meczow")
	if saveErr := archiveResponse(ctx, "wynik-analizy-algorytm-"+fairModelVersion, nil, "", job, err); saveErr != nil {
		job.Status, job.Stage, job.Error = "failed", "failed", errors.Join(err, saveErr).Error()
		job.Message = job.Error
		job.Result = nil
	}
	if saveErr := atomicJSON(filepath.Join(s.dataDir, "analysis-jobs", job.ID+".json"), job); saveErr != nil {
		job.Status, job.Stage, job.Error = "failed", "failed", errors.Join(err, saveErr).Error()
		job.Message = job.Error
		job.Result = nil
		log.Printf("ANALYSIS_SAVE_ERROR id=%s %v", job.ID, saveErr)
	}
	task.mu.Lock()
	task.job = job
	task.mu.Unlock()
	log.Printf("ANALYSIS_END id=%s status=%s results=%d error=%q", job.ID, job.Status, len(job.Result), job.Error)
}

type analysisEngine struct {
	client *ResilientClient
	task   *analysisTask
	server *calendarServer
}

type sofaMatchContext struct {
	StartTime time.Time
	Gender    string
}

// Use the already cached calendar; detecting teams must not download it again.
func (s *calendarServer) sofaContextForURL(input string) sofaMatchContext {
	result := sofaMatchContext{}
	if s == nil || s.cache == nil || input == "" {
		return result
	}
	linkKey := func(raw string) string {
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil {
			return ""
		}
		parsed.RawQuery, parsed.Fragment = "", ""
		return strings.TrimRight(parsed.String(), "/")
	}
	key := linkKey(input)
	if key == "" {
		return result
	}
	s.cache.once.Do(s.cache.readDisk)
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	var latest time.Time
	for _, entry := range s.cache.entries {
		if entry.Calendar == nil || entry.FetchedAt.Before(latest) {
			continue
		}
		for _, event := range entry.Calendar.Events {
			if linkKey(event.URL) != key {
				continue
			}
			latest = entry.FetchedAt
			result.StartTime, _ = time.Parse(time.RFC3339, event.StartTime)
			result.Gender = "M"
			_, competitionGender := sofaTeamNameAndGender(event.Competition)
			if competitionGender == "F" || sofaWomenPattern.MatchString(removeDiacritics(event.Competition+" "+event.Home+" "+event.Away)) {
				result.Gender = "F"
			}
		}
	}
	return result
}

func matchSofaTeamPair(client *ResilientClient, home, away []SofaTeamItem, match sofaMatchContext) (int, int, error) {
	if len(home) == 0 || len(away) == 0 || len(home) > 6 || len(away) > 6 {
		return 0, 0, fmt.Errorf("nie można rozstrzygnąć drużyn na podstawie terminarza (%d i %d kandydatów)", len(home), len(away))
	}
	homeIDs, awayIDs := map[int]bool{}, map[int]bool{}
	for _, team := range home {
		homeIDs[team.ID] = true
	}
	for _, team := range away {
		awayIDs[team.ID] = true
	}
	pairs := map[[2]int]bool{}
	var lastErr error
	for _, side := range [][]SofaTeamItem{home, away} {
		complete := true
		for _, team := range side {
			body, err := client.withScope("rozpoznanie-druzyn-terminarz-" + strconv.Itoa(team.ID)).GetSofa(fmt.Sprintf("/team/%d/events/next/0", team.ID))
			var response SofaTeamEventsResponse
			if err == nil {
				err = json.Unmarshal(body, &response)
			}
			if err != nil {
				complete, lastErr = false, err
				continue
			}
			for _, event := range response.Events {
				if event.ID <= 0 || event.StartTS <= 0 || event.Status.Code >= 100 {
					continue
				}
				when := time.Unix(event.StartTS, 0)
				if !match.StartTime.IsZero() {
					if delta := when.Sub(match.StartTime); delta < -time.Hour || delta > time.Hour {
						continue
					}
				} else if when.Before(time.Now().Add(-2 * time.Hour)) {
					continue
				}
				if gender := strings.ToUpper(event.HomeTeam.Gender); gender != "" && gender != match.Gender {
					continue
				}
				if gender := strings.ToUpper(event.AwayTeam.Gender); gender != "" && gender != match.Gender {
					continue
				}
				h, a := event.HomeTeam.ID, event.AwayTeam.ID
				if homeIDs[a] && awayIDs[h] {
					h, a = a, h
				}
				if h == a || !homeIDs[h] || !awayIDs[a] {
					continue
				}
				pairs[[2]int{h, a}] = true
				log.Printf("[SOFA-DETECT] FIXTURE event_id=%d home_id=%d away_id=%d start=%s expected_start=%s", event.ID, h, a, when.UTC().Format(time.RFC3339), match.StartTime.Format(time.RFC3339))
			}
		}
		if complete {
			if len(pairs) == 1 {
				for pair := range pairs {
					return pair[0], pair[1], nil
				}
			}
			return 0, 0, fmt.Errorf("terminarz nie wskazał jednej pary drużyn: %d pasujących par", len(pairs))
		}
	}
	return 0, 0, fmt.Errorf("nie udało się sprawdzić całego terminarza kandydatów: %w", lastErr)
}

func detectTeamsBySlugs(client *ResilientClient, home, away string, match sofaMatchContext) TeamDetectionResult {
	_, homeGender := sofaTeamNameAndGender(home)
	_, awayGender := sofaTeamNameAndGender(away)
	if homeGender == "F" || awayGender == "F" {
		match.Gender = "F"
	}
	if match.Gender == "" {
		match.Gender = "M"
	}
	cache := loadTeamCache()
	result := TeamDetectionResult{HomeSlug: home, AwaySlug: away, HomeID: verifiedTeamIDInCache(home, cache, match.Gender), AwayID: verifiedTeamIDInCache(away, cache, match.Gender), Warnings: []string{}}
	var candidates [2][]SofaTeamItem
	var searchErrors [2]error
	for index, team := range []struct {
		label, slug string
		id          *string
	}{{"Gospodarze", home, &result.HomeID}, {"Goście", away, &result.AwayID}} {
		if *team.id != "" {
			id, _ := strconv.Atoi(*team.id)
			candidates[index] = []SofaTeamItem{{ID: id}}
			continue
		}
		var err error
		if team.slug == "" {
			err = fmt.Errorf("nie udało się odczytać nazwy drużyny")
		} else {
			candidates[index], err = searchSofaTeamCandidates(client.withScope("wyszukiwanie-druzyny-"+team.slug), team.slug, match.Gender)
		}
		searchErrors[index] = err
	}
	if (len(candidates[0]) > 1 || len(candidates[1]) > 1) && len(candidates[0]) > 0 && len(candidates[1]) > 0 {
		homeID, awayID, err := matchSofaTeamPair(client, candidates[0], candidates[1], match)
		for index, id := range []int{homeID, awayID} {
			if err != nil {
				if len(candidates[index]) > 1 {
					searchErrors[index] = err
				}
				continue
			}
			for _, team := range candidates[index] {
				if team.ID == id {
					candidates[index] = []SofaTeamItem{team}
					break
				}
			}
		}
	}
	for index, team := range []struct {
		label, slug string
		id          *string
	}{{"Gospodarze", home, &result.HomeID}, {"Goście", away, &result.AwayID}} {
		if *team.id != "" {
			continue
		}
		if searchErrors[index] == nil && len(candidates[index]) == 1 {
			selected := candidates[index][0]
			*team.id = strconv.Itoa(selected.ID)
			saveTeamCache(team.slug, selected, match.Gender)
			log.Printf("[SOFA-SEARCH] MATCH slug=%q id=%s name=%q gender=%q", team.slug, *team.id, selected.Name, match.Gender)
			continue
		}
		err := searchErrors[index]
		if err == nil {
			err = fmt.Errorf("%d zgodnych kandydatów; brak jednoznacznego potwierdzenia w terminarzu", len(candidates[index]))
		}
		message := fmt.Sprintf("%s (%s): %v", team.label, team.slug, err)
		result.Warnings = append(result.Warnings, message)
		log.Printf("[SOFA-DETECT] ERROR %s", message)
	}
	log.Printf("[SOFA-DETECT] RESULT home=%q home_id=%q away=%q away_id=%q warnings=%d", home, result.HomeID, away, result.AwayID, len(result.Warnings))
	return result
}

func (engine *analysisEngine) analyze(query AnalysisRequest) ([]AnalyzedBet, error) {
	task, client := engine.task, engine.client
	var match sofaMatchContext
	task.report("teams", "Rozpoznawanie drużyn w SofaScore", 0, 0)
	// Revalidate automatic IDs before fetching any player statistics. An old
	// desktop/history entry can still carry a wrong ID from the legacy cache.
	{
		for _, input := range []string{query.SuperbetURL, query.BetclicURL, query.STSURL} {
			if input == "" {
				continue
			}
			_, slug, provider := extractIDAndSlug(input)
			home, away := parseTeamsFromSlug(slug, provider)
			cached := loadTeamCache()
			oldHomeID, oldAwayID := findTeamIDInCache(home, cached), findTeamIDInCache(away, cached)
			match = engine.server.sofaContextForURL(input)
			teams := detectTeamsBySlugs(client, home, away, match)
			for _, warning := range teams.Warnings {
				task.warn("SofaScore: " + warning)
			}
			resolve := func(label, provided, detected, oldID string) (string, error) {
				if detected == "" {
					if provided != "" && provided != oldID {
						task.warn(label + ": użyto ręcznie podanego ID; automatyczne dopasowanie nazwy nie powiodło się")
						return provided, nil
					}
					return "", fmt.Errorf("%s: nie udało się zweryfikować drużyny; nie użyto niezweryfikowanego ID %q ze starego cache", label, provided)
				}
				if provided != "" && provided != detected {
					// The desktop history can outlive the server cache. A verified
					// detection (including a saved manual mapping) takes precedence
					// even when the stale ID is no longer present in that cache.
					task.warn(fmt.Sprintf("%s: zastąpiono ID SofaScore przesłane przez klienta %s -> %s po weryfikacji drużyny", label, provided, detected))
				}
				return detected, nil
			}
			var resolveErr error
			query.HomeID, resolveErr = resolve("Gospodarze", query.HomeID, teams.HomeID, oldHomeID)
			if resolveErr != nil {
				return nil, resolveErr
			}
			query.AwayID, resolveErr = resolve("Goście", query.AwayID, teams.AwayID, oldAwayID)
			if resolveErr != nil {
				return nil, resolveErr
			}
			break
		}
	}
	if query.HomeID == "" || query.AwayID == "" || query.HomeID == query.AwayID {
		return nil, fmt.Errorf("nie udało się ustalić dwóch różnych ID drużyn w SofaScore; sprawdź ostrzeżenia detekcji")
	}
	task.mu.Lock()
	task.job.Request = query
	task.mu.Unlock()
	task.report("odds", "Przygotowywanie oferty Superbet, Betclic i STS", 0, 0)
	type sourceResult struct {
		book  string
		props []NormalizedProp
		odds  matchOddsSnapshot
		err   error
	}
	sources := make(chan sourceResult, 3)
	count := 0
	for _, source := range []struct{ book, input string }{{"SUPERBET", query.SuperbetURL}, {"BETCLIC", query.BetclicURL}, {"STS", query.STSURL}} {
		if source.input == "" {
			continue
		}
		count++
		go func(book, input string) {
			id, _, _ := extractIDAndSlug(input)
			ctx := analysisContext(task.ctx, task.snapshot(), book, "kursy-mecz-"+id)
			odds, err := engine.server.matchOdds(ctx, book, id)
			props := odds.Props
			engine.server.rememberMarketAvailability(marketAvailabilityFromOdds(book, id, odds, err))
			if err == nil && len(props) == 0 {
				err = fmt.Errorf("brak obsługiwanych rynków zawodników w odpowiedzi")
			}
			if saveErr := archiveResponse(ctx, "kursy-odczytane-rynki-zawodnikow", nil, "", map[string]any{"url": input, "eventId": id, "archiveVersion": odds.ArchiveVersion, "oddsObservedAt": odds.ObservedAt, "oddsExpiresAt": odds.ExpiresAt, "reusedFromCache": odds.FromCache, "propsCount": len(props)}, err); saveErr != nil {
				err = errors.Join(err, saveErr)
			}
			sources <- sourceResult{book: book, props: props, odds: odds, err: err}
		}(source.book, source.input)
	}
	allProps := []NormalizedProp{}
	oddsSources := map[string]any{}
	var archiveErr error
	for i := 0; i < count; i++ {
		result := <-sources
		if result.err != nil {
			task.warn(result.book + ": " + result.err.Error())
			if isArchiveFailure(result.err) {
				archiveErr = errors.Join(archiveErr, result.err)
			}
			continue
		}
		allProps = append(allProps, result.props...)
		oddsSources[result.book] = map[string]any{"eventId": result.odds.EventID, "archiveVersion": result.odds.ArchiveVersion, "observedAt": result.odds.ObservedAt, "expiresAt": result.odds.ExpiresAt, "reusedFromCache": result.odds.FromCache}
	}
	if archiveErr != nil {
		return nil, archiveErr
	}
	if len(allProps) == 0 {
		return nil, fmt.Errorf("brak rynków do analizy; sprawdź ostrzeżenia źródeł")
	}
	sort.Slice(allProps, func(i, j int) bool {
		if allProps[i].Bookmaker != allProps[j].Bookmaker {
			return allProps[i].Bookmaker < allProps[j].Bookmaker
		}
		if allProps[i].PlayerName != allProps[j].PlayerName {
			return allProps[i].PlayerName < allProps[j].PlayerName
		}
		if allProps[i].MarketType != allProps[j].MarketType {
			return allProps[i].MarketType < allProps[j].MarketType
		}
		return allProps[i].Line < allProps[j].Line
	})
	targets := []string{}
	seen := map[string]bool{}
	for _, prop := range allProps {
		if !seen[prop.PlayerName] {
			seen[prop.PlayerName] = true
			targets = append(targets, prop.PlayerName)
		}
	}
	task.report("statistics", "Pobieranie historii zawodników i składów z SofaScore", 0, 0)
	fixture, reversed, fixtureErr := engine.server.statsFixtureForMatch(client, query.HomeID, query.AwayID, match.StartTime)
	if fixtureErr != nil {
		task.warn("SofaScore: nie udało się powiązać statystyk z nadchodzącym meczem: " + fixtureErr.Error())
	} else {
		client.statsFixture = fixture
	}
	var homePlayers, awayPlayers []*PlayerStatHistory
	var homeErr, awayErr error
	var homeLineup, awayLineup map[string]bool
	var lineupsConfirmed bool
	lineupEventID := fixture.ID
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		homePlayers, _, homeErr = fetchSofaTeamDataComprehensive(client.withScope("gospodarze-druzyna-"+query.HomeID), query.HomeID, query.MatchLimit, query.TournamentIDs, targets)
	}()
	go func() {
		defer wg.Done()
		awayPlayers, _, awayErr = fetchSofaTeamDataComprehensive(client.withScope("goscie-druzyna-"+query.AwayID), query.AwayID, query.MatchLimit, query.TournamentIDs, targets)
	}()
	go func() {
		defer wg.Done()
		if lineupEventID != "" {
			var lineupErr error
			homeLineup, awayLineup, lineupsConfirmed, lineupErr = fetchSofaLineups(client, lineupEventID)
			if reversed {
				homeLineup, awayLineup = awayLineup, homeLineup
			}
			if lineupErr != nil {
				task.warn("SofaScore: nie udało się odczytać składów: " + lineupErr.Error())
			} else {
				if !lineupsConfirmed {
					task.warn("SofaScore: wykorzystano przewidywane składy (XI?), jeszcze niepotwierdzone")
				}
				if len(homeLineup) == 0 || len(awayLineup) == 0 {
					task.warn("SofaScore: brak składu jednej z drużyn")
				}
			}
		} else {
			task.warn("SofaScore: nie znaleziono nadchodzącego meczu do sprawdzenia składów")
		}
	}()
	wg.Wait()
	if err := errors.Join(homeErr, awayErr, client.archiveError(), task.ctx.Err()); err != nil {
		return nil, err
	}
	if len(homePlayers) == 0 && len(awayPlayers) == 0 {
		return nil, fmt.Errorf("SofaScore nie zwrócił historii zawodników; sprawdź ostrzeżenia")
	}
	if len(homePlayers) == 0 {
		task.warn("SofaScore: brak historii zawodników gospodarzy")
	}
	if len(awayPlayers) == 0 {
		task.warn("SofaScore: brak historii zawodników gości")
	}
	task.report("matchup", "Pobieranie kontekstu drużyn, rywala i tabeli", 0, 0)
	contextPlayers := append(append([]*PlayerStatHistory(nil), homePlayers...), awayPlayers...)
	matchContext := fetchAnalysisMatchContext(client.withScope("matchup-druzyny-i-tabela"), query.HomeID, query.AwayID, contextPlayers)
	if err := errors.Join(client.archiveError(), task.ctx.Err()); err != nil {
		return nil, err
	}
	task.report("calculation", "Obliczanie hitrate, kursów fair i EV", 0, 0)
	ctx := analysisContext(task.ctx, task.snapshot(), "ANALIZA", "dane-wejsciowe")
	if err := archiveResponse(ctx, "kursy-statystyki-sklady-matchup-uzyte-do-obliczen", nil, "", map[string]any{"algorithm": fairModelVersion, "request": query, "propsCount": len(allProps), "oddsSources": oddsSources, "homePlayers": homePlayers, "awayPlayers": awayPlayers, "homeLineup": homeLineup, "awayLineup": awayLineup, "lineupsConfirmed": lineupsConfirmed, "lineupEventId": lineupEventID, "matchContext": matchContext}, nil); err != nil {
		return nil, err
	}
	results := CalculateValueBets(allProps, homePlayers, awayPlayers, homeLineup, awayLineup, lineupsConfirmed, matchContext)
	lineupCounts := map[string]int{}
	for _, bet := range results {
		lineupCounts[bet.LineupStatus]++
	}
	log.Printf("[SOFA-LINEUP] MATCHED eventID=%q confirmed=%v result_statuses=%v", lineupEventID, lineupsConfirmed, lineupCounts)
	if len(results) > 0 && lineupCounts["STARTER"]+lineupCounts["PREDICTED_STARTER"] == 0 {
		task.warn(fmt.Sprintf("Składy SofaScore (mecz %s): nie dopasowano żadnego startera do wyników analizy; status nieznany: %d/%d. Filtr XI nie będzie miał wyników.", lineupEventID, lineupCounts["UNKNOWN"], len(results)))
	}
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].HitRate == results[j].HitRate {
			return results[i].EVWithTax > results[j].EVWithTax
		}
		return results[i].HitRate > results[j].HitRate
	})
	if len(results) == 0 {
		task.warn("Nie dopasowano żadnego rynku do dostępnych statystyk zawodników")
	}
	return results, nil
}

// Availability requires a supported player market with a usable quote. A
// BetBuilder flag, market title or empty offer is not sufficient.
var marketPlayerIDPattern = regexp.MustCompile(`^(\d+m\w+|f\d+|m\d+|\d+)$`)
var marketEventIDPattern = regexp.MustCompile(`^[0-9]{1,18}$`)

func usableMatchProps(props []NormalizedProp) []NormalizedProp {
	result := make([]NormalizedProp, 0, len(props))
	for _, prop := range props {
		switch prop.MarketType {
		case MarketShotsTotal, MarketShotsOnTarget, MarketShotsBlocked, MarketShotsOffTarget,
			MarketShotsOutsideBox, MarketShotsInsideBox, MarketShotsOnTargetOutsideBox,
			MarketShotsOnTargetInsideBox, MarketTackles, MarketDispossessed,
			MarketFoulsCommitted, MarketFoulsDrawn, MarketInterceptions, MarketOffsides:
		default:
			continue
		}
		if strings.TrimSpace(prop.PlayerName) == "" || marketPlayerIDPattern.MatchString(strings.TrimSpace(prop.PlayerName)) ||
			prop.Odds <= 1 || math.IsNaN(prop.Odds) || math.IsInf(prop.Odds, 0) ||
			prop.Line < 0 || math.IsNaN(prop.Line) || math.IsInf(prop.Line, 0) {
			continue
		}
		result = append(result, prop)
	}
	return result
}

const matchOddsTTL = 15 * time.Minute

func missingMarketsTTL(checks int) time.Duration {
	if checks >= 4 {
		return time.Hour
	}
	if checks == 3 {
		return 30 * time.Minute
	}
	return matchOddsTTL
}

type matchOddsSnapshot struct {
	Bookmaker          string           `json:"bookmaker"`
	EventID            string           `json:"eventId"`
	Match              string           `json:"match,omitempty"`
	ObservedAt         time.Time        `json:"observedAt"`
	ExpiresAt          time.Time        `json:"expiresAt"`
	Props              []NormalizedProp `json:"props"`
	FromCache          bool             `json:"-"`
	ConsecutiveMissing int              `json:"consecutiveMissing,omitempty"`
	ArchiveVersion     string           `json:"archiveVersion,omitempty"`
}

var oddsArchiveMu sync.Mutex

// Store each parsed offer once per bookmaker/event, even across restarts and
// A -> B -> A changes. Observation timestamps stay in a separate small journal.
func archiveOddsObservation(ctx context.Context, snapshot *matchOddsSnapshot, buffer *oddsArchiveBuffer) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: archiwum kursów: %w", errArchive, err)
			log.Printf("ARCHIVE_WRITE_ERROR: %v", err)
		}
	}()
	if archiveRoot == "" {
		return fmt.Errorf("archiwum nie zostało skonfigurowane")
	}
	// Labels and transport timestamps do not change a bet. IDs, players, markets,
	// sides, lines and odds do; sorting makes source ordering irrelevant.
	canonical := make([]string, 0, len(snapshot.Props))
	for _, prop := range snapshot.Props {
		prop.MarketLabel = ""
		encoded, encodeErr := json.Marshal(prop)
		if encodeErr != nil {
			return encodeErr
		}
		canonical = append(canonical, string(encoded))
	}
	sort.Strings(canonical)
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	directory := filepath.Join(archiveRoot, "oferty-kursow", strings.ToLower(snapshot.Bookmaker), "mecz-"+archiveName(snapshot.EventID), fmt.Sprintf("wersja-%x", digest))
	oddsArchiveMu.Lock()
	defer oddsArchiveMu.Unlock()
	if err := os.MkdirAll(directory, 0750); err != nil {
		return err
	}
	files, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	var versionPath string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), "_kursy.json.gz") {
			versionPath = filepath.Join(directory, file.Name())
			break
		}
	}
	newVersion := versionPath == ""
	if newVersion {
		if buffer != nil {
			if err := buffer.flush(ctx, filepath.Join(directory, "surowe-zrodlo")); err != nil {
				return err
			}
		}
		versionPath = filepath.Join(directory, strings.ToLower(snapshot.Bookmaker)+"_"+archiveName(snapshot.Match)+"_mecz-"+archiveName(snapshot.EventID)+"_"+snapshot.ObservedAt.Format("20060102T150405Z")+"_kursy.json.gz")
		data, err := json.MarshalIndent(map[string]any{"schemaVersion": 2, "bookmaker": snapshot.Bookmaker, "eventId": snapshot.EventID, "match": snapshot.Match, "firstObservedAt": snapshot.ObservedAt, "props": snapshot.Props}, "", "  ")
		if err != nil {
			return err
		}
		if err := writeGzip(versionPath, append(data, '\n')); err != nil {
			return err
		}
	}
	relative, err := filepath.Rel(archiveRoot, versionPath)
	if err != nil {
		return err
	}
	snapshot.ArchiveVersion = filepath.ToSlash(relative)
	now := time.Now().UTC()
	source := "bukmacher"
	if snapshot.FromCache {
		source = "cache"
	}
	observation, err := json.Marshal(map[string]any{"recordedAt": now, "observedAt": snapshot.ObservedAt, "expiresAt": snapshot.ExpiresAt, "bookmaker": snapshot.Bookmaker, "eventId": snapshot.EventID, "match": snapshot.Match, "source": source, "newVersion": newVersion, "archiveVersion": snapshot.ArchiveVersion, "analysisId": archiveInfoFrom(ctx).AnalysisID})
	if err != nil {
		return err
	}
	journalDir := filepath.Join(archiveRoot, "obserwacje-kursow", now.Format("2006-01-02"))
	if err := os.MkdirAll(journalDir, 0750); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(journalDir, strings.ToLower(snapshot.Bookmaker)+"_sprawdzenia-i-uzycie-kursow.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(observation, '\n'))
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return err
	}
	log.Printf("[ODDS-ARCHIVE] OBSERVATION bookmaker=%s event_id=%s source=%s new_version=%t version=%q", snapshot.Bookmaker, snapshot.EventID, source, newVersion, snapshot.ArchiveVersion)
	return nil
}

func (snapshot matchOddsSnapshot) fresh(now time.Time) bool {
	ttl := matchOddsTTL
	if len(snapshot.Props) == 0 {
		ttl = missingMarketsTTL(snapshot.ConsecutiveMissing)
	}
	return !snapshot.ObservedAt.IsZero() && !snapshot.ObservedAt.After(now) && now.Before(snapshot.ExpiresAt) &&
		!snapshot.ExpiresAt.After(snapshot.ObservedAt.Add(ttl))
}

func (snapshot matchOddsSnapshot) retain(now time.Time) bool {
	// Keep expired negative observations for the retry counter, never as fresh odds.
	return snapshot.fresh(now) || (len(snapshot.Props) == 0 && snapshot.ConsecutiveMissing > 0 &&
		!snapshot.ObservedAt.After(now) && snapshot.ObservedAt.After(now.Add(-24*time.Hour)))
}

func (snapshot matchOddsSnapshot) copy(reused bool) matchOddsSnapshot {
	snapshot.Props = append([]NormalizedProp{}, snapshot.Props...)
	snapshot.FromCache = reused
	return snapshot
}

type matchOddsFlight struct {
	done     chan struct{}
	snapshot matchOddsSnapshot
	err      error
}

// All consumers share full parsed offers and their original observation time.
// A hit never extends freshness; failed requests never become cached offers.
func (s *calendarServer) matchOdds(ctx context.Context, book, id string) (matchOddsSnapshot, error) {
	key := book + ":" + id
	for {
		if err := ctx.Err(); err != nil {
			return matchOddsSnapshot{}, err
		}
		s.oddsMu.Lock()
		if snapshot, ok := s.oddsCache[key]; ok && snapshot.fresh(time.Now()) {
			s.oddsMu.Unlock()
			log.Printf("[ODDS-CACHE] HIT bookmaker=%s event_id=%s age=%s observed_at=%s", book, id, time.Since(snapshot.ObservedAt).Round(time.Second), snapshot.ObservedAt.Format(time.RFC3339))
			snapshot = snapshot.copy(true)
			err := archiveOddsObservation(ctx, &snapshot, nil)
			return snapshot, err
		}
		if flight := s.oddsFlights[key]; flight != nil {
			s.oddsMu.Unlock()
			log.Printf("[ODDS-CACHE] JOIN bookmaker=%s event_id=%s", book, id)
			select {
			case <-ctx.Done():
				return matchOddsSnapshot{}, ctx.Err()
			case <-flight.done:
				if ctx.Err() == nil && (errors.Is(flight.err, context.Canceled) || errors.Is(flight.err, context.DeadlineExceeded)) {
					continue
				}
				if flight.err == nil && !flight.snapshot.fresh(time.Now()) {
					continue
				}
				snapshot := flight.snapshot.copy(true)
				if flight.err != nil {
					return snapshot, flight.err
				}
				err := archiveOddsObservation(ctx, &snapshot, nil)
				return snapshot, err
			}
		}
		previous := s.oddsCache[key]
		flight := &matchOddsFlight{done: make(chan struct{})}
		s.oddsFlights[key] = flight
		s.oddsMu.Unlock()
		log.Printf("[ODDS-CACHE] FETCH bookmaker=%s event_id=%s", book, id)
		snapshot := matchOddsSnapshot{Bookmaker: book, EventID: id, Match: archiveInfoFrom(ctx).Match, ObservedAt: time.Now().UTC()}
		buffer := &oddsArchiveBuffer{}
		fetchCtx := context.WithValue(ctx, oddsArchiveBufferKey{}, buffer)
		var err error
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					err = fmt.Errorf("pobieranie kursów: %v", recovered)
				}
			}()
			snapshot.Props, err = fetchMatchProps(fetchCtx, book, id)
		}()
		if err == nil {
			err = ctx.Err()
		}
		snapshot.ExpiresAt = snapshot.ObservedAt.Add(matchOddsTTL)
		if err == nil && len(snapshot.Props) == 0 {
			snapshot.ConsecutiveMissing = 1
			if len(previous.Props) == 0 && previous.ObservedAt.After(snapshot.ObservedAt.Add(-24*time.Hour)) {
				snapshot.ConsecutiveMissing = min(previous.ConsecutiveMissing+1, 4)
			}
			snapshot.ExpiresAt = snapshot.ObservedAt.Add(missingMarketsTTL(snapshot.ConsecutiveMissing))
		}
		if err == nil {
			err = archiveOddsObservation(ctx, &snapshot, buffer)
		} else {
			// Failed or incomplete responses remain available for diagnosis.
			err = errors.Join(err, buffer.flush(ctx, ""))
		}
		s.oddsMu.Lock()
		if err == nil {
			s.oddsCache[key] = snapshot
			for oldKey, entry := range s.oddsCache {
				if !entry.retain(time.Now()) {
					delete(s.oddsCache, oldKey)
				}
			}
			if saveErr := atomicJSON(filepath.Join(s.dataDir, "bookmaker_match_odds_cache.json"), s.oddsCache); saveErr != nil {
				log.Printf("ODDS_CACHE_WRITE_ERROR: %v", saveErr)
			}
			log.Printf("[ODDS-CACHE] STORED bookmaker=%s event_id=%s props=%d consecutive_missing=%d expires_at=%s", book, id, len(snapshot.Props), snapshot.ConsecutiveMissing, snapshot.ExpiresAt.Format(time.RFC3339))
		}
		flight.snapshot, flight.err = snapshot, err
		delete(s.oddsFlights, key)
		close(flight.done)
		s.oddsMu.Unlock()
		return snapshot.copy(false), err
	}
}

func marketAvailabilityFromOdds(book, id string, snapshot matchOddsSnapshot, err error) MatchMarketAvailability {
	result := marketAvailabilityResult(book, id, snapshot.Props, err)
	if err == nil && !snapshot.ObservedAt.IsZero() {
		result.CheckedAt, result.ExpiresAt = snapshot.ObservedAt, snapshot.ExpiresAt
	}
	return result
}

type MatchMarketAvailability struct {
	Bookmaker string            `json:"bookmaker"`
	EventID   string            `json:"eventId"`
	Status    string            `json:"status"`
	Markets   []CanonicalMarket `json:"markets"`
	CheckedAt time.Time         `json:"checkedAt"`
	ExpiresAt time.Time         `json:"expiresAt"`
	Error     string            `json:"error,omitempty"`
}

type marketAvailabilityFlight struct {
	done   chan struct{}
	result MatchMarketAvailability
}

func marketAvailabilityResult(book, id string, props []NormalizedProp, err error) MatchMarketAvailability {
	now := time.Now().UTC()
	result := MatchMarketAvailability{Bookmaker: book, EventID: id, Status: "unavailable", Markets: []CanonicalMarket{}, CheckedAt: now, ExpiresAt: now.Add(matchOddsTTL)}
	if err != nil {
		result.Status, result.Error = "error", err.Error()
		result.ExpiresAt = now.Add(time.Minute)
		return result
	}
	seen := map[CanonicalMarket]bool{}
	for _, prop := range usableMatchProps(props) {
		if !seen[prop.MarketType] {
			seen[prop.MarketType] = true
			result.Markets = append(result.Markets, prop.MarketType)
		}
	}
	sort.Slice(result.Markets, func(i, j int) bool { return result.Markets[i] < result.Markets[j] })
	if len(result.Markets) > 0 {
		result.Status = "available"
	}
	return result
}

func (s *calendarServer) rememberMarketAvailability(result MatchMarketAvailability) {
	s.marketMu.Lock()
	defer s.marketMu.Unlock()
	s.marketCache[result.Bookmaker+":"+result.EventID] = result
	for key, entry := range s.marketCache {
		if entry.CheckedAt.Before(time.Now().Add(-24 * time.Hour)) {
			delete(s.marketCache, key)
		}
	}
	if err := atomicJSON(filepath.Join(s.dataDir, "market_availability_cache.json"), s.marketCache); err != nil {
		log.Printf("MARKET_CACHE_WRITE_ERROR: %v", err)
	}
}

func (s *calendarServer) checkMatchMarkets(ctx context.Context, book, id, home, away string) MatchMarketAvailability {
	key := book + ":" + id
	s.marketMu.Lock()
	if cached, ok := s.marketCache[key]; ok && time.Now().Before(cached.ExpiresAt) {
		s.marketMu.Unlock()
		log.Printf("[MARKET-CACHE] HIT bookmaker=%s event_id=%s status=%s expires_at=%s", book, id, cached.Status, cached.ExpiresAt.Format(time.RFC3339))
		return cached
	}
	if flight := s.marketFlights[key]; flight != nil {
		s.marketMu.Unlock()
		select {
		case <-ctx.Done():
			return marketAvailabilityResult(book, id, nil, ctx.Err())
		case <-flight.done:
			return flight.result
		}
	}
	flight := &marketAvailabilityFlight{done: make(chan struct{})}
	s.marketFlights[key] = flight
	s.marketMu.Unlock()

	result := func() MatchMarketAvailability {
		select {
		case <-ctx.Done():
			return marketAvailabilityResult(book, id, nil, ctx.Err())
		case s.marketSlots <- struct{}{}:
		}
		defer func() { <-s.marketSlots }()
		scope := "dostepnosc-rynkow-" + home + "-vs-" + away + "-mecz-" + id
		ctx = context.WithValue(ctx, archiveContextKey{}, archiveInfo{Bookmaker: book, Match: home + " vs " + away, Scope: scope})
		odds, err := s.matchOdds(ctx, book, id)
		result := marketAvailabilityFromOdds(book, id, odds, err)
		if saveErr := archiveResponse(ctx, "sprawdzone-obslugiwane-rynki-zawodnikow", nil, "", result, err); saveErr != nil {
			result = marketAvailabilityResult(book, id, nil, errors.Join(err, saveErr))
		}
		return result
	}()
	s.rememberMarketAvailability(result)
	s.marketMu.Lock()
	flight.result = result
	delete(s.marketFlights, key)
	close(flight.done)
	s.marketMu.Unlock()
	return result
}

func (s *calendarServer) registerMarketAvailabilityAPI(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/markets/check", func(w http.ResponseWriter, r *http.Request) {
		var query struct {
			Bookmaker string `json:"bookmaker"`
			EventID   string `json:"eventId"`
			Home      string `json:"home"`
			Away      string `json:"away"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&query); err != nil {
			apiError(w, 400, err)
			return
		}
		query.Bookmaker = strings.ToUpper(strings.TrimSpace(query.Bookmaker))
		if (query.Bookmaker != "STS" && query.Bookmaker != "BETCLIC" && query.Bookmaker != "SUPERBET") ||
			!marketEventIDPattern.MatchString(query.EventID) || strings.Trim(query.EventID, "0") == "" ||
			len(query.Home) > 200 || len(query.Away) > 200 {
			apiError(w, 400, fmt.Errorf("niepoprawny bukmacher lub wydarzenie"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		stop := context.AfterFunc(s.ctx, cancel)
		defer stop()
		result := s.checkMatchMarkets(ctx, query.Bookmaker, query.EventID, query.Home, query.Away)
		jsonResponse(w, 200, result)
	})
}

func fetchMatchProps(ctx context.Context, book, id string) ([]NormalizedProp, error) {
	props, err := fetchRawMatchProps(ctx, book, id)
	return usableMatchProps(props), err
}

func fetchRawMatchProps(ctx context.Context, book, id string) ([]NormalizedProp, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	switch book {
	case "SUPERBET":
		endpoint := "https://production-superbet-bmb.freetls.fastly.net/betbuilder/v2/getBetbuilderMarketsForMatch?match_id=" + url.QueryEscape(id) + "&lang=pl-PL&target=SB_PL"
		var decoded json.RawMessage
		if err := getSuperbetCalendarJSON(ctx, client, endpoint, &decoded); err != nil {
			return nil, err
		}
		return ParseSuperbetProps(string(decoded))
	case "STS":
		raw, err := FetchSTSSnapshot(ctx, STSWebSocket, id)
		if err != nil {
			return nil, err
		}
		return ParseSTSProps(string(raw))
	case "BETCLIC":
		matchID, err := strconv.ParseUint(id, 10, 64)
		if err != nil || matchID == 0 {
			return nil, fmt.Errorf("niepoprawne ID Betclic")
		}
		body := ProtoUint(1, matchID)
		body = append(body, ProtoString(2, "pl")...)
		body = append(body, ProtoString(3, "ca_ftb_prp")...)
		raw, err := FetchBetclicRPC(ctx, client, BetclicMatchService+"GetMatchWithNotification", body)
		if err != nil {
			return nil, err
		}
		return ParseBetclicProps(raw), nil
	}
	return nil, fmt.Errorf("nieobsługiwane źródło")
}

type sofaClientState struct {
	mu          sync.Mutex
	archiveErr  error
	lineupRoles map[int]string
}
type ResilientClient struct {
	statsFixture  statsFixture
	statsPrefetch bool
	background    bool
	tlsClient     tls_client.HttpClient
	ctx           context.Context
	state         *sofaClientState
	warning       func(string)
	progress      func(string, string, int, int)
}

func newSofaClient(ctx context.Context, warning func(string), progress func(string, string, int, int)) (*ResilientClient, error) {
	info := archiveInfoFrom(ctx)
	info.Bookmaker = "SOFASCORE"
	if info.Scope == "" {
		info.Scope = "statystyki-i-status-meczu"
	}
	ctx = context.WithValue(ctx, archiveContextKey{}, info)
	cli, err := newSofaHTTPClient()
	if err != nil {
		return nil, err
	}
	background, _ := ctx.Value(sofaBackgroundContextKey{}).(bool)
	return &ResilientClient{tlsClient: cli, ctx: ctx, background: background, state: &sofaClientState{}, warning: warning, progress: progress}, nil
}

func newSofaHTTPClient(extra ...tls_client.HttpClientOption) (tls_client.HttpClient, error) {
	// The installed library provides Chrome 152, not an exact Chrome 154 profile.
	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(8),
		tls_client.WithClientProfile(profiles.Chrome_152),
		tls_client.WithCookieJar(tls_client.NewCookieJar()),
		tls_client.WithRandomTLSExtensionOrder(),
	}
	return tls_client.NewHttpClient(tls_client.NewNoopLogger(), append(options, extra...)...)
}

// This manual diagnostic uses the production headers and TLS configuration.
// It makes four requests, does not start collectors and does not alter caches.
func runSofaDiagnostics(ctx context.Context, dataDir string) error {
	serverDataDir = dataDir
	archiveRoot = filepath.Join(dataDir, "archive")
	dir := filepath.Join(dataDir, "diagnostics")
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	const endpoint = "/search/all?q=chicago+fire&page=0"
	variants := []struct {
		name    string
		options []tls_client.HttpClientOption
	}{
		{"domyslne", nil},
		{"ipv4", []tls_client.HttpClientOption{tls_client.WithDisableIPV6()}},
		{"ipv6", []tls_client.HttpClientOption{tls_client.WithDisableIPV4()}},
		{"ipv4-http1", []tls_client.HttpClientOption{tls_client.WithDisableIPV6(), tls_client.WithForceHttp1()}},
	}
	type probeResult struct {
		Variant      string         `json:"variant"`
		HTTPStatus   int            `json:"httpStatus"`
		Protocol     string         `json:"protocol"`
		Server       string         `json:"server,omitempty"`
		ContentType  string         `json:"contentType,omitempty"`
		ResponseSize int            `json:"responseBytes"`
		Duration     string         `json:"duration"`
		Teams        []SofaTeamItem `json:"teams"`
		Error        string         `json:"error,omitempty"`
		BodyPreview  string         `json:"bodyPreview,omitempty"`
	}
	report := struct {
		RecordedAt         time.Time     `json:"recordedAt"`
		Platform           string        `json:"platform"`
		GoVersion          string        `json:"goVersion"`
		TLSProfile         string        `json:"tlsProfile"`
		Endpoint           string        `json:"endpoint"`
		SuccessfulVariants []string      `json:"successfulVariants"`
		Results            []probeResult `json:"results"`
	}{time.Now().UTC(), runtime.GOOS + "/" + runtime.GOARCH, runtime.Version(), "Chrome_152", sofaBase + endpoint, []string{}, []probeResult{}}
	log.Printf("[SOFA-DIAG] START platform=%s go=%s endpoint=%q", report.Platform, report.GoVersion, report.Endpoint)
	for i, variant := range variants {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if i > 0 {
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		probeCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
		started := time.Now()
		result := probeResult{Variant: variant.name, Teams: []SofaTeamItem{}}
		var body []byte
		cli, fetchErr := newSofaHTTPClient(variant.options...)
		if fetchErr == nil {
			var req *fhttp.Request
			req, fetchErr = fhttp.NewRequestWithContext(probeCtx, fhttp.MethodGet, report.Endpoint, nil)
			if fetchErr == nil {
				fetchErr = applySofaRequestHeaders(cli, req)
			}
			if fetchErr == nil {
				var resp *fhttp.Response
				resp, fetchErr = cli.Do(req)
				if resp != nil {
					result.HTTPStatus, result.Protocol = resp.StatusCode, resp.Proto
					result.Server, result.ContentType = resp.Header.Get("Server"), resp.Header.Get("Content-Type")
					var readErr error
					body, readErr = readBounded(resp.Body, 32<<20)
					_ = resp.Body.Close()
					fetchErr = errors.Join(fetchErr, readErr)
					if resp.StatusCode != fhttp.StatusOK {
						fetchErr = errors.Join(fetchErr, fmt.Errorf("SofaScore HTTP %d", resp.StatusCode))
					}
				}
			}
			cli.CloseIdleConnections()
		}
		cancel()
		if fetchErr == nil {
			var search SofaSearchResponse
			if err := json.Unmarshal(body, &search); err != nil {
				fetchErr = fmt.Errorf("niepoprawny JSON wyszukiwania: %w", err)
			} else {
				result.Teams = search.ExtractTeams()
				if len(result.Teams) == 0 {
					fetchErr = fmt.Errorf("wyszukiwanie Chicago Fire nie zwróciło drużyn")
				}
			}
		}
		result.ResponseSize, result.Duration = len(body), time.Since(started).String()
		if fetchErr != nil {
			result.Error = fetchErr.Error()
			result.BodyPreview = string(body[:min(len(body), 512)])
		} else {
			report.SuccessfulVariants = append(report.SuccessfulVariants, variant.name)
		}
		info := archiveInfo{Bookmaker: "SOFASCORE", Scope: "diagnostyka-wyszukiwanie-chicago-fire-" + variant.name, Endpoint: report.Endpoint, Method: "GET", Status: result.HTTPStatus, ContentType: result.ContentType}
		if err := archiveResponse(context.WithValue(ctx, archiveContextKey{}, info), "odpowiedz-http", body, "http.bin", readableJSONValues(body), fetchErr); err != nil {
			return err
		}
		report.Results = append(report.Results, result)
		log.Printf("[SOFA-DIAG] variant=%s status=%d protocol=%q teams=%d duration=%s error=%q", result.Variant, result.HTTPStatus, result.Protocol, len(result.Teams), result.Duration, result.Error)
	}
	path := filepath.Join(dir, "sofascore_wyszukiwanie-chicago-fire_porownanie-ipv4-ipv6-http_"+report.RecordedAt.Format("20060102T150405.000000000Z")+".json")
	if err := atomicJSON(path, report); err != nil {
		return err
	}
	log.Printf("[SOFA-DIAG] END successful_variants=%q report=%q", report.SuccessfulVariants, path)
	if len(report.SuccessfulVariants) == 0 {
		return fmt.Errorf("żaden wariant nie zwrócił drużyn SofaScore; raport: %s", path)
	}
	return nil
}

func (client *ResilientClient) withScope(scope string) *ResilientClient {
	cp := *client
	info := archiveInfoFrom(client.ctx)
	info.Scope = scope
	cp.ctx = context.WithValue(client.ctx, archiveContextKey{}, info)
	return &cp
}
func (client *ResilientClient) archiveError() error {
	client.state.mu.Lock()
	defer client.state.mu.Unlock()
	return client.state.archiveErr
}
func (client *ResilientClient) recordArchiveError(err error) {
	if err == nil {
		return
	}
	client.state.mu.Lock()
	client.state.archiveErr = errors.Join(client.state.archiveErr, err)
	client.state.mu.Unlock()
}
func (client *ResilientClient) warn(message string) {
	if client.warning != nil {
		client.warning(message)
	}
}
func (client *ResilientClient) report(done, total int) {
	if client.progress != nil {
		client.progress("", "", done, total)
	}
}

func (client *ResilientClient) GetSofa(endpoint string) ([]byte, error) {
	var body []byte
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		if client.ctx.Err() != nil {
			return nil, client.ctx.Err()
		}
		if err = client.archiveError(); err != nil {
			return nil, err
		}
		body, err = client.getSofaAttempt(endpoint)
		if err == nil {
			return body, nil
		}
		if isArchiveFailure(err) || client.ctx.Err() != nil {
			break
		}
		lower := strings.ToLower(err.Error())
		if attempt != 0 || (!strings.Contains(lower, "goaway") && !strings.Contains(lower, "connection reset")) {
			break
		}
		client.tlsClient.CloseIdleConnections()
		timer := time.NewTimer(time.Second)
		select {
		case <-client.ctx.Done():
			timer.Stop()
			return nil, client.ctx.Err()
		case <-timer.C:
		}
	}
	client.warn("SofaScore " + endpoint + ": " + err.Error())
	return nil, err
}

func (client *ResilientClient) getSofaAttempt(endpoint string) ([]byte, error) {
	if client.background {
		release, err := waitBackgroundSofa(client.ctx)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	select {
	case globalSofaThrottle <- struct{}{}:
	case <-client.ctx.Done():
		return nil, client.ctx.Err()
	}
	defer func() { <-globalSofaThrottle }()
	timer := time.NewTimer(time.Duration(40+rand.IntN(30)) * time.Millisecond)
	select {
	case <-client.ctx.Done():
		timer.Stop()
		return nil, client.ctx.Err()
	case <-timer.C:
	}
	// Recheck after waiting for slots: another request may have received a block.
	if err := client.ctx.Err(); err != nil {
		return nil, err
	}
	if err := sofaCooldownError(); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(endpoint, "/") {
		return nil, fmt.Errorf("niepoprawny endpoint SofaScore")
	}
	reqURL := sofaBase + endpoint
	req, err := fhttp.NewRequestWithContext(client.ctx, fhttp.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	if err := applySofaRequestHeaders(client.tlsClient, req); err != nil {
		return nil, err
	}
	if strings.HasPrefix(endpoint, "/search/") {
		// Header.Get canonicalizes keys; these deliberately lowercase keys must be read directly.
		log.Printf("[SOFA-HTTP] REQUEST endpoint=%q tls_profile=Chrome_152 user_agent=%q requested_with=%q referer=%q captcha_configured=%t header_order=%q", endpoint, strings.Join(req.Header["user-agent"], ""), strings.Join(req.Header["x-requested-with"], ""), strings.Join(req.Header["referer"], ""), len(req.Header["x-captcha"]) > 0, req.Header[fhttp.HeaderOrderKey])
	}
	info := archiveInfoFrom(client.ctx)
	info.Endpoint, info.Method = reqURL, "GET"
	started := time.Now()
	resp, fetchErr := client.tlsClient.Do(req)
	var body []byte
	protocol := ""
	if resp != nil {
		defer resp.Body.Close()
		noteSofaBlocked(resp.StatusCode, resp.Header.Get("Retry-After"))
		protocol = resp.Proto
		info.Status, info.ContentType = resp.StatusCode, resp.Header.Get("Content-Type")
		var readErr error
		body, readErr = readBounded(resp.Body, 32<<20)
		fetchErr = errors.Join(fetchErr, readErr)
		if resp.StatusCode != 200 {
			fetchErr = errors.Join(fetchErr, fmt.Errorf("SofaScore HTTP %d", resp.StatusCode))
		}
	}
	log.Printf("[SOFA-HTTP] endpoint=%q status=%d protocol=%q content_type=%q bytes=%d duration=%s error=%v", endpoint, info.Status, protocol, info.ContentType, len(body), time.Since(started), fetchErr)
	if fetchErr != nil && len(body) > 0 {
		preview := body
		if len(preview) > 512 {
			preview = preview[:512]
		}
		log.Printf("[SOFA-HTTP] ERROR_BODY endpoint=%q preview=%q", endpoint, preview)
	}
	ctx := context.WithValue(client.ctx, archiveContextKey{}, info)
	if err := archiveResponse(ctx, "odpowiedz-"+calendarTeamSlug(endpoint), body, "http.bin", readableJSONValues(body), fetchErr); err != nil {
		client.recordArchiveError(err)
		return nil, errors.Join(fetchErr, err)
	}
	return body, fetchErr
}

var sofaCaptchaPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$`)
var sofaRequestedWithPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var sofaCaptchaLog = struct {
	sync.Mutex
	status string
}{}

func reportSofaCaptcha(status, message string) {
	sofaCaptchaLog.Lock()
	defer sofaCaptchaLog.Unlock()
	if sofaCaptchaLog.status == status {
		return
	}
	sofaCaptchaLog.status = status
	if message != "" {
		log.Printf("[SOFA-CAPTCHA] %s", message)
	}
}

func configuredSofaCaptcha() (string, time.Time) {
	token := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_CAPTCHA"))
	if token == "" {
		path := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_CAPTCHA_FILE"))
		if path == "" {
			path = filepath.Join(serverDataDir, "sofascore_captcha.txt")
		}
		raw, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			reportSofaCaptcha("missing", "")
			return "", time.Time{}
		}
		if err != nil {
			reportSofaCaptcha("read-error", "READ_ERROR nie można odczytać pliku tokenu")
			return "", time.Time{}
		}
		token = strings.TrimSpace(string(raw))
	}
	return validateSofaCaptcha(token)
}

func validateSofaCaptcha(token string) (string, time.Time) {
	token = strings.TrimSpace(token)
	if token == "" {
		reportSofaCaptcha("missing", "")
		return "", time.Time{}
	}
	if len(token) > 16*1024 || !sofaCaptchaPattern.MatchString(token) {
		reportSofaCaptcha("invalid", "INVALID niepoprawny format tokenu; pominięto nagłówki captcha")
		return "", time.Time{}
	}
	// Decode expiry metadata only; signature verification belongs to SofaScore.
	payload, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
	var claims struct {
		Expires int64 `json:"exp"`
	}
	if err != nil || json.Unmarshal(payload, &claims) != nil || claims.Expires <= 0 {
		reportSofaCaptcha("invalid", "INVALID brak poprawnego terminu ważności tokenu")
		return "", time.Time{}
	}
	expires := time.Unix(claims.Expires, 0).UTC()
	if !time.Now().Before(expires) {
		reportSofaCaptcha("expired", "EXPIRED token wygasł; pominięto nagłówki captcha, odśwież plik konfiguracji")
		return "", time.Time{}
	}
	reportSofaCaptcha("active-"+expires.Format(time.RFC3339), "ACTIVE expires_at="+expires.Format(time.RFC3339))
	return token, expires
}

type sofaBrowserSession struct {
	Headers fhttp.Header
	Cookies []*fhttp.Cookie
}

var sofaSessionLog = struct {
	sync.Mutex
	digest [32]byte
	loaded bool
}{}

// Parse a browser's POSIX "Copy as cURL" text as data. Never invoke a shell,
// expand variables or execute any option from the imported command.
func splitSofaCurl(raw string) ([]string, error) {
	var args []string
	var word strings.Builder
	var quote byte
	active := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if quote == '\'' {
			if ch == '\'' {
				quote = 0
			} else {
				word.WriteByte(ch)
			}
			continue
		}
		if ch == '\\' {
			if i+1 >= len(raw) {
				return nil, fmt.Errorf("sesja SofaScore: niedokończona sekwencja escape w pliku cURL")
			}
			i++
			if raw[i] == '\r' && i+1 < len(raw) && raw[i+1] == '\n' {
				i++
				continue
			}
			if raw[i] == '\n' {
				continue
			}
			word.WriteByte(raw[i])
			active = true
			continue
		}
		if quote == '"' {
			if ch == '"' {
				quote = 0
			} else {
				word.WriteByte(ch)
			}
			continue
		}
		switch ch {
		case '\'', '"':
			quote, active = ch, true
		case ' ', '\t', '\r', '\n':
			if active {
				args = append(args, word.String())
				word.Reset()
				active = false
			}
		case ';', '|', '<', '>', '&', '`':
			return nil, fmt.Errorf("sesja SofaScore: plik powinien zawierać jedno skopiowane polecenie cURL")
		default:
			word.WriteByte(ch)
			active = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("sesja SofaScore: niedomknięty cudzysłów w pliku cURL")
	}
	if active {
		args = append(args, word.String())
	}
	return args, nil
}

func readSofaBrowserSession() (*sofaBrowserSession, error) {
	path := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_CURL_FILE"))
	if path == "" {
		path = filepath.Join(serverDataDir, "sofascore_request.curl")
	}
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sesja SofaScore: nie można otworzyć pliku cURL")
	}
	defer file.Close()
	raw, err := readBounded(file, 128<<10)
	if err != nil {
		return nil, fmt.Errorf("sesja SofaScore: błąd odczytu lub przekroczony limit pliku cURL (128 KiB)")
	}
	args, err := splitSofaCurl(strings.TrimPrefix(string(raw), "\ufeff"))
	if err != nil {
		return nil, err
	}
	if len(args) == 0 || (args[0] != "curl" && args[0] != "curl.exe") {
		return nil, fmt.Errorf("sesja SofaScore: wklej pełne polecenie Copy as cURL (bash)")
	}
	session := &sofaBrowserSession{Headers: fhttp.Header{}}
	allowed := map[string]bool{"accept": true, "accept-language": true, "priority": true, "referer": true, "user-agent": true, "sec-ch-ua": true, "sec-ch-ua-mobile": true, "sec-ch-ua-platform": true, "sec-fetch-dest": true, "sec-fetch-mode": true, "sec-fetch-site": true, "x-captcha": true, "x-requested-with": true}
	var sourceURL, cookieRaw string
	method := "GET"
	for i := 1; i < len(args); i++ {
		option, value, inline := strings.Cut(args[i], "=")
		switch option {
		case "-H", "--header", "-b", "--cookie", "--url", "-X", "--request":
			if !inline {
				i++
				if i >= len(args) {
					return nil, fmt.Errorf("sesja SofaScore: brak wartości opcji w pliku cURL")
				}
				value = args[i]
			}
			if strings.ContainsAny(value, "\r\n\x00") {
				return nil, fmt.Errorf("sesja SofaScore: niepoprawne znaki w wartości opcji")
			}
			switch option {
			case "--url":
				sourceURL = value
			case "-X", "--request":
				method = strings.ToUpper(value)
			case "-b", "--cookie":
				cookieRaw += "; " + value
			default:
				name, headerValue, ok := strings.Cut(value, ":")
				if !ok {
					return nil, fmt.Errorf("sesja SofaScore: niepoprawny nagłówek w pliku cURL")
				}
				name, headerValue = strings.ToLower(strings.TrimSpace(name)), strings.TrimSpace(headerValue)
				if name == "cookie" {
					cookieRaw += "; " + headerValue
				} else if allowed[name] {
					session.Headers[name] = []string{headerValue}
				}
			}
		case "-I", "--head":
			method = "HEAD"
		default:
			if strings.HasPrefix(args[i], "https://") {
				sourceURL = args[i]
			}
		}
	}
	origin, err := url.Parse(sourceURL)
	if err != nil || origin.Scheme != "https" || origin.Hostname() != "www.sofascore.com" || (origin.Port() != "" && origin.Port() != "443") || !strings.HasPrefix(origin.Path, "/api/v1/") {
		return nil, fmt.Errorf("sesja SofaScore: źródło cURL musi być endpointem HTTPS www.sofascore.com/api/v1/")
	}
	if method != "GET" && method != "HEAD" {
		return nil, fmt.Errorf("sesja SofaScore: skopiuj żądanie GET albo HEAD")
	}
	if strings.Join(session.Headers["user-agent"], "") == "" {
		return nil, fmt.Errorf("sesja SofaScore: brak User-Agent w skopiowanym żądaniu")
	}
	request := &fhttp.Request{Header: fhttp.Header{"Cookie": {strings.TrimPrefix(cookieRaw, "; ")}}}
	seen := map[string]bool{}
	for _, cookie := range request.Cookies() {
		if seen[cookie.Name] {
			return nil, fmt.Errorf("sesja SofaScore: powtórzone nazwy cookies; skopiuj pojedyncze świeże żądanie")
		}
		seen[cookie.Name] = true
		cookie.Path, cookie.Secure = "/", true
		session.Cookies = append(session.Cookies, cookie)
	}
	if strings.Trim(cookieRaw, "; ") != "" && len(session.Cookies) == 0 {
		return nil, fmt.Errorf("sesja SofaScore: nie udało się odczytać cookies z pliku cURL")
	}
	digest := sha256.Sum256(raw)
	sofaSessionLog.Lock()
	if !sofaSessionLog.loaded || sofaSessionLog.digest != digest {
		log.Printf("[SOFA-SESSION] LOADED source=curl headers=%d cookies=%d captured_method=%s request_method=GET tls_profile=Chrome_152", len(session.Headers), len(session.Cookies), method)
		sofaSessionLog.digest, sofaSessionLog.loaded = digest, true
	}
	sofaSessionLog.Unlock()
	return session, nil
}

func applySofaRequestHeaders(client tls_client.HttpClient, req *fhttp.Request) error {
	req.Header = sofaRequestHeaders()
	session, err := readSofaBrowserSession()
	if err != nil {
		return err
	}
	var importedToken string
	var cookies []*fhttp.Cookie
	if session != nil {
		req.Header = session.Headers
		for _, cookie := range session.Cookies {
			if cookie.Name == "sofa_captcha" {
				importedToken = cookie.Value
			} else {
				cookies = append(cookies, cookie)
			}
		}
		if value := strings.Join(req.Header["x-captcha"], ""); value != "" {
			if importedToken != "" && importedToken != value {
				return fmt.Errorf("sesja SofaScore: cookie sofa_captcha i X-Captcha mają różne wartości; skopiuj nowe żądanie")
			}
			importedToken = value
		}
		delete(req.Header, "x-captcha")
	}
	if value := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_REQUESTED_WITH")); sofaRequestedWithPattern.MatchString(value) {
		req.Header["x-requested-with"] = []string{value}
	}
	var token string
	var expires time.Time
	if value := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_CAPTCHA")); value != "" {
		token, expires = validateSofaCaptcha(value)
	} else if importedToken != "" {
		token, expires = validateSofaCaptcha(importedToken)
	} else {
		token, expires = configuredSofaCaptcha()
	}
	if token != "" {
		req.Header["x-captcha"] = []string{token}
		cookies = append(cookies, &fhttp.Cookie{Name: "sofa_captcha", Value: token, Path: "/", Secure: true, Expires: expires})
	} else {
		// tls-client's jar uses MaxAge < 0 for removal, not Expires alone.
		cookies = append(cookies, &fhttp.Cookie{Name: "sofa_captcha", Value: "removed", Path: "/", Secure: true, MaxAge: -1})
	}
	// The jar adds Cookie once, without a second manually constructed header.
	client.SetCookies(req.URL, cookies)
	order := []string{"sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform", "user-agent", "accept", "accept-language", "priority", "referer", "sec-fetch-site", "sec-fetch-mode", "sec-fetch-dest", "x-requested-with", "x-captcha", "accept-encoding", "cookie"}
	req.Header[fhttp.HeaderOrderKey] = order
	return nil
}

func sofaRequestHeaders() fhttp.Header {
	requestedWith := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_REQUESTED_WITH"))
	headers := fhttp.Header{
		"user-agent":         {"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"},
		"accept":             {"*/*"},
		"accept-language":    {"en-US,en;q=0.9"},
		"referer":            {"https://www.sofascore.com/"},
		"sec-ch-ua":          {`"Not A(Brand";v="99", "Chromium";v="154"`},
		"sec-ch-ua-mobile":   {"?0"},
		"sec-ch-ua-platform": {`"Linux"`},
		"priority":           {"u=1, i"},
		"sec-fetch-dest":     {"empty"},
		"sec-fetch-mode":     {"cors"},
		"sec-fetch-site":     {"same-origin"},
	}
	if sofaRequestedWithPattern.MatchString(requestedWith) {
		headers["x-requested-with"] = []string{requestedWith}
	}
	return headers
}

func persistTeamCache(slug, id string) error {
	return persistTeamIdentity(slug, sofaTeamIdentity{ID: id, Manual: true, Version: 3})
}

func persistTeamIdentity(slug string, identity sofaTeamIdentity) error {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	path := getCacheFilePath("sofa_team_cache.json")
	m := map[string]string{}
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if m == nil {
		m = map[string]string{}
	}
	slug = strings.ToLower(strings.TrimSpace(slug))
	identitiesPath := getCacheFilePath("sofa_team_identity_cache.json")
	identities := map[string]sofaTeamIdentity{}
	if raw, err := os.ReadFile(identitiesPath); err == nil {
		if err := json.Unmarshal(raw, &identities); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if identities == nil {
		identities = map[string]sofaTeamIdentity{}
	}
	identities[slug] = identity
	if err := atomicJSON(identitiesPath, identities); err != nil {
		return err
	}
	if oldID := m[slug]; oldID != "" && oldID != identity.ID {
		log.Printf("[CACHE] TEAM_REPAIRED slug=%q old_id=%s new_id=%s name=%q", slug, oldID, identity.ID, identity.Name)
	}
	m[slug] = identity.ID
	return atomicJSON(path, m)
}
