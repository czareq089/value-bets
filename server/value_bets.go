// Value Bets server, phase 1: calendars, persistent cache, archives and HTTP API.
// All server source lives in this file. No Wails or desktop imports.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
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
			calendar.Warnings = append(calendar.Warnings, "Nie zapisano kalendarza na dysku VPS; cache działa do restartu serwera: "+diskErr.Error())
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
// together so the VPS needs just value_bets.go, go.mod and go.sum.
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
	Bookmaker   string `json:"bookmaker"`
	Scope       string `json:"scope"`
	Endpoint    string `json:"endpoint,omitempty"`
	Method      string `json:"method,omitempty"`
	RequestHex  string `json:"requestProtobufHex,omitempty"`
	Status      int    `json:"httpStatus,omitempty"`
	ContentType string `json:"contentType,omitempty"`
}

var archiveRoot string
var archiveSequence atomic.Uint64

func archiveInfoFrom(ctx context.Context) archiveInfo {
	info, _ := ctx.Value(archiveContextKey{}).(archiveInfo)
	return info
}

// Raw bytes are preserved before parsing. The JSON companion describes the
// request, errors and decoded content; field-number trees do not invent a schema.
var errArchive = errors.New("błąd zapisu archiwum")

func isArchiveFailure(err error) bool { return errors.Is(err, errArchive) }

func archiveResponse(ctx context.Context, kind string, raw []byte, extension string, decoded any, fetchErr error) (err error) {
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
	if len(s) > 110 {
		s = strings.TrimRight(s[:110], "-")
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
	cache    *CalendarCache
	client   *http.Client
	dataDir  string
	ctx      context.Context
	mu       sync.Mutex
	schedule map[string]collectorSchedule
	running  map[string]bool
	slots    chan struct{}
}

var serverBookmakers = []string{"STS", "BETCLIC", "SUPERBET"}

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
	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			apiError(w, 405, fmt.Errorf("wymagana metoda GET"))
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		jsonResponse(w, 200, map[string]any{"status": "ok", "apiVersion": 1, "phase": "calendars", "collectors": s.schedule, "running": s.running})
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
			filtered.Warnings = append(filtered.Warnings, "Dane z cache VPS, pobrane: "+calendar.FetchedAt+". Oczekiwanie na odświeżenie.")
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
	return mux
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
	listen := flag.String("listen", "0.0.0.0:8090", "adres nasłuchu HTTP API")
	dataDir := flag.String("data", "data", "katalog cache, harmonogramu i archiwum")
	cert := flag.String("tls-cert", "", "plik certyfikatu HTTPS (opcjonalnie)")
	key := flag.String("tls-key", "", "plik klucza HTTPS (opcjonalnie)")
	flag.Parse()
	if (*cert == "") != (*key == "") {
		log.Fatal("tls-cert i tls-key muszą być podane razem")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	s, err := newCalendarServer(ctx, *dataDir)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Addr: *listen, Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 180 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	go s.scheduler()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("VALUE_BETS_START listen=%s data=%s calendars=STS,BETCLIC,SUPERBET", *listen, *dataDir)
	if *cert != "" {
		err = server.ListenAndServeTLS(*cert, *key)
	} else {
		err = server.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	stop()
	<-shutdownDone
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.cache.stopAndWait(shutdownCtx)
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
