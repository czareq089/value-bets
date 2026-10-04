// Value Bets server: calendars, match analysis, SofaScore statistics and archives.
// All server source lives in this file. No Wails or desktop imports.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	cryptorand "crypto/rand"
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
	if info.AnalysisID != "" {
		day := now.Format("2006-01-02")
		if !info.AnalysisCreatedAt.IsZero() {
			day = info.AnalysisCreatedAt.UTC().Format("2006-01-02")
		}
		dir = filepath.Join(archiveRoot, day, "mecze", archiveName(info.Match)+"__"+info.AnalysisID, strings.ToLower(info.Bookmaker))
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
	analysisMu    sync.Mutex
	jobs          map[string]*analysisTask
	analysisQueue chan *analysisTask
	analysisDone  chan struct{}
	cache         *CalendarCache
	client        *http.Client
	dataDir       string
	ctx           context.Context
	mu            sync.Mutex
	schedule      map[string]collectorSchedule
	running       map[string]bool
	slots         chan struct{}
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
	serverDataDir = dir
	s.jobs = map[string]*analysisTask{}
	s.analysisQueue = make(chan *analysisTask, 4)
	s.analysisDone = make(chan struct{})
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
	go s.analysisWorker()
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
	select {
	case <-s.analysisDone:
	case <-shutdownCtx.Done():
		log.Printf("ANALYSIS_SHUTDOWN_TIMEOUT")
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
	History   PlayerStatHistory `json:"history"`
	CreatedAt time.Time         `json:"createdAt"`
}

type AnalyzedBet struct {
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
	HomeSlug string `json:"homeSlug"`
	AwaySlug string `json:"awaySlug"`
	HomeID   string `json:"homeId"`
	AwayID   string `json:"awayId"`
}

type LeagueOption struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

const sofaBase = "https://www.sofascore.com/api/v1"

type SofaTeamItem struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Slug    string `json:"slug"`
	Country struct {
		Name string `json:"name"`
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
	MinutesPlayed          int `json:"minutesPlayed"`
	TotalShots             int `json:"totalShots"`
	OnTargetScoringAttempt int `json:"onTargetScoringAttempt"`
	ShotOffTarget          int `json:"shotOffTarget"`
	BlockedScoringAttempt  int `json:"blockedScoringAttempt"`
	TotalTackle            int `json:"totalTackle"`
	WonTackle              int `json:"wonTackle"`
	InterceptionWon        int `json:"interceptionWon"`
	Fouls                  int `json:"fouls"`
	WasFouled              int `json:"wasFouled"`
	TotalOffside           int `json:"totalOffside"`
	Dispossessed           int `json:"dispossessed"`
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
	Substitute bool              `json:"substitute"`
	Position   string            `json:"position"`
	Statistics SofaPlayerStats   `json:"statistics"`
}

type SofaTeamPlayersResponse struct {
	Players []struct {
		Player SofaPlayerDetails `json:"player"`
	} `json:"players"`
}

type SofaLineupResponse struct {
	Confirmed bool `json:"confirmed"`
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

const playerCacheTTL = 6 * time.Hour
const playerCacheVersion = 4

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

	if time.Since(item.CreatedAt) > playerCacheTTL {
		delete(playerStatsCache, key)
		return nil, false
	}

	cp := item.History
	cp.DataFetchedAt = item.CreatedAt
	cp.FromCache = true
	return &cp, true
}

func savePlayerCache(key string, hist *PlayerStatHistory) {
	cacheLoadedOnce.Do(initPlayerCache)

	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()

	playerStatsCache[key] = CachedPlayerHistory{
		History:   *hist,
		CreatedAt: hist.DataFetchedAt,
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

func saveTeamCache(slug, id string) {
	slug = strings.TrimSpace(strings.ToLower(slug))
	id = strings.TrimSpace(id)
	if slug == "" || id == "" {
		return
	}

	if err := persistTeamCache(slug, id); err != nil {
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

	prefixes := []string{"1-fc-", "1-fsv-", "fc-", "sc-", "sv-", "vfb-", "vfl-", "tsg-", "rb-", "1-"}
	for _, p := range prefixes {
		s = strings.TrimPrefix(s, p)
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

	for cacheSlug, id := range cache {
		if canonicalTeamCore(cacheSlug) == cleanCore {
			log.Printf("[CACHE] Trafienie kanoniczne w cache dla: %s (rdzeń: %s) -> %s (ID: %s)", teamSlug, cleanCore, cacheSlug, id)
			return id
		}
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

func searchSofaTeamID(client *ResilientClient, teamSlug string) (string, error) {
	log.Printf("[SOFA-SEARCH] Rozpoczynam wyszukiwanie ID drużyny dla sluga: %s", teamSlug)
	if strings.TrimSpace(teamSlug) == "" {
		return "", fmt.Errorf("pusty slug")
	}

	queryName := strings.ReplaceAll(canonicalTeamCore(teamSlug), "-", " ")
	q := url.QueryEscape(queryName)

	body, err := client.GetSofa(fmt.Sprintf("/search/all?q=%s&page=0", q))
	if err != nil {
		log.Printf("[SOFA-SEARCH] Błąd zapytania wyszukiwania dla %s: %v", queryName, err)
		return "", err
	}

	var res SofaSearchResponse
	if err := json.Unmarshal(body, &res); err != nil {
		log.Printf("[SOFA-SEARCH] Błąd parsowania JSON dla %s: %v", queryName, err)
		return "", err
	}

	teams := res.ExtractTeams()
	if len(teams) == 0 {
		log.Printf("[SOFA-SEARCH] Brak wyników dla pełnej nazwy '%s'. Próba wyszukania po pierwszym tokenie...", queryName)
		parts := strings.Fields(queryName)
		if len(parts) > 1 {
			qShort := url.QueryEscape(parts[0])
			if bodyShort, errShort := client.GetSofa(fmt.Sprintf("/search/all?q=%s&page=0", qShort)); errShort == nil {
				var resShort SofaSearchResponse
				_ = json.Unmarshal(bodyShort, &resShort)
				teams = resShort.ExtractTeams()
				log.Printf("[SOFA-SEARCH] Wyniki po pierwszym słowie '%s': znaleziono %d drużyn", parts[0], len(teams))
			}
		}
	}

	if len(teams) == 0 {
		log.Printf("[SOFA-SEARCH] Nie odnaleziono żadnej drużyny dla: %s", teamSlug)
		return "", fmt.Errorf("nie znaleziono drużyny dla: %s", teamSlug)
	}

	cleanTarget := canonicalTeamCore(teamSlug)
	for _, t := range teams {
		if canonicalTeamCore(t.Slug) == cleanTarget || canonicalTeamCore(t.Name) == cleanTarget {
			idStr := strconv.Itoa(t.ID)
			log.Printf("[SOFA-SEARCH] Sukces! Dopasowano kanonicznie: %s -> ID %s (nazwa: %s)", teamSlug, idStr, t.Name)
			saveTeamCache(teamSlug, idStr)
			return idStr, nil
		}
	}

	firstID := strconv.Itoa(teams[0].ID)
	log.Printf("[SOFA-SEARCH] Brak dokładnego dopasowania kanonicznego. Wybór pierwszego wyniku: %s -> ID %s (nazwa: %s)", teamSlug, firstID, teams[0].Name)
	saveTeamCache(teamSlug, firstID)
	return firstID, nil
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
	// Sprawdzenie pamięci podręcznej (Cache 6h)
	cacheKey := fmt.Sprintf("v%d_%d_%d_%s", playerCacheVersion, playerID, matchLimit, allowedTournamentIDs)
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
		StatisticsMap    map[string]struct {
			MinutesPlayed int `json:"minutesPlayed"`
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

		opp := "Mecz"
		if e.HomeTeam.Name != "" && e.AwayTeam.Name != "" {
			playedTeamID := pEvents.PlayedForTeamMap[evIDStr]
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

	matchSem := make(chan struct{}, 2)

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

			resultsChan <- matchStatResult{
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
		savePlayerCache(cacheKey, hist)
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
		playersToFetch = squad
		log.Printf("[COMPREHENSIVE] Brak pasujących filtrów graczy. Pobieranie historii dla CAŁEJ kadry (%d) dla %s", len(playersToFetch), teamDisplayName)
	}
	sort.Slice(playersToFetch, func(i, j int) bool { return playersToFetch[i].ID < playersToFetch[j].ID })
	client.report(0, len(playersToFetch))

	var finalHistories []*PlayerStatHistory
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 2)

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

func findSofaUpcomingEventID(client *ResilientClient, homeID, awayID string) string {
	log.Printf("[SOFA-EVENT] Szukanie nadchodzącego meczu bezpośredniego pomiędzy %s a %s", homeID, awayID)
	body, err := client.GetSofa(fmt.Sprintf("/team/%s/events/next/0", homeID))
	if err != nil {
		log.Printf("[SOFA-EVENT] Błąd pobierania nadchodzących meczów: %v", err)
		return ""
	}

	var evRes SofaTeamEventsResponse
	if err := json.Unmarshal(body, &evRes); err != nil {
		log.Printf("[SOFA-EVENT] Błąd parsowania odpowiedzi zdarzeń: %v", err)
		return ""
	}

	hID, _ := strconv.Atoi(homeID)
	aID, _ := strconv.Atoi(awayID)

	for _, e := range evRes.Events {
		if (e.HomeTeam.ID == hID && e.AwayTeam.ID == aID) ||
			(e.HomeTeam.ID == aID && e.AwayTeam.ID == hID) {
			evIDStr := strconv.Itoa(e.ID)
			log.Printf("[SOFA-EVENT] Znaleziono nadchodzący mecz: EventID=%s (%s vs %s)", evIDStr, e.HomeTeam.Name, e.AwayTeam.Name)
			return evIDStr
		}
	}
	log.Printf("[SOFA-EVENT] Nie znaleziono nadchodzącego bezpośredniego meczu dla %s i %s", homeID, awayID)
	return ""
}

func fetchSofaConfirmedLineups(client *ResilientClient, eventID string) (map[string]bool, map[string]bool, bool) {
	if eventID == "" {
		return nil, nil, false
	}
	log.Printf("[SOFA-LINEUP] Pobieranie składów dla meczu eventID=%s", eventID)
	body, err := client.GetSofa(fmt.Sprintf("/event/%s/lineups", eventID))
	if err != nil {
		log.Printf("[SOFA-LINEUP] Błąd pobierania składów: %v", err)
		return nil, nil, false
	}

	var res SofaLineupResponse
	if err := json.Unmarshal(body, &res); err != nil || !res.Confirmed {
		log.Printf("[SOFA-LINEUP] Składy niepotwierdzone lub brak danych w odpowiedzi")
		return nil, nil, false
	}

	homeMap := make(map[string]bool)
	awayMap := make(map[string]bool)

	regLineup := func(m map[string]bool, p SofaLineupPlayer) {
		isStarter := !p.Substitute
		for _, raw := range []string{p.Player.Name, p.Player.ShortName, strings.ReplaceAll(p.Player.Slug, "-", " ")} {
			norm := NormalizePlayerName(raw)
			if norm != "" {
				m[norm] = isStarter
				parts := strings.Fields(norm)
				if len(parts) >= 2 {
					m[parts[len(parts)-1]] = isStarter
				}
			}
		}
	}

	for _, p := range res.Home.Players {
		regLineup(homeMap, p)
	}
	for _, p := range res.Away.Players {
		regLineup(awayMap, p)
	}

	log.Printf("[SOFA-LINEUP] Składy załadowane (potwierdzone: %v) - Gospodarze: %d graczy, Goście: %d graczy", res.Confirmed, len(res.Home.Players), len(res.Away.Players))
	return homeMap, awayMap, true
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

func CalculateValueBets(
	props []NormalizedProp,
	homePlayers, awayPlayers []*PlayerStatHistory,
	homeLineup, awayLineup map[string]bool,
) []AnalyzedBet {
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

	getLineupStatus := func(pName, side string) (string, bool) {
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

		if isStarter, exists := lineupMap[norm]; exists {
			if isStarter {
				return "STARTER", true
			}
			return "BENCH", false
		}

		parts := strings.Fields(norm)
		if len(parts) > 0 {
			last := parts[len(parts)-1]
			for lName, isStarter := range lineupMap {
				if strings.Contains(lName, last) {
					if isStarter {
						return "STARTER", true
					}
					return "BENCH", false
				}
			}
		}
		return "BENCH", false
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
		hitsOver, hitsUnder, total := 0, 0, 0
		for _, v := range values {
			total += v
			if float64(v) > prop.Line {
				hitsOver++
			} else {
				hitsUnder++
			}
		}

		var pHit, pPois float64
		lambda := float64(total) / float64(sample)

		if prop.IsUnder {
			pHit = float64(hitsUnder) / float64(sample)
			pPois = calcPoissonUnder(prop.Line, lambda)
		} else {
			pHit = float64(hitsOver) / float64(sample)
			pPois = calcPoissonOver(prop.Line, lambda)
		}

		pFinal := (pHit * 0.65) + (pPois * 0.35)
		if pFinal < 0.005 {
			pFinal = 0.005
		}
		if pFinal > 0.95 {
			pFinal = 0.95
		}

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

		status, isStarting := getLineupStatus(hist.FullName, hist.TeamSide)

		results = append(results, AnalyzedBet{
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
		task := &analysisTask{ctx: ctx, job: AnalysisJob{ID: fmt.Sprintf("%x", random[:]), Match: analysisTitle(query), Status: "queued", Stage: "queued", Message: "Oczekiwanie na analizę na VPS", CreatedAt: time.Now().UTC(), Request: query, Warnings: []string{}}, cancel: cancel}
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
		result := detectTeamsBySlugs(client, home, away)
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
	engine := analysisEngine{client: client, task: task}
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
	if saveErr := archiveResponse(ctx, "wynik-analizy-algorytm-poisson-v1", nil, "", job, err); saveErr != nil {
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
}

func detectTeamsBySlugs(client *ResilientClient, home, away string) TeamDetectionResult {
	cache := loadTeamCache()
	result := TeamDetectionResult{HomeSlug: home, AwaySlug: away, HomeID: findTeamIDInCache(home, cache), AwayID: findTeamIDInCache(away, cache)}
	if result.HomeID == "" && home != "" {
		result.HomeID, _ = searchSofaTeamID(client, home)
	}
	if result.AwayID == "" && away != "" {
		result.AwayID, _ = searchSofaTeamID(client, away)
	}
	return result
}

func (engine *analysisEngine) analyze(query AnalysisRequest) ([]AnalyzedBet, error) {
	task, client := engine.task, engine.client
	task.report("teams", "Rozpoznawanie drużyn w SofaScore", 0, 0)
	if query.HomeID == "" || query.AwayID == "" {
		for _, input := range []string{query.SuperbetURL, query.BetclicURL, query.STSURL} {
			if input == "" {
				continue
			}
			_, slug, provider := extractIDAndSlug(input)
			home, away := parseTeamsFromSlug(slug, provider)
			teams := detectTeamsBySlugs(client, home, away)
			if query.HomeID == "" {
				query.HomeID = teams.HomeID
			}
			if query.AwayID == "" {
				query.AwayID = teams.AwayID
			}
			break
		}
	}
	if query.HomeID == "" || query.AwayID == "" || query.HomeID == query.AwayID {
		return nil, fmt.Errorf("nie udało się ustalić dwóch różnych ID drużyn w SofaScore; wpisz je ręcznie")
	}
	task.report("odds", "Pobieranie aktualnych kursów Superbet, Betclic i STS", 0, 0)
	type sourceResult struct {
		book  string
		props []NormalizedProp
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
			props, err := fetchMatchProps(ctx, book, id)
			if err == nil && len(props) == 0 {
				err = fmt.Errorf("brak obsługiwanych rynków zawodników w odpowiedzi")
			}
			if saveErr := archiveResponse(ctx, "kursy-odczytane-rynki-zawodnikow", nil, "", map[string]any{"url": input, "eventId": id, "oddsObservedAt": time.Now().UTC(), "props": props}, err); saveErr != nil {
				err = errors.Join(err, saveErr)
			}
			sources <- sourceResult{book: book, props: props, err: err}
		}(source.book, source.input)
	}
	allProps := []NormalizedProp{}
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
	var homePlayers, awayPlayers []*PlayerStatHistory
	var homeErr, awayErr error
	var homeLineup, awayLineup map[string]bool
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
		eventID := findSofaUpcomingEventID(client, query.HomeID, query.AwayID)
		if eventID != "" {
			var ok bool
			homeLineup, awayLineup, ok = fetchSofaConfirmedLineups(client, eventID)
			if !ok {
				task.warn("SofaScore: brak potwierdzonych składów")
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
	task.report("calculation", "Obliczanie hitrate, kursów fair i EV", 0, 0)
	ctx := analysisContext(task.ctx, task.snapshot(), "ANALIZA", "dane-wejsciowe")
	if err := archiveResponse(ctx, "kursy-statystyki-sklady-uzyte-do-obliczen", nil, "", map[string]any{"algorithm": "poisson-v1", "request": query, "props": allProps, "homePlayers": homePlayers, "awayPlayers": awayPlayers, "homeLineup": homeLineup, "awayLineup": awayLineup}, nil); err != nil {
		return nil, err
	}
	results := CalculateValueBets(allProps, homePlayers, awayPlayers, homeLineup, awayLineup)
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

func fetchMatchProps(ctx context.Context, book, id string) ([]NormalizedProp, error) {
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
	mu         sync.Mutex
	archiveErr error
}
type ResilientClient struct {
	tlsClient tls_client.HttpClient
	ctx       context.Context
	state     *sofaClientState
	warning   func(string)
	progress  func(string, string, int, int)
}

func newSofaClient(ctx context.Context, warning func(string), progress func(string, string, int, int)) (*ResilientClient, error) {
	// Preserve the existing browser TLS profile and cookie handling on Ubuntu.
	cli, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), tls_client.WithTimeoutSeconds(8), tls_client.WithClientProfile(profiles.Firefox_120), tls_client.WithCookieJar(tls_client.NewCookieJar()), tls_client.WithRandomTLSExtensionOrder())
	if err != nil {
		return nil, err
	}
	return &ResilientClient{tlsClient: cli, ctx: ctx, state: &sofaClientState{}, warning: warning, progress: progress}, nil
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
	if !strings.HasPrefix(endpoint, "/") {
		return nil, fmt.Errorf("niepoprawny endpoint SofaScore")
	}
	reqURL := sofaBase + endpoint
	req, err := fhttp.NewRequestWithContext(client.ctx, fhttp.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header = fhttp.Header{
		"user-agent": {"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"},
		"accept":     {"*/*"}, "accept-language": {"en-US,en;q=0.9,pl;q=0.8"}, "referer": {"https://www.sofascore.com/"}, "x-requested-with": {"c88344"}, "sec-fetch-dest": {"empty"}, "sec-fetch-mode": {"cors"}, "sec-fetch-site": {"same-origin"}, "te": {"trailers"},
		fhttp.HeaderOrderKey: {"user-agent", "accept", "accept-language", "referer", "x-requested-with", "sec-fetch-dest", "sec-fetch-mode", "sec-fetch-site", "te"},
	}
	info := archiveInfoFrom(client.ctx)
	info.Endpoint, info.Method = reqURL, "GET"
	resp, fetchErr := client.tlsClient.Do(req)
	var body []byte
	if resp != nil {
		defer resp.Body.Close()
		info.Status, info.ContentType = resp.StatusCode, resp.Header.Get("Content-Type")
		var readErr error
		body, readErr = readBounded(resp.Body, 32<<20)
		fetchErr = errors.Join(fetchErr, readErr)
		if resp.StatusCode != 200 {
			fetchErr = errors.Join(fetchErr, fmt.Errorf("SofaScore HTTP %d", resp.StatusCode))
		}
	}
	ctx := context.WithValue(client.ctx, archiveContextKey{}, info)
	if err := archiveResponse(ctx, "odpowiedz-"+calendarTeamSlug(endpoint), body, "http.bin", readableJSONValues(body), fetchErr); err != nil {
		client.recordArchiveError(err)
		return nil, errors.Join(fetchErr, err)
	}
	return body, fetchErr
}

func persistTeamCache(slug, id string) error {
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
	m[strings.ToLower(strings.TrimSpace(slug))] = id
	return atomicJSON(path, m)
}
