// Manual analysis API: FootyMetrics counts and SofaScore enrichment.
package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"github.com/gorilla/websocket"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"io"
	"log"
	"math"
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
)

// betclic.go
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
	started := time.Now()
	betclicDebugf(parent, "RPC_START endpoint=%q client_timeout=%s rpc_timeout=25s request_bytes=%d protobuf_hex=%s", endpoint, client.Timeout, len(body), betclicPreview(body))
	defer func() {
		betclicDebugf(parent, "RPC_END duration=%s snapshot_bytes=%d error=%v", time.Since(started), len(snapshot), err)
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
	betclicDebugf(ctx, "HTTP_RESPONSE elapsed=%s status=%d protocol=%q content_type=%q content_length=%d grpc_status=%q grpc_message=%q", time.Since(started), resp.StatusCode, resp.Proto, resp.Header.Get("Content-Type"), resp.ContentLength, resp.Header.Get("Grpc-Status"), resp.Header.Get("Grpc-Message"))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Betclic HTTP %d", resp.StatusCode)
	}
	if err := grpcStatusError(resp.Header); err != nil {
		return nil, err
	}
	if contentType := resp.Header.Get("Content-Type"); !strings.HasPrefix(contentType, "application/grpc-web") || strings.Contains(contentType, "grpc-web-text") {
		return nil, fmt.Errorf("Betclic nieobsługiwany Content-Type: %q", contentType)
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
	readerCtx, stop := context.WithCancel(ctx)
	defer stop()
	defer reader.Close() // interrupts a pending read on timeout or cancellation
	go func() {
		for {
			header := make([]byte, 5)
			_, err := io.ReadFull(reader, header[:1])
			if err == nil {
				inFrame.Store(true)
				_, err = io.ReadFull(reader, header[1:])
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
					_, err = io.ReadFull(reader, frame[5:])
					if err == io.EOF {
						err = io.ErrUnexpectedEOF
					}
				}
			}
			inFrame.Store(false)
			select {
			case frames <- result{frame, err}:
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

// betclic_debug.go
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

// betclic_calendar.go
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

// calendar_cache.go
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
		} else {
			log.Printf("[CALENDAR-CACHE] DISK_MISSING path=%q", cache.path)
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
		if entry.Calendar != nil && !entry.FetchedAt.IsZero() && !entry.FetchedAt.After(now) && now.Sub(entry.FetchedAt) < 7*24*time.Hour {
			entry.Calendar.FetchedAt = entry.FetchedAt.Format(time.RFC3339Nano)
			cache.entries[key] = entry
			log.Printf("[CALENDAR-CACHE] RESTORED key=%q fetched_at=%s refresh_at=%s fresh=%t", key, entry.FetchedAt.UTC().Format(time.RFC3339), entry.FetchedAt.Add(CalendarCacheTTL).UTC().Format(time.RFC3339), now.Sub(entry.FetchedAt) < CalendarCacheTTL)
		} else {
			log.Printf("[CALENDAR-CACHE] DISK_REJECTED key=%q fetched_at=%s now=%s", key, entry.FetchedAt.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
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
		reason := "missing"
		var fetchedAt time.Time
		if entry, exists := cache.entries[key]; exists {
			reason, fetchedAt = "expired", entry.FetchedAt
		}
		if force {
			reason = "manual_refresh"
		}
		log.Printf("[CALENDAR-CACHE] FETCH key=%q force=%t reason=%s previous_fetched_at=%s", key, force, reason, fetchedAt.UTC().Format(time.RFC3339))
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
			cache.mu.Lock()
			previous, exists := cache.entries[key]
			cache.mu.Unlock()
			if exists && previous.Calendar != nil {
				stale := cloneCalendar(previous.Calendar)
				stale.Warnings = append(stale.Warnings, "Użyto poprzedniego cache; odświeżenie nie powiodło się: "+flight.err.Error())
				return stale, nil
			}
			return nil, flight.err
		}
		return cloneCalendar(flight.calendar), nil
	}
}

func (cache *CalendarCache) fetch(parent context.Context, key string, flight *calendarFlight, load func(context.Context) (*Calendar, error)) {
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
			if now.Sub(entry.FetchedAt) >= 7*24*time.Hour || entry.FetchedAt.After(now) {
				delete(cache.entries, oldKey)
			}
		}
		cache.entries[key] = calendarCacheEntry{FetchedAt: now, Calendar: calendar}
		if diskErr := cache.writeDisk(); diskErr != nil {
			log.Printf("[CALENDAR-CACHE] WRITE_ERROR path=%q error=%v", cache.path, diskErr)
			calendar.Warnings = append(calendar.Warnings, "Nie zapisano kalendarza na dysku; cache działa do zamknięcia aplikacji: "+diskErr.Error())
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

// sts.go
const STSWebSocket = "wss://www.sts.pl/sbk/api/sbk"

type CalendarEvent struct {
	AnalysisEligibility *AnalysisEligibility     `json:"analysisEligibility,omitempty"`
	MarketAvailability  *MatchMarketAvailability `json:"marketAvailability,omitempty"`
	ID                  string                   `json:"id"`
	Home                string                   `json:"home"`
	Away                string                   `json:"away"`
	Competition         string                   `json:"competition"`
	Country             string                   `json:"country"`
	StartTime           string                   `json:"startTime"`
	URL                 string                   `json:"url"`
	Live                bool                     `json:"live"`
	BetBuilder          bool                     `json:"betBuilder"`
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
		_, msg, err := conn.ReadMessage()
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

type SuperbetCalendarEvent = CalendarEvent
type SuperbetCalendar = Calendar

// Server runtime. Collectors below and above this section are intentionally kept
// together so the server needs just value_bets.go, go.mod and go.sum.
var strokeLetterReplacer = strings.NewReplacer(
	"ł", "l", "Ł", "L", "đ", "d", "Đ", "D", "ø", "o", "Ø", "O",
	"æ", "ae", "Æ", "AE", "œ", "oe", "Œ", "OE", "ß", "ss",
	"ẞ", "SS", "ð", "d", "Ð", "D", "þ", "th", "Þ", "TH", "ı", "i", "ŋ", "n", "Ŋ", "N",
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
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Superbet HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(result)
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
	MarketPasses                  CanonicalMarket = "PASSES"
	MarketAccuratePasses          CanonicalMarket = "ACCURATE_PASSES"
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
	SuperSubEligible *bool           `json:"superSubEligible,omitempty"`
	Bookmaker        Bookmaker       `json:"bookmaker"`
	PlayerName       string          `json:"playerName"`
	MarketType       CanonicalMarket `json:"marketType"`
	MarketLabel      string          `json:"marketLabel"`
	Line             float64         `json:"line"`
	IsUnder          bool            `json:"isUnder"`
	Odds             float64         `json:"odds"`
	MarketID         string          `json:"marketId"`
	SelectionID      string          `json:"selectionId"`
}

type PlayerStatHistory struct {
	footyData *footyTable
	// Public analysis IDs belong to FootyMetrics. SofaScore IDs are optional,
	// resolved only when the user opens this player's chart.
	SofaPlayerID             int                                    `json:"sofaPlayerId,omitempty"`
	PlayerAliases            []string                               `json:"-"`
	StatsSource              string                                 `json:"statsSource,omitempty"`
	SourcePlayerID           int                                    `json:"sourcePlayerId,omitempty"`
	SourceTeamID             int                                    `json:"sourceTeamId,omitempty"`
	SourceEventIDs           []int                                  `json:"sourceEventIds,omitempty"`
	MarketHistories          map[CanonicalMarket]*PlayerStatHistory `json:"marketHistories,omitempty"`
	HistoryNotes             []string                               `json:"historyNotes,omitempty"`
	HistoryExhausted         bool                                   `json:"historyExhausted,omitempty"`
	MetricAvailability       map[CanonicalMarket][]bool             `json:"metricAvailability,omitempty"`
	SuperSubAvailability     map[CanonicalMarket][]bool             `json:"superSubAvailability,omitempty"`
	Ratings                  []*float64                             `json:"ratings"`
	MatchContexts            []PlayerMatchContext                   `json:"matchContexts"`
	EventIDs                 []int                                  `json:"eventIds"`
	MatchStartedAt           []time.Time                            `json:"matchStartedAt"`
	MatchTeamNames           []string                               `json:"matchTeamNames,omitempty"`
	MatchShotCounts          []*int                                 `json:"matchShotCounts,omitempty"`
	SubPlayerIDs             []int                                  `json:"subPlayerIds,omitempty"`
	SubShotCounts            []*int                                 `json:"subShotCounts,omitempty"`
	Incomplete               bool                                   `json:"incomplete"`
	FromCache                bool                                   `json:"fromCache"`
	DataFetchedAt            time.Time                              `json:"dataFetchedAt"`
	ID                       int                                    `json:"id"`
	FullName                 string                                 `json:"fullName"`
	TeamSide                 string                                 `json:"teamSide"`
	TeamID                   string                                 `json:"teamId"`
	TeamName                 string                                 `json:"teamName"`
	Matches                  int                                    `json:"matches"`
	MetricValues             map[CanonicalMarket][]int              `json:"metricValues"`
	MetricValuesWithSuperSub map[CanonicalMarket][]int              `json:"metricValuesWithSuperSub"`
	Minutes                  []int                                  `json:"minutes"`
	Positions                []string                               `json:"positions"`
	Opponents                []string                               `json:"opponents"`
	Dates                    []string                               `json:"dates"`
	MatchShots               [][]ShotCoordinate                     `json:"matchShots"`
}

type AnalyzedBet struct {
	SampleEventIDs          []int              `json:"sampleEventIds,omitempty"`
	HistoryNotes            []string           `json:"historyNotes,omitempty"`
	AnalysisID              string             `json:"analysisId,omitempty"`
	PlayerID                int                `json:"playerId"`
	ContextPending          bool               `json:"contextPending,omitempty"`
	SampleRatings           []*float64         `json:"sampleRatings"`
	FairModel               *FairModelDetails  `json:"fairModel,omitempty"`
	Bookmaker               Bookmaker          `json:"bookmaker"`
	PlayerName              string             `json:"playerName"`
	MarketType              CanonicalMarket    `json:"marketType"`
	MarketLabel             string             `json:"marketLabel"`
	Line                    float64            `json:"line"`
	IsUnder                 bool               `json:"isUnder"`
	BookmakerOdds           float64            `json:"bookmakerOdds"`
	FairOdds                float64            `json:"fairOdds"`
	HitRate                 float64            `json:"hitRate"`
	EdgePercent             float64            `json:"edgePercent"`
	EVWithTax               float64            `json:"evWithTax"`
	MatchesSample           int                `json:"matchesSample"`
	SampleValues            []int              `json:"sampleValues"`
	SampleValuesSuperSub    []int              `json:"sampleValuesSuperSub"`
	SampleSuperSubAvailable []bool             `json:"sampleSuperSubAvailable,omitempty"`
	SampleMinutes           []int              `json:"sampleMinutes"`
	SamplePositions         []string           `json:"samplePositions"`
	SampleOpponents         []string           `json:"sampleOpponents"`
	SampleDates             []string           `json:"sampleDates"`
	SampleShots             [][]ShotCoordinate `json:"sampleShots"`
	StatsSource             string             `json:"statsSource"`
	TeamSide                string             `json:"teamSide"`
	TeamID                  string             `json:"teamId"`
	TeamName                string             `json:"teamName"`
	Verdict                 string             `json:"verdict"`
	LineupStatus            string             `json:"lineupStatus"`
	IsStartingXI            bool               `json:"isStartingXI"`
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
	Rating *float64 `json:"rating"`
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
	// Optional display metadata can be a JSON number, string or null in SofaScore.
	// Preserve its source representation without blocking lineup classification.
	JerseyNumber json.RawMessage `json:"jerseyNumber,omitempty"`
	Grid         string          `json:"grid,omitempty"`
	Statistics   SofaPlayerStats `json:"statistics"`
}

type SofaTeamPlayersResponse struct {
	Players []struct {
		Player SofaPlayerDetails `json:"player"`
	} `json:"players"`
}

type SofaLineupResponse struct {
	Confirmed *bool `json:"confirmed"`
	Home      struct {
		Players   []SofaLineupPlayer `json:"players"`
		Formation string             `json:"formation,omitempty"`
	} `json:"home"`
	Away struct {
		Players   []SofaLineupPlayer `json:"players"`
		Formation string             `json:"formation,omitempty"`
	} `json:"away"`
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
	shots := []ShotCoordinate{}
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

type SofaPlayerMatchStatResponse struct {
	Statistics SofaPlayerStats `json:"statistics"`
	Position   string          `json:"position"`
}

type SofaSubstitutionsResponse struct {
	Incidents []struct {
		IncidentType string            `json:"incidentType"`
		PlayerIn     SofaPlayerDetails `json:"playerIn"`
		PlayerOut    SofaPlayerDetails `json:"playerOut"`
	} `json:"incidents"`
}

type SofaTeamEventsResponse struct {
	HasNextPage bool               `json:"hasNextPage"`
	Events      []SofaHistoryEvent `json:"events"`
}

type SofaPlayerEventsResponse struct {
	SofaTeamEventsResponse
	PlayedForTeamMap map[string]int `json:"playedForTeamMap"`
	StatisticsMap    map[string]struct {
		MinutesPlayed *int     `json:"minutesPlayed"`
		Rating        *float64 `json:"rating"`
	} `json:"statisticsMap"`
}

type SofaHistoryEvent struct {
	Season struct {
		ID int `json:"id"`
	} `json:"season"`
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
	HomeTeam SofaTeamItem `json:"homeTeam"`
	AwayTeam SofaTeamItem `json:"awayTeam"`
	Status   struct {
		Code int    `json:"code"`
		Type string `json:"type"`
	} `json:"status"`
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
		return id, slug, "STS"
	}

	reSb := regexp.MustCompile(`^(.*?)-(\d+)$`)
	if m := reSb.FindStringSubmatch(lastSegment); len(m) == 3 {
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
	prefixes := []string{"1-fc-", "1-fsv-", "fc-", "sc-", "sv-", "vfb-", "vfl-", "tsg-", "se-", "ec-", "1-"}
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
	"betis":                "real-betis",
	"betis-sevilla":        "real-betis",
	"betis-sewilla":        "real-betis",
	"real-betis-sevilla":   "real-betis",
	"real-betis-balompie":  "real-betis",
	"rb-lipsk":             "rb-leipzig",
	"lipsk":                "leipzig",
	"bahia-salvador":       "bahia",
	"bahia-salvador-ba":    "bahia",
	"werder":               "werder-bremen",
	"werder-brema":         "werder-bremen",
	"borussia-dortmun":     "borussia-dortmund",
	"alaves":               "deportivo-alaves",
	"athletic-bilbao":      "athletic-club",
	"athletic-club-bilbao": "athletic-club",
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
		if sofaWomenPattern.MatchString(part) || (i == len(parts)-1 && (part == "w" || part == "k" || part == "f" || part == "(k)" || part == "(w)" || part == "(f)")) {
			gender = "F"
			continue
		}
		kept = append(kept, part)
	}
	core = strings.Join(kept, "-")
	qualifier := sofaTeamQualifier(core)
	base := core
	if qualifier != "" {
		for _, suffix := range []string{"-" + qualifier, "-b", "-ii", "-reserves", "-reserve"} {
			base = strings.TrimSuffix(base, suffix)
		}
	}
	if alias, ok := teamSlugAliases[base]; ok {
		core = alias + strings.TrimPrefix(core, base)
	}
	return core, gender
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
	matched, _ := sofaTeamMatchReason(teamSlug, team, gender, false)
	return matched
}

// Optional numeric club-name components must not erase reserve numbers or age
// categories. Only zero-prefixed two-digit tokens or four-digit year forms are
// eligible, and only the fixture resolver may accept the resulting candidate.
func sofaClubYearToken(word string) bool {
	if len(word) != 2 && len(word) != 4 {
		return false
	}
	for _, digit := range word {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	if len(word) == 2 {
		return word[0] == '0'
	}
	return strings.HasPrefix(word, "18") || strings.HasPrefix(word, "19") || strings.HasPrefix(word, "20")
}

// A shared, distinctive sequence of whole words produces a candidate, not
// a verified identity. One internal club-year token may separate those words.
// Only the fixture resolver may accept these variants.
func sofaTeamNameVariant(left, right string) bool {
	a, aq := calendarTeamIdentity(left)
	b, bq := calendarTeamIdentity(right)
	if a == "" || b == "" || aq != bq || a == b {
		return false
	}
	shorter, longer := strings.Split(a, "-"), strings.Split(b, "-")
	if len(shorter) > len(longer) {
		shorter, longer = longer, shorter
	}
	if len(shorter) == len(longer) || len(longer)-len(shorter) > 3 {
		return false
	}
	anchor := false
	for _, word := range shorter {
		anchor = anchor || (len(word) >= 4 && !calendarGenericWords[word])
	}
	if !anchor {
		return false
	}
	for start := 0; start+len(shorter) <= len(longer); start++ {
		matched, skippedYear := 0, false
		for i := start; i < len(longer) && matched < len(shorter); i++ {
			if shorter[matched] == longer[i] {
				matched++
				continue
			}
			if matched > 0 && !skippedYear && sofaClubYearToken(longer[i]) {
				skippedYear = true
				continue
			}
			break
		}
		if matched == len(shorter) {
			return true
		}
	}
	return false
}

func sofaTeamMatchReason(teamSlug string, team SofaTeamItem, gender string, allowNameVariants bool) (bool, string) {
	if team.ID <= 0 {
		return false, "brak poprawnego ID drużyny"
	}
	if (team.Sport.ID != 0 && team.Sport.ID != 1) ||
		(team.Sport.Slug != "" && team.Sport.Slug != "football") {
		return false, "inna dyscyplina niż piłka nożna"
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
		return false, fmt.Sprintf("inna płeć: %s zamiast %s", actualGender, gender)
	}
	if target == "" {
		return false, "pusta nazwa drużyny"
	}
	_, qualifiers := calendarTeamIdentity(teamSlug)
	for _, name := range []string{team.Name, team.Slug} {
		_, nameGender := sofaTeamNameAndGender(name)
		if nameGender != "" && nameGender != gender {
			return false, fmt.Sprintf("nazwa wskazuje inną płeć: %s zamiast %s", nameGender, gender)
		}
		_, candidateQualifiers := calendarTeamIdentity(name)
		if candidateQualifiers != "" && candidateQualifiers != qualifiers {
			wanted := qualifiers
			if wanted == "" {
				wanted = "pierwszy zespół"
			}
			labels := strings.NewReplacer("reserves", "rezerwy", "women", "kobiety", "|", ", ")
			return false, fmt.Sprintf("inna kategoria: %s zamiast %s", labels.Replace(candidateQualifiers), labels.Replace(wanted))
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
		if core == target {
			return true
		}
		a, aq := calendarTeamIdentity(core)
		b, bq := calendarTeamIdentity(target)
		return a != "" && a == b && aq == bq
	}
	names := []string{team.Name, team.Slug}
	for _, name := range team.FieldTranslations.NameTranslation {
		names = append(names, name)
	}
	variant := false
	for _, name := range names {
		if matchesName(name) {
			return true, "zgodna nazwa po normalizacji"
		}
		core, _ := sofaTeamNameAndGender(name)
		variant = variant || sofaTeamNameVariant(target, core)
	}
	if variant {
		if !allowNameVariants {
			return false, "krótszy lub dłuższy wariant nazwy wymaga daty i potwierdzenia meczu"
		}
		if team.Sport.ID != 1 && team.Sport.Slug != "football" {
			return false, "wariant nazwy bez potwierdzonej dyscypliny"
		}
		return true, "wariant nazwy; wymagane potwierdzenie obu drużyn i godziny meczu"
	}
	candidate, _ := sofaTeamNameAndGender(team.Name)
	return false, fmt.Sprintf("różne nazwy po normalizacji: %q i %q", target, candidate)
}

func searchSofaTeamCandidates(client *ResilientClient, teamSlug, gender string, allowNameVariants bool) ([]SofaTeamItem, error) {
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
	if len(parts) > 1 {
		// Bookmakers often append a city. Search the club portion before it,
		// rather than returning unrelated clubs from a city-only query.
		shortened := parts[:len(parts)-1]
		for _, part := range shortened {
			if len(part) >= 4 && !calendarGenericWords[part] && !calendarClubWords[part] && sofaTeamQualifier(part) == "" {
				queries = append(queries, strings.Join(shortened, " "))
				break
			}
		}
		if len(parts[0]) >= 4 && !calendarGenericWords[parts[0]] && sofaTeamQualifier(parts[0]) == "" {
			queries = append(queries, parts[0])
		}
	}
	seenQueries := map[string]bool{}
	rejected := []string{}
	seenRejected := map[int]bool{}
	matches := []SofaTeamItem{}
	seenTeams := map[int]bool{}
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
		for _, team := range teams {
			matched, reason := sofaTeamMatchReason(teamSlug, team, gender, allowNameVariants)
			if !matched && !seenRejected[team.ID] && len(rejected) < 5 {
				seenRejected[team.ID] = true
				rejected = append(rejected, fmt.Sprintf("%s (ID %d, płeć %s): %s", team.Name, team.ID, team.Gender, reason))
			}
			log.Printf("INFO SofaScore: %s (ID %d): %s", team.Name, team.ID, reason)
			if matched && !seenTeams[team.ID] {
				matches = append(matches, team)
				seenTeams[team.ID] = true
			}
		}
	}
	// Include all relevant queries before resolving ambiguity by the fixture.
	if len(matches) > 0 {
		return matches, nil
	}
	if len(rejected) > 0 {
		return nil, fmt.Errorf("brak dopasowania nazwy %q i kategorii %s w SofaScore; odrzucone wyniki: %s", teamSlug, gender, strings.Join(rejected, "; "))
	}
	return nil, fmt.Errorf("brak dopasowania nazwy %q i kategorii %s w SofaScore; wyszukiwarka nie zwróciła drużyn", teamSlug, gender)
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

	refresh, err := getSofaTeamRefresh(client, teamID)
	evBody := refresh.Events
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

// FootyMetrics supplies the identities used by the analysis. Its IDs must never
// be used in SofaScore URLs; chart enrichment resolves that namespace separately.

type footyTeam struct {
	APID int    `json:"apid"`
	Name string `json:"name"`
}
type footyFixture struct {
	APID      int       `json:"apid"`
	Timestamp time.Time `json:"timestamp"`
	HomeID    int       `json:"hid"`
	AwayID    int       `json:"aid"`
	TeamID    int       `json:"tid"`
	LeagueID  int       `json:"lid"`
	Home      footyTeam `json:"Home"`
	Away      footyTeam `json:"Away"`
	Opponent  footyTeam `json:"opponent"`
}
type footyPlayer struct {
	APID      int             `json:"apid"`
	Name      string          `json:"name"`
	ShortName string          `json:"shortName"`
	Slug      string          `json:"slug"`
	Team      int             `json:"team"`
	Position  string          `json:"position"`
	TeamInfo  json.RawMessage `json:"Team,omitempty"`
}

// FootyMetrics sends both "team" (provider ID) and "Team" (display metadata).
// encoding/json otherwise also matches "Team" to a field tagged "team".
// Read the exact lowercase key separately and never substitute Team.id, which
// belongs to the site's internal ID namespace rather than provider APIDs.
func (player *footyPlayer) UnmarshalJSON(body []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return err
	}
	team := bytes.TrimSpace(fields["team"])
	delete(fields, "team")
	cleaned, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	type wirePlayer footyPlayer
	var decoded wirePlayer
	if err = json.Unmarshal(cleaned, &decoded); err != nil {
		return err
	}
	// /teams/table supplies shortName and slug, with no name field. Keep a
	// human-readable name for results and retain the full slug as a matching
	// alias; an empty display name must never erase an otherwise valid identity.
	decoded.Name = strings.TrimSpace(decoded.Name)
	decoded.ShortName = strings.TrimSpace(decoded.ShortName)
	decoded.Slug = strings.TrimSpace(decoded.Slug)
	if decoded.Name == "" {
		decoded.Name = decoded.ShortName
	}
	if decoded.Name == "" {
		decoded.Name = strings.TrimSpace(strings.ReplaceAll(decoded.Slug, "-", " "))
	}
	if len(team) > 0 && !bytes.Equal(team, []byte("null")) {
		if team[0] == '{' {
			var ref footyRecord
			if err = json.Unmarshal(team, &ref); err != nil {
				return err
			}
			if raw, exists := ref["apid"]; exists && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				id, valid := footyReferenceID(ref, "apid")
				if !valid {
					return fmt.Errorf("FootyMetrics: niepoprawne team.apid zawodnika %d", decoded.APID)
				}
				decoded.Team = id
			}
		} else {
			id, valid := footyReferenceID(footyRecord{"team": team}, "team")
			if !valid {
				return fmt.Errorf("FootyMetrics: niepoprawne team zawodnika %d", decoded.APID)
			}
			decoded.Team = id
		}
	}
	*player = footyPlayer(decoded)
	return nil
}

type footyRecord map[string]json.RawMessage
type footyTable struct {
	Fixtures  []footyFixture                    `json:"fixtures"`
	PivotData map[string]map[string]footyRecord `json:"pivotData"`
	Players   []footyPlayer                     `json:"players"`
}
type footyTeamBundle struct {
	TeamID        int
	Table         footyTable
	SofaEvents    SofaTeamEventsResponse
	Notes         []string
	NoPlayerFetch bool
}
type footyTeamFlight struct {
	done   chan struct{}
	bundle *footyTeamBundle
	err    error
}

func playerHistorySource(hist *PlayerStatHistory) string {
	if hist.StatsSource != "" {
		return hist.StatsSource
	}
	return "SOFASCORE"
}

func footyNumber(record footyRecord, key string) (float64, bool) {
	raw := bytes.TrimSpace(record[key])
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return 0, false
	}
	if raw[0] == '"' {
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return 0, false
		}
		raw = []byte(text)
	}
	n, err := strconv.ParseFloat(string(raw), 64)
	return n, err == nil && !math.IsNaN(n) && !math.IsInf(n, 0) && n >= 0
}

func footyCount(record footyRecord, key string) (int, bool) {
	n, ok := footyNumber(record, key)
	if !ok || n > 1000000 || math.Trunc(n) != n {
		return 0, false
	}
	return int(n), true
}

func footyReferenceID(record footyRecord, key string) (int, bool) {
	raw := bytes.TrimSpace(record[key])
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return 0, false
	}
	if raw[0] == '"' {
		var value string
		if json.Unmarshal(raw, &value) != nil {
			return 0, false
		}
		raw = []byte(value)
	}
	id, err := strconv.Atoi(string(raw))
	return id, err == nil && id >= 0
}

func footyText(record footyRecord, key string) string {
	var value string
	_ = json.Unmarshal(record[key], &value)
	return value
}

var footyMetricFields = map[CanonicalMarket]string{
	MarketShotsTotal: "sh", MarketShotsOnTarget: "sot", MarketShotsOffTarget: "sof", MarketShotsBlocked: "shbl",
	MarketShotsOutsideBox: "shob", MarketShotsOnTargetOutsideBox: "sotob", MarketTackles: "tacklesWon",
	MarketPasses: "passes", MarketAccuratePasses: "accp", MarketInterceptions: "interceptions",
	MarketFoulsCommitted: "foulsC", MarketFoulsDrawn: "foulsD", MarketOffsides: "offsides", MarketDispossessed: "dispossessed",
}

func footyMetricGroup(market CanonicalMarket) string {
	switch market {
	case MarketPasses, MarketAccuratePasses:
		return "distribution"
	case MarketFoulsCommitted, MarketFoulsDrawn:
		return "discipline"
	case MarketTackles, MarketInterceptions, MarketDispossessed:
		return "defense"
	default:
		return "attack"
	}
}

// /players/table uses sparse counters: an omitted counter in a played row
// means zero (the FootyMetrics chart uses Number(row[key]) || 0). Apply that
// convention only to this full player-stat response, never to an unrelated
// group in /teams/table. Explicit nulls and malformed values remain unknown.
func normalizeFootyPlayerCounters(table *footyTable) {
	for _, fixtures := range table.PivotData {
		for _, record := range fixtures {
			minutes, played := footyCount(record, "mins")
			if !played || minutes <= 0 || minutes > 150 {
				continue
			}
			for _, field := range footyMetricFields {
				if _, present := record[field]; !present {
					record[field] = json.RawMessage("0")
				}
			}
		}
	}
}

func footyMetric(record footyRecord, market CanonicalMarket) (int, bool) {
	if field := footyMetricFields[market]; field != "" {
		return footyCount(record, field)
	}
	var total, outside string
	if market == MarketShotsInsideBox {
		total, outside = "sh", "shob"
	}
	if market == MarketShotsOnTargetInsideBox {
		total, outside = "sot", "sotob"
	}
	if total != "" {
		a, knownA := footyCount(record, total)
		b, knownB := footyCount(record, outside)
		return a - b, knownA && knownB && b <= a
	}
	return 0, false
}

func playerStatMarkets() []CanonicalMarket {
	return []CanonicalMarket{MarketShotsTotal, MarketShotsOnTarget, MarketShotsOffTarget, MarketShotsBlocked, MarketShotsInsideBox, MarketShotsOutsideBox, MarketShotsOnTargetInsideBox, MarketShotsOnTargetOutsideBox, MarketTackles, MarketPasses, MarketAccuratePasses, MarketInterceptions, MarketFoulsCommitted, MarketFoulsDrawn, MarketOffsides, MarketDispossessed}
}

func footyTeamNameMatches(left, right string) bool {
	a, aq := calendarTeamIdentity(left)
	b, bq := calendarTeamIdentity(right)
	return a != "" && b != "" && aq == bq && (a == b || sofaTeamNameVariant(a, b))
}

func resolveFootyFixture(client *ResilientClient) (footyFixture, error) {
	if client.statsHomeName == "" || client.statsAwayName == "" || client.statsFixture.StartTime.IsZero() {
		return footyFixture{}, fmt.Errorf("brak zweryfikowanej pary drużyn i terminu dla FootyMetrics")
	}
	zone, zoneErr := time.LoadLocation("Europe/Warsaw")
	if zoneErr != nil {
		return footyFixture{}, fmt.Errorf("FootyMetrics: strefa kalendarza: %w", zoneErr)
	}
	start := client.statsFixture.StartTime.In(zone)
	dates := []time.Time{start}
	if start.Hour() == 0 && start.Minute() < 10 {
		dates = append(dates, start.AddDate(0, 0, -1))
	}
	if start.Hour() == 23 && start.Minute() >= 50 {
		dates = append(dates, start.AddDate(0, 0, 1))
	}
	matches := map[int]footyFixture{}
	for _, date := range dates {
		var groups []struct {
			Fixtures []footyFixture `json:"fixtures"`
			Late     []footyFixture `json:"late"`
		}
		query := url.Values{"date": {date.Format("2006-01-02")}, "tz": {"Europe/Warsaw"}, "late": {"1"}}
		if err := getFootyJSON(client, "/api/front/fixtures?"+query.Encode(), &groups); err != nil {
			return footyFixture{}, err
		}
		for _, group := range groups {
			for _, fixture := range append(group.Fixtures, group.Late...) {
				if fixture.APID <= 0 || fixture.Home.APID <= 0 || fixture.Away.APID <= 0 || fixture.Home.APID == fixture.Away.APID || fixture.Timestamp.IsZero() {
					continue
				}
				if fixture.Timestamp.Sub(start).Abs() > 10*time.Minute {
					continue
				}
				if footyTeamNameMatches(fixture.Home.Name, client.statsHomeName) && footyTeamNameMatches(fixture.Away.Name, client.statsAwayName) {
					matches[fixture.APID] = fixture
				}
			}
		}
	}
	if len(matches) != 1 {
		return footyFixture{}, fmt.Errorf("FootyMetrics: nie potwierdzono jednoznacznie obu drużyn i terminu (%d meczów)", len(matches))
	}
	for _, fixture := range matches {
		return fixture, nil
	}
	return footyFixture{}, fmt.Errorf("FootyMetrics: brak meczu")
}

func footyTableURL(teamID int, group string) string {
	stat := map[string]string{"attack": "shots", "defense": "tackles-won", "discipline": "fouls-committed", "distribution": "passes"}[group]
	query := url.Values{"id": {strconv.Itoa(teamID)}, "stat": {stat}, "period": {"30"}, "location": {"both"}, "group": {group}, "selectedLeagues": {""}, "half": {""}, "dl": {"true"}, "se": {"false"}, "sm": {"easier"}}
	return "/api/front/teams/table?" + query.Encode()
}

func mergeFootyTable(target *footyTable, source footyTable, group string) error {
	if group != "player" && group != "attack" && group != "defense" && group != "discipline" && group != "distribution" {
		return fmt.Errorf("FootyMetrics: nieobsługiwana grupa %q", group)
	}
	if source.PivotData == nil || len(source.Fixtures) == 0 {
		return fmt.Errorf("FootyMetrics: brak fixtures/pivotData (%s)", group)
	}
	// Publish only a fully validated merge; an inconsistent response must not
	// partially overwrite the valid bulk observations already in use.
	destination := target
	merged := footyTable{Fixtures: append([]footyFixture(nil), target.Fixtures...), Players: append([]footyPlayer(nil), target.Players...), PivotData: map[string]map[string]footyRecord{}}
	for player, fixtures := range target.PivotData {
		merged.PivotData[player] = map[string]footyRecord{}
		for event, record := range fixtures {
			cloned := footyRecord{}
			for field, value := range record {
				cloned[field] = value
			}
			merged.PivotData[player][event] = cloned
		}
	}
	target = &merged
	if target.PivotData == nil {
		target.PivotData = map[string]map[string]footyRecord{}
	}
	seen := map[int]footyFixture{}
	for _, fixture := range target.Fixtures {
		seen[fixture.APID] = fixture
	}
	for _, fixture := range source.Fixtures {
		if old, ok := seen[fixture.APID]; ok {
			if !old.Timestamp.Equal(fixture.Timestamp) || old.HomeID != fixture.HomeID || old.AwayID != fixture.AwayID {
				return fmt.Errorf("FootyMetrics: niespójna tożsamość meczu %d", fixture.APID)
			}
		} else {
			target.Fixtures = append(target.Fixtures, fixture)
			seen[fixture.APID] = fixture
		}
	}
	players := map[int]bool{}
	for _, player := range target.Players {
		players[player.APID] = true
	}
	for _, player := range source.Players {
		if player.APID > 0 && !players[player.APID] {
			target.Players = append(target.Players, player)
			players[player.APID] = true
		}
	}
	allowed := map[string]bool{}
	for _, field := range []string{"mins", "sub", "subName", "subApid", "detailed", "rating", "team", "timestamp", "fid", "isHome"} {
		allowed[field] = true
	}
	for _, market := range playerStatMarkets() {
		field := footyMetricFields[market]
		if field != "" && (group == "player" || group == footyMetricGroup(market)) {
			allowed[field] = true
		}
	}
	for player, fixtures := range source.PivotData {
		if target.PivotData[player] == nil {
			target.PivotData[player] = map[string]footyRecord{}
		}
		for event, record := range fixtures {
			if target.PivotData[player][event] == nil {
				target.PivotData[player][event] = footyRecord{}
			}
			out := target.PivotData[player][event]
			if a, ok := footyCount(out, "mins"); ok {
				if b, known := footyCount(record, "mins"); known && a != b {
					return fmt.Errorf("FootyMetrics: niespójne minuty zawodnika %s w meczu %s", player, event)
				}
			}
			for field, value := range record {
				if allowed[field] {
					// A sparse player response may omit a count that the bulk
					// table already established. Null cannot erase that evidence.
					if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
						continue
					}
					out[field] = value
				}
			}
		}
	}
	*destination = merged
	return nil
}

func getFootyTeam(client *ResilientClient, teamID string) (*footyTeamBundle, error) {
	key := client.statsFixture.ID + "/" + teamID
	client.state.mu.Lock()
	if client.state.footyTeams == nil {
		client.state.footyTeams = map[string]*footyTeamFlight{}
	}
	if flight := client.state.footyTeams[key]; flight != nil {
		client.state.mu.Unlock()
		select {
		case <-client.ctx.Done():
			return nil, client.ctx.Err()
		case <-flight.done:
			return flight.bundle, flight.err
		}
	}
	flight := &footyTeamFlight{done: make(chan struct{})}
	client.state.footyTeams[key] = flight
	client.state.mu.Unlock()
	defer func() {
		client.state.mu.Lock()
		if errors.Is(flight.err, context.Canceled) || errors.Is(flight.err, context.DeadlineExceeded) {
			delete(client.state.footyTeams, key)
		}
		close(flight.done)
		client.state.mu.Unlock()
	}()
	footyTeamID, err := strconv.Atoi(teamID)
	if err != nil || footyTeamID <= 0 || (teamID != client.statsFixture.HomeID && teamID != client.statsFixture.AwayID) {
		flight.err = fmt.Errorf("FootyMetrics: nie potwierdzono strony drużyny")
		return nil, flight.err
	}
	bundle := &footyTeamBundle{TeamID: footyTeamID}
	for _, group := range []string{"attack", "defense", "discipline", "distribution"} {
		var table footyTable
		fetchErr := getFootyJSON(client, footyTableURL(footyTeamID, group), &table)
		if fetchErr == nil {
			fetchErr = mergeFootyTable(&bundle.Table, table, group)
		}
		if fetchErr != nil {
			bundle.Notes = append(bundle.Notes, "FootyMetrics "+group+": "+fetchErr.Error())
			if strings.Contains(fetchErr.Error(), "HTTP 403") || strings.Contains(fetchErr.Error(), "HTTP 429") {
				bundle.NoPlayerFetch = true
			}
			if group == "attack" {
				flight.err = fetchErr
				return nil, fetchErr
			}
		}
	}
	flight.bundle = bundle
	log.Printf("INFO FOOTYMETRICS_TEAM footy_team_id=%d players=%d fixtures=%d", footyTeamID, len(bundle.Table.PivotData), len(bundle.Table.Fixtures))
	return bundle, nil
}

func footyAnalysisSquad(bundle *footyTeamBundle) []SofaPlayerDetails {
	// This shared name/alias container carries FootyMetrics IDs here, never
	// SofaScore IDs. A separate lookup is required before browser enrichment.
	var players []SofaPlayerDetails
	for _, player := range bundle.Table.Players {
		if player.APID > 0 && (player.Team == 0 || player.Team == bundle.TeamID) {
			if NormalizePlayerName(player.Name) == "" {
				log.Printf("[FOOTYMETRICS-IDENTITY] pominięto zawodnika bez nazwy: team_id=%d player_id=%d", bundle.TeamID, player.APID)
				continue
			}
			players = append(players, SofaPlayerDetails{ID: player.APID, Name: player.Name, ShortName: player.ShortName, Slug: player.Slug, Position: player.Position})
		}
	}
	return players
}

type footyLineupPlayer struct {
	APID      int    `json:"apid"`
	Name      string `json:"name"`
	ShortName string `json:"shortName"`
	Slug      string `json:"slug"`
	Position  string `json:"position"`
	Detailed  string `json:"detailed"`
}

type footyLineupGrid struct {
	Lineup [][]*footyLineupPlayer `json:"lineup"`
}

type footyLineupBenchPlayer struct {
	Player   footyPlayer `json:"Player"`
	Detailed string      `json:"detailed"`
}

type footyMatchLineup struct {
	Home, Away map[string]bool
	Roles      map[int]string
	Confirmed  bool
	Notes      []string
}

func fetchFootyLineups(client *ResilientClient, home, away *footyTeamBundle) footyMatchLineup {
	result := footyMatchLineup{Home: map[string]bool{}, Away: map[string]bool{}, Roles: map[int]string{}}
	var response struct {
		ConfirmedAvailable bool                     `json:"confirmedAvailable"`
		PredictedAvailable bool                     `json:"predictedAvailable"`
		HomeConfirmedGrid  footyLineupGrid          `json:"homeConfirmedGrid"`
		AwayConfirmedGrid  footyLineupGrid          `json:"awayConfirmedGrid"`
		HomePredictedGrid  footyLineupGrid          `json:"homePredictedGrid"`
		AwayPredictedGrid  footyLineupGrid          `json:"awayPredictedGrid"`
		HomeConfirmedBench []footyLineupBenchPlayer `json:"homeConfirmedBench"`
		AwayConfirmedBench []footyLineupBenchPlayer `json:"awayConfirmedBench"`
	}
	// One optional fixture request; a slow or missing lineup must not stop the
	// FootyMetrics analysis or trigger a SofaScore request.
	ctx, cancel := context.WithTimeout(client.ctx, 10*time.Second)
	defer cancel()
	scoped := *client
	scoped.ctx = ctx
	if err := getFootyJSON(&scoped, "/api/front/fixtures/"+client.statsFixture.ID+"/lineup", &response); err != nil {
		result.Notes = append(result.Notes, "Składy FootyMetrics niedostępne; status zawodników nieznany: "+err.Error())
		return result
	}
	homeGrid, awayGrid := response.HomeConfirmedGrid, response.AwayConfirmedGrid
	result.Confirmed = response.ConfirmedAvailable
	if !response.ConfirmedAvailable {
		if !response.PredictedAvailable {
			// recentAvailable/homeRecentGrid describe a previous match, not the
			// upcoming fixture. They must never establish a current Starting XI.
			result.Notes = append(result.Notes, "FootyMetrics nie udostępnia jeszcze potwierdzonej ani przewidywanej XI dla tego meczu.")
			return result
		}
		homeGrid, awayGrid = response.HomePredictedGrid, response.AwayPredictedGrid
	}
	readSide := func(label string, bundle *footyTeamBundle, grid footyLineupGrid, bench []footyLineupBenchPlayer) map[string]bool {
		starters := map[int]footyPlayer{}
		roles := map[int]string{}
		for _, row := range grid.Lineup {
			for _, entry := range row {
				if entry == nil || entry.APID <= 0 {
					continue
				}
				name := strings.TrimSpace(entry.Name)
				if name == "" {
					name = strings.TrimSpace(entry.ShortName)
				}
				if name == "" {
					name = strings.ReplaceAll(entry.Slug, "-", " ")
				}
				starters[entry.APID] = footyPlayer{APID: entry.APID, Name: name, ShortName: entry.ShortName, Slug: entry.Slug, Position: entry.Position, Team: bundle.TeamID}
				roles[entry.APID] = strings.TrimSpace(entry.Detailed)
			}
		}
		if len(starters) != 11 {
			result.Notes = append(result.Notes, fmt.Sprintf("FootyMetrics: niepełna XI (%s: %d/11); status tej drużyny nieznany.", label, len(starters)))
			return map[string]bool{}
		}
		members := map[string]bool{}
		known := map[int]bool{}
		for _, player := range bundle.Table.Players {
			known[player.APID] = true
		}
		for id, player := range starters {
			members["player:"+strconv.Itoa(id)] = true
			if roles[id] != "" {
				result.Roles[id] = roles[id]
			}
			if !known[id] && NormalizePlayerName(player.Name) != "" {
				bundle.Table.Players = append(bundle.Table.Players, player)
				known[id] = true
			}
		}
		if result.Confirmed {
			for _, entry := range bench {
				if entry.Player.Team > 0 && entry.Player.Team != bundle.TeamID {
					continue
				}
				id := entry.Player.APID
				if id <= 0 {
					id = resolveFootyPlayer(bundle.Table.Players, bundle.TeamID, SofaPlayerDetails{Name: entry.Player.Name, ShortName: entry.Player.ShortName, Slug: entry.Player.Slug})
				}
				if id > 0 {
					if _, isStarter := starters[id]; !isStarter {
						members["player:"+strconv.Itoa(id)] = false
					}
				}
			}
		}
		log.Printf("[FOOTYMETRICS-LINEUP] fixture_id=%s team_id=%d confirmed=%t starters=%d listed=%d", client.statsFixture.ID, bundle.TeamID, result.Confirmed, len(starters), len(members))
		return members
	}
	result.Home = readSide("gospodarze", home, homeGrid, response.HomeConfirmedBench)
	result.Away = readSide("goście", away, awayGrid, response.AwayConfirmedBench)
	if len(result.Home) == 0 && len(result.Away) == 0 {
		return result
	}
	if response.ConfirmedAvailable {
		result.Notes = append(result.Notes, "Źródło składów: FootyMetrics, potwierdzona XI. Brak zawodnika w odpowiedzi nie potwierdza ławki.")
	} else {
		result.Notes = append(result.Notes, "Źródło składów: FootyMetrics, przewidywana XI (XI?); skład nie jest potwierdzony.")
	}
	return result
}

func footyHistoricalTeams(bundle *footyTeamBundle, fixture footyFixture, ownFootyID int, currentName string) (venue, ownName, opponentName string) {
	if ownFootyID <= 0 {
		return
	}
	switch ownFootyID {
	case fixture.HomeID:
		venue, ownName, opponentName = "HOME", fixture.Home.Name, fixture.Away.Name
	case fixture.AwayID:
		venue, ownName, opponentName = "AWAY", fixture.Away.Name, fixture.Home.Name
	default:
		return
	}
	if ownName == "" && ownFootyID == bundle.TeamID {
		ownName = currentName
	}
	if fixture.Opponent.APID == ownFootyID {
		if ownName == "" {
			ownName = fixture.Opponent.Name
		}
	} else if opponentName == "" && (fixture.Opponent.APID == fixture.HomeID || fixture.Opponent.APID == fixture.AwayID || (fixture.Opponent.APID == 0 && (fixture.TeamID == ownFootyID || ownFootyID == bundle.TeamID))) {
		opponentName = fixture.Opponent.Name
	}
	return
}

func footyHistoricalContext(bundle *footyTeamBundle, fixture footyFixture, ownFootyID int, currentName string) (PlayerMatchContext, int) {
	venue, ownName, opponentName := footyHistoricalTeams(bundle, fixture, ownFootyID, currentName)
	result := PlayerMatchContext{Venue: venue}
	// Retain the historical venue even when a player represented another club.
	// Its SofaScore identity is resolved from the player's own event feed later.
	if venue == "" || ownName == "" || opponentName == "" || ownFootyID != bundle.TeamID {
		return result, 0
	}
	homeName, awayName := ownName, opponentName
	if venue == "AWAY" {
		homeName, awayName = awayName, homeName
	}
	var found []int
	for i, event := range bundle.SofaEvents.Events {
		if event.Status.Code != 100 && event.Status.Type != "finished" {
			continue
		}
		if event.StartTS <= 0 || time.Unix(event.StartTS, 0).Sub(fixture.Timestamp).Abs() > 10*time.Minute {
			continue
		}
		if footyTeamNameMatches(event.HomeTeam.Name, homeName) && footyTeamNameMatches(event.AwayTeam.Name, awayName) {
			found = append(found, i)
		}
	}
	if len(found) != 1 {
		return result, 0
	}
	event := bundle.SofaEvents.Events[found[0]]
	result.LeagueID, result.SeasonID = event.Tournament.UniqueTournament.ID, event.Season.ID
	result.TeamID, result.OpponentID = event.HomeTeam.ID, event.AwayTeam.ID
	if result.Venue == "AWAY" {
		result.TeamID, result.OpponentID = result.OpponentID, result.TeamID
	}
	return result, event.ID
}

func buildFootyHistory(client *ResilientClient, bundle *footyTeamBundle, table footyTable, player SofaPlayerDetails, footyPlayerID int, teamName, tournaments string) *PlayerStatHistory {
	hist := &PlayerStatHistory{ID: player.ID, FullName: player.Name, TeamID: client.statsTeamID, TeamName: teamName, StatsSource: "FOOTYMETRICS", SourcePlayerID: footyPlayerID, SourceTeamID: bundle.TeamID, DataFetchedAt: time.Now().UTC(), MetricValues: map[CanonicalMarket][]int{}, MetricValuesWithSuperSub: map[CanonicalMarket][]int{}, MetricAvailability: map[CanonicalMarket][]bool{}, SuperSubAvailability: map[CanonicalMarket][]bool{}, HistoryNotes: append([]string{"Źródło historii: FootyMetrics; wartości nie zawierają automatycznie dodanych statystyk zmienników."}, bundle.Notes...)}
	allowed := map[int]bool{}
	if tournaments != "" && tournaments != "ALL" {
		for _, s := range strings.Split(tournaments, ",") {
			id, _ := strconv.Atoi(s)
			allowed[id] = true
		}
	}
	fixtures := append([]footyFixture(nil), table.Fixtures...)
	sort.Slice(fixtures, func(i, j int) bool { return fixtures[i].Timestamp.After(fixtures[j].Timestamp) })
	seen := map[int]bool{}
	for _, fixture := range fixtures {
		if fixture.APID <= 0 || seen[fixture.APID] || fixture.Timestamp.IsZero() || !fixture.Timestamp.Before(client.statsFixture.StartTime) {
			continue
		}
		seen[fixture.APID] = true
		record := table.PivotData[strconv.Itoa(footyPlayerID)][strconv.Itoa(fixture.APID)]
		minutes, known := footyCount(record, "mins")
		if !known || minutes <= 0 || minutes > 150 {
			continue
		}
		own := bundle.TeamID
		if id, ok := footyReferenceID(record, "team"); ok && id > 0 {
			own = id
		}
		context, eventID := footyHistoricalContext(bundle, fixture, own, teamName)
		_, historicalTeamName, opponentName := footyHistoricalTeams(bundle, fixture, own, teamName)
		if eventID == 0 && time.Now().Before(fixture.Timestamp.Add(4*time.Hour)) {
			continue
		}
		if len(allowed) > 0 && !allowed[fixture.LeagueID] {
			continue
		}
		hist.Matches++
		hist.EventIDs = append(hist.EventIDs, eventID)
		hist.SourceEventIDs = append(hist.SourceEventIDs, fixture.APID)
		hist.MatchStartedAt = append(hist.MatchStartedAt, fixture.Timestamp)
		hist.MatchTeamNames = append(hist.MatchTeamNames, historicalTeamName)
		hist.MatchContexts = append(hist.MatchContexts, context)
		hist.Minutes = append(hist.Minutes, minutes)
		hist.Positions = append(hist.Positions, footyText(record, "detailed"))
		hist.Opponents = append(hist.Opponents, opponentName)
		zone, zoneErr := time.LoadLocation("Europe/Warsaw")
		if zoneErr != nil {
			zone = time.UTC
		}
		hist.Dates = append(hist.Dates, fixture.Timestamp.In(zone).Format("02.01"))
		hist.MatchShots = append(hist.MatchShots, nil)
		hist.Ratings = append(hist.Ratings, nil)
		var shotCount *int
		if value, known := footyMetric(record, MarketShotsTotal); known {
			shotCount = &value
		}
		hist.MatchShotCounts = append(hist.MatchShotCounts, shotCount)
		hist.SubPlayerIDs = append(hist.SubPlayerIDs, -1)
		hist.SubShotCounts = append(hist.SubShotCounts, nil)
		subID, hasSub := footyReferenceID(record, "subApid")
		if minutes >= 90 && subID == 0 {
			hist.SubPlayerIDs[hist.Matches-1] = 0
			zero := 0
			hist.SubShotCounts[hist.Matches-1] = &zero
		}
		subRecord := table.PivotData[strconv.Itoa(subID)][strconv.Itoa(fixture.APID)]
		subMinutes, subPlayed := footyCount(subRecord, "mins")
		// A full 90 minutes needs no replacement. A missing replacement ID for
		// a shorter appearance does not prove there was no replacement.
		subKnown := minutes >= 90 || (hasSub && subID > 0 && subPlayed && subMinutes > 0)
		for _, market := range playerStatMarkets() {
			value, ok := footyMetric(record, market)
			subValue, subOK := footyMetric(subRecord, market)
			if minutes >= 90 && (!hasSub || subID == 0) {
				subValue, subOK = 0, true
			}
			hist.MetricValues[market] = append(hist.MetricValues[market], value)
			hist.MetricAvailability[market] = append(hist.MetricAvailability[market], ok)
			hist.MetricValuesWithSuperSub[market] = append(hist.MetricValuesWithSuperSub[market], value+subValue)
			hist.SuperSubAvailability[market] = append(hist.SuperSubAvailability[market], ok && subKnown && subOK)
		}
	}
	if len(allowed) == 0 {
		hist.HistoryNotes = append(hist.HistoryNotes, "Zakres historii: wszystkie rozgrywki dostępne w odpowiedzi FootyMetrics.")
	} else {
		hist.HistoryNotes = append(hist.HistoryNotes, "Zakres historii: wybrane rozgrywki FootyMetrics.")
	}
	return hist
}

func requiredPlayerMarkets(client *ResilientClient, player SofaPlayerDetails) []CanonicalMarket {
	found := map[CanonicalMarket]bool{}
	aliases := map[string]int{}
	regSofaAliases(aliases, player.Name, player.ShortName, player.Slug, player.ID)
	lookup := lookupMapInt(aliases)
	for _, prop := range client.statsProps {
		if matchPlayerStrict(prop.PlayerName, lookup) == strconv.Itoa(player.ID) {
			found[prop.MarketType] = true
		}
	}
	if len(found) == 0 {
		log.Printf("WARNING Nie przypisano rynków oferty do player_id=%d name=%q; przygotowanie pełnego zakresu statystyk", player.ID, player.Name)
		return playerStatMarkets()
	}
	result := []CanonicalMarket{}
	for market := range found {
		result = append(result, market)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func resolveFootyPlayer(players []footyPlayer, teamID int, player SofaPlayerDetails) int {
	forms := map[string]bool{}
	for _, name := range []string{player.Name, player.ShortName, strings.ReplaceAll(player.Slug, "-", " ")} {
		if normalized := NormalizePlayerName(name); normalized != "" {
			forms[normalized] = true
		}
	}
	matches := map[int]bool{}
	for _, candidate := range players {
		if candidate.APID <= 0 || (candidate.Team != 0 && candidate.Team != teamID) {
			continue
		}
		for _, name := range []string{candidate.Name, candidate.ShortName, strings.ReplaceAll(candidate.Slug, "-", " ")} {
			if forms[NormalizePlayerName(name)] {
				matches[candidate.APID] = true
			}
		}
	}
	if len(matches) != 1 {
		return 0
	}
	for id := range matches {
		return id
	}
	return 0
}

func regSofaAliases(m map[string]int, name, shortName, slug string, id int) {
	put := func(alias string) {
		if previous, exists := m[alias]; exists && previous != id {
			m[alias] = 0 // An ambiguous surname/initial never selects the last player.
			return
		}
		m[alias] = id
	}
	for _, raw := range []string{name, shortName, strings.ReplaceAll(slug, "-", " ")} {
		norm := NormalizePlayerName(raw)
		if norm == "" {
			continue
		}
		put(norm)
		parts := strings.Fields(norm)
		if len(parts) >= 2 {
			first := parts[0]
			last := parts[len(parts)-1]
			put(last)
			put(last + " " + first)
			if r := []rune(first); len(r) > 0 {
				put(string(r[0]) + " " + last)
				put(last + " " + string(r[0]))
			}
		}
	}
}

func lookupMapInt(src map[string]int) map[string]string {
	res := make(map[string]string, len(src))
	for k, v := range src {
		if v > 0 {
			res[k] = strconv.Itoa(v)
		} else {
			res[k] = ""
		}
	}
	return res
}

type sofaMatchLineup struct {
	Details                  *SofaLineupResponse
	Home, Away               map[string]bool
	HomePlayers, AwayPlayers []SofaPlayerDetails
	Confirmed                bool
}

func fetchSofaLineups(client *ResilientClient, eventID string) (sofaMatchLineup, error) {
	if eventID == "" {
		return sofaMatchLineup{}, fmt.Errorf("brak ID meczu do pobrania składów")
	}
	log.Printf("[SOFA-LINEUP] Pobieranie składów dla meczu eventID=%s", eventID)
	body, err := client.GetSofa(fmt.Sprintf("/event/%s/lineups", eventID))
	if err != nil {
		log.Printf("[SOFA-LINEUP] Błąd pobierania składów: %v", err)
		return sofaMatchLineup{}, err
	}

	var res SofaLineupResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return sofaMatchLineup{}, fmt.Errorf("odczyt składów: %w", err)
	}
	log.Printf("[SOFA-LINEUP] RESPONSE eventID=%s confirmed_present=%v home_players=%d away_players=%d", eventID, res.Confirmed != nil, len(res.Home.Players), len(res.Away.Players))
	if res.Confirmed == nil {
		return sofaMatchLineup{}, fmt.Errorf("odpowiedź składów nie zawiera statusu confirmed")
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
		return sofaMatchLineup{}, fmt.Errorf("brak zawodników z oznaczeniem substitute w składach (confirmed=%v)", *res.Confirmed)
	}
	log.Printf("[SOFA-LINEUP] Składy załadowane (potwierdzone: %v) - Gospodarze: %d graczy, Goście: %d graczy", *res.Confirmed, len(res.Home.Players), len(res.Away.Players))
	result := sofaMatchLineup{Home: homeMap, Away: awayMap, Confirmed: *res.Confirmed, Details: &res}
	for _, p := range res.Home.Players {
		if p.Player.ID > 0 {
			result.HomePlayers = append(result.HomePlayers, p.Player)
		}
	}
	for _, p := range res.Away.Players {
		if p.Player.ID > 0 {
			result.AwayPlayers = append(result.AwayPlayers, p.Player)
		}
	}
	return result, nil
}

var numRe = regexp.MustCompile(`(\d+(?:[.,]\d+)?)`)

type offerAudit struct {
	Reasons           map[string]int       `json:"reasons"`
	UnlistedOmissions int                  `json:"unlistedOmissions"`
	Bookmaker         string               `json:"bookmaker"`
	Coverage          string               `json:"coverage"`
	FullState         *bool                `json:"fullState,omitempty"`
	RawMarkets        int                  `json:"rawMarkets"`
	RawSelections     int                  `json:"rawSelections"`
	ParsedSelections  int                  `json:"parsedSelections"`
	UsableSelections  int                  `json:"usableSelections"`
	Omissions         []offerAuditOmission `json:"omissions"`
	Warnings          []string             `json:"warnings"`
}

type offerAuditOmission struct {
	Label  string `json:"label"`
	Reason string `json:"reason"`
	Count  int    `json:"count"`
}

type offerAuditContextKey struct{}

func parserAudit(book string, audits []*offerAudit) *offerAudit {
	audit := &offerAudit{}
	if len(audits) > 0 && audits[0] != nil {
		audit = audits[0]
	}
	*audit = offerAudit{Bookmaker: book, Coverage: "structured", Reasons: map[string]int{}, Omissions: []offerAuditOmission{}, Warnings: []string{}}
	return audit
}

func (audit *offerAudit) omit(label, reason string, count int) {
	if count == 0 {
		return
	}
	audit.Reasons[reason] += count
	for i := range audit.Omissions {
		if audit.Omissions[i].Label == label && audit.Omissions[i].Reason == reason {
			audit.Omissions[i].Count += count
			return
		}
	}
	if len(audit.Omissions) < 200 {
		audit.Omissions = append(audit.Omissions, offerAuditOmission{Label: label, Reason: reason, Count: count})
	} else {
		audit.UnlistedOmissions += count
	}
}

func finishParserAudit(audit *offerAudit, props []NormalizedProp) {
	audit.ParsedSelections = len(props)
	audit.UsableSelections = len(usableMatchProps(props))
	if audit.UsableSelections < len(props) {
		audit.Warnings = append(audit.Warnings, "Część selekcji odrzucono z powodu niepoprawnego kursu, linii lub nazwy zawodnika.")
	}
	sort.Slice(audit.Omissions, func(i, j int) bool {
		if audit.Omissions[i].Reason != audit.Omissions[j].Reason {
			return audit.Omissions[i].Reason < audit.Omissions[j].Reason
		}
		return audit.Omissions[i].Label < audit.Omissions[j].Label
	})
	log.Printf("[OFFER-AUDIT] bookmaker=%s coverage=%s markets=%d selections=%d parsed=%d usable=%d omitted_groups=%d", audit.Bookmaker, audit.Coverage, audit.RawMarkets, audit.RawSelections, audit.ParsedSelections, audit.UsableSelections, len(audit.Omissions))
}

func parsePropLine(text string) (float64, bool) {
	text = strings.ReplaceAll(strings.TrimSpace(text), ",", ".")
	match := numRe.FindStringSubmatch(text)
	if len(match) < 2 {
		return 0, false
	}
	line, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, false
	}
	if strings.Contains(text, match[1]+"+") {
		line -= .5
	}
	return line, line >= 0
}

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
	if strings.Contains(low, "odbior") && (strings.Contains(low, "przegran") || strings.Contains(low, "nieskuteczn") || strings.Contains(low, "nieudan")) {
		return true
	}
	compact := strings.NewReplacer(" ", "", "-", "", "_", "").Replace(low)
	for _, marker := range []string{"tackleslost", "losttackle", "unsuccessfultackle", "failedtackle"} {
		if strings.Contains(compact, marker) {
			return true
		}
	}
	won := strings.Contains(compact, "tackleswon") || strings.Contains(compact, "tacklewon") || strings.Contains(compact, "wontackle") || strings.Contains(compact, "successfultackle")
	if strings.Contains(compact, "totaltackle") && !won {
		return true // Total attempts are a different market from won tackles.
	}
	markers := []string{
		"lewa noga", "lewej nogi", "prawa noga", "prawej nogi",
		"glowa", "glowka", "head",
		"kluczow", "key pass", "cross", "dlugich podan", "long pass", "celnych dlugich",
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
	if strings.Contains(sd, "podan") || strings.Contains(s, "pass") {
		if isUnsupportedMethodOrZone(raw) {
			return "", false
		}
		if strings.Contains(sd, "celn") || strings.Contains(s, "accurate") || strings.Contains(s, "completed") {
			return MarketAccuratePasses, true
		}
		return MarketPasses, true
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

// Direction markers describe the outcome, not fragments of a player's name
// (for example, Gundersen contains "under"). Keep one-sided ladders as overs.
func propSelectionDirection(selection, playerName string) (isUnder, unambiguous bool) {
	text := removeDiacritics(strings.ToLower(selection))
	if name := removeDiacritics(strings.ToLower(strings.TrimSpace(playerName))); name != "" {
		text = strings.ReplaceAll(text, name, " ")
	}
	under, over := false, false
	for _, word := range strings.FieldsFunc(text, func(r rune) bool { return !unicode.IsLetter(r) }) {
		switch word {
		case "under", "ponizej":
			under = true
		case "over", "powyzej":
			over = true
		}
	}
	return under, !(under && over)
}

func ParseSuperbetProps(rawJSON string, audits ...*offerAudit) ([]NormalizedProp, error) {
	audit := parserAudit("SUPERBET", audits)
	log.Printf("[PARSER-SB] Rozpoczynam parsowanie oferty Superbet...")
	var resp struct {
		FullState *bool `json:"isFullState"`
		Markets   []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			Odds []struct {
				UUID             string            `json:"uuid"`
				MarketUUID       string            `json:"marketUuid"`
				SuperSubEligible *bool             `json:"superSubEligible"`
				Name             string            `json:"name"`
				Price            float64           `json:"price"`
				Status           string            `json:"status"`
				Specifiers       map[string]string `json:"specifiers"`
			} `json:"odds"`
		} `json:"markets"`
	}
	if err := json.Unmarshal([]byte(rawJSON), &resp); err != nil {
		log.Printf("[PARSER-SB] Błąd unmarshalu JSON: %v", err)
		return nil, err
	}

	var props []NormalizedProp
	audit.FullState = resp.FullState
	if resp.FullState != nil && !*resp.FullState {
		return nil, fmt.Errorf("Superbet: odpowiedź jest aktualizacją częściową, a nie pełną ofertą")
	}
	audit.RawMarkets = len(resp.Markets)
	for _, m := range resp.Markets {
		audit.RawSelections += len(m.Odds)
		if isHalfOrPeriodMarket(m.Name) || isUnsupportedMethodOrZone(m.Name) {
			audit.omit(m.Name, "unsupported-period-or-method", len(m.Odds))
			continue
		}

		canon, ok := mapMarketCanonical(m.Name)
		if !ok {
			audit.omit(m.Name, "unsupported-market", len(m.Odds))
			continue
		}

		for _, odd := range m.Odds {
			if odd.Status != "ACTIVE" && odd.Status != "" {
				audit.omit(m.Name, "inactive-selection", 1)
				continue
			}
			if isHalfOrPeriodMarket(odd.Name) || isUnsupportedMethodOrZone(odd.Name) {
				audit.omit(m.Name, "unsupported-selection", 1)
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
				audit.omit(m.Name, "missing-player", 1)
				continue
			}

			line, lineKnown := 0.0, false
			if odd.Specifiers != nil {
				if l, exists := odd.Specifiers["total"]; exists {
					line, lineKnown = parsePropLine(l)
				}
			}
			if !lineKnown {
				line, lineKnown = parsePropLine(odd.Name)
			}
			if !lineKnown {
				audit.omit(m.Name, "missing-line", 1)
				continue
			}

			isUnder, directionKnown := propSelectionDirection(odd.Name, pName)
			if !directionKnown {
				audit.omit(m.Name, "ambiguous-selection-direction", 1)
				continue
			}

			props = append(props, NormalizedProp{
				MarketID:         m.ID,
				SelectionID:      odd.UUID,
				SuperSubEligible: odd.SuperSubEligible,
				Bookmaker:        BookmakerSuperbet,
				PlayerName:       pName,
				MarketType:       canon,
				MarketLabel:      m.Name,
				Line:             line,
				IsUnder:          isUnder,
				Odds:             odd.Price,
			})
		}
	}
	log.Printf("[PARSER-SB] Sukces: sparsowano %d propozycji zakładów z Superbet", len(props))
	finishParserAudit(audit, props)
	return props, nil
}

func ParseSTSProps(rawJSON string, audits ...*offerAudit) ([]NormalizedProp, error) {
	audit := parserAudit("STS", audits)
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
			audit.RawMarkets++
			mMap, _ := mVal.(map[string]any)
			linesMap, _ := mMap["l"].(map[string]any)
			if len(linesMap) == 0 {
				audit.omit(mID, "missing-market-details", 1)
			}

			for _, lVal := range linesMap {
				lMap, _ := lVal.(map[string]any)
				outcomeData, _ := lMap["o"].(map[string]any)
				audit.RawSelections += len(outcomeData)
				rawName, _ := lMap["n"].(string)
				if rawName == "" {
					audit.omit(mID, "missing-market-label", len(outcomeData))
					continue
				}

				cleanName := strings.ReplaceAll(rawName, "(z dogrywką)", "")
				cleanName = strings.ReplaceAll(cleanName, "(z dogrywka)", "")
				cleanName = strings.TrimSpace(cleanName)

				if isHalfOrPeriodMarket(cleanName) {
					audit.omit(cleanName, "unsupported-period", len(outcomeData))
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
					audit.omit(cleanName, "unsupported-or-unrecognized-player-market", len(outcomeData))
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
						audit.omit(cleanName, "unusable-odds", 1)
						continue
					}

					lowerLine := strings.ToLower(strings.TrimSpace(lineStr))
					isUnder, directionKnown := propSelectionDirection(lowerLine, pName)
					if !directionKnown {
						audit.omit(cleanName, "ambiguous-selection-direction", 1)
						continue
					}
					isUnder = isUnder || strings.HasPrefix(lowerLine, "-")

					lineVal, lineKnown := parsePropLine(lowerLine)
					cleanLineStr := strings.TrimSpace(lowerLine)
					if strings.HasSuffix(cleanLineStr, "+") {
						valStr := strings.TrimSuffix(cleanLineStr, "+")
						if parsed, err := strconv.ParseFloat(valStr, 64); err == nil {
							lineVal = parsed - 0.5
							lineKnown = true
							isUnder = false
						}
					} else if m := numRe.FindStringSubmatch(cleanLineStr); len(m) > 1 {
						if parsed, err := strconv.ParseFloat(strings.ReplaceAll(m[1], ",", "."), 64); err == nil {
							lineVal = parsed
							lineKnown = true
						}
					}

					if !lineKnown {
						audit.omit(cleanName, "missing-line", 1)
						continue
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
	finishParserAudit(audit, props)
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

func ParseBetclicProps(rawBuf []byte, audits ...*offerAudit) []NormalizedProp {
	audit := parserAudit("BETCLIC", audits)
	audit.Coverage = "heuristic-protobuf"
	audit.Warnings = append(audit.Warnings, "Parser protobuf jest heurystyczny; liczby kandydatów nie dowodzą kompletności całej oferty. Porównaj odrzucone etykiety i surowe ramki z ofertą bukmachera.")
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
				if _, ok := mapMarketCanonical(t); ok {
					audit.RawMarkets++
					audit.omit(t, "detected-label-check-parser-coverage", 1)
				}
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
				audit.RawSelections++
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
							IsUnder:     dir == "poniżej",
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
	finishParserAudit(audit, unique)
	return unique
}

// ==========================================
// 6. ADAPTACYJNY FUZZY MATCHING I STATYSTYKA
// ==========================================

func sampleIndexes[T any](values []T, indexes []int) []T {
	if len(values) == 0 {
		return nil
	}
	result := make([]T, len(indexes))
	for i, index := range indexes {
		if index < len(values) {
			result[i] = values[index]
		}
	}
	return result
}

// Keep all sample metadata aligned while excluding unknown metric values.
// Legacy evidence without masks retains its original interpretation.
func playerMetricHistoryNotes(source *PlayerStatHistory, market CanonicalMarket) []string {
	if other := source.MarketHistories[market]; other != nil {
		source = other
	}
	notes := append([]string{}, source.HistoryNotes...)
	if mask, ok := source.MetricAvailability[market]; ok {
		missing := 0
		for i := range source.MetricValues[market] {
			if i < len(mask) && mask[i] {
				continue
			}
			missing++
			if missing > 5 {
				continue
			}
			date, opponent := "?", "?"
			if i < len(source.Dates) {
				date = source.Dates[i]
			}
			if i < len(source.Opponents) {
				opponent = source.Opponents[i]
			}
			notes = append(notes, fmt.Sprintf("%s · %s: brak potwierdzonych danych dla rynku %s; pominięto, bez zastępowania zerem.", date, opponent, market))
		}
		if missing > 5 {
			notes = append(notes, fmt.Sprintf("Dalsze %d występy również nie mają potwierdzonych danych tego rynku.", missing-5))
		}
	}
	if len(notes) > 12 {
		notes = append(notes[:11], fmt.Sprintf("Dalsze uwagi do historii: %d; szczegóły znajdują się w logach.", len(notes)-11))
	}
	return notes
}

func metricHistory(source *PlayerStatHistory, market CanonicalMarket) *PlayerStatHistory {
	owner := source
	if other := source.MarketHistories[market]; other != nil {
		source = other
	}
	result := *source
	result.TeamSide, result.TeamID, result.TeamName = owner.TeamSide, owner.TeamID, owner.TeamName
	result.MarketHistories = nil
	values := source.MetricValues[market]
	availability, masked := source.MetricAvailability[market]
	indexes := []int{}
	for i := range values {
		if !masked || (i < len(availability) && availability[i]) {
			indexes = append(indexes, i)
		}
	}
	result.Matches = len(indexes)
	result.MetricValues = map[CanonicalMarket][]int{market: sampleIndexes(values, indexes)}
	result.MetricValuesWithSuperSub = map[CanonicalMarket][]int{}
	result.SuperSubAvailability = nil
	if len(source.MetricValuesWithSuperSub[market]) == len(values) {
		result.MetricValuesWithSuperSub[market] = sampleIndexes(source.MetricValuesWithSuperSub[market], indexes)
		mask := source.SuperSubAvailability[market]
		if mask == nil {
			mask = make([]bool, len(values))
			for i := range mask {
				mask[i] = true
			}
		}
		result.SuperSubAvailability = map[CanonicalMarket][]bool{market: sampleIndexes(mask, indexes)}
	}
	// The selected rows are already validated. Reapplying the original sparse
	// mask to their new indices would drop different appearances on a second read.
	result.MetricAvailability = nil
	result.Ratings = sampleIndexes(source.Ratings, indexes)
	result.MatchContexts = sampleIndexes(source.MatchContexts, indexes)
	result.EventIDs = sampleIndexes(source.EventIDs, indexes)
	result.SourceEventIDs = sampleIndexes(source.SourceEventIDs, indexes)
	result.MatchStartedAt = sampleIndexes(source.MatchStartedAt, indexes)
	result.MatchTeamNames = sampleIndexes(source.MatchTeamNames, indexes)
	result.MatchShotCounts = sampleIndexes(source.MatchShotCounts, indexes)
	result.SubPlayerIDs = sampleIndexes(source.SubPlayerIDs, indexes)
	result.SubShotCounts = sampleIndexes(source.SubShotCounts, indexes)
	result.Minutes = sampleIndexes(source.Minutes, indexes)
	result.Positions = sampleIndexes(source.Positions, indexes)
	result.Opponents = sampleIndexes(source.Opponents, indexes)
	result.Dates = sampleIndexes(source.Dates, indexes)
	result.MatchShots = sampleIndexes(source.MatchShots, indexes)
	return &result
}

func NormalizePlayerName(s string) string {
	s = removeDiacritics(norm.NFKD.String(s))
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

const fairModelVersion = "context-hit-v14-footymetrics-lineups"

type PlayerMatchContext struct {
	TeamID     int    `json:"teamId"`
	OpponentID int    `json:"opponentId"`
	LeagueID   int    `json:"leagueId"`
	SeasonID   int    `json:"seasonId"`
	Venue      string `json:"venue"`
}

func historyMatchContexts(hist *PlayerStatHistory) []PlayerMatchContext {
	seen := map[PlayerMatchContext]bool{}
	result := []PlayerMatchContext{}
	add := func(contexts []PlayerMatchContext) {
		for _, match := range contexts {
			if !seen[match] {
				seen[match] = true
				result = append(result, match)
			}
		}
	}
	add(hist.MatchContexts)
	for _, other := range hist.MarketHistories {
		if other != nil {
			add(other.MatchContexts)
		}
	}
	return result
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
		// Aggregate tackles do not establish the number of won tackles.
		return "", ""
	case MarketPasses:
		return "totalPasses", ""
	case MarketAccuratePasses:
		return "accuratePasses", ""
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
	model := &FairModelDetails{Version: fairModelVersion, Distribution: "Weighted empirical / Jeffreys", MatchupFactor: 1, Features: []string{}, Notes: []string{"Model eksperymentalny; parametry wymagają kalibracji."}}
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
	// Each observed appearance contributes a hit or miss. A 90-minute zero
	// has the same weight as any other miss; a large surplus is just one hit.
	var hitWeight float64
	for i, value := range values {
		if float64(value) > line {
			hitWeight += weights[i]
		}
	}
	// Scale weighted proportions to the effective sample before Jeffreys smoothing.
	pOver := (hitWeight/sumW*model.EffectiveSample + .5) / (model.EffectiveSample + 1)
	// Existing matchup correction acts on the odds of a hit, with the same cap.
	oddsOver := pOver / (1 - pOver) * model.MatchupFactor
	pOver = oddsOver / (1 + oddsOver)
	probability := pOver
	if under {
		probability = 1 - pOver
	}
	model.Features = append(model.Features, "ważone trafienia linii, wygładzenie Jeffreysa")
	model.Notes = append(model.Notes, "Zapas ponad linię nie zwiększa szansy trafienia; zera z rozegranych występów są pełnymi nietrafieniami over. Minuty nie przeliczają historii na per90.", "Średnia liczność jest opisem historii; fair kurs wynika z trafień linii i korekty matchupu.")
	model.Probability = math.Max(.005, math.Min(.995, probability))
	return model
}

func CalculateValueBets(
	props []NormalizedProp,
	homePlayers, awayPlayers []*PlayerStatHistory,
	homeLineup, awayLineup map[string]bool,
	lineupsConfirmed bool,
	contexts ...analysisMatchContext,
) []AnalyzedBet {
	results, _ := calculateValueBetsContext(context.Background(), props, homePlayers, awayPlayers, homeLineup, awayLineup, lineupsConfirmed, nil, contexts...)
	return results
}

func calculateValueBetsContext(ctx context.Context, props []NormalizedProp, homePlayers, awayPlayers []*PlayerStatHistory, homeLineup, awayLineup map[string]bool, lineupsConfirmed bool, progress func(int, int), contexts ...analysisMatchContext) ([]AnalyzedBet, error) {
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

			registerHistoryAliases(strictMap, p)
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
	matchedPlayers := map[string]string{}
	type metricKey struct {
		player string
		market CanonicalMarket
	}
	type modelKey struct {
		metricKey
		line  float64
		under bool
	}
	metricHistories := map[metricKey]*PlayerStatHistory{}
	models := map[modelKey]*FairModelDetails{}

	for index, prop := range props {
		if index%32 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if progress != nil && index%250 == 0 {
			progress(index, len(props))
		}
		matchedID, resolved := matchedPlayers[prop.PlayerName]
		if !resolved {
			matchedID = matchPlayerStrict(prop.PlayerName, strictMap)
			matchedPlayers[prop.PlayerName] = matchedID
		}
		if matchedID == "" {
			continue
		}

		hist, exists := playerHistory[matchedID]
		if !exists || hist.Matches == 0 {
			continue
		}
		metric := metricKey{matchedID, prop.MarketType}
		if cached, ok := metricHistories[metric]; ok {
			hist = cached
		} else {
			hist = metricHistory(hist, prop.MarketType)
			metricHistories[metric] = hist
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
		var model *FairModelDetails
		var fairOdds, edge, evTax float64
		verdict := "PENDING"
		if !contextData.Pending {
			key := modelKey{metric, prop.Line, prop.IsUnder}
			model = models[key]
			if model == nil {
				model = contextualFairModel(hist, prop.MarketType, prop.Line, prop.IsUnder, contextData.Input)
				models[key] = model
			}
			fairOdds = 1.0 / model.Probability
			edge = (prop.Odds - fairOdds) / fairOdds * 100.0
			evTax = ((1.0/fairOdds)*(prop.Odds*0.88-1.0) - (1.0 - (1.0 / fairOdds))) * 100.0
			verdict = "NEUTRAL"
			if evTax > 0.0 {
				verdict = "VALUE"
			} else if evTax < -15.0 {
				verdict = "TRAP"
			}
		}

		label := prop.MarketLabel
		if prop.IsUnder && !strings.Contains(label, "Poniżej") {
			label += " (Poniżej)"
		}

		status, isStarting := getLineupStatus(hist.ID, hist.FullName, hist.TeamSide)

		results = append(results, AnalyzedBet{
			SampleEventIDs:          append([]int(nil), hist.EventIDs...),
			HistoryNotes:            playerMetricHistoryNotes(playerHistory[matchedID], prop.MarketType),
			PlayerID:                hist.ID,
			ContextPending:          contextData.Pending,
			SampleRatings:           append([]*float64(nil), hist.Ratings...),
			FairModel:               model,
			Bookmaker:               prop.Bookmaker,
			PlayerName:              hist.FullName,
			MarketType:              prop.MarketType,
			MarketLabel:             label,
			Line:                    prop.Line,
			IsUnder:                 prop.IsUnder,
			BookmakerOdds:           prop.Odds,
			FairOdds:                math.Round(fairOdds*100) / 100,
			HitRate:                 math.Round(pHit*1000) / 10,
			EdgePercent:             math.Round(edge*10) / 10,
			EVWithTax:               math.Round(evTax*10) / 10,
			MatchesSample:           sample,
			SampleValues:            append([]int(nil), values...),
			SampleValuesSuperSub:    append([]int(nil), valuesSuperSub...),
			SampleSuperSubAvailable: append([]bool(nil), hist.SuperSubAvailability[prop.MarketType]...),
			SampleMinutes:           append([]int(nil), hist.Minutes...),
			SamplePositions:         append([]string(nil), hist.Positions...),
			SampleOpponents:         append([]string(nil), hist.Opponents...),
			SampleDates:             append([]string(nil), hist.Dates...),
			SampleShots:             hist.MatchShots,
			StatsSource:             playerHistorySource(hist),
			TeamSide:                hist.TeamSide,
			TeamID:                  hist.TeamID,
			TeamName:                hist.TeamName,
			Verdict:                 verdict,
			LineupStatus:            status,
			IsStartingXI:            isStarting,
		})
	}

	// Keep the best quote for each distinct bookmaker/player/market/line.
	unique, indices := []AnalyzedBet{}, map[string]int{}
	for _, bet := range results {
		key := dataHash([]any{bet.Bookmaker, bet.PlayerID, bet.MarketType, bet.Line, bet.IsUnder})
		if i, exists := indices[key]; exists {
			if bet.BookmakerOdds > unique[i].BookmakerOdds {
				unique[i] = bet
			}
		} else {
			indices[key] = len(unique)
			unique = append(unique, bet)
		}
	}
	results = unique
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if progress != nil {
		progress(len(props), len(props))
	}
	log.Printf("[CALCULATE] Zakończono analizę. Dopasowano %d zakładów, utworzono %d przeanalizowanych rekordów", matchedPropsCount, len(results))
	return results, nil
}

// === Asynchronous analysis API and context-aware source transport ===

type sofaMatchContext struct {
	StartTime time.Time
	Gender    string
}

// Use the already cached calendar; detecting teams must not download it again.

type sofaResolvedTeamPair struct {
	Home, Away SofaTeamItem
}

func matchSofaTeamPair(client *ResilientClient, home, away []SofaTeamItem, match sofaMatchContext, unverified map[int]string) (sofaResolvedTeamPair, error) {
	if len(home) == 0 || len(away) == 0 || len(home) > 6 || len(away) > 6 {
		return sofaResolvedTeamPair{}, fmt.Errorf("nie można rozstrzygnąć drużyn na podstawie terminarza (%d i %d kandydatów)", len(home), len(away))
	}
	homeIDs, awayIDs := map[int]bool{}, map[int]bool{}
	for _, team := range home {
		homeIDs[team.ID] = true
	}
	for _, team := range away {
		awayIDs[team.ID] = true
	}
	pairs := map[[2]int]sofaResolvedTeamPair{}
	var lastErr error
	var rejectedIdentity string
	completeSide := false
	seenTeams := map[int]bool{}
	for _, side := range [][]SofaTeamItem{home, away} {
		complete := true
		for _, team := range side {
			if seenTeams[team.ID] {
				complete = false
				continue
			}
			seenTeams[team.ID] = true
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
					if delta := when.Sub(match.StartTime); delta < -10*time.Minute || delta > 10*time.Minute {
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
				homeTeam, awayTeam := event.HomeTeam, event.AwayTeam
				if homeIDs[awayTeam.ID] && awayIDs[homeTeam.ID] {
					homeTeam, awayTeam = awayTeam, homeTeam
				}
				h, a := homeTeam.ID, awayTeam.ID
				if h == a || !homeIDs[h] || !awayIDs[a] {
					continue
				}
				valid := true
				for _, candidate := range []SofaTeamItem{homeTeam, awayTeam} {
					if slug := unverified[candidate.ID]; slug != "" {
						if matched, reason := sofaTeamMatchReason(slug, candidate, match.Gender, !match.StartTime.IsZero()); !matched {
							valid = false
							rejectedIdentity = fmt.Sprintf("ID %d ze starego cache: %s", candidate.ID, reason)
							log.Printf("WARNING SofaScore: %s", rejectedIdentity)
						}
					}
				}
				if !valid {
					continue
				}
				pairs[[2]int{h, a}] = sofaResolvedTeamPair{Home: homeTeam, Away: awayTeam}
				log.Printf("[SOFA-DETECT] FIXTURE event_id=%d home_id=%d away_id=%d start=%s expected_start=%s", event.ID, h, a, when.UTC().Format(time.RFC3339), match.StartTime.Format(time.RFC3339))
			}
		}
		completeSide = completeSide || complete
	}
	// An empty home schedule must not prevent checking the away team's feed.
	if len(pairs) == 1 && completeSide {
		for _, pair := range pairs {
			return pair, nil
		}
	}
	if lastErr != nil {
		return sofaResolvedTeamPair{}, fmt.Errorf("weryfikacja terminarza: %w", lastErr)
	}
	if rejectedIdentity != "" {
		return sofaResolvedTeamPair{}, errors.New(rejectedIdentity)
	}
	return sofaResolvedTeamPair{}, fmt.Errorf("terminarz nie wskazał jednej pary drużyn: %d pasujących par", len(pairs))
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
	var searches sync.WaitGroup
	var legacyHints [2]bool
	unverified := map[int]string{}
	searchSide := func(index int, slug string) {
		searches.Add(1)
		go func() {
			defer searches.Done()
			if slug == "" {
				searchErrors[index] = fmt.Errorf("nie udało się odczytać nazwy drużyny")
			} else {
				candidates[index], searchErrors[index] = searchSofaTeamCandidates(client.withScope("wyszukiwanie-druzyny-"+slug), slug, match.Gender, !match.StartTime.IsZero())
			}
		}()
	}
	for index, team := range []struct {
		label, slug string
		id          *string
	}{{"Gospodarze", home, &result.HomeID}, {"Goście", away, &result.AwayID}} {
		if *team.id != "" {
			id, _ := strconv.Atoi(*team.id)
			candidates[index] = []SofaTeamItem{{ID: id}}
			continue
		}
		if !match.StartTime.IsZero() {
			if id, err := strconv.Atoi(findTeamIDInCache(team.slug, cache)); err == nil && id > 0 {
				// An old mapping is only a hint. Validate source name, category,
				// opponent and kickoff before promoting it to a verified mapping.
				candidates[index] = []SofaTeamItem{{ID: id}}
				legacyHints[index], unverified[id] = true, team.slug
				continue
			}
		}
		searchSide(index, team.slug)
	}
	searches.Wait()
	requiresPair := !match.StartTime.IsZero() || len(candidates[0]) > 1 || len(candidates[1]) > 1
	pairVerified := false
	if requiresPair {
		for attempt := 0; attempt < 2; attempt++ {
			var pairErr error
			if len(candidates[0]) > 0 && len(candidates[1]) > 0 {
				pair, err := matchSofaTeamPair(client, candidates[0], candidates[1], match, unverified)
				pairErr = err
				if err == nil {
					candidates[0], candidates[1] = []SofaTeamItem{pair.Home}, []SofaTeamItem{pair.Away}
					pairVerified = true
					break
				}
			}
			if attempt == 0 && (legacyHints[0] || legacyHints[1]) && client.ctx.Err() == nil {
				for index, slug := range []string{home, away} {
					if legacyHints[index] {
						log.Printf("WARNING SofaScore: ponowne wyszukiwanie %q po nieudanej weryfikacji ID ze starego cache", slug)
						searchSide(index, slug)
					}
				}
				searches.Wait()
				unverified = nil
				continue
			}
			if pairErr != nil {
				searchErrors[0], searchErrors[1] = pairErr, pairErr
			}
			break
		}
	}
	for index, team := range []struct {
		label, slug string
		id          *string
	}{{"Gospodarze", home, &result.HomeID}, {"Goście", away, &result.AwayID}} {
		// Never save a name variant (or retain a cached ID for this match)
		// if the opponent was missing or fixture verification did not finish.
		if requiresPair && !pairVerified {
			*team.id = ""
			if searchErrors[index] == nil {
				searchErrors[index] = fmt.Errorf("nie potwierdzono obu drużyn i terminu meczu w SofaScore")
			}
		}
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

var marketPlayerIDPattern = regexp.MustCompile(`^(\d+m\w+|f\d+|m\d+|\d+)$`)
var marketEventIDPattern = regexp.MustCompile(`^[0-9]{1,18}$`)

func usableMatchProps(props []NormalizedProp) []NormalizedProp {
	result := make([]NormalizedProp, 0, len(props))
	for _, prop := range props {
		switch prop.MarketType {
		case MarketShotsTotal, MarketShotsOnTarget, MarketShotsBlocked, MarketShotsOffTarget,
			MarketShotsOutsideBox, MarketShotsInsideBox, MarketShotsOnTargetOutsideBox,
			MarketShotsOnTargetInsideBox, MarketTackles, MarketPasses, MarketAccuratePasses, MarketDispossessed,
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

func fetchMatchProps(ctx context.Context, book, id string) ([]NormalizedProp, error) {
	props, err := fetchRawMatchProps(ctx, book, id)
	return usableMatchProps(props), err
}

func fetchRawMatchProps(ctx context.Context, book, id string) ([]NormalizedProp, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	audit, _ := ctx.Value(offerAuditContextKey{}).(*offerAudit)
	switch book {
	case "SUPERBET":
		endpoint := "https://production-superbet-bmb.freetls.fastly.net/betbuilder/v3/getBetbuilderMarketsForMatch?match_id=" + url.QueryEscape(id) + "&lang=pl-PL&target=SB_PL"
		var decoded json.RawMessage
		if err := getSuperbetCalendarJSON(ctx, client, endpoint, &decoded); err != nil {
			return nil, err
		}
		var identity struct {
			MatchID   string          `json:"matchId"`
			FullState *bool           `json:"isFullState"`
			Markets   json.RawMessage `json:"markets"`
		}
		if err := json.Unmarshal(decoded, &identity); err != nil {
			return nil, err
		}
		if identity.MatchID != id || identity.FullState == nil || !*identity.FullState || len(identity.Markets) == 0 || string(identity.Markets) == "null" {
			return nil, fmt.Errorf("Superbet v3: brak pełnego snapshotu dla meczu %s (matchId=%q)", id, identity.MatchID)
		}
		return ParseSuperbetProps(string(decoded), audit)
	case "STS":
		raw, err := FetchSTSSnapshot(ctx, STSWebSocket, id)
		if err != nil {
			return nil, err
		}
		return ParseSTSProps(string(raw), audit)
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
		return ParseBetclicProps(raw, audit), nil
	}
	return nil, fmt.Errorf("nieobsługiwane źródło")
}

type sofaTeamRefreshState struct {
	TeamID          string          `json:"teamId"`
	Revision        string          `json:"lastFinishedRevision"`
	LastFinished    time.Time       `json:"lastFinishedAt,omitempty"`
	FetchedAt       time.Time       `json:"fetchedAt"`
	NextCheckAt     time.Time       `json:"nextCheckAt"`
	AwaitFinishedID string          `json:"awaitFinishedId,omitempty"`
	Events          json.RawMessage `json:"events"`
}

func sofaHistoryTeamMatches(name string, team SofaTeamItem) bool {
	if footyTeamNameMatches(name, team.Name) || footyTeamNameMatches(name, team.Slug) {
		return true
	}
	for _, translated := range team.FieldTranslations.NameTranslation {
		if footyTeamNameMatches(name, translated) {
			return true
		}
	}
	return false
}

// Only identities and ratings come from this feed. All history counts and the
// selected sample remain exactly as supplied by FootyMetrics.
func resolveSofaHistoryEvents(client *ResilientClient, hist *PlayerStatHistory, needed map[int]bool, withRatings bool) string {
	if hist.SofaPlayerID <= 0 {
		return "Brak potwierdzonego ID zawodnika SofaScore; historia FootyMetrics pozostaje dostępna."
	}
	targets := map[int]bool{}
	var earliest time.Time
	for i := 0; i < hist.Matches; i++ {
		if needed != nil && !needed[i] {
			continue
		}
		if i >= len(hist.EventIDs) || i >= len(hist.MatchStartedAt) || hist.MatchStartedAt[i].IsZero() {
			continue
		}
		missingRating := withRatings && (i >= len(hist.Ratings) || hist.Ratings[i] == nil)
		if hist.EventIDs[i] > 0 && !missingRating {
			continue
		}
		targets[i] = true
		if earliest.IsZero() || hist.MatchStartedAt[i].Before(earliest) {
			earliest = hist.MatchStartedAt[i]
		}
	}
	if len(targets) == 0 {
		return ""
	}
	var all SofaPlayerEventsResponse
	all.PlayedForTeamMap = map[string]int{}
	all.StatisticsMap = make(map[string]struct {
		MinutesPlayed *int     `json:"minutesPlayed"`
		Rating        *float64 `json:"rating"`
	})
	var fetchErr error
	for page := 0; page < 20; page++ {
		endpoint := fmt.Sprintf("/player/%d/events/last/%d", hist.SofaPlayerID, page)
		var response SofaPlayerEventsResponse
		raw, _, err := getSofaHistoryData(client, endpoint, "player-events", time.Time{}, func(raw []byte) error {
			if err := json.Unmarshal(raw, &response); err != nil {
				return err
			}
			if response.Events == nil {
				return fmt.Errorf("brak listy meczów zawodnika")
			}
			return nil
		})
		if err != nil {
			fetchErr = err
			break
		}
		if err := json.Unmarshal(raw, &response); err != nil {
			fetchErr = err
			break
		}
		all.Events = append(all.Events, response.Events...)
		for id, teamID := range response.PlayedForTeamMap {
			all.PlayedForTeamMap[id] = teamID
		}
		for id, statistics := range response.StatisticsMap {
			all.StatisticsMap[id] = statistics
		}
		oldest := int64(0)
		for _, event := range response.Events {
			if event.StartTS > 0 && (oldest == 0 || event.StartTS < oldest) {
				oldest = event.StartTS
			}
		}
		if !response.HasNextPage || len(response.Events) == 0 || (oldest > 0 && oldest < earliest.Add(-10*time.Minute).Unix()) {
			break
		}
	}
	missing := 0
	for i := range targets {
		if hist.EventIDs[i] <= 0 && i < len(hist.Opponents) && i < len(hist.MatchContexts) {
			context := hist.MatchContexts[i]
			found := map[int]SofaHistoryEvent{}
			for _, event := range all.Events {
				if event.ID <= 0 || event.StartTS <= 0 || (event.Status.Code != 100 && event.Status.Type != "finished") || time.Unix(event.StartTS, 0).Sub(hist.MatchStartedAt[i]).Abs() > 10*time.Minute {
					continue
				}
				playedTeamID := all.PlayedForTeamMap[strconv.Itoa(event.ID)]
				// Some event pages omit playedForTeamMap. A positive appearance
				// in this player's statistics plus the Footy venue still confirms
				// the side; opponent and team names are checked below as usual.
				if playedTeamID == 0 {
					minutes := all.StatisticsMap[strconv.Itoa(event.ID)].MinutesPlayed
					if minutes != nil && *minutes > 0 {
						if context.Venue == "HOME" {
							playedTeamID = event.HomeTeam.ID
						}
						if context.Venue == "AWAY" {
							playedTeamID = event.AwayTeam.ID
						}
					}
				}
				own, opponent, venue := event.HomeTeam, event.AwayTeam, "HOME"
				if playedTeamID == event.AwayTeam.ID && playedTeamID > 0 {
					own, opponent, venue = event.AwayTeam, event.HomeTeam, "AWAY"
				}
				if playedTeamID <= 0 || playedTeamID != own.ID || (context.Venue != "" && context.Venue != venue) || (context.TeamID > 0 && context.TeamID != own.ID) {
					continue
				}
				if !sofaHistoryTeamMatches(hist.Opponents[i], opponent) {
					continue
				}
				if i < len(hist.MatchTeamNames) && hist.MatchTeamNames[i] != "" && !sofaHistoryTeamMatches(hist.MatchTeamNames[i], own) {
					continue
				}
				found[event.ID] = event
			}
			if len(found) == 1 {
				for id, event := range found {
					hist.EventIDs[i] = id
					own, opponent, venue := event.HomeTeam, event.AwayTeam, "HOME"
					if context.Venue == "AWAY" || all.PlayedForTeamMap[strconv.Itoa(id)] == event.AwayTeam.ID {
						own, opponent, venue = event.AwayTeam, event.HomeTeam, "AWAY"
					}
					hist.MatchContexts[i] = PlayerMatchContext{TeamID: own.ID, OpponentID: opponent.ID, Venue: venue, LeagueID: event.Tournament.UniqueTournament.ID, SeasonID: event.Season.ID}
				}
			}
		}
		if hist.EventIDs[i] <= 0 {
			missing++
			sourceID := 0
			if i < len(hist.SourceEventIDs) {
				sourceID = hist.SourceEventIDs[i]
			}
			opponent := ""
			if i < len(hist.Opponents) {
				opponent = hist.Opponents[i]
			}
			candidates := []string{}
			for _, event := range all.Events {
				if time.Unix(event.StartTS, 0).Sub(hist.MatchStartedAt[i]).Abs() <= 10*time.Minute && len(candidates) < 3 {
					candidates = append(candidates, fmt.Sprintf("%d:%s-%s", event.ID, event.HomeTeam.Name, event.AwayTeam.Name))
				}
			}
			log.Printf("[SOFA-MAPPING] player_id=%d name=%q footy_event_id=%d date=%s opponent=%q sofa_candidates=%q: brak jednoznacznego meczu w historii zawodnika", hist.SofaPlayerID, hist.FullName, sourceID, hist.MatchStartedAt[i].Format(time.RFC3339), opponent, strings.Join(candidates, "; "))
		}
		if withRatings && i < len(hist.Ratings) {
			if rating := all.StatisticsMap[strconv.Itoa(hist.EventIDs[i])].Rating; rating != nil && *rating > 0 && *rating <= 10 {
				hist.Ratings[i] = rating
			}
		}
	}
	if fetchErr != nil {
		return "Nie uzupełniono identyfikatorów lub ratingów SofaScore: " + fetchErr.Error()
	}
	if missing > 0 {
		return fmt.Sprintf("%d występów bez jednoznacznego odpowiednika w historii zawodnika SofaScore; szczegóły w logach [SOFA-MAPPING].", missing)
	}
	return ""
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

// Calendar snapshots use Polish dates even when the host runs in UTC.

var calendarClubWords = map[string]bool{"fc": true, "cf": true, "afc": true, "fk": true, "sk": true, "ks": true, "sc": true, "ac": true, "ssc": true, "as": true, "rc": true, "cd": true, "se": true, "ec": true, "sv": true}

var calendarGenericWords = map[string]bool{"real": true, "atletico": true, "athletic": true, "sporting": true, "racing": true, "nacional": true, "national": true, "america": true, "inter": true, "united": true, "city": true, "town": true, "deportivo": true, "club": true, "de": true, "da": true, "do": true, "the": true, "borussia": true, "dynamo": true, "dinamo": true, "olympique": true, "olympic": true, "hapoel": true, "maccabi": true, "red": true, "bull": true}

var calendarTeamAliases = map[string]string{
	"man-utd": "manchester-united", "manchester-utd": "manchester-united", "man-united": "manchester-united", "man-city": "manchester-city",
	"psg": "paris-saint-germain", "paris-sg": "paris-saint-germain", "bayern-munich": "bayern-munchen",
	"inter-mediolan": "inter", "inter-milan": "inter", "internazionale": "inter", "sporting-lizbona": "sporting-lisboa", "sporting-lisbon": "sporting-lisboa", "sporting-cp": "sporting-lisboa",
	"juventus-turyn": "juventus", "ajax-amsterdam": "ajax", "benfica-lizbona": "benfica", "arsenal-londyn": "arsenal", "chelsea-londyn": "chelsea", "tottenham-londyn": "tottenham", "tottenham-hotspur": "tottenham", "west-ham-united": "west-ham", "bvb": "borussia-dortmund", "borussia-m-gladbach": "borussia-monchengladbach", "borussia-moenchengladbach": "borussia-monchengladbach",
}

func calendarTeamIdentity(name string) (string, string) {
	core, gender := sofaTeamNameAndGender(calendarTeamSlug(name))
	words, qualifiers := []string{}, []string{}
	if gender == "F" {
		qualifiers = append(qualifiers, "women")
	}
	parts := strings.Split(core, "-")
	for i, word := range parts {
		switch {
		case sofaAgePattern.MatchString(word):
			qualifiers = append(qualifiers, word)
		case word == "ii" || word == "res" || word == "reserve" || word == "reserves" || word == "rezerwy" || word == "castilla" || word == "primavera" || (word == "b" && i == len(parts)-1):
			qualifiers = append(qualifiers, "reserves")
		case word == "iii" || word == "iv":
			qualifiers = append(qualifiers, word)
		case word != "" && !calendarClubWords[word]:
			words = append(words, word)
		}
	}
	core = strings.Join(words, "-")
	if strings.HasPrefix(core, "red-bull-") {
		qualifiers = append(qualifiers, "red-bull")
	}
	if alias, ok := calendarTeamAliases[core]; ok {
		core = alias
	}
	if alias, ok := teamSlugAliases[core]; ok {
		core = canonicalTeamCore(alias)
	}
	sort.Strings(qualifiers)
	return core, strings.Join(qualifiers, "|")
}

func sameCalendarTeam(left, right string) bool {
	a, aq := calendarTeamIdentity(left)
	b, bq := calendarTeamIdentity(right)
	if a == "" || b == "" || aq != bq {
		return false
	}
	if a == b {
		return true
	}
	shorter, longer := strings.Split(a, "-"), strings.Split(b, "-")
	if len(shorter) > len(longer) {
		shorter, longer = longer, shorter
	}
	if len(longer)-len(shorter) > 3 {
		return false
	}
	anchor := false
	for _, word := range shorter {
		found := false
		for _, other := range longer {
			found = found || word == other
		}
		if !found {
			return false
		}
		anchor = anchor || (len(word) >= 5 && !calendarGenericWords[word])
	}
	return anchor
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
	FixtureID      string          `json:"fixtureId,omitempty"`
	StartTime      time.Time       `json:"startTime,omitempty"`
	ResultRevision int             `json:"resultRevision"`
	ID             string          `json:"id"`
	Match          string          `json:"match"`
	Status         string          `json:"status"`
	Stage          string          `json:"stage"`
	Message        string          `json:"message"`
	PlayersDone    int             `json:"playersDone"`
	PlayersTotal   int             `json:"playersTotal"`
	CreatedAt      time.Time       `json:"createdAt"`
	StartedAt      time.Time       `json:"startedAt,omitempty"`
	FinishedAt     time.Time       `json:"finishedAt,omitempty"`
	Request        AnalysisRequest `json:"request"`
	Warnings       []string        `json:"warnings"`
	Result         []AnalyzedBet   `json:"result,omitempty"`
	Error          string          `json:"error,omitempty"`
}

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

func registerHistoryAliases(target map[string]string, history *PlayerStatHistory) {
	id := strconv.Itoa(history.ID)
	norm := NormalizePlayerName(history.FullName)
	if norm == "" {
		return
	}
	put := func(alias string) {
		if previous, exists := target[alias]; exists && previous != id {
			target[alias] = ""
			return
		}
		target[alias] = id
	}
	put(norm)
	for _, alias := range history.PlayerAliases {
		if normalized := NormalizePlayerName(alias); normalized != "" {
			put(normalized)
		}
	}
	parts := strings.Fields(norm)
	if len(parts) >= 2 {
		first, last := parts[0], parts[len(parts)-1]
		put(last + " " + strings.Join(parts[:len(parts)-1], " "))
		if runes := []rune(first); len(runes) > 0 {
			put(string(runes[0]) + " " + last)
			put(last + " " + string(runes[0]))
		}
	}
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

func dataHash(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:])
}

var serverDataDir = "data"
var cacheMutex sync.Mutex

type analysisMatchContext struct {
	Pending bool
	Input   fairModelInput
}
type sofaClientState struct {
	mu          sync.Mutex
	footyTeams  map[string]*footyTeamFlight
	lineupRoles map[int]string
	teamRefresh map[string]sofaTeamRefreshState
	sofaBlock   error
}
type ResilientClient struct {
	ctx                                                                           context.Context
	tlsClient                                                                     tls_client.HttpClient
	state                                                                         *sofaClientState
	warning                                                                       func(string)
	statsFixture                                                                  statsFixture
	statsHomeName, statsAwayName, statsQueryHomeID, statsQueryAwayID, statsTeamID string
	statsProps                                                                    []NormalizedProp
}

func newSofaClient(ctx context.Context, warning func(string)) (*ResilientClient, error) {
	cli, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), tls_client.WithTimeoutSeconds(20), tls_client.WithClientProfile(profiles.Chrome_152), tls_client.WithCookieJar(tls_client.NewCookieJar()), tls_client.WithRandomTLSExtensionOrder())
	if err != nil {
		return nil, err
	}
	return &ResilientClient{ctx: ctx, tlsClient: cli, warning: warning, state: &sofaClientState{lineupRoles: map[int]string{}, teamRefresh: map[string]sofaTeamRefreshState{}}}, nil
}
func (client *ResilientClient) withScope(_ string) *ResilientClient { cp := *client; return &cp }
func (client *ResilientClient) warn(message string) {
	if client.warning != nil {
		client.warning(message)
	}
}

func (client *ResilientClient) GetSofa(endpoint string) ([]byte, error) {
	if err := client.ctx.Err(); err != nil {
		return nil, err
	}
	client.state.mu.Lock()
	blocked := client.state.sofaBlock
	client.state.mu.Unlock()
	if blocked != nil {
		return nil, blocked
	}
	clientID, _ := client.ctx.Value(sofaRelayContextKey{}).(string)
	result, err := sofaRelays.request(client.ctx, clientID, endpoint)
	if err != nil {
		return nil, err
	}
	if result.Error != "" {
		return nil, fmt.Errorf("SofaScore na komputerze klienta: %s", result.Error)
	}
	if result.Status != http.StatusOK {
		failure := fmt.Errorf("SofaScore na komputerze klienta HTTP %d", result.Status)
		if result.Status == http.StatusForbidden || result.Status == http.StatusTooManyRequests {
			client.state.mu.Lock()
			client.state.sofaBlock = failure
			client.state.mu.Unlock()
		}
		return nil, failure
	}
	if len(result.Body) > 32<<20 || !json.Valid(result.Body) {
		return nil, fmt.Errorf("niepoprawna odpowiedź SofaScore z klienta")
	}
	return result.Body, nil
}

type sourceResponse struct {
	Body  []byte
	Until time.Time
}

var sourceCache = struct {
	sync.Mutex
	values map[string]sourceResponse
	bytes  int
}{values: map[string]sourceResponse{}}
var sourceLocks = func() [256]chan struct{} {
	var locks [256]chan struct{}
	for i := range locks {
		locks[i] = make(chan struct{}, 1)
	}
	return locks
}()

func sourceJSON(ctx context.Context, key string, ttl time.Duration, load func() ([]byte, error), validate func([]byte) error) ([]byte, bool, error) {
	digest := sha256.Sum256([]byte(key))
	lock := sourceLocks[digest[0]]
	select {
	case lock <- struct{}{}:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	defer func() { <-lock }()
	sourceCache.Lock()
	cached, ok := sourceCache.values[key]
	sourceCache.Unlock()
	if ok && time.Now().Before(cached.Until) {
		if validate != nil {
			if err := validate(cached.Body); err != nil {
				return nil, true, err
			}
		}
		return cached.Body, true, nil
	}
	body, err := load()
	if err != nil {
		return nil, false, err
	}
	if !json.Valid(body) {
		return nil, false, fmt.Errorf("odpowiedź źródła nie jest JSON")
	}
	if validate != nil {
		if err = validate(body); err != nil {
			return nil, false, err
		}
	}
	if len(body) > 8<<20 {
		return body, false, nil
	}
	sourceCache.Lock()
	if len(sourceCache.values) >= 1024 || sourceCache.bytes+len(body) > 64<<20 {
		for k, v := range sourceCache.values {
			if time.Now().After(v.Until) {
				sourceCache.bytes -= len(v.Body)
				delete(sourceCache.values, k)
			}
		}
		if len(sourceCache.values) >= 1024 || sourceCache.bytes+len(body) > 64<<20 {
			sourceCache.values = map[string]sourceResponse{}
			sourceCache.bytes = 0
		}
	}
	if previous, ok := sourceCache.values[key]; ok {
		sourceCache.bytes -= len(previous.Body)
	}
	sourceCache.values[key] = sourceResponse{Body: body, Until: time.Now().Add(ttl)}
	sourceCache.bytes += len(body)
	sourceCache.Unlock()
	return body, false, nil
}
func getSofaHistoryData(client *ResilientClient, endpoint, scope string, until time.Time, validate func([]byte) error) ([]byte, bool, error) {
	ttl := 15 * time.Minute
	if !until.IsZero() {
		ttl = min(ttl, time.Until(until))
	}
	return sourceJSON(client.ctx, "sofa/"+scope+endpoint, ttl, func() ([]byte, error) { return client.GetSofa(endpoint) }, validate)
}

var footySlot = make(chan struct{}, 1)

func getFootyJSON(client *ResilientClient, endpoint string, target any) error {
	if !strings.HasPrefix(endpoint, "/api/front/") || strings.Contains(endpoint, "#") {
		return fmt.Errorf("FootyMetrics: niepoprawny endpoint")
	}
	ttl := 5 * time.Minute
	if strings.HasSuffix(endpoint, "/lineup") {
		ttl = time.Minute
	}
	body, _, err := sourceJSON(client.ctx, "footy/"+endpoint, ttl, func() ([]byte, error) {
		select {
		case footySlot <- struct{}{}:
		case <-client.ctx.Done():
			return nil, client.ctx.Err()
		}
		defer func() { <-footySlot }()
		req, err := fhttp.NewRequestWithContext(client.ctx, http.MethodGet, "https://www.footymetrics.com"+endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36")
		req.Header.Set("Referer", "https://www.footymetrics.com/")
		if cookie := strings.TrimSpace(os.Getenv("VALUE_BETS_FOOTY_COOKIE")); cookie != "" {
			req.Header.Set("Cookie", cookie)
		}
		resp, err := client.tlsClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("FootyMetrics HTTP %d", resp.StatusCode)
		}
		return readBounded(resp.Body, 32<<20)
	}, nil)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, target)
}
func getSofaTeamRefresh(client *ResilientClient, teamID string) (sofaTeamRefreshState, error) {
	client.state.mu.Lock()
	cached, ok := client.state.teamRefresh[teamID]
	client.state.mu.Unlock()
	if ok {
		return cached, nil
	}
	var all SofaTeamEventsResponse
	for page := 0; page < 3; page++ {
		endpoint := fmt.Sprintf("/team/%s/events/last/%d", teamID, page)
		raw, _, err := getSofaHistoryData(client, endpoint, "team-events", time.Time{}, nil)
		if err != nil {
			return sofaTeamRefreshState{}, err
		}
		var response SofaTeamEventsResponse
		if err = json.Unmarshal(raw, &response); err != nil {
			return sofaTeamRefreshState{}, err
		}
		all.Events = append(all.Events, response.Events...)
		all.HasNextPage = response.HasNextPage
		if !response.HasNextPage || len(response.Events) == 0 {
			break
		}
	}
	raw, _ := json.Marshal(all)
	state := sofaTeamRefreshState{TeamID: teamID, Events: raw, FetchedAt: time.Now().UTC()}
	client.state.mu.Lock()
	client.state.teamRefresh[teamID] = state
	client.state.mu.Unlock()
	return state, nil
}

func subsetMetricHistory(hist *PlayerStatHistory, market CanonicalMarket, indexes []int) *PlayerStatHistory {
	result := *hist
	result.Matches = len(indexes)
	result.MarketHistories = nil
	result.MetricValues = map[CanonicalMarket][]int{market: sampleIndexes(hist.MetricValues[market], indexes)}
	result.MetricValuesWithSuperSub = map[CanonicalMarket][]int{market: sampleIndexes(hist.MetricValuesWithSuperSub[market], indexes)}
	result.Minutes = sampleIndexes(hist.Minutes, indexes)
	result.Positions = sampleIndexes(hist.Positions, indexes)
	result.Opponents = sampleIndexes(hist.Opponents, indexes)
	result.Dates = sampleIndexes(hist.Dates, indexes)
	result.Ratings = sampleIndexes(hist.Ratings, indexes)
	result.MatchContexts = sampleIndexes(hist.MatchContexts, indexes)
	result.EventIDs = sampleIndexes(hist.EventIDs, indexes)
	result.SourceEventIDs = sampleIndexes(hist.SourceEventIDs, indexes)
	result.MatchStartedAt = sampleIndexes(hist.MatchStartedAt, indexes)
	result.MatchTeamNames = sampleIndexes(hist.MatchTeamNames, indexes)
	result.MatchShotCounts = sampleIndexes(hist.MatchShotCounts, indexes)
	result.SubPlayerIDs = sampleIndexes(hist.SubPlayerIDs, indexes)
	result.SubShotCounts = sampleIndexes(hist.SubShotCounts, indexes)
	result.SuperSubAvailability = nil
	if mask := hist.SuperSubAvailability[market]; mask != nil {
		result.SuperSubAvailability = map[CanonicalMarket][]bool{market: sampleIndexes(mask, indexes)}
	}
	result.MatchShots = sampleIndexes(hist.MatchShots, indexes)
	return &result
}

func latestMetricHistory(hist *PlayerStatHistory, market CanonicalMarket, limit int) *PlayerStatHistory {
	metric := metricHistory(hist, market)
	if limit <= 0 || limit >= metric.Matches {
		return metric
	}
	indexes := make([]int, limit)
	for i := range indexes {
		indexes[i] = i
	}
	result := subsetMetricHistory(metric, market, indexes)
	result.HistoryNotes = append(append([]string(nil), metric.HistoryNotes...), fmt.Sprintf("Próbka widoku: ostatnie %d z %d pobranych występów tego rynku; odcięto starsze mecze.", limit, metric.Matches))
	return result
}

func latestPlayerHistory(hist *PlayerStatHistory, limit int) *PlayerStatHistory {
	result := *hist
	result.MarketHistories = map[CanonicalMarket]*PlayerStatHistory{}
	result.Matches = 0
	for market := range hist.MarketHistories {
		metric := latestMetricHistory(hist, market, limit)
		result.MarketHistories[market] = metric
		result.Matches = max(result.Matches, metric.Matches)
	}
	return &result
}
func fetchPlayerHistory(client *ResilientClient, player SofaPlayerDetails, teamName string, limit int, tournaments string, markets []CanonicalMarket) (*PlayerStatHistory, error) {
	bundle, err := getFootyTeam(client, client.statsTeamID)
	if err != nil {
		return nil, err
	}
	id := resolveFootyPlayer(bundle.Table.Players, bundle.TeamID, player)
	if id <= 0 {
		return nil, fmt.Errorf("FootyMetrics: niejednoznaczne lub brak dopasowania zawodnika %s", player.Name)
	}
	if len(markets) == 0 {
		markets = requiredPlayerMarkets(client, player)
	}
	// A club table cannot establish the latest all-competition appearances of
	// a player: international games and previous clubs must come from his feed.
	// Do not silently substitute a different sample if that feed is unavailable.
	if bundle.NoPlayerFetch {
		return nil, fmt.Errorf("FootyMetrics: nie można potwierdzić pełnej historii zawodnika %s po blokadzie źródła (403/429)", player.Name)
	}
	var table footyTable
	query := url.Values{"stat": {"shots"}, "id": {strconv.Itoa(id)}, "tid": {strconv.Itoa(bundle.TeamID)}, "period": {"30"}, "started": {"false"}, "location": {"both"}, "scope": {"all"}, "l": {""}, "p": {""}, "dl": {"true"}, "se": {"false"}, "sm": {"easier"}}
	if err = getFootyJSON(client, "/api/front/players/table?"+query.Encode(), &table); err != nil {
		return nil, fmt.Errorf("FootyMetrics: historia zawodnika %s: %w", player.Name, err)
	}
	if len(table.PivotData[strconv.Itoa(id)]) == 0 {
		return nil, fmt.Errorf("FootyMetrics: brak rekordów historii zawodnika %s", player.Name)
	}
	normalizeFootyPlayerCounters(&table)
	historyTable := footyTable{}
	if err = mergeFootyTable(&historyTable, bundle.Table, "player"); err != nil {
		return nil, err
	}
	if err = mergeFootyTable(&historyTable, table, "player"); err != nil {
		return nil, err
	}
	// The player response owns the sample boundary. Club rows may fill counts
	// for those fixtures, but cannot append older matches to replace zero rows.
	historyTable.Fixtures = append([]footyFixture(nil), table.Fixtures...)
	full := buildFootyHistory(client, bundle, historyTable, player, id, teamName, tournaments)
	if full.Matches == 0 {
		return nil, fmt.Errorf("FootyMetrics: brak potwierdzonych występów %s w wybranym zakresie", player.Name)
	}
	full.footyData = &historyTable
	full.PlayerAliases = []string{player.Name, player.ShortName, strings.ReplaceAll(player.Slug, "-", " ")}
	enrichFootyReplacements(client, full, historyTable, markets, limit)
	full.HistoryNotes = append(full.HistoryNotes, "SofaScore jest pobierany dopiero po otwarciu wykresu. Korekta profilu EV = 1.")
	base := *full
	base.MarketHistories = map[CanonicalMarket]*PlayerStatHistory{}
	for _, market := range markets {
		history := metricHistory(full, market)
		history.HistoryNotes = playerMetricHistoryNotes(full, market)
		indexes := make([]int, min(limit, history.Matches))
		for i := range indexes {
			indexes[i] = i
		}
		history = subsetMetricHistory(history, market, indexes)
		if history.Matches < limit {
			history.Incomplete = true
			base.Incomplete = true
			history.HistoryNotes = append(history.HistoryNotes, fmt.Sprintf("FootyMetrics: dostępne %d/%d występów tego rynku; braków nie zastąpiono SofaScore ani zerem.", history.Matches, limit))
		}
		base.MarketHistories[market] = history
	}
	return &base, nil
}
func enrichSofaHistory(client *ResilientClient, hist *PlayerStatHistory, playerID int, markets []CanonicalMarket, limit int) {
	needed := selectedHistoryIndexes(hist, markets, limit)
	if note := resolveSofaHistoryEvents(client, hist, needed, true); note != "" {
		hist.HistoryNotes = append(hist.HistoryNotes, note)
	}
	for i, eventID := range hist.EventIDs {
		if eventID <= 0 || !needed[i] || hist.Ratings[i] != nil {
			continue
		}
		endpoint := fmt.Sprintf("/event/%d/player/%d/statistics", eventID, playerID)
		raw, _, err := getSofaHistoryData(client, endpoint, "ratings", time.Time{}, nil)
		if err != nil {
			hist.HistoryNotes = append(hist.HistoryNotes, fmt.Sprintf("Brak ratingu SofaScore dla meczu %d: %v", eventID, err))
			continue
		}
		var response SofaPlayerMatchStatResponse
		if json.Unmarshal(raw, &response) == nil && response.Statistics.Rating != nil && *response.Statistics.Rating > 0 && *response.Statistics.Rating <= 10 {
			hist.Ratings[i] = response.Statistics.Rating
		}
	}
}

func selectedHistoryIndexes(hist *PlayerStatHistory, markets []CanonicalMarket, limit int) map[int]bool {
	needed := map[int]bool{}
	for _, market := range markets {
		count := 0
		for index, present := range hist.MetricAvailability[market] {
			if present && count < limit {
				needed[index] = true
				count++
			}
		}
		// A market sample is already filtered and no longer has a sparse mask.
		if hist.MetricAvailability == nil {
			for i := 0; i < min(limit, len(hist.MetricValues[market])); i++ {
				needed[i] = true
			}
		}
	}
	return needed
}

func enrichFootyReplacements(client *ResilientClient, hist *PlayerStatHistory, table footyTable, markets []CanonicalMarket, limit int) {
	// /players/table includes the replacement rows. Resolve known FootyMetrics
	// substitutions without any SofaScore requests or counter fallback.
	loaded := map[string]footyTable{}
	for i := range selectedHistoryIndexes(hist, markets, limit) {
		fixtureID := strconv.Itoa(hist.SourceEventIDs[i])
		record := table.PivotData[strconv.Itoa(hist.SourcePlayerID)][fixtureID]
		subID, known := footyReferenceID(record, "subApid")
		if !known || subID <= 0 {
			continue
		}
		ownID := hist.SourceTeamID
		if team, ok := footyReferenceID(record, "team"); ok && team > 0 {
			ownID = team
		}
		subRecord := table.PivotData[strconv.Itoa(subID)][fixtureID]
		missing := false
		for _, market := range markets {
			_, present := footyMetric(subRecord, market)
			missing = missing || (hist.MetricAvailability[market][i] && !present)
		}
		if missing {
			key := fmt.Sprintf("%d/%d", ownID, subID)
			subTable, exists := loaded[key]
			if !exists {
				query := url.Values{"stat": {"shots"}, "id": {strconv.Itoa(subID)}, "tid": {strconv.Itoa(ownID)}, "period": {"30"}, "started": {"false"}, "location": {"both"}, "scope": {"all"}, "l": {""}, "p": {""}, "dl": {"true"}, "se": {"false"}, "sm": {"easier"}}
				if err := getFootyJSON(client, "/api/front/players/table?"+query.Encode(), &subTable); err != nil {
					log.Printf("[SUPERSUB] footy_player_id=%d: %v", subID, err)
				} else {
					normalizeFootyPlayerCounters(&subTable)
				}
				loaded[key] = subTable
			}
			if row := subTable.PivotData[strconv.Itoa(subID)][fixtureID]; row != nil {
				subRecord = row
			}
		}
		minutes, played := footyCount(subRecord, "mins")
		if !played || minutes <= 0 || minutes > 150 {
			continue
		}
		if team, ok := footyReferenceID(subRecord, "team"); ok && team > 0 && team != ownID {
			continue
		}
		if shots, ok := footyMetric(subRecord, MarketShotsTotal); ok {
			hist.SubShotCounts[i] = &shots
		}
		for _, market := range markets {
			if value, ok := footyMetric(subRecord, market); ok && hist.MetricAvailability[market][i] {
				hist.MetricValuesWithSuperSub[market][i] = hist.MetricValues[market][i] + value
				hist.SuperSubAvailability[market][i] = true
			}
		}
	}
}

func enrichFootySuperSubs(client *ResilientClient, hist *PlayerStatHistory, table footyTable, markets []CanonicalMarket, limit int, allowFetch bool) {
	needed := selectedHistoryIndexes(hist, markets, limit)
	loaded := map[string]footyTable{}
	for i, eventID := range hist.EventIDs {
		if !needed[i] || eventID <= 0 {
			continue
		}
		fixtureID := strconv.Itoa(hist.SourceEventIDs[i])
		record := table.PivotData[strconv.Itoa(hist.SourcePlayerID)][fixtureID]
		sourceSubID, _ := footyReferenceID(record, "subApid")
		if hist.Minutes[i] >= 90 && sourceSubID == 0 {
			continue
		}
		var incidents SofaSubstitutionsResponse
		raw, _, err := getSofaHistoryData(client, fmt.Sprintf("/event/%d/incidents", eventID), "substitutions", time.Time{}, func(raw []byte) error {
			if err := json.Unmarshal(raw, &incidents); err != nil {
				return err
			}
			if incidents.Incidents == nil {
				return fmt.Errorf("brak listy zmian")
			}
			return nil
		})
		if err != nil || json.Unmarshal(raw, &incidents) != nil {
			log.Printf("[SUPERSUB] player_id=%d event_id=%d: brak potwierdzenia zmian: %v", hist.SofaPlayerID, eventID, err)
			continue
		}
		incoming := map[int]SofaPlayerDetails{}
		hasSubstitutions := false
		for _, incident := range incidents.Incidents {
			if incident.IncidentType == "substitution" {
				hasSubstitutions = true
			}
			if incident.IncidentType == "substitution" && incident.PlayerOut.ID == hist.SofaPlayerID && incident.PlayerIn.ID > 0 {
				incoming[incident.PlayerIn.ID] = incident.PlayerIn
			}
		}
		if len(incoming) > 1 {
			continue
		}
		// No substitution in a valid incident list confirms the player's own
		// totals, even for a short appearance (e.g. a dismissal).
		if len(incoming) == 0 {
			if sourceSubID > 0 {
				continue
			} // Conflicting providers stay unknown.
			if !hasSubstitutions {
				continue
			} // An empty/limited feed does not confirm zero.
			zero := 0
			hist.SubPlayerIDs[i] = 0
			hist.SubShotCounts[i] = &zero
			for _, market := range markets {
				hist.MetricValuesWithSuperSub[market][i] = hist.MetricValues[market][i]
				hist.SuperSubAvailability[market][i] = historyMetricKnown(hist, market, i)
			}
			continue
		}
		ownFootyID := hist.SourceTeamID
		if id, known := footyReferenceID(record, "team"); known && id > 0 {
			ownFootyID = id
		}
		conflicting := false
		for _, player := range incoming {
			hist.SubPlayerIDs[i] = player.ID
			resolved := resolveFootyPlayer(table.Players, ownFootyID, player)
			if sourceSubID > 0 && resolved > 0 && sourceSubID != resolved {
				log.Printf("[SUPERSUB] player_id=%d event_id=%d: sprzeczne identyfikatory zmiennika FootyMetrics", hist.SofaPlayerID, eventID)
				conflicting = true
				continue
			}
			if sourceSubID == 0 {
				sourceSubID = resolved
			}
		}
		if conflicting {
			for _, market := range markets {
				hist.SuperSubAvailability[market][i] = false
			}
			continue
		}
		if sourceSubID <= 0 {
			continue
		}
		subRecord := table.PivotData[strconv.Itoa(sourceSubID)][fixtureID]
		missing := false
		for _, market := range markets {
			_, known := footyMetric(subRecord, market)
			missing = missing || (historyMetricKnown(hist, market, i) && !known)
		}
		if missing && allowFetch {
			key := fmt.Sprintf("%d/%d", ownFootyID, sourceSubID)
			subTable, exists := loaded[key]
			if !exists {
				query := url.Values{"stat": {"shots"}, "id": {strconv.Itoa(sourceSubID)}, "tid": {strconv.Itoa(ownFootyID)}, "period": {"30"}, "started": {"false"}, "location": {"both"}, "scope": {"all"}, "l": {""}, "p": {""}, "dl": {"true"}, "se": {"false"}, "sm": {"easier"}}
				err = getFootyJSON(client, "/api/front/players/table?"+query.Encode(), &subTable)
				if err == nil {
					normalizeFootyPlayerCounters(&subTable)
				}
				loaded[key] = subTable
				if err != nil {
					log.Printf("[SUPERSUB] footy_player_id=%d: %v", sourceSubID, err)
					if strings.Contains(err.Error(), "HTTP 403") || strings.Contains(err.Error(), "HTTP 429") {
						allowFetch = false
					}
				}
			}
			if replacement := subTable.PivotData[strconv.Itoa(sourceSubID)][fixtureID]; replacement != nil {
				subRecord = replacement
			}
		}
		minutes, played := footyCount(subRecord, "mins")
		if !played || minutes <= 0 || minutes > 150 {
			continue
		}
		if id, known := footyReferenceID(subRecord, "team"); known && id > 0 && id != ownFootyID {
			continue
		}
		if shots, known := footyMetric(subRecord, MarketShotsTotal); known {
			hist.SubShotCounts[i] = &shots
		}
		for _, market := range markets {
			value, known := footyMetric(subRecord, market)
			if known && historyMetricKnown(hist, market, i) {
				hist.MetricValuesWithSuperSub[market][i] = hist.MetricValues[market][i] + value
				hist.SuperSubAvailability[market][i] = true
			}
		}
	}
}

func historyMetricKnown(hist *PlayerStatHistory, market CanonicalMarket, i int) bool {
	if i < 0 || i >= len(hist.MetricValues[market]) {
		return false
	}
	mask, exists := hist.MetricAvailability[market]
	return !exists || (i < len(mask) && mask[i])
}

func (task *analysisTask) enrichChartIdentity(client *ResilientClient, hist *PlayerStatHistory) error {
	if hist.SofaPlayerID > 0 {
		return nil
	}
	if task.sofaTeams.HomeID == "" || task.sofaTeams.AwayID == "" {
		gender := "M"
		if isWomenEvent(task.seed) {
			gender = "F"
		}
		detected := detectTeamsBySlugs(client, calendarTeamSlug(task.seed.Home), calendarTeamSlug(task.seed.Away), sofaMatchContext{StartTime: task.fixture.StartTime, Gender: gender})
		if detected.HomeID == "" || detected.AwayID == "" {
			return fmt.Errorf("SofaScore: nie potwierdzono drużyn dla wykresu: %s", strings.Join(detected.Warnings, "; "))
		}
		task.sofaTeams = detected
	}
	teamID := task.sofaTeams.HomeID
	if hist.TeamSide == "AWAY" {
		teamID = task.sofaTeams.AwayID
	}
	if task.sofaSquads == nil {
		task.sofaSquads = map[string][]SofaPlayerDetails{}
	}
	squad := task.sofaSquads[teamID]
	if len(squad) == 0 {
		// Only the current roster is required. Do not download several team
		// event pages and historical lineups for every chart.
		var response SofaTeamPlayersResponse
		raw, _, err := getSofaHistoryData(client, "/team/"+teamID+"/players", "chart-squad", time.Time{}, nil)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(raw, &response); err != nil {
			return err
		}
		for _, item := range response.Players {
			if item.Player.ID > 0 {
				squad = append(squad, item.Player)
			}
		}
		if len(squad) == 0 {
			return fmt.Errorf("SofaScore: brak kadry dla wybranego wykresu")
		}
		task.sofaSquads[teamID] = squad
	}
	// Verify the player against a unique roster identity. Never transfer the
	// FootyMetrics ID into a SofaScore endpoint, even if the numbers coincide.
	selected := selectSofaMarketPlayers(squad, append([]string{hist.FullName}, hist.PlayerAliases...))
	if len(selected) != 1 {
		return fmt.Errorf("SofaScore: brak jednoznacznego dopasowania zawodnika %s w potwierdzonej drużynie", hist.FullName)
	}
	hist.SofaPlayerID = selected[0].ID
	log.Printf("[SOFA-CHART] footy_player_id=%d sofa_player_id=%d name=%q", hist.SourcePlayerID, hist.SofaPlayerID, hist.FullName)
	return nil
}

func fetchAnalysisMatchContext(client *ResilientClient, homeID, awayID string, players []*PlayerStatHistory) analysisMatchContext {
	input := fairModelInput{Roles: map[int]string{}, Profiles: map[int]teamMatchProfile{}}
	client.state.mu.Lock()
	for id, role := range client.state.lineupRoles {
		input.Roles[id] = role
	}
	client.state.mu.Unlock()
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
	raw, err := client.GetSofa("/event/" + client.statsFixture.ID)
	if err != nil || json.Unmarshal(raw, &event) != nil {
		client.warn("Brak kontekstu meczu dla korekty EV")
		return analysisMatchContext{Input: input}
	}
	input.HomeID, input.AwayID = event.Event.HomeTeam.ID, event.Event.AwayTeam.ID
	input.LeagueID, input.SeasonID = event.Event.Tournament.UniqueTournament.ID, event.Event.Season.ID
	if !((strconv.Itoa(input.HomeID) == homeID && strconv.Itoa(input.AwayID) == awayID) || (strconv.Itoa(input.HomeID) == awayID && strconv.Itoa(input.AwayID) == homeID)) {
		client.warn("Drużyny kontekstu nie zgadzają się z analizą; korekta profilu wyłączona")
		input.HomeID, input.AwayID = 0, 0
		return analysisMatchContext{Input: input}
	}
	if input.LeagueID <= 0 || input.SeasonID <= 0 {
		return analysisMatchContext{Input: input}
	}
	ids := map[int]bool{input.HomeID: true, input.AwayID: true}
	for _, hist := range players {
		for _, match := range historyMatchContexts(hist) {
			if match.LeagueID == input.LeagueID && match.SeasonID == input.SeasonID {
				if match.TeamID > 0 {
					ids[match.TeamID] = true
				}
				if match.OpponentID > 0 {
					ids[match.OpponentID] = true
				}
			}
		}
	}
	ordered := []int{}
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Ints(ordered)
	for _, id := range ordered {
		endpoint := fmt.Sprintf("/team/%d/unique-tournament/%d/season/%d/statistics/overall", id, input.LeagueID, input.SeasonID)
		raw, _, err := getSofaHistoryData(client, endpoint, "profiles", time.Time{}, nil)
		if err != nil {
			client.warn(fmt.Sprintf("Profil drużyny %d: %v", id, err))
			continue
		}
		var profile teamMatchProfile
		if json.Unmarshal(raw, &profile) == nil {
			profile.FetchedAt = time.Now().UTC()
			input.Profiles[id] = profile
		}
	}
	return analysisMatchContext{Input: input}
}

type analysisTask struct {
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	job         AnalysisJob
	histories   map[int]*PlayerStatHistory
	fixture     statsFixture
	calculation *analysisCalculation
	seed        CalendarEvent
	chartGate   chan struct{}
	// These fields are accessed only while holding chartGate. Initial results
	// and histories stay immutable when charts receive optional enrichment.
	chartHistories map[string]*PlayerStatHistory
	sofaTeams      TeamDetectionResult
	sofaSquads     map[string][]SofaPlayerDetails
}

// Immutable inputs retained for changing sample length without provider calls.
type analysisCalculation struct {
	Props                  []NormalizedProp
	Home, Away             []*PlayerStatHistory
	HomeLineup, AwayLineup map[string]bool
	Confirmed              bool
	Context                analysisMatchContext
}
type calendarServer struct {
	ctx                 context.Context
	cache               *CalendarCache
	mu                  sync.Mutex
	jobs                map[string]*analysisTask
	slots               chan struct{}
	calendarServices    sync.Once
	calendarMu          sync.Mutex
	calendarDates       map[string]bool
	marketOnce          sync.Once
	marketMu            sync.Mutex
	marketPersistMu     sync.Mutex
	marketStates        map[string]MatchMarketAvailability
	marketFlights       map[string]*marketAvailabilityFlight
	qualificationMu     sync.Mutex
	qualificationQueued map[string]bool
	qualificationQueue  chan map[string]CalendarEvent
}

func (s *calendarServer) collect(ctx context.Context, book, date string, force bool) (*Calendar, error) {
	if _, _, err := calendarDay(date); err != nil {
		return nil, err
	}
	switch book {
	case "STS":
		return FetchCachedSTSCalendar(ctx, s.cache, STSWebSocket, date, force)
	case "BETCLIC":
		return FetchCachedBetclicCalendar(ctx, s.cache, &http.Client{Timeout: 30 * time.Second}, BetclicMatchService+"GetMatchesBySportWithNotifications", date, force)
	case "SUPERBET":
		return s.cache.Get(ctx, "SUPERBET:"+date, force, func(ctx context.Context) (*Calendar, error) {
			return loadSuperbetCalendarContext(ctx, &http.Client{Timeout: 30 * time.Second}, date, "https://production-superbet-offer-pl.freetls.fastly.net", "https://production-superbet-bmb.freetls.fastly.net")
		})
	}
	return nil, fmt.Errorf("nieobsługiwany bukmacher")
}
func (s *calendarServer) statsFixtureForMatch(client *ResilientClient, homeID, awayID string, expected time.Time) (statsFixture, bool, error) {
	matches := map[int]statsFixture{}
	var failure error
	for _, teamID := range []string{homeID, awayID} {
		raw, err := client.GetSofa("/team/" + teamID + "/events/next/0")
		if err != nil {
			failure = err
			continue
		}
		var response SofaTeamEventsResponse
		if err = json.Unmarshal(raw, &response); err != nil {
			failure = err
			continue
		}
		for _, event := range response.Events {
			h, a := strconv.Itoa(event.HomeTeam.ID), strconv.Itoa(event.AwayTeam.ID)
			if !((h == homeID && a == awayID) || (h == awayID && a == homeID)) || event.ID <= 0 || event.StartTS <= 0 || event.Status.Code >= 100 || event.Status.Type == "canceled" || event.Status.Type == "postponed" {
				continue
			}
			start := time.Unix(event.StartTS, 0).UTC()
			if start.Before(time.Now()) {
				continue
			}
			if !expected.IsZero() && start.Sub(expected).Abs() > 10*time.Minute {
				continue
			}
			matches[event.ID] = statsFixture{ID: strconv.Itoa(event.ID), HomeID: h, AwayID: a, StartTime: start, ValidUntil: start}
		}
	}
	if len(matches) != 1 {
		return statsFixture{}, false, fmt.Errorf("SofaScore: wymagany jeden zgodny przyszły mecz, znaleziono %d (błąd źródła: %v)", len(matches), failure)
	}
	for _, fixture := range matches {
		return fixture, fixture.HomeID != homeID, nil
	}
	return statsFixture{}, false, fmt.Errorf("brak meczu")
}

func (s *calendarServer) analyze(task *analysisTask) ([]AnalyzedBet, error) {
	query := task.job.Request
	client, err := newSofaClient(task.ctx, task.warn)
	if err != nil {
		return nil, err
	}
	defer client.tlsClient.CloseIdleConnections()
	seed, err := s.verifyBookmakerFixtures(task.ctx, query)
	if err != nil {
		return nil, err
	}
	task.report("offers", "Pobieranie ofert bukmacherów", 0, 0)
	var props []NormalizedProp
	for _, source := range []struct{ Book, URL string }{{"SUPERBET", query.SuperbetURL}, {"BETCLIC", query.BetclicURL}, {"STS", query.STSURL}} {
		if source.URL == "" {
			continue
		}
		id, _, _ := extractIDAndSlug(source.URL)
		rows, readErr := fetchMatchProps(task.ctx, source.Book, id)
		if readErr != nil {
			return nil, fmt.Errorf("%s: nie potwierdzono aktualnych rynków zawodników: %w", source.Book, readErr)
		}
		if len(rows) == 0 {
			return nil, fmt.Errorf("%s: brak obsługiwanych rynków zawodników; mecz nie kwalifikuje się do analizy trzech bukmacherów", source.Book)
		}
		props = append(props, rows...)
	}
	if len(props) == 0 {
		return nil, fmt.Errorf("brak obsługiwanych rynków zawodników w ofertach")
	}
	start, err := time.Parse(time.RFC3339, seed.StartTime)
	if err != nil || start.IsZero() || !start.After(time.Now()) {
		return nil, fmt.Errorf("oferta nie potwierdza terminu przyszłego meczu")
	}
	client.statsHomeName, client.statsAwayName = seed.Home, seed.Away
	client.statsFixture.StartTime = start
	task.report("teams", "Dopasowanie meczu i drużyn w FootyMetrics", 0, 0)
	footyFixture, err := resolveFootyFixture(client)
	if err != nil {
		return nil, err
	}
	fixture := statsFixture{ID: strconv.Itoa(footyFixture.APID), HomeID: strconv.Itoa(footyFixture.Home.APID), AwayID: strconv.Itoa(footyFixture.Away.APID), StartTime: footyFixture.Timestamp, ValidUntil: footyFixture.Timestamp}
	client.statsFixture = fixture
	client.statsQueryHomeID, client.statsQueryAwayID = fixture.HomeID, fixture.AwayID
	client.statsProps = props
	homeName, awayName := footyFixture.Home.Name, footyFixture.Away.Name
	client.statsHomeName, client.statsAwayName = homeName, awayName
	task.mu.Lock()
	task.fixture = fixture
	task.seed = seed
	task.job.FixtureID = fixture.ID
	task.job.StartTime = fixture.StartTime
	task.job.Match = homeName + " — " + awayName
	task.mu.Unlock()
	homeBundle, err := getFootyTeam(client, fixture.HomeID)
	if err != nil {
		return nil, err
	}
	awayBundle, err := getFootyTeam(client, fixture.AwayID)
	if err != nil {
		return nil, err
	}
	task.report("lineups", "Pobieranie XI z FootyMetrics", 0, 0)
	lineups := fetchFootyLineups(client, homeBundle, awayBundle)
	homeSquad, awaySquad := footyAnalysisSquad(homeBundle), footyAnalysisSquad(awayBundle)
	targets := []string{}
	for _, prop := range props {
		targets = append(targets, prop.PlayerName)
	}
	homeSelected := selectSofaMarketPlayers(homeSquad, targets)
	awaySelected := selectSofaMarketPlayers(awaySquad, targets)
	task.report("history", "Historia zawodników z FootyMetrics", 0, len(homeSelected)+len(awaySelected))
	fetch := func(teamID, name string, selected []SofaPlayerDetails) []*PlayerStatHistory {
		result := []*PlayerStatHistory{}
		for _, player := range selected {
			if task.ctx.Err() != nil {
				break
			}
			copy := *client
			copy.statsTeamID = teamID
			history, err := fetchPlayerHistory(&copy, player, name, query.MatchLimit, query.TournamentIDs, nil)
			if err != nil {
				task.warn(player.Name + ": " + err.Error())
			} else {
				result = append(result, history)
			}
			task.report("history", "Pobrano historię: "+player.Name, 1, 0)
		}
		return result
	}
	home := fetch(fixture.HomeID, homeName, homeSelected)
	away := fetch(fixture.AwayID, awayName, awaySelected)
	if err = task.ctx.Err(); err != nil {
		return nil, err
	}
	players := append(append([]*PlayerStatHistory{}, home...), away...)
	if len(players) == 0 {
		return nil, fmt.Errorf("FootyMetrics nie zwrócił historii dopasowanych zawodników")
	}
	task.report("calculate", "Obliczanie EV na podstawie FootyMetrics", 0, 0)
	roles := map[int]string{}
	for _, hist := range players {
		notes := append([]string(nil), lineups.Notes...)
		if role := lineups.Roles[hist.ID]; role != "" {
			roles[hist.ID] = role
			notes = append(notes, "Rola w modelu: pozycja ze składu FootyMetrics.")
		} else if len(hist.Positions) > 0 {
			roles[hist.ID] = hist.Positions[0]
			notes = append(notes, "Brak pozycji w aktualnym składzie; rola w modelu to ostatnia znana pozycja z historii FootyMetrics.")
		}
		hist.HistoryNotes = append(hist.HistoryNotes, notes...)
		for _, marketHistory := range hist.MarketHistories {
			marketHistory.HistoryNotes = append(marketHistory.HistoryNotes, notes...)
		}
	}
	contextData := analysisMatchContext{Input: fairModelInput{HomeID: footyFixture.Home.APID, AwayID: footyFixture.Away.APID, Roles: roles}}
	result, err := calculateValueBetsContext(task.ctx, props, home, away, lineups.Home, lineups.Away, lineups.Confirmed, nil, contextData)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("nie dopasowano zakładów do historii FootyMetrics (%d zawodników z historią, %d ofert); sprawdź nazwy zawodników i dostępność statystyk", len(players), len(props))
	}
	for i := range result {
		result[i].AnalysisID = task.job.ID
	}
	task.mu.Lock()
	task.calculation = &analysisCalculation{Props: props, Home: home, Away: away, HomeLineup: lineups.Home, AwayLineup: lineups.Away, Confirmed: lineups.Confirmed, Context: contextData}
	task.histories = map[int]*PlayerStatHistory{}
	for _, hist := range players {
		task.histories[hist.ID] = hist
	}
	task.mu.Unlock()
	return result, nil
}

// Verify every supplied offer against source identities before comparing prices.
func (s *calendarServer) verifyBookmakerFixtures(ctx context.Context, query AnalysisRequest) (CalendarEvent, error) {
	var seed CalendarEvent
	for _, source := range []struct{ book, link string }{{"SUPERBET", query.SuperbetURL}, {"BETCLIC", query.BetclicURL}, {"STS", query.STSURL}} {
		if source.link == "" {
			continue
		}
		event, err := s.bookmakerFixture(ctx, source.book, source.link)
		if err != nil {
			return CalendarEvent{}, fmt.Errorf("weryfikacja meczu %s: %w", source.book, err)
		}
		if seed.ID == "" {
			seed = event
			continue
		}
		a, errA := time.Parse(time.RFC3339, seed.StartTime)
		b, errB := time.Parse(time.RFC3339, event.StartTime)
		if errA != nil || errB != nil || a.Sub(b).Abs() > 10*time.Minute || !footyTeamNameMatches(seed.Home, event.Home) || !footyTeamNameMatches(seed.Away, event.Away) {
			return CalendarEvent{}, fmt.Errorf("link %s dotyczy innego meczu lub terminu: %s — %s (%s)", source.book, event.Home, event.Away, event.StartTime)
		}
	}
	return seed, nil
}

func (s *calendarServer) bookmakerFixture(ctx context.Context, book, link string) (CalendarEvent, error) {
	id, _, provider := extractIDAndSlug(link)
	if id == "" || provider != book {
		return CalendarEvent{}, fmt.Errorf("niepoprawny link")
	}
	lookup := func() (CalendarEvent, bool) {
		s.cache.once.Do(s.cache.readDisk)
		s.cache.mu.Lock()
		defer s.cache.mu.Unlock()
		var found CalendarEvent
		var latest time.Time
		for _, entry := range s.cache.entries {
			if entry.Calendar == nil || time.Since(entry.FetchedAt) > CalendarCacheTTL || entry.FetchedAt.Before(latest) {
				continue
			}
			for _, event := range entry.Calendar.Events {
				eventID, _, eventBook := extractIDAndSlug(event.URL)
				if eventID == id && eventBook == book {
					found, latest = event, entry.FetchedAt
				}
			}
		}
		return found, found.ID != ""
	}
	if event, ok := lookup(); ok {
		return event, nil
	}
	if book == "SUPERBET" {
		var response superbetEventsResponse
		endpoint := "https://production-superbet-offer-pl.freetls.fastly.net/v3/pl-PL/events?events=" + url.QueryEscape(id) + "&includeOnly=fixture"
		if err := getSuperbetCalendarJSON(ctx, &http.Client{Timeout: 30 * time.Second}, endpoint, &response); err != nil {
			return CalendarEvent{}, err
		}
		for _, event := range response.Events {
			if strconv.Itoa(event.ID) != id || event.Fixture.SportID != 5 {
				continue
			}
			teams := strings.SplitN(event.Fixture.Name, "·", 2)
			when, err := time.Parse(time.RFC3339, event.Fixture.Date)
			if len(teams) == 2 && err == nil {
				return CalendarEvent{ID: id, Home: strings.TrimSpace(teams[0]), Away: strings.TrimSpace(teams[1]), StartTime: when.Format(time.RFC3339), URL: link}, nil
			}
		}
	} else {
		zone, err := time.LoadLocation("Europe/Warsaw")
		if err != nil {
			return CalendarEvent{}, err
		}
		// STS and Betclic collectors cache the complete feed, across dates.
		if _, err := s.collect(ctx, book, time.Now().In(zone).Format("2006-01-02"), false); err != nil {
			return CalendarEvent{}, err
		}
		if event, ok := lookup(); ok {
			return event, nil
		}
	}
	return CalendarEvent{}, fmt.Errorf("źródło nie potwierdziło ID meczu %s", id)
}
func mergeSquad(squad, extra []SofaPlayerDetails) []SofaPlayerDetails {
	seen := map[int]bool{}
	for _, player := range squad {
		seen[player.ID] = true
	}
	for _, player := range extra {
		if !seen[player.ID] {
			squad = append(squad, player)
			seen[player.ID] = true
		}
	}
	return squad
}
func selectSofaMarketPlayers(squad []SofaPlayerDetails, targets []string) []SofaPlayerDetails {
	aliases := map[string]int{}
	byID := map[int]SofaPlayerDetails{}
	for _, player := range squad {
		regSofaAliases(aliases, player.Name, player.ShortName, player.Slug, player.ID)
		byID[player.ID] = player
	}
	lookup := lookupMapInt(aliases)
	selected := map[int]bool{}
	result := []SofaPlayerDetails{}
	for _, name := range targets {
		id, _ := strconv.Atoi(matchPlayerStrict(name, lookup))
		if id > 0 && !selected[id] {
			result = append(result, byID[id])
			selected[id] = true
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func jsonResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("API response: %v", err)
	}
}
func apiError(w http.ResponseWriter, status int, err error) {
	jsonResponse(w, status, map[string]string{"error": err.Error()})
}

var analysisIDPattern = regexp.MustCompile(`^[a-f0-9]{24}$`)

func (s *calendarServer) handler() http.Handler {
	s.startCalendarServices()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/client/sofa", sofaRelays.connect)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { jsonResponse(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /api/v1/calendars", func(w http.ResponseWriter, r *http.Request) {
		calendar, err := s.qualifiedCalendar(r.Context(), r.URL.Query().Get("bookmaker"), r.URL.Query().Get("date"), false)
		if err != nil {
			apiError(w, 502, err)
			return
		}
		jsonResponse(w, 200, calendar)
	})
	mux.HandleFunc("POST /api/v1/calendars/refresh", func(w http.ResponseWriter, r *http.Request) {
		var query struct {
			Bookmaker string `json:"bookmaker"`
			Date      string `json:"date"`
		}
		if err := decodeAPIRequest(w, r, &query); err != nil {
			apiError(w, 400, err)
			return
		}
		log.Printf("[CALENDAR-CACHE] MANUAL_REFRESH_REQUEST bookmaker=%s date=%s", query.Bookmaker, query.Date)
		calendar, err := s.qualifiedCalendar(r.Context(), query.Bookmaker, query.Date, true)
		if err != nil {
			apiError(w, 502, err)
			return
		}
		jsonResponse(w, 200, calendar)
	})
	mux.HandleFunc("POST /api/v1/teams/detect", s.detectTeams)
	mux.HandleFunc("POST /api/v1/teams/mapping", func(w http.ResponseWriter, r *http.Request) {
		var query struct {
			Slug   string `json:"slug"`
			SofaID string `json:"sofaId"`
		}
		if err := decodeAPIRequest(w, r, &query); err != nil || query.Slug == "" || !marketEventIDPattern.MatchString(query.SofaID) || query.SofaID == "0" {
			apiError(w, 400, fmt.Errorf("niepoprawne ID lub nazwa drużyny"))
			return
		}
		if err := persistTeamCache(query.Slug, query.SofaID); err != nil {
			apiError(w, 500, err)
			return
		}
		jsonResponse(w, 200, true)
	})
	mux.HandleFunc("GET /api/v1/tournaments", s.tournaments)
	mux.HandleFunc("POST /api/v1/analyses", s.startAnalysis)
	mux.HandleFunc("GET /api/v1/analyses/{id}", func(w http.ResponseWriter, r *http.Request) {
		task := s.clientTask(r, r.PathValue("id"))
		if task == nil {
			apiError(w, 404, fmt.Errorf("brak analizy w pamięci serwera; uruchom analizę ponownie"))
			return
		}
		jsonResponse(w, 200, task.snapshot())
	})
	mux.HandleFunc("POST /api/v1/analyses/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		task := s.clientTask(r, r.PathValue("id"))
		if task == nil {
			apiError(w, 404, fmt.Errorf("brak analizy"))
			return
		}
		task.cancel()
		jsonResponse(w, 200, true)
	})
	mux.HandleFunc("POST /api/v1/analyses/shotmaps", s.analysisShotmaps)
	mux.HandleFunc("POST /api/v1/analyses/{id}/sample", s.analysisSample)
	mux.HandleFunc("POST /api/v1/markets/prepare", s.prepareMatch)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientID := r.Header.Get("X-ValueBets-Client")
		if clientID != "" && !relayClientIDPattern.MatchString(clientID) {
			apiError(w, 400, fmt.Errorf("niepoprawna sesja klienta"))
			return
		}
		ctx := context.WithValue(r.Context(), sofaRelayContextKey{}, clientID)
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
func (s *calendarServer) findTask(id string) *analysisTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jobs[id]
}
func (s *calendarServer) clientTask(r *http.Request, id string) *analysisTask {
	task := s.findTask(id)
	if task == nil {
		return nil
	}
	owner, _ := task.ctx.Value(sofaRelayContextKey{}).(string)
	clientID, _ := r.Context().Value(sofaRelayContextKey{}).(string)
	if owner == "" || owner != clientID {
		return nil
	}
	return task
}

func (s *calendarServer) analysisSample(w http.ResponseWriter, r *http.Request) {
	var query struct {
		Limit int `json:"limit"`
	}
	if err := decodeAPIRequest(w, r, &query); err != nil {
		apiError(w, 400, err)
		return
	}
	if query.Limit < 1 || query.Limit > 30 {
		apiError(w, 400, fmt.Errorf("próbka musi zawierać od 1 do 30 meczów"))
		return
	}
	task := s.clientTask(r, r.PathValue("id"))
	if task == nil {
		apiError(w, 404, fmt.Errorf("analiza wygasła lub serwer został uruchomiony ponownie; wykonaj analizę ponownie"))
		return
	}
	task.mu.Lock()
	input, completed := task.calculation, task.job.Status == "completed"
	limit := min(query.Limit, task.job.Request.MatchLimit)
	task.mu.Unlock()
	if input == nil || !completed {
		apiError(w, 409, fmt.Errorf("zmiana próbki wymaga zakończonej analizy"))
		return
	}
	latest := func(players []*PlayerStatHistory) []*PlayerStatHistory {
		result := make([]*PlayerStatHistory, 0, len(players))
		for _, hist := range players {
			result = append(result, latestPlayerHistory(hist, limit))
		}
		return result
	}
	// Calculation mutates TeamSide on its inputs, so every request owns copies.
	result, err := calculateValueBetsContext(r.Context(), input.Props, latest(input.Home), latest(input.Away), input.HomeLineup, input.AwayLineup, input.Confirmed, nil, input.Context)
	if err != nil {
		apiError(w, 500, err)
		return
	}
	for i := range result {
		result[i].AnalysisID = r.PathValue("id")
	}
	jsonResponse(w, 200, result)
}
func (s *calendarServer) startAnalysis(w http.ResponseWriter, r *http.Request) {
	var query AnalysisRequest
	if err := decodeAPIRequest(w, r, &query); err != nil {
		apiError(w, 400, err)
		return
	}
	if err := validateAnalysisRequest(&query); err != nil {
		apiError(w, 400, err)
		return
	}
	bytes := make([]byte, 12)
	if _, err := cryptorand.Read(bytes); err != nil {
		apiError(w, 500, err)
		return
	}
	id := fmt.Sprintf("%x", bytes)
	clientID, _ := r.Context().Value(sofaRelayContextKey{}).(string)
	ctx, cancel := context.WithTimeout(context.WithValue(s.ctx, sofaRelayContextKey{}, clientID), 22*time.Minute)
	task := &analysisTask{ctx: ctx, cancel: cancel, chartGate: make(chan struct{}, 1), job: AnalysisJob{ID: id, Status: "queued", Stage: "queued", Message: "Oczekiwanie na analizę", CreatedAt: time.Now().UTC(), Request: query, Warnings: []string{}}}
	s.mu.Lock()
	for key, old := range s.jobs {
		job := old.snapshot()
		if !job.FinishedAt.IsZero() && time.Since(job.FinishedAt) > 2*time.Hour {
			delete(s.jobs, key)
		}
	}
	if len(s.jobs) >= 100 {
		s.mu.Unlock()
		cancel()
		apiError(w, 429, fmt.Errorf("zbyt wiele analiz w pamięci; spróbuj później"))
		return
	}
	s.jobs[id] = task
	s.mu.Unlock()
	go func() {
		defer cancel()
		defer func() {
			if value := recover(); value != nil {
				task.mu.Lock()
				task.job.Status = "failed"
				task.job.Stage = "failed"
				task.job.Error = fmt.Sprintf("błąd analizy: %v", value)
				task.job.FinishedAt = time.Now().UTC()
				task.mu.Unlock()
			}
		}()
		var result []AnalyzedBet
		var err error
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
			task.mu.Lock()
			task.job.Status = "running"
			task.job.StartedAt = time.Now().UTC()
			task.mu.Unlock()
			result, err = s.analyze(task)
		case <-ctx.Done():
			err = ctx.Err()
		}
		task.mu.Lock()
		defer task.mu.Unlock()
		task.job.FinishedAt = time.Now().UTC()
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			task.job.Status = "failed"
			task.job.Stage = "failed"
			task.job.Error = err.Error()
			task.job.Message = err.Error()
		} else {
			task.job.Status = "completed"
			task.job.Stage = "completed"
			task.job.Message = "Analiza zakończona"
			task.job.Result = result
			task.job.ResultRevision++
		}
	}()
	jsonResponse(w, 202, task.snapshot())
}
func (s *calendarServer) detectTeams(w http.ResponseWriter, r *http.Request) {
	var query struct {
		URL  string `json:"url"`
		Home string `json:"home"`
		Away string `json:"away"`
	}
	if err := decodeAPIRequest(w, r, &query); err != nil {
		apiError(w, 400, err)
		return
	}
	if query.Home == "" || query.Away == "" {
		_, slug, _ := extractIDAndSlug(query.URL)
		query.Home, query.Away = parseTeamsFromSlug(slug, "")
	}
	client, err := newSofaClient(r.Context(), nil)
	if err != nil {
		apiError(w, 500, err)
		return
	}
	defer client.tlsClient.CloseIdleConnections()
	result := detectTeamsBySlugs(client, calendarTeamSlug(query.Home), calendarTeamSlug(query.Away), s.sofaContextForURL(query.URL))
	jsonResponse(w, 200, result)
}
func (s *calendarServer) tournaments(w http.ResponseWriter, r *http.Request) {
	client, err := newSofaClient(r.Context(), nil)
	if err != nil {
		apiError(w, 500, err)
		return
	}
	defer client.tlsClient.CloseIdleConnections()
	options := map[int]string{}
	for _, teamID := range []string{r.URL.Query().Get("homeId"), r.URL.Query().Get("awayId")} {
		if !marketEventIDPattern.MatchString(teamID) {
			apiError(w, 400, fmt.Errorf("niepoprawne ID drużyny"))
			return
		}
		raw, err := client.GetSofa("/team/" + teamID + "/events/last/0")
		if err != nil {
			apiError(w, 502, err)
			return
		}
		var response SofaTeamEventsResponse
		if err = json.Unmarshal(raw, &response); err != nil {
			apiError(w, 502, err)
			return
		}
		for _, event := range response.Events {
			league := event.Tournament.UniqueTournament
			if league.ID > 0 {
				options[league.ID] = league.Name
			}
		}
	}
	result := []LeagueOption{}
	for id, name := range options {
		result = append(result, LeagueOption{ID: id, Name: name})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	jsonResponse(w, 200, result)
}
func fetchPlayerShotmap(client *ResilientClient, eventID, playerID int, expected *int) ([]ShotCoordinate, error) {
	var lastErr error
	// Match maps are shared by players; the player endpoint also covers matches
	// whose full map is absent or incomplete.
	for _, endpoint := range []string{fmt.Sprintf("/event/%d/shotmap", eventID), fmt.Sprintf("/event/%d/shotmap/player/%d", eventID, playerID)} {
		raw, _, err := getSofaHistoryData(client, endpoint, "shotmaps", time.Time{}, func(raw []byte) error {
			var response SofaShotmapResponse
			if err := json.Unmarshal(raw, &response); err != nil {
				return err
			}
			if response.Shotmap == nil {
				return fmt.Errorf("brak mapy strzałów")
			}
			return nil
		})
		if err != nil {
			lastErr = err
			if client.ctx.Err() != nil || strings.Contains(err.Error(), "HTTP 403") || strings.Contains(err.Error(), "HTTP 429") {
				return nil, err
			}
			continue
		}
		var response SofaShotmapResponse
		if err := json.Unmarshal(raw, &response); err != nil {
			lastErr = err
			continue
		}
		shots := response.PlayerShots(playerID, 0)
		if len(shots) == 0 && expected != nil && *expected > 0 {
			lastErr = fmt.Errorf("SofaScore nie zwrócił strzałów zawodnika; FootyMetrics potwierdza %d (nie oznaczono jako zero)", *expected)
			continue
		}
		log.Printf("[SOFA-SHOTMAP] player_id=%d event_id=%d shots=%d endpoint=%s", playerID, eventID, len(shots), endpoint)
		return shots, nil
	}
	return nil, lastErr
}

func (s *calendarServer) analysisShotmaps(w http.ResponseWriter, r *http.Request) {
	var query struct {
		AnalysisID string          `json:"analysisId"`
		PlayerID   int             `json:"playerId"`
		Market     CanonicalMarket `json:"market"`
		Mode       string          `json:"mode"`
		Limit      int             `json:"limit"`
	}
	if err := decodeAPIRequest(w, r, &query); err != nil {
		apiError(w, 400, err)
		return
	}
	if !analysisIDPattern.MatchString(query.AnalysisID) || query.PlayerID <= 0 || query.Limit < 0 || query.Limit > 30 || (query.Mode != "" && query.Mode != "all") {
		apiError(w, 400, fmt.Errorf("niepoprawna próbka map"))
		return
	}
	task := s.clientTask(r, query.AnalysisID)
	if task == nil {
		apiError(w, 404, fmt.Errorf("analiza wygasła lub serwer został uruchomiony ponownie; wykonaj analizę ponownie"))
		return
	}
	task.mu.Lock()
	history := task.histories[query.PlayerID]
	fixture := task.fixture
	validSample := false
	for _, bet := range task.job.Result {
		if bet.PlayerID == query.PlayerID && bet.MarketType == query.Market {
			validSample = true
			break
		}
	}
	task.mu.Unlock()
	if history == nil || !validSample {
		apiError(w, 409, fmt.Errorf("brak zakończonej próbki zawodnika"))
		return
	}
	// Serialize optional enrichment per analysis, with cancellation while waiting.
	select {
	case task.chartGate <- struct{}{}:
		defer func() { <-task.chartGate }()
	case <-r.Context().Done():
		return
	}
	key := fmt.Sprintf("%d/%s/%d", query.PlayerID, query.Market, query.Limit)
	if task.chartHistories == nil {
		task.chartHistories = map[string]*PlayerStatHistory{}
	}
	hist := task.chartHistories[key]
	if hist == nil {
		hist = latestMetricHistory(history, query.Market, query.Limit)
		hist.HistoryNotes = append([]string(nil), hist.HistoryNotes...)
		task.chartHistories[key] = hist
	}
	if hist.Matches == 0 || hist.Matches > 30 {
		apiError(w, 400, fmt.Errorf("brak zgodnej próbki rynku"))
		return
	}
	client, err := newSofaClient(r.Context(), nil)
	if err != nil {
		apiError(w, 500, err)
		return
	}
	defer client.tlsClient.CloseIdleConnections()
	client.statsFixture = fixture
	shots := make([][]ShotCoordinate, hist.Matches)
	available := make([]bool, hist.Matches)
	subAvailable := make([]bool, hist.Matches)
	notes := []string{}
	if err := task.enrichChartIdentity(client, hist); err != nil {
		apiError(w, 502, err)
		return
	}
	beforeNotes := len(hist.HistoryNotes)
	enrichSofaHistory(client, hist, hist.SofaPlayerID, []CanonicalMarket{query.Market}, hist.Matches)
	if hist.footyData != nil {
		enrichFootySuperSubs(client, hist, *hist.footyData, []CanonicalMarket{query.Market}, hist.Matches, true)
	}
	notes = append(notes, hist.HistoryNotes[beforeNotes:]...)
	for i, eventID := range hist.EventIDs {
		if eventID <= 0 {
			notes = append(notes, fmt.Sprintf("Mecz %d: brak jednoznacznego ID SofaScore", i+1))
			continue
		}
		var expected *int
		if i < len(hist.MatchShotCounts) {
			expected = hist.MatchShotCounts[i]
		}
		playerShots, err := fetchPlayerShotmap(client, eventID, hist.SofaPlayerID, expected)
		if err != nil {
			notes = append(notes, fmt.Sprintf("Mecz %d: %v", i+1, err))
			log.Printf("[SOFA-SHOTMAP] player_id=%d event_id=%d: %v", hist.SofaPlayerID, eventID, err)
			continue
		}
		shots[i] = playerShots
		available[i] = true
		if expected != nil && len(playerShots) != *expected {
			notes = append(notes, fmt.Sprintf("Mecz %d: mapa SofaScore zawiera %d strzałów, FootyMetrics: %d", i+1, len(playerShots), *expected))
		}
		if i < len(hist.SubPlayerIDs) && hist.SubPlayerIDs[i] > 0 {
			var subExpected *int
			if i < len(hist.SubShotCounts) {
				subExpected = hist.SubShotCounts[i]
			}
			subShots, subErr := fetchPlayerShotmap(client, eventID, hist.SubPlayerIDs[i], subExpected)
			if subErr != nil {
				notes = append(notes, fmt.Sprintf("Mecz %d: brak mapy zmiennika: %v", i+1, subErr))
			} else {
				for j := range subShots {
					subShots[j].IsSub = true
				}
				shots[i] = append(shots[i], subShots...)
				subAvailable[i] = true
			}
		} else if i < len(hist.SubShotCounts) && hist.SubShotCounts[i] != nil && *hist.SubShotCounts[i] == 0 {
			subAvailable[i] = true
		}
	}
	jsonResponse(w, 200, map[string]any{"sampleShots": shots, "sampleShotmapAvailable": available, "sampleSubShotmapAvailable": subAvailable, "sampleEventIds": hist.EventIDs, "sampleRatings": hist.Ratings, "sampleValuesSuperSub": hist.MetricValuesWithSuperSub[query.Market], "sampleSuperSubAvailable": hist.SuperSubAvailability[query.Market], "notes": notes})
}
func main() {
	listen := flag.String("listen", "127.0.0.1:8090", "HTTP listen address")
	dir := flag.String("data-dir", "data", "Existing calendar and team cache directory")
	flag.Parse()
	serverDataDir = *dir
	if err := os.MkdirAll(serverDataDir, 0700); err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	service := &calendarServer{ctx: ctx, cache: NewCalendarCache(filepath.Join(serverDataDir, "bookmaker_calendar_cache.json")), jobs: map[string]*analysisTask{}, slots: make(chan struct{}, 2)}
	server := &http.Server{Addr: *listen, Handler: service.handler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 8 * time.Minute, IdleTimeout: 90 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()
	log.Printf("ValueBets manual API: %s", *listen)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func (task *analysisTask) snapshot() AnalysisJob {
	task.mu.Lock()
	defer task.mu.Unlock()
	job := task.job
	job.Warnings = append([]string{}, job.Warnings...)
	// Results are immutable after publication.
	return job
}

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
			if competitionGender == "F" || isWomenEvent(event) {
				result.Gender = "F"
			}
		}
	}
	return result
}

func atomicJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return atomicJSONBytes(path, data)
}

func atomicJSONBytes(path string, data []byte) error {
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
		if len(parts) > 30 {
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
	if count != 3 {
		return fmt.Errorf("analiza wymaga linków do tego samego meczu w Superbet, Betclic i STS")
	}
	return nil
}

func (task *analysisTask) report(stage, message string, done, total int) {
	task.mu.Lock()
	if stage != "" {
		task.job.Stage = stage
	}
	if message != "" {
		task.job.Message = message
	}
	task.job.PlayersDone += done
	task.job.PlayersTotal += total
	job := task.job
	task.mu.Unlock()
	log.Printf("[ANALYSIS-PROGRESS] id=%s match=%q stage=%s players=%d/%d remaining=%d message=%q", job.ID, job.Match, job.Stage, job.PlayersDone, job.PlayersTotal, max(0, job.PlayersTotal-job.PlayersDone), job.Message)
}

func (task *analysisTask) warn(message string) {
	task.mu.Lock()
	for _, old := range task.job.Warnings {
		if old == message {
			task.mu.Unlock()
			return
		}
	}
	task.job.Warnings = append(task.job.Warnings, message)
	id := task.job.ID
	task.mu.Unlock()
	log.Printf("ANALYSIS_WARNING id=%s %s", id, message)
}

var womenCompetitionPattern = regexp.MustCompile(`(?i)(^|[^a-z])(wsl|nwsl|frauen|femenil|feminines|feminina|feminino|femenina|femenino|dames|ladies|femmes|femminile|damallsvenskan|toppserien|liga-f|we-league|a-league-women)([^a-z]|$)`)

func isWomenEvent(event CalendarEvent) bool {
	for _, name := range []string{event.Home, event.Away, event.Competition, event.Country} {
		_, gender := sofaTeamNameAndGender(name)
		if gender == "F" || sofaWomenPattern.MatchString(removeDiacritics(name)) || womenCompetitionPattern.MatchString(removeDiacritics(name)) {
			return true
		}
	}
	return false
}

type sofaRelayContextKey struct{}
type sofaRelayRequest struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
	Cancel   bool   `json:"cancel,omitempty"`
}
type sofaRelayResponse struct {
	ID     string `json:"id"`
	Status int    `json:"status"`
	Body   []byte `json:"body,omitempty"`
	Error  string `json:"error,omitempty"`
}
type sofaRelayConnection struct {
	conn    *websocket.Conn
	writes  sync.Mutex
	mu      sync.Mutex
	pending map[string]chan sofaRelayResponse
	closed  chan struct{}
	slots   chan struct{}
}
type sofaRelayRegistry struct {
	sync.Mutex
	clients map[string]*sofaRelayConnection
}

var sofaRelays = sofaRelayRegistry{clients: map[string]*sofaRelayConnection{}}
var relayClientIDPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (registry *sofaRelayRegistry) connected(clientID string) bool {
	registry.Lock()
	connection := registry.clients[clientID]
	registry.Unlock()
	if connection == nil {
		return false
	}
	select {
	case <-connection.closed:
		return false
	default:
		return true
	}
}

func (registry *sofaRelayRegistry) connect(w http.ResponseWriter, r *http.Request) {
	clientID, _ := r.Context().Value(sofaRelayContextKey{}).(string)
	if !relayClientIDPattern.MatchString(clientID) {
		apiError(w, 400, fmt.Errorf("brak sesji klienta"))
		return
	}
	upgrader := websocket.Upgrader{HandshakeTimeout: 10 * time.Second}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	connection := &sofaRelayConnection{conn: conn, pending: map[string]chan sofaRelayResponse{}, closed: make(chan struct{}), slots: make(chan struct{}, 4)}
	registry.Lock()
	previous := registry.clients[clientID]
	registry.clients[clientID] = connection
	registry.Unlock()
	if previous != nil {
		previous.conn.Close()
	}
	defer func() {
		registry.Lock()
		if registry.clients[clientID] == connection {
			delete(registry.clients, clientID)
		}
		registry.Unlock()
		close(connection.closed)
		conn.Close()
	}()
	conn.SetReadLimit(48 << 20)
	conn.SetReadDeadline(time.Now().Add(75 * time.Second))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(75 * time.Second)) })
	go func() {
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-connection.closed:
				return
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
					conn.Close()
					return
				}
			}
		}
	}()
	log.Printf("[SOFA-RELAY] Klient połączony; serwer nie wykonuje bezpośrednich żądań SofaScore")
	for {
		var result sofaRelayResponse
		if err := conn.ReadJSON(&result); err != nil {
			return
		}
		if len(result.Body) > 32<<20 || len(result.Error) > 8192 {
			return
		}
		connection.mu.Lock()
		waiting := connection.pending[result.ID]
		connection.mu.Unlock()
		if waiting != nil {
			select {
			case waiting <- result:
			default:
			}
		}
	}
}

func (registry *sofaRelayRegistry) request(parent context.Context, clientID, endpoint string) (sofaRelayResponse, error) {
	if err := parent.Err(); err != nil {
		return sofaRelayResponse{}, err
	}
	if !strings.HasPrefix(endpoint, "/") || strings.HasPrefix(endpoint, "//") || len(endpoint) > 4096 {
		return sofaRelayResponse{}, fmt.Errorf("niepoprawny endpoint SofaScore")
	}
	registry.Lock()
	connection := registry.clients[clientID]
	registry.Unlock()
	if connection == nil {
		return sofaRelayResponse{}, fmt.Errorf("brak połączenia SofaScore z komputerem klienta; pozostaw aplikację Windows uruchomioną")
	}
	select {
	case connection.slots <- struct{}{}:
		defer func() { <-connection.slots }()
	case <-parent.Done():
		return sofaRelayResponse{}, parent.Err()
	case <-connection.closed:
		return sofaRelayResponse{}, fmt.Errorf("klient SofaScore rozłączył się")
	}
	raw := make([]byte, 12)
	if _, err := cryptorand.Read(raw); err != nil {
		return sofaRelayResponse{}, err
	}
	request := sofaRelayRequest{ID: fmt.Sprintf("%x", raw), Endpoint: endpoint}
	completed := false
	defer func() {
		if completed {
			return
		}
		connection.writes.Lock()
		defer connection.writes.Unlock()
		connection.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_ = connection.conn.WriteJSON(sofaRelayRequest{ID: request.ID, Cancel: true})
	}()
	response := make(chan sofaRelayResponse, 1)
	connection.mu.Lock()
	connection.pending[request.ID] = response
	connection.mu.Unlock()
	defer func() { connection.mu.Lock(); delete(connection.pending, request.ID); connection.mu.Unlock() }()
	// The desktop may be waiting for the user to solve SofaScore's captcha.
	ctx, cancel := context.WithTimeout(parent, 6*time.Minute)
	defer cancel()
	connection.writes.Lock()
	connection.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err := connection.conn.WriteJSON(request)
	connection.writes.Unlock()
	if err != nil {
		connection.conn.Close()
		return sofaRelayResponse{}, fmt.Errorf("połączenie z klientem SofaScore: %w", err)
	}
	select {
	case result := <-response:
		completed = true
		return result, nil
	case <-ctx.Done():
		return sofaRelayResponse{}, fmt.Errorf("oczekiwanie na SofaScore z klienta: %w", ctx.Err())
	case <-connection.closed:
		return sofaRelayResponse{}, fmt.Errorf("klient SofaScore rozłączył się")
	}
}

// Calendar refresh and market discovery are independent of player analysis.
var serverBookmakers = []string{"SUPERBET", "BETCLIC", "STS"}

const matchOddsTTL = 15 * time.Minute

type AnalysisEligibility struct {
	Status    string            `json:"status"`
	Reason    string            `json:"reason,omitempty"`
	Links     map[string]string `json:"links"`
	ExpiresAt time.Time         `json:"expiresAt,omitempty"`
}
type MatchMarketAvailability struct {
	Bookmaker string            `json:"bookmaker"`
	EventID   string            `json:"eventId"`
	Status    string            `json:"status"`
	Markets   []CanonicalMarket `json:"markets"`
	CheckedAt time.Time         `json:"checkedAt"`
	ExpiresAt time.Time         `json:"expiresAt"`
	Error     string            `json:"error,omitempty"`
	Home      string            `json:"home,omitempty"`
	Away      string            `json:"away,omitempty"`
	StartTime time.Time         `json:"startTime,omitempty"`
}
type MatchPreparation struct {
	Qualified bool                               `json:"qualified"`
	Reason    string                             `json:"reason,omitempty"`
	Links     map[string]string                  `json:"links"`
	Sources   map[string]MatchMarketAvailability `json:"sources"`
}
type marketAvailabilityFlight struct {
	done   chan struct{}
	result MatchMarketAvailability
}
type calendarDayBundle struct {
	Calendars map[string]*Calendar
	Errors    map[string]string
}
type calendarTeamKey struct {
	Core, Qualifier string
	Words           []string
}
type calendarOfferCandidate struct {
	Event      CalendarEvent
	Start      time.Time
	Home, Away calendarTeamKey
}
type calendarOfferIndex map[string]map[int64][]calendarOfferCandidate

func sameCalendarIdentity(a, b calendarTeamKey) bool {
	if a.Core == "" || b.Core == "" || a.Qualifier != b.Qualifier {
		return false
	}
	if a.Core == b.Core {
		return true
	}
	shorter, longer := a.Words, b.Words
	if len(shorter) > len(longer) {
		shorter, longer = longer, shorter
	}
	if len(longer)-len(shorter) > 3 {
		return false
	}
	anchor := false
	for _, word := range shorter {
		found := false
		for _, other := range longer {
			found = found || word == other
		}
		if !found {
			return false
		}
		anchor = anchor || (len(word) >= 5 && !calendarGenericWords[word])
	}
	return anchor
}
func calendarIdentity(name string) calendarTeamKey {
	core, qualifier := calendarTeamIdentity(name)
	return calendarTeamKey{core, qualifier, strings.Split(core, "-")}
}
func indexCalendarOffers(bundle calendarDayBundle) calendarOfferIndex {
	index := calendarOfferIndex{}
	identities := map[string]calendarTeamKey{}
	identity := func(name string) calendarTeamKey {
		if key, ok := identities[name]; ok {
			return key
		}
		key := calendarIdentity(name)
		identities[name] = key
		return key
	}
	for book, calendar := range bundle.Calendars {
		index[book] = map[int64][]calendarOfferCandidate{}
		seen := map[string]bool{}
		for _, event := range calendar.Events {
			start, err := time.Parse(time.RFC3339, event.StartTime)
			if err != nil || seen[event.ID] || event.URL == "" {
				continue
			}
			seen[event.ID] = true
			candidate := calendarOfferCandidate{event, start, identity(event.Home), identity(event.Away)}
			bucket := start.Unix() / 600
			index[book][bucket] = append(index[book][bucket], candidate)
		}
	}
	return index
}
func matchCalendarOffers(seed CalendarEvent, index calendarOfferIndex) (map[string]CalendarEvent, error) {
	start, err := time.Parse(time.RFC3339, seed.StartTime)
	if err != nil {
		return nil, fmt.Errorf("niepoprawny termin meczu")
	}
	home, away := calendarIdentity(seed.Home), calendarIdentity(seed.Away)
	matches := map[string]CalendarEvent{}
	for _, book := range serverBookmakers {
		count := 0
		for offset := int64(-1); offset <= 1; offset++ {
			for _, candidate := range index[book][start.Unix()/600+offset] {
				if candidate.Start.Sub(start).Abs() <= 10*time.Minute && sameCalendarIdentity(home, candidate.Home) && sameCalendarIdentity(away, candidate.Away) {
					matches[book] = candidate.Event
					count++
				}
			}
		}
		if count != 1 {
			return nil, fmt.Errorf("%s: liczba jednoznacznych dopasowań meczu: %d", book, count)
		}
	}
	for _, left := range matches {
		for _, right := range matches {
			a, _ := time.Parse(time.RFC3339, left.StartTime)
			b, _ := time.Parse(time.RFC3339, right.StartTime)
			if a.Sub(b).Abs() > 10*time.Minute || !sameCalendarIdentity(calendarIdentity(left.Home), calendarIdentity(right.Home)) || !sameCalendarIdentity(calendarIdentity(left.Away), calendarIdentity(right.Away)) {
				return nil, fmt.Errorf("bukmacherzy nie wskazują tej samej pary drużyn i terminu")
			}
		}
	}
	return matches, nil
}

func (s *calendarServer) loadCalendarDay(ctx context.Context, date string, force bool) (calendarDayBundle, error) {
	if _, _, err := calendarDay(date); err != nil {
		return calendarDayBundle{}, err
	}
	s.calendarMu.Lock()
	s.calendarDates[date] = true
	s.calendarMu.Unlock()
	bundle := calendarDayBundle{Calendars: map[string]*Calendar{}, Errors: map[string]string{}}
	var mu sync.Mutex
	var workers sync.WaitGroup
	for _, book := range serverBookmakers {
		workers.Add(1)
		go func(book string) {
			defer workers.Done()
			calendar, err := s.collect(ctx, book, date, force)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				bundle.Errors[book] = err.Error()
				return
			}
			bundle.Calendars[book] = calendar
			fetched, err := time.Parse(time.RFC3339Nano, calendar.FetchedAt)
			if err != nil || time.Since(fetched) >= CalendarCacheTTL || fetched.After(time.Now()) {
				bundle.Errors[book] = "kalendarz nie jest aktualny; odświeżenie źródła nie powiodło się"
			}
		}(book)
	}
	workers.Wait()
	return bundle, nil
}
func (s *calendarServer) qualifiedCalendar(ctx context.Context, book, date string, force bool) (*Calendar, error) {
	if book != "SUPERBET" && book != "BETCLIC" && book != "STS" {
		return nil, fmt.Errorf("nieobsługiwany bukmacher")
	}
	bundle, err := s.loadCalendarDay(ctx, date, force)
	if err != nil {
		return nil, err
	}
	calendar := bundle.Calendars[book]
	if calendar == nil {
		return nil, fmt.Errorf("%s: %s", book, bundle.Errors[book])
	}
	for _, other := range serverBookmakers {
		if reason := bundle.Errors[other]; reason != "" {
			calendar.Warnings = append(calendar.Warnings, other+": "+reason)
		}
	}
	index := indexCalendarOffers(bundle)
	for i := range calendar.Events {
		event := &calendar.Events[i]
		eligibility := &AnalysisEligibility{Status: "pending", Links: map[string]string{}}
		event.AnalysisEligibility = eligibility
		start, err := time.Parse(time.RFC3339, event.StartTime)
		if err != nil || event.Live || !start.After(time.Now()) {
			eligibility.Status, eligibility.Reason = "inactive", "mecz rozpoczęty lub niepoprawny termin"
			continue
		}
		if len(bundle.Errors) > 0 {
			eligibility.Status, eligibility.Reason = "error", "nie potwierdzono aktualności wszystkich trzech kalendarzy"
			continue
		}
		matches, err := matchCalendarOffers(*event, index)
		if err != nil {
			eligibility.Status, eligibility.Reason = "unavailable", err.Error()
			continue
		}
		for _, other := range serverBookmakers {
			eligibility.Links[other] = matches[other].URL
		}
		eligibility.Status = "available"
		for _, other := range serverBookmakers {
			state, valid := s.cachedMarketAvailability(other, matches[other])
			if other == book && valid {
				copy := state
				event.MarketAvailability = &copy
			}
			if !valid {
				eligibility.Status, eligibility.Reason = "pending", "mecz u wszystkich trzech; trwa sprawdzanie rynków zawodników"
				continue
			}
			if eligibility.ExpiresAt.IsZero() || state.ExpiresAt.Before(eligibility.ExpiresAt) {
				eligibility.ExpiresAt = state.ExpiresAt
			}
			if state.Status == "error" {
				eligibility.Status, eligibility.Reason = "error", other+": "+state.Error
				break
			}
			if state.Status != "available" {
				eligibility.Status, eligibility.Reason = "unavailable", other+": brak obsługiwanych rynków zawodników"
				break
			}
		}
		if eligibility.Status == "pending" {
			s.queueQualification(matches)
		}
	}
	return calendar, nil
}

func (s *calendarServer) startCalendarServices() {
	s.calendarServices.Do(func() {
		// Restore disk entries before starting any background download workers.
		// Their original fetchedAt survives restart and still determines the 2h TTL.
		s.cache.once.Do(s.cache.readDisk)
		s.calendarDates = map[string]bool{}
		s.qualificationQueued = map[string]bool{}
		s.qualificationQueue = make(chan map[string]CalendarEvent, 256)
		s.loadMarketStates()
		for i := 0; i < 2; i++ {
			go s.qualificationWorker()
		}
		go s.calendarRefreshLoop()
	})
}
func calendarCacheKey(book, date string) string {
	switch book {
	case "STS":
		return "sts:" + STSWebSocket
	case "BETCLIC":
		return "betclic:" + BetclicMatchService + "GetMatchesBySportWithNotifications"
	default:
		return "SUPERBET:" + date
	}
}
func (s *calendarServer) calendarRefreshLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		s.refreshDueCalendars()
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (s *calendarServer) refreshDueCalendars() {
	zone, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		log.Printf("[CALENDAR-REFRESH] %v", err)
		return
	}
	now := time.Now().In(zone)
	today := now.Format("2006-01-02")
	s.cache.once.Do(s.cache.readDisk)
	s.cache.mu.Lock()
	cachedDates := []string{}
	for key := range s.cache.entries {
		if strings.HasPrefix(key, "SUPERBET:") {
			cachedDates = append(cachedDates, strings.TrimPrefix(key, "SUPERBET:"))
		}
	}
	s.cache.mu.Unlock()
	s.calendarMu.Lock()
	for _, date := range cachedDates {
		s.calendarDates[date] = true
	}
	s.calendarDates[today] = true
	s.calendarDates[now.AddDate(0, 0, 1).Format("2006-01-02")] = true
	dates := []string{}
	for date := range s.calendarDates {
		if date < today || date > now.AddDate(0, 0, 30).Format("2006-01-02") {
			delete(s.calendarDates, date)
		} else {
			dates = append(dates, date)
		}
	}
	s.calendarMu.Unlock()
	sort.Strings(dates)
	seen := map[string]bool{}
	var workers sync.WaitGroup
	for _, date := range dates {
		for _, book := range serverBookmakers {
			key := calendarCacheKey(book, date)
			if seen[key] {
				continue
			}
			seen[key] = true
			s.cache.mu.Lock()
			entry, ok := s.cache.entries[key]
			s.cache.mu.Unlock()
			if ok && entry.Calendar != nil && time.Since(entry.FetchedAt) >= 0 && time.Since(entry.FetchedAt) < CalendarCacheTTL {
				continue
			}
			workers.Add(1)
			go func(book, date string) {
				defer workers.Done()
				ctx, cancel := context.WithTimeout(s.ctx, 150*time.Second)
				defer cancel()
				if _, err := s.collect(ctx, book, date, false); err != nil {
					log.Printf("[CALENDAR-REFRESH] %s %s: %v", book, date, err)
				}
			}(book, date)
		}
	}
	workers.Wait()
}

func (s *calendarServer) loadMarketStates() {
	s.marketOnce.Do(func() {
		s.marketStates = map[string]MatchMarketAvailability{}
		s.marketFlights = map[string]*marketAvailabilityFlight{}
		var disk struct {
			Version int                                `json:"version"`
			States  map[string]MatchMarketAvailability `json:"states"`
		}
		raw, err := os.ReadFile(filepath.Join(serverDataDir, "calendar_markets_cache.json"))
		if err == nil && json.Unmarshal(raw, &disk) == nil && disk.Version == 1 {
			for key, state := range disk.States {
				if state.CheckedAt.After(time.Now()) || !time.Now().Before(state.ExpiresAt) {
					continue
				}
				s.marketStates[key] = state
			}
		}
	})
}
func marketAvailabilityMatches(state MatchMarketAvailability, event CalendarEvent) bool {
	start, err := time.Parse(time.RFC3339, event.StartTime)
	return err == nil && state.StartTime.Equal(start) && sameCalendarTeam(state.Home, event.Home) && sameCalendarTeam(state.Away, event.Away)
}
func (s *calendarServer) cachedMarketAvailability(book string, event CalendarEvent) (MatchMarketAvailability, bool) {
	s.loadMarketStates()
	s.marketMu.Lock()
	state, ok := s.marketStates[book+":"+event.ID]
	s.marketMu.Unlock()
	return state, ok && !state.CheckedAt.After(time.Now()) && time.Now().Before(state.ExpiresAt) && marketAvailabilityMatches(state, event)
}
func (s *calendarServer) persistMarketStates() {
	s.marketPersistMu.Lock()
	defer s.marketPersistMu.Unlock()
	s.marketMu.Lock()
	states := map[string]MatchMarketAvailability{}
	for key, state := range s.marketStates {
		if time.Now().Before(state.ExpiresAt) {
			states[key] = state
		} else {
			delete(s.marketStates, key)
		}
	}
	s.marketMu.Unlock()
	if err := atomicJSON(filepath.Join(serverDataDir, "calendar_markets_cache.json"), struct {
		Version int                                `json:"version"`
		States  map[string]MatchMarketAvailability `json:"states"`
	}{1, states}); err != nil {
		log.Printf("[MARKET-CACHE] WRITE_ERROR: %v", err)
	}
}

var marketCheckSlots = make(chan struct{}, 4)

func (s *calendarServer) checkMatchMarkets(ctx context.Context, book string, event CalendarEvent, force bool) MatchMarketAvailability {
	s.loadMarketStates()
	if !force {
		if state, valid := s.cachedMarketAvailability(book, event); valid {
			return state
		}
	}
	key := book + ":" + event.ID + ":" + event.StartTime
	s.marketMu.Lock()
	flight := s.marketFlights[key]
	if flight == nil {
		flight = &marketAvailabilityFlight{done: make(chan struct{})}
		s.marketFlights[key] = flight
		go func() {
			work, cancel := context.WithTimeout(s.ctx, 65*time.Second)
			defer cancel()
			var props []NormalizedProp
			var err error
			select {
			case marketCheckSlots <- struct{}{}:
				props, err = fetchMatchProps(work, book, event.ID)
				<-marketCheckSlots
			case <-work.Done():
				err = work.Err()
			}
			now := time.Now().UTC()
			start, _ := time.Parse(time.RFC3339, event.StartTime)
			state := MatchMarketAvailability{Bookmaker: book, EventID: event.ID, Status: "unavailable", Markets: []CanonicalMarket{}, CheckedAt: now, ExpiresAt: now.Add(matchOddsTTL), Home: event.Home, Away: event.Away, StartTime: start}
			if err != nil {
				state.Status, state.Error, state.ExpiresAt = "error", err.Error(), now.Add(time.Minute)
			} else {
				seen := map[CanonicalMarket]bool{}
				for _, prop := range props {
					if !seen[prop.MarketType] {
						state.Markets = append(state.Markets, prop.MarketType)
						seen[prop.MarketType] = true
					}
				}
				sort.Slice(state.Markets, func(i, j int) bool { return state.Markets[i] < state.Markets[j] })
				if len(state.Markets) > 0 {
					state.Status, state.ExpiresAt = "available", start
				}
			}
			if start.Before(state.ExpiresAt) {
				state.ExpiresAt = start
			}
			s.marketMu.Lock()
			s.marketStates[book+":"+event.ID] = state
			flight.result = state
			delete(s.marketFlights, key)
			close(flight.done)
			s.marketMu.Unlock()
			log.Printf("[MARKET-CHECK] bookmaker=%s event=%s status=%s markets=%d error=%q", book, event.ID, state.Status, len(state.Markets), state.Error)
			s.persistMarketStates()
		}()
	}
	s.marketMu.Unlock()
	select {
	case <-flight.done:
		return flight.result
	case <-ctx.Done():
		return MatchMarketAvailability{Bookmaker: book, EventID: event.ID, Status: "error", Error: ctx.Err().Error()}
	}
}
func qualificationKey(matches map[string]CalendarEvent) string {
	parts := []string{}
	for _, book := range serverBookmakers {
		event := matches[book]
		parts = append(parts, book, event.ID, event.StartTime)
	}
	return dataHash(parts)
}
func (s *calendarServer) queueQualification(matches map[string]CalendarEvent) {
	key := qualificationKey(matches)
	s.qualificationMu.Lock()
	defer s.qualificationMu.Unlock()
	if s.qualificationQueued[key] {
		return
	}
	select {
	case s.qualificationQueue <- matches:
		s.qualificationQueued[key] = true
	default:
	}
}
func (s *calendarServer) qualificationWorker() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case matches := <-s.qualificationQueue:
			var workers sync.WaitGroup
			for _, book := range serverBookmakers {
				workers.Add(1)
				go func(book string) { defer workers.Done(); s.checkMatchMarkets(s.ctx, book, matches[book], false) }(book)
			}
			workers.Wait()
			s.qualificationMu.Lock()
			delete(s.qualificationQueued, qualificationKey(matches))
			s.qualificationMu.Unlock()
		}
	}
}
func (s *calendarServer) prepareMatch(w http.ResponseWriter, r *http.Request) {
	var query AnalysisRequest
	if err := decodeAPIRequest(w, r, &query); err != nil {
		apiError(w, 400, err)
		return
	}
	if err := validateAnalysisRequest(&query); err != nil {
		apiError(w, 400, err)
		return
	}
	seed, err := s.verifyBookmakerFixtures(r.Context(), query)
	if err != nil {
		apiError(w, 400, err)
		return
	}
	start, _ := time.Parse(time.RFC3339, seed.StartTime)
	zone, _ := time.LoadLocation("Europe/Warsaw")
	bundle, err := s.loadCalendarDay(r.Context(), start.In(zone).Format("2006-01-02"), false)
	if err != nil {
		apiError(w, 502, err)
		return
	}
	if len(bundle.Errors) > 0 {
		apiError(w, 502, fmt.Errorf("nie potwierdzono aktualności trzech kalendarzy: %v", bundle.Errors))
		return
	}
	matches, err := matchCalendarOffers(seed, indexCalendarOffers(bundle))
	if err != nil {
		apiError(w, 400, err)
		return
	}
	for _, source := range []struct{ book, link string }{{"SUPERBET", query.SuperbetURL}, {"BETCLIC", query.BetclicURL}, {"STS", query.STSURL}} {
		id, _, _ := extractIDAndSlug(source.link)
		if matches[source.book].ID != id {
			apiError(w, 400, fmt.Errorf("%s: link wskazuje inne wydarzenie", source.book))
			return
		}
	}
	prepared := MatchPreparation{Qualified: true, Links: map[string]string{}, Sources: map[string]MatchMarketAvailability{}}
	results := make(chan MatchMarketAvailability, 3)
	for _, book := range serverBookmakers {
		prepared.Links[book] = matches[book].URL
		go func(book string) { results <- s.checkMatchMarkets(r.Context(), book, matches[book], true) }(book)
	}
	for range serverBookmakers {
		state := <-results
		prepared.Sources[state.Bookmaker] = state
		if state.Status != "available" || len(state.Markets) == 0 {
			prepared.Qualified = false
			prepared.Reason += state.Bookmaker + ": " + state.Status + " " + state.Error + "; "
		}
	}
	jsonResponse(w, 200, prepared)
}
