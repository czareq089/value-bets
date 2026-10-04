package offering

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
