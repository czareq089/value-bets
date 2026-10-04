package offering

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func protoBytes(field uint64, value []byte) []byte {
	body := appendVarint(nil, field<<3|2)
	body = appendVarint(body, uint64(len(value)))
	return append(body, value...)
}

func calendarMatch(id uint64, when string, contestants bool) []byte {
	body := ProtoUint(1, id)
	body = append(body, ProtoString(2, "A - B")...)
	body = append(body, ProtoString(3, when)...)
	body = append(body, ProtoUint(4, 1)...)
	league := append(ProtoString(2, "Premier League"), ProtoString(5, "GB")...)
	body = append(body, protoBytes(8, league)...)
	if contestants {
		body = append(body, protoBytes(12, ProtoString(3, "A"))...)
		body = append(body, protoBytes(12, ProtoString(3, "B"))...)
	}
	return body
}

func calendarFrame(total uint64, matches ...[]byte) []byte {
	body := ProtoString(1, "ftb_rsm")
	for _, match := range matches {
		body = append(body, protoBytes(3, match)...)
	}
	body = append(body, ProtoString(4, "Piłka nożna")...)
	body = append(body, ProtoUint(5, total)...)
	return frame(0, protoBytes(1, body))
}

func TestBetclicCalendarParsesOnlyMatchListAndPreservesIDs(t *testing.T) {
	const id = uint64(1237846040018944)
	raw := calendarFrame(2, calendarMatch(id, "2026-10-04T18:45:00.0000000Z", true), calendarMatch(id+1, "2026-10-05T12:00:00Z", false))
	page, err := parseBetclicCalendarPage(raw)
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 2 || len(page.IDs) != 2 || len(page.Events) != 1 {
		t.Fatalf("unexpected page: %+v", page)
	}
	event := page.Events[0]
	if event.ID != strconv.FormatUint(id, 10) || event.Home != "A" || event.Away != "B" || !event.Live || event.Competition != "Premier League" || event.Country != "GB" || event.BetBuilder {
		t.Fatalf("wrong metadata: %+v", event)
	}
	if !strings.HasSuffix(event.URL, "-m"+event.ID) {
		t.Fatalf("wrong match URL: %s", event.URL)
	}
	if _, err := parseBetclicCalendarPage(raw[:len(raw)-1]); err == nil {
		t.Fatal("truncated frame accepted")
	}
	if _, err := decodeProtoFields([]byte{10, 255, 255}); err == nil {
		t.Fatal("malformed protobuf length accepted")
	}
	emptyBody := append(ProtoString(1, "ftb_rsm"), ProtoString(4, "")...)
	emptyBody = append(emptyBody, ProtoUint(5, 1281)...)
	empty, err := parseBetclicCalendarPage(frame(0, protoBytes(1, emptyBody)))
	if err != nil || empty.Total != 1281 || len(empty.IDs) != 0 {
		t.Fatalf("empty tail page not recognised: %+v %v", empty, err)
	}
}

func TestBetclicCalendarPaginatesAndFiltersWarsawDay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, _ := io.ReadAll(r.Body)
		if len(request) < 5 {
			t.Error("missing envelope")
			return
		}
		fields, err := decodeProtoFields(request[5:])
		if err != nil {
			t.Error(err)
			return
		}
		if protoText(fields, 1) != "football" || protoText(fields, 2) != "pl" {
			t.Errorf("wrong request body: %x", request)
		}
		offset := protoNumber(fields, 4)
		var matches [][]byte
		switch offset {
		case 0:
			matches = [][]byte{calendarMatch(1, "2026-10-03T22:00:00Z", true), calendarMatch(2, "2026-10-04T22:00:00Z", true)}
		case 2:
			// An outright occupies an offset even though it is not a match.
			matches = [][]byte{calendarMatch(3, "2026-10-04T10:00:00Z", false), calendarMatch(4, "2026-10-04T18:00:00Z", true)}
		case 4:
			matches = [][]byte{calendarMatch(5, "2026-10-04T19:00:00Z", true)}
		default:
			t.Errorf("wrong pagination offset: %d", offset)
		}
		w.Header().Set("Content-Type", "application/grpc-web+proto")
		_, _ = w.Write(calendarFrame(5, matches...))
	}))
	defer server.Close()
	result, err := FetchBetclicCalendar(context.Background(), server.Client(), server.URL, "2026-10-04")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Events) != 3 || len(result.Warnings) != 0 {
		t.Fatalf("unexpected calendar: %+v", result)
	}
	if result.Events[0].ID != "1" || result.Events[0].StartTime != "2026-10-04T00:00:00+02:00" || result.Events[2].ID != "5" {
		t.Fatalf("wrong filtering/order: %+v", result.Events)
	}
}

func TestBetclicCalendarReportsChangingOfferAndPageFailure(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				fields, _ := decodeProtoFields(raw[5:])
				w.Header().Set("Content-Type", "application/grpc-web+proto")
				if protoNumber(fields, 4) == 0 {
					_, _ = w.Write(calendarFrame(2, calendarMatch(1, "2026-10-04T12:00:00Z", true)))
					return
				}
				if failure {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				_, _ = w.Write(calendarFrame(1, calendarMatch(1, "2026-10-04T12:00:00Z", true)))
			}))
			defer server.Close()
			result, err := FetchBetclicCalendar(context.Background(), server.Client(), server.URL, "2026-10-04")
			if failure {
				if err == nil || result != nil {
					t.Fatalf("page failure treated as success: %+v %v", result, err)
				}
			} else if err != nil || len(result.Warnings) != 1 || len(result.Events) != 1 {
				t.Fatalf("changing offer not reported: %+v %v", result, err)
			}
		})
	}
	if !bytes.Equal(BetclicCalendarRequest(40), []byte{10, 8, 'f', 'o', 'o', 't', 'b', 'a', 'l', 'l', 18, 2, 'p', 'l', 32, 40}) {
		t.Fatal("live-verified request encoding changed")
	}
}
