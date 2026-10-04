// Existing desktop and collector tests consolidated into one file. Not run by the agent.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Analiza, statystyki, kolejka pobierania i kalendarz Superbet.
func TestPrefetchTodayThenTomorrow(t *testing.T) {
	a := &App{prefetchGeneration: 1, prefetchExcluded: map[string]bool{"1": true}, prefetchProgress: PrefetchProgress{Running: true}}
	var timeline []string
	makeEvent := func(id string, builder bool) SuperbetCalendarEvent {
		return SuperbetCalendarEvent{ID: id, Home: "A", Away: "B", BetBuilder: builder}
	}
	a.runSuperbetPrefetchQueue(context.Background(), 1, "2026-10-24",
		func(ctx context.Context, date string) (*SuperbetCalendar, error) {
			timeline = append(timeline, "day:"+date)
			if date == "2026-10-24" {
				return &SuperbetCalendar{Events: []SuperbetCalendarEvent{makeEvent("1", true), makeEvent("2", true), makeEvent("3", false)}}, nil
			}
			return &SuperbetCalendar{Events: []SuperbetCalendarEvent{makeEvent("2", true), makeEvent("4", true)}}, nil
		},
		func(ctx context.Context, event SuperbetCalendarEvent) (string, error) {
			timeline = append(timeline, "match:"+event.ID)
			return "cached", nil
		})
	if got := strings.Join(timeline, ","); got != "day:2026-10-24,match:2,day:2026-10-25,match:4" {
		t.Fatalf("wrong queue order: %s", got)
	}
	p := a.GetPrefetchProgress()
	if p.Running || p.Completed != 4 || p.Total != 4 || p.Cached != 2 || p.Skipped != 2 || p.Failed != 0 {
		t.Fatalf("wrong progress: %+v", p)
	}
}

func TestPrefetchContinuesAfterMatchFailure(t *testing.T) {
	a := &App{prefetchGeneration: 1, prefetchExcluded: map[string]bool{}, prefetchProgress: PrefetchProgress{Running: true}}
	a.runSuperbetPrefetchQueue(context.Background(), 1, "2026-10-04",
		func(ctx context.Context, date string) (*SuperbetCalendar, error) {
			return &SuperbetCalendar{Events: []SuperbetCalendarEvent{{ID: date, BetBuilder: true}}}, nil
		},
		func(ctx context.Context, event SuperbetCalendarEvent) (string, error) {
			if event.ID == "2026-10-04" {
				return "failed", fmt.Errorf("offline")
			}
			return "cached", nil
		})
	p := a.GetPrefetchProgress()
	if p.Completed != 2 || p.Cached != 1 || p.Failed != 1 || p.Running {
		t.Fatalf("failed queue: %+v", p)
	}
}

func TestPrefetchCancellationAndStaleProgress(t *testing.T) {
	a := &App{prefetchGeneration: 2, prefetchProgress: PrefetchProgress{Message: "new worker"}}
	a.setPrefetchProgress(1, func(p *PrefetchProgress) { p.Message = "old worker" })
	if a.GetPrefetchProgress().Message != "new worker" {
		t.Fatal("old worker changed new progress")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a.runSuperbetPrefetchQueue(ctx, 2, "2026-10-04",
		func(context.Context, string) (*SuperbetCalendar, error) {
			t.Fatal("cancelled queue loaded calendar")
			return nil, nil
		},
		func(context.Context, SuperbetCalendarEvent) (string, error) {
			t.Fatal("cancelled queue loaded match")
			return "", nil
		})
	if a.GetPrefetchProgress().Running {
		t.Fatal("cancelled queue still running")
	}
}

func TestPrefetchYieldsToForeground(t *testing.T) {
	foregroundAnalyses.Add(1)
	defer foregroundAnalyses.Add(-1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := waitForPrefetch(ctx); err != context.DeadlineExceeded {
		t.Fatalf("background did not yield: %v", err)
	}
}

func TestCancelledPrefetchDoesNotSaveHistory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &ResilientClient{ctx: ctx, background: true}
	if _, err := client.GetSofa("/event/1/shotmap"); err != context.Canceled {
		t.Fatalf("cancelled request: %v", err)
	}
}

func TestPrefetchDoesNotRestartOnNextAnalysis(t *testing.T) {
	zone, _ := time.LoadLocation("Europe/Warsaw")
	a := &App{prefetchGeneration: 7, prefetchDay: time.Now().In(zone).Format("2006-01-02"), prefetchLimit: 10,
		prefetchExcluded: map[string]bool{"1": true}, prefetchProgress: PrefetchProgress{Running: true, Completed: 5}}
	a.StartSuperbetPrefetch(10, "https://superbet.pl/kursy/pilka-nozna/a-vs-b-2")
	if a.prefetchGeneration != 7 || a.GetPrefetchProgress().Completed != 5 || !a.prefetchExcluded["2"] {
		t.Fatalf("next analysis restarted the worker: %+v", a.GetPrefetchProgress())
	}
}

func TestStopPrefetchCancelsWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	a := &App{prefetchCancel: cancel, prefetchGeneration: 2, prefetchDay: "2026-10-04", prefetchProgress: PrefetchProgress{Running: true}}
	a.StopSuperbetPrefetch()
	if ctx.Err() != context.Canceled || a.GetPrefetchProgress().Running || a.prefetchDay != "" || a.prefetchGeneration != 3 {
		t.Fatal("worker was not stopped and invalidated")
	}
}

func TestPrefetchPacerSpacing(t *testing.T) {
	p := prefetchPacer{interval: 30 * time.Millisecond}
	ctx := context.Background()
	if err := p.wait(ctx); err != nil {
		t.Fatal(err)
	}
	first := time.Now()
	if err := p.wait(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(first); elapsed < 25*time.Millisecond {
		t.Fatalf("requests too close: %v", elapsed)
	}
}

func TestPrefetchPacerCooldownAndCancellation(t *testing.T) {
	p := prefetchPacer{interval: time.Millisecond}
	p.cooldown(time.Minute)
	deadline := p.next
	p.cooldown(time.Second)
	if p.next.Before(deadline) {
		t.Fatal("shorter pause shortened an existing cooldown")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := p.wait(ctx); err != context.DeadlineExceeded {
		t.Fatalf("cooldown ignores cancellation: %v", err)
	}
}

func TestPrefetchErrorHandling(t *testing.T) {
	for _, tt := range []struct {
		err   error
		delay time.Duration
		retry bool
	}{
		{fmt.Errorf("http2: server sent GOAWAY; graceful shutdown"), 5 * time.Second, true},
		{&prefetchHTTPError{status: 429}, time.Minute, true},
		{&prefetchHTTPError{status: 429, retryAfter: 2 * time.Minute}, 2 * time.Minute, true},
		{&prefetchHTTPError{status: 503}, 10 * time.Second, true},
		{&prefetchHTTPError{status: 403}, time.Minute, false},
		{&prefetchHTTPError{status: 404}, 5 * time.Second, false},
		{context.Canceled, 5 * time.Second, false},
	} {
		if delay := prefetchErrorDelay(tt.err); delay != tt.delay {
			t.Errorf("%v: delay %v, want %v", tt.err, delay, tt.delay)
		}
		if retry := retryablePrefetchError(tt.err); retry != tt.retry {
			t.Errorf("%v: retry %v, want %v", tt.err, retry, tt.retry)
		}
	}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	for value, want := range map[string]time.Duration{"120": 2 * time.Minute, now.Add(time.Minute).Format(http.TimeFormat): time.Minute, "invalid": 0, "-1": 0} {
		if got := parseRetryAfter(value, now); got != want {
			t.Errorf("Retry-After %q: %v, want %v", value, got, want)
		}
	}
}

func TestPlayerShotsExcludesOwnGoalAgainstWolverhampton(t *testing.T) {
	// Mavropanos, event 16391640: four shots and one goal; the API also
	// includes his own goal as shotType=goal, goalType=own.
	const fixture = `{"shotmap":[
		{"player":{"id":829022},"shotType":"miss","playerCoordinates":{"x":5.1,"y":47.2}},
		{"player":{"id":829022},"shotType":"goal","goalType":"own","playerCoordinates":{"x":8.2,"y":52.3}},
		{"player":{"id":829022},"shotType":"goal","goalType":"regular","playerCoordinates":{"x":6.9,"y":50.6}},
		{"player":{"id":829022},"shotType":"block","playerCoordinates":{"x":4.8,"y":53}},
		{"player":{"id":829022},"shotType":"block","playerCoordinates":{"x":10,"y":39.1}}
	]}`
	var response SofaShotmapResponse
	if err := json.Unmarshal([]byte(fixture), &response); err != nil {
		t.Fatal(err)
	}
	shots := response.PlayerShots(829022, 0)
	if len(shots) != 4 {
		t.Fatalf("got %d shots, want 4", len(shots))
	}
	target, goals, inside := 0, 0, 0
	for _, shot := range shots {
		if shot.OnTarget {
			target++
		}
		if shot.ShotType == "goal" {
			goals++
		}
		if shot.InBox {
			inside++
		}
		if shot.IsSub {
			t.Fatal("player shot marked as substitute")
		}
	}
	if target != 1 || goals != 1 || inside != 4 {
		t.Fatalf("got on-target=%d, goals=%d, inside=%d; want 1, 1, 4", target, goals, inside)
	}
}

func TestPlayerShotsFiltersSubstituteAndUnrelatedPlayers(t *testing.T) {
	const fixture = `{"shotmap":[
		{"player":{"id":1},"shotType":"goal","goalType":"own"},
		{"player":{"id":2},"shotType":"goal","goalType":"own"},
		{"player":{"id":2},"shotType":"save","playerCoordinates":{"x":25,"y":50}},
		{"player":{"id":3},"shotType":"goal","goalType":"regular"},
		{"player":{"id":0},"shotType":"goal"}
	]}`
	var response SofaShotmapResponse
	if err := json.Unmarshal([]byte(fixture), &response); err != nil {
		t.Fatal(err)
	}
	shots := response.PlayerShots(1, 2)
	if len(shots) != 1 || !shots[0].IsSub || !shots[0].OnTarget || shots[0].InBox {
		t.Fatalf("unexpected substitute shots: %+v", shots)
	}
	if shots := response.PlayerShots(1, 0); len(shots) != 0 {
		t.Fatalf("unexpected shots without a substitute: %+v", shots)
	}
}

func TestExcludeBlockedShotsFromOnTarget(t *testing.T) {
	tests := []struct {
		name       string
		stats      SofaPlayerStats
		shots      []ShotCoordinate
		isSub      bool
		wantTarget int
		wantBlock  int
	}{
		{
			name:  "goal-line block is separate from saves and goals",
			stats: SofaPlayerStats{TotalShots: 5, OnTargetScoringAttempt: 3, BlockedScoringAttempt: 2},
			shots: []ShotCoordinate{
				{ShotType: "save"}, {ShotType: "goal"}, {ShotType: "block"},
				{ShotType: "blocked"}, {ShotType: "blocked-off-line"},
			},
			wantTarget: 2, wantBlock: 3,
		},
		{
			name:       "ordinary blocks are already excluded by SofaScore",
			stats:      SofaPlayerStats{TotalShots: 3, OnTargetScoringAttempt: 1, BlockedScoringAttempt: 2},
			shots:      []ShotCoordinate{{ShotType: "save"}, {ShotType: "block"}, {ShotType: "blocked"}},
			wantTarget: 1, wantBlock: 2,
		},
		{
			name:       "player does not include substitute goal-line block",
			stats:      SofaPlayerStats{TotalShots: 1, OnTargetScoringAttempt: 1},
			shots:      []ShotCoordinate{{ShotType: "save"}, {ShotType: "blocked-off-line", IsSub: true}},
			wantTarget: 1, wantBlock: 0,
		},
		{
			name:  "substitute excludes only their own goal-line block",
			stats: SofaPlayerStats{TotalShots: 2, OnTargetScoringAttempt: 2},
			shots: []ShotCoordinate{
				{ShotType: "blocked-off-line"}, {ShotType: "blocked-off-line", IsSub: true},
				{ShotType: "goal", IsSub: true},
			},
			isSub: true, wantTarget: 1, wantBlock: 1,
		},
		{
			name:       "missing shotmap preserves available statistics",
			stats:      SofaPlayerStats{TotalShots: 3, OnTargetScoringAttempt: 1, BlockedScoringAttempt: 2},
			wantTarget: 1, wantBlock: 2,
		},
		{
			name:       "inconsistent shotmap cannot make count negative",
			stats:      SofaPlayerStats{TotalShots: 1, BlockedScoringAttempt: 1},
			shots:      []ShotCoordinate{{ShotType: "blocked-off-line"}},
			wantTarget: 0, wantBlock: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := tt.stats
			excludeBlockedShotsFromOnTarget(&stats, tt.shots, tt.isSub)
			if stats.OnTargetScoringAttempt != tt.wantTarget || stats.BlockedScoringAttempt != tt.wantBlock {
				t.Fatalf("got on-target=%d, blocked=%d; want on-target=%d, blocked=%d",
					stats.OnTargetScoringAttempt, stats.BlockedScoringAttempt, tt.wantTarget, tt.wantBlock)
			}
			if stats.TotalShots != tt.stats.TotalShots {
				t.Fatalf("total shots changed from %d to %d", tt.stats.TotalShots, stats.TotalShots)
			}
		})
	}
}

func TestSuperbetCalendarDayWarsaw(t *testing.T) {
	for _, tt := range []struct {
		date  string
		hours time.Duration
		utc   string
	}{
		{"2026-10-04", 24, "2026-10-03T22:00:00Z"},
		{"2026-03-29", 23, "2026-03-28T23:00:00Z"},
		{"2026-10-25", 25, "2026-10-24T22:00:00Z"},
	} {
		t.Run(tt.date, func(t *testing.T) {
			start, end, err := superbetCalendarDay(tt.date)
			if err != nil {
				t.Fatal(err)
			}
			if start.UTC().Format(time.RFC3339) != tt.utc || end.Sub(start) != tt.hours*time.Hour {
				t.Fatalf("unexpected day boundaries: %v to %v", start, end)
			}
		})
	}
	if _, _, err := superbetCalendarDay("2026-02-30"); err == nil {
		t.Fatal("invalid date accepted")
	}
}

func TestSuperbetCalendarLoadsMatchesAndMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v3/pl-PL/events":
			q := r.URL.Query()
			if q.Get("startDate") != "2026-10-03T22:00:00Z" || q.Get("endDate") != "2026-10-04T22:00:00Z" || q.Get("sports") != "5" {
				t.Errorf("wrong calendar query: %v", q)
			}
			if q.Get("index") == "active-live" {
				fmt.Fprint(w, `{"events":[{"event_id":12,"fixture":{"event_name":"Łódź·Gdańsk","utc_date":"2026-10-04T18:00:00Z","sport_id":5,"tournament_id":106,"category_id":45}}]}`)
			} else {
				fmt.Fprint(w, `{"events":[
				{"event_id":12,"fixture":{"event_name":"Łódź·Gdańsk","utc_date":"2026-10-04T18:00:00Z","sport_id":5,"tournament_id":106,"category_id":45}},
				{"event_id":11,"fixture":{"event_name":"A·B","utc_date":"2026-10-03T22:00:00Z","sport_id":5,"tournament_id":106,"category_id":45}},
				{"event_id":13,"fixture":{"event_name":"Next·Day","utc_date":"2026-10-04T22:00:00Z","sport_id":5}},
				{"event_id":14,"fixture":{"event_name":"Other·Sport","utc_date":"2026-10-04T15:00:00Z","sport_id":2}},
				{"event_id":15,"fixture":{"event_name":"Outright","utc_date":"2026-10-04T15:00:00Z","sport_id":5}}
				]}`)
			}
		case "/v2/pl-PL/struct":
			fmt.Fprint(w, `{"data":{"tournaments":[{"id":"106","localNames":{"pl-PL":"Premier League"}}],"categories":[{"id":"45","localNames":{"pl-PL":"Anglia"}}]}}`)
		case "/betbuilder/v2/getBetbuilderEvents":
			fmt.Fprint(w, `{"events":["12"]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	result, err := loadSuperbetCalendar(server.Client(), "2026-10-04", server.URL, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Events) != 2 || len(result.Warnings) != 0 {
		t.Fatalf("unexpected calendar: %+v", result)
	}
	if result.Events[0].ID != "11" || result.Events[0].StartTime != "2026-10-04T00:00:00+02:00" {
		t.Fatalf("wrong ordering or timezone: %+v", result.Events)
	}
	event := result.Events[1]
	if !event.Live || !event.BetBuilder || event.Competition != "Premier League" || event.Country != "Anglia" {
		t.Fatalf("wrong metadata: %+v", event)
	}
	id, slug, provider := extractIDAndSlug(event.URL)
	home, away := parseTeamsFromSlug(slug, provider)
	if id != "12" || home != "lodz" || away != "gdansk" || provider != "SUPERBET" {
		t.Fatalf("calendar URL incompatible with analysis: %s", event.URL)
	}
}

func TestSuperbetCalendarPartialFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v3/pl-PL/events" && r.URL.Query().Get("index") == "active-prematch" {
			fmt.Fprint(w, `{"events":[{"event_id":12,"fixture":{"event_name":"A·B","utc_date":"2026-10-04T18:00:00Z","sport_id":5}}]}`)
			return
		}
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	result, err := loadSuperbetCalendar(server.Client(), "2026-10-04", server.URL, server.URL)
	if err != nil || len(result.Events) != 1 || len(result.Warnings) != 3 {
		t.Fatalf("partial failure: result=%+v error=%v", result, err)
	}
	result, err = loadSuperbetCalendar(server.Client(), "2026-10-05", server.URL+"/missing", server.URL)
	if err == nil || result != nil || !strings.Contains(err.Error(), "kalendarza") {
		t.Fatalf("total failure reported as success: result=%+v error=%v", result, err)
	}
}

// Linki wydarzeń zgodne z analizą.
func TestBookmakerCalendarURLsWorkWithExistingAnalysis(t *testing.T) {
	for _, tt := range []struct {
		url      string
		provider string
	}{
		{"https://www.sts.pl/kursy/pilka-nozna/f123", "STS"},
		{"https://www.betclic.pl/pilka-nozna-s1/example-m123", "BETCLIC"},
	} {
		result := desktopCalendar("2026-10-04", []CalendarEvent{{ID: "123", Home: "Bayern München", Away: "Łódź", URL: tt.url}}, []string{"warning"})
		if len(result.Events) != 1 || len(result.Warnings) != 1 {
			t.Fatalf("missing data: %+v", result)
		}
		id, slug, provider := extractIDAndSlug(result.Events[0].URL)
		home, away := parseTeamsFromSlug(slug, provider)
		if id != "123" || provider != tt.provider || home != "bayern-munchen" || away != "lodz" {
			t.Fatalf("incompatible calendar URL: %s (%s %s %s)", result.Events[0].URL, id, home, away)
		}
	}
}

// Transport Betclic i STS.
func frame(flags byte, body []byte) []byte {
	header := make([]byte, 5)
	header[0] = flags
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
	return append(header, body...)
}

func TestBetclicRPCPreservesRequestAndReadsFragmentedFrames(t *testing.T) {
	body := append(ProtoUint(1, 123), ProtoString(2, "pl")...)
	data := frame(0, []byte{8, 123})
	trailers := frame(0x80, []byte("grpc-status: 0\r\n"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, _ := io.ReadAll(r.Body)
		if r.Method != "POST" || r.URL.Path != "/GetMatchesBySportWithNotifications" || !bytes.Equal(request, frame(0, body)) {
			t.Errorf("wrong RPC request: %s %s %x", r.Method, r.URL.Path, request)
		}
		for name, expected := range map[string]string{"Content-Type": "application/grpc-web+proto", "X-Bg-Ref-Brand": "BETCLIC", "X-Bg-Regulation": "PL", "X-Grpc-Web": "1"} {
			if r.Header.Get(name) != expected {
				t.Errorf("wrong %s header", name)
			}
		}
		w.Header().Set("Content-Type", "application/grpc-web+proto")
		for _, b := range append(data, trailers...) {
			_, _ = w.Write([]byte{b})
			w.(http.Flusher).Flush()
		}
	}))
	defer server.Close()
	got, err := FetchBetclicRPC(context.Background(), server.Client(), server.URL+"/GetMatchesBySportWithNotifications", body)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("snapshot=%x error=%v", got, err)
	}
}

func TestBetclicSnapshotRejectsGRPCErrorsAndTruncation(t *testing.T) {
	for _, tt := range []struct {
		name      string
		raw       []byte
		errorText string
	}{
		{"grpc error", frame(0x80, []byte("grpc-status: 3\r\ngrpc-message: Invalid%20sport\r\n")), "Invalid sport"},
		{"truncated first", []byte{0, 0, 0}, "unexpected EOF"},
		{"truncated after data", append(frame(0, []byte{8, 1}), []byte{0, 0, 0}...), "unexpected EOF"},
		{"compressed", frame(1, []byte{8, 1}), "flagi"},
		{"empty", nil, "EOF"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readBetclicSnapshot(context.Background(), io.NopCloser(bytes.NewReader(tt.raw)), time.Second)
			if err == nil || got != nil || !strings.Contains(err.Error(), tt.errorText) {
				t.Fatalf("got=%x error=%v", got, err)
			}
		})
	}
}

func TestBetclicSnapshotIdleAndCancellation(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			reader, writer := io.Pipe()
			defer writer.Close()
			data := frame(0, []byte{8, 1})
			go func() {
				_, _ = writer.Write(data)
				if partial {
					_, _ = writer.Write([]byte{0, 0})
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := readBetclicSnapshot(ctx, reader, 40*time.Millisecond)
			if partial {
				if !errors.Is(err, io.ErrUnexpectedEOF) || got != nil {
					t.Fatalf("partial frame accepted: %x %v", got, err)
				}
			} else if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("idle snapshot: %x %v", got, err)
			}
		})
	}
	reader, writer := io.Pipe()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readBetclicSnapshot(ctx, reader, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSTSSnapshotSelectsRequestedChannel(t *testing.T) {
	for _, fixtureID := range []string{"", "123"} {
		t.Run("fixture="+fixtureID, func(t *testing.T) {
			upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "https://www.sts.pl" }}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				var sub struct {
					Type     int `json:"t"`
					Channels []struct {
						Name string `json:"s"`
					} `json:"u"`
				}
				if conn.ReadJSON(&sub) != nil {
					return
				}
				if sub.Type != 1 || sub.Channels[0].Name != "i_pl" {
					t.Errorf("wrong subscription: %+v", sub)
				}
				calendar := `{"B":{"S":{"1":{"n":"Piłka Nożna","C":{}}}},"P":{"unrelated":{}}}`
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"s":"i_pl","f":1}`+"\n"+calendar))
				if fixtureID != "" {
					if len(sub.Channels) != 2 || sub.Channels[1].Name != "f_f123_pl" {
						t.Errorf("missing fixture subscription")
					}
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"s":"f_f123_pl","f":1}`))
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"P":{"1m456":{"f":"f123","m":{"2003":{}}}}}`))
				}
			}))
			defer server.Close()
			got, err := FetchSTSSnapshot(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), fixtureID)
			if err != nil {
				t.Fatal(err)
			}
			var result map[string]json.RawMessage
			if json.Unmarshal(got, &result) != nil {
				t.Fatalf("not JSON: %s", got)
			}
			if fixtureID == "" && !hasSTSCalendar(result) {
				t.Fatalf("missing calendar")
			}
			if fixtureID != "" && !hasSTSFixtureMarkets(result["P"], fixtureID) {
				t.Fatalf("calendar mistaken for fixture markets: %s", got)
			}
		})
	}
}

func TestSTSCalendarFiltersAndSortsDST(t *testing.T) {
	raw := []byte(`{"s":"i_pl","f":1}
{"B":{"S":{"1":{"C":{"1":{"n":"Anglia","T":{"17":{"n":"Premier League","FX":{
"f1":{"ft":"m","H":{"n":" A "},"A":{"n":"B"},"a":{"1m22":1},"t":"2026-10-25T00:30:00Z","Bb":true},
"f2":{"ft":"m","H":{"n":"C"},"A":{"n":"D"},"t":"2026-10-25T01:15:00Z"},
"f3":{"ft":"s","t":"2026-10-25T00:00:00Z"},
"f4":{"ft":"m","H":{"n":"E"},"A":{"n":"F"},"t":"2026-10-25T23:00:00Z"},
"f5":{"ft":"m","H":{"n":"G"},"A":{"n":"H"},"t":"2026-10-24T22:00:00Z"},
"f6":{"ft":"m","H":{"n":"I"},"A":{"n":"J"},"t":"bad date"}
}}}}}}}},"Y":{"f1":{}}}`)
	events, err := ParseSTSCalendar(raw, "2026-10-25")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].ID != "5" || events[1].ID != "1" || events[2].ID != "2" {
		t.Fatalf("wrong calendar: %+v", events)
	}
	if event := events[1]; event.Home != "A" || event.Away != "B" || !event.Live || !event.BetBuilder || event.Competition != "Premier League" || event.Country != "Anglia" {
		t.Fatalf("wrong metadata: %+v", event)
	}
	if _, err := ParseSTSCalendar(raw, "2026-02-30"); err == nil {
		t.Fatal("invalid date accepted")
	}
	if _, err := ParseSTSCalendar([]byte(`{"recommendations":[]}`), "2026-10-25"); err == nil {
		t.Fatal("recommendations accepted as a calendar")
	}
}

// Parsowanie katalogu Betclic.
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
