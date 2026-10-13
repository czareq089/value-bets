package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
)

func TestFairV3CountsHitsRatherThanSurplus(t *testing.T) {
	model := func(values []int, under bool) *FairModelDetails {
		return contextualFairModel(&PlayerStatHistory{MetricValues: map[CanonicalMarket][]int{MarketShotsTotal: values}, Minutes: []int{90, 90, 90, 90}}, MarketShotsTotal, 0.5, under, fairModelInput{})
	}
	a, b := model([]int{0, 0, 1, 1}, false), model([]int{0, 0, 9, 20}, false)
	if a.Probability != b.Probability || a.Probability != .5 {
		t.Fatalf("surplus changed probability: %v vs %v", a.Probability, b.Probability)
	}
	if model([]int{0, 0, 0, 20}, false).Probability >= a.Probability {
		t.Fatal("an additional 90-minute zero must lower the over probability")
	}
	if math.Abs(model([]int{0, 0, 0, 20}, false).Probability+model([]int{0, 0, 0, 20}, true).Probability-1) > 1e-12 {
		t.Fatal("under and over must be complementary")
	}
	if model([]int{0, 0, 0, 0}, false).Probability <= 0 || model([]int{1, 1, 1, 1}, false).Probability >= 1 {
		t.Fatal("small samples must retain uncertainty")
	}
	zeros := make([]int, 50)
	if math.Abs(model(zeros, true).Probability+model(zeros, false).Probability-1) > 1e-12 {
		t.Fatal("large samples must retain complementary probabilities")
	}
}

type mockSofaHTTP struct {
	tlsclient.HttpClient
	mu       sync.Mutex
	paths    []string
	response func(string) string
}

func (m *mockSofaHTTP) SetCookies(*url.URL, []*fhttp.Cookie) {}
func (m *mockSofaHTTP) Do(r *fhttp.Request) (*fhttp.Response, error) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1")
	m.mu.Lock()
	m.paths = append(m.paths, path)
	m.mu.Unlock()
	return &fhttp.Response{StatusCode: 200, Proto: "HTTP/2", Header: fhttp.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(m.response(path)))}, nil
}

func TestExplicitHistoryPaginatesDeduplicatesAndKeepsNinetyMinuteZeros(t *testing.T) {
	setupArchiveForTest(t)
	previousDir := serverDataDir
	serverDataDir = t.TempDir()
	t.Cleanup(func() { serverDataDir = previousDir })
	fixture := statsFixture{ID: "987654", StartTime: time.Now().Add(time.Hour), ValidUntil: time.Now().Add(5 * time.Hour)}
	page := func(first, last int, next bool) string {
		events := []any{}
		played := map[string]int{}
		stats := map[string]any{}
		for id := first; id <= last; id++ {
			events = append(events, map[string]any{"id": id, "startTimestamp": fixture.StartTime.Add(-time.Duration(id) * time.Hour).Unix(), "status": map[string]int{"code": 100}, "homeTeam": map[string]any{"id": 1, "name": "Home"}, "awayTeam": map[string]any{"id": 2, "name": "Away"}, "tournament": map[string]any{"uniqueTournament": map[string]int{"id": 8}}})
			played[fmt.Sprint(id)] = 1
			stats[fmt.Sprint(id)] = map[string]int{"minutesPlayed": 90}
		}
		// A completed-looking event after the frozen fixture must never enter history.
		events = append(events, map[string]any{"id": 999, "startTimestamp": fixture.StartTime.Add(time.Hour).Unix(), "status": map[string]int{"code": 100}})
		raw, _ := json.Marshal(map[string]any{"events": events, "playedForTeamMap": played, "statisticsMap": stats, "hasNextPage": next})
		return string(raw)
	}
	mock := &mockSofaHTTP{response: func(path string) string {
		if strings.HasSuffix(path, "/events/last/0") {
			return page(1, 10, true)
		}
		if strings.HasSuffix(path, "/events/last/1") {
			return page(10, 25, false)
		}
		if strings.HasSuffix(path, "/statistics") {
			return `{"statistics":{"minutesPlayed":90,"totalShots":0},"position":"D"}`
		}
		t.Errorf("unexpected network request: %s", path)
		return `{}`
	}}
	client := &ResilientClient{ctx: archiveTestContext(t), state: &sofaClientState{}, tlsClient: mock, statsFixture: fixture}
	h, err := fetchSofaPlayerCompleteHistory(client, 876543, 20, "ALL")
	if err != nil {
		t.Fatal(err)
	}
	if h.Matches != 20 || h.Incomplete {
		t.Fatalf("wrong extended history: matches=%d incomplete=%v", h.Matches, h.Incomplete)
	}
	for i, id := range h.EventIDs {
		if id != i+1 || h.Minutes[i] != 90 || h.MetricValues[MarketShotsTotal][i] != 0 {
			t.Fatalf("history order, duplicate, cutoff or zero wrong: %+v", h)
		}
	}
	mock.mu.Lock()
	count := len(mock.paths)
	mock.mu.Unlock()
	if count != 22 {
		t.Fatalf("extra requests: %d", count)
	}
}

func TestManualCalculationDeduplicatesSameBookmakerOffer(t *testing.T) {
	h := &PlayerStatHistory{ID: 1, FullName: "Player Test", Matches: 10, MetricValues: map[CanonicalMarket][]int{MarketTackles: {0, 1}}}
	props := []NormalizedProp{{PlayerName: "Player Test", Bookmaker: BookmakerSTS, MarketType: MarketTackles, Line: .5, Odds: 2}, {PlayerName: "Player Test", Bookmaker: BookmakerSTS, MarketType: MarketTackles, Line: .5, Odds: 3}, {PlayerName: "Player Test", Bookmaker: BookmakerSuperbet, MarketType: MarketTackles, Line: .5, Odds: 2}}
	bets := CalculateValueBets(props, []*PlayerStatHistory{h}, nil, nil, nil, false)
	if len(bets) != 2 || bets[0].BookmakerOdds != 3 {
		t.Fatalf("duplicates in manual results: %+v", bets)
	}
}

func TestSpanishAliasesKeepGenderAndReserveIdentity(t *testing.T) {
	for _, tt := range []struct{ input, name, slug string }{{"alaves", "Deportivo Alavés", "deportivo-alaves"}, {"athletic-bilbao", "Athletic Club", "athletic-club"}} {
		team := SofaTeamItem{ID: 2885, Name: tt.name, Slug: tt.slug, Gender: "M"}
		if !sofaTeamMatchesSlug(tt.input, team, "M") {
			t.Fatalf("alias %s failed", tt.input)
		}
		team.Gender = "F"
		if sofaTeamMatchesSlug(tt.input, team, "M") {
			t.Fatal("matched women's club")
		}
		team.Gender, team.Name, team.Slug = "M", tt.name+" B", tt.slug+"-b"
		if sofaTeamMatchesSlug(tt.input, team, "M") {
			t.Fatal("matched reserve club")
		}
		team.Name, team.Slug = tt.name+" U19", tt.slug+"-u19"
		if sofaTeamMatchesSlug(tt.input, team, "M") {
			t.Fatal("matched youth club")
		}
	}
}

func TestWomenOffersExcludedBeforeDiscovery(t *testing.T) {
	if !isWomenEvent(CalendarEvent{Competition: "Superliga", Country: "Dania, Kobiety"}) {
		t.Fatal("missed women's category label")
	}
	for _, name := range []string{"Premier League Women", "NWSL", "Frauen Bundesliga", "Liga F", "Serie A Femminile", "Division 1 Féminine", "Ekstraliga kobiet"} {
		if !isWomenEvent(CalendarEvent{Competition: name}) {
			t.Errorf("missed league %q", name)
		}
	}
	for _, team := range []string{"Arsenal (K)", "Athletic Bilbao (F)", "Chelsea Women", "Barcelona W"} {
		if !isWomenEvent(CalendarEvent{Home: team}) {
			t.Errorf("missed team %q", team)
		}
	}
	if isWomenEvent(CalendarEvent{Home: "Alaves", Away: "Athletic Bilbao", Competition: "La Liga"}) {
		t.Fatal("excluded men's fixture")
	}
	cache := NewCalendarCache("")
	cache.once.Do(func() {})
	now := time.Now()
	cache.entries["sts:test"] = calendarCacheEntry{FetchedAt: now, Calendar: &Calendar{Events: []CalendarEvent{
		{ID: "1", Home: "Arsenal Women", Away: "Chelsea", StartTime: now.Add(time.Hour).Format(time.RFC3339), URL: "https://sts.pl/1"},
		{ID: "2", Home: "Alaves", Away: "Athletic Bilbao", StartTime: now.Add(time.Hour).Format(time.RFC3339), URL: "https://sts.pl/2"},
	}}}
	s := &calendarServer{cache: cache, marketCache: map[string]MatchMarketAvailability{}, marketFlights: map[string]*marketAvailabilityFlight{}, oddsCache: map[string]matchOddsSnapshot{}, prefetch: statsBackgroundState{Events: map[string]statsWarmupState{}}}
	candidates := s.marketScanCandidates(now)
	if len(candidates) != 1 || candidates[0].Event.ID != "2" {
		t.Fatalf("unexpected discovery candidates: %+v", candidates)
	}
	if _, ok := s.nextStatsWarmup(); ok {
		t.Fatal("analysis must wait for independent offer discovery")
	}
	result := s.checkMatchMarkets(context.Background(), "STS", "1", "Arsenal Women", "Chelsea")
	if result.Status != "error" || !strings.Contains(result.Error, "kobiet") {
		t.Fatalf("direct women's download allowed: %+v", result)
	}
}

func TestCalendarArchiveOneFilePerBookmakerDay(t *testing.T) {
	setupArchiveForTest(t)
	for _, date := range []string{"2026-10-09", "2026-10-10", "2026-10-11", "2026-10-09"} {
		ctx := context.WithValue(context.Background(), archiveContextKey{}, archiveInfo{Bookmaker: "SUPERBET", Scope: "kalendarz-pilka-nozna-dzien-" + date})
		if err := archiveResponse(ctx, "odpowiedz-transport", []byte(`{}`), "http.bin", nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := archiveResponse(ctx, "odczytane-mecze-liczba-0", nil, "", Calendar{Date: date}, nil); err != nil {
			t.Fatal(err)
		}
	}
	files := []string{}
	filepath.WalkDir(archiveRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if len(files) != 1 {
		t.Fatalf("expected one daily archive, got %v", files)
	}
	f, err := os.Open(files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var calendars map[string]Calendar
	if err := json.NewDecoder(r).Decode(&calendars); err != nil {
		t.Fatal(err)
	}
	if len(calendars) != 3 {
		t.Fatalf("lost day snapshots: %+v", calendars)
	}
	if CalendarCacheTTL != 12*time.Hour || refreshInterval() != 12*time.Hour {
		t.Fatal("calendar must refresh every 12 hours")
	}
}

type testTransport func(*http.Request) (*http.Response, error)

func (f testTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCalendarCollectionDoesNotCancelBackgroundAnalysis(t *testing.T) {
	setupArchiveForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &calendarServer{ctx: ctx, cache: NewCalendarCache(filepath.Join(t.TempDir(), "calendar.json")), slots: make(chan struct{}, 3), dataDir: t.TempDir(), client: &http.Client{Transport: testTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"events":[],"data":{"tournaments":[],"categories":[]}}`))}, nil
	})}}
	background, finish, ok := s.beginBackground()
	if !ok {
		t.Fatal("background did not start")
	}
	defer finish()
	if _, err := s.collect(ctx, "SUPERBET", polishDay(time.Now()), true); err != nil {
		t.Fatal(err)
	}
	if background.Err() != nil {
		t.Fatal("calendar cancelled ongoing analysis")
	}
}

func TestSavedCalendarRunReproducesCountersAndHistory(t *testing.T) {
	dir := t.TempDir()
	store, err := openResearchStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()
	s := &calendarServer{research: store}
	now := time.Now().UTC()
	h := &PlayerStatHistory{ID: 1, Matches: 10, MetricValues: map[CanonicalMarket][]int{MarketTackles: {0, 1}}, Minutes: []int{90, 80}, Dates: []string{"09.10", "01.10"}}
	bet := AnalyzedBet{PlayerID: 1, Bookmaker: BookmakerSTS, MarketType: MarketTackles, Line: .5, BookmakerOdds: 2, Verdict: "VALUE", FairModel: &FairModelDetails{Probability: .5}}
	duplicate := bet
	duplicate.BookmakerOdds = 3
	trap := bet
	trap.Line = 1.5
	trap.Verdict = "TRAP"
	job := AnalysisJob{ID: strings.Repeat("a", 24), Origin: "automatic", Match: "A vs B", FinishedAt: now, Request: AnalysisRequest{MatchLimit: 10}, Result: []AnalyzedBet{bet, duplicate, trap}}
	e := &predictionEvidence{Fixture: statsFixture{ID: "123", StartTime: now.Add(time.Hour)}, HomePlayers: []*PlayerStatHistory{h}, OddsSources: map[string]any{"STS": map[string]any{"eventId": "456", "observedAt": now, "expiresAt": now.Add(15 * time.Minute)}}}
	if err := s.savePredictions(job, e); err != nil {
		t.Fatal(err)
	}
	summaries, err := store.summaries()
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 {
		t.Fatalf("wrong summaries %+v", summaries)
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.SetPathValue("id", summaries[0].RunID)
	w := httptest.NewRecorder()
	s.automaticRun(w, r)
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	var bets []AnalyzedBet
	if err := json.Unmarshal(w.Body.Bytes(), &bets); err != nil {
		t.Fatal(err)
	}
	value, traps := 0, 0
	for _, bet := range bets {
		if bet.Verdict == "VALUE" {
			value++
			if bet.BookmakerOdds != 3 {
				t.Fatal("not best duplicate price")
			}
		}
		if bet.Verdict == "TRAP" {
			traps++
		}
		if len(bet.SampleValues) != 2 || bet.SampleMinutes[0] != 90 {
			t.Fatal("lost archived history")
		}
	}
	if value != summaries[0].Value || traps != summaries[0].Traps || len(bets) != summaries[0].Total {
		t.Fatal(fmt.Sprintf("counters mismatch %+v vs %+v", summaries, bets))
	}
}
