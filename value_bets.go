// All desktop Go code is kept in this file; the VPS server is server/value_bets.go.
package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
)

// Uruchomienie aplikacji Wails.
//
//go:embed all:frontend
var assets embed.FS

func main() {
	app := NewApp()

	err := wails.Run(&options.App{
		Title:  "ValueBets 3.0",
		Width:  1920,
		Height: 1080,
		AssetServer: &assetserver.Options{
			Assets: assets,
		},
		OnStartup:  app.startup,
		OnShutdown: app.shutdown,
		Bind: []interface{}{
			app,
		},
	})

	if err != nil {
		println("Error:", err.Error())
	}
}

// API response types and local personal data.
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

type MatchHistoryEntry struct {
	Timestamp   string `json:"timestamp"`
	Title       string `json:"title"`
	SbURL       string `json:"sbUrl"`
	BetclicURL  string `json:"betclicUrl"`
	StsURL      string `json:"stsUrl"`
	HomeStatsID string `json:"homeStatsId"`
	AwayStatsID string `json:"awayStatsId"`
	SampleLimit int    `json:"sampleLimit,omitempty"`
}

type MyBetItem struct {
	ID                   string             `json:"id"`
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

type App struct {
	ctx              context.Context
	analysisMu       sync.Mutex
	analysisActive   bool
	analysisProgress AnalysisJob
}

var cacheMutex sync.Mutex

func NewApp() *App                         { return &App{} }
func (a *App) startup(ctx context.Context) { a.ctx = ctx }
func (a *App) shutdown(context.Context)    {}

func getCacheFilePath(filename string) string {
	if exePath, err := os.Executable(); err == nil {
		dir := filepath.Join(filepath.Dir(exePath), "data")
		candidate := filepath.Join(dir, filename)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	path := filepath.Join("data", filename)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		log.Printf("[CACHE] Nie można utworzyć katalogu danych: %v", err)
	}
	return path
}

func (a *App) LoadMatchHistory() []MatchHistoryEntry {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("match_history.json")
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[HISTORY] Brak pliku historii (%s)", path)
		return []MatchHistoryEntry{}
	}
	var list []MatchHistoryEntry
	_ = json.Unmarshal(data, &list)
	log.Printf("[HISTORY] Załadowano %d wpisów historii z pliku %s", len(list), path)
	return list
}

func (a *App) SaveMatchHistory(entry MatchHistoryEntry) []MatchHistoryEntry {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("match_history.json")
	var list []MatchHistoryEntry
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &list)
	}

	entry.Timestamp = time.Now().Format("2006-01-02 15:04:05")

	var updated []MatchHistoryEntry
	updated = append(updated, entry)
	for _, old := range list {
		if old.SbURL == entry.SbURL && old.BetclicURL == entry.BetclicURL && old.StsURL == entry.StsURL {
			continue
		}
		updated = append(updated, old)
		if len(updated) >= 50 {
			break
		}
	}

	out, _ := json.MarshalIndent(updated, "", "  ")
	_ = os.WriteFile(path, out, 0644)
	log.Printf("[HISTORY] Zapisano historię dla: '%s' (łączna ilość wpisów: %d)", entry.Title, len(updated))
	return updated
}

func (a *App) DeleteMatchHistory(index int) []MatchHistoryEntry {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("match_history.json")
	var list []MatchHistoryEntry
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &list)
	}

	if index < 0 || index >= len(list) {
		log.Printf("[HISTORY] Nieprawidłowy indeks do usunięcia: %d", index)
		return list
	}

	log.Printf("[HISTORY] Usuwanie z historii wpisu #%d: '%s'", index, list[index].Title)
	updated := append(list[:index], list[index+1:]...)

	out, _ := json.MarshalIndent(updated, "", "  ")
	_ = os.WriteFile(path, out, 0644)
	return updated
}

func (a *App) LoadMyBets() []MyBetItem {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("bets_cache.json")
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[MYBETS] Brak pliku cache zakładów (%s)", path)
		return []MyBetItem{}
	}

	var list []MyBetItem
	if err := json.Unmarshal(data, &list); err != nil {
		log.Printf("[MYBETS] Błąd unmarshalu %s: %v", path, err)
		return []MyBetItem{}
	}
	log.Printf("[MYBETS] Załadowano %d zapisanych zakładów użytkownika", len(list))
	return list
}

func (a *App) SaveMyBets(bets []MyBetItem) bool {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("bets_cache.json")
	data, err := json.MarshalIndent(bets, "", "  ")
	if err != nil {
		log.Printf("[MYBETS] Błąd serializacji zakładek: %v", err)
		return false
	}

	err = os.WriteFile(path, data, 0644)
	log.Printf("[MYBETS] Zapisano %d zakładów do %s (sukces: %v)", len(bets), path, err == nil)
	return err == nil
}

type SuperbetCalendarEvent struct {
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

type SuperbetCalendar struct {
	Date      string                  `json:"date"`
	Events    []SuperbetCalendarEvent `json:"events"`
	Warnings  []string                `json:"warnings"`
	FetchedAt string                  `json:"fetchedAt,omitempty"`
}

func (a *App) GetSuperbetCalendar(date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), "SUPERBET", date, false)
}

func (a *App) GetSTSCalendar(date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), "STS", date, false)
}

func (a *App) calendarContext() context.Context {
	ctx := a.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx
}

func (a *App) GetBetclicCalendar(date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), "BETCLIC", date, false)
}

func (a *App) RefreshBookmakerCalendar(bookmaker, date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), strings.ToUpper(strings.TrimSpace(bookmaker)), date, true)
}

func (a *App) getBookmakerCalendar(ctx context.Context, bookmaker, date string, force bool) (*SuperbetCalendar, error) {
	if _, err := time.Parse("2006-01-02", date); err != nil {
		return nil, err
	}
	if bookmaker != "STS" && bookmaker != "BETCLIC" && bookmaker != "SUPERBET" {
		return nil, fmt.Errorf("nieobsługiwany bukmacher kalendarza: %s", bookmaker)
	}
	var calendar SuperbetCalendar
	query := url.Values{"bookmaker": {bookmaker}, "date": {date}}
	method, path := http.MethodGet, "/api/v1/calendars?"+query.Encode()
	var body []byte
	if force {
		method, path = http.MethodPost, "/api/v1/calendars/refresh"
		body, _ = json.Marshal(map[string]string{"bookmaker": bookmaker, "date": date})
	}
	if err := a.serverRequest(ctx, method, path, body, &calendar); err != nil {
		return nil, err
	}
	return &calendar, nil
}

// The desktop connects to the user's VPS automatically, without configuration.
const serverBaseURL = "http://130.61.39.101:8090"

var calendarAPIClient = &http.Client{
	Timeout:       180 * time.Second,
	CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
}

func (a *App) serverRequest(ctx context.Context, method, path string, body []byte, target any) error {
	req, err := http.NewRequestWithContext(ctx, method, serverBaseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := calendarAPIClient.Do(req)
	if err != nil {
		return fmt.Errorf("połączenie z API: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (32<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 32<<20 {
		return fmt.Errorf("odpowiedź API przekracza 32 MB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var failure struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(raw, &failure)
		if failure.Error == "" {
			failure.Error = resp.Status
		}
		return fmt.Errorf("API HTTP %d: %s", resp.StatusCode, failure.Error)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("niepoprawna odpowiedź API: %w", err)
	}
	return nil
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

func (a *App) SaveTeamSofaID(slug, sofaID string) bool {
	body, _ := json.Marshal(map[string]string{"slug": slug, "sofaId": sofaID})
	var ok bool
	if err := a.serverRequest(a.calendarContext(), http.MethodPost, "/api/v1/teams/mapping", body, &ok); err != nil {
		log.Printf("[VPS] Zapis ID drużyny: %v", err)
		return false
	}
	return ok
}

func (a *App) DetectTeamsFromURL(rawURL string) TeamDetectionResult {
	return a.detectTeams(map[string]string{"url": rawURL})
}
func (a *App) DetectCalendarTeams(home, away string) TeamDetectionResult {
	return a.detectTeams(map[string]string{"home": home, "away": away})
}
func (a *App) detectTeams(query map[string]string) TeamDetectionResult {
	body, _ := json.Marshal(query)
	var result TeamDetectionResult
	if err := a.serverRequest(a.calendarContext(), http.MethodPost, "/api/v1/teams/detect", body, &result); err != nil {
		log.Printf("[VPS] Rozpoznawanie drużyn: %v", err)
	}
	return result
}
func (a *App) GetAvailableTournaments(homeID, awayID string) ([]LeagueOption, error) {
	var leagues []LeagueOption
	query := url.Values{"homeId": {homeID}, "awayId": {awayID}}
	err := a.serverRequest(a.calendarContext(), http.MethodGet, "/api/v1/tournaments?"+query.Encode(), nil, &leagues)
	return leagues, err
}

func (a *App) setAnalysisProgress(job AnalysisJob) {
	a.analysisMu.Lock()
	defer a.analysisMu.Unlock()
	job.Result = nil
	a.analysisProgress = job
}
func (a *App) GetAnalysisProgress() AnalysisJob {
	a.analysisMu.Lock()
	defer a.analysisMu.Unlock()
	job := a.analysisProgress
	job.Warnings = append([]string{}, job.Warnings...)
	return job
}

// Wails waits locally while the VPS performs an independent asynchronous job.
// Each poll has a short timeout; no long-lived HTTP analysis request is needed.
func (a *App) AnalyzeThreeBookmakers(sb, betclic, sts, home, away string, limit int, tournaments string) (result []AnalyzedBet, err error) {
	a.analysisMu.Lock()
	if a.analysisActive {
		a.analysisMu.Unlock()
		return nil, fmt.Errorf("analiza jest już uruchomiona")
	}
	a.analysisActive = true
	a.analysisProgress = AnalysisJob{Status: "queued", Stage: "sending", Message: "Wysyłanie zlecenia na VPS", Warnings: []string{}}
	a.analysisMu.Unlock()
	defer func() {
		a.analysisMu.Lock()
		defer a.analysisMu.Unlock()
		a.analysisActive = false
		if err != nil {
			a.analysisProgress.Status = "failed"
			a.analysisProgress.Stage = "failed"
			a.analysisProgress.Error = err.Error()
			a.analysisProgress.Message = err.Error()
		}
	}()
	ctx, cancel := context.WithTimeout(a.calendarContext(), 23*time.Minute)
	defer cancel()
	body, _ := json.Marshal(AnalysisRequest{SuperbetURL: sb, BetclicURL: betclic, STSURL: sts, HomeID: home, AwayID: away, MatchLimit: limit, TournamentIDs: tournaments})
	var job AnalysisJob
	sendCtx, sendCancel := context.WithTimeout(ctx, 20*time.Second)
	err = a.serverRequest(sendCtx, http.MethodPost, "/api/v1/analyses", body, &job)
	sendCancel()
	if err != nil {
		return nil, err
	}
	if job.ID == "" {
		return nil, fmt.Errorf("VPS nie zwrócił ID zlecenia")
	}
	a.setAnalysisProgress(job)
	id := job.ID
	failures := 0
	for {
		timer := time.NewTimer(1500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("oczekiwanie na analizę %s: %w", id, ctx.Err())
		case <-timer.C:
		}
		pollCtx, pollCancel := context.WithTimeout(ctx, 20*time.Second)
		err = a.serverRequest(pollCtx, http.MethodGet, "/api/v1/analyses/"+url.PathEscape(id), nil, &job)
		pollCancel()
		if err != nil {
			failures++
			state := a.GetAnalysisProgress()
			state.Message = "Oczekiwanie na połączenie z VPS — zlecenie " + id
			a.setAnalysisProgress(state)
			if failures >= 4 {
				return nil, fmt.Errorf("nie udało się odczytać analizy %s: %w", id, err)
			}
			continue
		}
		failures = 0
		a.setAnalysisProgress(job)
		switch job.Status {
		case "completed":
			if job.Result == nil {
				return []AnalyzedBet{}, nil
			}
			return job.Result, nil
		case "failed":
			return nil, fmt.Errorf("%s", job.Error)
		}
	}
}
