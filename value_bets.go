// Desktop client. The analysis server lives in server/value_bets.go.
package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"github.com/gorilla/websocket"
	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
	wailsruntime "github.com/wailsapp/wails/v2/pkg/runtime"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"
)

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
	AnalysisID              string             `json:"analysisId,omitempty"`
	PlayerID                int                `json:"playerId,omitempty"`
	SampleEventIDs          []int              `json:"sampleEventIds,omitempty"`
	HistoryNotes            []string           `json:"historyNotes,omitempty"`
	SampleRatings           []*float64         `json:"sampleRatings"`
	FairModel               *FairModelDetails  `json:"fairModel,omitempty"`
	ID                      string             `json:"id"`
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

type App struct {
	sofaMu               sync.Mutex
	sofaRecovery         *sofaCaptchaRecovery
	sofaBrowserState     SofaBrowserState
	sofaBrowserAgent     string
	sofaBrowserToken     string
	sofaBrowserPreferred string
	sofaBrowserLast      time.Time
	sofaBrowserMode      bool
	calendarDiskMu       sync.Mutex
	calendarDiskOnce     sync.Once
	calendarDisk         map[string]clientCalendarCacheEntry
	clientMu             sync.Mutex
	clientID             string
	relayOnce            sync.Once
	relayCancel          context.CancelFunc
	relayConnected       bool
	relayChanged         chan struct{}
	ctx                  context.Context
	analysisMu           sync.Mutex
	analysisActive       bool
	analysisCancel       context.CancelFunc
	analysisProgress     AnalysisJob
	detectionMu          sync.Mutex
	detectionCancel      context.CancelFunc
	detectionRevision    uint64
}

var cacheMutex sync.Mutex

func NewApp() *App                         { return &App{} }
func (a *App) startup(ctx context.Context) { a.ctx = ctx }
func (a *App) shutdown(context.Context) {
	a.CancelSofaCaptcha()
	a.clientMu.Lock()
	if a.relayCancel != nil {
		a.relayCancel()
	}
	a.clientMu.Unlock()
	a.analysisMu.Lock()
	if a.analysisCancel != nil {
		a.analysisCancel()
	}
	a.analysisMu.Unlock()
	a.detectionMu.Lock()
	if a.detectionCancel != nil {
		a.detectionCancel()
	}
	a.detectionMu.Unlock()
}
func getCacheFilePath(filename string) string {
	if exePath, err := os.Executable(); err == nil {
		dir := filepath.Dir(exePath)
		candidate := filepath.Join(dir, filename)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return filename
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

func (a *App) GetSuperbetCalendar(date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), "SUPERBET", date, false)
}

func (a *App) GetSTSCalendar(date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), "STS", date, false)
}

func (a *App) GetBetclicCalendar(date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), "BETCLIC", date, false)
}

func (a *App) calendarContext() context.Context {
	ctx := a.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx
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
		if cached, ok := a.cachedLocalCalendar(bookmaker, date); ok {
			cached.Warnings = append(cached.Warnings, "Kalendarz z lokalnego dysku; brak połączenia z serwerem: "+err.Error())
			for i := range cached.Events {
				cached.Events[i].AnalysisEligibility = &AnalysisEligibility{Status: "error", Reason: "brak połączenia z serwerem; kwalifikacja wymaga ponownego sprawdzenia", Links: map[string]string{}}
			}
			return cached, nil
		}
		return nil, err
	}
	a.saveLocalCalendar(bookmaker, date, &calendar)
	return &calendar, nil
}

var calendarAPIClient = &http.Client{
	Timeout:       8 * time.Minute,
	CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
}

func (a *App) serverRequest(ctx context.Context, method, path string, body []byte, target any) error {
	clientID, err := a.sessionID()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, serverBaseURL()+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-ValueBets-Client", clientID)
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

func (a *App) SaveTeamSofaID(slug, sofaID string) bool {
	body, _ := json.Marshal(map[string]string{"slug": slug, "sofaId": sofaID})
	var ok bool
	if err := a.serverRequest(a.calendarContext(), http.MethodPost, "/api/v1/teams/mapping", body, &ok); err != nil {
		log.Printf("[API] Zapis ID drużyny: %v", err)
		return false
	}
	return ok
}

func (a *App) DetectTeamsFromURL(rawURL string) (TeamDetectionResult, error) {
	return a.detectTeams(map[string]string{"url": rawURL})
}
func (a *App) DetectCalendarTeams(home, away string) (TeamDetectionResult, error) {
	return a.detectTeams(map[string]string{"home": home, "away": away})
}
func (a *App) DetectCalendarEventTeams(home, away, matchURL string) (TeamDetectionResult, error) {
	return a.detectTeams(map[string]string{"home": home, "away": away, "url": matchURL})
}
func (a *App) detectTeams(query map[string]string) (TeamDetectionResult, error) {
	ctx, cancel := context.WithTimeout(a.calendarContext(), 7*time.Minute)
	defer cancel()
	if err := a.ensureSofaRelay(ctx); err != nil {
		return TeamDetectionResult{}, err
	}
	a.detectionMu.Lock()
	if a.detectionCancel != nil {
		a.detectionCancel()
	}
	a.detectionRevision++
	revision := a.detectionRevision
	a.detectionCancel = cancel
	a.detectionMu.Unlock()
	defer func() {
		cancel()
		a.detectionMu.Lock()
		if a.detectionRevision == revision {
			a.detectionCancel = nil
		}
		a.detectionMu.Unlock()
	}()
	body, _ := json.Marshal(query)
	var result TeamDetectionResult
	if err := a.serverRequest(ctx, http.MethodPost, "/api/v1/teams/detect", body, &result); err != nil {
		log.Printf("[API] Rozpoznawanie drużyn: %v", err)
		return result, fmt.Errorf("detekcja SofaScore przez komputer klienta: %w", err)
	}
	return result, nil
}

func (a *App) GetAvailableTournaments(homeID, awayID string) ([]LeagueOption, error) {
	if err := a.ensureSofaRelay(a.calendarContext()); err != nil {
		return nil, err
	}
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

// Wails waits locally while the server performs an independent asynchronous job.
// Each poll has a short timeout; no long-lived HTTP analysis request is needed.
func (a *App) CancelAnalysis() error {
	a.CancelSofaCaptcha()
	a.analysisMu.Lock()
	id, cancel := a.analysisProgress.ID, a.analysisCancel
	a.analysisMu.Unlock()
	if id == "" || cancel == nil {
		return fmt.Errorf("brak aktywnego zlecenia do anulowania")
	}
	ctx, stop := context.WithTimeout(a.calendarContext(), 10*time.Second)
	defer stop()
	var accepted bool
	if err := a.serverRequest(ctx, http.MethodPost, "/api/v1/analyses/"+url.PathEscape(id)+"/cancel", nil, &accepted); err != nil {
		return err
	}
	if !accepted {
		return fmt.Errorf("serwer nie potwierdził anulowania")
	}
	cancel()
	return nil
}

func (a *App) AnalyzeThreeBookmakers(sb, betclic, sts, home, away string, limit int, tournaments string) (result []AnalyzedBet, err error) {
	a.analysisMu.Lock()
	if a.analysisActive {
		a.analysisMu.Unlock()
		return nil, fmt.Errorf("analiza jest już uruchomiona")
	}
	a.analysisActive = true
	a.analysisProgress = AnalysisJob{Status: "queued", Stage: "sending", Message: "Wysyłanie zlecenia na serwer", Warnings: []string{}}
	a.analysisMu.Unlock()
	defer func() {
		a.analysisMu.Lock()
		defer a.analysisMu.Unlock()
		a.analysisActive = false
		a.analysisCancel = nil
		if err != nil {
			a.analysisProgress.Status = "failed"
			a.analysisProgress.Stage = "failed"
			a.analysisProgress.Error = err.Error()
			a.analysisProgress.Message = err.Error()
			if errors.Is(err, context.Canceled) {
				a.analysisProgress.Message = "Analiza anulowana"
			}
		}
	}()
	ctx, cancel := context.WithTimeout(a.calendarContext(), 23*time.Minute)
	defer cancel()
	a.analysisMu.Lock()
	a.analysisCancel = cancel
	a.analysisMu.Unlock()
	body, _ := json.Marshal(AnalysisRequest{SuperbetURL: sb, BetclicURL: betclic, STSURL: sts, HomeID: home, AwayID: away, MatchLimit: limit, TournamentIDs: tournaments})
	var job AnalysisJob
	sendCtx, sendCancel := context.WithTimeout(ctx, 20*time.Second)
	err = a.serverRequest(sendCtx, http.MethodPost, "/api/v1/analyses", body, &job)
	sendCancel()
	if err != nil {
		return nil, err
	}
	if job.ID == "" {
		return nil, fmt.Errorf("Serwer nie zwrócił ID zlecenia")
	}
	a.setAnalysisProgress(job)
	id := job.ID
	lastRevision := 0
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
		var nextJob AnalysisJob
		err = a.serverRequest(pollCtx, http.MethodGet, "/api/v1/analyses/"+url.PathEscape(id)+"?afterRevision="+fmt.Sprint(lastRevision), nil, &nextJob)
		pollCancel()
		if err != nil {
			failures++
			state := a.GetAnalysisProgress()
			state.Message = "Oczekiwanie na połączenie z serwerem — zlecenie " + id
			a.setAnalysisProgress(state)
			if failures >= 4 {
				return nil, fmt.Errorf("nie udało się odczytać analizy %s: %w", id, err)
			}
			continue
		}
		failures = 0
		job = nextJob
		a.setAnalysisProgress(job)
		if job.ResultRevision > lastRevision {
			lastRevision = job.ResultRevision
			if (job.Status == "running" || job.Status == "failed") && len(job.Result) > 0 && a.ctx != nil {
				wailsruntime.EventsEmit(a.ctx, "analysis:players-ready", job)
			}
		}
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

// Coordinates are fetched on chart opening without changing the saved sample.
func (a *App) GetAnalysisShotmaps(analysisID string, playerID int, market, mode string, limit int) (map[string]any, error) {
	if err := a.ensureSofaRelay(a.calendarContext()); err != nil {
		return nil, err
	}
	if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(analysisID) || playerID <= 0 || limit < 0 || limit > 30 {
		return nil, fmt.Errorf("niepoprawna analiza lub zawodnik")
	}
	body, err := json.Marshal(map[string]any{"analysisId": analysisID, "playerId": playerID, "market": market, "mode": mode, "limit": limit})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 7*time.Minute)
	defer cancel()
	var result map[string]any
	err = a.serverRequest(ctx, http.MethodPost, "/api/v1/analyses/shotmaps", body, &result)
	return result, err
}

func (a *App) GetAnalysisSample(analysisID string, limit int) ([]AnalyzedBet, error) {
	if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(analysisID) || limit < 1 || limit > 30 {
		return nil, fmt.Errorf("niepoprawna analiza lub długość próbki")
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 30*time.Second)
	defer cancel()
	body, err := json.Marshal(map[string]int{"limit": limit})
	if err != nil {
		return nil, err
	}
	var result []AnalyzedBet
	err = a.serverRequest(ctx, http.MethodPost, "/api/v1/analyses/"+analysisID+"/sample", body, &result)
	return result, err
}

func (a *App) GetRelatedStats(analysisID string, playerID int, baseMarket, market string, limit int) (map[string]any, error) {
	if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(analysisID) || playerID <= 0 || limit < 1 || limit > 30 {
		return nil, fmt.Errorf("niepoprawna próbka")
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 20*time.Second)
	defer cancel()
	body, err := json.Marshal(map[string]any{"analysisId": analysisID, "playerId": playerID, "baseMarket": baseMarket, "market": market, "limit": limit})
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = a.serverRequest(ctx, http.MethodPost, "/api/v1/analyses/related", body, &result)
	return result, err
}

// Resolve the optional widget only after a chart is opened.
func (a *App) GetAnalysisLineupEvent(analysisID string) (map[string]any, error) {
	if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(analysisID) {
		return nil, fmt.Errorf("niepoprawna analiza")
	}
	if err := a.ensureSofaRelay(a.calendarContext()); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 7*time.Minute)
	defer cancel()
	var result map[string]any
	err := a.serverRequest(ctx, http.MethodGet, "/api/v1/analyses/"+analysisID+"/lineup-event", nil, &result)
	return result, err
}

// VALUE_BETS_SERVER_URL overrides the server address without rebuilding the client.
func serverBaseURL() string {
	if value := strings.TrimSpace(os.Getenv("VALUE_BETS_SERVER_URL")); value != "" {
		return strings.TrimRight(value, "/")
	}
	return "http://130.61.39.101:8090"
}

type SuperbetCalendar struct {
	Date      string                  `json:"date"`
	Events    []SuperbetCalendarEvent `json:"events"`
	Warnings  []string                `json:"warnings"`
	FetchedAt string                  `json:"fetchedAt,omitempty"`
}

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

func (a *App) PrepareCalendarMatch(superbet, betclic, sts string) (MatchPreparation, error) {
	ctx, cancel := context.WithTimeout(a.calendarContext(), 150*time.Second)
	defer cancel()
	body, _ := json.Marshal(AnalysisRequest{SuperbetURL: superbet, BetclicURL: betclic, STSURL: sts, MatchLimit: 10, TournamentIDs: "ALL"})
	var prepared MatchPreparation
	err := a.serverRequest(ctx, http.MethodPost, "/api/v1/markets/prepare", body, &prepared)
	return prepared, err
}

type clientCalendarCacheEntry struct {
	SavedAt  time.Time        `json:"savedAt"`
	Calendar SuperbetCalendar `json:"calendar"`
}

func (a *App) loadLocalCalendars() {
	a.calendarDiskOnce.Do(func() {
		a.calendarDisk = map[string]clientCalendarCacheEntry{}
		if raw, err := os.ReadFile(getCacheFilePath("client_bookmaker_calendar_cache.json")); err == nil {
			if err := json.Unmarshal(raw, &a.calendarDisk); err != nil {
				log.Printf("[CALENDAR-CLIENT-CACHE] READ_ERROR: %v", err)
			}
			if a.calendarDisk == nil {
				a.calendarDisk = map[string]clientCalendarCacheEntry{}
			}
		}
	})
}
func (a *App) cachedLocalCalendar(book, date string) (*SuperbetCalendar, bool) {
	a.calendarDiskMu.Lock()
	defer a.calendarDiskMu.Unlock()
	a.loadLocalCalendars()
	entry, ok := a.calendarDisk[serverBaseURL()+"|"+book+"|"+date]
	if !ok || time.Since(entry.SavedAt) < 0 || time.Since(entry.SavedAt) > 7*24*time.Hour {
		return nil, false
	}
	copy := entry.Calendar
	copy.Events = append([]SuperbetCalendarEvent(nil), copy.Events...)
	copy.Warnings = append([]string{}, copy.Warnings...)
	return &copy, true
}
func (a *App) saveLocalCalendar(book, date string, calendar *SuperbetCalendar) {
	a.calendarDiskMu.Lock()
	defer a.calendarDiskMu.Unlock()
	a.loadLocalCalendars()
	key := serverBaseURL() + "|" + book + "|" + date
	if previous, ok := a.calendarDisk[key]; ok {
		oldJSON, _ := json.Marshal(previous.Calendar)
		newJSON, _ := json.Marshal(calendar)
		if bytes.Equal(oldJSON, newJSON) {
			return
		}
	}
	copy := *calendar
	copy.Events = append([]SuperbetCalendarEvent(nil), calendar.Events...)
	copy.Warnings = append([]string{}, calendar.Warnings...)
	a.calendarDisk[key] = clientCalendarCacheEntry{SavedAt: time.Now().UTC(), Calendar: copy}
	for oldKey, entry := range a.calendarDisk {
		if time.Since(entry.SavedAt) > 7*24*time.Hour {
			delete(a.calendarDisk, oldKey)
		}
	}
	raw, err := json.Marshal(a.calendarDisk)
	if err != nil {
		log.Printf("[CALENDAR-CLIENT-CACHE] ENCODE_ERROR: %v", err)
		return
	}
	path := getCacheFilePath("client_bookmaker_calendar_cache.json")
	file, err := os.CreateTemp(filepath.Dir(path), ".calendar-client-*.tmp")
	if err == nil {
		defer os.Remove(file.Name())
		_, writeErr := file.Write(raw)
		err = errors.Join(writeErr, file.Sync(), file.Close())
		if err == nil {
			err = os.Rename(file.Name(), path)
		}
	}
	if err != nil {
		log.Printf("[CALENDAR-CLIENT-CACHE] WRITE_ERROR: %v", err)
	}
}

// The desktop opens one outbound WebSocket. SofaScore is fetched only here;
// A local browser debugging port is used only to recover blocked SofaScore calls.
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

func (a *App) sessionID() (string, error) {
	a.clientMu.Lock()
	defer a.clientMu.Unlock()
	if a.clientID == "" {
		raw := make([]byte, 32)
		if _, err := cryptorand.Read(raw); err != nil {
			return "", err
		}
		a.clientID = fmt.Sprintf("%x", raw)
	}
	return a.clientID, nil
}

func (a *App) ensureSofaRelay(parent context.Context) error {
	if _, err := a.sessionID(); err != nil {
		return err
	}
	a.relayOnce.Do(func() {
		ctx, cancel := context.WithCancel(a.calendarContext())
		a.clientMu.Lock()
		a.relayCancel = cancel
		a.relayChanged = make(chan struct{})
		a.clientMu.Unlock()
		go a.sofaRelayLoop(ctx)
	})
	ctx, cancel := context.WithTimeout(parent, 18*time.Second)
	defer cancel()
	for {
		a.clientMu.Lock()
		connected, changed := a.relayConnected, a.relayChanged
		a.clientMu.Unlock()
		if connected {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("brak połączenia klienta SofaScore z serwerem; zaktualizuj i uruchom obie części aplikacji: %w", ctx.Err())
		case <-changed:
		}
	}
}

func (a *App) relayState(connected bool) {
	a.clientMu.Lock()
	a.relayConnected = connected
	close(a.relayChanged)
	a.relayChanged = make(chan struct{})
	a.clientMu.Unlock()
}

func (a *App) sofaRelayLoop(ctx context.Context) {
	clientID, err := a.sessionID()
	if err != nil {
		return
	}
	transport, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(),
		tls_client.WithTimeoutSeconds(22), tls_client.WithClientProfile(profiles.Chrome_152),
		tls_client.WithCookieJar(tls_client.NewCookieJar()), tls_client.WithRandomTLSExtensionOrder(),
		tls_client.WithNotFollowRedirects())
	if err != nil {
		log.Printf("[SOFA-CLIENT] TLS: %v", err)
		return
	}
	defer transport.CloseIdleConnections()
	for ctx.Err() == nil {
		err := a.serveSofaRelay(ctx, clientID, transport)
		a.relayState(false)
		if ctx.Err() != nil {
			return
		}
		log.Printf("[SOFA-CLIENT] Połączenie z serwerem przerwane: %v", err)
		timer := time.NewTimer(3 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (a *App) serveSofaRelay(ctx context.Context, clientID string, transport tls_client.HttpClient) error {
	address, err := url.Parse(serverBaseURL())
	if err != nil {
		return err
	}
	switch address.Scheme {
	case "http":
		address.Scheme = "ws"
	case "https":
		address.Scheme = "wss"
	default:
		return fmt.Errorf("niepoprawny adres serwera")
	}
	address.Path = strings.TrimRight(address.Path, "/") + "/api/v1/client/sofa"
	address.RawQuery, address.Fragment = "", ""
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second, Proxy: http.ProxyFromEnvironment}
	headers := http.Header{"X-ValueBets-Client": {clientID}}
	conn, response, err := dialer.DialContext(ctx, address.String(), headers)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return err
	}
	defer conn.Close()
	connectionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopClose := context.AfterFunc(connectionCtx, func() { conn.Close() })
	defer stopClose()
	conn.SetReadLimit(64 << 10)
	conn.SetReadDeadline(time.Now().Add(75 * time.Second))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(75 * time.Second)) })
	a.relayState(true)
	log.Printf("[SOFA-CLIENT] Połączono; żądania SofaScore wychodzą z komputera klienta")
	go func() {
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-connectionCtx.Done():
				return
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	var writes sync.Mutex
	slots := make(chan struct{}, 4)
	var workers sync.WaitGroup
	var pendingMu sync.Mutex
	pending := map[string]context.CancelFunc{}
	defer func() { cancel(); workers.Wait() }()
	for {
		var request sofaRelayRequest
		if err := conn.ReadJSON(&request); err != nil {
			return err
		}
		if request.Cancel {
			pendingMu.Lock()
			if cancel := pending[request.ID]; cancel != nil {
				cancel()
			}
			pendingMu.Unlock()
			continue
		}
		requestCtx, requestCancel := context.WithCancel(connectionCtx)
		pendingMu.Lock()
		pending[request.ID] = requestCancel
		pendingMu.Unlock()
		workers.Add(1)
		go func(request sofaRelayRequest, requestCtx context.Context, requestCancel context.CancelFunc) {
			defer workers.Done()
			defer func() { requestCancel(); pendingMu.Lock(); delete(pending, request.ID); pendingMu.Unlock() }()
			select {
			case slots <- struct{}{}:
			case <-requestCtx.Done():
				return
			}
			defer func() { <-slots }()
			response := a.fetchLocalSofa(requestCtx, transport, request)
			writes.Lock()
			defer writes.Unlock()
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteJSON(response); err != nil {
				cancel()
			}
		}(request, requestCtx, requestCancel)
	}
}

func (a *App) fetchLocalSofaHTTP(parent context.Context, client tls_client.HttpClient, request sofaRelayRequest) sofaRelayResponse {
	result := sofaRelayResponse{ID: request.ID}
	endpoint, err := url.Parse(request.Endpoint)
	allowed := strings.HasPrefix(request.Endpoint, "/search/") || strings.HasPrefix(request.Endpoint, "/team/") || strings.HasPrefix(request.Endpoint, "/event/") || strings.HasPrefix(request.Endpoint, "/player/") || strings.HasPrefix(request.Endpoint, "/unique-tournament/")
	if err != nil || !allowed || len(request.Endpoint) > 4096 || endpoint.IsAbs() || endpoint.Host != "" || endpoint.Fragment != "" {
		result.Error = "odrzucono niepoprawny endpoint SofaScore"
		return result
	}
	ctx, cancel := context.WithTimeout(parent, 24*time.Second)
	defer cancel()
	req, err := fhttp.NewRequestWithContext(ctx, http.MethodGet, "https://www.sofascore.com/api/v1"+request.Endpoint, nil)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	req.Header = fhttp.Header{
		"user-agent": {"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"},
		"accept":     {"application/json"}, "accept-language": {"en-US,en;q=0.9"}, "referer": {"https://www.sofascore.com/"},
		"sec-fetch-site": {"same-origin"}, "sec-fetch-mode": {"cors"}, "sec-fetch-dest": {"empty"},
		fhttp.HeaderOrderKey: {"user-agent", "accept", "accept-language", "referer", "sec-fetch-site", "sec-fetch-mode", "sec-fetch-dest", "x-requested-with", "x-captcha", "cookie"},
	}
	if value := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_REQUESTED_WITH")); value != "" {
		req.Header.Set("X-Requested-With", value)
	}
	a.sofaMu.Lock()
	agent, token := a.sofaBrowserAgent, a.sofaBrowserToken
	a.sofaMu.Unlock()
	if agent != "" {
		req.Header.Set("User-Agent", agent)
	}
	if token == "" && agent == "" {
		token = strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_CAPTCHA"))
	}
	if token != "" {
		req.Header.Set("X-Captcha", token)
		client.SetCookies(req.URL, []*fhttp.Cookie{{Name: "sofa_captcha", Value: token, Path: "/", Secure: true}})
	}
	response, err := client.Do(req)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer response.Body.Close()
	result.Status = response.StatusCode
	if response.StatusCode != http.StatusOK {
		return result
	}
	result.Body, err = io.ReadAll(io.LimitReader(response.Body, (32<<20)+1))
	if err != nil {
		result.Error = err.Error()
		result.Body = nil
	} else if len(result.Body) > 32<<20 {
		result.Error = "odpowiedź SofaScore przekracza 32 MB"
		result.Body = nil
	} else if !json.Valid(result.Body) {
		result.Error = "SofaScore nie zwrócił JSON"
		result.Body = nil
	}
	log.Printf("[SOFA-CLIENT] endpoint=%q status=%d bytes=%d", request.Endpoint, result.Status, len(result.Body))
	return result
}

// Browser recovery is client-only. No browser cookies or debugging addresses
// are sent to the analysis server or persisted to disk.
type SofaBrowserState struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Address string `json:"address"`
	Pending bool   `json:"pending"`
	Browser string `json:"browser"`
}

type sofaCaptchaRecovery struct {
	done          chan struct{}
	retry         chan struct{}
	cancel        context.CancelFunc
	endpoint      string
	response      sofaRelayResponse
	err           error
	attemptCancel context.CancelFunc
}

func sofaBrowserAddress(browser string) (string, error) {
	raw := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_BROWSER"))
	if browser == "chrome" {
		raw = strings.TrimSpace(os.Getenv("VALUE_BETS_CHROME_BROWSER"))
		if raw == "" {
			raw = "http://127.0.0.1:9223"
		}
	}
	if raw == "" {
		raw = "http://127.0.0.1:9222"
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", fmt.Errorf("DevTools: wymagany lokalny adres HTTP z portem, np. http://127.0.0.1:9222")
	}
	host := u.Hostname()
	if host == "localhost" {
		host = "127.0.0.1"
	}
	ip, port := net.ParseIP(host), u.Port()
	number, err := strconv.Atoi(port)
	if ip == nil || !ip.IsLoopback() || err != nil || number < 1 || number > 65535 {
		return "", fmt.Errorf("DevTools SofaScore musi używać adresu loopback i poprawnego portu")
	}
	return "http://" + net.JoinHostPort(host, port), nil
}

func (a *App) GetSofaBrowserState() SofaBrowserState {
	a.sofaMu.Lock()
	state := a.sofaBrowserState
	state.Browser = a.sofaBrowserPreferred
	a.sofaMu.Unlock()
	if state.Browser == "" {
		state.Browser = "floorp"
	}
	if state.Status == "" {
		state.Status = "idle"
	}
	if state.Address == "" {
		state.Address, _ = sofaBrowserAddress(state.Browser)
	}
	return state
}

func (a *App) setSofaBrowserState(status, message string, pending bool) {
	a.sofaMu.Lock()
	browser := a.sofaBrowserPreferred
	if browser == "" {
		browser = "floorp"
	}
	address, _ := sofaBrowserAddress(browser)
	state := SofaBrowserState{Status: status, Message: message, Address: address, Pending: pending, Browser: browser}
	a.sofaBrowserState = state
	a.sofaMu.Unlock()
	if a.ctx != nil && a.ctx.Err() == nil {
		wailsruntime.EventsEmit(a.ctx, "sofa-browser-state", state)
	}
	log.Printf("[SOFA-CAPTCHA] state=%s %s", status, message)
}

func (a *App) SetSofaBrowser(browser string) error {
	if browser != "floorp" && browser != "chrome" {
		return fmt.Errorf("wybierz Floorp albo Chrome")
	}
	if _, err := sofaBrowserAddress(browser); err != nil {
		return err
	}
	a.sofaMu.Lock()
	a.sofaBrowserPreferred = browser
	a.sofaBrowserMode = false
	recovery := a.sofaRecovery
	var stopAttempt context.CancelFunc
	if recovery != nil {
		stopAttempt = recovery.attemptCancel
	}
	a.sofaMu.Unlock()
	if recovery != nil {
		if stopAttempt != nil {
			stopAttempt()
		}
		a.setSofaBrowserState("waiting", "Zmieniono przeglądarkę. Łączenie z jej otwartą kartą SofaScore…", true)
		select {
		case recovery.retry <- struct{}{}:
		default:
		}
	} else {
		a.setSofaBrowserState("idle", "", false)
	}
	return nil
}

func (a *App) ContinueSofaCaptcha() error {
	a.sofaMu.Lock()
	recovery := a.sofaRecovery
	a.sofaMu.Unlock()
	if recovery == nil {
		return fmt.Errorf("brak oczekiwania na captchę; otwórz ponownie wykres zawodnika")
	}
	select {
	case recovery.retry <- struct{}{}:
	default:
	}
	return nil
}

func (a *App) CancelSofaCaptcha() {
	a.sofaMu.Lock()
	if a.sofaRecovery != nil {
		a.sofaRecovery.cancel()
	}
	a.sofaMu.Unlock()
}

func (a *App) fetchLocalSofa(ctx context.Context, client tls_client.HttpClient, request sofaRelayRequest) sofaRelayResponse {
	for {
		if err := ctx.Err(); err != nil {
			return sofaRelayResponse{ID: request.ID, Error: err.Error()}
		}
		// Join an existing manual challenge before issuing another blocked request.
		a.sofaMu.Lock()
		waiting := a.sofaRecovery
		useBrowser := a.sofaBrowserMode
		a.sofaMu.Unlock()
		if waiting != nil {
			if err := waitSofaRecovery(ctx, waiting); err != nil {
				return sofaRelayResponse{ID: request.ID, Error: err.Error()}
			}
		}
		var response sofaRelayResponse
		if !useBrowser {
			response = a.fetchLocalSofaHTTP(ctx, client, request)
			if response.Status != http.StatusForbidden && !(response.Status == http.StatusOK && response.Error == "SofaScore nie zwrócił JSON") {
				return response
			}
		}
		a.sofaMu.Lock()
		recovery := a.sofaRecovery
		if recovery == nil {
			work, cancel := context.WithTimeout(a.calendarContext(), 5*time.Minute)
			recovery = &sofaCaptchaRecovery{done: make(chan struct{}), retry: make(chan struct{}, 1), cancel: cancel, endpoint: request.Endpoint}
			a.sofaRecovery = recovery
			go a.recoverSofaBrowser(work, client, recovery)
		}
		a.sofaMu.Unlock()
		if err := waitSofaRecovery(ctx, recovery); err != nil {
			return sofaRelayResponse{ID: request.ID, Error: err.Error()}
		}
		if request.Endpoint == recovery.endpoint {
			response = recovery.response
			response.ID = request.ID
			return response
		}
		a.sofaMu.Lock()
		useBrowser = a.sofaBrowserMode
		a.sofaMu.Unlock()
		if useBrowser {
			continue
		}
		// The recovery fetched another endpoint. Retry this one with the refreshed
		// session. If TLS is still rejected, use the existing browser tab for this GET.
		response = a.fetchLocalSofaHTTP(ctx, client, request)
		if response.Status == http.StatusForbidden {
			continue
		}
		return response
	}
}

func waitSofaRecovery(ctx context.Context, recovery *sofaCaptchaRecovery) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-recovery.done:
		return recovery.err
	}
}

func (a *App) recoverSofaBrowser(ctx context.Context, client tls_client.HttpClient, recovery *sofaCaptchaRecovery) {
	defer recovery.cancel()
	defer func() {
		a.sofaMu.Lock()
		if a.sofaRecovery == recovery {
			a.sofaRecovery = nil
		}
		close(recovery.done)
		a.sofaMu.Unlock()
	}()
	a.setSofaBrowserState("connecting", "SofaScore blokuje HTTP. Łączenie z otwartą kartą w wybranej przeglądarce…", true)
	for {
		selected := a.GetSofaBrowserState().Browser
		attempt, cancelAttempt := context.WithTimeout(ctx, 45*time.Second)
		a.sofaMu.Lock()
		recovery.attemptCancel = cancelAttempt
		a.sofaMu.Unlock()
		response, err := a.trySofaBrowser(attempt, client, recovery.endpoint)
		cancelAttempt()
		a.sofaMu.Lock()
		recovery.attemptCancel = nil
		a.sofaMu.Unlock()
		if ctx.Err() == nil && selected != a.GetSofaBrowserState().Browser {
			continue
		}
		if err == nil && response.Status != http.StatusForbidden && response.Status != http.StatusUnauthorized {
			recovery.response = response
			message := "Sesja SofaScore odświeżona; wznowiono żądania."
			if response.Status != http.StatusOK {
				message = fmt.Sprintf("Przeglądarka odpowiedziała HTTP %d; sprawdź uwagi tego żądania.", response.Status)
			}
			a.setSofaBrowserState("ready", message, false)
			return
		}
		message := "Odśwież SofaScore w wybranej przeglądarce i rozwiąż captchę ręcznie. Następnie kliknij „Rozwiązane / ponów”."
		if err == nil {
			message = fmt.Sprintf("Połączenie z kartą działa, ale API SofaScore zwraca HTTP %d. Odśwież stronę i rozwiąż ewentualną captchę, potem kliknij „Rozwiązane / ponów”.", response.Status)
		}
		if err != nil {
			message = err.Error() + ". Sprawdź przeglądarkę i jej port, otwórz https://www.sofascore.com/ i kliknij „Rozwiązane / ponów”."
		}
		a.setSofaBrowserState("waiting", message, true)
		select {
		case <-recovery.retry:
			a.setSofaBrowserState("connecting", "Sprawdzanie sesji po ręcznej weryfikacji…", true)
		case <-ctx.Done():
			recovery.err = fmt.Errorf("oczekiwanie na ręczną weryfikację SofaScore zakończone: %w", ctx.Err())
			a.setSofaBrowserState("error", recovery.err.Error(), false)
			return
		}
	}
}

type sofaDevTools struct {
	conn             *websocket.Conn
	bidi             bool
	session          bool
	target           string
	sequence         int
	executionContext int
}

func (browser *sofaDevTools) call(parent context.Context, method string, params any, out any) error {
	ctx, cancel := context.WithTimeout(parent, 25*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	browser.conn.SetWriteDeadline(deadline)
	browser.conn.SetReadDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { browser.conn.Close() })
	defer stop()
	browser.sequence++
	if err := browser.conn.WriteJSON(map[string]any{"id": browser.sequence, "method": method, "params": params}); err != nil {
		return fmt.Errorf("DevTools: wysłanie %s nie powiodło się", method)
	}
	for {
		var message struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err := browser.conn.ReadJSON(&message); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("DevTools: przekroczono czas lub anulowano %s: %w", method, ctx.Err())
			}
			return fmt.Errorf("DevTools: odczyt %s nie powiódł się", method)
		}
		if message.ID != browser.sequence {
			continue
		}
		// Never include protocol error text: it can echo cookies or expressions.
		if len(message.Error) > 0 && string(message.Error) != "null" {
			return fmt.Errorf("DevTools: odrzucono %s; sprawdź kartę SofaScore i inne sesje debugowania", method)
		}
		if out == nil {
			return nil
		}
		if err := json.Unmarshal(message.Result, out); err != nil {
			return fmt.Errorf("DevTools: niepoprawny wynik %s", method)
		}
		return nil
	}
}

func (browser *sofaDevTools) close() {
	if browser.bidi && browser.session {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = browser.call(ctx, "session.end", map[string]any{}, nil)
		cancel()
	}
	browser.conn.Close()
}

func sofaBrowserPage(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() == "www.sofascore.com" && (u.Port() == "" || u.Port() == "443") && u.User == nil
}

func connectSofaDevTools(ctx context.Context, base, kind string) (*sofaDevTools, error) {
	u, _ := url.Parse(base)
	dialer := websocket.Dialer{HandshakeTimeout: 4 * time.Second, Proxy: nil}
	// Chrome speaks CDP, not Firefox BiDi. Do not probe /session on its port.
	if kind != "chrome" {
		conn, response, err := dialer.DialContext(ctx, "ws://"+u.Host+"/session", nil)
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		if err == nil {
			browser := &sofaDevTools{conn: conn, bidi: true}
			conn.SetReadLimit((192 << 20) + (1 << 20))
			if err = browser.call(ctx, "session.new", map[string]any{"capabilities": map[string]any{}}, nil); err != nil {
				browser.close()
				return nil, err
			}
			browser.session = true
			var tree struct {
				Contexts []struct {
					Context string `json:"context"`
					URL     string `json:"url"`
				} `json:"contexts"`
			}
			if err = browser.call(ctx, "browsingContext.getTree", map[string]any{"maxDepth": 0}, &tree); err != nil {
				browser.close()
				return nil, err
			}
			for _, page := range tree.Contexts {
				if sofaBrowserPage(page.URL) && (browser.target == "" || !strings.Contains(page.URL, "/api/v1/")) {
					browser.target = page.Context
				}
			}
			if browser.target == "" {
				browser.close()
				return nil, fmt.Errorf("Floorp: brak otwartej karty https://www.sofascore.com/")
			}
			return browser, nil
		}
	}
	// Legacy Floorp/Chromium: reuse split's /json/list + page CDP connection.
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Timeout: 4 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/json/list", nil)
	listing, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("DevTools przeglądarki niedostępne pod %s", base)
	}
	defer listing.Body.Close()
	if listing.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("port %s nie udostępnia BiDi ani listy kart CDP", base)
	}
	var targets []struct {
		Type      string `json:"type"`
		URL       string `json:"url"`
		WebSocket string `json:"webSocketDebuggerUrl"`
	}
	if json.NewDecoder(io.LimitReader(listing.Body, 1<<20)).Decode(&targets) != nil {
		return nil, fmt.Errorf("DevTools: niepoprawna lista kart")
	}
	sort.SliceStable(targets, func(i, j int) bool {
		return !strings.Contains(targets[i].URL, "/api/v1/") && strings.Contains(targets[j].URL, "/api/v1/")
	})
	for _, page := range targets {
		if page.Type != "page" || !sofaBrowserPage(page.URL) {
			continue
		}
		ws, err := url.Parse(page.WebSocket)
		if err != nil || ws.Scheme != "ws" || ws.User != nil || ws.RawQuery != "" || ws.Fragment != "" || ws.Port() != u.Port() || !strings.HasPrefix(ws.Path, "/devtools/page/") {
			continue
		}
		ip := net.ParseIP(ws.Hostname())
		if ws.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			continue
		}
		ws.Host = u.Host
		conn, response, err := dialer.DialContext(ctx, ws.String(), nil)
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("CDP: nie można połączyć się z kartą SofaScore")
		}
		conn.SetReadLimit((192 << 20) + (1 << 20))
		browser := &sofaDevTools{conn: conn}
		if err := browser.createIsolatedWorld(ctx); err != nil {
			browser.close()
			return nil, err
		}
		return browser, nil
	}
	return nil, fmt.Errorf("DevTools: brak otwartej karty https://www.sofascore.com/")
}

func (browser *sofaDevTools) createIsolatedWorld(ctx context.Context) error {
	var tree struct {
		FrameTree struct {
			Frame struct {
				ID  string `json:"id"`
				URL string `json:"url"`
			} `json:"frame"`
		} `json:"frameTree"`
	}
	if err := browser.call(ctx, "Page.getFrameTree", map[string]any{}, &tree); err != nil {
		return err
	}
	if tree.FrameTree.Frame.ID == "" || !sofaBrowserPage(tree.FrameTree.Frame.URL) {
		return fmt.Errorf("Chrome: karta zmieniła adres; pozostaw otwartą stronę https://www.sofascore.com/")
	}
	var world struct {
		ID int `json:"executionContextId"`
	}
	if err := browser.call(ctx, "Page.createIsolatedWorld", map[string]any{"frameId": tree.FrameTree.Frame.ID, "worldName": "value-bets-sofa"}, &world); err != nil {
		return err
	}
	if world.ID <= 0 {
		return fmt.Errorf("Chrome: brak izolowanego kontekstu strony SofaScore")
	}
	browser.executionContext = world.ID
	return nil
}

func (browser *sofaDevTools) evaluate(ctx context.Context, expression string) (string, error) {
	var result struct {
		Type   string `json:"type"`
		Result struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"result"`
		Exception json.RawMessage `json:"exceptionDetails"`
	}
	method := "Runtime.evaluate"
	params := map[string]any{"expression": expression, "awaitPromise": true, "returnByValue": true, "silent": true}
	if browser.bidi {
		method = "script.evaluate"
		params = map[string]any{"expression": expression, "awaitPromise": true, "target": map[string]any{"context": browser.target, "sandbox": "value-bets-sofa"}, "resultOwnership": "none"}
	} else {
		params["contextId"] = browser.executionContext
	}
	if err := browser.call(ctx, method, params, &result); err != nil {
		return "", err
	}
	if result.Type == "exception" || (len(result.Exception) > 0 && string(result.Exception) != "null") || result.Result.Type != "string" {
		return "", fmt.Errorf("DevTools: odczyt SofaScore nie powiódł się; sprawdź adres i captchę w karcie")
	}
	return result.Result.Value, nil
}

func (browser *sofaDevTools) cookies(ctx context.Context, requestURL string) ([]*fhttp.Cookie, error) {
	method, params := "Network.getCookies", map[string]any{"urls": []string{requestURL, "https://www.sofascore.com/"}}
	if browser.bidi {
		method, params = "storage.getCookies", map[string]any{"partition": map[string]any{"type": "context", "context": browser.target}}
	}
	var response struct {
		Cookies []struct {
			Name     string          `json:"name"`
			Value    json.RawMessage `json:"value"`
			Domain   string          `json:"domain"`
			Path     string          `json:"path"`
			Secure   bool            `json:"secure"`
			HTTPOnly bool            `json:"httpOnly"`
			Expires  float64         `json:"expires"`
			Expiry   float64         `json:"expiry"`
		} `json:"cookies"`
	}
	domains := []string{""}
	if browser.bidi {
		domains = []string{"www.sofascore.com", ".sofascore.com", "sofascore.com"}
	}
	combined := response.Cookies
	for _, domain := range domains {
		if browser.bidi {
			params["filter"] = map[string]any{"domain": domain}
		}
		response.Cookies = nil
		if err := browser.call(ctx, method, params, &response); err != nil {
			return nil, err
		}
		combined = append(combined, response.Cookies...)
	}
	response.Cookies = combined
	cookies := []*fhttp.Cookie{}
	for _, cookie := range response.Cookies {
		domain := strings.TrimPrefix(strings.ToLower(cookie.Domain), ".")
		if domain != "www.sofascore.com" && domain != "sofascore.com" {
			continue
		}
		var value string
		if browser.bidi {
			var encoded struct {
				Type  string `json:"type"`
				Value string `json:"value"`
			}
			if json.Unmarshal(cookie.Value, &encoded) != nil {
				continue
			}
			value = encoded.Value
			if encoded.Type == "base64" {
				raw, err := base64.StdEncoding.DecodeString(value)
				if err != nil {
					continue
				}
				value = string(raw)
			} else if encoded.Type != "string" {
				continue
			}
		} else if json.Unmarshal(cookie.Value, &value) != nil {
			continue
		}
		if strings.ContainsAny(value, "\r\n") || len(value) > 16<<10 {
			continue
		}
		result := &fhttp.Cookie{Name: cookie.Name, Value: value, Domain: cookie.Domain, Path: cookie.Path, Secure: cookie.Secure, HttpOnly: cookie.HTTPOnly}
		expires := cookie.Expires
		if browser.bidi {
			expires = cookie.Expiry
		}
		if expires > 0 {
			result.Expires = time.Unix(int64(expires), 0)
		}
		cookies = append(cookies, result)
	}
	return cookies, nil
}

func (a *App) trySofaBrowser(ctx context.Context, client tls_client.HttpClient, endpoint string) (sofaRelayResponse, error) {
	a.sofaMu.Lock()
	selected, last := a.sofaBrowserPreferred, a.sofaBrowserLast
	a.sofaMu.Unlock()
	base, err := sofaBrowserAddress(selected)
	if err != nil {
		return sofaRelayResponse{}, err
	}
	var jitter [1]byte
	_, _ = cryptorand.Read(jitter[:])
	if delay := time.Until(last.Add(500*time.Millisecond + time.Duration(jitter[0])*2*time.Millisecond)); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return sofaRelayResponse{}, ctx.Err()
		case <-timer.C:
		}
	}
	defer func() { a.sofaMu.Lock(); a.sofaBrowserLast = time.Now(); a.sofaMu.Unlock() }()
	a.setSofaBrowserState("connecting", "Łączenie z DevTools: "+base+"…", true)
	browser, err := connectSofaDevTools(ctx, base, selected)
	if err != nil {
		return sofaRelayResponse{}, err
	}
	defer browser.close()
	a.setSofaBrowserState("connecting", "DevTools połączone. Odczyt sesji wyłącznie dla SofaScore…", true)
	cookies, err := browser.cookies(ctx, "https://www.sofascore.com/api/v1"+endpoint)
	if err != nil {
		return sofaRelayResponse{}, err
	}
	headers := map[string]string{"accept": "application/json"}
	for _, cookie := range cookies {
		if cookie.Name == "sofa_captcha" {
			headers["x-captcha"] = cookie.Value
		}
	}
	if value := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_REQUESTED_WITH")); value != "" {
		headers["x-requested-with"] = value
	}
	address, _ := json.Marshal("https://www.sofascore.com/api/v1" + endpoint)
	headerJSON, _ := json.Marshal(headers)
	// Same-origin fetch through the user's existing tab, as on split. Captcha
	// interaction is exclusively manual. The script never clicks or solves it.
	expression := `(async () => {
  if (location.origin !== "https://www.sofascore.com") return JSON.stringify({status:0,error:"origin"});
  const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 20000);
  try {
    const response = await fetch(` + string(address) + `, {method:"GET", credentials:"include", cache:"no-store", redirect:"error", headers:` + string(headerJSON) + `, signal:controller.signal});
    if (response.status !== 200) return JSON.stringify({status:response.status, body:"", agent:navigator.userAgent});
    const reader = response.body.getReader(), decoder = new TextDecoder("utf-8", {fatal:true});
    const parts = []; let size = 0;
    while (true) {
      const chunk = await reader.read(); if (chunk.done) break;
      size += chunk.value.byteLength; if (size > 33554432) { controller.abort(); throw new Error("limit"); }
      parts.push(decoder.decode(chunk.value, {stream:true}));
    }
    parts.push(decoder.decode());
    return JSON.stringify({status:response.status, body:parts.join(""), agent:navigator.userAgent});
  } catch (error) {
    const code = error?.name === "AbortError" ? "timeout" : error?.message === "limit" ? "limit" : "network";
    return JSON.stringify({status:0,error:code});
  } finally { clearTimeout(timer); }
})()`
	a.setSofaBrowserState("connecting", "Pobieranie API przez kartę SofaScore (limit 20 s)…", true)
	raw, err := browser.evaluate(ctx, expression)
	if err != nil {
		return sofaRelayResponse{}, err
	}
	var result struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
		Agent  string `json:"agent"`
		Error  string `json:"error"`
	}
	if json.Unmarshal([]byte(raw), &result) != nil {
		return sofaRelayResponse{}, fmt.Errorf("DevTools: niepoprawna odpowiedź SofaScore")
	}
	if result.Status == 0 {
		switch result.Error {
		case "timeout":
			return sofaRelayResponse{}, fmt.Errorf("SofaScore: pobranie w karcie przekroczyło 20 sekund; DevTools jest połączone")
		case "origin":
			return sofaRelayResponse{}, fmt.Errorf("SofaScore: karta zmieniła domenę; otwórz https://www.sofascore.com/")
		case "limit":
			return sofaRelayResponse{}, fmt.Errorf("SofaScore: odpowiedź w karcie przekracza 32 MB")
		case "network":
			return sofaRelayResponse{}, fmt.Errorf("SofaScore: błąd sieci podczas pobierania w karcie; sprawdź stronę i odśwież ją")
		}
	}
	if result.Status < 100 || result.Status > 599 {
		return sofaRelayResponse{}, fmt.Errorf("DevTools: brak statusu HTTP odpowiedzi SofaScore")
	}
	log.Printf("[SOFA-BROWSER] endpoint=%q status=%d bidi=%t bytes=%d", endpoint, result.Status, browser.bidi, len(result.Body))
	if result.Status == http.StatusForbidden || result.Status == http.StatusUnauthorized {
		if browser.bidi {
			_ = browser.call(ctx, "browsingContext.activate", map[string]any{"context": browser.target}, nil)
		} else {
			_ = browser.call(ctx, "Page.bringToFront", map[string]any{}, nil)
		}
		return sofaRelayResponse{Status: result.Status}, nil
	}
	if result.Status == http.StatusOK && (!json.Valid([]byte(result.Body)) || len(result.Body) > 32<<20) {
		return sofaRelayResponse{}, fmt.Errorf("SofaScore w przeglądarce nie zwrócił poprawnego JSON; rozwiąż challenge ręcznie")
	}
	if result.Status == http.StatusOK {
		origin, _ := url.Parse("https://www.sofascore.com/")
		client.SetCookies(origin, []*fhttp.Cookie{{Name: "sofa_captcha", Value: "removed", Path: "/", Secure: true, MaxAge: -1}})
		client.SetCookies(origin, cookies)
		a.sofaMu.Lock()
		if len(result.Agent) <= 512 && !strings.ContainsAny(result.Agent, "\r\n") {
			a.sofaBrowserAgent = result.Agent
		}
		a.sofaBrowserToken = headers["x-captcha"]
		a.sofaBrowserMode = true
		a.sofaMu.Unlock()
	}
	return sofaRelayResponse{Status: result.Status, Body: []byte(result.Body)}, nil
}
