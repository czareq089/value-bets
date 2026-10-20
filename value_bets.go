// Desktop client. The analysis server lives in server/value_bets.go.
package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
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
	"unicode"

	"golang.org/x/net/html"
	"golang.org/x/text/unicode/norm"
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
	SampleFrom              string             `json:"sampleFrom,omitempty"`
	SampleTo                string             `json:"sampleTo,omitempty"`
	FixtureID               int                `json:"fixtureId,omitempty"`
	MatchStart              time.Time          `json:"matchStart,omitempty"`
	MatchName               string             `json:"matchName,omitempty"`
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
	TeamCountry             string             `json:"teamCountry,omitempty"`
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
	SampleFrom              string             `json:"sampleFrom,omitempty"`
	SampleTo                string             `json:"sampleTo,omitempty"`
	FixtureID               int                `json:"fixtureId,omitempty"`
	MatchStart              time.Time          `json:"matchStart,omitempty"`
	MatchName               string             `json:"matchName,omitempty"`
	AddedAt                 string             `json:"addedAt,omitempty"`
	IncludeSuperSub         bool               `json:"includeSuperSub"`
	ArchivedAt              string             `json:"archivedAt,omitempty"`
	SettlementStatus        string             `json:"settlementStatus,omitempty"`
	SettlementSource        string             `json:"settlementSource,omitempty"`
	SettlementValue         *int               `json:"settlementValue"`
	PlayerValue             *int               `json:"playerValue"`
	SuperSubValue           *int               `json:"superSubValue"`
	SettlementNote          string             `json:"settlementNote,omitempty"`
	SettlementCheckedAt     string             `json:"settlementCheckedAt,omitempty"`
	FixtureStatus           string             `json:"fixtureStatus,omitempty"`
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
	TeamCountry             string             `json:"teamCountry,omitempty"`
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

type FootyMatchChoice struct {
	FixtureID   int       `json:"fixtureId"`
	Home        string    `json:"home"`
	Away        string    `json:"away"`
	StartTime   time.Time `json:"startTime"`
	LeagueID    int       `json:"leagueId"`
	HomeMatched bool      `json:"homeMatched"`
	AwayMatched bool      `json:"awayMatched"`
	Reason      string    `json:"reason"`
}

type AnalysisJob struct {
	MatchChoices   []FootyMatchChoice `json:"matchChoices,omitempty"`
	FixtureID      string             `json:"fixtureId,omitempty"`
	StartTime      time.Time          `json:"startTime,omitempty"`
	ResultRevision int                `json:"resultRevision"`
	ID             string             `json:"id"`
	Match          string             `json:"match"`
	Status         string             `json:"status"`
	Stage          string             `json:"stage"`
	Message        string             `json:"message"`
	PlayersDone    int                `json:"playersDone"`
	PlayersTotal   int                `json:"playersTotal"`
	CreatedAt      time.Time          `json:"createdAt"`
	StartedAt      time.Time          `json:"startedAt,omitempty"`
	FinishedAt     time.Time          `json:"finishedAt,omitempty"`
	Request        AnalysisRequest    `json:"request"`
	Warnings       []string           `json:"warnings"`
	Result         []AnalyzedBet      `json:"result,omitempty"`
	Error          string             `json:"error,omitempty"`
}

type App struct {
	teamLogoIndexMu      sync.Mutex
	teamLogoFlights      map[string]chan struct{}
	teamLogoSlots        chan struct{}
	teamLogoBlockedUntil time.Time
	teamLogoMu           sync.Mutex
	teamLogoCache        *teamLogoDiskCache
	teamLogoRefreshAfter time.Time
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

// Search and image downloads run on the desktop, independently of analysis.
// The UI receives validated image bytes; it never hotlinks the provider's CDN.
const teamLogoMatchVersion = 4
const teamLogoSearchMinScore = 35.0
const teamLogoAutoMinScore = 75.0
const teamLogoAutoMinGap = 12.0

type teamLogoIndexEntry struct {
	Label             string `json:"label"`
	Href              string `json:"href"`
	Kind              string `json:"kind"`
	Preview           string `json:"preview,omitempty"`
	Meta              string `json:"meta,omitempty"`
	Terms             string `json:"terms,omitempty"`
	VariantParentHref string `json:"variantParentHref,omitempty"`
}
type teamLogoPage struct {
	Names     []string  `json:"names"`
	Images    []string  `json:"images"`
	ExpiresAt time.Time `json:"expiresAt"`
}
type teamLogoAsset struct {
	ExpiresAt time.Time `json:"expiresAt"`
}
type teamLogoDiskCache struct {
	Version    int                      `json:"version"`
	UpdatedAt  time.Time                `json:"updatedAt"`
	Index      []teamLogoIndexEntry     `json:"index"`
	Selections map[string]string        `json:"selections"`
	Pages      map[string]teamLogoPage  `json:"pages"`
	Assets     map[string]teamLogoAsset `json:"assets"`
}
type teamLogoCandidate struct {
	Label   string  `json:"label"`
	Href    string  `json:"href"`
	Country string  `json:"country"`
	Score   float64 `json:"-"`
}

func teamLogoCachePath() string {
	dir, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "ValueBets", "team_logos.json")
}
func teamLogoAssetPath(href string) string {
	path := teamLogoCachePath()
	if path == "" {
		return ""
	}
	hash := sha256.Sum256([]byte(href))
	return filepath.Join(filepath.Dir(path), "team_logos", fmt.Sprintf("%x.img", hash))
}

var teamLogoLatin = strings.NewReplacer("ł", "l", "ø", "o", "ß", "ss", "æ", "ae", "œ", "oe", "đ", "d", "ı", "i")
var teamLogoClubTokens = map[string]bool{"fc": true, "cf": true, "afc": true, "ac": true, "as": true, "sc": true, "sk": true, "fk": true, "ks": true, "rks": true, "club": true}
var teamLogoGeneric = map[string]bool{"united": true, "city": true, "real": true, "racing": true, "sporting": true, "atletico": true, "athletic": true, "olympique": true, "dynamo": true, "dinamo": true, "inter": true, "nacional": true, "national": true, "saint": true, "borussia": true}
var teamLogoHrefPattern = regexp.MustCompile(`^/logos/[a-z0-9-]+$`)
var teamLogoInitialsPattern = regexp.MustCompile(`(?i)\b(?:[a-z]\.){2,}`)

func teamLogoName(name string) string {
	var text strings.Builder
	for _, r := range norm.NFKD.String(teamLogoLatin.Replace(strings.ToLower(name))) {
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			text.WriteRune(r)
		} else {
			text.WriteByte(' ')
		}
	}
	words := strings.Fields(text.String())
	kept := words[:0]
	for _, word := range words {
		if !teamLogoClubTokens[word] {
			kept = append(kept, word)
		}
	}
	return strings.Join(kept, " ")
}

// Search identity is separate from cache keys/country names: changing matching
// rules must not invalidate saved manual selections or downloaded images.
func teamLogoSearchIdentity(name string) (string, string) {
	// F.C./S.K. are club initials, not a C reserve team or a K women's suffix.
	name = teamLogoInitialsPattern.ReplaceAllStringFunc(name, func(initials string) string {
		return strings.ReplaceAll(initials, ".", "")
	})
	words := strings.Fields(teamLogoName(name))
	kept := make([]string, 0, len(words))
	qualifiers := map[string]bool{}
	for i := 0; i < len(words); i++ {
		word := words[i]
		switch word {
		case "women", "womens", "female", "feminino", "feminina", "femenino", "femenina", "feminin", "feminine", "kobiety", "kobiet", "wfc":
			qualifiers["women"] = true
			continue
		case "reserve", "reserves", "rezerwy":
			qualifiers["reserve:2"] = true
			continue
		}
		age := ""
		if (word == "u" || word == "under") && i+1 < len(words) {
			age = words[i+1]
		} else if strings.HasPrefix(word, "u") && len(word) > 1 {
			age = strings.TrimPrefix(word, "u")
		}
		if value, err := strconv.Atoi(age); err == nil && value >= 10 && value <= 23 {
			qualifiers["u"+strconv.Itoa(value)] = true
			if word == "u" || word == "under" {
				i++
			}
			continue
		}
		kept = append(kept, word)
	}
	// Strip terminal category markers after age/women words, so combinations
	// such as "Club B U21" and "Club II Women" keep every qualifier.
	for len(kept) > 1 {
		qualifier := ""
		switch kept[len(kept)-1] {
		case "b", "ii":
			qualifier = "reserve:2"
		case "c", "iii":
			qualifier = "reserve:3"
		case "d", "iv":
			qualifier = "reserve:4"
		case "w", "f", "k":
			qualifier = "women"
		}
		if qualifier == "" {
			break
		}
		qualifiers[qualifier] = true
		kept = kept[:len(kept)-1]
	}
	core := make([]string, 0, len(kept))
	for i, word := range kept {
		if i > 0 && i < len(kept)-1 {
			switch word {
			case "de", "del", "da", "do", "das", "dos":
				continue
			}
		}
		core = append(core, word)
	}
	category := make([]string, 0, len(qualifiers))
	for qualifier := range qualifiers {
		category = append(category, qualifier)
	}
	sort.Strings(category)
	return strings.Join(core, " "), strings.Join(category, "|")
}

func teamLogoEntryCategory(entry teamLogoIndexEntry) string {
	_, labelCategory := teamLogoSearchIdentity(entry.Label)
	_, slugCategory := teamLogoSearchIdentity(strings.TrimPrefix(entry.Href, "/logos/"))
	if labelCategory != "" && slugCategory != "" && labelCategory != slugCategory {
		return "conflicting_category"
	}
	if labelCategory != "" {
		return labelCategory
	}
	return slugCategory
}

func teamLogoKey(q map[string]string) string {
	// A provider team ID and country distinguish clubs with the same short name.
	if q["key"] != "" {
		return q["key"]
	}
	return strings.Join([]string{q["teamId"], teamLogoName(q["name"]), teamLogoName(q["country"])}, "|")
}
func teamLogoCountryKey(name string) string {
	key := teamLogoName(name)
	switch key {
	case "united states", "united states of america", "usa":
		return "usa"
	case "turkiye", "turkey":
		return "turkey"
	case "czechia", "czech republic":
		return "czech republic"
	case "korea republic", "south korea":
		return "south korea"
	}
	return key
}
func teamLogoCountry(entry teamLogoIndexEntry) string {
	parts := strings.Split(entry.Meta, "·")
	if len(parts) > 1 {
		return strings.TrimSpace(parts[len(parts)-1])
	}
	return ""
}
func validTeamLogoURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host == "assets.footylogos.com" && u.User == nil && u.Fragment == "" &&
		(strings.HasPrefix(u.Path, "/logos/") && strings.HasSuffix(u.Path, ".svg") || strings.HasPrefix(u.Path, "/previews/") && strings.HasSuffix(u.Path, ".webp"))
}
func writeTeamLogoFile(path string, body []byte) error {
	if path == "" {
		return fmt.Errorf("brak katalogu cache")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".team-logo-*.tmp")
	if err != nil {
		return err
	}
	temp := file.Name()
	defer os.Remove(temp)
	_, err = file.Write(body)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(temp, path)
}
func (a *App) loadTeamLogosLocked() {
	if a.teamLogoCache != nil {
		return
	}
	cache := teamLogoDiskCache{}
	if file, err := os.Open(teamLogoCachePath()); err == nil {
		if json.NewDecoder(io.LimitReader(file, 8<<20)).Decode(&cache) != nil {
			cache = teamLogoDiskCache{}
		}
		file.Close()
	}
	if cache.Version != teamLogoMatchVersion {
		cache = teamLogoDiskCache{Version: teamLogoMatchVersion}
	}
	if cache.Selections == nil {
		cache.Selections = map[string]string{}
	}
	if cache.Pages == nil {
		cache.Pages = map[string]teamLogoPage{}
	}
	if cache.Assets == nil {
		cache.Assets = map[string]teamLogoAsset{}
	}
	a.teamLogoCache = &cache
	a.teamLogoFlights = map[string]chan struct{}{}
	a.teamLogoSlots = make(chan struct{}, 3)
}
func (a *App) saveTeamLogosLocked() {
	data, err := json.Marshal(a.teamLogoCache)
	if err == nil {
		err = writeTeamLogoFile(teamLogoCachePath(), data)
	}
	if err != nil {
		log.Printf("[TEAM-LOGOS] zapis cache: %v", err)
	}
}
func (a *App) fetchTeamLogoSource(ctx context.Context, address string, limit int64) ([]byte, error) {
	u, err := url.Parse(address)
	if err != nil || u.Scheme != "https" || (u.Host != "www.footylogos.com" && !validTeamLogoURL(address)) {
		return nil, fmt.Errorf("niepoprawny adres Footylogos")
	}
	a.teamLogoMu.Lock()
	blocked := a.teamLogoBlockedUntil
	slots := a.teamLogoSlots
	a.teamLogoMu.Unlock()
	if time.Now().Before(blocked) {
		return nil, fmt.Errorf("limit Footylogos; ponów za %d s", max(1, int(time.Until(blocked).Seconds())))
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	client := &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) > 2 || req.URL.Scheme != "https" || (req.URL.Host != "www.footylogos.com" && !validTeamLogoURL(req.URL.String())) {
			return fmt.Errorf("niedozwolone przekierowanie Footylogos")
		}
		return nil
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "ValueBets/3.0")
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == 429 || response.StatusCode == 503 {
		until := time.Now().Add(time.Minute)
		if seconds, e := strconv.Atoi(response.Header.Get("Retry-After")); e == nil && seconds > 0 {
			until = time.Now().Add(time.Duration(min(seconds, 3600)) * time.Second)
		} else if date, e := http.ParseTime(response.Header.Get("Retry-After")); e == nil && date.After(until) {
			until = date
			if limit := time.Now().Add(time.Hour); until.After(limit) {
				until = limit
			}
		}
		a.teamLogoMu.Lock()
		a.teamLogoBlockedUntil = until
		a.teamLogoMu.Unlock()
	}
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("Footylogos HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("zbyt duża odpowiedź Footylogos")
	}
	return body, nil
}
func (a *App) teamLogoIndex(ctx context.Context, force bool) ([]teamLogoIndexEntry, error) {
	a.teamLogoIndexMu.Lock()
	defer a.teamLogoIndexMu.Unlock()
	a.teamLogoMu.Lock()
	a.loadTeamLogosLocked()
	index, updated, next := a.teamLogoCache.Index, a.teamLogoCache.UpdatedAt, a.teamLogoRefreshAfter
	a.teamLogoMu.Unlock()
	if !force && len(index) > 0 && time.Since(updated) < 7*24*time.Hour {
		return index, nil
	}
	if time.Now().Before(next) {
		if len(index) > 0 {
			return index, nil
		}
		return nil, fmt.Errorf("katalog Footylogos chwilowo niedostępny")
	}
	body, err := a.fetchTeamLogoSource(ctx, "https://www.footylogos.com/data/search-index.json", 4<<20)
	var source []teamLogoIndexEntry
	if err == nil {
		err = json.Unmarshal(body, &source)
	}
	clean := []teamLogoIndexEntry{}
	if err == nil {
		for _, entry := range source {
			if (entry.Kind == "logo" || entry.Kind == "national-team") && entry.VariantParentHref == "" && teamLogoHrefPattern.MatchString(entry.Href) {
				clean = append(clean, entry)
			}
		}
		if len(clean) == 0 {
			err = fmt.Errorf("pusty katalog Footylogos")
		}
	}
	a.teamLogoMu.Lock()
	defer a.teamLogoMu.Unlock()
	a.teamLogoRefreshAfter = time.Now().Add(time.Minute)
	if err != nil {
		if len(index) > 0 {
			return index, nil
		}
		return nil, err
	}
	a.teamLogoCache.Index = clean
	a.teamLogoCache.UpdatedAt = time.Now()
	a.saveTeamLogosLocked()
	return clean, nil
}
func teamLogoNameScore(query, candidate string) float64 {
	queryName, queryCategory := teamLogoSearchIdentity(query)
	candidateName, candidateCategory := teamLogoSearchIdentity(candidate)
	if queryCategory != candidateCategory {
		return 0
	}
	q, c := strings.Fields(queryName), strings.Fields(candidateName)
	if len(q) == 0 || len(c) == 0 {
		return 0
	}
	if strings.Join(q, " ") == strings.Join(c, " ") {
		return 100
	}
	found, specific := 0, false
	for _, word := range q {
		for _, other := range c {
			if word == other {
				found++
				if !teamLogoGeneric[word] && len([]rune(word)) >= 3 {
					specific = true
				}
				break
			}
		}
	}
	if !specific {
		return 0
	}
	coverage := float64(found) / float64(len(q))
	if coverage < 0.5 {
		return 0
	}
	// Match anywhere in the name, in any word order: Dortmund -> Borussia Dortmund.
	return 70*coverage + 20*float64(found)/float64(len(c))
}
func (a *App) searchTeamLogos(index []teamLogoIndexEntry, q map[string]string) []teamLogoCandidate {
	a.teamLogoMu.Lock()
	aliases := map[string][]string{}
	for href, page := range a.teamLogoCache.Pages {
		aliases[href] = page.Names
	}
	a.teamLogoMu.Unlock()
	results := []teamLogoCandidate{}
	country := teamLogoCountryKey(q["country"])
	_, queryCategory := teamLogoSearchIdentity(q["name"])
	for _, entry := range index {
		// The entry's own label/slug establish its category. Broad searchable
		// terms or a learned short name must never promote a reserve/youth club
		// to the senior team's candidate list.
		if teamLogoEntryCategory(entry) != queryCategory {
			continue
		}
		ownCountry := teamLogoCountry(entry)
		names := append([]string{entry.Label, strings.TrimPrefix(entry.Href, "/logos/")}, aliases[entry.Href]...)
		score := 0.0
		for _, name := range names {
			score = max(score, teamLogoNameScore(q["name"], name))
		}
		// The catalogue's own searchable terms provide additional spelling/name tokens.
		if score == 0 {
			score = teamLogoNameScore(q["name"], entry.Terms) * 0.85
		}
		if score < teamLogoSearchMinScore {
			continue
		}
		if country != "" && ownCountry != "" {
			if country == teamLogoCountryKey(ownCountry) {
				score += 25
			} else {
				score -= 10
			}
		}
		results = append(results, teamLogoCandidate{Label: entry.Label, Href: entry.Href, Country: ownCountry, Score: score})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score == results[j].Score {
			return results[i].Label < results[j].Label
		}
		return results[i].Score > results[j].Score
	})
	if len(results) > 8 {
		results = results[:8]
	}
	return results
}
func teamLogoImageType(body []byte) string {
	if len(body) >= 12 && string(body[:4]) == "RIFF" && string(body[8:12]) == "WEBP" {
		return "image/webp"
	}
	if len(body) >= 8 && bytes.Equal(body[:8], []byte{137, 80, 78, 71, 13, 10, 26, 10}) {
		return "image/png"
	}
	if len(body) >= 3 && body[0] == 255 && body[1] == 216 && body[2] == 255 {
		return "image/jpeg"
	}

	decoder := xml.NewDecoder(bytes.NewReader(body))
	seen, depth := false, 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			if seen && depth == 0 {
				return "image/svg+xml"
			}
			return ""
		}
		if err != nil {
			return ""
		}
		switch part := token.(type) {
		case xml.StartElement:
			if depth == 0 {
				if seen || part.Name.Local != "svg" {
					return ""
				}
				seen = true
			}
			depth++
		case xml.EndElement:
			depth--
		case xml.CharData:
			if depth == 0 && len(bytes.TrimSpace(part)) > 0 {
				return ""
			}
		}
	}
}
func teamLogoImageData(body []byte) string {
	mime := teamLogoImageType(body)
	if mime == "" {
		return ""
	}
	return "data:" + mime + ";base64," + base64.StdEncoding.EncodeToString(body)
}
func parseTeamLogoPage(body []byte, entry teamLogoIndexEntry) teamLogoPage {
	page := teamLogoPage{ExpiresAt: time.Now().Add(7 * 24 * time.Hour)}
	tokens := html.NewTokenizer(bytes.NewReader(body))
	slug := strings.TrimPrefix(entry.Href, "/logos/")
	var collect func(any)
	collect = func(value any) {
		switch data := value.(type) {
		case []any:
			for _, item := range data {
				collect(item)
			}
		case map[string]any:
			if data["@type"] == "SportsTeam" {
				if name, ok := data["name"].(string); ok {
					page.Names = append(page.Names, name)
				}
				switch names := data["alternateName"].(type) {
				case string:
					page.Names = append(page.Names, names)
				case []any:
					for _, name := range names {
						if text, ok := name.(string); ok {
							page.Names = append(page.Names, text)
						}
					}
				}
			}
			if graph, ok := data["@graph"]; ok {
				collect(graph)
			}
		}
	}
	for {
		kind := tokens.Next()
		if kind == html.ErrorToken {
			break
		}
		if kind != html.StartTagToken && kind != html.SelfClosingTagToken {
			continue
		}
		tag := tokens.Token()
		attrs := map[string]string{}
		for _, attr := range tag.Attr {
			attrs[attr.Key] = attr.Val
		}
		if tag.Data == "script" && attrs["type"] == "application/ld+json" && tokens.Next() == html.TextToken {
			var data any
			if json.Unmarshal(tokens.Text(), &data) == nil {
				collect(data)
			}
		}
		if tag.Data == "img" && validTeamLogoURL(attrs["src"]) && strings.Contains(attrs["src"], ".svg") {
			// Identify the main team image by its own slug/name, not a presentation attribute.
			own := strings.Contains(attrs["src"], "/"+slug+"/") || teamLogoName(attrs["alt"]) == teamLogoName(entry.Label+" logo")
			if own && len(page.Images) == 0 {
				page.Images = append(page.Images, attrs["src"])
			}
		}
	}
	if validTeamLogoURL(entry.Preview) {
		page.Images = append(page.Images, entry.Preview)
	}
	return page
}
func (a *App) loadTeamLogoImage(ctx context.Context, entry teamLogoIndexEntry, force bool) (string, error) {
	href := entry.Href
	for {
		a.teamLogoMu.Lock()
		asset := a.teamLogoCache.Assets[href]
		waiting := a.teamLogoFlights[href]
		if waiting != nil {
			a.teamLogoMu.Unlock()
			select {
			case <-waiting:
				force = false
				continue
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		if !force && time.Now().Before(asset.ExpiresAt) {
			if body, err := os.ReadFile(teamLogoAssetPath(href)); err == nil && len(body) <= 512<<10 {
				if data := teamLogoImageData(body); data != "" {
					a.teamLogoMu.Unlock()
					return data, nil
				}
			}
		}
		done := make(chan struct{})
		a.teamLogoFlights[href] = done
		page := a.teamLogoCache.Pages[href]
		a.teamLogoMu.Unlock()
		defer func() { a.teamLogoMu.Lock(); delete(a.teamLogoFlights, href); close(done); a.teamLogoMu.Unlock() }()
		if force || !time.Now().Before(page.ExpiresAt) {
			body, err := a.fetchTeamLogoSource(ctx, "https://www.footylogos.com"+href, 2<<20)
			if err == nil {
				page = parseTeamLogoPage(body, entry)
				a.teamLogoMu.Lock()
				a.teamLogoCache.Pages[href] = page
				a.teamLogoMu.Unlock()
			} else if len(page.Images) == 0 && validTeamLogoURL(entry.Preview) {
				page.Images = []string{entry.Preview}
			}
		}
		var last error
		for _, address := range page.Images {
			if !validTeamLogoURL(address) {
				continue
			}
			body, err := a.fetchTeamLogoSource(ctx, address, 512<<10)
			if err != nil {
				last = err
				continue
			}
			data := teamLogoImageData(body)
			if data == "" {
				last = fmt.Errorf("źródło nie zwróciło poprawnego obrazu")
				continue
			}
			if err = writeTeamLogoFile(teamLogoAssetPath(href), body); err != nil {
				log.Printf("[TEAM-LOGOS] plik %s: %v", href, err)
			}
			a.teamLogoMu.Lock()
			a.teamLogoCache.Assets[href] = teamLogoAsset{ExpiresAt: time.Now().Add(7 * 24 * time.Hour)}
			a.saveTeamLogosLocked()
			a.teamLogoMu.Unlock()
			return data, nil
		}
		// A stale, validated local image is usable during a temporary provider failure.
		if body, err := os.ReadFile(teamLogoAssetPath(href)); err == nil && len(body) <= 512<<10 {
			if data := teamLogoImageData(body); data != "" {
				return data, nil
			}
		}
		if last == nil {
			last = fmt.Errorf("brak pliku herbu na stronie źródła")
		}
		return "", last
	}
}
func teamLogoRejectionReason(candidates []teamLogoCandidate) string {
	if len(candidates) == 0 {
		return fmt.Sprintf("brak kandydatów spełniających próg wyszukiwania %.1f", teamLogoSearchMinScore)
	}
	reasons := []string{}
	if candidates[0].Score < teamLogoAutoMinScore {
		reasons = append(reasons, fmt.Sprintf("najlepszy wynik %.1f poniżej progu %.1f", candidates[0].Score, teamLogoAutoMinScore))
	}
	if len(candidates) > 1 {
		gap := candidates[0].Score - candidates[1].Score
		if gap < teamLogoAutoMinGap {
			reasons = append(reasons, fmt.Sprintf("niejednoznaczny wynik: różnica dwóch najlepszych %.1f poniżej progu %.1f", gap, teamLogoAutoMinGap))
		}
	}
	return strings.Join(reasons, "; ")
}

func logTeamLogoDecision(query map[string]string, stage, reason string, candidates []teamLogoCandidate, selected *teamLogoIndexEntry) {
	// Keep the actual ranking visible in logs; API candidates intentionally omit Score.
	entries := make([]map[string]any, 0, len(candidates))
	for _, candidate := range candidates {
		entries = append(entries, map[string]any{"name": candidate.Label, "country": candidate.Country, "score": candidate.Score, "href": candidate.Href})
	}
	ranking, _ := json.Marshal(entries)
	sourceName := query["sourceName"]
	if sourceName == "" {
		sourceName = query["name"]
	}
	var selectedName, selectedCountry, selectedHref string
	if selected != nil {
		selectedName, selectedCountry, selectedHref = selected.Label, teamLogoCountry(*selected), selected.Href
	}
	identity, category := teamLogoSearchIdentity(query["name"])
	log.Printf("[TEAM-LOGOS] stage=%s team_id=%q source_name=%q query=%q normalized=%q category=%q country=%q reason=%q min_score=%.1f min_gap=%.1f candidates=%s selected_name=%q selected_country=%q selected_href=%q",
		stage, query["teamId"], sourceName, query["name"], identity, category, query["country"], reason,
		teamLogoAutoMinScore, teamLogoAutoMinGap, ranking, selectedName, selectedCountry, selectedHref)
}

func (a *App) resolveTeamLogo(index []teamLogoIndexEntry, q map[string]string, selection string) map[string]any {
	candidates := a.searchTeamLogos(index, q)
	result := map[string]any{"status": "not_found", "note": "Nie znaleziono pewnego dopasowania. Wyszukaj klub.", "candidates": candidates}
	rejectionReason := teamLogoRejectionReason(candidates)
	if q["search"] == "true" {
		reason := rejectionReason
		if reason == "" {
			reason = "wyszukiwanie ręczne: kandydaci dostępni do wyboru"
		}
		logTeamLogoDecision(q, "MANUAL_SEARCH", reason, candidates, nil)
		if len(candidates) > 0 {
			result["status"] = "ambiguous"
			result["note"] = "Wybierz właściwy klub z listy."
		}
		return result
	}
	savedSelection := false
	automaticSelection := false
	a.teamLogoMu.Lock()
	if selection == "" {
		selection = a.teamLogoCache.Selections[teamLogoKey(q)]
		savedSelection = selection != ""
	}
	a.teamLogoMu.Unlock()
	if selection == "" {
		if rejectionReason != "" {
			logTeamLogoDecision(q, "AUTO_REJECT", rejectionReason, candidates, nil)
			if len(candidates) > 0 {
				result["status"] = "ambiguous"
				result["note"] = "Wybierz właściwy klub z listy."
			}
			return result
		}
		selection = candidates[0].Href
		automaticSelection = true
	}
	var entry *teamLogoIndexEntry
	for i := range index {
		if index[i].Href == selection {
			entry = &index[i]
			break
		}
	}
	if entry == nil {
		logTeamLogoDecision(q, "SELECTION_REJECT", "wybrany klub nie występuje w aktualnym katalogu", candidates, nil)
		return result
	}
	if savedSelection {
		// A saved choice can bypass an unresolved automatic match; retain its diagnostics.
		reason := rejectionReason
		if reason == "" {
			reason = "zastosowano zapisany ręczny wybór; automatyczny wynik spełnia obecne progi"
		}
		logTeamLogoDecision(q, "MANUAL_REUSE", reason, candidates, entry)
	}
	if automaticSelection {
		logTeamLogoDecision(q, "AUTO_SELECT", "zgodna kategoria drużyny i wynik spełniający progi dopasowania", candidates, entry)
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 25*time.Second)
	defer cancel()
	data, err := a.loadTeamLogoImage(ctx, *entry, q["refresh"] == "true")
	if err != nil {
		a.teamLogoMu.Lock()
		retry := max(15, int(time.Until(a.teamLogoBlockedUntil).Seconds())+1)
		a.teamLogoMu.Unlock()
		logTeamLogoDecision(q, "IMAGE_ERROR", err.Error(), candidates, entry)
		result["status"] = "error"
		result["note"] = err.Error()
		result["retryAfter"] = retry
		return result
	}
	result["status"] = "ready"
	result["data"] = data
	result["label"] = entry.Label
	result["href"] = entry.Href
	result["note"] = entry.Label + " · " + teamLogoCountry(*entry) + " · Footylogos"
	return result
}

// GetTeamLogos searches the provider's complete catalogue, then returns local image data.
func (a *App) GetTeamLogos(queries []map[string]string) (map[string]any, error) {
	if len(queries) > 12 {
		return nil, fmt.Errorf("maksymalnie 12 drużyn naraz")
	}
	for _, q := range queries {
		if len(q["key"]) > 1200 || len(q["name"]) > 200 || len(q["teamId"]) > 100 || len(q["country"]) > 100 {
			return nil, fmt.Errorf("niepoprawne zapytanie o herb")
		}
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 15*time.Second)
	force := false
	for _, q := range queries {
		force = force || q["refresh"] == "true"
	}
	index, err := a.teamLogoIndex(ctx, force)
	cancel()
	if err != nil {
		for _, query := range queries {
			logTeamLogoDecision(query, "CATALOG_ERROR", err.Error(), nil, nil)
		}
		return nil, err
	}
	result := map[string]any{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	workers := make(chan struct{}, 3)
	for _, query := range queries {
		q := query
		workers <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-workers }()
			item := a.resolveTeamLogo(index, q, "")
			mu.Lock()
			result[teamLogoKey(q)] = item
			mu.Unlock()
		}()
	}
	wg.Wait()
	return result, nil
}

// SetTeamLogo persists an explicit user choice for this provider team and country.
func (a *App) SetTeamLogo(query map[string]string, href string) (map[string]any, error) {
	if !teamLogoHrefPattern.MatchString(href) || len(query["key"]) > 1200 || len(query["name"]) > 200 || len(query["teamId"]) > 100 || len(query["country"]) > 100 {
		return nil, fmt.Errorf("niepoprawny wybór herbu")
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 15*time.Second)
	index, err := a.teamLogoIndex(ctx, false)
	cancel()
	if err != nil {
		logTeamLogoDecision(query, "CATALOG_ERROR", err.Error(), nil, nil)
		return nil, err
	}
	var selected *teamLogoIndexEntry
	for i := range index {
		if index[i].Href == href {
			selected = &index[i]
			break
		}
	}
	candidates := a.searchTeamLogos(index, query)
	if selected == nil {
		logTeamLogoDecision(query, "SELECTION_REJECT", "ręcznie wybrany klub nie występuje w katalogu", candidates, nil)
		return nil, fmt.Errorf("klub nie występuje w katalogu")
	}
	reason := teamLogoRejectionReason(candidates)
	if reason == "" {
		reason = "użytkownik wskazał klub ręcznie"
	}
	logTeamLogoDecision(query, "MANUAL_SELECT", reason, candidates, selected)
	// The identity choice remains valid even if downloading its image temporarily fails.
	a.teamLogoMu.Lock()
	a.teamLogoCache.Selections[teamLogoKey(query)] = href
	a.saveTeamLogosLocked()
	a.teamLogoMu.Unlock()
	return a.resolveTeamLogo(index, query, href), nil
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

	// Replace the complete snapshot only after the temporary file is durable.
	file, err := os.CreateTemp(filepath.Dir(path), ".mybets-*.tmp")
	if err != nil {
		log.Printf("[MYBETS] Błąd zapisu: %v", err)
		return false
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(data)
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err == nil {
		err = os.Rename(file.Name(), path)
	}
	log.Printf("[MYBETS] Zapisano %d zakładów do %s (sukces: %v)", len(bets), path, err == nil)
	return err == nil
}

// Stateless settlement survives server restarts: MyBets identity and outcome stay local.
func (a *App) RefreshMyBets(bets []MyBetItem) ([]map[string]any, error) {
	if len(bets) == 0 || len(bets) > 10 {
		return nil, fmt.Errorf("rozliczenie wymaga od 1 do 10 typów")
	}
	requests := make([]map[string]any, 0, len(bets))
	for _, bet := range bets {
		if bet.StatsSource != "FOOTYMETRICS" {
			return nil, fmt.Errorf("brak tożsamości FootyMetrics zapisanej dla typu")
		}
		teamID, err := strconv.Atoi(bet.TeamID)
		if err != nil {
			return nil, fmt.Errorf("brak ID drużyny dla typu")
		}
		requests = append(requests, map[string]any{"id": bet.ID, "fixtureId": bet.FixtureID, "playerId": bet.PlayerID, "teamId": teamID, "matchStart": bet.MatchStart, "marketType": bet.MarketType, "line": bet.Line, "isUnder": bet.IsUnder, "includeSuperSub": bet.IncludeSuperSub})
	}
	body, err := json.Marshal(map[string]any{"bets": requests})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 3*time.Minute)
	defer cancel()
	var result []map[string]any
	err = a.serverRequest(ctx, http.MethodPost, "/api/v1/mybets/settle", body, &result)
	return result, err
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
	job.MatchChoices = append([]FootyMatchChoice(nil), job.MatchChoices...)
	return job
}

func (a *App) SelectFootyMatch(analysisID string, fixtureID int) error {
	a.analysisMu.Lock()
	active := a.analysisActive && a.analysisProgress.ID == analysisID && a.analysisProgress.Status == "waiting_match"
	a.analysisMu.Unlock()
	if !active || fixtureID <= 0 {
		return fmt.Errorf("brak aktywnej analizy oczekującej na wybór meczu")
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 15*time.Second)
	defer cancel()
	body, _ := json.Marshal(map[string]int{"fixtureId": fixtureID})
	var accepted bool
	if err := a.serverRequest(ctx, http.MethodPost, "/api/v1/analyses/"+url.PathEscape(analysisID)+"/footy-match", body, &accepted); err != nil {
		return err
	}
	if !accepted {
		return fmt.Errorf("serwer nie potwierdził wyboru meczu")
	}
	return nil
}

// Wails waits locally while the server performs an independent asynchronous job.
// Progress polls are small. Result downloads have their own longer timeout.
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
	var downloadedResult []AnalyzedBet
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
		err = a.serverRequest(pollCtx, http.MethodGet, "/api/v1/analyses/"+url.PathEscape(id)+"?summary=1", nil, &nextJob)
		pollCancel()
		if err == nil && (nextJob.ResultRevision > lastRevision || nextJob.Status == "completed") {
			if nextJob.ResultRevision > lastRevision || downloadedResult == nil {
				state := nextJob
				state.Status, state.Stage = "running", "downloading"
				state.Message = "Pobieranie wyniku analizy z serwera"
				a.setAnalysisProgress(state)
				transferStarted := time.Now()
				resultCtx, resultCancel := context.WithTimeout(ctx, 2*time.Minute)
				err = a.serverRequest(resultCtx, http.MethodGet, "/api/v1/analyses/"+url.PathEscape(id), nil, &nextJob)
				resultCancel()
				log.Printf("[ANALYSIS-TRANSFER] analysis=%s records=%d elapsed=%s error=%v", id, len(nextJob.Result), time.Since(transferStarted).Round(time.Millisecond), err)
				if err == nil {
					downloadedResult = nextJob.Result
				}
			} else {
				nextJob.Result = downloadedResult
			}
		}
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
func (a *App) GetAnalysisShotmaps(analysisID string, playerID int, market, mode string, limit int, from, to string) (map[string]any, error) {
	if err := a.ensureSofaRelay(a.calendarContext()); err != nil {
		return nil, err
	}
	if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(analysisID) || playerID <= 0 || limit < 0 || limit > 30 {
		return nil, fmt.Errorf("niepoprawna analiza lub zawodnik")
	}
	body, err := json.Marshal(map[string]any{"analysisId": analysisID, "playerId": playerID, "market": market, "mode": mode, "limit": limit, "from": from, "to": to})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 7*time.Minute)
	defer cancel()
	var result map[string]any
	err = a.serverRequest(ctx, http.MethodPost, "/api/v1/analyses/shotmaps", body, &result)
	return result, err
}

func (a *App) GetAnalysisSample(analysisID string, limit int, from, to string) ([]AnalyzedBet, error) {
	if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(analysisID) || limit < 1 || limit > 30 {
		return nil, fmt.Errorf("niepoprawna analiza lub długość próbki")
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 2*time.Minute)
	defer cancel()
	body, err := json.Marshal(map[string]any{"limit": limit, "from": from, "to": to})
	if err != nil {
		return nil, err
	}
	var result []AnalyzedBet
	err = a.serverRequest(ctx, http.MethodPost, "/api/v1/analyses/"+analysisID+"/sample", body, &result)
	return result, err
}

func (a *App) GetRelatedStats(analysisID string, playerID int, baseMarket, market string, limit int, from, to string) (map[string]any, error) {
	if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(analysisID) || playerID <= 0 || limit < 1 || limit > 30 {
		return nil, fmt.Errorf("niepoprawna próbka")
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 20*time.Second)
	defer cancel()
	body, err := json.Marshal(map[string]any{"analysisId": analysisID, "playerId": playerID, "baseMarket": baseMarket, "market": market, "limit": limit, "from": from, "to": to})
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
