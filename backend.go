package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	_ "time/tzdata"
	"unicode"
	"unicode/utf8"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"value-bets-3.0/internal/offering"
)

// ==========================================
// 1. STAŁE I STRUKTURY DANYCH
// ==========================================

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
const playerCacheVersion = 3

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
	return &cp, true
}

func savePlayerCache(key string, hist *PlayerStatHistory) {
	cacheLoadedOnce.Do(initPlayerCache)

	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()

	playerStatsCache[key] = CachedPlayerHistory{
		History:   *hist,
		CreatedAt: time.Now(),
	}

	path := getCacheFilePath("sofa_player_stats_cache.json")
	b, err := json.Marshal(playerStatsCache)
	if err == nil {
		_ = os.WriteFile(path, b, 0644)
	}
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

	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("sofa_team_cache.json")
	m := make(map[string]string)
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &m)
	}
	m[slug] = id
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		log.Printf("[CACHE] Błąd serializacji zapisu cache: %v", err)
		return
	}
	if err := os.WriteFile(path, b, 0644); err != nil {
		log.Printf("[CACHE] Błąd zapisu pliku %s: %v", path, err)
	} else {
		log.Printf("[CACHE] Zapisano do cache: %s -> ID %s", slug, id)
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

var (
	sharedSofaClient       *ResilientClient
	sharedSofaOnce         sync.Once
	globalSofaThrottle     = make(chan struct{}, 6)
	backgroundSofaThrottle = make(chan struct{}, 1)
	foregroundAnalyses     atomic.Int32
)

func GetSharedSofaClient() *ResilientClient {
	sharedSofaOnce.Do(func() {
		sharedSofaClient = NewResilientClient()
	})
	return sharedSofaClient
}

type ResilientClient struct {
	httpClient *http.Client
	tlsClient  tls_client.HttpClient
	ctx        context.Context
	background bool
}

func NewResilientClient() *ResilientClient {
	jar, _ := cookiejar.New(nil)
	tlsJar := tls_client.NewCookieJar()

	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(8),
		tls_client.WithClientProfile(profiles.Firefox_120),
		tls_client.WithCookieJar(tlsJar),
		tls_client.WithRandomTLSExtensionOrder(),
	}

	tlsCli, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), options...)
	if err != nil {
		log.Fatalf("[TLS] KRYTYCZNY BŁĄD: Inicjalizacja TLS klienta nie powiodła się: %v", err)
	}

	return &ResilientClient{
		httpClient: &http.Client{
			Timeout: 8 * time.Second,
			Jar:     jar,
		},
		tlsClient: tlsCli,
	}
}

func (c *ResilientClient) GetSofa(endpoint string) ([]byte, error) {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	for attempt := 0; ; attempt++ {
		body, err := c.getSofaAttempt(endpoint)
		if err == nil || ctx.Err() != nil {
			return body, err
		}
		goaway := strings.Contains(strings.ToLower(err.Error()), "goaway")
		if c.background {
			backgroundSofaPacer.cooldown(prefetchErrorDelay(err))
		}
		if attempt >= 1 || (!c.background && !goaway) || !retryablePrefetchError(err) {
			return nil, err
		}
		if goaway {
			c.tlsClient.CloseIdleConnections()
		}
		log.Printf("[SOFA] Ponowna próba po błędzie połączenia (%s): %v", endpoint, err)
		if !c.background {
			if err := sleepWithContext(ctx, time.Second); err != nil {
				return nil, err
			}
		}
	}
}

func (c *ResilientClient) getSofaAttempt(endpoint string) ([]byte, error) {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if c.background {
		if err := waitForPrefetch(ctx); err != nil {
			return nil, err
		}
		select {
		case backgroundSofaThrottle <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		defer func() { <-backgroundSofaThrottle }()
		if err := waitForPrefetch(ctx); err != nil {
			return nil, err
		}
		if err := backgroundSofaPacer.wait(ctx); err != nil {
			return nil, err
		}
	}
	select {
	case globalSofaThrottle <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-globalSofaThrottle }()

	sleepMs := 40 + rand.Intn(30)
	time.Sleep(time.Duration(sleepMs) * time.Millisecond)

	reqURL := endpoint
	if !strings.HasPrefix(reqURL, "http") {
		reqURL = sofaBase + endpoint
	}

	start := time.Now()
	req, err := fhttp.NewRequestWithContext(ctx, fhttp.MethodGet, reqURL, nil)
	if err != nil {
		log.Printf("[SOFA] Błąd tworzenia requesta do %s: %v", reqURL, err)
		return nil, err
	}

	req.Header = fhttp.Header{
		"user-agent":       {"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"},
		"accept":           {"*/*"},
		"accept-language":  {"en-US,en;q=0.9,pl;q=0.8"},
		"referer":          {"https://www.sofascore.com/"},
		"x-requested-with": {"c88344"},
		"sec-fetch-dest":   {"empty"},
		"sec-fetch-mode":   {"cors"},
		"sec-fetch-site":   {"same-origin"},
		"te":               {"trailers"},
		fhttp.HeaderOrderKey: {
			"user-agent",
			"accept",
			"accept-language",
			"referer",
			"x-requested-with",
			"sec-fetch-dest",
			"sec-fetch-mode",
			"sec-fetch-site",
			"te",
		},
	}

	resp, err := c.tlsClient.Do(req)
	duration := time.Since(start)
	if err != nil {
		log.Printf("[SOFA] HTTP Error dla %s po %v: %v", reqURL, duration, err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[SOFA] Błąd odczytu body z %s: %v", reqURL, err)
		return nil, err
	}

	if resp.StatusCode != fhttp.StatusOK {
		log.Printf("[SOFA] Niepoprawny status %d z %s (body len: %d)", resp.StatusCode, reqURL, len(body))
		return nil, &prefetchHTTPError{provider: "SofaScore", status: resp.StatusCode, retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())}
	}

	return body, nil
}

func SearchSofaTeamID(teamSlug string) (string, error) {
	return searchSofaTeamID(GetSharedSofaClient(), teamSlug)
}

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

func FetchSuperbetMatchData(matchID string) (string, error) {
	return fetchSuperbetMatchData(context.Background(), matchID)
}

func fetchSuperbetMatchData(ctx context.Context, matchID string) (string, error) {
	log.Printf("[SUPERBET] Pobieranie oferty dla meczu match_id=%s", matchID)
	start := time.Now()
	client := GetSharedSofaClient()
	urlStr := fmt.Sprintf("https://production-superbet-bmb.freetls.fastly.net/betbuilder/v2/getBetbuilderMarketsForMatch?match_id=%s&lang=pl-PL&target=SB_PL", matchID)

	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		log.Printf("[SUPERBET] Błąd tworzenia requesta: %v", err)
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0")
	req.Header.Set("Accept", "application/json, text/plain, */*")

	resp, err := client.httpClient.Do(req)
	if err != nil {
		log.Printf("[SUPERBET] Błąd połączenia po %v: %v", time.Since(start), err)
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", &prefetchHTTPError{provider: "Superbet", status: resp.StatusCode, retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())}
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[SUPERBET] Błąd odczytu odpowiedzi: %v", err)
		return "", err
	}
	log.Printf("[SUPERBET] Pobrano pomyślnie w %v | kod: %d | bajtów: %d", time.Since(start), resp.StatusCode, len(b))
	return string(b), err
}

func FetchSTSSnapshotData(fixtureID string) (string, error) {
	if fixtureID == "" {
		return "", fmt.Errorf("brak ID meczu STS")
	}
	raw, err := offering.FetchSTSSnapshot(context.Background(), offering.STSWebSocket, fixtureID)
	return string(raw), err
}
func FetchBetclicStreamData(matchIDStr string) ([]byte, error) {
	matchID, err := strconv.ParseUint(matchIDStr, 10, 64)
	if err != nil || matchID == 0 {
		return nil, fmt.Errorf("niepoprawne numeryczne ID Betclic: %s", matchIDStr)
	}
	body := offering.ProtoUint(1, matchID)
	body = append(body, offering.ProtoString(2, "pl")...)
	body = append(body, offering.ProtoString(3, "ca_ftb_prp")...)
	return offering.FetchBetclicRPC(context.Background(), GetSharedSofaClient().httpClient,
		offering.BetclicMatchService+"GetMatchWithNotification", body)
}

// ==========================================
// 4. SOFASCORE SERVICE (PEŁNA KADRA + HISTORIA ZAWODNIKÓW)
// ==========================================

func FetchSofaTeamSquad(teamID string) ([]SofaPlayerDetails, string, error) {
	return fetchSofaTeamSquad(GetSharedSofaClient(), teamID)
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

func FetchSofaPlayerCompleteHistory(playerID int, matchLimit int, allowedTournamentIDs string) (*PlayerStatHistory, error) {
	return fetchSofaPlayerCompleteHistory(GetSharedSofaClient(), playerID, matchLimit, allowedTournamentIDs)
}

func fetchSofaPlayerCompleteHistory(client *ResilientClient, playerID int, matchLimit int, allowedTournamentIDs string) (*PlayerStatHistory, error) {
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
			dStr = time.Unix(e.StartTS, 0).Format("02.01")
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
			matchSem <- struct{}{}
			defer func() { <-matchSem }()

			// 1. Statystyki bazowe zawodnika
			statURL := fmt.Sprintf("/event/%d/player/%d/statistics", match.EventID, playerID)
			sBody, err := client.GetSofa(statURL)
			if err != nil {
				return
			}
			var pStat SofaPlayerMatchStatResponse
			if err := json.Unmarshal(sBody, &pStat); err != nil || pStat.Statistics.MinutesPlayed <= 0 {
				return
			}

			// 2. Wyszukiwanie zmiennika (SuperSub) tylko gdy zagrał poniżej pełnych 90 minut
			var subStats SofaPlayerStats
			subPlayerID := 0
			if pStat.Statistics.MinutesPlayed < 85 {
				incURL := fmt.Sprintf("/event/%d/incidents", match.EventID)
				if incBody, err := client.GetSofa(incURL); err == nil {
					var incRes SofaIncidentResponse
					if json.Unmarshal(incBody, &incRes) == nil {
						for _, inc := range incRes.Incidents {
							if inc.IncidentType == "substitution" && inc.PlayerOut.ID == playerID {
								subPlayerID = inc.PlayerIn.ID
								break
							}
						}
						if subPlayerID > 0 {
							subURL := fmt.Sprintf("/event/%d/player/%d/statistics", match.EventID, subPlayerID)
							if subBody, err := client.GetSofa(subURL); err == nil {
								var subStatRes SofaPlayerMatchStatResponse
								if json.Unmarshal(subBody, &subStatRes) == nil {
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
				if smBody, err := client.GetSofa(smURL); err == nil {
					var smRes SofaShotmapResponse
					if json.Unmarshal(smBody, &smRes) == nil {
						matchShots = smRes.PlayerShots(playerID, subPlayerID)
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

	log.Printf("[SOFA-PLAYER] Zakończono zbieranie statystyk gracza %d. Zebrano mecze: %d. Zapisuję do cache (TTL: 6h)...", playerID, hist.Matches)
	savePlayerCache(cacheKey, hist)
	return hist, nil
}

func FetchSofaTeamDataComprehensive(teamID string, matchLimit int, tournamentIDs string, targetPlayerNames []string) ([]*PlayerStatHistory, string, error) {
	log.Printf("[COMPREHENSIVE] Analiza kompleksowa drużyny %s (wymagani gracze: %d)", teamID, len(targetPlayerNames))
	squad, teamDisplayName, err := FetchSofaTeamSquad(teamID)
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

	var finalHistories []*PlayerStatHistory
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 2)

	for _, p := range playersToFetch {
		wg.Add(1)
		go func(player SofaPlayerDetails) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			h, err := FetchSofaPlayerCompleteHistory(player.ID, matchLimit, tournamentIDs)
			if err != nil || h == nil || h.Matches == 0 {
				return
			}
			h.FullName = player.Name
			h.TeamID = teamID
			h.TeamName = teamDisplayName

			mu.Lock()
			finalHistories = append(finalHistories, h)
			mu.Unlock()
		}(p)
	}

	wg.Wait()
	log.Printf("[COMPREHENSIVE] Zakończono analizę zespołu %s. Zwrócono %d profili statystycznych", teamDisplayName, len(finalHistories))
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

func FindSofaUpcomingEventID(homeID, awayID string) string {
	log.Printf("[SOFA-EVENT] Szukanie nadchodzącego meczu bezpośredniego pomiędzy %s a %s", homeID, awayID)
	client := GetSharedSofaClient()
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

func FetchSofaConfirmedLineups(eventID string) (map[string]bool, map[string]bool, bool) {
	if eventID == "" {
		return nil, nil, false
	}
	log.Printf("[SOFA-LINEUP] Pobieranie składów dla meczu eventID=%s", eventID)
	client := GetSharedSofaClient()
	body, err := client.GetSofa(fmt.Sprintf("/event/%s/lineups", eventID))
	if err != nil {
		log.Printf("[SOFA-LINEUP] Błąd pobierania składów: %v", err)
		return nil, nil, false
	}

	var res SofaLineupResponse
	if err := json.Unmarshal(body, &res); err != nil || (!res.Confirmed && len(res.Home.Players) == 0) {
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
			lenVal := int(l)
			if pos+lenVal > len(buf) {
				return nodes
			}
			slice := buf[pos : pos+lenVal]
			pos += lenVal

			if utf8.Valid(slice) && !bytes.ContainsAny(slice, "\x00\x01\x02\x03\x04\x05") && len(bytes.TrimSpace(slice)) > 0 {
				nodes = append(nodes, protoNode{fieldNo: fieldNo, wireType: wireType, text: strings.TrimSpace(string(slice))})
			} else {
				nodes = append(nodes, protoNode{fieldNo: fieldNo, wireType: wireType, children: parseProtoNodes(slice)})
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

var strokeLetterReplacer = strings.NewReplacer(
	"ł", "l", "Ł", "L",
	"đ", "d", "Đ", "D",
	"ø", "o", "Ø", "O",
	"æ", "ae", "Æ", "AE",
	"œ", "oe", "Œ", "OE",
	"ß", "ss",
)

func removeDiacritics(s string) string {
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	res, _, _ := transform.String(t, s)
	return strokeLetterReplacer.Replace(res)
}

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

// ==========================================
// 7. WAILS APP BRIDGE
// ==========================================

type App struct {
	ctx                context.Context
	prefetchMu         sync.Mutex
	prefetchCancel     context.CancelFunc
	prefetchGeneration int
	prefetchDay        string
	prefetchLimit      int
	prefetchExcluded   map[string]bool
	prefetchProgress   PrefetchProgress
}

func NewApp() *App {
	return &App{}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	log.Printf("[APP] Wails App pomyślnie wystartował")
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

func (a *App) SaveTeamSofaID(slug, sofaID string) bool {
	log.Printf("[BRIDGE] Ręczny zapis relacji: slug '%s' -> sofaID '%s'", slug, sofaID)
	saveTeamCache(slug, sofaID)
	return true
}

func (a *App) DetectTeamsFromURL(rawURL string) TeamDetectionResult {
	log.Printf("[DETECT] Wykrywanie zespołów z adresu URL: %s", rawURL)
	_, slug, provider := extractIDAndSlug(rawURL)
	homeSlug, awaySlug := parseTeamsFromSlug(slug, provider)
	return a.detectTeamsBySlugs(homeSlug, awaySlug)
}

// DetectCalendarTeams uses the names supplied by the calendar rather than
// trying to infer team boundaries from a bookmaker's URL.
func (a *App) DetectCalendarTeams(home, away string) TeamDetectionResult {
	return a.detectTeamsBySlugs(calendarTeamSlug(home), calendarTeamSlug(away))
}

func (a *App) detectTeamsBySlugs(homeSlug, awaySlug string) TeamDetectionResult {
	foregroundAnalyses.Add(1)
	defer foregroundAnalyses.Add(-1)

	cache := loadTeamCache()
	homeID := findTeamIDInCache(homeSlug, cache)
	awayID := findTeamIDInCache(awaySlug, cache)

	if homeID == "" && homeSlug != "" {
		log.Printf("[DETECT] Brak ID gospodarzy (%s) w cache. Zapytanie do SofaScore API...", homeSlug)
		if id, err := SearchSofaTeamID(homeSlug); err == nil && id != "" {
			homeID = id
		}
	}
	if awayID == "" && awaySlug != "" {
		log.Printf("[DETECT] Brak ID gości (%s) w cache. Zapytanie do SofaScore API...", awaySlug)
		if id, err := SearchSofaTeamID(awaySlug); err == nil && id != "" {
			awayID = id
		}
	}

	log.Printf("[DETECT] Wynik detekcji: Gospodarze[%s -> ID:%s], Goście[%s -> ID:%s]", homeSlug, homeID, awaySlug, awayID)
	return TeamDetectionResult{
		HomeSlug: homeSlug,
		AwaySlug: awaySlug,
		HomeID:   homeID,
		AwayID:   awayID,
	}
}

func (a *App) GetAvailableTournaments(homeID, awayID string) ([]LeagueOption, error) {
	foregroundAnalyses.Add(1)
	defer foregroundAnalyses.Add(-1)
	log.Printf("[TOURNAMENTS] Pobieranie lig dla homeID=%s", homeID)
	client := GetSharedSofaClient()
	body, err := client.GetSofa(fmt.Sprintf("/team/%s/standings/seasons", homeID))
	if err != nil {
		log.Printf("[TOURNAMENTS] Błąd pobierania sezonów: %v", err)
		return nil, err
	}

	var res SofaSeasonsResponse
	if err := json.Unmarshal(body, &res); err != nil {
		log.Printf("[TOURNAMENTS] Błąd deserializacji sezonów: %v", err)
		return nil, err
	}

	seen := make(map[int]bool)
	var list []LeagueOption

	for _, ts := range res.TournamentSeasons {
		tID := ts.Tournament.UniqueTournament.ID
		tName := ts.Tournament.UniqueTournament.Name
		if tID > 0 && !seen[tID] {
			seen[tID] = true
			list = append(list, LeagueOption{ID: tID, Name: tName})
		}
	}

	log.Printf("[TOURNAMENTS] Znaleziono %d dostępnych rozgrywek dla drużyny %s", len(list), homeID)
	return list, nil
}

func (a *App) AnalyzeThreeBookmakers(sbInput, betclicInput, stsInput, homeInput, awayInput string, matchLimit int, tournamentIDs string) ([]AnalyzedBet, error) {
	foregroundAnalyses.Add(1)
	defer foregroundAnalyses.Add(-1)
	startGlobal := time.Now()
	log.Printf("[ANALYZE-START] Rozpoczęcie kompleksowej analizy 3 bukmacherów:")
	log.Printf("  • Superbet: '%s'", sbInput)
	log.Printf("  • Betclic:  '%s'", betclicInput)
	log.Printf("  • STS:      '%s'", stsInput)
	log.Printf("  • Home/Away ID: '%s' / '%s' | Limit meczów: %d | Turnieje: %s", homeInput, awayInput, matchLimit, tournamentIDs)

	if matchLimit <= 0 {
		matchLimit = 10
	}

	homeID := strings.TrimSpace(homeInput)
	awayID := strings.TrimSpace(awayInput)

	if homeID == "" || awayID == "" {
		log.Printf("[ANALYZE] Brakuje ID drużyn w inputach. Próba automatycznej detekcji...")
		sampleURL := sbInput
		if sampleURL == "" {
			sampleURL = betclicInput
		}
		if sampleURL == "" {
			sampleURL = stsInput
		}
		if sampleURL != "" {
			det := a.DetectTeamsFromURL(sampleURL)
			if homeID == "" {
				homeID = det.HomeID
			}
			if awayID == "" {
				awayID = det.AwayID
			}
		}
	}

	teamIDPattern := regexp.MustCompile(`^\d+$`)
	if !teamIDPattern.MatchString(homeID) || !teamIDPattern.MatchString(awayID) {
		log.Printf("[ANALYZE] BŁĄD WALIDACJI: Nie udało się ustalić ID zespołów (Home: '%s', Away: '%s')", homeID, awayID)
		return nil, fmt.Errorf("nie udało się ustalić ID zespołów w SofaScore (Gospodarze: %s, Goście: %s)", homeID, awayID)
	}

	var allProps []NormalizedProp
	var propsMu sync.Mutex
	var fetchWg sync.WaitGroup

	// SUPERBET
	if strings.TrimSpace(sbInput) != "" {
		fetchWg.Add(1)
		go func() {
			defer fetchWg.Done()
			id, _, _ := extractIDAndSlug(sbInput)
			if id == "" {
				log.Printf("[ANALYZE-SB] Brak ID meczu w linku: %s", sbInput)
				return
			}
			raw, err := FetchSuperbetMatchData(id)
			if err != nil {
				log.Printf("[ANALYZE-SB] Błąd pobierania danych: %v", err)
				return
			}
			props, err := ParseSuperbetProps(raw)
			if err == nil {
				propsMu.Lock()
				allProps = append(allProps, props...)
				propsMu.Unlock()
			}
		}()
	}

	// BETCLIC
	if strings.TrimSpace(betclicInput) != "" {
		fetchWg.Add(1)
		go func() {
			defer fetchWg.Done()
			id, _, _ := extractIDAndSlug(betclicInput)
			if id == "" {
				log.Printf("[ANALYZE-BETCLIC] Brak ID meczu w linku: %s", betclicInput)
				return
			}
			rawBytes, err := FetchBetclicStreamData(id)
			if err != nil {
				log.Printf("[ANALYZE-BETCLIC] Błąd strumienia: %v", err)
				return
			}
			props := ParseBetclicProps(rawBytes)
			propsMu.Lock()
			allProps = append(allProps, props...)
			propsMu.Unlock()
		}()
	}

	// STS
	if strings.TrimSpace(stsInput) != "" {
		fetchWg.Add(1)
		go func() {
			defer fetchWg.Done()
			id, _, _ := extractIDAndSlug(stsInput)
			if id == "" {
				log.Printf("[ANALYZE-STS] Brak ID meczu w linku: %s", stsInput)
				return
			}
			raw, err := FetchSTSSnapshotData(id)
			if err != nil {
				log.Printf("[ANALYZE-STS] Błąd WebSocket: %v", err)
				return
			}
			props, err := ParseSTSProps(raw)
			if err == nil {
				propsMu.Lock()
				allProps = append(allProps, props...)
				propsMu.Unlock()
			}
		}()
	}

	fetchWg.Wait()
	log.Printf("[ANALYZE] Zakończono pobieranie z bukmacherów. Zebrano łącznie %d rynków", len(allProps))

	if len(allProps) == 0 {
		log.Printf("[ANALYZE] KRYTYCZNY BŁĄD: Brak pobranych rynków z podanych linków!")
		return nil, fmt.Errorf("brak pobranych rynków od bukmacherów (sprawdź wprowadzone linki)")
	}

	var targetPlayerNames []string
	for _, p := range allProps {
		targetPlayerNames = append(targetPlayerNames, p.PlayerName)
	}

	// SOFASCORE PEŁNA HISTORIA ZAWODNIKÓW
	var homePlayers, awayPlayers []*PlayerStatHistory
	var homeErr, awayErr error

	fetchWg.Add(2)
	go func() {
		defer fetchWg.Done()
		log.Printf("[ANALYZE] Uruchamianie pobierania historii graczy Gospodarzy (ID: %s)", homeID)
		homePlayers, _, homeErr = FetchSofaTeamDataComprehensive(homeID, matchLimit, tournamentIDs, targetPlayerNames)
	}()
	go func() {
		defer fetchWg.Done()
		log.Printf("[ANALYZE] Uruchamianie pobierania historii graczy Gości (ID: %s)", awayID)
		awayPlayers, _, awayErr = FetchSofaTeamDataComprehensive(awayID, matchLimit, tournamentIDs, targetPlayerNames)
	}()

	// SOFASCORE POTWIERDZONE SKŁADY
	var homeLineup, awayLineup map[string]bool
	fetchWg.Add(1)
	go func() {
		defer fetchWg.Done()
		evID := FindSofaUpcomingEventID(homeID, awayID)
		if evID != "" {
			hMap, aMap, ok := FetchSofaConfirmedLineups(evID)
			if ok {
				homeLineup = hMap
				awayLineup = aMap
			}
		}
	}()

	fetchWg.Wait()

	if homeErr != nil {
		log.Printf("[ANALYZE] Błąd SofaScore dla gospodarzy: %v", homeErr)
		return nil, fmt.Errorf("SofaScore błąd gospodarzy: %w", homeErr)
	}
	if awayErr != nil {
		log.Printf("[ANALYZE] Błąd SofaScore dla gości: %v", awayErr)
		return nil, fmt.Errorf("SofaScore błąd gości: %w", awayErr)
	}

	log.Printf("[ANALYZE] Dane zebrane. Rozpoczynanie obliczeń matematycznych EV...")
	results := CalculateValueBets(allProps, homePlayers, awayPlayers, homeLineup, awayLineup)

	sort.Slice(results, func(i, j int) bool {
		if results[i].HitRate == results[j].HitRate {
			return results[i].EVWithTax > results[j].EVWithTax
		}
		return results[i].HitRate > results[j].HitRate
	})

	log.Printf("[ANALYZE-COMPLETE] Analiza zakończona pomyślnie w %v. Zwrócono %d przetworzonych typów.", time.Since(startGlobal), len(results))
	a.StartSuperbetPrefetch(matchLimit, sbInput)
	return results, nil
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

func (a *App) GetSuperbetCalendar(date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), "SUPERBET", date, false)
}

type PrefetchProgress struct {
	Running      bool   `json:"running"`
	Day          string `json:"day"`
	Completed    int    `json:"completed"`
	Total        int    `json:"total"`
	Cached       int    `json:"cached"`
	Skipped      int    `json:"skipped"`
	Failed       int    `json:"failed"`
	CurrentMatch string `json:"currentMatch"`
	PlayersDone  int    `json:"playersDone"`
	PlayersTotal int    `json:"playersTotal"`
	Message      string `json:"message"`
}

type prefetchPacer struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

var backgroundSofaPacer = prefetchPacer{interval: 1500 * time.Millisecond}
var backgroundSuperbetPacer = prefetchPacer{interval: 2 * time.Second}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func (p *prefetchPacer) wait(ctx context.Context) error {
	for {
		if err := waitForPrefetch(ctx); err != nil {
			return err
		}
		p.mu.Lock()
		delay := time.Until(p.next)
		if delay <= 0 {
			p.next = time.Now().Add(p.interval)
			p.mu.Unlock()
			return ctx.Err()
		}
		p.mu.Unlock()
		if err := sleepWithContext(ctx, delay); err != nil {
			return err
		}
	}
}

func (p *prefetchPacer) cooldown(delay time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	until := time.Now().Add(delay)
	if until.After(p.next) {
		p.next = until
	}
	log.Printf("[PREFETCH] Przerwa po błędzie: %s", delay)
}

type prefetchHTTPError struct {
	provider   string
	status     int
	retryAfter time.Duration
}

func (e *prefetchHTTPError) Error() string { return fmt.Sprintf("%s HTTP %d", e.provider, e.status) }

func parseRetryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(value); err == nil && date.After(now) {
		return date.Sub(now)
	}
	return 0
}

func prefetchErrorDelay(err error) time.Duration {
	delay := 5 * time.Second
	var response *prefetchHTTPError
	if errors.As(err, &response) {
		if response.status == 429 || response.status == 403 {
			delay = time.Minute
		} else if response.status >= 500 {
			delay = 10 * time.Second
		}
		if response.retryAfter > delay {
			delay = response.retryAfter
		}
	}
	return delay
}

func retryablePrefetchError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var response *prefetchHTTPError
	if errors.As(err, &response) {
		return response.status == 429 || response.status >= 500
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{"goaway", "timeout", "connection reset", "unexpected eof", "connection refused"} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return errors.Is(err, io.EOF)
}

func fetchSuperbetPrefetchData(ctx context.Context, id string) (string, error) {
	for attempt := 0; ; attempt++ {
		if err := backgroundSuperbetPacer.wait(ctx); err != nil {
			return "", err
		}
		raw, err := fetchSuperbetMatchData(ctx, id)
		if err == nil || ctx.Err() != nil {
			return raw, err
		}
		backgroundSuperbetPacer.cooldown(prefetchErrorDelay(err))
		if attempt >= 1 || !retryablePrefetchError(err) {
			return "", err
		}
		log.Printf("[PREFETCH] Ponowna próba oferty Superbeta dla ID=%s: %v", id, err)
	}
}

func waitForPrefetch(ctx context.Context) error {
	for foregroundAnalyses.Load() > 0 {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

func (a *App) GetPrefetchProgress() PrefetchProgress {
	a.prefetchMu.Lock()
	defer a.prefetchMu.Unlock()
	return a.prefetchProgress
}

func (a *App) setPrefetchProgress(generation int, update func(*PrefetchProgress)) {
	a.prefetchMu.Lock()
	defer a.prefetchMu.Unlock()
	if a.prefetchGeneration == generation {
		update(&a.prefetchProgress)
	}
}

func (a *App) StopSuperbetPrefetch() {
	a.prefetchMu.Lock()
	defer a.prefetchMu.Unlock()
	if a.prefetchCancel != nil {
		a.prefetchCancel()
	}
	a.prefetchGeneration++
	a.prefetchDay = ""
	a.prefetchProgress.Running = false
	a.prefetchProgress.Message = "Pobieranie w tle zatrzymane"
	log.Printf("[PREFETCH] Pobieranie w tle zatrzymane")
}

func (a *App) shutdown(ctx context.Context) { a.StopSuperbetPrefetch() }

func (a *App) StartSuperbetPrefetch(matchLimit int, currentURL string) {
	if matchLimit <= 0 {
		matchLimit = 10
	}
	if matchLimit > 30 {
		matchLimit = 30
	}
	zone, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		log.Printf("[PREFETCH] Błąd strefy czasowej: %v", err)
		return
	}
	today := time.Now().In(zone).Format("2006-01-02")
	currentID := ""
	if strings.TrimSpace(currentURL) != "" {
		currentID, _, _ = extractIDAndSlug(currentURL)
	}
	a.prefetchMu.Lock()
	if a.prefetchDay == today && a.prefetchLimit == matchLimit {
		if currentID != "" {
			a.prefetchExcluded[currentID] = true
		}
		a.prefetchMu.Unlock()
		return
	}
	if a.prefetchCancel != nil {
		a.prefetchCancel()
	}
	parent := a.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	a.prefetchCancel = cancel
	a.prefetchGeneration++
	generation := a.prefetchGeneration
	a.prefetchDay, a.prefetchLimit = today, matchLimit
	a.prefetchExcluded = map[string]bool{}
	if currentID != "" {
		a.prefetchExcluded[currentID] = true
	}
	a.prefetchProgress = PrefetchProgress{Running: true, Day: today, Message: "Pobieranie kalendarza na dziś"}
	a.prefetchMu.Unlock()
	log.Printf("[PREFETCH] Start: dziś %s, potem jutro; próbka %d meczów", today, matchLimit)
	go a.runSuperbetPrefetch(ctx, generation, today, matchLimit)
}

func (a *App) runSuperbetPrefetch(ctx context.Context, generation int, today string, matchLimit int) {
	shared := GetSharedSofaClient()
	client := &ResilientClient{httpClient: shared.httpClient, tlsClient: shared.tlsClient, ctx: ctx, background: true}
	a.runSuperbetPrefetchQueue(ctx, generation, today,
		func(ctx context.Context, date string) (*SuperbetCalendar, error) {
			return a.getBookmakerCalendar(ctx, "SUPERBET", date, false)
		},
		func(ctx context.Context, event SuperbetCalendarEvent) (string, error) {
			return a.prefetchSuperbetMatch(ctx, client, generation, event, matchLimit)
		})
}

func (a *App) runSuperbetPrefetchQueue(ctx context.Context, generation int, today string,
	loadCalendar func(context.Context, string) (*SuperbetCalendar, error),
	processMatch func(context.Context, SuperbetCalendarEvent) (string, error)) {
	defer a.setPrefetchProgress(generation, func(p *PrefetchProgress) {
		p.Running = false
		p.CurrentMatch = ""
		if ctx.Err() != nil {
			p.Message = "Pobieranie w tle zatrzymane"
		} else {
			p.Message = "Zakończono pobieranie na dziś i jutro"
		}
		log.Printf("[PREFETCH] Koniec: %d/%d; gotowe=%d, pominięte=%d, błędy=%d", p.Completed, p.Total, p.Cached, p.Skipped, p.Failed)
	})
	start, _, err := superbetCalendarDay(today)
	if err != nil {
		log.Printf("[PREFETCH] %v", err)
		return
	}
	seen := make(map[string]bool)
	for day := 0; day < 2; day++ {
		if waitForPrefetch(ctx) != nil {
			return
		}
		date := start.AddDate(0, 0, day).Format("2006-01-02")
		a.setPrefetchProgress(generation, func(p *PrefetchProgress) { p.Day = date; p.Message = "Pobieranie kalendarza " + date })
		log.Printf("[PREFETCH] Pobieranie kalendarza: %s", date)
		calendar, err := loadCalendar(ctx, date)
		if err != nil {
			log.Printf("[PREFETCH] Błąd kalendarza %s: %v", date, err)
			a.setPrefetchProgress(generation, func(p *PrefetchProgress) { p.Failed++ })
			if ctx.Err() != nil {
				return
			}
			continue
		}
		for _, warning := range calendar.Warnings {
			log.Printf("[PREFETCH] %s", warning)
		}
		var queue []SuperbetCalendarEvent
		for _, event := range calendar.Events {
			if !seen[event.ID] {
				seen[event.ID] = true
				queue = append(queue, event)
			}
		}
		a.setPrefetchProgress(generation, func(p *PrefetchProgress) { p.Total += len(queue) })
		log.Printf("[PREFETCH] %s: %d meczów w kolejce", date, len(queue))
		for _, event := range queue {
			if waitForPrefetch(ctx) != nil {
				return
			}
			a.prefetchMu.Lock()
			excluded := a.prefetchExcluded[event.ID]
			a.prefetchMu.Unlock()
			name := event.Home + " – " + event.Away
			a.setPrefetchProgress(generation, func(p *PrefetchProgress) {
				p.CurrentMatch = name
				p.PlayersDone = 0
				p.PlayersTotal = 0
				p.Message = "Pobieranie danych meczu"
			})
			log.Printf("[PREFETCH] Mecz %s (%s), ID=%s", name, date, event.ID)
			outcome := "skipped"
			if excluded {
				log.Printf("[PREFETCH] Pomijam aktualnie analizowany mecz: %s", name)
			} else if !event.BetBuilder && len(calendar.Warnings) == 0 {
				log.Printf("[PREFETCH] Pomijam mecz bez BetBuildera: %s", name)
			} else {
				outcome, err = processMatch(ctx, event)
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					outcome = "failed"
					log.Printf("[PREFETCH] Błąd %s: %v", name, err)
				}
			}
			a.setPrefetchProgress(generation, func(p *PrefetchProgress) {
				p.Completed++
				switch outcome {
				case "cached":
					p.Cached++
				case "failed":
					p.Failed++
				default:
					p.Skipped++
				}
				log.Printf("[PREFETCH] Postęp %d/%d: %s (%s)", p.Completed, p.Total, name, outcome)
			})
		}
	}
}

func (a *App) prefetchSuperbetMatch(ctx context.Context, client *ResilientClient, generation int, event SuperbetCalendarEvent, matchLimit int) (string, error) {
	raw, err := fetchSuperbetPrefetchData(ctx, event.ID)
	if err != nil {
		return "failed", err
	}
	props, err := ParseSuperbetProps(raw)
	if err != nil {
		return "failed", err
	}
	if len(props) == 0 {
		log.Printf("[PREFETCH] Brak rynków zawodników dla %s – %s", event.Home, event.Away)
		return "skipped", nil
	}
	log.Printf("[PREFETCH] %d rynków zawodników; przygotowuję historie", len(props))
	cache := loadTeamCache()
	players := make(map[int]SofaPlayerDetails)
	for _, team := range []string{event.Home, event.Away} {
		if waitForPrefetch(ctx) != nil {
			return "failed", ctx.Err()
		}
		id := findTeamIDInCache(team, cache)
		if id == "" {
			id, err = searchSofaTeamID(client, team)
			if err != nil {
				return "failed", err
			}
		}
		squad, _, err := fetchSofaTeamSquad(client, id)
		if err != nil {
			return "failed", err
		}
		lookup := make(map[int]string)
		aliases := make(map[string]int)
		for _, player := range squad {
			regSofaAliases(aliases, player.Name, player.ShortName, player.Slug, player.ID)
			lookup[player.ID] = player.Name
		}
		stringAliases := lookupMapInt(aliases)
		for _, prop := range props {
			matched := matchPlayerStrict(prop.PlayerName, stringAliases)
			if matched == "" {
				continue
			}
			playerID, _ := strconv.Atoi(matched)
			players[playerID] = SofaPlayerDetails{ID: playerID, Name: lookup[playerID]}
		}
	}
	if len(players) == 0 {
		log.Printf("[PREFETCH] Brak dopasowanych zawodników: %s – %s", event.Home, event.Away)
		return "skipped", nil
	}
	ids := make([]int, 0, len(players))
	for id := range players {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	a.setPrefetchProgress(generation, func(p *PrefetchProgress) { p.PlayersTotal = len(ids); p.Message = "Pobieranie historii zawodników" })
	loaded := 0
	for i, id := range ids {
		if waitForPrefetch(ctx) != nil {
			return "failed", ctx.Err()
		}
		log.Printf("[PREFETCH] Zawodnik %d/%d: %s (ID=%d)", i+1, len(ids), players[id].Name, id)
		history, err := fetchSofaPlayerCompleteHistory(client, id, matchLimit, "ALL")
		if ctx.Err() != nil {
			return "failed", ctx.Err()
		}
		if err != nil {
			log.Printf("[PREFETCH] Błąd historii %s: %v", players[id].Name, err)
		} else if history != nil && history.Matches > 0 {
			loaded++
			log.Printf("[PREFETCH] Cache gotowy: %s, %d meczów", players[id].Name, history.Matches)
		}
		a.setPrefetchProgress(generation, func(p *PrefetchProgress) { p.PlayersDone = i + 1 })
	}
	if loaded != len(ids) {
		return "failed", fmt.Errorf("pobrano historie %d/%d zawodników", loaded, len(ids))
	}
	return "cached", nil
}
