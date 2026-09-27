package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
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
	"time"
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
	ShotType string  `json:"shotType"` // "goal", "save", "miss", "post", "blocked"
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
	ID                       int
	FullName                 string
	TeamSide                 string
	TeamID                   string
	TeamName                 string
	Matches                  int
	MetricValues             map[CanonicalMarket][]int
	MetricValuesWithSuperSub map[CanonicalMarket][]int
	Minutes                  []int
	Positions                []string
	Opponents                []string
	Dates                    []string
	MatchShots               [][]ShotCoordinate
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
		PlayerCoordinates struct {
			X float64 `json:"x"`
			Y float64 `json:"y"`
		} `json:"playerCoordinates"`
	} `json:"shotmap"`
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

var cacheMutex sync.Mutex

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
		return make(map[string]string)
	}

	var m map[string]string
	_ = json.Unmarshal(data, &m)
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
	b, _ := json.MarshalIndent(m, "", "  ")
	_ = os.WriteFile(path, b, 0644)
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
	return id, lastSegment, "UNKNOWN"
}

func parseTeamsFromSlug(slug, _ string) (string, string) {
	slug = strings.ToLower(slug)
	slug = strings.ReplaceAll(slug, "–", "-")

	if strings.Contains(slug, "-vs-") {
		pts := strings.Split(slug, "-vs-")
		return strings.TrimSpace(pts[0]), strings.TrimSpace(pts[1])
	}
	if strings.Contains(slug, "-v-") {
		pts := strings.Split(slug, "-v-")
		return strings.TrimSpace(pts[0]), strings.TrimSpace(pts[1])
	}

	pts := strings.Split(slug, "-")
	if len(pts) == 2 {
		return pts[0], pts[1]
	}

	if len(pts) == 3 {
		p0 := pts[0]
		if p0 == "fc" || p0 == "sc" || p0 == "sv" || p0 == "vfb" || p0 == "vfl" || p0 == "tsg" || p0 == "rb" || p0 == "real" {
			return pts[0] + "-" + pts[1], pts[2]
		}
		p1 := pts[1]
		if p1 == "fc" || p1 == "sc" || p1 == "sv" || p1 == "vfb" || p1 == "vfl" || p1 == "tsg" || p1 == "rb" || p1 == "real" {
			return pts[0], pts[1] + "-" + pts[2]
		}
	}

	if len(pts) >= 4 {
		if pts[0] == "real" || pts[0] == "atletico" || pts[0] == "rayo" || (pts[0] == "1" && pts[1] == "fc") {
			splitIdx := 2
			if pts[0] == "1" && pts[1] == "fc" {
				splitIdx = 3
			}
			return strings.Join(pts[:splitIdx], "-"), strings.Join(pts[splitIdx:], "-")
		}
		mid := len(pts) / 2
		return strings.Join(pts[:mid], "-"), strings.Join(pts[mid:], "-")
	}

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
		return id
	}

	for cacheSlug, id := range cache {
		if canonicalTeamCore(cacheSlug) == cleanCore {
			return id
		}
	}

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
// 3. POBIERANIE SIECIOWE (TLS BROWSER IMPERSONATION)
// ==========================================

type ResilientClient struct {
	httpClient *http.Client
	tlsClient  tls_client.HttpClient
}

func NewResilientClient() *ResilientClient {
	jar, _ := cookiejar.New(nil)
	tlsJar := tls_client.NewCookieJar()

	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(20),
		tls_client.WithClientProfile(profiles.Firefox_120),
		tls_client.WithCookieJar(tlsJar),
		tls_client.WithRandomTLSExtensionOrder(),
	}

	tlsCli, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), options...)
	if err != nil {
		panic(fmt.Sprintf("Inicjalizacja TLS klienta nie powiodła się: %v", err))
	}

	return &ResilientClient{
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
			Jar:     jar,
		},
		tlsClient: tlsCli,
	}
}

func (c *ResilientClient) GetSofa(endpoint string) ([]byte, error) {
	time.Sleep(time.Duration(90+rand.Intn(90)) * time.Millisecond)

	reqURL := endpoint
	if !strings.HasPrefix(reqURL, "http") {
		reqURL = sofaBase + endpoint
	}

	req, err := fhttp.NewRequest(fhttp.MethodGet, reqURL, nil)
	if err != nil {
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
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != fhttp.StatusOK {
		return nil, fmt.Errorf("SofaScore HTTP %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}

func SearchSofaTeamID(teamSlug string) (string, error) {
	if strings.TrimSpace(teamSlug) == "" {
		return "", fmt.Errorf("pusty slug")
	}

	client := NewResilientClient()
	queryName := strings.ReplaceAll(canonicalTeamCore(teamSlug), "-", " ")
	q := url.QueryEscape(queryName)

	body, err := client.GetSofa(fmt.Sprintf("/search/all?q=%s&page=0", q))
	if err != nil {
		return "", err
	}

	var res SofaSearchResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return "", err
	}

	teams := res.ExtractTeams()
	if len(teams) == 0 {
		parts := strings.Fields(queryName)
		if len(parts) > 1 {
			qShort := url.QueryEscape(parts[0])
			if bodyShort, errShort := client.GetSofa(fmt.Sprintf("/search/all?q=%s&page=0", qShort)); errShort == nil {
				var resShort SofaSearchResponse
				_ = json.Unmarshal(bodyShort, &resShort)
				teams = resShort.ExtractTeams()
			}
		}
	}

	if len(teams) == 0 {
		return "", fmt.Errorf("nie znaleziono drużyny dla: %s", teamSlug)
	}

	cleanTarget := canonicalTeamCore(teamSlug)
	for _, t := range teams {
		if canonicalTeamCore(t.Slug) == cleanTarget || canonicalTeamCore(t.Name) == cleanTarget {
			idStr := strconv.Itoa(t.ID)
			saveTeamCache(teamSlug, idStr)
			return idStr, nil
		}
	}

	firstID := strconv.Itoa(teams[0].ID)
	saveTeamCache(teamSlug, firstID)
	return firstID, nil
}

func FetchSuperbetMatchData(matchID string) (string, error) {
	client := NewResilientClient()
	urlStr := fmt.Sprintf("https://production-superbet-bmb.freetls.fastly.net/betbuilder/v2/getBetbuilderMarketsForMatch?match_id=%s&lang=pl-PL&target=SB_PL", matchID)

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0")
	req.Header.Set("Accept", "application/json, text/plain, */*")

	resp, err := client.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

func FetchSTSSnapshotData(fixtureID string) (string, error) {
	channel := fmt.Sprintf("f_f%s_pl", fixtureID)
	wsURL := "wss://www.sts.pl/sbk/api/sbk"
	header := http.Header{}
	header.Set("Origin", "https://www.sts.pl")
	header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(wsURL, header)
	if err != nil {
		return "", fmt.Errorf("błąd połączenia z WS STS: %w", err)
	}
	defer conn.Close()

	sub := map[string]any{
		"t": 1,
		"u": []map[string]any{
			{"s": "i_pl", "n": 0},
			{"s": channel, "n": 0},
		},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return "", err
	}

	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(4 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			_ = conn.WriteJSON(map[string]any{"t": 0})
			continue
		}
		raw := string(msg)
		if strings.Contains(raw, `"P":`) || strings.Contains(raw, fmt.Sprintf("f%s", fixtureID)) {
			return raw, nil
		}
	}
	return "", fmt.Errorf("timeout oczekiwania na ofertę STS")
}

func encodeVarint(val uint64) []byte {
	var res []byte
	for val > 127 {
		res = append(res, byte((val&0x7F)|0x80))
		val >>= 7
	}
	res = append(res, byte(val&0x7F))
	return res
}

func FetchBetclicStreamData(matchIDStr string) ([]byte, error) {
	matchID, err := strconv.ParseUint(matchIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("niepoprawne numeryczne ID Betclic: %w", err)
	}

	var body []byte
	body = append(body, encodeVarint(1<<3)...)
	body = append(body, encodeVarint(matchID)...)
	body = append(body, encodeVarint((2<<3)|2)...)
	body = append(body, encodeVarint(2)...)
	body = append(body, []byte("pl")...)
	cat := "ca_ftb_prp"
	body = append(body, encodeVarint((3<<3)|2)...)
	body = append(body, encodeVarint(uint64(len(cat)))...)
	body = append(body, []byte(cat)...)

	hdr := make([]byte, 5)
	hdr[0] = 0
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(body)))
	payload := append(hdr, body...)

	req, err := http.NewRequest("POST", "https://offering.begmedia.com/web/offering.access.api/offering.access.api.MatchService/GetMatchWithNotification", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "pl-PL")
	req.Header.Set("Appversion", "10.4.0-0")
	req.Header.Set("Content-Type", "application/grpc-web+proto")
	req.Header.Set("Origin", "https://www.betclic.pl")
	req.Header.Set("Referer", "https://www.betclic.pl/")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	req.Header.Set("X-Bg-Ref-Brand", "BETCLIC")
	req.Header.Set("X-Bg-Ref-Platform", "DESKTOP")
	req.Header.Set("X-Bg-Ref-Regulator-Zone", "PL")
	req.Header.Set("X-Bg-Regulation", "PL")
	req.Header.Set("X-Grpc-Web", "1")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Betclic sieć błąd: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Betclic HTTP status: %d (%s)", resp.StatusCode, resp.Status)
	}

	var fullBuffer bytes.Buffer
	buf := make([]byte, 32*1024)

	type readResult struct {
		n   int
		err error
	}
	ch := make(chan readResult, 1)
	idleTimeout := 2500 * time.Millisecond

	for {
		go func() {
			n, err := resp.Body.Read(buf)
			ch <- readResult{n, err}
		}()

		select {
		case res := <-ch:
			if res.n > 0 {
				fullBuffer.Write(buf[:res.n])
			}
			if res.err != nil {
				goto FinishStream
			}
		case <-time.After(idleTimeout):
			goto FinishStream
		}
	}

FinishStream:
	if fullBuffer.Len() == 0 {
		return nil, fmt.Errorf("nie odebrano żadnych danych ze strumienia Betclic")
	}

	return fullBuffer.Bytes(), nil
}

// ==========================================
// 4. SOFASCORE SERVICE (PEŁNA KADRA + HISTORIA ZAWODNIKÓW)
// ==========================================

func FetchSofaTeamSquad(teamID string) ([]SofaPlayerDetails, string, error) {
	client := NewResilientClient()

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
		}
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
		return nil, teamDisplayName, fmt.Errorf("nie udało się pobrać kadry dla drużyny %s", teamID)
	}

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
	client := NewResilientClient()

	endpoint := fmt.Sprintf("/player/%d/events/last/0", playerID)
	body, err := client.GetSofa(endpoint)
	if err != nil {
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

	for i, m := range allPlayedMatches {
		wg.Add(1)
		go func(idx int, match matchCandidate) {
			defer wg.Done()

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

			// 2. Wyszukiwanie zmiennika (SuperSub)
			var subStats SofaPlayerStats
			subPlayerID := 0
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

			// 3. Pobieranie mapy strzałów (Shotmap) dla stref i celności
			var playerZone, subZone zoneShots
			var matchShots []ShotCoordinate

			smURL := fmt.Sprintf("/event/%d/shotmap", match.EventID)
			if smBody, err := client.GetSofa(smURL); err == nil {
				var smRes SofaShotmapResponse
				if json.Unmarshal(smBody, &smRes) == nil {
					for _, shot := range smRes.Shotmap {
						inBox := isShotInsideBox(shot.PlayerCoordinates.X, shot.PlayerCoordinates.Y)
						isOnTarget := shot.ShotType == "save" || shot.ShotType == "goal"

						if shot.Player.ID == playerID {
							if inBox {
								playerZone.inside++
								if isOnTarget {
									playerZone.insideTarget++
								}
							} else {
								playerZone.outside++
								if isOnTarget {
									playerZone.outsideTarget++
								}
							}

							matchShots = append(matchShots, ShotCoordinate{
								X:        shot.PlayerCoordinates.X,
								Y:        shot.PlayerCoordinates.Y,
								ShotType: shot.ShotType,
								InBox:    inBox,
								OnTarget: isOnTarget,
								IsSub:    false,
							})
						} else if subPlayerID > 0 && shot.Player.ID == subPlayerID {
							if inBox {
								subZone.inside++
								if isOnTarget {
									subZone.insideTarget++
								}
							} else {
								subZone.outside++
								if isOnTarget {
									subZone.outsideTarget++
								}
							}

							matchShots = append(matchShots, ShotCoordinate{
								X:        shot.PlayerCoordinates.X,
								Y:        shot.PlayerCoordinates.Y,
								ShotType: shot.ShotType,
								InBox:    inBox,
								OnTarget: isOnTarget,
								IsSub:    true,
							})
						}
					}
				}
			}

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

	return hist, nil
}

func FetchSofaTeamDataComprehensive(teamID string, matchLimit int, tournamentIDs string, targetPlayerNames []string) ([]*PlayerStatHistory, string, error) {
	squad, teamDisplayName, err := FetchSofaTeamSquad(teamID)
	if err != nil {
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
	} else {
		playersToFetch = squad
	}

	var finalHistories []*PlayerStatHistory
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 6)

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
	client := NewResilientClient()
	body, err := client.GetSofa(fmt.Sprintf("/team/%s/events/next/0", homeID))
	if err != nil {
		return ""
	}

	var evRes SofaTeamEventsResponse
	if err := json.Unmarshal(body, &evRes); err != nil {
		return ""
	}

	hID, _ := strconv.Atoi(homeID)
	aID, _ := strconv.Atoi(awayID)

	for _, e := range evRes.Events {
		if (e.HomeTeam.ID == hID && e.AwayTeam.ID == aID) ||
			(e.HomeTeam.ID == aID && e.AwayTeam.ID == hID) {
			return strconv.Itoa(e.ID)
		}
	}
	return ""
}

func FetchSofaConfirmedLineups(eventID string) (map[string]bool, map[string]bool, bool) {
	if eventID == "" {
		return nil, nil, false
	}
	client := NewResilientClient()
	body, err := client.GetSofa(fmt.Sprintf("/event/%s/lineups", eventID))
	if err != nil {
		return nil, nil, false
	}

	var res SofaLineupResponse
	if err := json.Unmarshal(body, &res); err != nil || (!res.Confirmed && len(res.Home.Players) == 0) {
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
	return props, nil
}

func ParseSTSProps(rawJSON string) ([]NormalizedProp, error) {
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

	return results
}

// ==========================================
// 7. WAILS APP BRIDGE
// ==========================================

type App struct {
	ctx context.Context
}

func NewApp() *App {
	return &App{}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
}

func (a *App) LoadMatchHistory() []MatchHistoryEntry {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	data, err := os.ReadFile(getCacheFilePath("match_history.json"))
	if err != nil {
		return []MatchHistoryEntry{}
	}
	var list []MatchHistoryEntry
	_ = json.Unmarshal(data, &list)
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
		return list
	}

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
		return []MyBetItem{}
	}

	var list []MyBetItem
	if err := json.Unmarshal(data, &list); err != nil {
		return []MyBetItem{}
	}
	return list
}

func (a *App) SaveMyBets(bets []MyBetItem) bool {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("bets_cache.json")
	data, err := json.MarshalIndent(bets, "", "  ")
	if err != nil {
		return false
	}

	return os.WriteFile(path, data, 0644) == nil
}

func (a *App) SaveTeamSofaID(slug, sofaID string) bool {
	saveTeamCache(slug, sofaID)
	return true
}

func (a *App) DetectTeamsFromURL(rawURL string) TeamDetectionResult {
	_, slug, provider := extractIDAndSlug(rawURL)
	homeSlug, awaySlug := parseTeamsFromSlug(slug, provider)

	cache := loadTeamCache()
	homeID := findTeamIDInCache(homeSlug, cache)
	awayID := findTeamIDInCache(awaySlug, cache)

	if homeID == "" && homeSlug != "" {
		if id, err := SearchSofaTeamID(homeSlug); err == nil && id != "" {
			homeID = id
		}
	}
	if awayID == "" && awaySlug != "" {
		if id, err := SearchSofaTeamID(awaySlug); err == nil && id != "" {
			awayID = id
		}
	}

	return TeamDetectionResult{
		HomeSlug: homeSlug,
		AwaySlug: awaySlug,
		HomeID:   homeID,
		AwayID:   awayID,
	}
}

func (a *App) GetAvailableTournaments(homeID, awayID string) ([]LeagueOption, error) {
	client := NewResilientClient()
	body, err := client.GetSofa(fmt.Sprintf("/team/%s/standings/seasons", homeID))
	if err != nil {
		return nil, err
	}

	var res SofaSeasonsResponse
	if err := json.Unmarshal(body, &res); err != nil {
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

	return list, nil
}

func (a *App) AnalyzeThreeBookmakers(sbInput, betclicInput, stsInput, homeInput, awayInput string, matchLimit int, tournamentIDs string) ([]AnalyzedBet, error) {
	if matchLimit <= 0 {
		matchLimit = 10
	}

	homeID := strings.TrimSpace(homeInput)
	awayID := strings.TrimSpace(awayInput)

	if homeID == "" || awayID == "" {
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
				return
			}
			raw, err := FetchSuperbetMatchData(id)
			if err != nil {
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
				return
			}
			rawBytes, err := FetchBetclicStreamData(id)
			if err != nil {
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
				return
			}
			raw, err := FetchSTSSnapshotData(id)
			if err != nil {
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

	if len(allProps) == 0 {
		return nil, fmt.Errorf("brak pobranych rynków od bukmacherów (sprawdź wprowadzone linki)")
	}

	var targetPlayerNames []string
	for _, p := range allProps {
		targetPlayerNames = append(targetPlayerNames, p.PlayerName)
	}

	// SOFASCORE PEŁNA HISTORIA ZAWODNIKÓW (WSZYSTKIE LIGI I ROZGRYWKI)
	var homePlayers, awayPlayers []*PlayerStatHistory
	var homeErr, awayErr error

	fetchWg.Add(2)
	go func() {
		defer fetchWg.Done()
		homePlayers, _, homeErr = FetchSofaTeamDataComprehensive(homeID, matchLimit, tournamentIDs, targetPlayerNames)
	}()
	go func() {
		defer fetchWg.Done()
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
		return nil, fmt.Errorf("SofaScore błąd gospodarzy: %w", homeErr)
	}
	if awayErr != nil {
		return nil, fmt.Errorf("SofaScore błąd gości: %w", awayErr)
	}

	results := CalculateValueBets(allProps, homePlayers, awayPlayers, homeLineup, awayLineup)

	sort.Slice(results, func(i, j int) bool {
		if results[i].HitRate == results[j].HitRate {
			return results[i].EVWithTax > results[j].EVWithTax
		}
		return results[i].HitRate > results[j].HitRate
	})

	return results, nil
}
