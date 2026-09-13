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
	MarketShotsTotal     CanonicalMarket = "SHOTS_TOTAL"
	MarketShotsOnTarget  CanonicalMarket = "SHOTS_ON_TARGET"
	MarketShotsBlocked   CanonicalMarket = "SHOTS_BLOCKED"
	MarketShotsOffTarget CanonicalMarket = "SHOTS_OFF_TARGET"
	MarketTackles        CanonicalMarket = "TACKLES"
	// Dispossessed: number of dispossessions (player dispossessed opponent or was dispossessed?) — uses StatsHub 'dispossessed'
	MarketDispossessed   CanonicalMarket = "DISPOSSESSED"
	MarketFoulsCommitted CanonicalMarket = "FOULS_COMMITTED"
	MarketFoulsDrawn     CanonicalMarket = "FOULS_DRAWN"
	MarketInterceptions  CanonicalMarket = "INTERCEPTIONS"
	MarketOffsides       CanonicalMarket = "OFFSIDES"
)

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

type AnalyzedBet struct {
	Bookmaker            Bookmaker       `json:"bookmaker"`
	PlayerName           string          `json:"playerName"`
	MarketType           CanonicalMarket `json:"marketType"`
	MarketLabel          string          `json:"marketLabel"`
	Line                 float64         `json:"line"`
	IsUnder              bool            `json:"isUnder"`
	BookmakerOdds        float64         `json:"bookmakerOdds"`
	FairOdds             float64         `json:"fairOdds"`
	HitRate              float64         `json:"hitRate"`
	EdgePercent          float64         `json:"edgePercent"`
	EVWithTax            float64         `json:"evWithTax"`
	MatchesSample        int             `json:"matchesSample"`
	SampleValues         []int           `json:"sampleValues"`
	SampleValuesSuperSub []int           `json:"sampleValuesSuperSub"`
	SampleMinutes        []int           `json:"sampleMinutes"`
	SamplePositions      []string        `json:"samplePositions"`
	SampleOpponents      []string        `json:"sampleOpponents"`
	SampleDates          []string        `json:"sampleDates"`
	Verdict              string          `json:"verdict"`
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

type StatsHubResponse struct {
	Data   []StatsHubPlayer `json:"data"`
	Events []StatsHubEvent  `json:"events"`
}

type StatsHubPlayer struct {
	ID    int                            `json:"id"`
	Name  string                         `json:"name"`
	Stats map[string]StatsHubMatchDetail `json:"stats"`
}

type StatsHubMatchDetail struct {
	EventID                int    `json:"eventId"`
	MinutesPlayed          int    `json:"minutesPlayed"`
	Position               string `json:"position"`
	SubstitutedOut         any    `json:"substitutedOut"`
	SubstitutedIn          any    `json:"substitutedIn"`
	Shots                  int    `json:"shots"`
	OnTargetScoringAttempt int    `json:"onTargetScoringAttempt"`
	ShotOffTarget          int    `json:"shotOffTarget"`
	BlockedScoringAttempt  int    `json:"blockedScoringAttempt"`
	// StatsHub uses "dispossessed" for the number of times a player dispossessed an opponent
	Dispossessed int `json:"dispossessed"`
	// Keep TotalTackle for backward compatibility if present
	TotalTackle     int `json:"totalTackle"`
	InterceptionWon int `json:"interceptionWon"`
	Fouls           int `json:"fouls"`
	WasFouled       int `json:"wasFouled"`
	TotalOffside    int `json:"totalOffside"`
}

type MatchHistoryEntry struct {
	Timestamp   string `json:"timestamp"`
	Title       string `json:"title"`
	SbURL       string `json:"sbUrl"`
	BetclicURL  string `json:"betclicUrl"`
	StsURL      string `json:"stsUrl"`
	HomeStatsID string `json:"homeStatsId"`
	AwayStatsID string `json:"awayStatsId"`
}

type StatsHubEvent struct {
	Events struct {
		ID                 int    `json:"id"`
		TournamentID       int    `json:"tournamentId"`
		TournamentName     string `json:"tournamentName"`
		TimeStartTimestamp int64  `json:"timeStartTimestamp"`
	} `json:"events"`
	Tournaments struct {
		ID                 int    `json:"id"`
		InternalID         int    `json:"internalId"`
		UniqueTournamentID int    `json:"uniqueTournamentId"`
		Name               string `json:"name"`
	} `json:"tournaments"`
	Tournament struct {
		ID         int    `json:"id"`
		InternalID int    `json:"internalId"`
		Name       string `json:"name"`
	} `json:"tournament"`
	HomeTeam struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	} `json:"homeTeam"`
	AwayTeam struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	} `json:"awayTeam"`
}

// ==========================================
// 2. CACHE I PRECYZYJNA DETEKCJA SLUGÓW
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

	path := getCacheFilePath("team_cache.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return make(map[string]string)
	}

	var m map[string]string
	_ = json.Unmarshal(data, &m)
	return m
}

func loadLeagueCache() map[string]string {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("league_cache.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return make(map[string]string)
	}

	var m map[string]string
	_ = json.Unmarshal(data, &m)
	return m
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

	// STS fixtures use a last segment like "f123456" — only treat as STS when
	// the segment is exactly 'f' followed by digits. Do not match general
	// slugs that simply begin with letter 'f' (e.g. 'fortuna-...').
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

func parseTeamsFromSlug(slug, provider string) (string, string) {
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

func splitTeamsFromCache(slug string, cache map[string]string) (string, string, string, string, bool) {
	parts := strings.Split(strings.Trim(slug, "-"), "-")
	if len(parts) < 2 {
		return "", "", "", "", false
	}

	type candidate struct {
		homeSlug string
		awaySlug string
		homeID   string
		awayID   string
	}
	var candidates []candidate
	for splitAt := 1; splitAt < len(parts); splitAt++ {
		homeSlug := strings.Join(parts[:splitAt], "-")
		awaySlug := strings.Join(parts[splitAt:], "-")
		homeID := findTeamIDInCache(homeSlug, cache)
		awayID := findTeamIDInCache(awaySlug, cache)
		if homeID != "" && awayID != "" {
			candidates = append(candidates, candidate{homeSlug, awaySlug, homeID, awayID})
		}
	}

	if len(candidates) != 1 {
		return "", "", "", "", false
	}

	match := candidates[0]
	fmt.Printf("[CACHE SPLIT] '%s' -> '%s' (ID: %s) vs '%s' (ID: %s)\n", slug, match.homeSlug, match.homeID, match.awaySlug, match.awayID)
	return match.homeSlug, match.awaySlug, match.homeID, match.awayID, true
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
	// Bukmacherzy często pomijają końcówkę formy prawnej klubu (np.
	// "hamburger" zamiast "hamburger-sv"). Nie jest ona częścią nazwy,
	// która rozróżnia drużynę w naszym cache.
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
		fmt.Printf("[CACHE MATCH] Ścisłe dopasowanie '%s' -> ID: %s\n", clean, id)
		return id
	}

	candidates := make(map[string][]string)
	for cacheSlug, id := range cache {
		if canonicalTeamCore(cacheSlug) == cleanCore {
			candidates[id] = append(candidates[id], cacheSlug)
		}
	}
	if len(candidates) == 1 {
		for id, slugs := range candidates {
			sort.Strings(slugs)
			fmt.Printf("[CACHE MATCH] Znormalizowane dopasowanie link='%s' -> cache='%s' -> ID: %s\n", clean, slugs[0], id)
			return id
		}
	}

	return ""
}

func unorderedTeamKey(homeID, awayID string) string {
	ids := []string{homeID, awayID}
	sort.Strings(ids)
	return strings.Join(ids, "|")
}

// displayOpponentName usuwa wyłącznie techniczne przedrostki klubów, które
// po skróceniu do jednego słowa dawały na wykresie bezużyteczne "FC" lub "1".
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
// 3. POBIERANIE SIECIOWE
// ==========================================

type ResilientClient struct {
	httpClient *http.Client
	userAgent  string
}

func NewResilientClient() *ResilientClient {
	jar, _ := cookiejar.New(nil)
	return &ResilientClient{
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
			Jar:     jar,
		},
		userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	}
}

func (c *ResilientClient) GetWithAntiBan(urlStr string, isStatsHub bool) (*http.Response, error) {
	time.Sleep(time.Duration(150+rand.Intn(150)) * time.Millisecond)

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "pl,en-US;q=0.9,en;q=0.8")

	if isStatsHub {
		req.Header.Set("Referer", "https://www.statshub.com/")
		req.Header.Set("Origin", "https://www.statshub.com")
	}

	return c.httpClient.Do(req)
}

func FetchSuperbetMatchData(matchID string) (string, error) {
	fmt.Printf("[SUPERBET] Pobieranie dla MatchID=%s...\n", matchID)
	client := NewResilientClient()
	urlStr := fmt.Sprintf("https://production-superbet-bmb.freetls.fastly.net/betbuilder/v2/getBetbuilderMarketsForMatch?match_id=%s&lang=pl-PL&target=SB_PL", matchID)
	resp, err := client.GetWithAntiBan(urlStr, false)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err == nil {
		fmt.Printf("[SUPERBET] Pobrano pomyślnie %d bajtów.\n", len(b))
	}
	return string(b), err
}

func FetchSTSSnapshotData(fixtureID string) (string, error) {
	fmt.Printf("[STS] Łączenie z WebSocket dla FixtureID=%s...\n", fixtureID)
	channel := fmt.Sprintf("f_f%s_pl", fixtureID)
	wsURL := "wss://www.sts.pl/sbk/api/sbk"
	header := http.Header{}
	header.Set("Origin", "https://www.sts.pl")
	header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

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
			fmt.Printf("[STS] Odebrano snapshot meczu (%d bajtów).\n", len(raw))
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
	fmt.Printf("[BETCLIC] Żądanie gRPC-Web dla MatchID=%s...\n", matchIDStr)
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
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
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

	fmt.Printf("[BETCLIC] Pomyślnie pobrano %d bajtów strumienia binarnego.\n", fullBuffer.Len())
	return fullBuffer.Bytes(), nil
}

// statsHubFetchLimit to ile meczów ZAWSZE pobieramy z API StatsHub, niezależnie
// od suwaka "Zakres próbki" w UI (który idzie do 30). Pobieramy z zapasem, żeby
// zmiana suwaka po stronie frontendu nie wymagała nowego zapytania do API —
// faktyczne obcięcie próbki robimy lokalnie w CalculateValueBets (maxMatches),
// na podstawie całego już pobranego zestawu meczów.
const statsHubFetchLimit = 40

func FetchStatsHubTeamData(idOrURL string, tournamentIDs string) (string, error) {
	client := NewResilientClient()
	teamIDPattern := regexp.MustCompile(`^\d+$`)
	id := strings.TrimSpace(idOrURL)

	if !teamIDPattern.MatchString(id) {
		return "", fmt.Errorf("niepoprawne ID drużyny StatsHub: %q", idOrURL)
	}

	params := url.Values{}
	params.Set("limit", strconv.Itoa(statsHubFetchLimit))
	params.Set("location", "both")

	tIDs := strings.TrimSpace(tournamentIDs)
	if tIDs == "" {
		leagueCache := loadLeagueCache()
		tIDs = strings.TrimSpace(leagueCache[id])
	}

	if tIDs != "" && tIDs != "ALL" {
		params.Set("tournamentId", tIDs)
		fmt.Printf("[STATSHUB] TeamID=%s | tournamentId=%s\n", id, tIDs)
	} else {
		fmt.Printf("[STATSHUB] TeamID=%s | brak tournamentId (pobieranie historii ogólnej)\n", id)
	}

	targetURL := fmt.Sprintf("https://www.statshub.com/api/team/%s/players/performance?%s", id, params.Encode())
	fmt.Printf("[STATSHUB] Pobieranie: %s\n", targetURL)

	resp, err := client.GetWithAntiBan(targetURL, true)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// FALLBACK 404: Gdy turniej nie ma rozegranych spotkań w wybranym oknie
	if resp.StatusCode == http.StatusNotFound && params.Get("tournamentId") != "" {
		fmt.Printf("[STATSHUB FALLBACK] Turniej %s nie zwrócił danych dla drużyny %s (HTTP 404). Pobieranie bez tournamentId...\n", params.Get("tournamentId"), id)
		params.Del("tournamentId")
		fallbackURL := fmt.Sprintf("https://www.statshub.com/api/team/%s/players/performance?%s", id, params.Encode())

		respFb, errFb := client.GetWithAntiBan(fallbackURL, true)
		if errFb != nil {
			return "", errFb
		}
		defer respFb.Body.Close()

		bFb, errFb := io.ReadAll(respFb.Body)
		if errFb == nil && respFb.StatusCode == http.StatusOK {
			fmt.Printf("[STATSHUB FALLBACK OK] Pobrano %d bajtów danych ogólnych.\n", len(bFb))
			return string(bFb), nil
		}
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		bodyPreview := strings.TrimSpace(string(b))
		if len(bodyPreview) > 300 {
			bodyPreview = bodyPreview[:300]
		}
		return "", fmt.Errorf("StatsHub HTTP %s: %s", resp.Status, bodyPreview)
	}
	return string(b), nil
}

// ==========================================
// 4. PARSERY I FILTRACJA BŁĘDNYCH RYNKÓW
// ==========================================

var numRe = regexp.MustCompile(`(\d+(?:\.\d+)?)`)

func isHalfOrPeriodMarket(s string) bool {
	low := strings.ToLower(s)
	keywords := []string{
		"połow", "polow", "half", "1st", "2nd",
		"1. p", "2. p", "1.p", "2.p", "1 p", "2 p",
		"dogryw", "extra time", "kwart", "tercj",
	}
	for _, kw := range keywords {
		if strings.Contains(low, kw) {
			return true
		}
	}
	return false
}

func isUnsupportedMethodOrZone(s string) bool {
	low := strings.ToLower(s)
	markers := []string{
		"lewą nog", "lewa nog", "lewa", "lewej nog",
		"prawą nog", "prawa nog", "prawa", "prawej nog",
		"nogą", "noga", "nogi",
		"głow", "glow", "head",
		"pola karnego", "z pola", "spoza",
		"podan", "podania", "podań", "pass",
		"kartek", "kartk", "card",
	}
	for _, m := range markers {
		if strings.Contains(low, m) {
			return true
		}
	}
	return false
}

func mapMarketCanonical(raw string) (CanonicalMarket, bool) {
	s := strings.ToLower(raw)
	// also check a diacritics-stripped version for Polish phrases
	sd := removeDiacritics(s)

	if strings.Contains(s, "drużyn") || strings.Contains(s, "zespół") || strings.Contains(s, "mecz") {
		return "", false
	}
	if isHalfOrPeriodMarket(raw) {
		return "", false
	}
	if isUnsupportedMethodOrZone(raw) {
		return "", false
	}

	// Specific Polish phrase mapping must be checked before more general tokens.
	// Map explicit "liczba odbiorów na zawodniku" -> DISPOSSESSED
	if strings.Contains(sd, "liczba odbiorow na zawodniku") || strings.Contains(sd, "liczba odbiorow na zawodnik") {
		return MarketDispossessed, true
	}

	switch {
	case strings.Contains(s, "zablokowan"):
		return MarketShotsBlocked, true
	case strings.Contains(s, "nieceln") || strings.Contains(s, "poza światło") || strings.Contains(s, "poza bramk"):
		return MarketShotsOffTarget, true
	case strings.Contains(s, "celn"):
		return MarketShotsOnTarget, true
	case strings.Contains(s, "strzał"):
		return MarketShotsTotal, true
	// English dispossessed keywords
	case strings.Contains(s, "disposs") || strings.Contains(s, "dispos") || strings.Contains(s, "dispossession"):
		return MarketDispossessed, true
	// General Polish "odbior" -> TACKLES
	case strings.Contains(sd, "odbior") || strings.Contains(s, "odbior"):
		return MarketTackles, true
	// Polish phrases indicating fouls drawn (wywalczone) but avoid catching the
	// specific "liczba odbiorow na zawodniku" handled above.
	case strings.Contains(s, "wywalczon") || strings.Contains(sd, "na zawodniku") || strings.Contains(s, "faulowany"):
		return MarketFoulsDrawn, true
	case strings.Contains(s, "faul"):
		return MarketFoulsCommitted, true
	case strings.Contains(s, "przechwyt"):
		return MarketInterceptions, true
	case strings.Contains(s, "spalon"):
		return MarketOffsides, true
	default:
		return "", false
	}
}

// --- SUPERBET ---
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
				// Dzielimy po " - " (spacja-myślnik-spacja), a NIE po samym "-",
				// bo nazwiska zawodników (np. "Pierre-Emerick Aubameyang") mogą
				// zawierać własny myślnik i zostałyby ucięte w złym miejscu.
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
	fmt.Printf("[SUPERBET] Sparsowano %d poprawnych rynków zawodników.\n", len(props))
	return props, nil
}

// --- STS ---
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

	var props []NormalizedProp
	for bundleKey, v := range pData {
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

				if isHalfOrPeriodMarket(rawName) || isHalfOrPeriodMarket(bundleKey) {
					continue
				}
				if isUnsupportedMethodOrZone(rawName) || isUnsupportedMethodOrZone(bundleKey) {
					continue
				}

				pName, mType := rawName, bundleKey
				if strings.Contains(rawName, " - ") {
					pts := strings.SplitN(rawName, " - ", 2)
					pName = strings.TrimSpace(pts[0])
					mType = strings.TrimSpace(pts[1])
				}

				canon, ok := mapMarketCanonical(mType)
				if !ok {
					continue
				}

				outcomes, _ := lMap["o"].(map[string]any)
				for outID, outVal := range outcomes {
					outMap, _ := outVal.(map[string]any)
					lineStr, _ := outMap["n"].(string)
					odds, _ := outMap["O"].(float64)

					if isHalfOrPeriodMarket(lineStr) || isUnsupportedMethodOrZone(lineStr) {
						continue
					}

					lineVal := 0.5
					lowerLine := strings.ToLower(strings.TrimSpace(lineStr))
					isUnder := strings.Contains(lowerLine, "poniżej") || strings.Contains(lowerLine, "under") || strings.Contains(lowerLine, "-")

					if strings.HasSuffix(lowerLine, "+") {
						valStr := strings.TrimSuffix(lowerLine, "+")
						if parsed, err := strconv.ParseFloat(valStr, 64); err == nil {
							lineVal = parsed - 0.5
							isUnder = false
						}
					} else if m := numRe.FindStringSubmatch(lineStr); len(m) > 1 {
						parsed, _ := strconv.ParseFloat(m[1], 64)
						lineVal = parsed
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
	fmt.Printf("[STS] Sparsowano %d poprawnych rynków zawodników.\n", len(props))
	return props, nil
}

// --- BETCLIC ---
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

	fmt.Printf("[BETCLIC] Rozpakowano %d ramek gRPC.\n", len(frames))

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

	fmt.Printf("[BETCLIC] Sparsowano %d poprawnych unikalnych rynków zawodników.\n", len(unique))
	return unique
}

// ==========================================
// 5. SILNIK POISSONA I STATYSTYK Z SUPERSUB
// ==========================================

func removeDiacritics(s string) string {
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	res, _, _ := transform.String(t, s)
	return res
}

func NormalizePlayerName(s string) string {
	s = removeDiacritics(s)
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, ",", " ")
	s = strings.ReplaceAll(s, ".", " ")
	s = strings.ReplaceAll(s, "-", " ")
	// Collapse multiple spaces and remove purely numeric tokens (like '1' or '2') which
	// appear in bookmaker labels ("1. drużyna").
	parts := strings.Fields(s)
	var filtered []string
	for _, p := range parts {
		if _, err := strconv.Atoi(p); err == nil {
			continue
		}
		filtered = append(filtered, p)
	}
	return strings.TrimSpace(strings.Join(filtered, " "))
}

func matchPlayerStrict(targetName string, playerMap map[string]string) string {
	// reuse scoring helper
	_ = playerMap
	return matchPlayerWithScoring(targetName, playerMap)
}

// matchPlayerWithScoring implements the heuristic matching used previously
func matchPlayerWithScoring(targetName string, playerMap map[string]string) string {
	targetNorm := NormalizePlayerName(targetName)
	if targetNorm == "" {
		return ""
	}
	// Exact normalized match
	if id, ok := playerMap[targetNorm]; ok {
		return id
	}

	parts := strings.Fields(targetNorm)
	if len(parts) == 0 {
		return ""
	}
	surname := parts[len(parts)-1]

	// Collect candidates that contain any token from the target name (robust for
	// formats like 'Surname1 Surname2 Givenname' where the last token isn't the
	// true surname). Fall back to surname-only behavior if that yields a single
	// match.
	var candidateNames []string
	var candidateIDs []string
	for fullName, id := range playerMap {
		matchAny := false
		for _, t := range parts {
			if strings.Contains(fullName, t) {
				matchAny = true
				break
			}
		}
		if matchAny {
			candidateNames = append(candidateNames, fullName)
			candidateIDs = append(candidateIDs, id)
		}
	}
	// If no candidates found via any-token, fall back to surname-only search
	if len(candidateIDs) == 0 {
		for fullName, id := range playerMap {
			if strings.Contains(fullName, surname) {
				candidateNames = append(candidateNames, fullName)
				candidateIDs = append(candidateIDs, id)
			}
		}
	}
	if len(candidateIDs) == 1 {
		return candidateIDs[0]
	}

	// Heuristic scoring: prefer candidate with most token overlap and matching first-initial
	bestIdx := -1
	bestScore := 0
	bestCount := 0
	for i, cand := range candidateNames {
		score := 0
		candTokens := strings.Fields(cand)
		for _, p := range parts {
			for _, ct := range candTokens {
				if p == ct {
					score++
					break
				}
			}
		}
		// bonus for matching first-initial when available
		if len(parts) > 0 && len(candTokens) > 0 {
			r1 := []rune(parts[0])
			r2 := []rune(candTokens[0])
			if len(r1) > 0 && len(r2) > 0 && r1[0] == r2[0] {
				score += 1
			}
		}
		if score > bestScore {
			bestScore = score
			bestIdx = i
			bestCount = 1
		} else if score == bestScore {
			bestCount++
		}
	}

	if bestScore > 0 && bestCount == 1 && bestIdx >= 0 {
		return candidateIDs[bestIdx]
	}

	return ""
}

// findTopPlayerCandidates returns up to n top candidate names with score for diagnostics
func findTopPlayerCandidates(targetNorm string, playerMap map[string]string, n int) []string {
	parts := strings.Fields(targetNorm)
	if len(parts) == 0 {
		return nil
	}
	surname := parts[len(parts)-1]
	type candScore struct {
		name  string
		score int
	}
	var list []candScore
	for fullName := range playerMap {
		score := 0
		if strings.Contains(fullName, surname) {
			score += 2
		}
		for _, p := range parts {
			for _, ct := range strings.Fields(fullName) {
				if p == ct {
					score++
					break
				}
			}
		}
		if score > 0 {
			list = append(list, candScore{fullName, score})
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].score > list[j].score })
	res := make([]string, 0, n)
	for i := 0; i < len(list) && i < n; i++ {
		res = append(res, fmt.Sprintf("%s(%d)", list[i].name, list[i].score))
	}
	return res
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

type PlayerStatHistory struct {
	ID                       int
	FullName                 string
	Matches                  int
	MetricValues             map[CanonicalMarket][]int
	MetricValuesWithSuperSub map[CanonicalMarket][]int
	Minutes                  []int
	Positions                []string
	Opponents                []string
	Dates                    []string
}

// extractPlayerID zwraca ID zawodnika zapisane w polu substitutedIn/substitutedOut.
// UWAGA: te pola w danych ze StatsHub NIE są boolami — trzymają konkretne ID
// zawodnika po drugiej stronie zmiany (kto wszedł / kto zszedł). null oznacza
// brak zmiany. Format bez liczbowego ID (np. bool lub mapa) nie może zostać
// bezpiecznie sparowany, więc jest pomijany.
func extractPlayerID(val any) (int, bool) {
	switch v := val.(type) {
	case float64:
		if v > 0 {
			return int(v), true
		}
	case int:
		if v > 0 {
			return v, true
		}
	case string:
		s := strings.TrimSpace(v)
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n, true
		}
	}
	return 0, false
}

func CalculateValueBets(props []NormalizedProp, homeSH_JSON, awaySH_JSON string, maxMatches int, tournamentIDs string, homeTeamID, awayTeamID string) ([]AnalyzedBet, error) {
	var homeSH, awaySH StatsHubResponse
	_ = json.Unmarshal([]byte(homeSH_JSON), &homeSH)
	_ = json.Unmarshal([]byte(awaySH_JSON), &awaySH)

	playerHistory := make(map[string]*PlayerStatHistory)
	strictMap := make(map[string]string)

	indexSH := func(sh StatsHubResponse, ownTeamID string) {
		type EventMeta struct {
			Timestamp    int64
			TournamentID int
			Opponent     string
			DateStr      string
		}
		eventMetaMap := make(map[int]EventMeta)
		allowedEventIDs := make(map[int]bool)

		selectedMap := make(map[int]bool)
		if tournamentIDs != "" && tournamentIDs != "ALL" {
			for _, tidStr := range strings.Split(tournamentIDs, ",") {
				if tid, err := strconv.Atoi(strings.TrimSpace(tidStr)); err == nil {
					selectedMap[tid] = true
				}
			}
		}

		for _, e := range sh.Events {
			eID := e.Events.ID
			if eID == 0 {
				eID = e.Tournaments.ID
			}
			if eID == 0 {
				eID = e.Tournament.ID
			}

			tID := e.Tournaments.InternalID
			if tID == 0 {
				tID = e.Tournament.InternalID
			}
			if tID == 0 {
				tID = e.Tournaments.ID
			}
			if tID == 0 {
				tID = e.Tournament.ID
			}

			if len(selectedMap) == 0 || selectedMap[tID] {
				allowedEventIDs[eID] = true
			}

			dStr := ""
			if e.Events.TimeStartTimestamp > 0 {
				t := time.Unix(e.Events.TimeStartTimestamp, 0)
				dStr = t.Format("02.01")
			}

			opp := "Mecz"
			if e.AwayTeam.Name != "" && e.HomeTeam.Name != "" {
				// UWAGA: wcześniej zawsze brano AwayTeam.Name, niezależnie od tego,
				// czy analizowana drużyna grała u siebie czy na wyjeździe. Gdy grała
				// na wyjeździe, "przeciwnikiem" błędnie stawała się jej WŁASNA nazwa.
				// Teraz porównujemy ID drużyny z zapytania, żeby wziąć właściwą stronę.
				switch ownTeamID {
				case strconv.Itoa(e.HomeTeam.ID):
					opp = e.AwayTeam.Name
				case strconv.Itoa(e.AwayTeam.ID):
					opp = e.HomeTeam.Name
				default:
					opp = e.AwayTeam.Name
				}
				opp = displayOpponentName(opp)
			}

			eventMetaMap[eID] = EventMeta{
				Timestamp:    e.Events.TimeStartTimestamp,
				TournamentID: tID,
				Opponent:     opp,
				DateStr:      dStr,
			}
		}

		// substituteFor: eventID -> ID zawodnika schodzącego -> zmiennik.
		// W danych StatsHub pola są nazwane z perspektywy samej zmiany: u
		// ZMIENNIKA substitutedOut zawiera ID zawodnika schodzącego, a u zawodnika
		// schodzącego substitutedIn zawiera ID zmiennika. Wpis jednostronny nie
		// wystarcza, dlatego przy doliczaniu wymagamy wzajemnego potwierdzenia.
		type substitute struct {
			PlayerID int
			Stats    StatsHubMatchDetail
		}
		substituteFor := make(map[int]map[int]substitute)
		for _, p := range sh.Data {
			for _, m := range p.Stats {
				if m.MinutesPlayed <= 0 {
					continue
				}
				if replacedID, ok := extractPlayerID(m.SubstitutedOut); ok {
					if substituteFor[m.EventID] == nil {
						substituteFor[m.EventID] = make(map[int]substitute)
					}
					substituteFor[m.EventID][replacedID] = substitute{PlayerID: p.ID, Stats: m}
				}
			}
		}

		for _, p := range sh.Data {
			hist := &PlayerStatHistory{
				ID:                       p.ID,
				FullName:                 p.Name,
				MetricValues:             make(map[CanonicalMarket][]int),
				MetricValuesWithSuperSub: make(map[CanonicalMarket][]int),
			}

			var matches []StatsHubMatchDetail
			for _, m := range p.Stats {
				// FILTRACJA: Bierzemy mecz tylko jeśli należy do wybranej ligi
				if m.MinutesPlayed > 0 && (len(allowedEventIDs) == 0 || allowedEventIDs[m.EventID]) {
					matches = append(matches, m)
				}
			}

			sort.Slice(matches, func(i, j int) bool {
				tI := eventMetaMap[matches[i].EventID].Timestamp
				tJ := eventMetaMap[matches[j].EventID].Timestamp
				if tI != tJ {
					return tI > tJ
				}
				return matches[i].EventID > matches[j].EventID
			})

			count := 0
			for _, m := range matches {
				if maxMatches > 0 && count >= maxMatches {
					break
				}
				hist.Matches++
				count++
				hist.Minutes = append(hist.Minutes, m.MinutesPlayed)
				pos := strings.TrimSpace(m.Position)
				hist.Positions = append(hist.Positions, pos)

				meta := eventMetaMap[m.EventID]
				hist.Opponents = append(hist.Opponents, meta.Opponent)
				hist.Dates = append(hist.Dates, meta.DateStr)

				hist.MetricValues[MarketShotsTotal] = append(hist.MetricValues[MarketShotsTotal], m.Shots)
				hist.MetricValues[MarketShotsOnTarget] = append(hist.MetricValues[MarketShotsOnTarget], m.OnTargetScoringAttempt)
				hist.MetricValues[MarketShotsBlocked] = append(hist.MetricValues[MarketShotsBlocked], m.BlockedScoringAttempt)
				hist.MetricValues[MarketShotsOffTarget] = append(hist.MetricValues[MarketShotsOffTarget], m.ShotOffTarget)
				// Use Dispossessed (number of times player dispossessed opponent) as tackles metric
				// Prefer TotalTackle (tackles) and fallback to Dispossessed if TotalTackle is absent
				starterTackles := m.TotalTackle
				if starterTackles == 0 && m.Dispossessed > 0 {
					starterTackles = m.Dispossessed
				}
				hist.MetricValues[MarketTackles] = append(hist.MetricValues[MarketTackles], starterTackles)
				hist.MetricValues[MarketInterceptions] = append(hist.MetricValues[MarketInterceptions], m.InterceptionWon)
				hist.MetricValues[MarketFoulsCommitted] = append(hist.MetricValues[MarketFoulsCommitted], m.Fouls)
				hist.MetricValues[MarketFoulsDrawn] = append(hist.MetricValues[MarketFoulsDrawn], m.WasFouled)
				hist.MetricValues[MarketOffsides] = append(hist.MetricValues[MarketOffsides], m.TotalOffside)
				// Also record dispossessed if present
				hist.MetricValues[MarketDispossessed] = append(hist.MetricValues[MarketDispossessed], m.Dispossessed)

				var subM StatsHubMatchDetail
				hasMatchingSub := false
				// Doliczamy tylko wzajemnie potwierdzoną parę:
				// zmiennik ma substitutedOut = p.ID, a p ma substitutedIn = zmiennik.ID.
				if evMap, exists := substituteFor[m.EventID]; exists {
					if sm, found := evMap[p.ID]; found {
						if replacementID, confirmed := extractPlayerID(m.SubstitutedIn); confirmed && replacementID == sm.PlayerID {
							subM = sm.Stats
							hasMatchingSub = true
						}
					}
				}

				addIfSub := func(starterVal, subVal int) int {
					if hasMatchingSub {
						return starterVal + subVal
					}
					return starterVal
				}

				hist.MetricValuesWithSuperSub[MarketShotsTotal] = append(hist.MetricValuesWithSuperSub[MarketShotsTotal], addIfSub(m.Shots, subM.Shots))
				hist.MetricValuesWithSuperSub[MarketShotsOnTarget] = append(hist.MetricValuesWithSuperSub[MarketShotsOnTarget], addIfSub(m.OnTargetScoringAttempt, subM.OnTargetScoringAttempt))
				hist.MetricValuesWithSuperSub[MarketShotsBlocked] = append(hist.MetricValuesWithSuperSub[MarketShotsBlocked], addIfSub(m.BlockedScoringAttempt, subM.BlockedScoringAttempt))
				hist.MetricValuesWithSuperSub[MarketShotsOffTarget] = append(hist.MetricValuesWithSuperSub[MarketShotsOffTarget], addIfSub(m.ShotOffTarget, subM.ShotOffTarget))
				// Compute replacement tackles for substitute with same fallback (prefer TotalTackle)
				subTackles := subM.TotalTackle
				if subTackles == 0 && subM.Dispossessed > 0 {
					subTackles = subM.Dispossessed
				}
				hist.MetricValuesWithSuperSub[MarketTackles] = append(hist.MetricValuesWithSuperSub[MarketTackles], addIfSub(starterTackles, subTackles))
				hist.MetricValuesWithSuperSub[MarketInterceptions] = append(hist.MetricValuesWithSuperSub[MarketInterceptions], addIfSub(m.InterceptionWon, subM.InterceptionWon))
				hist.MetricValuesWithSuperSub[MarketFoulsCommitted] = append(hist.MetricValuesWithSuperSub[MarketFoulsCommitted], addIfSub(m.Fouls, subM.Fouls))
				hist.MetricValuesWithSuperSub[MarketFoulsDrawn] = append(hist.MetricValuesWithSuperSub[MarketFoulsDrawn], addIfSub(m.WasFouled, subM.WasFouled))
				hist.MetricValuesWithSuperSub[MarketOffsides] = append(hist.MetricValuesWithSuperSub[MarketOffsides], addIfSub(m.TotalOffside, subM.TotalOffside))
				// Also mirror dispossessed into WithSuperSub
				hist.MetricValuesWithSuperSub[MarketDispossessed] = append(hist.MetricValuesWithSuperSub[MarketDispossessed], addIfSub(m.Dispossessed, subM.Dispossessed))
			}

			if hist.Matches > 0 {
				playerHistory[strconv.Itoa(p.ID)] = hist
				// Populate strictMap with several normalized variants to improve matching
				idStr := strconv.Itoa(p.ID)
				norm := NormalizePlayerName(p.Name)
				if norm != "" {
					strictMap[norm] = idStr
					parts := strings.Fields(norm)
					if len(parts) >= 2 {
						// first + last
						firstLast := parts[0] + " " + parts[len(parts)-1]
						strictMap[firstLast] = idStr
						// initial + last (e.g., "l openda")
						runes := []rune(parts[0])
						if len(runes) > 0 {
							initial := string(runes[0])
							strictMap[initial+" "+parts[len(parts)-1]] = idStr
						}
						// last + first (for "Surname, First" styles)
						lastFirst := parts[len(parts)-1] + " " + parts[0]
						strictMap[lastFirst] = idStr
					}
				}
			}
		}
	}

	indexSH(homeSH, homeTeamID)
	indexSH(awaySH, awayTeamID)

	// UWAGA: musi być pustym slice'em, nie nil-em. "var results []AnalyzedBet"
	// zostaje w Go nil-em, jeśli żaden zakład nie przejdzie filtrów, a nil slice
	// serializuje się do JSON jako `null` (nie `[]`). Frontend robi wtedy
	// `rawBets.filter(...)` na `null` i wywala się z "Cannot read properties
	// of null (reading 'filter')".
	results := make([]AnalyzedBet, 0)

	for _, prop := range props {
		matchedID := matchPlayerStrict(prop.PlayerName, strictMap)
		if matchedID == "" {
			// Debug: show likely candidates to help diagnose missing mappings
			targetNorm := NormalizePlayerName(prop.PlayerName)
			cands := findTopPlayerCandidates(targetNorm, strictMap, 5)
			fmt.Printf("[PLAYER MATCH FAIL] '%s' (norm: '%s') nie znaleziono w strictMap. Top candidates: %v\n", prop.PlayerName, targetNorm, cands)
			continue
		}

		hist, exists := playerHistory[matchedID]
		if !exists || hist.Matches == 0 {
			continue
		}

		minRequired := 1
		if maxMatches < minRequired {
			minRequired = maxMatches
		}
		if hist.Matches < minRequired {
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
			Verdict:              verdict,
		})
	}

	return results, nil
}

// ==========================================
// 6. MOST WAILS APP
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

func (a *App) DetectTeamsFromURL(rawURL string) TeamDetectionResult {
	_, slug, provider := extractIDAndSlug(rawURL)
	homeSlug, awaySlug := parseTeamsFromSlug(slug, provider)

	cache := loadTeamCache()
	homeID := findTeamIDInCache(homeSlug, cache)
	awayID := findTeamIDInCache(awaySlug, cache)

	// Fallback: jeśli bezpośrednie dopasowanie się nie powiodło, spróbuj uproszczonych wariantów slugów
	tryFind := func(s string) string {
		if s == "" {
			return ""
		}
		if id := findTeamIDInCache(s, cache); id != "" {
			return id
		}
		if strings.Contains(s, "-") {
			parts := strings.Split(s, "-")
			candidates := []string{parts[0], parts[len(parts)-1]}
			if len(parts) >= 2 {
				candidates = append(candidates, strings.Join(parts[:2], "-"))
				candidates = append(candidates, strings.Join(parts[len(parts)-2:], "-"))
			}
			for _, c := range candidates {
				if id := findTeamIDInCache(c, cache); id != "" {
					fmt.Printf("[DETECTION FALLBACK] '%s' -> trying '%s' -> ID: %s\n", s, c, id)
					return id
				}
			}
		}
		return ""
	}

	if homeID == "" {
		homeID = tryFind(homeSlug)
	}
	if awayID == "" {
		awayID = tryFind(awaySlug)
	}

	if (homeID == "" || awayID == "") && !strings.Contains(slug, "-vs-") && !strings.Contains(slug, "-v-") {
		if splitHomeSlug, splitAwaySlug, splitHomeID, splitAwayID, ok := splitTeamsFromCache(slug, cache); ok {
			homeSlug, awaySlug = splitHomeSlug, splitAwaySlug
			homeID, awayID = splitHomeID, splitAwayID
		}
	}

	fmt.Printf("[DETECTION] Provider: %s | HomeSlug: '%s' -> ID: '%s' | AwaySlug: '%s' -> ID: '%s'\n",
		provider, homeSlug, homeID, awaySlug, awayID)

	return TeamDetectionResult{
		HomeSlug: homeSlug,
		AwaySlug: awaySlug,
		HomeID:   homeID,
		AwayID:   awayID,
	}
}

func (a *App) GetAvailableTournaments(homeID, awayID string) ([]LeagueOption, error) {
	client := NewResilientClient()
	urlStr := fmt.Sprintf("https://www.statshub.com/api/team/%s/players/performance?limit=15&location=both", homeID)
	resp, err := client.GetWithAntiBan(urlStr, true)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	var sh StatsHubResponse
	_ = json.Unmarshal(b, &sh)

	seen := make(map[int]bool)
	var list []LeagueOption

	lCache := loadLeagueCache()
	if tidStr, ok := lCache[homeID]; ok {
		if tid, err := strconv.Atoi(tidStr); err == nil && tid > 0 {
			seen[tid] = true
			tName := fmt.Sprintf("Liga główna (%d)", tid)
			if tid == 1 {
				tName = "Premier League (1)"
			} else if tid == 8 {
				tName = "La Liga (8)"
			} else if tid == 21 {
				tName = "Serie A (21)"
			} else if tid == 35 {
				tName = "Bundesliga (35)"
			} else if tid == 34 {
				tName = "Ligue 1 (34)"
			}
			list = append(list, LeagueOption{ID: tid, Name: tName})
		}
	}

	for _, e := range sh.Events {
		tid := e.Tournaments.InternalID
		tname := e.Tournaments.Name
		if tid == 0 {
			tid = e.Tournament.InternalID
			tname = e.Tournament.Name
		}
		if tid == 0 {
			tid = e.Tournaments.ID
		}
		if tid > 0 && tid < 100000 && !seen[tid] {
			seen[tid] = true
			if tname == "" {
				tname = fmt.Sprintf("Rozgrywki %d", tid)
			}
			list = append(list, LeagueOption{ID: tid, Name: tname})
		}
	}

	fmt.Printf("[TOURNAMENTS] Wykryto %d rozgrywek dla TeamID=%s (internalId)\n", len(list), homeID)
	return list, nil
}

func (a *App) AnalyzeThreeBookmakers(sbInput, betclicInput, stsInput, homeInput, awayInput string, matchLimit int, tournamentIDs string) ([]AnalyzedBet, error) {
	if matchLimit <= 0 {
		matchLimit = 10
	}

	teamIDPattern := regexp.MustCompile(`^\d+$`)
	homeID := strings.TrimSpace(homeInput)
	awayID := strings.TrimSpace(awayInput)

	if !teamIDPattern.MatchString(homeID) || !teamIDPattern.MatchString(awayID) {
		return nil, fmt.Errorf("podaj numeryczne ID StatsHub dla obu zespołów")
	}

	var detectedMatchKey, detectedSource string
	for _, source := range []struct {
		name string
		url  string
	}{
		{name: "Superbet", url: sbInput},
		{name: "Betclic", url: betclicInput},
		{name: "STS", url: stsInput},
	} {
		if strings.TrimSpace(source.url) != "" {
			det := a.DetectTeamsFromURL(source.url)
			if det.HomeID != "" && det.AwayID != "" {
				key := unorderedTeamKey(det.HomeID, det.AwayID)
				if detectedMatchKey != "" && detectedMatchKey != key {
					return nil, fmt.Errorf("link %s dotyczy innego meczu niż link %s; nie można łączyć kursów z różnych spotkań", source.name, detectedSource)
				}
				detectedMatchKey = key
				detectedSource = source.name
			}
			if det.HomeID != "" && det.HomeID != homeID {
				fmt.Printf("[ID WARNING] Gospodarze z linku '%s' wskazują TeamID=%s, ale formularz ma TeamID=%s. Użyto ID z formularza.\n", det.HomeSlug, det.HomeID, homeID)
			}
			if det.AwayID != "" && det.AwayID != awayID {
				fmt.Printf("[ID WARNING] Goście z linku '%s' wskazują TeamID=%s, ale formularz ma TeamID=%s. Użyto ID z formularza.\n", det.AwaySlug, det.AwayID, awayID)
			}
		}
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
				fmt.Println("[SUPERBET] Nie udało się wyodrębnić ID meczu.")
				return
			}
			raw, err := FetchSuperbetMatchData(id)
			if err != nil {
				fmt.Printf("[SUPERBET] Błąd pobierania: %v\n", err)
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
				fmt.Println("[BETCLIC] Nie udało się wyodrębnić ID meczu.")
				return
			}
			rawBytes, err := FetchBetclicStreamData(id)
			if err != nil {
				fmt.Printf("[BETCLIC] Błąd gRPC: %v\n", err)
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
				fmt.Println("[STS] Nie udało się wyodrębnić ID meczu.")
				return
			}
			raw, err := FetchSTSSnapshotData(id)
			if err != nil {
				fmt.Printf("[STS] Błąd WS: %v\n", err)
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

	// STATSHUB
	var homeJSON, awayJSON string
	var homeErr, awayErr error

	fetchWg.Add(2)
	go func() {
		defer fetchWg.Done()
		homeJSON, homeErr = FetchStatsHubTeamData(homeID, tournamentIDs)
	}()
	go func() {
		defer fetchWg.Done()
		awayJSON, awayErr = FetchStatsHubTeamData(awayID, tournamentIDs)
	}()

	fetchWg.Wait()

	if homeErr != nil {
		return nil, fmt.Errorf("StatsHub gospodarze: %w", homeErr)
	}
	if awayErr != nil {
		return nil, fmt.Errorf("StatsHub goście: %w", awayErr)
	}
	if len(allProps) == 0 {
		return nil, fmt.Errorf("brak pobranych rynków od bukmacherów (sprawdź logi w terminalu)")
	}

	results, err := CalculateValueBets(allProps, homeJSON, awayJSON, matchLimit, tournamentIDs, homeID, awayID)
	if err != nil {
		return nil, err
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].HitRate == results[j].HitRate {
			return results[i].EVWithTax > results[j].EVWithTax
		}
		return results[i].HitRate > results[j].HitRate
	})

	fmt.Printf("[ENGINE] Łącznie wyliczono %d poprawnych zakładów dla regulaminowego czasu gry.\n", len(results))
	return results, nil
}
