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
	// Liczone z FootyMetrics (pola sh / shob), bo StatsHub nie dzieli strzałów na strefy.
	MarketShotsOutsideBox CanonicalMarket = "SHOTS_OUTSIDE_BOX"
	MarketShotsInsideBox  CanonicalMarket = "SHOTS_INSIDE_BOX"
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
	StatsSource          string          `json:"statsSource"`
	TeamSide             string          `json:"teamSide"`
	Verdict              string          `json:"verdict"`
}

type TeamDetectionResult struct {
	HomeSlug string `json:"homeSlug"`
	AwaySlug string `json:"awaySlug"`
	HomeID   string `json:"homeId"`
	AwayID   string `json:"awayId"`
	HomeFmID string `json:"homeFmId"`
	AwayFmID string `json:"awayFmId"`
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

// loadTeamCache zwraca same ID StatsHub. Pełny wpis (razem z ID FootyMetrics)
// czyta loadTeamCacheFull z sekcji 7.4.
func loadTeamCache() map[string]string {
	full := loadTeamCacheFull()
	out := make(map[string]string, len(full))
	for slug, entry := range full {
		if entry.StatsHub != "" {
			out[slug] = entry.StatsHub
		}
	}
	return out
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

// foldLabel sprowadza etykietę rynku do porównywalnej postaci:
// małe litery, bez ogonków, bez ł, pojedyncze spacje.
func foldLabel(s string) string {
	return strings.Join(strings.Fields(removeDiacritics(strings.ToLower(s))), " ")
}

// containsZonePhrase mówi, czy etykieta w ogóle dotyczy strefy boiska.
func containsZonePhrase(raw string) bool {
	s := foldLabel(raw)
	phrases := []string{
		"spoza pola", "spoza szesnast", "spoza", "z dystansu", "outside the box", "outside box",
		"z pola karnego", "w polu karnym", "w szesnastce", "z szesnastki", "inside the box", "inside box",
	}
	for _, p := range phrases {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

// mapZoneShotMarket mapuje WYŁĄCZNIE "gołe" strzały w danej strefie boiska.
// Celne / niecelne / zablokowane spoza pola odrzucamy, bo z FootyMetrics bierzemy
// tylko sh i shob — reszty wariantów nie da się z tego odtworzyć.
func mapZoneShotMarket(raw string) (CanonicalMarket, bool) {
	s := foldLabel(raw)

	if !strings.Contains(s, "strzal") && !strings.Contains(s, "shot") {
		return "", false
	}
	// wariant statystyki, którego nie umiemy policzyć
	if strings.Contains(s, "celn") || strings.Contains(s, "zablokowan") ||
		strings.Contains(s, "on target") || strings.Contains(s, "off target") || strings.Contains(s, "blocked") {
		return "", false
	}
	// sposób wykonania
	if strings.Contains(s, "glow") || strings.Contains(s, "head") ||
		strings.Contains(s, "nog") || strings.Contains(s, "foot") {
		return "", false
	}

	// UWAGA na kolejność: "spoza pola karnego" zawiera w sobie "pola karnego".
	if strings.Contains(s, "spoza") || strings.Contains(s, "z dystansu") ||
		strings.Contains(s, "outside the box") || strings.Contains(s, "outside box") {
		return MarketShotsOutsideBox, true
	}
	if strings.Contains(s, "z pola karnego") || strings.Contains(s, "w polu karnym") ||
		strings.Contains(s, "w szesnastce") || strings.Contains(s, "z szesnastki") ||
		strings.Contains(s, "inside the box") || strings.Contains(s, "inside box") {
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

	// Strefy boiska (FootyMetrics)
	if zone, ok := mapZoneShotMarket(raw); ok {
		return zone, true
	}
	if containsZonePhrase(raw) {
		return "", false
	}

	if isUnsupportedMethodOrZone(raw) {
		return "", false
	}

	// 1. Przechwyty STS / Opta (np. "przechwyty (z dogrywką)", "liczba przechwytów")
	// Mapuje do StatsHub: m.InterceptionWon -> MarketInterceptions
	if strings.Contains(sd, "przechwyt") || strings.Contains(s, "interception") {
		return MarketInterceptions, true
	}

	// 2. Odbiory na zawodniku (straty / dispossessed)
	if strings.Contains(sd, "odbiorow na zawodniku") || strings.Contains(sd, "odbior na zawodnik") ||
		strings.Contains(sd, "odbiory na zawodniku") {
		return MarketDispossessed, true
	}

	// 3. Odbiory STS / Opta (np. "odbiory (z dogrywką)", "liczba odbiorów")
	// Mapuje do StatsHub: m.TotalTackle -> MarketTackles
	if strings.Contains(sd, "odbior") || strings.Contains(s, "tackle") {
		return MarketTackles, true
	}

	// 4. Strzały
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

	// 5. Faule i spalone
	switch {
	case strings.Contains(s, "wywalczon") || strings.Contains(s, "faulowany") || strings.Contains(sd, "faulowany"):
		return MarketFoulsDrawn, true
	case strings.Contains(s, "faul"):
		return MarketFoulsCommitted, true
	case strings.Contains(sd, "spalon") || strings.Contains(s, "offside"):
		return MarketOffsides, true
	}

	return "", false
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
		if isHalfOrPeriodMarket(m.Name) {
			continue
		}
		if isUnsupportedMethodOrZone(m.Name) && !containsZonePhrase(m.Name) {
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
			if isHalfOrPeriodMarket(odd.Name) {
				continue
			}
			if isUnsupportedMethodOrZone(odd.Name) && !containsZonePhrase(odd.Name) {
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

				// Usuwamy techniczny dopisek STS przed sprawdzeniem okresu gry
				cleanName := strings.ReplaceAll(rawName, "(z dogrywką)", "")
				cleanName = strings.ReplaceAll(cleanName, "(z dogrywka)", "")
				cleanName = strings.TrimSpace(cleanName)

				// Odrzucamy tylko faktyczne rynki połówek/kwart
				if isHalfOrPeriodMarket(cleanName) {
					continue
				}

				pName := ""
				mType := ""
				var canon CanonicalMarket
				var okMarket bool

				// Format STS: "Nazwisko Imię - Typ Rynku"
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

				// Jeśli rynek nie został rozpoznany (np. asysty, spalone bez obsługi itp.) lub to ID, pomijamy
				if !okMarket || pName == "" || isIDToken(pName) {
					continue
				}

				if isUnsupportedMethodOrZone(mType) && !containsZonePhrase(mType) {
					continue
				}

				// Filtrowanie rynków zespołowych
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

					// STS formatuje linie jako: "1+", "2+", "3+" lub czyste liczby
					cleanLineStr := strings.TrimSpace(lowerLine)
					if strings.HasSuffix(cleanLineStr, "+") {
						valStr := strings.TrimSuffix(cleanLineStr, "+")
						if parsed, err := strconv.ParseFloat(valStr, 64); err == nil {
							// "1+" to linia powyżej 0.5, "2+" to powyżej 1.5 itd.
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
				if isUnsupportedMethodOrZone(current.market) && !containsZonePhrase(current.market) {
					continue
				}
				if isUnsupportedMethodOrZone(current.playerRaw) && !containsZonePhrase(current.playerRaw) {
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

var strokeLetterReplacer = strings.NewReplacer(
	"ł", "l", "Ł", "L",
	"đ", "d", "Đ", "D",
	"ø", "o", "Ø", "O",
	"æ", "ae", "Æ", "AE",
	"œ", "oe", "Œ", "OE",
	"ß", "ss",
)

// removeDiacritics zdejmuje znaki diakrytyczne. Sam NFD nie wystarcza: litery
// z przekreśleniem (ł, đ, ø) nie mają formy zdekomponowanej, więc "Michał" ze
// strony bukmachera nigdy nie zrównałby się z "Michal" ze StatsHuba.
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
	if id, ok := playerMap[targetNorm]; ok {
		return id
	}

	parts := strings.Fields(targetNorm)
	if len(parts) == 0 {
		return ""
	}
	surname := parts[len(parts)-1]

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

	// Grupowanie kandydatów i punktacja
	type scoredCandidate struct {
		id    string
		score int
	}
	var scored []scoredCandidate
	bestScore := 0

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
		if len(parts) > 0 && len(candTokens) > 0 {
			r1 := []rune(parts[0])
			r2 := []rune(candTokens[0])
			if len(r1) > 0 && len(r2) > 0 && r1[0] == r2[0] {
				score += 1
			}
		}
		if score > bestScore {
			bestScore = score
		}
		scored = append(scored, scoredCandidate{id: candidateIDs[i], score: score})
	}

	// Sprawdzamy czy wszyscy z najwyższym wynikiem wskazują na TO SAMO ID zawodnika
	if bestScore > 0 {
		matchedID := ""
		for _, sc := range scored {
			if sc.score == bestScore {
				if matchedID == "" {
					matchedID = sc.id
				} else if matchedID != sc.id {
					// Prawdziwy konflikt: dwóch różnych zawodników z tym samym wynikiem
					return ""
				}
			}
		}
		return matchedID
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
	TeamSide                 string
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
	return CalculateValueBetsWithZones(props, homeSH_JSON, awaySH_JSON, maxMatches, tournamentIDs, homeTeamID, awayTeamID, nil, nil)
}

// CalculateValueBetsWithZones dokłada do silnika rynki strefowe, dla których
// próbka pochodzi z FootyMetrics, a nie ze StatsHuba.
func CalculateValueBetsWithZones(props []NormalizedProp, homeSH_JSON, awaySH_JSON string, maxMatches int, tournamentIDs string, homeTeamID, awayTeamID string, fmHome, fmAway *FMTeamIndex) ([]AnalyzedBet, error) {
	var homeSH, awaySH StatsHubResponse
	_ = json.Unmarshal([]byte(homeSH_JSON), &homeSH)
	_ = json.Unmarshal([]byte(awaySH_JSON), &awaySH)

	playerHistory := make(map[string]*PlayerStatHistory)
	strictMap := make(map[string]string)

	indexSH := func(sh StatsHubResponse, ownTeamID, side string) {
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
				TeamSide:                 side,
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
						strictMap[parts[len(parts)-1]+" "+strings.Join(parts[:len(parts)-1], " ")] = idStr
						if len(parts) == 3 {
							strictMap[parts[0]+" "+parts[1]] = idStr
							strictMap[parts[1]+" "+parts[0]] = idStr
						}
					}
				}
			}
		}
	}

	indexSH(homeSH, homeTeamID, "HOME")
	indexSH(awaySH, awayTeamID, "AWAY")

	// UWAGA: musi być pustym slice'em, nie nil-em. "var results []AnalyzedBet"
	// zostaje w Go nil-em, jeśli żaden zakład nie przejdzie filtrów, a nil slice
	// serializuje się do JSON jako `null` (nie `[]`). Frontend robi wtedy
	// `rawBets.filter(...)` na `null` i wywala się z "Cannot read properties
	// of null (reading 'filter')".
	results := make([]AnalyzedBet, 0)

	for _, prop := range props {
		// Rynki strefowe mają własne źródło danych — StatsHub ich nie liczy.
		if isZoneShotMarket(prop.MarketType) {
			if bet, ok := BuildZoneBet(prop, []*FMTeamIndex{fmHome, fmAway}, maxMatches); ok {
				results = append(results, bet)
			}
			continue
		}

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
			StatsSource:          "STATSHUB",
			TeamSide:             hist.TeamSide,
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
		HomeFmID: findFMTeamIDBySlug(homeSlug),
		AwayFmID: findFMTeamIDBySlug(awaySlug),
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

func (a *App) AnalyzeThreeBookmakers(sbInput, betclicInput, stsInput, homeInput, awayInput string, matchLimit int, tournamentIDs string, fmHomeInput, fmAwayInput string) ([]AnalyzedBet, error) {
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

	// FOOTYMETRICS (strzały z / spoza pola karnego)
	var fmHomeIdx, fmAwayIdx *FMTeamIndex
	fmHomeID := strings.TrimSpace(fmHomeInput)
	fmAwayID := strings.TrimSpace(fmAwayInput)
	if fmHomeID == "" {
		fmHomeID = findFMTeamIDByStatsHubID(homeID)
	}
	if fmAwayID == "" {
		fmAwayID = findFMTeamIDByStatsHubID(awayID)
	}

	if fmHomeID != "" {
		fetchWg.Add(1)
		go func() {
			defer fetchWg.Done()
			idx, err := FetchFootyMetricsTeamIndex(fmHomeID, matchLimit)
			if err != nil {
				fmt.Printf("[FOOTYMETRICS] Gospodarze: %v\n", err)
				return
			}
			idx.Side = "HOME"
			fmHomeIdx = idx
		}()
	}
	if fmAwayID != "" {
		fetchWg.Add(1)
		go func() {
			defer fetchWg.Done()
			idx, err := FetchFootyMetricsTeamIndex(fmAwayID, matchLimit)
			if err != nil {
				fmt.Printf("[FOOTYMETRICS] Goście: %v\n", err)
				return
			}
			idx.Side = "AWAY"
			fmAwayIdx = idx
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

	// Zapamiętujemy powiązanie slug -> ID FootyMetrics na przyszłość.
	saveTeamCacheFM(map[string]string{
		slugForStatsHubID(homeID): fmHomeID,
		slugForStatsHubID(awayID): fmAwayID,
	})

	// Filtr oferty: "z pola karnego" bierzemy wyłącznie od Betclica.
	allProps = sanitizeZoneProps(allProps)

	if fmHomeIdx == nil && fmAwayIdx == nil {
		zoneCount := 0
		for _, p := range allProps {
			if isZoneShotMarket(p.MarketType) {
				zoneCount++
			}
		}
		if zoneCount > 0 {
			fmt.Printf("[FOOTYMETRICS] Pomijam %d rynków strefowych — brak danych FootyMetrics dla tych drużyn.\n", zoneCount)
		}
	}

	results, err := CalculateValueBetsWithZones(allProps, homeJSON, awayJSON, matchLimit, tournamentIDs, homeID, awayID, fmHomeIdx, fmAwayIdx)
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

// ==========================================
// 7. FOOTYMETRICS — STRZAŁY Z / SPOZA POLA KARNEGO
// ==========================================
//
// StatsHub nie dzieli strzałów na strefy, więc rynki SHOTS_OUTSIDE_BOX i
// SHOTS_INSIDE_BOX liczymy z wewnętrznego API FootyMetrics:
//
//   /api/front/fixtures/h2h?home=377&away=528   -> nazwy i ID drużyn (do cache)
//   /api/front/teams/table?id=377&period=5&...  -> pivotData ze statystykami
//
// Układ pivotData (zweryfikowany na realnej odpowiedzi):
//   pivotData[ID_ZAWODNIKA][ID_SPOTKANIA] = { "sh": 2, "shob": 1, "mins": 90, ... }
//
// UWAGA: to zawodnik jest kluczem zewnętrznym, a spotkanie wewnętrznym.
// Rozróżnienie bierzemy jednak nie z układu, tylko z tablic `fixtures` i
// `players`, które jednoznacznie mówią, które apid jest czym.
//
//   sh   – wszystkie strzały
//   shob – strzały spoza pola karnego
//   strzały z pola karnego = max(0, sh - shob)

const footyMetricsBase = "https://www.footymetrics.com"

var fmNumericID = regexp.MustCompile(`^\d+$`)
var fmAnyNumber = regexp.MustCompile(`\d+`)
var fmLogoTeamID = regexp.MustCompile(`/teams/(\d+)\.`)

// ------------------------------------------
// 7.1 Warstwa sieciowa
// ------------------------------------------

func (c *ResilientClient) getFootyMetrics(urlStr string) (*http.Response, error) {
	time.Sleep(time.Duration(150+rand.Intn(150)) * time.Millisecond)

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "pl,en-US;q=0.9,en;q=0.8")
	req.Header.Set("Referer", footyMetricsBase+"/")
	req.Header.Set("Origin", footyMetricsBase)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	return c.httpClient.Do(req)
}

func fmFetch(urlStr string) ([]byte, error) {
	client := NewResilientClient()
	resp, err := client.getFootyMetrics(urlStr)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		preview := strings.TrimSpace(string(b))
		if len(preview) > 300 {
			preview = preview[:300]
		}
		return nil, fmt.Errorf("FootyMetrics HTTP %s: %s", resp.Status, preview)
	}
	return b, nil
}

func footyMetricsTableURL(teamID, period string) string {
	params := url.Values{}
	params.Set("stat", "shots-outside-box")
	params.Set("id", teamID)
	params.Set("period", period)
	params.Set("location", "both")
	params.Set("group", "attack")
	params.Set("selectedLeagues", "")
	params.Set("half", "")
	params.Set("dl", "true")
	params.Set("se", "false")
	params.Set("sm", "easier")
	return footyMetricsBase + "/api/front/teams/table?" + params.Encode()
}

// ------------------------------------------
// 7.2 Struktury odpowiedzi
// ------------------------------------------

type FMTeamIDs struct {
	HomeID string `json:"homeId"`
	AwayID string `json:"awayId"`
}

type FMHarvestResult struct {
	HomeID     string `json:"homeId"`
	AwayID     string `json:"awayId"`
	TeamsAdded int    `json:"teamsAdded"`
	TeamsTotal int    `json:"teamsTotal"`
	Message    string `json:"message"`
}

type fmPivotStats struct {
	Shots int `json:"sh"`
	// shob bywa nullem. Przy sh=0 znaczy to po prostu zero, ale przy sh>0
	// oznacza brak rozbicia na strefy — takiego meczu nie wolno policzyć jako
	// "z pola karnego", więc wskaźnik pozwala odróżnić null od zera.
	ShotsOB  *int   `json:"shob"`
	SOT      int    `json:"sot"`
	SOTOB    *int   `json:"sotob"`
	Minutes  int    `json:"mins"`
	Detailed string `json:"detailed"`
	Sub      bool   `json:"sub"`
	SubName  string `json:"subName"`
	SubAPID  int64  `json:"subApid"`
}

type fmFixture struct {
	APID      int64  `json:"apid"`
	Timestamp string `json:"timestamp"`
	LID       int    `json:"lid"`
	HID       int    `json:"hid"`
	AID       int    `json:"aid"`
	Opponent  struct {
		APID int    `json:"apid"`
		Name string `json:"name"`
	} `json:"opponent"`
}

type fmPlayer struct {
	APID      int64  `json:"apid"`
	ShortName string `json:"shortName"`
	Slug      string `json:"slug"`
	Position  string `json:"position"`
	Team      any    `json:"team"`
	HasStats  bool   `json:"hasStats"`
}

type fmLeague struct {
	Name string `json:"name"`
}

type fmTableResponse struct {
	Fixtures  []fmFixture                        `json:"fixtures"`
	PivotData map[string]map[string]fmPivotStats `json:"pivotData"`
	Players   []fmPlayer                         `json:"players"`
	Leagues   map[string]fmLeague                `json:"leagues"`
}

// ------------------------------------------
// 7.3 Historia zawodnika
// ------------------------------------------

type FMPlayerHistory struct {
	ID           string
	Name         string
	Shots        []int
	ShotsOutside []int
	ShotsInside  []int
	Minutes      []int
	Positions    []string
	Opponents    []string
	Dates        []string
}

func (h *FMPlayerHistory) valuesFor(market CanonicalMarket) []int {
	switch market {
	case MarketShotsOutsideBox:
		return h.ShotsOutside
	case MarketShotsInsideBox:
		return h.ShotsInside
	case MarketShotsTotal:
		return h.Shots
	}
	return nil
}

type FMTeamIndex struct {
	TeamID   string
	Side     string
	Players  map[string]*FMPlayerHistory
	NameMap  map[string]string
	Matches  int
	Leagues  []string
	Warnings []string
}

// Lookup korzysta z tego samego dopasowania nazwisk co ścieżka StatsHub.
func (idx *FMTeamIndex) Lookup(playerName string) *FMPlayerHistory {
	if idx == nil || len(idx.NameMap) == 0 {
		return nil
	}
	id := matchPlayerStrict(playerName, idx.NameMap)
	if id == "" {
		return nil
	}
	return idx.Players[id]
}

// ------------------------------------------
// 7.4 Cache ID drużyn (team_cache.json)
// ------------------------------------------
//
// Oba identyfikatory siedzą teraz pod jednym slugiem w team_cache.json:
//
//   "rayo-vallecano": { "statshub": "2818", "footymetrics": "377" }
//
// Stary format (goły string z ID StatsHub) nadal się wczytuje, a wpisy bez ID
// FootyMetrics zapisujemy z powrotem jako string, żeby plik nie puchł bez potrzeby.

type TeamCacheEntry struct {
	StatsHub     string `json:"statshub"`
	FootyMetrics string `json:"footymetrics,omitempty"`
}

func (e *TeamCacheEntry) UnmarshalJSON(data []byte) error {
	var legacy string
	if err := json.Unmarshal(data, &legacy); err == nil {
		e.StatsHub = strings.TrimSpace(legacy)
		e.FootyMetrics = ""
		return nil
	}
	type rawEntry TeamCacheEntry
	var r rawEntry
	if err := json.Unmarshal(data, &r); err != nil {
		return err
	}
	e.StatsHub = strings.TrimSpace(r.StatsHub)
	e.FootyMetrics = strings.TrimSpace(r.FootyMetrics)
	return nil
}

func (e TeamCacheEntry) MarshalJSON() ([]byte, error) {
	if e.FootyMetrics == "" {
		return json.Marshal(e.StatsHub)
	}
	type rawEntry TeamCacheEntry
	return json.Marshal(rawEntry(e))
}

func loadTeamCacheFull() map[string]TeamCacheEntry {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	data, err := os.ReadFile(getCacheFilePath("team_cache.json"))
	if err != nil {
		return make(map[string]TeamCacheEntry)
	}
	m := make(map[string]TeamCacheEntry)
	_ = json.Unmarshal(data, &m)
	return m
}

// saveTeamCacheFM dopisuje ID FootyMetrics do istniejących wpisów.
// Nie zakładamy nowych drużyn — bez ID StatsHub i tak byłyby bezużyteczne.
func saveTeamCacheFM(pairs map[string]string) int {
	if len(pairs) == 0 {
		return 0
	}

	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("team_cache.json")
	m := make(map[string]TeamCacheEntry)
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &m)
	}
	if len(m) == 0 {
		return 0
	}

	added := 0
	for slug, fmID := range pairs {
		slug = strings.TrimSpace(slug)
		fmID = strings.TrimSpace(fmID)
		if slug == "" || fmID == "" {
			continue
		}
		entry, ok := m[slug]
		if !ok || entry.StatsHub == "" {
			continue
		}
		if entry.FootyMetrics == fmID {
			continue
		}
		entry.FootyMetrics = fmID
		m[slug] = entry
		added++
	}

	if added == 0 {
		return 0
	}

	out, _ := json.MarshalIndent(m, "", "  ")
	_ = os.WriteFile(path, out, 0644)
	fmt.Printf("[TEAM CACHE] Dopisano ID FootyMetrics dla %d drużyn.\n", added)
	return added
}

// forgetTeamCacheFM kasuje błędne ID, gdy API zwróci dane innej drużyny.
func forgetTeamCacheFM(fmID string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("team_cache.json")
	m := make(map[string]TeamCacheEntry)
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &m)

	changed := false
	for slug, entry := range m {
		if entry.FootyMetrics == fmID {
			entry.FootyMetrics = ""
			m[slug] = entry
			changed = true
			fmt.Printf("[TEAM CACHE] Usunięto błędne ID FootyMetrics %s z '%s'.\n", fmID, slug)
		}
	}
	if changed {
		out, _ := json.MarshalIndent(m, "", "  ")
		_ = os.WriteFile(path, out, 0644)
	}
}

// findFMTeamIDBySlug szuka ID FootyMetrics po slugu z linku bukmachera,
// tym samym znormalizowanym dopasowaniem co findTeamIDInCache.
func findFMTeamIDBySlug(slug string) string {
	if strings.TrimSpace(slug) == "" {
		return ""
	}
	clean := strings.ToLower(strings.TrimSpace(slug))
	if alias, ok := teamSlugAliases[clean]; ok {
		clean = alias
	}

	cache := loadTeamCacheFull()
	if entry, ok := cache[clean]; ok && entry.FootyMetrics != "" {
		return entry.FootyMetrics
	}

	// Cache trzyma aliasy ("real-madrid" i "real-madryt" pod jednym ID StatsHub),
	// więc kilka trafień wskazujących to samo ID to nadal jednoznaczny wynik.
	core := canonicalTeamCore(clean)
	distinct := make(map[string]bool)
	for cacheSlug, entry := range cache {
		if entry.FootyMetrics == "" {
			continue
		}
		if canonicalTeamCore(cacheSlug) == core {
			distinct[entry.FootyMetrics] = true
		}
	}
	if len(distinct) == 1 {
		for id := range distinct {
			return id
		}
	}
	return ""
}

// slugForStatsHubID odwraca cache: ID StatsHub -> slug klubu.
func slugForStatsHubID(statsHubID string) string {
	statsHubID = strings.TrimSpace(statsHubID)
	if statsHubID == "" {
		return ""
	}
	for slug, entry := range loadTeamCacheFull() {
		if entry.StatsHub == statsHubID {
			return slug
		}
	}
	return ""
}

func findFMTeamIDByStatsHubID(statsHubID string) string {
	statsHubID = strings.TrimSpace(statsHubID)
	if statsHubID == "" {
		return ""
	}
	for _, entry := range loadTeamCacheFull() {
		if entry.StatsHub == statsHubID && entry.FootyMetrics != "" {
			return entry.FootyMetrics
		}
	}
	return ""
}

// fmSlugForTeamName dopasowuje nazwę klubu z FootyMetrics ("Deportivo Alavés")
// do slugu w team_cache.json ("deportivo-alaves").
func fmSlugForTeamName(name string, cache map[string]TeamCacheEntry) string {
	core := canonicalTeamCore(name)
	if core == "" {
		return ""
	}
	if _, ok := cache[core]; ok {
		return core
	}

	if slug := fmUniqueSlug(cache, func(cacheSlug string) bool {
		return canonicalTeamCore(cacheSlug) == core
	}); slug != "" {
		return slug
	}

	// FootyMetrics skraca część nazw ("Levante" wobec "levante-ud" w cache).
	// Dopuszczamy dopasowanie po przedrostku, ale tylko gdy jest jednoznaczne —
	// samo "real" pasowałoby do czterech klubów i zostanie odrzucone.
	if len(core) < 4 {
		return ""
	}
	return fmUniqueSlug(cache, func(cacheSlug string) bool {
		return strings.HasPrefix(canonicalTeamCore(cacheSlug), core+"-")
	})
}

// fmUniqueSlug zwraca slug tylko wtedy, gdy wszystkie trafienia wskazują tę samą
// drużynę (te samo ID StatsHub). Aliasy w cache nie są więc traktowane jako konflikt.
func fmUniqueSlug(cache map[string]TeamCacheEntry, match func(string) bool) string {
	var hits []string
	ids := make(map[string]bool)
	for slug, entry := range cache {
		if match(slug) {
			hits = append(hits, slug)
			ids[entry.StatsHub] = true
		}
	}
	if len(hits) == 0 || len(ids) != 1 {
		return ""
	}
	sort.Strings(hits)
	return hits[0]
}

// ------------------------------------------
// 7.5 Ustalanie ID drużyny w FootyMetrics
// ------------------------------------------
//
// Uwaga na dwa różne identyfikatory w serwisie:
//   - adres strony drużyny to /teams/256-rayo-vallecano  (wewnętrzne ID serwisu)
//   - API tabeli przyjmuje  ?id=377                      (apid, to samo co w herbie)
// Dlatego każdy kandydat na ID jest najpierw sprawdzany zapytaniem do API,
// a gdy nie przejdzie — czytamy apid z adresu herbu na stronie drużyny.

var fmLogoRe = regexp.MustCompile(`teams/(\d+)\.webp`)
var fmNameRe = regexp.MustCompile(`"name":"([^"]{2,60})"`)
var fmTeamPathRe = regexp.MustCompile(`/teams/(\d+)-([a-z0-9-]+)`)
var fmLeaguePathRe = regexp.MustCompile(`/leagues/\d+-[a-z0-9-]+`)

func fmFetchPage(urlStr string) (string, error) {
	client := NewResilientClient()
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", client.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "pl,en-US;q=0.9,en;q=0.8")
	req.Header.Set("Referer", footyMetricsBase+"/")

	resp, err := client.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("FootyMetrics HTTP %s dla %s", resp.Status, urlStr)
	}
	return string(b), nil
}

// harvestFMFromText paruje adresy herbów z najbliższą nazwą klubu.
// Działa tak samo na odpowiedzi h2h, na stronie drużyny i na stronie ligi,
// bo wszędzie nazwa i herb leżą w tym samym obiekcie, kilkadziesiąt znaków od siebie.
func harvestFMFromText(text string) map[string]string {
	clean := strings.ReplaceAll(text, `\`, "")

	logos := fmLogoRe.FindAllStringSubmatchIndex(clean, -1)
	names := fmNameRe.FindAllStringSubmatchIndex(clean, -1)
	if len(logos) == 0 || len(names) == 0 {
		return nil
	}

	const maxDistance = 220
	out := make(map[string]string)
	conflicted := make(map[string]bool)

	for _, l := range logos {
		id := clean[l[2]:l[3]]
		best, bestDist := "", maxDistance+1

		for _, n := range names {
			dist := l[0] - n[1]
			if dist < 0 {
				dist = n[0] - l[1]
			}
			if dist < 0 || dist > maxDistance {
				continue
			}
			if dist < bestDist {
				bestDist = dist
				best = clean[n[2]:n[3]]
			}
		}

		if best == "" {
			continue
		}
		core := canonicalTeamCore(best)
		if core == "" || conflicted[core] {
			continue
		}
		if existing, ok := out[core]; ok && existing != id {
			// dwa różne ID pod tą samą nazwą = pomyłka parowania, odpuszczamy klub
			delete(out, core)
			conflicted[core] = true
			continue
		}
		out[core] = id
	}
	return out
}

// storeHarvestedFMNames zapisuje znalezione pary do team_cache.json.
func storeHarvestedFMNames(byName map[string]string) int {
	if len(byName) == 0 {
		return 0
	}
	cache := loadTeamCacheFull()
	pairs := make(map[string]string)
	for name, id := range byName {
		if slug := fmSlugForTeamName(name, cache); slug != "" {
			pairs[slug] = id
		}
	}
	return saveTeamCacheFM(pairs)
}

// harvestFMOpponents wyciąga przeciwników z pobranej tabeli — dane pewne,
// bo fixtures[].opponent niesie i apid, i nazwę.
func harvestFMOpponents(resp *fmTableResponse) int {
	byName := make(map[string]string)
	for _, f := range resp.Fixtures {
		if f.Opponent.APID > 0 && strings.TrimSpace(f.Opponent.Name) != "" {
			byName[canonicalTeamCore(f.Opponent.Name)] = strconv.Itoa(f.Opponent.APID)
		}
	}
	return storeHarvestedFMNames(byName)
}

// fmValidateTeamID sprawdza, czy API tabeli rozumie to ID jako tę właśnie drużynę.
func fmValidateTeamID(id string) bool {
	if !fmNumericID.MatchString(id) {
		return false
	}
	raw, err := fmFetch(footyMetricsTableURL(id, "5"))
	if err != nil {
		return false
	}
	var resp fmTableResponse
	if json.Unmarshal(raw, &resp) != nil {
		return false
	}
	if len(resp.PivotData) == 0 || len(resp.Fixtures) == 0 {
		return false
	}
	if !fmResponseBelongsTo(&resp, id) {
		return false
	}
	harvestFMOpponents(&resp)
	return true
}

// fmResponseBelongsTo potwierdza, że odpowiedź dotyczy drużyny o podanym ID.
func fmResponseBelongsTo(resp *fmTableResponse, id string) bool {
	for _, p := range resp.Players {
		if p.Team != nil {
			var teamIDStr string
			switch v := p.Team.(type) {
			case float64:
				teamIDStr = strconv.FormatInt(int64(v), 10)
			case string:
				teamIDStr = v
			case map[string]any:
				if rawID, ok := v["id"]; ok {
					teamIDStr = fmt.Sprintf("%v", rawID)
				} else if rawAPID, ok := v["apid"]; ok {
					teamIDStr = fmt.Sprintf("%v", rawAPID)
				}
			}
			if teamIDStr == id {
				return true
			}
		}
	}
	for _, f := range resp.Fixtures {
		if strconv.Itoa(f.HID) == id || strconv.Itoa(f.AID) == id {
			return true
		}
	}
	return false
}

// fmTeamIDFromPage czyta apid ze strony drużyny: herb własnej drużyny powtarza
// się tam przy każdym meczu, więc wygrywa liczebnością.
func fmTeamIDFromPage(pageURL string) string {
	body, err := fmFetchPage(pageURL)
	if err != nil {
		fmt.Printf("[FOOTYMETRICS] Strona %s: %v\n", pageURL, err)
		return ""
	}

	clean := strings.ReplaceAll(body, `\`, "")
	counts := make(map[string]int)
	for _, m := range fmLogoRe.FindAllStringSubmatch(clean, -1) {
		counts[m[1]]++
	}

	best, bestCount := "", 0
	for id, n := range counts {
		if n > bestCount {
			best, bestCount = id, n
		}
	}

	if names := harvestFMFromText(body); len(names) > 0 {
		storeHarvestedFMNames(names)
	}

	if best != "" {
		fmt.Printf("[FOOTYMETRICS] Ze strony %s odczytano apid=%s (%d wystąpień herbu).\n", pageURL, best, bestCount)
	}
	return best
}

// resolveFMCandidate zamienia ID z adresu strony na ID, które rozumie API.
func resolveFMCandidate(candidateID, pageURL string) string {
	if candidateID != "" && fmValidateTeamID(candidateID) {
		fmt.Printf("[FOOTYMETRICS] ID %s z adresu działa bezpośrednio w API.\n", candidateID)
		return candidateID
	}
	if pageURL == "" {
		return ""
	}
	fromPage := fmTeamIDFromPage(pageURL)
	if fromPage == "" || fromPage == candidateID {
		return ""
	}
	if fmValidateTeamID(fromPage) {
		return fromPage
	}
	return ""
}

// ParseFootyMetricsIDs wyciąga parę ID z linku h2h ("?home=377&away=528")
// albo ze skrótu "377/528", "377-528", "377 528".
func ParseFootyMetricsIDs(input string) (string, string, bool) {
	s := strings.TrimSpace(input)
	if s == "" {
		return "", "", false
	}

	if u, err := url.Parse(s); err == nil && u.RawQuery != "" {
		q := u.Query()
		h := strings.TrimSpace(q.Get("home"))
		a := strings.TrimSpace(q.Get("away"))
		if fmNumericID.MatchString(h) && fmNumericID.MatchString(a) {
			return h, a, true
		}
	}

	nums := fmAnyNumber.FindAllString(s, -1)
	if len(nums) >= 2 {
		return nums[0], nums[1], true
	}
	return "", "", false
}

// FetchFootyMetricsH2H pobiera h2h i dopisuje do cache wszystkie kluby,
// które pojawiły się w odpowiedzi.
func FetchFootyMetricsH2H(homeID, awayID string) (int, error) {
	if !fmNumericID.MatchString(homeID) || !fmNumericID.MatchString(awayID) {
		return 0, fmt.Errorf("ID FootyMetrics muszą być liczbami")
	}
	target := fmt.Sprintf("%s/api/front/fixtures/h2h?home=%s&away=%s", footyMetricsBase, homeID, awayID)
	fmt.Printf("[FOOTYMETRICS] h2h: %s\n", target)

	raw, err := fmFetch(target)
	if err != nil {
		return 0, err
	}
	return storeHarvestedFMNames(harvestFMFromText(string(raw))), nil
}

// ------------------------------------------
// 7.6 Parser tabeli drużyny
// ------------------------------------------

func FetchFootyMetricsTeamIndex(teamID string, maxMatches int) (*FMTeamIndex, error) {
	id := strings.TrimSpace(teamID)
	if !fmNumericID.MatchString(id) {
		return nil, fmt.Errorf("niepoprawne ID drużyny FootyMetrics: %q", teamID)
	}
	if maxMatches <= 0 {
		maxMatches = 10
	}

	// period przyjmuje liczbę ostatnich spotkań; gdyby dana wartość nie była
	// obsługiwana, wracamy do 5, które na pewno działa.
	periods := []string{strconv.Itoa(maxMatches)}
	if maxMatches != 5 {
		periods = append(periods, "5")
	}

	var lastErr error
	for _, period := range periods {
		target := footyMetricsTableURL(id, period)
		fmt.Printf("[FOOTYMETRICS] Pobieranie: %s\n", target)

		raw, err := fmFetch(target)
		if err != nil {
			lastErr = err
			continue
		}

		// Samokontrola: jeśli odpowiedź nie dotyczy tej drużyny, ID w cache jest
		// błędne i lepiej je od razu skasować, niż liczyć na nim zakłady.
		var probe fmTableResponse
		if json.Unmarshal(raw, &probe) == nil {
			if len(probe.Fixtures) > 0 && !fmResponseBelongsTo(&probe, id) {
				forgetTeamCacheFM(id)
				return nil, fmt.Errorf("FootyMetrics: ID %s zwraca dane innej drużyny", id)
			}
			harvestFMOpponents(&probe)
		}

		idx, err := ParseFootyMetricsTable(raw, maxMatches)
		if err != nil {
			lastErr = err
			continue
		}
		if idx.Matches == 0 {
			lastErr = fmt.Errorf("FootyMetrics zwrócił 0 spotkań dla period=%s", period)
			continue
		}

		idx.TeamID = id
		fmt.Printf("[FOOTYMETRICS] TeamID=%s | zawodników: %d | spotkań: %d | ligi: %s\n",
			id, len(idx.Players), idx.Matches, strings.Join(idx.Leagues, ", "))
		return idx, nil
	}

	return nil, lastErr
}

func ParseFootyMetricsTable(raw []byte, maxMatches int) (*FMTeamIndex, error) {
	var resp fmTableResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("FootyMetrics: nie udało się sparsować JSON: %w", err)
	}
	if len(resp.PivotData) == 0 {
		return nil, fmt.Errorf("FootyMetrics: brak pivotData w odpowiedzi")
	}
	if len(resp.Fixtures) == 0 {
		return nil, fmt.Errorf("FootyMetrics: brak listy spotkań (fixtures)")
	}

	// Spotkania od najnowszego.
	fixtures := append([]fmFixture(nil), resp.Fixtures...)
	sort.Slice(fixtures, func(i, j int) bool {
		return fmTimestamp(fixtures[i].Timestamp) > fmTimestamp(fixtures[j].Timestamp)
	})
	if maxMatches > 0 && len(fixtures) > maxMatches {
		fixtures = fixtures[:maxMatches]
	}

	type fixtureMeta struct {
		Order    int
		Opponent string
		Date     string
	}
	fixtureByID := make(map[string]fixtureMeta, len(fixtures))
	leagueSeen := make(map[int]bool)
	var leagues []string

	for i, f := range fixtures {
		opp := strings.TrimSpace(f.Opponent.Name)
		if opp == "" {
			opp = "Mecz"
		} else {
			opp = displayOpponentName(opp)
		}
		date := ""
		if ts := fmTimestamp(f.Timestamp); ts > 0 {
			date = time.Unix(ts, 0).Format("02.01")
		}
		fixtureByID[strconv.FormatInt(f.APID, 10)] = fixtureMeta{Order: i, Opponent: opp, Date: date}

		if f.LID > 0 && !leagueSeen[f.LID] {
			leagueSeen[f.LID] = true
			name := fmt.Sprintf("liga %d", f.LID)
			if l, ok := resp.Leagues[strconv.Itoa(f.LID)]; ok && l.Name != "" {
				name = l.Name
			}
			leagues = append(leagues, name)
		}
	}

	// Nazwiska. Slug ("andrei-ratiu") jest pełną nazwą, shortName ("A. Rațiu")
	// tylko inicjałem — rejestrujemy oba warianty, wyświetlamy ładniejszy.
	type nameInfo struct{ display, slug, short string }
	names := make(map[string]nameInfo, len(resp.Players))
	for _, p := range resp.Players {
		id := strconv.FormatInt(p.APID, 10)
		slugName := strings.TrimSpace(strings.ReplaceAll(p.Slug, "-", " "))
		short := strings.TrimSpace(p.ShortName)

		display := fmTitleCase(slugName)
		if display == "" {
			display = short
		}
		names[id] = nameInfo{display: display, slug: slugName, short: short}
	}

	idx := &FMTeamIndex{
		Players: make(map[string]*FMPlayerHistory),
		NameMap: make(map[string]string),
		Matches: len(fixtures),
		Leagues: leagues,
	}

	unnamed := 0
	unknownZone := 0
	for playerID, perFixture := range resp.PivotData {
		type cell struct {
			order int
			stats fmPivotStats
			meta  fixtureMeta
		}
		var cells []cell

		for fixtureID, st := range perFixture {
			meta, ok := fixtureByID[fixtureID]
			if !ok {
				continue // spotkanie poza wybranym oknem
			}
			if st.Minutes <= 0 {
				continue
			}
			// Brak rozbicia przy oddanych strzałach = nie wiemy, gdzie padły.
			// Zaliczenie ich domyślnie do pola karnego zawyżałoby próbkę,
			// więc taki mecz wypada z niej w całości.
			if st.ShotsOB == nil && st.Shots > 0 {
				unknownZone++
				continue
			}
			cells = append(cells, cell{order: meta.Order, stats: st, meta: meta})
		}
		if len(cells) == 0 {
			continue
		}
		sort.Slice(cells, func(i, j int) bool { return cells[i].order < cells[j].order })

		info, hasName := names[playerID]
		if !hasName || (info.display == "" && info.short == "") {
			unnamed++
			continue // bez nazwiska nie da się dopasować do oferty bukmachera
		}

		display := info.display
		if display == "" {
			display = info.short
		}

		hist := &FMPlayerHistory{ID: playerID, Name: display}
		for _, c := range cells {
			outside := 0
			if c.stats.ShotsOB != nil {
				outside = *c.stats.ShotsOB
			}
			inside := c.stats.Shots - outside
			if inside < 0 {
				inside = 0
			}
			hist.Shots = append(hist.Shots, c.stats.Shots)
			hist.ShotsOutside = append(hist.ShotsOutside, outside)
			hist.ShotsInside = append(hist.ShotsInside, inside)
			hist.Minutes = append(hist.Minutes, c.stats.Minutes)
			hist.Positions = append(hist.Positions, strings.TrimSpace(c.stats.Detailed))
			hist.Opponents = append(hist.Opponents, c.meta.Opponent)
			hist.Dates = append(hist.Dates, c.meta.Date)
		}

		idx.Players[playerID] = hist
		fmRegisterName(idx.NameMap, info.slug, playerID)
		fmRegisterName(idx.NameMap, info.short, playerID)
		fmRegisterName(idx.NameMap, display, playerID)
	}

	if unnamed > 0 {
		msg := fmt.Sprintf("%d zawodników w pivotData nie ma wpisu w `players` — pominięci", unnamed)
		idx.Warnings = append(idx.Warnings, msg)
		fmt.Printf("[FOOTYMETRICS] %s.\n", msg)
	}
	if unknownZone > 0 {
		fmt.Printf("[FOOTYMETRICS] %d wystąpień bez rozbicia strzałów na strefy — pominięte w próbce.\n", unknownZone)
	}
	return idx, nil
}

func fmTimestamp(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	layouts := []string{time.RFC3339, "2006-01-02T15:04:05.000Z", "2006-01-02T15:04:05Z", "2006-01-02 15:04:05", "2006-01-02"}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.Unix()
		}
	}
	return 0
}

func fmTitleCase(s string) string {
	parts := strings.Fields(s)
	for i, p := range parts {
		r := []rune(p)
		if len(r) == 0 {
			continue
		}
		parts[i] = strings.ToUpper(string(r[0])) + string(r[1:])
	}
	return strings.Join(parts, " ")
}

func fmRegisterName(nameMap map[string]string, name, id string) {
	norm := NormalizePlayerName(name)
	if norm == "" {
		return
	}
	if _, exists := nameMap[norm]; !exists {
		nameMap[norm] = id
	}

	parts := strings.Fields(norm)
	if len(parts) < 2 {
		return
	}
	last := parts[len(parts)-1]
	first := parts[0]

	variants := []string{
		first + " " + last,
		last + " " + first,
	}
	if r := []rune(first); len(r) > 0 {
		variants = append(variants, string(r[0])+" "+last)
	}
	for _, v := range variants {
		if _, exists := nameMap[v]; !exists {
			nameMap[v] = id
		}
	}
}

// ------------------------------------------
// 7.7 Wycena rynków strefowych
// ------------------------------------------

func isZoneShotMarket(m CanonicalMarket) bool {
	return m == MarketShotsOutsideBox || m == MarketShotsInsideBox
}

type betScore struct {
	HitRate  float64
	FairOdds float64
	Edge     float64
	EVTax    float64
	Verdict  string
}

// scoreSample powiela wycenę z CalculateValueBets (65% hit-rate + 35% Poisson,
// podatek 12%), żeby rynki z FootyMetrics liczyły się identycznie jak ze StatsHuba.
func scoreSample(values []int, line float64, isUnder bool, odds float64) betScore {
	sample := len(values)
	if sample == 0 {
		return betScore{}
	}

	hits, total := 0, 0
	for _, v := range values {
		total += v
		if isUnder {
			if float64(v) <= line {
				hits++
			}
		} else if float64(v) > line {
			hits++
		}
	}

	lambda := float64(total) / float64(sample)
	pHit := float64(hits) / float64(sample)

	var pPois float64
	if isUnder {
		pPois = calcPoissonUnder(line, lambda)
	} else {
		pPois = calcPoissonOver(line, lambda)
	}

	pFinal := pHit*0.65 + pPois*0.35
	if pFinal < 0.005 {
		pFinal = 0.005
	}
	if pFinal > 0.95 {
		pFinal = 0.95
	}

	fair := 1.0 / pFinal
	edge := (odds - fair) / fair * 100.0
	ev := (pFinal*(odds*0.88-1.0) - (1.0 - pFinal)) * 100.0

	verdict := "NEUTRAL"
	if ev > 0.0 {
		verdict = "VALUE"
	} else if ev < -15.0 {
		verdict = "TRAP"
	}

	return betScore{HitRate: pHit, FairOdds: fair, Edge: edge, EVTax: ev, Verdict: verdict}
}

// BuildZoneBet szuka zawodnika w obu indeksach FootyMetrics i wycenia rynek strefowy.
func BuildZoneBet(prop NormalizedProp, indexes []*FMTeamIndex, maxMatches int) (AnalyzedBet, bool) {
	for _, idx := range indexes {
		if idx == nil {
			continue
		}
		hist := idx.Lookup(prop.PlayerName)
		if hist == nil {
			continue
		}

		values := hist.valuesFor(prop.MarketType)
		if len(values) == 0 {
			continue
		}

		limit := len(values)
		if maxMatches > 0 && maxMatches < limit {
			limit = maxMatches
		}
		values = append([]int(nil), values[:limit]...)

		score := scoreSample(values, prop.Line, prop.IsUnder, prop.Odds)

		label := prop.MarketLabel
		if prop.IsUnder && !strings.Contains(label, "Poniżej") {
			label += " (Poniżej)"
		}

		return AnalyzedBet{
			Bookmaker:            prop.Bookmaker,
			PlayerName:           hist.Name,
			MarketType:           prop.MarketType,
			MarketLabel:          label,
			Line:                 prop.Line,
			IsUnder:              prop.IsUnder,
			BookmakerOdds:        prop.Odds,
			FairOdds:             math.Round(score.FairOdds*100) / 100,
			HitRate:              math.Round(score.HitRate*1000) / 10,
			EdgePercent:          math.Round(score.Edge*10) / 10,
			EVWithTax:            math.Round(score.EVTax*10) / 10,
			MatchesSample:        len(values),
			SampleValues:         values,
			SampleValuesSuperSub: append([]int(nil), values...), // FootyMetrics nie ma SuperSub
			SampleMinutes:        fmTrimInt(hist.Minutes, limit),
			SamplePositions:      fmTrimStr(hist.Positions, limit),
			SampleOpponents:      fmTrimStr(hist.Opponents, limit),
			SampleDates:          fmTrimStr(hist.Dates, limit),
			StatsSource:          "FOOTYMETRICS",
			TeamSide:             idx.Side,
			Verdict:              score.Verdict,
		}, true
	}

	fmt.Printf("[FOOTYMETRICS MATCH FAIL] '%s' (%s) nie znaleziony w danych FootyMetrics.\n",
		prop.PlayerName, prop.MarketType)
	return AnalyzedBet{}, false
}

func fmTrimInt(src []int, limit int) []int {
	if limit > len(src) {
		limit = len(src)
	}
	return append([]int(nil), src[:limit]...)
}

func fmTrimStr(src []string, limit int) []string {
	if limit > len(src) {
		limit = len(src)
	}
	return append([]string(nil), src[:limit]...)
}

// sanitizeZoneProps pilnuje, żeby rynek „strzały z pola karnego" pochodził
// wyłącznie od Betclica — jedynego bukmachera, który go realnie wystawia.
// Wszystko inne oznaczone jako INSIDE_BOX u konkurencji to najpewniej źle
// zmapowany zwykły rynek strzałów, więc wylatuje.
func sanitizeZoneProps(props []NormalizedProp) []NormalizedProp {
	out := make([]NormalizedProp, 0, len(props))
	dropped := 0

	for _, p := range props {
		if p.MarketType == MarketShotsInsideBox && p.Bookmaker != BookmakerBetclic {
			fmt.Printf("[ZONE GUARD] Odrzucono '%s' (%s, %s) — strzały z pola karnego wystawia tylko Betclic.\n",
				p.MarketLabel, p.Bookmaker, p.PlayerName)
			dropped++
			continue
		}
		out = append(out, p)
	}

	if dropped > 0 {
		fmt.Printf("[ZONE GUARD] Odfiltrowano %d podejrzanych rynków strefowych.\n", dropped)
	}
	return out
}

// ------------------------------------------
// 7.8 Metody mostu Wails
// ------------------------------------------

// ResolveFootyMetricsForMatch ustala ID obu drużyn po wklejeniu linków bukmachera.
// Kolejność: cache -> douczenie się z listy przeciwników drugiej drużyny.
// Gdy obie strony są nieznane, trzeba raz wkleić link do FootyMetrics — potem
// cache rozrasta się sam, bo każda pobrana tabela niesie ID wszystkich rywali.
func (a *App) ResolveFootyMetricsForMatch(homeSlug, awaySlug string) FMTeamIDs {
	res := FMTeamIDs{
		HomeID: findFMTeamIDBySlug(homeSlug),
		AwayID: findFMTeamIDBySlug(awaySlug),
	}
	if res.HomeID != "" && res.AwayID != "" {
		return res
	}

	known := res.HomeID
	if known == "" {
		known = res.AwayID
	}
	if known == "" {
		fmt.Printf("[FOOTYMETRICS] Brak ID w cache dla '%s' i '%s' — potrzebny jednorazowy link.\n", homeSlug, awaySlug)
		return res
	}

	fmt.Printf("[FOOTYMETRICS] Douczam cache z przeciwników drużyny %s...\n", known)
	if raw, err := fmFetch(footyMetricsTableURL(known, "38")); err == nil {
		var resp fmTableResponse
		if json.Unmarshal(raw, &resp) == nil {
			harvestFMOpponents(&resp)
		}
	} else {
		fmt.Printf("[FOOTYMETRICS] Douczanie nieudane: %v\n", err)
	}

	if res.HomeID == "" {
		res.HomeID = findFMTeamIDBySlug(homeSlug)
	}
	if res.AwayID == "" {
		res.AwayID = findFMTeamIDBySlug(awaySlug)
	}
	return res
}

// HarvestFootyMetrics przyjmuje dowolny link z FootyMetrics i uzupełnia cache:
//   - strona drużyny   /teams/256-rayo-vallecano
//   - strona ligi      /leagues/11-la-liga        (cała liga naraz)
//   - zapytanie h2h    ?home=377&away=528
//   - same ID          "377/528"
//
// Adres strony drużyny niesie wewnętrzne ID serwisu (256), a API tabeli chce
// apid (377), więc każdy kandydat jest weryfikowany zapytaniem, a w razie
// niepowodzenia apid odczytujemy z adresu herbu na stronie.
func (a *App) HarvestFootyMetrics(input, homeSlug, awaySlug string) FMHarvestResult {
	text := strings.TrimSpace(input)
	if text == "" {
		return FMHarvestResult{Message: "Wklej link do FootyMetrics albo dwa ID."}
	}

	res := FMHarvestResult{}
	added := 0

	// 1) Linki do stron drużyn
	teamLinks := fmTeamPathRe.FindAllStringSubmatch(text, -1)
	if len(teamLinks) > 0 {
		cache := loadTeamCacheFull()
		seen := make(map[string]bool)
		homeCore := canonicalTeamCore(homeSlug)
		awayCore := canonicalTeamCore(awaySlug)

		for _, m := range teamLinks {
			candidate, slug := m[1], m[2]
			if seen[slug] {
				continue // ten sam link wklejony dwa razy (np. w składni markdown)
			}
			seen[slug] = true

			pageURL := fmt.Sprintf("%s/teams/%s-%s", footyMetricsBase, candidate, slug)
			resolved := resolveFMCandidate(candidate, pageURL)
			if resolved == "" {
				res.Message = fmt.Sprintf("Nie udało się ustalić ID dla '%s'.", slug)
				continue
			}

			target := fmSlugForTeamName(slug, cache)
			if target != "" {
				added += saveTeamCacheFM(map[string]string{target: resolved})
			} else {
				fmt.Printf("[FOOTYMETRICS] '%s' nie ma odpowiednika w team_cache.json — ID %s nie zostanie zapamiętane.\n", slug, resolved)
			}

			// Przypisanie po nazwie, nie po kolejności wklejenia.
			core := canonicalTeamCore(slug)
			switch {
			case core == homeCore || (target != "" && target == fmSlugForTeamName(homeSlug, cache)):
				res.HomeID = resolved
			case core == awayCore || (target != "" && target == fmSlugForTeamName(awaySlug, cache)):
				res.AwayID = resolved
			case res.HomeID == "":
				res.HomeID = resolved
			case res.AwayID == "":
				res.AwayID = resolved
			}
		}
	}

	// 2) Strona ligi — jedno wejście wypełnia cache dla całych rozgrywek
	if leagueURL := fmLeaguePathRe.FindString(text); leagueURL != "" && len(teamLinks) == 0 {
		if body, err := fmFetchPage(footyMetricsBase + leagueURL); err == nil {
			added += storeHarvestedFMNames(harvestFMFromText(body))
		} else {
			res.Message = fmt.Sprintf("Strona ligi nie odpowiedziała: %v", err)
		}
	}

	// 3) h2h albo gołe ID
	if res.HomeID == "" && res.AwayID == "" {
		if home, away, ok := ParseFootyMetricsIDs(text); ok {
			h2hAdded, err := FetchFootyMetricsH2H(home, away)
			if err != nil {
				res.Message = fmt.Sprintf("h2h nie odpowiedziało: %v", err)
			} else {
				added += h2hAdded
				res.HomeID, res.AwayID = home, away
			}
		}
	}

	// 4) Domknięcie z cache dla drużyn z aktualnego meczu
	if res.HomeID == "" {
		res.HomeID = findFMTeamIDBySlug(homeSlug)
	}
	if res.AwayID == "" {
		res.AwayID = findFMTeamIDBySlug(awaySlug)
	}

	// Przypięcie wyniku do slugów bieżącego meczu (z uwzględnieniem aliasów)
	cache := loadTeamCacheFull()
	saveTeamCacheFM(map[string]string{
		fmSlugForTeamName(homeSlug, cache): res.HomeID,
		fmSlugForTeamName(awaySlug, cache): res.AwayID,
	})

	res.TeamsAdded = added
	for _, entry := range loadTeamCacheFull() {
		if entry.FootyMetrics != "" {
			res.TeamsTotal++
		}
	}
	if res.Message == "" {
		res.Message = fmt.Sprintf("Dodano %d drużyn (w team_cache.json łącznie %d z ID FootyMetrics).", added, res.TeamsTotal)
	}
	return res
}

// GetFootyMetricsIDs podpowiada ID z cache po ID StatsHub.
func (a *App) GetFootyMetricsIDs(homeStatsID, awayStatsID string) FMTeamIDs {
	return FMTeamIDs{
		HomeID: findFMTeamIDByStatsHubID(homeStatsID),
		AwayID: findFMTeamIDByStatsHubID(awayStatsID),
	}
}

// DumpFootyMetrics zrzuca surową odpowiedź do pliku obok pliku wykonywalnego.
func (a *App) DumpFootyMetrics(teamID string, period int) (string, error) {
	if period <= 0 {
		period = 5
	}
	raw, err := fmFetch(footyMetricsTableURL(strings.TrimSpace(teamID), strconv.Itoa(period)))
	if err != nil {
		return "", err
	}
	path := getCacheFilePath(fmt.Sprintf("fm_dump_%s.json", teamID))
	if err := os.WriteFile(path, raw, 0644); err != nil {
		return "", err
	}
	return path, nil
}
