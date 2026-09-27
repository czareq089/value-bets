package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	http "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

const sofaBase = "https://www.sofascore.com/api/v1"

// ==========================================
// SCHEMAS
// ==========================================

type TeamItem struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Slug    string `json:"slug"`
	Country struct {
		Name string `json:"name"`
	} `json:"country"`
}

type SofaSearchResponse struct {
	Teams   []TeamItem `json:"teams"`
	Results []struct {
		Type   string   `json:"type"`
		Entity TeamItem `json:"entity"`
	} `json:"results"`
}

func (s *SofaSearchResponse) ExtractTeams() []TeamItem {
	var list []TeamItem
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

type SofaLineupPlayer struct {
	Player struct {
		ID        int    `json:"id"`
		Name      string `json:"name"`
		Slug      string `json:"slug"`
		ShortName string `json:"shortName"`
	} `json:"player"`
	Substitute bool            `json:"substitute"`
	Position   string          `json:"position"`
	Statistics SofaPlayerStats `json:"statistics"`
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

type SofaTeamEventsResponse struct {
	Events []struct {
		ID         int    `json:"id"`
		Slug       string `json:"slug"`
		StartTS    int64  `json:"startTimestamp"`
		Tournament struct {
			Name string `json:"name"`
		} `json:"tournament"`
		HomeTeam struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		} `json:"homeTeam"`
		AwayTeam struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		} `json:"awayTeam"`
	} `json:"events"`
}

// ==========================================
// TLS IMPERSONATING CLIENT
// ==========================================

type Client struct {
	tlsClient tls_client.HttpClient
}

func NewClient() *Client {
	jar := tls_client.NewCookieJar()
	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(25),
		tls_client.WithClientProfile(profiles.Firefox_120),
		tls_client.WithCookieJar(jar),
		tls_client.WithRandomTLSExtensionOrder(),
	}

	c, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), options...)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize TLS client: %v", err))
	}

	return &Client{tlsClient: c}
}

func (c *Client) Get(endpoint string) ([]byte, error) {
	reqURL := endpoint
	if !strings.HasPrefix(reqURL, "http") {
		reqURL = sofaBase + endpoint
	}

	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	// Browser header ordering and tokens matching real Firefox sessions
	req.Header = http.Header{
		"user-agent":       {"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"},
		"accept":           {"*/*"},
		"accept-language":  {"en-US,en;q=0.9"},
		"referer":          {"https://www.sofascore.com/"},
		"x-requested-with": {"c88344"},
		"sec-fetch-dest":   {"empty"},
		"sec-fetch-mode":   {"cors"},
		"sec-fetch-site":   {"same-origin"},
		"te":               {"trailers"},
		http.HeaderOrderKey: {
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

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}

func saveCache(slug, id string) {
	cachePath := "sofa_team_cache.json"
	m := make(map[string]string)
	if data, err := os.ReadFile(cachePath); err == nil {
		_ = json.Unmarshal(data, &m)
	}
	m[strings.ToLower(slug)] = id
	b, _ := json.MarshalIndent(m, "", "  ")
	_ = os.WriteFile(cachePath, b, 0644)
	fmt.Printf(" Saved to cache: %s -> %s\n", slug, id)
}

func saveJSON(filename string, data any) error {
	_ = os.MkdirAll(filepath.Dir(filename), 0755)
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, b, 0644)
}

// ==========================================
// CLI
// ==========================================

func main() {
	searchTeam := flag.String("search-team", "", "Search team by name to get SofaScore ID")
	teamID := flag.String("team", "", "Team ID to inspect tournaments and recent matches")
	eventID := flag.Int("event", 0, "Event ID to inspect lineups and player stats")
	dumpAll := flag.Bool("dump", false, "Save responses to disk")
	flag.Parse()

	client := NewClient()

	// 1. Search Team by Name
	if *searchTeam != "" {
		fmt.Printf("Searching team: %q...\n", *searchTeam)
		q := url.QueryEscape(*searchTeam)
		endpoint := fmt.Sprintf("/search/all?q=%s&page=0", q)

		body, err := client.Get(endpoint)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}

		var res SofaSearchResponse
		if err := json.Unmarshal(body, &res); err != nil {
			fmt.Printf("JSON parse error: %v\n", err)
			return
		}

		teams := res.ExtractTeams()
		if len(teams) == 0 {
			fmt.Println("No teams found.")
			return
		}

		fmt.Printf("Found %d team(s):\n", len(teams))
		for _, t := range teams {
			fmt.Printf("  • ID: %-8d | Name: %-25s | Slug: %s\n", t.ID, t.Name, t.Slug)
			saveCache(t.Slug, strconv.Itoa(t.ID))
		}

		if *dumpAll {
			_ = saveJSON(fmt.Sprintf("sofa_search_%s.json", *searchTeam), teams)
		}
		return
	}

	// 2. Fetch Team Tournaments & Matches
	if *teamID != "" {
		fmt.Printf("Fetching data for Team ID: %s...\n", *teamID)

		// Tournaments & Seasons
		seasonsBody, err := client.Get(fmt.Sprintf("/team/%s/standings/seasons", *teamID))
		if err == nil {
			var seasonsRes struct {
				TournamentSeasons []struct {
					Tournament struct {
						Name             string `json:"name"`
						UniqueTournament struct {
							Name string `json:"name"`
							ID   int    `json:"id"`
						} `json:"uniqueTournament"`
					} `json:"tournament"`
					Seasons []struct {
						ID   int    `json:"id"`
						Name string `json:"name"`
						Year string `json:"year"`
					} `json:"seasons"`
				} `json:"tournamentSeasons"`
			}
			if json.Unmarshal(seasonsBody, &seasonsRes) == nil {
				fmt.Println("\n Tournaments:")
				for _, ts := range seasonsRes.TournamentSeasons {
					fmt.Printf("  • %s (ID: %d)\n", ts.Tournament.UniqueTournament.Name, ts.Tournament.UniqueTournament.ID)
					for _, s := range ts.Seasons {
						fmt.Printf("     ↳ %s (Season ID: %d, Year: %s)\n", s.Name, s.ID, s.Year)
					}
				}
			}
		}

		// Match Events
		fmt.Printf("\n Fetching match events...\n")
		body, err := client.Get(fmt.Sprintf("/team/%s/events/last/0", *teamID))
		if err != nil {
			fmt.Printf("Events error: %v\n", err)
			return
		}

		var evRes SofaTeamEventsResponse
		if err := json.Unmarshal(body, &evRes); err == nil {
			fmt.Printf("Found %d matches:\n", len(evRes.Events))
			for _, e := range evRes.Events {
				fmt.Printf("  • Match ID: %-10d | %s vs %s (%s)\n",
					e.ID, e.HomeTeam.Name, e.AwayTeam.Name, e.Tournament.Name)
			}
			if *dumpAll {
				_ = saveJSON(fmt.Sprintf("sofa_team_%s_events.json", *teamID), evRes)
			}
		}
		return
	}

	// 3. Match Lineup & Stats
	if *eventID > 0 {
		fmt.Printf("Fetching lineups for Event ID: %d...\n", *eventID)
		body, err := client.Get(fmt.Sprintf("/event/%d/lineups", *eventID))
		if err != nil {
			fmt.Printf("Lineup error: %v\n", err)
			return
		}

		var lineup SofaLineupResponse
		if err := json.Unmarshal(body, &lineup); err != nil {
			fmt.Printf("Parse error: %v\n", err)
			return
		}

		printSide := func(title string, players []SofaLineupPlayer) {
			fmt.Printf("\n--- %s (%d players) ---\n", title, len(players))
			for _, p := range players {
				role := "STARTER"
				if p.Substitute {
					role = "BENCH  "
				}
				st := p.Statistics
				fmt.Printf("  [%s] ID: %-8d | %-22s | Min: %-2d | Shots: %d | Tackles: %d\n",
					role, p.Player.ID, p.Player.Name, st.MinutesPlayed, st.TotalShots, st.WonTackle)
			}
		}

		printSide("Home Team", lineup.Home.Players)
		printSide("Away Team", lineup.Away.Players)

		if *dumpAll {
			_ = saveJSON(fmt.Sprintf("sofa_event_%d_lineups.json", *eventID), lineup)
		}
		return
	}

	fmt.Println("Usage:")
	fmt.Println("  go run .\\cmd\\importer\\main.go -search-team \"England\"")
	fmt.Println("  go run .\\cmd\\importer\\main.go -team 4713")
	fmt.Println("  go run .\\cmd\\importer\\main.go -event 15534084")
}
