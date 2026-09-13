package main

import (
	"encoding/json"
	"testing"
)

func TestFindTeamIDInCacheRecognizesBookmakerVariants(t *testing.T) {
	cache := map[string]string{
		"rb-leipzig":   "36360",
		"hamburger-sv": "2676",
	}
	if got := findTeamIDInCache("rb-lipsk", cache); got != "36360" {
		t.Fatalf("rb-lipsk mapped to %q, want 36360", got)
	}
	if got := findTeamIDInCache("hamburger", cache); got != "2676" {
		t.Fatalf("hamburger mapped to %q, want 2676", got)
	}
}

func TestUnorderedTeamKeyIgnoresHomeAwayOrder(t *testing.T) {
	if got, want := unorderedTeamKey("36360", "2676"), unorderedTeamKey("2676", "36360"); got != want {
		t.Fatalf("team keys differ for the same pair: %q != %q", got, want)
	}
}

func TestDisplayOpponentNameDoesNotLeaveClubPrefixesAsLabels(t *testing.T) {
	tests := map[string]string{
		"1. FC Köln":          "Köln",
		"FC Heidenheim":       "Heidenheim",
		"RB Leipzig":          "Leipzig",
		"Bayer 04 Leverkusen": "Bayer 04",
	}
	for input, want := range tests {
		if got := displayOpponentName(input); got != want {
			t.Errorf("displayOpponentName(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestCalculateValueBetsAddsTheIncomingPlayersStatsToThePlayerWhoLeft(t *testing.T) {
	// Starter Player schodzi z boiska, a Replacement Player wchodzi za niego.
	bet := analyzeSingleSubstitution(t, float64(20), nil, nil, float64(10), 2)
	if got := bet.SampleValues; len(got) != 1 || got[0] != 1 {
		t.Fatalf("base sample = %v, want [1]", got)
	}
	if got := bet.SampleValuesSuperSub; len(got) != 1 || got[0] != 3 {
		t.Fatalf("SuperSub sample = %v, want [3]", got)
	}
}

func TestCalculateValueBetsDoesNotAttributeOutgoingPlayersStatsToIncomingPlayer(t *testing.T) {
	// Paul Mendy (16 min, 1 strzał) wszedł za Gennaro Borellego (74 min,
	// 5 strzałów) w meczu Milan–Cagliari. Statystyki Borellego nie mogą
	// zostać doliczone do Mendy'ego.
	bet := analyzeSingleSubstitution(t, nil, float64(20), float64(10), nil, 5)
	if got := bet.SampleValuesSuperSub; len(got) != 1 || got[0] != 1 {
		t.Fatalf("incoming player's SuperSub sample = %v, want [1]", got)
	}
}

func analyzeSingleSubstitution(t *testing.T, starterSubstitutedIn, starterSubstitutedOut, replacementSubstitutedIn, replacementSubstitutedOut any, replacementShots int) AnalyzedBet {
	t.Helper()
	response := StatsHubResponse{
		Data: []StatsHubPlayer{
			{
				ID:   10,
				Name: "Starter Player",
				Stats: map[string]StatsHubMatchDetail{
					"match": {
						EventID:        100,
						MinutesPlayed:  65,
						SubstitutedIn:  starterSubstitutedIn,
						SubstitutedOut: starterSubstitutedOut,
						Shots:          1,
					},
				},
			},
			{
				ID:   20,
				Name: "Replacement Player",
				Stats: map[string]StatsHubMatchDetail{
					"match": {
						EventID:        100,
						MinutesPlayed:  25,
						SubstitutedIn:  replacementSubstitutedIn,
						SubstitutedOut: replacementSubstitutedOut,
						Shots:          replacementShots,
					},
				},
			},
		},
	}
	response.Events = append(response.Events, StatsHubEvent{})
	response.Events[0].Events.ID = 100
	response.Events[0].Events.TimeStartTimestamp = 1

	raw, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}

	bets, err := CalculateValueBets([]NormalizedProp{{
		PlayerName:  "Starter Player",
		MarketType:  MarketShotsTotal,
		MarketLabel: "Strzały",
		Line:        1.5,
		Odds:        2,
	}}, string(raw), "{}", 1, "ALL", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(bets) != 1 {
		t.Fatalf("expected one bet, got %d", len(bets))
	}
	return bets[0]
}
