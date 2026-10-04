package main

import (
	"encoding/json"
	"testing"
)

func TestPlayerShotsExcludesOwnGoalAgainstWolverhampton(t *testing.T) {
	// Mavropanos, event 16391640: four shots and one goal; the API also
	// includes his own goal as shotType=goal, goalType=own.
	const fixture = `{"shotmap":[
		{"player":{"id":829022},"shotType":"miss","playerCoordinates":{"x":5.1,"y":47.2}},
		{"player":{"id":829022},"shotType":"goal","goalType":"own","playerCoordinates":{"x":8.2,"y":52.3}},
		{"player":{"id":829022},"shotType":"goal","goalType":"regular","playerCoordinates":{"x":6.9,"y":50.6}},
		{"player":{"id":829022},"shotType":"block","playerCoordinates":{"x":4.8,"y":53}},
		{"player":{"id":829022},"shotType":"block","playerCoordinates":{"x":10,"y":39.1}}
	]}`
	var response SofaShotmapResponse
	if err := json.Unmarshal([]byte(fixture), &response); err != nil {
		t.Fatal(err)
	}
	shots := response.PlayerShots(829022, 0)
	if len(shots) != 4 {
		t.Fatalf("got %d shots, want 4", len(shots))
	}
	target, goals, inside := 0, 0, 0
	for _, shot := range shots {
		if shot.OnTarget {
			target++
		}
		if shot.ShotType == "goal" {
			goals++
		}
		if shot.InBox {
			inside++
		}
		if shot.IsSub {
			t.Fatal("player shot marked as substitute")
		}
	}
	if target != 1 || goals != 1 || inside != 4 {
		t.Fatalf("got on-target=%d, goals=%d, inside=%d; want 1, 1, 4", target, goals, inside)
	}
}

func TestPlayerShotsFiltersSubstituteAndUnrelatedPlayers(t *testing.T) {
	const fixture = `{"shotmap":[
		{"player":{"id":1},"shotType":"goal","goalType":"own"},
		{"player":{"id":2},"shotType":"goal","goalType":"own"},
		{"player":{"id":2},"shotType":"save","playerCoordinates":{"x":25,"y":50}},
		{"player":{"id":3},"shotType":"goal","goalType":"regular"},
		{"player":{"id":0},"shotType":"goal"}
	]}`
	var response SofaShotmapResponse
	if err := json.Unmarshal([]byte(fixture), &response); err != nil {
		t.Fatal(err)
	}
	shots := response.PlayerShots(1, 2)
	if len(shots) != 1 || !shots[0].IsSub || !shots[0].OnTarget || shots[0].InBox {
		t.Fatalf("unexpected substitute shots: %+v", shots)
	}
	if shots := response.PlayerShots(1, 0); len(shots) != 0 {
		t.Fatalf("unexpected shots without a substitute: %+v", shots)
	}
}

func TestExcludeBlockedShotsFromOnTarget(t *testing.T) {
	tests := []struct {
		name       string
		stats      SofaPlayerStats
		shots      []ShotCoordinate
		isSub      bool
		wantTarget int
		wantBlock  int
	}{
		{
			name:  "goal-line block is separate from saves and goals",
			stats: SofaPlayerStats{TotalShots: 5, OnTargetScoringAttempt: 3, BlockedScoringAttempt: 2},
			shots: []ShotCoordinate{
				{ShotType: "save"}, {ShotType: "goal"}, {ShotType: "block"},
				{ShotType: "blocked"}, {ShotType: "blocked-off-line"},
			},
			wantTarget: 2, wantBlock: 3,
		},
		{
			name:       "ordinary blocks are already excluded by SofaScore",
			stats:      SofaPlayerStats{TotalShots: 3, OnTargetScoringAttempt: 1, BlockedScoringAttempt: 2},
			shots:      []ShotCoordinate{{ShotType: "save"}, {ShotType: "block"}, {ShotType: "blocked"}},
			wantTarget: 1, wantBlock: 2,
		},
		{
			name:       "player does not include substitute goal-line block",
			stats:      SofaPlayerStats{TotalShots: 1, OnTargetScoringAttempt: 1},
			shots:      []ShotCoordinate{{ShotType: "save"}, {ShotType: "blocked-off-line", IsSub: true}},
			wantTarget: 1, wantBlock: 0,
		},
		{
			name:  "substitute excludes only their own goal-line block",
			stats: SofaPlayerStats{TotalShots: 2, OnTargetScoringAttempt: 2},
			shots: []ShotCoordinate{
				{ShotType: "blocked-off-line"}, {ShotType: "blocked-off-line", IsSub: true},
				{ShotType: "goal", IsSub: true},
			},
			isSub: true, wantTarget: 1, wantBlock: 1,
		},
		{
			name:       "missing shotmap preserves available statistics",
			stats:      SofaPlayerStats{TotalShots: 3, OnTargetScoringAttempt: 1, BlockedScoringAttempt: 2},
			wantTarget: 1, wantBlock: 2,
		},
		{
			name:       "inconsistent shotmap cannot make count negative",
			stats:      SofaPlayerStats{TotalShots: 1, BlockedScoringAttempt: 1},
			shots:      []ShotCoordinate{{ShotType: "blocked-off-line"}},
			wantTarget: 0, wantBlock: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := tt.stats
			excludeBlockedShotsFromOnTarget(&stats, tt.shots, tt.isSub)
			if stats.OnTargetScoringAttempt != tt.wantTarget || stats.BlockedScoringAttempt != tt.wantBlock {
				t.Fatalf("got on-target=%d, blocked=%d; want on-target=%d, blocked=%d",
					stats.OnTargetScoringAttempt, stats.BlockedScoringAttempt, tt.wantTarget, tt.wantBlock)
			}
			if stats.TotalShots != tt.stats.TotalShots {
				t.Fatalf("total shots changed from %d to %d", tt.stats.TotalShots, stats.TotalShots)
			}
		})
	}
}
