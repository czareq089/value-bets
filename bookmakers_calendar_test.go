package main

import (
	"testing"

	"value-bets-3.0/internal/offering"
)

func TestBookmakerCalendarURLsWorkWithExistingAnalysis(t *testing.T) {
	for _, tt := range []struct {
		url      string
		provider string
	}{
		{"https://www.sts.pl/kursy/pilka-nozna/f123", "STS"},
		{"https://www.betclic.pl/pilka-nozna-s1/example-m123", "BETCLIC"},
	} {
		result := desktopCalendar("2026-10-04", []offering.CalendarEvent{{ID: "123", Home: "Bayern München", Away: "Łódź", URL: tt.url}}, []string{"warning"})
		if len(result.Events) != 1 || len(result.Warnings) != 1 {
			t.Fatalf("missing data: %+v", result)
		}
		id, slug, provider := extractIDAndSlug(result.Events[0].URL)
		home, away := parseTeamsFromSlug(slug, provider)
		if id != "123" || provider != tt.provider || home != "bayern-munchen" || away != "lodz" {
			t.Fatalf("incompatible calendar URL: %s (%s %s %s)", result.Events[0].URL, id, home, away)
		}
	}
}
