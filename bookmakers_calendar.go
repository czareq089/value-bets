package main

import (
	"context"
	"fmt"
	"strings"

	"value-bets-3.0/internal/offering"
)

// Calendar lookups share persistent backend snapshots across dates and callers.
func (a *App) GetSTSCalendar(date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), "STS", date, false)
}

func (a *App) calendarContext() context.Context {
	ctx := a.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx
}

func parseSTSCalendar(raw []byte, date string) (*SuperbetCalendar, error) {
	events, err := offering.ParseSTSCalendar(raw, date)
	if err != nil {
		return nil, err
	}
	return desktopCalendar(date, events, []string{}), nil
}

func (a *App) GetBetclicCalendar(date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), "BETCLIC", date, false)
}

func (a *App) RefreshBookmakerCalendar(bookmaker, date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), strings.ToUpper(strings.TrimSpace(bookmaker)), date, true)
}

func (a *App) getBookmakerCalendar(ctx context.Context, bookmaker, date string, force bool) (*SuperbetCalendar, error) {
	if _, _, err := superbetCalendarDay(date); err != nil {
		return nil, err
	}
	cache := offering.DefaultCalendarCache()
	var result *offering.Calendar
	var err error
	switch bookmaker {
	case "STS":
		result, err = offering.FetchCachedSTSCalendar(ctx, cache, offering.STSWebSocket, date, force)
	case "BETCLIC":
		result, err = offering.FetchCachedBetclicCalendar(ctx, cache, GetSharedSofaClient().httpClient,
			offering.BetclicMatchService+"GetMatchesBySportWithNotifications", date, force)
		if err != nil {
			return nil, fmt.Errorf("%w (diagnostyka: %s)", err, offering.BetclicDebugLogPath())
		}
	case "SUPERBET":
		result, err = cache.Get(ctx, "superbet:"+date, force, func(ctx context.Context) (*offering.Calendar, error) {
			calendar, err := loadSuperbetCalendarContext(ctx, GetSharedSofaClient().httpClient, date,
				"https://production-superbet-offer-pl.freetls.fastly.net", "https://production-superbet-bmb.freetls.fastly.net")
			if err != nil {
				return nil, err
			}
			full := &offering.Calendar{Date: calendar.Date, Events: []offering.CalendarEvent{}, Warnings: calendar.Warnings}
			for _, event := range calendar.Events {
				full.Events = append(full.Events, offering.CalendarEvent{
					ID: event.ID, Home: event.Home, Away: event.Away, Competition: event.Competition,
					Country: event.Country, StartTime: event.StartTime, URL: event.URL, Live: event.Live, BetBuilder: event.BetBuilder,
				})
			}
			return full, nil
		})
	default:
		return nil, fmt.Errorf("nieobsługiwany bukmacher kalendarza: %s", bookmaker)
	}
	if err != nil {
		return nil, err
	}
	calendar := desktopCalendar(date, result.Events, result.Warnings)
	calendar.FetchedAt = result.FetchedAt
	return calendar, nil
}

func desktopCalendar(date string, events []offering.CalendarEvent, warnings []string) *SuperbetCalendar {
	result := &SuperbetCalendar{Date: date, Events: []SuperbetCalendarEvent{}, Warnings: warnings}
	for _, event := range events {
		slug := calendarTeamSlug(event.Home) + "-vs-" + calendarTeamSlug(event.Away)
		if strings.Contains(event.URL, "www.sts.pl") {
			event.URL = "https://www.sts.pl/kursy/pilka-nozna/" + slug + "/f" + event.ID
		}
		if strings.Contains(event.URL, "www.betclic.pl") {
			event.URL = "https://www.betclic.pl/pilka-nozna-s1/" + slug + "-m" + event.ID
		}
		result.Events = append(result.Events, SuperbetCalendarEvent{
			ID: event.ID, Home: event.Home, Away: event.Away,
			Competition: event.Competition, Country: event.Country,
			StartTime: event.StartTime, URL: event.URL, Live: event.Live, BetBuilder: event.BetBuilder,
		})
	}
	return result
}
