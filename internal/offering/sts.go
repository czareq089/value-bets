package offering

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/gorilla/websocket"
)

const STSWebSocket = "wss://www.sts.pl/sbk/api/sbk"

type CalendarEvent struct {
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

// FetchSTSSnapshot subscribes to the existing STS endpoint. Empty fixtureID asks
// for i_pl; otherwise only the selected fixture's market snapshot is accepted.
func FetchSTSSnapshot(parent context.Context, endpoint, fixtureID string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	channel := "i_pl"
	subscriptions := []map[string]any{{"s": channel, "n": 0}}
	if fixtureID != "" {
		for _, c := range fixtureID {
			if c < '0' || c > '9' {
				return nil, fmt.Errorf("niepoprawne ID STS")
			}
		}
		channel = "f_f" + fixtureID + "_pl"
		subscriptions = append(subscriptions, map[string]any{"s": channel, "n": 0})
	}
	dialer := websocket.Dialer{HandshakeTimeout: 7 * time.Second}
	header := http.Header{}
	header.Set("Origin", "https://www.sts.pl")
	header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	conn, _, err := dialer.DialContext(ctx, endpoint, header)
	if err != nil {
		return nil, fmt.Errorf("STS WebSocket: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(32 << 20)
	deadline, _ := ctx.Deadline()
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return nil, err
	}
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	if err := conn.WriteJSON(map[string]any{"t": 1, "u": subscriptions}); err != nil {
		return nil, err
	}
	// The only writer after subscribing sends the same heartbeat as the desktop.
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if conn.WriteJSON(map[string]any{"t": 0}) != nil {
					return
				}
			}
		}
	}()
	pendingChannel := ""
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("STS odczyt snapshotu: %w", err)
		}
		// STS can send channel metadata and payload as two JSON values. Support
		// both a combined message and a separate metadata message.
		decoder := json.NewDecoder(bytes.NewReader(msg))
		for {
			var object map[string]json.RawMessage
			if err := decoder.Decode(&object); err != nil {
				break
			}
			if rawChannel, ok := object["s"]; ok {
				_ = json.Unmarshal(rawChannel, &pendingChannel)
				continue
			}
			if len(object) == 0 {
				continue
			}
			matches := false
			if fixtureID == "" {
				matches = (pendingChannel == "" || pendingChannel == channel) && hasSTSCalendar(object)
			} else {
				matches = (pendingChannel == channel && object["P"] != nil) || (pendingChannel == "" && hasSTSFixtureMarkets(object["P"], fixtureID))
			}
			pendingChannel = ""
			if matches {
				return json.Marshal(object)
			}
		}
	}
}

func hasSTSCalendar(object map[string]json.RawMessage) bool {
	var b struct {
		S map[string]json.RawMessage `json:"S"`
	}
	return json.Unmarshal(object["B"], &b) == nil && b.S["1"] != nil
}

func hasSTSFixtureMarkets(raw json.RawMessage, fixtureID string) bool {
	var p map[string]struct {
		Fixture string                     `json:"f"`
		Markets map[string]json.RawMessage `json:"m"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return false
	}
	for key, bundle := range p {
		if (key == "f"+fixtureID || bundle.Fixture == "f"+fixtureID) && len(bundle.Markets) > 0 {
			return true
		}
	}
	return false
}

func ParseSTSCalendar(raw []byte, date string) ([]CalendarEvent, error) {
	if date == "" {
		return nil, fmt.Errorf("niepoprawna data kalendarza: pusta data")
	}
	return parseSTSCalendar(raw, date)
}

func ParseSTSFullCalendar(raw []byte) ([]CalendarEvent, error) {
	return parseSTSCalendar(raw, "")
}

func parseSTSCalendar(raw []byte, date string) ([]CalendarEvent, error) {
	zone, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		return nil, err
	}
	var start, end time.Time
	if date != "" {
		start, err = time.ParseInLocation("2006-01-02", date, zone)
		if err != nil {
			return nil, fmt.Errorf("niepoprawna data kalendarza: %w", err)
		}
		end = start.AddDate(0, 0, 1)
	}
	type fixture struct {
		Home struct {
			Name string `json:"n"`
		} `json:"H"`
		Away struct {
			Name string `json:"n"`
		} `json:"A"`
		Type    string `json:"ft"`
		Start   string `json:"t"`
		Builder bool   `json:"Bb"`
	}
	var snapshot struct {
		B struct {
			Sports map[string]struct {
				Categories map[string]struct {
					Name        string `json:"n"`
					Tournaments map[string]struct {
						Name     string             `json:"n"`
						Fixtures map[string]fixture `json:"FX"`
					} `json:"T"`
				} `json:"C"`
			} `json:"S"`
		} `json:"B"`
		Live map[string]json.RawMessage `json:"Y"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	found := false
	for {
		var object json.RawMessage
		err := decoder.Decode(&object)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("STS niepoprawny snapshot: %w", err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(object, &fields); err != nil {
			return nil, err
		}
		if hasSTSCalendar(fields) {
			if err := json.Unmarshal(object, &snapshot); err != nil {
				return nil, err
			}
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("STS brak kalendarza piłki nożnej w snapshocie i_pl")
	}
	byID := make(map[string]CalendarEvent)
	for _, category := range snapshot.B.Sports["1"].Categories {
		for _, tournament := range category.Tournaments {
			for key, fixture := range tournament.Fixtures {
				id := strings.TrimPrefix(key, "f")
				if id == key || id == "" {
					continue
				}
				validID := true
				for _, digit := range id {
					if digit < '0' || digit > '9' {
						validID = false
					}
				}
				when, err := time.Parse(time.RFC3339, fixture.Start)
				home, away := strings.TrimSpace(fixture.Home.Name), strings.TrimSpace(fixture.Away.Name)
				if !validID || fixture.Type != "m" || err != nil || home == "" || away == "" {
					continue
				}
				if date != "" && (when.Before(start) || !when.Before(end)) {
					continue
				}
				byID[id] = CalendarEvent{ID: id, Home: home, Away: away,
					Competition: strings.TrimSpace(tournament.Name), Country: strings.TrimSpace(category.Name),
					StartTime: when.In(zone).Format(time.RFC3339), BetBuilder: fixture.Builder,
					Live: snapshot.Live[key] != nil,
					URL:  "https://www.sts.pl/kursy/pilka-nozna/" + key,
				}
			}
		}
	}
	events := make([]CalendarEvent, 0, len(byID))
	for _, event := range byID {
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool {
		left, _ := time.Parse(time.RFC3339, events[i].StartTime)
		right, _ := time.Parse(time.RFC3339, events[j].StartTime)
		if !left.Equal(right) {
			return left.Before(right)
		}
		return events[i].ID < events[j].ID
	})
	return events, nil
}
