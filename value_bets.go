// All desktop Go code is kept in this file; the Debian server is server/value_bets.go.
package main

import (
	"archive/zip"
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/pkg/browser"
	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
	wailsruntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

// Uruchomienie aplikacji Wails.
//
//go:embed all:frontend
var assets embed.FS

//go:embed assets/bet_journal_template.xlsx
var journalTemplate []byte

// The journal is owned exclusively by this Windows client. JSON is the durable
// source; Excel is a replaceable view, including when Excel locks the workbook.
type journalMeta struct {
	AnalysisID string    `json:"analysisId"`
	FixtureID  string    `json:"fixtureId"`
	Match      string    `json:"match"`
	StartTime  time.Time `json:"startTime"`
	AnalyzedAt time.Time `json:"analyzedAt"`
}
type journalRow struct {
	Key       string          `json:"key"`
	Meta      journalMeta     `json:"meta"`
	PlayerID  int             `json:"playerId"`
	Player    string          `json:"player"`
	Market    CanonicalMarket `json:"market"`
	Label     string          `json:"label"`
	Bookmaker Bookmaker       `json:"bookmaker"`
	Line      float64         `json:"line"`
	Under     bool            `json:"under"`
	Odds      float64         `json:"odds"`
	HitRate   float64         `json:"hitRate"`
	Sample    int             `json:"sample"`
	Verdict   string          `json:"verdict"`
	Mode      string          `json:"mode"`
	Source    string          `json:"source"`
	SavedAt   time.Time       `json:"savedAt"`
	Outcome   string          `json:"outcome"`
	Actual    *int            `json:"actual,omitempty"`
	Minutes   *int            `json:"minutes,omitempty"`
	SettledAt time.Time       `json:"settledAt,omitempty"`
	Note      string          `json:"note,omitempty"`
}
type betJournal struct {
	Version   int                   `json:"version"`
	Selected  map[string]journalRow `json:"selected"`
	All       map[string]journalRow `json:"all"`
	Retry     map[string]time.Time  `json:"retry"`
	UpdatedAt time.Time             `json:"updatedAt"`
}
type JournalStatus struct {
	Path      string    `json:"path"`
	Selected  int       `json:"selected"`
	All       int       `json:"all"`
	Pending   int       `json:"pending"`
	Missing   int       `json:"missing"`
	Busy      bool      `json:"busy"`
	Error     string    `json:"error"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func journalDir() (string, error) {
	// Use a stable per-user location rather than the process working directory.
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(base, "ValueBets", "journal")
	return dir, os.MkdirAll(dir, 0700)
}
func (a *App) loadJournalLocked() error {
	if a.journal != nil {
		return nil
	}
	dir, err := journalDir()
	if err != nil {
		return err
	}
	j := &betJournal{Version: 1}
	raw, err := os.ReadFile(filepath.Join(dir, "bet_journal.json"))
	if err == nil {
		if err = json.Unmarshal(raw, j); err != nil {
			return fmt.Errorf("nie można odczytać lokalnego rejestru; plik zachowano: %w", err)
		}
		if j.Version != 1 {
			return fmt.Errorf("nieobsługiwana wersja lokalnego rejestru: %d", j.Version)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if j.Selected == nil {
		j.Selected = map[string]journalRow{}
	}
	if j.All == nil {
		j.All = map[string]journalRow{}
	}
	if j.Retry == nil {
		j.Retry = map[string]time.Time{}
	}
	a.journal = j
	return nil
}
func journalAtomic(path string, raw []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".journal-*.tmp")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}
func (a *App) saveJournalLocked() error {
	dir, err := journalDir()
	if err != nil {
		return err
	}
	a.journal.UpdatedAt = time.Now()
	raw, err := json.Marshal(a.journal)
	if err != nil {
		return err
	}
	if err = journalAtomic(filepath.Join(dir, "bet_journal.json"), raw); err != nil {
		return fmt.Errorf("zapis lokalnego rejestru: %w", err)
	}
	return nil
}
func journalOfferKey(row journalRow) string {
	fixture := row.Meta.FixtureID
	if fixture == "" {
		fixture = row.Meta.AnalysisID
	}
	return fmt.Sprintf("%s|%s|%d|%s|%g|%t", fixture, row.Bookmaker, row.PlayerID, row.Market, row.Line, row.Under)
}
func (a *App) recordJournal(bets []AnalyzedBet, selected bool, mode string, meta journalMeta) error {
	a.journalMu.Lock()
	err := a.loadJournalLocked()
	if err == nil {
		now := time.Now()
		knownMeta := map[string]journalMeta{}
		for _, rows := range []map[string]journalRow{a.journal.All, a.journal.Selected} {
			for _, row := range rows {
				if row.Meta.FixtureID != "" {
					knownMeta[row.Meta.AnalysisID] = row.Meta
				}
			}
		}
		for _, bet := range bets {
			if bet.ContextPending || bet.PlayerID <= 0 || bet.AnalysisID == "" || bet.BookmakerOdds <= 1 {
				continue
			}
			m := meta
			if m.AnalysisID == "" {
				m.AnalysisID = bet.AnalysisID
			}
			if m.FixtureID == "" {
				if known, ok := knownMeta[m.AnalysisID]; ok {
					m = known
				}
			}
			if mode == "" {
				mode = "all"
			}
			row := journalRow{Meta: m, PlayerID: bet.PlayerID, Player: bet.PlayerName, Market: bet.MarketType, Label: bet.MarketLabel, Bookmaker: bet.Bookmaker, Line: bet.Line, Under: bet.IsUnder, Odds: bet.BookmakerOdds, HitRate: bet.HitRate, Sample: bet.MatchesSample, Verdict: bet.Verdict, Mode: mode, Source: bet.StatsSource, SavedAt: now, Outcome: "OCZEKUJE"}
			row.Key = fmt.Sprintf("%s|%s|%d|%s|%g|%t|%s", bet.AnalysisID, bet.Bookmaker, bet.PlayerID, bet.MarketType, bet.Line, bet.IsUnder, mode)
			if selected {
				if _, exists := a.journal.Selected[row.Key]; !exists {
					a.journal.Selected[row.Key] = row
				}
			} else {
				// One current snapshot of each offer, updated only by an analysis the user opened.
				key := journalOfferKey(row)
				old, exists := a.journal.All[key]
				if !exists || (old.SettledAt.IsZero() && !row.Meta.AnalyzedAt.Before(old.Meta.AnalyzedAt)) {
					row.Key = key
					a.journal.All[key] = row
				}
			}
		}
		err = a.saveJournalLocked()
	}
	if err != nil {
		a.journalError = err.Error()
	}
	a.journalMu.Unlock()
	if err != nil {
		log.Printf("ERROR Lokalny rejestr: %v", err)
	}
	a.wakeJournal()
	a.emitJournalStatus()
	return err
}

// Selecting is idempotent. Removing a card from the temporary picks tray does
// not erase the historical selection and its frozen price from the journal.
func (a *App) SelectJournalBet(item MyBetItem) error {
	if item.AnalysisID == "" || item.PlayerID <= 0 {
		return fmt.Errorf("wybór nie ma ID analizy lub zawodnika; otwórz aktualną analizę")
	}
	if item.BookmakerOdds <= 1 || item.MatchesSample <= 0 || item.MarketType == "" {
		return fmt.Errorf("wybór nie ma kompletnego kursu, rynku lub próbki")
	}
	bet := AnalyzedBet{AnalysisID: item.AnalysisID, PlayerID: item.PlayerID, PlayerName: item.PlayerName, MarketType: item.MarketType, MarketLabel: item.MarketLabel, Bookmaker: item.Bookmaker, Line: item.Line, IsUnder: item.IsUnder, BookmakerOdds: item.BookmakerOdds, HitRate: item.HitRate, MatchesSample: item.MatchesSample, Verdict: item.Verdict, StatsSource: item.StatsSource}
	return a.recordJournal([]AnalyzedBet{bet}, true, item.OpponentFilterMode, journalMeta{})
}
func (a *App) GetJournalStatus() JournalStatus {
	a.journalMu.Lock()
	defer a.journalMu.Unlock()
	dir, err := journalDir()
	status := JournalStatus{Path: filepath.Join(dir, "value_bets.xlsx"), Busy: a.journalBusy, Error: a.journalError}
	if err == nil {
		err = a.loadJournalLocked()
	}
	if err != nil {
		status.Error = err.Error()
		return status
	}
	status.Selected, status.All, status.UpdatedAt = len(a.journal.Selected), len(a.journal.All), a.journal.UpdatedAt
	for _, rows := range []map[string]journalRow{a.journal.Selected, a.journal.All} {
		for _, row := range rows {
			if row.Outcome == "OCZEKUJE" {
				status.Pending++
			}
			if row.Outcome == "BRAK DANYCH" {
				status.Missing++
			}
		}
	}
	return status
}
func (a *App) emitJournalStatus() {
	if a.ctx != nil {
		wailsruntime.EventsEmit(a.ctx, "journal:status", a.GetJournalStatus())
	}
}
func (a *App) wakeJournal() {
	if a.journalWake != nil {
		select {
		case a.journalWake <- struct{}{}:
		default:
		}
	}
}
func (a *App) RefreshJournalResults() JournalStatus {
	a.journalMu.Lock()
	if err := a.loadJournalLocked(); err == nil && !a.journalBusy {
		a.journal.Retry = map[string]time.Time{}
		a.journalError = ""
	}
	a.journalMu.Unlock()
	a.wakeJournal()
	return a.GetJournalStatus()
}
func (a *App) OpenJournalWorkbook() error {
	if err := a.exportJournal(); err != nil {
		return err
	}
	dir, err := journalDir()
	if err != nil {
		return err
	}
	// On Windows this library passes the path to ShellExecute, preserving the
	// user's .xlsx association rather than constructing a browser file URL.
	return browser.OpenURL(filepath.Join(dir, "value_bets.xlsx"))
}
func (a *App) journalLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	a.wakeJournal()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-a.journalWake:
		}
		a.journalMu.Lock()
		a.journalBusy = true
		a.journalMu.Unlock()
		a.emitJournalStatus()
		if err := a.exportJournal(); err != nil {
			a.journalMu.Lock()
			a.journalError = err.Error()
			a.journalMu.Unlock()
		}
		a.refreshJournal(ctx)
		if err := a.exportJournal(); err != nil {
			a.journalMu.Lock()
			a.journalError = err.Error()
			a.journalMu.Unlock()
		}
		a.journalMu.Lock()
		a.journalBusy = false
		a.journalMu.Unlock()
		a.emitJournalStatus()
	}
}
func (a *App) refreshJournal(ctx context.Context) {
	a.journalMu.Lock()
	if err := a.loadJournalLocked(); err != nil {
		a.journalError = err.Error()
		a.journalMu.Unlock()
		return
	}
	// Resolve metadata once per analysis; never obtain fixture IDs from user text.
	missing := map[string]bool{}
	for _, rows := range []map[string]journalRow{a.journal.Selected, a.journal.All} {
		for _, row := range rows {
			if (row.Meta.FixtureID == "" || row.Meta.Match == "") && time.Now().After(a.journal.Retry["meta:"+row.Meta.AnalysisID]) {
				missing[row.Meta.AnalysisID] = true
			}
		}
	}
	a.journalMu.Unlock()
	for id := range missing {
		if ctx.Err() != nil {
			return
		}
		var meta journalMeta
		requestCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
		err := a.serverRequest(requestCtx, http.MethodGet, "/api/v1/local-journal/analysis/"+url.PathEscape(id), nil, &meta)
		cancel()
		a.journalMu.Lock()
		if err != nil || meta.FixtureID == "" || meta.StartTime.IsZero() {
			a.journal.Retry["meta:"+id] = time.Now().Add(5 * time.Minute)
			if err != nil {
				a.journalError = "Metadane rejestru: " + err.Error()
			}
		} else {
			if strings.HasPrefix(a.journalError, "Metadane rejestru:") {
				a.journalError = ""
			}
			for _, rows := range []map[string]journalRow{a.journal.Selected, a.journal.All} {
				for key, row := range rows {
					if row.Meta.AnalysisID == id {
						row.Meta = meta
						rows[key] = row
					}
				}
			}
		}
		if err := a.saveJournalLocked(); err != nil {
			a.journalError = err.Error()
		}
		a.journalMu.Unlock()
	}
	type group struct {
		Meta    journalMeta
		Players map[int]bool
		Markets map[CanonicalMarket]bool
	}
	groups := map[string]*group{}
	a.journalMu.Lock()
	// Merge duplicate rows whose verified fixture identity became available later.
	merged := map[string]journalRow{}
	for _, row := range a.journal.All {
		key := journalOfferKey(row)
		old, ok := merged[key]
		if !ok || row.Meta.AnalyzedAt.After(old.Meta.AnalyzedAt) || (row.Meta.AnalyzedAt.Equal(old.Meta.AnalyzedAt) && row.SavedAt.After(old.SavedAt)) {
			row.Key = key
			merged[key] = row
		}
	}
	a.journal.All = merged
	for _, rows := range []map[string]journalRow{a.journal.Selected, a.journal.All} {
		for _, row := range rows {
			if row.Meta.FixtureID == "" || !row.SettledAt.IsZero() || time.Now().Before(row.Meta.StartTime.Add(110*time.Minute)) || time.Now().Before(a.journal.Retry[row.Meta.FixtureID]) {
				continue
			}
			// Player sets can vary between analyses; use one request per analysis and fixture.
			key := row.Meta.FixtureID + "|" + row.Meta.AnalysisID
			g := groups[key]
			if g == nil {
				g = &group{Meta: row.Meta, Players: map[int]bool{}, Markets: map[CanonicalMarket]bool{}}
				groups[key] = g
			}
			g.Players[row.PlayerID] = true
			g.Markets[row.Market] = true
		}
	}
	a.journalMu.Unlock()
	for _, g := range groups {
		if ctx.Err() != nil {
			return
		}
		ids := []int{}
		markets := []CanonicalMarket{}
		for id := range g.Players {
			ids = append(ids, id)
		}
		for market := range g.Markets {
			markets = append(markets, market)
		}
		sort.Ints(ids)
		// Limit each request to a small batch so user work can take priority.
		for offset := 0; offset < len(ids); offset += 64 {
			end := min(offset+64, len(ids))
			body, _ := json.Marshal(map[string]any{"analysisId": g.Meta.AnalysisID, "players": ids[offset:end], "markets": markets})
			var result struct {
				FixtureID         string    `json:"fixtureId"`
				StartTime         time.Time `json:"startTime"`
				Status            string    `json:"status"`
				UnsupportedPeriod bool      `json:"unsupportedPeriod"`
				Players           map[string]struct {
					Played  *bool                   `json:"played"`
					Minutes *int                    `json:"minutes"`
					Values  map[CanonicalMarket]int `json:"values"`
					Reason  string                  `json:"reason"`
				} `json:"players"`
			}
			requestCtx, cancel := context.WithTimeout(ctx, 110*time.Second)
			err := a.serverRequest(requestCtx, http.MethodPost, "/api/v1/local-journal/results", body, &result)
			cancel()
			if ctx.Err() != nil {
				return
			}
			a.journalMu.Lock()
			now := time.Now()
			retry := 15 * time.Minute
			if now.After(g.Meta.StartTime.Add(72 * time.Hour)) {
				retry = 24 * time.Hour
			}
			a.journal.Retry[g.Meta.FixtureID] = now.Add(retry)
			if err != nil {
				a.journalError = "Wyniki rejestru: " + err.Error()
			} else if result.FixtureID != g.Meta.FixtureID {
				a.journalError = "Niezgodne ID meczu w odpowiedzi z wynikami"
			} else {
				if strings.HasPrefix(a.journalError, "Wyniki rejestru:") {
					a.journalError = ""
				}
				for _, rows := range []map[string]journalRow{a.journal.Selected, a.journal.All} {
					for id, row := range rows {
						if row.Meta.AnalysisID != g.Meta.AnalysisID || !row.SettledAt.IsZero() {
							continue
						}
						player, hasPlayer := result.Players[strconv.Itoa(row.PlayerID)]
						inBatch := false
						for _, pid := range ids[offset:end] {
							if pid == row.PlayerID {
								inBatch = true
								break
							}
						}
						if !inBatch {
							continue
						}
						if !result.StartTime.IsZero() {
							row.Meta.StartTime = result.StartTime
						}
						switch result.Status {
						case "canceled", "cancelled":
							row.Outcome = "ZWROT"
							row.Note = "Mecz odwołany"
							row.SettledAt = now
						case "finished":
							row.Outcome = "BRAK DANYCH"
							row.Note = "Brak potwierdzonej statystyki zawodnika"
							if result.UnsupportedPeriod {
								row.Note = "Dogrywka lub karne: brak oddzielnej statystyki 90 minut"
							} else if hasPlayer {
								row.Minutes = player.Minutes
								if player.Reason != "" {
									row.Note = player.Reason
								}
								if player.Played != nil && !*player.Played {
									row.Outcome = "ZWROT"
									row.Note = "Zawodnik nie zagrał"
									row.SettledAt = now
								} else if value, known := player.Values[row.Market]; known && player.Played != nil && *player.Played {
									row.Actual = &value
									row.SettledAt = now
									row.Note = "Statystyka SofaScore; regulaminowy czas, bez SuperSub"
									if float64(value) == row.Line {
										row.Outcome = "ZWROT"
									} else if (row.Under && float64(value) < row.Line) || (!row.Under && float64(value) > row.Line) {
										row.Outcome = "TAK"
									} else {
										row.Outcome = "NIE"
									}
								}
							}
						}
						rows[id] = row
					}
				}
			}
			if err := a.saveJournalLocked(); err != nil {
				a.journalError = err.Error()
			}
			a.journalMu.Unlock()
			if err := a.exportJournal(); err != nil {
				a.journalMu.Lock()
				a.journalError = err.Error()
				a.journalMu.Unlock()
			}
			a.emitJournalStatus()
		}
	}
}

func journalProfit(row journalRow) (float64, bool) {
	switch row.Outcome {
	case "TAK":
		return row.Odds*0.88 - 1, true
	case "NIE":
		return -1, true
	case "ZWROT":
		return 0, true
	}
	return 0, false
}
func journalXML(text string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(text))
	return b.String()
}
func journalCell(ref, style string, value any, formula string) string {
	attrs := ` r="` + ref + `" s="` + style + `"`
	if formula != "" {
		switch v := value.(type) {
		case string:
			return `<x:c` + attrs + ` t="str"><x:f>` + journalXML(formula) + `</x:f><x:v>` + journalXML(v) + `</x:v></x:c>`
		default:
			return `<x:c` + attrs + `><x:f>` + journalXML(formula) + `</x:f><x:v>` + fmt.Sprint(value) + `</x:v></x:c>`
		}
	}
	if value == nil {
		return `<x:c` + attrs + ` />`
	}
	if str, ok := value.(string); ok {
		return `<x:c` + attrs + ` t="inlineStr"><x:is><x:t xml:space="preserve">` + journalXML(str) + `</x:t></x:is></x:c>`
	}
	return `<x:c` + attrs + `><x:v>` + fmt.Sprint(value) + `</x:v></x:c>`
}
func journalDate(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	loc, err := time.LoadLocation("Europe/Warsaw")
	if err == nil {
		t = t.In(loc)
	}
	y, m, d := t.Date()
	h, min, sec := t.Clock()
	return time.Date(y, m, d, h, min, sec, 0, time.UTC).Sub(time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)).Hours() / 24
}
func journalSheet(template string, data []journalRow) (string, error) {
	if len(data) > 1048564 {
		return "", fmt.Errorf("przekroczono limit wierszy arkusza Excel; pełny rejestr pozostaje w JSON")
	}
	last := max(13, 12+len(data))
	cellRE := regexp.MustCompile(`(?s)<x:c\b[^>]*(?:/>|>.*?</x:c>)`)
	refRE := regexp.MustCompile(`\br="([A-Z]+)([0-9]+)"`)
	styleRE := regexp.MustCompile(`\bs="([0-9]+)"`)
	rowRE := regexp.MustCompile(`(?s)<x:row\b[^>]*>.*?</x:row>`)
	rows := map[int]map[string]string{}
	styles := map[string]string{}
	for _, rowXML := range rowRE.FindAllString(template, -1) {
		for _, cell := range cellRE.FindAllString(rowXML, -1) {
			ref := refRE.FindStringSubmatch(cell)
			if len(ref) != 3 {
				continue
			}
			r, _ := strconv.Atoi(ref[2])
			if rows[r] == nil {
				rows[r] = map[string]string{}
			}
			rows[r][ref[1]] = cell
			if r == 13 {
				if style := styleRE.FindStringSubmatch(cell); len(style) == 2 {
					styles[ref[1]] = style[1]
				}
			}
		}
	}
	stats := map[string]int{}
	profit := 0.0
	breakdown := map[string]map[string]int{}
	for _, row := range data {
		stats[row.Outcome]++
		if p, ok := journalProfit(row); ok {
			profit += p
		}
		for _, key := range []string{string(row.Bookmaker), row.Verdict} {
			if breakdown[key] == nil {
				breakdown[key] = map[string]int{}
			}
			breakdown[key]["total"]++
			breakdown[key][row.Outcome]++
		}
	}
	win, loss, void := stats["TAK"], stats["NIE"], stats["ZWROT"]
	cached := map[string]any{"W3": len(data), "W4": win, "W5": loss, "W6": void, "W7": stats["OCZEKUJE"], "W8": stats["BRAK DANYCH"], "W9": win + loss + void, "W10": "—", "W11": profit, "W12": "—"}
	if win+loss > 0 {
		cached["W10"] = float64(win) / float64(win+loss)
		cached["W12"] = profit / float64(win+loss)
	}
	for _, part := range []struct {
		Start int
		Names []string
	}{{22, []string{"SUPERBET", "BETCLIC", "STS"}}, {29, []string{"VALUE", "TRAP", "NEUTRAL"}}} {
		for i, name := range part.Names {
			r := strconv.Itoa(part.Start + i)
			b := breakdown[name]
			cached["W"+r] = b["total"]
			cached["X"+r] = b["TAK"]
			cached["Y"+r] = b["NIE"]
			cached["Z"+r] = "—"
			if b["TAK"]+b["NIE"] > 0 {
				cached["Z"+r] = float64(b["TAK"]) / float64(b["TAK"]+b["NIE"])
			}
		}
	}
	for r, cells := range rows {
		for col, cell := range cells {
			ref := col + strconv.Itoa(r)
			value, ok := cached[ref]
			if !ok {
				continue
			}
			var parsed struct {
				Formula string `xml:"f"`
			}
			if err := xml.Unmarshal([]byte(cell), &parsed); err != nil {
				return "", err
			}
			formula := parsed.Formula
			for _, c := range []string{"C", "G", "K", "M", "T"} {
				formula = strings.ReplaceAll(formula, c+"13:"+c+"13", c+"13:"+c+strconv.Itoa(last))
			}
			style := "1"
			if found := styleRE.FindStringSubmatch(cell); len(found) == 2 {
				style = found[1]
			}
			cells[col] = journalCell(ref, style, value, formula)
		}
	}
	columns := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}
	for i, row := range data {
		r := 13 + i
		if rows[r] == nil {
			rows[r] = map[string]string{}
		}
		dir := "Powyżej"
		if row.Under {
			dir = "Poniżej"
		}
		mode := map[string]string{"all": "Wszyscy", "easier": "Łatwiejsi", "harder": "Trudniejsi"}[row.Mode]
		var actual, minutes any
		if row.Actual != nil {
			actual = *row.Actual
		}
		if row.Minutes != nil {
			minutes = *row.Minutes
		}
		values := []any{journalDate(row.Meta.StartTime), row.Meta.Match, row.Player, row.Label, dir, row.Line, string(row.Bookmaker), row.Odds, row.HitRate / 100, row.Sample, row.Verdict, actual, row.Outcome, minutes, journalDate(row.SavedAt), journalDate(row.SettledAt), mode, row.Source, row.Note, nil}
		for j, value := range values {
			col := columns[j]
			style := styles[col]
			if style == "" {
				style = "1"
			}
			formula := ""
			if col == "T" {
				n := strconv.Itoa(r)
				formula = `IF(M` + n + `="TAK",H` + n + `*$W$14*$W$15-$W$15,IF(M` + n + `="NIE",-$W$15,IF(M` + n + `="ZWROT",0,"")))`
				if p, ok := journalProfit(row); ok {
					value = p
				} else {
					value = ""
				}
			}
			rows[r][col] = journalCell(col+strconv.Itoa(r), style, value, formula)
		}
	}
	var body strings.Builder
	body.WriteString("<x:sheetData>")
	for r := 1; r <= max(last, 33); r++ {
		cells := rows[r]
		if len(cells) == 0 {
			continue
		}
		height := 24
		if r == 12 {
			height = 38
		} else if r >= 13 && r <= last && len(data) > 0 {
			height = 42
		}
		fmt.Fprintf(&body, `<x:row r="%d" ht="%d" customHeight="1">`, r, height)
		for _, col := range columns {
			body.WriteString(cells[col])
		}
		body.WriteString("</x:row>")
	}
	body.WriteString("</x:sheetData>")
	start, end := strings.Index(template, "<x:sheetData>"), strings.Index(template, "</x:sheetData>")
	if start < 0 || end < 0 {
		return "", fmt.Errorf("brak danych w osadzonym szablonie")
	}
	result := template[:start] + body.String() + template[end+len("</x:sheetData>"):]
	result = strings.ReplaceAll(result, `sqref="M13:M13"`, `sqref="M13:M`+strconv.Itoa(last)+`"`)
	return result, nil
}
func (a *App) exportJournal() error {
	a.journalExportMu.Lock()
	defer a.journalExportMu.Unlock()
	a.journalMu.Lock()
	if err := a.loadJournalLocked(); err != nil {
		a.journalMu.Unlock()
		return err
	}
	if err := a.saveJournalLocked(); err != nil {
		a.journalMu.Unlock()
		return err
	}
	data := make([][]journalRow, 2)
	for i, rows := range []map[string]journalRow{a.journal.Selected, a.journal.All} {
		for _, row := range rows {
			data[i] = append(data[i], row)
		}
		sort.Slice(data[i], func(j, k int) bool {
			a, b := data[i][j], data[i][k]
			if !a.Meta.StartTime.Equal(b.Meta.StartTime) {
				return a.Meta.StartTime.After(b.Meta.StartTime)
			}
			if a.Meta.Match != b.Meta.Match {
				return a.Meta.Match < b.Meta.Match
			}
			return a.Key < b.Key
		})
	}
	a.journalMu.Unlock()
	z, err := zip.NewReader(bytes.NewReader(journalTemplate), int64(len(journalTemplate)))
	if err != nil {
		return err
	}
	var output bytes.Buffer
	writer := zip.NewWriter(&output)
	for _, file := range z.File {
		r, err := file.Open()
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(r)
		closeErr := r.Close()
		if err = errors.Join(err, closeErr); err != nil {
			return err
		}
		for i := 0; i < 2; i++ {
			if file.Name == fmt.Sprintf("xl/worksheets/sheet%d.xml", i+1) {
				content, err := journalSheet(string(raw), data[i])
				if err != nil {
					return err
				}
				raw = []byte(content)
			}
			if file.Name == fmt.Sprintf("xl/tables/table%d.xml", i+1) {
				raw = []byte(strings.ReplaceAll(string(raw), "A12:T13", "A12:T"+strconv.Itoa(max(13, len(data[i])+12))))
				if !bytes.Contains(raw, []byte("<x:autoFilter")) {
					raw = bytes.Replace(raw, []byte("<x:tableColumns"), []byte(`<x:autoFilter ref="A12:T`+strconv.Itoa(max(13, len(data[i])+12))+`" /><x:tableColumns`), 1)
				}
			}
		}
		if file.Name == "xl/workbook.xml" {
			raw = []byte(strings.ReplaceAll(string(raw), "</x:workbook>", `<x:calcPr calcMode="auto" fullCalcOnLoad="1" forceFullCalc="1" /></x:workbook>`))
		}
		w, err := writer.Create(file.Name)
		if err != nil {
			return err
		}
		if _, err = w.Write(raw); err != nil {
			return err
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}
	dir, err := journalDir()
	if err != nil {
		return err
	}
	if err = journalAtomic(filepath.Join(dir, "value_bets.xlsx"), output.Bytes()); err != nil {
		return fmt.Errorf("Excel: nie można odświeżyć skoroszytu. Zamknij plik w Excelu i kliknij Odśwież. Dane zapisano w lokalnym JSON: %w", err)
	}
	a.journalMu.Lock()
	if strings.HasPrefix(a.journalError, "Excel:") {
		a.journalError = ""
	}
	a.journalMu.Unlock()
	return nil
}

func main() {
	app := NewApp()

	err := wails.Run(&options.App{
		Title:  "ValueBets 3.0",
		Width:  1920,
		Height: 1080,
		AssetServer: &assetserver.Options{
			Assets: assets,
		},
		OnStartup:  app.startup,
		OnShutdown: app.shutdown,
		Bind: []interface{}{
			app,
		},
	})

	if err != nil {
		println("Error:", err.Error())
	}
}

// API response types and local personal data.
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
	MarketPasses                  CanonicalMarket = "PASSES"
	MarketAccuratePasses          CanonicalMarket = "ACCURATE_PASSES"
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

type FairModelDetails struct {
	Version         string   `json:"version"`
	Distribution    string   `json:"distribution"`
	Probability     float64  `json:"probability"`
	BaseMean        float64  `json:"baseMean"`
	ExpectedCount   float64  `json:"expectedCount"`
	MatchupFactor   float64  `json:"matchupFactor"`
	EffectiveSample float64  `json:"effectiveSample"`
	ContextMatches  int      `json:"contextMatches"`
	Role            string   `json:"role"`
	Venue           string   `json:"venue"`
	TeamID          string   `json:"teamId"`
	OpponentID      string   `json:"opponentId"`
	Features        []string `json:"features"`
	Notes           []string `json:"notes"`
}

type OpponentFilterVariant struct {
	Indexes   []int             `json:"indexes"`
	HitRate   float64           `json:"hitRate"`
	FairModel *FairModelDetails `json:"fairModel,omitempty"`
}

type OpponentFilterData struct {
	TargetSample   int                    `json:"targetSample,omitempty"`
	ScannedMatches int                    `json:"scannedMatches,omitempty"`
	SampleNote     string                 `json:"sampleNote,omitempty"`
	Rule           string                 `json:"rule"`
	Basis          string                 `json:"basis"`
	OwnTeam        string                 `json:"ownTeam,omitempty"`
	Opponent       string                 `json:"opponent,omitempty"`
	OwnPPG         *float64               `json:"ownPPG,omitempty"`
	OpponentPPG    *float64               `json:"opponentPPG,omitempty"`
	KnownMatches   int                    `json:"knownMatches"`
	UnknownMatches int                    `json:"unknownMatches"`
	EqualMatches   int                    `json:"equalMatches"`
	Reason         string                 `json:"reason,omitempty"`
	Easier         *OpponentFilterVariant `json:"easier,omitempty"`
	Harder         *OpponentFilterVariant `json:"harder,omitempty"`
}

type TableSummary struct {
	Position int `json:"position"`
	Matches  int `json:"matches"`
	Points   int `json:"points"`
}

type TeamOverview struct {
	Flow         *TeamFlowOverview  `json:"flow,omitempty"`
	Name         string             `json:"name"`
	Venue        string             `json:"venue"`
	Table        *TableSummary      `json:"table,omitempty"`
	VenueTable   *TableSummary      `json:"venueTable,omitempty"`
	Form         []string           `json:"form"`
	VenueForm    []string           `json:"venueForm"`
	StatsMatches int                `json:"statsMatches"`
	Metrics      map[string]float64 `json:"metrics"`
}

type MatchOverview struct {
	AsOf     time.Time                `json:"asOf"`
	LeagueID int                      `json:"leagueId"`
	SeasonID int                      `json:"seasonId"`
	Teams    map[string]*TeamOverview `json:"teams"`
	Notes    []string                 `json:"notes"`
}

type FlowWindow struct {
	Points   int     `json:"points"`
	Mean     float64 `json:"mean"`
	Positive int     `json:"positive"`
	Negative int     `json:"negative"`
}

type FlowStateSummary struct {
	State   string     `json:"state"`
	Matches int        `json:"matches"`
	Window  FlowWindow `json:"window"`
}

type FlowReactionSummary struct {
	Kind    string  `json:"kind"`
	Samples int     `json:"samples"`
	Matches int     `json:"matches"`
	Before  float64 `json:"before"`
	After   float64 `json:"after"`
	Delta   float64 `json:"delta"`
}

type FlowMatchReference struct {
	EventID   string    `json:"eventId"`
	Revision  string    `json:"revision"`
	StartedAt time.Time `json:"startedAt"`
	Opponent  string    `json:"opponent"`
	Venue     string    `json:"venue"`
	HomeName  string    `json:"homeName"`
	AwayName  string    `json:"awayName"`
	HomeScore *int      `json:"homeScore"`
	AwayScore *int      `json:"awayScore"`
	Available bool      `json:"available"`
	Reason    string    `json:"reason,omitempty"`
}

type TeamFlowOverview struct {
	Requested int                   `json:"requested"`
	Available int                   `json:"available"`
	Matches   []FlowMatchReference  `json:"matches"`
	States    []FlowStateSummary    `json:"states"`
	Reactions []FlowReactionSummary `json:"reactions"`
	Notes     []string              `json:"notes"`
}

type FlowGraphPoint struct {
	Minute float64 `json:"minute"`
	Value  float64 `json:"value"`
	State  string  `json:"state,omitempty"`
}

type FlowReaction struct {
	Kind        string     `json:"kind"`
	Minute      float64    `json:"minute"`
	Clock       string     `json:"clock"`
	IsHome      *bool      `json:"isHome"`
	Description string     `json:"description"`
	HomeScore   *int       `json:"homeScore"`
	AwayScore   *int       `json:"awayScore"`
	StateBefore string     `json:"stateBefore"`
	StateAfter  string     `json:"stateAfter"`
	Before      FlowWindow `json:"before"`
	After       FlowWindow `json:"after"`
	Eligible    bool       `json:"eligible"`
}

type HistoricalMatchFlow struct {
	Version       int              `json:"version"`
	EventID       string           `json:"eventId"`
	Revision      string           `json:"revision"`
	FetchedAt     time.Time        `json:"fetchedAt"`
	HomeID        int              `json:"homeId"`
	AwayID        int              `json:"awayId"`
	HomeName      string           `json:"homeName"`
	AwayName      string           `json:"awayName"`
	HomeScore     *int             `json:"homeScore"`
	AwayScore     *int             `json:"awayScore"`
	TimelineValid bool             `json:"timelineValid"`
	GraphPoints   []FlowGraphPoint `json:"graphPoints"`
	Reactions     []FlowReaction   `json:"reactions"`
	Notes         []string         `json:"notes"`
}

type AnalyzedBet struct {
	SampleEventIDs       []int               `json:"sampleEventIds,omitempty"`
	HistoryNotes         []string            `json:"historyNotes,omitempty"`
	AnalysisID           string              `json:"analysisId,omitempty"`
	OpponentFilter       *OpponentFilterData `json:"opponentFilter,omitempty"`
	AnalysisOdds         float64             `json:"analysisOdds,omitempty"`
	QuoteObservedAt      time.Time           `json:"quoteObservedAt,omitempty"`
	PriceImproved        bool                `json:"priceImproved,omitempty"`
	PlayerID             int                 `json:"playerId"`
	ContextPending       bool                `json:"contextPending,omitempty"`
	SampleRatings        []*float64          `json:"sampleRatings"`
	FairModel            *FairModelDetails   `json:"fairModel,omitempty"`
	MatchOverview        *MatchOverview      `json:"matchOverview,omitempty"`
	Bookmaker            Bookmaker           `json:"bookmaker"`
	PlayerName           string              `json:"playerName"`
	MarketType           CanonicalMarket     `json:"marketType"`
	MarketLabel          string              `json:"marketLabel"`
	Line                 float64             `json:"line"`
	IsUnder              bool                `json:"isUnder"`
	BookmakerOdds        float64             `json:"bookmakerOdds"`
	FairOdds             float64             `json:"fairOdds"`
	HitRate              float64             `json:"hitRate"`
	EdgePercent          float64             `json:"edgePercent"`
	EVWithTax            float64             `json:"evWithTax"`
	MatchesSample        int                 `json:"matchesSample"`
	SampleValues         []int               `json:"sampleValues"`
	SampleValuesSuperSub []int               `json:"sampleValuesSuperSub"`
	SampleMinutes        []int               `json:"sampleMinutes"`
	SamplePositions      []string            `json:"samplePositions"`
	SampleOpponents      []string            `json:"sampleOpponents"`
	SampleDates          []string            `json:"sampleDates"`
	SampleShots          [][]ShotCoordinate  `json:"sampleShots"`
	StatsSource          string              `json:"statsSource"`
	TeamSide             string              `json:"teamSide"`
	TeamID               string              `json:"teamId"`
	TeamName             string              `json:"teamName"`
	Verdict              string              `json:"verdict"`
	LineupStatus         string              `json:"lineupStatus"`
	IsStartingXI         bool                `json:"isStartingXI"`
}

type TeamDetectionResult struct {
	HomeSlug string   `json:"homeSlug"`
	AwaySlug string   `json:"awaySlug"`
	HomeID   string   `json:"homeId"`
	AwayID   string   `json:"awayId"`
	Warnings []string `json:"warnings"`
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
	AnalysisID           string             `json:"analysisId,omitempty"`
	PlayerID             int                `json:"playerId,omitempty"`
	OpponentFilterMode   string             `json:"opponentFilterMode,omitempty"`
	SampleEventIDs       []int              `json:"sampleEventIds,omitempty"`
	HistoryNotes         []string           `json:"historyNotes,omitempty"`
	SampleRatings        []*float64         `json:"sampleRatings"`
	FairModel            *FairModelDetails  `json:"fairModel,omitempty"`
	MatchOverview        *MatchOverview     `json:"matchOverview,omitempty"`
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

type App struct {
	journalMu           sync.Mutex
	journalExportMu     sync.Mutex
	journal             *betJournal
	journalError        string
	journalBusy         bool
	journalCancel       context.CancelFunc
	journalWake         chan struct{}
	ctx                 context.Context
	analysisMu          sync.Mutex
	analysisActive      bool
	analysisCancel      context.CancelFunc
	analysisProgress    AnalysisJob
	detectionMu         sync.Mutex
	detectionCancel     context.CancelFunc
	detectionRevision   uint64
	preparationMu       sync.Mutex
	preparationCancel   context.CancelFunc
	preparationRevision uint64
}

var cacheMutex sync.Mutex

func NewApp() *App { return &App{} }
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	journalCtx, cancel := context.WithCancel(ctx)
	a.journalCancel = cancel
	a.journalWake = make(chan struct{}, 1)
	go a.journalLoop(journalCtx)
}
func (a *App) shutdown(context.Context) {
	if a.journalCancel != nil {
		a.journalCancel()
	}
	a.detectionMu.Lock()
	defer a.detectionMu.Unlock()
	if a.detectionCancel != nil {
		a.detectionCancel()
	}
	a.preparationMu.Lock()
	defer a.preparationMu.Unlock()
	if a.preparationCancel != nil {
		a.preparationCancel()
	}
}

func getCacheFilePath(filename string) string {
	var candidates []string
	if exePath, err := os.Executable(); err == nil {
		dir := filepath.Dir(exePath)
		candidates = append(candidates, filepath.Join(dir, "data", filename), filepath.Join(dir, filename))
	}
	candidates = append(candidates, filepath.Join("data", filename), filename)
	// Keep using existing personal data from the local version without moving it.
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	path := filepath.Join("data", filename)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		log.Printf("[CACHE] Nie można utworzyć katalogu danych: %v", err)
	}
	return path
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

type SuperbetCalendarEvent struct {
	ID                  string                   `json:"id"`
	Home                string                   `json:"home"`
	Away                string                   `json:"away"`
	Competition         string                   `json:"competition"`
	Country             string                   `json:"country"`
	StartTime           string                   `json:"startTime"`
	URL                 string                   `json:"url"`
	Live                bool                     `json:"live"`
	BetBuilder          bool                     `json:"betBuilder"`
	MarketAvailability  *MatchMarketAvailability `json:"marketAvailability,omitempty"`
	AnalysisEligibility *AnalysisEligibility     `json:"analysisEligibility,omitempty"`
}

type AnalysisEligibility struct {
	Status    string            `json:"status"`
	Reason    string            `json:"reason,omitempty"`
	Links     map[string]string `json:"links"`
	ExpiresAt time.Time         `json:"expiresAt,omitempty"`
}

type SuperbetCalendar struct {
	Date      string                  `json:"date"`
	Events    []SuperbetCalendarEvent `json:"events"`
	Warnings  []string                `json:"warnings"`
	FetchedAt string                  `json:"fetchedAt,omitempty"`
}

type MatchMarketAvailability struct {
	Bookmaker string            `json:"bookmaker"`
	EventID   string            `json:"eventId"`
	Status    string            `json:"status"`
	Markets   []CanonicalMarket `json:"markets"`
	CheckedAt time.Time         `json:"checkedAt"`
	ExpiresAt time.Time         `json:"expiresAt"`
	Error     string            `json:"error,omitempty"`
}

func (a *App) CheckMatchMarkets(bookmaker, eventID, home, away string) (MatchMarketAvailability, error) {
	ctx, cancel := context.WithTimeout(a.calendarContext(), 65*time.Second)
	defer cancel()
	body, err := json.Marshal(map[string]string{"bookmaker": bookmaker, "eventId": eventID, "home": home, "away": away})
	if err != nil {
		return MatchMarketAvailability{}, err
	}
	var result MatchMarketAvailability
	err = a.serverRequest(ctx, http.MethodPost, "/api/v1/markets/check", body, &result)
	return result, err
}

type MatchPreparation struct {
	Qualified bool                               `json:"qualified"`
	Sources   map[string]MatchMarketAvailability `json:"sources"`
}

func (a *App) PrepareCalendarMatch(superbet, betclic, sts string) (MatchPreparation, error) {
	ctx, cancel := context.WithTimeout(a.calendarContext(), 100*time.Second)
	a.preparationMu.Lock()
	if a.preparationCancel != nil {
		a.preparationCancel()
	}
	a.preparationRevision++
	revision := a.preparationRevision
	a.preparationCancel = cancel
	a.preparationMu.Unlock()
	defer func() {
		cancel()
		a.preparationMu.Lock()
		if a.preparationRevision == revision {
			a.preparationCancel = nil
		}
		a.preparationMu.Unlock()
	}()
	body, err := json.Marshal(map[string]string{"superbetUrl": superbet, "betclicUrl": betclic, "stsUrl": sts})
	if err != nil {
		return MatchPreparation{}, err
	}
	var result MatchPreparation
	err = a.serverRequest(ctx, http.MethodPost, "/api/v1/markets/prepare", body, &result)
	return result, err
}

func (a *App) GetSuperbetCalendar(date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), "SUPERBET", date, false)
}

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

func (a *App) GetBetclicCalendar(date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), "BETCLIC", date, false)
}

func (a *App) RefreshBookmakerCalendar(bookmaker, date string) (*SuperbetCalendar, error) {
	return a.getBookmakerCalendar(a.calendarContext(), strings.ToUpper(strings.TrimSpace(bookmaker)), date, true)
}

func (a *App) getBookmakerCalendar(ctx context.Context, bookmaker, date string, force bool) (*SuperbetCalendar, error) {
	if _, err := time.Parse("2006-01-02", date); err != nil {
		return nil, err
	}
	if bookmaker != "STS" && bookmaker != "BETCLIC" && bookmaker != "SUPERBET" {
		return nil, fmt.Errorf("nieobsługiwany bukmacher kalendarza: %s", bookmaker)
	}
	var calendar SuperbetCalendar
	query := url.Values{"bookmaker": {bookmaker}, "date": {date}}
	method, path := http.MethodGet, "/api/v1/calendars?"+query.Encode()
	var body []byte
	if force {
		method, path = http.MethodPost, "/api/v1/calendars/refresh"
		body, _ = json.Marshal(map[string]string{"bookmaker": bookmaker, "date": date})
	}
	if err := a.serverRequest(ctx, method, path, body, &calendar); err != nil {
		return nil, err
	}
	return &calendar, nil
}

// Cloudflare Tunnel forwards this public HTTPS endpoint to the home Debian server.
const serverBaseURL = "https://api.value-bets.pl"

type AutomaticSummary struct {
	QuoteCheckedAt time.Time `json:"quoteCheckedAt,omitempty"`
	Improved       int       `json:"improved,omitempty"`
	NewSelections  int       `json:"newSelections,omitempty"`
	Missing        int       `json:"missing,omitempty"`
	Bookmaker      string    `json:"bookmaker"`
	EventID        string    `json:"eventId"`
	FixtureID      string    `json:"fixtureId"`
	Match          string    `json:"match"`
	Status         string    `json:"status"`
	Stage          string    `json:"stage"`
	Value          int       `json:"value"`
	Traps          int       `json:"traps"`
	Total          int       `json:"total"`
	AnalyzedAt     time.Time `json:"analyzedAt"`
	ExpiresAt      time.Time `json:"expiresAt"`
	RunID          string    `json:"runId"`
	Error          string    `json:"error,omitempty"`
}

func (a *App) GetAutomaticSummaries() ([]AutomaticSummary, error) {
	ctx, cancel := context.WithTimeout(a.calendarContext(), 15*time.Second)
	defer cancel()
	var result []AutomaticSummary
	err := a.serverRequest(ctx, http.MethodGet, "/api/v1/research/summaries", nil, &result)
	return result, err
}

var calendarAPIClient = &http.Client{
	Timeout:       180 * time.Second,
	CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
}

func (a *App) serverRequest(ctx context.Context, method, path string, body []byte, target any) error {
	req, err := http.NewRequestWithContext(ctx, method, serverBaseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := calendarAPIClient.Do(req)
	if err != nil {
		return fmt.Errorf("połączenie z API: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (32<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 32<<20 {
		return fmt.Errorf("odpowiedź API przekracza 32 MB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var failure struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(raw, &failure)
		if failure.Error == "" {
			failure.Error = resp.Status
		}
		return fmt.Errorf("API HTTP %d: %s", resp.StatusCode, failure.Error)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("niepoprawna odpowiedź API: %w", err)
	}
	return nil
}

type AnalysisRequest struct {
	SuperbetURL   string `json:"superbetUrl"`
	BetclicURL    string `json:"betclicUrl"`
	STSURL        string `json:"stsUrl"`
	HomeID        string `json:"homeId"`
	AwayID        string `json:"awayId"`
	MatchLimit    int    `json:"matchLimit"`
	TournamentIDs string `json:"tournamentIds"`
}

type AnalysisJob struct {
	FixtureID      string          `json:"fixtureId,omitempty"`
	StartTime      time.Time       `json:"startTime,omitempty"`
	ResultRevision int             `json:"resultRevision"`
	ID             string          `json:"id"`
	Match          string          `json:"match"`
	Status         string          `json:"status"`
	Stage          string          `json:"stage"`
	Message        string          `json:"message"`
	PlayersDone    int             `json:"playersDone"`
	PlayersTotal   int             `json:"playersTotal"`
	CreatedAt      time.Time       `json:"createdAt"`
	StartedAt      time.Time       `json:"startedAt,omitempty"`
	FinishedAt     time.Time       `json:"finishedAt,omitempty"`
	Request        AnalysisRequest `json:"request"`
	Warnings       []string        `json:"warnings"`
	Result         []AnalyzedBet   `json:"result,omitempty"`
	Error          string          `json:"error,omitempty"`
}

func (a *App) SaveTeamSofaID(slug, sofaID string) bool {
	body, _ := json.Marshal(map[string]string{"slug": slug, "sofaId": sofaID})
	var ok bool
	if err := a.serverRequest(a.calendarContext(), http.MethodPost, "/api/v1/teams/mapping", body, &ok); err != nil {
		log.Printf("[API] Zapis ID drużyny: %v", err)
		return false
	}
	return ok
}

func (a *App) DetectTeamsFromURL(rawURL string) (TeamDetectionResult, error) {
	return a.detectTeams(map[string]string{"url": rawURL})
}
func (a *App) DetectCalendarTeams(home, away string) (TeamDetectionResult, error) {
	return a.detectTeams(map[string]string{"home": home, "away": away})
}
func (a *App) DetectCalendarEventTeams(home, away, matchURL string) (TeamDetectionResult, error) {
	return a.detectTeams(map[string]string{"home": home, "away": away, "url": matchURL})
}
func (a *App) detectTeams(query map[string]string) (TeamDetectionResult, error) {
	ctx, cancel := context.WithTimeout(a.calendarContext(), 100*time.Second)
	a.detectionMu.Lock()
	if a.detectionCancel != nil {
		a.detectionCancel()
	}
	a.detectionRevision++
	revision := a.detectionRevision
	a.detectionCancel = cancel
	a.detectionMu.Unlock()
	defer func() {
		cancel()
		a.detectionMu.Lock()
		if a.detectionRevision == revision {
			a.detectionCancel = nil
		}
		a.detectionMu.Unlock()
	}()
	body, _ := json.Marshal(query)
	var result TeamDetectionResult
	if err := a.serverRequest(ctx, http.MethodPost, "/api/v1/teams/detect", body, &result); err != nil {
		log.Printf("[API] Rozpoznawanie drużyn: %v", err)
		return result, fmt.Errorf("detekcja SofaScore na serwerze: %w", err)
	}
	return result, nil
}
func (a *App) GetMatchFlow(eventID, revision string) (*HistoricalMatchFlow, error) {
	ctx, cancel := context.WithTimeout(a.calendarContext(), 15*time.Second)
	defer cancel()
	query := url.Values{"eventId": {eventID}, "revision": {revision}}
	var flow HistoricalMatchFlow
	if err := a.serverRequest(ctx, http.MethodGet, "/api/v1/match-flow?"+query.Encode(), nil, &flow); err != nil {
		return nil, err
	}
	return &flow, nil
}

func (a *App) GetAvailableTournaments(homeID, awayID string) ([]LeagueOption, error) {
	var leagues []LeagueOption
	query := url.Values{"homeId": {homeID}, "awayId": {awayID}}
	err := a.serverRequest(a.calendarContext(), http.MethodGet, "/api/v1/tournaments?"+query.Encode(), nil, &leagues)
	return leagues, err
}

func (a *App) setAnalysisProgress(job AnalysisJob) {
	a.analysisMu.Lock()
	defer a.analysisMu.Unlock()
	job.Result = nil
	a.analysisProgress = job
}
func (a *App) GetAnalysisProgress() AnalysisJob {
	a.analysisMu.Lock()
	defer a.analysisMu.Unlock()
	job := a.analysisProgress
	job.Warnings = append([]string{}, job.Warnings...)
	return job
}

// Wails waits locally while the server performs an independent asynchronous job.
// Each poll has a short timeout; no long-lived HTTP analysis request is needed.
func (a *App) CancelAnalysis() error {
	a.analysisMu.Lock()
	id, cancel := a.analysisProgress.ID, a.analysisCancel
	a.analysisMu.Unlock()
	if id == "" || cancel == nil {
		return fmt.Errorf("brak aktywnego zlecenia do anulowania")
	}
	ctx, stop := context.WithTimeout(a.calendarContext(), 10*time.Second)
	defer stop()
	var accepted bool
	if err := a.serverRequest(ctx, http.MethodPost, "/api/v1/analyses/"+url.PathEscape(id)+"/cancel", nil, &accepted); err != nil {
		return err
	}
	if !accepted {
		return fmt.Errorf("serwer nie potwierdził anulowania")
	}
	cancel()
	return nil
}

func (a *App) AnalyzeThreeBookmakers(sb, betclic, sts, home, away string, limit int, tournaments string) (result []AnalyzedBet, err error) {
	a.analysisMu.Lock()
	if a.analysisActive {
		a.analysisMu.Unlock()
		return nil, fmt.Errorf("analiza jest już uruchomiona")
	}
	a.analysisActive = true
	a.analysisProgress = AnalysisJob{Status: "queued", Stage: "sending", Message: "Wysyłanie zlecenia na serwer", Warnings: []string{}}
	a.analysisMu.Unlock()
	defer func() {
		a.analysisMu.Lock()
		defer a.analysisMu.Unlock()
		a.analysisActive = false
		a.analysisCancel = nil
		if err != nil {
			a.analysisProgress.Status = "failed"
			a.analysisProgress.Stage = "failed"
			a.analysisProgress.Error = err.Error()
			a.analysisProgress.Message = err.Error()
			if errors.Is(err, context.Canceled) {
				a.analysisProgress.Message = "Analiza anulowana"
			}
		}
	}()
	ctx, cancel := context.WithTimeout(a.calendarContext(), 23*time.Minute)
	defer cancel()
	a.analysisMu.Lock()
	a.analysisCancel = cancel
	a.analysisMu.Unlock()
	body, _ := json.Marshal(AnalysisRequest{SuperbetURL: sb, BetclicURL: betclic, STSURL: sts, HomeID: home, AwayID: away, MatchLimit: limit, TournamentIDs: tournaments})
	var job AnalysisJob
	sendCtx, sendCancel := context.WithTimeout(ctx, 20*time.Second)
	err = a.serverRequest(sendCtx, http.MethodPost, "/api/v1/analyses", body, &job)
	sendCancel()
	if err != nil {
		return nil, err
	}
	if job.ID == "" {
		return nil, fmt.Errorf("Serwer nie zwrócił ID zlecenia")
	}
	a.setAnalysisProgress(job)
	id := job.ID
	lastRevision := 0
	failures := 0
	for {
		timer := time.NewTimer(1500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("oczekiwanie na analizę %s: %w", id, ctx.Err())
		case <-timer.C:
		}
		pollCtx, pollCancel := context.WithTimeout(ctx, 20*time.Second)
		var nextJob AnalysisJob
		err = a.serverRequest(pollCtx, http.MethodGet, "/api/v1/analyses/"+url.PathEscape(id)+"?afterRevision="+fmt.Sprint(lastRevision), nil, &nextJob)
		pollCancel()
		if err != nil {
			failures++
			state := a.GetAnalysisProgress()
			state.Message = "Oczekiwanie na połączenie z serwerem — zlecenie " + id
			a.setAnalysisProgress(state)
			if failures >= 4 {
				return nil, fmt.Errorf("nie udało się odczytać analizy %s: %w", id, err)
			}
			continue
		}
		failures = 0
		job = nextJob
		a.setAnalysisProgress(job)
		if job.ResultRevision > lastRevision {
			lastRevision = job.ResultRevision
			if (job.Status == "running" || job.Status == "failed") && len(job.Result) > 0 && a.ctx != nil {
				wailsruntime.EventsEmit(a.ctx, "analysis:players-ready", job)
			}
		}
		switch job.Status {
		case "completed":
			a.recordJournal(job.Result, false, "all", journalMeta{AnalysisID: job.ID, FixtureID: job.FixtureID, Match: job.Match, StartTime: job.StartTime, AnalyzedAt: job.FinishedAt})
			if job.Result == nil {
				return []AnalyzedBet{}, nil
			}
			return job.Result, nil
		case "failed":
			return nil, fmt.Errorf("%s", job.Error)
		}
	}
}

// Coordinates are fetched on chart opening without changing the saved sample.
func (a *App) GetAnalysisShotmaps(analysisID string, playerID int, market, mode string) (map[string]any, error) {
	if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(analysisID) || playerID <= 0 {
		return nil, fmt.Errorf("niepoprawna analiza lub zawodnik")
	}
	body, err := json.Marshal(map[string]any{"analysisId": analysisID, "playerId": playerID, "market": market, "mode": mode})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 100*time.Second)
	defer cancel()
	var result map[string]any
	err = a.serverRequest(ctx, http.MethodPost, "/api/v1/research/shotmaps", body, &result)
	return result, err
}

// A derived filtered sample reuses the baseline fixture, odds and context.
// Only missing historical inputs are downloaded by the server job.
func (a *App) GetOpponentAnalysis(analysisID, mode string) (map[string]any, error) {
	if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(analysisID) || (mode != "easier" && mode != "harder") {
		return nil, fmt.Errorf("niepoprawne ID lub filtr historii")
	}
	ctx, cancel := context.WithTimeout(a.calendarContext(), 23*time.Minute)
	defer cancel()
	path := "/api/v1/research/matchup/" + url.PathEscape(analysisID) + "?mode=" + url.QueryEscape(mode)
	method := http.MethodPost
	failures := 0
	for {
		requestCtx, requestCancel := context.WithTimeout(ctx, 25*time.Second)
		var job map[string]any
		err := a.serverRequest(requestCtx, method, path, nil, &job)
		requestCancel()
		if err != nil {
			failures++
			if method == http.MethodPost || failures >= 4 {
				return nil, fmt.Errorf("uzupełnianie próbki: %w", err)
			}
		} else {
			failures = 0
			status, _ := job["status"].(string)
			if status == "completed" {
				return job, nil
			}
			if status == "error" {
				return nil, fmt.Errorf("uzupełnianie próbki: %v", job["error"])
			}
			if a.ctx != nil {
				wailsruntime.EventsEmit(a.ctx, "analysis:opponent-progress", map[string]any{"analysisId": analysisID, "mode": mode, "done": job["done"], "total": job["total"], "message": job["message"]})
			}
			method = http.MethodGet
		}
		timer := time.NewTimer(1500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("uzupełnianie próbki: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func (a *App) GetAutomaticAnalysis(runID string) ([]AnalyzedBet, error) {
	ctx, cancel := context.WithTimeout(a.calendarContext(), 30*time.Second)
	defer cancel()
	var result []AnalyzedBet
	err := a.serverRequest(ctx, http.MethodGet, "/api/v1/research/runs/"+url.PathEscape(runID), nil, &result)
	if err == nil {
		a.recordJournal(result, false, "all", journalMeta{})
	}
	return result, err
}

// Inspect the already fetched offers; this never starts an analysis or fetch.
func (a *App) GetOfferDiagnostics(superbet, betclic, sts string) ([]map[string]any, error) {
	ctx, cancel := context.WithTimeout(a.calendarContext(), 15*time.Second)
	defer cancel()
	result := []map[string]any{}
	for _, source := range []struct{ book, input string }{{"SUPERBET", superbet}, {"BETCLIC", betclic}, {"STS", sts}} {
		if strings.TrimSpace(source.input) == "" {
			continue
		}
		var item map[string]any
		query := url.Values{"bookmaker": {source.book}, "matchUrl": {source.input}}
		if err := a.serverRequest(ctx, http.MethodGet, "/api/v1/markets/audit?"+query.Encode(), nil, &item); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, nil
}

func (a *App) GetAutomaticLeagues() (map[string]any, error) {
	ctx, cancel := context.WithTimeout(a.calendarContext(), 15*time.Second)
	defer cancel()
	var result map[string]any
	err := a.serverRequest(ctx, http.MethodGet, "/api/v1/automatic/leagues", nil, &result)
	return result, err
}

func (a *App) SetAutomaticLeague(key string, enabled bool) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(a.calendarContext(), 15*time.Second)
	defer cancel()
	body, err := json.Marshal(map[string]any{"key": key, "enabled": enabled})
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = a.serverRequest(ctx, http.MethodPatch, "/api/v1/automatic/leagues", body, &result)
	return result, err
}
