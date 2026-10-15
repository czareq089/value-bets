// Value Bets server: calendars, match analysis, SofaScore statistics and archives.
// All server source lives in this file. No Wails or desktop imports.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"database/sql"

	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"

	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	_ "time/tzdata"
	"unicode"
	"unicode/utf8"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"github.com/gorilla/websocket"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	_ "modernc.org/sqlite"
)

// === Persistent predictions, automatic evaluation and read-only review API ===

const researchSchema = 1
const evaluationStrategy = "prematch-T60-single-v1"

var errNoPlayerHistory = errors.New("SofaScore nie zwrócił historii zawodników; sprawdź ostrzeżenia")
var errSofaTeamVerification = errors.New("nie potwierdzono drużyn w SofaScore")

const teamDetectionRevision = "fixture-identity-v5-club-number"
const tacklesStatsField = "wonTackle"
const settlementMetricsVersion = "stats-v2-tackles-won"

type researchStore struct {
	db        *sql.DB
	summaryDB *sql.DB // Dedicated read-only connections; writers and exports cannot occupy them.
	dir       string
	mu        sync.Mutex // serialize report exports, never shared with the review service
}

type predictionEvidence struct {
	BookmakerStartTime time.Time            `json:"bookmakerStartTime,omitempty"`
	LineupDetails      *SofaLineupResponse  `json:"lineupDetails,omitempty"`
	SchemaVersion      int                  `json:"schemaVersion"`
	Fixture            statsFixture         `json:"fixture"`
	Request            AnalysisRequest      `json:"request"`
	HomePlayers        []*PlayerStatHistory `json:"homePlayers"`
	AwayPlayers        []*PlayerStatHistory `json:"awayPlayers"`
	HomeLineup         map[string]bool      `json:"homeLineup"`
	AwayLineup         map[string]bool      `json:"awayLineup"`
	LineupsConfirmed   bool                 `json:"lineupsConfirmed"`
	Context            analysisMatchContext `json:"matchContext"`
	OddsSources        map[string]any       `json:"oddsSources"`
}

type predictionRecord struct {
	SchemaVersion   int         `json:"schemaVersion"`
	ID              string      `json:"id"`
	RunID           string      `json:"runId"`
	BetKey          string      `json:"betKey"`
	Algorithm       string      `json:"algorithm"`
	Origin          string      `json:"origin"`
	Match           string      `json:"match"`
	FixtureID       string      `json:"fixtureId"`
	EventID         string      `json:"eventId"`
	StartAt         time.Time   `json:"startAt"`
	AnalyzedAt      time.Time   `json:"analyzedAt"`
	ObservedAt      time.Time   `json:"observedAt"`
	ExpiresAt       time.Time   `json:"expiresAt"`
	HistoryComplete bool        `json:"historyComplete"`
	EvidenceID      string      `json:"evidenceId"`
	EvidencePath    string      `json:"-"`
	Bet             AnalyzedBet `json:"bet"`
}

type automaticSummary struct {
	manual         bool      // Internal provenance for suppressing superseded background progress.
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

func openResearchStore(dir string) (*researchStore, error) {
	db, err := sql.Open("sqlite", filepath.Join(dir, "predictions.sqlite"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	statements := []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`, `PRAGMA foreign_keys=ON`,
		`CREATE TABLE IF NOT EXISTS research_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS prediction_runs (id TEXT PRIMARY KEY, analysis_id TEXT NOT NULL, origin TEXT NOT NULL, bookmaker TEXT NOT NULL, event_id TEXT NOT NULL, fixture_id TEXT NOT NULL, match_name TEXT NOT NULL, start_at INTEGER NOT NULL, analyzed_at INTEGER NOT NULL, observed_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, algorithm TEXT NOT NULL, value_count INTEGER NOT NULL, trap_count INTEGER NOT NULL, total INTEGER NOT NULL, evidence_path TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS prediction_runs_event ON prediction_runs(bookmaker,event_id,analyzed_at)`,
		`CREATE INDEX IF NOT EXISTS prediction_runs_fixture ON prediction_runs(fixture_id,origin,analyzed_at)`,
		`CREATE TABLE IF NOT EXISTS automatic_price_checks (run_id TEXT PRIMARY KEY REFERENCES prediction_runs(id), payload TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS automatic_analysis_identity (analysis_id TEXT PRIMARY KEY, bookmaker_start_at INTEGER NOT NULL)`,
		// A completed manual run becomes the live baseline. Its immutable
		// predecessors remain available to settlement and research reports.
		`CREATE VIEW IF NOT EXISTS current_prediction_runs AS SELECT r.* FROM prediction_runs r WHERE r.id=(SELECT r2.id FROM prediction_runs r2 WHERE r2.bookmaker=r.bookmaker AND r2.event_id=r.event_id AND r2.algorithm=r.algorithm AND (r2.origin='automatic' OR (r2.origin='manual' AND EXISTS(SELECT 1 FROM automatic_analysis_identity i WHERE i.analysis_id=r2.analysis_id))) ORDER BY r2.start_at DESC,CASE WHEN r2.origin='manual' THEN 1 ELSE 0 END DESC,r2.analyzed_at DESC,r2.observed_at DESC,r2.id DESC LIMIT 1)`,
		`CREATE TABLE IF NOT EXISTS predictions (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES prediction_runs(id), bet_key TEXT NOT NULL, player_id INTEGER NOT NULL, verdict TEXT NOT NULL, payload TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS predictions_run ON predictions(run_id)`,
		`CREATE TABLE IF NOT EXISTS fixture_results (fixture_id TEXT PRIMARY KEY, match_name TEXT NOT NULL, start_at INTEGER NOT NULL, next_check INTEGER NOT NULL, checked_at INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'scheduled', revision TEXT NOT NULL DEFAULT '', data_path TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS settlements (prediction_id TEXT NOT NULL REFERENCES predictions(id), rules TEXT NOT NULL, fixture_id TEXT NOT NULL, revision TEXT NOT NULL, status TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(prediction_id,rules))`,
		`CREATE INDEX IF NOT EXISTS settlements_fixture ON settlements(fixture_id)`,
		`CREATE TABLE IF NOT EXISTS settlement_versions (prediction_id TEXT NOT NULL, rules TEXT NOT NULL, revision TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(prediction_id,rules,revision))`,
	}
	for _, statement := range statements {
		if _, err = db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if _, err = db.Exec(`INSERT OR IGNORE INTO research_meta(key,value) VALUES('schema','1')`); err != nil {
		_ = db.Close()
		return nil, err
	}
	var schema string
	if err = db.QueryRow(`SELECT value FROM research_meta WHERE key='schema'`).Scan(&schema); err != nil || schema != "1" {
		_ = db.Close()
		return nil, fmt.Errorf("nieobsługiwany schemat bazy %q: %v", schema, err)
	}
	path, err := filepath.Abs(filepath.Join(dir, "predictions.sqlite"))
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	uriPath := filepath.ToSlash(path)
	if !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath // Windows drive path in a file URI.
	}
	uri := url.URL{Scheme: "file", Path: uriPath}
	uri.RawQuery = url.Values{"mode": {"ro"}, "_pragma": {"busy_timeout(1000)", "query_only(1)"}}.Encode()
	summaryDB, err := sql.Open("sqlite", uri.String())
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	summaryDB.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := summaryDB.PingContext(ctx); err != nil {
		_ = summaryDB.Close()
		_ = db.Close()
		return nil, fmt.Errorf("połączenie do odczytu podsumowań: %w", err)
	}
	log.Printf("[PREDICTIONS] stage=loaded database=%q schema=%d", filepath.Join(dir, "predictions.sqlite"), researchSchema)
	return &researchStore{db: db, summaryDB: summaryDB, dir: dir}, nil
}

func (store *researchStore) close() error {
	var readErr error
	if store.summaryDB != nil {
		readErr = store.summaryDB.Close()
	}
	return errors.Join(readErr, store.db.Close())
}

func researchHash(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:])
}

func (store *researchStore) saveDocument(kind, label string, value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	rel := filepath.Join("archive", "badanie-algorytmu", kind, archiveName(label), "wersja-"+researchHash(value), archiveName(label)+"_"+archiveName(kind)+".json.gz")
	path := filepath.Join(store.dir, rel)
	if _, err := os.Stat(path); err == nil {
		return rel, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return "", err
	}
	return rel, writeGzip(path, raw)
}

func (store *researchStore) readDocument(rel string, target any) error {
	path := filepath.Join(store.dir, rel)
	root, err := filepath.Abs(store.dir)
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(path)
	if err != nil || !strings.HasPrefix(abs, root+string(filepath.Separator)) {
		return fmt.Errorf("niepoprawna ścieżka archiwum")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer reader.Close()
	return json.NewDecoder(io.LimitReader(reader, 64<<20)).Decode(target)
}

func (s *calendarServer) savePredictions(job AnalysisJob, evidence *predictionEvidence, parents ...context.Context) error {
	if len(evidence.OddsSources) != len(serverBookmakers) {
		return fmt.Errorf("zapis prognoz wymaga pełnej oferty wszystkich trzech bukmacherów")
	}
	for _, book := range serverBookmakers {
		if evidence.OddsSources[book] == nil {
			return fmt.Errorf("brak oferty %s w prognozie", book)
		}
	}
	if evidence.Fixture.ID == "" || evidence.Fixture.StartTime.IsZero() || !job.FinishedAt.Before(evidence.Fixture.StartTime) {
		return fmt.Errorf("prognoza wymaga zweryfikowanego, nierozpoczętego wydarzenia SofaScore")
	}
	store := s.research
	// Observation times belong to predictions, not duplicated history documents.
	canonical := *evidence
	canonical.OddsSources = nil
	if evidence.Context.Overview != nil {
		overview := *evidence.Context.Overview
		overview.AsOf = time.Time{}
		canonical.Context.Overview = &overview
	}
	canonical.HomePlayers = canonicalHistories(evidence.HomePlayers)
	canonical.AwayPlayers = canonicalHistories(evidence.AwayPlayers)
	evidenceID := researchHash(canonical)
	path, err := store.saveDocument("dane-przedmeczowe", job.Match+"-mecz-"+evidence.Fixture.ID, canonical)
	if err != nil {
		return err
	}
	histories := map[int]*PlayerStatHistory{}
	for _, h := range append(append([]*PlayerStatHistory{}, evidence.HomePlayers...), evidence.AwayPlayers...) {
		histories[h.ID] = h
	}
	saveParent := s.ctx
	if len(parents) > 0 {
		saveParent = parents[0]
	}
	if saveParent == nil {
		saveParent = context.Background()
	}
	saveCtx, cancel := context.WithTimeout(saveParent, time.Minute)
	defer cancel()
	tx, err := store.db.BeginTx(saveCtx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(saveCtx, `INSERT INTO fixture_results(fixture_id,match_name,start_at,next_check) VALUES(?,?,?,?) ON CONFLICT(fixture_id) DO UPDATE SET start_at=excluded.start_at WHERE fixture_results.status IN ('scheduled','postponed')`, evidence.Fixture.ID, job.Match, evidence.Fixture.StartTime.Unix(), evidence.Fixture.StartTime.Add(110*time.Minute).Unix()); err != nil {
		return err
	}
	if evidence.BookmakerStartTime.IsZero() {
		return fmt.Errorf("brak terminu bukmachera dla analizy bazowej")
	}
	if _, err = tx.ExecContext(saveCtx, `INSERT OR IGNORE INTO automatic_analysis_identity(analysis_id,bookmaker_start_at) VALUES(?,?)`, job.ID, evidence.BookmakerStartTime.Unix()); err != nil {
		return err
	}
	for book, rawSource := range evidence.OddsSources {
		var source struct {
			EventID    string    `json:"eventId"`
			ObservedAt time.Time `json:"observedAt"`
			ExpiresAt  time.Time `json:"expiresAt"`
		}
		encoded, err := json.Marshal(rawSource)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(encoded, &source); err != nil {
			return err
		}
		if source.EventID == "" || source.ObservedAt.IsZero() {
			continue
		}
		runID := job.ID + "-" + strings.ToLower(book)
		var bets []AnalyzedBet
		indices := map[string]int{}
		valueCount, trapCount := 0, 0
		for _, bet := range job.Result {
			if string(bet.Bookmaker) != book || bet.ContextPending || bet.FairModel == nil || bet.PlayerID <= 0 {
				continue
			}
			key := researchHash([]any{evidence.Fixture.ID, bet.PlayerID, bet.MarketType, bet.Line, bet.IsUnder})
			if index, exists := indices[key]; exists {
				if bet.BookmakerOdds > bets[index].BookmakerOdds {
					bets[index] = bet
				}
				continue
			}
			indices[key] = len(bets)
			bets = append(bets, bet)
		}
		for _, bet := range bets {
			if bet.Verdict == "VALUE" {
				valueCount++
			}
			if bet.Verdict == "TRAP" {
				trapCount++
			}
		}
		if _, err = tx.ExecContext(saveCtx, `INSERT OR IGNORE INTO prediction_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, runID, job.ID, job.Origin, book, source.EventID, evidence.Fixture.ID, job.Match, evidence.Fixture.StartTime.Unix(), job.FinishedAt.Unix(), source.ObservedAt.Unix(), source.ExpiresAt.Unix(), fairModelVersion, valueCount, trapCount, len(bets), path); err != nil {
			return err
		}
		for _, bet := range bets {
			key := researchHash([]any{evidence.Fixture.ID, bet.PlayerID, bet.MarketType, bet.Line, bet.IsUnder})
			h := histories[bet.PlayerID]
			if h != nil {
				h = metricHistory(h, bet.MarketType)
			}
			complete := h != nil && !h.Incomplete && h.Matches >= job.Request.MatchLimit
			// Histories, shots and team overviews live once in the compressed evidence.
			bet.MatchOverview, bet.SampleValues, bet.SampleValuesSuperSub = nil, nil, nil
			bet.SampleMinutes, bet.SamplePositions, bet.SampleOpponents, bet.SampleDates, bet.SampleShots, bet.SampleRatings = nil, nil, nil, nil, nil, nil
			p := predictionRecord{SchemaVersion: researchSchema, ID: researchHash([]string{runID, key}), RunID: runID, BetKey: key, Algorithm: fairModelVersion, Origin: job.Origin, Match: job.Match, FixtureID: evidence.Fixture.ID, EventID: source.EventID, StartAt: evidence.Fixture.StartTime, AnalyzedAt: job.FinishedAt, ObservedAt: source.ObservedAt, ExpiresAt: source.ExpiresAt, HistoryComplete: complete, EvidenceID: evidenceID, EvidencePath: path, Bet: bet}
			raw, err := json.Marshal(p)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(saveCtx, `INSERT OR IGNORE INTO predictions VALUES(?,?,?,?,?,?)`, p.ID, runID, key, bet.PlayerID, bet.Verdict, string(raw)); err != nil {
				return err
			}
		}
		log.Printf("[PREDICTIONS] id=%s match=%q stage=saved bookmaker=%s value=%d traps=%d total=%d", job.ID, job.Match, book, valueCount, trapCount, len(bets))
	}
	return tx.Commit()
}

func canonicalHistories(source []*PlayerStatHistory) []*PlayerStatHistory {
	result := make([]*PlayerStatHistory, 0, len(source))
	for _, history := range source {
		copy := *history
		copy.FromCache = false
		result = append(result, &copy)
	}
	return result
}

func (s *calendarServer) autoAnalyze(ctx context.Context, candidate statsWarmupCandidate, odds matchOddsSnapshot) (count int, outcome string, failure error) {
	query := AnalysisRequest{MatchLimit: 10, TournamentIDs: "ALL"}
	query.STSURL = candidate.Matches["STS"].URL
	query.BetclicURL = candidate.Matches["BETCLIC"].URL
	query.SuperbetURL = candidate.Matches["SUPERBET"].URL
	if err := validateAnalysisRequest(&query); err != nil {
		return 0, "error", err
	}
	var random [12]byte
	if _, err := cryptorand.Read(random[:]); err != nil {
		return 0, "error", err
	}
	taskCtx, cancel := context.WithCancel(ctx)
	task := &analysisTask{ctx: taskCtx, cancel: cancel, job: AnalysisJob{ID: fmt.Sprintf("%x", random[:]), Origin: "automatic", Match: candidate.Event.Home + " vs " + candidate.Event.Away, Request: query, Status: "running", Stage: "teams", CreatedAt: time.Now().UTC(), StartedAt: time.Now().UTC(), Warnings: []string{}}}
	defer cancel()
	defer func() {
		if value := recover(); value != nil {
			failure = fmt.Errorf("automatyczna analiza: panic: %v", value)
			outcome = "error"
			s.finishAnalysisTask(task, nil, failure)
		}
	}()
	s.analysisMu.Lock()
	for id, old := range s.jobs {
		if len(s.jobs) < 64 {
			break
		}
		state := old.snapshot()
		if state.Status != "running" && state.Status != "queued" {
			delete(s.jobs, id)
		}
	}
	s.jobs[task.job.ID] = task
	s.analysisMu.Unlock()
	client, err := newSofaClient(analysisContext(taskCtx, task.snapshot(), "SOFASCORE", "automatyczna-analiza"), task.warn, task.report)
	if err != nil {
		s.finishAnalysisTask(task, nil, err)
		return 0, "error", err
	}
	defer client.tlsClient.CloseIdleConnections()
	client.statsPrefetch = true
	log.Printf("[AUTO-ANALYSIS] id=%s match=%q stage=start bookmaker=%s event_id=%s odds_age=%s", task.job.ID, task.job.Match, candidate.Bookmaker, candidate.Event.ID, time.Since(odds.ObservedAt).Round(time.Second))
	engine := analysisEngine{client: client, task: task, server: s}
	result, err := engine.analyze(query)
	task.mu.Lock()
	if task.evidence != nil {
		task.evidence.BookmakerStartTime = candidate.StartTime
	}
	task.mu.Unlock()
	err = errors.Join(err, client.archiveError(), ctx.Err())
	s.finishAnalysisTask(task, result, err)
	state := task.snapshot()
	client.state.mu.Lock()
	unavailable, pending := client.state.statsUnavailable, client.state.statsMissingPending
	client.state.mu.Unlock()
	if state.Status != "completed" {
		if errors.Is(err, errSofaTeamVerification) {
			return 0, "sofa-teams", err
		}
		if errors.Is(err, errNoPlayerHistory) && unavailable > 0 && ctx.Err() == nil && client.archiveError() == nil {
			if pending == 0 {
				return 0, "sofa-missing", nil
			}
			return 0, "sofa-unavailable", nil
		}
		if err == nil {
			err = errors.New(state.Error)
		}
		return 0, "error", err
	}
	players := map[int]bool{}
	for _, bet := range result {
		players[bet.PlayerID] = true
	}
	if unavailable > 0 {
		if pending == 0 {
			return len(players), "sofa-missing", nil
		}
		return len(players), "sofa-partial", nil
	}
	return len(players), "completed", nil
}

// Quotes are an overlay on immutable predictions, not another model run.
// The original run and evidence remain the source for evaluation and samples.
type automaticPriceCheck struct {
	OfferVersion  string             `json:"offerVersion"`
	CheckedAt     time.Time          `json:"checkedAt"`
	ExpiresAt     time.Time          `json:"expiresAt"`
	Bets          []AnalyzedBet      `json:"bets"`
	BestOdds      map[string]float64 `json:"bestOdds"`
	Improved      int                `json:"improved"`
	NewSelections int                `json:"newSelections"`
	Missing       int                `json:"missing"`
}

// Calendar cards need counts and timestamps, not histories, shotmaps and
// match context attached to every priced bet in the archived quote overlay.
type automaticPriceSummary struct {
	CheckedAt     time.Time `json:"checkedAt"`
	ExpiresAt     time.Time `json:"expiresAt"`
	Improved      int       `json:"improved"`
	NewSelections int       `json:"newSelections"`
	Missing       int       `json:"missing"`
	Bets          []struct {
		Verdict string `json:"verdict"`
	} `json:"bets"`
}

func automaticBetKey(bet AnalyzedBet) string {
	return researchHash([]any{bet.Bookmaker, bet.PlayerID, bet.MarketType, bet.Line, bet.IsUnder})
}

func (store *researchStore) priceCheck(runID string) (*automaticPriceCheck, error) {
	var raw string
	err := store.db.QueryRow(`SELECT payload FROM automatic_price_checks WHERE run_id=?`, runID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var check automaticPriceCheck
	if err = json.Unmarshal([]byte(raw), &check); err != nil {
		return nil, err
	}
	return &check, nil
}

func registerHistoryAliases(target map[string]string, history *PlayerStatHistory) {
	id := strconv.Itoa(history.ID)
	norm := NormalizePlayerName(history.FullName)
	if norm == "" {
		return
	}
	target[norm] = id
	parts := strings.Fields(norm)
	if len(parts) >= 2 {
		first, last := parts[0], parts[len(parts)-1]
		target[last+" "+strings.Join(parts[:len(parts)-1], " ")] = id
		target[string(first[0])+" "+last] = id
		target[last+" "+string(first[0])] = id
	}
}

func (store *researchStore) runPredictions(runID string) ([]predictionRecord, error) {
	rows, err := store.db.Query(`SELECT payload FROM predictions WHERE run_id=? ORDER BY bet_key`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []predictionRecord{}
	for rows.Next() {
		var raw string
		var p predictionRecord
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), &p); err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

func (s *calendarServer) monitorAutomaticPrices(ctx context.Context, candidate statsWarmupCandidate) (int, bool, error) {
	var analysisID string
	// A new algorithm or changed fixture identity needs one new baseline.
	err := s.research.db.QueryRow(`SELECT r.analysis_id FROM current_prediction_runs r JOIN automatic_analysis_identity i ON i.analysis_id=r.analysis_id WHERE r.bookmaker='STS' AND r.event_id=? AND i.bookmaker_start_at=? AND r.algorithm=?`, candidate.Event.ID, candidate.StartTime.Unix(), fairModelVersion).Scan(&analysisID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, true, err
	}
	type run struct{ id, book, event, path string }
	rows, err := s.research.db.Query(`SELECT id,bookmaker,event_id,evidence_path FROM prediction_runs WHERE analysis_id=?`, analysisID)
	if err != nil {
		return 0, true, err
	}
	runs := []run{}
	for rows.Next() {
		var item run
		if err = rows.Scan(&item.id, &item.book, &item.event, &item.path); err != nil {
			_ = rows.Close()
			return 0, true, err
		}
		runs = append(runs, item)
	}
	err = rows.Err()
	_ = rows.Close() // SQLite uses one connection; close before another query.
	if err != nil {
		return 0, true, err
	}
	if len(runs) != len(serverBookmakers) {
		return 0, true, fmt.Errorf("niekompletna analiza bazowa %s", analysisID)
	}
	for _, item := range runs {
		if candidate.Matches[item.book].ID != item.event {
			return 0, false, nil
		}
	}
	var aliases map[string]string
	updates := map[string]automaticPriceCheck{}
	players := map[int]bool{}
	newImprovements := 0
	for _, item := range runs {
		if !s.automaticLeagueEnabled(candidate.Matches) {
			return len(players), true, context.Canceled
		}
		previous, err := s.research.priceCheck(item.id)
		if err != nil {
			return 0, true, err
		}
		odds, err := s.matchOdds(ctx, item.book, item.event)
		if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
			s.rememberMarketAvailability(marketAvailabilityFromOdds(item.book, item.event, odds, err))
		}
		if err != nil {
			return 0, true, err
		}
		if previous != nil && previous.OfferVersion == odds.ArchiveVersion {
			previous.CheckedAt, previous.ExpiresAt = odds.ObservedAt, odds.ExpiresAt
			for i := range previous.Bets {
				previous.Bets[i].QuoteObservedAt = odds.ObservedAt
				players[previous.Bets[i].PlayerID] = true
			}
			updates[item.id] = *previous
			continue
		}
		base, err := s.research.runPredictions(item.id)
		if err != nil {
			return 0, true, err
		}
		if aliases == nil {
			var evidence predictionEvidence
			if err = s.research.readDocument(runs[0].path, &evidence); err != nil {
				return 0, true, err
			}
			aliases = map[string]string{}
			for _, history := range append(append([]*PlayerStatHistory{}, evidence.HomePlayers...), evidence.AwayPlayers...) {
				registerHistoryAliases(aliases, history)
			}
		}
		check := automaticPriceCheck{OfferVersion: odds.ArchiveVersion, CheckedAt: odds.ObservedAt, ExpiresAt: odds.ExpiresAt, Bets: []AnalyzedBet{}, BestOdds: map[string]float64{}}
		if previous != nil {
			for key, price := range previous.BestOdds {
				check.BestOdds[key] = price
			}
		}
		current := map[string]float64{}
		for _, prop := range odds.Props {
			id, _ := strconv.Atoi(matchPlayerStrict(prop.PlayerName, aliases))
			key := automaticBetKey(AnalyzedBet{Bookmaker: prop.Bookmaker, PlayerID: id, MarketType: prop.MarketType, Line: prop.Line, IsUnder: prop.IsUnder})
			if id == 0 {
				key = researchHash([]any{prop.Bookmaker, "unmatched", NormalizePlayerName(prop.PlayerName), prop.MarketType, prop.Line, prop.IsUnder})
			}
			if prop.Odds > current[key] {
				current[key] = prop.Odds
			}
		}
		modeled := map[string]bool{}
		for _, prediction := range base {
			bet := prediction.Bet
			players[bet.PlayerID] = true
			key := automaticBetKey(bet)
			modeled[key] = true
			price, available := current[key]
			if !available {
				check.Missing++
				continue
			}
			if check.BestOdds[key] == 0 {
				check.BestOdds[key] = bet.BookmakerOdds
			}
			if price > check.BestOdds[key]+1e-9 {
				newImprovements++
				check.BestOdds[key] = price
			}
			if price > bet.BookmakerOdds+1e-9 {
				check.Improved++
			}
			bet.AnalysisOdds, bet.QuoteObservedAt = bet.BookmakerOdds, odds.ObservedAt
			bet.PriceImproved = price > bet.BookmakerOdds+1e-9
			bet.BookmakerOdds = price
			// Use the exact frozen probability, not the rounded display fair odds.
			if bet.FairModel != nil && bet.FairModel.Probability > 0 {
				p := bet.FairModel.Probability
				bet.EdgePercent = math.Round((price*p-1)*1000) / 10
				evTax := (p*price*.88 - 1) * 100
				bet.EVWithTax = math.Round(evTax*10) / 10
				bet.Verdict = "NEUTRAL"
				if evTax > 0 {
					bet.Verdict = "VALUE"
				} else if evTax < -15 {
					bet.Verdict = "TRAP"
				}
			}
			check.Bets = append(check.Bets, bet)
		}
		for key := range current {
			if !modeled[key] {
				check.NewSelections++
			}
		}
		updates[item.id] = check
	}
	if err = ctx.Err(); err != nil {
		return len(players), true, err
	}
	if !s.automaticLeagueEnabled(candidate.Matches) {
		return len(players), true, context.Canceled
	}
	tx, err := s.research.db.BeginTx(ctx, nil)
	if err != nil {
		return len(players), true, err
	}
	defer tx.Rollback()
	for runID, check := range updates {
		raw, err := json.Marshal(check)
		if err != nil {
			return len(players), true, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO automatic_price_checks(run_id,payload) VALUES(?,?) ON CONFLICT(run_id) DO UPDATE SET payload=excluded.payload`, runID, string(raw)); err != nil {
			return len(players), true, err
		}
	}
	if err = tx.Commit(); err != nil {
		return len(players), true, err
	}
	log.Printf("[AUTO-ODDS] analysis_id=%s event_id=%s new_improvements=%d model_reused=true sofa_requests=0", analysisID, candidate.Event.ID, newImprovements)
	return len(players), true, nil
}

func (store *researchStore) predictionsForFixture(id string) ([]predictionRecord, error) {
	rows, err := store.db.Query(`SELECT p.payload,r.evidence_path FROM predictions p JOIN prediction_runs r ON r.id=p.run_id WHERE r.fixture_id=?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []predictionRecord
	for rows.Next() {
		var raw, path string
		if err := rows.Scan(&raw, &path); err != nil {
			return nil, err
		}
		var p predictionRecord
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return nil, err
		}
		p.EvidencePath = path
		result = append(result, p)
	}
	return result, rows.Err()
}

func (store *researchStore) prediction(id string) (predictionRecord, error) {
	var p predictionRecord
	var raw, path string
	err := store.db.QueryRow(`SELECT p.payload,r.evidence_path FROM predictions p JOIN prediction_runs r ON r.id=p.run_id WHERE p.id=?`, id).Scan(&raw, &path)
	if err != nil {
		return p, err
	}
	err = json.Unmarshal([]byte(raw), &p)
	p.EvidencePath = path
	return p, err
}

func (store *researchStore) summaries() ([]automaticSummary, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	return store.summariesContext(ctx)
}

func (store *researchStore) summariesContext(ctx context.Context) ([]automaticSummary, error) {
	db := store.summaryDB
	if db == nil {
		db = store.db
	}
	rows, err := db.QueryContext(ctx, `SELECT r.id,r.bookmaker,r.event_id,r.fixture_id,r.match_name,r.analyzed_at,r.expires_at,r.value_count,r.trap_count,r.total,COALESCE(q.payload,''),r.origin FROM current_prediction_runs r LEFT JOIN automatic_price_checks q ON q.run_id=r.id WHERE r.algorithm=? AND r.start_at>?`, fairModelVersion, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []automaticSummary{}
	for rows.Next() {
		var item automaticSummary
		var analyzed, expires int64
		var quoteRaw string
		var origin string
		if err := rows.Scan(&item.RunID, &item.Bookmaker, &item.EventID, &item.FixtureID, &item.Match, &analyzed, &expires, &item.Value, &item.Traps, &item.Total, &quoteRaw, &origin); err != nil {
			return nil, err
		}
		item.manual = origin == "manual"
		item.AnalyzedAt = time.Unix(analyzed, 0).UTC()
		item.ExpiresAt = time.Unix(expires, 0).UTC()
		item.Status = "completed"
		if quoteRaw != "" {
			var check automaticPriceSummary
			if err := json.Unmarshal([]byte(quoteRaw), &check); err != nil {
				item.Status, item.Error = "error", "Nie udało się odczytać aktualizacji kursów: "+err.Error()
				log.Printf("ERROR Podsumowanie meczu %q: %s", item.Match, item.Error)
				result = append(result, item)
				continue // Keep this baseline and every other match's counters.
			}
			item.Status, item.ExpiresAt, item.QuoteCheckedAt = "monitoring", check.ExpiresAt, check.CheckedAt
			item.Improved, item.NewSelections, item.Missing = check.Improved, check.NewSelections, check.Missing
			item.Total, item.Value, item.Traps = len(check.Bets), 0, 0
			for _, bet := range check.Bets {
				if bet.Verdict == "VALUE" {
					item.Value++
				}
				if bet.Verdict == "TRAP" {
					item.Traps++
				}
			}
		}
		if !time.Now().Before(item.ExpiresAt) {
			item.Status = "stale"
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *calendarServer) researchSummaries(ctx context.Context) ([]automaticSummary, error) {
	items, err := s.research.summariesContext(ctx)
	if err != nil {
		return nil, err
	}
	indexes := map[string]int{}
	for i, item := range items {
		indexes[item.Bookmaker+":"+item.EventID] = i
	}
	s.prefetchMu.Lock()
	for key, state := range s.prefetch.Events {
		if !state.StartTime.After(time.Now()) {
			continue
		}
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		i, ok := indexes[key]
		if !ok {
			i = len(items)
			indexes[key] = i
			items = append(items, automaticSummary{Bookmaker: parts[0], EventID: parts[1], Match: state.Match, Status: "waiting"})
		}
		// Until the new manual baseline has been monitored, these states refer
		// to the preparation it superseded, not to the saved manual result.
		if items[i].manual && items[i].QuoteCheckedAt.IsZero() {
			continue
		}
		if state.Status == "error" || state.Status == "partial" || state.Status == "unavailable" || strings.HasPrefix(state.Status, "sofa-") {
			items[i].Status = state.Status
			items[i].Error = state.Error
			if state.Error != "" && state.NextAttempt.After(time.Now()) {
				zone, _ := time.LoadLocation("Europe/Warsaw")
				items[i].Error += "; ponowienie po " + state.NextAttempt.In(zone).Format("15:04")
			}
		}
	}
	s.prefetchMu.Unlock()
	s.analysisMu.Lock()
	tasks := make([]*analysisTask, 0, len(s.jobs))
	for _, task := range s.jobs {
		tasks = append(tasks, task)
	}
	s.analysisMu.Unlock()
	for _, task := range tasks {
		job := task.snapshot()
		if job.Origin != "automatic" || job.Status != "running" {
			continue
		}
		for _, input := range []string{job.Request.SuperbetURL, job.Request.BetclicURL, job.Request.STSURL} {
			if input == "" {
				continue
			}
			id, _, book := extractIDAndSlug(input)
			key := book + ":" + id
			i, ok := indexes[key]
			if !ok {
				i = len(items)
				indexes[key] = i
				items = append(items, automaticSummary{Bookmaker: book, EventID: id, Match: job.Match})
			}
			if items[i].manual {
				continue // An older automatic task cannot cover the manual result.
			}
			items[i].Status = "running"
			items[i].Stage = job.Stage
			items[i].Error = ""
		}
	}
	offers := s.calendarOffers()
	for i := range items {
		if candidate, ok := offers.ByID[items[i].Bookmaker+":"+items[i].EventID]; ok {
			if matches, err := matchCalendarOffers(candidate.Event, offers); err == nil && !s.automaticLeagueEnabled(matches) {
				items[i].Status = "disabled"
			}
		}
	}
	return items, nil
}

type settledPrediction struct {
	SchemaVersion int       `json:"schemaVersion"`
	PredictionID  string    `json:"predictionId"`
	FixtureID     string    `json:"fixtureId"`
	Revision      string    `json:"revision"`
	Rules         string    `json:"rules"`
	Status        string    `json:"status"`
	Actual        *int      `json:"actual"`
	Minutes       int       `json:"minutes"`
	SubstituteID  int       `json:"substituteId,omitempty"`
	SettledAt     time.Time `json:"settledAt"`
	Reason        string    `json:"reason,omitempty"`
}

type postMatchData struct {
	SchemaVersion int                                   `json:"schemaVersion"`
	Event         json.RawMessage                       `json:"event"`
	Lineups       json.RawMessage                       `json:"lineups,omitempty"`
	Incidents     json.RawMessage                       `json:"incidents,omitempty"`
	Shotmap       json.RawMessage                       `json:"shotmap,omitempty"`
	Players       map[string]map[string]json.RawMessage `json:"players"`
	BenchAbsent   map[string]bool                       `json:"benchAbsent"`
	Errors        map[string]string                     `json:"errors"`
}

func (s *calendarServer) settlementStep(ctx context.Context) bool {
	var fixtureID, matchName, status string
	var start, previousCheck int64
	var attempts int
	err := s.research.db.QueryRow(`SELECT fixture_id,match_name,start_at,checked_at,status,attempts FROM fixture_results WHERE next_check>0 AND next_check<=? ORDER BY next_check LIMIT 1`, time.Now().Unix()).Scan(&fixtureID, &matchName, &start, &previousCheck, &status, &attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		log.Printf("[SETTLEMENT] stage=queue-error error=%v", err)
		return false
	}
	log.Printf("[SETTLEMENT] fixture=%s match=%q stage=event attempt=%d", fixtureID, matchName, attempts+1)
	client, err := newSofaClient(context.WithValue(ctx, archiveContextKey{}, archiveInfo{Bookmaker: "SOFASCORE", Match: matchName, Scope: "rozliczenie-mecz-" + fixtureID, Automatic: true}), nil, nil)
	if err != nil {
		s.settlementRetry(fixtureID, err)
		return true
	}
	defer client.tlsClient.CloseIdleConnections()
	client.statsPrefetch = true
	body, err := client.GetSofa("/event/" + fixtureID)
	if err != nil {
		s.settlementRetry(fixtureID, err)
		return true
	}
	var event struct {
		Event struct {
			ID     int   `json:"id"`
			Start  int64 `json:"startTimestamp"`
			Status struct {
				Type        string `json:"type"`
				Description string `json:"description"`
				Code        int    `json:"code"`
			} `json:"status"`
			HomeScore map[string]json.RawMessage `json:"homeScore"`
			AwayScore map[string]json.RawMessage `json:"awayScore"`
		} `json:"event"`
	}
	if err := json.Unmarshal(body, &event); err != nil || strconv.Itoa(event.Event.ID) != fixtureID {
		s.settlementRetry(fixtureID, fmt.Errorf("niepoprawna odpowiedź wydarzenia: %v", err))
		return true
	}
	if event.Event.Start > 0 {
		start = event.Event.Start
	}
	typeName := event.Event.Status.Type
	canceled := typeName == "canceled" || typeName == "cancelled"
	finished := typeName == "finished" || event.Event.Status.Code == 100
	if !finished && !canceled {
		next := time.Now().Add(5 * time.Minute)
		if start > time.Now().Unix() {
			next = time.Unix(start, 0).Add(110 * time.Minute)
		} else if time.Since(time.Unix(start, 0)) > 6*time.Hour {
			next = time.Now().Add(time.Hour)
		}
		if _, err := s.research.db.Exec(`UPDATE fixture_results SET status=?,start_at=?,next_check=?,error='' WHERE fixture_id=?`, typeName, start, next.Unix(), fixtureID); err != nil {
			log.Printf("[SETTLEMENT] fixture=%s stage=save-error error=%v", fixtureID, err)
		}
		log.Printf("[SETTLEMENT] fixture=%s stage=waiting status=%s next=%s", fixtureID, typeName, next.UTC().Format(time.RFC3339))
		return true
	}
	predictions, err := s.research.predictionsForFixture(fixtureID)
	if err != nil {
		s.settlementRetry(fixtureID, err)
		return true
	}
	data := postMatchData{SchemaVersion: researchSchema, Event: body, Players: map[string]map[string]json.RawMessage{}, BenchAbsent: map[string]bool{}, Errors: map[string]string{}}
	// Full-time totals including extra time cannot settle regulation-only markets.
	unsupportedPeriod := false
	for _, scores := range []map[string]json.RawMessage{event.Event.HomeScore, event.Event.AwayScore} {
		for _, key := range []string{"extra1", "extra2", "overtime", "penalties"} {
			if raw, ok := scores[key]; ok && string(raw) != "null" {
				unsupportedPeriod = true
			}
		}
	}
	description := strings.ToLower(event.Event.Status.Description)
	if strings.Contains(description, "extra") || strings.Contains(description, "penalt") {
		unsupportedPeriod = true
	}
	var inc SofaIncidentResponse
	var shotmap SofaShotmapResponse
	players := map[int]bool{}
	for _, p := range predictions {
		players[p.Bet.PlayerID] = true
	}
	if !canceled && !unsupportedPeriod {
		fetch := func(endpoint string, target *json.RawMessage) {
			raw, fetchErr := client.GetSofa("/event/" + fixtureID + endpoint)
			if fetchErr != nil {
				data.Errors[endpoint] = fetchErr.Error()
				return
			}
			if !json.Valid(raw) {
				data.Errors[endpoint] = "niepoprawny JSON"
				return
			}
			*target = raw
		}
		fetch("/lineups", &data.Lineups)
		fetch("/incidents", &data.Incidents)
		if len(data.Incidents) > 0 {
			if err := json.Unmarshal(data.Incidents, &inc); err != nil {
				data.Errors["/incidents"] = err.Error()
				data.Incidents = nil
			}
		}
		for _, i := range inc.Incidents {
			if i.IncidentType == "substitution" && players[i.PlayerOut.ID] && i.PlayerIn.ID > 0 {
				players[i.PlayerIn.ID] = true
			}
		}
		needsShots := false
		for _, p := range predictions {
			if strings.HasPrefix(string(p.Bet.MarketType), "SHOTS_") && p.Bet.MarketType != MarketShotsTotal {
				needsShots = true
			}
		}
		if needsShots {
			fetch("/shotmap", &data.Shotmap)
			if len(data.Shotmap) > 0 {
				if err := json.Unmarshal(data.Shotmap, &shotmap); err != nil {
					data.Errors["/shotmap"] = err.Error()
					data.Shotmap = nil
				}
			}
		}
		var lineups struct {
			Confirmed *bool `json:"confirmed"`
			Home      struct {
				Players []struct {
					Player     SofaPlayerDetails          `json:"player"`
					Substitute *bool                      `json:"substitute"`
					Statistics map[string]json.RawMessage `json:"statistics"`
				} `json:"players"`
			} `json:"home"`
			Away struct {
				Players []struct {
					Player     SofaPlayerDetails          `json:"player"`
					Substitute *bool                      `json:"substitute"`
					Statistics map[string]json.RawMessage `json:"statistics"`
				} `json:"players"`
			} `json:"away"`
		}
		if len(data.Lineups) > 0 {
			if err := json.Unmarshal(data.Lineups, &lineups); err != nil {
				data.Errors["/lineups"] = err.Error()
				data.Lineups = nil
			}
		}
		for _, p := range append(lineups.Home.Players, lineups.Away.Players...) {
			id := strconv.Itoa(p.Player.ID)
			if p.Statistics != nil {
				data.Players[id] = p.Statistics
			}
			if lineups.Confirmed != nil && *lineups.Confirmed && p.Substitute != nil && *p.Substitute && len(data.Incidents) > 0 {
				entered := false
				for _, i := range inc.Incidents {
					if i.IncidentType == "substitution" && i.PlayerIn.ID == p.Player.ID {
						entered = true
					}
				}
				minutes, _ := statInteger(p.Statistics, "minutesPlayed")
				if !entered && minutes == 0 {
					data.BenchAbsent[id] = true
				}
			}
		}
		ids := []int{}
		for id := range players {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		for i, id := range ids {
			if ctx.Err() != nil {
				return true
			}
			key := strconv.Itoa(id)
			fields := data.Players[key]
			if data.BenchAbsent[key] {
				continue
			}
			// Player endpoints preserve absent keys; missing statistics are not zero.
			endpoint := "/event/" + fixtureID + "/player/" + key + "/statistics"
			raw, fetchErr := client.GetSofa(endpoint)
			if fetchErr == nil {
				var response struct {
					Statistics map[string]json.RawMessage `json:"statistics"`
				}
				fetchErr = json.Unmarshal(raw, &response)
				if fetchErr == nil && response.Statistics == nil {
					fetchErr = fmt.Errorf("brak obiektu statistics")
				}
				if fetchErr == nil {
					fields = response.Statistics
					data.Players[key] = fields
				}
			}
			if fetchErr != nil {
				data.Errors[endpoint] = fetchErr.Error()
			}
			log.Printf("[SETTLEMENT] fixture=%s match=%q stage=players players=%d/%d remaining=%d player_id=%d error=%v", fixtureID, matchName, i+1, len(ids), len(ids)-i-1, id, fetchErr)
		}
	}
	if ctx.Err() != nil {
		return true
	}
	if err := client.archiveError(); err != nil {
		s.settlementRetry(fixtureID, err)
		return true
	}
	revision := researchHash(map[string]any{"metricsVersion": settlementMetricsVersion, "data": data})
	path, err := s.research.saveDocument("wyniki-statystyki", matchName+"-mecz-"+fixtureID, data)
	if err != nil {
		s.settlementRetry(fixtureID, err)
		return true
	}
	now := time.Now().UTC()
	tx, err := s.research.db.Begin()
	if err != nil {
		s.settlementRetry(fixtureID, err)
		return true
	}
	defer tx.Rollback()
	counts := map[string]int{}
	unresolved := 0
	for _, p := range predictions {
		for _, rules := range []string{"standard-v1", "supersub-v1"} {
			out := settledPrediction{SchemaVersion: researchSchema, PredictionID: p.ID, FixtureID: fixtureID, Revision: revision, Rules: rules, Status: "unresolved", SettledAt: now}
			switch {
			case canceled:
				out.Status = "void"
				out.Reason = "wydarzenie odwołane"
			case unsupportedPeriod:
				out.Reason = "dogrywka lub karne: brak jednoznacznych statystyk regulaminowego czasu"
			case data.BenchAbsent[strconv.Itoa(p.Bet.PlayerID)]:
				out.Status = "void"
				out.Reason = "potwierdzony brak udziału zawodnika"
			default:
				fields := data.Players[strconv.Itoa(p.Bet.PlayerID)]
				minutes, hasMinutes := statInteger(fields, "minutesPlayed")
				out.Minutes = minutes
				if !hasMinutes || minutes <= 0 {
					out.Reason = "brak potwierdzonych statystyk udziału"
					break
				}
				value, valid := settlementMetric(fields, p.Bet.MarketType, shotmap, p.Bet.PlayerID, len(data.Shotmap) > 0)
				if rules == "supersub-v1" && minutes < 85 {
					if len(data.Incidents) == 0 {
						valid = false
						out.Reason = "brak incydentów do sprawdzenia SuperSub"
					} else {
						for _, i := range inc.Incidents {
							if i.IncidentType == "substitution" && i.PlayerOut.ID == p.Bet.PlayerID {
								out.SubstituteID = i.PlayerIn.ID
								sub, ok := settlementMetric(data.Players[strconv.Itoa(i.PlayerIn.ID)], p.Bet.MarketType, shotmap, i.PlayerIn.ID, len(data.Shotmap) > 0)
								value += sub
								valid = valid && ok
								break
							}
						}
					}
				}
				if !valid {
					if out.Reason == "" {
						out.Reason = "brak wymaganej statystyki lub mapy strzałów"
					}
					break
				}
				out.Actual = &value
				if float64(value) == p.Bet.Line {
					out.Status = "void"
					out.Reason = "wynik równy linii całkowitej"
				} else if (float64(value) > p.Bet.Line) != p.Bet.IsUnder {
					out.Status = "won"
				} else {
					out.Status = "lost"
				}
			}
			if out.Status == "unresolved" {
				unresolved++
			}
			if rules == "standard-v1" {
				counts[out.Status]++
			}
			raw, marshalErr := json.Marshal(out)
			if marshalErr != nil {
				err = marshalErr
				break
			}
			if _, err = tx.Exec(`INSERT INTO settlements VALUES(?,?,?,?,?,?) ON CONFLICT(prediction_id,rules) DO UPDATE SET revision=excluded.revision,status=excluded.status,payload=excluded.payload`, p.ID, rules, fixtureID, revision, out.Status, string(raw)); err != nil {
				break
			}
			if _, err = tx.Exec(`INSERT OR IGNORE INTO settlement_versions VALUES(?,?,?,?)`, p.ID, rules, revision, string(raw)); err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	if err != nil {
		_ = tx.Rollback()
		s.settlementRetry(fixtureID, err)
		return true
	}
	next := int64(0)
	if unresolved > 0 {
		next = now.Add(time.Hour).Unix()
		if attempts < 3 {
			next = now.Add(15 * time.Minute).Unix()
		}
		if now.Unix()-start > 72*3600 {
			next = now.Add(24 * time.Hour).Unix()
		}
	} else if previousCheck == 0 || now.Unix()-previousCheck < 20*3600 {
		next = now.Add(24 * time.Hour).Unix()
	}
	if _, err = tx.Exec(`UPDATE fixture_results SET status=?,start_at=?,next_check=?,checked_at=?,revision=?,data_path=?,error='',attempts=attempts+1 WHERE fixture_id=?`, typeName, start, next, now.Unix(), revision, path, fixtureID); err == nil {
		err = tx.Commit()
	}
	if err != nil {
		_ = tx.Rollback()
		s.settlementRetry(fixtureID, err)
		return true
	}
	log.Printf("[SETTLEMENT] fixture=%s match=%q stage=completed predictions=%d won=%d lost=%d void=%d unresolved=%d revision=%s", fixtureID, matchName, len(predictions), counts["won"], counts["lost"], counts["void"], counts["unresolved"], revision[:12])
	for _, rules := range []string{"standard-v1", "supersub-v1"} {
		if _, err := s.writeEvaluationReport(30, .88, rules); err != nil {
			log.Printf("[REPORT] stage=save-error rules=%s error=%v", rules, err)
		}
	}
	return true
}

func (s *calendarServer) settlementRetry(id string, err error) {
	next := time.Now().Add(15 * time.Minute)
	if until := sofaCooldownUntil(); until.After(next) {
		next = until
	}
	if _, saveErr := s.research.db.Exec(`UPDATE fixture_results SET next_check=?,error=?,attempts=attempts+1 WHERE fixture_id=?`, next.Unix(), err.Error(), id); saveErr != nil {
		log.Printf("[SETTLEMENT] fixture=%s stage=save-error error=%v", id, saveErr)
	}
	log.Printf("[SETTLEMENT] fixture=%s stage=retry next=%s error=%v", id, next.UTC().Format(time.RFC3339), err)
}

func statInteger(fields map[string]json.RawMessage, key string) (int, bool) {
	raw, ok := fields[key]
	if !ok || string(raw) == "null" {
		return 0, false
	}
	var value int
	if json.Unmarshal(raw, &value) != nil {
		return 0, false
	}
	return value, value >= 0
}

func settlementMetric(fields map[string]json.RawMessage, market CanonicalMarket, shots SofaShotmapResponse, playerID int, hasShots bool) (int, bool) {
	keys := map[CanonicalMarket]string{MarketShotsTotal: "totalShots", MarketShotsOnTarget: "onTargetScoringAttempt", MarketShotsBlocked: "blockedScoringAttempt", MarketShotsOffTarget: "shotOffTarget", MarketFoulsCommitted: "fouls", MarketFoulsDrawn: "wasFouled", MarketInterceptions: "interceptionWon", MarketOffsides: "totalOffside", MarketDispossessed: "dispossessed"}
	if market == MarketTackles {
		return statInteger(fields, tacklesStatsField)
	}
	if market == MarketPasses {
		return statInteger(fields, "totalPass")
	}
	if market == MarketAccuratePasses {
		value, valid := statInteger(fields, "accuratePass")
		if total, known := statInteger(fields, "totalPass"); known && value > total {
			return 0, false
		}
		return value, valid
	}
	if key, ok := keys[market]; ok {
		value, valid := statInteger(fields, key)
		if market == MarketShotsOnTarget || market == MarketShotsBlocked {
			total, totalOK := statInteger(fields, "totalShots")
			if !totalOK {
				return 0, false
			}
			if total > 0 && (!hasShots || len(shots.PlayerShots(playerID, 0)) != total) {
				return 0, false
			}
			for _, shot := range shots.PlayerShots(playerID, 0) {
				if shot.ShotType == "blocked-off-line" {
					if market == MarketShotsOnTarget && value > 0 {
						value--
					}
					if market == MarketShotsBlocked {
						value++
					}
				}
			}
		}
		return value, valid
	}
	if !strings.HasPrefix(string(market), "SHOTS_") {
		return 0, false
	}
	total, ok := statInteger(fields, "totalShots")
	if !ok {
		return 0, false
	}
	if total == 0 {
		return 0, true
	}
	if !hasShots {
		return 0, false
	}
	playerShots := shots.PlayerShots(playerID, 0)
	if len(playerShots) != total {
		return 0, false
	}
	value := 0
	for _, shot := range playerShots {
		switch market {
		case MarketShotsInsideBox:
			if shot.InBox {
				value++
			}
		case MarketShotsOutsideBox:
			if !shot.InBox {
				value++
			}
		case MarketShotsOnTargetInsideBox:
			if shot.InBox && shot.OnTarget {
				value++
			}
		case MarketShotsOnTargetOutsideBox:
			if !shot.InBox && shot.OnTarget {
				value++
			}
		default:
			return 0, false
		}
	}
	return value, true
}

type evaluationRow struct {
	Algorithm         string   `json:"algorithm"`
	Bookmaker         string   `json:"bookmaker"`
	Verdict           string   `json:"verdict"`
	MinimumHitRate    float64  `json:"minimumHitRate"`
	Bets              int      `json:"bets"`
	Matches           int      `json:"matches"`
	Won               int      `json:"won"`
	Lost              int      `json:"lost"`
	Void              int      `json:"void"`
	Pending           int      `json:"pending"`
	HistoricalHitRate float64  `json:"historicalHitRate"`
	ActualHitRate     float64  `json:"actualHitRate"`
	Stakes            float64  `json:"stakes"`
	Returns           float64  `json:"returns"`
	WinsProfit        float64  `json:"winsProfit"`
	Losses            float64  `json:"losses"`
	Net               float64  `json:"net"`
	ROI               float64  `json:"roi"`
	Brier             *float64 `json:"brier"`
	fixtures          map[string]bool
	brierSum          float64
}

type evaluationReport struct {
	SchemaVersion    int              `json:"schemaVersion"`
	Strategy         string           `json:"strategy"`
	AsOf             time.Time        `json:"asOf"`
	From             time.Time        `json:"from"`
	Days             int              `json:"days"`
	MinimumSample    int              `json:"minimumSample"`
	PayoutMultiplier float64          `json:"payoutMultiplier"`
	Rules            string           `json:"rules"`
	Rows             []*evaluationRow `json:"rows"`
	Notes            []string         `json:"notes"`
}

func (s *calendarServer) evaluationReport(days int, multiplier float64, rules string) (evaluationReport, error) {
	now := time.Now().UTC()
	report := evaluationReport{SchemaVersion: researchSchema, Strategy: evaluationStrategy, AsOf: now, From: now.AddDate(0, 0, -days), Days: days, MinimumSample: 10, PayoutMultiplier: multiplier, Rules: rules, Rows: []*evaluationRow{}, Notes: []string{"Single po 1 jednostce. Progi są kumulatywne i dotyczą historycznego hitrate, nie prawdopodobieństwa modelu.", "Ostatnia automatyczna analiza przed T-60 min; oferta najwyżej 15-minutowa w chwili analizy; kompletna próbka minimum 10 meczów.", "ROI = wynik netto / stawki rozliczone wygraną lub przegraną. Zwroty nie zwiększają mianownika; nierozliczone nie są stratami.", "Symulacja standard-v1: udział zawodnika wymagany, linia całkowita równa wynikowi oznacza zwrot. supersub-v1 odtwarza dotychczasowy wariant programu (pierwszy zmiennik, poniżej 85 min), nie potwierdzone rozliczenie bukmachera.", "BEST_PRICE wybiera jedną najwyższą cenę typu z migawek T-60 poszczególnych bukmacherów; ceny nie musiały występować jednocześnie. Wyniki według bukmachera są podstawowym porównaniem."}}
	rows, err := s.research.db.Query(`SELECT p.payload,COALESCE(st.payload,'') FROM predictions p JOIN prediction_runs r ON r.id=p.run_id JOIN fixture_results f ON f.fixture_id=r.fixture_id LEFT JOIN settlements st ON st.prediction_id=p.id AND st.rules=? WHERE f.start_at>=? AND f.start_at<=? AND r.origin='automatic' AND r.id=(SELECT r2.id FROM prediction_runs r2 WHERE r2.origin='automatic' AND r2.bookmaker=r.bookmaker AND r2.event_id=r.event_id AND r2.algorithm=r.algorithm AND r2.analyzed_at<=f.start_at-3600 AND r2.analyzed_at>=f.start_at-172800 AND r2.observed_at<=r2.analyzed_at AND r2.analyzed_at-r2.observed_at<=900 AND r2.expires_at>r2.analyzed_at ORDER BY r2.analyzed_at DESC,r2.id DESC LIMIT 1)`, rules, report.From.Unix(), now.Unix())
	if err != nil {
		return report, err
	}
	type observation struct {
		p   predictionRecord
		out settledPrediction
	}
	var observations []observation
	best := map[string]observation{}
	for rows.Next() {
		var raw, outRaw string
		if err := rows.Scan(&raw, &outRaw); err != nil {
			rows.Close()
			return report, err
		}
		var o observation
		if err := json.Unmarshal([]byte(raw), &o.p); err != nil {
			rows.Close()
			return report, err
		}
		if !o.p.HistoryComplete || o.p.Bet.MatchesSample < 10 {
			continue
		}
		if outRaw != "" {
			if err := json.Unmarshal([]byte(outRaw), &o.out); err != nil {
				rows.Close()
				return report, err
			}
		}
		observations = append(observations, o)
		key := o.p.Algorithm + ":" + o.p.BetKey
		if old, ok := best[key]; !ok || o.p.Bet.BookmakerOdds > old.p.Bet.BookmakerOdds {
			best[key] = o
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return report, err
	}
	groups := map[string]*evaluationRow{}
	add := func(o observation, book string) {
		for _, threshold := range []float64{0, 60, 70, 80, 90, 100} {
			if o.p.Bet.HitRate < threshold {
				continue
			}
			for _, category := range []string{o.p.Bet.Verdict, "ALL"} {
				key := fmt.Sprintf("%s:%s:%s:%g", o.p.Algorithm, book, category, threshold)
				row := groups[key]
				if row == nil {
					row = &evaluationRow{Algorithm: o.p.Algorithm, Bookmaker: book, Verdict: category, MinimumHitRate: threshold, fixtures: map[string]bool{}}
					groups[key] = row
				}
				row.Bets++
				row.fixtures[o.p.FixtureID] = true
				row.HistoricalHitRate += o.p.Bet.HitRate
				switch o.out.Status {
				case "won":
					row.Won++
					row.Stakes++
					payout := o.p.Bet.BookmakerOdds * multiplier
					row.Returns += payout
					row.WinsProfit += payout - 1
					if o.p.Bet.FairModel != nil {
						row.brierSum += math.Pow(1-o.p.Bet.FairModel.Probability, 2)
					}
				case "lost":
					row.Lost++
					row.Stakes++
					row.Losses++
					if o.p.Bet.FairModel != nil {
						row.brierSum += math.Pow(o.p.Bet.FairModel.Probability, 2)
					}
				case "void":
					row.Void++
					row.Stakes++
					row.Returns++
				default:
					row.Pending++
				}
			}
		}
	}
	for _, o := range observations {
		add(o, string(o.p.Bet.Bookmaker))
	}
	for _, o := range best {
		add(o, "BEST_PRICE")
	}
	for _, row := range groups {
		row.Matches = len(row.fixtures)
		row.HistoricalHitRate /= float64(row.Bets)
		row.Net = row.Returns - row.Stakes
		if row.Won+row.Lost > 0 {
			n := float64(row.Won + row.Lost)
			row.ActualHitRate = 100 * float64(row.Won) / n
			row.ROI = 100 * row.Net / n
			brier := row.brierSum / n
			if rules == "standard-v1" {
				row.Brier = &brier
			}
		}
		report.Rows = append(report.Rows, row)
	}
	sort.Slice(report.Rows, func(i, j int) bool {
		a, b := report.Rows[i], report.Rows[j]
		if a.Algorithm != b.Algorithm {
			return a.Algorithm < b.Algorithm
		}
		if a.Bookmaker != b.Bookmaker {
			return a.Bookmaker < b.Bookmaker
		}
		if a.Verdict != b.Verdict {
			return a.Verdict < b.Verdict
		}
		return a.MinimumHitRate < b.MinimumHitRate
	})
	return report, nil
}

func (s *calendarServer) writeEvaluationReport(days int, multiplier float64, rules string) (evaluationReport, error) {
	s.research.mu.Lock()
	defer s.research.mu.Unlock()
	report, err := s.evaluationReport(days, multiplier, rules)
	if err != nil {
		return report, err
	}
	label := time.Now().UTC().Format("2006-01-02") + "_" + evaluationStrategy + "_" + rules + "_" + strconv.Itoa(days) + "-dni"
	if _, err := s.research.saveDocument("raport-skutecznosci", label, report); err != nil {
		return report, err
	}
	dir := filepath.Join(s.dataDir, "reports")
	if err := os.MkdirAll(dir, 0750); err != nil {
		return report, err
	}
	err = atomicJSON(filepath.Join(dir, label+".json"), report)
	log.Printf("[REPORT] stage=completed strategy=%s rules=%s days=%d groups=%d error=%v", evaluationStrategy, rules, days, len(report.Rows), err)
	return report, err
}

type researchProgress struct {
	At                 time.Time        `json:"at"`
	CalendarOffers     int              `json:"calendarOffers"`
	MarketsAvailable   int              `json:"marketsAvailable"`
	AnalysesReady      int              `json:"analysesReady"`
	AnalysesWaiting    int              `json:"analysesWaiting"`
	StatisticsMissing  int              `json:"statisticsMissing"`
	Errors             int              `json:"errors"`
	SettlementsWaiting int              `json:"settlementsWaiting"`
	SettlementsMissing int              `json:"settlementsMissing"`
	Active             []map[string]any `json:"active"`
}

type readableLogWriter struct{ output io.Writer }

var logDatePrefix = regexp.MustCompile(`^\d{4}[/-]\d{2}[/-]\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?\s+`)
var logModuleTag = regexp.MustCompile(`^\[(?:[A-Z][A-Z0-9_-]*|Betclic)\]\s*`)
var logExplicitLevel = regexp.MustCompile(`^(INFO|WARNING|WARN|ERROR|FATAL|DEBUG)(?:\s+|:\s*)`)
var logEmptyError = regexp.MustCompile(`(?i)\b[a-z0-9_-]*error=(?:"?(?:<nil>|nil|null|none|false)"?|""|0)(?:\s|$)`)
var logErrorField = regexp.MustCompile(`(?i)\b[a-z0-9_-]*error=\S`)
var logErrorMarker = regexp.MustCompile(`(?i)(?:\b(?:error|fatal|failed|panic)(?:_[a-z0-9_-]+)?\b|\b[a-z0-9_-]+_(?:error|failed)\b|stage=[a-z0-9_-]*(?:error|failed)|status=(?:error|failed)|błąd|blad\b|nie można|niepopraw|nieprawidł)`)
var logCanceledError = regexp.MustCompile(`(?i)\b[a-z0-9_-]*error=(?:"context canceled"|context canceled)(?:\s|$)`)
var logTimeoutSetting = regexp.MustCompile(`(?i)\b(?:[a-z0-9_-]+_)?timeout=(?:\d+(?:\.\d+)?(?:ns|us|µs|ms|s|m|h))+(?:\s|$)`)
var logWarningMarker = regexp.MustCompile(`(?i)(?:\b(?:warning|warn|timeout|pause|paused|cooldown)\b|\b[a-z0-9_-]+_(?:warning|timeout)\b|ostrzeż|niejednoznacz|niekompletn)`)
var logProgressSuffix = regexp.MustCompile(`\s+\|\s+(?:analysis_ready|analysis_waiting|stats_missing|settlement_waiting)=.*$`)

// Adapt existing call sites to one readable format. Severity is evaluated
// before internal tags are removed; error=<nil> is not an error.
func readableLogLine(message string) string {
	message = strings.TrimSpace(logDatePrefix.ReplaceAllString(strings.TrimSpace(message), ""))
	message = logProgressSuffix.ReplaceAllString(message, "")
	level := "INFO"
	clean := message
	for logModuleTag.MatchString(clean) {
		clean = strings.TrimSpace(logModuleTag.ReplaceAllString(clean, ""))
	}
	if match := logExplicitLevel.FindStringSubmatch(clean); len(match) > 0 {
		level = match[1]
		if level == "WARN" {
			level = "WARNING"
		}
		if level == "FATAL" {
			level = "ERROR"
		}
		if level == "DEBUG" {
			level = "INFO"
		}
		clean = strings.TrimSpace(clean[len(match[0]):])
	} else {
		classification := logEmptyError.ReplaceAllString(message, " ")
		// A configured request budget is not a timeout event. Keep the value
		// in the output, but exclude it from automatic severity inference.
		classification = logTimeoutSetting.ReplaceAllString(classification, " ")
		switch {
		case logCanceledError.MatchString(classification):
			level = "WARNING"
		case logErrorField.MatchString(classification), logErrorMarker.MatchString(classification):
			level = "ERROR"
		case logWarningMarker.MatchString(classification):
			level = "WARNING"
		}
	}
	clean = strings.TrimSpace(logEmptyError.ReplaceAllString(clean, " "))
	return level + " " + clean
}

func (w readableLogWriter) Write(data []byte) (int, error) {
	entry := readableLogLine(strings.TrimRight(string(data), "\r\n"))
	level, body, _ := strings.Cut(entry, " ")
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if _, err := fmt.Fprintln(w.output, level+" "+strings.TrimRight(line, "\r")); err != nil {
			return 0, err
		}
	}
	return len(data), nil
}

// Protocol details remain available on disk. The main stream receives their
// warnings and errors, alongside ordinary application INFO events.
type diagnosticLogWriter struct{ file io.Writer }

func (w diagnosticLogWriter) Write(data []byte) (int, error) {
	if _, err := w.file.Write(data); err != nil {
		return 0, err
	}
	if bytes.HasPrefix(data, []byte("ERROR ")) || bytes.HasPrefix(data, []byte("WARNING ")) {
		if _, err := os.Stderr.Write(data); err != nil {
			return 0, err
		}
	}
	return len(data), nil
}

func (s *calendarServer) progressSnapshot() researchProgress {
	p := researchProgress{At: time.Now().UTC(), Active: []map[string]any{}}
	unique := map[string]bool{}
	s.cache.mu.Lock()
	for _, entry := range s.cache.entries {
		if entry.Calendar == nil {
			continue
		}
		for _, e := range entry.Calendar.Events {
			start, err := time.Parse(time.RFC3339, e.StartTime)
			if err == nil && start.After(p.At) && start.Before(p.At.Add(statsWarmupHorizon)) {
				id, _, book := extractIDAndSlug(e.URL)
				unique[book+":"+id] = true
			}
		}
	}
	s.cache.mu.Unlock()
	p.CalendarOffers = len(unique)
	qualified := map[string]bool{}
	offers := s.calendarOffers()
	for _, candidate := range offers.ByBook["STS"] {
		if !unique["STS:"+candidate.Event.ID] {
			continue
		}
		matches, err := matchCalendarOffers(candidate.Event, offers)
		if err != nil {
			continue
		}
		if !s.automaticLeagueEnabled(matches) {
			continue
		}
		if _, complete := s.bundleOddsVersion(matches, p.At); !complete {
			continue
		}
		for _, book := range serverBookmakers {
			qualified[book+":"+matches[book].ID] = true
		}
	}
	p.MarketsAvailable = len(qualified)
	s.prefetchMu.Lock()
	for key, state := range s.prefetch.Events {
		if !unique[key] {
			continue
		}
		if state.Status == "error" {
			p.Errors++
		}
		if strings.HasPrefix(state.Status, "sofa-") || state.Status == "partial" {
			p.StatisticsMissing++
		}
	}
	s.prefetchMu.Unlock()
	if s.research != nil {
		items, _ := s.research.summaries()
		for _, item := range items {
			if qualified[item.Bookmaker+":"+item.EventID] && (item.Status == "completed" || item.Status == "monitoring") {
				p.AnalysesReady++
			}
		}
		_ = s.research.db.QueryRow(`SELECT COUNT(*) FROM fixture_results WHERE next_check>0 AND start_at<=?`, p.At.Unix()).Scan(&p.SettlementsWaiting)
		_ = s.research.db.QueryRow(`SELECT COUNT(DISTINCT fixture_id) FROM settlements WHERE status='unresolved'`).Scan(&p.SettlementsMissing)
	}
	p.AnalysesWaiting = max(0, p.MarketsAvailable-p.AnalysesReady)
	s.analysisMu.Lock()
	for _, task := range s.jobs {
		job := task.snapshot()
		if job.Status == "running" || job.Status == "queued" {
			p.Active = append(p.Active, map[string]any{"id": job.ID, "match": job.Match, "origin": job.Origin, "stage": job.Stage, "playersDone": job.PlayersDone, "playersTotal": job.PlayersTotal})
		}
	}
	s.analysisMu.Unlock()
	return p
}

func (s *calendarServer) registerResearchAPI(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/research/matchup/{id}", s.opponentSample)
	mux.HandleFunc("GET /api/v1/research/matchup/{id}", s.opponentSample)
	mux.HandleFunc("GET /api/v1/research/runs/{id}", s.automaticRun)
	mux.HandleFunc("POST /api/v1/research/predictions/{id}/history", s.extendedPredictionHistory)
	mux.HandleFunc("GET /api/v1/research/predictions/{id}/history", s.extendedPredictionHistory)
	mux.HandleFunc("GET /api/v1/research/summaries", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		items, err := s.researchSummaries(ctx)
		if err != nil {
			log.Printf("ERROR Odczyt podsumowań analiz: %v", err)
			apiError(w, 500, err)
			return
		}
		jsonResponse(w, 200, items)
	})
	mux.HandleFunc("GET /api/v1/research/progress", func(w http.ResponseWriter, r *http.Request) { jsonResponse(w, 200, s.progressSnapshot()) })
	mux.HandleFunc("GET /api/v1/research/candidates", func(w http.ResponseWriter, r *http.Request) {
		rows, err := s.research.db.Query(`SELECT p.payload,COALESCE(q.payload,'') FROM predictions p JOIN current_prediction_runs r ON r.id=p.run_id LEFT JOIN automatic_price_checks q ON q.run_id=r.id WHERE r.algorithm=? AND r.start_at>? AND r.start_at<=? ORDER BY r.start_at,p.bet_key,r.bookmaker`, fairModelVersion, time.Now().Unix(), time.Now().Add(statsWarmupHorizon).Unix())
		if err != nil {
			apiError(w, 500, err)
			return
		}
		defer rows.Close()
		items := []predictionRecord{}
		quotes := map[string]*automaticPriceCheck{}
		for rows.Next() {
			var raw, quoteRaw string
			var p predictionRecord
			if err := rows.Scan(&raw, &quoteRaw); err != nil {
				apiError(w, 500, err)
				return
			}
			if err := json.Unmarshal([]byte(raw), &p); err != nil {
				apiError(w, 500, err)
				return
			}
			if quoteRaw != "" {
				check := quotes[p.RunID]
				if check == nil {
					check = &automaticPriceCheck{}
					if err := json.Unmarshal([]byte(quoteRaw), check); err != nil {
						apiError(w, 500, err)
						return
					}
					quotes[p.RunID] = check
				}
				found := false
				for _, bet := range check.Bets {
					if automaticBetKey(bet) == automaticBetKey(p.Bet) {
						p.Bet, found = bet, true
						break
					}
				}
				if !found {
					continue
				}
				p.ObservedAt, p.ExpiresAt = check.CheckedAt, check.ExpiresAt
			}
			if p.Bet.Verdict != "VALUE" || !time.Now().Before(p.ExpiresAt) {
				continue
			}
			items = append(items, p)
		}
		if err := rows.Err(); err != nil {
			apiError(w, 500, err)
			return
		}
		jsonResponse(w, 200, items)
	})
	mux.HandleFunc("GET /api/v1/research/predictions/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(id) {
			apiError(w, 400, fmt.Errorf("niepoprawne ID prognozy"))
			return
		}
		p, err := s.research.prediction(id)
		if errors.Is(err, sql.ErrNoRows) {
			apiError(w, 404, err)
			return
		}
		if err != nil {
			apiError(w, 500, err)
			return
		}
		var evidence predictionEvidence
		if err := s.research.readDocument(p.EvidencePath, &evidence); err != nil {
			apiError(w, 500, err)
			return
		}
		rows, err := s.research.db.Query(`SELECT payload FROM settlements WHERE prediction_id=? ORDER BY rules`, id)
		if err != nil {
			apiError(w, 500, err)
			return
		}
		outcomes := []json.RawMessage{}
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				rows.Close()
				apiError(w, 500, err)
				return
			}
			outcomes = append(outcomes, json.RawMessage(raw))
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			apiError(w, 500, err)
			return
		}
		var finalData *postMatchData
		var finalPath string
		if err := s.research.db.QueryRow(`SELECT data_path FROM fixture_results WHERE fixture_id=?`, p.FixtureID).Scan(&finalPath); err != nil && !errors.Is(err, sql.ErrNoRows) {
			apiError(w, 500, err)
			return
		}
		if finalPath != "" {
			finalData = &postMatchData{}
			if err := s.research.readDocument(finalPath, finalData); err != nil {
				apiError(w, 500, err)
				return
			}
		}
		var currentQuote map[string]any
		if p.Origin == "automatic" {
			check, err := s.research.priceCheck(p.RunID)
			if err != nil {
				apiError(w, 500, err)
				return
			}
			if check != nil {
				currentQuote = map[string]any{"available": false, "observedAt": check.CheckedAt, "expiresAt": check.ExpiresAt}
				for _, bet := range check.Bets {
					if automaticBetKey(bet) == automaticBetKey(p.Bet) {
						currentQuote["available"], currentQuote["bet"] = true, bet
						break
					}
				}
			}
		}
		jsonResponse(w, 200, map[string]any{"prediction": p, "evidence": evidence, "currentQuote": currentQuote, "outcomes": outcomes, "postMatch": finalData})
	})
	mux.HandleFunc("GET /api/v1/research/reports", func(w http.ResponseWriter, r *http.Request) {
		days := 30
		if raw := r.URL.Query().Get("days"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > 365 {
				apiError(w, 400, fmt.Errorf("days: 1–365"))
				return
			}
			days = n
		}
		multiplier := .88
		if raw := r.URL.Query().Get("payout"); raw != "" {
			n, err := strconv.ParseFloat(raw, 64)
			if err != nil || math.IsNaN(n) || n <= 0 || n > 1 {
				apiError(w, 400, fmt.Errorf("payout: >0 i <=1"))
				return
			}
			multiplier = n
		}
		rules := r.URL.Query().Get("rules")
		if rules == "" {
			rules = "standard-v1"
		}
		if rules != "standard-v1" && rules != "supersub-v1" {
			apiError(w, 400, fmt.Errorf("nieznane zasady"))
			return
		}
		report, err := s.evaluationReport(days, multiplier, rules)
		if err != nil {
			apiError(w, 500, err)
			return
		}
		jsonResponse(w, 200, report)
	})
	mux.HandleFunc("GET /api/v1/research/export", s.exportResearch)
}

func (s *calendarServer) exportResearch(w http.ResponseWriter, r *http.Request) {
	kind := r.URL.Query().Get("kind")
	days := 30
	if raw := r.URL.Query().Get("days"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 3650 {
			apiError(w, 400, fmt.Errorf("days: 1–3650"))
			return
		}
		days = n
	}
	queries := map[string]string{
		"predictions": `SELECT p.payload FROM predictions p JOIN prediction_runs r ON r.id=p.run_id WHERE r.analyzed_at>=? ORDER BY r.analyzed_at,p.id`,
		"outcomes":    `SELECT v.payload FROM settlement_versions v JOIN predictions p ON p.id=v.prediction_id JOIN prediction_runs r ON r.id=p.run_id WHERE r.analyzed_at>=? ORDER BY r.analyzed_at,v.prediction_id,v.rules,v.revision`,
		"inputs":      `SELECT DISTINCT evidence_path FROM prediction_runs WHERE analyzed_at>=? ORDER BY evidence_path`,
	}
	query, ok := queries[kind]
	if !ok {
		apiError(w, 400, fmt.Errorf("kind: predictions, outcomes lub inputs"))
		return
	}
	rows, err := s.research.db.Query(query, time.Now().AddDate(0, 0, -days).Unix())
	if err != nil {
		apiError(w, 500, err)
		return
	}
	defer rows.Close()
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s_%s.jsonl"`, kind, time.Now().UTC().Format("2006-01-02")))
	encoder := json.NewEncoder(w)
	for rows.Next() {
		var raw string
		if err = rows.Scan(&raw); err != nil {
			break
		}
		if kind == "inputs" {
			var evidence predictionEvidence
			if err = s.research.readDocument(raw, &evidence); err != nil {
				break
			}
			err = encoder.Encode(map[string]any{"schemaVersion": researchSchema, "evidenceId": researchHash(evidence), "evidence": evidence})
		} else {
			_, err = io.WriteString(w, raw+"\n")
		}
		if err != nil {
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	if err != nil {
		log.Printf("[EXPORT] stage=failed kind=%s error=%v", kind, err)
	} else {
		log.Printf("[EXPORT] stage=completed kind=%s days=%d", kind, days)
	}
}

// === Existing collectors, protocol parsers and cache ===

// === betclic.go ===
const BetclicMatchService = "https://offering.begmedia.com/web/offering.access.api/offering.access.api.MatchService/"

const maxGRPCFrame = 32 << 20

// ProtoUint and ProtoString build the same protobuf fields used by the desktop client.
func ProtoUint(field, value uint64) []byte {
	return appendVarint(appendVarint(nil, field<<3), value)
}

func ProtoString(field uint64, value string) []byte {
	b := appendVarint(nil, field<<3|2)
	b = appendVarint(b, uint64(len(value)))
	return append(b, value...)
}

func appendVarint(b []byte, value uint64) []byte {
	for value >= 128 {
		b = append(b, byte(value)|0x80)
		value >>= 7
	}
	return append(b, byte(value))
}

// FetchBetclicRPC reuses the desktop gRPC-Web request headers for any MatchService
// method. body is the protobuf message without the five-byte gRPC-Web envelope.
// Notification streams are read one complete frame at a time. A short idle period
// after complete data frames ends the snapshot, but never accepts a partial frame.
func FetchBetclicRPC(parent context.Context, client *http.Client, endpoint string, body []byte) (snapshot []byte, err error) {
	parent = betclicTrace(parent, "rpc")
	info := archiveInfoFrom(parent)
	info.Endpoint, info.Method, info.RequestHex = endpoint, http.MethodPost, fmt.Sprintf("%x", body)
	parent = context.WithValue(parent, archiveContextKey{}, info)
	started := time.Now()
	betclicDebugf(parent, "RPC_START endpoint=%q client_timeout=%s rpc_timeout=25s request_bytes=%d protobuf_hex=%s", endpoint, client.Timeout, len(body), betclicPreview(body))
	defer func() {
		betclicDebugf(parent, "RPC_END duration=%s snapshot_bytes=%d error=%v", time.Since(started), len(snapshot), err)
		if saveErr := archiveResponse(parent, "podsumowanie-rpc", nil, "", map[string]any{"duration": time.Since(started).String(), "snapshotBytes": len(snapshot)}, err); saveErr != nil {
			err = errors.Join(err, saveErr)
		}
	}()
	ctx, cancel := context.WithTimeout(parent, 25*time.Second)
	defer cancel()
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(append(header, body...)))
	if err != nil {
		return nil, err
	}
	for key, value := range map[string]string{
		"Accept": "*/*", "Accept-Language": "pl-PL", "Appversion": "10.5.0-1",
		"Content-Type": "application/grpc-web+proto", "Origin": "https://www.betclic.pl",
		"Referer": "https://www.betclic.pl/", "Sec-Fetch-Dest": "empty",
		"Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "cross-site",
		"User-Agent":     "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
		"X-Bg-Ref-Brand": "BETCLIC", "X-Bg-Ref-Platform": "DESKTOP",
		"X-Bg-Ref-Regulator-Zone": "PL", "X-Bg-Regulation": "PL", "X-Grpc-Web": "1",
	} {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Betclic połączenie: %w", err)
	}
	defer resp.Body.Close()
	info.Status, info.ContentType = resp.StatusCode, resp.Header.Get("Content-Type")
	parent = context.WithValue(parent, archiveContextKey{}, info)
	ctx = context.WithValue(ctx, archiveContextKey{}, info)
	betclicDebugf(ctx, "HTTP_RESPONSE elapsed=%s status=%d protocol=%q content_type=%q content_length=%d grpc_status=%q grpc_message=%q", time.Since(started), resp.StatusCode, resp.Proto, resp.Header.Get("Content-Type"), resp.ContentLength, resp.Header.Get("Grpc-Status"), resp.Header.Get("Grpc-Message"))
	responseErr := grpcStatusError(resp.Header)
	if resp.StatusCode != http.StatusOK {
		responseErr = fmt.Errorf("Betclic HTTP %d", resp.StatusCode)
	} else if contentType := resp.Header.Get("Content-Type"); !strings.HasPrefix(contentType, "application/grpc-web") || strings.Contains(contentType, "grpc-web-text") {
		responseErr = fmt.Errorf("Betclic nieobsługiwany Content-Type: %q", contentType)
	}
	if responseErr != nil {
		raw, readErr := readBounded(resp.Body, maxGRPCFrame)
		if saveErr := archiveResponse(ctx, "bledna-odpowiedz-http", raw, "bin", string(raw), errors.Join(responseErr, readErr)); saveErr != nil {
			return nil, errors.Join(responseErr, saveErr)
		}
		return nil, errors.Join(responseErr, readErr)
	}
	return readBetclicSnapshot(ctx, resp.Body, 2500*time.Millisecond)
}

func grpcStatusError(headers http.Header) error {
	status := headers.Get("Grpc-Status")
	if status == "" || status == "0" {
		return nil
	}
	message := headers.Get("Grpc-Message")
	if decoded, err := url.PathUnescape(message); err == nil {
		message = decoded
	}
	return fmt.Errorf("Betclic gRPC status %s: %s", status, message)
}

func readBetclicSnapshot(ctx context.Context, reader io.ReadCloser, idle time.Duration) ([]byte, error) {
	type result struct {
		frame []byte
		err   error
	}
	frames := make(chan result, 1)
	var inFrame atomic.Bool
	var archiving atomic.Bool
	readerCtx, stop := context.WithCancel(ctx)
	readerDone := make(chan struct{})
	defer func() {
		stop()
		_ = reader.Close() // interrupts a pending read on timeout or cancellation
		<-readerDone       // complete archival of already received bytes before returning
	}()
	go func() {
		defer close(readerDone)
		for {
			header := make([]byte, 5)
			headerBytes, err := io.ReadFull(reader, header[:1])
			if err == nil {
				inFrame.Store(true)
				var count int
				count, err = io.ReadFull(reader, header[1:])
				headerBytes += count
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
			}
			var frame []byte
			if err == nil {
				size := binary.BigEndian.Uint32(header[1:])
				betclicDebugf(ctx, "FRAME_HEADER flags=0x%02x payload_bytes=%d", header[0], size)
				if size > maxGRPCFrame {
					err = fmt.Errorf("zbyt duża ramka gRPC: %d", size)
				} else {
					frame = make([]byte, 5+int(size))
					copy(frame, header)
					var count int
					count, err = io.ReadFull(reader, frame[5:])
					frame = frame[:5+count]
					if err == io.EOF {
						err = io.ErrUnexpectedEOF
					}
				}
			}
			if frame == nil && headerBytes > 0 {
				frame = header[:headerBytes]
			}
			archiving.Store(true)
			inFrame.Store(false)
			if len(frame) > 0 {
				if saveErr := archiveResponse(ctx, "ramka-grpc", frame, "grpc.bin", readableGRPC(frame), err); saveErr != nil {
					err = errors.Join(err, saveErr)
				}
			}
			select {
			case frames <- result{frame, err}:
				archiving.Store(false)
			case <-readerCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var snapshot []byte
	for {
		// No short idle deadline before the first frame.
		var timer *time.Timer
		var timeout <-chan time.Time
		if len(snapshot) > 0 {
			timer = time.NewTimer(idle)
			timeout = timer.C
		}
		select {
		case <-ctx.Done():
			betclicDebugf(ctx, "STREAM_CONTEXT_END received_bytes=%d in_frame=%t error=%v", len(snapshot), inFrame.Load(), ctx.Err())
			if timer != nil {
				timer.Stop()
			}
			return nil, ctx.Err()
		case <-timeout:
			if archiving.Load() || len(frames) > 0 {
				continue // disk compression is not network idleness
			}
			betclicDebugf(ctx, "STREAM_IDLE idle=%s received_bytes=%d in_frame=%t", idle, len(snapshot), inFrame.Load())
			if inFrame.Load() {
				return nil, fmt.Errorf("Betclic przerwana ramka gRPC: %w", io.ErrUnexpectedEOF)
			}
			return snapshot, nil
		case next := <-frames:
			if timer != nil {
				timer.Stop()
			}
			if next.err != nil {
				if next.err == io.EOF && len(snapshot) > 0 {
					betclicDebugf(ctx, "STREAM_COMPLETE received_bytes=%d", len(snapshot))
					return snapshot, nil
				}
				betclicDebugf(ctx, "STREAM_READ_END received_bytes=%d error=%v", len(snapshot), next.err)
				return nil, fmt.Errorf("Betclic odczyt strumienia: %w", next.err)
			}
			flags := next.frame[0]
			betclicDebugf(ctx, "FRAME_COMPLETE flags=0x%02x frame_bytes=%d received_bytes=%d", flags, len(next.frame), len(snapshot))
			if flags == 0x80 {
				trailers := http.Header{}
				for _, line := range strings.Split(string(next.frame[5:]), "\r\n") {
					if key, value, ok := strings.Cut(line, ":"); ok {
						trailers.Set(strings.TrimSpace(key), strings.TrimSpace(value))
					}
				}
				if err := grpcStatusError(trailers); err != nil {
					betclicDebugf(ctx, "GRPC_TRAILERS status=%q message=%q", trailers.Get("Grpc-Status"), trailers.Get("Grpc-Message"))
					return nil, err
				}
				betclicDebugf(ctx, "GRPC_TRAILERS status=%q message=%q", trailers.Get("Grpc-Status"), trailers.Get("Grpc-Message"))
				if len(snapshot) == 0 {
					return nil, fmt.Errorf("Betclic nie zwrócił danych")
				}
				return snapshot, nil
			}
			if flags != 0 {
				return nil, fmt.Errorf("Betclic nieobsługiwane flagi ramki: %d", flags)
			}
			if len(snapshot)+len(next.frame) > maxGRPCFrame {
				return nil, fmt.Errorf("Betclic snapshot przekracza limit rozmiaru")
			}
			snapshot = append(snapshot, next.frame...)
		}
	}
}

// === betclic_debug.go ===
type betclicTraceKey struct{}

var betclicDebug struct {
	once   sync.Once
	logger *log.Logger
	path   string
	seq    atomic.Uint64
	dumpMu sync.Mutex
}

func initBetclicDebug() {
	betclicDebug.once.Do(func() {
		// Use the working directory so desktop and CLI diagnostics are easy to find.
		dir, err := os.Getwd()
		if err != nil {
			dir = "."
		}
		betclicDebug.path = filepath.Join(dir, "logs", "betclic-debug.log")
		betclicDebug.logger = log.New(readableLogWriter{output: os.Stderr}, "", 0)
		if err := os.MkdirAll(filepath.Dir(betclicDebug.path), 0755); err != nil {
			betclicDebug.logger.Printf("Nie można utworzyć katalogu logów: %v", err)
			return
		}
		file, err := os.OpenFile(betclicDebug.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			betclicDebug.logger.Printf("Nie można otworzyć logu %q: %v", betclicDebug.path, err)
			return
		}
		// Kept open for the process lifetime; log.Logger serializes concurrent writes.
		betclicDebug.logger.SetOutput(readableLogWriter{output: diagnosticLogWriter{file: file}})
		log.Printf("INFO Szczegółowa diagnostyka Betclic: %s", betclicDebug.path)
	})
}

// BetclicDebugLogPath returns the diagnostic file location, including for GUI errors.
func BetclicDebugLogPath() string {
	initBetclicDebug()
	return betclicDebug.path
}

func betclicTrace(ctx context.Context, stage string) context.Context {
	initBetclicDebug()
	parent, _ := ctx.Value(betclicTraceKey{}).(string)
	id := fmt.Sprintf("%s-%d", stage, betclicDebug.seq.Add(1))
	if parent != "" {
		id = parent + "/" + id
	}
	return context.WithValue(ctx, betclicTraceKey{}, id)
}

func betclicDebugf(ctx context.Context, format string, args ...any) {
	initBetclicDebug()
	betclicDebug.logger.Printf(format, args...)
}

func betclicPreview(raw []byte) string {
	const limit = 256
	if len(raw) > limit {
		return fmt.Sprintf("%x... (%d bytes total)", raw[:limit], len(raw))
	}
	return fmt.Sprintf("%x", raw)
}

func betclicProtoSummary(fields []protoField) string {
	var summary []string
	for i, field := range fields {
		if i == 30 {
			summary = append(summary, "...")
			break
		}
		if field.Wire == 0 {
			summary = append(summary, fmt.Sprintf("%d:varint=%d", field.Number, field.Uint))
		} else {
			summary = append(summary, fmt.Sprintf("%d:wire=%d,len=%d", field.Number, field.Wire, len(field.Bytes)))
		}
	}
	return strings.Join(summary, " ")
}

func betclicSaveParserFailure(ctx context.Context, raw []byte) {
	betclicSaveSnapshot(ctx, "betclic-last-parser-error.bin", raw)
}

func betclicSaveSnapshot(ctx context.Context, filename string, raw []byte) {
	initBetclicDebug()
	// Fixed diagnostic filenames keep only the latest response of each kind.
	path := filepath.Join(filepath.Dir(betclicDebug.path), filename)
	betclicDebug.dumpMu.Lock()
	err := os.WriteFile(path, raw, 0600)
	betclicDebug.dumpMu.Unlock()
	betclicDebugf(ctx, "SNAPSHOT_DUMP path=%q bytes=%d error=%v", path, len(raw), err)
}

// === betclic_calendar.go ===
type Calendar struct {
	Date      string          `json:"date"`
	Events    []CalendarEvent `json:"events"`
	Warnings  []string        `json:"warnings"`
	FetchedAt string          `json:"fetchedAt,omitempty"`
}

type betclicPage struct {
	Offset int
	Events []CalendarEvent
	Total  int
	IDs    []string
}

// The request and field numbers below were checked against live responses from
// GetMatchesBySportWithNotifications: sport_code=football, language=pl, offset=4.
// The service returns 40 matches per page by default. Never treat the first page
// as the complete calendar; field 5 of the response holds the total count.
func BetclicCalendarRequest(offset int) []byte {
	body := append(ProtoString(1, "football"), ProtoString(2, "pl")...)
	if offset > 0 {
		body = append(body, ProtoUint(4, uint64(offset))...)
	}
	return body
}

func FetchBetclicCalendar(parent context.Context, client *http.Client, endpoint, date string) (*Calendar, error) {
	if _, _, err := calendarDay(date); err != nil {
		return nil, err
	}
	full, err := FetchBetclicFullCalendar(parent, client, endpoint)
	if err != nil {
		return nil, err
	}
	return FilterCalendar(full, date)
}

// FetchBetclicFullCalendar keeps every date from the paginated source snapshot.
func FetchBetclicFullCalendar(parent context.Context, client *http.Client, endpoint string) (calendar *Calendar, err error) {
	parent = betclicTrace(parent, "calendar")
	started := time.Now()
	betclicDebugf(parent, "CALENDAR_START scope=full_catalog timeout=2m")
	defer func() {
		if calendar != nil {
			betclicDebugf(parent, "CALENDAR_END duration=%s events=%d warnings=%q error=%v", time.Since(started), len(calendar.Events), calendar.Warnings, err)
		} else {
			betclicDebugf(parent, "CALENDAR_END duration=%s error=%v", time.Since(started), err)
		}
	}()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	loadPage := func(offset int) (betclicPage, error) {
		pageCtx := betclicTrace(ctx, fmt.Sprintf("page-offset-%d", offset))
		info := archiveInfoFrom(pageCtx)
		info.Scope += fmt.Sprintf("_strona-offset-%d", offset)
		pageCtx = context.WithValue(pageCtx, archiveContextKey{}, info)
		betclicDebugf(pageCtx, "PAGE_START offset=%d", offset)
		raw, err := FetchBetclicRPC(pageCtx, client, endpoint, BetclicCalendarRequest(offset))
		if err != nil {
			betclicDebugf(pageCtx, "PAGE_FETCH_ERROR offset=%d error=%v", offset, err)
			return betclicPage{}, err
		}
		betclicDebugf(pageCtx, "PAGE_SNAPSHOT bytes=%d preview_hex=%s", len(raw), betclicPreview(raw))
		page, err := parseBetclicCalendarPageWithContext(pageCtx, raw)
		if err != nil {
			betclicDebugf(pageCtx, "PAGE_PARSE_ERROR offset=%d error=%v", offset, err)
			betclicSaveParserFailure(pageCtx, raw)
			return betclicPage{}, fmt.Errorf("Betclic parser strony od %d: %w", offset, err)
		}
		page.Offset = offset
		if err := archiveResponse(pageCtx, "odczytane-mecze-strony", nil, "", page, nil); err != nil {
			return betclicPage{}, err
		}
		betclicDebugf(pageCtx, "PAGE_END offset=%d ids=%d football_matches=%d total=%d", offset, len(page.IDs), len(page.Events), page.Total)
		if len(page.IDs) == 0 && offset < page.Total {
			betclicDebugf(pageCtx, "PAGE_EMPTY_BEFORE_TOTAL offset=%d declared_total=%d", offset, page.Total)
			betclicSaveSnapshot(pageCtx, "betclic-last-empty-page.bin", raw)
		}
		if len(page.Events) > 0 {
			betclicDebugf(pageCtx, "PAGE_DATES first=%s last=%s", page.Events[0].StartTime, page.Events[len(page.Events)-1].StartTime)
		}
		return page, nil
	}
	first, err := loadPage(0)
	if err != nil {
		return nil, err
	}
	if first.Total > 5120 {
		return nil, fmt.Errorf("Betclic oferta przekracza limit 5120 meczów: %d", first.Total)
	}
	if first.Total > len(first.IDs) && len(first.IDs) == 0 {
		return nil, fmt.Errorf("Betclic pusta pierwsza strona niepełnej oferty")
	}
	pages := []betclicPage{first}
	pageSize := len(first.IDs)
	betclicDebugf(ctx, "PAGINATION page_size=%d total=%d parallel_readers=2", pageSize, first.Total)
	// Two readers bound source load and keep a calendar scan below the timeout.
	for offset := pageSize; pageSize > 0 && offset < first.Total; offset += 2 * pageSize {
		var results [2]betclicPage
		var errs [2]error
		count := 1
		if offset+pageSize < first.Total {
			count = 2
		}
		var wg sync.WaitGroup
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(i int) { defer wg.Done(); results[i], errs[i] = loadPage(offset + i*pageSize) }(i)
		}
		wg.Wait()
		for i := 0; i < count; i++ {
			if errs[i] != nil {
				return nil, fmt.Errorf("Betclic strona od %d: %w", offset+i*pageSize, errs[i])
			}
			pages = append(pages, results[i])
		}
	}
	byID := map[string]CalendarEvent{}
	allIDs := map[string]int{}
	records, duplicates := 0, 0
	var emptyOffsets []int
	changed := false
	for _, page := range pages {
		if page.Total != first.Total {
			changed = true
		}
		if len(page.IDs) == 0 {
			emptyOffsets = append(emptyOffsets, page.Offset)
		}
		records += len(page.IDs)
		for _, id := range page.IDs {
			if firstOffset, exists := allIDs[id]; exists {
				duplicates++
				betclicDebugf(ctx, "DUPLICATE_ID id=%s first_offset=%d repeated_offset=%d", id, firstOffset, page.Offset)
			} else {
				allIDs[id] = page.Offset
			}
		}
		for _, event := range page.Events {
			byID[event.ID] = event
		}
	}
	result := &Calendar{Events: []CalendarEvent{}, Warnings: []string{}}
	betclicDebugf(ctx, "CATALOG pages=%d records=%d duplicates=%d unique_ids=%d football_matches=%d without_two_teams=%d declared_total=%d total_changed=%t empty_offsets=%v", len(pages), records, duplicates, len(allIDs), len(byID), len(allIDs)-len(byID), first.Total, changed, emptyOffsets)
	if changed || len(allIDs) != first.Total {
		warning := fmt.Sprintf("Betclic zwrócił %d rekordów (%d unikalnych ID, %d powtórzeń), a początkowo deklarował %d wydarzeń.", records, len(allIDs), duplicates, first.Total)
		if len(emptyOffsets) > 0 {
			warning += fmt.Sprintf(" Puste strony od pozycji: %v.", emptyOffsets)
		}
		if changed {
			warning += " Licznik wydarzeń zmieniał się podczas pobierania."
		}
		warning += " Nie można potwierdzić kompletności kalendarza."
		result.Warnings = append(result.Warnings, warning)
	}
	for _, event := range byID {
		result.Events = append(result.Events, event)
	}
	sort.Slice(result.Events, func(i, j int) bool {
		left, _ := time.Parse(time.RFC3339, result.Events[i].StartTime)
		right, _ := time.Parse(time.RFC3339, result.Events[j].StartTime)
		if !left.Equal(right) {
			return left.Before(right)
		}
		return result.Events[i].ID < result.Events[j].ID
	})
	return result, nil
}

// Only known message levels are decoded: frame -> response[1] -> matches[3].
// No recursive string guessing, and no IDs from recommendations or markets.
func parseBetclicCalendarPage(raw []byte) (betclicPage, error) {
	return parseBetclicCalendarPageWithContext(context.Background(), raw)
}

func parseBetclicCalendarPageWithContext(ctx context.Context, raw []byte) (betclicPage, error) {
	frameIndex := 0
	for len(raw) > 0 {
		frameIndex++
		if len(raw) < 5 {
			return betclicPage{}, fmt.Errorf("Betclic niepełny nagłówek ramki")
		}
		size := int(binary.BigEndian.Uint32(raw[1:5]))
		if size > len(raw)-5 || raw[0] != 0 {
			return betclicPage{}, fmt.Errorf("Betclic niepoprawna ramka kalendarza")
		}
		fields, err := decodeProtoFields(raw[5 : 5+size])
		if err != nil {
			return betclicPage{}, fmt.Errorf("ramka %d, pola główne: %w", frameIndex, err)
		}
		betclicDebugf(ctx, "PROTO_ROOT frame=%d fields=%s", frameIndex, betclicProtoSummary(fields))
		raw = raw[5+size:]
		for _, root := range fields {
			if root.Number != 1 || root.Wire != 2 {
				continue
			}
			response, err := decodeProtoFields(root.Bytes)
			if err != nil {
				return betclicPage{}, fmt.Errorf("ramka %d, odpowiedź: %w", frameIndex, err)
			}
			betclicDebugf(ctx, "PROTO_RESPONSE frame=%d sport=%q field4=%q fields=%s", frameIndex, protoText(response, 1), protoText(response, 4), betclicProtoSummary(response))
			// Empty tail pages omit the sport label but retain the market code.
			if protoText(response, 1) == "" {
				betclicDebugf(ctx, "PROTO_SKIP_RESPONSE reason=empty_field_1 frame=%d", frameIndex)
				continue
			}
			page := betclicPage{Events: []CalendarEvent{}, Total: int(protoNumber(response, 5))}
			for _, record := range response {
				if record.Number != 3 || record.Wire != 2 {
					continue
				}
				match, err := decodeProtoFields(record.Bytes)
				if err != nil {
					return betclicPage{}, fmt.Errorf("rekord meczu %d: %w", len(page.IDs)+1, err)
				}
				id := protoNumber(match, 1)
				when, err := time.Parse(time.RFC3339, protoText(match, 3))
				if err != nil || id == 0 {
					betclicDebugf(ctx, "PROTO_INVALID_MATCH id=%d start=%q fields=%s date_error=%v", id, protoText(match, 3), betclicProtoSummary(match), err)
					return betclicPage{}, fmt.Errorf("Betclic niepoprawne ID lub termin meczu: id=%d start=%q", id, protoText(match, 3))
				}
				page.IDs = append(page.IDs, strconv.FormatUint(id, 10))
				teams := []string{}
				competition, country := "", ""
				for _, field := range match {
					if field.Wire != 2 {
						continue
					}
					if field.Number == 12 {
						team, err := decodeProtoFields(field.Bytes)
						if err != nil {
							return betclicPage{}, fmt.Errorf("mecz %d, drużyna: %w", id, err)
						}
						teams = append(teams, strings.TrimSpace(protoText(team, 3)))
					}
					if field.Number == 8 {
						league, err := decodeProtoFields(field.Bytes)
						if err != nil {
							return betclicPage{}, fmt.Errorf("mecz %d, liga: %w", id, err)
						}
						competition, country = protoText(league, 2), protoText(league, 5)
					}
				}
				// Outrights do not have two contestants; they are not football matches.
				if len(teams) != 2 {
					betclicDebugf(ctx, "PROTO_SKIP_MATCH id=%d reason=contestant_count contestants=%d", id, len(teams))
					continue
				}
				if teams[0] == "" || teams[1] == "" {
					return betclicPage{}, fmt.Errorf("Betclic brak nazwy drużyny: id=%d teams=%q", id, teams)
				}
				matchID := strconv.FormatUint(id, 10)
				page.Events = append(page.Events, CalendarEvent{ID: matchID, Home: teams[0], Away: teams[1],
					Competition: competition, Country: country, StartTime: when.UTC().Format(time.RFC3339),
					Live: protoNumber(match, 4) == 1,
					URL:  "https://www.betclic.pl/pilka-nozna-s1/" + url.PathEscape(teams[0]+"-"+teams[1]) + "-m" + matchID,
				})
			}
			return page, nil
		}
	}
	return betclicPage{}, fmt.Errorf("Betclic brak początkowego snapshotu kalendarza")
}

type protoField struct {
	Number int
	Wire   byte
	Uint   uint64
	Bytes  []byte
}

func decodeProtoFields(raw []byte) ([]protoField, error) {
	fields := []protoField{}
	for len(raw) > 0 {
		tag, n := binary.Uvarint(raw)
		if n <= 0 || tag>>3 == 0 || tag>>3 > 0x1fffffff {
			return nil, fmt.Errorf("Betclic niepoprawny tag Protobuf")
		}
		raw = raw[n:]
		field := protoField{Number: int(tag >> 3), Wire: byte(tag & 7)}
		switch field.Wire {
		case 0:
			value, n := binary.Uvarint(raw)
			if n <= 0 {
				return nil, fmt.Errorf("Betclic niepełny varint")
			}
			field.Uint = value
			raw = raw[n:]
		case 1, 5:
			size := 8
			if field.Wire == 5 {
				size = 4
			}
			if len(raw) < size {
				return nil, fmt.Errorf("Betclic niepełne pole fixed")
			}
			field.Bytes = raw[:size]
			raw = raw[size:]
		case 2:
			size, n := binary.Uvarint(raw)
			if n <= 0 || size > uint64(len(raw)-n) {
				return nil, fmt.Errorf("Betclic niepełne pole bytes")
			}
			raw = raw[n:]
			field.Bytes = raw[:int(size)]
			raw = raw[int(size):]
		default:
			return nil, fmt.Errorf("Betclic nieobsługiwany wire type %d", field.Wire)
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func protoText(fields []protoField, number int) string {
	for _, field := range fields {
		if field.Number == number && field.Wire == 2 {
			return string(field.Bytes)
		}
	}
	return ""
}

func protoNumber(fields []protoField, number int) uint64 {
	for _, field := range fields {
		if field.Number == number && field.Wire == 0 {
			return field.Uint
		}
	}
	return 0
}

// === calendar_cache.go ===
const CalendarCacheTTL = 12 * time.Hour
const calendarCacheVersion = 1

type calendarCacheEntry struct {
	FetchedAt time.Time `json:"fetchedAt"`
	Calendar  *Calendar `json:"calendar"`
}

type calendarFlight struct {
	done     chan struct{}
	calendar *Calendar
	err      error
}

// CalendarCache persists complete source responses and shares concurrent fetches.
// STS/Betclic keys cover every offered date; Superbet keys cover complete days.
type CalendarCache struct {
	path     string
	once     sync.Once
	mu       sync.Mutex
	entries  map[string]calendarCacheEntry
	inflight map[string]*calendarFlight
	active   sync.WaitGroup
	closing  bool
	revision uint64
}

func NewCalendarCache(path string) *CalendarCache {
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	return &CalendarCache{path: path, entries: map[string]calendarCacheEntry{}, inflight: map[string]*calendarFlight{}}
}

var defaultCalendarCache = NewCalendarCache("bookmaker_calendar_cache.json")

func DefaultCalendarCache() *CalendarCache { return defaultCalendarCache }

func cloneCalendar(source *Calendar) *Calendar {
	result := *source
	result.Events = append([]CalendarEvent{}, source.Events...)
	result.Warnings = append([]string{}, source.Warnings...)
	return &result
}

func (cache *CalendarCache) readDisk() {
	file, err := os.Open(cache.path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[CALENDAR-CACHE] READ_ERROR path=%q error=%v", cache.path, err)
		}
		return
	}
	defer file.Close()
	var disk struct {
		Version int                           `json:"version"`
		Entries map[string]calendarCacheEntry `json:"entries"`
	}
	if err := json.NewDecoder(io.LimitReader(file, 64<<20)).Decode(&disk); err != nil {
		log.Printf("[CALENDAR-CACHE] READ_ERROR path=%q error=%v", cache.path, err)
		return
	}
	if disk.Version != calendarCacheVersion {
		log.Printf("[CALENDAR-CACHE] VERSION_MISS path=%q version=%d", cache.path, disk.Version)
		return
	}
	now := time.Now()
	for key, entry := range disk.Entries {
		if entry.Calendar != nil && !entry.FetchedAt.IsZero() && !entry.FetchedAt.After(now) {
			entry.Calendar.FetchedAt = entry.FetchedAt.Format(time.RFC3339Nano)
			cache.entries[key] = entry
		}
	}
	log.Printf("[CALENDAR-CACHE] DISK_LOADED path=%q entries=%d", cache.path, len(cache.entries))
	cache.revision++
}

// Caller cancellation stops waiting without aborting a fetch shared by others.
// Errors never replace the last successfully downloaded calendar.
func (cache *CalendarCache) Get(ctx context.Context, key string, force bool, load func(context.Context) (*Calendar, error)) (*Calendar, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cache.once.Do(cache.readDisk)
	cache.mu.Lock()
	if cache.closing {
		cache.mu.Unlock()
		return nil, context.Canceled
	}
	flight := cache.inflight[key]
	if flight == nil {
		if entry, exists := cache.entries[key]; exists && !force && time.Since(entry.FetchedAt) >= 0 && time.Since(entry.FetchedAt) < CalendarCacheTTL {
			result := cloneCalendar(entry.Calendar)
			cache.mu.Unlock()
			log.Printf("[CALENDAR-CACHE] HIT key=%q events=%d fetched_at=%s", key, len(result.Events), result.FetchedAt)
			return result, nil
		}
		flight = &calendarFlight{done: make(chan struct{})}
		cache.inflight[key] = flight
		cache.active.Add(1)
		log.Printf("[CALENDAR-CACHE] FETCH key=%q force=%t", key, force)
		go cache.fetch(context.WithoutCancel(ctx), key, flight, load)
	} else {
		log.Printf("[CALENDAR-CACHE] JOIN key=%q", key)
	}
	cache.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-flight.done:
		if flight.err != nil {
			return nil, flight.err
		}
		return cloneCalendar(flight.calendar), nil
	}
}

func (cache *CalendarCache) fetch(parent context.Context, key string, flight *calendarFlight, load func(context.Context) (*Calendar, error)) {
	defer cache.active.Done()
	ctx, cancel := context.WithTimeout(parent, 125*time.Second)
	defer cancel()
	calendar, err := load(ctx)
	if err == nil && calendar == nil {
		err = fmt.Errorf("pobieranie kalendarza nie zwróciło wyniku")
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if err == nil {
		now := time.Now()
		calendar = cloneCalendar(calendar)
		calendar.FetchedAt = now.Format(time.RFC3339Nano)
		for oldKey, entry := range cache.entries {
			if strings.HasPrefix(oldKey, "superbet:") && now.Sub(entry.FetchedAt) > 7*24*time.Hour {
				delete(cache.entries, oldKey)
			}
		}
		cache.entries[key] = calendarCacheEntry{FetchedAt: now, Calendar: calendar}
		cache.revision++
		if diskErr := cache.writeDisk(); diskErr != nil {
			log.Printf("[CALENDAR-CACHE] WRITE_ERROR path=%q error=%v", cache.path, diskErr)
			calendar.Warnings = append(calendar.Warnings, "Nie zapisano kalendarza na dysku serwera; cache działa do restartu serwera: "+diskErr.Error())
		}
		log.Printf("[CALENDAR-CACHE] STORED key=%q events=%d path=%q", key, len(calendar.Events), cache.path)
	} else {
		log.Printf("[CALENDAR-CACHE] FETCH_ERROR key=%q error=%v", key, err)
	}
	flight.calendar, flight.err = calendar, err
	delete(cache.inflight, key)
	close(flight.done)
}

// Called with cache.mu held; an atomic replacement preserves the old file if
// encoding or writing fails, and simultaneous bookmaker fetches cannot lose entries.
func (cache *CalendarCache) writeDisk() error {
	data, err := json.Marshal(struct {
		Version int                           `json:"version"`
		Entries map[string]calendarCacheEntry `json:"entries"`
	}{calendarCacheVersion, cache.entries})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cache.path), 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(cache.path), ".calendar-cache-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), cache.path)
}

func calendarDay(date string) (time.Time, time.Time, error) {
	zone, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	start, err := time.ParseInLocation("2006-01-02", date, zone)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("niepoprawna data kalendarza: %w", err)
	}
	return start, start.AddDate(0, 0, 1), nil
}

func FilterCalendar(source *Calendar, date string) (*Calendar, error) {
	start, end, err := calendarDay(date)
	if err != nil {
		return nil, err
	}
	result := cloneCalendar(source)
	result.Date = date
	result.Events = []CalendarEvent{}
	for _, event := range mensCalendarEvents(source.Events) {
		when, err := time.Parse(time.RFC3339, event.StartTime)
		if err != nil || when.Before(start) || !when.Before(end) {
			continue
		}
		event.StartTime = when.In(start.Location()).Format(time.RFC3339)
		result.Events = append(result.Events, event)
	}
	return result, nil
}

func FetchCachedSTSCalendar(ctx context.Context, cache *CalendarCache, endpoint, date string, force bool) (*Calendar, error) {
	if _, _, err := calendarDay(date); err != nil {
		return nil, err
	}
	full, err := cache.Get(ctx, "sts:"+endpoint, force, func(ctx context.Context) (*Calendar, error) {
		raw, err := FetchSTSSnapshot(ctx, endpoint, "")
		if err != nil {
			return nil, err
		}
		events, err := ParseSTSFullCalendar(raw)
		if err != nil {
			return nil, err
		}
		return &Calendar{Events: events, Warnings: []string{}}, nil
	})
	if err != nil {
		return nil, err
	}
	return FilterCalendar(full, date)
}

func FetchCachedBetclicCalendar(ctx context.Context, cache *CalendarCache, client *http.Client, endpoint, date string, force bool) (*Calendar, error) {
	betclicDebugf(ctx, "CACHED_CALENDAR_REQUEST date=%q force=%t", date, force)
	if _, _, err := calendarDay(date); err != nil {
		return nil, err
	}
	full, err := cache.Get(ctx, "betclic:"+endpoint, force, func(ctx context.Context) (*Calendar, error) {
		return FetchBetclicFullCalendar(ctx, client, endpoint)
	})
	if err != nil {
		return nil, err
	}
	result, err := FilterCalendar(full, date)
	if err == nil {
		betclicDebugf(ctx, "DATE_FILTER_RESULT date=%q catalog_events=%d included=%d fetched_at=%q", date, len(full.Events), len(result.Events), full.FetchedAt)
	}
	return result, err
}

// === sts.go ===
const STSWebSocket = "wss://www.sts.pl/sbk/api/sbk"

type CalendarEvent struct {
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
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) {
			resolver := dnsErr.Server
			if resolver == "" {
				resolver = "systemowy"
			}
			return nil, fmt.Errorf("STS WebSocket: błąd DNS backendu dla %s (resolver %s): %w", dnsErr.Name, resolver, err)
		}
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
		messageType, msg, err := conn.ReadMessage()
		if len(msg) > 0 || err != nil {
			info := archiveInfoFrom(ctx)
			info.Endpoint, info.Method = endpoint, "WEBSOCKET"
			archiveCtx := context.WithValue(ctx, archiveContextKey{}, info)
			if saveErr := archiveResponse(archiveCtx, "ramka-websocket-kanal-"+channel, msg, "ws.bin", map[string]any{"messageType": messageType, "values": readableJSONValues(msg)}, err); saveErr != nil {
				return nil, saveErr
			}
		}
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

// === Superbet calendar collector ===
type SuperbetCalendarEvent = CalendarEvent
type SuperbetCalendar = Calendar

// Server runtime. Collectors below and above this section are intentionally kept
// together so the server needs just value_bets.go, go.mod and go.sum.
var strokeLetterReplacer = strings.NewReplacer(
	"ł", "l", "Ł", "L", "đ", "d", "Đ", "D", "ø", "o", "Ø", "O",
	"æ", "ae", "Æ", "AE", "œ", "oe", "Œ", "OE", "ß", "ss",
	"ẞ", "SS", "ð", "d", "Ð", "D", "þ", "th", "Þ", "TH", "ı", "i", "ŋ", "n", "Ŋ", "N",
)

func removeDiacritics(s string) string {
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	result, _, _ := transform.String(t, s)
	return strokeLetterReplacer.Replace(result)
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if int64(len(raw)) > limit {
		return raw, fmt.Errorf("odpowiedź przekracza limit %d bajtów", limit)
	}
	return raw, err
}

type archiveContextKey struct{}
type archiveInfo struct {
	Automatic         bool      `json:"automatic,omitempty"`
	Directory         string    `json:"-"`
	AnalysisCreatedAt time.Time `json:"analysisCreatedAt,omitempty"`
	AnalysisID        string    `json:"analysisId,omitempty"`
	Match             string    `json:"match,omitempty"`
	Bookmaker         string    `json:"bookmaker"`
	Scope             string    `json:"scope"`
	Endpoint          string    `json:"endpoint,omitempty"`
	Method            string    `json:"method,omitempty"`
	RequestHex        string    `json:"requestProtobufHex,omitempty"`
	Status            int       `json:"httpStatus,omitempty"`
	ContentType       string    `json:"contentType,omitempty"`
}

var archiveRoot string
var archiveSequence atomic.Uint64

type oddsArchiveBufferKey struct{}
type oddsArchiveRecord struct {
	info            archiveInfo
	kind, extension string
	raw             []byte
	decoded         any
	err             error
}
type oddsArchiveBuffer struct {
	mu      sync.Mutex
	records []oddsArchiveRecord
}

func (buffer *oddsArchiveBuffer) flush(ctx context.Context, directory string) error {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	ctx = context.WithValue(ctx, oddsArchiveBufferKey{}, (*oddsArchiveBuffer)(nil))
	for _, record := range buffer.records {
		info := record.info
		info.Directory = directory
		if err := archiveResponse(context.WithValue(ctx, archiveContextKey{}, info), record.kind, record.raw, record.extension, record.decoded, record.err); err != nil {
			return err
		}
	}
	return nil
}

func archiveInfoFrom(ctx context.Context) archiveInfo {
	info, _ := ctx.Value(archiveContextKey{}).(archiveInfo)
	return info
}

// Raw bytes are preserved before parsing. The JSON companion describes the
// request, errors and decoded content; field-number trees do not invent a schema.
var errArchive = errors.New("błąd zapisu archiwum")

func isArchiveFailure(err error) bool { return errors.Is(err, errArchive) }

func archiveResponse(ctx context.Context, kind string, raw []byte, extension string, decoded any, fetchErr error) (err error) {
	infoForCalendar := archiveInfoFrom(ctx)
	if strings.HasPrefix(infoForCalendar.Scope, "kalendarz-pilka-nozna-") {
		if fetchErr != nil || !strings.HasPrefix(kind, "odczytane-mecze-liczba-") {
			return nil
		}
		if err := archiveDailyCalendar(infoForCalendar, decoded); err != nil {
			return fmt.Errorf("%w: %w", errArchive, err)
		}
		return nil
	}
	// Match offers defer transport archival until their parsed content is known.
	// An unchanged offer needs only an observation, not another raw payload.
	if buffer, _ := ctx.Value(oddsArchiveBufferKey{}).(*oddsArchiveBuffer); buffer != nil {
		buffer.mu.Lock()
		buffer.records = append(buffer.records, oddsArchiveRecord{info: archiveInfoFrom(ctx), kind: kind, extension: extension, raw: append([]byte(nil), raw...), decoded: decoded, err: fetchErr})
		buffer.mu.Unlock()
		return nil
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %w", errArchive, err)
			log.Printf("ARCHIVE_WRITE_ERROR: %v", err)
		}
	}()
	info := archiveInfoFrom(ctx)
	if archiveRoot == "" {
		return fmt.Errorf("nie ustawiono katalogu archiwum")
	}
	if info.Bookmaker == "" {
		return fmt.Errorf("brak oznaczenia bukmachera w kontekście zapisu archiwum")
	}
	if info.Automatic && fetchErr == nil && (strings.HasPrefix(kind, "kursy-statystyki-sklady-matchup") || strings.HasPrefix(kind, "historia-zawodnika-statystyki-proba-") || strings.HasPrefix(kind, "wynik-analizy-algorytm-")) {
		// Canonical evidence and predictions are retained in the research store.
		return nil
	}
	now := time.Now().UTC()
	dir := filepath.Join(archiveRoot, now.Format("2006-01-02"), strings.ToLower(info.Bookmaker))
	if info.AnalysisID != "" {
		day := now.Format("2006-01-02")
		if !info.AnalysisCreatedAt.IsZero() {
			day = info.AnalysisCreatedAt.UTC().Format("2006-01-02")
		}
		dir = filepath.Join(archiveRoot, day, "mecze", archiveName(info.Match)+"__"+info.AnalysisID, strings.ToLower(info.Bookmaker))
	}
	if info.Directory != "" {
		dir = info.Directory
	}
	name := strings.ToLower(info.Bookmaker) + "_" + archiveName(info.Scope) + "_" + archiveName(kind) + "_" + now.Format("20060102T150405.000000000Z") + fmt.Sprintf("_%06d", archiveSequence.Add(1))
	if info.Automatic && info.Directory == "" && fetchErr == nil {
		version := researchHash([]any{info.Endpoint, info.Status, kind, raw, decoded})
		dir = filepath.Join(archiveRoot, "automatyczne-zrodla", strings.ToLower(info.Bookmaker), archiveName(info.Match), archiveName(info.Scope), "wersja-"+version)
		name = strings.ToLower(info.Bookmaker) + "_" + archiveName(kind)
		if _, statErr := os.Stat(filepath.Join(dir, name+".json.gz")); statErr == nil {
			return nil
		} else if !os.IsNotExist(statErr) {
			return statErr
		}
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("archiwum: %w", err)
	}
	rawFile := ""
	if raw != nil {
		rawFile = name + "." + extension + ".gz"
		if err := writeGzip(filepath.Join(dir, rawFile), raw); err != nil {
			return fmt.Errorf("archiwum surowej odpowiedzi: %w", err)
		}
	}
	envelope := struct {
		SchemaVersion int         `json:"schemaVersion"`
		RecordedAt    string      `json:"recordedAt"`
		Request       archiveInfo `json:"request"`
		Kind          string      `json:"kind"`
		RawFile       string      `json:"rawFile,omitempty"`
		RawBytes      int         `json:"rawBytes"`
		Error         string      `json:"error,omitempty"`
		Content       any         `json:"content"`
	}{SchemaVersion: 1, RecordedAt: now.Format(time.RFC3339Nano), Request: info, Kind: kind, RawFile: rawFile, RawBytes: len(raw), Content: decoded}
	if fetchErr != nil {
		envelope.Error = fetchErr.Error()
	}
	data, err := json.MarshalIndent(envelope, "", "  ")
	if err == nil {
		err = writeGzip(filepath.Join(dir, name+".json.gz"), append(data, '\n'))
	}
	if err != nil {
		return fmt.Errorf("archiwum JSON: %w", err)
	}
	return nil
}

func archiveName(s string) string {
	s = calendarTeamSlug(s)
	if len(s) > 80 {
		s = strings.TrimRight(s[:80], "-")
	}
	if s == "" {
		return "bez-opisu"
	}
	return s
}

func writeGzip(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".archive-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	writer, err := gzip.NewWriterLevel(file, gzip.BestSpeed)
	if err != nil {
		_ = file.Close()
		return err
	}
	_, writeErr := writer.Write(data)
	err = errors.Join(writeErr, writer.Close(), file.Sync(), file.Close())
	if err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

func readableJSONValues(raw []byte) any {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	values := []any{}
	for {
		var value any
		err := decoder.Decode(&value)
		if err == io.EOF {
			return values
		}
		if err != nil {
			return map[string]any{"values": values, "decodeError": err.Error(), "rawStoredSeparately": true}
		}
		values = append(values, value)
	}
}

func readableGRPC(raw []byte) any {
	if len(raw) < 5 {
		return map[string]any{"partialHeaderHex": fmt.Sprintf("%x", raw)}
	}
	result := map[string]any{"flags": raw[0], "declaredPayloadBytes": binary.BigEndian.Uint32(raw[1:5]), "receivedPayloadBytes": len(raw) - 5}
	if raw[0] == 0x80 {
		result["trailers"] = string(raw[5:])
	} else if raw[0] == 0 {
		result["protobufFields"] = readableProto(raw[5:], 0)
	} else {
		result["unsupportedFlags"] = true
	}
	return result
}

func readableProto(raw []byte, depth int) any {
	fields, err := decodeProtoFields(raw)
	if err != nil {
		return map[string]any{"decodeError": err.Error(), "bytesHex": fmt.Sprintf("%x", raw)}
	}
	result := make([]map[string]any, 0, len(fields))
	for _, field := range fields {
		item := map[string]any{"field": field.Number, "wire": field.Wire}
		if field.Wire == 2 {
			item["bytes"] = len(field.Bytes)
			text := utf8.Valid(field.Bytes)
			for _, c := range string(field.Bytes) {
				if !unicode.IsPrint(c) && c != '\n' && c != '\r' && c != '\t' {
					text = false
					break
				}
			}
			if text {
				item["text"] = string(field.Bytes)
			} else if depth < 12 {
				item["possibleMessage"] = readableProto(field.Bytes, depth+1)
			} else {
				item["bytesHex"] = fmt.Sprintf("%x", field.Bytes)
			}
		} else if field.Wire == 0 {
			item["unsignedDecimal"] = strconv.FormatUint(field.Uint, 10)
		} else {
			item["bytesHex"] = fmt.Sprintf("%x", field.Bytes)
		}
		result = append(result, item)
	}
	return result
}

func (cache *CalendarCache) Peek(key string) *Calendar {
	cache.once.Do(cache.readDisk)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry, ok := cache.entries[key]; ok {
		return cloneCalendar(entry.Calendar)
	}
	return nil
}

func (cache *CalendarCache) stopAndWait(ctx context.Context) {
	cache.mu.Lock()
	cache.closing = true
	cache.mu.Unlock()
	done := make(chan struct{})
	go func() { cache.active.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		log.Printf("SHUTDOWN: przekroczono czas kończenia pobrań")
	}
}

// Superbet serves individual days. Fetch all local dates intersecting the
// rolling warmup window; calendars outside it remain available on demand.
func (cache *CalendarCache) superbetRefreshDates() []string {
	return polishWarmupDates(time.Now())
}

type collectorSchedule struct {
	NextRun     time.Time `json:"nextRun"`
	LastSuccess time.Time `json:"lastSuccess,omitempty"`
	LastError   string    `json:"lastError,omitempty"`
}

type calendarServer struct {
	leaguesMu        sync.RWMutex
	leagues          automaticLeaguePolicy
	research         *researchStore
	excludedMu       sync.Mutex
	excludedOffers   map[string]bool
	historyMu        sync.Mutex
	historyJobs      map[string]*extendedHistoryJob
	opponentJobs     map[string]*opponentSampleJob
	historyWG        sync.WaitGroup
	workMu           sync.Mutex
	foregroundWork   int
	qualifiedWork    bool
	backgroundCancel context.CancelFunc
	backgroundFlight chan struct{}
	backgroundDone   chan struct{}
	marketDone       chan struct{}
	prefetchMu       sync.Mutex
	prefetch         statsBackgroundState
	oddsMu           sync.Mutex
	oddsCache        map[string]matchOddsSnapshot
	oddsFlights      map[string]*matchOddsFlight
	oddsSlots        prioritySlots
	offerIndexMu     sync.Mutex
	offerIndex       calendarOfferIndex
	offerRevision    uint64
	persistMu        sync.Mutex
	oddsDirty        bool
	marketsDirty     bool
	statsDirty       bool
	persistStop      chan struct{}
	persistDone      chan struct{}
	marketMu         sync.Mutex
	marketCache      map[string]MatchMarketAvailability
	marketFlights    map[string]*marketAvailabilityFlight
	analysisMu       sync.Mutex
	jobs             map[string]*analysisTask
	analysisQueue    chan *analysisTask
	analysisDone     chan struct{}
	cache            *CalendarCache
	client           *http.Client
	dataDir          string
	ctx              context.Context
	mu               sync.Mutex
	schedule         map[string]collectorSchedule
	running          map[string]bool
	slots            chan struct{}
}

var serverBookmakers = []string{"STS", "BETCLIC", "SUPERBET"}

type automaticLeaguePolicy struct {
	Configured bool            `json:"configured"`
	Choices    map[string]bool `json:"choices"`
}

type automaticLeagueSource struct {
	Key         string `json:"key"`
	Bookmaker   string `json:"bookmaker"`
	Country     string `json:"country"`
	Competition string `json:"competition"`
}

type automaticLeagueOption struct {
	Key     string                  `json:"key"`
	Label   string                  `json:"label"`
	Enabled bool                    `json:"enabled"`
	Sources []automaticLeagueSource `json:"sources"`
}

func automaticLeagueKey(book string, event CalendarEvent) string {
	normalize := func(text string) string {
		return strings.Join(strings.Fields(removeDiacritics(strings.ToLower(text))), " ")
	}
	return researchHash([]string{book, normalize(event.Country), normalize(event.Competition)})
}

func (policy automaticLeaguePolicy) enabled(keys []string) bool {
	if !policy.Configured {
		return true
	}
	allowed := false
	for _, key := range keys {
		if enabled, known := policy.Choices[key]; known {
			if !enabled {
				return false
			}
			allowed = true
		}
	}
	return allowed
}

func (s *calendarServer) automaticLeagueEnabled(matches map[string]CalendarEvent) bool {
	keys := []string{}
	for book, event := range matches {
		keys = append(keys, automaticLeagueKey(book, event))
	}
	s.leaguesMu.RLock()
	defer s.leaguesMu.RUnlock()
	return s.leagues.enabled(keys)
}

// Join provider league names only through already verified fixture matches.
// Unmatched leagues remain selectable; their choices survive later matching.
func (s *calendarServer) automaticLeagues() []automaticLeagueOption {
	offers := s.calendarOffers()
	nodes := map[string]automaticLeagueSource{}
	parents := map[string]string{}
	var root func(string) string
	root = func(key string) string {
		if parents[key] != key {
			parents[key] = root(parents[key])
		}
		return parents[key]
	}
	for book, candidates := range offers.ByBook {
		for _, candidate := range candidates {
			e := candidate.Event
			if strings.TrimSpace(e.Competition) == "" {
				continue
			}
			key := automaticLeagueKey(book, e)
			nodes[key] = automaticLeagueSource{Key: key, Bookmaker: book, Country: e.Country, Competition: e.Competition}
			parents[key] = key
		}
	}
	for _, candidate := range offers.ByBook["STS"] {
		matches, err := matchCalendarOffers(candidate.Event, offers)
		if err != nil {
			continue
		}
		anchor := automaticLeagueKey("STS", candidate.Event)
		if _, ok := nodes[anchor]; !ok {
			continue
		}
		for book, event := range matches {
			key := automaticLeagueKey(book, event)
			if _, ok := nodes[key]; ok {
				parents[root(key)] = root(anchor)
			}
		}
	}
	groups := map[string][]automaticLeagueSource{}
	for key, source := range nodes {
		groups[root(key)] = append(groups[root(key)], source)
	}
	result := []automaticLeagueOption{}
	s.leaguesMu.RLock()
	defer s.leaguesMu.RUnlock()
	for _, sources := range groups {
		sort.Slice(sources, func(i, j int) bool {
			if sources[i].Bookmaker != sources[j].Bookmaker {
				if sources[i].Bookmaker == "STS" {
					return true
				}
				if sources[j].Bookmaker == "STS" {
					return false
				}
				return sources[i].Bookmaker < sources[j].Bookmaker
			}
			return sources[i].Key < sources[j].Key
		})
		keys := []string{}
		for _, source := range sources {
			keys = append(keys, source.Key)
		}
		label := strings.Trim(strings.TrimSpace(sources[0].Country)+" · "+strings.TrimSpace(sources[0].Competition), " ·")
		result = append(result, automaticLeagueOption{Key: sources[0].Key, Label: label, Enabled: s.leagues.enabled(keys), Sources: sources})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Label != result[j].Label {
			return result[i].Label < result[j].Label
		}
		return result[i].Key < result[j].Key
	})
	return result
}

func (s *calendarServer) registerAutomaticLeagueAPI(mux *http.ServeMux) {
	response := func(w http.ResponseWriter) { jsonResponse(w, 200, map[string]any{"leagues": s.automaticLeagues()}) }
	mux.HandleFunc("GET /api/v1/automatic/leagues", func(w http.ResponseWriter, r *http.Request) { response(w) })
	mux.HandleFunc("PATCH /api/v1/automatic/leagues", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Key     string `json:"key"`
			Enabled *bool  `json:"enabled"`
		}
		if err := decodeAPIRequest(w, r, &request); err != nil {
			apiError(w, 400, err)
			return
		}
		if len(request.Key) != 64 || request.Enabled == nil {
			apiError(w, 400, fmt.Errorf("wymagana liga i stan checkboxa"))
			return
		}
		options := s.automaticLeagues()
		var selected *automaticLeagueOption
		for i := range options {
			if options[i].Key == request.Key {
				selected = &options[i]
				break
			}
		}
		if selected == nil {
			apiError(w, 404, fmt.Errorf("liga nie występuje w zapisanych kalendarzach"))
			return
		}
		s.leaguesMu.Lock()
		next := automaticLeaguePolicy{Configured: true, Choices: map[string]bool{}}
		for key, enabled := range s.leagues.Choices {
			next.Choices[key] = enabled
		}
		if !s.leagues.Configured {
			for _, option := range options {
				for _, source := range option.Sources {
					next.Choices[source.Key] = true
				}
			}
		}
		for _, source := range selected.Sources {
			next.Choices[source.Key] = *request.Enabled
		}
		err := atomicJSON(filepath.Join(s.dataDir, "automatic_leagues.json"), next)
		if err == nil {
			s.leagues = next
		}
		s.leaguesMu.Unlock()
		if err != nil {
			apiError(w, 500, err)
			return
		}
		s.cancelDisabledAutomaticAnalyses()
		log.Printf("[AUTO-LEAGUES] key=%s league=%q enabled=%t", request.Key, selected.Label, *request.Enabled)
		response(w)
	})
}

func (s *calendarServer) cancelDisabledAutomaticAnalyses() {
	s.analysisMu.Lock()
	tasks := []*analysisTask{}
	for _, task := range s.jobs {
		job := task.snapshot()
		if job.Origin == "automatic" && (job.Status == "running" || job.Status == "queued") {
			tasks = append(tasks, task)
		}
	}
	s.analysisMu.Unlock()
	offers := s.calendarOffers()
	for _, task := range tasks {
		job := task.snapshot()
		matches := map[string]CalendarEvent{}
		for book, input := range map[string]string{"STS": job.Request.STSURL, "SUPERBET": job.Request.SuperbetURL, "BETCLIC": job.Request.BetclicURL} {
			id, _, _ := extractIDAndSlug(input)
			if candidate, ok := offers.ByID[book+":"+id]; ok {
				matches[book] = candidate.Event
			}
		}
		if len(matches) == len(serverBookmakers) && !s.automaticLeagueEnabled(matches) {
			task.cancel()
		}
	}
}

// Background analyses keep their progress. Request queues give manual analysis
// priority without canceling an entire fixture on each interactive request.
func (s *calendarServer) beginForeground(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.workMu.Lock()
	s.foregroundWork++
	s.workMu.Unlock()
	release := func() { s.workMu.Lock(); s.foregroundWork--; s.workMu.Unlock() }
	return release, nil
}

func (s *calendarServer) beginBackground() (context.Context, func(), bool) {
	s.workMu.Lock()
	defer s.workMu.Unlock()
	if s.ctx.Err() != nil || s.backgroundFlight != nil || s.foregroundWork != 0 || len(s.analysisQueue) != 0 {
		return nil, nil, false
	}
	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Minute)
	ctx = context.WithValue(ctx, sofaBackgroundContextKey{}, true)
	done := make(chan struct{})
	s.backgroundCancel, s.backgroundFlight = cancel, done
	return ctx, func() {
		cancel()
		s.workMu.Lock()
		s.backgroundCancel, s.backgroundFlight = nil, nil
		close(done)
		s.workMu.Unlock()
	}, true
}

type statsFixture struct {
	ID          string    `json:"id"`
	HomeID      string    `json:"homeId"`
	AwayID      string    `json:"awayId"`
	StartTime   time.Time `json:"startTime"`
	ValidUntil  time.Time `json:"validUntil"`
	NextCheckAt time.Time `json:"nextCheckAt"`
	Finished    bool      `json:"finished"`
}

type statsWarmupState struct {
	Algorithm         string    `json:"algorithm,omitempty"`
	Match             string    `json:"match"`
	StartTime         time.Time `json:"startTime"`
	CalendarFetchedAt time.Time `json:"calendarFetchedAt"`
	NextAttempt       time.Time `json:"nextAttempt"`
	Status            string    `json:"status"`
	Players           int       `json:"players"`
	Error             string    `json:"error,omitempty"`
	OddsVersion       string    `json:"oddsVersion,omitempty"`
}

type statsBackgroundState struct {
	Events   map[string]statsWarmupState `json:"events"`
	Fixtures map[string]statsFixture     `json:"fixtures"`
}

func (s *calendarServer) saveStatsBackgroundLocked() {
	for key, entry := range s.prefetch.Events {
		if entry.StartTime.Before(time.Now().Add(-7 * 24 * time.Hour)) {
			delete(s.prefetch.Events, key)
		}
	}
	for key, entry := range s.prefetch.Fixtures {
		if entry.StartTime.Before(time.Now().Add(-7 * 24 * time.Hour)) {
			delete(s.prefetch.Fixtures, key)
		}
	}
	// Readers of summaries share prefetchMu. Never serialize or fsync the
	// growing background state while holding that lock.
	s.persistMu.Lock()
	s.statsDirty = true
	s.persistMu.Unlock()
}

func (s *calendarServer) rememberStatsFixture(fixture statsFixture) {
	s.prefetchMu.Lock()
	s.prefetch.Fixtures[fixture.ID] = fixture
	s.saveStatsBackgroundLocked()
	s.prefetchMu.Unlock()
	cacheLoadedOnce.Do(initPlayerCache)
	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()
	changed := false
	for key, entry := range playerStatsCache {
		validUntil := fixture.ValidUntil
		if entry.History.Incomplete && !entry.PartialRetryAt.IsZero() && (validUntil.IsZero() || entry.PartialRetryAt.Before(validUntil)) {
			validUntil = entry.PartialRetryAt
		}
		if entry.FixtureID == fixture.ID && !entry.ValidUntil.Equal(validUntil) {
			entry.ValidUntil = validUntil
			if entry.ConsecutiveMissing >= sofaStatsMissingLimit || entry.MissingRetryAt.After(fixture.ValidUntil) {
				entry.MissingRetryAt = fixture.ValidUntil
			}
			playerStatsCache[key], changed = entry, true
		}
	}
	if changed {
		if err := atomicJSON(getCacheFilePath("sofa_player_stats_cache.json"), playerStatsCache); err != nil {
			log.Printf("STATS_CACHE_WRITE_ERROR: %v", err)
		}
	}
}

func (s *calendarServer) refreshStatsFixture(client *ResilientClient, fixture statsFixture) (statsFixture, error) {
	wasFinished := fixture.Finished
	body, err := client.withScope("status-meczu-waznosc-statystyk-" + fixture.ID).GetSofa("/event/" + fixture.ID)
	if err != nil {
		return fixture, err
	}
	var response struct {
		Event struct {
			ID       int          `json:"id"`
			StartTS  int64        `json:"startTimestamp"`
			HomeTeam SofaTeamItem `json:"homeTeam"`
			AwayTeam SofaTeamItem `json:"awayTeam"`
			Status   struct {
				Code int    `json:"code"`
				Type string `json:"type"`
			} `json:"status"`
		} `json:"event"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return fixture, err
	}
	event := response.Event
	if strconv.Itoa(event.ID) != fixture.ID {
		return fixture, fmt.Errorf("brak właściwego wydarzenia w odpowiedzi statusu")
	}
	if event.StartTS > 0 {
		fixture.StartTime = time.Unix(event.StartTS, 0).UTC()
	}
	fixture.Finished = event.Status.Type == "finished" || event.Status.Type == "canceled" || event.Status.Code == 100
	fixture.ValidUntil = fixture.StartTime.Add(4 * time.Hour)
	if fixture.Finished {
		fixture.ValidUntil = time.Now().UTC()
		log.Printf("[STATS-CACHE] FIXTURE_ENDED event_id=%s; invalidating pre-match histories", fixture.ID)
	}
	fixture.NextCheckAt = time.Now().Add(5 * time.Minute)
	s.rememberStatsFixture(fixture)
	if fixture.Finished && !wasFinished {
		finishedID := ""
		if event.Status.Type == "finished" || event.Status.Code == 100 {
			finishedID = fixture.ID
		}
		for _, teamID := range []string{fixture.HomeID, fixture.AwayID} {
			if err := markSofaTeamRefreshDue(client, teamID, finishedID); err != nil {
				client.warn("Odświeżenie kontekstu po meczu: " + err.Error())
			}
		}
	}
	return fixture, nil
}

func (s *calendarServer) statsFixtureForMatch(client *ResilientClient, homeID, awayID string, expected time.Time) (statsFixture, bool, error) {
	s.prefetchMu.Lock()
	var cached statsFixture
	for _, fixture := range s.prefetch.Fixtures {
		if fixture.Finished || !time.Now().Before(fixture.ValidUntil) {
			continue
		}
		if !((fixture.HomeID == homeID && fixture.AwayID == awayID) || (fixture.HomeID == awayID && fixture.AwayID == homeID)) {
			continue
		}
		if !expected.IsZero() {
			if delta := fixture.StartTime.Sub(expected); delta < -10*time.Minute || delta > 10*time.Minute {
				continue
			}
		}
		if cached.ID == "" || fixture.StartTime.Before(cached.StartTime) {
			cached = fixture
		}
	}
	s.prefetchMu.Unlock()
	if cached.ID != "" {
		if !time.Now().Before(cached.StartTime) && !time.Now().Before(cached.NextCheckAt) {
			var err error
			cached, err = s.refreshStatsFixture(client, cached)
			if err != nil {
				return statsFixture{}, false, err
			}
			if cached.Finished {
				return statsFixture{}, false, fmt.Errorf("mecz %s jest już zakończony", cached.ID)
			}
		}
		return cached, cached.HomeID != homeID, nil
	}
	var lastErr error
	for _, teamID := range []string{homeID, awayID} {
		body, err := client.withScope("terminarz-druzyny-" + teamID).GetSofa("/team/" + teamID + "/events/next/0")
		if err != nil {
			lastErr = err
			continue
		}
		var response SofaTeamEventsResponse
		if err := json.Unmarshal(body, &response); err != nil {
			lastErr = err
			continue
		}
		for _, event := range response.Events {
			h, a := strconv.Itoa(event.HomeTeam.ID), strconv.Itoa(event.AwayTeam.ID)
			if !((h == homeID && a == awayID) || (h == awayID && a == homeID)) || event.ID <= 0 || event.StartTS <= 0 || event.Status.Code >= 100 {
				continue
			}
			when := time.Unix(event.StartTS, 0).UTC()
			if when.Before(time.Now().Add(-2 * time.Hour)) {
				continue
			}
			if !expected.IsZero() {
				if delta := when.Sub(expected); delta < -10*time.Minute || delta > 10*time.Minute {
					continue
				}
			}
			if cached.ID == "" || when.Before(cached.StartTime) {
				cached = statsFixture{ID: strconv.Itoa(event.ID), HomeID: h, AwayID: a, StartTime: when, ValidUntil: when.Add(4 * time.Hour), NextCheckAt: when}
			}
		}
		if cached.ID != "" {
			s.rememberStatsFixture(cached)
			log.Printf("[SOFA-EVENT] MATCH eventID=%s home_id=%s away_id=%s start=%s", cached.ID, cached.HomeID, cached.AwayID, cached.StartTime.Format(time.RFC3339))
			return cached, cached.HomeID != homeID, nil
		}
	}
	if lastErr != nil {
		return statsFixture{}, false, lastErr
	}
	return statsFixture{}, false, fmt.Errorf("nie znaleziono nadchodzącego meczu %s vs %s", homeID, awayID)
}

type statsWarmupCandidate struct {
	Bookmaker      string
	Event          CalendarEvent
	StartTime      time.Time
	FetchedAt      time.Time
	OfferCheckedAt time.Time
	KnownMarkets   bool
	Matches        map[string]CalendarEvent
	OddsVersion    string
	HomeIdentity   calendarTeamKey
	AwayIdentity   calendarTeamKey
}

const statsWarmupHorizon = 48 * time.Hour

func (s *calendarServer) nextStatsWarmup() (statsWarmupCandidate, bool) {
	now := time.Now()
	offers := s.calendarOffers()
	var candidates []statsWarmupCandidate
	// One anchor per fixture avoids analyzing the same match three times.
	for _, candidate := range offers.ByBook["STS"] {
		if candidate.Event.Live || !candidate.StartTime.After(now) || candidate.StartTime.After(now.Add(statsWarmupHorizon)) {
			continue
		}
		matches, err := matchCalendarOffers(candidate.Event, offers)
		if err != nil {
			continue
		}
		if !s.automaticLeagueEnabled(matches) {
			continue
		}
		s.marketMu.Lock()
		known := true
		for _, book := range serverBookmakers {
			state, ok := s.marketCache[book+":"+matches[book].ID]
			known = known && ok && state.Status == "available" && len(state.Markets) > 0 && marketAvailabilityValid(state, matches[book], now)
		}
		s.marketMu.Unlock()
		if !known {
			continue
		}
		version, complete := s.bundleOddsVersion(matches, now)
		candidate.Matches, candidate.OddsVersion, candidate.KnownMarkets = matches, version, true
		key := candidate.Bookmaker + ":" + candidate.Event.ID
		s.prefetchMu.Lock()
		state, exists := s.prefetch.Events[key]
		s.prefetchMu.Unlock()
		if exists && state.Algorithm == fairModelVersion && now.Before(state.NextAttempt) {
			if !complete || (state.Status != "completed" && state.Status != "monitoring" && state.Status != "sofa-missing") || state.OddsVersion == version {
				continue
			}
		}
		candidates = append(candidates, candidate)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].KnownMarkets != candidates[j].KnownMarkets {
			return candidates[i].KnownMarkets
		}
		if !candidates[i].StartTime.Equal(candidates[j].StartTime) {
			return candidates[i].StartTime.Before(candidates[j].StartTime)
		}
		return candidates[i].Bookmaker+":"+candidates[i].Event.ID < candidates[j].Bookmaker+":"+candidates[j].Event.ID
	})
	if len(candidates) == 0 {
		return statsWarmupCandidate{}, false
	}
	return candidates[0], true
}

func (s *calendarServer) warmupPlayerStats(ctx context.Context, candidate statsWarmupCandidate) (int, string, error) {
	if !s.automaticLeagueEnabled(candidate.Matches) {
		return 0, "paused", context.Canceled
	}
	if s.research != nil {
		players, handled, err := s.monitorAutomaticPrices(ctx, candidate)
		if handled {
			if err != nil {
				return players, "error", err
			}
			return players, "monitoring", nil
		}
	}
	event, book := candidate.Event, candidate.Bookmaker
	ctx = context.WithValue(ctx, archiveContextKey{}, archiveInfo{Bookmaker: book, Match: event.Home + " vs " + event.Away, Scope: "tlo-statystyki-" + event.Home + "-vs-" + event.Away + "-mecz-" + event.ID, Automatic: true})
	odds, err := s.matchOdds(ctx, book, event.ID)
	props := odds.Props
	if ctx.Err() != nil {
		return 0, "paused", ctx.Err()
	}
	availability := marketAvailabilityFromOdds(book, event.ID, odds, err)
	if !errors.Is(err, context.Canceled) {
		availability = s.rememberMarketAvailability(availability)
	}
	if saveErr := archiveResponse(ctx, "oferta-do-pobrania-statystyk-w-tle", nil, "", map[string]any{"event": event, "availability": availability, "archiveVersion": odds.ArchiveVersion, "propsCount": len(props), "oddsObservedAt": odds.ObservedAt, "oddsExpiresAt": odds.ExpiresAt, "reusedFromCache": odds.FromCache}, err); saveErr != nil {
		return 0, "error", errors.Join(err, saveErr)
	}
	if err != nil {
		return 0, "error", err
	}
	if len(props) == 0 {
		return 0, "unavailable", nil
	}
	if s.research != nil {
		if !sofaCooldownUntil().IsZero() {
			return 0, "sofa-paused", nil
		}
		return s.autoAnalyze(ctx, candidate, odds)
	}
	// The analysis engine validates every current offer in production. Keep the
	// same gate for a statistics-only worker without a research store.
	for _, other := range serverBookmakers {
		if other == book {
			continue
		}
		matched := candidate.Matches[other]
		if matched.ID == "" {
			return 0, "error", fmt.Errorf("%s: brak dopasowanej oferty", other)
		}
		snapshot, fetchErr := s.matchOdds(ctx, other, matched.ID)
		if !errors.Is(fetchErr, context.Canceled) && ctx.Err() == nil {
			s.rememberMarketAvailability(marketAvailabilityFromOdds(other, matched.ID, snapshot, fetchErr))
		}
		if fetchErr != nil {
			return 0, "error", fetchErr
		}
		if len(snapshot.Props) == 0 {
			return 0, "unavailable", nil
		}
	}
	s.prefetchMu.Lock()
	previous := s.prefetch.Events[book+":"+event.ID]
	s.prefetchMu.Unlock()
	start, _ := time.Parse(time.RFC3339, event.StartTime)
	if (previous.Status == "completed" || previous.Status == "sofa-missing") && previous.OddsVersion == odds.ArchiveVersion && previous.StartTime.Equal(start) {
		// Recheck bookmakers indefinitely, but unchanged offers do not require
		// rediscovering squads with complete or repeatedly unavailable histories.
		log.Printf("[STATS-BACKGROUND] REUSE bookmaker=%s event_id=%s stats_status=%s reason=unchanged-offer", book, event.ID, previous.Status)
		return previous.Players, previous.Status, nil
	}
	if !sofaCooldownUntil().IsZero() {
		return previous.Players, "sofa-paused", nil
	}
	client, err := newSofaClient(ctx, func(message string) { log.Printf("[STATS-BACKGROUND] WARNING %s", message) }, nil)
	if err != nil {
		return 0, "error", err
	}
	client.statsPrefetch = true
	defer client.tlsClient.CloseIdleConnections()
	match := s.sofaContextForURL(event.URL)
	match.StartTime, _ = time.Parse(time.RFC3339, event.StartTime)
	home, away := calendarTeamSlug(event.Home), calendarTeamSlug(event.Away)
	teams := detectTeamsBySlugs(client, home, away, match)
	if teams.HomeID == "" || teams.AwayID == "" {
		return 0, "error", fmt.Errorf("rozpoznanie drużyn: %s", strings.Join(teams.Warnings, "; "))
	}
	fixture, reversed, err := s.statsFixtureForMatch(client, teams.HomeID, teams.AwayID, match.StartTime)
	if err != nil {
		return 0, "error", err
	}
	client.statsFixture = fixture
	targets, seen := []string{}, map[string]bool{}
	for _, prop := range props {
		if !seen[prop.PlayerName] {
			seen[prop.PlayerName] = true
			targets = append(targets, prop.PlayerName)
		}
	}
	data, err := fetchSofaMatchPlayers(client, teams.HomeID, teams.AwayID, event.Home, event.Away, fixture.ID, reversed, 10, "ALL", targets, nil)
	players := append(append([]*PlayerStatHistory(nil), data.Home...), data.Away...)
	if err != nil {
		return len(players), "partial", err
	}
	if err := errors.Join(ctx.Err(), client.archiveError()); err != nil {
		return len(players), "error", err
	}
	client.state.mu.Lock()
	unavailable, pending := client.state.statsUnavailable, client.state.statsMissingPending
	client.state.mu.Unlock()
	if len(players) == 0 && unavailable == 0 {
		return 0, "error", fmt.Errorf("brak historii zawodników dla obsługiwanych rynków")
	}
	for _, player := range players {
		if player.Incomplete {
			return len(players), "partial", fmt.Errorf("część historii jest niepełna; ponowienie w czasie bezczynności")
		}
	}
	if err := archiveResponse(ctx, "statystyki-w-tle-gotowe-do-analizy", nil, "", map[string]any{"fixture": fixture, "bookmakerEvent": event, "matchLimit": 10, "tournamentIds": "ALL", "players": players}, nil); err != nil {
		return len(players), "error", err
	}
	if unavailable > 0 {
		if pending == 0 {
			return len(players), "sofa-missing", nil
		}
		if len(players) == 0 {
			return 0, "sofa-unavailable", nil
		}
		return len(players), "sofa-partial", nil
	}
	return len(players), "completed", nil
}

func (s *calendarServer) statsMaintenanceStep(ctx context.Context) bool {
	if s.research != nil && sofaCooldownUntil().IsZero() && s.settlementStep(ctx) {
		return true
	}
	// Status checks expire pre-match statistics after the game actually ends.
	s.prefetchMu.Lock()
	var due statsFixture
	for _, fixture := range s.prefetch.Fixtures {
		if !fixture.Finished && !time.Now().Before(fixture.StartTime) && !time.Now().Before(fixture.NextCheckAt) && time.Now().Before(fixture.StartTime.Add(24*time.Hour)) {
			if due.ID == "" || fixture.NextCheckAt.Before(due.NextCheckAt) {
				due = fixture
			}
		}
	}
	s.prefetchMu.Unlock()
	if due.ID != "" && sofaCooldownUntil().IsZero() {
		client, err := newSofaClient(ctx, nil, nil)
		if err == nil {
			_, err = s.refreshStatsFixture(client, due)
			client.tlsClient.CloseIdleConnections()
		}
		if err != nil && ctx.Err() == nil {
			log.Printf("[STATS-BACKGROUND] STATUS_ERROR event_id=%s error=%v", due.ID, err)
			due.NextCheckAt = time.Now().Add(5 * time.Minute)
			if until := sofaCooldownUntil(); until.After(due.NextCheckAt) {
				due.NextCheckAt = until
			}
			s.rememberStatsFixture(due)
		}
		return ctx.Err() == nil
	}
	return false
}

func (s *calendarServer) statsBackgroundStep(ctx context.Context) bool {
	candidate, ok := s.nextStatsWarmup()
	if !ok {
		return s.statsMaintenanceStep(ctx)
	}
	// This is the same three-bookmaker qualification as the calendar filter.
	// Finish its analysis/price check before discovering additional offers.
	s.workMu.Lock()
	s.qualifiedWork = true
	s.workMu.Unlock()
	defer func() {
		s.workMu.Lock()
		s.qualifiedWork = false
		s.workMu.Unlock()
	}()
	key := candidate.Bookmaker + ":" + candidate.Event.ID
	log.Printf("[STATS-BACKGROUND] START bookmaker=%s event_id=%s match=%q", candidate.Bookmaker, candidate.Event.ID, candidate.Event.Home+" vs "+candidate.Event.Away)
	players, status, err := s.warmupPlayerStats(ctx, candidate)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		status = "error"
		err = errors.Join(err, ctx.Err())
	} else if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		status = "paused"
	}
	start, _ := time.Parse(time.RFC3339, candidate.Event.StartTime)
	state := statsWarmupState{Algorithm: fairModelVersion, Match: candidate.Event.Home + " vs " + candidate.Event.Away, StartTime: start, CalendarFetchedAt: candidate.FetchedAt, Status: status, Players: players, OddsVersion: candidate.OddsVersion}
	if version, complete := s.bundleOddsVersion(candidate.Matches, time.Now()); complete {
		state.OddsVersion = version
	}
	if err != nil {
		state.Error = err.Error()
	}
	switch status {
	case "paused":
		state.NextAttempt = time.Now().Add(15 * time.Second)
	case "partial", "error", "sofa-teams":
		state.NextAttempt = time.Now().Add(15 * time.Minute)
	case "completed", "monitoring", "unavailable", "sofa-unavailable", "sofa-partial", "sofa-missing", "sofa-paused":
		state.NextAttempt = time.Now().Add(matchOddsTTL)
	}
	s.oddsMu.Lock()
	if offer, ok := s.oddsCache[key]; ok {
		if status == "unavailable" || status == "completed" || status == "monitoring" || (strings.HasPrefix(status, "sofa-") && status != "sofa-teams") {
			state.NextAttempt = offer.ExpiresAt
			for _, book := range serverBookmakers {
				if other, ok := s.oddsCache[book+":"+candidate.Matches[book].ID]; ok && other.ExpiresAt.Before(state.NextAttempt) {
					state.NextAttempt = other.ExpiresAt
				}
			}
		}
	}
	s.oddsMu.Unlock()
	s.prefetchMu.Lock()
	s.prefetch.Events[key] = state
	for _, book := range serverBookmakers {
		if event, ok := candidate.Matches[book]; ok {
			s.prefetch.Events[book+":"+event.ID] = state
		}
	}
	s.saveStatsBackgroundLocked()
	s.prefetchMu.Unlock()
	log.Printf("[STATS-BACKGROUND] END bookmaker=%s event_id=%s status=%s players=%d error=%v", candidate.Bookmaker, candidate.Event.ID, status, players, err)
	return ctx.Err() == nil
}

func (s *calendarServer) statsBackgroundWorker() {
	defer close(s.backgroundDone)
	if sofaBrowserBase != "" {
		log.Printf("[STATS-BACKGROUND] PACING horizon=%s transport=chromium-cdp requests=1 delay=500ms-1s between_matches=5s-10s", statsWarmupHorizon)
	} else {
		log.Printf("[STATS-BACKGROUND] PACING horizon=%s requests=1 delay=2s-4s between_matches=30s-60s", statsWarmupHorizon)
	}
	for {
		delay := 15 * time.Second
		ctx, finish, ok := s.beginBackground()
		if ok {
			didWork := false
			func() {
				defer finish()
				defer func() {
					if recovered := recover(); recovered != nil {
						log.Printf("[STATS-BACKGROUND] INTERNAL_ERROR error=%v", recovered)
					}
				}()
				didWork = s.statsBackgroundStep(ctx)
			}()
			if didWork {
				delay = 30*time.Second + time.Duration(rand.Int64N(int64(30*time.Second)+1))
				if sofaBrowserBase != "" {
					delay = 5*time.Second + time.Duration(rand.Int64N(int64(5*time.Second)+1))
				}
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func sourceKey(bookmaker, date string) string {
	switch bookmaker {
	case "STS":
		return "sts:" + STSWebSocket
	case "BETCLIC":
		return "betclic:" + BetclicMatchService + "GetMatchesBySportWithNotifications"
	default:
		return "superbet:" + date
	}
}

func polishDates() (string, string) {
	zone, _ := time.LoadLocation("Europe/Warsaw")
	today := time.Now().In(zone)
	return today.Format("2006-01-02"), today.AddDate(0, 0, 1).Format("2006-01-02")
}

func polishWarmupDates(now time.Time) []string {
	zone, _ := time.LoadLocation("Europe/Warsaw")
	local := now.In(zone)
	day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, zone)
	end := now.Add(statsWarmupHorizon)
	var dates []string
	// Calendar-day iteration also covers a 48-hour window across a DST change.
	for !day.After(end) {
		dates = append(dates, day.Format("2006-01-02"))
		day = day.AddDate(0, 0, 1)
	}
	return dates
}

func calendarFetchedAt(calendar *Calendar) time.Time {
	if calendar == nil {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339Nano, calendar.FetchedAt)
	return t
}

func refreshInterval() time.Duration {
	return CalendarCacheTTL
}

func newCalendarServer(ctx context.Context, dir string) (*calendarServer, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	s := &calendarServer{cache: NewCalendarCache(filepath.Join(dir, "bookmaker_calendar_cache.json")), client: &http.Client{Timeout: 30 * time.Second}, dataDir: dir, ctx: ctx, schedule: map[string]collectorSchedule{}, running: map[string]bool{}, slots: make(chan struct{}, 3)}
	if raw, err := os.ReadFile(filepath.Join(dir, "automatic_leagues.json")); err == nil {
		if err = json.Unmarshal(raw, &s.leagues); err != nil {
			return nil, fmt.Errorf("ustawienia lig automatycznych: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	defaultCalendarCache = s.cache
	s.excludedOffers = map[string]bool{}
	if raw, err := os.ReadFile(filepath.Join(dir, "excluded_women_events.json")); err == nil {
		if err := json.Unmarshal(raw, &s.excludedOffers); err != nil {
			return nil, fmt.Errorf("lista wyłączonych ofert: %w", err)
		}
	}
	if s.excludedOffers == nil {
		s.excludedOffers = map[string]bool{}
	}
	s.cache.once.Do(s.cache.readDisk)
	s.cache.mu.Lock()
	for key, entry := range s.cache.entries {
		if entry.Calendar == nil {
			continue
		}
		book := ""
		for _, name := range serverBookmakers {
			if strings.HasPrefix(key, strings.ToLower(name)+":") {
				book = name
				break
			}
		}
		if book != "" {
			s.rememberExcludedCalendar(book, entry.Calendar.Events)
		}
		entry.Calendar.Events = mensCalendarEvents(entry.Calendar.Events)
		s.cache.entries[key] = entry
	}
	if err := s.cache.writeDisk(); err != nil {
		s.cache.mu.Unlock()
		return nil, err
	}
	s.cache.mu.Unlock()
	serverDataDir = dir
	loadSofaBackoff()
	s.oddsCache = map[string]matchOddsSnapshot{}
	s.oddsFlights = map[string]*matchOddsFlight{}
	if raw, err := os.ReadFile(filepath.Join(dir, "bookmaker_match_odds_cache.json")); err == nil {
		if err := json.Unmarshal(raw, &s.oddsCache); err != nil {
			log.Printf("ODDS_CACHE_READ_ERROR: %v", err)
			s.oddsCache = map[string]matchOddsSnapshot{}
		}
	} else if !os.IsNotExist(err) {
		log.Printf("ODDS_CACHE_READ_ERROR: %v", err)
	}
	if s.oddsCache == nil {
		s.oddsCache = map[string]matchOddsSnapshot{}
	}
	for key, snapshot := range s.oddsCache {
		if s.womenOffer(snapshot.Bookmaker, snapshot.EventID) || key != snapshot.Bookmaker+":"+snapshot.EventID || !snapshot.retain(time.Now()) {
			delete(s.oddsCache, key)
			continue
		}
		props := usableMatchProps(snapshot.Props)
		if len(props) != len(snapshot.Props) {
			delete(s.oddsCache, key)
			continue
		}
		snapshot.Props = props
		s.oddsCache[key] = snapshot
	}
	if err := atomicJSON(filepath.Join(dir, "bookmaker_match_odds_cache.json"), s.oddsCache); err != nil {
		return nil, err
	}
	s.marketCache = map[string]MatchMarketAvailability{}
	s.marketFlights = map[string]*marketAvailabilityFlight{}
	s.oddsSlots = prioritySlots{capacity: 4, backgroundLimit: 1}
	s.persistStop, s.persistDone = make(chan struct{}), make(chan struct{})
	if raw, err := os.ReadFile(filepath.Join(dir, "market_availability_cache.json")); err == nil {
		if err := json.Unmarshal(raw, &s.marketCache); err != nil {
			log.Printf("MARKET_CACHE_READ_ERROR: %v", err)
			s.marketCache = map[string]MatchMarketAvailability{}
		}
	} else if !os.IsNotExist(err) {
		log.Printf("MARKET_CACHE_READ_ERROR: %v", err)
	}
	if s.marketCache == nil {
		s.marketCache = map[string]MatchMarketAvailability{}
	}
	availabilityOffers := s.calendarOffers()
	now := time.Now()
	for key, entry := range s.marketCache {
		if s.womenOffer(entry.Bookmaker, entry.EventID) || key != entry.Bookmaker+":"+entry.EventID ||
			entry.CheckedAt.IsZero() || entry.CheckedAt.After(now) {
			delete(s.marketCache, key)
			continue
		}
		candidate, found := availabilityOffers.ByID[key]
		// Legacy records had the odds TTL and no fixture identity. Restore their
		// knowledge only for a current fixture checked within the scan horizon.
		if entry.StartTime.IsZero() && now.Sub(entry.CheckedAt) > statsWarmupHorizon {
			delete(s.marketCache, key)
			continue
		}
		if found && !marketAvailabilityMatches(entry, candidate.Event) {
			delete(s.marketCache, key)
			continue
		}
		entry = normalizeMarketAvailability(entry, candidate.Event)
		if entry.ExpiresAt.IsZero() || now.After(entry.ExpiresAt.Add(statsWarmupHorizon)) {
			delete(s.marketCache, key)
			continue
		}
		s.marketCache[key] = entry
	}
	// Recover a discovery result from a complete persisted price snapshot if
	// shutdown happened between the two batched cache writes.
	for key, snapshot := range s.oddsCache {
		candidate, found := availabilityOffers.ByID[key]
		if !found {
			continue
		}
		if previous, ok := s.marketCache[key]; ok && !snapshot.ObservedAt.After(previous.CheckedAt) && marketAvailabilityMatches(previous, candidate.Event) {
			continue
		}
		s.marketCache[key] = normalizeMarketAvailability(marketAvailabilityFromOdds(snapshot.Bookmaker, snapshot.EventID, snapshot, nil), candidate.Event)
	}
	if err := atomicJSON(filepath.Join(dir, "market_availability_cache.json"), s.marketCache); err != nil {
		return nil, err
	}
	log.Printf("[MARKET-CACHE] DISK_LOADED path=%q entries=%d", filepath.Join(dir, "market_availability_cache.json"), len(s.marketCache))
	s.jobs = map[string]*analysisTask{}
	s.analysisQueue = make(chan *analysisTask, 4)
	s.analysisDone = make(chan struct{})
	s.backgroundDone = make(chan struct{})
	s.marketDone = make(chan struct{})
	s.prefetch = statsBackgroundState{Events: map[string]statsWarmupState{}, Fixtures: map[string]statsFixture{}}
	if raw, err := os.ReadFile(filepath.Join(dir, "player_stats_background.json")); err == nil {
		if err := json.Unmarshal(raw, &s.prefetch); err != nil {
			log.Printf("STATS_BACKGROUND_READ_ERROR: %v", err)
		}
	} else if !os.IsNotExist(err) {
		log.Printf("STATS_BACKGROUND_READ_ERROR: %v", err)
	}
	if s.prefetch.Events == nil {
		s.prefetch.Events = map[string]statsWarmupState{}
	}
	if s.prefetch.Fixtures == nil {
		s.prefetch.Fixtures = map[string]statsFixture{}
	}
	// Only obsolete failed attempts are rescheduled. Completed baselines and
	// archived jobs remain intact; normal offer/league gates still apply.
	resetFailures := 0
	for key, state := range s.prefetch.Events {
		if state.Status != "error" || !state.StartTime.After(time.Now()) || !strings.Contains(state.Error, "nie użyto niezweryfikowanego ID") {
			continue
		}
		state.Status, state.Error, state.NextAttempt = "waiting", "", time.Now().UTC()
		s.prefetch.Events[key] = state
		resetFailures++
	}
	if resetFailures > 0 {
		s.prefetchMu.Lock()
		s.saveStatsBackgroundLocked()
		s.prefetchMu.Unlock()
		log.Printf("INFO Ponowienie rozpoznawania drużyn po aktualizacji: %d zapisanych błędów, wersja=%s", resetFailures, teamDetectionRevision)
	}
	if err := os.MkdirAll(filepath.Join(dir, "analysis-jobs"), 0750); err != nil {
		return nil, err
	}
	archiveRoot = filepath.Join(dir, "archive")
	if err := os.MkdirAll(archiveRoot, 0750); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(dir, "calendar_schedule.json"))
	if err == nil {
		if err := json.Unmarshal(raw, &s.schedule); err != nil {
			log.Printf("SCHEDULE_READ_ERROR: %v", err)
		}
	} else if !os.IsNotExist(err) {
		log.Printf("SCHEDULE_READ_ERROR: %v", err)
	}
	if s.schedule == nil {
		s.schedule = map[string]collectorSchedule{}
	}
	warmupDates := polishWarmupDates(time.Now())
	today := warmupDates[0]
	for _, book := range serverBookmakers {
		entry := s.schedule[book]
		calendar := s.cache.Peek(sourceKey(book, today))
		if book == "SUPERBET" {
			for _, date := range warmupDates[1:] {
				other := s.cache.Peek(sourceKey(book, date))
				if other == nil {
					calendar = nil
					break
				}
				if calendar != nil && calendarFetchedAt(other).Before(calendarFetchedAt(calendar)) {
					calendar = other
				}
			}
		}
		if calendar == nil {
			entry.NextRun = time.Now()
		} else {
			entry.LastSuccess = calendarFetchedAt(calendar)
			entry.NextRun = entry.LastSuccess.Add(refreshInterval())
		}
		s.schedule[book] = entry
	}
	if err := s.saveScheduleLocked(); err != nil {
		return nil, err
	}
	store, err := openResearchStore(dir)
	if err != nil {
		return nil, fmt.Errorf("baza prognoz: %w", err)
	}
	s.research = store
	s.historyJobs = map[string]*extendedHistoryJob{}
	return s, nil
}

func atomicJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return atomicJSONBytes(path, data)
}

func atomicJSONBytes(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".state-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(append(data, '\n'))
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

func (s *calendarServer) markMarketCacheDirty(odds bool) {
	s.persistMu.Lock()
	if odds {
		s.oddsDirty = true
	} else {
		s.marketsDirty = true
	}
	s.persistMu.Unlock()
}

// Publish live results before serializing growing cache files. Coalesce writes
// and copy only the map under its lock; encoding and disk I/O run independently.
func (s *calendarServer) flushMarketCaches() {
	s.persistMu.Lock()
	oddsDirty, marketsDirty, statsDirty := s.oddsDirty, s.marketsDirty, s.statsDirty
	s.oddsDirty, s.marketsDirty, s.statsDirty = false, false, false
	s.persistMu.Unlock()
	if statsDirty {
		s.prefetchMu.Lock()
		copy := statsBackgroundState{Events: make(map[string]statsWarmupState, len(s.prefetch.Events)), Fixtures: make(map[string]statsFixture, len(s.prefetch.Fixtures))}
		for key, entry := range s.prefetch.Events {
			copy.Events[key] = entry
		}
		for key, entry := range s.prefetch.Fixtures {
			copy.Fixtures[key] = entry
		}
		s.prefetchMu.Unlock()
		raw, err := json.Marshal(copy)
		if err == nil {
			err = atomicJSONBytes(filepath.Join(s.dataDir, "player_stats_background.json"), raw)
		}
		if err != nil {
			log.Printf("STATS_BACKGROUND_WRITE_ERROR: %v", err)
			s.persistMu.Lock()
			s.statsDirty = true
			s.persistMu.Unlock()
		}
	}
	if oddsDirty {
		s.oddsMu.Lock()
		copy := make(map[string]matchOddsSnapshot, len(s.oddsCache))
		now := time.Now()
		for key, snapshot := range s.oddsCache {
			if !snapshot.retain(now) {
				delete(s.oddsCache, key)
				continue
			}
			copy[key] = snapshot
		}
		s.oddsMu.Unlock()
		raw, err := json.Marshal(copy)
		if err == nil {
			err = atomicJSONBytes(filepath.Join(s.dataDir, "bookmaker_match_odds_cache.json"), raw)
		}
		if err != nil {
			log.Printf("ODDS_CACHE_WRITE_ERROR: %v", err)
			s.markMarketCacheDirty(true)
		}
	}
	if marketsDirty {
		s.marketMu.Lock()
		copy := make(map[string]MatchMarketAvailability, len(s.marketCache))
		now := time.Now()
		for key, entry := range s.marketCache {
			if entry.ExpiresAt.IsZero() || now.After(entry.ExpiresAt.Add(statsWarmupHorizon)) {
				delete(s.marketCache, key)
				continue
			}
			copy[key] = entry
		}
		s.marketMu.Unlock()
		raw, err := json.Marshal(copy)
		if err == nil {
			err = atomicJSONBytes(filepath.Join(s.dataDir, "market_availability_cache.json"), raw)
		}
		if err != nil {
			log.Printf("MARKET_CACHE_WRITE_ERROR: %v", err)
			s.markMarketCacheDirty(false)
		}
	}
}

func (s *calendarServer) marketCachePersistenceWorker() {
	defer close(s.persistDone)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.flushMarketCaches()
		case <-s.persistStop:
			s.flushMarketCaches()
			return
		}
	}
}

// The caller holds s.mu, or initializes the service before starting goroutines.
func (s *calendarServer) saveScheduleLocked() error {
	return atomicJSON(filepath.Join(s.dataDir, "calendar_schedule.json"), s.schedule)
}

func (s *calendarServer) recordRun(book string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.schedule[book]
	if err == nil {
		entry.LastSuccess, entry.LastError = time.Now(), ""
		entry.NextRun = time.Now().Add(refreshInterval())
	} else {
		entry.LastError = err.Error()
		entry.NextRun = time.Now().Add(5*time.Minute + time.Duration(rand.Int64N(int64(time.Minute))) - 30*time.Second)
	}
	s.schedule[book] = entry
	if saveErr := s.saveScheduleLocked(); saveErr != nil {
		log.Printf("SCHEDULE_WRITE_ERROR: %v", saveErr)
	}
	log.Printf("CALENDAR_RUN bookmaker=%s next=%s error=%v", book, entry.NextRun.Format(time.RFC3339), err)
}

func normalizeCalendarLinks(calendar *Calendar) {
	for i := range calendar.Events {
		event := &calendar.Events[i]
		slug := calendarTeamSlug(event.Home) + "-vs-" + calendarTeamSlug(event.Away)
		if strings.Contains(event.URL, "www.sts.pl") {
			event.URL = "https://www.sts.pl/kursy/pilka-nozna/" + slug + "/f" + event.ID
		}
		if strings.Contains(event.URL, "www.betclic.pl") {
			event.URL = "https://www.betclic.pl/pilka-nozna-s1/" + slug + "-m" + event.ID
		}
	}
}

func (s *calendarServer) collect(ctx context.Context, book, date string, force bool) (*Calendar, error) {
	key := sourceKey(book, date)
	// Coalesce concurrent requests in cache.Get; repeated refresh clicks within
	// 30 seconds reuse the most recent snapshot rather than re-download it.
	if previous := s.cache.Peek(key); force && previous != nil && time.Since(calendarFetchedAt(previous)) < 30*time.Second {
		return FilterCalendar(previous, date)
	}
	result, err := s.cache.Get(ctx, key, force, func(fetchCtx context.Context) (calendar *Calendar, fetchErr error) {
		select {
		case s.slots <- struct{}{}:
		case <-fetchCtx.Done():
			return nil, fetchCtx.Err()
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		}
		defer func() { <-s.slots }()
		fetchCtx, cancel := context.WithCancel(fetchCtx)
		stop := context.AfterFunc(s.ctx, cancel)
		defer cancel()
		defer stop()
		scope := "kalendarz-pilka-nozna-wszystkie-daty"
		if book == "SUPERBET" {
			scope = "kalendarz-pilka-nozna-dzien-" + date
		}
		fetchCtx = context.WithValue(fetchCtx, archiveContextKey{}, archiveInfo{Bookmaker: book, Scope: scope})
		defer func() {
			if fetchErr != nil {
				if err := archiveResponse(fetchCtx, "blad-pobierania-kalendarza", nil, "", nil, fetchErr); err != nil {
					fetchErr = errors.Join(fetchErr, err)
				}
			}
		}()
		switch book {
		case "STS":
			var raw []byte
			raw, fetchErr = FetchSTSSnapshot(fetchCtx, STSWebSocket, "")
			if fetchErr == nil {
				var events []CalendarEvent
				events, fetchErr = ParseSTSFullCalendar(raw)
				calendar = &Calendar{Events: events, Warnings: []string{}}
			}
		case "BETCLIC":
			calendar, fetchErr = FetchBetclicFullCalendar(fetchCtx, s.client, BetclicMatchService+"GetMatchesBySportWithNotifications")
		case "SUPERBET":
			calendar, fetchErr = loadSuperbetCalendarContext(fetchCtx, s.client, date, "https://production-superbet-offer-pl.freetls.fastly.net", "https://production-superbet-bmb.freetls.fastly.net")
		default:
			return nil, fmt.Errorf("nieobsługiwany bukmacher: %s", book)
		}
		if fetchErr != nil {
			return nil, fetchErr
		}
		s.rememberExcludedCalendar(book, calendar.Events)
		calendar.Events = mensCalendarEvents(calendar.Events)
		normalizeCalendarLinks(calendar)
		calendar.FetchedAt = time.Now().UTC().Format(time.RFC3339Nano)
		fetchErr = archiveResponse(fetchCtx, fmt.Sprintf("odczytane-mecze-liczba-%d", len(calendar.Events)), nil, "", calendar, nil)
		return calendar, fetchErr
	})
	if err != nil {
		return nil, err
	}
	return FilterCalendar(result, date)
}

func (s *calendarServer) scheduler() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		for _, book := range serverBookmakers {
			if s.running[book] || time.Now().Before(s.schedule[book].NextRun) {
				continue
			}
			s.running[book] = true
			go func(book string) {
				today, _ := polishDates()
				dates := []string{today}
				if book == "SUPERBET" {
					dates = s.cache.superbetRefreshDates()
				}
				var err error
				for _, date := range dates {
					if s.ctx.Err() != nil {
						return
					}
					_, dayErr := s.collect(s.ctx, book, date, true)
					err = errors.Join(err, dayErr)
				}
				s.recordRun(book, err)
				s.mu.Lock()
				s.running[book] = false
				s.mu.Unlock()
			}(book)
		}
		s.mu.Unlock()
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func jsonResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("API_WRITE_ERROR: %v", err)
	}
}

func apiError(w http.ResponseWriter, status int, err error) {
	jsonResponse(w, status, map[string]string{"error": err.Error()})
}

func (s *calendarServer) handler() http.Handler {
	mux := http.NewServeMux()
	s.registerAutomaticLeagueAPI(mux)
	mux.HandleFunc("GET /api/v1/markets/audit", func(w http.ResponseWriter, r *http.Request) {
		book, id := strings.ToUpper(r.URL.Query().Get("bookmaker")), r.URL.Query().Get("eventId")
		if input := r.URL.Query().Get("matchUrl"); id == "" && input != "" {
			if err := validateMatchURL(input, book); err != nil {
				apiError(w, 400, err)
				return
			}
			var provider string
			id, _, provider = extractIDAndSlug(input)
			if provider != book {
				apiError(w, 400, fmt.Errorf("link wskazuje innego bukmachera"))
				return
			}
		}
		if (book != "STS" && book != "SUPERBET" && book != "BETCLIC") || !marketEventIDPattern.MatchString(id) {
			apiError(w, 400, fmt.Errorf("niepoprawny bukmacher lub ID meczu"))
			return
		}
		s.oddsMu.Lock()
		snapshot, ok := s.oddsCache[book+":"+id]
		s.oddsMu.Unlock()
		if !ok || snapshot.Audit == nil {
			jsonResponse(w, 200, map[string]any{"bookmaker": book, "eventId": id, "status": "pending", "message": "Brak audytu. Wybierz mecz i poczekaj na przygotowanie ofert."})
			return
		}
		jsonResponse(w, 200, map[string]any{"bookmaker": book, "eventId": id, "status": "ready", "observedAt": snapshot.ObservedAt, "expiresAt": snapshot.ExpiresAt, "fresh": snapshot.fresh(time.Now()), "audit": snapshot.Audit})
	})
	s.registerResearchAPI(mux)
	s.registerAnalysisAPI(mux)
	s.registerMarketAvailabilityAPI(mux)
	s.registerMatchFlowAPI(mux)
	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			apiError(w, 405, fmt.Errorf("wymagana metoda GET"))
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		jsonResponse(w, 200, map[string]any{"status": "ok", "apiVersion": 1, "phase": "match-analysis", "teamDetectionRevision": teamDetectionRevision, "fairModelVersion": fairModelVersion, "bookmakerParserVersion": bookmakerParserVersion, "tacklesStatistic": tacklesStatsField, "collectors": s.schedule, "running": s.running})
	})
	mux.HandleFunc("/api/v1/calendars", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			apiError(w, 405, fmt.Errorf("wymagana metoda GET"))
			return
		}
		book, date := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("bookmaker"))), r.URL.Query().Get("date")
		if err := validateCalendarQuery(book, date); err != nil {
			apiError(w, 400, err)
			return
		}
		calendar := s.cache.Peek(sourceKey(book, date))
		if calendar == nil {
			var err error
			calendar, err = s.collect(r.Context(), book, date, false)
			if err != nil {
				apiError(w, 502, err)
				return
			}
		}
		filtered, err := FilterCalendar(calendar, date)
		if err != nil {
			apiError(w, 400, err)
			return
		}
		s.mu.Lock()
		state := s.schedule[book]
		s.mu.Unlock()
		if time.Since(calendarFetchedAt(calendar)) > CalendarCacheTTL || state.LastError != "" {
			filtered.Warnings = append(filtered.Warnings, "Dane z cache serwera, pobrane: "+calendar.FetchedAt+". Oczekiwanie na odświeżenie.")
		}
		if state.LastError != "" {
			filtered.Warnings = append(filtered.Warnings, "Ostatnie odświeżenie: "+state.LastError)
		}
		s.decorateCalendar(book, filtered)
		jsonResponse(w, 200, filtered)
	})
	mux.HandleFunc("/api/v1/calendars/refresh", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			apiError(w, 405, fmt.Errorf("wymagana metoda POST"))
			return
		}
		var query struct {
			Bookmaker string `json:"bookmaker"`
			Date      string `json:"date"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&query); err != nil {
			apiError(w, 400, err)
			return
		}
		query.Bookmaker = strings.ToUpper(strings.TrimSpace(query.Bookmaker))
		if err := validateCalendarQuery(query.Bookmaker, query.Date); err != nil {
			apiError(w, 400, err)
			return
		}
		calendar, err := s.collect(r.Context(), query.Bookmaker, query.Date, true)
		if query.Bookmaker != "SUPERBET" {
			s.recordRun(query.Bookmaker, err)
		}
		if err != nil {
			apiError(w, 502, err)
			return
		}
		s.decorateCalendar(query.Bookmaker, calendar)
		jsonResponse(w, 200, calendar)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Polling, health checks and cached calendars do not stop idle work.
		switch r.URL.Path {
		case "/api/v1/teams/detect", "/api/v1/tournaments", "/api/v1/analyses", "/api/v1/markets/prepare":
			if r.URL.Path != "/api/v1/analyses" || r.Method == http.MethodPost {
				release, err := s.beginForeground(r.Context())
				if err != nil {
					apiError(w, 503, err)
					return
				}
				defer release()
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func validateCalendarQuery(book, date string) error {
	if book != "STS" && book != "BETCLIC" && book != "SUPERBET" {
		return fmt.Errorf("nieobsługiwany bukmacher: %s", book)
	}
	start, _, err := calendarDay(date)
	if err != nil {
		return err
	}
	// Avoid an unbounded day cache caused by accidental or malicious dates.
	today, _ := polishDates()
	current, _, _ := calendarDay(today)
	if start.Before(current.AddDate(0, 0, -7)) || start.After(current.AddDate(0, 0, 60)) {
		return fmt.Errorf("data musi mieścić się między 7 dni temu a 60 dni naprzód")
	}
	return nil
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("")
	log.SetOutput(readableLogWriter{output: os.Stderr})
	listen := flag.String("listen", "127.0.0.1:8090", "adres nasłuchu HTTP API (lokalnie dla Cloudflare Tunnel)")
	dataDir := flag.String("data", "data", "katalog cache, harmonogramu i archiwum")
	sofaDiagnose := flag.Bool("sofa-diagnose", false, "porównaj połączenia SofaScore i zakończ bez uruchamiania API")
	sofaBrowser := flag.String("sofa-browser", os.Getenv("VALUE_BETS_SOFA_BROWSER"), "lokalny adres DevTools działającego Chromium, np. http://127.0.0.1:9222; pusty = HTTP")
	sofaBrowserGapMS := flag.String("sofa-browser-gap-ms", sofaBrowserGapSetting(), "minimalny odstęp po żądaniu Chromium w ms (0–10000); jitter do 2x tej wartości")
	flag.Parse()
	if err := configureSofaBrowserGap(*sofaBrowserGapMS); err != nil {
		log.Fatal("ERROR ", err)
	}
	if err := configureSofaBrowser(*sofaBrowser); err != nil {
		log.Fatal("ERROR ", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *sofaDiagnose {
		if err := runSofaDiagnostics(ctx, *dataDir); err != nil {
			log.Fatal("ERROR ", err)
		}
		return
	}
	s, err := newCalendarServer(ctx, *dataDir)
	if err != nil {
		log.Fatal("ERROR ", err)
	}
	server := &http.Server{Addr: *listen, Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 180 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	go s.scheduler()
	go s.analysisWorker()
	go s.statsBackgroundWorker()
	go s.marketBackgroundWorker()
	go s.marketCachePersistenceWorker()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("INFO Serwer uruchomiony: adres=%s, katalog danych=%s, detekcja drużyn=%s", *listen, *dataDir, teamDetectionRevision)
	err = server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("ERROR ", err)
	}
	stop()
	<-shutdownDone
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.cache.stopAndWait(shutdownCtx)
	select {
	case <-s.analysisDone:
	case <-shutdownCtx.Done():
		log.Printf("ANALYSIS_SHUTDOWN_TIMEOUT")
	}
	select {
	case <-s.backgroundDone:
	case <-shutdownCtx.Done():
		log.Printf("STATS_BACKGROUND_SHUTDOWN_TIMEOUT")
	}
	select {
	case <-s.marketDone:
	case <-shutdownCtx.Done():
		log.Printf("MARKET_BACKGROUND_SHUTDOWN_TIMEOUT")
	}
	historyDone := make(chan struct{})
	go func() { s.historyWG.Wait(); close(historyDone) }()
	select {
	case <-historyDone:
	case <-shutdownCtx.Done():
		log.Printf("HISTORY_SHUTDOWN_TIMEOUT")
	}
	if s.research != nil {
		_ = s.research.close()
	}
	close(s.persistStop)
	select {
	case <-s.persistDone:
	case <-time.After(5 * time.Second):
		log.Printf("MARKET_CACHE_SHUTDOWN_TIMEOUT")
	}
}

type superbetEventsResponse struct {
	Events []struct {
		ID      int `json:"event_id"`
		Fixture struct {
			Name         string `json:"event_name"`
			Date         string `json:"utc_date"`
			SportID      int    `json:"sport_id"`
			TournamentID int    `json:"tournament_id"`
			CategoryID   int    `json:"category_id"`
		} `json:"fixture"`
	} `json:"events"`
}

type superbetNamedItem struct {
	ID    string            `json:"id"`
	Names map[string]string `json:"localNames"`
}

func superbetCalendarDay(date string) (time.Time, time.Time, error) {
	zone, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	start, err := time.ParseInLocation("2006-01-02", date, zone)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("niepoprawna data kalendarza: %s", date)
	}
	return start, start.AddDate(0, 0, 1), nil
}

func getSuperbetCalendarJSON(ctx context.Context, client *http.Client, endpoint string, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0")
	resp, err := client.Do(req)
	if err != nil {
		info := archiveInfoFrom(ctx)
		info.Endpoint, info.Method = endpoint, http.MethodGet
		saveErr := archiveResponse(context.WithValue(ctx, archiveContextKey{}, info), "blad-polaczenia", nil, "", nil, err)
		return errors.Join(err, saveErr)
	}
	defer resp.Body.Close()
	raw, readErr := readBounded(resp.Body, 32<<20)
	info := archiveInfoFrom(ctx)
	info.Endpoint, info.Method, info.Status, info.ContentType = endpoint, http.MethodGet, resp.StatusCode, resp.Header.Get("Content-Type")
	u, _ := url.Parse(endpoint)
	if saveErr := archiveResponse(context.WithValue(ctx, archiveContextKey{}, info), "odpowiedz-"+calendarTeamSlug(u.Path)+"-"+calendarTeamSlug(u.RawQuery), raw, "http.bin", readableJSONValues(raw), readErr); saveErr != nil {
		return saveErr
	}
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Superbet HTTP %d", resp.StatusCode)
	}
	return json.Unmarshal(raw, result)
}

var calendarSlugPattern = regexp.MustCompile(`[^a-z0-9]+`)

func calendarTeamSlug(name string) string {
	return strings.Trim(calendarSlugPattern.ReplaceAllString(removeDiacritics(strings.ToLower(name)), "-"), "-")
}

func loadSuperbetCalendar(client *http.Client, date, offerBase, builderBase string) (*SuperbetCalendar, error) {
	return loadSuperbetCalendarContext(context.Background(), client, date, offerBase, builderBase)
}

func loadSuperbetCalendarContext(parent context.Context, client *http.Client, date, offerBase, builderBase string) (*SuperbetCalendar, error) {
	start, end, err := superbetCalendarDay(date)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 25*time.Second)
	defer cancel()
	var prematch, live superbetEventsResponse
	var metadata struct {
		Error bool `json:"error"`
		Data  struct {
			Tournaments []superbetNamedItem `json:"tournaments"`
			Categories  []superbetNamedItem `json:"categories"`
		} `json:"data"`
	}
	var builder struct {
		Events []string `json:"events"`
	}
	errors := make([]error, 4)
	var wg sync.WaitGroup
	for i, index := range []string{"active-prematch", "active-live"} {
		wg.Add(1)
		go func(i int, index string) {
			defer wg.Done()
			query := url.Values{
				"startDate": {start.UTC().Format(time.RFC3339)},
				"endDate":   {end.UTC().Format(time.RFC3339)},
				"index":     {index}, "sports": {"5"},
			}
			result := &prematch
			if i == 1 {
				result = &live
			}
			errors[i] = getSuperbetCalendarJSON(ctx, client, offerBase+"/v3/pl-PL/events?"+query.Encode(), result)
		}(i, index)
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		errors[2] = getSuperbetCalendarJSON(ctx, client, offerBase+"/v2/pl-PL/struct", &metadata)
		if errors[2] == nil && metadata.Error {
			errors[2] = fmt.Errorf("błąd katalogu rozgrywek")
		}
	}()
	go func() {
		defer wg.Done()
		errors[3] = getSuperbetCalendarJSON(ctx, client, builderBase+"/betbuilder/v2/getBetbuilderEvents?target=SB_PL", &builder)
	}()
	wg.Wait()
	for _, err := range errors {
		if isArchiveFailure(err) {
			return nil, err
		}
	}
	if errors[0] != nil && errors[1] != nil {
		return nil, fmt.Errorf("nie udało się pobrać kalendarza Superbeta: %w", errors[0])
	}
	result := &SuperbetCalendar{Date: date, Events: []SuperbetCalendarEvent{}, Warnings: []string{}}
	labels := []string{"Nie pobrano meczów przed rozpoczęciem", "Nie pobrano meczów na żywo", "Nie pobrano nazw rozgrywek", "Nie sprawdzono dostępności BetBuildera"}
	for i, err := range errors {
		if err != nil {
			result.Warnings = append(result.Warnings, labels[i]+": "+err.Error())
		}
	}
	names := make(map[string]string)
	countries := make(map[string]string)
	for _, item := range metadata.Data.Tournaments {
		names[item.ID] = item.Names["pl-PL"]
	}
	for _, item := range metadata.Data.Categories {
		countries[item.ID] = item.Names["pl-PL"]
	}
	builders := make(map[string]bool)
	for _, id := range builder.Events {
		builders[id] = true
	}
	byID := make(map[string]SuperbetCalendarEvent)
	for i, response := range []superbetEventsResponse{prematch, live} {
		if errors[i] != nil {
			continue
		}
		for _, event := range response.Events {
			fixture := event.Fixture
			when, err := time.Parse(time.RFC3339, fixture.Date)
			if err != nil || when.Before(start) || !when.Before(end) || fixture.SportID != 5 || event.ID <= 0 {
				continue
			}
			teams := strings.SplitN(fixture.Name, "·", 2)
			if len(teams) != 2 || strings.TrimSpace(teams[0]) == "" || strings.TrimSpace(teams[1]) == "" {
				continue
			}
			home, away := strings.TrimSpace(teams[0]), strings.TrimSpace(teams[1])
			id := strconv.Itoa(event.ID)
			competition := names[strconv.Itoa(fixture.TournamentID)]
			if competition == "" {
				competition = "Pozostałe rozgrywki"
			}
			byID[id] = SuperbetCalendarEvent{
				ID: id, Home: home, Away: away, Competition: competition,
				Country:   countries[strconv.Itoa(fixture.CategoryID)],
				StartTime: when.In(start.Location()).Format(time.RFC3339),
				URL:       "https://superbet.pl/kursy/pilka-nozna/" + calendarTeamSlug(home) + "-vs-" + calendarTeamSlug(away) + "-" + id + "?mdt=o",
				Live:      i == 1, BetBuilder: builders[id],
			}
		}
	}
	for _, event := range byID {
		result.Events = append(result.Events, event)
	}
	sort.Slice(result.Events, func(i, j int) bool {
		left, _ := time.Parse(time.RFC3339, result.Events[i].StartTime)
		right, _ := time.Parse(time.RFC3339, result.Events[j].StartTime)
		if !left.Equal(right) {
			return left.Before(right)
		}
		return result.Events[i].ID < result.Events[j].ID
	})
	return result, nil
}

// === Match analysis and statistics (same algorithm as the local application) ===
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

type NormalizedProp struct {
	SuperSubEligible *bool           `json:"superSubEligible,omitempty"`
	Bookmaker        Bookmaker       `json:"bookmaker"`
	PlayerName       string          `json:"playerName"`
	MarketType       CanonicalMarket `json:"marketType"`
	MarketLabel      string          `json:"marketLabel"`
	Line             float64         `json:"line"`
	IsUnder          bool            `json:"isUnder"`
	Odds             float64         `json:"odds"`
	MarketID         string          `json:"marketId"`
	SelectionID      string          `json:"selectionId"`
}

type PlayerStatHistory struct {
	HistoryNotes             []string                   `json:"historyNotes,omitempty"`
	HistoryExhausted         bool                       `json:"historyExhausted,omitempty"`
	MetricAvailability       map[CanonicalMarket][]bool `json:"metricAvailability,omitempty"`
	SuperSubAvailability     map[CanonicalMarket][]bool `json:"superSubAvailability,omitempty"`
	Ratings                  []*float64                 `json:"ratings"`
	MatchContexts            []PlayerMatchContext       `json:"matchContexts"`
	EventIDs                 []int                      `json:"eventIds"`
	MatchStartedAt           []time.Time                `json:"matchStartedAt"`
	Incomplete               bool                       `json:"incomplete"`
	FromCache                bool                       `json:"fromCache"`
	DataFetchedAt            time.Time                  `json:"dataFetchedAt"`
	ID                       int                        `json:"id"`
	FullName                 string                     `json:"fullName"`
	TeamSide                 string                     `json:"teamSide"`
	TeamID                   string                     `json:"teamId"`
	TeamName                 string                     `json:"teamName"`
	Matches                  int                        `json:"matches"`
	MetricValues             map[CanonicalMarket][]int  `json:"metricValues"`
	MetricValuesWithSuperSub map[CanonicalMarket][]int  `json:"metricValuesWithSuperSub"`
	Minutes                  []int                      `json:"minutes"`
	Positions                []string                   `json:"positions"`
	Opponents                []string                   `json:"opponents"`
	Dates                    []string                   `json:"dates"`
	MatchShots               [][]ShotCoordinate         `json:"matchShots"`
}

type CachedPlayerHistory struct {
	History            PlayerStatHistory `json:"history"`
	CreatedAt          time.Time         `json:"createdAt"`
	FixtureID          string            `json:"fixtureId,omitempty"`
	ValidUntil         time.Time         `json:"validUntil,omitempty"`
	PartialRetryAt     time.Time         `json:"partialRetryAt,omitempty"`
	ConsecutiveMissing int               `json:"consecutiveMissing,omitempty"`
	MissingCheckedAt   time.Time         `json:"missingCheckedAt,omitempty"`
	MissingRetryAt     time.Time         `json:"missingRetryAt,omitempty"`
}

type AnalyzedBet struct {
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

const sofaBase = "https://www.sofascore.com/api/v1"

type SofaTeamItem struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Slug   string `json:"slug"`
	Gender string `json:"gender"`
	Sport  struct {
		ID   int    `json:"id"`
		Slug string `json:"slug"`
	} `json:"sport"`
	FieldTranslations struct {
		NameTranslation map[string]string `json:"nameTranslation"`
	} `json:"fieldTranslations"`
	Country struct {
		Name   string `json:"name"`
		Alpha2 string `json:"alpha2"`
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
	Present                map[string]bool `json:"-"`
	TotalPass              int             `json:"totalPass"`
	AccuratePass           int             `json:"accuratePass"`
	Rating                 *float64        `json:"rating"`
	MinutesPlayed          int             `json:"minutesPlayed"`
	TotalShots             int             `json:"totalShots"`
	OnTargetScoringAttempt int             `json:"onTargetScoringAttempt"`
	ShotOffTarget          int             `json:"shotOffTarget"`
	BlockedScoringAttempt  int             `json:"blockedScoringAttempt"`
	TotalTackle            int             `json:"totalTackle"`
	WonTackle              int             `json:"wonTackle"`
	InterceptionWon        int             `json:"interceptionWon"`
	Fouls                  int             `json:"fouls"`
	WasFouled              int             `json:"wasFouled"`
	TotalOffside           int             `json:"totalOffside"`
	Dispossessed           int             `json:"dispossessed"`
}

func (stats *SofaPlayerStats) UnmarshalJSON(raw []byte) error {
	type plain SofaPlayerStats
	var decoded plain
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	decoded.Present = make(map[string]bool, len(fields))
	for key, value := range fields {
		decoded.Present[key] = string(value) != "null"
	}
	*stats = SofaPlayerStats(decoded)
	return nil
}

func (stats SofaPlayerStats) metricPresent(field string) bool {
	if !stats.Present[field] {
		return false
	}
	switch field {
	case tacklesStatsField:
		return stats.WonTackle >= 0
	case "totalPass":
		return stats.TotalPass >= 0
	case "accuratePass":
		return stats.AccuratePass >= 0 && (!stats.Present["totalPass"] || stats.AccuratePass <= stats.TotalPass)
	}
	return true
}

// In a full football payload, event counters such as shots on target are
// sparse (zero counters may be omitted). Participation-only lineup payloads
// must not be interpreted in the same way.
func (stats SofaPlayerStats) fullFootballCoverage() bool {
	return (stats.Present["touches"] && (stats.Present["totalPass"] || stats.Present["accuratePass"])) || (stats.Present["totalPass"] && stats.Present["accuratePass"])
}

func excludeBlockedShotsFromOnTarget(stats *SofaPlayerStats, shots []ShotCoordinate, isSub bool) {
	for _, shot := range shots {
		// SofaScore includes goal-line blocks in onTargetScoringAttempt.
		// Our shot markets classify every blocked shot separately from saves and goals.
		if shot.IsSub == isSub && shot.ShotType == "blocked-off-line" && stats.OnTargetScoringAttempt > 0 {
			stats.OnTargetScoringAttempt--
			stats.BlockedScoringAttempt++
		}
	}
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
	Substitute *bool             `json:"substitute"`
	Position   string            `json:"position"`
	// Optional display metadata can be a JSON number, string or null in SofaScore.
	// Preserve its source representation without blocking lineup classification.
	JerseyNumber json.RawMessage `json:"jerseyNumber,omitempty"`
	Grid         string          `json:"grid,omitempty"`
	Statistics   SofaPlayerStats `json:"statistics"`
}

type SofaTeamPlayersResponse struct {
	Players []struct {
		Player SofaPlayerDetails `json:"player"`
	} `json:"players"`
}

type SofaLineupResponse struct {
	Confirmed *bool `json:"confirmed"`
	Home      struct {
		Players   []SofaLineupPlayer `json:"players"`
		Formation string             `json:"formation,omitempty"`
	} `json:"home"`
	Away struct {
		Players   []SofaLineupPlayer `json:"players"`
		Formation string             `json:"formation,omitempty"`
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
		GoalType          string `json:"goalType"`
		PlayerCoordinates struct {
			X float64 `json:"x"`
			Y float64 `json:"y"`
		} `json:"playerCoordinates"`
	} `json:"shotmap"`
}

func (s SofaShotmapResponse) PlayerShots(playerID, subPlayerID int) []ShotCoordinate {
	var shots []ShotCoordinate
	for _, shot := range s.Shotmap {
		// Own goals are defensive incidents, not the player's scoring attempts.
		if shot.GoalType == "own" {
			continue
		}
		isSub := subPlayerID > 0 && shot.Player.ID == subPlayerID
		if shot.Player.ID != playerID && !isSub {
			continue
		}
		shots = append(shots, ShotCoordinate{
			X:        shot.PlayerCoordinates.X,
			Y:        shot.PlayerCoordinates.Y,
			ShotType: shot.ShotType,
			InBox:    isShotInsideBox(shot.PlayerCoordinates.X, shot.PlayerCoordinates.Y),
			OnTarget: shot.ShotType == "save" || shot.ShotType == "goal",
			IsSub:    shot.Player.ID != playerID,
		})
	}
	return shots
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
	HasNextPage bool `json:"hasNextPage"`
	Events      []struct {
		Season struct {
			ID int `json:"id"`
		} `json:"season"`
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
		HomeTeam  SofaTeamItem  `json:"homeTeam"`
		AwayTeam  SofaTeamItem  `json:"awayTeam"`
		HomeScore SofaFlowScore `json:"homeScore"`
		AwayScore SofaFlowScore `json:"awayScore"`
		Status    struct {
			Code int    `json:"code"`
			Type string `json:"type"`
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

var (
	cacheMutex         sync.Mutex
	playerStatsCacheMu sync.Mutex
	playerStatsCache   map[string]CachedPlayerHistory
	cacheLoadedOnce    sync.Once
)

// Used only when a specific upcoming fixture cannot be established.
const playerCacheTTL = 24 * time.Hour
const playerCacheVersion = 9

func initPlayerCache() {
	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()

	playerStatsCache = make(map[string]CachedPlayerHistory)
	path := getCacheFilePath("sofa_player_stats_cache.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &playerStatsCache)
	if playerStatsCache == nil {
		playerStatsCache = make(map[string]CachedPlayerHistory)
	}
	log.Printf("[STATS-CACHE] Załadowano z dysku %d wpisów historii graczy", len(playerStatsCache))
}

func getPlayerCache(key string) (*PlayerStatHistory, bool) {
	cacheLoadedOnce.Do(initPlayerCache)

	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()

	item, exists := playerStatsCache[key]
	if !exists || item.ConsecutiveMissing > 0 {
		return nil, false
	}

	expires := item.ValidUntil
	if expires.IsZero() {
		expires = item.CreatedAt.Add(playerCacheTTL)
	}
	if !time.Now().Before(expires) {
		delete(playerStatsCache, key)
		return nil, false
	}

	cp := item.History
	cp.DataFetchedAt = item.CreatedAt
	cp.FromCache = true
	return &cp, true
}

func savePlayerCache(key string, hist *PlayerStatHistory, fixture statsFixture) {
	cacheLoadedOnce.Do(initPlayerCache)

	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()

	validUntil := fixture.ValidUntil
	var partialRetryAt time.Time
	if hist.Incomplete {
		// Keep valid observations and their missing-data flags together. Retry
		// missing source data soon instead of rebuilding the same partial sample
		// on every click or server restart.
		partialRetryAt = time.Now().UTC().Add(sofaHistoryMissingTTL)
		if validUntil.IsZero() || partialRetryAt.Before(validUntil) {
			validUntil = partialRetryAt
		}
	}
	playerStatsCache[key] = CachedPlayerHistory{
		History:        *hist,
		CreatedAt:      hist.DataFetchedAt,
		FixtureID:      fixture.ID,
		ValidUntil:     validUntil,
		PartialRetryAt: partialRetryAt,
	}

	if err := atomicJSON(getCacheFilePath("sofa_player_stats_cache.json"), playerStatsCache); err != nil {
		log.Printf("STATS_CACHE_WRITE_ERROR: %v", err)
	}
}

const sofaStatsMissingLimit = 3

var errSofaStatsUnavailable = errors.New("SofaScore: brak historii ze statystykami zawodnika")

func sofaPlayerHistoryKey(client *ResilientClient, playerID, matchLimit int, tournaments string) string {
	return fmt.Sprintf("v%d_%s_%d_%d_%s", playerCacheVersion, client.statsFixture.ID, playerID, matchLimit, tournaments)
}

func sofaStatsMissingCached(key string, background bool) bool {
	cacheLoadedOnce.Do(initPlayerCache)
	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()
	entry, ok := playerStatsCache[key]
	if !ok || entry.ConsecutiveMissing == 0 {
		return false
	}
	now := time.Now()
	if !now.Before(entry.ValidUntil) {
		delete(playerStatsCache, key)
		return false
	}
	if !background || !now.Before(entry.MissingRetryAt) {
		return false
	}
	log.Printf("[SOFA-STATS-MISS] SKIP key=%q consecutive_missing=%d retry_at=%s", key, entry.ConsecutiveMissing, entry.MissingRetryAt.Format(time.RFC3339))
	return true
}

func recordSofaStatsMissing(client *ResilientClient, key string, playerID int) error {
	if err := errors.Join(client.ctx.Err(), client.archiveError(), sofaCooldownError()); err != nil {
		return err
	}
	cacheLoadedOnce.Do(initPlayerCache)
	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()
	now := time.Now().UTC()
	previous := playerStatsCache[key]
	count := 1
	if previous.ConsecutiveMissing > 0 && now.Before(previous.ValidUntil) {
		count = min(previous.ConsecutiveMissing+1, sofaStatsMissingLimit)
	}
	until := client.statsFixture.ValidUntil
	if until.IsZero() {
		until = now.Add(playerCacheTTL)
	}
	retry := now.Add(15 * time.Minute)
	if count >= sofaStatsMissingLimit || retry.After(until) {
		retry = until
	}
	entry := CachedPlayerHistory{History: PlayerStatHistory{ID: playerID}, CreatedAt: now, FixtureID: client.statsFixture.ID, ValidUntil: until, ConsecutiveMissing: count, MissingCheckedAt: now, MissingRetryAt: retry}
	playerStatsCache[key] = entry
	if err := atomicJSON(getCacheFilePath("sofa_player_stats_cache.json"), playerStatsCache); err != nil {
		log.Printf("STATS_CACHE_WRITE_ERROR: %v", err)
	}
	log.Printf("[SOFA-STATS-MISS] OBSERVATION player_id=%d fixture_id=%s consecutive_missing=%d retry_at=%s", playerID, entry.FixtureID, count, retry.Format(time.RFC3339))
	return errSofaStatsUnavailable
}

func clearSofaStatsMissing(key string) {
	cacheLoadedOnce.Do(initPlayerCache)
	playerStatsCacheMu.Lock()
	defer playerStatsCacheMu.Unlock()
	if entry, ok := playerStatsCache[key]; ok && entry.ConsecutiveMissing > 0 {
		delete(playerStatsCache, key)
		if err := atomicJSON(getCacheFilePath("sofa_player_stats_cache.json"), playerStatsCache); err != nil {
			log.Printf("STATS_CACHE_WRITE_ERROR: %v", err)
		}
		log.Printf("[SOFA-STATS-MISS] RESET key=%q reason=statistics-available", key)
	}
}

type sofaHistorySnapshot struct {
	Endpoint    string          `json:"endpoint"`
	Scope       string          `json:"scope"`
	FetchedAt   time.Time       `json:"fetchedAt"`
	ValidUntil  time.Time       `json:"validUntil,omitempty"`
	Body        json.RawMessage `json:"body"`
	Unavailable string          `json:"unavailable,omitempty"`
}

const sofaHistoryMissingTTL = 15 * time.Minute

var errSofaHistoryParticipation = errors.New("brak kompletnych statystyk udziału zawodnika")

func sofaHistoryDataUnavailable(err error) bool {
	if err == nil || isArchiveFailure(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var status sofaHTTPStatusError
	return errors.Is(err, errSofaHistoryParticipation) || (errors.As(err, &status) && status == sofaHTTPStatusError(http.StatusNotFound))
}

// Bound the lock registry, while serializing readers/fetchers of the same raw
// endpoint across players and analyses. Disk snapshots survive server restarts.
var sofaHistoryCacheLocks = func() [256]chan struct{} {
	var locks [256]chan struct{}
	for i := range locks {
		locks[i] = make(chan struct{}, 1)
	}
	return locks
}()

func getSofaHistoryData(client *ResilientClient, endpoint, scope string, validUntil time.Time, validate func([]byte) error) ([]byte, bool, error) {
	if err := errors.Join(client.ctx.Err(), client.archiveError()); err != nil {
		return nil, false, err
	}
	digest := sha256.Sum256([]byte(endpoint + "\n" + scope))
	lock := sofaHistoryCacheLocks[digest[0]]
	select {
	case lock <- struct{}{}:
	case <-client.ctx.Done():
		return nil, false, client.ctx.Err()
	}
	defer func() { <-lock }()
	if err := errors.Join(client.ctx.Err(), client.archiveError()); err != nil {
		return nil, false, err
	}
	path := filepath.Join(serverDataDir, "sofa_history_cache", fmt.Sprintf("%x", digest[:1]), archiveName(endpoint)+fmt.Sprintf("_%x.json", digest))
	now := time.Now().UTC()
	var cached sofaHistorySnapshot
	if raw, err := os.ReadFile(path); err == nil && json.Unmarshal(raw, &cached) == nil && cached.Endpoint == endpoint && cached.Scope == scope && !cached.FetchedAt.IsZero() && !cached.FetchedAt.After(now) && (cached.ValidUntil.IsZero() || now.Before(cached.ValidUntil)) {
		// Missing data is temporary, including for old matches. Never give a
		// negative entry the unlimited lifetime of a mature successful snapshot.
		negativeValid := !cached.ValidUntil.IsZero() && !now.Before(cached.FetchedAt) && now.Sub(cached.FetchedAt) < sofaHistoryMissingTTL
		if cached.Unavailable == "http-404" && negativeValid {
			return nil, true, sofaHTTPStatusError(http.StatusNotFound)
		}
		validationErr := validate(cached.Body)
		if cached.Unavailable != "http-404" && validationErr == nil {
			return cached.Body, true, nil
		}
		if cached.Unavailable == "participation" && negativeValid && errors.Is(validationErr, errSofaHistoryParticipation) {
			return cached.Body, true, fmt.Errorf("SofaScore %s: %w", endpoint, validationErr)
		}
	}
	saveSnapshot := func(body []byte, unavailable string) {
		fetchedAt := time.Now().UTC()
		expiresAt := validUntil
		if unavailable != "" {
			expiresAt = fetchedAt.Add(sofaHistoryMissingTTL)
			if !validUntil.IsZero() && validUntil.Before(expiresAt) {
				expiresAt = validUntil
			}
		}
		snapshot := sofaHistorySnapshot{Endpoint: endpoint, Scope: scope, FetchedAt: fetchedAt, ValidUntil: expiresAt, Body: body, Unavailable: unavailable}
		if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
			log.Printf("WARNING Nie udało się zapisać cache historii %s: %v", endpoint, err)
		} else if err := atomicJSON(path, snapshot); err != nil {
			log.Printf("WARNING Nie udało się zapisać cache historii %s: %v", endpoint, err)
		}
	}
	body, err := client.GetSofa(endpoint)
	if err != nil {
		var status sofaHTTPStatusError
		if errors.As(err, &status) && status == sofaHTTPStatusError(http.StatusNotFound) && !isArchiveFailure(err) && client.ctx.Err() == nil && client.archiveError() == nil {
			saveSnapshot(nil, "http-404")
		}
		return nil, false, err
	}
	if err := validate(body); err != nil {
		if errors.Is(err, errSofaHistoryParticipation) {
			saveSnapshot(body, "participation")
		}
		err = fmt.Errorf("SofaScore %s: %w", endpoint, err)
		client.warn(err.Error())
		return nil, false, err
	}
	saveSnapshot(body, "")
	return body, false, nil
}

func validateSofaHistoryStats(body []byte) error {
	var envelope struct {
		Statistics json.RawMessage `json:"statistics"`
		Error      json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	statistics := bytes.TrimSpace(envelope.Statistics)
	if len(statistics) == 0 || statistics[0] != '{' || (len(envelope.Error) > 0 && string(bytes.TrimSpace(envelope.Error)) != "null") {
		return fmt.Errorf("brak prawidłowego obiektu statistics w odpowiedzi zawodnika")
	}
	var response SofaPlayerMatchStatResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return err
	}
	if response.Statistics.MinutesPlayed <= 0 {
		if !response.Statistics.Present["minutesPlayed"] {
			return fmt.Errorf("%w: brak pola minutesPlayed, liczba pól statystyk=%d", errSofaHistoryParticipation, len(response.Statistics.Present))
		}
		return fmt.Errorf("%w: minutesPlayed=%d, liczba pól statystyk=%d", errSofaHistoryParticipation, response.Statistics.MinutesPlayed, len(response.Statistics.Present))
	}
	return nil
}

func getCacheFilePath(filename string) string {
	return filepath.Join(serverDataDir, filename)
}

func loadTeamCache() map[string]string {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	path := getCacheFilePath("sofa_team_cache.json")
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[CACHE] Brak pliku cache drużyn (%s) lub błąd odczytu: %v", path, err)
		return make(map[string]string)
	}

	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("[CACHE] Błąd deserializacji cache: %v", err)
		return make(map[string]string)
	}
	log.Printf("[CACHE] Załadowano %d drużyn z pamięci podręcznej (%s)", len(m), path)
	return m
}

type sofaTeamIdentity struct {
	ID      string `json:"id"`
	Name    string `json:"name,omitempty"`
	Slug    string `json:"slug,omitempty"`
	Manual  bool   `json:"manual,omitempty"`
	Gender  string `json:"gender,omitempty"`
	Version int    `json:"version"`
}

func verifiedTeamIDInCache(teamSlug string, cache map[string]string, gender string) string {
	id := findTeamIDInCache(teamSlug, cache)
	if id == "" {
		return ""
	}
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	var identities map[string]sofaTeamIdentity
	raw, err := os.ReadFile(getCacheFilePath("sofa_team_identity_cache.json"))
	if err == nil && json.Unmarshal(raw, &identities) == nil {
		for slug, identity := range identities {
			if (identity.Manual || (identity.Version == 3 && identity.Gender == gender)) && identity.ID == id &&
				(strings.EqualFold(slug, teamSlug) || canonicalTeamCore(slug) == canonicalTeamCore(teamSlug)) {
				return id
			}
		}
	}
	log.Printf("[CACHE] UNVERIFIED slug=%q id=%s; ponowne rozpoznanie drużyny zamiast zaufania staremu dopasowaniu", teamSlug, id)
	return ""
}

func saveTeamCache(slug string, team SofaTeamItem, gender string) {
	slug = strings.TrimSpace(strings.ToLower(slug))
	if slug == "" || team.ID <= 0 {
		return
	}

	if err := persistTeamIdentity(slug, sofaTeamIdentity{ID: strconv.Itoa(team.ID), Name: team.Name, Slug: team.Slug, Gender: gender, Version: 3}); err != nil {
		log.Printf("TEAM_CACHE_WRITE_ERROR: %v", err)
	}
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
	log.Printf("[EXTRACT] Nieznany bukmacher (UNKNOWN): ID=%s, Segment=%s", id, lastSegment)
	return id, lastSegment, "UNKNOWN"
}

func parseTeamsFromSlug(slug, _ string) (string, string) {
	log.Printf("[PARSER] Parsowanie slug drużyn: %s", slug)
	slug = strings.ToLower(slug)
	slug = strings.ReplaceAll(slug, "–", "-")

	if strings.Contains(slug, "-vs-") {
		pts := strings.Split(slug, "-vs-")
		h, a := strings.TrimSpace(pts[0]), strings.TrimSpace(pts[1])
		log.Printf("[PARSER] Rozpoznano wg '-vs-': %s vs %s", h, a)
		return h, a
	}
	if strings.Contains(slug, "-v-") {
		pts := strings.Split(slug, "-v-")
		h, a := strings.TrimSpace(pts[0]), strings.TrimSpace(pts[1])
		log.Printf("[PARSER] Rozpoznano wg '-v-': %s vs %s", h, a)
		return h, a
	}

	pts := strings.Split(slug, "-")
	if len(pts) == 2 {
		log.Printf("[PARSER] Rozpoznano wg 2 tokenów: %s vs %s", pts[0], pts[1])
		return pts[0], pts[1]
	}

	if len(pts) == 3 {
		p0 := pts[0]
		if p0 == "fc" || p0 == "sc" || p0 == "sv" || p0 == "vfb" || p0 == "vfl" || p0 == "tsg" || p0 == "rb" || p0 == "real" {
			log.Printf("[PARSER] Rozpoznano prefiks w 3 tokenach: %s-%s vs %s", pts[0], pts[1], pts[2])
			return pts[0] + "-" + pts[1], pts[2]
		}
		p1 := pts[1]
		if p1 == "fc" || p1 == "sc" || p1 == "sv" || p1 == "vfb" || p1 == "vfl" || p1 == "tsg" || p1 == "rb" || p1 == "real" {
			log.Printf("[PARSER] Rozpoznano prefiks gości w 3 tokenach: %s vs %s-%s", pts[0], pts[1], pts[2])
			return pts[0], pts[1] + "-" + pts[2]
		}
	}

	if len(pts) >= 4 {
		if pts[0] == "real" || pts[0] == "atletico" || pts[0] == "rayo" || (pts[0] == "1" && pts[1] == "fc") {
			splitIdx := 2
			if pts[0] == "1" && pts[1] == "fc" {
				splitIdx = 3
			}
			h, a := strings.Join(pts[:splitIdx], "-"), strings.Join(pts[splitIdx:], "-")
			log.Printf("[PARSER] Rozpoznano złożony klub na początku: %s vs %s", h, a)
			return h, a
		}
		mid := len(pts) / 2
		h, a := strings.Join(pts[:mid], "-"), strings.Join(pts[mid:], "-")
		log.Printf("[PARSER] Podział przez środek (długość %d): %s vs %s", len(pts), h, a)
		return h, a
	}

	log.Printf("[PARSER] Nie udało się jednoznacznie podzielić sluga: %s", slug)
	return slug, ""
}

func canonicalTeamCore(s string) string {
	s = removeDiacritics(strings.ToLower(strings.TrimSpace(s)))
	if alias, ok := teamSlugAliases[s]; ok {
		s = alias
	}
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
	prefixes := []string{"1-fc-", "1-fsv-", "fc-", "sc-", "sv-", "vfb-", "vfl-", "tsg-", "se-", "ec-", "1-"}
	for _, p := range prefixes {
		s = strings.TrimPrefix(s, p)
	}
	// RB distinguishes clubs such as Red Bull Bragantino from other Bragantinos.
	// Expand this qualifier instead of deleting it during normalization.
	if strings.HasPrefix(s, "rb-") {
		s = "red-bull-" + strings.TrimPrefix(s, "rb-")
	}
	for _, suffix := range []string{"-sv", "-fc", "-cf", "-afc"} {
		s = strings.TrimSuffix(s, suffix)
	}
	return strings.Trim(s, "-")
}

var teamSlugAliases = map[string]string{
	"betis":                "real-betis",
	"betis-sevilla":        "real-betis",
	"betis-sewilla":        "real-betis",
	"real-betis-sevilla":   "real-betis",
	"real-betis-balompie":  "real-betis",
	"rb-lipsk":             "rb-leipzig",
	"lipsk":                "leipzig",
	"bahia-salvador":       "bahia",
	"bahia-salvador-ba":    "bahia",
	"werder":               "werder-bremen",
	"werder-brema":         "werder-bremen",
	"borussia-dortmun":     "borussia-dortmund",
	"alaves":               "deportivo-alaves",
	"athletic-bilbao":      "athletic-club",
	"athletic-club-bilbao": "athletic-club",
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
		log.Printf("[CACHE] Trafienie dokładne w cache dla: %s -> %s", clean, id)
		return id
	}

	var matchingID string
	for cacheSlug, id := range cache {
		if canonicalTeamCore(cacheSlug) == cleanCore {
			if matchingID != "" && matchingID != id {
				log.Printf("[CACHE] Niejednoznaczne ID dla sluga %q; wymagane ponowne dopasowanie", teamSlug)
				return ""
			}
			matchingID = id
		}
	}
	if matchingID != "" {
		log.Printf("[CACHE] Trafienie kanoniczne w cache dla: %s -> %s", teamSlug, matchingID)
		return matchingID
	}

	log.Printf("[CACHE] Brak drużyny w cache dla sluga: %s (rdzeń: %s)", teamSlug, cleanCore)
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
// 3. POBIERANIE SIECIOWE (TLS BROWSER IMPERSONATION + REUSE)
// ==========================================

// Context-aware SofaScore transport is defined below.

var sofaWomenPattern = regexp.MustCompile(`(?i)(^|[^a-z])(women|womens|female|feminino|feminina|femenino|femenina|feminin|feminine|kobiet|kobiety)([^a-z]|$)`)
var sofaAgePattern = regexp.MustCompile(`(^|-)(u[0-9]{2})(-|$)`)

func sofaTeamNameAndGender(name string) (string, string) {
	core := canonicalTeamCore(name)
	gender := ""
	parts := strings.Split(core, "-")
	kept := make([]string, 0, len(parts))
	for i, part := range parts {
		if sofaWomenPattern.MatchString(part) || (i == len(parts)-1 && (part == "w" || part == "k" || part == "f" || part == "(k)" || part == "(w)" || part == "(f)")) {
			gender = "F"
			continue
		}
		kept = append(kept, part)
	}
	core = strings.Join(kept, "-")
	qualifier := sofaTeamQualifier(core)
	base := core
	if qualifier != "" {
		for _, suffix := range []string{"-" + qualifier, "-b", "-ii", "-reserves", "-reserve"} {
			base = strings.TrimSuffix(base, suffix)
		}
	}
	if alias, ok := teamSlugAliases[base]; ok {
		core = alias + strings.TrimPrefix(core, base)
	}
	return core, gender
}

// Brazilian bookmakers append state abbreviations to club names. Keep them
// in cache keys, but allow a Brazilian search result without that suffix.
func sofaBrazilianCore(core string) (string, bool) {
	index := strings.LastIndexByte(core, '-')
	if index < 1 {
		return core, false
	}
	switch core[index+1:] {
	case "ac", "al", "ap", "am", "ba", "ce", "df", "es", "go", "ma", "mt", "ms", "mg", "pa", "pb", "pr", "pe", "pi", "rj", "rn", "rs", "ro", "rr", "sc", "sp", "se", "to":
		return core[:index], true
	}
	return core, false
}

func sofaTeamQualifier(core string) string {
	if match := sofaAgePattern.FindStringSubmatch(core); len(match) > 2 {
		return match[2]
	}
	for _, suffix := range []string{"-b", "-ii", "-reserves", "-reserve"} {
		if strings.HasSuffix(core, suffix) {
			return "reserve"
		}
	}
	return ""
}

func sofaTeamMatchesSlug(teamSlug string, team SofaTeamItem, gender string) bool {
	matched, _ := sofaTeamMatchReason(teamSlug, team, gender, false)
	return matched
}

// Optional numeric club-name components must not erase reserve numbers or age
// categories. Only zero-prefixed two-digit tokens or four-digit year forms are
// eligible, and only the fixture resolver may accept the resulting candidate.
func sofaClubYearToken(word string) bool {
	if len(word) != 2 && len(word) != 4 {
		return false
	}
	for _, digit := range word {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	if len(word) == 2 {
		return word[0] == '0'
	}
	return strings.HasPrefix(word, "18") || strings.HasPrefix(word, "19") || strings.HasPrefix(word, "20")
}

// A shared, distinctive sequence of whole words produces a candidate, not
// a verified identity. One internal club-year token may separate those words.
// Only the fixture resolver may accept these variants.
func sofaTeamNameVariant(left, right string) bool {
	a, aq := calendarTeamIdentity(left)
	b, bq := calendarTeamIdentity(right)
	if a == "" || b == "" || aq != bq || a == b {
		return false
	}
	shorter, longer := strings.Split(a, "-"), strings.Split(b, "-")
	if len(shorter) > len(longer) {
		shorter, longer = longer, shorter
	}
	if len(shorter) == len(longer) || len(longer)-len(shorter) > 3 {
		return false
	}
	anchor := false
	for _, word := range shorter {
		anchor = anchor || (len(word) >= 4 && !calendarGenericWords[word])
	}
	if !anchor {
		return false
	}
	for start := 0; start+len(shorter) <= len(longer); start++ {
		matched, skippedYear := 0, false
		for i := start; i < len(longer) && matched < len(shorter); i++ {
			if shorter[matched] == longer[i] {
				matched++
				continue
			}
			if matched > 0 && !skippedYear && sofaClubYearToken(longer[i]) {
				skippedYear = true
				continue
			}
			break
		}
		if matched == len(shorter) {
			return true
		}
	}
	return false
}

func sofaTeamMatchReason(teamSlug string, team SofaTeamItem, gender string, allowNameVariants bool) (bool, string) {
	if team.ID <= 0 {
		return false, "brak poprawnego ID drużyny"
	}
	if (team.Sport.ID != 0 && team.Sport.ID != 1) ||
		(team.Sport.Slug != "" && team.Sport.Slug != "football") {
		return false, "inna dyscyplina niż piłka nożna"
	}
	target, namedGender := sofaTeamNameAndGender(teamSlug)
	if namedGender != "" {
		gender = namedGender
	}
	actualGender := strings.ToUpper(strings.TrimSpace(team.Gender))
	if actualGender == "" {
		_, actualGender = sofaTeamNameAndGender(team.Name + "-" + team.Slug)
	}
	if actualGender != "" && actualGender != gender {
		return false, fmt.Sprintf("inna płeć: %s zamiast %s", actualGender, gender)
	}
	if target == "" {
		return false, "pusta nazwa drużyny"
	}
	_, qualifiers := calendarTeamIdentity(teamSlug)
	for _, name := range []string{team.Name, team.Slug} {
		_, nameGender := sofaTeamNameAndGender(name)
		if nameGender != "" && nameGender != gender {
			return false, fmt.Sprintf("nazwa wskazuje inną płeć: %s zamiast %s", nameGender, gender)
		}
		_, candidateQualifiers := calendarTeamIdentity(name)
		if candidateQualifiers != "" && candidateQualifiers != qualifiers {
			wanted := qualifiers
			if wanted == "" {
				wanted = "pierwszy zespół"
			}
			labels := strings.NewReplacer("reserves", "rezerwy", "women", "kobiety", "|", ", ")
			return false, fmt.Sprintf("inna kategoria: %s zamiast %s", labels.Replace(candidateQualifiers), labels.Replace(wanted))
		}
	}
	base, hasState := sofaBrazilianCore(target)
	brazilian := strings.EqualFold(team.Country.Alpha2, "BR") || strings.EqualFold(team.Country.Name, "Brazil") || strings.EqualFold(team.Country.Name, "Brasil")
	if hasState && brazilian {
		target = base
	}
	matchesName := func(name string) bool {
		core, _ := sofaTeamNameAndGender(name)
		if hasState && brazilian {
			core, _ = sofaBrazilianCore(core)
		}
		if core == target {
			return true
		}
		a, aq := calendarTeamIdentity(core)
		b, bq := calendarTeamIdentity(target)
		return a != "" && a == b && aq == bq
	}
	names := []string{team.Name, team.Slug}
	for _, name := range team.FieldTranslations.NameTranslation {
		names = append(names, name)
	}
	variant := false
	for _, name := range names {
		if matchesName(name) {
			return true, "zgodna nazwa po normalizacji"
		}
		core, _ := sofaTeamNameAndGender(name)
		variant = variant || sofaTeamNameVariant(target, core)
	}
	if variant {
		if !allowNameVariants {
			return false, "krótszy lub dłuższy wariant nazwy wymaga daty i potwierdzenia meczu"
		}
		if team.Sport.ID != 1 && team.Sport.Slug != "football" {
			return false, "wariant nazwy bez potwierdzonej dyscypliny"
		}
		return true, "wariant nazwy; wymagane potwierdzenie obu drużyn i godziny meczu"
	}
	candidate, _ := sofaTeamNameAndGender(team.Name)
	return false, fmt.Sprintf("różne nazwy po normalizacji: %q i %q", target, candidate)
}

func searchSofaTeamCandidates(client *ResilientClient, teamSlug, gender string, allowNameVariants bool) ([]SofaTeamItem, error) {
	log.Printf("[SOFA-SEARCH] Rozpoczynam wyszukiwanie ID drużyny dla sluga: %s", teamSlug)
	core, _ := sofaTeamNameAndGender(teamSlug)
	searchCore, _ := sofaBrazilianCore(core)
	queryName := strings.ReplaceAll(searchCore, "-", " ")
	if queryName == "" {
		return nil, fmt.Errorf("pusty slug")
	}
	queries := []string{queryName}
	rawName := strings.ReplaceAll(calendarTeamSlug(teamSlug), "-", " ")
	if rawName != queryName {
		queries = append(queries, rawName)
	}
	parts := strings.Fields(queryName)
	if len(parts) > 1 {
		// Bookmakers often append a city. Search the club portion before it,
		// rather than returning unrelated clubs from a city-only query.
		shortened := parts[:len(parts)-1]
		for _, part := range shortened {
			if len(part) >= 4 && !calendarGenericWords[part] && !calendarClubWords[part] && sofaTeamQualifier(part) == "" {
				queries = append(queries, strings.Join(shortened, " "))
				break
			}
		}
		if len(parts[0]) >= 4 && !calendarGenericWords[parts[0]] && sofaTeamQualifier(parts[0]) == "" {
			queries = append(queries, parts[0])
		}
	}
	seenQueries := map[string]bool{}
	rejected := []string{}
	seenRejected := map[int]bool{}
	matches := []SofaTeamItem{}
	seenTeams := map[int]bool{}
	for _, query := range queries {
		if seenQueries[query] {
			continue
		}
		seenQueries[query] = true
		body, err := client.GetSofa("/search/all?q=" + url.QueryEscape(query) + "&page=0")
		if err != nil {
			return nil, err
		}
		var res SofaSearchResponse
		if err := json.Unmarshal(body, &res); err != nil {
			return nil, fmt.Errorf("niepoprawna odpowiedź wyszukiwania SofaScore dla %q: %w", query, err)
		}
		teams := res.ExtractTeams()
		for _, team := range teams {
			matched, reason := sofaTeamMatchReason(teamSlug, team, gender, allowNameVariants)
			if !matched && !seenRejected[team.ID] && len(rejected) < 5 {
				seenRejected[team.ID] = true
				rejected = append(rejected, fmt.Sprintf("%s (ID %d, płeć %s): %s", team.Name, team.ID, team.Gender, reason))
			}
			log.Printf("INFO SofaScore: %s (ID %d): %s", team.Name, team.ID, reason)
			if matched && !seenTeams[team.ID] {
				matches = append(matches, team)
				seenTeams[team.ID] = true
			}
		}
	}
	// Include all relevant queries before resolving ambiguity by the fixture.
	if len(matches) > 0 {
		return matches, nil
	}
	if len(rejected) > 0 {
		return nil, fmt.Errorf("brak dopasowania nazwy %q i kategorii %s w SofaScore; odrzucone wyniki: %s", teamSlug, gender, strings.Join(rejected, "; "))
	}
	return nil, fmt.Errorf("brak dopasowania nazwy %q i kategorii %s w SofaScore; wyszukiwarka nie zwróciła drużyn", teamSlug, gender)
}

func fetchSofaTeamSquad(client *ResilientClient, teamID string) ([]SofaPlayerDetails, string, error) {
	log.Printf("[SOFA-SQUAD] Pobieranie kadry dla drużyny ID: %s", teamID)

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
			log.Printf("[SOFA-SQUAD] Załadowano %d zawodników z endpointu /team/%s/players", len(playerMap), teamID)
		}
	} else {
		log.Printf("[SOFA-SQUAD] Ostrzeżenie: nie udało się pobrać /team/%s/players: %v", teamID, err)
	}

	refresh, err := getSofaTeamRefresh(client, teamID)
	evBody := refresh.Events
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
			log.Printf("[SOFA-SQUAD] Wykryta nazwa drużyny dla ID %s: '%s'", teamID, teamDisplayName)

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
		log.Printf("[SOFA-SQUAD] BŁĄD: Pusta kadra dla drużyny ID: %s", teamID)
		return nil, teamDisplayName, fmt.Errorf("nie udało się pobrać kadry dla drużyny %s", teamID)
	}

	log.Printf("[SOFA-SQUAD] Łącznie unikalnych zawodników w kadrze (%s): %d", teamDisplayName, len(squad))
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

type sofaPlayerHistoryFlight struct {
	done     chan struct{}
	cancel   context.CancelFunc
	parent   context.Context
	priority int
	promoted bool
	history  *PlayerStatHistory
	err      error
}

func (flight *sofaPlayerHistoryFlight) result() (*PlayerStatHistory, error) {
	if flight.history == nil {
		return nil, flight.err
	}
	// Callers add display names and team metadata. The assembled arrays are
	// immutable, but the struct itself must not be shared between analyses.
	copy := *flight.history
	return &copy, flight.err
}

var sofaPlayerHistoryFlights = struct {
	sync.Mutex
	items map[string]*sofaPlayerHistoryFlight
}{items: make(map[string]*sofaPlayerHistoryFlight)}

func fetchSofaPlayerCompleteHistory(client *ResilientClient, playerID int, matchLimit int, allowedTournamentIDs string) (*PlayerStatHistory, error) {
	key := sofaPlayerHistoryKey(client, playerID, matchLimit, allowedTournamentIDs) + "/team-" + client.statsTeamID
	for {
		if err := errors.Join(client.ctx.Err(), client.archiveError()); err != nil {
			return nil, err
		}
		sofaPlayerHistoryFlights.Lock()
		if flight := sofaPlayerHistoryFlights.items[key]; flight != nil {
			// Give the remaining work the foreground caller's context, pacing and
			// concurrency. Already fetched components stay in the raw cache.
			if sofaRequestPriority(client.ctx) < flight.priority {
				flight.promoted = true
				flight.cancel()
			}
			sofaPlayerHistoryFlights.Unlock()
			select {
			case <-client.ctx.Done():
				return nil, client.ctx.Err()
			case <-flight.done:
				if (errors.Is(flight.err, context.Canceled) || errors.Is(flight.err, context.DeadlineExceeded)) && (flight.promoted || flight.parent.Err() != nil) {
					continue
				}
				return flight.result()
			}
		}
		ctx, cancel := context.WithCancel(client.ctx)
		flight := &sofaPlayerHistoryFlight{done: make(chan struct{}), cancel: cancel, parent: client.ctx, priority: sofaRequestPriority(client.ctx)}
		sofaPlayerHistoryFlights.items[key] = flight
		sofaPlayerHistoryFlights.Unlock()
		func() {
			defer cancel()
			defer func() {
				if value := recover(); value != nil {
					flight.err = fmt.Errorf("historia zawodnika %d: %v", playerID, value)
				}
				sofaPlayerHistoryFlights.Lock()
				delete(sofaPlayerHistoryFlights.items, key)
				close(flight.done)
				sofaPlayerHistoryFlights.Unlock()
			}()
			copy := *client
			copy.ctx = ctx
			flight.history, flight.err = fetchSofaPlayerHistoryScan(&copy, playerID, matchLimit, allowedTournamentIDs)
		}()
		if flight.promoted && errors.Is(flight.err, context.Canceled) && client.ctx.Err() == nil {
			// Promotion must not make the automatic analysis silently omit this
			// player. Join the foreground fetch or reuse its completed cache.
			continue
		}
		return flight.result()
	}
}

func fetchSofaPlayerHistoryScan(client *ResilientClient, playerID int, matchLimit int, allowedTournamentIDs string, candidateBudgets ...int) (*PlayerStatHistory, error) {
	var incomplete atomic.Bool
	var baseStatsFailure atomic.Bool
	var retryableFailure atomic.Bool
	var cachedResponses, fetchedResponses, cachedMissingResponses atomic.Int32
	readHistoryData := func(c *ResilientClient, endpoint, scope string, validUntil time.Time, validate func([]byte) error) ([]byte, error) {
		body, reused, err := getSofaHistoryData(c, endpoint, scope, validUntil, validate)
		if err != nil {
			incomplete.Store(true)
			if !sofaHistoryDataUnavailable(err) {
				retryableFailure.Store(true)
			}
			if reused {
				cachedMissingResponses.Add(1)
			}
		} else if reused {
			cachedResponses.Add(1)
		} else {
			fetchedResponses.Add(1)
		}
		return body, err
	}
	decodeHistoryJSON := func(body []byte, target any, kind string) error {
		err := json.Unmarshal(body, target)
		if err != nil {
			incomplete.Store(true)
			retryableFailure.Store(true)
			client.warn(fmt.Sprintf("Błąd odczytu %s zawodnika %d: %v", kind, playerID, err))
		}
		return err
	}
	// A pre-match history belongs to this fixture, not the next match of a team.
	cacheKey := sofaPlayerHistoryKey(client, playerID, matchLimit, allowedTournamentIDs)
	if cached, ok := getPlayerCache(cacheKey); ok && len(candidateBudgets) == 0 {
		log.Printf("[SOFA-PLAYER-CACHE] Historia z cache player_id=%d mecze=%d niepelna=%t limit=%d turnieje=%s", playerID, cached.Matches, cached.Incomplete, matchLimit, allowedTournamentIDs)
		return cached, nil
	}
	if sofaStatsMissingCached(cacheKey, client.statsPrefetch) {
		return nil, errSofaStatsUnavailable
	}

	log.Printf("[SOFA-PLAYER] Przygotowanie historii player_id=%d limit=%d; ponowne użycie zapisanych danych meczów", playerID, matchLimit)

	type playerEventsPage struct {
		HasNextPage bool `json:"hasNextPage"`
		Events      []struct {
			ID      int    `json:"id"`
			Slug    string `json:"slug"`
			StartTS int64  `json:"startTimestamp"`
			Season  struct {
				ID int `json:"id"`
			} `json:"season"`
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
				Code int    `json:"code"`
				Type string `json:"type"`
			} `json:"status"`
		} `json:"events"`
		PlayedForTeamMap map[string]int `json:"playedForTeamMap"`
		StatisticsMap    map[string]struct {
			MinutesPlayed *int     `json:"minutesPlayed"`
			Rating        *float64 `json:"rating"`
		} `json:"statisticsMap"`
	}

	// Page contents are independent of league filters and sample size. Freeze
	// their membership for the fixture; a subsequent fixture gets a new list.
	pageScope := "fixture-" + client.statsFixture.ID + "-" + strconv.FormatInt(client.statsFixture.StartTime.Unix(), 10)
	pageValidUntil := client.statsFixture.ValidUntil
	if pageValidUntil.IsZero() {
		pageValidUntil = time.Now().UTC().Add(playerCacheTTL)
	}
	fetchPage := func(page int) ([]byte, error) {
		endpoint := fmt.Sprintf("/player/%d/events/last/%d", playerID, page)
		return readHistoryData(client, endpoint, pageScope, pageValidUntil, func(body []byte) error {
			var parsed playerEventsPage
			if err := json.Unmarshal(body, &parsed); err != nil {
				return err
			}
			if parsed.Events == nil {
				return fmt.Errorf("brak listy events gracza %d", playerID)
			}
			return nil
		})
	}
	body, err := fetchPage(0)
	if err != nil {
		log.Printf("[SOFA-PLAYER] Błąd pobierania zdarzeń dla gracza %d: %v", playerID, err)
		if strings.Contains(err.Error(), "SofaScore HTTP 404") && !isArchiveFailure(err) && client.ctx.Err() == nil {
			return nil, recordSofaStatsMissing(client, cacheKey, playerID)
		}
		return nil, err
	}
	var pEvents playerEventsPage
	if err := json.Unmarshal(body, &pEvents); err != nil {
		log.Printf("[SOFA-PLAYER] Błąd unmarshalu zdarzeń dla gracza %d: %v", playerID, err)
		return nil, err
	}
	if pEvents.Events == nil {
		return nil, fmt.Errorf("SofaScore: odpowiedź historii gracza %d nie zawiera listy events", playerID)
	}

	allowedTIDs := make(map[int]bool)
	if allowedTournamentIDs != "" && allowedTournamentIDs != "ALL" {
		for _, part := range strings.Split(allowedTournamentIDs, ",") {
			if id, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
				allowedTIDs[id] = true
			}
		}
	}

	scanBudget := matchLimit
	if len(candidateBudgets) > 0 {
		scanBudget = candidateBudgets[0]
	}
	if scanBudget < matchLimit {
		scanBudget = matchLimit
	}
	if scanBudget > 200 {
		scanBudget = 200
	}
	{
		eligibleCount := func() int {
			n := 0
			for _, e := range pEvents.Events {
				if (e.Status.Code != 100 && e.Status.Type != "finished") || (!client.statsFixture.StartTime.IsZero() && e.StartTS >= client.statsFixture.StartTime.Unix()) {
					continue
				}
				tid := e.Tournament.UniqueTournament.ID
				if tid == 0 {
					tid = e.Tournament.ID
				}
				if len(allowedTIDs) > 0 && !allowedTIDs[tid] {
					continue
				}
				n++
			}
			return n
		}
		seen := map[int]bool{}
		for _, e := range pEvents.Events {
			seen[e.ID] = true
		}
		for page := 1; page < 20 && pEvents.HasNextPage && eligibleCount() < scanBudget; page++ {
			pageBody, fetchErr := fetchPage(page)
			if fetchErr != nil {
				break
			}
			pageData := pEvents
			pageData.Events = nil
			pageData.StatisticsMap = nil
			pageData.PlayedForTeamMap = nil
			pageData.HasNextPage = false
			if decodeHistoryJSON(pageBody, &pageData, "kolejnej strony historii") != nil {
				break
			}
			if pageData.Events == nil {
				incomplete.Store(true)
				break
			}
			pEvents.HasNextPage = pageData.HasNextPage
			added := 0
			for _, e := range pageData.Events {
				if !seen[e.ID] {
					seen[e.ID] = true
					pEvents.Events = append(pEvents.Events, e)
					added++
				}
			}
			if pEvents.StatisticsMap == nil {
				pEvents.StatisticsMap = pageData.StatisticsMap
			} else {
				for id, stat := range pageData.StatisticsMap {
					pEvents.StatisticsMap[id] = stat
				}
			}
			if pEvents.PlayedForTeamMap == nil {
				pEvents.PlayedForTeamMap = pageData.PlayedForTeamMap
			} else {
				for id, team := range pageData.PlayedForTeamMap {
					pEvents.PlayedForTeamMap[id] = team
				}
			}
			if added == 0 {
				incomplete.Store(true)
				break
			}
		}
	}

	type matchCandidate struct {
		ReportedZero    bool
		OnlyTeam        bool
		NeedsAppearance bool
		HomeID          int
		AwayID          int
		HomeName        string
		AwayName        string
		Competition     string
		Context         PlayerMatchContext
		Rating          *float64
		EventID         int
		Opponent        string
		Date            string
		StartTS         int64
	}

	var allPlayedMatches []matchCandidate
	seenCandidates := map[int]bool{}

	for _, e := range pEvents.Events {
		if e.ID <= 0 || seenCandidates[e.ID] {
			continue
		}
		seenCandidates[e.ID] = true
		if e.Status.Code != 100 && e.Status.Type != "finished" {
			continue
		}
		if !client.statsFixture.StartTime.IsZero() && e.StartTS >= client.statsFixture.StartTime.Unix() {
			continue
		}

		evIDStr := strconv.Itoa(e.ID)
		minutes := pEvents.StatisticsMap[evIDStr].MinutesPlayed
		reportedZero := minutes != nil && *minutes <= 0

		uTID := e.Tournament.UniqueTournament.ID
		if uTID == 0 {
			uTID = e.Tournament.ID
		}
		if len(allowedTIDs) > 0 && !allowedTIDs[uTID] {
			continue
		}

		playedTeamID := pEvents.PlayedForTeamMap[evIDStr]
		matchContext := PlayerMatchContext{TeamID: playedTeamID, LeagueID: e.Tournament.UniqueTournament.ID, SeasonID: e.Season.ID}
		if playedTeamID > 0 && playedTeamID == e.HomeTeam.ID {
			matchContext.OpponentID, matchContext.Venue = e.AwayTeam.ID, "HOME"
		} else if playedTeamID > 0 && playedTeamID == e.AwayTeam.ID {
			matchContext.OpponentID, matchContext.Venue = e.HomeTeam.ID, "AWAY"
		}
		opp := "Mecz"
		if e.HomeTeam.Name != "" && e.AwayTeam.Name != "" {
			if playedTeamID == e.HomeTeam.ID {
				opp = displayOpponentName(e.AwayTeam.Name)
			} else {
				opp = displayOpponentName(e.HomeTeam.Name)
			}
		}

		dStr := ""
		if e.StartTS > 0 {
			zone, _ := time.LoadLocation("Europe/Warsaw")
			dStr = time.Unix(e.StartTS, 0).In(zone).Format("02.01")
		}

		allPlayedMatches = append(allPlayedMatches, matchCandidate{
			ReportedZero:    reportedZero,
			NeedsAppearance: reportedZero || matchContext.TeamID <= 0 || matchContext.OpponentID <= 0,
			HomeID:          e.HomeTeam.ID, AwayID: e.AwayTeam.ID, HomeName: e.HomeTeam.Name, AwayName: e.AwayTeam.Name,
			Competition: e.Tournament.Name,
			Context:     matchContext,
			Rating:      pEvents.StatisticsMap[evIDStr].Rating,
			EventID:     e.ID,
			Opponent:    opp,
			Date:        dStr,
			StartTS:     e.StartTS,
		})
	}
	// A player's endpoint can omit entire competitions. Merge recent team
	// fixtures, then verify the player's actual participation before inclusion.
	sourceNotes := []string{}
	teamHasMore := false
	if len(allowedTIDs) == 0 {
		sourceNotes = append(sourceNotes, "Zakres historii: wszystkie rozgrywki.")
	} else {
		sourceNotes = append(sourceNotes, "Zakres historii: wybrane rozgrywki (ID: "+allowedTournamentIDs+").")
	}
	if client.statsTeamID != "" {
		state, sourceErr := getSofaTeamRefresh(client, client.statsTeamID)
		var teamEvents SofaTeamEventsResponse
		if sourceErr == nil {
			sourceErr = json.Unmarshal(state.Events, &teamEvents)
		}
		if sourceErr == nil {
			for page := 1; page < 20 && teamEvents.HasNextPage && len(teamEvents.Events) < scanBudget; page++ {
				endpoint := fmt.Sprintf("/team/%s/events/last/%d", client.statsTeamID, page)
				body, err := readHistoryData(client, endpoint, pageScope, pageValidUntil, func(body []byte) error {
					var parsed SofaTeamEventsResponse
					if err := json.Unmarshal(body, &parsed); err != nil {
						return err
					}
					if parsed.Events == nil {
						return fmt.Errorf("brak listy meczów drużyny")
					}
					return nil
				})
				if err != nil {
					sourceErr = err
					baseStatsFailure.Store(true)
					break
				}
				var next SofaTeamEventsResponse
				if err := json.Unmarshal(body, &next); err != nil {
					sourceErr = err
					break
				}
				teamEvents.Events = append(teamEvents.Events, next.Events...)
				teamEvents.HasNextPage = next.HasNextPage
				if len(next.Events) == 0 {
					break
				}
			}
		}
		teamHasMore = teamEvents.HasNextPage
		if sourceErr != nil {
			incomplete.Store(true)
			if !sofaHistoryDataUnavailable(sourceErr) {
				retryableFailure.Store(true)
			}
			sourceNotes = append(sourceNotes, "Nie udało się sprawdzić uzupełniającej historii drużyny: "+sourceErr.Error())
		}
		if len(teamEvents.Events) > 0 {
			seen := map[int]bool{}
			for _, e := range pEvents.Events {
				seen[e.ID] = true
			}
			for _, e := range teamEvents.Events {
				if e.ID <= 0 || seen[e.ID] || e.StartTS <= 0 || (e.Status.Code != 100 && e.Status.Type != "finished") ||
					(!client.statsFixture.StartTime.IsZero() && e.StartTS >= client.statsFixture.StartTime.Unix()) {
					continue
				}
				if strconv.Itoa(e.HomeTeam.ID) != client.statsTeamID && strconv.Itoa(e.AwayTeam.ID) != client.statsTeamID {
					continue
				}
				tid := e.Tournament.UniqueTournament.ID
				if tid == 0 {
					tid = e.Tournament.ID
				}
				if len(allowedTIDs) > 0 && !allowedTIDs[tid] {
					continue
				}
				seen[e.ID] = true
				zone, _ := time.LoadLocation("Europe/Warsaw")
				allPlayedMatches = append(allPlayedMatches, matchCandidate{OnlyTeam: true, NeedsAppearance: true, EventID: e.ID, StartTS: e.StartTS, HomeID: e.HomeTeam.ID, AwayID: e.AwayTeam.ID, HomeName: e.HomeTeam.Name, AwayName: e.AwayTeam.Name, Competition: e.Tournament.Name, Opponent: e.HomeTeam.Name + " – " + e.AwayTeam.Name, Date: time.Unix(e.StartTS, 0).In(zone).Format("02.01"), Context: PlayerMatchContext{LeagueID: e.Tournament.UniqueTournament.ID, SeasonID: e.Season.ID}})
			}
		}
	}

	sort.Slice(allPlayedMatches, func(i, j int) bool {
		return allPlayedMatches[i].StartTS > allPlayedMatches[j].StartTS
	})

	historyExhausted := !pEvents.HasNextPage && !teamHasMore && len(allPlayedMatches) <= scanBudget
	if scanBudget > 0 && len(allPlayedMatches) > scanBudget {
		allPlayedMatches = allPlayedMatches[:scanBudget]
	}

	log.Printf("[SOFA-PLAYER] Historia player_id=%d kandydaci=%d docelowa_probka=%d; sprawdzanie udziału i dostępności statystyk", playerID, len(allPlayedMatches), matchLimit)

	hist := &PlayerStatHistory{
		HistoryNotes:             sourceNotes,
		HistoryExhausted:         historyExhausted,
		MetricAvailability:       make(map[CanonicalMarket][]bool),
		SuperSubAvailability:     make(map[CanonicalMarket][]bool),
		DataFetchedAt:            time.Now().UTC(),
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
		zonesKnown    bool
		subZonesKnown bool
		context       PlayerMatchContext
		hasSub        bool
		subKnown      bool
		rating        *float64
		order         int
		opp           string
		date          string
		stats         SofaPlayerStats
		subStats      SofaPlayerStats
		zone          zoneShots
		subZone       zoneShots
		shots         []ShotCoordinate
		pos           string
	}

	resultsChan := make(chan matchStatResult, len(allPlayedMatches))
	var wg sync.WaitGroup

	matchConcurrency := 2
	if client.statsPrefetch {
		matchConcurrency = 1
	}
	matchSem := make(chan struct{}, matchConcurrency)
	var issuesMu sync.Mutex
	issues := map[int]string{}
	noteMatch := func(match matchCandidate, reason string) {
		message := fmt.Sprintf("%s · %s · %s · event_id=%d: %s", match.Date, match.Opponent, match.Competition, match.EventID, reason)
		issuesMu.Lock()
		if previous := issues[match.EventID]; previous != "" {
			message = previous + "; " + reason
		}
		issues[match.EventID] = message
		issuesMu.Unlock()
		level := "WARNING"
		if strings.HasPrefix(reason, "użyto") || strings.HasPrefix(reason, "uzupełniono") || strings.HasPrefix(reason, "występ przekracza") || strings.HasPrefix(reason, "nie potwierdzono zawodnika w składzie") {
			level = "INFO"
		}
		// Expanding the candidate window revisits its beginning. Keep every
		// reason in the returned evidence, but log it once per analysis.
		if client.firstHistoryNote(fmt.Sprintf("%d/%d/%s", playerID, match.EventID, reason)) {
			log.Printf("%s Historia zawodnika player_id=%d: %s", level, playerID, message)
		}
	}

	var collected []matchStatResult
	processed := 0
	for processed < len(allPlayedMatches) && len(collected) < matchLimit {
		end := processed + matchLimit - len(collected)
		if end > len(allPlayedMatches) {
			end = len(allPlayedMatches)
		}
		for i := processed; i < end; i++ {
			m := allPlayedMatches[i]
			wg.Add(1)
			go func(idx int, match matchCandidate) {
				defer wg.Done()
				select {
				case matchSem <- struct{}{}:
				case <-client.ctx.Done():
					return
				}
				defer func() { <-matchSem }()
				if client.background && !sofaCooldownUntil().IsZero() {
					incomplete.Store(true)
					return
				}
				client := client.withScope(archiveInfoFrom(client.ctx).Scope + "-mecz-" + match.Opponent + "-" + time.Unix(match.StartTS, 0).UTC().Format("2006-01-02") + "-event-" + strconv.Itoa(match.EventID))
				// Only completed matches reach this point. Reuse raw data across
				// fixtures, filters and sample sizes, even if another endpoint failed.
				// Recent results get one later read for source corrections; mature
				// snapshots have no time-based expiry. Settlement uses its own reads.
				var matchValidUntil time.Time
				if correctionCheck := time.Unix(match.StartTS, 0).UTC().Add(26 * time.Hour); match.StartTS > 0 && time.Now().Before(correctionCheck) {
					matchValidUntil = correctionCheck
				} else if match.StartTS <= 0 {
					matchValidUntil = time.Now().UTC().Add(playerCacheTTL)
				}
				getMatchData := func(endpoint string, validate func([]byte) error) ([]byte, error) {
					return readHistoryData(client, endpoint, "finished-event-v1", matchValidUntil, validate)
				}
				var rosterPlayer *SofaLineupPlayer
				rosterLoaded, rosterComplete := false, false
				loadRoster := func() bool {
					if rosterLoaded {
						return rosterPlayer != nil
					}
					rosterLoaded = true
					body, err := getMatchData(fmt.Sprintf("/event/%d/lineups", match.EventID), func(body []byte) error {
						var lineup SofaLineupResponse
						if err := json.Unmarshal(body, &lineup); err != nil {
							return err
						}
						if len(lineup.Home.Players) == 0 || len(lineup.Away.Players) == 0 {
							return fmt.Errorf("niekompletne składy meczu %d", match.EventID)
						}
						return nil
					})
					if err != nil {
						return false
					}
					var lineup SofaLineupResponse
					if json.Unmarshal(body, &lineup) != nil {
						return false
					}
					rosterComplete = true
					for side, players := range [][]SofaLineupPlayer{lineup.Home.Players, lineup.Away.Players} {
						for _, player := range players {
							if player.Player.ID != playerID {
								continue
							}
							copy := player
							rosterPlayer = &copy
							match.Context.TeamID, match.Context.OpponentID, match.Context.Venue = match.HomeID, match.AwayID, "HOME"
							match.Opponent = displayOpponentName(match.AwayName)
							if side == 1 {
								match.Context.TeamID, match.Context.OpponentID, match.Context.Venue = match.AwayID, match.HomeID, "AWAY"
								match.Opponent = displayOpponentName(match.HomeName)
							}
							return true
						}
					}
					return false
				}
				if match.NeedsAppearance && !loadRoster() && match.OnlyTeam {
					if !rosterComplete {
						incomplete.Store(true)
						noteMatch(match, "brak potwierdzenia udziału zawodnika w uzupełniającej historii drużyny")
					} else {
						noteMatch(match, "nie potwierdzono zawodnika w składzie tego spotkania; nie dopisano występu drużyny")
					}
					return
				}
				if (match.OnlyTeam || match.ReportedZero) && rosterPlayer != nil && rosterPlayer.Statistics.Present["minutesPlayed"] && rosterPlayer.Statistics.MinutesPlayed <= 0 {
					return
				}

				// 1. Statystyki bazowe zawodnika
				statURL := fmt.Sprintf("/event/%d/player/%d/statistics", match.EventID, playerID)
				sBody, reused, err := getSofaHistoryData(client, statURL, "finished-event-v1", matchValidUntil, validateSofaHistoryStats)
				if err != nil && reused {
					cachedMissingResponses.Add(1)
				}
				if err == nil {
					if reused {
						cachedResponses.Add(1)
					} else {
						fetchedResponses.Add(1)
					}
				}
				var pStat SofaPlayerMatchStatResponse
				if err != nil {
					canFallback := client.ctx.Err() == nil && sofaHistoryDataUnavailable(err)
					if !canFallback {
						retryableFailure.Store(true)
					}
					if canFallback && loadRoster() && rosterPlayer.Statistics.MinutesPlayed > 0 {
						pStat.Statistics, pStat.Position = rosterPlayer.Statistics, rosterPlayer.Position
						noteMatch(match, "użyto potwierdzonych statystyk ze składu; szczegóły zawodnika niedostępne")
					} else {
						incomplete.Store(true)
						noteMatch(match, "pominięto występ: brak potwierdzonych statystyk zawodnika: "+err.Error())
						if !canFallback {
							baseStatsFailure.Store(true)
						}
						return
					}
				} else {
					if err := json.Unmarshal(sBody, &pStat); err != nil {
						incomplete.Store(true)
						baseStatsFailure.Store(true)
						noteMatch(match, "pominięto występ: niepoprawne statystyki: "+err.Error())
						return
					}
				}
				if pStat.Statistics.MinutesPlayed <= 0 {
					incomplete.Store(true)
					client.warn(fmt.Sprintf("Niekompletne statystyki gracza %d w meczu %d", playerID, match.EventID))
					return
				}
				if match.OnlyTeam {
					noteMatch(match, "uzupełniono potwierdzony występ nieobecny na liście zawodnika")
				}
				if pStat.Statistics.MinutesPlayed > 90 {
					noteMatch(match, "występ przekracza 90 minut; dane źródłowe mogą obejmować dogrywkę")
				}

				// 2. Wyszukiwanie zmiennika (SuperSub) tylko gdy zagrał poniżej pełnych 90 minut
				var subStats SofaPlayerStats
				subPlayerID := 0
				subKnown := pStat.Statistics.MinutesPlayed >= 90
				if pStat.Statistics.MinutesPlayed < 90 {
					incURL := fmt.Sprintf("/event/%d/incidents", match.EventID)
					if incBody, err := getMatchData(incURL, func(body []byte) error {
						var parsed SofaIncidentResponse
						if err := json.Unmarshal(body, &parsed); err != nil {
							return err
						}
						if parsed.Incidents == nil {
							return fmt.Errorf("brak listy incydentów meczu %d", match.EventID)
						}
						return nil
					}); err == nil {
						var incRes SofaIncidentResponse
						if decodeHistoryJSON(incBody, &incRes, "incydentów") == nil {
							subKnown = true
							for _, inc := range incRes.Incidents {
								if inc.IncidentType == "substitution" && inc.PlayerOut.ID == playerID {
									subPlayerID = inc.PlayerIn.ID
									break
								}
							}
							if subPlayerID > 0 {
								subURL := fmt.Sprintf("/event/%d/player/%d/statistics", match.EventID, subPlayerID)
								if subBody, err := getMatchData(subURL, validateSofaHistoryStats); err == nil {
									var subStatRes SofaPlayerMatchStatResponse
									if decodeHistoryJSON(subBody, &subStatRes, "statystyk zmiennika") == nil {
										subStats = subStatRes.Statistics
									}
								}
							}
						}
					}
				}

				// A map can also recover shot zones when lineups have participation
				// statistics but the player-statistics endpoint lacks shot totals.
				var playerZone, subZone zoneShots
				var matchShots []ShotCoordinate
				zonesKnown := (pStat.Statistics.fullFootballCoverage() || pStat.Statistics.metricPresent("totalShots")) && pStat.Statistics.TotalShots == 0
				subZonesKnown := subPlayerID == 0 || ((subStats.fullFootballCoverage() || subStats.metricPresent("totalShots")) && subStats.TotalShots == 0)

				if !zonesKnown || !subZonesKnown {
					smURL := fmt.Sprintf("/event/%d/shotmap", match.EventID)
					if smBody, err := getMatchData(smURL, func(body []byte) error {
						var parsed SofaShotmapResponse
						if err := json.Unmarshal(body, &parsed); err != nil {
							return err
						}
						shots := parsed.PlayerShots(playerID, subPlayerID)
						playerShots, subShots := 0, 0
						for _, shot := range shots {
							if shot.IsSub {
								subShots++
							} else {
								playerShots++
							}
						}
						if parsed.Shotmap == nil || playerShots < pStat.Statistics.TotalShots || subShots < subStats.TotalShots {
							return fmt.Errorf("niekompletna mapa strzałów gracza %d lub zmiennika w meczu %d", playerID, match.EventID)
						}
						if !pStat.Statistics.metricPresent("totalShots") || (subPlayerID > 0 && !subStats.metricPresent("totalShots")) {
							for _, shot := range parsed.Shotmap {
								if shot.Player.ID <= 0 {
									return fmt.Errorf("mapa strzałów bez identyfikacji strzelców")
								}
							}
						}
						return nil
					}); err == nil {
						var smRes SofaShotmapResponse
						if decodeHistoryJSON(smBody, &smRes, "mapy strzałów") == nil {
							zonesKnown, subZonesKnown = true, true
							matchShots = smRes.PlayerShots(playerID, subPlayerID)
							if len(matchShots) == 0 && (pStat.Statistics.TotalShots > 0 || subStats.TotalShots > 0) {
								incomplete.Store(true)
								client.warn(fmt.Sprintf("Brak mapy strzałów gracza %d w meczu %d pomimo dodatniej liczby strzałów", playerID, match.EventID))
							}
							for _, shot := range matchShots {
								zone := &playerZone
								if shot.IsSub {
									zone = &subZone
								}
								if shot.InBox {
									zone.inside++
									if shot.OnTarget {
										zone.insideTarget++
									}
								} else {
									zone.outside++
									if shot.OnTarget {
										zone.outsideTarget++
									}
								}
							}
							if !pStat.Statistics.metricPresent("totalShots") {
								if pStat.Statistics.Present == nil {
									pStat.Statistics.Present = map[string]bool{}
								}
								pStat.Statistics.TotalShots = playerZone.inside + playerZone.outside
								pStat.Statistics.Present["totalShots"] = true
							}
							if subPlayerID > 0 && !subStats.metricPresent("totalShots") {
								if subStats.Present == nil {
									subStats.Present = map[string]bool{}
								}
								subStats.TotalShots = subZone.inside + subZone.outside
								subStats.Present["totalShots"] = true
							}
						}
					} else {
						noteMatch(match, "brak kompletnej mapy strzałów; rynki stref strzału mają brak danych, nie zero")
					}
				}

				excludeBlockedShotsFromOnTarget(&pStat.Statistics, matchShots, false)
				excludeBlockedShotsFromOnTarget(&subStats, matchShots, true)

				rating := pStat.Statistics.Rating
				if rating == nil {
					rating = match.Rating
				}
				resultsChan <- matchStatResult{
					zonesKnown: zonesKnown, subZonesKnown: subZonesKnown,
					context:  match.Context,
					hasSub:   subPlayerID > 0,
					subKnown: subKnown,
					rating:   rating,
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
		for len(resultsChan) > 0 {
			collected = append(collected, <-resultsChan)
		}
		processed = end
		if client.ctx.Err() != nil || baseStatsFailure.Load() {
			break
		}
	}
	close(resultsChan)
	if client.ctx != nil && client.ctx.Err() != nil {
		return nil, client.ctx.Err()
	}

	for r := range resultsChan {
		collected = append(collected, r)
	}

	sort.Slice(collected, func(i, j int) bool {
		return collected[i].order < collected[j].order
	})

	for _, c := range collected {
		hist.Matches++
		hist.Ratings = append(hist.Ratings, c.rating)
		hist.MatchContexts = append(hist.MatchContexts, c.context)
		hist.EventIDs = append(hist.EventIDs, allPlayedMatches[c.order].EventID)
		hist.MatchStartedAt = append(hist.MatchStartedAt, time.Unix(allPlayedMatches[c.order].StartTS, 0).UTC())
		hist.Minutes = append(hist.Minutes, c.stats.MinutesPlayed)
		hist.Positions = append(hist.Positions, c.pos)
		hist.Opponents = append(hist.Opponents, c.opp)
		hist.Dates = append(hist.Dates, c.date)
		hist.MatchShots = append(hist.MatchShots, c.shots)

		// The tackles market uses won tackles only. Missing wonTackle is not
		// an observed zero and must never fall back to totalTackle.
		tackles, subTackles := c.stats.WonTackle, c.subStats.WonTackle
		for market, field := range map[CanonicalMarket]string{MarketTackles: tacklesStatsField, MarketPasses: "totalPass", MarketAccuratePasses: "accuratePass", MarketShotsTotal: "totalShots", MarketShotsOnTarget: "onTargetScoringAttempt", MarketShotsOffTarget: "shotOffTarget", MarketShotsBlocked: "blockedScoringAttempt", MarketFoulsCommitted: "fouls", MarketFoulsDrawn: "wasFouled", MarketInterceptions: "interceptionWon", MarketOffsides: "totalOffside", MarketDispossessed: "dispossessed"} {
			valid := c.stats.metricPresent(field)
			subValid := c.subStats.metricPresent(field)
			if market != MarketTackles && market != MarketPasses && market != MarketAccuratePasses {
				valid = valid || c.stats.fullFootballCoverage()
				subValid = subValid || c.subStats.fullFootballCoverage()
			}
			if market == MarketShotsOnTarget || market == MarketShotsOffTarget || market == MarketShotsBlocked {
				valid = valid || (c.stats.metricPresent("totalShots") && c.stats.TotalShots == 0)
				subValid = subValid || (c.subStats.metricPresent("totalShots") && c.subStats.TotalShots == 0)
			}
			hist.MetricAvailability[market] = append(hist.MetricAvailability[market], valid)
			hist.SuperSubAvailability[market] = append(hist.SuperSubAvailability[market], valid && c.subKnown && (!c.hasSub || subValid))
		}
		for _, market := range []CanonicalMarket{MarketShotsInsideBox, MarketShotsOutsideBox, MarketShotsOnTargetInsideBox, MarketShotsOnTargetOutsideBox} {
			hist.MetricAvailability[market] = append(hist.MetricAvailability[market], c.zonesKnown)
			hist.SuperSubAvailability[market] = append(hist.SuperSubAvailability[market], c.zonesKnown && c.subKnown && (!c.hasSub || c.subZonesKnown))
		}
		hist.MetricValues[MarketPasses] = append(hist.MetricValues[MarketPasses], c.stats.TotalPass)
		hist.MetricValues[MarketAccuratePasses] = append(hist.MetricValues[MarketAccuratePasses], c.stats.AccuratePass)
		hist.MetricValuesWithSuperSub[MarketPasses] = append(hist.MetricValuesWithSuperSub[MarketPasses], c.stats.TotalPass+c.subStats.TotalPass)
		hist.MetricValuesWithSuperSub[MarketAccuratePasses] = append(hist.MetricValuesWithSuperSub[MarketAccuratePasses], c.stats.AccuratePass+c.subStats.AccuratePass)

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

	hist.HistoryExhausted = historyExhausted && processed == len(allPlayedMatches)
	for _, match := range allPlayedMatches {
		if note := issues[match.EventID]; note != "" {
			hist.HistoryNotes = append(hist.HistoryNotes, note)
		}
	}
	if hist.Matches < matchLimit && !hist.HistoryExhausted && !baseStatsFailure.Load() && !retryableFailure.Load() && client.ctx.Err() == nil && scanBudget < 200 {
		nextBudget := scanBudget + 10
		if nextBudget > 200 {
			nextBudget = 200
		}
		return fetchSofaPlayerHistoryScan(client, playerID, matchLimit, allowedTournamentIDs, nextBudget)
	}
	if hist.Matches < matchLimit && !hist.HistoryExhausted {
		incomplete.Store(true)
		hist.HistoryNotes = append(hist.HistoryNotes, "Próbka jest krótsza od docelowej; nie potwierdzono końca historii lub wystąpił błąd źródła.")
	}
	hist.Incomplete = incomplete.Load()
	log.Printf("[SOFA-PLAYER] Historia player_id=%d mecze=%d niepelna=%t dane_z_cache=%d braki_z_cache=%d nowe_odpowiedzi=%d", playerID, hist.Matches, hist.Incomplete, cachedResponses.Load(), cachedMissingResponses.Load(), fetchedResponses.Load())
	if hist.Matches == 0 && !baseStatsFailure.Load() && !hist.Incomplete {
		return nil, recordSofaStatsMissing(client, cacheKey, playerID)
	}
	if hist.Matches > 0 {
		clearSofaStatsMissing(cacheKey)
	}
	if hist.Matches > 0 && !retryableFailure.Load() && !baseStatsFailure.Load() && client.ctx.Err() == nil && client.archiveError() == nil && sofaCooldownError() == nil {
		savePlayerCache(cacheKey, hist, client.statsFixture)
	}
	return hist, nil
}

func selectSofaMarketPlayers(squad []SofaPlayerDetails, targetPlayerNames []string) []SofaPlayerDetails {
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

	playersToFetch := make([]SofaPlayerDetails, 0, len(matchedPlayerIDs))
	for _, p := range matchedPlayerIDs {
		playersToFetch = append(playersToFetch, p)
	}
	sort.Slice(playersToFetch, func(i, j int) bool { return playersToFetch[i].ID < playersToFetch[j].ID })
	return playersToFetch
}

func fetchSofaPlayerHistories(client *ResilientClient, teamID, teamDisplayName string, playersToFetch []SofaPlayerDetails, matchLimit int, tournamentIDs string) ([]*PlayerStatHistory, error) {
	client.report(0, len(playersToFetch))

	var finalHistories []*PlayerStatHistory
	var mu sync.Mutex
	var wg sync.WaitGroup
	var unavailable atomic.Int32
	playerConcurrency := 2
	if client.statsPrefetch {
		playerConcurrency = 1
	}
	sem := make(chan struct{}, playerConcurrency)

schedulePlayers:
	for _, p := range playersToFetch {
		// Reserve the slot before starting the goroutine, preserving queue order.
		select {
		case sem <- struct{}{}:
		case <-client.ctx.Done():
			break schedulePlayers
		}
		wg.Add(1)
		go func(player SofaPlayerDetails) {
			defer wg.Done()
			defer func() { <-sem }()
			if client.background && !sofaCooldownUntil().IsZero() {
				return
			}
			defer client.report(1, 0)
			client := client.withScope("zawodnik-" + player.Name + "-druzyna-" + teamDisplayName + "-proba-" + strconv.Itoa(matchLimit))
			client.statsTeamID = teamID

			h, err := fetchSofaPlayerCompleteHistory(client, player.ID, matchLimit, tournamentIDs)
			if err != nil {
				if errors.Is(err, errSofaStatsUnavailable) {
					unavailable.Add(1)
					playerStatsCacheMu.Lock()
					pending := playerStatsCache[sofaPlayerHistoryKey(client, player.ID, matchLimit, tournamentIDs)].ConsecutiveMissing < sofaStatsMissingLimit
					playerStatsCacheMu.Unlock()
					client.state.mu.Lock()
					client.state.statsUnavailable++
					if pending {
						client.state.statsMissingPending++
					}
					client.state.mu.Unlock()
					client.warn("Brak historii ze statystykami SofaScore: " + player.Name)
					return
				}
				client.warn("Historia " + player.Name + ": " + err.Error())
				return
			}
			if h == nil || h.Matches == 0 {
				client.warn("Brak zakończonych meczów ze statystykami: " + player.Name)
				return
			}
			h.FullName = player.Name
			h.TeamID = teamID
			h.TeamName = teamDisplayName
			if err := archiveResponse(client.ctx, "historia-zawodnika-statystyki-proba-"+strconv.Itoa(h.Matches)+"-meczow", nil, "", map[string]any{"history": h, "fetchedAt": h.DataFetchedAt, "reusedFromCache": h.FromCache, "requestedMatchLimit": matchLimit, "tournamentIds": tournamentIDs}, nil); err != nil {
				client.recordArchiveError(err)
				return
			}

			mu.Lock()
			finalHistories = append(finalHistories, h)
			mu.Unlock()
		}(p)
	}

	wg.Wait()
	log.Printf("[COMPREHENSIVE] Drużyna %s: historie %d/%d", teamDisplayName, len(finalHistories), len(playersToFetch))
	if err := errors.Join(client.ctx.Err(), client.archiveError()); err != nil {
		return finalHistories, err
	}
	if client.background {
		if err := sofaCooldownError(); err != nil {
			return finalHistories, err
		}
	}
	if client.statsPrefetch && len(finalHistories)+int(unavailable.Load()) != len(playersToFetch) {
		return finalHistories, fmt.Errorf("pobrano historie %d/%d dopasowanych zawodników", len(finalHistories), len(playersToFetch))
	}
	sort.Slice(finalHistories, func(i, j int) bool { return finalHistories[i].ID < finalHistories[j].ID })
	return finalHistories, nil
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

type sofaMatchLineup struct {
	Details                  *SofaLineupResponse
	Home, Away               map[string]bool
	HomePlayers, AwayPlayers []SofaPlayerDetails
	Confirmed                bool
}

func fetchSofaLineups(client *ResilientClient, eventID string) (sofaMatchLineup, error) {
	if eventID == "" {
		return sofaMatchLineup{}, fmt.Errorf("brak ID meczu do pobrania składów")
	}
	log.Printf("[SOFA-LINEUP] Pobieranie składów dla meczu eventID=%s", eventID)
	body, err := client.GetSofa(fmt.Sprintf("/event/%s/lineups", eventID))
	if err != nil {
		log.Printf("[SOFA-LINEUP] Błąd pobierania składów: %v", err)
		return sofaMatchLineup{}, err
	}

	var res SofaLineupResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return sofaMatchLineup{}, fmt.Errorf("odczyt składów: %w", err)
	}
	log.Printf("[SOFA-LINEUP] RESPONSE eventID=%s confirmed_present=%v home_players=%d away_players=%d", eventID, res.Confirmed != nil, len(res.Home.Players), len(res.Away.Players))
	if res.Confirmed == nil {
		return sofaMatchLineup{}, fmt.Errorf("odpowiedź składów nie zawiera statusu confirmed")
	}

	homeMap := make(map[string]bool)
	awayMap := make(map[string]bool)

	regLineup := func(m map[string]bool, p SofaLineupPlayer) {
		if p.Substitute == nil {
			return // Missing classification must not default to a starting player.
		}
		isStarter := !*p.Substitute
		if p.Player.ID > 0 {
			m["player:"+strconv.Itoa(p.Player.ID)] = isStarter
			position := p.Position
			if position == "" {
				position = p.Player.Position
			}
			client.state.mu.Lock()
			if client.state.lineupRoles == nil {
				client.state.lineupRoles = make(map[int]string)
			}
			client.state.lineupRoles[p.Player.ID] = position
			client.state.mu.Unlock()
		}
		for _, raw := range []string{p.Player.Name, p.Player.ShortName, strings.ReplaceAll(p.Player.Slug, "-", " ")} {
			norm := NormalizePlayerName(raw)
			if norm != "" {
				m[norm] = isStarter
			}
		}
	}

	for _, p := range res.Home.Players {
		regLineup(homeMap, p)
	}
	for _, p := range res.Away.Players {
		regLineup(awayMap, p)
	}

	if len(homeMap) == 0 && len(awayMap) == 0 {
		return sofaMatchLineup{}, fmt.Errorf("brak zawodników z oznaczeniem substitute w składach (confirmed=%v)", *res.Confirmed)
	}
	log.Printf("[SOFA-LINEUP] Składy załadowane (potwierdzone: %v) - Gospodarze: %d graczy, Goście: %d graczy", *res.Confirmed, len(res.Home.Players), len(res.Away.Players))
	result := sofaMatchLineup{Home: homeMap, Away: awayMap, Confirmed: *res.Confirmed, Details: &res}
	for _, p := range res.Home.Players {
		if p.Player.ID > 0 {
			result.HomePlayers = append(result.HomePlayers, p.Player)
		}
	}
	for _, p := range res.Away.Players {
		if p.Player.ID > 0 {
			result.AwayPlayers = append(result.AwayPlayers, p.Player)
		}
	}
	return result, nil
}

type sofaMatchPlayerData struct {
	Home, Away []*PlayerStatHistory
	Lineup     sofaMatchLineup
}

// Both teams finish each phase before any history from the next phase starts.
// Lineup IDs let XI and bench histories bypass full squad discovery.
// The synchronous preview hook may finish and price XI context before reserves.
func fetchSofaMatchPlayers(client *ResilientClient, homeID, awayID, homeName, awayName, eventID string, reversed bool, matchLimit int, tournamentIDs string, targets []string, preview func(sofaMatchPlayerData, string, string) error) (sofaMatchPlayerData, error) {
	data := sofaMatchPlayerData{}
	if client.progress != nil {
		client.progress("lineups", "Odczytywanie predicted / confirmed XI", 0, 0)
	}
	lineup, err := fetchSofaLineups(client, eventID)
	if err != nil {
		client.warn("SofaScore: nie udało się odczytać składów; pobieranie zawodników bez priorytetu XI: " + err.Error())
	} else {
		if reversed {
			lineup.Home, lineup.Away = lineup.Away, lineup.Home
			lineup.HomePlayers, lineup.AwayPlayers = lineup.AwayPlayers, lineup.HomePlayers
			if lineup.Details != nil {
				copy := *lineup.Details
				copy.Home, copy.Away = copy.Away, copy.Home
				lineup.Details = &copy
			}
		}
		data.Lineup = lineup
		if !lineup.Confirmed {
			client.warn("SofaScore: wykorzystano przewidywane składy (XI?), jeszcze niepotwierdzone")
		}
		if len(lineup.Home) == 0 || len(lineup.Away) == 0 {
			client.warn("SofaScore: brak składu jednej z drużyn")
		}
	}
	if err := errors.Join(client.ctx.Err(), client.archiveError()); err != nil {
		return data, err
	}
	if homeName == "" {
		homeName = "Gospodarze"
	}
	if awayName == "" {
		awayName = "Goście"
	}
	type teamPlan struct {
		id, name  string
		lineup    map[string]bool
		players   []SofaPlayerDetails
		attempted map[int]bool
		histories *[]*PlayerStatHistory
	}
	plans := []teamPlan{
		{homeID, homeName, data.Lineup.Home, data.Lineup.HomePlayers, make(map[int]bool), &data.Home},
		{awayID, awayName, data.Lineup.Away, data.Lineup.AwayPlayers, make(map[int]bool), &data.Away},
	}
	phase := func(stage, message, nextStage, nextMessage string, lists [2][]SofaPlayerDetails) error {
		count := len(lists[0]) + len(lists[1])
		if client.progress != nil {
			client.progress(stage, message, 0, 0)
		}
		log.Printf("[SOFA-PLAYERS] PHASE stage=%s home=%d away=%d", stage, len(lists[0]), len(lists[1]))
		var wg sync.WaitGroup
		var phaseErrors [2]error
		fetch := func(i int) {
			plan := &plans[i]
			for _, p := range lists[i] {
				plan.attempted[p.ID] = true
			}
			histories, err := fetchSofaPlayerHistories(client.withScope(stage+"-druzyna-"+plan.id), plan.id, plan.name, lists[i], matchLimit, tournamentIDs)
			*plan.histories = append(*plan.histories, histories...)
			phaseErrors[i] = err
		}
		for i := range plans {
			if client.statsPrefetch {
				fetch(i)
				if phaseErrors[i] != nil {
					break
				}
			} else {
				wg.Add(1)
				go func(i int) { defer wg.Done(); fetch(i) }(i)
			}
		}
		wg.Wait()
		if err := errors.Join(phaseErrors[0], phaseErrors[1], client.ctx.Err(), client.archiveError()); err != nil {
			return err
		}
		if preview != nil && count > 0 && len(data.Home)+len(data.Away) > 0 {
			return preview(data, nextStage, nextMessage)
		}
		return nil
	}
	var starters, bench [2][]SofaPlayerDetails
	for i, plan := range plans {
		for _, player := range selectSofaMarketPlayers(plan.players, targets) {
			isStarter, classified := plan.lineup["player:"+strconv.Itoa(player.ID)]
			if !classified {
				continue
			}
			if isStarter {
				starters[i] = append(starters[i], player)
			} else {
				bench[i] = append(bench[i], player)
			}
		}
	}
	if err := phase("statistics-xi", "Pobieranie statystyk predicted / confirmed XI", "matchup-xi", "XI gotowe — pobieranie kontekstu i wyceny", starters); err != nil {
		return data, err
	}
	if err := phase("statistics-bench", "Pobieranie statystyk ławki", "statistics-other", "Uzupełnianie pozostałych zawodników z oferty", bench); err != nil {
		return data, err
	}
	if client.progress != nil {
		client.progress("statistics-other", "Uzupełnianie kadry i pozostałych zawodników z oferty", 0, 0)
	}
	var remaining [2][]SofaPlayerDetails
	var squadErrors [2]error
	var wg sync.WaitGroup
	prepare := func(i int) {
		plan := &plans[i]
		squad, name, err := fetchSofaTeamSquad(client.withScope("kadra-druzyna-"+plan.id), plan.id)
		if err != nil {
			squadErrors[i] = err
			return
		}
		if name != "" && name != plan.id {
			plan.name = name
		}
		// Retain fixture players even when the roster omits them.
		seen := make(map[int]bool)
		for _, p := range squad {
			seen[p.ID] = true
		}
		for _, p := range plan.players {
			if !seen[p.ID] {
				squad = append(squad, p)
				seen[p.ID] = true
			}
		}
		matched := selectSofaMarketPlayers(squad, targets)
		if len(matched) == 0 && !client.statsPrefetch && len(plan.attempted) == 0 {
			matched = squad
		}
		for _, p := range matched {
			if !plan.attempted[p.ID] {
				remaining[i] = append(remaining[i], p)
			}
		}
		for _, history := range *plan.histories {
			history.TeamName = plan.name
		}
	}
	for i := range plans {
		if client.statsPrefetch {
			prepare(i)
			if squadErrors[i] != nil {
				break
			}
		} else {
			wg.Add(1)
			go func(i int) { defer wg.Done(); prepare(i) }(i)
		}
	}
	wg.Wait()
	if err := errors.Join(squadErrors[0], squadErrors[1], client.ctx.Err(), client.archiveError()); err != nil {
		return data, err
	}
	if err := phase("statistics-other", "Pobieranie statystyk pozostałych zawodników", "calculation", "Uzupełnianie wyceny pozostałych zawodników", remaining); err != nil {
		return data, err
	}
	return data, nil
}

// ==========================================
// 5. PARSERY BUKMACHERSKIE I MAPOWANIE RYNKÓW
// ==========================================

var numRe = regexp.MustCompile(`(\d+(?:[.,]\d+)?)`)

type offerAudit struct {
	Reasons           map[string]int       `json:"reasons"`
	UnlistedOmissions int                  `json:"unlistedOmissions"`
	Bookmaker         string               `json:"bookmaker"`
	Coverage          string               `json:"coverage"`
	FullState         *bool                `json:"fullState,omitempty"`
	RawMarkets        int                  `json:"rawMarkets"`
	RawSelections     int                  `json:"rawSelections"`
	ParsedSelections  int                  `json:"parsedSelections"`
	UsableSelections  int                  `json:"usableSelections"`
	Omissions         []offerAuditOmission `json:"omissions"`
	Warnings          []string             `json:"warnings"`
}

type offerAuditOmission struct {
	Label  string `json:"label"`
	Reason string `json:"reason"`
	Count  int    `json:"count"`
}

type offerAuditContextKey struct{}

func parserAudit(book string, audits []*offerAudit) *offerAudit {
	audit := &offerAudit{}
	if len(audits) > 0 && audits[0] != nil {
		audit = audits[0]
	}
	*audit = offerAudit{Bookmaker: book, Coverage: "structured", Reasons: map[string]int{}, Omissions: []offerAuditOmission{}, Warnings: []string{}}
	return audit
}

func (audit *offerAudit) omit(label, reason string, count int) {
	if count == 0 {
		return
	}
	audit.Reasons[reason] += count
	for i := range audit.Omissions {
		if audit.Omissions[i].Label == label && audit.Omissions[i].Reason == reason {
			audit.Omissions[i].Count += count
			return
		}
	}
	if len(audit.Omissions) < 200 {
		audit.Omissions = append(audit.Omissions, offerAuditOmission{Label: label, Reason: reason, Count: count})
	} else {
		audit.UnlistedOmissions += count
	}
}

func finishParserAudit(audit *offerAudit, props []NormalizedProp) {
	audit.ParsedSelections = len(props)
	audit.UsableSelections = len(usableMatchProps(props))
	if audit.UsableSelections < len(props) {
		audit.Warnings = append(audit.Warnings, "Część selekcji odrzucono z powodu niepoprawnego kursu, linii lub nazwy zawodnika.")
	}
	sort.Slice(audit.Omissions, func(i, j int) bool {
		if audit.Omissions[i].Reason != audit.Omissions[j].Reason {
			return audit.Omissions[i].Reason < audit.Omissions[j].Reason
		}
		return audit.Omissions[i].Label < audit.Omissions[j].Label
	})
	log.Printf("[OFFER-AUDIT] bookmaker=%s coverage=%s markets=%d selections=%d parsed=%d usable=%d omitted_groups=%d", audit.Bookmaker, audit.Coverage, audit.RawMarkets, audit.RawSelections, audit.ParsedSelections, audit.UsableSelections, len(audit.Omissions))
}

func parsePropLine(text string) (float64, bool) {
	text = strings.ReplaceAll(strings.TrimSpace(text), ",", ".")
	match := numRe.FindStringSubmatch(text)
	if len(match) < 2 {
		return 0, false
	}
	line, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, false
	}
	if strings.Contains(text, match[1]+"+") {
		line -= .5
	}
	return line, line >= 0
}

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
	if strings.Contains(low, "odbior") && (strings.Contains(low, "przegran") || strings.Contains(low, "nieskuteczn") || strings.Contains(low, "nieudan")) {
		return true
	}
	compact := strings.NewReplacer(" ", "", "-", "", "_", "").Replace(low)
	for _, marker := range []string{"tackleslost", "losttackle", "unsuccessfultackle", "failedtackle"} {
		if strings.Contains(compact, marker) {
			return true
		}
	}
	won := strings.Contains(compact, "tackleswon") || strings.Contains(compact, "tacklewon") || strings.Contains(compact, "wontackle") || strings.Contains(compact, "successfultackle")
	if strings.Contains(compact, "totaltackle") && !won {
		return true // Total attempts are a different market from won tackles.
	}
	markers := []string{
		"lewa noga", "lewej nogi", "prawa noga", "prawej nogi",
		"glowa", "glowka", "head",
		"kluczow", "key pass", "cross", "dlugich podan", "long pass", "celnych dlugich",
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
	if strings.Contains(sd, "podan") || strings.Contains(s, "pass") {
		if isUnsupportedMethodOrZone(raw) {
			return "", false
		}
		if strings.Contains(sd, "celn") || strings.Contains(s, "accurate") || strings.Contains(s, "completed") {
			return MarketAccuratePasses, true
		}
		return MarketPasses, true
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

// Direction markers describe the outcome, not fragments of a player's name
// (for example, Gundersen contains "under"). Keep one-sided ladders as overs.
func propSelectionDirection(selection, playerName string) (isUnder, unambiguous bool) {
	text := removeDiacritics(strings.ToLower(selection))
	if name := removeDiacritics(strings.ToLower(strings.TrimSpace(playerName))); name != "" {
		text = strings.ReplaceAll(text, name, " ")
	}
	under, over := false, false
	for _, word := range strings.FieldsFunc(text, func(r rune) bool { return !unicode.IsLetter(r) }) {
		switch word {
		case "under", "ponizej":
			under = true
		case "over", "powyzej":
			over = true
		}
	}
	return under, !(under && over)
}

func ParseSuperbetProps(rawJSON string, audits ...*offerAudit) ([]NormalizedProp, error) {
	audit := parserAudit("SUPERBET", audits)
	log.Printf("[PARSER-SB] Rozpoczynam parsowanie oferty Superbet...")
	var resp struct {
		FullState *bool `json:"isFullState"`
		Markets   []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			Odds []struct {
				UUID             string            `json:"uuid"`
				MarketUUID       string            `json:"marketUuid"`
				SuperSubEligible *bool             `json:"superSubEligible"`
				Name             string            `json:"name"`
				Price            float64           `json:"price"`
				Status           string            `json:"status"`
				Specifiers       map[string]string `json:"specifiers"`
			} `json:"odds"`
		} `json:"markets"`
	}
	if err := json.Unmarshal([]byte(rawJSON), &resp); err != nil {
		log.Printf("[PARSER-SB] Błąd unmarshalu JSON: %v", err)
		return nil, err
	}

	var props []NormalizedProp
	audit.FullState = resp.FullState
	if resp.FullState != nil && !*resp.FullState {
		return nil, fmt.Errorf("Superbet: odpowiedź jest aktualizacją częściową, a nie pełną ofertą")
	}
	audit.RawMarkets = len(resp.Markets)
	for _, m := range resp.Markets {
		audit.RawSelections += len(m.Odds)
		if isHalfOrPeriodMarket(m.Name) || isUnsupportedMethodOrZone(m.Name) {
			audit.omit(m.Name, "unsupported-period-or-method", len(m.Odds))
			continue
		}

		canon, ok := mapMarketCanonical(m.Name)
		if !ok {
			audit.omit(m.Name, "unsupported-market", len(m.Odds))
			continue
		}

		for _, odd := range m.Odds {
			if odd.Status != "ACTIVE" && odd.Status != "" {
				audit.omit(m.Name, "inactive-selection", 1)
				continue
			}
			if isHalfOrPeriodMarket(odd.Name) || isUnsupportedMethodOrZone(odd.Name) {
				audit.omit(m.Name, "unsupported-selection", 1)
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
				audit.omit(m.Name, "missing-player", 1)
				continue
			}

			line, lineKnown := 0.0, false
			if odd.Specifiers != nil {
				if l, exists := odd.Specifiers["total"]; exists {
					line, lineKnown = parsePropLine(l)
				}
			}
			if !lineKnown {
				line, lineKnown = parsePropLine(odd.Name)
			}
			if !lineKnown {
				audit.omit(m.Name, "missing-line", 1)
				continue
			}

			isUnder, directionKnown := propSelectionDirection(odd.Name, pName)
			if !directionKnown {
				audit.omit(m.Name, "ambiguous-selection-direction", 1)
				continue
			}

			props = append(props, NormalizedProp{
				MarketID:         m.ID,
				SelectionID:      odd.UUID,
				SuperSubEligible: odd.SuperSubEligible,
				Bookmaker:        BookmakerSuperbet,
				PlayerName:       pName,
				MarketType:       canon,
				MarketLabel:      m.Name,
				Line:             line,
				IsUnder:          isUnder,
				Odds:             odd.Price,
			})
		}
	}
	log.Printf("[PARSER-SB] Sukces: sparsowano %d propozycji zakładów z Superbet", len(props))
	finishParserAudit(audit, props)
	return props, nil
}

func ParseSTSProps(rawJSON string, audits ...*offerAudit) ([]NormalizedProp, error) {
	audit := parserAudit("STS", audits)
	log.Printf("[PARSER-STS] Rozpoczynam parsowanie oferty STS...")
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
		log.Printf("[PARSER-STS] Błąd: Brak obiektu P w odpowiedzi STS")
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
			audit.RawMarkets++
			mMap, _ := mVal.(map[string]any)
			linesMap, _ := mMap["l"].(map[string]any)
			if len(linesMap) == 0 {
				audit.omit(mID, "missing-market-details", 1)
			}

			for _, lVal := range linesMap {
				lMap, _ := lVal.(map[string]any)
				outcomeData, _ := lMap["o"].(map[string]any)
				audit.RawSelections += len(outcomeData)
				rawName, _ := lMap["n"].(string)
				if rawName == "" {
					audit.omit(mID, "missing-market-label", len(outcomeData))
					continue
				}

				cleanName := strings.ReplaceAll(rawName, "(z dogrywką)", "")
				cleanName = strings.ReplaceAll(cleanName, "(z dogrywka)", "")
				cleanName = strings.TrimSpace(cleanName)

				if isHalfOrPeriodMarket(cleanName) {
					audit.omit(cleanName, "unsupported-period", len(outcomeData))
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
					audit.omit(cleanName, "unsupported-or-unrecognized-player-market", len(outcomeData))
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
						audit.omit(cleanName, "unusable-odds", 1)
						continue
					}

					lowerLine := strings.ToLower(strings.TrimSpace(lineStr))
					isUnder, directionKnown := propSelectionDirection(lowerLine, pName)
					if !directionKnown {
						audit.omit(cleanName, "ambiguous-selection-direction", 1)
						continue
					}
					isUnder = isUnder || strings.HasPrefix(lowerLine, "-")

					lineVal, lineKnown := parsePropLine(lowerLine)
					cleanLineStr := strings.TrimSpace(lowerLine)
					if strings.HasSuffix(cleanLineStr, "+") {
						valStr := strings.TrimSuffix(cleanLineStr, "+")
						if parsed, err := strconv.ParseFloat(valStr, 64); err == nil {
							lineVal = parsed - 0.5
							lineKnown = true
							isUnder = false
						}
					} else if m := numRe.FindStringSubmatch(cleanLineStr); len(m) > 1 {
						if parsed, err := strconv.ParseFloat(strings.ReplaceAll(m[1], ",", "."), 64); err == nil {
							lineVal = parsed
							lineKnown = true
						}
					}

					if !lineKnown {
						audit.omit(cleanName, "missing-line", 1)
						continue
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

	log.Printf("[PARSER-STS] Sukces: sparsowano %d propozycji zakładów z STS", len(props))
	finishParserAudit(audit, props)
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
	return parseProtoNodesDepth(buf, 0)
}

func parseProtoNodesDepth(buf []byte, depth int) []protoNode {
	if depth > 32 {
		return nil
	}
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
			if l > uint64(len(buf)-pos) {
				return nodes
			}
			lenVal := int(l)
			slice := buf[pos : pos+lenVal]
			pos += lenVal

			if utf8.Valid(slice) && !bytes.ContainsAny(slice, "\x00\x01\x02\x03\x04\x05") && len(bytes.TrimSpace(slice)) > 0 {
				nodes = append(nodes, protoNode{fieldNo: fieldNo, wireType: wireType, text: strings.TrimSpace(string(slice))})
			} else {
				nodes = append(nodes, protoNode{fieldNo: fieldNo, wireType: wireType, children: parseProtoNodesDepth(slice, depth+1)})
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

func ParseBetclicProps(rawBuf []byte, audits ...*offerAudit) []NormalizedProp {
	audit := parserAudit("BETCLIC", audits)
	audit.Coverage = "heuristic-protobuf"
	audit.Warnings = append(audit.Warnings, "Parser protobuf jest heurystyczny; liczby kandydatów nie dowodzą kompletności całej oferty. Porównaj odrzucone etykiety i surowe ramki z ofertą bukmachera.")
	log.Printf("[PARSER-BETCLIC] Rozpoczynam dekodowanie Protobuf Betclic (rozmiar: %d bajtów)...", len(rawBuf))
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
				if _, ok := mapMarketCanonical(t); ok {
					audit.RawMarkets++
					audit.omit(t, "detected-label-check-parser-coverage", 1)
				}
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
				audit.RawSelections++
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
							IsUnder:     dir == "poniżej",
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

	log.Printf("[PARSER-BETCLIC] Sukces: wyodrębniono %d unikalnych kursów z Betclic", len(unique))
	finishParserAudit(audit, unique)
	return unique
}

// ==========================================
// 6. ADAPTACYJNY FUZZY MATCHING I STATYSTYKA
// ==========================================

func sampleIndexes[T any](values []T, indexes []int) []T {
	if len(values) == 0 {
		return nil
	}
	result := make([]T, len(indexes))
	for i, index := range indexes {
		if index < len(values) {
			result[i] = values[index]
		}
	}
	return result
}

// Keep all sample metadata aligned while excluding unknown metric values.
// Legacy evidence without masks retains its original interpretation.
func playerMetricHistoryNotes(source *PlayerStatHistory, market CanonicalMarket) []string {
	notes := append([]string{}, source.HistoryNotes...)
	if mask, ok := source.MetricAvailability[market]; ok {
		missing := 0
		for i := range source.MetricValues[market] {
			if i < len(mask) && mask[i] {
				continue
			}
			missing++
			if missing > 5 {
				continue
			}
			date, opponent := "?", "?"
			if i < len(source.Dates) {
				date = source.Dates[i]
			}
			if i < len(source.Opponents) {
				opponent = source.Opponents[i]
			}
			notes = append(notes, fmt.Sprintf("%s · %s: brak potwierdzonych danych dla rynku %s; pominięto, bez zastępowania zerem.", date, opponent, market))
		}
		if missing > 5 {
			notes = append(notes, fmt.Sprintf("Dalsze %d występy również nie mają potwierdzonych danych tego rynku.", missing-5))
		}
	}
	if len(notes) > 12 {
		notes = append(notes[:11], fmt.Sprintf("Dalsze uwagi do historii: %d; pełny zapis znajduje się w archiwum i logach.", len(notes)-11))
	}
	return notes
}

func metricHistory(source *PlayerStatHistory, market CanonicalMarket) *PlayerStatHistory {
	result := *source
	values := source.MetricValues[market]
	availability, masked := source.MetricAvailability[market]
	indexes := []int{}
	for i := range values {
		if !masked || (i < len(availability) && availability[i]) {
			indexes = append(indexes, i)
		}
	}
	result.Matches = len(indexes)
	result.MetricValues = map[CanonicalMarket][]int{market: sampleIndexes(values, indexes)}
	result.MetricValuesWithSuperSub = map[CanonicalMarket][]int{}
	subValid := len(source.MetricValuesWithSuperSub[market]) == len(values)
	if mask, ok := source.SuperSubAvailability[market]; ok {
		for _, i := range indexes {
			subValid = subValid && i < len(mask) && mask[i]
		}
	}
	if subValid {
		result.MetricValuesWithSuperSub[market] = sampleIndexes(source.MetricValuesWithSuperSub[market], indexes)
	}
	result.Ratings = sampleIndexes(source.Ratings, indexes)
	result.MatchContexts = sampleIndexes(source.MatchContexts, indexes)
	result.EventIDs = sampleIndexes(source.EventIDs, indexes)
	result.MatchStartedAt = sampleIndexes(source.MatchStartedAt, indexes)
	result.Minutes = sampleIndexes(source.Minutes, indexes)
	result.Positions = sampleIndexes(source.Positions, indexes)
	result.Opponents = sampleIndexes(source.Opponents, indexes)
	result.Dates = sampleIndexes(source.Dates, indexes)
	result.MatchShots = sampleIndexes(source.MatchShots, indexes)
	return &result
}

func NormalizePlayerName(s string) string {
	s = removeDiacritics(norm.NFKD.String(s))
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

// The overall model retains weighted hits. Standings or form supply easier/
// harder matchup subsets, without changing the published overall prediction.
const fairModelVersion = "context-hit-v10-history-completeness"

type PlayerMatchContext struct {
	TeamID     int    `json:"teamId"`
	OpponentID int    `json:"opponentId"`
	LeagueID   int    `json:"leagueId"`
	SeasonID   int    `json:"seasonId"`
	Venue      string `json:"venue"`
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

type opponentTeamStrength struct {
	PPG     float64 `json:"ppg"`
	Matches int     `json:"matches"`
	Group   int     `json:"group"`
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

type SofaFlowScore struct {
	Current    *int `json:"current"`
	Normaltime *int `json:"normaltime"`
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

type FlowIncident struct {
	ID        int64             `json:"id"`
	Type      string            `json:"incidentType"`
	Class     string            `json:"incidentClass"`
	Time      int               `json:"time"`
	AddedTime int               `json:"addedTime"`
	IsHome    *bool             `json:"isHome"`
	HomeScore *int              `json:"homeScore"`
	AwayScore *int              `json:"awayScore"`
	Rescinded bool              `json:"rescinded"`
	Player    SofaPlayerDetails `json:"player"`
	PlayerIn  SofaPlayerDetails `json:"playerIn"`
	PlayerOut SofaPlayerDetails `json:"playerOut"`
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

const matchFlowVersion = 1

var matchFlowRevisionPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func flowClock(incident FlowIncident) float64 {
	if incident.AddedTime > 0 && (incident.Time == 45 || incident.Time == 90) {
		return float64(incident.Time) + .5
	}
	return float64(incident.Time)
}

func flowState(home, away int) string {
	if home > away {
		return "HOME_LEAD"
	}
	if home < away {
		return "AWAY_LEAD"
	}
	return "DRAW"
}

func addFlowPoint(window *FlowWindow, value float64) {
	window.Mean = (window.Mean*float64(window.Points) + value) / float64(window.Points+1)
	window.Points++
	if value > 0 {
		window.Positive++
	} else if value < 0 {
		window.Negative++
	}
}

func flowPerspective(window FlowWindow, home bool) FlowWindow {
	if !home {
		window.Mean = -window.Mean
		window.Positive, window.Negative = window.Negative, window.Positive
	}
	return window
}

func flowCachePath(eventID, revision string) string {
	return filepath.Join(serverDataDir, "match_flow_cache", "mecz-"+eventID, "wersja-"+revision, "przebieg-meczu.json")
}

func readMatchFlow(eventID, revision string) (*HistoricalMatchFlow, error) {
	if !marketEventIDPattern.MatchString(eventID) || !matchFlowRevisionPattern.MatchString(revision) {
		return nil, fmt.Errorf("niepoprawny identyfikator przebiegu meczu")
	}
	raw, err := os.ReadFile(flowCachePath(eventID, revision))
	if err != nil {
		return nil, err
	}
	var flow HistoricalMatchFlow
	if err := json.Unmarshal(raw, &flow); err != nil {
		return nil, err
	}
	if flow.Version != matchFlowVersion || flow.EventID != eventID || flow.Revision != revision {
		return nil, fmt.Errorf("niezgodna wersja przebiegu meczu")
	}
	return &flow, nil
}

func (s *calendarServer) registerMatchFlowAPI(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/match-flow", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			apiError(w, 405, fmt.Errorf("wymagana metoda GET"))
			return
		}
		id, revision := r.URL.Query().Get("eventId"), r.URL.Query().Get("revision")
		if !marketEventIDPattern.MatchString(id) || !matchFlowRevisionPattern.MatchString(revision) {
			apiError(w, 400, fmt.Errorf("niepoprawne ID lub wersja przebiegu"))
			return
		}
		flow, err := readMatchFlow(id, revision)
		if err != nil {
			apiError(w, 404, fmt.Errorf("brak zapisanej wersji przebiegu meczu"))
			return
		}
		// Read-only: opening a chart cannot start new SofaScore requests.
		jsonResponse(w, 200, flow)
	})
}

func fetchHistoricalMatchFlow(client *ResilientClient, ref FlowMatchReference, homeID, awayID int, regulationScoreKnown bool, refreshRevision string) (*HistoricalMatchFlow, error) {
	indexPath := filepath.Join(serverDataDir, "match_flow_cache", "mecz-"+ref.EventID, "ostatnia-wersja.json")
	var index struct {
		Revision  string    `json:"revision"`
		FetchedAt time.Time `json:"fetchedAt"`
	}
	// A successfully saved finished match is immutable for subsequent analyses.
	if raw, err := os.ReadFile(indexPath); err == nil && json.Unmarshal(raw, &index) == nil {
		if flow, err := readMatchFlow(ref.EventID, index.Revision); err == nil {
			return flow, nil
		}
	}
	var graph struct {
		GraphPoints   []FlowGraphPoint `json:"graphPoints"`
		PeriodTime    int              `json:"periodTime"`
		PeriodCount   int              `json:"periodCount"`
		OvertimeCount *int             `json:"overtimeCount"`
	}
	_, err := getSofaContextJSON(client, "/event/"+ref.EventID+"/graph", 0, &graph, refreshRevision)
	if err != nil {
		return nil, err
	}
	if len(graph.GraphPoints) == 0 || graph.PeriodTime != 45 || graph.PeriodCount != 2 {
		return nil, fmt.Errorf("brak wykresu momentum dla regulaminowych dwóch połów")
	}
	var response struct {
		Incidents []FlowIncident `json:"incidents"`
	}
	_, err = getSofaContextJSON(client, "/event/"+ref.EventID+"/incidents", 0, &response, refreshRevision)
	if err != nil {
		return nil, err
	}
	flow := &HistoricalMatchFlow{Version: matchFlowVersion, EventID: ref.EventID, FetchedAt: time.Now().UTC(), HomeID: homeID, AwayID: awayID, HomeName: ref.HomeName, AwayName: ref.AwayName, HomeScore: ref.HomeScore, AwayScore: ref.AwayScore, TimelineValid: true, GraphPoints: []FlowGraphPoint{}, Reactions: []FlowReaction{}, Notes: []string{}}
	if graph.OvertimeCount != nil && *graph.OvertimeCount > 0 {
		flow.Notes = append(flow.Notes, "Podgląd obejmuje regulaminowe 90 minut; dogrywka i seria karnych są pominięte.")
		if !regulationScoreKnown {
			flow.TimelineValid = false
		}
	}
	sort.SliceStable(response.Incidents, func(i, j int) bool {
		a, b := response.Incidents[i], response.Incidents[j]
		if a.Time != b.Time {
			return a.Time < b.Time
		}
		if a.AddedTime != b.AddedTime {
			return a.AddedTime < b.AddedTime
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		if a.Type == "goal" {
			aKnown, bKnown := a.HomeScore != nil && a.AwayScore != nil, b.HomeScore != nil && b.AwayScore != nil
			if aKnown != bKnown {
				return aKnown
			}
			if aKnown {
				return *a.HomeScore+*a.AwayScore < *b.HomeScore+*b.AwayScore
			}
		}
		return false
	})
	home, away := 0, 0
	seen := map[int64]bool{}
	for _, incident := range response.Incidents {
		if incident.Rescinded || incident.Time < 0 || incident.Time > 90 || incident.AddedTime >= 999 {
			continue
		}
		if incident.ID > 0 && seen[incident.ID] {
			continue
		}
		if incident.ID > 0 {
			seen[incident.ID] = true
		}
		kind := ""
		switch incident.Type {
		case "goal":
			kind = "GOAL"
		case "substitution":
			kind = "SUBSTITUTION"
		case "card":
			if incident.Class == "red" || incident.Class == "yellowRed" {
				kind = "RED_CARD"
			} else if incident.Class == "yellow" {
				kind = "YELLOW_CARD"
			}
		}
		if kind == "" {
			continue
		}
		r := FlowReaction{Kind: kind, Minute: flowClock(incident), Clock: strconv.Itoa(incident.Time), IsHome: incident.IsHome, HomeScore: incident.HomeScore, AwayScore: incident.AwayScore, StateBefore: flowState(home, away)}
		if incident.AddedTime > 0 {
			r.Clock += "+" + strconv.Itoa(incident.AddedTime)
		}
		r.Clock += "′"
		r.Description = incident.Player.Name
		if kind == "SUBSTITUTION" {
			r.Description = incident.PlayerOut.Name + " → " + incident.PlayerIn.Name
		}
		if kind == "GOAL" {
			if incident.HomeScore == nil || incident.AwayScore == nil || *incident.HomeScore < home || *incident.AwayScore < away || (*incident.HomeScore-home)+(*incident.AwayScore-away) != 1 {
				flow.TimelineValid = false
			} else {
				// The score, not the scorer's team, resolves own goals correctly.
				isHome := *incident.HomeScore > home
				r.IsHome = &isHome
			}
			if incident.HomeScore != nil && incident.AwayScore != nil {
				home, away = *incident.HomeScore, *incident.AwayScore
			}
		}
		r.StateAfter = flowState(home, away)
		flow.Reactions = append(flow.Reactions, r)
	}
	if ref.HomeScore == nil || ref.AwayScore == nil || home != *ref.HomeScore || away != *ref.AwayScore {
		flow.TimelineValid = false
	}
	if !flow.TimelineValid {
		flow.Notes = append(flow.Notes, "Incydenty nie odtwarzają wyniku końcowego. Stany wyniku i agregaty reakcji są pominięte.")
	}
	sort.SliceStable(graph.GraphPoints, func(i, j int) bool { return graph.GraphPoints[i].Minute < graph.GraphPoints[j].Minute })
	lastMinute := -1.0
	for _, point := range graph.GraphPoints {
		if math.IsNaN(point.Minute) || math.IsInf(point.Minute, 0) || point.Minute <= 0 || point.Minute > 90.5 || point.Minute == lastMinute || math.IsNaN(point.Value) || math.IsInf(point.Value, 0) {
			continue
		}
		lastMinute = point.Minute
		point.State = "UNKNOWN"
		if flow.TimelineValid && point.Minute == math.Floor(point.Minute) {
			h, a := 0, 0
			ambiguous := false
			for _, r := range flow.Reactions {
				if r.Kind == "GOAL" {
					if r.Minute == point.Minute {
						ambiguous = true
					}
					if r.Minute < point.Minute && r.HomeScore != nil && r.AwayScore != nil {
						h, a = *r.HomeScore, *r.AwayScore
					}
				}
			}
			if !ambiguous {
				point.State = flowState(h, a)
			}
		}
		flow.GraphPoints = append(flow.GraphPoints, point)
	}
	if len(flow.GraphPoints) == 0 {
		return nil, fmt.Errorf("brak poprawnych punktów wykresu momentum")
	}
	for i := range flow.Reactions {
		r := &flow.Reactions[i]
		if r.Minute != math.Floor(r.Minute) {
			continue
		} // Added time is compressed into one ambiguous graph slot.
		start, end := 0.0, 45.0
		if r.Minute > 45 {
			start, end = 45, 90
		}
		beforeStart, afterEnd := math.Max(start, r.Minute-5), math.Min(end, r.Minute+5)
		confounded := false
		for j, other := range flow.Reactions {
			if i == j {
				continue
			}
			if other.Minute == r.Minute {
				// Only a batch of substitutions for the same side is one incident.
				if r.Kind != "SUBSTITUTION" || other.Kind != r.Kind || r.IsHome == nil || other.IsHome == nil || *r.IsHome != *other.IsHome {
					confounded = true
				}
				if r.Kind != "GOAL" && other.Kind == "GOAL" {
					r.StateBefore, r.StateAfter = "UNKNOWN", "UNKNOWN"
				}
				continue
			}
			if other.Minute < r.Minute && other.Minute > beforeStart {
				beforeStart = other.Minute
			}
			if other.Minute > r.Minute && other.Minute < afterEnd {
				afterEnd = other.Minute
			}
		}
		for _, point := range flow.GraphPoints {
			if point.Minute != math.Floor(point.Minute) || point.Minute <= start || point.Minute > end {
				continue
			}
			// Skip every incident minute; ordering within a minute is unknown.
			atIncident := false
			for _, other := range flow.Reactions {
				if other.Minute == point.Minute {
					atIncident = true
					break
				}
			}
			if atIncident {
				continue
			}
			if point.Minute >= beforeStart && point.Minute < r.Minute {
				addFlowPoint(&r.Before, point.Value)
			}
			if point.Minute > r.Minute && point.Minute <= afterEnd {
				addFlowPoint(&r.After, point.Value)
			}
		}
		r.Eligible = flow.TimelineValid && !confounded && r.IsHome != nil && r.Before.Points >= 3 && r.After.Points >= 3
	}
	flow.Notes = append(flow.Notes, "Momentum jest indeksem przebiegu gry, nie liczbą strzałów ani xG. Reakcje porównują do 5 minut przed i po incydencie; inne incydenty i przerwa skracają okno. To obserwacja, nie dowód przyczynowości.", "Minuta incydentu i ułamkowe punkty doliczonego czasu nie wchodzą do agregatów. Co najmniej 3 punkty po każdej stronie są wymagane do agregacji reakcji.", "Nakładające się incydenty w tej samej minucie są pomijane w agregatach reakcji; wyjątkiem jest pakiet zmian jednej drużyny, liczony jako jedno okno.")
	// Stable content identity excludes the observation time. A -> B -> A reuses A.
	identity := *flow
	identity.FetchedAt = time.Time{}
	raw, err := json.Marshal(identity)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	flow.Revision = fmt.Sprintf("%x", digest)
	path := flowCachePath(ref.EventID, flow.Revision)
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, err
	}
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := atomicJSON(path, flow); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	archiveDir := filepath.Join(archiveRoot, "przebieg-meczow", "mecz-"+ref.EventID, "wersja-"+flow.Revision)
	archivePath := filepath.Join(archiveDir, archiveName(ref.HomeName+"-vs-"+ref.AwayName)+"_przebieg-meczu.json.gz")
	if _, err := os.Stat(archivePath); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(archiveDir, 0750); err != nil {
			client.recordArchiveError(err)
			return nil, err
		}
		data, err := json.MarshalIndent(flow, "", "  ")
		if err != nil {
			return nil, err
		}
		if err := writeGzip(archivePath, data); err != nil {
			client.recordArchiveError(err)
			return nil, err
		}
	} else if err != nil {
		client.recordArchiveError(err)
		return nil, err
	}
	index.Revision, index.FetchedAt = flow.Revision, flow.FetchedAt
	if err := atomicJSON(indexPath, index); err != nil {
		return nil, err
	}
	return flow, nil
}

func fetchTeamFlowOverview(client *ResilientClient, teamID string, leagueID int, shared map[string]*HistoricalMatchFlow) *TeamFlowOverview {
	result := &TeamFlowOverview{Requested: 5, Matches: []FlowMatchReference{}, States: []FlowStateSummary{}, Reactions: []FlowReactionSummary{}, Notes: []string{}}
	var events SofaTeamEventsResponse
	refresh, err := getSofaTeamRefresh(client, teamID)
	if err == nil {
		err = json.Unmarshal(refresh.Events, &events)
	}
	if err != nil {
		result.Notes = append(result.Notes, "Nie udało się pobrać historii meczów: "+err.Error())
		return result
	}
	sort.Slice(events.Events, func(i, j int) bool { return events.Events[i].StartTS > events.Events[j].StartTS })
	stateTotals := map[string]*FlowStateSummary{}
	type reactionTotal struct {
		row     FlowReactionSummary
		matches map[string]bool
	}
	reactionTotals := map[string]*reactionTotal{}
	for _, event := range events.Events {
		if client.ctx.Err() != nil {
			break
		}
		if len(result.Matches) >= result.Requested {
			break
		}
		if event.Status.Code != 100 || event.StartTS <= 0 || (!client.statsFixture.StartTime.IsZero() && event.StartTS >= client.statsFixture.StartTime.Unix()) || (leagueID > 0 && event.Tournament.UniqueTournament.ID != leagueID) {
			continue
		}
		home := strconv.Itoa(event.HomeTeam.ID) == teamID
		if !home && strconv.Itoa(event.AwayTeam.ID) != teamID {
			continue
		}
		ref := FlowMatchReference{EventID: strconv.Itoa(event.ID), StartedAt: time.Unix(event.StartTS, 0).UTC(), HomeName: event.HomeTeam.Name, AwayName: event.AwayTeam.Name, HomeScore: event.HomeScore.Normaltime, AwayScore: event.AwayScore.Normaltime, Venue: "HOME", Opponent: event.AwayTeam.Name}
		if ref.HomeScore == nil {
			ref.HomeScore = event.HomeScore.Current
		}
		if ref.AwayScore == nil {
			ref.AwayScore = event.AwayScore.Current
		}
		if !home {
			ref.Venue, ref.Opponent = "AWAY", event.HomeTeam.Name
		}
		flow := shared[ref.EventID]
		var err error
		if flow == nil {
			flow, err = fetchHistoricalMatchFlow(client.withScope("przebieg-"+ref.HomeName+"-vs-"+ref.AwayName+"-mecz-"+ref.EventID), ref, event.HomeTeam.ID, event.AwayTeam.ID, event.HomeScore.Normaltime != nil && event.AwayScore.Normaltime != nil, refresh.Revision)
		}
		if err != nil {
			ref.Reason = err.Error()
			result.Matches = append(result.Matches, ref)
			continue
		}
		shared[ref.EventID] = flow
		ref.Available, ref.Revision = true, flow.Revision
		result.Available++
		result.Matches = append(result.Matches, ref)
		if !flow.TimelineValid {
			continue
		}
		matchStates := map[string]bool{}
		for _, point := range flow.GraphPoints {
			if point.State == "UNKNOWN" {
				continue
			}
			state := "DRAW"
			value := point.Value
			if (home && point.State == "HOME_LEAD") || (!home && point.State == "AWAY_LEAD") {
				state = "LEADING"
			} else if point.State != "DRAW" {
				state = "TRAILING"
			}
			if !home {
				value = -value
			}
			if stateTotals[state] == nil {
				stateTotals[state] = &FlowStateSummary{State: state}
			}
			addFlowPoint(&stateTotals[state].Window, value)
			matchStates[state] = true
		}
		for state := range matchStates {
			stateTotals[state].Matches++
		}
		seenReactions := map[string]bool{}
		for _, r := range flow.Reactions {
			if !r.Eligible || r.IsHome == nil {
				continue
			}
			own := *r.IsHome == home
			keys := []string{}
			if r.Kind == "GOAL" {
				if own {
					keys = append(keys, "AFTER_SCORED")
				} else {
					keys = append(keys, "AFTER_CONCEDED")
				}
				leading := "HOME_LEAD"
				trailing := "AWAY_LEAD"
				if !home {
					leading, trailing = trailing, leading
				}
				if r.StateBefore != leading && r.StateAfter == leading {
					keys = append(keys, "TOOK_LEAD")
				}
				if r.StateBefore != trailing && r.StateAfter == trailing {
					keys = append(keys, "FELL_BEHIND")
				}
				if r.StateBefore != "DRAW" && r.StateAfter == "DRAW" {
					if own {
						keys = append(keys, "EQUALIZED")
					} else {
						keys = append(keys, "LOST_LEAD")
					}
				}
			} else {
				prefix := "OPP_"
				if own {
					prefix = "OWN_"
				}
				keys = append(keys, prefix+r.Kind)
			}
			before, after := flowPerspective(r.Before, home), flowPerspective(r.After, home)
			for _, key := range keys {
				groupKey := key + "/" + r.Clock
				if seenReactions[groupKey] {
					continue
				}
				seenReactions[groupKey] = true
				if reactionTotals[key] == nil {
					reactionTotals[key] = &reactionTotal{row: FlowReactionSummary{Kind: key}, matches: map[string]bool{}}
				}
				total := reactionTotals[key]
				total.row.Samples++
				total.row.Before += before.Mean
				total.row.After += after.Mean
				total.matches[ref.EventID] = true
			}
		}
	}
	for _, state := range []string{"LEADING", "DRAW", "TRAILING"} {
		if row := stateTotals[state]; row != nil {
			result.States = append(result.States, *row)
		}
	}
	var keys []string
	for key := range reactionTotals {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		total := reactionTotals[key]
		row := total.row
		row.Before /= float64(row.Samples)
		row.After /= float64(row.Samples)
		row.Delta = row.After - row.Before
		row.Matches = len(total.matches)
		result.Reactions = append(result.Reactions, row)
	}
	result.Notes = append(result.Notes, "Ostatnie maksymalnie 5 zakończonych meczów w tych rozgrywkach z pierwszej strony historii. Dodatnie saldo oznacza przewagę analizowanej drużyny.", "Agregaty są poglądowe i nie zmieniają fair kursu. Brak stanu lub reakcji oznacza brak wystarczającej próbki.")
	log.Printf("[MATCH-FLOW] team=%s selected=%d available=%d reactions=%d", teamID, len(result.Matches), result.Available, len(result.Reactions))
	return result
}

type teamMatchProfile struct {
	FetchedAt  time.Time          `json:"fetchedAt"`
	Statistics map[string]float64 `json:"statistics"`
}

func (profile *teamMatchProfile) UnmarshalJSON(raw []byte) error {
	var envelope struct {
		Statistics map[string]json.RawMessage `json:"statistics"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return err
	}
	profile.Statistics = make(map[string]float64)
	for key, rawValue := range envelope.Statistics {
		var value *float64
		// Metadata such as statisticsType is an object, not a count.
		if json.Unmarshal(rawValue, &value) == nil && value != nil && !math.IsNaN(*value) && !math.IsInf(*value, 0) {
			profile.Statistics[key] = *value
		}
	}
	return nil
}

type fairModelInput struct {
	RecentStrengths map[int]opponentTeamStrength `json:"recentStrengths,omitempty"`
	Strengths       map[int]opponentTeamStrength `json:"strengths,omitempty"`
	HomeID          int                          `json:"homeId"`
	AwayID          int                          `json:"awayId"`
	LeagueID        int                          `json:"leagueId"`
	SeasonID        int                          `json:"seasonId"`
	Roles           map[int]string               `json:"roles"`
	Profiles        map[int]teamMatchProfile     `json:"profiles"`
}

type analysisMatchContext struct {
	Pending          bool           `json:"pending,omitempty"`
	Input            fairModelInput `json:"fairInput"`
	Overview         *MatchOverview `json:"overview"`
	profileAttempts  map[int]bool
	strengthAttempts map[int]bool
	strengthLimit    int
}

type sofaContextSnapshot struct {
	FetchedAt time.Time       `json:"fetchedAt"`
	Body      json.RawMessage `json:"body"`
	Revision  string          `json:"sourceRevision,omitempty"`
}

// This clock follows completed matches. NextCheckAt schedules small fixture
// discovery requests; it never expires a season profile or an existing graph.
type sofaTeamRefreshState struct {
	TeamID          string          `json:"teamId"`
	Revision        string          `json:"lastFinishedRevision"`
	LastFinished    time.Time       `json:"lastFinishedAt,omitempty"`
	FetchedAt       time.Time       `json:"fetchedAt"`
	NextCheckAt     time.Time       `json:"nextCheckAt"`
	AwaitFinishedID string          `json:"awaitFinishedId,omitempty"`
	Events          json.RawMessage `json:"events"`
}

var sofaTeamRefreshLocks = struct {
	sync.Mutex
	byTeam map[string]chan struct{}
}{byTeam: map[string]chan struct{}{}}

func lockSofaTeamRefresh(ctx context.Context, teamID string) (func(), error) {
	sofaTeamRefreshLocks.Lock()
	lock := sofaTeamRefreshLocks.byTeam[teamID]
	if lock == nil {
		lock = make(chan struct{}, 1)
		sofaTeamRefreshLocks.byTeam[teamID] = lock
	}
	sofaTeamRefreshLocks.Unlock()
	select {
	case lock <- struct{}{}:
		return func() { <-lock }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func sofaTeamRefreshPath(teamID string) string {
	return filepath.Join(serverDataDir, "matchup_cache", "druzyna-"+teamID+"_stan-po-meczu.json")
}

func markSofaTeamRefreshDue(client *ResilientClient, teamID, finishedID string) error {
	unlock, err := lockSofaTeamRefresh(client.ctx, teamID)
	if err != nil {
		return err
	}
	defer unlock()
	path := sofaTeamRefreshPath(teamID)
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	var state sofaTeamRefreshState
	if err == nil {
		err = json.Unmarshal(raw, &state)
	}
	if err != nil {
		return err
	}
	state.NextCheckAt = time.Time{}
	state.AwaitFinishedID = finishedID
	if err := atomicJSON(path, state); err != nil {
		return err
	}
	client.state.mu.Lock()
	delete(client.state.teamRefresh, teamID)
	client.state.mu.Unlock()
	log.Printf("[TEAM-CONTEXT] MATCH_ENDED team_id=%s; kontrola nowych danych przy kolejnym uzyciu", teamID)
	return nil
}

func archiveSofaTeamVersion(client *ResilientClient, teamID, label string, body json.RawMessage, fetchedAt time.Time, revision string) error {
	if len(body) == 0 {
		return nil
	}
	digest := sha256.Sum256(body)
	name := archiveName(label)
	path := filepath.Join(archiveRoot, "kontekst-druzyn", "druzyna-"+teamID, name, fmt.Sprintf("wersja-%x", digest), name+".json.gz")
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(map[string]any{"teamId": teamID, "sourceRevision": revision, "fetchedAt": fetchedAt, "data": body}, "", "  ")
	if err == nil {
		err = writeGzip(path, raw)
	}
	if err != nil {
		client.recordArchiveError(err)
	}
	return err
}

func getSofaTeamRefresh(client *ResilientClient, teamID string) (sofaTeamRefreshState, error) {
	client.state.mu.Lock()
	memo, ok := client.state.teamRefresh[teamID]
	client.state.mu.Unlock()
	if ok {
		return memo, nil
	}
	unlock, err := lockSofaTeamRefresh(client.ctx, teamID)
	if err != nil {
		return sofaTeamRefreshState{}, err
	}
	defer unlock()
	path := sofaTeamRefreshPath(teamID)
	var previous sofaTeamRefreshState
	if raw, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(raw, &previous)
	}
	now := time.Now().UTC()
	state := previous
	if previous.TeamID != teamID || previous.Revision == "" || len(previous.Events) == 0 || previous.FetchedAt.After(now) || !now.Before(previous.NextCheckAt) {
		body, err := client.GetSofa("/team/" + teamID + "/events/last/0")
		if err != nil {
			return previous, err
		}
		var recent SofaTeamEventsResponse
		if err := json.Unmarshal(body, &recent); err != nil {
			return previous, err
		}
		lastID, lastTS := 0, int64(0)
		for _, event := range recent.Events {
			if (strconv.Itoa(event.HomeTeam.ID) != teamID && strconv.Itoa(event.AwayTeam.ID) != teamID) || (event.Status.Code != 100 && event.Status.Type != "finished") {
				continue
			}
			if event.StartTS > lastTS || (event.StartTS == lastTS && event.ID > lastID) {
				lastID, lastTS = event.ID, event.StartTS
			}
		}
		if !previous.LastFinished.IsZero() && lastTS < previous.LastFinished.Unix() {
			return previous, fmt.Errorf("SofaScore: historia drużyny %s cofnęła się; zachowano poprzedni cache", teamID)
		}
		state = sofaTeamRefreshState{TeamID: teamID, Revision: fmt.Sprintf("mecz-%d-%d", lastID, lastTS), FetchedAt: now, NextCheckAt: now.Add(24 * time.Hour), AwaitFinishedID: previous.AwaitFinishedID, Events: body}
		if lastTS > 0 {
			state.LastFinished = time.Unix(lastTS, 0).UTC()
		}
		// Look at the schedule once, then wait until that match can have ended.
		// A daily schedule check catches rescheduling/teams without known fixtures.
		var upcoming SofaTeamEventsResponse
		nextBody, nextErr := client.GetSofa("/team/" + teamID + "/events/next/0")
		if err := errors.Join(client.ctx.Err(), client.archiveError()); err != nil {
			return previous, err
		}
		if nextErr == nil {
			nextErr = json.Unmarshal(nextBody, &upcoming)
		}
		if nextErr != nil && !strings.Contains(nextErr.Error(), "SofaScore HTTP 404") {
			client.warn("Terminarz do odświeżania kontekstu drużyny " + teamID + ": " + nextErr.Error())
			state.NextCheckAt = now.Add(time.Hour)
			if strings.Contains(nextErr.Error(), "SofaScore HTTP 403") {
				state.NextCheckAt = now // No pause imposed on manual challenge retries.
			}
		}
		for _, events := range []SofaTeamEventsResponse{recent, upcoming} {
			for _, event := range events.Events {
				if event.StartTS <= 0 || (strconv.Itoa(event.HomeTeam.ID) != teamID && strconv.Itoa(event.AwayTeam.ID) != teamID) || event.Status.Code == 100 || event.Status.Type == "finished" || event.Status.Type == "canceled" || event.Status.Type == "postponed" {
					continue
				}
				start := time.Unix(event.StartTS, 0).UTC()
				if start.Before(now.Add(-4*time.Hour)) && event.Status.Type != "inprogress" {
					continue
				}
				check := start.Add(2*time.Hour + 15*time.Minute)
				if !check.After(now) {
					check = now.Add(5 * time.Minute)
				}
				if check.Before(state.NextCheckAt) {
					state.NextCheckAt = check
				}
			}
		}
		if state.AwaitFinishedID != "" {
			for _, event := range recent.Events {
				if strconv.Itoa(event.ID) == state.AwaitFinishedID && (event.Status.Code == 100 || event.Status.Type == "finished") {
					state.AwaitFinishedID = ""
					break
				}
			}
			// The event-status endpoint can report finished before the team list
			// catches up. Keep the previous revision and check again shortly.
			if state.AwaitFinishedID != "" && now.Add(5*time.Minute).Before(state.NextCheckAt) {
				state.NextCheckAt = now.Add(5 * time.Minute)
			}
		}
		if previous.Revision != state.Revision {
			if err := archiveSofaTeamVersion(client, teamID, "lista-ostatnich-meczow", previous.Events, previous.FetchedAt, previous.Revision); err != nil {
				return previous, err
			}
			if err := archiveSofaTeamVersion(client, teamID, "lista-ostatnich-meczow", state.Events, state.FetchedAt, state.Revision); err != nil {
				return previous, err
			}
			log.Printf("[TEAM-CONTEXT] NEW_FINISHED team_id=%s previous=%q revision=%q; profile i lista do aktualizacji", teamID, previous.Revision, state.Revision)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
			return previous, err
		}
		if err := atomicJSON(path, state); err != nil {
			return previous, err
		}
		log.Printf("[TEAM-CONTEXT] CHECKED team_id=%s revision=%q next_check=%s", teamID, state.Revision, state.NextCheckAt.Format(time.RFC3339))
	}
	client.state.mu.Lock()
	if client.state.teamRefresh == nil {
		client.state.teamRefresh = map[string]sofaTeamRefreshState{}
	}
	client.state.teamRefresh[teamID] = state
	client.state.mu.Unlock()
	return state, nil
}

func getSofaSeasonProfile(client *ResilientClient, teamID, endpoint string, target any) (time.Time, error) {
	state, err := getSofaTeamRefresh(client, teamID)
	if err != nil {
		return time.Time{}, err
	}
	path := filepath.Join(serverDataDir, "matchup_cache", archiveName(endpoint)+".json")
	var old sofaContextSnapshot
	if raw, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(raw, &old)
	}
	if old.Revision != state.Revision {
		if err := archiveSofaTeamVersion(client, teamID, "profil-sezonowy-"+archiveName(endpoint), old.Body, old.FetchedAt, old.Revision); err != nil {
			return time.Time{}, err
		}
	}
	fetchedAt, err := getSofaContextJSON(client, endpoint, 0, target, state.Revision)
	if err != nil {
		return fetchedAt, err
	}
	snapshot := old
	if old.Revision != state.Revision || !old.FetchedAt.Equal(fetchedAt) {
		raw, err := os.ReadFile(path)
		if err == nil {
			err = json.Unmarshal(raw, &snapshot)
		}
		if err != nil {
			return fetchedAt, err
		}
	}
	// Also repairs an archive write interrupted after the active cache was saved.
	if err := archiveSofaTeamVersion(client, teamID, "profil-sezonowy-"+archiveName(endpoint), snapshot.Body, snapshot.FetchedAt, snapshot.Revision); err != nil {
		return fetchedAt, err
	}
	return fetchedAt, nil
}

// Small shared snapshots survive restart. Actual HTTP calls retain the existing
// raw archive; cache hits are captured by the analysis input archive instead.
var sofaContextFailures = struct {
	sync.Mutex
	until map[string]time.Time
}{until: make(map[string]time.Time)}

func getSofaContextJSON(client *ResilientClient, endpoint string, ttl time.Duration, target any, revisions ...string) (time.Time, error) {
	path := filepath.Join(serverDataDir, "matchup_cache", archiveName(endpoint)+".json")
	revision := ""
	if len(revisions) > 0 {
		revision = revisions[0]
	}
	var snapshot sofaContextSnapshot
	if raw, err := os.ReadFile(path); err == nil && json.Unmarshal(raw, &snapshot) == nil && !snapshot.FetchedAt.IsZero() && time.Since(snapshot.FetchedAt) >= 0 && (ttl == 0 || time.Since(snapshot.FetchedAt) < ttl) && (revision == "" || snapshot.Revision == revision) {
		if err := json.Unmarshal(snapshot.Body, target); err == nil {
			return snapshot.FetchedAt, nil
		}
	}
	sofaContextFailures.Lock()
	retryAt := sofaContextFailures.until[path]
	sofaContextFailures.Unlock()
	if time.Now().Before(retryAt) {
		return time.Time{}, fmt.Errorf("źródło chwilowo niedostępne (ponowienie po %s)", retryAt.Format("15:04"))
	}
	body, err := client.GetSofa(endpoint)
	if err == nil {
		err = json.Unmarshal(body, target)
	}
	if err != nil {
		// A 403 must not impose an endpoint-specific pause on manual attempts.
		if client.ctx.Err() == nil && !strings.Contains(err.Error(), "SofaScore HTTP 403") && !strings.HasPrefix(err.Error(), "sesja SofaScore:") && !strings.HasPrefix(err.Error(), "Chromium SofaScore:") {
			sofaContextFailures.Lock()
			sofaContextFailures.until[path] = time.Now().Add(15 * time.Minute)
			sofaContextFailures.Unlock()
		}
		return time.Time{}, err
	}
	snapshot = sofaContextSnapshot{FetchedAt: time.Now().UTC(), Body: body, Revision: revision}
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return snapshot.FetchedAt, err
	}
	if err := atomicJSON(path, snapshot); err != nil {
		return snapshot.FetchedAt, fmt.Errorf("zapis cache matchupu: %w", err)
	}
	return snapshot.FetchedAt, nil
}

// Recent form provides one consistent scale when the current competition has
// too few rounds, no table, or the player's history spans multiple competitions.
// This is a form proxy, not an opponent-adjusted global club rating.
func opponentUsesRecentStrength(hist *PlayerStatHistory, input fairModelInput) bool {
	if input.LeagueID <= 0 {
		return true
	}
	for _, match := range hist.MatchContexts {
		pastOwn, knownOwn := input.Strengths[match.TeamID]
		pastOpponent, knownOpponent := input.Strengths[match.OpponentID]
		if match.LeagueID != input.LeagueID || !knownOwn || !knownOpponent || pastOwn.Group != pastOpponent.Group {
			return true
		}
	}
	return false
}

func extendOpponentStrengths(client *ResilientClient, result analysisMatchContext, players []*PlayerStatHistory) analysisMatchContext {
	strengths := make(map[int]opponentTeamStrength, len(result.Input.RecentStrengths))
	for id, strength := range result.Input.RecentStrengths {
		strengths[id] = strength
	}
	result.Input.RecentStrengths = strengths
	attempts := make(map[int]bool, len(result.strengthAttempts))
	for id, attempted := range result.strengthAttempts {
		attempts[id] = attempted
	}
	result.strengthAttempts = attempts
	frequency := map[int]int{}
	needed := false
	for _, hist := range players {
		if !opponentUsesRecentStrength(hist, result.Input) {
			continue
		}
		needed = true
		for _, match := range hist.MatchContexts {
			if match.TeamID > 0 {
				frequency[match.TeamID]++
			}
			if match.OpponentID > 0 {
				frequency[match.OpponentID]++
			}
		}
	}
	if !needed {
		return result
	}
	ids := []int{result.Input.HomeID, result.Input.AwayID}
	others := []int{}
	for id := range frequency {
		if id != result.Input.HomeID && id != result.Input.AwayID {
			others = append(others, id)
		}
	}
	sort.Slice(others, func(i, j int) bool {
		if frequency[others[i]] != frequency[others[j]] {
			return frequency[others[i]] > frequency[others[j]]
		}
		return others[i] < others[j]
	})
	ids = append(ids, others...)
	missing := 0
	firstError := ""
	strengthLimit := result.strengthLimit
	if strengthLimit <= 0 {
		strengthLimit = 32
	}
	for _, id := range ids {
		if client.ctx.Err() != nil || len(attempts) >= strengthLimit {
			break
		}
		if _, known := strengths[id]; known {
			continue
		}
		if id <= 0 || attempts[id] {
			continue
		}
		attempts[id] = true
		state, err := getSofaTeamRefresh(client, strconv.Itoa(id))
		if err != nil {
			missing++
			if firstError == "" {
				firstError = fmt.Sprintf("drużyna %d: %v", id, err)
			}
			continue
		}
		var recent SofaTeamEventsResponse
		if json.Unmarshal(state.Events, &recent) != nil {
			missing++
			continue
		}
		sort.Slice(recent.Events, func(i, j int) bool { return recent.Events[i].StartTS > recent.Events[j].StartTS })
		points, matches := 0, 0
		seen := map[int]bool{}
		for _, event := range recent.Events {
			if matches >= 10 {
				break
			}
			if event.ID <= 0 || seen[event.ID] || event.StartTS <= 0 || !time.Unix(event.StartTS, 0).Before(client.statsFixture.StartTime) ||
				(event.HomeTeam.ID != id && event.AwayTeam.ID != id) || (event.Status.Code != 100 && event.Status.Type != "finished") {
				continue
			}
			name := strings.ToLower(event.Tournament.Name + " " + event.Tournament.UniqueTournament.Name)
			if strings.Contains(name, "friendly") || strings.Contains(name, "friendlies") || strings.Contains(name, "towarzysk") {
				continue
			}
			home, away := event.HomeScore.Normaltime, event.AwayScore.Normaltime
			if home == nil || away == nil {
				if event.Status.Code != 100 {
					continue
				} // No penalty/extra-time score as a 90-minute result.
				home, away = event.HomeScore.Current, event.AwayScore.Current
			}
			if home == nil || away == nil || *home < 0 || *away < 0 {
				continue
			}
			seen[event.ID] = true
			own, opponent := *home, *away
			if event.AwayTeam.ID == id {
				own, opponent = opponent, own
			}
			if own > opponent {
				points += 3
			} else if own == opponent {
				points++
			}
			matches++
		}
		if matches >= 3 {
			strengths[id] = opponentTeamStrength{PPG: float64(points) / float64(matches), Matches: matches}
		} else {
			missing++
		}
	}
	if missing > 0 {
		message := fmt.Sprintf("Filtr rywali: brak porównywalnej formy %d drużyn (minimum 3 zakończone mecze); szczegóły przy ofercie.", missing)
		if firstError != "" {
			message += " Przyczyna pobierania: " + firstError
		}
		client.warn(message)
	}
	return result
}

// Add only profiles needed by newly available histories. Published overviews
// stay immutable; tables and match flows are fetched only in the initial phase.
func extendAnalysisMatchProfiles(client *ResilientClient, current analysisMatchContext, players []*PlayerStatHistory, includeTeamOverview bool) analysisMatchContext {
	result := current
	if result.Input.HomeID == 0 || result.Input.AwayID == 0 || result.Overview == nil {
		return result
	}
	result = extendOpponentStrengths(client, result, players)
	if result.Input.LeagueID == 0 || result.Input.SeasonID == 0 {
		return result
	}
	result.Input.Profiles = make(map[int]teamMatchProfile, len(current.Input.Profiles))
	for id, profile := range current.Input.Profiles {
		result.Input.Profiles[id] = profile
	}
	result.profileAttempts = make(map[int]bool, len(current.profileAttempts))
	for id, attempted := range current.profileAttempts {
		result.profileAttempts[id] = attempted
	}
	overview := *current.Overview
	overview.Notes = append([]string(nil), current.Overview.Notes...)
	result.Overview = &overview
	in := &result.Input
	base := fmt.Sprintf("/unique-tournament/%d/season/%d", in.LeagueID, in.SeasonID)
	warn := func(label string, err error) {
		message := label + ": " + err.Error()
		result.Overview.Notes = append(result.Overview.Notes, message)
		client.warn(message)
	}
	// Historical comparisons use profiles of the same league and season, observed
	// now. They are NOT reconstructed statistics as of each historical fixture.
	ids := map[int]bool{in.HomeID: true, in.AwayID: true}
	for _, hist := range players {
		for _, m := range hist.MatchContexts {
			if m.LeagueID == in.LeagueID && m.SeasonID == in.SeasonID {
				if m.TeamID > 0 {
					ids[m.TeamID] = true
				}
				if m.OpponentID > 0 {
					ids[m.OpponentID] = true
				}
			}
		}
	}
	orderedIDs := []int{in.HomeID, in.AwayID}
	var historicalIDs []int
	for id := range ids {
		if id != in.HomeID && id != in.AwayID {
			historicalIDs = append(historicalIDs, id)
		}
	}
	sort.Ints(historicalIDs)
	orderedIDs = append(orderedIDs, historicalIDs...)
	// One budget across XI and reserve phases, including unsuccessful attempts.
	if result.profileAttempts == nil {
		result.profileAttempts = make(map[int]bool)
	}
	missing, attempted := 0, 0
	for _, id := range orderedIDs {
		if client.ctx.Err() != nil {
			break
		}
		if result.profileAttempts[id] {
			continue
		}
		if len(result.profileAttempts) >= 32 {
			break
		}
		result.profileAttempts[id] = true
		attempted++
		var profile teamMatchProfile
		var err error
		profile.FetchedAt, err = getSofaSeasonProfile(client, strconv.Itoa(id), fmt.Sprintf("/team/%d%s/statistics/overall", id, base), &profile)
		if err != nil || profile.Statistics["matches"] <= 0 {
			missing++
			continue
		}
		if overview := result.Overview.Teams[strconv.Itoa(id)]; includeTeamOverview && overview != nil {
			overview.StatsMatches = int(profile.Statistics["matches"])
			for _, key := range []string{"shots", "shotsAgainst", "shotsOnTarget", "shotsOnTargetAgainst", "expectedGoals", "tackles", "interceptions", "fouls", "totalPasses", "accuratePasses"} {
				if value, ok := profile.Statistics[key]; ok {
					overview.Metrics[key] = value / profile.Statistics["matches"]
				}
			}
			for _, key := range []string{"averageBallPossession", "avgRating"} {
				if value, ok := profile.Statistics[key]; ok {
					overview.Metrics[key] = value
				}
			}
		}
		modelStats := map[string]float64{"matches": profile.Statistics["matches"]}
		for _, market := range []CanonicalMarket{MarketShotsTotal, MarketShotsOnTarget, MarketShotsOffTarget, MarketShotsBlocked, MarketShotsInsideBox, MarketShotsOutsideBox, MarketTackles, MarketPasses, MarketAccuratePasses, MarketInterceptions, MarketOffsides, MarketFoulsDrawn} {
			own, opponent := matchupFeatures(market)
			for _, key := range []string{own, opponent} {
				if value, ok := profile.Statistics[key]; ok && key != "" {
					modelStats[key] = value
				}
			}
		}
		in.Profiles[id] = teamMatchProfile{FetchedAt: profile.FetchedAt, Statistics: modelStats}
	}
	if missing > 0 {
		warn("Kontekst drużyn", fmt.Errorf("brak %d/%d profili; dla brakujących danych model pomija korektę", missing, attempted))
	}
	return result
}

func fetchAnalysisMatchContext(client *ResilientClient, homeID, awayID string, players []*PlayerStatHistory) analysisMatchContext {
	result := analysisMatchContext{Input: fairModelInput{Roles: map[int]string{}, Profiles: map[int]teamMatchProfile{}}, Overview: &MatchOverview{AsOf: time.Now().UTC(), Teams: map[string]*TeamOverview{}, Notes: []string{}}}
	client.state.mu.Lock()
	for id, role := range client.state.lineupRoles {
		result.Input.Roles[id] = role
	}
	client.state.mu.Unlock()
	warn := func(label string, err error) {
		if err != nil {
			message := label + ": " + err.Error()
			result.Overview.Notes = append(result.Overview.Notes, message)
			client.warn(message)
		}
	}
	if client.statsFixture.ID == "" {
		result.Overview.Notes = append(result.Overview.Notes, "Brak powiązanego wydarzenia: matchup niedostępny.")
		return result
	}
	var event struct {
		Event struct {
			HomeTeam SofaTeamItem `json:"homeTeam"`
			AwayTeam SofaTeamItem `json:"awayTeam"`
			Season   struct {
				ID int `json:"id"`
			} `json:"season"`
			Tournament struct {
				UniqueTournament struct {
					ID int `json:"id"`
				} `json:"uniqueTournament"`
			} `json:"tournament"`
		} `json:"event"`
	}
	_, err := getSofaContextJSON(client, "/event/"+client.statsFixture.ID, time.Hour, &event)
	if err != nil {
		warn("Kontekst meczu", err)
		return result
	}
	in := &result.Input
	in.HomeID, in.AwayID = event.Event.HomeTeam.ID, event.Event.AwayTeam.ID
	in.LeagueID, in.SeasonID = event.Event.Tournament.UniqueTournament.ID, event.Event.Season.ID
	if !((strconv.Itoa(in.HomeID) == homeID && strconv.Itoa(in.AwayID) == awayID) || (strconv.Itoa(in.HomeID) == awayID && strconv.Itoa(in.AwayID) == homeID)) {
		result.Input.HomeID, result.Input.AwayID = 0, 0
		warn("Kontekst meczu", fmt.Errorf("drużyny wydarzenia nie zgadzają się z analizą"))
		return result
	}
	result.Overview.LeagueID, result.Overview.SeasonID = in.LeagueID, in.SeasonID
	for _, item := range []struct {
		team  SofaTeamItem
		venue string
	}{{event.Event.HomeTeam, "HOME"}, {event.Event.AwayTeam, "AWAY"}} {
		result.Overview.Teams[strconv.Itoa(item.team.ID)] = &TeamOverview{Name: item.team.Name, Venue: item.venue, Form: []string{}, VenueForm: []string{}, Metrics: map[string]float64{}}
	}
	if in.LeagueID == 0 || in.SeasonID == 0 {
		warn("Kontekst ligi", fmt.Errorf("brak identyfikatora ligi lub sezonu"))
		return extendOpponentStrengths(client, result, players)
	}
	base := fmt.Sprintf("/unique-tournament/%d/season/%d", in.LeagueID, in.SeasonID)
	for _, venue := range []string{"total", "home", "away"} {
		if client.ctx.Err() != nil {
			break
		}
		var table struct {
			Standings []struct {
				Rows []struct {
					Team     SofaTeamItem `json:"team"`
					Position int          `json:"position"`
					Matches  *int         `json:"matches"`
					Points   *int         `json:"points"`
				} `json:"rows"`
			} `json:"standings"`
		}
		_, err := getSofaContextJSON(client, base+"/standings/"+venue, time.Hour, &table)
		if err != nil {
			warn("Tabela "+venue, err)
			continue
		}
		if venue == "total" {
			in.Strengths = map[int]opponentTeamStrength{}
		}
		for group, standing := range table.Standings {
			for _, row := range standing.Rows {
				if row.Matches == nil || row.Points == nil {
					continue
				}
				if venue == "total" && row.Team.ID > 0 && *row.Matches >= 3 && *row.Points >= 0 && *row.Points <= 3*(*row.Matches) {
					in.Strengths[row.Team.ID] = opponentTeamStrength{PPG: float64(*row.Points) / float64(*row.Matches), Matches: *row.Matches, Group: group}
				}
				if team := result.Overview.Teams[strconv.Itoa(row.Team.ID)]; team != nil {
					summary := &TableSummary{Position: row.Position, Matches: *row.Matches, Points: *row.Points}
					if venue == "total" {
						team.Table = summary
					} else if strings.EqualFold(venue, team.Venue) {
						team.VenueTable = summary
					}
				}
			}
		}
	}
	result = extendAnalysisMatchProfiles(client, result, players, true)
	// Keep the pointer in sync with the copied profile/strength maps.
	in = &result.Input
	// The outer team-events key is tournament.id, NOT uniqueTournament.id.
	for _, venue := range []string{"home", "away"} {
		if client.ctx.Err() != nil {
			break
		}
		var events struct {
			TournamentTeamEvents map[string]map[string][]struct {
				ID        int          `json:"id"`
				StartTS   int64        `json:"startTimestamp"`
				HomeTeam  SofaTeamItem `json:"homeTeam"`
				AwayTeam  SofaTeamItem `json:"awayTeam"`
				HomeScore struct {
					Current *int `json:"current"`
				} `json:"homeScore"`
				AwayScore struct {
					Current *int `json:"current"`
				} `json:"awayScore"`
				Status struct {
					Code int `json:"code"`
				} `json:"status"`
			} `json:"tournamentTeamEvents"`
		}
		_, err := getSofaContextJSON(client, base+"/team-events/"+venue, time.Hour, &events)
		if err != nil {
			warn("Forma "+venue, err)
			continue
		}
		for id, team := range result.Overview.Teams {
			if !strings.EqualFold(team.Venue, venue) {
				continue
			}
			type formItem struct {
				id      int
				started int64
				value   string
			}
			var form []formItem
			seen := map[int]bool{}
			for _, teams := range events.TournamentTeamEvents {
				for _, e := range teams[id] {
					if seen[e.ID] || e.Status.Code != 100 || e.HomeScore.Current == nil || e.AwayScore.Current == nil || e.StartTS >= client.statsFixture.StartTime.Unix() {
						continue
					}
					own, opp := *e.HomeScore.Current, *e.AwayScore.Current
					if venue == "home" && strconv.Itoa(e.HomeTeam.ID) != id {
						continue
					}
					if venue == "away" {
						if strconv.Itoa(e.AwayTeam.ID) != id {
							continue
						}
						own, opp = opp, own
					}
					value := "D"
					if own > opp {
						value = "W"
					} else if own < opp {
						value = "L"
					}
					form = append(form, formItem{e.ID, e.StartTS, value})
					seen[e.ID] = true
				}
			}
			sort.Slice(form, func(i, j int) bool { return form[i].started > form[j].started })
			for i, e := range form {
				if i == 5 {
					break
				}
				team.VenueForm = append(team.VenueForm, e.value)
			}
		}
	}
	var pregame struct {
		HomeTeam struct {
			Form []string `json:"form"`
		} `json:"homeTeam"`
		AwayTeam struct {
			Form []string `json:"form"`
		} `json:"awayTeam"`
	}
	if _, err := getSofaContextJSON(client, "/event/"+client.statsFixture.ID+"/pregame-form", time.Hour, &pregame); err == nil {
		result.Overview.Teams[strconv.Itoa(in.HomeID)].Form = pregame.HomeTeam.Form
		result.Overview.Teams[strconv.Itoa(in.AwayID)].Form = pregame.AwayTeam.Form
	} else {
		warn("Forma przedmeczowa", err)
	}
	result.Overview.Notes = append(result.Overview.Notes, "Tabela, forma, posiadanie, xG i rating są podglądem; nie mnożą fair kursu. Na początku sezonu tabela ma małą próbkę.")
	sharedFlows := map[string]*HistoricalMatchFlow{}
	for _, id := range []int{in.HomeID, in.AwayID} {
		if client.ctx.Err() != nil {
			break
		}
		result.Overview.Teams[strconv.Itoa(id)].Flow = fetchTeamFlowOverview(client, strconv.Itoa(id), in.LeagueID, sharedFlows)
	}
	log.Printf("[MATCHUP] event=%s league=%d season=%d profiles=%d roles=%d", client.statsFixture.ID, in.LeagueID, in.SeasonID, len(in.Profiles), len(in.Roles))
	return result
}

// Only verified count fields enter this allowlist. No synthetic xGA, no rating,
// no table position and no assumption that possession raises every market.
func matchupFeatures(market CanonicalMarket) (own, opponent string) {
	switch market {
	case MarketShotsTotal:
		return "shots", "shotsAgainst"
	case MarketShotsOnTarget:
		return "shotsOnTarget", "shotsOnTargetAgainst"
	case MarketShotsOffTarget:
		return "shotsOffTarget", "shotsOffTargetAgainst"
	case MarketShotsBlocked:
		return "blockedScoringAttempt", "blockedScoringAttemptAgainst"
	case MarketShotsInsideBox:
		return "shotsFromInsideTheBox", "shotsFromInsideTheBoxAgainst"
	case MarketShotsOutsideBox:
		return "shotsFromOutsideTheBox", "shotsFromOutsideTheBoxAgainst"
	case MarketTackles:
		// Aggregate tackles do not establish the number of won tackles.
		return "", ""
	case MarketPasses:
		return "totalPasses", ""
	case MarketAccuratePasses:
		return "accuratePasses", ""
	case MarketInterceptions:
		return "interceptions", "interceptionsAgainst"
	case MarketOffsides:
		return "offsides", "offsidesAgainst"
	case MarketFoulsDrawn:
		return "", "fouls"
	default:
		return "", ""
	}
}

func matchupRate(input fairModelInput, teamID int, key string) (float64, float64, bool) {
	p := input.Profiles[teamID].Statistics
	n, ok := p["matches"]
	v, exists := p[key]
	if !ok || !exists || n <= 0 || v < 0 || key == "" {
		return 0, 0, false
	}
	return v / n, n, true
}

func contextualFairModel(hist *PlayerStatHistory, market CanonicalMarket, line float64, under bool, input fairModelInput) *FairModelDetails {
	model := &FairModelDetails{Version: fairModelVersion, Distribution: "Weighted empirical / Jeffreys", MatchupFactor: 1, Features: []string{}, Notes: []string{"Model eksperymentalny; parametry wymagają kalibracji na zapisanych wynikach."}}
	model.TeamID = hist.TeamID
	teamID, _ := strconv.Atoi(hist.TeamID)
	opponentID := 0
	if teamID > 0 && teamID == input.HomeID {
		model.Venue, opponentID = "HOME", input.AwayID
	} else if teamID > 0 && teamID == input.AwayID {
		model.Venue, opponentID = "AWAY", input.HomeID
	}
	if opponentID > 0 {
		model.OpponentID = strconv.Itoa(opponentID)
	}
	model.Role = strings.ToUpper(strings.TrimSpace(input.Roles[hist.ID]))
	values := hist.MetricValues[market]
	weights := make([]float64, len(values))
	var sumW, sumW2, sumV float64
	for i, v := range values {
		w := 1.0
		if model.Role != "" && i < len(hist.Positions) && strings.TrimSpace(hist.Positions[i]) != "" && !strings.EqualFold(model.Role, strings.TrimSpace(hist.Positions[i])) {
			w *= .5
		}
		if model.Venue != "" && i < len(hist.MatchContexts) && hist.MatchContexts[i].Venue != "" && hist.MatchContexts[i].Venue != model.Venue {
			w *= .7
		}
		weights[i] = w
		sumW += w
		sumW2 += w * w
		sumV += w * float64(v)
	}
	if sumW == 0 {
		return model
	}
	model.BaseMean = sumV / sumW
	model.EffectiveSample = sumW * sumW / sumW2
	model.Features = append(model.Features, "liczności na mecz")
	if model.Role != "" {
		model.Features = append(model.Features, "pozycja "+model.Role)
	} else {
		model.Notes = append(model.Notes, "Brak aktualnej pozycji: bez ważenia według roli.")
	}
	if model.Venue != "" {
		model.Features = append(model.Features, "home/away")
	}
	own, opp := matchupFeatures(market)
	logAdjustment := 0.0
	contextUsed := make([]bool, len(values))
	appliedFeatures := 0
	for _, feature := range []struct {
		key    string
		target int
		own    bool
	}{{own, teamID, true}, {opp, opponentID, false}} {
		target, n, ok := matchupRate(input, feature.target, feature.key)
		if !ok {
			continue
		}
		var baseline, covered, support float64
		coveredIndexes := []int{}
		for i, m := range hist.MatchContexts {
			if i >= len(weights) || m.LeagueID != input.LeagueID || m.SeasonID != input.SeasonID {
				continue
			}
			id := m.OpponentID
			if feature.own {
				id = m.TeamID
			}
			rate, matches, ok := matchupRate(input, id, feature.key)
			if !ok {
				continue
			}
			baseline += weights[i] * rate
			covered += weights[i]
			support += weights[i] * matches
			coveredIndexes = append(coveredIndexes, i)
		}
		if covered == 0 || baseline <= 0 {
			continue
		}
		baseline /= covered
		// Smooth sparse counts, shrink early-season and incomplete context. The
		// final cap is a guardrail, not a learned coefficient or confidence level.
		reliability := math.Min(n, support/covered) / (math.Min(n, support/covered) + 8)
		logAdjustment += .5 * math.Log((target+.25)/(baseline+.25)) * reliability * (covered / sumW)
		model.Features = append(model.Features, feature.key+" / mecz")
		appliedFeatures++
		for _, i := range coveredIndexes {
			contextUsed[i] = true
		}
	}
	for _, used := range contextUsed {
		if used {
			model.ContextMatches++
		}
	}
	model.MatchupFactor = math.Max(.8, math.Min(1.25, math.Exp(logAdjustment)))
	model.ExpectedCount = model.BaseMean * model.MatchupFactor
	if appliedFeatures == 0 {
		model.Notes = append(model.Notes, "Brak zgodnych danych dla tego rynku: korekta matchupu = 1.")
	} else if own != "" && opp != "" && appliedFeatures < 2 {
		model.Notes = append(model.Notes, "Częściowy profil matchupu: brak jednego z czynników drużyna/rywal.")
	}
	if model.EffectiveSample < 5 {
		model.Notes = append(model.Notes, "Mała efektywna próbka; fair kurs ma wysoką niepewność.")
	}
	// Each observed appearance contributes a hit or miss. A 90-minute zero
	// has the same weight as any other miss; a large surplus is just one hit.
	var hitWeight float64
	for i, value := range values {
		if float64(value) > line {
			hitWeight += weights[i]
		}
	}
	// Scale weighted proportions to the effective sample before Jeffreys smoothing.
	pOver := (hitWeight/sumW*model.EffectiveSample + .5) / (model.EffectiveSample + 1)
	// Existing matchup correction acts on the odds of a hit, with the same cap.
	oddsOver := pOver / (1 - pOver) * model.MatchupFactor
	pOver = oddsOver / (1 + oddsOver)
	probability := pOver
	if under {
		probability = 1 - pOver
	}
	model.Features = append(model.Features, "ważone trafienia linii, wygładzenie Jeffreysa")
	model.Notes = append(model.Notes, "Zapas ponad linię nie zwiększa szansy trafienia; zera z rozegranych występów są pełnymi nietrafieniami over. Minuty nie przeliczają historii na per90.", "Średnia liczność jest opisem historii; fair kurs wynika z trafień linii i korekty matchupu.")
	model.Probability = math.Max(.005, math.Min(.995, probability))
	return model
}

func opponentMatchClass(match PlayerMatchContext, input fairModelInput, useForm bool) (int, bool) {
	strengths := input.Strengths
	if useForm {
		strengths = input.RecentStrengths
	}
	own, ownOK := strengths[match.TeamID]
	opponent, opponentOK := strengths[match.OpponentID]
	if !ownOK || !opponentOK || (!useForm && (match.LeagueID != input.LeagueID || own.Group != opponent.Group)) {
		return 0, false
	}
	difference := opponent.PPG - own.PPG
	if difference < -1e-9 {
		return -1, true
	}
	if difference > 1e-9 {
		return 1, true
	}
	return 0, true
}

func opponentFilterForHistory(hist *PlayerStatHistory, market CanonicalMarket, line float64, under bool, input analysisMatchContext) *OpponentFilterData {
	data := &OpponentFilterData{Rule: "opponent-vs-team", Basis: "W każdym historycznym meczu porównujemy rywala z drużyną, w której grał zawodnik: niższe PPG rywala = łatwiejszy, wyższe = trudniejszy, równe = poza obiema grupami. Źródło: bieżąca tabela tych samych rozgrywek, minimum 3 mecze każdej drużyny. Ocena siły jest bieżąca, nie odtworzona na dzień historycznego występu."}
	teamID, _ := strconv.Atoi(hist.TeamID)
	opponentID := 0
	if teamID == input.Input.HomeID {
		opponentID = input.Input.AwayID
	} else if teamID == input.Input.AwayID {
		opponentID = input.Input.HomeID
	}
	strengths := input.Input.Strengths
	own, ownOK := strengths[teamID]
	opponent, opponentOK := strengths[opponentID]
	useForm := opponentUsesRecentStrength(hist, input.Input)
	if useForm {
		strengths = input.Input.RecentStrengths
		own, ownOK = strengths[teamID]
		opponent, opponentOK = strengths[opponentID]
		data.Basis = "W każdym historycznym meczu porównujemy rywala z drużyną, w której grał zawodnik: niższe PPG rywala = łatwiejszy, wyższe = trudniejszy, równe = poza obiema grupami. Źródło: forma z maksymalnie 10 ostatnich zakończonych meczów o stawkę przed analizowanym wydarzeniem, minimum 3 na klub. Forma nie jest rankingiem jakości lig ani oceną odtworzoną na dzień historycznego występu."
	}
	if input.Pending {
		data.UnknownMatches = hist.Matches
		data.Reason = "Oczekiwanie na dane do klasyfikacji historycznych rywali."
		return data
	}
	// Current fixture strengths are descriptive; they never gate or set the
	// threshold for classifying historical opponents.
	if ownOK {
		data.OwnPPG = &own.PPG
	}
	if opponentOK {
		data.OpponentPPG = &opponent.PPG
	}
	if input.Overview != nil {
		if team := input.Overview.Teams[strconv.Itoa(teamID)]; team != nil {
			data.OwnTeam = team.Name
		}
		if team := input.Overview.Teams[strconv.Itoa(opponentID)]; team != nil {
			data.Opponent = team.Name
		}
	}
	easier, harder := []int{}, []int{}
	values := hist.MetricValues[market]
	for index := range values {
		if index >= len(hist.MatchContexts) {
			data.UnknownMatches++
			continue
		}
		match := hist.MatchContexts[index]
		class, known := opponentMatchClass(match, input.Input, useForm)
		if !known {
			data.UnknownMatches++
			continue
		}
		data.KnownMatches++
		if class < 0 {
			easier = append(easier, index)
		} else if class > 0 {
			harder = append(harder, index)
		} else {
			data.EqualMatches++
		}
	}
	if data.KnownMatches == 0 {
		data.Reason = "Historia nie zawiera występów z porównywalną oceną obu drużyn; brakujące dane nie są zastępowane zerem."
	}
	variant := func(indexes []int, label string) *OpponentFilterVariant {
		if len(indexes) == 0 {
			return nil
		}
		subset := *hist
		subset.Matches = len(indexes)
		subset.MetricValues = map[CanonicalMarket][]int{market: sampleIndexes(values, indexes)}
		subset.MatchContexts = sampleIndexes(hist.MatchContexts, indexes)
		subset.Positions = sampleIndexes(hist.Positions, indexes)
		subset.Minutes = sampleIndexes(hist.Minutes, indexes)
		hits := 0
		for _, value := range subset.MetricValues[market] {
			if (float64(value) > line) != under {
				hits++
			}
		}
		model := contextualFairModel(&subset, market, line, under, input.Input)
		model.Features = append(model.Features, "rywal względem drużyny zawodnika w danym historycznym meczu: "+label)
		model.Notes = append(model.Notes, data.Basis, fmt.Sprintf("Podgląd z bazowej historii: %d z %d występów; bez klasyfikacji: %d, z równą oceną obu drużyn: %d. Po wyborze filtra klient uzupełnia docelową próbkę starszymi występami na żądanie.", len(indexes), len(values), data.UnknownMatches, data.EqualMatches))
		for _, index := range indexes {
			match := hist.MatchContexts[index]
			if strengths[match.TeamID].Matches < 8 || strengths[match.OpponentID].Matches < 8 {
				model.Notes = append(model.Notes, "Co najmniej jedna porównywana drużyna ma ocenę z mniej niż 8 spotkań; podział według siły może być niestabilny.")
				break
			}
		}
		return &OpponentFilterVariant{Indexes: indexes, HitRate: math.Round(float64(hits)/float64(len(indexes))*1000) / 10, FairModel: model}
	}
	data.Easier, data.Harder = variant(easier, "rywal słabszy od własnej drużyny"), variant(harder, "rywal silniejszy od własnej drużyny")
	return data
}

func CalculateValueBets(
	props []NormalizedProp,
	homePlayers, awayPlayers []*PlayerStatHistory,
	homeLineup, awayLineup map[string]bool,
	lineupsConfirmed bool,
	contexts ...analysisMatchContext,
) []AnalyzedBet {
	results, _ := calculateValueBetsContext(context.Background(), props, homePlayers, awayPlayers, homeLineup, awayLineup, lineupsConfirmed, nil, contexts...)
	return results
}

func calculateValueBetsContext(ctx context.Context, props []NormalizedProp, homePlayers, awayPlayers []*PlayerStatHistory, homeLineup, awayLineup map[string]bool, lineupsConfirmed bool, progress func(int, int), contexts ...analysisMatchContext) ([]AnalyzedBet, error) {
	var contextData analysisMatchContext
	if len(contexts) > 0 {
		contextData = contexts[0]
	}
	log.Printf("[CALCULATE] Rozpoczynam ewaluację Value Betów dla %d zakładów bukmacherskich...", len(props))
	playerHistory := make(map[string]*PlayerStatHistory)
	strictMap := make(map[string]string)

	registerPlayers := func(list []*PlayerStatHistory, side string) {
		for _, p := range list {
			p.TeamSide = side
			idStr := strconv.Itoa(p.ID)
			playerHistory[idStr] = p

			registerHistoryAliases(strictMap, p)
		}
	}

	registerPlayers(homePlayers, "HOME")
	registerPlayers(awayPlayers, "AWAY")
	log.Printf("[CALCULATE] Zarejestrowano w mapie dopasowań %d zawodników (Gospodarze: %d, Goście: %d)", len(playerHistory), len(homePlayers), len(awayPlayers))

	getLineupStatus := func(playerID int, pName, side string) (string, bool) {
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

		statusFor := func(isStarter bool) (string, bool) {
			prefix := ""
			if !lineupsConfirmed {
				prefix = "PREDICTED_"
			}
			if isStarter {
				return prefix + "STARTER", true
			}
			return prefix + "BENCH", false
		}
		if isStarter, exists := lineupMap["player:"+strconv.Itoa(playerID)]; exists {
			return statusFor(isStarter)
		}
		if isStarter, exists := lineupMap[norm]; exists {
			return statusFor(isStarter)
		}
		// A prediction can list just eleven players. Missing names do not prove
		// that the other players will be substitutes or outside the squad.
		return "UNKNOWN", false
	}

	results := make([]AnalyzedBet, 0)
	matchedPropsCount := 0
	matchedPlayers := map[string]string{}
	type metricKey struct {
		player string
		market CanonicalMarket
	}
	type modelKey struct {
		metricKey
		line  float64
		under bool
	}
	metricHistories := map[metricKey]*PlayerStatHistory{}
	models := map[modelKey]*FairModelDetails{}
	opponentFilters := map[modelKey]*OpponentFilterData{}

	for index, prop := range props {
		if index%32 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if progress != nil && index%250 == 0 {
			progress(index, len(props))
		}
		matchedID, resolved := matchedPlayers[prop.PlayerName]
		if !resolved {
			matchedID = matchPlayerStrict(prop.PlayerName, strictMap)
			matchedPlayers[prop.PlayerName] = matchedID
		}
		if matchedID == "" {
			continue
		}

		hist, exists := playerHistory[matchedID]
		if !exists || hist.Matches == 0 {
			continue
		}
		historySamples := hist.Matches
		metric := metricKey{matchedID, prop.MarketType}
		if cached, ok := metricHistories[metric]; ok {
			hist = cached
		} else {
			hist = metricHistory(hist, prop.MarketType)
			metricHistories[metric] = hist
		}

		values, ok := hist.MetricValues[prop.MarketType]
		if !ok || len(values) == 0 {
			continue
		}
		valuesSuperSub := hist.MetricValuesWithSuperSub[prop.MarketType]

		matchedPropsCount++
		sample := len(values)
		hitsOver, hitsUnder := 0, 0
		for _, v := range values {
			if float64(v) > prop.Line {
				hitsOver++
			} else {
				hitsUnder++
			}
		}

		var pHit float64
		if prop.IsUnder {
			pHit = float64(hitsUnder) / float64(sample)
		} else {
			pHit = float64(hitsOver) / float64(sample)
		}
		var model *FairModelDetails
		var opponentFilter *OpponentFilterData
		var fairOdds, edge, evTax float64
		verdict := "PENDING"
		if !contextData.Pending {
			key := modelKey{metric, prop.Line, prop.IsUnder}
			model = models[key]
			if model == nil {
				model = contextualFairModel(hist, prop.MarketType, prop.Line, prop.IsUnder, contextData.Input)
				if hist.Matches < historySamples {
					model.Notes = append(model.Notes, fmt.Sprintf("Dla tego rynku pominięto %d występów bez potwierdzonej wartości statystyki; brak danych nie oznacza zera.", historySamples-hist.Matches))
				}
				models[key] = model
			}
			opponentFilter = opponentFilters[key]
			if opponentFilter == nil {
				opponentFilter = opponentFilterForHistory(hist, prop.MarketType, prop.Line, prop.IsUnder, contextData)
				opponentFilters[key] = opponentFilter
			}
			fairOdds = 1.0 / model.Probability
			edge = (prop.Odds - fairOdds) / fairOdds * 100.0
			evTax = ((1.0/fairOdds)*(prop.Odds*0.88-1.0) - (1.0 - (1.0 / fairOdds))) * 100.0
			verdict = "NEUTRAL"
			if evTax > 0.0 {
				verdict = "VALUE"
			} else if evTax < -15.0 {
				verdict = "TRAP"
			}
		}

		label := prop.MarketLabel
		if prop.IsUnder && !strings.Contains(label, "Poniżej") {
			label += " (Poniżej)"
		}

		status, isStarting := getLineupStatus(hist.ID, hist.FullName, hist.TeamSide)

		results = append(results, AnalyzedBet{
			HistoryNotes:         playerMetricHistoryNotes(playerHistory[matchedID], prop.MarketType),
			OpponentFilter:       opponentFilter,
			PlayerID:             hist.ID,
			ContextPending:       contextData.Pending,
			SampleRatings:        append([]*float64(nil), hist.Ratings...),
			FairModel:            model,
			MatchOverview:        contextData.Overview,
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

	// Use the same offer identity and best-price rule as persisted predictions.
	unique, indices := []AnalyzedBet{}, map[string]int{}
	for _, bet := range results {
		key := researchHash([]any{bet.Bookmaker, bet.PlayerID, bet.MarketType, bet.Line, bet.IsUnder})
		if i, exists := indices[key]; exists {
			if bet.BookmakerOdds > unique[i].BookmakerOdds {
				unique[i] = bet
			}
		} else {
			indices[key] = len(unique)
			unique = append(unique, bet)
		}
	}
	results = unique
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if progress != nil {
		progress(len(props), len(props))
	}
	log.Printf("[CALCULATE] Zakończono analizę. Dopasowano %d zakładów, utworzono %d przeanalizowanych rekordów", matchedPropsCount, len(results))
	return results, nil
}

// === Asynchronous analysis API and context-aware source transport ===

var serverDataDir = "data"
var globalSofaThrottle = prioritySlots{capacity: 6}

type sofaManualContextKey struct{}
type offerDiscoveryContextKey struct{}

// Manual analysis > interactive requests > qualified background work > offer
// discovery. Both transports use this queue, including shared Chromium.
type prioritySlots struct {
	mu               sync.Mutex
	capacity, active int
	backgroundLimit  int
	activeByPriority [4]int
	waiting          [4]int
	changed          chan struct{}
}

func sofaRequestPriority(ctx context.Context) int {
	if manual, _ := ctx.Value(sofaManualContextKey{}).(bool); manual {
		return 0
	}
	if discovery, _ := ctx.Value(offerDiscoveryContextKey{}).(bool); discovery {
		return 3
	}
	if background, _ := ctx.Value(sofaBackgroundContextKey{}).(bool); background {
		return 2
	}
	return 1
}

func (slots *prioritySlots) acquire(ctx context.Context) (func(), error) {
	priority := sofaRequestPriority(ctx)
	slots.mu.Lock()
	if slots.capacity < 1 {
		slots.capacity = 1
	}
	if slots.changed == nil {
		slots.changed = make(chan struct{})
	}
	slots.waiting[priority]++
	signal := func() { close(slots.changed); slots.changed = make(chan struct{}) }
	for {
		if err := ctx.Err(); err != nil {
			slots.waiting[priority]--
			signal()
			slots.mu.Unlock()
			return nil, err
		}
		higher := false
		for i := 0; i < priority; i++ {
			higher = higher || slots.waiting[i] > 0
		}
		backgroundActive := slots.activeByPriority[2] + slots.activeByPriority[3]
		backgroundAllowed := priority < 2 || slots.backgroundLimit == 0 || backgroundActive < slots.backgroundLimit
		if slots.active < slots.capacity && !higher && backgroundAllowed {
			slots.waiting[priority]--
			slots.active++
			slots.activeByPriority[priority]++
			signal()
			slots.mu.Unlock()
			return func() {
				slots.mu.Lock()
				slots.active--
				slots.activeByPriority[priority]--
				signal()
				slots.mu.Unlock()
			}, nil
		}
		changed := slots.changed
		slots.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		slots.mu.Lock()
	}
}

type sofaBackgroundContextKey struct{}

type sofaBackoffState struct {
	Status      int       `json:"status"`
	Until       time.Time `json:"until"`
	LastBlocked time.Time `json:"lastBlocked"`
	Blocks      int       `json:"consecutiveBlocks"`
}

var sofaBackoff = struct {
	sync.Mutex
	state sofaBackoffState
}{}
var sofaBackgroundSlot = make(chan struct{}, 1)
var sofaBackgroundLastFinished time.Time // protected by sofaBackgroundSlot

func loadSofaBackoff() {
	sofaBackoff.Lock()
	defer sofaBackoff.Unlock()
	var state sofaBackoffState
	raw, err := os.ReadFile(filepath.Join(serverDataDir, "sofa_request_backoff.json"))
	if err == nil {
		err = json.Unmarshal(raw, &state)
	}
	if err != nil && !os.IsNotExist(err) {
		log.Printf("[SOFA-BACKOFF] READ_ERROR error=%v", err)
	}
	// Legacy snapshots mixed 403 and 429 without identifying their source.
	// Ignore them so an earlier 403 cannot keep manual attempts blocked.
	if state.Status != 429 {
		if !state.Until.IsZero() {
			log.Printf("[SOFA-BACKOFF] IGNORED_LEGACY reason=no-explicit-429-status")
		}
		state = sofaBackoffState{}
	}
	if state.Blocks < 0 {
		state.Blocks = 0
	}
	if state.Blocks > 3 {
		state.Blocks = 3
	}
	sofaBackoff.state = state
	if time.Now().Before(state.Until) {
		log.Printf("[SOFA-BACKOFF] LOADED until=%s blocks=%d", state.Until.Format(time.RFC3339), state.Blocks)
	}
}

func sofaCooldownUntil() time.Time {
	sofaBackoff.Lock()
	defer sofaBackoff.Unlock()
	if sofaBackoff.state.Status == 429 && time.Now().Before(sofaBackoff.state.Until) {
		return sofaBackoff.state.Until
	}
	return time.Time{}
}

func sofaCooldownError() error {
	if until := sofaCooldownUntil(); !until.IsZero() {
		return fmt.Errorf("SofaScore: przerwa po HTTP 429 do %s; nie wysłano kolejnego żądania", until.Format(time.RFC3339))
	}
	return nil
}

func noteSofaBlocked(status int, retryAfter string) {
	if status != 429 {
		return
	}
	sofaBackoff.Lock()
	defer sofaBackoff.Unlock()
	now := time.Now().UTC()
	state := &sofaBackoff.state
	// Concurrent responses from a foreground analysis are one blocking episode.
	if now.Before(state.Until) {
		return
	}
	if now.Sub(state.LastBlocked) >= 24*time.Hour {
		state.Blocks = 0
	}
	if state.Blocks < 3 {
		state.Blocks++
	}
	delay := 15 * time.Minute * time.Duration(1<<(state.Blocks-1))
	if seconds, err := strconv.ParseInt(strings.TrimSpace(retryAfter), 10, 64); err == nil && seconds > 0 {
		// Bound untrusted input before multiplying to avoid duration overflow.
		if seconds > 24*60*60 {
			seconds = 24 * 60 * 60
		}
		if requested := time.Duration(seconds) * time.Second; requested > delay {
			delay = requested
		}
	} else if date, err := http.ParseTime(retryAfter); err == nil {
		requested := date.Sub(now)
		if requested > 24*time.Hour {
			requested = 24 * time.Hour
		}
		if requested > delay {
			delay = requested
		}
	}
	state.Status, state.LastBlocked, state.Until = status, now, now.Add(delay)
	if err := atomicJSON(filepath.Join(serverDataDir, "sofa_request_backoff.json"), state); err != nil {
		log.Printf("[SOFA-BACKOFF] WRITE_ERROR error=%v", err)
	}
	log.Printf("[SOFA-BACKOFF] PAUSE status=%d blocks=%d duration=%s until=%s", status, state.Blocks, delay, state.Until.Format(time.RFC3339))
}

func waitBackgroundSofa(ctx context.Context) (func(), error) {
	if err := sofaCooldownError(); err != nil {
		return nil, err
	}
	select {
	case sofaBackgroundSlot <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := func() {
		sofaBackgroundLastFinished = time.Now()
		<-sofaBackgroundSlot
	}
	if err := sofaCooldownError(); err != nil {
		release()
		return nil, err
	}
	gap := 2*time.Second + time.Duration(rand.Int64N(int64(2*time.Second)+1))
	if sofaBrowserBase != "" {
		gap = 500*time.Millisecond + time.Duration(rand.Int64N(int64(500*time.Millisecond)+1))
	}
	if delay := time.Until(sofaBackgroundLastFinished.Add(gap)); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			release()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
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
	Origin         string          `json:"origin,omitempty"`
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

type analysisTask struct {
	ctx      context.Context
	mu       sync.Mutex
	job      AnalysisJob
	cancel   context.CancelFunc
	evidence *predictionEvidence
}

func (task *analysisTask) snapshot() AnalysisJob {
	task.mu.Lock()
	defer task.mu.Unlock()
	job := task.job
	job.Warnings = append([]string{}, job.Warnings...)
	// Results are immutable after publication.
	return job
}

func (task *analysisTask) report(stage, message string, done, total int) {
	task.mu.Lock()
	if stage != "" {
		task.job.Stage = stage
	}
	if message != "" {
		task.job.Message = message
	}
	task.job.PlayersDone += done
	task.job.PlayersTotal += total
	job := task.job
	task.mu.Unlock()
	log.Printf("[ANALYSIS-PROGRESS] id=%s match=%q stage=%s players=%d/%d remaining=%d message=%q", job.ID, job.Match, job.Stage, job.PlayersDone, job.PlayersTotal, max(0, job.PlayersTotal-job.PlayersDone), job.Message)
}

func (task *analysisTask) publishPlayers(result []AnalyzedBet, stage, message string) {
	task.mu.Lock()
	for i := range result {
		result[i].AnalysisID = task.job.ID
	}
	// Published slices are immutable. The final calculation creates a new slice.
	task.job.Result = result
	task.job.ResultRevision++
	task.job.Stage = stage
	task.job.Message = message
	id, revision := task.job.ID, task.job.ResultRevision
	task.mu.Unlock()
	log.Printf("ANALYSIS_PLAYERS_READY id=%s stage=%s revision=%d results=%d", id, stage, revision, len(result))
}

func (task *analysisTask) warn(message string) {
	task.mu.Lock()
	for _, old := range task.job.Warnings {
		if old == message {
			task.mu.Unlock()
			return
		}
	}
	task.job.Warnings = append(task.job.Warnings, message)
	id := task.job.ID
	task.mu.Unlock()
	log.Printf("ANALYSIS_WARNING id=%s %s", id, message)
}

func analysisContext(ctx context.Context, job AnalysisJob, source, scope string) context.Context {
	return context.WithValue(ctx, archiveContextKey{}, archiveInfo{Automatic: job.Origin == "automatic", AnalysisID: job.ID, AnalysisCreatedAt: job.CreatedAt, Match: job.Match, Bookmaker: source, Scope: scope})
}

func validateAnalysisRequest(query *AnalysisRequest) error {
	query.HomeID, query.AwayID = strings.TrimSpace(query.HomeID), strings.TrimSpace(query.AwayID)
	if query.MatchLimit == 0 {
		query.MatchLimit = 10
	}
	if query.MatchLimit < 1 || query.MatchLimit > 50 {
		return fmt.Errorf("próba musi zawierać od 1 do 50 meczów")
	}
	for _, id := range []string{query.HomeID, query.AwayID} {
		if id != "" && !regexp.MustCompile(`^[0-9]{1,12}$`).MatchString(id) {
			return fmt.Errorf("niepoprawne ID SofaScore")
		}
	}
	if query.HomeID != "" && query.HomeID == query.AwayID {
		return fmt.Errorf("wybierz dwie różne drużyny")
	}
	query.TournamentIDs = strings.TrimSpace(query.TournamentIDs)
	if query.TournamentIDs == "" {
		query.TournamentIDs = "ALL"
	}
	if query.TournamentIDs != "ALL" {
		parts := strings.Split(query.TournamentIDs, ",")
		if len(parts) > 50 {
			return fmt.Errorf("zbyt wiele rozgrywek")
		}
		for _, part := range parts {
			if id, err := strconv.Atoi(strings.TrimSpace(part)); err != nil || id <= 0 {
				return fmt.Errorf("niepoprawny identyfikator rozgrywek")
			}
		}
	}
	count := 0
	for _, source := range []struct {
		input    *string
		provider string
	}{{&query.SuperbetURL, "SUPERBET"}, {&query.BetclicURL, "BETCLIC"}, {&query.STSURL, "STS"}} {
		*source.input = strings.TrimSpace(*source.input)
		if *source.input == "" {
			continue
		}
		if err := validateMatchURL(*source.input, source.provider); err != nil {
			return err
		}
		count++
	}
	if count != len(serverBookmakers) {
		return fmt.Errorf("analiza wymaga linków do tego samego meczu w Superbet, Betclic i STS")
	}
	return nil
}

// Bookmaker URLs are identifiers only. Network destinations remain fixed in collectors.
func validateMatchURL(input, provider string) error {
	u, err := url.Parse(input)
	if err != nil || len(input) > 2048 {
		return fmt.Errorf("niepoprawny link %s", provider)
	}
	hosts := map[string]string{"SUPERBET": "superbet.pl", "BETCLIC": "betclic.pl", "STS": "sts.pl"}
	host := strings.ToLower(u.Hostname())
	if (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || (host != hosts[provider] && host != "www."+hosts[provider]) {
		return fmt.Errorf("link musi prowadzić do %s", hosts[provider])
	}
	id, _, detected := extractIDAndSlug(input)
	if id == "" || detected != provider || !regexp.MustCompile(`^[0-9]{1,20}$`).MatchString(id) {
		return fmt.Errorf("brak poprawnego ID meczu %s w linku", provider)
	}
	return nil
}

func analysisTitle(query AnalysisRequest) string {
	for _, input := range []string{query.SuperbetURL, query.BetclicURL, query.STSURL} {
		_, slug, provider := extractIDAndSlug(input)
		home, away := parseTeamsFromSlug(slug, provider)
		if home != "" && away != "" {
			return strings.ReplaceAll(home, "-", " ") + " vs " + strings.ReplaceAll(away, "-", " ")
		}
	}
	return "druzyny-" + query.HomeID + "-vs-" + query.AwayID
}

func decodeAPIRequest(w http.ResponseWriter, r *http.Request, target any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("wymagany jeden obiekt JSON")
	}
	return nil
}

func (s *calendarServer) registerAnalysisAPI(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/analyses/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(id) {
			apiError(w, 400, fmt.Errorf("niepoprawne ID analizy"))
			return
		}
		s.analysisMu.Lock()
		task := s.jobs[id]
		s.analysisMu.Unlock()
		if task == nil {
			apiError(w, 404, fmt.Errorf("nie znaleziono aktywnego zlecenia"))
			return
		}
		job := task.snapshot()
		if job.Origin != "manual" {
			apiError(w, 409, fmt.Errorf("anulowanie dotyczy ręcznej analizy"))
			return
		}
		if job.Status == "queued" || job.Status == "running" {
			task.cancel()
			log.Printf("INFO Anulowanie analizy na żądanie użytkownika id=%s", id)
		}
		jsonResponse(w, 200, true)
	})
	mux.HandleFunc("POST /api/v1/analyses", func(w http.ResponseWriter, r *http.Request) {
		var query AnalysisRequest
		if err := decodeAPIRequest(w, r, &query); err != nil {
			apiError(w, 400, err)
			return
		}
		if err := validateAnalysisRequest(&query); err != nil {
			apiError(w, 400, err)
			return
		}
		if _, err := s.analysisEvents(query); err != nil {
			apiError(w, 400, fmt.Errorf("kwalifikacja meczu: %w", err))
			return
		}
		var random [12]byte
		if _, err := cryptorand.Read(random[:]); err != nil {
			apiError(w, 500, err)
			return
		}
		ctx, cancel := context.WithTimeout(s.ctx, 20*time.Minute)
		task := &analysisTask{ctx: ctx, job: AnalysisJob{ID: fmt.Sprintf("%x", random[:]), Origin: "manual", Match: analysisTitle(query), Status: "queued", Stage: "queued", Message: "Oczekiwanie na analizę na serwerze", CreatedAt: time.Now().UTC(), Request: query, Warnings: []string{}}, cancel: cancel}
		s.analysisMu.Lock()
		defer s.analysisMu.Unlock()
		if s.ctx.Err() != nil || len(s.analysisQueue) >= cap(s.analysisQueue) {
			cancel()
			apiError(w, 503, fmt.Errorf("kolejka analiz jest pełna lub serwer jest zatrzymywany"))
			return
		}
		job := task.snapshot()
		archiveCtx := analysisContext(ctx, job, "ANALIZA", "zlecenie-proba-"+strconv.Itoa(query.MatchLimit)+"-meczow")
		if err := archiveResponse(archiveCtx, "parametry-analizy", nil, "", job, nil); err != nil {
			cancel()
			apiError(w, 500, err)
			return
		}
		if err := atomicJSON(filepath.Join(s.dataDir, "analysis-jobs", job.ID+".json"), job); err != nil {
			cancel()
			apiError(w, 500, err)
			return
		}
		// Keep at most 64 completed jobs in memory; all final jobs remain on disk.
		if len(s.jobs) >= 64 {
			for id, old := range s.jobs {
				state := old.snapshot()
				if state.Status != "queued" && state.Status != "running" {
					delete(s.jobs, id)
					if len(s.jobs) < 64 {
						break
					}
				}
			}
		}
		s.jobs[job.ID] = task
		s.analysisQueue <- task
		jsonResponse(w, http.StatusAccepted, job)
	})
	mux.HandleFunc("GET /api/v1/analyses/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(id) {
			apiError(w, 400, fmt.Errorf("niepoprawne ID analizy"))
			return
		}
		s.analysisMu.Lock()
		task := s.jobs[id]
		s.analysisMu.Unlock()
		if task != nil {
			job := task.snapshot()
			// Older clients omit afterRevision and still receive the full result.
			if after, err := strconv.Atoi(r.URL.Query().Get("afterRevision")); err == nil && after >= job.ResultRevision {
				job.Result = nil
			}
			jsonResponse(w, 200, job)
			return
		}
		var job AnalysisJob
		raw, err := os.ReadFile(filepath.Join(s.dataDir, "analysis-jobs", id+".json"))
		if os.IsNotExist(err) {
			apiError(w, 404, fmt.Errorf("nie znaleziono analizy"))
			return
		}
		if err == nil {
			err = json.Unmarshal(raw, &job)
		}
		if err != nil {
			apiError(w, 500, err)
			return
		}
		if job.Status == "queued" || job.Status == "running" {
			job.Status = "failed"
			job.Stage = "failed"
			job.Error = "Serwer został uruchomiony ponownie. Zleć analizę jeszcze raz."
			job.Message = job.Error
		}
		jsonResponse(w, 200, job)
	})
	mux.HandleFunc("POST /api/v1/teams/detect", func(w http.ResponseWriter, r *http.Request) {
		var query struct {
			URL  string `json:"url"`
			Home string `json:"home"`
			Away string `json:"away"`
		}
		if err := decodeAPIRequest(w, r, &query); err != nil {
			apiError(w, 400, err)
			return
		}
		if len(query.Home) > 200 || len(query.Away) > 200 || len(query.URL) > 2048 {
			apiError(w, 400, fmt.Errorf("zbyt długa nazwa lub link"))
			return
		}
		home, away := calendarTeamSlug(query.Home), calendarTeamSlug(query.Away)
		if query.URL != "" {
			id, slug, provider := extractIDAndSlug(query.URL)
			if err := validateMatchURL(query.URL, provider); err != nil {
				apiError(w, 400, err)
				return
			}
			if s.womenOffer(provider, id) {
				apiError(w, 400, fmt.Errorf("detekcja meczów kobiet jest wyłączona"))
				return
			}
			if home == "" || away == "" {
				home, away = parseTeamsFromSlug(slug, provider)
			}
		}
		if isWomenEvent(CalendarEvent{Home: home, Away: away}) || s.sofaContextForURL(query.URL).Gender == "F" {
			apiError(w, 400, fmt.Errorf("detekcja meczów kobiet jest wyłączona"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
		defer cancel()
		stop := context.AfterFunc(s.ctx, cancel)
		defer stop()
		client, err := newSofaClient(context.WithValue(ctx, archiveContextKey{}, archiveInfo{Bookmaker: "SOFASCORE", Scope: "wyszukiwanie-druzyn-" + home + "-vs-" + away}), nil, nil)
		if err != nil {
			apiError(w, 502, err)
			return
		}
		defer client.tlsClient.CloseIdleConnections()
		result := detectTeamsBySlugs(client, home, away, s.sofaContextForURL(query.URL))
		if err := client.archiveError(); err != nil {
			apiError(w, 500, err)
			return
		}
		jsonResponse(w, 200, result)
	})
	mux.HandleFunc("POST /api/v1/teams/mapping", func(w http.ResponseWriter, r *http.Request) {
		var query struct {
			Slug   string `json:"slug"`
			SofaID string `json:"sofaId"`
		}
		if err := decodeAPIRequest(w, r, &query); err != nil {
			apiError(w, 400, err)
			return
		}
		if query.Slug == "" || len(query.Slug) > 200 || !regexp.MustCompile(`^[0-9]{1,12}$`).MatchString(query.SofaID) {
			apiError(w, 400, fmt.Errorf("niepoprawna nazwa lub ID drużyny"))
			return
		}
		if err := persistTeamCache(query.Slug, query.SofaID); err != nil {
			apiError(w, 500, err)
			return
		}
		jsonResponse(w, 200, true)
	})
	mux.HandleFunc("GET /api/v1/tournaments", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("homeId")
		if !regexp.MustCompile(`^[0-9]{1,12}$`).MatchString(id) {
			apiError(w, 400, fmt.Errorf("niepoprawne ID drużyny"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		client, err := newSofaClient(context.WithValue(ctx, archiveContextKey{}, archiveInfo{Bookmaker: "SOFASCORE", Scope: "rozgrywki-druzyny-" + id}), nil, nil)
		if err != nil {
			apiError(w, 502, err)
			return
		}
		defer client.tlsClient.CloseIdleConnections()
		body, err := client.GetSofa("/team/" + id + "/standings/seasons")
		if err != nil {
			apiError(w, 502, err)
			return
		}
		var res SofaSeasonsResponse
		if err := json.Unmarshal(body, &res); err != nil {
			apiError(w, 502, err)
			return
		}
		seen := map[int]bool{}
		list := []LeagueOption{}
		for _, ts := range res.TournamentSeasons {
			tournament := ts.Tournament.UniqueTournament
			if tournament.ID > 0 && !seen[tournament.ID] {
				seen[tournament.ID] = true
				list = append(list, LeagueOption{ID: tournament.ID, Name: tournament.Name})
			}
		}
		jsonResponse(w, 200, list)
	})
}

func (s *calendarServer) analysisWorker() {
	defer close(s.analysisDone)
	for {
		select {
		case task := <-s.analysisQueue:
			s.runAnalysisTask(task)
		case <-s.ctx.Done():
			for {
				select {
				case task := <-s.analysisQueue:
					s.finishAnalysisTask(task, nil, s.ctx.Err())
				default:
					return
				}
			}
		}
	}
}

func (s *calendarServer) runAnalysisTask(task *analysisTask) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.finishAnalysisTask(task, nil, fmt.Errorf("błąd wewnętrzny analizy: %v", recovered))
		}
	}()
	if err := task.ctx.Err(); err != nil {
		s.finishAnalysisTask(task, nil, err)
		return
	}
	release, err := s.beginForeground(task.ctx)
	if err != nil {
		s.finishAnalysisTask(task, nil, err)
		return
	}
	defer release()
	task.ctx = context.WithValue(task.ctx, sofaManualContextKey{}, true)
	task.mu.Lock()
	task.job.Status = "running"
	task.job.StartedAt = time.Now().UTC()
	task.mu.Unlock()
	job := task.snapshot()
	log.Printf("ANALYSIS_START id=%s match=%q", job.ID, job.Match)
	client, err := newSofaClient(analysisContext(task.ctx, job, "SOFASCORE", "statystyki"), task.warn, task.report)
	if err != nil {
		s.finishAnalysisTask(task, nil, err)
		return
	}
	defer client.tlsClient.CloseIdleConnections()
	engine := analysisEngine{client: client, task: task, server: s}
	result, err := engine.analyze(job.Request)
	err = errors.Join(err, client.archiveError(), task.ctx.Err())
	s.finishAnalysisTask(task, result, err)
}

func (s *calendarServer) finishAnalysisTask(task *analysisTask, result []AnalyzedBet, err error) {
	defer task.cancel()
	job := task.snapshot()
	for i := range result {
		result[i].AnalysisID = job.ID
	}
	preview := job.Result
	job.FinishedAt = time.Now().UTC()
	job.ResultRevision++
	job.Result = result
	job.Status, job.Stage, job.Message = "completed", "completed", fmt.Sprintf("Analiza zakończona: %d zakładów", len(result))
	if err != nil {
		job.Status, job.Stage, job.Error = "failed", "failed", err.Error()
		job.Message = job.Error
		// Keep the latest published snapshot, including an already priced XI,
		// if context or reserve fetching fails later.
		job.Result = preview
	}
	ctx := analysisContext(context.Background(), job, "ANALIZA", "wynik-proba-"+strconv.Itoa(job.Request.MatchLimit)+"-meczow")
	task.report("saving-results", "Zapisywanie wyniku analizy w archiwum", 0, 0)
	if saveErr := archiveResponse(ctx, "wynik-analizy-algorytm-"+fairModelVersion, nil, "", job, err); saveErr != nil {
		job.Status, job.Stage, job.Error = "failed", "failed", errors.Join(err, saveErr).Error()
		job.Message = job.Error
		job.Result = nil
	}
	if job.Status == "completed" && s.research != nil && task.evidence != nil && (job.Origin == "automatic" || (task.evidence.Fixture.ID != "" && job.FinishedAt.Before(task.evidence.Fixture.StartTime))) {
		task.report("saving-predictions", "Zapisywanie prognoz Superbet, Betclic i STS", 0, 0)
		if saveErr := s.savePredictions(job, task.evidence, task.ctx); saveErr != nil {
			job.Status, job.Stage, job.Error = "failed", "failed", "zapis prognoz: "+saveErr.Error()
			job.Message = job.Error
			log.Printf("[PREDICTIONS] id=%s stage=save-error error=%v", job.ID, saveErr)
		}
	}
	resultCount := len(job.Result)
	if job.Origin == "automatic" {
		job.Result = nil
	}
	if job.Origin != "automatic" {
		task.report("saving-results", "Zapisywanie końcowego stanu analizy", 0, 0)
		if saveErr := atomicJSON(filepath.Join(s.dataDir, "analysis-jobs", job.ID+".json"), job); saveErr != nil {
			job.Status, job.Stage, job.Error = "failed", "failed", errors.Join(err, saveErr).Error()
			job.Message = job.Error
			job.Result = nil
			log.Printf("ANALYSIS_SAVE_ERROR id=%s %v", job.ID, saveErr)
		}
	}
	task.mu.Lock()
	task.job = job
	task.mu.Unlock()
	log.Printf("ANALYSIS_END id=%s status=%s results=%d error=%q", job.ID, job.Status, resultCount, job.Error)
}

type analysisEngine struct {
	client *ResilientClient
	task   *analysisTask
	server *calendarServer
}

type sofaMatchContext struct {
	StartTime time.Time
	Gender    string
}

// Use the already cached calendar; detecting teams must not download it again.
func (s *calendarServer) sofaContextForURL(input string) sofaMatchContext {
	result := sofaMatchContext{}
	if s == nil || s.cache == nil || input == "" {
		return result
	}
	linkKey := func(raw string) string {
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil {
			return ""
		}
		parsed.RawQuery, parsed.Fragment = "", ""
		return strings.TrimRight(parsed.String(), "/")
	}
	key := linkKey(input)
	if key == "" {
		return result
	}
	s.cache.once.Do(s.cache.readDisk)
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	var latest time.Time
	for _, entry := range s.cache.entries {
		if entry.Calendar == nil || entry.FetchedAt.Before(latest) {
			continue
		}
		for _, event := range entry.Calendar.Events {
			if linkKey(event.URL) != key {
				continue
			}
			latest = entry.FetchedAt
			result.StartTime, _ = time.Parse(time.RFC3339, event.StartTime)
			result.Gender = "M"
			_, competitionGender := sofaTeamNameAndGender(event.Competition)
			if competitionGender == "F" || isWomenEvent(event) {
				result.Gender = "F"
			}
		}
	}
	return result
}

type sofaResolvedTeamPair struct {
	Home, Away SofaTeamItem
}

func matchSofaTeamPair(client *ResilientClient, home, away []SofaTeamItem, match sofaMatchContext, unverified map[int]string) (sofaResolvedTeamPair, error) {
	if len(home) == 0 || len(away) == 0 || len(home) > 6 || len(away) > 6 {
		return sofaResolvedTeamPair{}, fmt.Errorf("nie można rozstrzygnąć drużyn na podstawie terminarza (%d i %d kandydatów)", len(home), len(away))
	}
	homeIDs, awayIDs := map[int]bool{}, map[int]bool{}
	for _, team := range home {
		homeIDs[team.ID] = true
	}
	for _, team := range away {
		awayIDs[team.ID] = true
	}
	pairs := map[[2]int]sofaResolvedTeamPair{}
	var lastErr error
	var rejectedIdentity string
	completeSide := false
	seenTeams := map[int]bool{}
	for _, side := range [][]SofaTeamItem{home, away} {
		complete := true
		for _, team := range side {
			if seenTeams[team.ID] {
				complete = false
				continue
			}
			seenTeams[team.ID] = true
			body, err := client.withScope("rozpoznanie-druzyn-terminarz-" + strconv.Itoa(team.ID)).GetSofa(fmt.Sprintf("/team/%d/events/next/0", team.ID))
			var response SofaTeamEventsResponse
			if err == nil {
				err = json.Unmarshal(body, &response)
			}
			if err != nil {
				complete, lastErr = false, err
				continue
			}
			for _, event := range response.Events {
				if event.ID <= 0 || event.StartTS <= 0 || event.Status.Code >= 100 {
					continue
				}
				when := time.Unix(event.StartTS, 0)
				if !match.StartTime.IsZero() {
					if delta := when.Sub(match.StartTime); delta < -10*time.Minute || delta > 10*time.Minute {
						continue
					}
				} else if when.Before(time.Now().Add(-2 * time.Hour)) {
					continue
				}
				if gender := strings.ToUpper(event.HomeTeam.Gender); gender != "" && gender != match.Gender {
					continue
				}
				if gender := strings.ToUpper(event.AwayTeam.Gender); gender != "" && gender != match.Gender {
					continue
				}
				homeTeam, awayTeam := event.HomeTeam, event.AwayTeam
				if homeIDs[awayTeam.ID] && awayIDs[homeTeam.ID] {
					homeTeam, awayTeam = awayTeam, homeTeam
				}
				h, a := homeTeam.ID, awayTeam.ID
				if h == a || !homeIDs[h] || !awayIDs[a] {
					continue
				}
				valid := true
				for _, candidate := range []SofaTeamItem{homeTeam, awayTeam} {
					if slug := unverified[candidate.ID]; slug != "" {
						if matched, reason := sofaTeamMatchReason(slug, candidate, match.Gender, !match.StartTime.IsZero()); !matched {
							valid = false
							rejectedIdentity = fmt.Sprintf("ID %d ze starego cache: %s", candidate.ID, reason)
							log.Printf("WARNING SofaScore: %s", rejectedIdentity)
						}
					}
				}
				if !valid {
					continue
				}
				pairs[[2]int{h, a}] = sofaResolvedTeamPair{Home: homeTeam, Away: awayTeam}
				log.Printf("[SOFA-DETECT] FIXTURE event_id=%d home_id=%d away_id=%d start=%s expected_start=%s", event.ID, h, a, when.UTC().Format(time.RFC3339), match.StartTime.Format(time.RFC3339))
			}
		}
		completeSide = completeSide || complete
	}
	// An empty home schedule must not prevent checking the away team's feed.
	if len(pairs) == 1 && completeSide {
		for _, pair := range pairs {
			return pair, nil
		}
	}
	if lastErr != nil {
		return sofaResolvedTeamPair{}, fmt.Errorf("weryfikacja terminarza: %w", lastErr)
	}
	if rejectedIdentity != "" {
		return sofaResolvedTeamPair{}, errors.New(rejectedIdentity)
	}
	return sofaResolvedTeamPair{}, fmt.Errorf("terminarz nie wskazał jednej pary drużyn: %d pasujących par", len(pairs))
}

func detectTeamsBySlugs(client *ResilientClient, home, away string, match sofaMatchContext) TeamDetectionResult {
	_, homeGender := sofaTeamNameAndGender(home)
	_, awayGender := sofaTeamNameAndGender(away)
	if homeGender == "F" || awayGender == "F" {
		match.Gender = "F"
	}
	if match.Gender == "" {
		match.Gender = "M"
	}
	cache := loadTeamCache()
	result := TeamDetectionResult{HomeSlug: home, AwaySlug: away, HomeID: verifiedTeamIDInCache(home, cache, match.Gender), AwayID: verifiedTeamIDInCache(away, cache, match.Gender), Warnings: []string{}}
	var candidates [2][]SofaTeamItem
	var searchErrors [2]error
	var searches sync.WaitGroup
	var legacyHints [2]bool
	unverified := map[int]string{}
	searchSide := func(index int, slug string) {
		searches.Add(1)
		go func() {
			defer searches.Done()
			if slug == "" {
				searchErrors[index] = fmt.Errorf("nie udało się odczytać nazwy drużyny")
			} else {
				candidates[index], searchErrors[index] = searchSofaTeamCandidates(client.withScope("wyszukiwanie-druzyny-"+slug), slug, match.Gender, !match.StartTime.IsZero())
			}
		}()
	}
	for index, team := range []struct {
		label, slug string
		id          *string
	}{{"Gospodarze", home, &result.HomeID}, {"Goście", away, &result.AwayID}} {
		if *team.id != "" {
			id, _ := strconv.Atoi(*team.id)
			candidates[index] = []SofaTeamItem{{ID: id}}
			continue
		}
		if !match.StartTime.IsZero() {
			if id, err := strconv.Atoi(findTeamIDInCache(team.slug, cache)); err == nil && id > 0 {
				// An old mapping is only a hint. Validate source name, category,
				// opponent and kickoff before promoting it to a verified mapping.
				candidates[index] = []SofaTeamItem{{ID: id}}
				legacyHints[index], unverified[id] = true, team.slug
				continue
			}
		}
		searchSide(index, team.slug)
	}
	searches.Wait()
	requiresPair := !match.StartTime.IsZero() || len(candidates[0]) > 1 || len(candidates[1]) > 1
	pairVerified := false
	if requiresPair {
		for attempt := 0; attempt < 2; attempt++ {
			var pairErr error
			if len(candidates[0]) > 0 && len(candidates[1]) > 0 {
				pair, err := matchSofaTeamPair(client, candidates[0], candidates[1], match, unverified)
				pairErr = err
				if err == nil {
					candidates[0], candidates[1] = []SofaTeamItem{pair.Home}, []SofaTeamItem{pair.Away}
					pairVerified = true
					break
				}
			}
			if attempt == 0 && (legacyHints[0] || legacyHints[1]) && client.ctx.Err() == nil {
				for index, slug := range []string{home, away} {
					if legacyHints[index] {
						log.Printf("WARNING SofaScore: ponowne wyszukiwanie %q po nieudanej weryfikacji ID ze starego cache", slug)
						searchSide(index, slug)
					}
				}
				searches.Wait()
				unverified = nil
				continue
			}
			if pairErr != nil {
				searchErrors[0], searchErrors[1] = pairErr, pairErr
			}
			break
		}
	}
	for index, team := range []struct {
		label, slug string
		id          *string
	}{{"Gospodarze", home, &result.HomeID}, {"Goście", away, &result.AwayID}} {
		// Never save a name variant (or retain a cached ID for this match)
		// if the opponent was missing or fixture verification did not finish.
		if requiresPair && !pairVerified {
			*team.id = ""
			if searchErrors[index] == nil {
				searchErrors[index] = fmt.Errorf("nie potwierdzono obu drużyn i terminu meczu w SofaScore")
			}
		}
		if *team.id != "" {
			continue
		}
		if searchErrors[index] == nil && len(candidates[index]) == 1 {
			selected := candidates[index][0]
			*team.id = strconv.Itoa(selected.ID)
			saveTeamCache(team.slug, selected, match.Gender)
			log.Printf("[SOFA-SEARCH] MATCH slug=%q id=%s name=%q gender=%q", team.slug, *team.id, selected.Name, match.Gender)
			continue
		}
		err := searchErrors[index]
		if err == nil {
			err = fmt.Errorf("%d zgodnych kandydatów; brak jednoznacznego potwierdzenia w terminarzu", len(candidates[index]))
		}
		message := fmt.Sprintf("%s (%s): %v", team.label, team.slug, err)
		result.Warnings = append(result.Warnings, message)
		log.Printf("[SOFA-DETECT] ERROR %s", message)
	}
	log.Printf("[SOFA-DETECT] RESULT home=%q home_id=%q away=%q away_id=%q warnings=%d", home, result.HomeID, away, result.AwayID, len(result.Warnings))
	return result
}

func (engine *analysisEngine) analyze(query AnalysisRequest) ([]AnalyzedBet, error) {
	task, client := engine.task, engine.client
	events, err := engine.server.analysisEvents(query)
	if err != nil {
		return nil, fmt.Errorf("kwalifikacja meczu: %w", err)
	}
	var match sofaMatchContext
	var homeName, awayName string
	task.report("odds", "Przygotowywanie oferty Superbet, Betclic i STS", 0, 0)
	type sourceResult struct {
		book  string
		props []NormalizedProp
		odds  matchOddsSnapshot
		err   error
	}
	sources := make(chan sourceResult, 3)
	count := 0
	for _, source := range []struct{ book, input string }{{"SUPERBET", query.SuperbetURL}, {"BETCLIC", query.BetclicURL}, {"STS", query.STSURL}} {
		if source.input == "" {
			continue
		}
		count++
		go func(book, input string) {
			id, _, _ := extractIDAndSlug(input)
			ctx := analysisContext(task.ctx, task.snapshot(), book, "kursy-mecz-"+id)
			if job := task.snapshot(); job.Origin == "manual" {
				// Reuse histories, but require quotes observed after this click.
				ctx = context.WithValue(ctx, oddsNotBeforeContextKey{}, job.CreatedAt)
			}
			odds, err := engine.server.matchOdds(ctx, book, id)
			props := odds.Props
			if !errors.Is(err, context.Canceled) {
				engine.server.rememberMarketAvailability(marketAvailabilityFromOdds(book, id, odds, err))
			}
			if err == nil && len(props) == 0 {
				err = fmt.Errorf("brak obsługiwanych rynków zawodników w odpowiedzi")
			}
			if saveErr := archiveResponse(ctx, "kursy-odczytane-rynki-zawodnikow", nil, "", map[string]any{"url": input, "eventId": id, "archiveVersion": odds.ArchiveVersion, "oddsObservedAt": odds.ObservedAt, "oddsExpiresAt": odds.ExpiresAt, "reusedFromCache": odds.FromCache, "propsCount": len(props)}, err); saveErr != nil {
				err = errors.Join(err, saveErr)
			}
			sources <- sourceResult{book: book, props: props, odds: odds, err: err}
		}(source.book, source.input)
	}
	allProps := []NormalizedProp{}
	oddsSources := map[string]any{}
	sourceErrors := map[string]error{}
	var archiveErr error
	for i := 0; i < count; i++ {
		result := <-sources
		if result.err != nil {
			sourceErrors[result.book] = result.err
			task.warn(result.book + ": " + result.err.Error())
			if isArchiveFailure(result.err) {
				archiveErr = errors.Join(archiveErr, result.err)
			}
			continue
		}
		allProps = append(allProps, result.props...)
		oddsSources[result.book] = map[string]any{"eventId": result.odds.EventID, "archiveVersion": result.odds.ArchiveVersion, "observedAt": result.odds.ObservedAt, "expiresAt": result.odds.ExpiresAt, "reusedFromCache": result.odds.FromCache}
	}
	if archiveErr != nil {
		return nil, archiveErr
	}
	if len(oddsSources) != len(serverBookmakers) {
		var reasons []string
		for _, book := range serverBookmakers {
			if sourceErr := sourceErrors[book]; sourceErr != nil {
				reasons = append(reasons, book+": "+sourceErr.Error())
			} else if _, ok := oddsSources[book]; !ok {
				reasons = append(reasons, book+": brak źródła oferty")
			}
		}
		return nil, fmt.Errorf("pominięto analizę: wymagane są aktualne oferty z obsługiwanymi rynkami wszystkich trzech bukmacherów; %s", strings.Join(reasons, "; "))
	}
	task.report("teams", "Rozpoznawanie drużyn w SofaScore", 0, 0)
	// Revalidate automatic IDs before fetching any player statistics. An old
	// desktop/history entry can still carry a wrong ID from the legacy cache.
	{
		for _, input := range []string{query.SuperbetURL, query.BetclicURL, query.STSURL} {
			if input == "" {
				continue
			}
			_, _, provider := extractIDAndSlug(input)
			event := events[provider]
			home, away := calendarTeamSlug(event.Home), calendarTeamSlug(event.Away)
			homeName, awayName = event.Home, event.Away
			cached := loadTeamCache()
			oldHomeID, oldAwayID := findTeamIDInCache(home, cached), findTeamIDInCache(away, cached)
			match.StartTime, _ = time.Parse(time.RFC3339, event.StartTime)
			match.Gender = "M"
			if match.Gender == "F" || isWomenEvent(CalendarEvent{Home: home, Away: away}) {
				return nil, fmt.Errorf("analizy meczów kobiet są wyłączone")
			}
			teams := detectTeamsBySlugs(client, home, away, match)
			for _, warning := range teams.Warnings {
				task.warn("SofaScore: " + warning)
			}
			resolve := func(label, provided, detected, oldID string) (string, error) {
				if detected == "" {
					if provided != "" && provided != oldID {
						task.warn(label + ": użyto ręcznie podanego ID; automatyczne dopasowanie nazwy nie powiodło się")
						return provided, nil
					}
					return "", fmt.Errorf("%w: %s; %s", errSofaTeamVerification, label, strings.Join(teams.Warnings, "; "))
				}
				if provided != "" && provided != detected {
					// The desktop history can outlive the server cache. A verified
					// detection (including a saved manual mapping) takes precedence
					// even when the stale ID is no longer present in that cache.
					task.warn(fmt.Sprintf("%s: zastąpiono ID SofaScore przesłane przez klienta %s -> %s po weryfikacji drużyny", label, provided, detected))
				}
				return detected, nil
			}
			var resolveErr error
			query.HomeID, resolveErr = resolve("Gospodarze", query.HomeID, teams.HomeID, oldHomeID)
			if resolveErr != nil {
				return nil, resolveErr
			}
			query.AwayID, resolveErr = resolve("Goście", query.AwayID, teams.AwayID, oldAwayID)
			if resolveErr != nil {
				return nil, resolveErr
			}
			break
		}
	}
	if query.HomeID == "" || query.AwayID == "" || query.HomeID == query.AwayID {
		return nil, fmt.Errorf("nie udało się ustalić dwóch różnych ID drużyn w SofaScore; sprawdź ostrzeżenia detekcji")
	}
	task.mu.Lock()
	task.job.Request = query
	task.mu.Unlock()
	sort.Slice(allProps, func(i, j int) bool {
		if allProps[i].Bookmaker != allProps[j].Bookmaker {
			return allProps[i].Bookmaker < allProps[j].Bookmaker
		}
		if allProps[i].PlayerName != allProps[j].PlayerName {
			return allProps[i].PlayerName < allProps[j].PlayerName
		}
		if allProps[i].MarketType != allProps[j].MarketType {
			return allProps[i].MarketType < allProps[j].MarketType
		}
		return allProps[i].Line < allProps[j].Line
	})
	targets := []string{}
	seen := map[string]bool{}
	for _, prop := range allProps {
		if !seen[prop.PlayerName] {
			seen[prop.PlayerName] = true
			targets = append(targets, prop.PlayerName)
		}
	}
	task.report("statistics", "Pobieranie historii zawodników i składów z SofaScore", 0, 0)
	fixture, reversed, fixtureErr := engine.server.statsFixtureForMatch(client, query.HomeID, query.AwayID, match.StartTime)
	if fixtureErr != nil {
		return nil, fmt.Errorf("SofaScore: nie potwierdzono właściwego meczu przed pobraniem statystyk: %w", fixtureErr)
	} else {
		client.statsFixture = fixture
	}
	lineupEventID := fixture.ID
	var matchContext analysisMatchContext
	contextReady := false
	playerData, err := fetchSofaMatchPlayers(client, query.HomeID, query.AwayID, homeName, awayName, lineupEventID, reversed, query.MatchLimit, query.TournamentIDs, targets, func(data sofaMatchPlayerData, stage, message string) error {
		players := append(append([]*PlayerStatHistory(nil), data.Home...), data.Away...)
		if !contextReady {
			preview, err := calculateValueBetsContext(task.ctx, allProps, data.Home, data.Away, data.Lineup.Home, data.Lineup.Away, data.Lineup.Confirmed, nil, analysisMatchContext{Pending: true})
			if err != nil {
				return err
			}
			if len(preview) > 0 {
				task.publishPlayers(preview, stage, message)
			}
			// Without an available XI, finish discovering players first.
			if stage != "matchup-xi" {
				return nil
			}
			task.report("matchup-xi", "Pobieranie kontekstu drużyn i wyceny XI", 0, 0)
			matchContext = fetchAnalysisMatchContext(client.withScope("matchup-XI-druzyny-i-tabela"), query.HomeID, query.AwayID, players)
			if err := errors.Join(client.archiveError(), task.ctx.Err()); err != nil {
				return err
			}
			// This priced snapshot can be used before reserves finish, so retain
			// its exact model inputs even if a later phase is interrupted.
			ctx := analysisContext(task.ctx, task.snapshot(), "ANALIZA", "dane-XI-z-kontekstem")
			if err := archiveResponse(ctx, "kursy-statystyki-sklady-matchup-XI-uzyte-do-obliczen", nil, "", map[string]any{"algorithm": fairModelVersion, "request": query, "oddsSources": oddsSources, "homePlayers": data.Home, "awayPlayers": data.Away, "homeLineup": data.Lineup.Home, "awayLineup": data.Lineup.Away, "lineupsConfirmed": data.Lineup.Confirmed, "lineupEventId": lineupEventID, "matchContext": matchContext}, nil); err != nil {
				return err
			}
			contextReady = true
			stage, message = "statistics-bench", "XI wycenione — pobieranie statystyk rezerwy"
		} else {
			// New histories may introduce other opponents. Reuse the existing
			// match context and add only missing season profiles within its budget.
			task.report("reserve-pricing", "Uzupełnianie wyceny rezerwy i brakujących profili rywali", 0, 0)
			matchContext = extendAnalysisMatchProfiles(client.withScope("matchup-rezerwa-brakujace-profile"), matchContext, players, false)
			if err := errors.Join(client.archiveError(), task.ctx.Err()); err != nil {
				return err
			}
		}
		preview, err := calculateValueBetsContext(task.ctx, allProps, data.Home, data.Away, data.Lineup.Home, data.Lineup.Away, data.Lineup.Confirmed, nil, matchContext)
		if err != nil {
			return err
		}
		if len(preview) > 0 {
			task.publishPlayers(preview, stage, message)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	homePlayers, awayPlayers := playerData.Home, playerData.Away
	homeLineup, awayLineup, lineupsConfirmed := playerData.Lineup.Home, playerData.Lineup.Away, playerData.Lineup.Confirmed
	if len(homePlayers) == 0 && len(awayPlayers) == 0 {
		return nil, errNoPlayerHistory
	}
	if len(homePlayers) == 0 {
		task.warn("SofaScore: brak historii zawodników gospodarzy")
	}
	if len(awayPlayers) == 0 {
		task.warn("SofaScore: brak historii zawodników gości")
	}
	if !contextReady {
		task.report("matchup", "Statystyki zawodników gotowe — uzupełnianie kontekstu i wyceny", 0, 0)
		contextPlayers := append(append([]*PlayerStatHistory(nil), homePlayers...), awayPlayers...)
		matchContext = fetchAnalysisMatchContext(client.withScope("matchup-druzyny-i-tabela"), query.HomeID, query.AwayID, contextPlayers)
	}
	if err := errors.Join(client.archiveError(), task.ctx.Err()); err != nil {
		return nil, err
	}
	task.report("saving-inputs", "Zapisywanie statystyk i kontekstu użytych w wycenie", 0, 0)
	ctx := analysisContext(task.ctx, task.snapshot(), "ANALIZA", "dane-wejsciowe")
	if err := archiveResponse(ctx, "kursy-statystyki-sklady-matchup-uzyte-do-obliczen", nil, "", map[string]any{"algorithm": fairModelVersion, "request": query, "propsCount": len(allProps), "oddsSources": oddsSources, "homePlayers": homePlayers, "awayPlayers": awayPlayers, "homeLineup": homeLineup, "awayLineup": awayLineup, "lineupsConfirmed": lineupsConfirmed, "lineupEventId": lineupEventID, "matchContext": matchContext}, nil); err != nil {
		return nil, err
	}
	results, err := calculateValueBetsContext(task.ctx, allProps, homePlayers, awayPlayers, homeLineup, awayLineup, lineupsConfirmed, func(done, total int) {
		task.report("calculation", fmt.Sprintf("Obliczanie hitrate, kursów fair i EV · selekcje %d/%d", done, total), 0, 0)
	}, matchContext)
	if err != nil {
		return nil, err
	}
	bookmakerStartTime, _ := time.Parse(time.RFC3339, events["STS"].StartTime)
	task.mu.Lock()
	task.evidence = &predictionEvidence{LineupDetails: playerData.Lineup.Details, SchemaVersion: 1, Fixture: fixture, BookmakerStartTime: bookmakerStartTime, Request: query, HomePlayers: homePlayers, AwayPlayers: awayPlayers, HomeLineup: homeLineup, AwayLineup: awayLineup, LineupsConfirmed: lineupsConfirmed, Context: matchContext, OddsSources: oddsSources}
	task.mu.Unlock()
	lineupCounts := map[string]int{}
	for _, bet := range results {
		lineupCounts[bet.LineupStatus]++
	}
	log.Printf("[SOFA-LINEUP] MATCHED eventID=%q confirmed=%v result_statuses=%v", lineupEventID, lineupsConfirmed, lineupCounts)
	if len(results) > 0 && lineupCounts["STARTER"]+lineupCounts["PREDICTED_STARTER"] == 0 {
		task.warn(fmt.Sprintf("Składy SofaScore (mecz %s): nie dopasowano żadnego startera do wyników analizy; status nieznany: %d/%d. Filtr XI nie będzie miał wyników.", lineupEventID, lineupCounts["UNKNOWN"], len(results)))
	}
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].HitRate == results[j].HitRate {
			return results[i].EVWithTax > results[j].EVWithTax
		}
		return results[i].HitRate > results[j].HitRate
	})
	if len(results) == 0 {
		task.warn("Nie dopasowano żadnego rynku do dostępnych statystyk zawodników")
	}
	return results, nil
}

// Availability requires a supported player market with a usable quote. A
// BetBuilder flag, market title or empty offer is not sufficient.
var marketPlayerIDPattern = regexp.MustCompile(`^(\d+m\w+|f\d+|m\d+|\d+)$`)
var marketEventIDPattern = regexp.MustCompile(`^[0-9]{1,18}$`)

func usableMatchProps(props []NormalizedProp) []NormalizedProp {
	result := make([]NormalizedProp, 0, len(props))
	for _, prop := range props {
		switch prop.MarketType {
		case MarketShotsTotal, MarketShotsOnTarget, MarketShotsBlocked, MarketShotsOffTarget,
			MarketShotsOutsideBox, MarketShotsInsideBox, MarketShotsOnTargetOutsideBox,
			MarketShotsOnTargetInsideBox, MarketTackles, MarketPasses, MarketAccuratePasses, MarketDispossessed,
			MarketFoulsCommitted, MarketFoulsDrawn, MarketInterceptions, MarketOffsides:
		default:
			continue
		}
		if strings.TrimSpace(prop.PlayerName) == "" || marketPlayerIDPattern.MatchString(strings.TrimSpace(prop.PlayerName)) ||
			prop.Odds <= 1 || math.IsNaN(prop.Odds) || math.IsInf(prop.Odds, 0) ||
			prop.Line < 0 || math.IsNaN(prop.Line) || math.IsInf(prop.Line, 0) {
			continue
		}
		result = append(result, prop)
	}
	return result
}

const matchOddsTTL = 15 * time.Minute
const bookmakerParserVersion = 5

type matchOddsSnapshot struct {
	ParserVersion  int              `json:"parserVersion"`
	Audit          *offerAudit      `json:"audit,omitempty"`
	Bookmaker      string           `json:"bookmaker"`
	EventID        string           `json:"eventId"`
	Match          string           `json:"match,omitempty"`
	ObservedAt     time.Time        `json:"observedAt"`
	ExpiresAt      time.Time        `json:"expiresAt"`
	Props          []NormalizedProp `json:"props"`
	FromCache      bool             `json:"-"`
	ArchiveVersion string           `json:"archiveVersion,omitempty"`
}

var oddsArchiveSlots = prioritySlots{capacity: 1}

// Store each parsed offer once per bookmaker/event, even across restarts and
// A -> B -> A changes. Observation timestamps stay in a separate small journal.
func archiveOddsObservation(ctx context.Context, snapshot *matchOddsSnapshot, buffer *oddsArchiveBuffer) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: archiwum kursów: %w", errArchive, err)
			log.Printf("ARCHIVE_WRITE_ERROR: %v", err)
		}
	}()
	if archiveRoot == "" {
		return fmt.Errorf("archiwum nie zostało skonfigurowane")
	}
	release, slotErr := oddsArchiveSlots.acquire(ctx)
	if slotErr != nil {
		return slotErr
	}
	defer release()
	// Labels and transport timestamps do not change a bet. IDs, players, markets,
	// sides, lines and odds do; sorting makes source ordering irrelevant.
	canonical := make([]string, 0, len(snapshot.Props))
	for _, prop := range snapshot.Props {
		prop.MarketLabel = ""
		encoded, encodeErr := json.Marshal(prop)
		if encodeErr != nil {
			return encodeErr
		}
		canonical = append(canonical, string(encoded))
	}
	sort.Strings(canonical)
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	directory := filepath.Join(archiveRoot, "oferty-kursow", strings.ToLower(snapshot.Bookmaker), "mecz-"+archiveName(snapshot.EventID), fmt.Sprintf("wersja-%x", digest))
	if err := os.MkdirAll(directory, 0750); err != nil {
		return err
	}
	files, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	var versionPath string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), "_kursy.json.gz") {
			versionPath = filepath.Join(directory, file.Name())
			break
		}
	}
	newVersion := versionPath == ""
	if newVersion {
		if buffer != nil {
			if err := buffer.flush(ctx, filepath.Join(directory, "surowe-zrodlo")); err != nil {
				return err
			}
		}
		versionPath = filepath.Join(directory, strings.ToLower(snapshot.Bookmaker)+"_"+archiveName(snapshot.Match)+"_mecz-"+archiveName(snapshot.EventID)+"_"+snapshot.ObservedAt.Format("20060102T150405Z")+"_kursy.json.gz")
		data, err := json.MarshalIndent(map[string]any{"schemaVersion": 3, "parserVersion": snapshot.ParserVersion, "audit": snapshot.Audit, "bookmaker": snapshot.Bookmaker, "eventId": snapshot.EventID, "match": snapshot.Match, "firstObservedAt": snapshot.ObservedAt, "props": snapshot.Props}, "", "  ")
		if err != nil {
			return err
		}
		if err := writeGzip(versionPath, append(data, '\n')); err != nil {
			return err
		}
	}
	relative, err := filepath.Rel(archiveRoot, versionPath)
	if err != nil {
		return err
	}
	snapshot.ArchiveVersion = filepath.ToSlash(relative)
	now := time.Now().UTC()
	source := "bukmacher"
	if snapshot.FromCache {
		source = "cache"
	}
	observation, err := json.Marshal(map[string]any{"recordedAt": now, "observedAt": snapshot.ObservedAt, "expiresAt": snapshot.ExpiresAt, "bookmaker": snapshot.Bookmaker, "eventId": snapshot.EventID, "match": snapshot.Match, "source": source, "newVersion": newVersion, "archiveVersion": snapshot.ArchiveVersion, "analysisId": archiveInfoFrom(ctx).AnalysisID})
	if err != nil {
		return err
	}
	journalDir := filepath.Join(archiveRoot, "obserwacje-kursow", now.Format("2006-01-02"))
	if err := os.MkdirAll(journalDir, 0750); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(journalDir, strings.ToLower(snapshot.Bookmaker)+"_sprawdzenia-i-uzycie-kursow.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(observation, '\n'))
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return err
	}
	log.Printf("[ODDS-ARCHIVE] OBSERVATION bookmaker=%s event_id=%s source=%s new_version=%t version=%q", snapshot.Bookmaker, snapshot.EventID, source, newVersion, snapshot.ArchiveVersion)
	return nil
}

func (snapshot matchOddsSnapshot) fresh(now time.Time) bool {
	return snapshot.ParserVersion == bookmakerParserVersion && !snapshot.ObservedAt.IsZero() && !snapshot.ObservedAt.After(now) && now.Before(snapshot.ExpiresAt) &&
		!snapshot.ExpiresAt.After(snapshot.ObservedAt.Add(matchOddsTTL))
}

func (snapshot matchOddsSnapshot) retain(now time.Time) bool {
	return snapshot.fresh(now)
}

func (snapshot matchOddsSnapshot) copy(reused bool) matchOddsSnapshot {
	snapshot.Props = append([]NormalizedProp{}, snapshot.Props...)
	snapshot.FromCache = reused
	return snapshot
}

type matchOddsFlight struct {
	done     chan struct{}
	snapshot matchOddsSnapshot
	err      error
	cancel   context.CancelFunc
	priority int
}

type oddsNotBeforeContextKey struct{}

// All consumers share full parsed offers and their original observation time.
// A hit never extends freshness; failed requests never become cached offers.
func (s *calendarServer) matchOdds(ctx context.Context, book, id string) (matchOddsSnapshot, error) {
	if s.womenOffer(book, id) {
		return matchOddsSnapshot{}, fmt.Errorf("oferty meczów kobiet są wyłączone")
	}
	// All callers, including quote-only monitoring, must supply the source to
	// transport archives. Preserve analysis identity and automatic provenance.
	info := archiveInfoFrom(ctx)
	info.Bookmaker = book
	if info.Scope == "" {
		info.Scope = "kursy-mecz-" + id
	}
	if info.Match == "" {
		if candidate, ok := s.calendarOffers().ByID[book+":"+id]; ok {
			info.Match = candidate.Event.Home + " vs " + candidate.Event.Away
		}
	}
	ctx = context.WithValue(ctx, archiveContextKey{}, info)
	notBefore, _ := ctx.Value(oddsNotBeforeContextKey{}).(time.Time)
	key := book + ":" + id
	for {
		if err := ctx.Err(); err != nil {
			return matchOddsSnapshot{}, err
		}
		s.oddsMu.Lock()
		if snapshot, ok := s.oddsCache[key]; ok && snapshot.fresh(time.Now()) && !snapshot.ObservedAt.Before(notBefore) {
			s.oddsMu.Unlock()
			log.Printf("[ODDS-CACHE] HIT bookmaker=%s event_id=%s age=%s observed_at=%s", book, id, time.Since(snapshot.ObservedAt).Round(time.Second), snapshot.ObservedAt.Format(time.RFC3339))
			snapshot = snapshot.copy(true)
			err := archiveOddsObservation(ctx, &snapshot, nil)
			return snapshot, err
		}
		if flight := s.oddsFlights[key]; flight != nil {
			if sofaRequestPriority(ctx) < flight.priority && flight.cancel != nil {
				log.Printf("[ODDS-CACHE] PROMOTE bookmaker=%s event_id=%s priority=%d", book, id, sofaRequestPriority(ctx))
				flight.cancel()
			}
			s.oddsMu.Unlock()
			log.Printf("[ODDS-CACHE] JOIN bookmaker=%s event_id=%s", book, id)
			select {
			case <-ctx.Done():
				return matchOddsSnapshot{}, ctx.Err()
			case <-flight.done:
				if ctx.Err() == nil && (errors.Is(flight.err, context.Canceled) || errors.Is(flight.err, context.DeadlineExceeded)) {
					continue
				}
				if flight.err == nil && (!flight.snapshot.fresh(time.Now()) || flight.snapshot.ObservedAt.Before(notBefore)) {
					continue
				}
				snapshot := flight.snapshot.copy(true)
				if flight.err != nil {
					return snapshot, flight.err
				}
				err := archiveOddsObservation(ctx, &snapshot, nil)
				return snapshot, err
			}
		}
		requestCtx, cancel := context.WithCancel(ctx)
		flight := &matchOddsFlight{done: make(chan struct{}), cancel: cancel, priority: sofaRequestPriority(ctx)}
		s.oddsFlights[key] = flight
		s.oddsMu.Unlock()
		log.Printf("[ODDS-CACHE] FETCH bookmaker=%s event_id=%s", book, id)
		snapshot := matchOddsSnapshot{ParserVersion: bookmakerParserVersion, Audit: &offerAudit{}, Bookmaker: book, EventID: id, Match: archiveInfoFrom(ctx).Match}
		buffer := &oddsArchiveBuffer{}
		fetchCtx := context.WithValue(requestCtx, oddsArchiveBufferKey{}, buffer)
		fetchCtx = context.WithValue(fetchCtx, offerAuditContextKey{}, snapshot.Audit)
		var err error
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					err = fmt.Errorf("pobieranie kursów: %v", recovered)
				}
			}()
			release, slotErr := s.oddsSlots.acquire(fetchCtx)
			if slotErr != nil {
				err = slotErr
				return
			}
			defer release()
			snapshot.ObservedAt = time.Now().UTC()
			snapshot.Props, err = fetchMatchProps(fetchCtx, book, id)
			if err == nil {
				err = fetchCtx.Err()
			}
			snapshot.ExpiresAt = snapshot.ObservedAt.Add(matchOddsTTL)
			if err == nil {
				err = archiveOddsObservation(fetchCtx, &snapshot, buffer)
			}
		}()
		if err != nil && !errors.Is(err, context.Canceled) {
			// Failed or incomplete responses remain available for diagnosis.
			err = errors.Join(err, buffer.flush(ctx, ""))
		}
		s.oddsMu.Lock()
		if err == nil {
			s.oddsCache[key] = snapshot
			s.markMarketCacheDirty(true)
			log.Printf("[ODDS-CACHE] STORED bookmaker=%s event_id=%s props=%d expires_at=%s", book, id, len(snapshot.Props), snapshot.ExpiresAt.Format(time.RFC3339))
		}
		flight.snapshot, flight.err = snapshot, err
		delete(s.oddsFlights, key)
		close(flight.done)
		s.oddsMu.Unlock()
		cancel()
		return snapshot.copy(false), err
	}
}

func marketAvailabilityFromOdds(book, id string, snapshot matchOddsSnapshot, err error) MatchMarketAvailability {
	result := marketAvailabilityResult(book, id, snapshot.Props, err)
	if err == nil && !snapshot.ObservedAt.IsZero() {
		result.CheckedAt, result.ExpiresAt = snapshot.ObservedAt, snapshot.ExpiresAt
	}
	return result
}

type MatchMarketAvailability struct {
	Bookmaker string            `json:"bookmaker"`
	EventID   string            `json:"eventId"`
	Status    string            `json:"status"`
	Markets   []CanonicalMarket `json:"markets"`
	CheckedAt time.Time         `json:"checkedAt"`
	ExpiresAt time.Time         `json:"expiresAt"`
	Error     string            `json:"error,omitempty"`
	Home      string            `json:"home,omitempty"`
	Away      string            `json:"away,omitempty"`
	StartTime time.Time         `json:"startTime,omitempty"`
}

// Calendar discovery is independent of the 15-minute price cache. Remember a
// confirmed market set until kickoff; analysis always validates current prices.
func normalizeMarketAvailability(state MatchMarketAvailability, event CalendarEvent) MatchMarketAvailability {
	if start, err := time.Parse(time.RFC3339, event.StartTime); err == nil {
		state.Home, state.Away, state.StartTime = event.Home, event.Away, start
	}
	state.ExpiresAt = state.CheckedAt.Add(matchOddsTTL)
	switch state.Status {
	case "available":
		if len(state.Markets) > 0 {
			state.ExpiresAt = state.StartTime
			if state.ExpiresAt.IsZero() {
				state.ExpiresAt = state.CheckedAt.Add(24 * time.Hour)
			}
		}
	case "error":
		state.ExpiresAt = state.CheckedAt.Add(time.Minute)
	}
	if !state.StartTime.IsZero() && state.StartTime.Before(state.ExpiresAt) {
		state.ExpiresAt = state.StartTime
	}
	return state
}

func marketAvailabilityMatches(state MatchMarketAvailability, event CalendarEvent) bool {
	if state.StartTime.IsZero() { // legacy entry, bound to the current fixture on load
		return true
	}
	start, err := time.Parse(time.RFC3339, event.StartTime)
	if err != nil || !state.StartTime.Equal(start) {
		return false
	}
	return (state.Home == event.Home || sameCalendarTeam(state.Home, event.Home)) &&
		(state.Away == event.Away || sameCalendarTeam(state.Away, event.Away))
}

func marketAvailabilityValid(state MatchMarketAvailability, event CalendarEvent, now time.Time) bool {
	return !state.CheckedAt.IsZero() && !state.CheckedAt.After(now) && now.Before(state.ExpiresAt) &&
		marketAvailabilityMatches(state, event)
}

type marketAvailabilityFlight struct {
	done   chan struct{}
	result MatchMarketAvailability
}

func marketAvailabilityResult(book, id string, props []NormalizedProp, err error) MatchMarketAvailability {
	now := time.Now().UTC()
	result := MatchMarketAvailability{Bookmaker: book, EventID: id, Status: "unavailable", Markets: []CanonicalMarket{}, CheckedAt: now, ExpiresAt: now.Add(matchOddsTTL)}
	if err != nil {
		result.Status, result.Error = "error", err.Error()
		result.ExpiresAt = now.Add(time.Minute)
		return result
	}
	seen := map[CanonicalMarket]bool{}
	for _, prop := range usableMatchProps(props) {
		if !seen[prop.MarketType] {
			seen[prop.MarketType] = true
			result.Markets = append(result.Markets, prop.MarketType)
		}
	}
	sort.Slice(result.Markets, func(i, j int) bool { return result.Markets[i] < result.Markets[j] })
	if len(result.Markets) > 0 {
		result.Status = "available"
	}
	return result
}

func (s *calendarServer) rememberMarketAvailability(result MatchMarketAvailability) MatchMarketAvailability {
	key := result.Bookmaker + ":" + result.EventID
	candidate := s.calendarOffers().ByID[key]
	result = normalizeMarketAvailability(result, candidate.Event)
	s.marketMu.Lock()
	if previous, ok := s.marketCache[key]; ok && result.Status == "error" && previous.Status == "available" &&
		len(previous.Markets) > 0 && marketAvailabilityValid(previous, candidate.Event, time.Now()) {
		s.marketMu.Unlock()
		log.Printf("[MARKET-CACHE] KEEP_KNOWN bookmaker=%s event_id=%s refresh_error=%q", result.Bookmaker, result.EventID, result.Error)
		return result
	}
	s.marketCache[key] = result
	s.marketMu.Unlock()
	s.markMarketCacheDirty(false)
	return result
}

type marketFreshOddsContextKey struct{}

func (s *calendarServer) checkMatchMarkets(ctx context.Context, book, id, home, away string) MatchMarketAvailability {
	if isWomenEvent(CalendarEvent{Home: home, Away: away}) || s.womenOffer(book, id) {
		return marketAvailabilityResult(book, id, nil, fmt.Errorf("oferty meczów kobiet są wyłączone"))
	}
	key := book + ":" + id
	candidate := s.calendarOffers().ByID[key]
	requireFreshOdds, _ := ctx.Value(marketFreshOddsContextKey{}).(bool)
	s.marketMu.Lock()
	if cached, ok := s.marketCache[key]; ok && !requireFreshOdds && marketAvailabilityValid(cached, candidate.Event, time.Now()) {
		s.marketMu.Unlock()
		log.Printf("[MARKET-CACHE] HIT bookmaker=%s event_id=%s status=%s expires_at=%s", book, id, cached.Status, cached.ExpiresAt.Format(time.RFC3339))
		return cached
	}
	s.marketMu.Unlock()
	scope := "dostepnosc-rynkow-" + home + "-vs-" + away + "-mecz-" + id
	ctx = context.WithValue(ctx, archiveContextKey{}, archiveInfo{Bookmaker: book, Match: home + " vs " + away, Scope: scope, Automatic: archiveInfoFrom(ctx).Automatic})
	odds, err := s.matchOdds(ctx, book, id)
	result := marketAvailabilityFromOdds(book, id, odds, err)
	// A promoted/canceled scan must not overwrite the foreground result with a
	// one-minute error entry. The shared odds flight owns coalescing and priority.
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return result
	}
	// A deadline is a completed failed attempt. Remember it so that the calendar
	// shows the error and retry time instead of staying pending indefinitely.
	if ctx.Err() == nil {
		if saveErr := archiveResponse(ctx, "sprawdzone-obslugiwane-rynki-zawodnikow", nil, "", result, err); saveErr != nil {
			result = marketAvailabilityResult(book, id, nil, errors.Join(err, saveErr))
		}
	}
	result = s.rememberMarketAvailability(result)
	return result
}

func (s *calendarServer) registerMarketAvailabilityAPI(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/markets/prepare", func(w http.ResponseWriter, r *http.Request) {
		var query AnalysisRequest
		if err := decodeAPIRequest(w, r, &query); err != nil {
			apiError(w, 400, err)
			return
		}
		if err := validateAnalysisRequest(&query); err != nil {
			apiError(w, 400, err)
			return
		}
		matches, err := s.analysisEvents(query)
		if err != nil {
			apiError(w, 400, fmt.Errorf("kwalifikacja meczu: %w", err))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
		defer cancel()
		stop := context.AfterFunc(s.ctx, cancel)
		defer stop()
		ctx = context.WithValue(ctx, sofaManualContextKey{}, true)
		ctx = context.WithValue(ctx, marketFreshOddsContextKey{}, true)
		results := make(chan MatchMarketAvailability, len(serverBookmakers))
		for _, book := range serverBookmakers {
			go func(book string, event CalendarEvent) {
				results <- s.checkMatchMarkets(ctx, book, event.ID, event.Home, event.Away)
			}(book, matches[book])
		}
		prepared := MatchPreparation{Qualified: true, Sources: map[string]MatchMarketAvailability{}}
		for range serverBookmakers {
			result := <-results
			prepared.Sources[result.Bookmaker] = result
			prepared.Qualified = prepared.Qualified && result.Status == "available" && len(result.Markets) > 0
		}
		jsonResponse(w, 200, prepared)
	})
	mux.HandleFunc("POST /api/v1/markets/check", func(w http.ResponseWriter, r *http.Request) {
		var query struct {
			Bookmaker string `json:"bookmaker"`
			EventID   string `json:"eventId"`
			Home      string `json:"home"`
			Away      string `json:"away"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&query); err != nil {
			apiError(w, 400, err)
			return
		}
		query.Bookmaker = strings.ToUpper(strings.TrimSpace(query.Bookmaker))
		if (query.Bookmaker != "STS" && query.Bookmaker != "BETCLIC" && query.Bookmaker != "SUPERBET") ||
			!marketEventIDPattern.MatchString(query.EventID) || strings.Trim(query.EventID, "0") == "" ||
			len(query.Home) > 200 || len(query.Away) > 200 {
			apiError(w, 400, fmt.Errorf("niepoprawny bukmacher lub wydarzenie"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		stop := context.AfterFunc(s.ctx, cancel)
		defer stop()
		result := s.checkMatchMarkets(ctx, query.Bookmaker, query.EventID, query.Home, query.Away)
		jsonResponse(w, 200, result)
	})
}

type MatchPreparation struct {
	Qualified bool                               `json:"qualified"`
	Sources   map[string]MatchMarketAvailability `json:"sources"`
}

func fetchMatchProps(ctx context.Context, book, id string) ([]NormalizedProp, error) {
	props, err := fetchRawMatchProps(ctx, book, id)
	return usableMatchProps(props), err
}

func fetchRawMatchProps(ctx context.Context, book, id string) ([]NormalizedProp, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	audit, _ := ctx.Value(offerAuditContextKey{}).(*offerAudit)
	switch book {
	case "SUPERBET":
		endpoint := "https://production-superbet-bmb.freetls.fastly.net/betbuilder/v3/getBetbuilderMarketsForMatch?match_id=" + url.QueryEscape(id) + "&lang=pl-PL&target=SB_PL"
		var decoded json.RawMessage
		if err := getSuperbetCalendarJSON(ctx, client, endpoint, &decoded); err != nil {
			return nil, err
		}
		var identity struct {
			MatchID   string          `json:"matchId"`
			FullState *bool           `json:"isFullState"`
			Markets   json.RawMessage `json:"markets"`
		}
		if err := json.Unmarshal(decoded, &identity); err != nil {
			return nil, err
		}
		if identity.MatchID != id || identity.FullState == nil || !*identity.FullState || len(identity.Markets) == 0 || string(identity.Markets) == "null" {
			return nil, fmt.Errorf("Superbet v3: brak pełnego snapshotu dla meczu %s (matchId=%q)", id, identity.MatchID)
		}
		return ParseSuperbetProps(string(decoded), audit)
	case "STS":
		raw, err := FetchSTSSnapshot(ctx, STSWebSocket, id)
		if err != nil {
			return nil, err
		}
		return ParseSTSProps(string(raw), audit)
	case "BETCLIC":
		matchID, err := strconv.ParseUint(id, 10, 64)
		if err != nil || matchID == 0 {
			return nil, fmt.Errorf("niepoprawne ID Betclic")
		}
		body := ProtoUint(1, matchID)
		body = append(body, ProtoString(2, "pl")...)
		body = append(body, ProtoString(3, "ca_ftb_prp")...)
		raw, err := FetchBetclicRPC(ctx, client, BetclicMatchService+"GetMatchWithNotification", body)
		if err != nil {
			return nil, err
		}
		return ParseBetclicProps(raw, audit), nil
	}
	return nil, fmt.Errorf("nieobsługiwane źródło")
}

type sofaClientState struct {
	mu                  sync.Mutex
	archiveErr          error
	historyNotes        map[string]bool
	lineupRoles         map[int]string
	teamRefresh         map[string]sofaTeamRefreshState
	statsUnavailable    int
	statsMissingPending int
}

func (client *ResilientClient) firstHistoryNote(key string) bool {
	if client.state == nil {
		return true
	}
	client.state.mu.Lock()
	defer client.state.mu.Unlock()
	if client.state.historyNotes == nil {
		client.state.historyNotes = make(map[string]bool)
	}
	if client.state.historyNotes[key] {
		return false
	}
	client.state.historyNotes[key] = true
	return true
}

type ResilientClient struct {
	statsTeamID   string
	statsFixture  statsFixture
	statsPrefetch bool
	background    bool
	tlsClient     tls_client.HttpClient
	ctx           context.Context
	state         *sofaClientState
	warning       func(string)
	progress      func(string, string, int, int)
}

func newSofaClient(ctx context.Context, warning func(string), progress func(string, string, int, int)) (*ResilientClient, error) {
	info := archiveInfoFrom(ctx)
	info.Bookmaker = "SOFASCORE"
	if info.Scope == "" {
		info.Scope = "statystyki-i-status-meczu"
	}
	ctx = context.WithValue(ctx, archiveContextKey{}, info)
	cli, err := newSofaHTTPClient()
	if err != nil {
		return nil, err
	}
	background, _ := ctx.Value(sofaBackgroundContextKey{}).(bool)
	return &ResilientClient{tlsClient: cli, ctx: ctx, background: background, state: &sofaClientState{}, warning: warning, progress: progress}, nil
}

func newSofaHTTPClient(extra ...tls_client.HttpClientOption) (tls_client.HttpClient, error) {
	// The installed library provides Chrome 152, not an exact Chrome 154 profile.
	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(8),
		tls_client.WithClientProfile(profiles.Chrome_152),
		tls_client.WithCookieJar(tls_client.NewCookieJar()),
		tls_client.WithRandomTLSExtensionOrder(),
	}
	return tls_client.NewHttpClient(tls_client.NewNoopLogger(), append(options, extra...)...)
}

// This manual diagnostic uses the production headers and TLS configuration.
// It makes four requests, does not start collectors and does not alter caches.
func runSofaDiagnostics(ctx context.Context, dataDir string) error {
	serverDataDir = dataDir
	archiveRoot = filepath.Join(dataDir, "archive")
	if sofaBrowserBase != "" {
		return runSofaBrowserDiagnostic(ctx, dataDir)
	}
	dir := filepath.Join(dataDir, "diagnostics")
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	const endpoint = "/search/all?q=chicago+fire&page=0"
	variants := []struct {
		name    string
		options []tls_client.HttpClientOption
	}{
		{"domyslne", nil},
		{"ipv4", []tls_client.HttpClientOption{tls_client.WithDisableIPV6()}},
		{"ipv6", []tls_client.HttpClientOption{tls_client.WithDisableIPV4()}},
		{"ipv4-http1", []tls_client.HttpClientOption{tls_client.WithDisableIPV6(), tls_client.WithForceHttp1()}},
	}
	type probeResult struct {
		Variant      string         `json:"variant"`
		HTTPStatus   int            `json:"httpStatus"`
		Protocol     string         `json:"protocol"`
		Server       string         `json:"server,omitempty"`
		ContentType  string         `json:"contentType,omitempty"`
		ResponseSize int            `json:"responseBytes"`
		Duration     string         `json:"duration"`
		Teams        []SofaTeamItem `json:"teams"`
		Error        string         `json:"error,omitempty"`
		BodyPreview  string         `json:"bodyPreview,omitempty"`
	}
	report := struct {
		RecordedAt         time.Time     `json:"recordedAt"`
		Platform           string        `json:"platform"`
		GoVersion          string        `json:"goVersion"`
		TLSProfile         string        `json:"tlsProfile"`
		Endpoint           string        `json:"endpoint"`
		SuccessfulVariants []string      `json:"successfulVariants"`
		Results            []probeResult `json:"results"`
	}{time.Now().UTC(), runtime.GOOS + "/" + runtime.GOARCH, runtime.Version(), "Chrome_152", sofaBase + endpoint, []string{}, []probeResult{}}
	log.Printf("[SOFA-DIAG] START platform=%s go=%s endpoint=%q", report.Platform, report.GoVersion, report.Endpoint)
	for i, variant := range variants {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if i > 0 {
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		probeCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
		started := time.Now()
		result := probeResult{Variant: variant.name, Teams: []SofaTeamItem{}}
		var body []byte
		cli, fetchErr := newSofaHTTPClient(variant.options...)
		if fetchErr == nil {
			var req *fhttp.Request
			req, fetchErr = fhttp.NewRequestWithContext(probeCtx, fhttp.MethodGet, report.Endpoint, nil)
			if fetchErr == nil {
				fetchErr = applySofaRequestHeaders(cli, req)
			}
			if fetchErr == nil {
				var resp *fhttp.Response
				resp, fetchErr = cli.Do(req)
				if resp != nil {
					result.HTTPStatus, result.Protocol = resp.StatusCode, resp.Proto
					result.Server, result.ContentType = resp.Header.Get("Server"), resp.Header.Get("Content-Type")
					var readErr error
					body, readErr = readBounded(resp.Body, 32<<20)
					_ = resp.Body.Close()
					fetchErr = errors.Join(fetchErr, readErr)
					if resp.StatusCode != fhttp.StatusOK {
						fetchErr = errors.Join(fetchErr, fmt.Errorf("SofaScore HTTP %d", resp.StatusCode))
					}
				}
			}
			cli.CloseIdleConnections()
		}
		cancel()
		if fetchErr == nil {
			var search SofaSearchResponse
			if err := json.Unmarshal(body, &search); err != nil {
				fetchErr = fmt.Errorf("niepoprawny JSON wyszukiwania: %w", err)
			} else {
				result.Teams = search.ExtractTeams()
				if len(result.Teams) == 0 {
					fetchErr = fmt.Errorf("wyszukiwanie Chicago Fire nie zwróciło drużyn")
				}
			}
		}
		result.ResponseSize, result.Duration = len(body), time.Since(started).String()
		if fetchErr != nil {
			result.Error = fetchErr.Error()
			result.BodyPreview = string(body[:min(len(body), 512)])
		} else {
			report.SuccessfulVariants = append(report.SuccessfulVariants, variant.name)
		}
		info := archiveInfo{Bookmaker: "SOFASCORE", Scope: "diagnostyka-wyszukiwanie-chicago-fire-" + variant.name, Endpoint: report.Endpoint, Method: "GET", Status: result.HTTPStatus, ContentType: result.ContentType}
		if err := archiveResponse(context.WithValue(ctx, archiveContextKey{}, info), "odpowiedz-http", body, "http.bin", readableJSONValues(body), fetchErr); err != nil {
			return err
		}
		report.Results = append(report.Results, result)
		log.Printf("[SOFA-DIAG] variant=%s status=%d protocol=%q teams=%d duration=%s error=%q", result.Variant, result.HTTPStatus, result.Protocol, len(result.Teams), result.Duration, result.Error)
	}
	path := filepath.Join(dir, "sofascore_wyszukiwanie-chicago-fire_porownanie-ipv4-ipv6-http_"+report.RecordedAt.Format("20060102T150405.000000000Z")+".json")
	if err := atomicJSON(path, report); err != nil {
		return err
	}
	log.Printf("[SOFA-DIAG] END successful_variants=%q report=%q", report.SuccessfulVariants, path)
	if len(report.SuccessfulVariants) == 0 {
		return fmt.Errorf("żaden wariant nie zwrócił drużyn SofaScore; raport: %s", path)
	}
	return nil
}

func (client *ResilientClient) withScope(scope string) *ResilientClient {
	cp := *client
	info := archiveInfoFrom(client.ctx)
	info.Scope = scope
	cp.ctx = context.WithValue(client.ctx, archiveContextKey{}, info)
	return &cp
}
func (client *ResilientClient) archiveError() error {
	client.state.mu.Lock()
	defer client.state.mu.Unlock()
	return client.state.archiveErr
}
func (client *ResilientClient) recordArchiveError(err error) {
	if err == nil {
		return
	}
	client.state.mu.Lock()
	client.state.archiveErr = errors.Join(client.state.archiveErr, err)
	client.state.mu.Unlock()
}
func (client *ResilientClient) warn(message string) {
	if client.warning != nil {
		client.warning(message)
	}
}
func (client *ResilientClient) report(done, total int) {
	if client.progress != nil {
		client.progress("", "", done, total)
	}
}

func (client *ResilientClient) GetSofa(endpoint string) ([]byte, error) {
	var body []byte
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		if client.ctx.Err() != nil {
			return nil, client.ctx.Err()
		}
		if err = client.archiveError(); err != nil {
			return nil, err
		}
		body, err = client.getSofaAttempt(endpoint)
		if err == nil {
			return body, nil
		}
		if isArchiveFailure(err) || client.ctx.Err() != nil {
			break
		}
		lower := strings.ToLower(err.Error())
		if attempt != 0 || (!strings.Contains(lower, "goaway") && !strings.Contains(lower, "connection reset")) {
			break
		}
		client.tlsClient.CloseIdleConnections()
		timer := time.NewTimer(time.Second)
		select {
		case <-client.ctx.Done():
			timer.Stop()
			return nil, client.ctx.Err()
		case <-timer.C:
		}
	}
	client.warn("SofaScore " + endpoint + ": " + err.Error())
	return nil, err
}

func (client *ResilientClient) getSofaAttempt(endpoint string) ([]byte, error) {
	attemptStarted := time.Now()
	if client.background {
		release, err := waitBackgroundSofa(client.ctx)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	release, err := globalSofaThrottle.acquire(client.ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	timer := time.NewTimer(time.Duration(40+rand.IntN(30)) * time.Millisecond)
	select {
	case <-client.ctx.Done():
		timer.Stop()
		return nil, client.ctx.Err()
	case <-timer.C:
	}
	// Recheck after waiting for slots: another request may have received a block.
	if err := client.ctx.Err(); err != nil {
		return nil, err
	}
	if err := sofaCooldownError(); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(endpoint, "/") || strings.HasPrefix(endpoint, "//") {
		return nil, fmt.Errorf("niepoprawny endpoint SofaScore")
	}
	reqURL := sofaBase + endpoint
	info := archiveInfoFrom(client.ctx)
	info.Endpoint, info.Method = reqURL, "GET"
	// Do not inherit HTTP metadata from an earlier archive scope on a failed fetch.
	info.Status, info.ContentType = 0, ""
	started := time.Now()
	var response sofaFetchResult
	var fetchErr error
	if sofaBrowserBase != "" {
		response, fetchErr = fetchSofaBrowser(client.ctx, reqURL)
	} else {
		response, fetchErr = client.fetchSofaHTTP(reqURL, endpoint)
	}
	// Include the global/background slots and pacing as well as the browser
	// queue. The total describes this attempt, before archive writes.
	response.QueueDuration += started.Sub(attemptStarted)
	info.Status, info.ContentType = response.Status, response.ContentType
	if response.Status != 0 {
		noteSofaBlocked(response.Status, response.RetryAfter)
		if response.Status != http.StatusOK {
			fetchErr = errors.Join(fetchErr, sofaHTTPStatusError(response.Status))
		}
	}
	body := response.Body
	log.Printf("[SOFA-HTTP] endpoint=%q status=%d protocol=%q content_type=%q bytes=%d queue_wait=%s setup=%s fetch=%s duration=%s error=%v", endpoint, info.Status, response.Protocol, info.ContentType, len(body), response.QueueDuration.Round(time.Millisecond), response.SetupDuration.Round(time.Millisecond), response.FetchDuration.Round(time.Millisecond), time.Since(attemptStarted).Round(time.Millisecond), fetchErr)
	if fetchErr != nil && len(body) > 0 {
		log.Printf("[SOFA-HTTP] ERROR_BODY endpoint=%q preview=%q", endpoint, body[:min(len(body), 512)])
	}
	ctx := context.WithValue(client.ctx, archiveContextKey{}, info)
	if err := archiveResponse(ctx, "odpowiedz-"+calendarTeamSlug(endpoint), body, "http.bin", readableJSONValues(body), fetchErr); err != nil {
		client.recordArchiveError(err)
		return nil, errors.Join(fetchErr, err)
	}
	return body, fetchErr
}

type sofaHTTPStatusError int

func (status sofaHTTPStatusError) Error() string {
	return fmt.Sprintf("SofaScore HTTP %d", int(status))
}

type sofaFetchResult struct {
	Status        int           `json:"status"`
	ContentType   string        `json:"contentType"`
	RetryAfter    string        `json:"retryAfter,omitempty"`
	Protocol      string        `json:"protocol"`
	Body          []byte        `json:"-"`
	QueueDuration time.Duration `json:"-"`
	SetupDuration time.Duration `json:"-"`
	FetchDuration time.Duration `json:"-"`
}

func (client *ResilientClient) fetchSofaHTTP(reqURL, endpoint string) (sofaFetchResult, error) {
	var result sofaFetchResult
	req, err := fhttp.NewRequestWithContext(client.ctx, fhttp.MethodGet, reqURL, nil)
	if err != nil {
		return result, err
	}
	if err := applySofaRequestHeaders(client.tlsClient, req); err != nil {
		return result, err
	}
	if strings.HasPrefix(endpoint, "/search/") {
		// Header.Get canonicalizes keys; these deliberately lowercase keys must be read directly.
		log.Printf("[SOFA-HTTP] REQUEST endpoint=%q tls_profile=Chrome_152 user_agent=%q requested_with=%q referer=%q captcha_configured=%t header_order=%q", endpoint, strings.Join(req.Header["user-agent"], ""), strings.Join(req.Header["x-requested-with"], ""), strings.Join(req.Header["referer"], ""), len(req.Header["x-captcha"]) > 0, req.Header[fhttp.HeaderOrderKey])
	}
	started := time.Now()
	resp, fetchErr := client.tlsClient.Do(req)
	if resp != nil {
		defer resp.Body.Close()
		result.Status, result.Protocol = resp.StatusCode, resp.Proto
		result.ContentType, result.RetryAfter = resp.Header.Get("Content-Type"), resp.Header.Get("Retry-After")
		var readErr error
		result.Body, readErr = readBounded(resp.Body, 32<<20)
		fetchErr = errors.Join(fetchErr, readErr)
	}
	result.FetchDuration = time.Since(started)
	return result, fetchErr
}

// Browser mode attaches to a manually opened local Chromium. It never launches,
// navigates or closes the user's browser, and never solves a challenge.
var sofaBrowserBase string
var sofaBrowserGap = 100 * time.Millisecond
var sofaBrowserSlot = prioritySlots{capacity: 1}
var sofaBrowserLastFinished time.Time // protected by sofaBrowserSlot
var sofaBrowserLastTarget string      // protected by sofaBrowserSlot

func sofaBrowserGapSetting() string {
	if value := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_BROWSER_GAP_MS")); value != "" {
		return value
	}
	return "100"
}

func configureSofaBrowserGap(raw string) error {
	ms, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || ms < 0 || ms > 10000 {
		return fmt.Errorf("Chromium SofaScore: -sofa-browser-gap-ms musi być liczbą 0–10000")
	}
	sofaBrowserGap = time.Duration(ms) * time.Millisecond
	return nil
}

func configureSofaBrowser(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.Port() == "" {
		return fmt.Errorf("Chromium SofaScore: -sofa-browser wymaga lokalnego adresu, np. http://127.0.0.1:9222")
	}
	ip := net.ParseIP(u.Hostname())
	port, err := strconv.Atoi(u.Port())
	if ip == nil || !ip.IsLoopback() || err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("Chromium SofaScore: DevTools musi nasłuchiwać na adresie loopback i poprawnym porcie")
	}
	sofaBrowserBase = "http://" + u.Host
	log.Printf("[SOFA-BROWSER] CONFIGURED devtools=%q transport=chromium-cdp gap=%s-%s", sofaBrowserBase, sofaBrowserGap, 2*sofaBrowserGap)
	return nil
}

func isSofaBrowserPage(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() == "www.sofascore.com" && (u.Port() == "" || u.Port() == "443") && u.User == nil
}

type sofaCDPTarget struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	URL       string `json:"url"`
	WebSocket string `json:"webSocketDebuggerUrl"`
}

func findSofaBrowserTarget(ctx context.Context) (sofaCDPTarget, error) {
	var empty sofaCDPTarget
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sofaBrowserBase+"/json/list", nil)
	if err != nil {
		return empty, fmt.Errorf("Chromium SofaScore: niepoprawny adres DevTools")
	}
	// Never send the local DevTools request through a system HTTP proxy.
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	cli := &http.Client{Transport: transport, Timeout: 4 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := cli.Do(req)
	if err != nil {
		return empty, fmt.Errorf("Chromium SofaScore: DevTools niedostępne; uruchom Chromium z portem debugowania: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return empty, fmt.Errorf("Chromium SofaScore: lista kart DevTools HTTP %d", resp.StatusCode)
	}
	raw, err := readBounded(resp.Body, 1<<20)
	var targets []sofaCDPTarget
	if err != nil || json.Unmarshal(raw, &targets) != nil {
		return empty, fmt.Errorf("Chromium SofaScore: niepoprawna lista kart DevTools")
	}
	// Prefer a normal website tab over a JSON API viewer. Use only existing tabs.
	sort.SliceStable(targets, func(i, j int) bool {
		return !strings.Contains(targets[i].URL, "/api/v1/") && strings.Contains(targets[j].URL, "/api/v1/")
	})
	base, _ := url.Parse(sofaBrowserBase)
	for _, target := range targets {
		if target.Type != "page" || !isSofaBrowserPage(target.URL) || target.WebSocket == "" {
			continue
		}
		ws, err := url.Parse(target.WebSocket)
		if err != nil || ws.Scheme != "ws" || ws.User != nil || ws.RawQuery != "" || ws.Fragment != "" || ws.Port() != base.Port() || !strings.HasPrefix(ws.Path, "/devtools/page/") {
			continue
		}
		host := net.ParseIP(ws.Hostname())
		if ws.Hostname() != "localhost" && (host == nil || !host.IsLoopback()) {
			continue
		}
		ws.Host = base.Host
		target.WebSocket = ws.String()
		return target, nil
	}
	return empty, fmt.Errorf("Chromium SofaScore: otwórz kartę https://www.sofascore.com/ w przeglądarce z włączonym DevTools; rozwiąż challenge ręcznie")
}

type sofaCDPConnection struct {
	conn   *websocket.Conn
	ctx    context.Context
	nextID int
	write  sync.Mutex
	abort  map[string]any // protected by write
}

func (c *sofaCDPConnection) call(method string, params any, out any) error {
	c.write.Lock()
	if err := c.ctx.Err(); err != nil {
		c.write.Unlock()
		return err
	}
	c.nextID++
	id := c.nextID
	deadline, _ := c.ctx.Deadline()
	_ = c.conn.SetWriteDeadline(deadline)
	err := c.conn.WriteJSON(map[string]any{"id": id, "method": method, "params": params})
	c.write.Unlock()
	if err != nil {
		return fmt.Errorf("Chromium SofaScore: wysłanie %s: %w", method, err)
	}
	for {
		var message struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Code int `json:"code"`
			} `json:"error"`
		}
		if err := c.conn.ReadJSON(&message); err != nil {
			if c.ctx.Err() != nil {
				return c.ctx.Err()
			}
			return fmt.Errorf("Chromium SofaScore: odczyt %s: %w", method, err)
		}
		if message.ID != id {
			continue // unsolicited CDP events or cancellation acknowledgement
		}
		if message.Error != nil {
			// CDP error text/exception details can contain an expression with a cookie.
			return fmt.Errorf("Chromium SofaScore: %s odrzucone (kod %d); sprawdź kartę przeglądarki", method, message.Error.Code)
		}
		if err := json.Unmarshal(message.Result, out); err != nil {
			return fmt.Errorf("Chromium SofaScore: niepoprawny wynik %s", method)
		}
		return nil
	}
}

func fetchSofaBrowser(parent context.Context, reqURL string) (result sofaFetchResult, err error) {
	result.Protocol = "Chromium/CDP"
	u, err := url.Parse(reqURL)
	if err != nil || !isSofaBrowserPage(reqURL) || !strings.HasPrefix(u.Path, "/api/v1/") || u.Fragment != "" {
		return result, fmt.Errorf("Chromium SofaScore: dozwolone są tylko endpointy www.sofascore.com/api/v1/")
	}
	queueStarted := time.Now()
	var setupStarted, fetchStarted time.Time
	defer func() {
		switch {
		case setupStarted.IsZero():
			result.QueueDuration = time.Since(queueStarted)
		case fetchStarted.IsZero():
			result.SetupDuration = time.Since(setupStarted)
		default:
			result.FetchDuration = time.Since(fetchStarted)
		}
	}()
	release, err := sofaBrowserSlot.acquire(parent)
	if err != nil {
		return result, err
	}
	defer func() {
		sofaBrowserLastFinished = time.Now()
		release()
	}()
	// One shared queue and pacing for both foreground and background browser fetches.
	gap := sofaBrowserGap + time.Duration(rand.Int64N(int64(sofaBrowserGap)+1))
	if delay := time.Until(sofaBrowserLastFinished.Add(gap)); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-parent.Done():
			timer.Stop()
			return result, parent.Err()
		case <-timer.C:
		}
	}
	setupStarted = time.Now()
	result.QueueDuration = setupStarted.Sub(queueStarted)
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	if err := sofaCooldownError(); err != nil {
		return result, err
	}
	target, err := findSofaBrowserTarget(ctx)
	if err != nil {
		return result, err
	}
	dialer := websocket.Dialer{HandshakeTimeout: 4 * time.Second, Proxy: nil}
	conn, resp, err := dialer.DialContext(ctx, target.WebSocket, nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return result, fmt.Errorf("Chromium SofaScore: połączenie z kartą DevTools: %w", err)
	}
	defer conn.Close()
	// A 32 MiB UTF-8 body can expand sixfold when escaped in CDP's JSON envelope.
	conn.SetReadLimit((192 << 20) + (1 << 20))
	deadline, _ := ctx.Deadline()
	_ = conn.SetReadDeadline(deadline)
	cdp := &sofaCDPConnection{conn: conn, ctx: ctx}
	stopCancel := context.AfterFunc(ctx, func() {
		cdp.write.Lock()
		if cdp.abort != nil {
			_ = conn.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
			_ = conn.WriteJSON(map[string]any{"id": -1, "method": "Runtime.evaluate", "params": cdp.abort})
		}
		cdp.write.Unlock()
		_ = conn.Close() // interrupts a pending CDP read even during setup
	})
	defer stopCancel()
	var tree struct {
		FrameTree struct {
			Frame struct {
				ID  string `json:"id"`
				URL string `json:"url"`
			} `json:"frame"`
		} `json:"frameTree"`
	}
	if err := cdp.call("Page.getFrameTree", map[string]any{}, &tree); err != nil {
		return result, err
	}
	if !isSofaBrowserPage(tree.FrameTree.Frame.URL) || tree.FrameTree.Frame.ID == "" {
		return result, fmt.Errorf("Chromium SofaScore: karta zmieniła adres; pozostaw otwartą stronę SofaScore")
	}
	var world struct {
		ID int `json:"executionContextId"`
	}
	if err := cdp.call("Page.createIsolatedWorld", map[string]any{"frameId": tree.FrameTree.Frame.ID, "worldName": "value-bets-sofascore"}, &world); err != nil {
		return result, err
	}
	if world.ID == 0 {
		return result, fmt.Errorf("Chromium SofaScore: brak kontekstu strony")
	}
	var cookies struct {
		Cookies []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"cookies"`
	}
	if err := cdp.call("Network.getCookies", map[string]any{"urls": []string{reqURL}}, &cookies); err != nil {
		return result, err
	}
	headers := map[string]string{"accept": "*/*"}
	for _, cookie := range cookies.Cookies {
		if cookie.Name == "sofa_captcha" {
			headers["x-captcha"] = cookie.Value
			break
		}
	}
	if requestedWith := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_REQUESTED_WITH")); requestedWith != "" {
		if !sofaRequestedWithPattern.MatchString(requestedWith) {
			return result, fmt.Errorf("Chromium SofaScore: niepoprawne VALUE_BETS_SOFA_REQUESTED_WITH")
		}
		headers["x-requested-with"] = requestedWith
	}
	urlJSON, _ := json.Marshal(reqURL)
	headersJSON, _ := json.Marshal(headers)
	var requestKey [12]byte
	if _, err := cryptorand.Read(requestKey[:]); err != nil {
		return result, fmt.Errorf("Chromium SofaScore: nie można utworzyć identyfikatora pobrania")
	}
	abortKeyJSON, _ := json.Marshal(fmt.Sprintf("__valueBetsFetch_%x", requestKey))
	// The isolated world shares the real tab's cookie store and networking stack.
	// No user-agent, TLS impersonation or cookies copied from a file are used here.
	expression := `(async () => {
  const result = {status:0, contentType:"", retryAfter:"", body:"", error:""};
  if (location.origin !== "https://www.sofascore.com") { result.error="origin"; return result; }
  const controller = new AbortController();
  const abortKey = ` + string(abortKeyJSON) + `;
  globalThis[abortKey] = controller;
  const timer = setTimeout(() => controller.abort(), 15000);
  try {
    const response = await fetch(` + string(urlJSON) + `, {
      method:"GET", credentials:"include", cache:"no-store", redirect:"error",
      headers:` + string(headersJSON) + `, signal:controller.signal
    });
    result.status = response.status;
    result.contentType = response.headers.get("content-type") || "";
    result.retryAfter = response.headers.get("retry-after") || "";
    const reader = response.body.getReader(), decoder = new TextDecoder("utf-8", {fatal:true});
    const parts = []; let size = 0;
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.byteLength;
      if (size > 33554432) { controller.abort(); result.error="limit"; return result; }
      parts.push(decoder.decode(chunk.value, {stream:true}));
    }
    parts.push(decoder.decode());
    result.body = parts.join("");
  } catch (_) { result.error = controller.signal.aborted ? "timeout" : "network"; }
  finally { clearTimeout(timer); delete globalThis[abortKey]; }
  return result;
})()`
	cdp.write.Lock()
	cdp.abort = map[string]any{"expression": "globalThis[" + string(abortKeyJSON) + "]?.abort()", "contextId": world.ID, "silent": true}
	cdp.write.Unlock()
	if target.ID != sofaBrowserLastTarget {
		log.Printf("[SOFA-BROWSER] ATTACHED target=%q transport=chromium-cdp", target.ID)
		sofaBrowserLastTarget = target.ID
	}
	log.Printf("[SOFA-BROWSER] REQUEST endpoint=%q captcha_cookie=%t", u.RequestURI(), headers["x-captcha"] != "")
	var evaluated struct {
		Result struct {
			Value struct {
				Status      int    `json:"status"`
				ContentType string `json:"contentType"`
				RetryAfter  string `json:"retryAfter"`
				Body        string `json:"body"`
				Error       string `json:"error"`
			} `json:"value"`
		} `json:"result"`
		Exception json.RawMessage `json:"exceptionDetails"`
	}
	fetchStarted = time.Now()
	result.SetupDuration = fetchStarted.Sub(setupStarted)
	if err := cdp.call("Runtime.evaluate", map[string]any{"expression": expression, "contextId": world.ID, "awaitPromise": true, "returnByValue": true, "silent": true}, &evaluated); err != nil {
		return result, err
	}
	if len(evaluated.Exception) > 0 && string(evaluated.Exception) != "null" {
		return result, fmt.Errorf("Chromium SofaScore: skrypt pobrania nie powiódł się; sprawdź kartę przeglądarki")
	}
	value := evaluated.Result.Value
	result.Status, result.ContentType, result.RetryAfter = value.Status, value.ContentType, value.RetryAfter
	result.Body = []byte(value.Body)
	if value.Error != "" {
		switch value.Error {
		case "origin":
			return result, fmt.Errorf("Chromium SofaScore: karta zmieniła domenę")
		case "timeout":
			return result, fmt.Errorf("Chromium SofaScore: przekroczono 15s na pobranie odpowiedzi")
		case "limit":
			return result, fmt.Errorf("Chromium SofaScore: odpowiedź przekracza 32 MiB")
		default:
			return result, fmt.Errorf("Chromium SofaScore: fetch nie powiódł się; sprawdź sieć i kartę przeglądarki")
		}
	}
	if result.Status == http.StatusOK && !json.Valid(result.Body) {
		return result, fmt.Errorf("Chromium SofaScore: HTTP 200 bez poprawnego JSON; sprawdź challenge ręcznie w przeglądarce")
	}
	if result.Status == 0 {
		return result, fmt.Errorf("Chromium SofaScore: brak statusu odpowiedzi fetch")
	}
	return result, nil
}

func runSofaBrowserDiagnostic(ctx context.Context, dataDir string) error {
	const endpoint = "/search/all?q=chicago+fire&page=0"
	started := time.Now().UTC()
	response, fetchErr := fetchSofaBrowser(ctx, sofaBase+endpoint)
	if response.Status != 0 {
		noteSofaBlocked(response.Status, response.RetryAfter)
		if response.Status != http.StatusOK {
			fetchErr = errors.Join(fetchErr, fmt.Errorf("SofaScore HTTP %d", response.Status))
		}
	}
	teams := []SofaTeamItem{}
	if fetchErr == nil {
		var search SofaSearchResponse
		if err := json.Unmarshal(response.Body, &search); err != nil {
			fetchErr = fmt.Errorf("niepoprawny JSON wyszukiwania")
		} else {
			teams = search.ExtractTeams()
			if len(teams) == 0 {
				fetchErr = fmt.Errorf("wyszukiwanie Chicago Fire nie zwróciło drużyn")
			}
		}
	}
	info := archiveInfo{Bookmaker: "SOFASCORE", Scope: "diagnostyka-chromium-wyszukiwanie-chicago-fire", Endpoint: sofaBase + endpoint, Method: "GET", Status: response.Status, ContentType: response.ContentType}
	if err := archiveResponse(context.WithValue(ctx, archiveContextKey{}, info), "odpowiedz-chromium", response.Body, "http.bin", readableJSONValues(response.Body), fetchErr); err != nil {
		return errors.Join(fetchErr, err)
	}
	report := struct {
		RecordedAt time.Time       `json:"recordedAt"`
		Endpoint   string          `json:"endpoint"`
		Response   sofaFetchResult `json:"response"`
		Teams      []SofaTeamItem  `json:"teams"`
		Error      string          `json:"error,omitempty"`
	}{RecordedAt: started, Endpoint: endpoint, Response: response, Teams: teams}
	if fetchErr != nil {
		report.Error = fetchErr.Error()
	}
	dir := filepath.Join(dataDir, "diagnostics")
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	path := filepath.Join(dir, "sofascore_chromium-wyszukiwanie-chicago-fire_"+started.Format("20060102T150405.000000000Z")+".json")
	if err := atomicJSON(path, report); err != nil {
		return err
	}
	log.Printf("[SOFA-BROWSER] DIAGNOSTIC status=%d teams=%d duration=%s report=%q error=%v", response.Status, len(teams), time.Since(started), path, fetchErr)
	return fetchErr
}

var sofaCaptchaPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$`)
var sofaRequestedWithPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var sofaCaptchaLog = struct {
	sync.Mutex
	status string
}{}

func reportSofaCaptcha(status, message string) {
	sofaCaptchaLog.Lock()
	defer sofaCaptchaLog.Unlock()
	if sofaCaptchaLog.status == status {
		return
	}
	sofaCaptchaLog.status = status
	if message != "" {
		log.Printf("[SOFA-CAPTCHA] %s", message)
	}
}

func configuredSofaCaptcha() (string, time.Time) {
	token := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_CAPTCHA"))
	if token == "" {
		path := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_CAPTCHA_FILE"))
		if path == "" {
			path = filepath.Join(serverDataDir, "sofascore_captcha.txt")
		}
		raw, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			reportSofaCaptcha("missing", "")
			return "", time.Time{}
		}
		if err != nil {
			reportSofaCaptcha("read-error", "READ_ERROR nie można odczytać pliku tokenu")
			return "", time.Time{}
		}
		token = strings.TrimSpace(string(raw))
	}
	return validateSofaCaptcha(token)
}

func validateSofaCaptcha(token string) (string, time.Time) {
	token = strings.TrimSpace(token)
	if token == "" {
		reportSofaCaptcha("missing", "")
		return "", time.Time{}
	}
	if len(token) > 16*1024 || !sofaCaptchaPattern.MatchString(token) {
		reportSofaCaptcha("invalid", "INVALID niepoprawny format tokenu; pominięto nagłówki captcha")
		return "", time.Time{}
	}
	// Decode expiry metadata only; signature verification belongs to SofaScore.
	payload, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
	var claims struct {
		Expires int64 `json:"exp"`
	}
	if err != nil || json.Unmarshal(payload, &claims) != nil || claims.Expires <= 0 {
		reportSofaCaptcha("invalid", "INVALID brak poprawnego terminu ważności tokenu")
		return "", time.Time{}
	}
	expires := time.Unix(claims.Expires, 0).UTC()
	if !time.Now().Before(expires) {
		reportSofaCaptcha("expired", "EXPIRED token wygasł; pominięto nagłówki captcha, odśwież plik konfiguracji")
		return "", time.Time{}
	}
	reportSofaCaptcha("active-"+expires.Format(time.RFC3339), "ACTIVE expires_at="+expires.Format(time.RFC3339))
	return token, expires
}

type sofaBrowserSession struct {
	Headers fhttp.Header
	Cookies []*fhttp.Cookie
}

var sofaSessionLog = struct {
	sync.Mutex
	digest [32]byte
	loaded bool
}{}

// Parse a browser's POSIX "Copy as cURL" text as data. Never invoke a shell,
// expand variables or execute any option from the imported command.
func splitSofaCurl(raw string) ([]string, error) {
	var args []string
	var word strings.Builder
	var quote byte
	active := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if quote == '\'' {
			if ch == '\'' {
				quote = 0
			} else {
				word.WriteByte(ch)
			}
			continue
		}
		if ch == '\\' {
			if i+1 >= len(raw) {
				return nil, fmt.Errorf("sesja SofaScore: niedokończona sekwencja escape w pliku cURL")
			}
			i++
			if raw[i] == '\r' && i+1 < len(raw) && raw[i+1] == '\n' {
				i++
				continue
			}
			if raw[i] == '\n' {
				continue
			}
			word.WriteByte(raw[i])
			active = true
			continue
		}
		if quote == '"' {
			if ch == '"' {
				quote = 0
			} else {
				word.WriteByte(ch)
			}
			continue
		}
		switch ch {
		case '\'', '"':
			quote, active = ch, true
		case ' ', '\t', '\r', '\n':
			if active {
				args = append(args, word.String())
				word.Reset()
				active = false
			}
		case ';', '|', '<', '>', '&', '`':
			return nil, fmt.Errorf("sesja SofaScore: plik powinien zawierać jedno skopiowane polecenie cURL")
		default:
			word.WriteByte(ch)
			active = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("sesja SofaScore: niedomknięty cudzysłów w pliku cURL")
	}
	if active {
		args = append(args, word.String())
	}
	return args, nil
}

func readSofaBrowserSession() (*sofaBrowserSession, error) {
	path := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_CURL_FILE"))
	if path == "" {
		path = filepath.Join(serverDataDir, "sofascore_request.curl")
	}
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sesja SofaScore: nie można otworzyć pliku cURL")
	}
	defer file.Close()
	raw, err := readBounded(file, 128<<10)
	if err != nil {
		return nil, fmt.Errorf("sesja SofaScore: błąd odczytu lub przekroczony limit pliku cURL (128 KiB)")
	}
	args, err := splitSofaCurl(strings.TrimPrefix(string(raw), "\ufeff"))
	if err != nil {
		return nil, err
	}
	if len(args) == 0 || (args[0] != "curl" && args[0] != "curl.exe") {
		return nil, fmt.Errorf("sesja SofaScore: wklej pełne polecenie Copy as cURL (bash)")
	}
	session := &sofaBrowserSession{Headers: fhttp.Header{}}
	allowed := map[string]bool{"accept": true, "accept-language": true, "priority": true, "referer": true, "user-agent": true, "sec-ch-ua": true, "sec-ch-ua-mobile": true, "sec-ch-ua-platform": true, "sec-fetch-dest": true, "sec-fetch-mode": true, "sec-fetch-site": true, "x-captcha": true, "x-requested-with": true}
	var sourceURL, cookieRaw string
	method := "GET"
	for i := 1; i < len(args); i++ {
		option, value, inline := strings.Cut(args[i], "=")
		switch option {
		case "-H", "--header", "-b", "--cookie", "--url", "-X", "--request":
			if !inline {
				i++
				if i >= len(args) {
					return nil, fmt.Errorf("sesja SofaScore: brak wartości opcji w pliku cURL")
				}
				value = args[i]
			}
			if strings.ContainsAny(value, "\r\n\x00") {
				return nil, fmt.Errorf("sesja SofaScore: niepoprawne znaki w wartości opcji")
			}
			switch option {
			case "--url":
				sourceURL = value
			case "-X", "--request":
				method = strings.ToUpper(value)
			case "-b", "--cookie":
				cookieRaw += "; " + value
			default:
				name, headerValue, ok := strings.Cut(value, ":")
				if !ok {
					return nil, fmt.Errorf("sesja SofaScore: niepoprawny nagłówek w pliku cURL")
				}
				name, headerValue = strings.ToLower(strings.TrimSpace(name)), strings.TrimSpace(headerValue)
				if name == "cookie" {
					cookieRaw += "; " + headerValue
				} else if allowed[name] {
					session.Headers[name] = []string{headerValue}
				}
			}
		case "-I", "--head":
			method = "HEAD"
		default:
			if strings.HasPrefix(args[i], "https://") {
				sourceURL = args[i]
			}
		}
	}
	origin, err := url.Parse(sourceURL)
	if err != nil || origin.Scheme != "https" || origin.Hostname() != "www.sofascore.com" || (origin.Port() != "" && origin.Port() != "443") || !strings.HasPrefix(origin.Path, "/api/v1/") {
		return nil, fmt.Errorf("sesja SofaScore: źródło cURL musi być endpointem HTTPS www.sofascore.com/api/v1/")
	}
	if method != "GET" && method != "HEAD" {
		return nil, fmt.Errorf("sesja SofaScore: skopiuj żądanie GET albo HEAD")
	}
	if strings.Join(session.Headers["user-agent"], "") == "" {
		return nil, fmt.Errorf("sesja SofaScore: brak User-Agent w skopiowanym żądaniu")
	}
	request := &fhttp.Request{Header: fhttp.Header{"Cookie": {strings.TrimPrefix(cookieRaw, "; ")}}}
	seen := map[string]bool{}
	for _, cookie := range request.Cookies() {
		if seen[cookie.Name] {
			return nil, fmt.Errorf("sesja SofaScore: powtórzone nazwy cookies; skopiuj pojedyncze świeże żądanie")
		}
		seen[cookie.Name] = true
		cookie.Path, cookie.Secure = "/", true
		session.Cookies = append(session.Cookies, cookie)
	}
	if strings.Trim(cookieRaw, "; ") != "" && len(session.Cookies) == 0 {
		return nil, fmt.Errorf("sesja SofaScore: nie udało się odczytać cookies z pliku cURL")
	}
	digest := sha256.Sum256(raw)
	sofaSessionLog.Lock()
	if !sofaSessionLog.loaded || sofaSessionLog.digest != digest {
		log.Printf("[SOFA-SESSION] LOADED source=curl headers=%d cookies=%d captured_method=%s request_method=GET tls_profile=Chrome_152", len(session.Headers), len(session.Cookies), method)
		sofaSessionLog.digest, sofaSessionLog.loaded = digest, true
	}
	sofaSessionLog.Unlock()
	return session, nil
}

func applySofaRequestHeaders(client tls_client.HttpClient, req *fhttp.Request) error {
	req.Header = sofaRequestHeaders()
	session, err := readSofaBrowserSession()
	if err != nil {
		return err
	}
	var importedToken string
	var cookies []*fhttp.Cookie
	if session != nil {
		req.Header = session.Headers
		for _, cookie := range session.Cookies {
			if cookie.Name == "sofa_captcha" {
				importedToken = cookie.Value
			} else {
				cookies = append(cookies, cookie)
			}
		}
		if value := strings.Join(req.Header["x-captcha"], ""); value != "" {
			if importedToken != "" && importedToken != value {
				return fmt.Errorf("sesja SofaScore: cookie sofa_captcha i X-Captcha mają różne wartości; skopiuj nowe żądanie")
			}
			importedToken = value
		}
		delete(req.Header, "x-captcha")
	}
	if value := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_REQUESTED_WITH")); sofaRequestedWithPattern.MatchString(value) {
		req.Header["x-requested-with"] = []string{value}
	}
	var token string
	var expires time.Time
	if value := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_CAPTCHA")); value != "" {
		token, expires = validateSofaCaptcha(value)
	} else if importedToken != "" {
		token, expires = validateSofaCaptcha(importedToken)
	} else {
		token, expires = configuredSofaCaptcha()
	}
	if token != "" {
		req.Header["x-captcha"] = []string{token}
		cookies = append(cookies, &fhttp.Cookie{Name: "sofa_captcha", Value: token, Path: "/", Secure: true, Expires: expires})
	} else {
		// tls-client's jar uses MaxAge < 0 for removal, not Expires alone.
		cookies = append(cookies, &fhttp.Cookie{Name: "sofa_captcha", Value: "removed", Path: "/", Secure: true, MaxAge: -1})
	}
	// The jar adds Cookie once, without a second manually constructed header.
	client.SetCookies(req.URL, cookies)
	order := []string{"sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform", "user-agent", "accept", "accept-language", "priority", "referer", "sec-fetch-site", "sec-fetch-mode", "sec-fetch-dest", "x-requested-with", "x-captcha", "accept-encoding", "cookie"}
	req.Header[fhttp.HeaderOrderKey] = order
	return nil
}

func sofaRequestHeaders() fhttp.Header {
	requestedWith := strings.TrimSpace(os.Getenv("VALUE_BETS_SOFA_REQUESTED_WITH"))
	headers := fhttp.Header{
		"user-agent":         {"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"},
		"accept":             {"*/*"},
		"accept-language":    {"en-US,en;q=0.9"},
		"referer":            {"https://www.sofascore.com/"},
		"sec-ch-ua":          {`"Not A(Brand";v="99", "Chromium";v="154"`},
		"sec-ch-ua-mobile":   {"?0"},
		"sec-ch-ua-platform": {`"Linux"`},
		"priority":           {"u=1, i"},
		"sec-fetch-dest":     {"empty"},
		"sec-fetch-mode":     {"cors"},
		"sec-fetch-site":     {"same-origin"},
	}
	if sofaRequestedWithPattern.MatchString(requestedWith) {
		headers["x-requested-with"] = []string{requestedWith}
	}
	return headers
}

func persistTeamCache(slug, id string) error {
	return persistTeamIdentity(slug, sofaTeamIdentity{ID: id, Manual: true, Version: 3})
}

func persistTeamIdentity(slug string, identity sofaTeamIdentity) error {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	path := getCacheFilePath("sofa_team_cache.json")
	m := map[string]string{}
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if m == nil {
		m = map[string]string{}
	}
	slug = strings.ToLower(strings.TrimSpace(slug))
	identitiesPath := getCacheFilePath("sofa_team_identity_cache.json")
	identities := map[string]sofaTeamIdentity{}
	if raw, err := os.ReadFile(identitiesPath); err == nil {
		if err := json.Unmarshal(raw, &identities); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if identities == nil {
		identities = map[string]sofaTeamIdentity{}
	}
	identities[slug] = identity
	if err := atomicJSON(identitiesPath, identities); err != nil {
		return err
	}
	if oldID := m[slug]; oldID != "" && oldID != identity.ID {
		log.Printf("[CACHE] TEAM_REPAIRED slug=%q old_id=%s new_id=%s name=%q", slug, oldID, identity.ID, identity.Name)
	}
	m[slug] = identity.ID
	return atomicJSON(path, m)
}

// Calendar snapshots use Polish dates even when the host runs in UTC.
func polishDay(now time.Time) string {
	zone, _ := time.LoadLocation("Europe/Warsaw")
	return now.In(zone).Format("2006-01-02")
}

var womenCompetitionPattern = regexp.MustCompile(`(?i)(^|[^a-z])(wsl|nwsl|frauen|femenil|feminines|feminina|feminino|femenina|femenino|dames|ladies|femmes|femminile|damallsvenskan|toppserien|liga-f|we-league|a-league-women)([^a-z]|$)`)

func isWomenEvent(event CalendarEvent) bool {
	for _, name := range []string{event.Home, event.Away, event.Competition, event.Country} {
		_, gender := sofaTeamNameAndGender(name)
		if gender == "F" || sofaWomenPattern.MatchString(removeDiacritics(name)) || womenCompetitionPattern.MatchString(removeDiacritics(name)) {
			return true
		}
	}
	return false
}

func mensCalendarEvents(events []CalendarEvent) []CalendarEvent {
	result := make([]CalendarEvent, 0, len(events))
	for _, event := range events {
		if !isWomenEvent(event) {
			result = append(result, event)
		}
	}
	return result
}

type AnalysisEligibility struct {
	Status    string            `json:"status"`
	Reason    string            `json:"reason,omitempty"`
	Links     map[string]string `json:"links"`
	ExpiresAt time.Time         `json:"expiresAt,omitempty"`
}

var calendarClubWords = map[string]bool{"fc": true, "cf": true, "afc": true, "fk": true, "sk": true, "ks": true, "sc": true, "ac": true, "ssc": true, "as": true, "rc": true, "cd": true, "se": true, "ec": true, "sv": true}
var calendarGenericWords = map[string]bool{"real": true, "atletico": true, "athletic": true, "sporting": true, "racing": true, "nacional": true, "national": true, "america": true, "inter": true, "united": true, "city": true, "town": true, "deportivo": true, "club": true, "de": true, "da": true, "do": true, "the": true, "borussia": true, "dynamo": true, "dinamo": true, "olympique": true, "olympic": true, "hapoel": true, "maccabi": true, "red": true, "bull": true}
var calendarTeamAliases = map[string]string{
	"man-utd": "manchester-united", "manchester-utd": "manchester-united", "man-united": "manchester-united", "man-city": "manchester-city",
	"psg": "paris-saint-germain", "paris-sg": "paris-saint-germain", "bayern-munich": "bayern-munchen",
	"inter-mediolan": "inter", "inter-milan": "inter", "internazionale": "inter", "sporting-lizbona": "sporting-lisboa", "sporting-lisbon": "sporting-lisboa", "sporting-cp": "sporting-lisboa",
	"juventus-turyn": "juventus", "ajax-amsterdam": "ajax", "benfica-lizbona": "benfica", "arsenal-londyn": "arsenal", "chelsea-londyn": "chelsea", "tottenham-londyn": "tottenham", "tottenham-hotspur": "tottenham", "west-ham-united": "west-ham", "bvb": "borussia-dortmund", "borussia-m-gladbach": "borussia-monchengladbach", "borussia-moenchengladbach": "borussia-monchengladbach",
}

func calendarTeamIdentity(name string) (string, string) {
	core, gender := sofaTeamNameAndGender(calendarTeamSlug(name))
	words, qualifiers := []string{}, []string{}
	if gender == "F" {
		qualifiers = append(qualifiers, "women")
	}
	parts := strings.Split(core, "-")
	for i, word := range parts {
		switch {
		case sofaAgePattern.MatchString(word):
			qualifiers = append(qualifiers, word)
		case word == "ii" || word == "res" || word == "reserve" || word == "reserves" || word == "rezerwy" || word == "castilla" || word == "primavera" || (word == "b" && i == len(parts)-1):
			qualifiers = append(qualifiers, "reserves")
		case word == "iii" || word == "iv":
			qualifiers = append(qualifiers, word)
		case word != "" && !calendarClubWords[word]:
			words = append(words, word)
		}
	}
	core = strings.Join(words, "-")
	if strings.HasPrefix(core, "red-bull-") {
		qualifiers = append(qualifiers, "red-bull")
	}
	if alias, ok := calendarTeamAliases[core]; ok {
		core = alias
	}
	if alias, ok := teamSlugAliases[core]; ok {
		core = canonicalTeamCore(alias)
	}
	sort.Strings(qualifiers)
	return core, strings.Join(qualifiers, "|")
}

func sameCalendarTeam(left, right string) bool {
	a, aq := calendarTeamIdentity(left)
	b, bq := calendarTeamIdentity(right)
	if a == "" || b == "" || aq != bq {
		return false
	}
	if a == b {
		return true
	}
	shorter, longer := strings.Split(a, "-"), strings.Split(b, "-")
	if len(shorter) > len(longer) {
		shorter, longer = longer, shorter
	}
	if len(longer)-len(shorter) > 3 {
		return false
	}
	anchor := false
	for _, word := range shorter {
		found := false
		for _, other := range longer {
			found = found || word == other
		}
		if !found {
			return false
		}
		anchor = anchor || (len(word) >= 5 && !calendarGenericWords[word])
	}
	return anchor
}

func sameCalendarFixture(left, right CalendarEvent) bool {
	a, errA := time.Parse(time.RFC3339, left.StartTime)
	b, errB := time.Parse(time.RFC3339, right.StartTime)
	if errA != nil || errB != nil {
		return false
	}
	delta := a.Sub(b)
	if delta < -10*time.Minute || delta > 10*time.Minute {
		return false
	}
	return !isWomenEvent(left) && !isWomenEvent(right) && sameCalendarTeam(left.Home, right.Home) && sameCalendarTeam(left.Away, right.Away)
}

type calendarOfferIndex struct {
	ByBook     map[string][]statsWarmupCandidate
	ByTime     map[string]map[int64][]statsWarmupCandidate
	ByID       map[string]statsWarmupCandidate
	ByURL      map[string]statsWarmupCandidate
	MatchCache *calendarMatchCache
}

type calendarTeamKey struct {
	Core, Qualifier string
	Words           []string
}

type calendarFixtureKey struct{ Home, Away, Start, URL string }
type calendarMatchResult struct {
	Matches map[string]CalendarEvent
	Err     error
}
type calendarMatchCache struct {
	sync.Mutex
	Results map[calendarFixtureKey]calendarMatchResult
}

func sameCalendarIdentity(a, b calendarTeamKey) bool {
	if a.Core == "" || b.Core == "" || a.Qualifier != b.Qualifier {
		return false
	}
	if a.Core == b.Core {
		return true
	}
	shorter, longer := a.Words, b.Words
	if len(shorter) > len(longer) {
		shorter, longer = longer, shorter
	}
	if len(longer)-len(shorter) > 3 {
		return false
	}
	anchor := false
	for _, word := range shorter {
		found := false
		for _, other := range longer {
			found = found || word == other
		}
		if !found {
			return false
		}
		anchor = anchor || (len(word) >= 5 && !calendarGenericWords[word])
	}
	return anchor
}

func sameIndexedFixture(a, b statsWarmupCandidate) bool {
	delta := a.StartTime.Sub(b.StartTime)
	return delta >= -10*time.Minute && delta <= 10*time.Minute && sameCalendarIdentity(a.HomeIdentity, b.HomeIdentity) && sameCalendarIdentity(a.AwayIdentity, b.AwayIdentity)
}

// Snapshot once, deduplicating repeated dates by bookmaker/event and taking
// the newest calendar. Matching never issues network requests or guesses IDs.
func (s *calendarServer) calendarOffers() calendarOfferIndex {
	s.cache.once.Do(s.cache.readDisk)
	s.offerIndexMu.Lock()
	defer s.offerIndexMu.Unlock()
	s.cache.mu.Lock()
	revision := s.cache.revision
	if s.offerIndex.ByBook != nil && s.offerRevision == revision {
		s.cache.mu.Unlock()
		return s.offerIndex
	}
	entries := make(map[string]calendarCacheEntry, len(s.cache.entries))
	for key, entry := range s.cache.entries {
		if entry.Calendar != nil {
			entry.Calendar = cloneCalendar(entry.Calendar)
		}
		entries[key] = entry
	}
	s.cache.mu.Unlock()
	unique := map[string]statsWarmupCandidate{}
	identities := map[string]calendarTeamKey{}
	identity := func(name string) calendarTeamKey {
		if key, ok := identities[name]; ok {
			return key
		}
		core, qualifier := calendarTeamIdentity(name)
		key := calendarTeamKey{Core: core, Qualifier: qualifier, Words: strings.Split(core, "-")}
		identities[name] = key
		return key
	}
	for key, entry := range entries {
		if entry.Calendar == nil {
			continue
		}
		for _, book := range serverBookmakers {
			if !strings.HasPrefix(key, strings.ToLower(book)+":") {
				continue
			}
			for _, event := range entry.Calendar.Events {
				if isWomenEvent(event) || event.URL == "" {
					continue
				}
				id := book + ":" + event.ID
				if old, ok := unique[id]; ok && !entry.FetchedAt.After(old.FetchedAt) {
					continue
				}
				start, err := time.Parse(time.RFC3339, event.StartTime)
				if err != nil {
					continue
				}
				unique[id] = statsWarmupCandidate{Bookmaker: book, Event: event, StartTime: start, FetchedAt: entry.FetchedAt, HomeIdentity: identity(event.Home), AwayIdentity: identity(event.Away)}
			}
		}
	}
	result := calendarOfferIndex{ByBook: map[string][]statsWarmupCandidate{}, ByTime: map[string]map[int64][]statsWarmupCandidate{}, ByID: unique, ByURL: map[string]statsWarmupCandidate{}, MatchCache: &calendarMatchCache{Results: map[calendarFixtureKey]calendarMatchResult{}}}
	for _, candidate := range unique {
		book := candidate.Bookmaker
		result.ByURL[candidate.Event.URL] = candidate
		result.ByBook[book] = append(result.ByBook[book], candidate)
		if result.ByTime[book] == nil {
			result.ByTime[book] = map[int64][]statsWarmupCandidate{}
		}
		bucket := candidate.StartTime.Unix() / 600
		result.ByTime[book][bucket] = append(result.ByTime[book][bucket], candidate)
	}
	s.offerIndex, s.offerRevision = result, revision
	log.Printf("[CALENDAR-INDEX] BUILT revision=%d offers=%d normalized_teams=%d", revision, len(unique), len(identities))
	return result
}

func matchCalendarOffers(seed CalendarEvent, offers calendarOfferIndex) (map[string]CalendarEvent, error) {
	key := calendarFixtureKey{seed.Home, seed.Away, seed.StartTime, seed.URL}
	if offers.MatchCache != nil {
		offers.MatchCache.Lock()
		defer offers.MatchCache.Unlock()
		if result, ok := offers.MatchCache.Results[key]; ok {
			return result.Matches, result.Err
		}
	}
	matches, err := resolveCalendarOffers(seed, offers)
	if offers.MatchCache != nil {
		offers.MatchCache.Results[key] = calendarMatchResult{matches, err}
	}
	return matches, err
}

func resolveCalendarOffers(seed CalendarEvent, offers calendarOfferIndex) (map[string]CalendarEvent, error) {
	matches := map[string]CalendarEvent{}
	start, err := time.Parse(time.RFC3339, seed.StartTime)
	if err != nil {
		return nil, fmt.Errorf("niepoprawny termin meczu")
	}
	seedCandidate, ok := offers.ByURL[seed.URL]
	if ok && (seedCandidate.Event.Home != seed.Home || seedCandidate.Event.Away != seed.Away || seedCandidate.Event.StartTime != seed.StartTime) {
		ok = false
	}
	if !ok {
		if isWomenEvent(seed) {
			return nil, fmt.Errorf("mecze kobiet są wyłączone")
		}
		home, hq := calendarTeamIdentity(seed.Home)
		away, aq := calendarTeamIdentity(seed.Away)
		seedCandidate = statsWarmupCandidate{Event: seed, StartTime: start, HomeIdentity: calendarTeamKey{home, hq, strings.Split(home, "-")}, AwayIdentity: calendarTeamKey{away, aq, strings.Split(away, "-")}}
	}
	selected := map[string]statsWarmupCandidate{}
	bucket := start.Unix() / 600
	for _, book := range serverBookmakers {
		count := 0
		nearby := []statsWarmupCandidate{}
		for offset := int64(-1); offset <= 1; offset++ {
			nearby = append(nearby, offers.ByTime[book][bucket+offset]...)
		}
		for _, candidate := range nearby {
			if sameIndexedFixture(seedCandidate, candidate) {
				matches[book], count = candidate.Event, count+1
				selected[book] = candidate
			}
		}
		if count != 1 {
			return matches, fmt.Errorf("%s: liczba jednoznacznych dopasowań meczu: %d", book, count)
		}
	}
	for _, left := range selected {
		for _, right := range selected {
			if !sameIndexedFixture(left, right) {
				return nil, fmt.Errorf("nazwy lub terminy bukmacherów nie wskazują tego samego meczu")
			}
		}
	}
	return matches, nil
}

func (s *calendarServer) analysisEvents(query AnalysisRequest) (map[string]CalendarEvent, error) {
	offers := s.calendarOffers()
	selected := map[string]CalendarEvent{}
	for _, source := range []struct{ book, input string }{{"SUPERBET", query.SuperbetURL}, {"BETCLIC", query.BetclicURL}, {"STS", query.STSURL}} {
		id, _, _ := extractIDAndSlug(source.input)
		if candidate, ok := offers.ByID[source.book+":"+id]; ok {
			selected[source.book] = candidate.Event
		}
		if _, ok := selected[source.book]; !ok {
			return nil, fmt.Errorf("%s: mecz nie jest dostępny w kalendarzu męskich rozgrywek; odśwież kalendarz", source.book)
		}
	}
	matches, err := matchCalendarOffers(selected["SUPERBET"], offers)
	if err != nil {
		return nil, err
	}
	for _, book := range serverBookmakers {
		if matches[book].ID != selected[book].ID {
			return nil, fmt.Errorf("%s: link wskazuje inne spotkanie", book)
		}
	}
	return matches, nil
}

func (s *calendarServer) bundleOddsVersion(matches map[string]CalendarEvent, now time.Time) (string, bool) {
	s.oddsMu.Lock()
	defer s.oddsMu.Unlock()
	versions := []string{}
	for _, book := range serverBookmakers {
		snapshot, ok := s.oddsCache[book+":"+matches[book].ID]
		if !ok || !snapshot.fresh(now) || len(snapshot.Props) == 0 {
			return "", false
		}
		versions = append(versions, snapshot.ArchiveVersion)
	}
	return researchHash(versions), true
}

func (s *calendarServer) decorateCalendar(book string, calendar *Calendar) {
	calendar.Events = mensCalendarEvents(calendar.Events)
	offers := s.calendarOffers()
	s.marketMu.Lock()
	states := make(map[string]MatchMarketAvailability, len(s.marketCache))
	for key, state := range s.marketCache {
		states[key] = state
	}
	s.marketMu.Unlock()
	now := time.Now()
	zone, _ := time.LoadLocation("Europe/Warsaw")
	for i := range calendar.Events {
		e := &calendar.Events[i]
		eligibility := &AnalysisEligibility{Status: "pending", Links: map[string]string{}}
		matches, err := matchCalendarOffers(*e, offers)
		if err != nil {
			eligibility.Status, eligibility.Reason = "unavailable", err.Error()
		} else {
			eligibility.Status = "available"
			for _, other := range serverBookmakers {
				eligibility.Links[other] = matches[other].URL
			}
			for _, other := range serverBookmakers {
				matched := matches[other]
				state, ok := states[other+":"+matched.ID]
				if ok && marketAvailabilityMatches(state, matched) && state.Status == "error" {
					eligibility.Status, eligibility.Reason = "error", other+": "+state.Error
					if state.ExpiresAt.After(now) {
						eligibility.Reason += "; ponowienie po " + state.ExpiresAt.In(zone).Format("15:04")
					} else {
						eligibility.Reason += "; oczekuje na ponowienie"
					}
					break
				} else if !ok || !marketAvailabilityValid(state, matched, now) {
					eligibility.Status, eligibility.Reason = "pending", other+": oczekiwanie na pełną ofertę"
				} else if state.Status != "available" || len(state.Markets) == 0 {
					eligibility.Status, eligibility.Reason = "unavailable", other+": brak dostępnych rynków zawodników"
					break
				}
				if eligibility.ExpiresAt.IsZero() || state.ExpiresAt.Before(eligibility.ExpiresAt) {
					eligibility.ExpiresAt = state.ExpiresAt
				}
			}
			if eligibility.Status == "pending" || eligibility.Status == "error" {
				start, parseErr := time.Parse(time.RFC3339, e.StartTime)
				switch {
				case parseErr != nil:
					eligibility.Status, eligibility.Reason = "unavailable", "Niepoprawny termin meczu"
				case e.Live || !start.After(now):
					eligibility.Status, eligibility.Reason = "inactive", "Mecz rozpoczęty; poza kolejką ofert przedmeczowych"
				case !s.automaticLeagueEnabled(matches):
					eligibility.Status, eligibility.Reason = "skipped", "Sprawdzanie w tle wyłączone dla tej ligi; wybierz mecz, aby sprawdzić ofertę ręcznie"
				case start.After(now.Add(statsWarmupHorizon)):
					eligibility.Status, eligibility.Reason = "deferred", "Sprawdzanie w tle rozpocznie się 48 godzin przed meczem"
				}
			}
		}
		e.AnalysisEligibility = eligibility
		if state, ok := states[book+":"+e.ID]; ok && marketAvailabilityMatches(state, *e) {
			copy := state
			e.MarketAvailability = &copy
		}
	}
}

// Offer discovery runs independently of SofaScore, settlement and analysis.
// It reads calendars, scans closest fixtures first and shares the ordinary odds cache.
func (s *calendarServer) marketScanCandidates(now time.Time) []statsWarmupCandidate {
	offers := s.calendarOffers()
	unique := map[string]statsWarmupCandidate{}
	for _, candidate := range offers.ByBook["STS"] {
		if candidate.Event.Live || !candidate.StartTime.After(now) || candidate.StartTime.After(now.Add(statsWarmupHorizon)) {
			continue
		}
		matches, err := matchCalendarOffers(candidate.Event, offers)
		if err != nil {
			continue
		}
		if !s.automaticLeagueEnabled(matches) {
			continue
		}
		for _, book := range serverBookmakers {
			event := matches[book]
			start, _ := time.Parse(time.RFC3339, event.StartTime)
			if event.Live || !start.After(now) || start.After(now.Add(statsWarmupHorizon)) {
				continue
			}
			unique[book+":"+event.ID] = statsWarmupCandidate{Bookmaker: book, Event: event, StartTime: start}
		}
	}
	s.marketMu.Lock()
	result := []statsWarmupCandidate{}
	for key, candidate := range unique {
		if state, ok := s.marketCache[key]; ok && marketAvailabilityValid(state, candidate.Event, now) {
			continue
		}
		if s.marketFlights[key] != nil {
			continue
		}
		if state, ok := s.marketCache[key]; ok {
			candidate.OfferCheckedAt = state.CheckedAt
		}
		result = append(result, candidate)
	}
	s.marketMu.Unlock()
	sort.Slice(result, func(i, j int) bool {
		if !result[i].OfferCheckedAt.Equal(result[j].OfferCheckedAt) {
			return result[i].OfferCheckedAt.Before(result[j].OfferCheckedAt)
		}
		if !result[i].StartTime.Equal(result[j].StartTime) {
			return result[i].StartTime.Before(result[j].StartTime)
		}
		return result[i].Bookmaker+result[i].Event.ID < result[j].Bookmaker+result[j].Event.ID
	})
	return result
}

func (s *calendarServer) marketBackgroundWorker() {
	defer close(s.marketDone)
	const concurrency = 1
	completed := make(chan string, concurrency)
	active := make(map[string]bool)
	var wg sync.WaitGroup
	defer wg.Wait()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var nextStart time.Time
	var analysisPriority bool
	log.Printf("[MARKET-SCAN] START workers=%d foreground_priority=true minimum_gap=3s", concurrency)
	for s.ctx.Err() == nil {
		s.workMu.Lock()
		foreground := s.foregroundWork != 0 || len(s.analysisQueue) != 0
		qualifiedRunning := s.qualifiedWork
		s.workMu.Unlock()
		qualifiedReady := false
		if !foreground && !qualifiedRunning {
			_, qualifiedReady = s.nextStatsWarmup()
		}
		prioritizeAnalysis := qualifiedRunning || qualifiedReady
		if prioritizeAnalysis != analysisPriority && (!foreground || qualifiedRunning) {
			analysisPriority = prioritizeAnalysis
			if analysisPriority {
				log.Printf("INFO Priorytet: analiza meczów z ofertą trzech bukmacherów; nowe skany ofert czekają")
			} else {
				log.Printf("INFO Gotowe mecze obsłużone; wznowienie sprawdzania pozostałych ofert")
			}
		}
		if !foreground && !prioritizeAnalysis && len(active) < concurrency && !time.Now().Before(nextStart) {
			for _, candidate := range s.marketScanCandidates(time.Now()) {
				if len(active) >= concurrency {
					break
				}
				key := candidate.Bookmaker + ":" + candidate.Event.ID
				if active[key] {
					continue
				}
				active[key] = true
				wg.Add(1)
				go func(c statsWarmupCandidate, key string) {
					defer wg.Done()
					defer func() { completed <- key }()
					matches, err := matchCalendarOffers(c.Event, s.calendarOffers())
					if err != nil || !s.automaticLeagueEnabled(matches) {
						return
					}
					ctx, cancel := context.WithTimeout(s.ctx, 60*time.Second)
					defer cancel()
					ctx = context.WithValue(ctx, sofaBackgroundContextKey{}, true)
					ctx = context.WithValue(ctx, offerDiscoveryContextKey{}, true)
					ctx = context.WithValue(ctx, archiveContextKey{}, archiveInfo{Automatic: true})
					started := time.Now()
					log.Printf("[MARKET-SCAN] CHECK bookmaker=%s event_id=%s", c.Bookmaker, c.Event.ID)
					result := s.checkMatchMarkets(ctx, c.Bookmaker, c.Event.ID, c.Event.Home, c.Event.Away)
					log.Printf("[MARKET-SCAN] RESULT bookmaker=%s event_id=%s status=%s duration=%s", c.Bookmaker, c.Event.ID, result.Status, time.Since(started).Round(time.Millisecond))
				}(candidate, key)
			}
		}
		select {
		case <-s.ctx.Done():
			return
		case key := <-completed:
			delete(active, key)
			nextStart = time.Now().Add(3 * time.Second)
		case <-ticker.C:
		}
	}
}

func hydratePredictionBet(p predictionRecord, evidence predictionEvidence) AnalyzedBet {
	bet := p.Bet
	if bet.AnalysisID == "" && len(p.RunID) >= 24 {
		bet.AnalysisID = p.RunID[:24]
	}
	bet.MatchOverview = evidence.Context.Overview
	for _, h := range append(append([]*PlayerStatHistory{}, evidence.HomePlayers...), evidence.AwayPlayers...) {
		if h.ID != bet.PlayerID {
			continue
		}
		h = metricHistory(h, bet.MarketType)
		bet.SampleValues = h.MetricValues[bet.MarketType]
		bet.SampleValuesSuperSub = h.MetricValuesWithSuperSub[bet.MarketType]
		bet.SampleMinutes, bet.SamplePositions = h.Minutes, h.Positions
		bet.SampleOpponents, bet.SampleDates, bet.SampleShots, bet.SampleRatings = h.Opponents, h.Dates, h.MatchShots, h.Ratings
		break
	}
	return bet
}

// The calendar opens the same saved run that supplied its counters.
func (s *calendarServer) automaticRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !regexp.MustCompile(`^[a-f0-9]{24}-(sts|superbet|betclic)$`).MatchString(id) {
		apiError(w, 400, fmt.Errorf("niepoprawne ID analizy"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	db := s.research.summaryDB
	if db == nil {
		db = s.research.db
	}
	// A bookmaker run identifies one shared match analysis. Read every source
	// from that exact analysis, never mixing another baseline or algorithm.
	rows, err := db.QueryContext(ctx, `SELECT r.id,r.bookmaker,r.evidence_path,COALESCE(q.payload,'') FROM prediction_runs r LEFT JOIN automatic_price_checks q ON q.run_id=r.id WHERE r.analysis_id=(SELECT analysis_id FROM prediction_runs WHERE id=? AND origin IN ('automatic','manual')) ORDER BY r.bookmaker`, id)
	if err != nil {
		apiError(w, 500, err)
		return
	}
	type savedRun struct{ id, book, path, quote string }
	runs := []savedRun{}
	for rows.Next() {
		var run savedRun
		if err := rows.Scan(&run.id, &run.book, &run.path, &run.quote); err != nil {
			rows.Close()
			apiError(w, 500, err)
			return
		}
		runs = append(runs, run)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		apiError(w, 500, err)
		return
	}
	if len(runs) == 0 {
		apiError(w, 404, fmt.Errorf("nie znaleziono zapisanej analizy"))
		return
	}
	if len(runs) != len(serverBookmakers) {
		apiError(w, 409, fmt.Errorf("zapisana analiza nie zawiera wszystkich trzech bukmacherów"))
		return
	}
	result := []AnalyzedBet{}
	evidenceByPath := map[string]predictionEvidence{}
	for _, run := range runs {
		evidence, found := evidenceByPath[run.path]
		if !found {
			if err := s.research.readDocument(run.path, &evidence); err != nil {
				apiError(w, 500, err)
				return
			}
			evidenceByPath[run.path] = evidence
		}
		if run.quote != "" {
			var check automaticPriceCheck
			if err := json.Unmarshal([]byte(run.quote), &check); err != nil {
				apiError(w, 500, err)
				return
			}
			for _, bet := range check.Bets {
				result = append(result, hydratePredictionBet(predictionRecord{Bet: bet, RunID: run.id}, evidence))
			}
			continue
		}
		rows, err := db.QueryContext(ctx, `SELECT payload FROM predictions WHERE run_id=? ORDER BY bet_key`, run.id)
		if err != nil {
			apiError(w, 500, err)
			return
		}
		for rows.Next() {
			var raw string
			var p predictionRecord
			if err = rows.Scan(&raw); err == nil {
				err = json.Unmarshal([]byte(raw), &p)
			}
			if err != nil {
				break
			}
			result = append(result, hydratePredictionBet(p, evidence))
		}
		err = errors.Join(err, rows.Err())
		rows.Close()
		if err != nil {
			apiError(w, 500, err)
			return
		}
	}
	jsonResponse(w, 200, result)
}

// Additional history is explicit, player-specific and anchored before the frozen fixture.
// It never rewrites the prediction, fair odds, or evidence used for an assessment.
func (s *calendarServer) extendedPredictionHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if !identifierPattern.MatchString(id) || err != nil || limit < 10 || limit > 50 {
		apiError(w, 400, fmt.Errorf("historia: limit od 10 do 50 meczów"))
		return
	}
	key := id + ":" + strconv.Itoa(limit)
	s.historyMu.Lock()
	cached := s.historyJobs[key]
	if cached != nil && !(cached.Status == "error" && r.Method == http.MethodPost) {
		copy := *cached
		s.historyMu.Unlock()
		jsonResponse(w, 200, copy)
		return
	}
	s.historyMu.Unlock()
	if r.Method != http.MethodPost {
		apiError(w, 404, fmt.Errorf("nie zlecono historii"))
		return
	}
	p, err := s.research.prediction(id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			apiError(w, 404, err)
		} else {
			apiError(w, 500, err)
		}
		return
	}
	var evidence predictionEvidence
	if err := s.research.readDocument(p.EvidencePath, &evidence); err != nil {
		apiError(w, 500, err)
		return
	}

	s.historyMu.Lock()
	if existing := s.historyJobs[key]; existing != nil && !(existing.Status == "error" && r.Method == http.MethodPost) {
		copy := *existing
		s.historyMu.Unlock()
		jsonResponse(w, 200, copy)
		return
	}
	if r.Method != http.MethodPost {
		s.historyMu.Unlock()
		apiError(w, 404, fmt.Errorf("nie zlecono historii"))
		return
	}
	// Bound retained jobs and concurrent explicit downloads.
	active := 0
	for key, job := range s.historyJobs {
		if job.Status == "running" {
			active++
		} else if len(s.historyJobs) >= 64 {
			delete(s.historyJobs, key)
		}
	}
	if active >= 2 {
		s.historyMu.Unlock()
		apiError(w, 429, fmt.Errorf("trwa pobieranie dwóch historii; spróbuj za chwilę"))
		return
	}
	job := &extendedHistoryJob{Status: "running"}
	s.historyJobs[key] = job
	s.historyWG.Add(1)
	s.historyMu.Unlock()
	go func() {
		defer s.historyWG.Done()
		defer func() {
			if value := recover(); value != nil {
				s.historyMu.Lock()
				job.Status = "error"
				job.Error = fmt.Sprintf("błąd pobierania historii: %v", value)
				s.historyMu.Unlock()
			}
		}()
		ctx, cancel := context.WithTimeout(s.ctx, 20*time.Minute)
		defer cancel()
		ctx = context.WithValue(ctx, archiveContextKey{}, archiveInfo{Bookmaker: "SOFASCORE", Match: p.Match, Scope: "historia-na-zadanie-" + strconv.Itoa(p.Bet.PlayerID), Automatic: true})
		client, err := newSofaClient(ctx, nil, nil)
		var h *PlayerStatHistory
		if err == nil {
			client.statsFixture = evidence.Fixture
			client.statsTeamID = p.Bet.TeamID
			h, err = fetchSofaPlayerCompleteHistory(client, p.Bet.PlayerID, limit, evidence.Request.TournamentIDs)
			err = errors.Join(err, client.archiveError())
			client.tlsClient.CloseIdleConnections()
		}
		s.historyMu.Lock()
		defer s.historyMu.Unlock()
		job.Status = "completed"
		job.History = h
		if err != nil {
			job.Status = "error"
			job.Error = err.Error()
		}
	}()
	jsonResponse(w, 202, extendedHistoryJob{Status: "running"})
}

// Refill only on explicit filter selection. Baseline forecasts and quotes stay
// immutable; the derived sample has its own persistent, versioned cache.
type opponentSampleJob struct {
	Status       string        `json:"status"`
	Mode         string        `json:"mode"`
	Target       int           `json:"target"`
	Done         int           `json:"done"`
	Total        int           `json:"total"`
	Message      string        `json:"message,omitempty"`
	Partial      bool          `json:"partial,omitempty"`
	Rows         []AnalyzedBet `json:"rows,omitempty"`
	Notes        []string      `json:"notes,omitempty"`
	Error        string        `json:"error,omitempty"`
	EvidencePath string        `json:"evidencePath,omitempty"`
}

func (s *calendarServer) opponentSample(w http.ResponseWriter, r *http.Request) {
	id, mode := r.PathValue("id"), r.URL.Query().Get("mode")
	if !regexp.MustCompile(`^[a-f0-9]{24}$`).MatchString(id) || (mode != "easier" && mode != "harder") {
		apiError(w, 400, fmt.Errorf("niepoprawne ID lub filtr historii"))
		return
	}
	key := researchHash([]string{"opponent-refill-v1", fairModelVersion, id, mode})
	rel := filepath.Join("matchup-samples", key+".json.gz")
	s.historyMu.Lock()
	if job := s.opponentJobs[key]; job != nil {
		copy := *job
		if copy.Status != "error" || r.Method != http.MethodPost {
			s.historyMu.Unlock()
			jsonResponse(w, 200, copy)
			return
		}
	}
	s.historyMu.Unlock()
	var saved opponentSampleJob
	if err := s.research.readDocument(rel, &saved); err == nil && saved.Status == "completed" {
		jsonResponse(w, 200, saved)
		return
	}
	if r.Method != http.MethodPost {
		apiError(w, 404, fmt.Errorf("nie zlecono uzupełnienia próbki"))
		return
	}
	s.historyMu.Lock()
	if s.opponentJobs == nil {
		s.opponentJobs = map[string]*opponentSampleJob{}
	}
	if job := s.opponentJobs[key]; job != nil && job.Status != "error" {
		copy := *job
		s.historyMu.Unlock()
		jsonResponse(w, 200, copy)
		return
	}
	active := 0
	for oldKey, job := range s.opponentJobs {
		if job.Status == "running" {
			active++
		} else if len(s.opponentJobs) >= 4 {
			delete(s.opponentJobs, oldKey)
		}
	}
	if s.ctx.Err() != nil || active >= 2 {
		s.historyMu.Unlock()
		apiError(w, 429, fmt.Errorf("trwa uzupełnianie dwóch próbek; spróbuj ponownie po ich zakończeniu"))
		return
	}
	job := &opponentSampleJob{Status: "running", Mode: mode, Message: "Przygotowywanie próbki z wybranym filtrem"}
	s.opponentJobs[key] = job
	s.historyWG.Add(1)
	s.historyMu.Unlock()
	go func() {
		defer s.historyWG.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				s.historyMu.Lock()
				job.Status, job.Error = "error", fmt.Sprintf("uzupełnianie próbki: %v", recovered)
				s.historyMu.Unlock()
			}
		}()
		ctx, cancel := context.WithTimeout(s.ctx, 20*time.Minute)
		defer cancel()
		ctx = context.WithValue(ctx, sofaManualContextKey{}, true)
		release, err := s.beginForeground(ctx)
		if err == nil {
			defer release()
		}
		result := opponentSampleJob{Mode: mode}
		if err == nil {
			result, err = s.buildOpponentSample(ctx, id, mode, func(done, total int, message string) {
				s.historyMu.Lock()
				job.Done, job.Total, job.Message = done, total, message
				s.historyMu.Unlock()
			})
		}
		if err == nil {
			result.Status = "completed"
			path := filepath.Join(s.research.dir, rel)
			var raw []byte
			raw, err = json.Marshal(result)
			if err == nil {
				err = os.MkdirAll(filepath.Dir(path), 0750)
			}
			if err == nil {
				err = writeGzip(path, raw)
			}
		}
		if err != nil {
			result.Status, result.Error = "error", err.Error()
			log.Printf("ERROR Uzupełnianie próbki id=%s mode=%s: %v", id, mode, err)
		}
		s.historyMu.Lock()
		*job = result
		s.historyMu.Unlock()
	}()
	jsonResponse(w, 202, opponentSampleJob{Status: "running", Mode: mode})
}

func subsetMetricHistory(hist *PlayerStatHistory, market CanonicalMarket, indexes []int) *PlayerStatHistory {
	result := *hist
	result.Matches = len(indexes)
	result.MetricValues = map[CanonicalMarket][]int{market: sampleIndexes(hist.MetricValues[market], indexes)}
	result.MetricValuesWithSuperSub = map[CanonicalMarket][]int{}
	if len(hist.MetricValuesWithSuperSub[market]) == len(hist.MetricValues[market]) {
		result.MetricValuesWithSuperSub[market] = sampleIndexes(hist.MetricValuesWithSuperSub[market], indexes)
	}
	result.MetricAvailability, result.SuperSubAvailability = nil, nil // Already masked by metricHistory.
	result.MatchContexts = sampleIndexes(hist.MatchContexts, indexes)
	result.EventIDs = sampleIndexes(hist.EventIDs, indexes)
	result.MatchStartedAt = sampleIndexes(hist.MatchStartedAt, indexes)
	result.Ratings, result.Minutes = sampleIndexes(hist.Ratings, indexes), sampleIndexes(hist.Minutes, indexes)
	result.Positions, result.Opponents = sampleIndexes(hist.Positions, indexes), sampleIndexes(hist.Opponents, indexes)
	result.Dates, result.MatchShots = sampleIndexes(hist.Dates, indexes), sampleIndexes(hist.MatchShots, indexes)
	return &result
}

func opponentSampleIndexes(hist *PlayerStatHistory, input fairModelInput, mode string, target int) []int {
	useForm := opponentUsesRecentStrength(hist, input)
	indexes := []int{}
	for i, match := range hist.MatchContexts {
		class, known := opponentMatchClass(match, input, useForm)
		if known && ((mode == "easier" && class < 0) || (mode == "harder" && class > 0)) {
			indexes = append(indexes, i)
			if len(indexes) == target {
				break
			}
		}
	}
	return indexes
}

func (s *calendarServer) buildOpponentSample(ctx context.Context, id, mode string, progress func(int, int, string)) (opponentSampleJob, error) {
	result := opponentSampleJob{Mode: mode, Rows: []AnalyzedBet{}, Notes: []string{}}
	db := s.research.summaryDB
	if db == nil {
		db = s.research.db
	}
	var path, algorithm, matchName string
	err := db.QueryRowContext(ctx, `SELECT evidence_path,algorithm,match_name FROM prediction_runs WHERE analysis_id=? LIMIT 1`, id).Scan(&path, &algorithm, &matchName)
	if err != nil {
		return result, fmt.Errorf("odczyt bazowej analizy (wymagany ukończony zapis): %w", err)
	}
	if algorithm != fairModelVersion {
		return result, fmt.Errorf("starsza wersja modelu; wykonaj nową analizę bazową")
	}
	var evidence predictionEvidence
	if err := s.research.readDocument(path, &evidence); err != nil {
		return result, err
	}
	if !time.Now().Before(evidence.Fixture.StartTime) {
		return result, fmt.Errorf("nową próbkę można uzupełniać przed rozpoczęciem meczu; zapisane próbki nadal można odczytać")
	}
	result.Target = evidence.Request.MatchLimit
	if result.Target < 1 || result.Target > 50 {
		return result, fmt.Errorf("niepoprawna docelowa liczba występów w analizie bazowej")
	}
	rows, err := db.QueryContext(ctx, `SELECT p.payload FROM predictions p JOIN prediction_runs r ON r.id=p.run_id WHERE r.analysis_id=? ORDER BY p.player_id,p.bet_key,r.bookmaker`, id)
	if err != nil {
		return result, err
	}
	byPlayer := map[int][]AnalyzedBet{}
	for rows.Next() {
		var raw string
		var prediction predictionRecord
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal([]byte(raw), &prediction)
		}
		if err != nil {
			break
		}
		byPlayer[prediction.Bet.PlayerID] = append(byPlayer[prediction.Bet.PlayerID], prediction.Bet)
	}
	err = errors.Join(err, rows.Err())
	rows.Close()
	if err != nil {
		return result, err
	}
	ctx = context.WithValue(ctx, archiveContextKey{}, archiveInfo{Bookmaker: "SOFASCORE", Match: matchName, AnalysisID: id, Scope: "probka-filtr-" + mode})
	client, err := newSofaClient(ctx, func(message string) { log.Printf("WARNING Próba z filtrem id=%s: %s", id, message) }, nil)
	if err != nil {
		return result, err
	}
	defer client.tlsClient.CloseIdleConnections()
	client.statsFixture = evidence.Fixture
	contextData := evidence.Context
	contextData.strengthLimit = 128
	histories := map[int]*PlayerStatHistory{}
	for _, hist := range append(append([]*PlayerStatHistory{}, evidence.HomePlayers...), evidence.AwayPlayers...) {
		histories[hist.ID] = hist
	}
	ids := []int{}
	for playerID := range byPlayer {
		ids = append(ids, playerID)
	}
	sort.Ints(ids)
	result.Total = len(ids)
	inputs := map[int]*PlayerStatHistory{}
	const scanLimit = 200
	for _, playerID := range ids {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		base := histories[playerID]
		if base == nil {
			return result, fmt.Errorf("brak historii zawodnika %d w danych analizy", playerID)
		}
		markets := map[CanonicalMarket]bool{}
		for _, bet := range byPlayer[playerID] {
			markets[bet.MarketType] = true
		}
		full := base
		scan := result.Target
		if full.Matches > scan {
			scan = full.Matches
		}
		limited := false
		for {
			contextData = extendOpponentStrengths(client, contextData, []*PlayerStatHistory{full})
			ready, minFound := true, result.Target
			for market := range markets {
				metric := metricHistory(full, market)
				n := len(opponentSampleIndexes(metric, contextData.Input, mode, result.Target))
				if n < minFound {
					minFound = n
				}
				if n < result.Target {
					ready = false
				}
			}
			progress(result.Done, result.Total, fmt.Sprintf("%s: wybrano co najmniej %d/%d pasujących występów · przejrzano %d", base.FullName, minFound, result.Target, full.Matches))
			if ready || full.HistoryExhausted {
				break
			}
			if scan >= scanLimit {
				limited = true
				break
			}
			scan += 10
			if scan > scanLimit {
				scan = scanLimit
			}
			playerClient := client.withScope("probka-filtr-" + mode + "-zawodnik-" + strconv.Itoa(playerID))
			playerClient.statsTeamID = base.TeamID
			next, fetchErr := fetchSofaPlayerCompleteHistory(playerClient, playerID, scan, evidence.Request.TournamentIDs)
			if fetchErr != nil {
				return result, fmt.Errorf("historia %s: %w", base.FullName, fetchErr)
			}
			if next == nil {
				return result, fmt.Errorf("brak rozszerzonej historii %s", base.FullName)
			}
			copy := *next
			copy.FullName, copy.TeamID, copy.TeamName, copy.TeamSide = base.FullName, base.TeamID, base.TeamName, base.TeamSide
			full = &copy
		}
		inputs[playerID] = full
		if full.Incomplete {
			result.Partial = true
		}
		type pricedMetric struct {
			hist *PlayerStatHistory
			data *OpponentFilterData
		}
		selected := map[CanonicalMarket]pricedMetric{}
		for market := range markets {
			metric := metricHistory(full, market)
			indexes := opponentSampleIndexes(metric, contextData.Input, mode, result.Target)
			data := opponentFilterForHistory(metric, market, 0, false, contextData)
			data.Easier, data.Harder = nil, nil
			data.TargetSample, data.ScannedMatches = result.Target, full.Matches
			if len(indexes) < result.Target {
				data.SampleNote = fmt.Sprintf("Znaleziono %d/%d pasujących występów dla rynku %s.", len(indexes), result.Target, market)
				if limited {
					data.SampleNote += " Osiągnięto limit przeglądania 200 występów."
				} else {
					data.SampleNote += " Wyczerpano dostępną historię."
				}
				if full.Incomplete {
					data.SampleNote += " Część danych źródłowych jest niepełna."
				}
				if len(contextData.strengthAttempts) >= contextData.strengthLimit {
					data.SampleNote += " Osiągnięto limit 128 nowych ocen drużyn; nieznana siła nie jest zastępowana zerem."
				}
				result.Notes = append(result.Notes, base.FullName+": "+data.SampleNote)
			}
			selected[market] = pricedMetric{subsetMetricHistory(metric, market, indexes), data}
		}
		type priceKey struct {
			market CanonicalMarket
			line   float64
			under  bool
		}
		models := map[priceKey]*FairModelDetails{}
		for _, baseBet := range byPlayer[playerID] {
			selection := selected[baseBet.MarketType]
			if selection.hist.Matches == 0 {
				continue
			}
			bet := baseBet
			bet.HistoryNotes = playerMetricHistoryNotes(full, bet.MarketType)
			bet.AnalysisID, bet.OpponentFilter = id, selection.data
			bet.MatchesSample = selection.hist.Matches
			bet.SampleValues = selection.hist.MetricValues[bet.MarketType]
			bet.SampleValuesSuperSub = selection.hist.MetricValuesWithSuperSub[bet.MarketType]
			bet.SampleMinutes, bet.SamplePositions = selection.hist.Minutes, selection.hist.Positions
			bet.SampleOpponents, bet.SampleDates = selection.hist.Opponents, selection.hist.Dates
			bet.SampleShots, bet.SampleRatings = selection.hist.MatchShots, selection.hist.Ratings
			key := priceKey{bet.MarketType, bet.Line, bet.IsUnder}
			model := models[key]
			if model == nil {
				model = contextualFairModel(selection.hist, bet.MarketType, bet.Line, bet.IsUnder, contextData.Input)
				model.Notes = append(model.Notes, selection.data.Basis, fmt.Sprintf("Próba z filtrem: %d/%d; starsze występy dobierane do docelowego limitu.", selection.hist.Matches, result.Target))
				if selection.data.SampleNote != "" {
					model.Notes = append(model.Notes, selection.data.SampleNote)
				}
				strengths := contextData.Input.Strengths
				if opponentUsesRecentStrength(metricHistory(full, bet.MarketType), contextData.Input) {
					strengths = contextData.Input.RecentStrengths
				}
				for _, match := range selection.hist.MatchContexts {
					if strengths[match.TeamID].Matches < 8 || strengths[match.OpponentID].Matches < 8 {
						model.Notes = append(model.Notes, "Co najmniej jedna ocena siły pochodzi z mniej niż 8 spotkań; podział może być niestabilny.")
						break
					}
				}
				models[key] = model
			}
			bet.FairModel, bet.MatchOverview = model, nil
			hits := 0
			for _, value := range bet.SampleValues {
				if (float64(value) > bet.Line) != bet.IsUnder {
					hits++
				}
			}
			bet.HitRate = math.Round(float64(hits)/float64(len(bet.SampleValues))*1000) / 10
			bet.FairOdds = math.Round(100/model.Probability) / 100
			bet.EdgePercent = math.Round((bet.BookmakerOdds*model.Probability-1)*1000) / 10
			ev := (bet.BookmakerOdds*.88*model.Probability - 1) * 100
			bet.EVWithTax, bet.Verdict = math.Round(ev*10)/10, "NEUTRAL"
			if ev > 0 {
				bet.Verdict = "VALUE"
			} else if ev < -15 {
				bet.Verdict = "TRAP"
			}
			result.Rows = append(result.Rows, bet)
		}
		result.Done++
		progress(result.Done, result.Total, "Uzupełnianie kolejnego zawodnika")
	}
	if err := errors.Join(ctx.Err(), client.archiveError()); err != nil {
		return result, err
	}
	result.EvidencePath, err = s.research.saveDocument("probki-z-filtrem", id+"-"+mode, map[string]any{"analysisId": id, "mode": mode, "target": result.Target, "baseEvidence": path, "context": contextData, "histories": inputs})
	return result, err
}

var identifierPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type extendedHistoryJob struct {
	Status  string             `json:"status"`
	History *PlayerStatHistory `json:"history,omitempty"`
	Error   string             `json:"error,omitempty"`
}

func (s *calendarServer) womenOffer(book, id string) bool {
	s.excludedMu.Lock()
	excluded := s.excludedOffers[book+":"+id]
	s.excludedMu.Unlock()
	if excluded {
		return true
	}
	s.cache.once.Do(s.cache.readDisk)
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	for key, entry := range s.cache.entries {
		if !strings.HasPrefix(key, strings.ToLower(book)+":") || entry.Calendar == nil {
			continue
		}
		for _, event := range entry.Calendar.Events {
			if event.ID == id && isWomenEvent(event) {
				return true
			}
		}
	}
	return false
}

var calendarArchiveMu sync.Mutex

func archiveDailyCalendar(info archiveInfo, decoded any) error {
	calendarArchiveMu.Lock()
	defer calendarArchiveMu.Unlock()
	dir := filepath.Join(archiveRoot, "kalendarze", polishDay(time.Now()), strings.ToLower(info.Bookmaker))
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	path := filepath.Join(dir, "kalendarz.json.gz")
	snapshots := map[string]json.RawMessage{}
	file, err := os.Open(path)
	if err == nil {
		reader, readErr := gzip.NewReader(file)
		if readErr == nil {
			readErr = json.NewDecoder(reader).Decode(&snapshots)
			reader.Close()
		}
		file.Close()
		if readErr != nil {
			return readErr
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if snapshots == nil {
		snapshots = map[string]json.RawMessage{}
	}
	raw, err := json.Marshal(decoded)
	if err != nil {
		return err
	}
	snapshots[info.Scope] = raw
	data, err := json.Marshal(snapshots)
	if err != nil {
		return err
	}
	return writeGzip(path, data)
}

func (s *calendarServer) rememberExcludedCalendar(book string, events []CalendarEvent) {
	s.excludedMu.Lock()
	defer s.excludedMu.Unlock()
	if s.excludedOffers == nil {
		s.excludedOffers = map[string]bool{}
	}
	changed := false
	for _, event := range events {
		key := book + ":" + event.ID
		if isWomenEvent(event) && !s.excludedOffers[key] {
			s.excludedOffers[key] = true
			changed = true
		}
	}
	if changed {
		if err := atomicJSON(filepath.Join(s.dataDir, "excluded_women_events.json"), s.excludedOffers); err != nil {
			log.Printf("EXCLUDED_OFFERS_WRITE_ERROR: %v", err)
		}
	}
}
