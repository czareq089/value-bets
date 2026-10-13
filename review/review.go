// Independent review service. Reads Value Bets through HTTP; owns only assessments.
package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

//go:embed index.html
var reviewHTML []byte

var identifier = regexp.MustCompile(`^[a-f0-9]{64}$`)
var usernamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{2,32}$`)

type service struct {
	db            *sql.DB
	backend       string
	publicOrigin  string
	client        *http.Client
	loginMu       sync.Mutex
	loginAttempts map[string][]time.Time
	assignMu      sync.Mutex
}

type account struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Admin bool   `json:"admin"`
	CSRF  string `json:"csrf"`
}
type identityKey struct{}

type candidate struct {
	SchemaVersion int       `json:"schemaVersion"`
	ID            string    `json:"id"`
	RunID         string    `json:"runId"`
	BetKey        string    `json:"betKey"`
	Algorithm     string    `json:"algorithm"`
	Match         string    `json:"match"`
	FixtureID     string    `json:"fixtureId"`
	StartAt       time.Time `json:"startAt"`
	AnalyzedAt    time.Time `json:"analyzedAt"`
	ObservedAt    time.Time `json:"observedAt"`
	ExpiresAt     time.Time `json:"expiresAt"`
	Bet           struct {
		PlayerID      int     `json:"playerId"`
		PlayerName    string  `json:"playerName"`
		Bookmaker     string  `json:"bookmaker"`
		MarketType    string  `json:"marketType"`
		MarketLabel   string  `json:"marketLabel"`
		Line          float64 `json:"line"`
		IsUnder       bool    `json:"isUnder"`
		BookmakerOdds float64 `json:"bookmakerOdds"`
	} `json:"bet"`
}

type rating struct {
	SchemaVersion int       `json:"schemaVersion"`
	ID            int64     `json:"id"`
	UserID        int64     `json:"userId"`
	UserName      string    `json:"userName"`
	PredictionID  string    `json:"predictionId"`
	BetKey        string    `json:"betKey"`
	Rating        *int      `json:"rating"`
	Unknown       bool      `json:"unknown"`
	UsedExternal  *bool     `json:"usedExternal"`
	Reason        string    `json:"reason"`
	Sources       string    `json:"sources"`
	MinimumOdds   *float64  `json:"minimumOdds"`
	RevisionOf    int64     `json:"revisionOf"`
	At            time.Time `json:"at"`
}

func main() {
	listen := flag.String("listen", "127.0.0.1:8092", "lokalny adres panelu")
	backend := flag.String("backend", "http://127.0.0.1:8090", "API Value Bets")
	data := flag.String("data", "data", "osobny katalog ocen")
	public := flag.String("public-url", "", "publiczny HTTPS panelu, np. https://review.value-bets.pl")
	initUser := flag.String("init-user", "", "utwórz/zmień hasło użytkownika z REVIEW_PASSWORD i zakończ")
	admin := flag.Bool("admin", false, "uprawnienia eksportu ocen przy -init-user")
	flag.Parse()
	base, err := url.Parse(*backend)
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		log.Fatal("niepoprawny -backend")
	}
	publicOrigin := ""
	if *public != "" {
		u, err := url.Parse(*public)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			log.Fatal("-public-url wymaga adresu HTTPS bez ścieżki")
		}
		publicOrigin = "https://" + u.Host
	}
	if err := os.MkdirAll(*data, 0700); err != nil {
		log.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(*data, "reviews.sqlite"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`, `PRAGMA foreign_keys=ON`,
		`CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY,name TEXT NOT NULL UNIQUE,password_hash TEXT NOT NULL,admin INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS sessions(hash TEXT PRIMARY KEY,user_id INTEGER NOT NULL REFERENCES users(id),expires_at INTEGER NOT NULL,csrf TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS assignments(user_id INTEGER NOT NULL REFERENCES users(id),prediction_id TEXT NOT NULL,bet_key TEXT NOT NULL,fixture_id TEXT NOT NULL,player_id INTEGER NOT NULL,start_at INTEGER NOT NULL,assigned_at INTEGER NOT NULL,status TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(user_id,prediction_id))`,
		`CREATE INDEX IF NOT EXISTS assignments_user_key ON assignments(user_id,bet_key)`,
		`CREATE TABLE IF NOT EXISTS shared_pool(bet_key TEXT PRIMARY KEY,prediction_id TEXT NOT NULL,start_at INTEGER NOT NULL,payload TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS reviews(id INTEGER PRIMARY KEY,user_id INTEGER NOT NULL REFERENCES users(id),prediction_id TEXT NOT NULL,bet_key TEXT NOT NULL,created_at INTEGER NOT NULL,payload TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS reviews_user_prediction ON reviews(user_id,prediction_id,id)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			log.Fatal(err)
		}
	}
	if *initUser != "" {
		password := os.Getenv("REVIEW_PASSWORD")
		if !usernamePattern.MatchString(*initUser) || len(password) < 10 || len(password) > 72 {
			log.Fatal("nazwa: 2–32 znaki a-z/0-9/_/-; REVIEW_PASSWORD: 10–72 bajty")
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			log.Fatal(err)
		}
		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		if _, err = tx.Exec(`INSERT INTO users(name,password_hash,admin) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET password_hash=excluded.password_hash,admin=excluded.admin`, *initUser, string(hash), *admin); err != nil {
			_ = tx.Rollback()
			log.Fatal(err)
		}
		if _, err = tx.Exec(`DELETE FROM sessions WHERE user_id=(SELECT id FROM users WHERE name=?)`, *initUser); err != nil {
			_ = tx.Rollback()
			log.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			log.Fatal(err)
		}
		log.Printf("[REVIEW] stage=user-saved user=%q admin=%v", *initUser, *admin)
		return
	}
	s := &service{db: db, backend: strings.TrimRight(*backend, "/"), publicOrigin: publicOrigin, client: &http.Client{Timeout: 90 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}, loginAttempts: map[string][]time.Time{}}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Addr: *listen, Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 120 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	go func() {
		<-ctx.Done()
		end, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(end)
	}()
	log.Printf("[REVIEW] stage=listening listen=%s backend=%s data=%s", *listen, *backend, *data)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("[REVIEW] stage=response-error error=%v", err)
	}
}
func fail(w http.ResponseWriter, status int, err error) {
	respond(w, status, map[string]string{"error": err.Error()})
}
func decode(w http.ResponseWriter, r *http.Request, target any) error {
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
func randomToken() (string, error) {
	var raw [32]byte
	if _, err := cryptorand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", sum[:])
}
func who(r *http.Request) account { return r.Context().Value(identityKey{}).(account) }

func (s *service) backendGET(ctx context.Context, path string, target any) error {
	return s.backendRequest(ctx, http.MethodGet, path, target)
}

func (s *service) backendRequest(ctx context.Context, method, path string, target any) error {
	req, err := http.NewRequestWithContext(ctx, method, s.backend+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (64<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 64<<20 {
		return fmt.Errorf("odpowiedź API przekracza 64 MB")
	}
	if resp.StatusCode != 200 && resp.StatusCode != http.StatusAccepted {
		var problem struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(raw, &problem)
		if problem.Error == "" {
			problem.Error = resp.Status
		}
		return fmt.Errorf("Value Bets: %s", problem.Error)
	}
	return json.Unmarshal(raw, target)
}

func (s *service) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/login", s.login)
	mux.HandleFunc("GET /api/me", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, who(r)) })
	mux.HandleFunc("POST /api/logout", func(w http.ResponseWriter, r *http.Request) {
		cookie, _ := r.Cookie("review_session")
		if cookie != nil {
			if _, err := s.db.Exec(`DELETE FROM sessions WHERE hash=?`, tokenHash(cookie.Value)); err != nil {
				fail(w, 500, err)
				return
			}
		}
		http.SetCookie(w, &http.Cookie{Name: "review_session", Value: "", Path: "/", HttpOnly: true, Secure: s.publicOrigin != "", SameSite: http.SameSiteStrictMode, MaxAge: -1})
		respond(w, 200, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /api/next", s.next)
	mux.HandleFunc("POST /api/skip", s.skip)
	mux.HandleFunc("POST /api/rate", s.rate)
	mux.HandleFunc("GET /api/prediction/{id}", s.detail)
	mux.HandleFunc("POST /api/prediction/{id}/history", s.extendedHistory)
	mux.HandleFunc("GET /api/prediction/{id}/history", s.extendedHistory)
	mux.HandleFunc("GET /api/history", s.history)
	mux.HandleFunc("GET /api/reports", func(w http.ResponseWriter, r *http.Request) {
		var report any
		if err := s.backendGET(r.Context(), "/api/v1/research/reports?"+r.URL.RawQuery, &report); err != nil {
			fail(w, 502, err)
			return
		}
		respond(w, 200, report)
	})
	mux.HandleFunc("GET /api/progress", func(w http.ResponseWriter, r *http.Request) {
		var progress any
		if err := s.backendGET(r.Context(), "/api/v1/research/progress", &progress); err != nil {
			fail(w, 502, err)
			return
		}
		respond(w, 200, progress)
	})
	mux.HandleFunc("GET /api/flow", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("eventId")
		if _, err := strconv.ParseUint(id, 10, 64); err != nil {
			fail(w, 400, err)
			return
		}
		var flow any
		if err := s.backendGET(r.Context(), "/api/v1/match-flow?"+r.URL.Query().Encode(), &flow); err != nil {
			fail(w, 502, err)
			return
		}
		respond(w, 200, flow)
	})
	mux.HandleFunc("GET /api/export", s.export)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(reviewHTML)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
		if strings.HasPrefix(r.URL.Path, "/api/") {
			if r.Method != "GET" {
				expected := s.publicOrigin
				if expected == "" {
					expected = "http://" + r.Host
				}
				if r.Header.Get("Origin") != expected {
					fail(w, 403, fmt.Errorf("niezgodne pochodzenie żądania"))
					return
				}
			}
			if r.URL.Path != "/api/login" {
				cookie, err := r.Cookie("review_session")
				if err != nil {
					fail(w, 401, fmt.Errorf("zaloguj się"))
					return
				}
				var user account
				err = s.db.QueryRow(`SELECT u.id,u.name,u.admin,se.csrf FROM sessions se JOIN users u ON u.id=se.user_id WHERE se.hash=? AND se.expires_at>?`, tokenHash(cookie.Value), time.Now().Unix()).Scan(&user.ID, &user.Name, &user.Admin, &user.CSRF)
				if errors.Is(err, sql.ErrNoRows) {
					fail(w, 401, fmt.Errorf("sesja wygasła"))
					return
				}
				if err != nil {
					fail(w, 500, err)
					return
				}
				if r.Method != "GET" && r.Header.Get("X-Review-CSRF") != user.CSRF {
					fail(w, 403, fmt.Errorf("niepoprawna sesja formularza"))
					return
				}
				r = r.WithContext(context.WithValue(r.Context(), identityKey{}, user))
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *service) login(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Name     string `json:"name"`
		Password string `json:"password"`
	}
	if err := decode(w, r, &input); err != nil {
		fail(w, 400, err)
		return
	}
	if !usernamePattern.MatchString(input.Name) || len(input.Password) > 72 {
		fail(w, 401, fmt.Errorf("niepoprawny login lub hasło"))
		return
	}
	s.loginMu.Lock()
	recent := []time.Time{}
	for _, at := range s.loginAttempts[input.Name] {
		if time.Since(at) < 5*time.Minute {
			recent = append(recent, at)
		}
	}
	limited := len(recent) >= 10
	s.loginAttempts[input.Name] = append(recent, time.Now())
	if len(s.loginAttempts) > 1000 {
		for name, times := range s.loginAttempts {
			if len(times) > 0 && time.Since(times[len(times)-1]) > 5*time.Minute {
				delete(s.loginAttempts, name)
			}
		}
	}
	s.loginMu.Unlock()
	if limited {
		fail(w, 429, fmt.Errorf("zbyt wiele prób; spróbuj za kilka minut"))
		return
	}
	var user account
	var hash string
	err := s.db.QueryRow(`SELECT id,name,admin,password_hash FROM users WHERE name=?`, input.Name).Scan(&user.ID, &user.Name, &user.Admin, &hash)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		fail(w, 500, err)
		return
	}
	if err != nil || bcrypt.CompareHashAndPassword([]byte(hash), []byte(input.Password)) != nil {
		fail(w, 401, fmt.Errorf("niepoprawny login lub hasło"))
		return
	}
	token, err := randomToken()
	if err != nil {
		fail(w, 500, err)
		return
	}
	csrf, err := randomToken()
	if err != nil {
		fail(w, 500, err)
		return
	}
	if _, err := s.db.Exec(`DELETE FROM sessions WHERE expires_at<=?`, time.Now().Unix()); err != nil {
		fail(w, 500, err)
		return
	}
	if _, err := s.db.Exec(`INSERT INTO sessions VALUES(?,?,?,?)`, tokenHash(token), user.ID, time.Now().Add(30*24*time.Hour).Unix(), csrf); err != nil {
		fail(w, 500, err)
		return
	}
	user.CSRF = csrf
	http.SetCookie(w, &http.Cookie{Name: "review_session", Value: token, Path: "/", HttpOnly: true, Secure: s.publicOrigin != "", SameSite: http.SameSiteStrictMode, MaxAge: 30 * 24 * 3600})
	log.Printf("[REVIEW] stage=login user=%q", user.Name)
	respond(w, 200, user)
}

func (s *service) next(w http.ResponseWriter, r *http.Request) {
	user := who(r)
	s.assignMu.Lock()
	defer s.assignMu.Unlock()
	var existing string
	err := s.db.QueryRow(`SELECT prediction_id FROM assignments WHERE user_id=? AND status='assigned' AND start_at>? ORDER BY assigned_at,prediction_id LIMIT 1`, user.ID, time.Now().Unix()).Scan(&existing)
	if err == nil {
		respond(w, 200, map[string]any{"predictionId": existing})
		return
	}
	if !errors.Is(err, sql.ErrNoRows) {
		fail(w, 500, err)
		return
	}
	var candidates []candidate
	if err := s.backendGET(r.Context(), "/api/v1/research/candidates", &candidates); err != nil {
		fail(w, 502, err)
		return
	}
	rows, err := s.db.Query(`SELECT bet_key FROM assignments WHERE user_id=?`, user.ID)
	if err != nil {
		fail(w, 500, err)
		return
	}
	seen := map[string]bool{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			fail(w, 500, err)
			return
		}
		seen[key] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		fail(w, 500, err)
		return
	}
	// Randomize games first, then players and lines: a large offer cannot dominate.
	groups := map[string]map[int]map[string]candidate{}
	for _, p := range candidates {
		if seen[p.BetKey] || !p.StartAt.After(time.Now()) || !identifier.MatchString(p.ID) {
			continue
		}
		if groups[p.FixtureID] == nil {
			groups[p.FixtureID] = map[int]map[string]candidate{}
		}
		if groups[p.FixtureID][p.Bet.PlayerID] == nil {
			groups[p.FixtureID][p.Bet.PlayerID] = map[string]candidate{}
		}
		old, ok := groups[p.FixtureID][p.Bet.PlayerID][p.BetKey]
		if !ok || p.Bet.BookmakerOdds > old.Bet.BookmakerOdds {
			groups[p.FixtureID][p.Bet.PlayerID][p.BetKey] = p
		}
	}
	games := []string{}
	for id := range groups {
		games = append(games, id)
	}
	var shared []candidate
	sharedRows, err := s.db.Query(`SELECT sp.payload FROM shared_pool sp WHERE sp.start_at>? AND NOT EXISTS(SELECT 1 FROM assignments a WHERE a.user_id=? AND a.bet_key=sp.bet_key) ORDER BY sp.start_at LIMIT 1000`, time.Now().Unix(), user.ID)
	if err != nil {
		fail(w, 500, err)
		return
	}
	for sharedRows.Next() {
		var raw string
		if err := sharedRows.Scan(&raw); err != nil {
			sharedRows.Close()
			fail(w, 500, err)
			return
		}
		var p candidate
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			sharedRows.Close()
			fail(w, 500, err)
			return
		}
		shared = append(shared, p)
	}
	err = sharedRows.Err()
	sharedRows.Close()
	if err != nil {
		fail(w, 500, err)
		return
	}
	if len(games) == 0 && len(shared) == 0 {
		respond(w, 200, map[string]any{"predictionId": "", "message": "Brak kolejnych świeżych propozycji VALUE. Serwer uzupełnia analizy w tle.", "remaining": 0})
		return
	}
	var chosen candidate
	// A quarter of selections explicitly target a shared, independently rated offer.
	if len(shared) > 0 && (len(games) == 0 || rand.IntN(4) == 0) {
		chosen = shared[rand.IntN(len(shared))]
	} else {
		fixture := games[rand.IntN(len(games))]
		players := []int{}
		for id := range groups[fixture] {
			players = append(players, id)
		}
		player := players[rand.IntN(len(players))]
		types := []candidate{}
		for _, p := range groups[fixture][player] {
			types = append(types, p)
		}
		chosen = types[rand.IntN(len(types))]
	}
	// Both reviewers receive the same frozen offer for the chosen underlying bet.
	var pooled string
	err = s.db.QueryRow(`SELECT payload FROM shared_pool WHERE bet_key=? AND start_at>?`, chosen.BetKey, time.Now().Unix()).Scan(&pooled)
	if err == nil {
		if err := json.Unmarshal([]byte(pooled), &chosen); err != nil {
			fail(w, 500, err)
			return
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		fail(w, 500, err)
		return
	}
	raw, err := json.Marshal(chosen)
	if err != nil {
		fail(w, 500, err)
		return
	}
	tx, err := s.db.Begin()
	if err != nil {
		fail(w, 500, err)
		return
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT OR IGNORE INTO shared_pool VALUES(?,?,?,?)`, chosen.BetKey, chosen.ID, chosen.StartAt.Unix(), string(raw)); err != nil {
		fail(w, 500, err)
		return
	}
	if _, err := tx.Exec(`INSERT INTO assignments VALUES(?,?,?,?,?,?,?,?,?)`, user.ID, chosen.ID, chosen.BetKey, chosen.FixtureID, chosen.Bet.PlayerID, chosen.StartAt.Unix(), time.Now().Unix(), "assigned", string(raw)); err != nil {
		fail(w, 500, err)
		return
	}
	if err := tx.Commit(); err != nil {
		fail(w, 500, err)
		return
	}
	log.Printf("[REVIEW] stage=assigned user=%q prediction=%s fixture=%s remaining_games=%d", user.Name, chosen.ID, chosen.FixtureID, len(games))
	respond(w, 200, map[string]any{"predictionId": chosen.ID, "remainingGames": len(games)})
}

func (s *service) skip(w http.ResponseWriter, r *http.Request) {
	var input struct {
		PredictionID string `json:"predictionId"`
	}
	if err := decode(w, r, &input); err != nil {
		fail(w, 400, err)
		return
	}
	result, err := s.db.Exec(`UPDATE assignments SET status='skipped' WHERE user_id=? AND prediction_id=? AND status='assigned'`, who(r).ID, input.PredictionID)
	if err != nil {
		fail(w, 500, err)
		return
	}
	count, err := result.RowsAffected()
	if err != nil {
		fail(w, 500, err)
		return
	}
	if count == 0 {
		fail(w, 409, fmt.Errorf("propozycja nie jest przypisana do oceny"))
		return
	}
	respond(w, 200, map[string]bool{"ok": true})
}

func (s *service) rate(w http.ResponseWriter, r *http.Request) {
	var input struct {
		PredictionID string   `json:"predictionId"`
		Rating       *int     `json:"rating"`
		Unknown      bool     `json:"unknown"`
		UsedExternal *bool    `json:"usedExternal"`
		Reason       string   `json:"reason"`
		Sources      string   `json:"sources"`
		MinimumOdds  *float64 `json:"minimumOdds"`
	}
	if err := decode(w, r, &input); err != nil {
		fail(w, 400, err)
		return
	}
	if !identifier.MatchString(input.PredictionID) || input.UsedExternal == nil || len(input.Reason) > 8000 || len(input.Sources) > 4000 || input.Unknown == (input.Rating != nil) || (input.Rating != nil && (*input.Rating < 1 || *input.Rating > 5)) || (input.MinimumOdds != nil && (math.IsNaN(*input.MinimumOdds) || *input.MinimumOdds <= 1 || *input.MinimumOdds > 1000)) {
		fail(w, 400, fmt.Errorf("wybierz ocenę 1–5 albo nie wiem oraz korzystanie z danych zewnętrznych; sprawdź długość notatki i kurs"))
		return
	}
	user := who(r)
	tx, err := s.db.Begin()
	if err != nil {
		fail(w, 500, err)
		return
	}
	defer tx.Rollback()
	var key string
	var start int64
	if err := tx.QueryRow(`SELECT bet_key,start_at FROM assignments WHERE user_id=? AND prediction_id=? AND status IN ('assigned','rated')`, user.ID, input.PredictionID).Scan(&key, &start); err != nil {
		fail(w, 404, fmt.Errorf("nie znaleziono przydzielonej propozycji"))
		return
	}
	if time.Now().Unix() >= start {
		fail(w, 409, fmt.Errorf("mecz już się rozpoczął — ocena przedmeczowa jest zamknięta"))
		return
	}
	value := rating{SchemaVersion: 1, UserID: user.ID, UserName: user.Name, PredictionID: input.PredictionID, BetKey: key, Rating: input.Rating, Unknown: input.Unknown, UsedExternal: input.UsedExternal, Reason: strings.TrimSpace(input.Reason), Sources: strings.TrimSpace(input.Sources), MinimumOdds: input.MinimumOdds, At: time.Now().UTC()}
	if err := tx.QueryRow(`SELECT COALESCE(MAX(id),0) FROM reviews WHERE user_id=? AND prediction_id=?`, user.ID, input.PredictionID).Scan(&value.RevisionOf); err != nil {
		fail(w, 500, err)
		return
	}
	raw, err := json.Marshal(value)
	if err != nil {
		fail(w, 500, err)
		return
	}
	result, err := tx.Exec(`INSERT INTO reviews(user_id,prediction_id,bet_key,created_at,payload) VALUES(?,?,?,?,?)`, user.ID, input.PredictionID, key, value.At.Unix(), string(raw))
	if err != nil {
		fail(w, 500, err)
		return
	}
	value.ID, err = result.LastInsertId()
	if err != nil {
		fail(w, 500, err)
		return
	}
	raw, err = json.Marshal(value)
	if err != nil {
		fail(w, 500, err)
		return
	}
	if _, err = tx.Exec(`UPDATE reviews SET payload=? WHERE id=?`, string(raw), value.ID); err != nil {
		fail(w, 500, err)
		return
	}
	if _, err = tx.Exec(`UPDATE assignments SET status='rated' WHERE user_id=? AND prediction_id=?`, user.ID, input.PredictionID); err != nil {
		fail(w, 500, err)
		return
	}
	if err := tx.Commit(); err != nil {
		fail(w, 500, err)
		return
	}
	log.Printf("[REVIEW] stage=rated user=%q prediction=%s revision=%d external=%v unknown=%v", user.Name, value.PredictionID, value.ID, *value.UsedExternal, value.Unknown)
	respond(w, 200, value)
}

func (s *service) detail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !identifier.MatchString(id) {
		fail(w, 400, fmt.Errorf("niepoprawny identyfikator"))
		return
	}
	user := who(r)
	var assigned int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM assignments WHERE user_id=? AND prediction_id=?`, user.ID, id).Scan(&assigned); err != nil {
		fail(w, 500, err)
		return
	}
	if assigned == 0 {
		fail(w, 404, fmt.Errorf("propozycja nie jest przydzielona"))
		return
	}
	var detail map[string]json.RawMessage
	if err := s.backendGET(r.Context(), "/api/v1/research/predictions/"+id, &detail); err != nil {
		fail(w, 502, err)
		return
	}
	var ownRaw string
	err := s.db.QueryRow(`SELECT payload FROM reviews WHERE user_id=? AND prediction_id=? ORDER BY id DESC LIMIT 1`, user.ID, id).Scan(&ownRaw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		fail(w, 500, err)
		return
	}
	peers := []json.RawMessage{}
	if err == nil {
		detail["ownReview"] = json.RawMessage(ownRaw)
		rows, err := s.db.Query(`SELECT r.payload FROM reviews r WHERE r.prediction_id=? AND r.user_id<>? AND r.id=(SELECT MAX(r2.id) FROM reviews r2 WHERE r2.user_id=r.user_id AND r2.prediction_id=r.prediction_id)`, id, user.ID)
		if err != nil {
			fail(w, 500, err)
			return
		}
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				rows.Close()
				fail(w, 500, err)
				return
			}
			peers = append(peers, json.RawMessage(raw))
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			fail(w, 500, err)
			return
		}
	}
	detail["peerReviews"], err = json.Marshal(peers)
	if err != nil {
		fail(w, 500, err)
		return
	}
	respond(w, 200, detail)
}

func (s *service) history(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(`SELECT a.payload,r.payload FROM assignments a JOIN reviews r ON r.user_id=a.user_id AND r.prediction_id=a.prediction_id WHERE a.user_id=? AND r.id=(SELECT MAX(r2.id) FROM reviews r2 WHERE r2.user_id=r.user_id AND r2.prediction_id=r.prediction_id) ORDER BY r.id DESC LIMIT 200`, who(r).ID)
	if err != nil {
		fail(w, 500, err)
		return
	}
	defer rows.Close()
	items := []map[string]json.RawMessage{}
	for rows.Next() {
		var p, value string
		if err := rows.Scan(&p, &value); err != nil {
			fail(w, 500, err)
			return
		}
		items = append(items, map[string]json.RawMessage{"prediction": json.RawMessage(p), "review": json.RawMessage(value)})
	}
	if err := rows.Err(); err != nil {
		fail(w, 500, err)
		return
	}
	respond(w, 200, items)
}

func (s *service) export(w http.ResponseWriter, r *http.Request) {
	if !who(r).Admin {
		fail(w, 403, fmt.Errorf("eksport wymaga konta administratora"))
		return
	}
	kind := r.URL.Query().Get("kind")
	if kind == "ratings" {
		rows, err := s.db.Query(`SELECT payload FROM reviews ORDER BY id`)
		if err != nil {
			fail(w, 500, err)
			return
		}
		defer rows.Close()
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		w.Header().Set("Content-Disposition", `attachment; filename="oceny-ekspertow_`+time.Now().UTC().Format("2006-01-02")+`.jsonl"`)
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				log.Printf("[REVIEW-EXPORT] stage=failed error=%v", err)
				return
			}
			if _, err := io.WriteString(w, raw+"\n"); err != nil {
				return
			}
		}
		if err := rows.Err(); err != nil {
			log.Printf("[REVIEW-EXPORT] stage=failed error=%v", err)
		}
		return
	}
	if kind != "predictions" && kind != "outcomes" && kind != "inputs" {
		fail(w, 400, fmt.Errorf("nieznany rodzaj eksportu"))
		return
	}
	query := url.Values{"kind": {kind}, "days": {r.URL.Query().Get("days")}}
	if query.Get("days") == "" {
		query.Set("days", "30")
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, s.backend+"/api/v1/research/export?"+query.Encode(), bytes.NewReader(nil))
	if err != nil {
		fail(w, 500, err)
		return
	}
	resp, err := s.client.Do(req)
	if err != nil {
		fail(w, 502, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != http.StatusAccepted {
		fail(w, 502, fmt.Errorf("eksport API: %s", resp.Status))
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Content-Disposition", resp.Header.Get("Content-Disposition"))
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("[REVIEW-EXPORT] stage=failed kind=%s error=%v", kind, err)
	}
}

func (s *service) extendedHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if !identifier.MatchString(id) || err != nil || limit < 10 || limit > 50 {
		fail(w, 400, fmt.Errorf("limit historii: 10–50"))
		return
	}
	var assigned int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM assignments WHERE user_id=? AND prediction_id=?`, who(r).ID, id).Scan(&assigned); err != nil {
		fail(w, 500, err)
		return
	}
	if assigned == 0 {
		fail(w, 404, fmt.Errorf("propozycja nie jest przydzielona"))
		return
	}
	var result any
	if err := s.backendRequest(r.Context(), r.Method, "/api/v1/research/predictions/"+id+"/history?limit="+strconv.Itoa(limit), &result); err != nil {
		fail(w, 502, err)
		return
	}
	respond(w, 200, result)
}
