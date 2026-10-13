package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExplicitHistoryProxyAcceptsAsyncJobAndChecksAssignment(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/history") || r.URL.Query().Get("limit") != "50" {
			t.Errorf("wrong backend request: %s %s", r.Method, r.URL)
		}
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprint(w, `{"status":"running"}`)
	}))
	defer backend.Close()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE assignments(user_id INTEGER,prediction_id TEXT)`); err != nil {
		t.Fatal(err)
	}
	s := &service{db: db, backend: backend.URL, client: backend.Client()}
	id := strings.Repeat("a", 64)
	request := func() *http.Request {
		r := httptest.NewRequest("POST", "/api/prediction/"+id+"/history?limit=50", nil)
		r.SetPathValue("id", id)
		return r.WithContext(context.WithValue(r.Context(), identityKey{}, account{ID: 1}))
	}
	w := httptest.NewRecorder()
	s.extendedHistory(w, request())
	if w.Code != 404 {
		t.Fatalf("unassigned history allowed: %d", w.Code)
	}
	if _, err = db.Exec(`INSERT INTO assignments VALUES(1,?)`, id); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	s.extendedHistory(w, request())
	if w.Code != 200 || !strings.Contains(w.Body.String(), "running") {
		t.Fatalf("async proxy failed: %d %s", w.Code, w.Body.String())
	}
}
