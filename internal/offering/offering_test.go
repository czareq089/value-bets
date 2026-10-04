package offering

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func frame(flags byte, body []byte) []byte {
	header := make([]byte, 5)
	header[0] = flags
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
	return append(header, body...)
}

func TestBetclicRPCPreservesRequestAndReadsFragmentedFrames(t *testing.T) {
	body := append(ProtoUint(1, 123), ProtoString(2, "pl")...)
	data := frame(0, []byte{8, 123})
	trailers := frame(0x80, []byte("grpc-status: 0\r\n"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, _ := io.ReadAll(r.Body)
		if r.Method != "POST" || r.URL.Path != "/GetMatchesBySportWithNotifications" || !bytes.Equal(request, frame(0, body)) {
			t.Errorf("wrong RPC request: %s %s %x", r.Method, r.URL.Path, request)
		}
		for name, expected := range map[string]string{"Content-Type": "application/grpc-web+proto", "X-Bg-Ref-Brand": "BETCLIC", "X-Bg-Regulation": "PL", "X-Grpc-Web": "1"} {
			if r.Header.Get(name) != expected {
				t.Errorf("wrong %s header", name)
			}
		}
		w.Header().Set("Content-Type", "application/grpc-web+proto")
		for _, b := range append(data, trailers...) {
			_, _ = w.Write([]byte{b})
			w.(http.Flusher).Flush()
		}
	}))
	defer server.Close()
	got, err := FetchBetclicRPC(context.Background(), server.Client(), server.URL+"/GetMatchesBySportWithNotifications", body)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("snapshot=%x error=%v", got, err)
	}
}

func TestBetclicSnapshotRejectsGRPCErrorsAndTruncation(t *testing.T) {
	for _, tt := range []struct {
		name      string
		raw       []byte
		errorText string
	}{
		{"grpc error", frame(0x80, []byte("grpc-status: 3\r\ngrpc-message: Invalid%20sport\r\n")), "Invalid sport"},
		{"truncated first", []byte{0, 0, 0}, "unexpected EOF"},
		{"truncated after data", append(frame(0, []byte{8, 1}), []byte{0, 0, 0}...), "unexpected EOF"},
		{"compressed", frame(1, []byte{8, 1}), "flagi"},
		{"empty", nil, "EOF"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readBetclicSnapshot(context.Background(), io.NopCloser(bytes.NewReader(tt.raw)), time.Second)
			if err == nil || got != nil || !strings.Contains(err.Error(), tt.errorText) {
				t.Fatalf("got=%x error=%v", got, err)
			}
		})
	}
}

func TestBetclicSnapshotIdleAndCancellation(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			reader, writer := io.Pipe()
			defer writer.Close()
			data := frame(0, []byte{8, 1})
			go func() {
				_, _ = writer.Write(data)
				if partial {
					_, _ = writer.Write([]byte{0, 0})
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := readBetclicSnapshot(ctx, reader, 40*time.Millisecond)
			if partial {
				if !errors.Is(err, io.ErrUnexpectedEOF) || got != nil {
					t.Fatalf("partial frame accepted: %x %v", got, err)
				}
			} else if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("idle snapshot: %x %v", got, err)
			}
		})
	}
	reader, writer := io.Pipe()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readBetclicSnapshot(ctx, reader, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSTSSnapshotSelectsRequestedChannel(t *testing.T) {
	for _, fixtureID := range []string{"", "123"} {
		t.Run("fixture="+fixtureID, func(t *testing.T) {
			upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "https://www.sts.pl" }}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				var sub struct {
					Type     int `json:"t"`
					Channels []struct {
						Name string `json:"s"`
					} `json:"u"`
				}
				if conn.ReadJSON(&sub) != nil {
					return
				}
				if sub.Type != 1 || sub.Channels[0].Name != "i_pl" {
					t.Errorf("wrong subscription: %+v", sub)
				}
				calendar := `{"B":{"S":{"1":{"n":"Piłka Nożna","C":{}}}},"P":{"unrelated":{}}}`
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"s":"i_pl","f":1}`+"\n"+calendar))
				if fixtureID != "" {
					if len(sub.Channels) != 2 || sub.Channels[1].Name != "f_f123_pl" {
						t.Errorf("missing fixture subscription")
					}
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"s":"f_f123_pl","f":1}`))
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"P":{"1m456":{"f":"f123","m":{"2003":{}}}}}`))
				}
			}))
			defer server.Close()
			got, err := FetchSTSSnapshot(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), fixtureID)
			if err != nil {
				t.Fatal(err)
			}
			var result map[string]json.RawMessage
			if json.Unmarshal(got, &result) != nil {
				t.Fatalf("not JSON: %s", got)
			}
			if fixtureID == "" && !hasSTSCalendar(result) {
				t.Fatalf("missing calendar")
			}
			if fixtureID != "" && !hasSTSFixtureMarkets(result["P"], fixtureID) {
				t.Fatalf("calendar mistaken for fixture markets: %s", got)
			}
		})
	}
}

func TestSTSCalendarFiltersAndSortsDST(t *testing.T) {
	raw := []byte(`{"s":"i_pl","f":1}
{"B":{"S":{"1":{"C":{"1":{"n":"Anglia","T":{"17":{"n":"Premier League","FX":{
"f1":{"ft":"m","H":{"n":" A "},"A":{"n":"B"},"a":{"1m22":1},"t":"2026-10-25T00:30:00Z","Bb":true},
"f2":{"ft":"m","H":{"n":"C"},"A":{"n":"D"},"t":"2026-10-25T01:15:00Z"},
"f3":{"ft":"s","t":"2026-10-25T00:00:00Z"},
"f4":{"ft":"m","H":{"n":"E"},"A":{"n":"F"},"t":"2026-10-25T23:00:00Z"},
"f5":{"ft":"m","H":{"n":"G"},"A":{"n":"H"},"t":"2026-10-24T22:00:00Z"},
"f6":{"ft":"m","H":{"n":"I"},"A":{"n":"J"},"t":"bad date"}
}}}}}}}},"Y":{"f1":{}}}`)
	events, err := ParseSTSCalendar(raw, "2026-10-25")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].ID != "5" || events[1].ID != "1" || events[2].ID != "2" {
		t.Fatalf("wrong calendar: %+v", events)
	}
	if event := events[1]; event.Home != "A" || event.Away != "B" || !event.Live || !event.BetBuilder || event.Competition != "Premier League" || event.Country != "Anglia" {
		t.Fatalf("wrong metadata: %+v", event)
	}
	if _, err := ParseSTSCalendar(raw, "2026-02-30"); err == nil {
		t.Fatal("invalid date accepted")
	}
	if _, err := ParseSTSCalendar([]byte(`{"recommendations":[]}`), "2026-10-25"); err == nil {
		t.Fatal("recommendations accepted as a calendar")
	}
}
