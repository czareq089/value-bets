// Command calendar probes bookmaker calendars without starting the desktop app.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"value-bets-3.0/internal/offering"
)

func main() {
	bookmaker := flag.String("bookmaker", "sts", "sts or betclic")
	date := flag.String("date", "", "calendar day in Europe/Warsaw (YYYY-MM-DD)")
	snapshot := flag.String("snapshot", "", "parse a saved STS i_pl snapshot instead of connecting")
	requestHex := flag.String("request-hex", "", "Betclic protobuf body as hex, WITHOUT its five-byte gRPC envelope")
	output := flag.String("output", "", "save STS source snapshot (bypasses cache) or Betclic calendar JSON (raw gRPC with -request-hex)")
	refresh := flag.Bool("refresh", false, "refresh the persistent calendar cache instead of reusing it")
	flag.Parse()
	zone, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		fail(err)
	}
	if *date == "" {
		*date = time.Now().In(zone).Format("2006-01-02")
	}
	if _, err := time.ParseInLocation("2006-01-02", *date, zone); err != nil {
		fail(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var raw []byte
	switch *bookmaker {
	case "sts":
		if *snapshot == "" && *output == "" {
			calendar, err := offering.FetchCachedSTSCalendar(ctx, offering.DefaultCalendarCache(), offering.STSWebSocket, *date, *refresh)
			if err != nil {
				fail(err)
			}
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			if err := encoder.Encode(calendar.Events); err != nil {
				fail(err)
			}
			return
		}
		if *snapshot != "" {
			raw, err = os.ReadFile(*snapshot)
		} else {
			raw, err = offering.FetchSTSSnapshot(ctx, offering.STSWebSocket, "")
		}
		if err != nil {
			fail(err)
		}
		if *output != "" {
			if err := os.WriteFile(*output, raw, 0600); err != nil {
				fail(err)
			}
		}
		events, err := offering.ParseSTSCalendar(raw, *date)
		if err != nil {
			fail(err)
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(events); err != nil {
			fail(err)
		}
	case "betclic":
		if *requestHex == "" {
			result, err := offering.FetchCachedBetclicCalendar(ctx, offering.DefaultCalendarCache(), http.DefaultClient,
				offering.BetclicMatchService+"GetMatchesBySportWithNotifications", *date, *refresh)
			if err != nil {
				fail(err)
			}
			data, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				fail(err)
			}
			if *output != "" {
				if err := os.WriteFile(*output, data, 0600); err != nil {
					fail(err)
				}
			}
			fmt.Println(string(data))
			return
		}
		if *output == "" {
			fail(fmt.Errorf("podaj -output, aby zapisać binarny snapshot Betclic do analizy"))
		}
		body, err := hex.DecodeString(*requestHex)
		if err != nil {
			fail(err)
		}
		raw, err = offering.FetchBetclicRPC(ctx, http.DefaultClient, offering.BetclicMatchService+"GetMatchesBySportWithNotifications", body)
		if err != nil {
			fail(err)
		}
		if err := os.WriteFile(*output, raw, 0600); err != nil {
			fail(err)
		}
		fmt.Printf("Zapisano %d bajtów gRPC-Web do %s. Schemat odpowiedzi kalendarza wymaga jeszcze weryfikacji.\n", len(raw), *output)
	default:
		fail(fmt.Errorf("nieobsługiwany bukmacher: %s", *bookmaker))
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
