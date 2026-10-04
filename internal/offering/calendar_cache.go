package offering

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const CalendarCacheTTL = 2 * time.Hour
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
		if entry.Calendar != nil && !entry.FetchedAt.IsZero() && !entry.FetchedAt.After(now) && now.Sub(entry.FetchedAt) < CalendarCacheTTL {
			entry.Calendar.FetchedAt = entry.FetchedAt.Format(time.RFC3339Nano)
			cache.entries[key] = entry
		}
	}
	log.Printf("[CALENDAR-CACHE] DISK_LOADED path=%q fresh_entries=%d", cache.path, len(cache.entries))
}

// Caller cancellation stops waiting without aborting a fetch shared by others.
// Errors never replace the last successfully downloaded calendar.
func (cache *CalendarCache) Get(ctx context.Context, key string, force bool, load func(context.Context) (*Calendar, error)) (*Calendar, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cache.once.Do(cache.readDisk)
	cache.mu.Lock()
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
			if now.Sub(entry.FetchedAt) >= CalendarCacheTTL || entry.FetchedAt.After(now) {
				delete(cache.entries, oldKey)
			}
		}
		cache.entries[key] = calendarCacheEntry{FetchedAt: now, Calendar: calendar}
		if diskErr := cache.writeDisk(); diskErr != nil {
			log.Printf("[CALENDAR-CACHE] WRITE_ERROR path=%q error=%v", cache.path, diskErr)
			calendar.Warnings = append(calendar.Warnings, "Nie zapisano kalendarza na dysku; cache działa do zamknięcia aplikacji: "+diskErr.Error())
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
	for _, event := range source.Events {
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
