# Value Bets — desktop i VPS

Gałąź `split`: kalendarze STS, Betclic i Superbet pobiera serwer Go.
Desktop Wails łączy się automatycznie z **http://130.61.39.101:8090** po otwarciu
aplikacji. Kliknięcie meczu uzupełnia linki z kalendarzy VPS. Adres jest stałą
`serverBaseURL` w kodzie desktopu; aplikacja nie wymaga ustawień, tokena ani
uruchamiania tunelu SSH. API działa bez uwierzytelniania.

Statystyki, kursy do analizy i sama analiza pozostają na tym etapie lokalne.

Kod desktopu jest w głównym `value_bets.go`, frontend w `frontend/index.html`,
a istniejące testy w `value_bets_test.go`. Nie ma osobnych pakietów aplikacji
w `internal/` ani narzędzi w `cmd/`. Desktop budujesz przez `wails build`
z głównego katalogu. Pliki `frontend/wailsjs/` generuje Wails; frontend korzysta
bezpośrednio z `window.go`.

Lokalne zakłady, historia i cache są zachowane w `data/`, poza Gitem.
W `build/` pozostają ikony i konfiguracja Wails; wygenerowane binarki są pomijane.
Cały kod backendu jest w `server/value_bets.go`, z własnymi `server/go.mod`
i `server/go.sum`. Na VPS trafiają wyłącznie te trzy źródłowe pliki oraz binarka.

## Automatyczne wdrażanie

Workflow: `.github/workflows/deploy.yml`. Uruchamia się po wysłaniu zmian
z `split` do GitHuba; nie ma filtra ścieżek, więc także zmiana samego desktopu
wyzwala wdrożenie. GitHub nie ma zdarzenia dla lokalnego `git commit`.
Commit musi najpierw znaleźć się w zdalnym repozytorium poprzez `git push`.
[Dokumentacja zdarzeń GitHub Actions](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#push).

W repozytorium `czareq089/value-bets` ustaw te same nazwy sekretów jak w FOSS:

| Sekret | Wartość |
| --- | --- |
| `VPS_HOST` | `130.61.39.101` |
| `VPS_USER` | `ubuntu` |
| `VPS_SSH_KEY` | Prywatny klucz SSH używany do wdrożeń na ten VPS |

Sekrety służą połączeniu GitHub → VPS; aplikacja desktopowa ich nie używa.
Jeżeli są zapisane tylko w repozytorium FOSS, trzeba ustawić je również tutaj.
VPS musi mieć Go **1.26.0 lub nowszy** dostępne przez PATH, `/usr/local/go/bin`
lub `/home/ubuntu/go/bin`. Użytkownik `ubuntu` musi móc wykonać polecenia `sudo`
użyte we workflow bez interaktywnego hasła, tak jak w dotychczasowym wdrażaniu.

Workflow:

1. Pobiera kod commita z GitHuba.
2. Przesyła trzy pliki `server/` do
   `/home/ubuntu/value-bets/releases/<commit>-<run>-<attempt>/`.
3. Buduje program **na VPS**, dla jego natywnej architektury, z `CGO_ENABLED=0`.
4. Instaluje jednostkę `value-bets.service`, podmienia binarkę i restartuje usługę.

Nieudana kompilacja kończy wdrożenie przed podmianą programu i konfiguracji
usługi. Wdrożenia są serializowane. `data/`, archiwum i cache pozostają na VPS
niezależnie od wersji programu. Źródła i binarka każdego wdrożenia zostają
w osobnym katalogu `releases/`.

W workflow jest też `workflow_dispatch`. Ręczny przycisk GitHuba jest dostępny,
gdy definicja workflow istnieje na domyślnej gałęzi repozytorium; uruchomienie
musi wskazywać `split`, ponieważ job odrzuca inne gałęzie. Nie trzeba zmieniać
lokalnej gałęzi, aby zwykłe wysłanie `split` uruchamiało automatyczne wdrożenie.

## Ręczne uruchomienie na Ubuntu

Przed pierwszym wdrożeniem można nadal skopiować trzy pliki z `server/`
do `/home/ubuntu/value-bets` i wykonać:

```bash
cd /home/ubuntu/value-bets
go build -o value_bets value_bets.go
./value_bets -listen 0.0.0.0:8090 -data /home/ubuntu/value-bets/data
```

Dostęp przychodzący do **TCP 8090** musi być dopuszczony w firewallu VPS
oraz w regułach sieci dostawcy. Jeśli korzystasz z aktywnego UFW:

```bash
sudo ufw allow 8090/tcp
```

Po wdrożeniu Actions usługą zarządza systemd:

```bash
sudo systemctl status value-bets
sudo systemctl restart value-bets
journalctl -u value-bets -f
```

Jeśli wcześniej utworzyłeś usługę według instrukcji z tokenem, pierwsze nowe
wdrożenie nadpisze ją konfiguracją bez `EnvironmentFile` i z portem 8090.
Stare `server.env` oraz ustawienia połączenia Wails nie są już odczytywane.
Przed przejściem z uruchomienia w terminalu na systemd zatrzymaj ten proces
przez Ctrl+C, aby zwolnić port.

## Pobieranie, cache i API

- STS i Betclic: jeden zapis oferty piłki nożnej obejmujący wszystkie dostępne
  daty. Zmiana daty w Wails filtruje zapis i nie pobiera całej oferty ponownie.
- Superbet: zapis całego dnia, ponieważ istniejący kolektor pracuje według daty.
  Harmonogram pobiera dziś i jutro, aby obejmować najbliższe 24 godziny.
  Pierwsze wejście na inny dzień może jednorazowo pobrać brakujący zapis.
  Zapisane przyszłe dni również trafiają do kolejnych odświeżeń tego źródła.
- Każdy bukmacher ma osobny termin: 2 godziny ± losowe 10 minut. Terminy są
  zapisane na dysku i przetrwają restart. Nieudane pobranie ponawia się po około
  5 minutach, poprzedni cache pozostaje dostępny z ostrzeżeniem.
- Równoczesne żądania tego samego źródła dołączają do jednego pobierania.
  Ręczne odświeżenia w ciągu 30 sekund od udanego pobrania używają zapisu.
- Zwykły odczyt zwraca także starszy zapis; sam odczyt nie uruchamia odświeżenia
  istniejących danych. Kalendarz zawiera `fetchedAt` i ostrzeżenia o błędach.
- Limit to trzy równoległe pobrania kalendarzy. Parser Betclic zachowuje istniejące
  dwa równoległe żądania stron. Wails przechowuje odpowiedzi API przez minutę;
  trwały cache oferty jest na VPS.
- Obsługiwane daty API: od 7 dni wstecz do 60 dni naprzód. Archiwum nie jest
  objęte tym limitem ani automatycznie usuwane.

API jest dostępne pod `http://130.61.39.101:8090`, bez tokena:

| Metoda | Endpoint | Znaczenie |
| --- | --- | --- |
| GET | `/api/v1/health` | Wersja API, terminy i błędy kolektorów |
| GET | `/api/v1/calendars?bookmaker=STS&date=2026-10-05` | Kalendarz wybranego dnia z cache |
| POST | `/api/v1/calendars/refresh` | Ręczne odświeżenie z body `{"bookmaker":"STS","date":"2026-10-05"}` |

Nazwy bukmacherów: `STS`, `BETCLIC`, `SUPERBET`. Odpowiedź kalendarza zachowuje
dotychczasowy format desktopu: `date`, `events`, `warnings`, `fetchedAt`.
Odświeżenie odpowiada po pobraniu, a nie przez osobne zadania do odpytywania.

## Czytelne archiwum

```text
data/
  bookmaker_calendar_cache.json
  calendar_schedule.json
  archive/
    2026-10-04/
      sts/
      betclic/
      superbet/
```

Każdy odczyt odpowiedzi zapisuje surowe bajty przed parsowaniem oraz towarzyszący
czytelny, wcięty JSON. Nazwy określają bukmachera, zawartość, zakres/dzień,
rodzaj odpowiedzi, czas UTC z nanosekundami i licznik. Na przykład:

```text
betclic_kalendarz-pilka-nozna-wszystkie-daty-strona-offset-40_ramka-grpc_20261004T212458.517673000Z_000012.json.gz
betclic_kalendarz-pilka-nozna-wszystkie-daty-strona-offset-40_ramka-grpc_20261004T212458.517673000Z_000012.grpc.bin.gz
superbet_kalendarz-pilka-nozna-dzien-2026-10-05_odczytane-mecze-liczba-120_20261004T212500.000000000Z_000030.json.gz
```

JSON zawiera czas zapisu, wersję formatu, endpoint, status HTTP, body Protobuf
w hex dla Betclic, rodzaj danych, błąd i nazwę surowego pliku. Ramki STS mają
rozbite wartości JSON, ramki Betclic mają drzewa numerów pól Protobuf i osobne
odczytane strony z drużynami, ligami, datami i linkami. Interpretacja zagnieżdżeń
Protobuf bez schematu jest pomocnicza; surowe bajty pozostają źródłem do ponownego
parsowania. Deklarowana liczba wydarzeń i ostrzeżenia Betclic pozostają zapisane;
archiwum nie uznaje automatycznie niepełnej oferty za kompletną.

Archiwizowane są również ramki kontrolne/trailery, otrzymane fragmenty przerwanych
ramek i błędy pobierania. Błąd zapisu archiwum przerywa dane pobranie; poprzedni
cache pozostaje dostępny. Odczyt cache nie tworzy kopii tej samej odpowiedzi.
Na tym etapie archiwizujemy kalendarze i ich odpowiedzi źródłowe; kursy i statystyki
dołączą po przeniesieniu ich kolektorów.

Odczyt pojedynczego JSON na Ubuntu:

```bash
gzip -dc /pelna/sciezka/do/pliku.json.gz | less
```

Log szczegółowy Betclic: `logs/betclic-debug.log` w katalogu roboczym procesu,
oprócz logów systemd. Logi i archiwa nie mają jeszcze polityki rotacji.

## Sprawdzenie ręczne

Kod nie został uruchomiony, skompilowany ani przetestowany przez agenta.
Po uruchomieniu sprawdź w Wails trzy kalendarze, zmianę dnia, uzupełnianie linków
po kliknięciu i ręczne odświeżenie. Po restarcie backendu cache i terminy powinny
pozostać zapisane. W `/api/v1/health` sprawdzisz błędy źródeł; w archiwum odczytasz
otrzymane dane. Dostępność endpointów bukmacherów z adresu VPS trzeba potwierdzić
tym ręcznym uruchomieniem.
