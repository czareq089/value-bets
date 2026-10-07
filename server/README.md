# Value Bets — domowy serwer Debian

Cały kod backendu jest w `server/value_bets.go`. Klient Wails ma swój kod w głównym `value_bets.go`, a interfejs w `frontend/index.html`. Serwer pobiera kalendarze, kursy i statystyki SofaScore oraz wykonuje analizę meczu. Klient wyświetla dane i przechowuje osobistą historię oraz zakłady.

Klient automatycznie korzysta z `https://api.value-bets.pl`. Istniejący Cloudflare Tunnel powinien przekazywać tę domenę do `http://localhost:8090` na Debianie. Backend domyślnie nasłuchuje na `127.0.0.1:8090`; HTTPS obsługuje tunel. Oracle, Chromium i GitHub Actions nie są potrzebne. Aplikacja nie wymaga tokena API.

## Ręczne wgranie i uruchomienie

Skopiuj z katalogu `server` tylko trzy pliki do wybranego katalogu na Debianie:

- `value_bets.go`
- `go.mod`
- `go.sum`

Przykład dla `/home/vstkxj/value-bets` (zmień ścieżkę i użytkownika, jeśli korzystasz z innych):

```sh
cd /home/vstkxj/value-bets
go build -o value_bets value_bets.go
./value_bets -data ./data
```

Wymagane jest Go 1.26 lub nowsze. Pierwsze budowanie może pobrać zależności. Backend nie wymaga Wails, GUI ani Chromium. Plik `value_bets_test.go` zawiera zachowane testy regresji; nie jest potrzebny do uruchomienia serwera.

## Proces systemd

Zapisz poniższą zawartość jako `/etc/systemd/system/value-bets.service`, dopasowując użytkownika i ścieżki:

```ini
[Unit]
Description=Value Bets API
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=vstkxj
WorkingDirectory=/home/vstkxj/value-bets
ExecStart=/home/vstkxj/value-bets/value_bets -listen 127.0.0.1:8090 -data /home/vstkxj/value-bets/data
Restart=on-failure
RestartSec=5
TimeoutStopSec=30
UMask=0027

[Install]
WantedBy=multi-user.target
```

Następnie:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now value-bets
journalctl -u value-bets -n 80 --no-pager
```

Przy kolejnej aktualizacji wgraj źródło, zbuduj nową binarkę pod inną nazwą i podmień ją:

```sh
cd /home/vstkxj/value-bets
go build -o value_bets.new value_bets.go
mv value_bets.new value_bets
sudo systemctl restart value-bets
```

Nie usuwaj katalogu `data` przy aktualizacji. Gdy działa usługa systemd, nie uruchamiaj drugiego procesu na tym samym porcie.

## Dane i harmonogram

Kalendarze STS, Betclic i Superbet mają cache na dysku. Serwer sam je odświeża co około 2 godziny, z losowym przesunięciem od −10 do +10 minut. Harmonogram działa również przy zamkniętej aplikacji desktopowej. Analiza korzysta ze wspólnego cache kursów i historii zawodników, które mogą być przygotowane wcześniej przez tło.

### Kursy ważne przez 15 minut

Sprawdzanie rynków, pobieranie statystyk w tle i ręczna analiza używają tej samej sparsowanej oferty dla danego bukmachera i ID meczu. Kursy są ważne przez 15 minut od pierwotnego pobrania. Kliknięcie „Analizuj” w tym czasie nie pobiera ponownie oferty; brakujący albo przeterminowany wpis powoduje pobranie tylko z odpowiedniego źródła. Równoczesne żądania tej samej oferty czekają na jedno pobranie. Odczyt cache nie przedłuża jego ważności.

Oferty są zapisywane w `data/bookmaker_match_odds_cache.json`, więc restart również zachowuje świeże kursy. Odpowiedź bez obsługiwanych rynków jest zapamiętywana początkowo na 15 minut. Trzecie kolejne potwierdzenie braku wydłuża ten czas do 30 minut, a czwarte i następne do 60 minut. Licznik zwiększa się tylko po rzeczywistym, poprawnym pobraniu pustej oferty; odczyty cache i błędy go nie zwiększają. Pojawienie się obsługiwanych rynków zeruje licznik. Wygasłe puste obserwacje pozostają najwyżej 24 godziny jako historia licznika, bez możliwości wykorzystania ich jako świeżej oferty. Nieudane lub anulowane pobranie nie zastępuje poprawnej oferty. Archiwum analizy zawiera pierwotny czas pobrania, termin wygaśnięcia i informację o wykorzystaniu cache. Składy SofaScore są nadal sprawdzane przy analizie niezależnie od kursów.

Przy ręcznej weryfikacji sprawdź rynki lub poczekaj na przygotowanie meczu w tle, a następnie uruchom analizę tego samego meczu przed upływem 15 minut. W logu `[ODDS-CACHE] HIT` oznacza użycie zapisanej oferty, `FETCH` nowe pobranie, `JOIN` oczekiwanie na już trwające pobranie, a `STORED` zapis wyniku. Po wygaśnięciu cache kolejna analiza powinna pokazać `FETCH` dla danego bukmachera i ID meczu.

### Statystyki pobierane podczas bezczynności

Serwer automatycznie przegląda zapisane kalendarze i wybiera mecze rozpoczynające się w najbliższych 24 godzinach. Najpierw obsługuje mecze ze znanymi obsługiwanymi rynkami, następnie sprawdza pozostałe oferty. Sama flaga BetBuildera nie wystarcza. Historie są pobierane tylko dla zawodników dopasowanych do obsługiwanych rynków; brak takich zawodników nie uruchamia pobierania całej kadry.

Tło przygotowuje domyślną próbę 10 ostatnich spotkań, bez filtra rozgrywek (`ALL`). Ręczna analiza z tymi parametrami korzysta z tego samego cache; inna wielkość próby lub filtr rozgrywek mają własny wpis i są uzupełniane na żądanie. Ważność kursów i składów jest niezależna od historii zawodników: kursy korzystają z cache opisanego powyżej, a składy są sprawdzane przy analizie.

Jednocześnie wykonywany jest jeden mecz w tle. Ręczna analiza, detekcja drużyn, pobranie rozgrywek, sprawdzanie rynków oraz odświeżanie kalendarza przerywają tło. Zwykłe odczyty postępu, sprawdzenie zdrowia API i odczyty kalendarza z cache go nie przerywają. Serwer czeka na zakończenie anulowanych żądań przed rozpoczęciem pobierania o wyższym priorytecie. W czasie ponowienia wykorzystuje już zapisane kompletne historie zawodników.

Pobieranie SofaScore w tle jest seryjne: jeden zawodnik i jeden historyczny mecz naraz, najwyżej jedno żądanie sieciowe. Po zakończeniu każdego żądania jest losowa przerwa 2–4 sekundy, a pomiędzy zadaniami tła 30–60 sekund. Dotyczy to także wyszukiwania drużyn, terminarzy i kontroli statusów spotkań w tle. Odczyty ważnego cache nie czekają na limit sieciowy. Wszystkie oczekiwania można przerwać przez ręczne żądanie lub zatrzymanie procesu. Dotychczasowy limit 20 minut na zadanie tła pozostaje; kolejne podejście korzysta z kompletnych historii już zapisanych w cache.

Tylko odpowiedź SofaScore HTTP 429, także podczas ręcznej analizy, uruchamia wspólną przerwę w nowych żądaniach do tego źródła. Pierwsza trwa 15 minut, następne 30 i 60 minut; 60 minut to maksymalna automatyczna eskalacja. Po 24 godzinach bez nowej blokady licznik zaczyna od początku. Dłuższe `Retry-After` jest uwzględniane do 24 godzin. Odpowiedzi z żądań już wysłanych przed blokadą nie zwiększają licznika w tej samej przerwie. Stan jest zapisany w `data/sofa_request_backoff.json` i przetrwa restart. Podczas przerwy tło nie rozpoczyna kolejnych zadań, a ręczne pobranie SofaScore zwraca komunikat z terminem ponowienia. Lokalny cache oraz pozostałe źródła danych pozostają dostępne. Logi `[STATS-BACKGROUND] PACING` oraz `[SOFA-BACKOFF] PAUSE`/`LOADED` pokazują konfigurację i termin wznowienia. HTTP 403 nie uruchamia wspólnej przerwy ani 15-minutowej przerwy danego endpointu kontekstu; ręczne próby można powtarzać od razu. Stare zapisane przerwy bez jawnego statusu 429 są ignorowane po aktualizacji, bez usuwania plików. Tło zachowuje zwykłe odstępy i harmonogram ponawiania nieudanych zadań. Spowolnienie ogranicza obciążenie, lecz nie gwarantuje, że źródło nie zwróci kolejnego challenge.

Cache historii jest powiązany z ID konkretnego meczu SofaScore. Po rozpoczęciu meczu serwer podczas bezczynności sprawdza jego status co około 5 minut; zakończenie lub anulowanie unieważnia historie sprzed tego spotkania. Gdy źródło nie odpowiada, zabezpieczeniem jest wygaśnięcie 4 godziny po zaplanowanym rozpoczęciu. Jeśli ręcznej analizy nie uda się powiązać z konkretnym meczem, używa osobnego cache ważnego najwyżej 24 godziny. Nowa wersja kluczy cache nie wykorzystuje starych wpisów z krótkim TTL; nie trzeba usuwać plików ręcznie.

Postęp tła i terminy meczów są zapisywane w `data/player_stats_background.json`, a historie w `data/sofa_player_stats_cache.json`. Restart zachowuje postęp. Po nowym pobraniu kalendarza oferty mogą być sprawdzone ponownie, aby uwzględnić nowych zawodników; istniejące ważne historie są używane ponownie. Brak rynków nie jest sprawdzany w pętli dla tego samego kalendarza. Niepełne pobrania i błędy mają przerwę 15 minut przed ponowieniem; przerwanie przez ręczne żądanie ma przerwę 15 sekund. Archiwum zawiera również ofertę używaną do przygotowania statystyk i opisane historie zawodników.

W logach szukaj `[STATS-BACKGROUND] START`, `END` (status `completed`, `unavailable`, `partial`, `error` lub `paused`), `YIELD` oraz `[STATS-CACHE] FIXTURE_ENDED`. Przy ręcznej weryfikacji pozostaw serwer bez aktywnych żądań, sprawdź wpis `completed`, a następnie uruchom analizę tego meczu z próbą 10 i bez ograniczenia rozgrywek. Log `[SOFA-PLAYER-CACHE]` powinien wskazać wykorzystanie przygotowanych historii. Uruchomienie ręcznej analizy podczas pracy tła powinno spowodować wpis `YIELD` i `paused`.

Filtr „Tylko mecze z obsługiwanymi rynkami” sprawdza rzeczywistą ofertę meczu tymi samymi parserami co analiza. Sama flaga BetBuildera nie kwalifikuje meczu. Potrzebny jest przynajmniej jeden obsługiwany rynek zawodnika z poprawnym kursem; gole i asysty nie wystarczają. Sprawdzanie obejmuje mecze wybranego bukmachera i dnia pasujące do wyszukiwania oraz rozgrywek, najwyżej trzy równolegle. Do zakończenia sprawdzania lista wyników może być niepełna; licznik pokazuje postęp. Błąd źródła jest osobnym stanem i nie oznacza braku rynków.

Filtr jest domyślnie włączony. Karty spotkań pokazują wiek sprawdzenia oraz godzinę wygaśnięcia wyniku. Świeża dostępność jest zielona, potwierdzony brak czerwony (widoczny po wyłączeniu filtra), a wygasły lub nieznany wynik szary. Wiek danych na widocznych kartach aktualizuje się co 30 sekund bez pobierania ofert. Nowy wygląd wymaga przebudowania klienta Wails.

Wyniki sprawdzeń trafiają do `data/market_availability_cache.json` i archiwum. Cache dostępnej oferty jest ważny 15 minut od pobrania kursów, braku rynków 15/30/60 minut według licznika opisanego powyżej, a błędu minutę. Każdy wpis dotyczy konkretnego bukmachera i ID meczu. Ponowne wczytanie kalendarza lub przełączenie filtra korzysta z ważnych wpisów zapisanych na serwerze; log `[MARKET-CACHE] HIT` potwierdza odczyt bez ponownego pobrania oferty. W trakcie przeglądania zachowywany jest wynik danego skanowania; ponowne wczytanie kalendarza lub przełączenie filtra ponawia sprawdzenia, których cache już wygasł. Sparsowana oferta użyta do sprawdzenia rynków trafia również do wspólnego cache kursów i może być od razu wykorzystana przez analizę. Oba cache zachowują oryginalny czas obserwacji i ten sam termin następnego sprawdzenia, także po restarcie. Licznik kolejnych pustych odpowiedzi w logu `[ODDS-CACHE] STORED` ma nazwę `consecutive_missing` i zatrzymuje się na 4 (cztery lub więcej).

Analiza jest niezależnym zadaniem serwera. Klient odczytuje jej postęp krótkimi zapytaniami, dzięki czemu całe obliczenie nie zajmuje jednego długiego połączenia przez Cloudflare. Parametry, wyniki i status zadań są zapisywane w `data/analysis-jobs`.

Składy SofaScore są odczytywane także przed potwierdzeniem. Oznaczenie `XI` to potwierdzony podstawowy skład, a `XI?` to przewidywana jedenastka (`confirmed: false`). Przewidywani rezerwowi mają oznaczenie `POZA XI?`. Filtr XI obejmuje potwierdzonych i przewidywanych starterów; wyklucza zawodników o nieznanym statusie. Brak zawodnika na liście nie oznacza automatycznie ławki. Dopasowanie opiera się przede wszystkim na ID zawodnika. Archiwum zapisuje również ID meczu SofaScore i flagę potwierdzenia składów.

Automatyczne dopasowanie drużyn wymaga zgodnej nazwy lub sluga, dyscypliny i kategorii. Nazwy bez oznaczenia płci domyślnie dotyczą drużyn męskich; oznaczenia kobiet w nazwie albo rozgrywkach z cache kalendarza przełączają wyszukiwanie na drużyny żeńskie. Prefiks `RB` jest zachowywany jako `Red Bull`, aby nie łączyć różnych klubów. Brazylijskie oznaczenia stanów, np. `-rs` w Internacionalu i Grêmio, mogą być pominięte przy porównaniu z drużyną z Brazylii; nie są usuwane z kluczy cache. Wyszukiwarka nie wysyła zapytań o samą końcówkę `rs`. Drużyny młodzieżowe i rezerwy nie zastępują pierwszej drużyny.

Jeśli kilka zespołów nadal ma tę samą nazwę, serwer sprawdza terminarze kandydatów i wymaga jednej pasującej pary przeciwników. Termin wybranego meczu jest odczytywany z istniejącego cache kalendarza, bez ponownego pobierania oferty; jeśli jest dostępny, dopuszczalna różnica czasu wynosi godzinę. Błąd pobrania terminarzy nie pozwala uznać częściowego sprawdzenia za jednoznaczny wynik. Przy braku potwierdzenia serwer nie wybiera pierwszego wyniku wyszukiwania.

Stary plik `sofa_team_cache.json` jest zachowany. Automatyczne wpisy bez potwierdzenia wersji 3 i kategorii w `sofa_team_identity_cache.json` są ponownie rozpoznawane przy użyciu; ręczne mapowania pozostają ważne. Poprawione ID trafia do cache. Analiza weryfikuje ID przed pobraniem statystyk. Gdy dopasowanie się powiedzie, zastępuje nieaktualne ID przesłane przez klienta lub zapisane w historii, także po usunięciu cache serwera. Korekta pojawia się w ostrzeżeniach i parametrach zadania, a dalsze obliczenia korzystają z poprawionego ID. W logu `[SOFA-SEARCH] CANDIDATE` zawiera również płeć, dyscyplinę i kraj, a `[SOFA-DETECT] FIXTURE` wskazuje mecz rozstrzygający niejednoznaczność. Wpisy `[SOFA-EVENT]` i `[SOFA-LINEUP]` pokazują wybór meczu, pobranie składów i dopasowane statusy. Gdy nie ma danych o XI, klient wyjaśnia pusty wynik filtra.

Archiwum znajduje się w `data/archive`. Katalogi analiz zawierają datę, nazwy drużyn i identyfikator zadania. Nazwy plików opisują źródło, zakres danych, rodzaj odpowiedzi i czas UTC. Większe pliki są kompresowane gzip; surowy protobuf pozostaje binarny, obok znajduje się jego reprezentacja JSON.

Kursy mają wspólne archiwum `data/archive/oferty-kursow/<bukmacher>/mecz-<id>/wersja-<hash>/`. Pełna sparsowana oferta jest zapisywana raz dla konkretnej wersji danego meczu i bukmachera. Nazwa pliku zawiera bukmachera, nazwy drużyn, ID meczu i czas pierwszej obserwacji. Zmiana kursu, linii, zawodnika, rynku, strony zakładu lub identyfikatora selekcji tworzy inną wersję; sama kolejność elementów, etykiety i techniczne znaczniki czasu nie tworzą kopii. Powrót A → B → A korzysta ponownie z pierwszego pliku A. Deduplikacja działa również po restarcie.

Każde rzeczywiste sprawdzenie oferty oraz użycie kursów z cache dopisuje mały wiersz do dziennego pliku `data/archive/obserwacje-kursow/<data>/<bukmacher>_sprawdzenia-i-uzycie-kursow.jsonl`. To czytelny tekst: jeden obiekt JSON na linię. Pole `source` rozróżnia `bukmacher` i `cache`, `recordedAt` oznacza czas zapisu obserwacji, a `observedAt` pierwotny czas pobrania kursów. `archiveVersion` wskazuje plik oferty względem `data/archive`. Nie dochodzi do dodatkowych zapytań sieciowych ani przedłużenia TTL. Log `[ODDS-ARCHIVE] OBSERVATION` pokazuje źródło i `new_version`.

Archiwa ręcznej analizy i przygotowania statystyk zapisują odwołanie `archiveVersion` i liczbę rynków zamiast kolejnej pełnej listy kursów. W danych wejściowych analizy odwołania do ofert znajdują się w `oddsSources`. Surowe odpowiedzi z poprawnego pobrania nowej wersji są zapisywane raz w jej podkatalogu `surowe-zrodlo`; przy ofercie identycznej z już zapisaną pozostaje tylko obserwacja. Nieudane pobrania nadal zachowują odpowiedzi diagnostyczne. Wersja utworzona ze starego cache bez zapytania sieciowego może nie mieć surowej odpowiedzi. Cache roboczy wciąż zawiera pełne kursy, aby analiza nie musiała czytać archiwum. Zmiana dotyczy nowych zapisów; istniejące archiwum nie jest usuwane ani przerabiane, a archiwizacja statystyk pozostaje bez zmian.

Odczyt wybranego pliku JSON:

```sh
gzip -dc /sciezka/do/pliku.json.gz | less
```

Automatyczny skaner okazji dla meczów w najbliższych 24 godzinach oraz powiadomienia pozostają kolejnym etapem. Obecnie analiza jest uruchamiana dla wybranego meczu.

## Nagłówki i sesja Chromium SofaScore

Transport SofaScore korzysta z profilu TLS/HTTP `Chrome_152`, dostępnego w obecnym `tls-client v1.16.0`, zamiast Firefox 120. Dostarczona przeglądarka ma Chromium 154; biblioteka nie ma dokładnego profilu tej wersji, więc jest to przybliżenie, a nie identyczne odwzorowanie przeglądarki. Domyślne nagłówki opisują dostarczone Chromium 154 na Linuxie. Skuteczność wymaga ręcznej weryfikacji; nie ma gwarancji, że przeniesiona sesja będzie akceptowana.

Pełną sesję można wczytać z pliku `data/sofascore_request.curl`, według katalogu `-data`. W Chromium na Debianie skopiuj żądanie API przez Network → Copy as cURL (bash), a następnie wklej całe polecenie do pliku:

```sh
cd /home/vstkxj/value-bets
mkdir -p data
(umask 077; touch data/sofascore_request.curl)
chmod 600 data/sofascore_request.curl
nano data/sofascore_request.curl
```

Plik jest traktowany wyłącznie jako dane, nigdy wykonywany w shellu. Import obsługuje postać bash ze skopiowanego żądania (`curl --url ... -H ... -b ... -X ...` albo adres jako argument). Odczytywane są wszystkie cookies oraz dozwolone nagłówki przeglądarki: User-Agent, accept, język, priority, referer, client hints, sec-fetch i nagłówki SofaScore. Dane nie trafiają do kodu ani repozytorium; katalog `data` jest ignorowany przez Git. Nagłówki `If-None-Match`, `If-Modified-Since`, Host i nagłówki transportowe nie są importowane. URL z pliku służy wyłącznie do sprawdzenia źródła, nie zastępuje endpointu pobieranego przez backend. Nawet jeśli przechwycono HEAD, backend wysyła GET, aby odebrać JSON. Nie są importowane proxy, dane POST ani pozostałe operacje cURL.

Cookies trafiają do jar klienta. Token `sofa_captcha` jest również przekazywany jako `X-Captcha`; jeśli źródło podaje obie wartości, muszą być identyczne. Niepusta zmienna `VALUE_BETS_SOFA_CAPTCHA` ma pierwszeństwo przed tokenem z importu. Przy braku tokenu w imporcie pozostaje zgodność z dawnym `data/sofascore_captcha.txt` (sam token bez cudzysłowów), `VALUE_BETS_SOFA_CAPTCHA_FILE` i `VALUE_BETS_SOFA_CAPTCHA`.

`X-Requested-With` jest teraz wysyłany tylko wtedy, gdy znajduje się w przechwyconym żądaniu lub jest jawnie ustawiony przez `VALUE_BETS_SOFA_REQUESTED_WITH`. W najnowszym dostarczonym żądaniu tego nagłówka nie ma. Ścieżkę importu można zmienić przez `VALUE_BETS_SOFA_CURL_FILE`. Brak pliku oznacza użycie domyślnych nagłówków i opcjonalnego osobnego tokenu. Istniejący, lecz niepoprawny plik daje czytelny błąd konfiguracji zamiast cichego pominięcia importu.

Plik cURL i osobny plik tokenu są odczytywane przed każdym żądaniem; podmiana ich zawartości nie wymaga restartu. Zmiana środowiska systemd wymaga restartu procesu. Metadane JWT `exp` są odczytywane bez lokalnej weryfikacji podpisu. Niepoprawny lub wygasły token jest pomijany, a stare cookie tego typu usuwane z jar. Cookie i token mogą zależeć od IP oraz cech sesji; potrzebne jest rzeczywiste HTTP 200 w kliencie Go, sam poprawny import tego nie dowodzi.

Log `[SOFA-SESSION] LOADED` pokazuje liczbę nagłówków i cookies, metodę źródła oraz profil TLS, bez ich wartości. `[SOFA-CAPTCHA]` podaje jedynie stan i termin ważności; `[SOFA-HTTP] REQUEST` pokazuje flagę `captcha_configured` oraz publiczne nagłówki diagnostyczne. Tokeny i cookies nie są wypisywane do logów. Błędy lokalnego pliku importu i HTTP 403 nie nakładają przerwy na ponowienie ręcznej próby; HTTP 429 nadal uruchamia przerwę.

Po wgraniu i przebudowaniu backendu sprawdź ręcznie analizę oraz logi `LOADED`, `captcha_configured` i rzeczywisty status `SOFA-HTTP`. Diagnostyka `-sofa-diagnose -data ./data` również korzysta z tej konfiguracji. Agent nie uruchamiał zapytań ani testów.

## Fair kurs i podgląd drużyn — context-count-v2

Fair kurs i historyczny hit rate są liczone osobno. Nowy model używa rzeczywistych liczności z historii zawodnika, bez per90 i bez przeliczania przez przewidywane minuty. Mechanizm SuperSub, jego dane i przełącznik wykresu zachowują dotychczasowe działanie. Podobnie jak wcześniej fair kurs jest obliczany z bazowych statystyk zawodnika; przełącznik SuperSub zmienia podgląd próbki, bez ponownej wyceny zakładu.

Średnia zawodnika jest ważona według aktualnej pozycji ze składu oraz miejsca meczu SofaScore (home/away, także przy odwróconej kolejności u bukmachera). Inna znana pozycja otrzymuje wagę 0,5, inne znane miejsce 0,7; brak danych nie powoduje kary. Następnie model porównuje profil obecnej drużyny i przeciwnika z profilami drużyn występujących w próbce zawodnika. Porównania matchupu obejmują tylko tę samą ligę i sezon.

Wykorzystywane są liczniki właściwe dla rynku: strzały i strzały dopuszczane, celne i dopuszczane celne, niecelne, zablokowane, strzały z wewnątrz/poza polem karnym, odbiory, przechwyty i spalone. Dla fauli otrzymanych używane są faule popełniane przez rywala. Każdy licznik drużynowy jest dzielony przez liczbę meczów drużyny, nigdy przez minuty zawodnika. Dla fauli popełnionych, strat piłki oraz celnych strzałów z konkretnych stref brakuje bezpośrednio porównywalnego pełnego profilu w dostarczonej mapie; te rynki korzystają z roli i home/away, z neutralną korektą matchupu. Brak konkretnego pola nie jest traktowany jako zero.

Korekta porównuje aktualną wartość z ważoną średnią profili w próbce. Logarytm ilorazu ma wagę 0,5 na każdy dostępny czynnik, wygładzanie 0,25 i osłabienie `n/(n+8)` zależne od liczby spotkań sezonu. Niepełne pokrycie historii dodatkowo zmniejsza wpływ korekty. Cały mnożnik jest ograniczony do 0,8–1,25. To jawne parametry początkowe, a nie wytrenowane współczynniki. Profile dawnych rywali opisują stan sezonu w momencie obecnej analizy; nie są rekonstrukcją stanu sprzed historycznego meczu.

Zamiast stałego miksu 65% hit rate + 35% Poisson model liczy prawdopodobieństwo z rozkładu liczności. Przy nadmiernym rozproszeniu próbki używa rozkładu ujemnego dwumianowego; oszacowanie dodatkowej wariancji jest osłabiane przez `effectiveSample/(effectiveSample+10)`. W pozostałych przypadkach używa Poissona. Dotychczasowe ograniczenie prawdopodobieństwa do 0,005–0,95 pozostaje. Wynik jest modelem eksperymentalnym, bez potwierdzonej kalibracji; mała efektywna próbka jest oznaczana w szczegółach.

Rating zawodnika pojawia się pod datą każdego meczu na wykresie i w dymku słupka, wraz ze średnią ratingów dostępnych w próbce. Brak ratingu to kreska, nie zero. Rating jest odczytywany ze statystyk meczu albo z `statisticsMap` historii; ocena zmiennika nie zastępuje oceny zawodnika. Rozwijane „Fair kurs i kontekst drużyn” pokazuje średnią, mnożnik matchupu, oczekiwaną liczność, rozkład, efektywną próbkę i wykorzystane czynniki. Osobny podgląd zawiera tabelę ogólną i home/away, formę, posiadanie, xG i rating drużyny. Rating, tabela, forma, posiadanie oraz xG nie wchodzą do wzoru fair. Przy tabeli poniżej 8 meczów widnieje ostrzeżenie o małej próbce.

Profile sezonowe mają cache 6 godzin, metadane meczu, tabele i forma godzinę w `data/matchup_cache`. Pliki są współdzielone między analizami tej samej ligi i drużyn oraz zachowywane po restarcie. Błędy źródła mają przerwę 15 minut w pamięci procesu; nie nadpisują poprawnego cache ani nie zamieniają braków w zera. Jedna analiza pobiera najwyżej 32 profile drużyn. Składy nadal są odświeżane osobno. Pierwsza analiza po aktualizacji może trwać dłużej, bo wersja cache historii zawodnika rośnie do 6 i uzupełnia rating oraz identyfikatory kontekstu. Nie usuwaj cache ręcznie.

Archiwum `kursy-statystyki-sklady-matchup-uzyte-do-obliczen` zapisuje profile z czasami pobrania, pozycje i kontekst historii. Wynik ma wersję modelu, obliczone prawdopodobieństwo i uzasadnienie. Późniejsza ocena modelu musi używać tych zapisanych danych sprzed predykcji, zamiast pobierać aktualne agregaty do dawnych spotkań. Szczegóły oraz ratingi są również zachowywane w zakładkach klienta. Starsze zapisane analizy pozostają czytelne, lecz nie dostają nowych pól bez ponownej analizy.

Ręczna weryfikacja po wgraniu backendu i przebudowaniu Wails: przeanalizuj mecz ze znanym składem, sprawdź ratingi, czynniki i tabelę w szczegółach, potem powtórz analizę i sprawdź wykorzystanie cache. W logu `[MATCHUP]` widać ID ligi i sezonu oraz liczbę profili i pozycji. Sprawdź też brakującą ocenę, wczesny sezon, mecz bez składów oraz zapis/odczyt zakładki. Przełączanie SuperSub powinno nadal zmieniać tylko dotychczasowy podgląd próbki i mapę strzałów.

## Przebieg spotkań i reakcje — match-flow v1

Analiza pobiera do 5 ostatnich zakończonych spotkań każdej drużyny w rozgrywkach nadchodzącego meczu, z pierwszej strony `/team/{id}/events/last/0`. Łączy `/event/{id}/graph` z `/event/{id}/incidents`. Wspólny mecz obu drużyn jest pobierany raz. Brak wykresu jest oznaczany jako brak danych, bez zastępowania zerami. Dotyczy to także lig bez funkcji momentum. Przebieg jest informacją pomocniczą; nie zmienia obecnego wzoru fair ani SuperSuba.

Momentum jest indeksem przebiegu gry, nie liczbą strzałów, posiadaniem ani xG. [Opis SofaScore](https://www.sofascore.com/news/how-live-attack-momentum-works-at-the-world-cup). Dodatnie wartości źródła odnoszą się do gospodarzy; przy podglądzie gości znak jest odwracany. Podsumowanie prowadzenia, remisu i straty pokazuje średnie saldo oraz udział dostępnych punktów wykresu z przewagą wybranej drużyny. Próba jest podana jako liczba punktów i meczów; nie udaje dokładnego czasu gry.

Oś wyniku powstaje z rosnąco uporządkowanych goli. Zapisane wyniki po golach muszą tworzyć spójną sekwencję od 0:0 do wyniku po regulaminowym czasie. Gol samobójczy jest przypisywany według zmiany wyniku, a anulowane incydenty są pomijane. Przy niezgodności wykres nadal jest dostępny, lecz klasyfikacja stanów i agregaty reakcji są wyłączone. Przetwarzane jest 90 minut; dogrywka i seria karnych są pomijane.

Reakcje obejmują zdobycie i stratę bramki, objęcie i utratę prowadzenia, wyrównanie, żółte i czerwone kartki oraz zmiany obu drużyn. Każdy incydent porównuje średnie momentum z maksymalnie 5 minut przed i po. Okno kończy się na kolejnym incydencie lub przerwie. Minuta samego incydentu jest wyłączona, bo kolejność akcji w tej minucie jest nieznana. Doliczony czas w wykresie może występować jako punkt ułamkowy, np. `45.5`; takie punkty są pokazywane, ale nie są traktowane jako pełne minuty w agregatach. Incydenty `45+N` i `90+N` nie dostają fikcyjnych okien reakcji. Co najmniej 3 zwykłe punkty po każdej stronie są wymagane do agregowania reakcji. Nakładające się incydenty w tej samej minucie są wyłączane z agregatów reakcji. Wyjątkiem jest pakiet zmian jednej drużyny, liczony jako jedno okno. Różnica przed/po opisuje obserwację, bez dowodu, że incydent spowodował zmianę.

W kliencie rozwijane „Fair kurs i kontekst drużyn” ma karty kursu fair, prawdopodobieństwa i EV, trzy kroki wyceny oraz tabelę porównującą drużyny. Założenia i komunikaty o dostępności są zwijane. Sekcja przebiegu ma wybór drużyny i historycznego spotkania, podsumowanie stanów wyniku, tabelę reakcji oraz wykres z klikalnymi znacznikami goli, kartek i zmian. Zielona część wykresu dotyczy wybranej drużyny, czerwona rywala. Kliknięcie znacznika wskazuje wiersz incydentu. Dymki pokazują zawodników i wartości wykresu.

Pełne przebiegi są zapisane w `data/match_flow_cache/mecz-{id}/wersja-{hash}/przebieg-meczu.json`. Podsumowania analizy i zakładek zawierają tylko odwołania do konkretnej wersji. Pełny wykres jest pobierany przez klienta osobno, przez odczytowe `GET /api/v1/match-flow?eventId=...&revision=...`, które nie uruchamia zapytań do SofaScore. Otwieranie wielu zakładów tego samego meczu korzysta również z pamięci klienta. Zapobiega to powielaniu setek punktów wykresu przy każdym rynku.

Cache zakończonych przebiegów jest ważny 7 dni; w ciągu pierwszej doby od rozpoczęcia historycznego meczu 30 minut, aby umożliwić uzupełnienia źródła. Lista ostatnich spotkań jest ważna 6 godzin. Pierwsza analiza po aktualizacji pobierze brakujące przebiegi. Czytelne archiwum przetworzonego przebiegu trafia do `data/archive/przebieg-meczow/mecz-{id}/wersja-{hash}/{druzyny}_przebieg-meczu.json.gz`, raz dla danej treści. Identyczna treść i powrót do wcześniejszej wersji nie tworzą kolejnego pełnego pliku. Zmiana treści tworzy nową wersję; zapisane analizy i zakładki nadal wskazują tę używaną wcześniej. Surowe odpowiedzi są archiwizowane przez dotychczasowy transport SofaScore. Nie usuwaj zapisanych wersji, jeśli zakładki mają nadal otwierać ich wykresy.

Do ręcznej weryfikacji wgraj backend i przebuduj klienta. Uruchom nową analizę, otwórz szczegóły i przełącz drużynę oraz historyczny mecz. Sprawdź wynik, znaki wykresu po przełączeniu gospodarze/goście, znacznik gola i okno reakcji, kartki, zmiany oraz incydent w doliczonym czasie. Powtórna analiza powinna korzystać z cache. W logu `[MATCH-FLOW]` widać liczbę wybranych i dostępnych meczów oraz typów reakcji. Starsze zakładki bez przebiegów pozostają czytelne i wymagają nowej analizy do ich uzupełnienia. Kod nie był uruchamiany ani testowany przez agenta.

## Klient i ręczna weryfikacja

Klienta zbuduj przez `wails build` z głównego katalogu projektu. Nowe pliki osobiste trafiają do `data`; istniejące pliki historii i zakładów obok aplikacji lub w katalogu roboczym nadal są odczytywane i zapisywane w dotychczasowym miejscu.

Ręcznie sprawdź lokalne API na Debianie, a następnie domenę przez tunel:

```sh
curl http://127.0.0.1:8090/api/v1/health
curl https://api.value-bets.pl/api/v1/health
```

Po uruchomieniu sprawdź w kliencie kalendarze trzech bukmacherów, uzupełnianie linków i ID SofaScore po wyborze meczu, postęp analizy oraz zapis danych w archiwum. Przy pierwszym starcie pobranie kalendarzy może chwilę potrwać.

Opcjonalna diagnostyka SofaScore, wykonywana ręcznie na Debianie:

```sh
./value_bets -sofa-diagnose -data ./data
```

Kod nie został uruchomiony, skompilowany ani przetestowany przez agenta; weryfikacja działania pozostaje ręczna zgodnie z ustaleniem.
