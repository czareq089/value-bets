# Value Bets — domowy serwer Debian

Cały kod backendu jest w `server/value_bets.go`. Klient Wails ma swój kod w głównym `value_bets.go`, a interfejs w `frontend/index.html`. Serwer pobiera kalendarze, kursy i statystyki SofaScore oraz wykonuje analizę meczu. Klient wyświetla dane i przechowuje osobistą historię oraz zakłady.

Klient automatycznie korzysta z `https://api.value-bets.pl`. Istniejący Cloudflare Tunnel powinien przekazywać tę domenę do `http://localhost:8090` na Debianie. Backend domyślnie nasłuchuje na `127.0.0.1:8090`; HTTPS obsługuje tunel. Oracle i GitHub Actions nie są potrzebne. Aplikacja nie wymaga tokena API. Dla SofaScore można włączyć pobieranie przez działającą sesję Chromium, opisaną niżej.

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

Wymagane jest Go 1.26 lub nowsze. Pierwsze budowanie może pobrać zależności. Backend nie wymaga Wails. Opcjonalny tryb SofaScore przez przeglądarkę wymaga uruchomionego Chromium; zwykły transport HTTP działa bez niego.

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

### Kwalifikacja meczu i priorytety

Analiza ręczna i automatyczna wymagają tego samego meczu w Superbet, Betclic i STS oraz poprawnego pobrania całej dostępnej oferty obsługiwanych rynków od każdego z nich. Zestawy rynków mogą się różnić; nie wymagamy identycznych typów, zawodników ani linii. Brak obsługiwanych rynków lub błąd jednego źródła blokuje pobieranie historii zawodników, obliczenia i zapis prognoz. Cache kalendarzy, dostępności ofert i diagnostyka służą także ustalaniu, które mecze spełniają warunki.

Serwer dopasowuje obie drużyny w tej samej kolejności, z tolerancją terminu 10 minut. Uwzględnia jawne aliasy, pełne słowa i oznaczenia juniorów, rezerw oraz Red Bulla. Kilka pasujących wydarzeń lub sprzeczność między dowolną parą bukmacherów wyklucza automatyczne dopasowanie. Klient korzysta z tych samych dopasowań serwera. Domyślny filtr kalendarza pokazuje mecze z zapisaną informacją o obecności rynków całej trójki. Aktualne kursy są sprawdzane przy analizie.

Tło wycenia wszystkie trzy oferty w jednej analizie meczu. Zmiana wersji kursów dowolnego bukmachera jest uwzględniana przy następnym podejściu; termin ponownego sprawdzenia bierze pod uwagę najwcześniejsze wygaśnięcie oferty. Do czasu kwalifikacji tło sprawdza oferty jedynie spotkań jednoznacznie dostępnych we wszystkich kalendarzach.

Priorytet obejmuje pobieranie rynków bukmacherów oraz żądania SofaScore. Wybranie meczu od razu przygotowuje oferty całej trójki przez `POST /api/v1/markets/prepare`. Ręczna analiza korzysta z tych samych pobrań i cache. Kolejka ofert ma cztery miejsca, z czego tło może zajmować najwyżej jedno; pozostałe są dostępne dla użytkownika. Gdy wybrana oferta jest pobierana z niższym priorytetem, żądanie jest anulowane i ponawiane z priorytetem użytkownika (log `[ODDS-CACHE] PROMOTE`). Przycisk „Analizuj” nie czeka na przygotowanie rynków ani odczyt rozgrywek.

Kolejki SofaScore obsługują najpierw ręczną analizę, następnie interaktywną detekcję i rozgrywki, a na końcu tło. Dotyczy to transportu HTTP i pojedynczej sesji Chromium/CDP. Rozpoczęte analizy tła pozostają aktywne z niższym priorytetem; nowe zadania tła i skany ofert czekają podczas pracy użytkownika. Żądanie SofaScore już wysłane kończy się przed obsługą następnego w kolejce.

Detekcja gospodarzy i gości pracuje równolegle, z zachowaniem limitów transportu. Wybór w kalendarzu przesyła nazwy oraz link wskazujący termin; para ID musi zgadzać się z terminarzem SofaScore. Wybór kolejnego meczu anuluje poprzednią detekcję po stronie klienta. Aktualizacja wymaga wgrania backendu na Debian i użycia przebudowanego klienta Windows.

Kalendarz archiwizuje jedną aktualizowaną paczkę `archive/kalendarze/<polska-data>/<bukmacher>/kalendarz.json.gz` dziennie. Paczka obejmuje wszystkie pobrane daty Superbet; odpowiedzi transportowe i pośrednie strony kalendarza nie tworzą osobnych archiwów. Ręczne odświeżenie nadal jest dostępne.

Oddzielny proces tła sprawdza rzeczywiste rynki w oknie 48 godzin, po jednej ofercie naraz, z co najmniej trzysekundową przerwą po zakończeniu pobrania. Nie rozpoczyna nowego skanu podczas detekcji, przygotowania wybranego meczu lub ręcznej analizy. Najpierw sprawdza nieodczytane oferty, od najbliższego startu, potem najstarsze wyniki. Logi `[MARKET-SCAN] CHECK` i `RESULT` pokazują sprawdzane wydarzenia oraz czas pobrania. Odczyt kalendarza zawiera zapisany stan dostępności rynków; klient odczytuje ten stan bez uruchamiania skanowania. Pobranie kalendarza nie przerywa analizy.

Indeks nazw drużyn i dopasowania ofert są ponownie wykorzystywane do kolejnej aktualizacji kalendarza, zamiast przeliczania przy każdym odczycie i skanie. Log `[CALENDAR-INDEX] BUILT` wskazuje przebudowę indeksu. Zapis cache kursów i dostępności jest grupowany co pięć sekund; kodowanie JSON i zapis dyskowy odbywają się poza blokadami odczytu tych cache. Przy zatrzymaniu wykonywany jest końcowy zapis. Archiwa nadal używają gzip, z szybszą kompresją kosztem potencjalnie większych plików.

Kalendarz pozostaje dostępny podczas ręcznej analizy. Można wybrać kolejny mecz i odczytać jego zapisaną analizę; podglądy i końcowy wynik poprzedniego zlecenia nie nadpiszą nowego wyboru. Wynik poprzedniej analizy nadal trafia do historii właściwego spotkania. Kolejną ręczną analizę można uruchomić po zakończeniu bieżącej.

Poprawka współbieżności z 10 października 2026 wymaga aktualizacji backendu oraz klienta Windows. Panel review pozostaje bez zmian. Dla tej poprawki wykonano kompilację; zgodnie z prośbą użytkownika nie uruchamiano testów.

Ligi i mecze kobiet są wyłączone przed zapisaniem kalendarza i pobieraniem ofert. Oznaczenia nazw oraz znane nazwy lig (np. NWSL, WSL, Frauen, Liga F) są rozpoznawane. Wyłączone ID są pamiętane w `excluded_women_events.json`, żeby odrzucać również ręczne żądania. Wspólny katalog bukmachera może zawierać obie płcie w jednej odpowiedzi; takie wydarzenia są odrzucane przed dalszym przetwarzaniem. Istniejące archiwa badań i oceny pozostają zachowane.

Alavés / Deportivo Alavés i Athletic Bilbao / Athletic Club korzystają z jawnych aliasów. Nadal obowiązuje zgodność płci, sportu oraz kategorii rezerw i juniorów.

Kliknięcie karty z gotową analizą otwiera ten sam zapisany run i bukmachera, z których pochodzą liczniki. Powtórzona oferta tej samej linii, gracza i kierunku u jednego bukmachera jest liczona raz, z najlepszym kursem, zarówno w analizie ręcznej, jak i zapisanej. „Analizuj” wykonuje aktualne obliczenie dla wybranych bukmacherów; zmiana kursów, próbki lub składu może zmienić wyniki.


Kalendarze STS, Betclic i Superbet mają cache na dysku. Serwer sam je odświeża co 12 godzin (dwa razy na dobę). Harmonogram działa również przy zamkniętej aplikacji desktopowej. Analiza korzysta ze wspólnego cache kursów i historii zawodników, które mogą być przygotowane wcześniej przez tło.

Uzupełnianie linków korzysta z dopasowania serwera opisanego powyżej. `Palmeiras` pasuje do `SE Palmeiras`, a `Bahia` do `EC Bahia` i `Bahia Salvador BA`. Ogólne słowo, np. Inter, United czy Sporting, nie wystarcza do dopasowania po wspólnym rdzeniu. Detekcja SofaScore rozpoznaje prefiksy SE/EC i alias Bahia Salvador BA, zachowując weryfikację tożsamości drużyny.

Nazwy Werder Brema/Bremen są normalizowane, a detekcja SofaScore obsługuje również skrócone Werder oraz Borussia Dortmun. Te aliasy nie zastępują weryfikacji płci, kategorii i tożsamości drużyny. Dopasowanie nie korzysta z rankingu podobieństwa liter ani z wyboru najlepszego spośród kilku kandydatów.

### Kursy ważne przez 15 minut

Sprawdzanie rynków, pobieranie statystyk w tle i ręczna analiza używają tej samej sparsowanej oferty dla danego bukmachera i ID meczu. Kursy są ważne przez 15 minut od pierwotnego pobrania. Kliknięcie „Analizuj” w tym czasie nie pobiera ponownie oferty; brakujący albo przeterminowany wpis powoduje pobranie tylko z odpowiedniego źródła. Równoczesne żądania tej samej oferty czekają na jedno pobranie. Odczyt cache nie przedłuża jego ważności.

Oferty są zapisywane w `data/bookmaker_match_odds_cache.json`, więc restart również zachowuje świeże kursy. Odpowiedź bez obsługiwanych rynków jest ważna przez 15 minut, tak samo jak dostępna oferta. Puste oferty nie mają licznika pomijania ani wydłużania TTL. Tło wraca do nich po wygaśnięciu cache, również bez nowego kalendarza, żeby wykrywać pojawiające się rynki. Dotyczy to także meczów z ukończonym przygotowaniem historii lub brakami statystyk SofaScore. Dla spotkań z potwierdzonymi rynkami automatyczna analiza odświeża przeterminowane kursy podczas swojego następnego zaplanowanego podejścia, używając ważnych historii i kontekstów z cache oraz sprawdzając aktualny skład. Nieudane lub anulowane pobranie nie zastępuje poprawnej oferty. Archiwum analizy zawiera pierwotny czas pobrania, termin wygaśnięcia i informację o wykorzystaniu cache. Stare liczniki bukmacherów są ignorowane; dawne oferty kursów z TTL 30/60 minut nie są używane jako świeże.

### Zapamiętana dostępność rynków

Plik `data/market_availability_cache.json` na Debianie przechowuje osobno informację o obecności obsługiwanych rynków: bukmachera, ID meczu, listę rynków, czas sprawdzenia, drużyny i termin spotkania. Potwierdzona dostępność jest pamiętana do rozpoczęcia tego meczu i odczytywana po restarcie backendu. Otwarcie klienta Windows nie uruchamia ponownego sprawdzania tych ofert. Zmiana drużyn lub terminu powoduje ponowną kwalifikację. Dla oferty spoza kalendarza potwierdzenie ma limit 24 godzin. Brak rynków jest sprawdzany ponownie po 15 minutach, a błąd nieznanej oferty po minucie.

Ten zapis nie przedłuża ważności cen. Przy wyborze meczu oraz „Analizuj” kursy starsze niż 15 minut są pobierane z priorytetem użytkownika. Tymczasowy błąd sieci nie usuwa wcześniejszego potwierdzenia obecności rynków, ale blokuje analizę bez kompletu aktualnych ofert. Poprawna odpowiedź bez rynków zastępuje wcześniejsze potwierdzenie. Kalendarz pokazuje czas sprawdzenia i informację, że kursy są odświeżane przy analizie.

Podczas aktualizacji stare potwierdzenia są wiązane z aktualnymi kalendarzami; kompletny świeży cache kursów może również odtworzyć brakujący wpis dostępności. Log `[MARKET-CACHE] DISK_LOADED path=... entries=...` pokazuje miejsce zapisu i liczbę odczytanych rekordów, a `KEEP_KNOWN` wskazuje zachowanie potwierdzenia po błędzie odświeżenia. Katalog wskazany przez `-data` musi pozostać ten sam przy kolejnych uruchomieniach i aktualizacjach serwera.

Przy ręcznej weryfikacji sprawdź rynki lub poczekaj na przygotowanie meczu w tle, a następnie uruchom analizę tego samego meczu przed upływem 15 minut. W logu `[ODDS-CACHE] HIT` oznacza użycie zapisanej oferty, `FETCH` nowe pobranie, `JOIN` oczekiwanie na już trwające pobranie, a `STORED` zapis wyniku. Po wygaśnięciu cache kolejna analiza powinna pokazać `FETCH` dla danego bukmachera i ID meczu.

### Statystyki pobierane w tle

Serwer automatycznie przegląda zapisane kalendarze i wybiera mecze rozpoczynające się w najbliższych 48 godzinach (ruchome okno dwóch dób). Najpierw obsługuje mecze ze znanymi obsługiwanymi rynkami, w kolejności od najbliższego startu, następnie sprawdza pozostałe oferty również od najbliższego startu. Sama flaga BetBuildera nie wystarcza. Historie są pobierane tylko dla zawodników dopasowanych do obsługiwanych rynków; brak takich zawodników nie uruchamia pobierania całej kadry. Superbet automatycznie pobiera wszystkie polskie daty obejmujące pełne 48 godzin, zwykle dziś, jutro i pojutrze; uwzględnia również zmianę czasu. Dalsze daty pozostają dostępne na żądanie i nie są stale odświeżane w tle. STS i Betclic udostępniają pełne katalogi, z których tło wybiera wydarzenia w tym samym oknie. W logu `[STATS-BACKGROUND] PACING` parametr `horizon=48h0m0s` pokazuje aktywny horyzont.

Tło przygotowuje domyślną próbę 10 ostatnich spotkań, bez filtra rozgrywek (`ALL`). Ręczna analiza z tymi parametrami korzysta z tego samego cache; inna wielkość próby lub filtr rozgrywek mają własny wpis i są uzupełniane na żądanie. Ważność kursów i składów jest niezależna od historii zawodników: kursy korzystają z cache opisanego powyżej, a składy są sprawdzane przy analizie.

Jednocześnie wykonywany jest jeden mecz w tle, równolegle z zadaniem ręcznym oraz niezależnym kalendarzem i sprawdzaniem ofert. Ręczna analiza ma pierwszeństwo w kolejkach żądań SofaScore. Odczyty postępu, sprawdzenie zdrowia API i odczyty kalendarza z cache nie zajmują tych kolejek. Kolejne podejścia wykorzystują już zapisane kompletne historie zawodników.

Pobieranie SofaScore w tle jest seryjne: jeden zawodnik i jeden historyczny mecz naraz, najwyżej jedno żądanie sieciowe. W trybie Chromium tło ma dodatkową przerwę 500–1000 ms po zakończeniu żądania, a pomiędzy zadaniami 5–10 sekund. Zwykły transport HTTP zachowuje przerwę 2–4 sekundy po żądaniu i 30–60 sekund pomiędzy zadaniami. Dotyczy to także wyszukiwania drużyn, terminarzy i kontroli statusów spotkań w tle. Odczyty ważnego cache nie czekają na limit sieciowy. Oczekiwania tła są przerywane przy zatrzymaniu procesu, a jego kolejki ustępują pierwszeństwa użytkownikowi. Dotychczasowy limit 20 minut na zadanie tła pozostaje; kolejne podejście korzysta z kompletnych historii już zapisanych w cache.

Tylko odpowiedź SofaScore HTTP 429, także podczas ręcznej analizy, uruchamia wspólną przerwę w nowych żądaniach do tego źródła. Pierwsza trwa 15 minut, następne 30 i 60 minut; 60 minut to maksymalna automatyczna eskalacja. Po 24 godzinach bez nowej blokady licznik zaczyna od początku. Dłuższe `Retry-After` jest uwzględniane do 24 godzin. Odpowiedzi z żądań już wysłanych przed blokadą nie zwiększają licznika w tej samej przerwie. Stan jest zapisany w `data/sofa_request_backoff.json` i przetrwa restart. Podczas przerwy tło nie wysyła kolejnych żądań do SofaScore, a ręczne pobranie SofaScore zwraca komunikat z terminem ponowienia. Sprawdzanie ofert bukmacherów w tle pozostaje aktywne. Lokalny cache oraz pozostałe źródła danych pozostają dostępne. Logi `[STATS-BACKGROUND] PACING` oraz `[SOFA-BACKOFF] PAUSE`/`LOADED` pokazują konfigurację i termin wznowienia. HTTP 403 nie uruchamia wspólnej przerwy ani 15-minutowej przerwy danego endpointu kontekstu; ręczne próby można powtarzać od razu. Stare zapisane przerwy bez jawnego statusu 429 są ignorowane po aktualizacji, bez usuwania plików. Tło zachowuje zwykłe odstępy i harmonogram ponawiania nieudanych zadań. Spowolnienie ogranicza obciążenie, lecz nie gwarantuje, że źródło nie zwróci kolejnego challenge.

Cache historii jest powiązany z ID konkretnego meczu SofaScore. Po rozpoczęciu meczu serwer podczas bezczynności sprawdza jego status co około 5 minut; zakończenie lub anulowanie unieważnia historie sprzed tego spotkania. Gdy źródło nie odpowiada, zabezpieczeniem jest wygaśnięcie 4 godziny po zaplanowanym rozpoczęciu. Jeśli ręcznej analizy nie uda się powiązać z konkretnym meczem, używa osobnego cache ważnego najwyżej 24 godziny. Nowa wersja kluczy cache nie wykorzystuje starych wpisów z krótkim TTL; nie trzeba usuwać plików ręcznie.

Postęp tła i terminy meczów są zapisywane w `data/player_stats_background.json`, a historie i potwierdzone braki SofaScore w `data/sofa_player_stats_cache.json`. Restart zachowuje postęp. Oferty są ponownie sprawdzane w tle po wygaśnięciu zwykłego 15-minutowego TTL, z uwzględnieniem kolejki, priorytetu najbliższych meczów i pierwszeństwa ręcznych pobrań. Nowa wersja oferty może uzupełnić nowych zawodników; istniejące ważne historie są używane ponownie. Niepełne pobrania i błędy mają przerwę 15 minut przed ponowieniem; przerwanie przez ręczne żądanie ma przerwę 15 sekund. Archiwum zawiera również ofertę używaną do przygotowania statystyk i opisane historie zawodników.

Licznik kolejnych braków dotyczy wyłącznie historii statystycznej SofaScore: konkretnego zawodnika, nadchodzącego meczu, limitu próbki i filtra rozgrywek. Zwiększa się raz po rzeczywistej próbie, która potwierdziła brak zakończonych występów ze statystykami: pusta poprawna lista, brak pasujących występów albo brak szczegółów statystycznych (HTTP 404 lub poprawna odpowiedź bez minut występu). Jeśli choć jedna wymagana odpowiedź miała timeout, HTTP 403/429/5xx, niepoprawny JSON lub anulowanie, cała próba bez historii nie zwiększa licznika. Odczyt zapamiętanego braku również go nie zwiększa. Po pierwszym i drugim braku tło ponawia najwcześniej po 15 minutach; po trzecim pomija daną historię do końca powiązanego spotkania (bez powiązanego meczu najwyżej 24 godziny). Zakończenie lub przełożenie meczu aktualizuje ważność tak samo jak dla pozytywnych historii. Inni zawodnicy nadal są pobierani. Ręczna analiza omija negatywny cache i może sprawdzić ponownie; znalezienie statystyk zeruje licznik. Logi `[SOFA-STATS-MISS] OBSERVATION`, `SKIP` i `RESET` pokazują stan. Brak SofaScore nigdy nie wyłącza sprawdzania bukmachera.

W logach szukaj `[STATS-BACKGROUND] START`, `END`, `REUSE`, `YIELD`, `[SOFA-STATS-MISS]` oraz `[STATS-CACHE] FIXTURE_ENDED`. `unavailable` oznacza brak rynków bukmachera; `sofa-unavailable` oznacza brak wszystkich historii SofaScore, `sofa-partial` — dostępność części historii i oczekiwanie na kolejne próby brakujących, a `sofa-missing` — osiągnięcie limitu braków przy wszystkich nadal niedostępnych historiach. `sofa-paused` oznacza przerwę SofaScore po 429, podczas której oferty bukmacherów nadal są sprawdzane. Błędy i niepełne pobrania pozostają `error`/`partial`, a przerwanie przez pracę ręczną — `paused`. Przy ręcznej weryfikacji sprawdź ponowne `ODDS-CACHE FETCH` po wygaśnięciu pustej oferty oraz `SOFA-STATS-MISS SKIP` po trzech rzeczywistych brakach. Agent nie uruchamiał kodu ani testów.

Filtr „Tylko mecze z obsługiwanymi rynkami” korzysta z wyników odczytu rzeczywistych ofert tymi samymi parserami co analiza. Sama flaga BetBuildera nie kwalifikuje meczu. Każdy z trzech bukmacherów musi mieć przynajmniej jeden obsługiwany rynek zawodnika z poprawnym kursem; gole i asysty nie wystarczają. Klient odczytuje kwalifikację z kalendarza serwera, a skaner serwera sprawdza po jednej nieznanej lub wygasłej ofercie naraz. Do zakończenia pierwszego sprawdzania lista wyników może być niepełna; licznik pokazuje postęp. Błąd źródła jest osobnym stanem i nie oznacza braku rynków.

Filtr jest domyślnie włączony. Karty spotkań pokazują wiek sprawdzenia oraz informację o odświeżeniu kursów przy analizie albo terminie ponowienia pustej oferty. Potwierdzona dostępność jest zielona, potwierdzony brak czerwony (widoczny po wyłączeniu filtra), a wygasły lub nieznany wynik szary. Wiek danych na widocznych kartach aktualizuje się co 30 sekund bez pobierania ofert. Nowy wygląd wymaga przebudowania klienta Wails.

Wyniki sprawdzeń trafiają do `data/market_availability_cache.json` i archiwum. Potwierdzona dostępność jest pamiętana do rozpoczęcia meczu, brak rynków przez 15 minut, a błąd nieznanej oferty przez minutę. Każdy wpis dotyczy konkretnego bukmachera, ID meczu, drużyn i terminu. Ponowne wczytanie kalendarza lub przełączenie filtra korzysta z zapisanych potwierdzeń; log `[MARKET-CACHE] HIT` oznacza odczyt bez ponownego pobrania oferty. Klient jedynie odczytuje stan, a ponowienia nieznanych lub pustych ofert obsługuje skaner serwera. Sparsowana oferta użyta do sprawdzenia rynków trafia również do wspólnego cache kursów i może być wykorzystana przez analizę przez 15 minut. Cache cen i dostępności zachowują oryginalny czas obserwacji, lecz mają niezależne terminy ważności, również po restarcie. Bukmacherzy nie mają limitu kolejnych pustych odpowiedzi.

Analiza jest niezależnym zadaniem serwera. Klient odczytuje jej postęp krótkimi zapytaniami, dzięki czemu całe obliczenie nie zajmuje jednego długiego połączenia przez Cloudflare. Parametry, wyniki i status zadań są zapisywane w `data/analysis-jobs`.

Składy SofaScore są odczytywane także przed potwierdzeniem. Oznaczenie `XI` to potwierdzony podstawowy skład, a `XI?` to przewidywana jedenastka (`confirmed: false`). Przewidywani rezerwowi mają oznaczenie `POZA XI?`. Filtr XI obejmuje potwierdzonych i przewidywanych starterów; wyklucza zawodników o nieznanym statusie. Brak zawodnika na liście nie oznacza automatycznie ławki. Dopasowanie opiera się przede wszystkim na ID zawodnika. Archiwum zapisuje również ID meczu SofaScore i flagę potwierdzenia składów.

Automatyczne dopasowanie drużyn wymaga zgodnej nazwy lub sluga, dyscypliny i kategorii. Nazwy bez oznaczenia płci domyślnie dotyczą drużyn męskich; oznaczenia kobiet w nazwie albo rozgrywkach z cache kalendarza przełączają wyszukiwanie na drużyny żeńskie. Prefiks `RB` jest zachowywany jako `Red Bull`, aby nie łączyć różnych klubów. Brazylijskie oznaczenia stanów, np. `-rs` w Internacionalu i Grêmio, mogą być pominięte przy porównaniu z drużyną z Brazylii; nie są usuwane z kluczy cache. Wyszukiwarka nie wysyła zapytań o samą końcówkę `rs`. Drużyny młodzieżowe i rezerwy nie zastępują pierwszej drużyny.

Jeśli kilka zespołów nadal ma tę samą nazwę, serwer sprawdza terminarze kandydatów i wymaga jednej pasującej pary przeciwników. Termin wybranego meczu jest odczytywany z istniejącego cache kalendarza, bez ponownego pobierania oferty; jeśli jest dostępny, dopuszczalna różnica czasu wynosi 10 minut. Błąd pobrania terminarzy nie pozwala uznać częściowego sprawdzenia za jednoznaczny wynik. Przy braku potwierdzenia serwer nie wybiera pierwszego wyniku wyszukiwania.

Stary plik `sofa_team_cache.json` jest zachowany. Automatyczne wpisy bez potwierdzenia wersji 3 i kategorii w `sofa_team_identity_cache.json` są ponownie rozpoznawane przy użyciu; ręczne mapowania pozostają ważne. Poprawione ID trafia do cache. Analiza weryfikuje ID przed pobraniem statystyk. Gdy dopasowanie się powiedzie, zastępuje nieaktualne ID przesłane przez klienta lub zapisane w historii, także po usunięciu cache serwera. Korekta pojawia się w ostrzeżeniach i parametrach zadania, a dalsze obliczenia korzystają z poprawionego ID. W logu `[SOFA-SEARCH] CANDIDATE` zawiera również płeć, dyscyplinę i kraj, a `[SOFA-DETECT] FIXTURE` wskazuje mecz rozstrzygający niejednoznaczność. Wpisy `[SOFA-EVENT]` i `[SOFA-LINEUP]` pokazują wybór meczu, pobranie składów i dopasowane statusy. Gdy nie ma danych o XI, klient wyjaśnia pusty wynik filtra.

Archiwum znajduje się w `data/archive`. Katalogi analiz zawierają datę, nazwy drużyn i identyfikator zadania. Nazwy plików opisują źródło, zakres danych, rodzaj odpowiedzi i czas UTC. Większe pliki są kompresowane gzip; surowy protobuf pozostaje binarny, obok znajduje się jego reprezentacja JSON.

Kursy mają wspólne archiwum `data/archive/oferty-kursow/<bukmacher>/mecz-<id>/wersja-<hash>/`. Pełna sparsowana oferta jest zapisywana raz dla konkretnej wersji danego meczu i bukmachera. Nazwa pliku zawiera bukmachera, nazwy drużyn, ID meczu i czas pierwszej obserwacji. Zmiana kursu, linii, zawodnika, rynku, strony zakładu lub identyfikatora selekcji tworzy inną wersję; sama kolejność elementów, etykiety i techniczne znaczniki czasu nie tworzą kopii. Powrót A → B → A korzysta ponownie z pierwszego pliku A. Deduplikacja działa również po restarcie.

Każde rzeczywiste sprawdzenie oferty oraz użycie kursów z cache dopisuje mały wiersz do dziennego pliku `data/archive/obserwacje-kursow/<data>/<bukmacher>_sprawdzenia-i-uzycie-kursow.jsonl`. To czytelny tekst: jeden obiekt JSON na linię. Pole `source` rozróżnia `bukmacher` i `cache`, `recordedAt` oznacza czas zapisu obserwacji, a `observedAt` pierwotny czas pobrania kursów. `archiveVersion` wskazuje plik oferty względem `data/archive`. Nie dochodzi do dodatkowych zapytań sieciowych ani przedłużenia TTL. Log `[ODDS-ARCHIVE] OBSERVATION` pokazuje źródło i `new_version`.

Archiwa ręcznej analizy i przygotowania statystyk zapisują odwołanie `archiveVersion` i liczbę rynków zamiast kolejnej pełnej listy kursów. W danych wejściowych analizy odwołania do ofert znajdują się w `oddsSources`. Surowe odpowiedzi z poprawnego pobrania nowej wersji są zapisywane raz w jej podkatalogu `surowe-zrodlo`; przy ofercie identycznej z już zapisaną pozostaje tylko obserwacja. Nieudane pobrania nadal zachowują odpowiedzi diagnostyczne. Wersja utworzona ze starego cache bez zapytania sieciowego może nie mieć surowej odpowiedzi. Cache roboczy wciąż zawiera pełne kursy, aby analiza nie musiała czytać archiwum. Zmiana dotyczy nowych zapisów; istniejące archiwum nie jest usuwane ani przerabiane, a archiwizacja statystyk pozostaje bez zmian.

Odczyt wybranego pliku JSON:

```sh
gzip -dc /sciezka/do/pliku.json.gz | less
```

Analiza ręczna obsługuje wybrany mecz, a tło automatycznie wycenia kwalifikujące się wydarzenia z najbliższych 48 godzin. Automatyczne powiadomienia pozostają kolejnym etapem.

## SofaScore przez działającą przeglądarkę — Chromium/CDP

Tryb `-sofa-browser http://127.0.0.1:9222` wysyła zapytania SofaScore przez `fetch` wewnątrz istniejącej karty Chromium na Debianie. Korzysta z rzeczywistego stosu sieciowego i cookies tej przeglądarki. Backend nadal parsuje odpowiedzi, liczy wyniki i obsługuje dotychczasowy cache oraz archiwum. Kod znajduje się w tym samym `value_bets.go`, bez Rod i nowych zależności. Tryb jest opcjonalny: brak flagi i zmiennej środowiskowej zachowuje dotychczasowy transport HTTP. Włączony tryb nie przełącza się sam na HTTP po błędzie.

Najpierw zamknij Chromium korzystające z profilu `/home/vstkxj/.local/share/sofascore-browser`. Uruchom je ponownie z debugowaniem. Jeśli dotychczas VNC uruchamiało bezpośrednio Chromium, odtwórz tę sesję poniższymi poleceniami, a potem połącz ponownie viewer VNC (tunel SSH na port 5901 pozostaje ten sam):

```sh
tigervncserver -kill :1
tigervncserver :1 -localhost yes -geometry 1440x900 -- /usr/bin/dbus-run-session /usr/bin/chromium --user-data-dir=/home/vstkxj/.local/share/sofascore-browser --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 --start-maximized https://www.sofascore.com/
```

Jeśli masz pełny pulpit VNC, wystarczy po zamknięciu poprzedniego Chromium uruchomić w terminalu tego pulpitu:

```sh
chromium --user-data-dir=/home/vstkxj/.local/share/sofascore-browser --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 --start-maximized https://www.sofascore.com/
```

Używaj tego samego profilu, na którym działało ręczne pobieranie JSON. Oddzielny `--user-data-dir` jest także wymagany dla debugowania nowszego Chrome ([dokumentacja Chrome](https://developer.chrome.com/blog/remote-debugging-port)). Samo ponowne uruchomienie polecenia, gdy profil jest już otwarty w Chromium bez debugowania, nie włączy portu. Port 9222 pozostaje dostępny tylko lokalnie; nie dodawaj go do Cloudflare Tunnel ani reguł dostępu z internetu.

W przeglądarce otwórz SofaScore, w razie potrzeby rozwiąż challenge ręcznie i pozostaw jedną zwykłą kartę strony. Backend nie rozwiązuje CAPTCHA, nie otwiera nowych kart i nie zmienia adresu istniejącej. Zamknięcie viewer VNC pozostawia sesję serwera VNC działającą; Chromium i karta SofaScore muszą pozostać uruchomione.

Po ręcznym wgraniu źródła na Debian zbuduj binarkę i wykonaj pojedynczą diagnostykę (bez uruchamiania API ani pobierania kalendarzy):

```sh
cd /home/vstkxj/value-bets
go build -o value_bets.new value_bets.go
./value_bets.new -sofa-diagnose -sofa-browser http://127.0.0.1:9222 -data ./data
```

Przy włączonym trybie przeglądarkowym `-sofa-diagnose` wykonuje jedno wyszukiwanie Chicago Fire przez Chromium, zamiast czterech wariantów HTTP. Oczekiwany rezultat: `[SOFA-BROWSER] DIAGNOSTIC status=200 teams=... error=<nil>` z dodatnią liczbą drużyn. Raport trafia do `data/diagnostics/sofascore_chromium-wyszukiwanie-chicago-fire_...json`, a odpowiedź do dotychczasowego archiwum. Skuteczność wymaga tego ręcznego sprawdzenia; uruchomienie DevTools samo nie dowodzi, że API zaakceptuje sesję.

Po udanej próbie podmień binarkę i ustaw tryb dla usługi:

```sh
mv value_bets.new value_bets
sudo systemctl edit value-bets
```

W edytorze override wpisz:

```ini
[Service]
Environment="VALUE_BETS_SOFA_BROWSER=http://127.0.0.1:9222"
```

Następnie:

```sh
sudo systemctl daemon-reload
sudo systemctl restart value-bets
journalctl -u value-bets -n 80 --no-pager
```

W logu startowym pojawia się `[SOFA-BROWSER] CONFIGURED`, a przy pobraniu `ATTACHED`, `REQUEST` i `[SOFA-HTTP] ... protocol="Chromium/CDP"`. Te same zapytania wykorzystują cache i archiwizację stosowane dotychczas. Nie trzeba przebudowywać klienta Wails. Tryb przeglądarkowy ignoruje pliki cURL/captcha oraz `VALUE_BETS_SOFA_CAPTCHA`; korzysta z cookies aktualnej przeglądarki i pobiera z nich `sofa_captcha` także do nagłówka `X-Captcha`. Nie zapisuje ani nie loguje tokenów i cookies. Opcjonalne `VALUE_BETS_SOFA_REQUESTED_WITH` nadal jest przekazywane, jeśli zostało ustawione; bez niego nagłówek jest pomijany.

Wszystkie zapytania przez Chromium są wykonywane pojedynczo, domyślnie z odstępem 100–200 ms po zakończeniu poprzedniego. Tło korzysta z tej samej kolejki z niższym priorytetem i dodatkowym odstępem 500–1000 ms, a przerwa między zadaniami tła wynosi 5–10 sekund. Zwykły transport HTTP zachowuje wcześniejsze wolniejsze tempo. Rozwiązanie CAPTCHA nie gwarantuje, że blokada nie pojawi się ponownie. Przy HTTP 403 nie ma czasowej ani ręcznie zwalnianej blokady: rozwiąż challenge w istniejącej przeglądarce i ponów analizę. Niekompletne historie nie są oznaczane jako kompletne.

Minimalny odstęp można ustawić przez `VALUE_BETS_SOFA_BROWSER_GAP_MS` lub flagę `-sofa-browser-gap-ms` (zakres 0–10000, domyślnie 100). Rzeczywista przerwa ma jitter od tej wartości do jej dwukrotności; 0 wyłącza dodatkową przerwę, ale zachowuje jedno zapytanie naraz. Flaga ma pierwszeństwo przed środowiskiem. Zmiana wymaga restartu backendu. Log `CONFIGURED ... gap=100ms-200ms` pokazuje zastosowane tempo. W razie częstych challenge można zwiększyć tę wartość bez edycji kodu.

Pobranie w przeglądarce ma limit 15 sekund, a podłączenie wraz z pobraniem 20 sekund (bez czasu kolejki). Odpowiedź ma limit 32 MiB. Anulowanie zadania przerywa oczekiwanie i wysyła `AbortController.abort()` do przeglądarki; wewnętrzny timeout ogranicza pobranie również przy utracie połączenia z DevTools. HTTP 429 zachowuje dotychczasową przerwę. Błędy podłączenia przeglądarki nie dostają 15-minutowego cache błędów kontekstu.

Do ręcznej weryfikacji sprawdź diagnostykę, następnie jedną analizę, ponowienie korzystające z cache i anulowanie analizy. Sprawdź również komunikat po zamknięciu karty i wznowienie po jej ponownym otwarciu. W bieżącej aktualizacji skompilowano backend i klienta Windows oraz sprawdzono składnię JavaScript. Testów nie uruchamiano.

### Automatyczny start Chromium/VNC po restarcie Debiana

Plik `sofascore-browser.service` jest gotową konfiguracją systemd dla użytkownika `vstkxj` i istniejącego profilu `/home/vstkxj/.local/share/sofascore-browser`. Skopiuj go z projektu obok `value_bets.go` do `/home/vstkxj/value-bets` na Debianie. Nie zastępuje plików cache ani profilu przeglądarki. Usługa uruchamia TigerVNC na `:1` w trybie foreground (`-fg`), dzięki czemu systemd zarządza czasem życia całej sesji ([podręcznik Debian TigerVNC](https://manpages.debian.org/trixie/tigervnc-standalone-server/tigervncserver.1.en.html)). VNC jest dostępne wyłącznie lokalnie na 5901, DevTools na 9222. Pozostaje dotychczasowy dostęp przez tunel SSH i viewer VNC.

Instalacja, wykonywana ręcznie na Debianie, po wgraniu źródła backendu i pliku usługi:

```sh
cd /home/vstkxj/value-bets
go build -o value_bets.new value_bets.go
sudo systemctl stop value-bets
tigervncserver -kill :1
mv value_bets.new value_bets
sudo install -m 0644 sofascore-browser.service /etc/systemd/system/sofascore-browser.service
sudo systemctl edit value-bets
```

W edytorze override zachowaj inne potrzebne ustawienia, a poniższe sekcje ustaw tak:

```ini
[Unit]
Wants=sofascore-browser.service
After=sofascore-browser.service

[Service]
Environment="VALUE_BETS_SOFA_BROWSER=http://127.0.0.1:9222"
Environment="VALUE_BETS_SOFA_BROWSER_GAP_MS=100"
```

Nie używaj `--full` dla tego override; pełna główna usługa backendu powinna już zawierać `ExecStart`, jak we wcześniejszej sekcji. Następnie:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now sofascore-browser
sudo systemctl restart value-bets
sudo systemctl status sofascore-browser value-bets --no-pager -l
sudo journalctl -u sofascore-browser -u value-bets -n 100 --no-pager
```

Pierwsze zatrzymanie VNC rozłączy viewer; po uruchomieniu usługi połącz się ponownie, korzystając z tego samego tunelu. Jeśli `tigervncserver -kill :1` zgłosi, że sesji nie ma, przejdź dalej. Nie uruchamiaj równolegle ręcznego Chromium z tym samym profilem ani drugiego VNC na `:1`. Plik usługi zakłada, że hasło VNC zostało już ustawione przez tego samego użytkownika, jak podczas dotychczasowego ręcznego uruchomienia. Nie stosuje `--no-sandbox` ani uprawnień root dla Chromium.

Usługa restartuje sesję także po zamknięciu Chromium (po 10 sekundach), zachowując profil. Rozwiązanie challenge nadal jest ręczne. Backend jest uruchamiany po rozpoczęciu usługi przeglądarki; to nie jest gwarancja gotowości strony ani DevTools. Jeśli pierwszy odczyt trafi na rozruch, ponów go po otwarciu strony. Restart samego backendu nie restartuje Chromium, więc zachowuje działającą sesję. Automatyczny start nie wymaga zalogowania przez SSH/VNC.

Ręcznie sprawdź analizę po instalacji, zamknięcie viewer VNC i dalsze pobieranie, a następnie działanie obu usług po restarcie Debiana. W logach backendu oczekuj `protocol="Chromium/CDP"` i HTTP 200. Osobną konfigurację przeglądarki możesz zatrzymać przez `sudo systemctl stop sofascore-browser`; automatyczne ponowne uruchamianie nie przeszkadza w takim celowym zatrzymaniu. Kod i konfiguracja usługi nie były uruchamiane ani testowane przez agenta.

## Nagłówki i import sesji dla zwykłego transportu HTTP

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

## Fair kurs i podgląd drużyn — context-hit-v3

Fair kurs i historyczny hit rate są liczone osobno. Model wycenia trafienia konkretnej linii w rzeczywistych występach, bez per90 i bez przeliczania przez przewidywane minuty. Liczność ponad linię jest jednym trafieniem, niezależnie od zapasu. Zero z rozegranego występu, również przy 90 minutach, jest pełnym nietrafieniem over. Mechanizm SuperSub, jego dane i przełącznik wykresu zachowują dotychczasowe działanie. Podobnie jak wcześniej fair kurs jest obliczany z bazowych statystyk zawodnika; przełącznik SuperSub zmienia podgląd próbki, bez ponownej wyceny zakładu.

Średnia zawodnika jest ważona według aktualnej pozycji ze składu oraz miejsca meczu SofaScore (home/away, także przy odwróconej kolejności u bukmachera). Inna znana pozycja otrzymuje wagę 0,5, inne znane miejsce 0,7; brak danych nie powoduje kary. Następnie model porównuje profil obecnej drużyny i przeciwnika z profilami drużyn występujących w próbce zawodnika. Porównania matchupu obejmują tylko tę samą ligę i sezon.

Wykorzystywane są liczniki właściwe dla rynku: strzały i strzały dopuszczane, celne i dopuszczane celne, niecelne, zablokowane, strzały z wewnątrz/poza polem karnym, odbiory, przechwyty i spalone. Dla fauli otrzymanych używane są faule popełniane przez rywala. Każdy licznik drużynowy jest dzielony przez liczbę meczów drużyny, nigdy przez minuty zawodnika. Dla fauli popełnionych, strat piłki oraz celnych strzałów z konkretnych stref brakuje bezpośrednio porównywalnego pełnego profilu w dostarczonej mapie; te rynki korzystają z roli i home/away, z neutralną korektą matchupu. Brak konkretnego pola nie jest traktowany jako zero.

Korekta porównuje aktualną wartość z ważoną średnią profili w próbce. Logarytm ilorazu ma wagę 0,5 na każdy dostępny czynnik, wygładzanie 0,25 i osłabienie `n/(n+8)` zależne od liczby spotkań sezonu. Niepełne pokrycie historii dodatkowo zmniejsza wpływ korekty. Cały mnożnik jest ograniczony do 0,8–1,25. To jawne parametry początkowe, a nie wytrenowane współczynniki. Profile dawnych rywali opisują stan sezonu w momencie obecnej analizy; nie są rekonstrukcją stanu sprzed historycznego meczu.

Prawdopodobieństwo bazowe wynika z ważonego udziału trafień i wygładzenia Jeffreysa: `(udziałTrafień * effectiveSample + 0.5) / (effectiveSample + 1)`. Wagi pozycji i home/away pozostają. Mnożnik matchupu zmienia iloraz szans over: `p/(1-p) * matchupFactor`; under jest dopełnieniem. Ograniczenie 0,005–0,995 jest symetryczne. Średnia i oczekiwana liczność są informacją opisową; duży zapas nie podnosi wyceny. Model jest eksperymentalny i wymaga oceny kalibracji na wynikach; raporty rozdzielają wersję `context-hit-v3` od wcześniejszej `context-count-v2`.

Rating zawodnika pojawia się pod datą każdego meczu na wykresie i w dymku słupka, wraz ze średnią ratingów dostępnych w próbce. Brak ratingu to kreska, nie zero. Rating jest odczytywany ze statystyk meczu albo z `statisticsMap` historii; ocena zmiennika nie zastępuje oceny zawodnika. Rozwijane „Fair kurs i kontekst drużyn” pokazuje średnią, mnożnik matchupu, oczekiwaną liczność, rozkład, efektywną próbkę i wykorzystane czynniki. Osobny podgląd zawiera tabelę ogólną i home/away, formę, posiadanie, xG i rating drużyny. Rating, tabela, forma, posiadanie oraz xG nie wchodzą do wzoru fair. Przy tabeli poniżej 8 meczów widnieje ostrzeżenie o małej próbce.

Profile sezonowe są ważne do wykrycia kolejnego zakończonego meczu danej drużyny; nie mają już 6-godzinnego TTL. Metadane meczu, tabele i forma nadal mają godzinę w `data/matchup_cache`. Pliki są współdzielone między analizami tej samej ligi i drużyn oraz zachowywane po restarcie. Błędy źródła mają przerwę 15 minut w pamięci procesu (z wyjątkiem HTTP 403 i błędów konfiguracji); nie nadpisują poprawnego cache ani nie zamieniają braków w zera. Jedna analiza pobiera najwyżej 32 profile drużyn. Składy nadal są odświeżane osobno. Cache historii zawodnika w wersji 6 zawiera rating i identyfikatory kontekstu. Nie usuwaj cache ręcznie.

Archiwum `kursy-statystyki-sklady-matchup-uzyte-do-obliczen` zapisuje profile z czasami pobrania, pozycje i kontekst historii. Wynik ma wersję modelu, obliczone prawdopodobieństwo i uzasadnienie. Późniejsza ocena modelu musi używać tych zapisanych danych sprzed predykcji, zamiast pobierać aktualne agregaty do dawnych spotkań. Szczegóły oraz ratingi są również zachowywane w zakładkach klienta. Starsze zapisane analizy pozostają czytelne, lecz nie dostają nowych pól bez ponownej analizy.

Ręczna weryfikacja po wgraniu backendu i przebudowaniu Wails: przeanalizuj mecz ze znanym składem, sprawdź ratingi, czynniki i tabelę w szczegółach, potem powtórz analizę i sprawdź wykorzystanie cache. W logu `[MATCHUP]` widać ID ligi i sezonu oraz liczbę profili i pozycji. Sprawdź też brakującą ocenę, wczesny sezon, mecz bez składów oraz zapis/odczyt zakładki. Przełączanie SuperSub powinno nadal zmieniać tylko dotychczasowy podgląd próbki i mapę strzałów.

## Przebieg spotkań i reakcje — match-flow v1

Analiza pobiera do 5 ostatnich zakończonych spotkań każdej drużyny w rozgrywkach nadchodzącego meczu, z pierwszej strony `/team/{id}/events/last/0`. Łączy `/event/{id}/graph` z `/event/{id}/incidents`. Wspólny mecz obu drużyn jest pobierany raz. Brak wykresu jest oznaczany jako brak danych, bez zastępowania zerami. Dotyczy to także lig bez funkcji momentum. Przebieg jest informacją pomocniczą; nie zmienia obecnego wzoru fair ani SuperSuba.

Momentum jest indeksem przebiegu gry, nie liczbą strzałów, posiadaniem ani xG. [Opis SofaScore](https://www.sofascore.com/news/how-live-attack-momentum-works-at-the-world-cup). Dodatnie wartości źródła odnoszą się do gospodarzy; przy podglądzie gości znak jest odwracany. Podsumowanie prowadzenia, remisu i straty pokazuje średnie saldo oraz udział dostępnych punktów wykresu z przewagą wybranej drużyny. Próba jest podana jako liczba punktów i meczów; nie udaje dokładnego czasu gry.

Oś wyniku powstaje z rosnąco uporządkowanych goli. Zapisane wyniki po golach muszą tworzyć spójną sekwencję od 0:0 do wyniku po regulaminowym czasie. Gol samobójczy jest przypisywany według zmiany wyniku, a anulowane incydenty są pomijane. Przy niezgodności wykres nadal jest dostępny, lecz klasyfikacja stanów i agregaty reakcji są wyłączone. Przetwarzane jest 90 minut; dogrywka i seria karnych są pomijane.

Reakcje obejmują zdobycie i stratę bramki, objęcie i utratę prowadzenia, wyrównanie, żółte i czerwone kartki oraz zmiany obu drużyn. Każdy incydent porównuje średnie momentum z maksymalnie 5 minut przed i po. Okno kończy się na kolejnym incydencie lub przerwie. Minuta samego incydentu jest wyłączona, bo kolejność akcji w tej minucie jest nieznana. Doliczony czas w wykresie może występować jako punkt ułamkowy, np. `45.5`; takie punkty są pokazywane, ale nie są traktowane jako pełne minuty w agregatach. Incydenty `45+N` i `90+N` nie dostają fikcyjnych okien reakcji. Co najmniej 3 zwykłe punkty po każdej stronie są wymagane do agregowania reakcji. Nakładające się incydenty w tej samej minucie są wyłączane z agregatów reakcji. Wyjątkiem jest pakiet zmian jednej drużyny, liczony jako jedno okno. Różnica przed/po opisuje obserwację, bez dowodu, że incydent spowodował zmianę.

W kliencie rozwijane „Fair kurs i kontekst drużyn” ma karty kursu fair, prawdopodobieństwa i EV, trzy kroki wyceny oraz tabelę porównującą drużyny. Założenia i komunikaty o dostępności są zwijane. Sekcja przebiegu ma wybór drużyny i historycznego spotkania, podsumowanie stanów wyniku, tabelę reakcji oraz wykres z klikalnymi znacznikami goli, kartek i zmian. Zielona część wykresu dotyczy wybranej drużyny, czerwona rywala. Kliknięcie znacznika wskazuje wiersz incydentu. Dymki pokazują zawodników i wartości wykresu.

Pełne przebiegi są zapisane w `data/match_flow_cache/mecz-{id}/wersja-{hash}/przebieg-meczu.json`. Podsumowania analizy i zakładek zawierają tylko odwołania do konkretnej wersji. Pełny wykres jest pobierany przez klienta osobno, przez odczytowe `GET /api/v1/match-flow?eventId=...&revision=...`, które nie uruchamia zapytań do SofaScore. Otwieranie wielu zakładów tego samego meczu korzysta również z pamięci klienta. Zapobiega to powielaniu setek punktów wykresu przy każdym rynku.

Poprawnie zapisany przebieg zakończonego meczu nie wygasa i jest odczytywany z dysku bez ponownego pobierania wykresu i incydentów. Lista ostatnich spotkań zmienia się po wykryciu nowego zakończonego meczu drużyny. Analiza pobiera tylko brakujące przebiegi; starsze zostają w cache i archiwum, nawet po wypadnięciu z ostatniej piątki. Niepełny albo niedostępny przebieg może zostać ponownie sprawdzony po kolejnym zakończonym meczu, bez stałego odpytywania co kilkadziesiąt minut. Czytelne archiwum przetworzonego przebiegu trafia do `data/archive/przebieg-meczow/mecz-{id}/wersja-{hash}/{druzyny}_przebieg-meczu.json.gz`, raz dla danej treści. Zapisane analizy i zakładki nadal wskazują konkretną używaną wersję. Surowe odpowiedzi są archiwizowane przez dotychczasowy transport SofaScore. Nie usuwaj zapisanych wersji, jeśli zakładki mają nadal otwierać ich wykresy.

Stan drużyny jest zapisany w `data/matchup_cache/druzyna-{id}_stan-po-meczu.json`: zawiera ostatnio odczytaną listę spotkań, znacznik ostatniego zakończonego meczu i termin następnej kontroli terminarza. Ten sam stan jest używany przez uzupełnianie kadry, listę przebiegów i sezonowy profil drużyny. Kontrola statusu znanego meczu oznacza obie drużyny do aktualizacji po jego zakończeniu. Bez takiego sygnału sprawdzenie następuje przy kolejnym użyciu po przewidywanym końcu meczu (start +2 godziny 15 minut); gdy spotkanie jeszcze trwa albo lista źródła nie uwzględnia potwierdzonego zakończenia, ponowienie jest za 5 minut. Przy braku bliskiego spotkania terminarz jest kontrolowany najwyżej raz na dobę, żeby wykrywać nowe terminy i przełożenia. Są to lekkie kontrole list last/next, nie ponowne pobieranie profili i wszystkich historycznych wykresów. Błąd terminarza może spowodować wcześniejsze ponowienie; HTTP 403 nie nakłada pauzy na ręczne próby.

Po zmianie znacznika profil sezonowy odświeża się przy kolejnym zapotrzebowaniu. Starszy i nowy profil oraz listy spotkań są archiwizowane w `data/archive/kontekst-druzyn/druzyna-{id}/{opis}/wersja-{hash}/{opis}.json.gz`, z deduplikacją identycznej treści. Pierwsza analiza po aktualizacji ustali stan terminarza i jednorazowo odświeży stare profile bez znacznika; istniejące zapisane przebiegi są od razu używane. Logi `[TEAM-CONTEXT] MATCH_ENDED`, `NEW_FINISHED` i `CHECKED` pokazują przyczynę aktualizacji i termin kontroli.

Do ręcznej weryfikacji wgraj backend i przebuduj klienta. Uruchom nową analizę, otwórz szczegóły i przełącz drużynę oraz historyczny mecz. Sprawdź wynik, znaki wykresu po przełączeniu gospodarze/goście, znacznik gola i okno reakcji, kartki, zmiany oraz incydent w doliczonym czasie. Powtórna analiza powinna korzystać z cache. W logu `[MATCH-FLOW]` widać liczbę wybranych i dostępnych meczów oraz typów reakcji. Starsze zakładki bez przebiegów pozostają czytelne i wymagają nowej analizy do ich uzupełnienia. Kod nie był uruchamiany ani testowany przez agenta.

## Wyświetlanie zawodników przed kontekstem

Serwer najpierw odczytuje składy i pobiera historie zawodników z obsługiwanych rynków należących do predicted / confirmed XI obu drużyn. Korzysta bezpośrednio z ID zawodników w składzie, więc pierwszy podgląd nie czeka na odczyt całej kadry i składów historycznych. Po publikacji statystyk XI pobiera kontekst drużyn, tabele i przebiegi oraz publikuje wycenę XI z fair kursem i EV. Dopiero potem pobiera ławkę i pozostałych zawodników z oferty. Rezerwowi korzystają z przygotowanego kontekstu; dla nowych historycznych rywali uzupełniane są jedynie brakujące profile sezonowe. Tabele, forma i przebiegi nie są pobierane ponownie. Łączny limit wynosi 32 próby pobrania profili na analizę, ze starterami jako pierwszymi. Zawodnik jest pobierany najwyżej raz w danej analizie; cache historii i jego ważność do końca spotkania pozostają bez zmian. Automatyczna analiza w tle korzysta z tego samego porządku i wzoru wyceny. Jeśli SofaScore nie udostępnia składu lub jego klasyfikacji albo nie ma dostępnych historii XI, analiza przechodzi do uzupełniania kadry, a kontekst pobiera po historiach zawodników.

Klient pokazuje zawodników, kursy bukmacherów, hitrate, próbkę i wykresy już po etapie XI. Do ukończenia kontekstu XI fair kurs, edge, EV i ocena są oczekujące; potem starterzy mają wycenę i mogą zostać zapisani, podczas gdy analiza jeszcze pobiera rezerwę. Kolejne podglądy rozszerzają listę i wycenę, także na otwartym wykresie. Komunikaty postępu rozróżniają XI, kontekst i wycenę XI, rezerwę oraz jej wycenę. Uzupełnianie wyników zachowuje filtry użytkownika, a domyślny zakres kursów rozszerza się wraz z nowymi rynkami. Przerwanie późniejszego etapu zachowuje ostatni opublikowany podgląd z wycenionym XI. Archiwum `kursy-statystyki-sklady-matchup-XI-uzyte-do-obliczen` zapisuje wejścia modelu jeszcze przed pobieraniem rezerwy.

API zwraca `resultRevision` i opcjonalny podgląd `result` w trwającym zadaniu. Klient przekazuje `afterRevision`, aby nie pobierać wielokrotnie tej samej dużej próbki. Log `[SOFA-PLAYERS] PHASE` pokazuje początek etapu (`statistics-xi`, `statistics-bench`, `statistics-other`), a `ANALYSIS_PLAYERS_READY` — publikację podglądu i numer jego wersji. Podgląd w etapie `matchup-xi` zawiera jeszcze oczekującą wycenę; następny, `statistics-bench`, ma już kontekst i wycenę XI. Jeśli dalszy etap zakończy się błędem, wcześniej odebrany podgląd pozostaje widoczny. Zmiana wymaga aktualizacji backendu i przebudowania klienta. Agent nie uruchamiał kodu ani testów.

## Aktualnie przechowywany cache

Ścieżki serwerowe poniżej są względne wobec katalogu `-data`. TTL określa świeżość danych, nie automatyczne usuwanie plików.

| Dane | Miejsce | Ważność |
| --- | --- | --- |
| Pełne kalendarze bukmacherów | `bookmaker_calendar_cache.json` | 12 godzin; harmonogram odświeża dwa razy na dobę. Przy istniejącym starym wpisie API może zwrócić go z ostrzeżeniem. |
| Sparsowane kursy pojedynczego meczu | `bookmaker_match_odds_cache.json` | 15 minut także dla pustych ofert, bez limitu ponowień. Trafienie w cache nie przedłuża TTL. |
| Potwierdzone braki historii zawodnika SofaScore | `sofa_player_stats_cache.json` | Pierwsze dwa braki: ponowienie w tle po 15 minutach. Od trzeciego: pomijanie do końca powiązanego meczu; ręczna analiza może ponowić od razu. |
| Dostępność obsługiwanych rynków | `market_availability_cache.json` | Potwierdzenie do rozpoczęcia konkretnego meczu; brak rynków 15 minut, błąd nieznanej oferty 1 minuta. Niezależne od ważności cen. |
| Pełne historie zawodników: statystyki, minuty, pozycje, ratingi, mapy strzałów i dane SuperSub | `sofa_player_stats_cache.json` | Powiązane z nadchodzącym meczem i unieważniane po rozpoznaniu jego zakończenia/anulowania. Awaryjny termin: start +4 godziny; bez powiązanego meczu 24 godziny. Klucz uwzględnia mecz, zawodnika, limit próbki i rozgrywki. Niepełne historie nie są zapisywane jako poprawny cache. |
| ID i zweryfikowana tożsamość drużyn | `sofa_team_cache.json`, `sofa_team_identity_cache.json` | Bez czasowego TTL; obowiązuje weryfikacja tożsamości i wersja formatu. |
| Sezonowe profile drużyn do matchupu | `matchup_cache/*.json` | Do kolejnego wykrytego zakończonego meczu drużyny; potem odświeżenie przy kolejnym użyciu. |
| Metadane nadchodzącego meczu, tabele total/home/away, forma i wyniki home/away | `matchup_cache/*.json` | 1 godzina. |
| Lista ostatnich spotkań używana do pobierania przebiegów i kadry | `matchup_cache/druzyna-{id}_stan-po-meczu.json` | Powiązana z zakończonymi meczami, kontrola terminarza według kolejnego spotkania; bez bliskiego terminu najwyżej raz na dobę. |
| Przebiegi zakończonych spotkań, wykresy i incydenty | `match_flow_cache/` oraz odpowiedzi w `matchup_cache/` | Poprawnie zapisany przebieg bez TTL. Brakujące dane ponownie sprawdzane po zmianie znacznika ostatniego zakończonego meczu. Starsze wersje pozostają dostępne dla wcześniejszych analiz i zakładek. |

Klient dodatkowo przechowuje kalendarze w pamięci przez 10 sekund, dostępność rynków według serwerowego `expiresAt` oraz odczytane wersje przebiegów na czas działania aplikacji. Co 15 sekund odczytuje kalendarz z cache serwera wraz z aktualnym stanem rynków, również podczas analizy i wyboru meczu. Kadry i aktualne składy XI nie mają osobnego trwałego cache i są pobierane ponownie przy analizie. Wyniki wyszukiwania drużyn nie mają osobnego cache odpowiedzi; zachowywane są dopasowane ID.

`player_stats_background.json` przechowuje stan kolejki i powiązania z nadchodzącymi meczami; status rozpoczętego meczu jest sprawdzany co około 5 minut. `analysis-jobs/` zawiera zapisane wyniki zadań, a klient zachowuje osobiste zakłady i historię. To trwałe zapisy, nie cache z TTL. Tak samo `archive/` służy historii, a nie unikaniu kolejnych zapytań: identyczne wersje kursów są deduplikowane, a sprawdzenia zapisywane jako małe obserwacje.

## Automatyczna wycena, prognozy i rozliczenia

Tło wykonuje teraz pełną analizę dla dostępnych obsługiwanych rynków w ruchomym oknie 48 godzin. Używa tej samej wyceny `context-hit-v3` co analiza ręczna, próby 10 meczów i filtra `ALL`. Kolejność pozostaje XI → kontekst → rezerwa. Historie i konteksty korzystają ze swoich dotychczasowych cache; aktualizacja oferty nie oznacza ponownego pobrania wszystkich statystyk. Tło działa przy zamkniętym Wails i ustępuje pobraniom ręcznym. SofaScore 429 wstrzymuje jego pobrania, ale sprawdzanie bukmacherów nadal działa. Automatyczne analizy są zapisywane z `origin=automatic`; ręczne z `origin=manual`.

Kalendarz Wails pokazuje na każdej karcie liczbę **Value** i **Pułapek**, wiek analizy oraz stan pobierania, niepełnych danych lub błędu. Liczniki dotyczą oferty wybranego bukmachera. Brak gotowej analizy nie jest przedstawiany jako zero okazji. Odczyty liczników co 15 sekund nie przerywają pracy tła i nie pobierają danych u źródeł.

`data/predictions.sqlite` przechowuje niezmienne prognozy sprzed startu: kurs i czas jego pobrania, linię, kierunek, ID zawodnika i wydarzenia, fair kurs, prawdopodobieństwo, historyczny hitrate, ocenę VALUE/TRAP/NEUTRAL, kompletność próbki i wersję algorytmu. Ręczne prognozy są zachowywane dla dostępu do szczegółów, lecz nie wchodzą do podstawowego raportu automatu. Prognoza powstaje tylko przy zweryfikowanym przyszłym wydarzeniu SofaScore. Analiza zakończona po starcie nie tworzy prognozy przedmeczowej.

Pełne wejścia modelu są współdzielonymi plikami JSON gzip, zamiast kopiować historie i mapy strzałów do każdego rynku. Zapis zawiera składy, wszystkie historie i kontekst wykorzystany przy danej analizie. Zmiana kursu tworzy nową prognozę; identyczne wejścia wskazują ten sam `evidenceId`. Sukcesy automatycznych pobrań źródłowych są również deduplikowane według treści. Deduplikacja kursów i małe obserwacje sprawdzeń pozostają aktywne. SQLite zawiera metadane kolejnych analiz; to nie jest magazyn o stałej wielkości.

Archiwum ma czytelne katalogi i nazwy, np.:

```text
data/predictions.sqlite
data/archive/badanie-algorytmu/dane-przedmeczowe/{druzyny-mecz-id}/wersja-{hash}/{druzyny-mecz-id}_dane-przedmeczowe.json.gz
data/archive/badanie-algorytmu/wyniki-statystyki/{druzyny-mecz-id}/wersja-{hash}/{druzyny-mecz-id}_wyniki-statystyki.json.gz
data/archive/badanie-algorytmu/raport-skutecznosci/{data-strategia-wariant}/wersja-{hash}/...
data/archive/automatyczne-zrodla/{zrodlo}/{druzyny}/{zakres}/wersja-{hash}/...
data/reports/{data}_prematch-T60-single-v1_standard-v1_30-dni.json
```

Pierwsza kontrola wyniku następuje około start +110 minut; jeśli spotkanie jeszcze trwa, ponowienie zwykle po 5 minutach. Wykonywane jest jedno zadanie tła naraz; rozliczenia mają pierwszeństwo przed kolejną wyceną. Zakończone wydarzenie uruchamia pobranie wyniku, składów, incydentów, potrzebnych map strzałów i statystyk zawodników z prognoz oraz ich zmienników. Te dane mają własną wersję treści; korekta źródła tworzy nową wersję rozliczenia, zachowując poprzednią. Po kompletnym pobraniu jest jeszcze kontrola po około 24 godzinach. Braki są ponawiane najpierw po 15 minutach, później co godzinę, a po 72 godzinach raz na dobę. Wszystko odbywa się w czasie bezczynności.

Stany rozliczenia: `won`, `lost`, `void`, `unresolved`. Brak pola statystyki, brak potwierdzonego udziału, niekompletna mapa strzałów albo błąd źródła nie są zamieniane w zero ani stratę. Potwierdzona ławka bez udziału oraz odwołane spotkanie dają zwrot. Dogrywka lub karne pozostają nierozliczone, jeśli nie można wydzielić statystyk regulaminowego czasu. Są dwa oddzielne warianty: `standard-v1` i `supersub-v1`. Ten drugi odtwarza dotychczasową logikę programu (pierwszy zmiennik przy mniej niż 85 minutach), a nie potwierdzony regulamin rozliczeń każdego bukmachera.

### Raport porównawczy aktualnego algorytmu

W Wails dostępny jest przycisk **Raport skuteczności algorytmu**. Taki sam raport czyta osobny panel WWW. Oceny ludzi nie zmieniają raportu algorytmu.

- Strategia `prematch-T60-single-v1`: dla każdego bukmachera, wydarzenia i wersji algorytmu wybiera ostatnią automatyczną analizę ukończoną nie później niż godzinę przed startem. Migawka musi pochodzić z 48 godzin przed startem; oferta musi być wtedy ważna i nie starsza niż 15 minut.
- Uwzględnia wyłącznie wycenione rynki zawodników z kompletną historią minimum 10 meczów. Dostępne historie innych zawodników mogą wejść do raportu nawet wtedy, gdy u części kadry źródło nie udostępnia danych. Niepełna historia danego zawodnika jest wykluczana.
- Jeden single po 1 jednostce na unikalny typ w tej migawce; kolejne odświeżenia nie są kolejnymi zakładami. Próg historycznego hitrate wynosi co najmniej 0/60/70/80/90/100%; progi są kumulatywne. Wariant SuperSub zachowuje ten sam filtr bazowego hitrate i ocenę algorytmu, zmienia rozliczenie.
- Osobne grupy VALUE, TRAP, NEUTRAL i ALL, wersje algorytmu oraz bukmacherzy. Dodatkowe BEST_PRICE wybiera jeden najwyższy kurs spośród ich migawek; te ceny nie musiały występować jednocześnie. Podstawowym porównaniem pozostają grupy poszczególnych bukmacherów.
- Raport pokazuje liczbę typów i różnych spotkań, wygrane/przegrane/zwroty/nierozliczone, rzeczywisty hitrate, stawki, wypłaty, zysk z wygranych, straty, wynik netto oraz ROI. Hitrate i ROI wykluczają zwroty i nierozliczone. Zwrot oddaje stawkę.
- Mnożnik wypłaty jest jawny, domyślnie 0,88 zgodnie z obecną wyceną. Wypłata wygranego singla = kurs × mnożnik. To symulacja przy wskazanych założeniach, bez kuponów AKO, promocji i rekonstrukcji indywidualnych rozliczeń konta.

Po rozliczeniu zapisywane są domyślne raporty 30-dniowe obu wariantów, z wersjami w archiwum i aktualnym plikiem JSON w `reports`. Interfejs pozwala wybrać 7/30/90/365 dni oraz mnożnik. Dane zaczynają się od uruchomienia tej wersji backendu; dawne analizy nie są importowane jako prognozy sprzed meczu.

### Postęp w journalctl

```sh
sudo journalctl -u value-bets -f
```

W logach są etapy `[AUTO-ANALYSIS]`, `ANALYSIS_PROGRESS`, `[PREDICTIONS]`, `[SETTLEMENT]` i `[REPORT]`. Każdy komunikat głównego loggera i wyjścia Betclic do journal ma dopisany ostatni stan kolejki oraz, jeśli trwa działanie, ID i etap. Przy zawodnikach widać licznik pobranych, całość i pozostałych. Co 60 sekund `[PROGRESS]` aktualizuje liczbę ofert kalendarza, dostępnych rynków, gotowych i oczekujących analiz, spotkań z brakami statystyk, błędów i rozliczeń. Liczniki kolejki są migawką z ostatniej minuty; licznik bieżącej analizy jest aktualizowany na każdym etapie. Całość zawodników może rosnąć po odkryciu rezerwy.

Liczniki ofert/analiz dotyczą pary bukmacher + ID, więc jedno spotkanie dostępne u trzech bukmacherów może liczyć się trzy razy. Rozliczenia i liczba różnych spotkań w raporcie używają ID SofaScore. Podgląd postępu nie powoduje nowych zapytań do źródeł.

### API dla oddzielnego panelu i eksportu

Wszystkie poniższe operacje są odczytowe:

- `GET /api/v1/research/summaries` — liczniki i stan automatycznych analiz;
- `GET /api/v1/research/progress` — kolejka i aktywne zadania;
- `GET /api/v1/research/candidates` — najnowsze świeże VALUE do 48 godzin;
- `GET /api/v1/research/predictions/{id}` — zamrożona prognoza, wejścia, rozliczenia i zarchiwizowane dane pomeczowe;
- `GET /api/v1/research/reports?days=30&payout=0.88&rules=standard-v1` — raport;
- `GET /api/v1/research/export?kind=predictions|inputs|outcomes&days=365` — JSONL.

Formaty mają `schemaVersion`. `predictionId` ocen/rozliczeń łączy się z `id` prognozy; `evidenceId` z eksportem wejść. `betKey` identyfikuje wspólny typ: mecz + zawodnik + rynek + linia + kierunek, niezależnie od bukmachera. Eksport rozliczeń zawiera wszystkie zachowane rewizje, nie tylko najnowszą. Bazy i archiwa trzeba zachować razem. Backup SQLite wykonuj przy zatrzymanej usłudze albo mechanizmem SQLite backup, nie kopiując samego pliku działającej bazy WAL.

Osobny panel, konta obu osób, eksport ocen i usługa systemd są opisane w `review/README.md`. Cały backend pozostaje w jednym `server/value_bets.go`; panel ma własne `review.go` i osadzany `index.html`.

### Kolejność ręcznej weryfikacji tej wersji

1. Wgraj trzy pliki backendu, zbuduj nową binarkę i zrestartuj istniejącą usługę, zachowując konfigurację Chromium oraz `data`.
2. Przebuduj Wails. Poczekaj na ukończoną analizę automatyczną meczu z rynkami i sprawdź liczniki Value/Pułapek oraz postęp w journal.
3. Uruchom panel według jego instrukcji, dodaj konta i hostname tunelu. Oceń propozycję z obu kont; opinie powinny pojawić się po własnym zapisie. Zmiana oceny powinna utworzyć nową wersję.
4. Po zakończonym meczu sprawdź wynik i statystykę rzeczywistą w historii panelu oraz raport dla danego bukmachera. Brak wymaganych danych powinien pozostać nierozliczony. Zweryfikuj osobno standard i SuperSub na znanej zmianie zawodnika.
5. Pobierz JSONL prognoz, wejść, wyników i ocen oraz sprawdź ich wspólne identyfikatory. Nie mieszaj linii jednego meczu między treningiem i walidacją.

Po poprawkach uruchomiono testy Go backendu i review, w tym regresje modelu, aliasów, filtrów kobiet, archiwum i zgodności liczników. Sprawdzono składnię JavaScript oraz widok review na lokalnych danych testowych. Zbudowano klienta Wails. Wdrożenie na Debianie i sprawdzenie zewnętrznych źródeł wymagają aktualizacji działających usług.

## Klient i ręczna weryfikacja

Wyszukiwanie zawodników w widoku zwykłym i porównaniu ignoruje wielkość liter oraz znaki diakrytyczne po obu stronach porównania, np. `joao` dla `João`, `lukasz` dla `Łukasz`, `odegaard` dla `Ødegaard`, `djuric` dla `Đurić`. Uwzględnia również znaki nierozkładające się standardowo na literę i akcent, takie jak æ/œ/ß. Filtrowanie reaguje na wpisywanie i wklejanie tekstu. Dopasowywanie zawodników po stronie backendu korzysta również z normalizacji Unicode i transliteracji tych liter.

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

Testy Go, kompilacja i lokalny podgląd review zostały wykonane. Po aktualizacji usług pozostaje sprawdzenie działania na rzeczywistych źródłach i danych serwera.
