# Osobny panel ocen

Panel jest osobną aplikacją Go, niezależną od Wails i `server/value_bets.go`. Cały jego kod jest w `review.go` i `index.html` (HTML jest osadzany przy budowaniu). Czyta wyłącznie HTTP API backendu. Własne konta, przydziały, sesje i wersje ocen zapisuje w `data/reviews.sqlite`; nie otwiera bazy backendu i nie pobiera danych z SofaScore ani od bukmacherów.

## Wgranie na Debiana

Najpierw zaktualizuj backend zgodnie z `server/README.md`. Następnie skopiuj katalog `review` do `/home/vstkxj/value-bets/review`. Potrzebne są `review.go`, `index.html`, `go.mod`, `go.sum`; plik usługi i instrukcja są pomocnicze.

```sh
cd /home/vstkxj/value-bets/review
go build -o review.new review.go
mv review.new review
```

Wymagane Go 1.26+. SQLite działa przez sterownik w Go, bez instalowania serwera bazy i bez CGO. Nie usuwaj `data` przy aktualizacji.

## Dwa konta

Hasła nie są wpisywane do źródła ani argumentów procesu. Poniższe polecenia Bash czytają je bez wyświetlania i tworzą konta. Wybierz własne loginy; `-admin` pozwala pobierać pełny eksport ocen obu osób.

```sh
cd /home/vstkxj/value-bets/review
read -r -s -p 'Hasło dla mojego konta: ' REVIEW_PASSWORD
printf '\n'
export REVIEW_PASSWORD
./review -data ./data -init-user ja -admin
unset REVIEW_PASSWORD

read -r -s -p 'Hasło dla kolegi: ' REVIEW_PASSWORD
printf '\n'
export REVIEW_PASSWORD
./review -data ./data -init-user kolega
unset REVIEW_PASSWORD
```

Hasło: 10–72 bajty, login: 2–32 znaki a-z/A-Z/0-9/_/-. Ponowne `-init-user` zmienia hasło, ustawia uprawnienia zgodnie z `-admin` i unieważnia sesje konta. Hasła są przechowywane jako bcrypt. Sesje są w ciasteczkach HttpOnly/SameSite, z ochroną żądań zapisu przed CSRF; wygasają po 30 dniach.

## systemd i Cloudflare

```sh
sudo install -m 0644 review.service /etc/systemd/system/value-bets-review.service
sudo systemctl daemon-reload
sudo systemctl enable --now value-bets-review
sudo journalctl -u value-bets-review -f
```

Dodaj w istniejącym Cloudflare Tunnel osobny publiczny hostname `review.value-bets.pl` prowadzący do `http://localhost:8092`. Nie otwieraj dodatkowego portu routera; proces nasłuchuje lokalnie. `api.value-bets.pl` nadal prowadzi do backendu na 8090. Jeśli wybierzesz inną domenę, zmień `-public-url` w usłudze: sprawdzane jest pochodzenie formularzy, a publiczna sesja wymaga HTTPS.

Do samego lokalnego użycia można uruchomić `./review -listen 127.0.0.1:8092 -backend http://127.0.0.1:8090 -data ./data` bez `-public-url`. Wtedy korzystaj z HTTP na tym samym hoście lub przez tunel SSH. Nigdy nie udostępniaj publicznie DevTools Chromium.

## Co i jak oceniamy

- Kolejka zawiera automatycznie wycenione propozycje VALUE z najbliższych 48 godzin i ze świeżą ofertą przy losowaniu. Najpierw losuje mecz, później zawodnika i typ; wiele linii jednego gracza nie zwiększa szans całego meczu. W obrębie tego samego typu wybierana jest najwyższa dostępna cena.
- 25% losowań, kiedy są dostępne, kieruje do wspólnej puli propozycji oglądanych przez drugą osobę. Dla wspólnego typu obie osoby dostają tę samą zamrożoną ofertę. Ocena odnosi się zawsze do zapisanego kursu i czasu; stara oferta jest oznaczona, nie przedstawiana jako aktualna cena.
- Ta sama osoba nie dostaje ponownie tego samego typu tylko dlatego, że zmienił się kurs lub bukmacher. Pominięcie nie jest negatywną oceną. Niedokończony przydział wraca po ponownym wejściu.
- Skala 1–5 ocenia opłacalność przy pokazanej cenie. `Nie wiem` jest osobną odpowiedzią. Korzystanie z danych zewnętrznych wymaga jawnego wyboru tak/nie. Powód, źródła i minimalny akceptowalny kurs są opcjonalne.
- Fair kurs, EV i opinia drugiej osoby pojawiają się dopiero po zapisaniu własnej oceny. Administracyjny eksport może zawierać wszystkie opinie; nie używaj go do podglądania ocen przed niezależną oceną.
- Oceny można zmieniać przed startem. Każdy zapis tworzy nową wersję; po starcie backend panelu odrzuca zapis. Podstawą terminu jest zamrożona prognoza.
- Widok historii zawiera ostatnią ocenę każdej propozycji, do 200 pozycji. Eksport zawiera wszystkie wersje, bez tego ograniczenia. Wyniki po meczu są pobierane z głównego backendu.

## Dane i eksport

SQLite jest głównym zapisem transakcyjnym. Konto administratora ma widok **Eksport ML** z JSONL:

- `ratings`: wszystkie wersje ocen, autor, moment, usedExternal, reason, sources, rating/unknown, minimumOdds, predictionId, betKey i revisionOf (0 przy pierwszej ocenie, ID wcześniejszej wersji przy zmianie);
- `predictions`: zamrożone prognozy; `id` łączy się z `predictionId` ocen i rozliczeń;
- `inputs`: współdzielone dane wejściowe; `evidenceId` łączy się z prognozą;
- `outcomes`: wszystkie zachowane wersje rozliczeń, rozdzielone na standard-v1 i supersub-v1.

Brak danych ma pozostać brakiem, a nie zerem ani przegraną. Nie mieszaj różnych wersji algorytmu i zasad rozliczenia. Do treningu wybieraj wersję oceny zapisaną przed startem, a dane wejściowe wyłącznie z prognozy. Pierwsza ocena (`revisionOf=0`) jest zapisywana przed ujawnieniem porównania w panelu; późniejsza zmiana może już uwzględniać fair kurs i opinię drugiej osoby. Zachowaj to rozróżnienie przy analizie niezależnych ocen. Podział trening/walidacja powinien odbywać się chronologicznie całymi meczami, bez rozrzucania linii i bukmacherów tego samego spotkania.

Kopie zapasowe SQLite wykonuj przy zatrzymanej usłudze albo za pomocą mechanizmu backup SQLite. Przy działającej bazie WAL samo skopiowanie pliku `.sqlite` może pominąć ostatnie zapisy.

## Historia i ustawienie po aktualizacji

Historia eksponuje oceniany rynek, linię i pokrycie każdego występu, z minutami, pozycją i dom/wyjazd. Zera są uwzględniane także przy pełnych 90 minutach. Pozostałe statystyki są rozwijane osobno. Shotmap obraca obie osie jednocześnie i pokazuje kierunek ataku w prawo.

„Pobierz historię” umożliwia jawny wybór 20, 30 lub 50 występów. Backend pobiera historię tylko ocenianego zawodnika, korzysta z cache i w razie potrzeby kolejnych stron. Żądanie działa asynchronicznie; wynik wskazuje rzeczywistą wielkość oraz ewentualną niekompletność próbki. Historia zawsze kończy się przed zamrożonym spotkaniem. Domyślnie pozostaje próbka prognozy (zwykle 10). Dodatkowy podgląd nie zmienia zapisanego kursu, modelu ani oceny. Nieprzydzielone propozycje nie udostępniają historii użytkownikowi.

Formacja, pozycja w ustawieniu i numer koszulki pochodzą z odpowiedzi składów, którą backend już pobiera do analizy. Nie dodano zapytań o historyczne składy. Starsze prognozy mogą nie zawierać tych pól. Dostępność szczegółów zależy od źródła; brak nie jest zastępowany wymyśloną pozycją.

Wgraj i przebuduj zarówno backend, jak i panel review. Zachowaj `data/reviews.sqlite` oraz dotychczasowe konta i oceny.
