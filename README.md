# Lunar Client Log Analyzer & Hypixel BedWars Scraper

Zwei Python-Skripte zur Extraktion von Minecraft-Spielernamen aus Lunar Client Logs und zum Scrapen ihrer Hypixel BedWars-Statistiken.

## 📋 Inhaltsverzeichnis

- [Übersicht](#übersicht)
- [Voraussetzungen](#voraussetzungen)
- [Installation](#installation)
- [Konfiguration](#konfiguration)
- [Verwendung](#verwendung)
- [Dateistruktur](#dateistruktur)
- [Funktionsweise](#funktionsweise)
- [Troubleshooting](#troubleshooting)
- [Wichtige Hinweise](#wichtige-hinweise)

---

## 🎯 Übersicht

### Script 1: `auto_extract.py`
Extrahiert Minecraft-Spielernamen aus Lunar Client Log-Dateien durch:
- Entschlüsselung komprimierter `.log.gz` Dateien
- Analyse verschiedener Log-Events (Chat, Join/Quit, Kills, Online-Listen, etc.)
- Filterung von False-Positives
- Erstellen einer bereinigten Spielerliste

### Script 2: `hypixel_scraper.py`
Scrapt detaillierte Hypixel BedWars-Statistiken für die extrahierten Spieler:
- Auflösung von Minecraft-Benutzernamen zu UUIDs (Mojang API)
- Abruf von Spielerstatistiken (Hypixel API)
- Speicherung in SQLite-Datenbank mit Zeitstempel
- Intelligentes Rate-Limiting und Caching
- Blacklist-System für ungültige Namen

---

## 🔧 Voraussetzungen

### System
- Python 3.10 oder höher
- Windows (Pfade sind für Windows konfiguriert, können aber angepasst werden)

### Python-Bibliotheken
```bash
pip install aiohttp
pip install tqdm
```

Die folgenden sind Standard-Bibliotheken (bereits in Python enthalten):
- `sqlite3`
- `gzip`
- `pathlib`
- `re`
- `argparse`
- `asyncio`
- `json`
- `datetime`

---

## 📦 Installation

1. **Dateien vorbereiten:**
   ```
   projekt/
   ├── auto_extract.py
   ├── hypixel_scraper.py
   └── README.md (diese Datei)
   ```

2. **Python-Pakete installieren:**
   ```bash
   pip install aiohttp tqdm
   ```

3. **Hypixel API-Keys besorgen:**
   - Gehe auf [Hypixel Developer Dashboard](https://developer.hypixel.net/)
   - Erstelle einen oder mehrere API-Keys
   - Füge sie in `hypixel_scraper.py` ein (Zeile 21-24)

---

## ⚙️ Konfiguration

### `auto_extract.py` - Pfade anpassen

**WICHTIG:** Passe diese Pfade in Zeile 154-156 an deine Umgebung an:

```python
# Pfad zu deinen Lunar Client Logs
input_dir = Path(r'C:\Users\DEINNAME\.lunarclient\profiles\lunar\1.8\logs')

# Pfad für entpackte Logs und Ausgabedateien
output_dir = Path(r'C:\Users\DEINNAME\Desktop\lunarFiles')

# Automatisch erstellt in output_dir
last_file = Path(output_dir/'lastScraped.txt')
```

**Standard Lunar Client Pfad:**
```
C:\Users\<BENUTZERNAME>\.lunarclient\profiles\lunar\1.8\logs
```

### `hypixel_scraper.py` - API-Keys und Dateien

**API-Keys (Zeile 21-24):**
```python
HYPIXEL_API_KEYS = [
    "DEIN-API-KEY-1",
    "DEIN-API-KEY-2",  # Optional: zweiter Key für doppelte Rate
]
```

**Dateipfade (Zeile 17-19):**
```python
DB_PATH = "hypixel_bedwars.db"           # SQLite-Datenbank
PLAYERS_FILE = "alle_spieler.txt"        # Von auto_extract.py erstellt
BLACKLIST_FILE = "blacklist.txt"         # Automatisch erstellt
```

**Rate Limits (Zeile 30-34):**
```python
HYPIXEL_RATE = 350      # 350 Requests pro Key
HYPIXEL_WINDOW = 300    # in 5 Minuten

MOJANG_RATE = 600       # 600 Requests
MOJANG_WINDOW = 600     # in 10 Minuten
```

---

## 🚀 Verwendung

### Schritt 1: Spielernamen extrahieren

```bash
python auto_extract.py
```

**Das Script macht:**
1. Liest die letzte verarbeitete Log-Datei aus `lastScraped.txt`
2. Entpackt alle neuen `.log.gz` Dateien im Lunar Client Ordner
3. Kopiert normale `.log` Dateien direkt
4. Analysiert alle Logs nach Spielernamen
5. Erstellt `alle_spieler.txt` mit eindeutigen Namen

**Ausgabe:**
```
Letzter Eintrag bei: 2024-01-15-1
Skipped: 2024-01-14-3
Skipped: 2024-01-14-4
Arrived at last scraped Log: 2024-01-15-1
------------------------------------------------------------
Decrypted ab jetzt die nächsten Einträge
Datei erfolgreich entpackt: C:\Users\Hans\Desktop\lunarFiles\2024-01-15-2
...
Gefundene Log-Dateien: 47
==================================================
Analysiere: 2024-01-15-1.log
156 Spieler gefunden (Gesamt bisher: 156)
...
Liste wurde in 'alle_spieler.txt' gespeichert!
Statistik: 47 Dateien analysiert, 892 eindeutige Spieler gefunden
```

### Schritt 2: Hypixel-Statistiken scrapen

**Erste Verwendung:**
```bash
python hypixel_scraper.py
```

Das Script scrapt ALLE Spieler aus `alle_spieler.txt`, überspringt aber automatisch:
- Namen auf der Blacklist
- Spieler, die in den letzten 7 Tagen bereits gescraped wurden

**Regelmäßige Updates:**
Einfach das Script erneut ausführen. Es wird automatisch:
- Neue Spieler scrapen
- Spieler aktualisieren, deren letzter Snapshot älter als 7 Tage ist
- Alle anderen überspringen

**Live-Ausgabe:**
```
Scraping: 100%|██████████| 892/892 [15:23<00:00, 0.97spieler/s, 
  ok=245, skip_7d=520, never=127, hyp_fail=3, moj_fail=12, 
  blacklist=8, src_cache=512, src_db=358, src_moj=22]

✅ Scraping abgeschlossen!
   Erfolgreich: 245
   Übersprungen (<7 Tage): 520
   Nie zuvor gescraped: 127
   Mojang fails: 12
   Hypixel fails: 3
   Build fails: 0
   Blacklist: 8 Namen
```

---

## 📁 Dateistruktur

Nach der Ausführung solltest du folgende Struktur haben:

```
projekt/
├── auto_extract.py
├── hypixel_scraper.py
├── README.md
├── alle_spieler.txt              # Extrahierte Spielernamen
├── blacklist.txt                 # Ungültige Namen (automatisch)
├── hypixel_bedwars.db           # SQLite-Datenbank
└── lunarFiles/                  # (dein output_dir)
    ├── lastScraped.txt          # Tracking der verarbeiteten Logs
    ├── 2024-01-15-1.log         # Entpackte Logs
    ├── 2024-01-15-2.log
    └── ...
```

---

## 🔍 Funktionsweise

### `auto_extract.py` - Namensextraktion

**Erkannte Muster:**

1. **Setting User:**
   ```
   [INFO] Setting user: MeinName
   ```

2. **Online-Listen (einzeilig):**
   ```
   ONLINE: Spieler1, Spieler2, Spieler3
   Online Players (8): Name1, Name2, Name3
   ```

3. **Online-Listen (mehrzeilig):**
   ```
   Online Players (5):
   - Spieler1
   - Spieler2
   - Spieler3
   ```

4. **Chat-Nachrichten:**
   ```
   [CHAT] Spieler1: Hallo!
   [CHAT] §aSpielername§r: test
   ```

5. **Join/Quit-Events:**
   ```
   [CHAT] Spieler1 has joined the game
   [CHAT] Spieler2 has quit
   [CHAT] Spieler3 disconnected
   ```

6. **Kill-Events:**
   ```
   [CHAT] Spieler1 was killed by Spieler2
   [CHAT] Spieler3 fell into the void
   [CHAT] FINAL KILL! Spieler4 by Spieler5
   ```

7. **Bed-Events:**
   ```
   [CHAT] Red BED destroyed by Spieler1!
   ```

8. **Team-Listen:**
   ```
   Red - Spieler1 Spieler2 Spieler3
   Blue: Name1, Name2, Name3
   ```

**Filterung:**
- Minecraft Color Codes (§a, §r, etc.)
- Bracket Tags ([MVP+], [VIP], etc.)
- Ausgeschlossene Wörter (siehe `EXCLUDED_NAMES`)
- Ungültige Namen (zu kurz, zu lang, nur Zahlen)

### `hypixel_scraper.py` - Statistik-Scraping

**Workflow:**

```
Spielername → Blacklist Check → UUID (Mojang) → Statistiken (Hypixel) → Datenbank
     ↓              ↓                  ↓                 ↓                  ↓
   Cache         Cached?           Cached?          JSON Parse        SQLite
     ↓              ↓                  ↓                 ↓
Blacklist       DB Check          DB Check         Snapshot
     ↓              ↓                  ↓
Ignoriert      API Call          API Call
```

**Rate-Limiting:**
- **Hypixel:** Max. 350 Requests pro 5 Minuten pro API-Key
- **Mojang:** Max. 600 Requests pro 10 Minuten + 1 Request/Sekunde
- Automatisches Warten bei Limit-Erreichen
- Multi-Key Support für höheren Durchsatz

**Caching:**
- In-Memory Cache für aktive Session
- SQLite Cache für spätere Sessions
- Reduziert API-Calls erheblich

**7-Tage-Logik:**
- Jeder Spieler wird nur alle 7 Tage neu gescraped
- Ermöglicht tägliches Ausführen ohne unnötige API-Calls
- Neue Spieler werden sofort gescraped

---

## 🛠️ Troubleshooting

### Problem: "Keine .log Dateien gefunden!"

**Lösung:** Überprüfe die Pfade in `auto_extract.py`:
```python
# Zeile 154-156
input_dir = Path(r'C:\Users\DEINNAME\.lunarclient\...')
output_dir = Path(r'C:\Users\DEINNAME\Desktop\lunarFiles')
```

### Problem: "Hypixel API Fehler"

**Mögliche Ursachen:**
1. **Ungültiger API-Key**
   - Lösung: Neuen Key auf [developer.hypixel.net](https://developer.hypixel.net/) erstellen

2. **Rate Limit erreicht**
   - Lösung: Script wartet automatisch, aber du kannst zweiten Key hinzufügen

3. **Daily Limit erreicht**
   ```
   Tägliches Maximum erreicht. Probiere es morgen nochmal
   ```
   - Lösung: 24 Stunden warten oder weiteren API-Key verwenden

### Problem: "Mojang API Fehler"

**Lösung:** 
- Mojang API ist manchmal überlastet
- Script wartet automatisch
- Ungültige Namen werden in `blacklist.txt` gespeichert

### Problem: "Database locked"

**Lösung:**
```python
# In hypixel_scraper.py ist bereits eingebaut:
SQLITE_PRAGMAS = [
    ("busy_timeout", "60000"),  # 60 Sekunden warten
]
```
Sollte automatisch funktionieren.

### Problem: "Viele False-Positives"

**Lösung:** Füge Wörter zur Excluded-Liste hinzu:
```python
# auto_extract.py, Zeile 14-22
EXCLUDED_NAMES = {
    # ... existing names ...
    "DeinWort1", "DeinWort2",
}
```

### Problem: "Script hängt bei einem Spieler"

**Watchdog meldet:**
```
[WATCHDOG] Seit >45s kein Fortschritt (aktuell: ProblematischerName)
```

**Mögliche Ursachen:**
- API-Timeout
- Netzwerkproblem
- Rate-Limit

**Lösung:** Script läuft automatisch weiter, aber du kannst:
1. Spieler zur Blacklist hinzufügen
2. Timeout erhöhen: `HTTP_TIMEOUT = 60` (Zeile 36)

---

## ⚠️ Wichtige Hinweise

### Datenschutz
- Die Datenbank enthält persönliche Statistiken von Spielern
- Behandle die Daten vertraulich
- Teile keine API-Keys öffentlich

### API-Limits
- **Hypixel:** 350 Requests pro 5 Min. pro Key (max. 100.800/Tag pro Key)
- **Mojang:** 600 Requests pro 10 Min. (max. 86.400/Tag)
- Bei 1000+ Spielern: mehrere API-Keys empfohlen

### Performance
- **auto_extract.py:** ~30 Sekunden für 50 Log-Dateien
- **hypixel_scraper.py:** 
  - ~0.5-2 Sekunden pro Spieler (mit Cache)
  - ~1000 Spieler = 10-30 Minuten
  - Beim ersten Durchlauf langsamer (keine UUIDs gecached)

### Datenbank
- **SQLite:** Single-File-Datenbank (`hypixel_bedwars.db`)
- **Backup empfohlen:** Kopiere die `.db` Datei regelmäßig
- **Größe:** ~1-2 KB pro Spieler, ~5-10 MB pro 5000 Spieler

### Wartung
- **Blacklist:** Regelmäßig prüfen und bereinigen
- **Alte Logs:** Können nach Extraktion archiviert werden
- **Datenbank:** Alte Snapshots können gelöscht werden (SQL-Kenntnisse erforderlich)

---

## 📊 Datenbankstruktur

### Tabelle: `players`
```sql
id                INTEGER PRIMARY KEY
uuid              TEXT UNIQUE NOT NULL
username          TEXT
first_seen        TIMESTAMP
last_seen         TIMESTAMP
```

### Tabelle: `bedwars_snapshots`
```sql
id                INTEGER PRIMARY KEY
player_id         INTEGER (Foreign Key)
snapshot_time     TIMESTAMP
-- Network Stats
network_level     REAL
network_exp       BIGINT
achievement_points INTEGER
karma             INTEGER
-- BedWars Overall
bedwars_level     INTEGER
bedwars_experience INTEGER
coins             INTEGER
games_played      INTEGER
wins              INTEGER
losses            INTEGER
winstreak         INTEGER
kills             INTEGER
deaths            INTEGER
final_kills       INTEGER
final_deaths      INTEGER
beds_broken       INTEGER
beds_lost         INTEGER
-- Resources
iron_collected    INTEGER
gold_collected    INTEGER
diamond_collected INTEGER
emerald_collected INTEGER
items_purchased   INTEGER
-- Mode-spezifische Stats (8v8 Solo, 8v8 Doubles, 4v4v4v4, 4v4, etc.)
... (siehe Schema in hypixel_scraper.py)
```

---

## 🎓 Beispiel-Queries

### Top 10 Spieler nach Wins
```sql
SELECT p.username, b.wins, b.losses, 
       ROUND(100.0 * b.wins / NULLIF(b.games_played, 0), 2) as winrate
FROM players p
JOIN bedwars_snapshots b ON p.id = b.player_id
WHERE b.snapshot_time = (
    SELECT MAX(snapshot_time) 
    FROM bedwars_snapshots 
    WHERE player_id = p.id
)
ORDER BY b.wins DESC
LIMIT 10;
```

### Fortschritt eines Spielers
```sql
SELECT snapshot_time, wins, final_kills, bedwars_level
FROM bedwars_snapshots
WHERE player_id = (SELECT id FROM players WHERE username = 'Spielername')
ORDER BY snapshot_time;
```

---

## 🐛 Debug-Modus

Für detailliertere Ausgaben kannst du folgendes hinzufügen:

**In `hypixel_scraper.py`:**
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

---

## 📝 Lizenz & Credits

Diese Scripts sind für den persönlichen Gebrauch gedacht.

**Credits:**
- Lunar Client für die Log-Files
- Mojang API für UUID-Auflösung
- Hypixel API für Spielerstatistiken

---

## 🤝 Support

Bei Problemen:
1. Überprüfe die Pfade in beiden Scripts
2. Prüfe, ob alle Abhängigkeiten installiert sind
3. Schaue dir die Ausgaben und Fehlermeldungen genau an
4. Konsultiere das Troubleshooting-Kapitel

---

**Viel Erfolg beim Scrapen! 🎮**
