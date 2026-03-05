import asyncio
import aiohttp
import sqlite3
import json
import time
import random
from datetime import datetime, timedelta, timezone
from collections import deque
from typing import Optional, Tuple, Dict, Any, List, Set

from tqdm import tqdm


# ----------------------------
# Konfig
# ----------------------------
DB_PATH = "hypixel_bedwars.db"
PLAYERS_FILE = "alle_spieler.txt"
BLACKLIST_FILE = "blacklist.txt"  # NEU: Blacklist für ungültige Namen

HYPIXEL_API_KEYS = [
    "3ac93f17-9b66-4164-9b4f-74b549ef15c1",
    "be5e3810-d627-4d70-b502-09bdcf2a8878",
]

MOJANG_PROFILE_URL = "https://api.mojang.com/users/profiles/minecraft/{username}"
HYPIXEL_PLAYER_URL = "https://api.hypixel.net/v2/player"

# Limits (wie im Original)
HYPIXEL_RATE = 350
HYPIXEL_WINDOW = 300  # 5 min pro Key

MOJANG_RATE = 600
MOJANG_WINDOW = 600   # 10 min gesamt (lassen wir drin; zusätzlich 1 req/sec hard cap)

HTTP_TIMEOUT = 30
MAX_RETRIES = 3  # NEU: Maximum Anzahl von Retries pro Request

# ----------------------------
# SQLite Schema
# ----------------------------
SQLITE_PRAGMAS = [
    ("journal_mode", "WAL"),
    ("synchronous", "NORMAL"),
    ("foreign_keys", "ON"),
    # Wenn die DB kurzzeitig gelockt ist (z.B. anderer Prozess),
    # warten wir bis zu 60s statt sofort zu scheitern.
    ("busy_timeout", "60000"),
]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY,
    uuid TEXT UNIQUE NOT NULL,
    username TEXT,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bedwars_snapshots (
    id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    network_level REAL,
    network_exp BIGINT,
    achievement_points INTEGER,
    karma INTEGER,
    bedwars_level INTEGER,
    bedwars_experience INTEGER,
    coins INTEGER,
    games_played INTEGER,
    wins INTEGER,
    losses INTEGER,
    winstreak INTEGER,
    kills INTEGER,
    deaths INTEGER,
    final_kills INTEGER,
    final_deaths INTEGER,
    beds_broken INTEGER,
    beds_lost INTEGER,
    iron_collected INTEGER,
    gold_collected INTEGER,
    diamond_collected INTEGER,
    emerald_collected INTEGER,
    items_purchased INTEGER,
    eight_one_wins INTEGER,
    eight_one_losses INTEGER,
    eight_one_kills INTEGER,
    eight_one_deaths INTEGER,
    eight_one_final_kills INTEGER,
    eight_one_final_deaths INTEGER,
    eight_one_beds_broken INTEGER,
    eight_one_beds_lost INTEGER,
    eight_two_wins INTEGER,
    eight_two_losses INTEGER,
    eight_two_kills INTEGER,
    eight_two_deaths INTEGER,
    eight_two_final_kills INTEGER,
    eight_two_final_deaths INTEGER,
    eight_two_beds_broken INTEGER,
    eight_two_beds_lost INTEGER,
    eight_two_winstreak INTEGER,
    four_three_wins INTEGER,
    four_three_losses INTEGER,
    four_three_kills INTEGER,
    four_three_deaths INTEGER,
    four_three_final_kills INTEGER,
    four_three_final_deaths INTEGER,
    four_three_beds_broken INTEGER,
    four_three_beds_lost INTEGER,
    four_four_wins INTEGER,
    four_four_losses INTEGER,
    four_four_kills INTEGER,
    four_four_deaths INTEGER,
    four_four_final_kills INTEGER,
    four_four_final_deaths INTEGER,
    four_four_beds_broken INTEGER,
    four_four_beds_lost INTEGER,
    four_four_winstreak INTEGER,
    raw_bedwars_json TEXT,
    FOREIGN KEY (player_id) REFERENCES players(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bedwars_player_time ON bedwars_snapshots(player_id, snapshot_time);
"""

INSERT_SNAPSHOT_SQL = """
INSERT INTO bedwars_snapshots (
    player_id, snapshot_time,
    network_level, network_exp, achievement_points, karma,
    bedwars_level, bedwars_experience, coins,
    games_played, wins, losses, winstreak,
    kills, deaths, final_kills, final_deaths,
    beds_broken, beds_lost,
    iron_collected, gold_collected, diamond_collected, emerald_collected,
    items_purchased,
    eight_one_wins, eight_one_losses, eight_one_kills, eight_one_deaths,
    eight_one_final_kills, eight_one_final_deaths, eight_one_beds_broken, eight_one_beds_lost,
    eight_two_wins, eight_two_losses, eight_two_kills, eight_two_deaths,
    eight_two_final_kills, eight_two_final_deaths, eight_two_beds_broken, eight_two_beds_lost,
    eight_two_winstreak,
    four_three_wins, four_three_losses, four_three_kills, four_three_deaths,
    four_three_final_kills, four_three_final_deaths, four_three_beds_broken, four_three_beds_lost,
    four_four_wins, four_four_losses, four_four_kills, four_four_deaths,
    four_four_final_kills, four_four_final_deaths, four_four_beds_broken, four_four_beds_lost,
    four_four_winstreak,
    raw_bedwars_json
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=60)
    cur = conn.cursor()
    for k, v in SQLITE_PRAGMAS:
        cur.execute(f"PRAGMA {k}={v};")
    conn.commit()
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def is_weekly_scrape_allowed(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT MAX(snapshot_time) FROM bedwars_snapshots;")
    row = cur.fetchone()
    if not row or not row[0]:
        return True
    try:
        last_snapshot = datetime.fromisoformat(row[0].replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return False
    return (datetime.now(timezone.utc) - last_snapshot) >= timedelta(days=7)


def get_player_last_snapshot_time(conn: sqlite3.Connection, player_id: int) -> Optional[datetime]:
    """Letzter Snapshot-Zeitpunkt pro Spieler (UTC) oder None wenn nie gescraped."""
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(snapshot_time) FROM bedwars_snapshots WHERE player_id = ?;",
        (player_id,),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return None
    try:
        return datetime.fromisoformat(str(row[0]).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        # Falls ältere SQLite-Daten in anderem Format vorliegen
        try:
            return datetime.strptime(str(row[0]), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None


def should_skip_player(conn: sqlite3.Connection, player_id: int, min_age_days: int = 7) -> Tuple[bool, Optional[datetime]]:
    """True => überspringen (letzter Snapshot jünger als min_age_days)."""
    last = get_player_last_snapshot_time(conn, player_id)
    if not last:
        return False, None
    age = datetime.now(timezone.utc) - last
    return age < timedelta(days=min_age_days), last


def preload_last_snapshot_times(conn: sqlite3.Connection) -> Dict[int, Optional[datetime]]:
    """Lädt die letzten Snapshot-Zeitpunkte für *alle* Spieler einmal in RAM.

    Warum: Ein SELECT pro Spieler kann bei großen Listen sporadisch "stuck" wirken,
    z.B. wenn SQLite kurzzeitig locked ist. Mit diesem Preload vermeiden wir
    zigtausende Einzel-Queries.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT player_id, MAX(snapshot_time) AS last_ts
        FROM bedwars_snapshots
        GROUP BY player_id;
        """
    )
    out: Dict[int, Optional[datetime]] = {}
    for pid, ts in cur.fetchall():
        if not ts:
            out[int(pid)] = None
            continue
        try:
            out[int(pid)] = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            try:
                out[int(pid)] = datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                out[int(pid)] = None
    return out


def calculate_network_level(network_exp: int) -> float:
    if not network_exp:
        return 0.0
    return (((2 * network_exp) + 30625) ** 0.5) / 50 - 2.5


def get_player_by_username(conn: sqlite3.Connection, username: str) -> Optional[Tuple[int, str, str]]:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, uuid, username FROM players WHERE username = ? COLLATE NOCASE LIMIT 1;",
        (username,),
    )
    return cur.fetchone()


def upsert_player(conn: sqlite3.Connection, uuid: str, username: str) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO players (uuid, username, last_seen)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(uuid) DO UPDATE SET
            username=excluded.username,
            last_seen=CURRENT_TIMESTAMP
        """,
        (uuid, username),
    )
    conn.commit()
    cur.execute("SELECT id FROM players WHERE uuid = ?", (uuid,))
    return cur.fetchone()[0]


def build_snapshot_row(player_id: int, snapshot_time: str, hyp_json: dict) -> Tuple:
    player_data = hyp_json.get("player") or {}
    stats = player_data.get("stats", {}).get("Bedwars", {}) or {}

    network_exp = int(player_data.get("networkExp", 0) or 0)
    network_level = float(calculate_network_level(network_exp))
    achievement_points = int(player_data.get("achievementPoints", 0) or 0)
    karma = int(player_data.get("karma", 0) or 0)

    bedwars_exp = int(stats.get("Experience", 0) or 0)
    bedwars_level = int(bedwars_exp / 500) if bedwars_exp else 0

    coins = int(stats.get("coins", 0) or 0)
    games_played = int(stats.get("games_played_bedwars", 0) or 0)
    wins = int(stats.get("wins_bedwars", 0) or 0)
    losses = int(stats.get("losses_bedwars", 0) or 0)
    winstreak = int(stats.get("winstreak", 0) or 0)
    kills = int(stats.get("kills_bedwars", 0) or 0)
    deaths = int(stats.get("deaths_bedwars", 0) or 0)
    final_kills = int(stats.get("final_kills_bedwars", 0) or 0)
    final_deaths = int(stats.get("final_deaths_bedwars", 0) or 0)
    beds_broken = int(stats.get("beds_broken_bedwars", 0) or 0)
    beds_lost = int(stats.get("beds_lost_bedwars", 0) or 0)

    iron = int(stats.get("iron_resources_collected_bedwars", 0) or 0)
    gold = int(stats.get("gold_resources_collected_bedwars", 0) or 0)
    diamond = int(stats.get("diamond_resources_collected_bedwars", 0) or 0)
    emerald = int(stats.get("emerald_resources_collected_bedwars", 0) or 0)
    items_purchased = int(stats.get("items_purchased_bedwars", 0) or 0)

    # Solo (8x1)
    eight_one_wins = int(stats.get("eight_one_wins_bedwars", 0) or 0)
    eight_one_losses = int(stats.get("eight_one_losses_bedwars", 0) or 0)
    eight_one_kills = int(stats.get("eight_one_kills_bedwars", 0) or 0)
    eight_one_deaths = int(stats.get("eight_one_deaths_bedwars", 0) or 0)
    eight_one_final_kills = int(stats.get("eight_one_final_kills_bedwars", 0) or 0)
    eight_one_final_deaths = int(stats.get("eight_one_final_deaths_bedwars", 0) or 0)
    eight_one_beds_broken = int(stats.get("eight_one_beds_broken_bedwars", 0) or 0)
    eight_one_beds_lost = int(stats.get("eight_one_beds_lost_bedwars", 0) or 0)

    # Doubles (8x2)
    eight_two_wins = int(stats.get("eight_two_wins_bedwars", 0) or 0)
    eight_two_losses = int(stats.get("eight_two_losses_bedwars", 0) or 0)
    eight_two_kills = int(stats.get("eight_two_kills_bedwars", 0) or 0)
    eight_two_deaths = int(stats.get("eight_two_deaths_bedwars", 0) or 0)
    eight_two_final_kills = int(stats.get("eight_two_final_kills_bedwars", 0) or 0)
    eight_two_final_deaths = int(stats.get("eight_two_final_deaths_bedwars", 0) or 0)
    eight_two_beds_broken = int(stats.get("eight_two_beds_broken_bedwars", 0) or 0)
    eight_two_beds_lost = int(stats.get("eight_two_beds_lost_bedwars", 0) or 0)
    eight_two_winstreak = int(stats.get("eight_two_winstreak", 0) or 0)

    # 3v3v3v3
    four_three_wins = int(stats.get("four_three_wins_bedwars", 0) or 0)
    four_three_losses = int(stats.get("four_three_losses_bedwars", 0) or 0)
    four_three_kills = int(stats.get("four_three_kills_bedwars", 0) or 0)
    four_three_deaths = int(stats.get("four_three_deaths_bedwars", 0) or 0)
    four_three_final_kills = int(stats.get("four_three_final_kills_bedwars", 0) or 0)
    four_three_final_deaths = int(stats.get("four_three_final_deaths_bedwars", 0) or 0)
    four_three_beds_broken = int(stats.get("four_three_beds_broken_bedwars", 0) or 0)
    four_three_beds_lost = int(stats.get("four_three_beds_lost_bedwars", 0) or 0)

    # 4v4v4v4
    four_four_wins = int(stats.get("four_four_wins_bedwars", 0) or 0)
    four_four_losses = int(stats.get("four_four_losses_bedwars", 0) or 0)
    four_four_kills = int(stats.get("four_four_kills_bedwars", 0) or 0)
    four_four_deaths = int(stats.get("four_four_deaths_bedwars", 0) or 0)
    four_four_final_kills = int(stats.get("four_four_final_kills_bedwars", 0) or 0)
    four_four_final_deaths = int(stats.get("four_four_final_deaths_bedwars", 0) or 0)
    four_four_beds_broken = int(stats.get("four_four_beds_broken_bedwars", 0) or 0)
    four_four_beds_lost = int(stats.get("four_four_beds_lost_bedwars", 0) or 0)
    four_four_winstreak = int(stats.get("four_four_winstreak", 0) or 0)

    raw_json = json.dumps(stats)

    return (
        player_id, snapshot_time,
        network_level, network_exp, achievement_points, karma,
        bedwars_level, bedwars_exp, coins,
        games_played, wins, losses, winstreak,
        kills, deaths, final_kills, final_deaths,
        beds_broken, beds_lost,
        iron, gold, diamond, emerald, items_purchased,
        eight_one_wins, eight_one_losses, eight_one_kills, eight_one_deaths,
        eight_one_final_kills, eight_one_final_deaths, eight_one_beds_broken, eight_one_beds_lost,
        eight_two_wins, eight_two_losses, eight_two_kills, eight_two_deaths,
        eight_two_final_kills, eight_two_final_deaths, eight_two_beds_broken, eight_two_beds_lost,
        eight_two_winstreak,
        four_three_wins, four_three_losses, four_three_kills, four_three_deaths,
        four_three_final_kills, four_three_final_deaths, four_three_beds_broken, four_three_beds_lost,
        four_four_wins, four_four_losses, four_four_kills, four_four_deaths,
        four_four_final_kills, four_four_final_deaths, four_four_beds_broken, four_four_beds_lost,
        four_four_winstreak,
        raw_json,
    )


# ----------------------------
# Blacklist Manager
# ----------------------------
class BlacklistManager:
    """Verwaltet Namen die keine UUID haben oder permanent fehlschlagen."""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.blacklist: Set[str] = set()
        self._load()
    
    def _load(self):
        """Lädt Blacklist aus Datei."""
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                self.blacklist = {line.strip().lower() for line in f if line.strip()}
            print(f"✓ Blacklist geladen: {len(self.blacklist)} Namen")
        except FileNotFoundError:
            print(f"✓ Neue Blacklist erstellt: {self.filepath}")
    
    def is_blacklisted(self, username: str) -> bool:
        """Prüft ob ein Name auf der Blacklist ist."""
        return username.lower() in self.blacklist
    
    def add(self, username: str):
        """Fügt einen Namen zur Blacklist hinzu."""
        key = username.lower()
        if key not in self.blacklist:
            self.blacklist.add(key)
            with open(self.filepath, "a", encoding="utf-8") as f:
                f.write(f"{username}\n")
    
    def size(self) -> int:
        return len(self.blacklist)


# ----------------------------
# DB Writer (Batching)
# ----------------------------
class DBWriter:
    def __init__(self, batch_size: int = 100):
        self.batch_size = batch_size
        self.queue = asyncio.Queue()
        self.task = None
        self.conn = None

    async def start(self):
        self.conn = get_conn()
        self.task = asyncio.create_task(self._worker())

    async def stop(self):
        await self.queue.put(None)
        if self.task:
            await self.task
        if self.conn:
            self.conn.close()

    async def write(self, sql: str, params: Tuple):
        await self.queue.put((sql, params))

    async def _worker(self):
        batch = []
        while True:
            item = await self.queue.get()
            if item is None:
                if batch:
                    self._flush_batch(batch)
                break
            batch.append(item)
            if len(batch) >= self.batch_size:
                self._flush_batch(batch)
                batch = []

    def _flush_batch(self, batch: List[Tuple[str, Tuple]]):
        cur = self.conn.cursor()
        for sql, params in batch:
            cur.execute(sql, params)
        self.conn.commit()


# ----------------------------
# FixedWindowLimiter
# ----------------------------
class FixedWindowLimiter:
    def __init__(self, rate: int, window: float, name: str = "limiter"):
        self.rate = rate
        self.window = window
        self.name = name
        self.timestamps = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Fixed-window Rate Limiter (thread-safe) ohne Lock-Rekursion."""
        while True:
            async with self._lock:
                now = time.time()
                cutoff = now - self.window
                while self.timestamps and self.timestamps[0] < cutoff:
                    self.timestamps.popleft()

                if len(self.timestamps) < self.rate:
                    self.timestamps.append(now)
                    return

                oldest = self.timestamps[0]
                sleep_time = (oldest + self.window) - now

            # Schlafen immer AUSSERHALB des Locks
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                # sehr selten, aber falls clock drift / rounding
                await asyncio.sleep(0)


# ----------------------------
# Hypixel Client
# ----------------------------
class HypixelClient:
    """Hypixel API Client mit *exakter* Einhaltung des Per-Key Limits.

    - Pro Key max. HYPIXEL_RATE Calls pro HYPIXEL_WINDOW Sekunden.
    - Nutzt mehrere Keys intelligent: nimmt immer den Key mit der kürzesten Wartezeit.
    - Respektiert Hypixel "Key throttle" Antworten: Key wird für ~HYPIXEL_WINDOW Sekunden geblacklisted.
    """

    def __init__(self, session: aiohttp.ClientSession, api_keys: List[str]):
        self.session = session
        self.keys = list(api_keys)

        # per-key timestamps (für den festen 5-Minuten-Window)
        self._ts: Dict[str, deque] = {k: deque() for k in self.keys}

        # wenn Hypixel "Key throttle" meldet, sperren wir den Key temporär
        self._throttled_until: Dict[str, float] = {k: 0.0 for k in self.keys}

        # --- Smooth pacing ---
        # Hypixel throttled teilweise auch, wenn man innerhalb des 5-Minuten-Fensters
        # "burstet" (z.B. 600 Requests in 2 Minuten). Deshalb takten wir zusätzlich
        # jeden Key auf ein Mindest-Intervall von window/rate.
        self._min_interval = HYPIXEL_WINDOW / float(HYPIXEL_RATE)  # ~0.857s pro Key
        self._next_allowed_at: Dict[str, float] = {k: 0.0 for k in self.keys}

        # ein Lock reicht, damit parallel laufende Tasks nicht gleichzeitig Tokens ziehen
        self._lock = asyncio.Lock()

    def _purge_old(self, key: str, now: float) -> None:
        dq = self._ts[key]
        cutoff = now - HYPIXEL_WINDOW
        while dq and dq[0] < cutoff:
            dq.popleft()

    def _key_wait(self, key: str, now: float) -> float:
        # throttle-sperre beachten
        if self._throttled_until.get(key, 0.0) > now:
            return self._throttled_until[key] - now

        self._purge_old(key, now)
        dq = self._ts[key]

        # 1) Rolling-window check (hartes Limit)
        window_wait = 0.0
        if len(dq) >= HYPIXEL_RATE:
            window_wait = max(0.0, (dq[0] + HYPIXEL_WINDOW) - now)

        # 2) Smooth pacing (verhindert Burst)
        pace_wait = max(0.0, self._next_allowed_at.get(key, 0.0) - now)

        return max(window_wait, pace_wait)

    async def acquire_key(self) -> str:
        """Blockiert bis ein Key verfügbar ist und reserviert dann 1 Call-Slot."""
        while True:
            async with self._lock:
                now = time.time()
                waits = [(self._key_wait(k, now), k) for k in self.keys]
                waits.sort(key=lambda x: x[0])
                wait, key = waits[0]

                if wait <= 0.0:
                    # Slot reservieren
                    self._ts[key].append(now)

                    # next allowed time für smooth pacing nach vorne schieben
                    next_at = self._next_allowed_at.get(key, 0.0)
                    base = now if next_at <= now else next_at
                    self._next_allowed_at[key] = base + self._min_interval
                    return key

            # Sleep außerhalb des Locks (mit kleinem Jitter gegen Thundering Herd)
            jitter = 0.05 * random.random()
            if wait > 10:
                # damit es nicht "stuck" aussieht
                print(f"[RATE] Alle Hypixel-Keys sind belegt/throttled – warte ~{wait:.0f}s …")
            await asyncio.sleep(wait + jitter)

    def mark_throttled(self, key: str, retry_after: Optional[float] = None) -> None:
        """Sperrt einen Key temporär.

        Wichtig: Wir sperren *nicht mehr pauschal 300s*, weil das unnötig lange "stuck" aussehen kann.
        Stattdessen schätzen wir die verbleibende Zeit bis zum nächsten verfügbaren Token im 5-Min-Fenster
        und addieren einen kleinen Puffer. Falls Hypixel einen Retry-After Header liefert, nutzen wir den.
        """
        now = time.time()

        # 1) Wenn Hypixel einen Retry-After liefert: verwenden (mit Cap)
        if retry_after is not None:
            ban = max(3.0, min(float(retry_after), float(HYPIXEL_WINDOW)))
        else:
            # 2) sonst: verbleibende Zeit im Window + kleiner Puffer
            self._purge_old(key, now)
            dq = self._ts[key]
            if dq and len(dq) >= 1:
                remaining = max(0.0, (dq[0] + HYPIXEL_WINDOW) - now)
            else:
                remaining = float(HYPIXEL_WINDOW)
            ban = max(5.0, min(float(HYPIXEL_WINDOW), remaining + 5.0))

        until = now + ban
        self._throttled_until[key] = max(self._throttled_until.get(key, 0.0), until)
        # auch pacing entsprechend nach hinten schieben
        self._next_allowed_at[key] = max(self._next_allowed_at.get(key, 0.0), self._throttled_until[key])

    async def fetch_player(self, uuid: str) -> Optional[dict]:
        """Fetch Hypixel Player Daten mit Retry-Logik.

        Hält das Rate-Limit *clientseitig* ein und reagiert auf:
        - HTTP 429
        - JSON: {"throttle": true, "cause": "Key throttle"}
        """
        tries = 0
        while tries <= MAX_RETRIES:
            key = await self.acquire_key()
            params = {"key": key, "uuid": uuid}

            try:
                async with self.session.get(HYPIXEL_PLAYER_URL, params=params) as resp:
                    if resp.status == 200:

                        data = await resp.json(content_type=None)
                        # Hypixel kann trotz 200 throttlen
                        if isinstance(data, dict) and data.get("throttle"):
                            # Key ist (temporär) dicht -> sperren & retry mit anderem Key
                            self.mark_throttled(key)
                            tries += 1
                            await asyncio.sleep(1.0)
                            continue
                        return data

                    if resp.status == 429:
                        # Key wahrscheinlich limitiert -> sperren & retry
                        ra = resp.headers.get("Retry-After")
                        try:
                            ra_f = float(ra) if ra is not None else None
                        except Exception:
                            ra_f = None
                        self.mark_throttled(key, retry_after=ra_f)
                        tries += 1
                        await asyncio.sleep(1.0)
                        continue

                    if resp.status in (403, 404):
                        text = await resp.text()
                        print(f"[DEBUG] Hypixel fail für {uuid}: Status {resp.status} - {text[:200]}")
                        return None

                    if 500 <= resp.status < 600:
                        print(f"[DEBUG] Hypixel Server Error {resp.status} für {uuid} - retry")
                        tries += 1
                        await asyncio.sleep(1.0)
                        continue

                    text = await resp.text()
                    print(f"[DEBUG] Hypixel fail für {uuid}: Unerwarteter Status {resp.status} - {text[:200]}")
                    return None

            except asyncio.TimeoutError:
                print(f"[DEBUG] Hypixel Timeout für {uuid} - retry")
                tries += 1
                await asyncio.sleep(1.0)
                continue
            except Exception as e:
                print(f"[DEBUG] Hypixel Exception für {uuid}: {type(e).__name__}: {str(e)[:200]} - retry")
                tries += 1
                await asyncio.sleep(1.0)
                continue

        print(f"[DEBUG] Hypixel fail für {uuid}: nach {MAX_RETRIES} Retries")
        return None

# ----------------------------
# Mojang Client
# ----------------------------
class MojangClient:
    """
    Hard cap: genau 1 Request pro Sekunde (keine Parallelität nötig).
    Mit Retry-Logik und Timeout-Protection.
    """
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.fw_limiter = FixedWindowLimiter(MOJANG_RATE, MOJANG_WINDOW, name="mojang")
        self._last_call = 0.0

    async def _sleep_until_next_second(self) -> None:
        now = time.time()
        delta = now - self._last_call
        if delta < 1.0:
            await asyncio.sleep(1.0 - delta)
        self._last_call = time.time()

    async def username_to_uuid(self, username: str) -> Optional[Tuple[str, str]]:
        """
        Konvertiert Username zu UUID.
        Gibt None zurück wenn der Name nicht existiert oder permanent fehlschlägt.
        """
        url = MOJANG_PROFILE_URL.format(username=username)
        retries = 0

        while retries < MAX_RETRIES:
            await self.fw_limiter.acquire()
            await self._sleep_until_next_second()

            try:
                async with self.session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        uuid = data.get("id")
                        name = data.get("name") or username
                        if uuid:
                            return uuid, name
                        return None

                    if resp.status in (204, 404):
                        # Name existiert nicht
                        return None

                    if resp.status == 429:
                        retries += 1
                        await asyncio.sleep(2.0 * retries)  # Exponential backoff
                        continue

                    if 500 <= resp.status < 600:
                        retries += 1
                        await asyncio.sleep(1.0 * retries)
                        continue

                    # Unerwarteter Status
                    return None
            
            except asyncio.TimeoutError:
                retries += 1
                await asyncio.sleep(1.0 * retries)
                continue
            except Exception:
                return None
        
        # Max retries erreicht
        return None


# ----------------------------
# Resolver Cache (nur für diesen Run)
# ----------------------------
_uuid_cache: Dict[str, Tuple[int, str, str]] = {}


async def resolve_player(
    conn: sqlite3.Connection,
    mojang: MojangClient,
    username: str,
    blacklist: BlacklistManager,
) -> Optional[Tuple[int, str, str, str]]:
    """
    Returns: (player_id, uuid, resolved_name, source)
    source in {"cache", "db", "mojang"}
    Fügt Namen zur Blacklist hinzu wenn keine UUID gefunden wird.
    """
    k = username.lower()

    # Blacklist check
    if blacklist.is_blacklisted(username):
        return None

    if k in _uuid_cache:
        pid, uuid, name = _uuid_cache[k]
        return pid, uuid, name, "cache"

    row = get_player_by_username(conn, username)
    if row:
        _uuid_cache[k] = row
        pid, uuid, name = row
        return pid, uuid, name, "db"

    mo = await mojang.username_to_uuid(username)
    if not mo:
        # Kein UUID gefunden -> Blacklist
        blacklist.add(username)
        return None

    uuid, resolved_name = mo
    pid = upsert_player(conn, uuid, resolved_name)

    out = (pid, uuid, resolved_name)
    _uuid_cache[k] = out
    return pid, uuid, resolved_name, "mojang"


# ----------------------------
# Main (SERIELL)
# ----------------------------
async def main_async():
    snapshot_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Blacklist initialisieren
    blacklist = BlacklistManager(BLACKLIST_FILE)

    # Hinweis: Der alte "Weekly guard" (globaler Abbruch wenn der letzte Snapshot < 7 Tage ist)
    # wurde bewusst entfernt. Stattdessen wird jetzt pro Spieler geprüft, wann er zuletzt
    # gescraped wurde (siehe should_skip_player). So kannst du das Script jederzeit laufen lassen.

    # Players laden
    with open(PLAYERS_FILE, "r", encoding="utf-8") as f:
        players = [l.strip() for l in f if l.strip() and not l.startswith("#")]

    # Filtere bereits geblacklistete Namen BEVOR wir starten
    original_count = len(players)
    players = [p for p in players if not blacklist.is_blacklisted(p)]
    filtered_count = original_count - len(players)
    
    if filtered_count > 0:
        print(f"✓ {filtered_count} Namen wurden übersprungen (Blacklist)")

    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        hyp = HypixelClient(session, HYPIXEL_API_KEYS)
        mojang = MojangClient(session)

        conn = get_conn()
        ensure_schema(conn)

        # Preload: letzte Snapshot-Zeit pro Spieler in RAM (beschleunigt massiv & reduziert "stuck")
        last_snapshot_by_pid = preload_last_snapshot_times(conn)

        writer = DBWriter()
        await writer.start()

        ok = 0
        fail = 0
        hyp_fail = 0
        mojang_fail = 0
        skipped_recent = 0
        never_scraped = 0
        skipped_recent = 0

        # Quelle fürs UUID-Resolving
        resolved_cache = 0
        resolved_db = 0
        resolved_mojang = 0

        pbar = tqdm(players, desc="Scraping", unit="spieler", dynamic_ncols=True)

        # Watchdog: wenn ein einzelner Schritt sehr lange dauert, loggen wir den aktuellen Spieler.
        last_progress_wall = time.time()
        last_progress_n = 0

        for username in pbar:
            # einfache Fortschritts-Überwachung
            if pbar.n != last_progress_n:
                last_progress_n = pbar.n
                last_progress_wall = time.time()
            elif time.time() - last_progress_wall > 45:
                print(f"[WATCHDOG] Seit >45s kein Fortschritt (aktuell: {username}). Wenn das oft passiert: DB lock / Netzwerk / Rate-Limit.")
                last_progress_wall = time.time()
            # 1) Mojang/DB/Cache resolve (mit Blacklist)
            resolved = await resolve_player(conn, mojang, username, blacklist)
            if not resolved:
                mojang_fail += 1
                pbar.set_postfix({
                    "blacklist": blacklist.size(),
                    "src_db": resolved_db,
                    "src_moj": resolved_mojang,
                    "src_cache": resolved_cache,
                    "ok": ok,
                    "skip_7d": skipped_recent,
                    "never": never_scraped,
                    "hyp_fail": hyp_fail,
                    "moj_fail": mojang_fail,
                    "build_fail": fail,
                })
                continue

            player_id, uuid, resolved_name, source = resolved
            if source == "cache":
                resolved_cache += 1
            elif source == "db":
                resolved_db += 1
            else:
                resolved_mojang += 1

            # 1b) Pro Spieler prüfen: wurde er in den letzten 7 Tagen bereits gescraped?
            # Wenn ja -> überspringen.
            # Wichtig: .get() liefert None auch wenn der Key fehlt.
            # Wenn player_id nicht im Dict ist, wurde er noch nie gescraped.
            if player_id not in last_snapshot_by_pid:
                last_ts = None
                skip = False
                never_scraped += 1
            else:
                last_ts = last_snapshot_by_pid[player_id]
                if last_ts is None:
                    skip = False
                    never_scraped += 1
                else:
                    skip = (datetime.now(timezone.utc) - last_ts) < timedelta(days=7)
            if skip:
                skipped_recent += 1
                pbar.set_postfix({
                    "blacklist": blacklist.size(),
                    "src_db": resolved_db,
                    "src_moj": resolved_mojang,
                    "src_cache": resolved_cache,
                    "ok": ok,
                    "skip_7d": skipped_recent,
                    "never": never_scraped,
                    "hyp_fail": hyp_fail,
                    "moj_fail": mojang_fail,
                    "build_fail": fail,
                })
                continue
            # never_scraped wird oben bereits gezählt

            # 2) Hypixel
            data = await hyp.fetch_player(uuid)
            if not data or data.get("success") is False:
                hyp_fail += 1
                pbar.set_postfix({
                    "blacklist": blacklist.size(),
                    "src_db": resolved_db,
                    "src_moj": resolved_mojang,
                    "src_cache": resolved_cache,
                    "ok": ok,
                    "skip_7d": skipped_recent,
                    "never": never_scraped,
                    "hyp_fail": hyp_fail,
                    "moj_fail": mojang_fail,
                    "build_fail": fail,
                })
                if "daily" in data.get("throttle"):
                    print("Tägliches Maximum erreicht. Probiere es morgen nochmal")
                    break
                continue

            # 3) Snapshot bauen + schreiben
            try:
                row = build_snapshot_row(player_id, snapshot_time, data)
                await writer.write(INSERT_SNAPSHOT_SQL, row)
                ok += 1
                # im aktuellen Run gilt der Spieler jetzt als gescraped
                try:
                    last_snapshot_by_pid[player_id] = datetime.fromisoformat(snapshot_time.replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    pass
            except Exception:
                fail += 1

            # Live status im Balken
            pbar.set_postfix({
                "skip_7d": skipped_recent,
                "blacklist": blacklist.size(),
                "src_db": resolved_db,
                "src_moj": resolved_mojang,
                "src_cache": resolved_cache,
                "ok": ok,
                "never": never_scraped,
                "hyp_fail": hyp_fail,
                "moj_fail": mojang_fail,
                "build_fail": fail,
            })

        await writer.stop()
        conn.close()

        print(f"\n✅ Scraping abgeschlossen!")
        print(f"   Erfolgreich: {ok}")
        print(f"   Übersprungen (<7 Tage): {skipped_recent}")
        print(f"   Nie zuvor gescraped (in diesem Run bearbeitet): {never_scraped}")
        print(f"   Mojang fails: {mojang_fail}")
        print(f"   Hypixel fails: {hyp_fail}")
        print(f"   Build fails: {fail}")
        print(f"   Blacklist: {blacklist.size()} Namen")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()