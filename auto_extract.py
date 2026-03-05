from pathlib import Path
import gzip
import shutil
import re
from typing import Optional, Iterable
import argparse
# =========================
#  KONFIG / VALIDIERUNG
# =========================

VALID_NAME_RE = re.compile(r"^[A-Za-z0-9_]{3,16}$")
COLOR_CODE_RE = re.compile(r"(?:\u00A7|§|Â§).")
BRACKET_TAG_RE = re.compile(r"\[[^\]]+\]")

EXCLUDED_NAMES = {
    "", "MVP", "VIP", "CHAT", "INFO", "ERROR", "WARN", "Thread", "Client", "Server",
    "STDOUT", "STDERR", "Unknown", "Source", "Genesis",

    "Red", "Blue", "Green", "Yellow", "Aqua", "White", "Gray", "Pink", "SPECTATOR",
    "Team", "BED", "DESTRUCTION", "FINAL", "KILL", "ELIMINATED",

    "You", "Your", "has", "was", "were", "been", "have", "will", "from", "into",
    "the", "and", "for", "with", "off", "out", "got", "not", "can", "are",

    "Diamond", "Iron", "Gold", "Stone", "Wool", "Sword", "Pickaxe", "Arrow",
    "Chest", "Bed", "Bridge", "Emerald", "Silver", "Coin", "Coins", "Token", "Tokens",

    # bekannte False-Positives aus deinen Logs
    "Lunar", "screen", "keybinds", "get",
}

EXCLUDED_LOWER = {n.lower() for n in EXCLUDED_NAMES}

IGNORE_LINE_PREFIXES = ("[LC]", "Lunar Client")
IGNORE_LINE_CONTAINS = (
    "took ", " ms", "ms)", "ms ",
    "Loading screen",
    "Registering keybinds",
)

# =========================
#  REGEX-PATTERNS
# =========================

USER_PATTERN = re.compile(r"Setting user:\s*([A-Za-z0-9_]{3,16})", re.IGNORECASE)

# ONLINE / Online Players (einzeilig)
ONLINE_INLINE_PATTERNS = [
    re.compile(r"\bONLINE:\s+([^\n]+)", re.IGNORECASE),
    re.compile(r"\bOnline\s+Players?\s*(?:\(\s*\d+\s*\))?\s*:\s*([^\n]+)", re.IGNORECASE),
    re.compile(r"\bPlayers\s+online\s*(?:\(\s*\d+\s*\))?\s*:\s*([^\n]+)", re.IGNORECASE),
]

# Online Players (mehrzeilig) - Headerzeile, danach Bullet-Lines
ONLINE_BLOCK_HEADER = re.compile(r"\bOnline\s+Players?\s*(?:\(\s*\d+\s*\))?\s*:\s*$", re.IGNORECASE)
ONLINE_BLOCK_ITEM = re.compile(r"^\s*(?:[-•*]|•)?\s*([A-Za-z0-9_]{3,16})\s*$")

# Chat: "<name>: message"
CHAT_NAME_COLON = re.compile(r"^([A-Za-z0-9_]{3,16})\s*:\s")

JOIN_CHAT = re.compile(r"^([A-Za-z0-9_]{3,16})\s+has\s+joined\s+(?:the\s+)?(?:game|lobby|party)\b", re.IGNORECASE)
QUIT_CHAT = re.compile(r"^([A-Za-z0-9_]{3,16})\s+has\s+quit\b", re.IGNORECASE)
DISCO_CHAT = re.compile(r"^([A-Za-z0-9_]{3,16})\s+disconnected\b", re.IGNORECASE)

DEATH_PATTERNS = [
    re.compile(r"^([A-Za-z0-9_]{3,16})\s+was\s+(?:killed|knocked|slain|eliminated|bested|struck\s+down)\b", re.IGNORECASE),
    re.compile(r"^([A-Za-z0-9_]{3,16})\s+(?:fell|flew|walked|ran)\s+into\b", re.IGNORECASE),
    re.compile(r"^([A-Za-z0-9_]{3,16})\s+(?:hit\s+the|met\s+their)\b", re.IGNORECASE),
    re.compile(r"^([A-Za-z0-9_]{3,16})\s+died\b", re.IGNORECASE),
    re.compile(r"^([A-Za-z0-9_]{3,16})\s+respawned\b", re.IGNORECASE),
]

KILLER_PATTERNS = [
    re.compile(r"\b(?:killed|eliminated|slain)\s+by\s+([A-Za-z0-9_]{3,16})[.!]?\b", re.IGNORECASE),
    re.compile(r"\bby\s+([A-Za-z0-9_]{3,16})\b.*\bFINAL\s+KILL\b", re.IGNORECASE),
]

BED_PATTERN = re.compile(r"\b(?:destroyed|broken)\s+by\s+([A-Za-z0-9_]{3,16})[!.]?\b", re.IGNORECASE)

TEAMLINE_PATTERN = re.compile(r"^(Red|Blue|Green|Yellow|Aqua|White|Gray|Pink)\s*[-:]\s*(.+)$", re.IGNORECASE)


def strip_color_codes(s: str) -> str:
    return COLOR_CODE_RE.sub("", s)

def strip_bracket_tags(s: str) -> str:
    return BRACKET_TAG_RE.sub("", s)

def normalize_chat_payload(line: str) -> str:
    s = line.strip()
    s = strip_color_codes(s)
    idx = s.find("[CHAT]")
    if idx != -1:
        s = s[idx + len("[CHAT]"):].strip()
    s = strip_bracket_tags(s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def is_valid_name(name: str) -> bool:
    if not name:
        return False
    name = name.strip()
    if not name:
        return False
    if name.lower() in EXCLUDED_LOWER:
        return False
    if not VALID_NAME_RE.match(name):
        return False
    if name.isdigit():
        return False
    if "___" in name:
        return False
    return True

def should_ignore_line(line: str) -> bool:
    s = line.strip()
    for p in IGNORE_LINE_PREFIXES:
        if s.startswith(p):
            return True
    lowered = s.lower()
    if any(tok.lower() in lowered for tok in IGNORE_LINE_CONTAINS):
        if "[CHAT]" not in s:
            return True
    return False

def add_player(players: dict[str, str], name: Optional[str]) -> None:
    if not name:
        return
    name = name.strip()
    if not is_valid_name(name):
        return
    players.setdefault(name.lower(), name)

def add_many(players: dict[str, str], names: Iterable[str]) -> None:
    for n in names:
        add_player(players, n)

def parse_name_list(raw: str) -> list[str]:
    """
    Robust: "name1, name2 name3 | name4" etc.
    """
    s = strip_color_codes(strip_bracket_tags(raw))
    s = s.replace(";", ",").replace("|", ",")
    s = re.sub(r"\s+", " ", s).strip()
    parts = [p.strip() for p in re.split(r",", s) if p.strip()]
    # falls es ohne Kommata kommt: "a b c"
    if len(parts) == 1 and " " in parts[0]:
        parts = [p for p in parts[0].split(" ") if p]
    return parts

def extract_online_players(log_content: str) -> list[str]:
    players: dict[str, str] = {}

    lines = log_content.splitlines()

    # 1) Einzeilige Varianten
    for rx in ONLINE_INLINE_PATTERNS:
        for m in rx.finditer(log_content):
            raw = m.group(1)
            add_many(players, parse_name_list(raw))

    # 2) Mehrzeilige Blöcke: "Online Players:" danach Namen-Zeilen
    i = 0
    while i < len(lines):
        line = strip_color_codes(strip_bracket_tags(lines[i])).rstrip()
        if ONLINE_BLOCK_HEADER.match(line.strip()):
            i += 1
            # bis leere Zeile oder bis etwas "nicht wie name/bullet" aussieht
            while i < len(lines):
                l2 = strip_color_codes(strip_bracket_tags(lines[i])).strip()
                if not l2:
                    break
                m_item = ONLINE_BLOCK_ITEM.match(l2)
                if m_item:
                    add_player(players, m_item.group(1))
                    i += 1
                    continue
                # wenn es wie "name1, name2" aussieht:
                if "," in l2:
                    add_many(players, parse_name_list(l2))
                    i += 1
                    continue
                # sonst Blockende
                break
        else:
            i += 1

    return sorted(players.values(), key=str.lower)

def extract_player_names(log_content: str) -> list[str]:
    players: dict[str, str] = {}

    # Sehr sichere globale Signale
    add_many(players, USER_PATTERN.findall(log_content))

    # Online Players Integration (NEU)
    add_many(players, extract_online_players(log_content))

    # Zeilenweise Events (nur Chat für Kill/Join)
    for raw_line in log_content.splitlines():
        if not raw_line.strip():
            continue
        if should_ignore_line(raw_line):
            continue

        is_chat = "[CHAT]" in raw_line
        cleaned = strip_color_codes(raw_line)

        # Teamlist-ähnliche Lines
        tm = TEAMLINE_PATTERN.match(strip_bracket_tags(cleaned).strip())
        if tm:
            members = strip_bracket_tags(tm.group(2))
            members = re.sub(r"\s+", " ", members).strip()
            tokens = [t for t in re.split(r"[,\s]+", members) if t]
            add_many(players, tokens)
            continue

        if not is_chat:
            continue

        payload = normalize_chat_payload(raw_line)
        if not payload:
            continue

        m = CHAT_NAME_COLON.match(payload)
        if m:
            add_player(players, m.group(1))

        for rx in (JOIN_CHAT, QUIT_CHAT, DISCO_CHAT):
            jm = rx.match(payload)
            if jm:
                add_player(players, jm.group(1))

        for rx in DEATH_PATTERNS:
            dm = rx.match(payload)
            if dm:
                add_player(players, dm.group(1))

        for rx in KILLER_PATTERNS:
            km = rx.search(payload)
            if km:
                add_player(players, km.group(1))

        bm = BED_PATTERN.search(payload)
        if bm:
            add_player(players, bm.group(1))

    return sorted(players.values(), key=str.lower)

def read_text_file(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            return path.read_text(encoding="latin-1")
        except Exception:
            return None
    except Exception:
        return None


input_dir  = Path(r'C:\Users\Hans\.lunarclient\profiles\lunar\1.8\logs') #Hier dein Path zu Lunar
output_dir  = Path(r'C:\Users\Hans\Desktop\lunarFiles') #Hier dein Path zu einem extra Folder, er sollte lastScraped.txt in sich haben (leer ist okay)
last_file = Path(output_dir/'lastScraped.txt')

def updateLastCheck(currentLastLog):
    name = currentLastLog.stem if isinstance(currentLastLog, Path) else str(currentLastLog)
    with open(last_file, "w", encoding="utf-8") as f:
        f.write("LastLog: " + name)
    print("Letztes gescraptes Log gespeichert!")


def getLastCheck(last_file):
    lastlogPath = Path("")  
    try: 
        with open(last_file, 'r') as f_in:
            text = f_in.read()
            lastlogPath = text.split(":", 1)[1].strip()  
            lastlogPath = Path(lastlogPath)
    except:
        print("Letzter decrypteter Log nicht vorhanden, startet beim ersten.")
    finally:
        return lastlogPath if len(lastlogPath.name) > 1 else ""


def decryptFile(log):
    input_file = log
    output_file = output_dir/log.stem
    with gzip.open(input_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f"Datei erfolgreich entpackt: {output_file}")




def main():
    alreadyProcced = True
    lastLogName = getLastCheck(last_file)
    lastDateLog = Path("")
    if lastLogName == "": 
        for log in input_dir.iterdir():
            if log.name.endswith('.log'):
                    output_file = output_dir/log.name
                    with open(log, 'rb') as f_in:
                        with open(output_file, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    print(f'File "{log.stem}" already Log Format')
            elif log.suffixes[-2:] == [".log", ".gz"]:
                lastDateLog = log.stem
                decryptFile(log)
    else: 
        print("Letzter Eintrag bei: " + lastLogName.stem)
        for log in input_dir.iterdir():
            if log.stem != lastLogName.name and alreadyProcced:
                print("Skipped: " + log.stem)
                continue
            elif log.stem == lastLogName.name:
                print("Arrived at last scraped Log: ", lastLogName.name)
                print("-"*60)
                print('Decrypted ab jetzt die nächsten Einträge')
                alreadyProcced = False
            elif alreadyProcced == False:
                if log.name.endswith('.log'):
                    output_file = output_dir/log.name
                    with open(log, 'rb') as f_in:
                        with open(output_file, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    print(f'File "{log.stem}" already Log Format')
                elif log.suffixes[-2:] == [".log", ".gz"]:
                    lastDateLog = log
                    decryptFile(log)
    
    updateLastCheck(lastDateLog)
    print("Fertig")
    print("-" * 60)


    #Start von Extraction
    parser = argparse.ArgumentParser(description="Extract Minecraft/Hypixel player names from log files.")
    parser.add_argument(
        "log_dir",
        nargs="?",
        default=output_dir,
        help=f"Directory containing .log files (default:{output_dir})",
    )
    parser.add_argument(
        "--out",
        default="alle_spieler.txt",
        help="Output text file (default: alle_spieler.txt)",
    )
    args = parser.parse_args()

    log_directory = Path(args.log_dir)
    log_files = sorted(log_directory.glob("*.log"))

    if not log_files:
        print("Keine .log Dateien gefunden!")
        print(f"Pfad: {log_directory}")
        return

    print(f"Gefundene Log-Dateien: {len(log_files)}")
    print("=" * 50)

    all_players: dict[str, str] = {}

    def add_global(name: str) -> None:
        if not name:
            return
        all_players.setdefault(name.lower(), name)

    for log_file in log_files:
        print(f"Analysiere: {log_file.name}")

        log_content = read_text_file(log_file)
        if log_content is None:
            print("Fehler beim Lesen (Encoding/IO).")
            continue

        found = extract_player_names(log_content)
        for p in found:
            add_global(p)

        print(f"{len(found)} Spieler gefunden (Gesamt bisher: {len(all_players)})")

    sorted_players = sorted(all_players.values(), key=str.lower)

    print("\n" + "=" * 50)
    print(f"ALLE GEFUNDENEN SPIELER ({len(sorted_players)} eindeutige Namen, case-insensitive):")
    print("=" * 50)
    for i, player in enumerate(sorted_players, 1):
        print(f"{i:4d}. {player}")

    output_file = Path(args.out)
    with output_file.open("w", encoding="utf-8") as f:
        f.write(f"Extrahierte Spielernamen aus {len(log_files)} Log-Dateien\n")
        f.write(f"Eindeutige Spieler (case-insensitive): {len(sorted_players)}\n")
        f.write("=" * 50 + "\n\n")
        for player in sorted_players:
            f.write(f"{player}\n")

    print(f"\nListe wurde in '{output_file}' gespeichert!")
    print(f"Statistik: {len(log_files)} Dateien analysiert, {len(sorted_players)} eindeutige Spieler gefunden")
    
   
        

     
    


if __name__ == "__main__":
    main()