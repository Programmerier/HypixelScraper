import json
import re
import time
import requests
import concurrent.futures
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ─────────────────────────────────────────────
HYPIXEL_API_KEY = "1323f537-aa2a-4571-a31e-32539c431c8d"
# ─────────────────────────────────────────────

PLATFORMS = {
    "twitter.com":   "Twitter/X",
    "x.com":         "Twitter/X",
    "youtube.com":   "YouTube",
    "twitch.tv":     "Twitch",
    "instagram.com": "Instagram",
    "discord.gg":    "Discord",
    "tiktok.com":    "TikTok",
    "reddit.com":    "Reddit",
    "github.com":    "GitHub",
}

_pw      = None
_browser = None
_context = None


# ══════════════════════════════════════════════
# Playwright – Browser starten / stoppen
# ══════════════════════════════════════════════

def start_browser():
    global _pw, _browser, _context
    print("⏳  Starte Chromium (Cloudflare-Bypass) ...")
    _pw      = sync_playwright().start()
    _browser = _pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
    )
    _context = _browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="de-DE",
        viewport={"width": 1280, "height": 800},
        extra_http_headers={"Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8"},
    )
    _context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
    )
    print("✅  Browser bereit.\n")


def stop_browser():
    global _browser, _pw
    if _browser:
        _browser.close()
    if _pw:
        _pw.stop()


def fetch_namemc_html(name: str) -> str | None:
    """Lädt namemc.com/profile/{name} im echten Chromium und gibt HTML zurück."""
    url  = f"https://namemc.com/profile/{name}"
    page = _context.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        for _ in range(15):
            if "Just a moment" not in page.title():
                break
            time.sleep(1)
        else:
            print(f"  [Warnung] Cloudflare-Challenge für '{name}' nicht gelöst.")
            return None
        try:
            page.wait_for_selector("body", timeout=10_000)
        except PWTimeout:
            pass
        return page.content()
    except Exception as e:
        print(f"  [Fehler] Playwright → '{url}': {e}")
        return None
    finally:
        page.close()


# ══════════════════════════════════════════════
# NameMC – Namenshistorie & Socials
# ══════════════════════════════════════════════

def fmt_time_tag(tag) -> str:
    """Liest <time datetime="..."> und gibt lesbares Datum zurück."""
    if tag is None:
        return "Original"
    raw = tag.get("datetime") or tag.get_text(strip=True)
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y %H:%M UTC")
    except Exception:
        return raw or "Original"


def scrape_namemc(name: str) -> dict:
    result = {
        "url": f"https://namemc.com/profile/{name}",
        "name_history": [],
        "socials": [],
        "skin_count": 0,
    }

    html = fetch_namemc_html(name)
    if not html:
        return result

    soup = BeautifulSoup(html, "html.parser")

    # ── Namenshistorie ──────────────────────────────────────────
    table = None

    for header in soup.find_all(class_="card-header"):
        if "Namensverlauf" in header.get_text() or "Name History" in header.get_text():
            body = header.find_next_sibling(class_="card-body")
            if body:
                table = body.find("table")
            break

    if not table:
        table = soup.find("table", class_="table-borderless")
    if not table:
        table = soup.find("table", id="namemc-names")

    if table:
        seen = set()
        for row in table.find_all("tr"):
            if "d-lg-none" in row.get("class", []):
                continue
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            a_tag = cells[1].find("a")
            n = (a_tag.get_text(strip=True) if a_tag
                 else cells[1].get_text(strip=True)).strip()
            if not n or n in seen:
                continue
            seen.add(n)
            changed = fmt_time_tag(row.find("time"))
            result["name_history"].append({"name": n, "changed_at": changed})

    if not result["name_history"]:
        for li in soup.select("ol li, ul.names li, .list-group-item"):
            raw     = li.get_text(" ", strip=True)
            n       = raw.split()[0] if raw else ""
            changed = fmt_time_tag(li.find("time"))
            if n:
                result["name_history"].append({"name": n, "changed_at": changed})

    # ── Soziale Medien ──────────────────────────────────────────
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        for domain, label in PLATFORMS.items():
            if domain in href:
                if not any(s["url"] == href for s in result["socials"]):
                    result["socials"].append({"platform": label, "url": href})

    # ── Skin-Anzahl ─────────────────────────────────────────────
    skins_div = soup.find(id="skins") or soup.find(class_="skins")
    if skins_div:
        result["skin_count"] = len(
            skins_div.find_all("canvas") or skins_div.find_all("img")
        )

    return result


# ══════════════════════════════════════════════
# Mojang API – UUID & aktueller Name
# ══════════════════════════════════════════════

def get_uuid(name: str) -> dict | None:
    try:
        r = requests.get(
            f"https://api.mojang.com/users/profiles/minecraft/{name}", timeout=10
        )
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"  [Fehler] Mojang-API: {e}")
    return None


# ══════════════════════════════════════════════
# Hypixel API
# ══════════════════════════════════════════════

def get_hypixel_data(uuid: str) -> dict | None:
    if not HYPIXEL_API_KEY or HYPIXEL_API_KEY == "DEIN-API-KEY-HIER":
        print("  [Hinweis] Kein Hypixel-API-Key – Hypixel wird übersprungen.")
        return None
    try:
        r = requests.get(
            "https://api.hypixel.net/player",
            params={"key": HYPIXEL_API_KEY, "uuid": uuid},
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("success") and data.get("player"):
                return data["player"]
            print(f"  [Hypixel] Kein Spieler (UUID: {uuid})")
    except Exception as e:
        print(f"  [Fehler] Hypixel-API: {e}")
    return None


def ms_to_dt(ms) -> str:
    if not ms:
        return "Unbekannt"
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime(
        "%d.%m.%Y %H:%M UTC"
    )


def name_at_first_login(name_history: list, first_login_ms: int) -> str:
    """Ermittelt welchen Namen der Account beim ersten Hypixel-Login hatte."""
    timed = []
    for entry in name_history:
        ts  = None
        raw = entry.get("changed_at", "")
        if raw not in ("Original", "?", ""):
            try:
                dt = datetime.strptime(raw, "%d.%m.%Y %H:%M UTC").replace(
                    tzinfo=timezone.utc
                )
                ts = int(dt.timestamp() * 1000)
            except Exception:
                pass
        timed.append({"name": entry["name"], "ts": ts})

    active = timed[-1]["name"]
    for entry in reversed(timed[:-1]):
        if entry["ts"] is None:
            continue
        if entry["ts"] <= first_login_ms:
            active = entry["name"]
        else:
            break

    return active


# ══════════════════════════════════════════════
# Web-Suche via DuckDuckGo
# ══════════════════════════════════════════════

def google_search(name: str, suffix: str, num_results: int = 5) -> list[dict]:
    """Suche via DuckDuckGo für '{name} {suffix}' – gibt Top-Links mit Begründung zurück.
    
    Rückgabe: Liste von Dicts mit 'url', 'found_reasons'
    Benötigt: pip install duckduckgo_search
    """
    try:
        from ddgs import DDGS
        
        with DDGS() as ddgs:
            results = list(ddgs.text(f'{name} {suffix}', max_results=num_results))
            output = []
            
            for r in results:
                if "href" not in r:
                    continue
                
                title = r.get("title", "").lower()
                snippet = r.get("body", "").lower()
                combined_text = f"{title} {snippet}".lower()
                
                reasons = []
                
                # Prüfe ob Name vorhanden ist
                if name.lower() in combined_text:
                    reasons.append(name)
                
                # Prüfe ob Suffix vorhanden ist
                if suffix.lower() in combined_text:
                    reasons.append(suffix)
                
                output.append({
                    "url": r["href"],
                    "found_reasons": reasons if reasons else ["Suchergebnis gefunden"]
                })
            
            return output
    except ImportError:
        print("  [Fehler] Paket fehlt – bitte installieren: pip install duckduckgo_search")
        return []
    except Exception as e:
        print(f"  [Fehler] Suche '{name} {suffix}': {e}")
        return []


# ══════════════════════════════════════════════
# OSINT – Username-Suche auf Social-Plattformen
# ══════════════════════════════════════════════

OSINT_PLATFORMS = [
    ("Instagram",
        "https://www.instagram.com/{u}/",
        ["page not found", "sorry, this page isn't available", "isn't available"],
        [], ""),
    ("GitHub",
        "https://github.com/{u}",
        ["not found", "this is not the web page you are looking for"],
        ["repositories", "contributions", "followers"], ""),
    ("X / Twitter",
        "https://x.com/{u}",
        ["this account doesn't exist", "account suspended", "caution: this account"],
        [], ""),
    ("TikTok",
        "https://www.tiktok.com/@{u}",
        ["couldn't find this account", "page not found"],
        [], ""),
    ("Reddit",
        "https://www.reddit.com/user/{u}/",
        ["sorry, nobody on reddit goes by that name", "page not found",
         "this account has been suspended", "must be 18"],
        [], ""),
    ("Twitch",
        "https://www.twitch.tv/{u}",
        ["sorry. unless you've got a time machine", "this channel does not exist",
         "page not found"],
        [], ""),
    ("Steam",
        "https://steamcommunity.com/id/{u}",
        ["the specified profile could not be found",
         "an error was encountered while processing your request"],
        ["steamid", "profile", "games"], ""),
    ("Kick",
        "https://kick.com/{u}",
        ["page not found", "404"],
        [], ""),
    ("SoundCloud",
        "https://soundcloud.com/{u}",
        ["we can't find that user", "page not found"],
        [], ""),
    ("Pinterest",
        "https://www.pinterest.com/{u}/",
        ["sorry! we couldn't find that page", "page not found"],
        [], ""),
    ("Snapchat",
        "https://www.snapchat.com/add/{u}",
        ["this snapchat user doesn't exist", "page not found", "sorry, we couldn't find"],
        [], ""),
    ("Spotify",
        "https://open.spotify.com/user/{u}",
        ["user not found", "page not found"],
        [], ""),
    ("Chess.com",
        "https://www.chess.com/member/{u}",
        ["oops! we couldn't find that page", "member not found", "404"],
        ["chess.com/member"], ""),
    ("Roblox",
        "https://www.roblox.com/user.aspx?username={u}",
        ["page not found", "this content is currently unavailable"],
        [], ""),
    ("LinkedIn",
        "https://www.linkedin.com/in/{u}/",
        ["page not found", "this linkedin page doesn't exist", "profile not available"],
        [], ""),
    ("Telegram",
        "https://t.me/{u}",
        ["if you have telegram, you can contact", "no username"],
        [], ""),
    ("VK",
        "https://vk.com/{u}",
        ["this page does not exist", "page not found"],
        [], ""),
    ("Patreon",
        "https://www.patreon.com/{u}",
        ["page not found", "hmm, we couldn't find that page"],
        [], ""),
    ("Mastodon",
        "https://mastodon.social/@{u}",
        ["this profile is not available", "page not found", "404"],
        [], ""),
    ("Linktree",
        "https://linktr.ee/{u}",
        ["sorry, this page isn't available", "not found"],
        [], ""),
    ("Behance",
        "https://www.behance.net/{u}",
        ["page not found", "404"],
        [], ""),
]

_OSINT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

_GENERIC_NOT_FOUND = [
    "404", "page not found", "not found", "does not exist",
    "no longer available", "account suspended", "account terminated",
    "this user doesn't exist", "user doesn't exist", "we can't find",
    "could not be found", "nothing here", "oops", "went wrong",
    "error 404", "404 error",
]

_REAL_CONTENT_SIGNALS = [
    "follower", "following", "subscribe", "posts", "videos", "tweets",
    "profile", "joined", "member since", "bio", "about", "links",
    "likes", "comments", "share", "message",
]


def _html_verdict(html: str, url: str, username: str,
                  not_found_strings: list, confirm_strings: list) -> tuple[bool, str]:
    soup      = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True).lower()

    for nf in not_found_strings + _GENERIC_NOT_FOUND:
        if nf.lower() in page_text:
            return False, f"Seite enthält '{nf}'"

    if soup.find("form", {"action": lambda a: a and ("login" in a or "signin" in a)}):
        return False, "Login-Formular gefunden – kein öffentliches Profil"

    for cs in confirm_strings:
        if cs.lower() in page_text:
            return True, f"Bestätigt durch '{cs}'"

    signals_found = [s for s in _REAL_CONTENT_SIGNALS if s in page_text]
    if len(signals_found) >= 2:
        return True, f"Inhalt erkannt ({', '.join(signals_found[:3])})"

    title = soup.find("title")
    h1    = soup.find("h1")
    if title and username.lower() in title.get_text().lower():
        return True, "Username im Titel gefunden"
    if h1 and username.lower() in h1.get_text().lower():
        return True, "Username in H1 gefunden"

    if len(page_text) < 200:
        return False, "Seite zu kurz / leer"

    return False, "Kein eindeutiger Profilinhalt gefunden"


def _check_youtube(username: str) -> dict | None:
    """Prüft ob ein YouTube-Kanal (@username) existiert via ytInitialData-JSON."""
    url = f"https://www.youtube.com/@{username}"
    try:
        r = requests.get(url, headers=_OSINT_HEADERS, timeout=12, allow_redirects=True)
        if r.status_code in (404, 410):
            return None

        match = re.search(r"ytInitialData\s*=\s*(\{)", r.text)
        if not match:
            return None

        start = match.start(1)
        depth = 0
        end   = start
        for i, ch in enumerate(r.text[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break

        try:
            data = json.loads(r.text[start:end])
        except json.JSONDecodeError:
            return None

        header = data.get("header", {})
        if not header:
            return None

        for alert in data.get("alerts", []):
            alert_text = str(alert).lower()
            if "not available" in alert_text or "does not exist" in alert_text:
                return None

        return {
            "platform": "YouTube",
            "url":      url,
            "status":   r.status_code,
            "reason":   "ytInitialData enthält Channel-Header",
        }
    except Exception:
        return None


def _check_one_platform(args) -> dict | None:
    """Prüft eine Plattform auf einen Username mit tiefer HTML-Analyse."""
    platform, url_tpl, not_found_strings, confirm_strings, _, username = args
    url = url_tpl.replace("{u}", username)
    try:
        r = requests.get(
            url,
            headers=_OSINT_HEADERS,
            timeout=12,
            allow_redirects=True,
        )
        if r.status_code in (404, 410, 451):
            return None

        found, reason = _html_verdict(
            r.text, url, username, not_found_strings, confirm_strings
        )
        if found:
            return {
                "platform": platform,
                "url":      url,
                "status":   r.status_code,
                "reason":   reason,
            }
    except Exception:
        pass
    return None


def search_username_osint(usernames: list[str], indent: str = "") -> dict[str, list]:
    """Sucht alle Usernames parallel auf allen OSINT_PLATFORMS."""
    results      = {}
    all_usernames = list({u.lower(): u for u in usernames}.values())

    print(f"\n{indent}🌐  OSINT-Suche für {len(all_usernames)} Username(n) "
          f"auf {len(OSINT_PLATFORMS)} Plattformen ...")

    for username in all_usernames:
        print(f"{indent}    🔎  Suche: {username}")
        tasks = [
            (platform, url_tpl, not_found, confirm, selector, username)
            for platform, url_tpl, not_found, confirm, selector in OSINT_PLATFORMS
        ]
        hits = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            futures   = {ex.submit(_check_one_platform, t): t for t in tasks}
            yt_future = ex.submit(_check_youtube, username)
            for fut in concurrent.futures.as_completed(futures):
                res = fut.result()
                if res:
                    hits.append(res)
            yt_res = yt_future.result()
            if yt_res:
                hits.append(yt_res)

        hits.sort(key=lambda x: x["platform"])
        results[username] = hits

        if hits:
            print(f"{indent}    ✅  {len(hits)} Treffer für '{username}':")
            for h in hits:
                print(f"{indent}        • {h['platform']:<15}  {h['url']}")
            print(f"{indent}          {'':15}  ✓ {h['reason']}")
        else:
            print(f"{indent}    ❌  Keine Accounts für '{username}' gefunden.")

    return results


# ══════════════════════════════════════════════
# Haupt-Analyse
# ══════════════════════════════════════════════

def analyse_player(name: str, depth: int = 0, visited: set = None):
    if visited is None:
        visited = set()

    indent = "  " * depth
    if name.lower() in visited:
        print(f"{indent}↩  '{name}' bereits analysiert.")
        return
    visited.add(name.lower())

    print(f"\n{'═'*60}")
    print(f"{indent}🔍  Spieler: {name}")
    print(f"{'═'*60}")

    # 1) UUID
    mojang = get_uuid(name)
    if not mojang:
        print(f"{indent}❌  Kein Minecraft-Account für '{name}'")
        return
    uuid     = mojang["id"]
    cur_name = mojang["name"]
    print(f"{indent}✅  UUID:           {uuid}")
    print(f"{indent}✅  Aktueller Name:  {cur_name}")

    # 2) NameMC
    print(f"\n{indent}📋  NameMC  →  namemc.com/profile/{cur_name}")
    nm = scrape_namemc(cur_name)

    if nm["name_history"]:
        print(f"{indent}    Namenshistorie ({len(nm['name_history'])} Einträge):")
        history = nm["name_history"]
        for i, e in enumerate(history):
            von = e["changed_at"] if e["changed_at"] not in ("Original", "?", "") else "Erstellung"
            bis = "present" if i == 0 else history[i - 1]["changed_at"]
            if bis in ("Original", "?", ""):
                bis = "?"
            def short(d):
                if d in ("present", "Erstellung", "?"):
                    return d
                return d.split(" ")[0]
            print(f"{indent}      • {e['name']:<22}  {short(von)}  →  {short(bis)}")
    else:
        print(f"{indent}    ⚠  Keine Namenshistorie gefunden.")

    if nm["socials"]:
        print(f"{indent}    Verknüpfte Socials:")
        for s in nm["socials"]:
            print(f"{indent}      • {s['platform']}: {s['url']}")
    else:
        print(f"{indent}    Keine Socials gefunden.")

    if nm["skin_count"]:
        print(f"{indent}    Gespeicherte Skins: {nm['skin_count']}")

    # 3) Hypixel
    print(f"\n{indent}🎮  Hypixel-Daten:")
    hp          = get_hypixel_data(uuid)
    first_login = None
    if hp:
        first_login = hp.get("firstLogin")
        last_login  = hp.get("lastLogin")
        rank        = hp.get("newPackageRank") or hp.get("packageRank") or "Kein Rank"
        xp          = hp.get("networkExp", 0)

        print(f"{indent}    Erster Login:   {ms_to_dt(first_login)}")
        print(f"{indent}    Letzter Login:  {ms_to_dt(last_login)}")
        print(f"{indent}    Rank:           {rank}")
        print(f"{indent}    Netzwerk-XP:    {xp:,.0f}")

        if nm["name_history"] and first_login:
            name_then = name_at_first_login(nm["name_history"], first_login)
            print(f"\n{indent}    📅  Name beim ersten Hypixel-Login:")
            print(f"{indent}        → '{name_then}'")
            old = [e["name"] for e in nm["name_history"]
                   if e["name"].lower() != cur_name.lower()]
            if old:
                print(f"{indent}        → Alle früheren Namen: {', '.join(old)}")
    else:
        print(f"{indent}    Kein Hypixel-Profil oder API-Key fehlt.")

    # 4) Google-Suche
    print(f"\n{indent}🔎  Google-Suche:")
    for suffix in ["minecraft", "hypixel"]:
        results = google_search(cur_name, suffix)
        print(f"{indent}    '{cur_name} {suffix}':")
        if results:
            for res in results:
                print(f"{indent}      • {res['url']}")
                print(f"{indent}        ✓ {', '.join(res['found_reasons'])}")
        else:
            print(f"{indent}      Keine Ergebnisse gefunden.")
    #Hier auch
    # 5) OSINT – Username-Suche
    all_names = (
        list({e["name"].lower(): e["name"] for e in nm["name_history"]}.values())
        if nm["name_history"] else [cur_name]
    )
    search_username_osint(all_names, indent=indent)

    # 6) Rekursion – alte Namen (1 Ebene)
    if depth == 0 and nm["name_history"]:
        old = [e["name"] for e in nm["name_history"]
               if e["name"].lower() != cur_name.lower()]
        if old:
            print(f"\n{indent}🔄  Analysiere {len(old)} früheren Namen rekursiv ...")
            for old_name in old:
                time.sleep(1.5)
                analyse_player(old_name, depth=1, visited=visited)


# ══════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════

if __name__ == "__main__":
    print("╔══════════════════════════════════════════════╗")
    print("║   NameMC + Hypixel History Tracker           ║")
    print("║   Cloudflare-Bypass via Playwright           ║")
    print("╚══════════════════════════════════════════════╝\n")

    player_input = input("Spielername eingeben: ").strip()
    if not player_input:
        print("Kein Name eingegeben.")
    else:
        start_browser()
        try:
            analyse_player(player_input)
        finally:
            stop_browser()
        print("\n✔  Analyse abgeschlossen.")