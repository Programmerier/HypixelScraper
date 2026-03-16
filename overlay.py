"""
BedWars Team Proxy (Hypixel 1.8.9)
-----------------------------------
pip install quarry twisted
python proxy.py
Server im Client: localhost:25565
"""

import logging
import json
import os
import re
from collections import defaultdict

from quarry.net.proxy import DownstreamFactory, Bridge
from quarry.net.auth import Profile
from quarry.types.uuid import UUID
from twisted.internet import reactor, endpoints

logging.basicConfig(level=logging.WARNING, format="%(message)s")

# ───────────────── Auth ─────────────────

def load_profile():
    path = r"C:\Users\Hans\.lunarclient\settings\game\accounts.json"

    if os.path.exists(path):
        with open(path, encoding="utf8") as f:
            data = json.load(f)

        accounts = data["accounts"]
        active = data["activeAccountLocalId"]
        acc = accounts[active]

        token = acc["accessToken"]
        name = acc["minecraftProfile"]["name"]
        uuid = acc["minecraftProfile"]["id"].replace("-", "")

        print("[Auth] Account geladen:", name)
        return Profile("quarry", token, name, UUID.from_hex(uuid))

    raise RuntimeError("Account nicht gefunden")

PROFILE = load_profile()

# ───────────────── Farben ─────────────────

COLOR_MAP = {
    "§c": "Red",
    "§9": "Blue",
    "§a": "Green",
    "§e": "Yellow",
    "§d": "Pink",
    "§b": "Aqua",
    "§7": "Gray",
    "§f": "White",
}

COLOR_NAMES = ["Red", "Blue", "Green", "Yellow", "Pink", "Aqua", "Gray", "White"]

STRIP = re.compile(r"§.")

def strip(text):
    return STRIP.sub("", text)

def color_from_prefix(text):
    for code, name in COLOR_MAP.items():
        if code in text:
            return name
    return None

def color_from_team_name(team_name):
    for color in COLOR_NAMES:
        if team_name.startswith(color):
            return color
    return None

def resolve_color(team_name, prefix, colors_dict):
    if team_name in colors_dict:
        return colors_dict[team_name]
    c = color_from_prefix(prefix) if prefix else None
    if c:
        return c
    return color_from_team_name(team_name)

# ───────────────── Game State ─────────────────

class GameState:

    def __init__(self):
        self.reset()

    def reset(self):
        self.players = set()
        self.teams = defaultdict(set)
        self.colors = {}
        self.started = False

    def start(self):
        # Lobby-Daten wegwerfen, frisch sammeln
        self.teams = defaultdict(set)
        self.colors = {}
        self.started = True

    def print(self):
        if not self.started:
            return

        clean_teams = {}
        for color, players in self.teams.items():
            if color is None:
                continue
            clean = {p for p in players if p.isprintable() and len(p) <= 20}
            if clean:
                clean_teams[color] = clean

        if not clean_teams:
            print("[Debug] Keine Teams empfangen")
            return

        print("\n" + "="*50)
        print(" BEDWARS TEAMS")
        print("="*50)


        

        for color in sorted(clean_teams):
            players = ", ".join(sorted(clean_teams[color]))
            print(f" {color:<10} {players}")

        print("="*50+"\n")
        print(clean_teams)
        
        

# ───────────────── Bridge ─────────────────

class BedwarsBridge(Bridge):

    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self.state = GameState()

    def make_profile(self):
        return PROFILE

    # ───── Teams (Scoreboard) ─────

    def packet_downstream_teams(self, buff):
        start = buff.save()

        try:
            name = buff.unpack_string()
            mode = buff.unpack("b")

            if mode == 0:
                buff.unpack_string()
                prefix = buff.unpack_string()
                buff.unpack_string()
                buff.unpack("b")
                buff.unpack_string()

                color = resolve_color(name, prefix, self.state.colors)
                if color:
                    self.state.colors[name] = color

                count = buff.unpack_varint()
                players = [buff.unpack_string() for _ in range(count)]

                if color and self.state.started:
                    for p in players:
                        self.state.teams[color].add(p)

            elif mode == 3:
                count = buff.unpack_varint()
                players = [buff.unpack_string() for _ in range(count)]

                color = resolve_color(name, None, self.state.colors)
                if color:
                    self.state.colors[name] = color
                    if self.state.started:
                        for p in players:
                            self.state.teams[color].add(p)

        except:
            pass

        buff.restore()
        self.downstream.send_packet("teams", buff.read())

    # ───── Player List ─────

    def packet_downstream_player_list_item(self, buff):
        start = buff.save()

        try:
            action = buff.unpack_varint()
            count = buff.unpack_varint()

            for _ in range(count):
                uuid = buff.unpack_uuid()

                if action == 0:
                    name = buff.unpack_string()
                    self.state.players.add(name)

        except:
            pass

        buff.restore()
        self.downstream.send_packet("player_list_item", buff.read())

    # ───── Chat ─────

    def packet_downstream_chat_message(self, buff):
        start = buff.save()

        try:
            text = strip(buff.unpack_string())

            if "Protect your bed" in text:
                self.state.start()
                reactor.callLater(3, self.state.print)

        except:
            pass

        buff.restore()
        self.downstream.send_packet("chat_message", buff.read())

    # ───── Block Lunar plugin packets ─────

    def packet_upstream_plugin_message(self, buff):
        try:
            buff.read()
        except:
            pass
        return

# ───────────────── Factory ─────────────────

class BedwarsFactory(DownstreamFactory):
    bridge_class = BedwarsBridge
    connect_host = "mc.hypixel.net"
    connect_port = 25565
    online_mode = False

# ───────────────── Start ─────────────────

def main():

    factory = BedwarsFactory()

    endpoint = endpoints.serverFromString(reactor, "tcp:25565")
    endpoint.listen(factory)

    print("="*50)
    print(" BedWars Team Proxy gestartet")
    print("="*50)
    print("Server im Client: localhost:25565")
    print("="*50)

    reactor.run()

if __name__ == "__main__":
    main()